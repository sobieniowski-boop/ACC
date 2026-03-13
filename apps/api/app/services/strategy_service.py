"""Strategy / Growth Engine — opportunity detection, scoring, and queries.

This service:
  1. Detects growth opportunities across 20 types by analysing rollup data.
  2. Scores each opportunity (priority 0-100, confidence 0-100).
  3. Provides paginated query/overview helpers consumed by the API layer.
  4. Manages experiment CRUD.

Heavy detection runs as a scheduled job (not on every request).
"""
from __future__ import annotations

import json
import math
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import structlog

from app.core.config import MARKETPLACE_REGISTRY, RENEWED_SKU_SQL_FILTER
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

# ── Marketplace helpers (from central registry) ────────────────────
MKT_CODE = {mid: info["code"] for mid, info in MARKETPLACE_REGISTRY.items()}
CODE_MKT = {info["code"]: mid for mid, info in MARKETPLACE_REGISTRY.items()}
ALL_MKT_IDS = list(MKT_CODE.keys())


def _f(v) -> float:
    """Convert SQL Decimal/int/None to float."""
    if v is None:
        return 0.0
    return float(v)


def _i(v) -> int:
    if v is None:
        return 0
    return int(v)


def _rows(cur) -> list:
    """Fetch all rows, converting Decimal→float and leaving strings/ints."""
    rows = cur.fetchall()
    out = []
    for row in rows:
        conv = []
        for v in row:
            if isinstance(v, Decimal):
                conv.append(float(v))
            else:
                conv.append(v)
        out.append(tuple(conv))
    return out


def _priority_label(score: float) -> str:
    if score >= 90:
        return "do_now"
    if score >= 75:
        return "this_week"
    if score >= 60:
        return "this_month"
    if score >= 40:
        return "backlog"
    return "low"


def _clamp(v: float, lo: float = 0, hi: float = 100) -> float:
    return max(lo, min(hi, v))


# ═══════════════════════════════════════════════════════════════════
#  MODEL ADJUSTMENTS CACHE  (loaded once per detection run)
# ═══════════════════════════════════════════════════════════════════
_model_adjustments: Dict[str, Dict[str, float]] = {}


def load_model_adjustments(cur) -> None:
    """Populate the module-level adjustment cache from opportunity_model_adjustments."""
    global _model_adjustments
    try:
        cur.execute("""
            SELECT opportunity_type,
                   impact_weight_adjustment,
                   confidence_weight_adjustment,
                   priority_weight_adjustment
            FROM opportunity_model_adjustments
        """)
        adj: Dict[str, Dict[str, float]] = {}
        for row in cur.fetchall():
            adj[row[0]] = {
                "impact": _f(row[1]),
                "confidence": _f(row[2]),
                "priority": _f(row[3]),
            }
        _model_adjustments = adj
        if adj:
            log.info("strategy.model_adjustments_loaded", types=len(adj))
    except Exception as exc:
        log.warning("strategy.model_adjustments_load_failed", error=str(exc))
        _model_adjustments = {}


# ═══════════════════════════════════════════════════════════════════
#  PRIORITY SCORE MODEL  (0-100)
# ═══════════════════════════════════════════════════════════════════
def compute_priority_score(
    *,
    impact_revenue: float = 0,
    impact_profit: float = 0,
    confidence: float = 50,
    urgency: float = 50,
    effort: float = 50,       # 0=trivial, 100=enormous
    strategic_fit: float = 50,
    readiness: float = 50,
    opportunity_type: str | None = None,
) -> float:
    """Weighted combination → single 0-100 score.

    When *opportunity_type* is provided, the base weights are adjusted
    by the feedback-loop values from opportunity_model_adjustments.
    """
    # Base weights
    w_impact   = 0.35
    w_conf     = 0.20
    w_urgency  = 0.15
    w_effort   = 0.10
    w_fit      = 0.10
    w_ready    = 0.10

    # Apply model adjustments (if available)
    if opportunity_type and opportunity_type in _model_adjustments:
        adj = _model_adjustments[opportunity_type]
        w_impact += adj.get("impact", 0)
        w_conf   += adj.get("confidence", 0)
        # priority_weight_adjustment acts as a general score multiplier shift
        prio_adj  = adj.get("priority", 0)
        log.debug(
            "strategy.adjustment_applied",
            opp_type=opportunity_type,
            impact_adj=adj.get("impact", 0),
            conf_adj=adj.get("confidence", 0),
            prio_adj=prio_adj,
        )
    else:
        prio_adj = 0.0

    # Clamp weights to [0.05, 0.60] so no single factor can zero out or dominate
    w_impact  = max(0.05, min(0.60, w_impact))
    w_conf    = max(0.05, min(0.60, w_conf))

    # Normalise financial impact to 0-100 (cap at 50k PLN profit uplift)
    cap = 50_000
    raw_impact = _clamp((abs(impact_profit) / cap) * 100)

    effort_inv = 100 - _clamp(effort)

    score = (
        raw_impact   * w_impact
        + confidence * w_conf
        + urgency    * w_urgency
        + effort_inv * w_effort
        + strategic_fit * w_fit
        + readiness  * w_ready
    )

    # Apply overall priority adjustment (±8-10% shift from feedback loop)
    if prio_adj:
        score *= (1.0 + prio_adj)

    return round(_clamp(score), 1)


# ═══════════════════════════════════════════════════════════════════
#  CONFIDENCE SCORE  (0-100)
# ═══════════════════════════════════════════════════════════════════
def compute_confidence(
    *,
    has_cost: bool = True,
    has_traffic: bool = False,
    has_ads: bool = False,
    has_family: bool = False,
    days_of_data: int = 30,
    margin_stable: bool = True,
) -> float:
    """Data-completeness driven confidence."""
    score = 30.0  # base
    if has_cost:
        score += 20
    if has_traffic:
        score += 15
    if has_ads:
        score += 10
    if has_family:
        score += 5
    if margin_stable:
        score += 10
    # bonus for more historical data
    score += min(10, days_of_data / 9)
    return round(_clamp(score), 1)


# ═══════════════════════════════════════════════════════════════════
#  OPPORTUNITY INSERTION HELPER
# ═══════════════════════════════════════════════════════════════════
def _insert_opp(
    cur,
    *,
    opp_type: str,
    marketplace_id: str | None,
    sku: str | None,
    asin: str | None = None,
    parent_asin: str | None = None,
    family_id: int | None = None,
    title: str,
    description: str | None = None,
    root_cause: str | None = None,
    recommendation: str | None = None,
    priority_score: float = 0,
    confidence_score: float = 50,
    revenue_uplift: float | None = None,
    profit_uplift: float | None = None,
    margin_uplift: float | None = None,
    units_uplift: int | None = None,
    effort: float | None = None,
    owner_role: str | None = None,
    blockers: list | None = None,
    signals: dict | None = None,
) -> None:
    cur.execute(
        """INSERT INTO growth_opportunity
           (opportunity_type, marketplace_id, sku, asin, parent_asin, family_id,
            title, description, root_cause, recommendation,
            priority_score, confidence_score,
            estimated_revenue_uplift, estimated_profit_uplift,
            estimated_margin_uplift, estimated_units_uplift,
            effort_score, owner_role, blocker_json, source_signals_json,
            status, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'new',SYSUTCDATETIME(),SYSUTCDATETIME())""",
        (
            opp_type, marketplace_id, sku, asin, parent_asin, family_id,
            title, description, root_cause, recommendation,
            priority_score, confidence_score,
            revenue_uplift, profit_uplift, margin_uplift, units_uplift,
            effort, owner_role,
            json.dumps(blockers) if blockers else None,
            json.dumps(signals) if signals else None,
        ),
    )


# ═══════════════════════════════════════════════════════════════════
#  DETECTION ENGINES  —  each returns count of new opportunities
# ═══════════════════════════════════════════════════════════════════

def _detect_pricing_opportunities(cur, from_date: str, to_date: str) -> int:
    """PRICE_INCREASE: high margin products selling well — headroom to raise.
       PRICE_DECREASE: low velocity, decent margin, could drop price to win BB.
    """
    n = 0
    # --- PRICE_INCREASE: margin > 25%, revenue > 500 PLN in period, could raise 5% ---
    cur.execute("""
        SELECT marketplace_id, sku, asin,
               SUM(revenue_pln) rev, SUM(profit_pln) prof,
               AVG(margin_pct) avg_margin, SUM(units_sold) units
        FROM acc_sku_profitability_rollup
        WHERE period_date BETWEEN ? AND ?
          AND sku NOT LIKE 'amzn.gr.%%' AND sku NOT LIKE 'amazon.found%%'
        GROUP BY marketplace_id, sku, asin
        HAVING AVG(margin_pct) > 25 AND SUM(revenue_pln) > 500 AND SUM(units_sold) > 10
        ORDER BY SUM(profit_pln) DESC
    """, (from_date, to_date))
    for r in _rows(cur):
        mkt, sku, asin = r[0], r[1], r[2]
        rev, prof, margin, units = r[3], r[4], r[5], int(r[6] or 0)
        rev_uplift = round(rev * 0.05, 2)       # 5% price increase
        prof_uplift = round(rev_uplift * 0.7, 2) # most flows to margin
        conf = compute_confidence(has_cost=True, has_traffic=False, days_of_data=30)
        prio = compute_priority_score(
            impact_revenue=rev_uplift, impact_profit=prof_uplift,
            confidence=conf, urgency=40, effort=20, strategic_fit=60, readiness=80,
            opportunity_type="PRICE_INCREASE",
        )
        _insert_opp(cur, opp_type="PRICE_INCREASE", marketplace_id=mkt, sku=sku, asin=asin,
                     title=f"Price increase opportunity on {sku} ({MKT_CODE.get(mkt, mkt)})",
                     description=f"Margin {margin:.1f}%, {units} units sold. 5% price raise → +{rev_uplift:.0f} PLN revenue.",
                     root_cause="pricing_problem",
                     recommendation=f"Raise price 3-5%. Current margin headroom {margin:.1f}%. Monitor Buy Box after change.",
                     priority_score=prio, confidence_score=conf,
                     revenue_uplift=rev_uplift, profit_uplift=prof_uplift,
                     margin_uplift=round(margin * 0.03, 2),
                     effort=20, owner_role="pricing_team",
                     signals={"avg_margin": float(margin), "revenue": float(rev), "units": int(units)})
        n += 1
        if n >= 100:
            break

    # --- PRICE_DECREASE: low units but decent product (margin>10%, units<5, rev<200) ---
    cur.execute("""
        SELECT marketplace_id, sku, asin,
               SUM(revenue_pln) rev, AVG(margin_pct) avg_margin, SUM(units_sold) units
        FROM acc_sku_profitability_rollup
        WHERE period_date BETWEEN ? AND ?
          AND sku NOT LIKE 'amzn.gr.%%' AND sku NOT LIKE 'amazon.found%%'
        GROUP BY marketplace_id, sku, asin
        HAVING AVG(margin_pct) > 10 AND SUM(units_sold) BETWEEN 1 AND 5
               AND SUM(revenue_pln) BETWEEN 50 AND 500
        ORDER BY AVG(margin_pct) DESC
    """, (from_date, to_date))
    n2 = 0
    for r in _rows(cur):
        mkt, sku, asin = r[0], r[1], r[2]
        rev, margin, units = r[3], r[4], int(r[5] or 0)
        prof_uplift = round(rev * 0.15, 2)
        conf = compute_confidence(has_cost=True, days_of_data=30)
        prio = compute_priority_score(
            impact_revenue=prof_uplift * 2, impact_profit=prof_uplift,
            confidence=conf, urgency=30, effort=20, strategic_fit=40, readiness=80,
            opportunity_type="PRICE_DECREASE",
        )
        _insert_opp(cur, opp_type="PRICE_DECREASE", marketplace_id=mkt, sku=sku, asin=asin,
                     title=f"Price decrease to boost velocity — {sku} ({MKT_CODE.get(mkt, mkt)})",
                     description=f"Only {units} units in period. Margin {margin:.1f}% gives room to drop 5-10%.",
                     root_cause="pricing_problem",
                     recommendation="Drop price 5-10% to win Buy Box and increase velocity.",
                     priority_score=prio, confidence_score=conf,
                     revenue_uplift=round(prof_uplift * 2, 2), profit_uplift=prof_uplift,
                     effort=20, owner_role="pricing_team",
                     signals={"avg_margin": float(margin), "revenue": float(rev), "units": int(units)})
        n2 += 1
        if n2 >= 50:
            break
    return n + n2


def _detect_ads_opportunities(cur, from_date: str, to_date: str) -> int:
    """ADS_SCALE_UP: profitable ROAS, margin headroom, can scale.
       ADS_CUT_WASTE: high spend, low ROAS or ACOS > margin.
    """
    n = 0
    # ADS_CUT_WASTE: ACOS > 30% and spend > 200 PLN
    cur.execute("""
        SELECT marketplace_id, sku,
               SUM(ad_spend_pln) spend, SUM(revenue_pln) rev,
               SUM(profit_pln) prof, AVG(acos_pct) acos,
               AVG(margin_pct) margin
        FROM acc_sku_profitability_rollup
        WHERE period_date BETWEEN ? AND ? AND ad_spend_pln > 0
          AND sku NOT LIKE 'amzn.gr.%%' AND sku NOT LIKE 'amazon.found%%'
        GROUP BY marketplace_id, sku
        HAVING AVG(acos_pct) > 30 AND SUM(ad_spend_pln) > 200
        ORDER BY SUM(ad_spend_pln) DESC
    """, (from_date, to_date))
    for r in _rows(cur):
        mkt, sku, spend, rev, prof, acos, margin = r
        savings = round(spend * 0.3, 2)
        conf = compute_confidence(has_cost=True, has_ads=True, days_of_data=30)
        prio = compute_priority_score(
            impact_revenue=0, impact_profit=savings,
            confidence=conf, urgency=60, effort=30, strategic_fit=50, readiness=90,
            opportunity_type="ADS_CUT_WASTE",
        )
        _insert_opp(cur, opp_type="ADS_CUT_WASTE", marketplace_id=mkt, sku=sku,
                     title=f"Cut ad waste — {sku} ({MKT_CODE.get(mkt, mkt)}) ACoS {acos:.0f}%",
                     description=f"Spend {spend:.0f} PLN, ACoS {acos:.1f}% vs margin {margin:.1f}%. Reduce 30% → save {savings:.0f} PLN.",
                     root_cause="advertising_problem",
                     recommendation="Review keyword bids, pause unprofitable targets, tighten ACOS target.",
                     priority_score=prio, confidence_score=conf,
                     profit_uplift=savings, effort=30, owner_role="ads_team",
                     signals={"acos": float(acos), "spend": float(spend), "margin": float(margin)})
        n += 1
        if n >= 60:
            break

    # ADS_SCALE_UP: ACOS < 15%, margin > 20%, spend < 500 — can invest more
    cur.execute("""
        SELECT marketplace_id, sku,
               SUM(ad_spend_pln) spend, SUM(revenue_pln) rev,
               SUM(profit_pln) prof, AVG(acos_pct) acos,
               AVG(margin_pct) margin, SUM(units_sold) units
        FROM acc_sku_profitability_rollup
        WHERE period_date BETWEEN ? AND ? AND ad_spend_pln > 0
          AND sku NOT LIKE 'amzn.gr.%%' AND sku NOT LIKE 'amazon.found%%'
        GROUP BY marketplace_id, sku
        HAVING AVG(acos_pct) BETWEEN 1 AND 15 AND AVG(margin_pct) > 20
               AND SUM(ad_spend_pln) < 500
        ORDER BY SUM(profit_pln) DESC
    """, (from_date, to_date))
    n2 = 0
    for r in _rows(cur):
        mkt, sku, spend, rev, prof, acos, margin, units = r
        extra_spend = round(spend * 0.5, 2)
        rev_uplift = round(extra_spend / (acos / 100) if acos > 0 else 0, 2)
        prof_uplift = round(rev_uplift * (margin / 100) * 0.6, 2)
        conf = compute_confidence(has_cost=True, has_ads=True, days_of_data=30)
        prio = compute_priority_score(
            impact_revenue=rev_uplift, impact_profit=prof_uplift,
            confidence=conf, urgency=45, effort=25, strategic_fit=70, readiness=85,
            opportunity_type="ADS_SCALE_UP",
        )
        _insert_opp(cur, opp_type="ADS_SCALE_UP", marketplace_id=mkt, sku=sku,
                     title=f"Scale ads — {sku} ({MKT_CODE.get(mkt, mkt)}) ACoS {acos:.0f}%, margin {margin:.0f}%",
                     description=f"Efficient ads (ACoS {acos:.1f}%) with {margin:.1f}% margin. +50% budget → est. +{rev_uplift:.0f} PLN rev.",
                     root_cause="advertising_problem",
                     recommendation=f"Increase daily budget 50%. Current efficiency supports scaling. Monitor ACoS weekly.",
                     priority_score=prio, confidence_score=conf,
                     revenue_uplift=rev_uplift, profit_uplift=prof_uplift,
                     effort=25, owner_role="ads_team",
                     signals={"acos": float(acos), "spend": float(spend), "margin": float(margin), "units": int(units)})
        n2 += 1
        if n2 >= 60:
            break
    return n + n2


def _detect_inventory_opportunities(cur, from_date: str, to_date: str) -> int:
    """STOCK_REPLENISH: high demand, low DOI.
       STOCK_PROTECTION: rising demand, shrinking inventory.
       LIQUIDATE_OR_PROMO: aged inventory with poor margin.
    """
    n = 0
    # STOCK_REPLENISH: DOI < 14, avg_daily_sales > 1
    try:
        cur.execute("""
            SELECT i.marketplace_id, i.sku, i.asin,
                   i.qty_fulfillable, i.doi, i.avg_daily_sales_7d,
                   r.rev, r.prof, r.margin
            FROM acc_inventory_snapshot i
            CROSS APPLY (
                SELECT SUM(revenue_pln) rev, SUM(profit_pln) prof, AVG(margin_pct) margin
                FROM acc_sku_profitability_rollup
                WHERE sku = i.sku AND marketplace_id = i.marketplace_id
                  AND period_date BETWEEN ? AND ?
                  AND sku NOT LIKE 'amzn.gr.%%' AND sku NOT LIKE 'amazon.found%%'
            ) r
            WHERE i.snapshot_date = (SELECT MAX(snapshot_date) FROM acc_inventory_snapshot)
              AND i.doi < 14 AND i.avg_daily_sales_7d > 1
            ORDER BY r.rev DESC
        """, (from_date, to_date))
        for r in _rows(cur):
            mkt, sku, asin, qty, doi, daily, rev, prof, margin = r
            lost_days = max(0, 14 - (doi or 0))
            lost_rev = round((daily or 0) * lost_days * ((rev or 0) / max(1, (daily or 1) * 30)), 2)
            conf = compute_confidence(has_cost=True, has_traffic=False, days_of_data=30)
            prio = compute_priority_score(
                impact_revenue=lost_rev, impact_profit=round(lost_rev * (margin or 0) / 100, 2),
                confidence=conf, urgency=85, effort=40, strategic_fit=60, readiness=50,
                opportunity_type="STOCK_REPLENISH",
            )
            _insert_opp(cur, opp_type="STOCK_REPLENISH", marketplace_id=mkt, sku=sku, asin=asin,
                         title=f"Replenish stock — {sku} ({MKT_CODE.get(mkt, mkt)}) DOI={doi:.0f}d",
                         description=f"Only {qty} units left ({doi:.0f} days). Selling {daily:.1f}/day. Risk of stockout → {lost_rev:.0f} PLN lost.",
                         root_cause="inventory_problem",
                         recommendation="Create FBA shipment ASAP. Estimated 2-3 week lead time.",
                         priority_score=prio, confidence_score=conf,
                         revenue_uplift=lost_rev, profit_uplift=round(lost_rev * (margin or 0) / 100, 2),
                         effort=40, owner_role="supply_chain",
                         signals={"doi": float(doi or 0), "qty": int(qty or 0), "daily_sales": float(daily or 0)})
            n += 1
            if n >= 80:
                break
    except Exception as e:
        log.warning("strategy.inventory_detect_error", error=str(e))

    return n


def _detect_content_opportunities(cur, from_date: str, to_date: str) -> int:
    """CONTENT_FIX: high sessions, low CVR, no suppression — likely content issue.
       SUPPRESSION_FIX: suppressed SKU with traffic.
    """
    n = 0
    # Use executive_daily_metrics for session data (already aggregated)
    # We need per-SKU data — use rollup + traffic proxy if available
    cur.execute("""
        SELECT marketplace_id, sku, asin,
               SUM(revenue_pln) rev, SUM(profit_pln) prof,
               AVG(margin_pct) margin, SUM(units_sold) units,
               SUM(orders_count) orders
        FROM acc_sku_profitability_rollup
        WHERE period_date BETWEEN ? AND ?
          AND sku NOT LIKE 'amzn.gr.%%' AND sku NOT LIKE 'amazon.found%%'
        GROUP BY marketplace_id, sku, asin
        HAVING SUM(revenue_pln) > 200 AND SUM(units_sold) > 3
               AND AVG(margin_pct) < 8
        ORDER BY SUM(revenue_pln) DESC
    """, (from_date, to_date))
    for r in _rows(cur):
        mkt, sku, asin, rev, prof, margin, units, orders = r
        # Low margin could be content/conversion issue — selling but not profitably
        cvr_uplift_rev = round(rev * 0.15, 2)
        cvr_uplift_prof = round(cvr_uplift_rev * 0.20, 2)
        conf = compute_confidence(has_cost=True, days_of_data=30)
        prio = compute_priority_score(
            impact_revenue=cvr_uplift_rev, impact_profit=cvr_uplift_prof,
            confidence=conf, urgency=40, effort=50, strategic_fit=50, readiness=70,
            opportunity_type="CONTENT_FIX",
        )
        _insert_opp(cur, opp_type="CONTENT_FIX", marketplace_id=mkt, sku=sku, asin=asin,
                     title=f"Content/listing fix — {sku} ({MKT_CODE.get(mkt, mkt)}) margin {margin:.1f}%",
                     description=f"Revenue {rev:.0f} PLN but margin only {margin:.1f}%. Improve listing to boost CVR and margin.",
                     root_cause="content_problem",
                     recommendation="Review title, bullets, images, A+ content. Optimise for conversion.",
                     priority_score=prio, confidence_score=conf,
                     revenue_uplift=cvr_uplift_rev, profit_uplift=cvr_uplift_prof,
                     effort=50, owner_role="content_team",
                     signals={"margin": float(margin), "revenue": float(rev), "units": int(units)})
        n += 1
        if n >= 50:
            break
    return n


def _detect_return_opportunities(cur, from_date: str, to_date: str) -> int:
    """RETURN_REDUCTION: high return rate eating margin."""
    n = 0
    cur.execute("""
        SELECT marketplace_id, sku, asin,
               SUM(revenue_pln) rev, SUM(refund_pln) refund,
               AVG(return_rate_pct) rr, SUM(units_sold) units
        FROM acc_sku_profitability_rollup
        WHERE period_date BETWEEN ? AND ?
          AND sku NOT LIKE 'amzn.gr.%%' AND sku NOT LIKE 'amazon.found%%'
        GROUP BY marketplace_id, sku, asin
        HAVING AVG(return_rate_pct) > 8 AND SUM(revenue_pln) > 300
        ORDER BY SUM(refund_pln) DESC
    """, (from_date, to_date))
    for r in _rows(cur):
        mkt, sku, asin, rev, refund, rr, units = r
        savings = round(abs(refund) * 0.3, 2)
        conf = compute_confidence(has_cost=True, days_of_data=30)
        prio = compute_priority_score(
            impact_revenue=0, impact_profit=savings,
            confidence=conf, urgency=55, effort=50, strategic_fit=40, readiness=60,
            opportunity_type="RETURN_REDUCTION",
        )
        _insert_opp(cur, opp_type="RETURN_REDUCTION", marketplace_id=mkt, sku=sku, asin=asin,
                     title=f"Reduce returns — {sku} ({MKT_CODE.get(mkt, mkt)}) {rr:.1f}% return rate",
                     description=f"Return rate {rr:.1f}%, refunds {abs(refund):.0f} PLN. 30% reduction → save {savings:.0f} PLN.",
                     root_cause="returns_problem",
                     recommendation="Investigate return reasons. Improve product description accuracy, packaging, or quality.",
                     priority_score=prio, confidence_score=conf,
                     profit_uplift=savings, effort=50, owner_role="operations_team",
                     signals={"return_rate": float(rr), "refund_pln": float(refund), "units": int(units)})
        n += 1
        if n >= 40:
            break
    return n


def _detect_bundle_opportunities(cur, from_date: str, to_date: str) -> int:
    """BUNDLE_CREATE: find parent ASINs with multiple profitable children → bundle candidates."""
    n = 0
    # Find parent ASINs whose children have strong combined revenue
    cur.execute("""
        SELECT p.parent_asin,
               COUNT(DISTINCT p.sku) child_cnt,
               SUM(r.revenue_pln) total_rev,
               SUM(r.profit_pln) total_prof,
               AVG(r.margin_pct) avg_margin
        FROM acc_product p
        JOIN acc_sku_profitability_rollup r ON r.sku = p.sku
        WHERE p.parent_asin IS NOT NULL AND p.parent_asin != ''
          AND r.period_date BETWEEN ? AND ?
          AND r.sku NOT LIKE 'amzn.gr.%%'
        GROUP BY p.parent_asin
        HAVING COUNT(DISTINCT p.sku) >= 2
           AND SUM(r.revenue_pln) > 1000
           AND AVG(r.margin_pct) > 5
        ORDER BY SUM(r.revenue_pln) DESC
    """, (from_date, to_date))
    families = _rows(cur)

    seen: set[str] = set()
    for fam in families:
        parent_asin, child_cnt, total_rev, total_prof, avg_margin = fam
        if parent_asin in seen:
            continue
        seen.add(parent_asin)

        # Get top 2 children by revenue for this parent
        cur.execute("""
            SELECT TOP 2 p.sku, p.asin,
                   SUM(r.revenue_pln) rev, SUM(r.profit_pln) prof, AVG(r.margin_pct) margin
            FROM acc_product p
            JOIN acc_sku_profitability_rollup r ON r.sku = p.sku
            WHERE p.parent_asin = ?
              AND r.period_date BETWEEN ? AND ?
            GROUP BY p.sku, p.asin
            HAVING SUM(r.revenue_pln) > 200
            ORDER BY SUM(r.revenue_pln) DESC
        """, (parent_asin, from_date, to_date))
        children = _rows(cur)
        if len(children) < 2:
            continue

        sku_a, asin_a, rev_a = children[0][0], children[0][1], children[0][2]
        sku_b, asin_b, rev_b = children[1][0], children[1][1], children[1][2]

        est_prof = round(total_prof * 0.10, 2)  # 10% bundle uplift estimate
        conf = compute_confidence(has_cost=True, has_family=True, days_of_data=30)
        prio = compute_priority_score(
            impact_revenue=total_rev * 0.10, impact_profit=est_prof,
            confidence=conf, urgency=30, effort=50, strategic_fit=70, readiness=50,
            opportunity_type="BUNDLE_CREATE",
        )

        blocker = None
        if avg_margin < 10:
            blocker = "low_margin"
        elif child_cnt > 10:
            blocker = "too_many_variants"

        _insert_opp(
            cur, opp_type="BUNDLE_CREATE", marketplace_id=None,
            sku=sku_a, asin=asin_a, parent_asin=parent_asin,
            title=f"Bundle {sku_a} + {sku_b}",
            description=(
                f"Parent {parent_asin} has {child_cnt} children with combined "
                f"revenue {total_rev:.0f} PLN ({avg_margin:.0f}% margin). "
                f"Top pair: {sku_a} ({rev_a:.0f} PLN) + {sku_b} ({rev_b:.0f} PLN)."
            ),
            root_cause="bundle_opportunity",
            recommendation="Create virtual bundle listing combining top 2 selling variants. Test with A+ content.",
            priority_score=prio, confidence_score=conf,
            profit_uplift=est_prof, effort=50, owner_role="product_team",
            blockers=[blocker] if blocker else None,
            signals={
                "parent_asin": parent_asin, "child_count": child_cnt,
                "sku_a": sku_a, "sku_b": sku_b,
                "rev_a": float(rev_a), "rev_b": float(rev_b),
                "total_rev": float(total_rev), "avg_margin": float(avg_margin),
            },
        )
        n += 1
        if n >= 60:
            break
    return n


def _detect_variant_expansion(cur, from_date: str, to_date: str) -> int:
    """VARIANT_EXPANSION: variant selling well on one market but missing from another."""
    n = 0
    # Find SKUs that sell on some marketplaces but not all where their parent family sells
    cur.execute("""
        SELECT p.parent_asin, p.sku, p.asin,
               r.marketplace_id, SUM(r.revenue_pln) rev, SUM(r.profit_pln) prof,
               AVG(r.margin_pct) margin
        FROM acc_product p
        JOIN acc_sku_profitability_rollup r ON r.sku = p.sku
        WHERE p.parent_asin IS NOT NULL AND p.parent_asin != ''
          AND r.period_date BETWEEN ? AND ?
          AND r.sku NOT LIKE 'amzn.gr.%%'
        GROUP BY p.parent_asin, p.sku, p.asin, r.marketplace_id
        HAVING SUM(r.revenue_pln) > 500
        ORDER BY SUM(r.revenue_pln) DESC
    """, (from_date, to_date))
    strong_skus = _rows(cur)

    # Build map: parent_asin → set of marketplaces where family sells
    family_mkts: dict[str, set[str]] = {}
    sku_mkts: dict[str, set[str]] = {}
    for row in strong_skus:
        parent, sku, asin, mkt = row[0], row[1], row[2], row[3]
        family_mkts.setdefault(parent, set()).add(mkt)
        sku_mkts.setdefault(sku, set()).add(mkt)

    seen: set[tuple[str, str]] = set()
    for row in strong_skus:
        parent, sku, asin, source_mkt, rev, prof, margin = row
        family_markets = family_mkts.get(parent, set())
        sku_markets = sku_mkts.get(sku, set())

        # Markets where family sells but this SKU doesn't
        missing = family_markets - sku_markets
        if not missing:
            continue

        for target in sorted(missing)[:2]:
            key = (sku, target)
            if key in seen:
                continue
            seen.add(key)

            target_code = MKT_CODE.get(target, target)
            source_code = MKT_CODE.get(source_mkt, source_mkt)
            est_rev = round(rev * 0.20, 2)
            est_prof = round(est_rev * (margin / 100) * 0.7, 2) if margin > 0 else 0

            conf = compute_confidence(has_cost=True, has_family=True, days_of_data=30)
            prio = compute_priority_score(
                impact_revenue=est_rev, impact_profit=est_prof,
                confidence=conf, urgency=35, effort=40, strategic_fit=75, readiness=55,
                opportunity_type="VARIANT_EXPANSION",
            )

            _insert_opp(
                cur, opp_type="VARIANT_EXPANSION", marketplace_id=target,
                sku=sku, asin=asin, parent_asin=parent,
                title=f"Expand variant {sku} to {target_code}",
                description=(
                    f"Variant sells {rev:.0f} PLN on {source_code} but missing on {target_code}. "
                    f"Parent family active on {len(family_markets)} markets. "
                    f"Est. +{est_rev:.0f} PLN revenue."
                ),
                root_cause="variant_gap",
                recommendation=f"List variant on {target_code}. Sibling variants already selling there.",
                priority_score=prio, confidence_score=conf,
                revenue_uplift=est_rev, profit_uplift=est_prof,
                effort=40, owner_role="product_team",
                signals={
                    "parent_asin": parent, "source_market": source_code,
                    "source_revenue": float(rev), "source_margin": float(margin),
                    "family_markets": len(family_markets), "sku_markets": len(sku_markets),
                },
            )
            n += 1
        if n >= 60:
            break
    return n


def _detect_marketplace_expansion(cur, from_date: str, to_date: str) -> int:
    """MARKETPLACE_EXPANSION: strong SKUs on any market, missing on others (incl. DE)."""
    DE_MKT = "A1PA6795UKMFR9"
    n = 0

    # --- Phase 1: High-revenue SKUs → expand to any missing market -----------
    cur.execute("""
        SELECT sku, asin, marketplace_id,
               SUM(revenue_pln) rev, SUM(profit_pln) prof,
               AVG(margin_pct) margin, SUM(units_sold) units
        FROM acc_sku_profitability_rollup
        WHERE period_date BETWEEN ? AND ?
          AND sku NOT LIKE 'amzn.gr.%%' AND sku NOT LIKE 'amazon.found%%'
        GROUP BY sku, asin, marketplace_id
        HAVING SUM(revenue_pln) > 500 AND AVG(margin_pct) > 5
        ORDER BY SUM(revenue_pln) DESC
    """, (from_date, to_date))
    top_skus = _rows(cur)

    # Pre-build presence set (any sales at all) — separate from source threshold
    cur.execute("""
        SELECT sku, marketplace_id
        FROM acc_sku_profitability_rollup
        WHERE period_date BETWEEN ? AND ?
          AND sku NOT LIKE 'amzn.gr.%%' AND sku NOT LIKE 'amazon.found%%'
        GROUP BY sku, marketplace_id
        HAVING SUM(units_sold) > 0
    """, (from_date, to_date))
    sku_active: dict[str, set[str]] = {}
    for row in _rows(cur):
        sku_active.setdefault(row[0], set()).add(row[1])

    # Also check physical inventory presence (FBA snapshot) by ASIN —
    # a product listed with zero recent sales should NOT be flagged for expansion
    asin_present: dict[str, set[str]] = {}
    try:
        cur.execute("""
            SELECT DISTINCT asin, marketplace_id
            FROM acc_fba_inventory_snapshot WITH (NOLOCK)
            WHERE snapshot_date = (
                SELECT MAX(snapshot_date) FROM acc_fba_inventory_snapshot
            )
        """)
        for row in _rows(cur):
            if row[0]:
                asin_present.setdefault(row[0], set()).add(row[1])
    except Exception:
        pass  # table may not exist; fall back to sales-only check

    # Build ASIN→SKU mapping from top_skus so we can cross-reference
    sku_asin: dict[str, str] = {}
    for row in top_skus:
        if row[1]:  # asin
            sku_asin.setdefault(row[0], row[1])

    def _is_present(sku: str, marketplace_id: str) -> bool:
        """Check if SKU is present on marketplace via sales OR physical inventory."""
        if marketplace_id in sku_active.get(sku, set()):
            return True
        asin = sku_asin.get(sku)
        if asin and marketplace_id in asin_present.get(asin, set()):
            return True
        return False

    seen: set[tuple[str, str]] = set()  # (sku, target) dedup

    # --- Phase 2: DE-first — SKUs selling on non-DE markets but missing on DE -
    # Prioritize DE as target because it's the biggest market
    de_candidates = [
        row for row in top_skus
        if row[2] != DE_MKT and not _is_present(row[0], DE_MKT)
    ]
    # Deduplicate by SKU, keep highest-revenue source
    de_seen_skus: set[str] = set()
    for row in de_candidates:
        sku, asin, source_mkt, rev, prof, margin, units = row
        if sku in de_seen_skus:
            continue
        de_seen_skus.add(sku)
        key = (sku, DE_MKT)
        seen.add(key)
        source_code = MKT_CODE.get(source_mkt, source_mkt)
        # Higher estimates for DE (biggest market)
        est_rev = round(rev * 0.25, 2)
        est_prof = round(est_rev * (margin / 100) * 0.7, 2)
        conf = compute_confidence(has_cost=True, has_family=False, days_of_data=30)
        prio = compute_priority_score(
            impact_revenue=est_rev, impact_profit=est_prof,
            confidence=conf, urgency=40, effort=60, strategic_fit=90, readiness=40,
            opportunity_type="MARKETPLACE_EXPANSION",
        )
        _insert_opp(cur, opp_type="MARKETPLACE_EXPANSION", marketplace_id=DE_MKT,
                     sku=sku, asin=asin,
                     title=f"Expand {sku} to DE",
                     description=f"{source_code} revenue {rev:.0f} PLN. Not active on DE (largest market). Est. +{est_rev:.0f} PLN.",
                     root_cause="expansion_gap",
                     recommendation="Launch on DE. Highest-volume market — prioritize listing translation and FBA readiness.",
                     priority_score=prio, confidence_score=conf,
                     revenue_uplift=est_rev, profit_uplift=est_prof,
                     effort=60, owner_role="expansion_team",
                     signals={"source_market": source_code, "source_revenue": float(rev),
                              "source_margin": float(margin),
                              "missing_markets": len([m for m in ALL_MKT_IDS if not _is_present(sku, m)])})
        n += 1
        if n >= 60:
            break

    # --- Phase 3: All other expansions (DE→others + others→others) -----------
    for row in top_skus:
        sku, asin, source_mkt, rev, prof, margin, units = row
        source_code = MKT_CODE.get(source_mkt, source_mkt)
        missing = [m for m in ALL_MKT_IDS if not _is_present(sku, m)]
        if not missing:
            continue
        for target in missing[:3]:
            key = (sku, target)
            if key in seen:
                continue
            seen.add(key)
            target_code = MKT_CODE.get(target, target)
            est_rev = round(rev * 0.15, 2)
            est_prof = round(est_rev * (margin / 100) * 0.7, 2)
            conf = compute_confidence(has_cost=True, has_family=False, days_of_data=30)
            prio = compute_priority_score(
                impact_revenue=est_rev, impact_profit=est_prof,
                confidence=conf, urgency=30, effort=60, strategic_fit=80, readiness=40,
                opportunity_type="MARKETPLACE_EXPANSION",
            )
            _insert_opp(cur, opp_type="MARKETPLACE_EXPANSION", marketplace_id=target,
                         sku=sku, asin=asin,
                         title=f"Expand {sku} to {target_code}",
                         description=f"{source_code} revenue {rev:.0f} PLN. Not active on {target_code}. Est. +{est_rev:.0f} PLN.",
                         root_cause="expansion_gap",
                         recommendation=f"Launch on {target_code}. Prepare listing translation, check FBA readiness.",
                         priority_score=prio, confidence_score=conf,
                         revenue_uplift=est_rev, profit_uplift=est_prof,
                         effort=60, owner_role="expansion_team",
                         signals={"source_market": source_code, "source_revenue": float(rev),
                                  "source_margin": float(margin), "missing_markets": len(missing)})
            n += 1
        if n >= 300:
            break
    return n


def _detect_family_repair(cur, from_date: str, to_date: str) -> int:
    """FAMILY_REPAIR: broken family coverage across markets."""
    n = 0
    try:
        cur.execute("""
            SELECT f.id, f.de_parent_asin, f.brand,
                   fc.marketplace, fc.coverage_pct, fc.missing_children_count
            FROM global_family f
            JOIN family_coverage_cache fc ON fc.global_family_id = f.id
            WHERE fc.coverage_pct < 70 AND fc.missing_children_count > 2
            ORDER BY fc.missing_children_count DESC
        """)
        for r in _rows(cur):
            fam_id, parent, brand, mkt, cov, missing = r
            conf = compute_confidence(has_family=True, days_of_data=30)
            prio = compute_priority_score(
                impact_revenue=missing * 200, impact_profit=missing * 40,
                confidence=conf, urgency=35, effort=40, strategic_fit=60, readiness=50,
                opportunity_type="FAMILY_REPAIR",
            )
            _insert_opp(cur, opp_type="FAMILY_REPAIR", marketplace_id=mkt,
                         sku=None, parent_asin=parent, family_id=fam_id,
                         title=f"Fix family {parent} on {MKT_CODE.get(mkt, mkt)} — {missing} children missing",
                         description=f"Coverage {cov:.0f}%, {missing} variants missing. Limits cross-selling and listing quality.",
                         root_cause="family_structure_problem",
                         recommendation="Map missing child ASINs. Use Family Mapper to link variants.",
                         priority_score=prio, confidence_score=conf,
                         revenue_uplift=round(missing * 200, 2), profit_uplift=round(missing * 40, 2),
                         effort=40, owner_role="catalog_team",
                         signals={"coverage_pct": float(cov), "missing_children": int(missing)})
            n += 1
            if n >= 40:
                break
    except Exception as e:
        log.warning("strategy.family_repair_error", error=str(e))
    return n


def _detect_cost_opportunities(cur, from_date: str, to_date: str) -> int:
    """COST_RENEGOTIATION: high COGS ratio eating margin.
       CATEGORY_WINNER_SCALE: top profitable SKUs worth doubling down.
    """
    n = 0
    # COST_RENEGOTIATION: COGS > 40% of revenue, revenue > 1000
    cur.execute("""
        SELECT marketplace_id, sku, asin,
               SUM(revenue_pln) rev, SUM(cogs_pln) cogs,
               SUM(profit_pln) prof, AVG(margin_pct) margin
        FROM acc_sku_profitability_rollup
        WHERE period_date BETWEEN ? AND ?
          AND sku NOT LIKE 'amzn.gr.%%' AND sku NOT LIKE 'amazon.found%%'
        GROUP BY marketplace_id, sku, asin
        HAVING SUM(cogs_pln) > SUM(revenue_pln) * 0.40
               AND SUM(revenue_pln) > 1000
        ORDER BY SUM(cogs_pln) DESC
    """, (from_date, to_date))
    for r in _rows(cur):
        mkt, sku, asin, rev, cogs, prof, margin = r
        savings = round(cogs * 0.10, 2)  # 10% cost renegotiation
        conf = compute_confidence(has_cost=True, days_of_data=30)
        prio = compute_priority_score(
            impact_revenue=0, impact_profit=savings,
            confidence=conf, urgency=30, effort=60, strategic_fit=50, readiness=40,
            opportunity_type="COST_RENEGOTIATION",
        )
        _insert_opp(cur, opp_type="COST_RENEGOTIATION", marketplace_id=mkt, sku=sku, asin=asin,
                     title=f"Renegotiate COGS — {sku} ({MKT_CODE.get(mkt, mkt)}) COGS ratio {cogs/max(rev,1)*100:.0f}%",
                     description=f"COGS {cogs:.0f} PLN ({cogs/max(rev,1)*100:.0f}% of revenue). 10% reduction → save {savings:.0f} PLN.",
                     root_cause="cost_problem",
                     recommendation="Contact supplier re: volume discounts. Evaluate alternative suppliers.",
                     priority_score=prio, confidence_score=conf,
                     profit_uplift=savings, effort=60, owner_role="procurement_team",
                     signals={"cogs": float(cogs), "cogs_ratio": round(float(cogs) / max(float(rev), 1) * 100, 1)})
        n += 1
        if n >= 40:
            break

    # CATEGORY_WINNER_SCALE: top 20 profit SKUs, margin > 20%
    cur.execute("""
        SELECT TOP 20 marketplace_id, sku, asin,
               SUM(revenue_pln) rev, SUM(profit_pln) prof,
               AVG(margin_pct) margin, SUM(units_sold) units
        FROM acc_sku_profitability_rollup
        WHERE period_date BETWEEN ? AND ?
          AND sku NOT LIKE 'amzn.gr.%%' AND sku NOT LIKE 'amazon.found%%'
        GROUP BY marketplace_id, sku, asin
        HAVING AVG(margin_pct) > 20 AND SUM(profit_pln) > 500
        ORDER BY SUM(profit_pln) DESC
    """, (from_date, to_date))
    n2 = 0
    for r in _rows(cur):
        mkt, sku, asin, rev, prof, margin, units = r
        scale_rev = round(rev * 0.25, 2)
        scale_prof = round(prof * 0.25, 2)
        conf = compute_confidence(has_cost=True, has_ads=True, days_of_data=30)
        prio = compute_priority_score(
            impact_revenue=scale_rev, impact_profit=scale_prof,
            confidence=conf, urgency=40, effort=30, strategic_fit=90, readiness=80,
            opportunity_type="CATEGORY_WINNER_SCALE",
        )
        _insert_opp(cur, opp_type="CATEGORY_WINNER_SCALE", marketplace_id=mkt, sku=sku, asin=asin,
                     title=f"Scale winner — {sku} ({MKT_CODE.get(mkt, mkt)}) profit {prof:.0f} PLN",
                     description=f"Top performer: {prof:.0f} PLN profit, {margin:.1f}% margin. Invest in ads + expansion for +25%.",
                     root_cause=None,
                     recommendation="Increase ad budget, improve content, consider marketplace expansion.",
                     priority_score=prio, confidence_score=conf,
                     revenue_uplift=scale_rev, profit_uplift=scale_prof,
                     effort=30, owner_role="growth_team",
                     signals={"profit": float(prof), "margin": float(margin), "units": int(units)})
        n2 += 1
    return n + n2


# ═══════════════════════════════════════════════════════════════════
#  MASTER DETECTION PIPELINE
# ═══════════════════════════════════════════════════════════════════

def run_strategy_detection(*, days_back: int = 30) -> dict:
    """Run all detection engines. Deactivates old 'new' opps, inserts fresh ones."""
    t0 = time.time()
    to_date = date.today().isoformat()
    from_date = (date.today() - timedelta(days=days_back)).isoformat()

    conn = connect_acc(autocommit=False, timeout=180)
    cur = conn.cursor()
    try:
        # Load Decision Intelligence feedback-loop adjustments
        load_model_adjustments(cur)

        # Archive old new/in_review opportunities (don't touch accepted/completed)
        cur.execute("""
            UPDATE growth_opportunity
            SET status = 'rejected', updated_at = SYSUTCDATETIME()
            WHERE status IN ('new', 'in_review')
              AND created_at < DATEADD(day, -7, SYSUTCDATETIME())
        """)

        # Remove expansion opps for marketplaces no longer in MARKETPLACE_REGISTRY
        valid_ids = list(MARKETPLACE_REGISTRY.keys())
        if valid_ids:
            placeholders = ",".join(["?"] * len(valid_ids))
            cur.execute(f"""
                DELETE FROM growth_opportunity
                WHERE opportunity_type = 'MARKETPLACE_EXPANSION'
                  AND marketplace_id IS NOT NULL
                  AND marketplace_id NOT IN ({placeholders})
            """, tuple(valid_ids))

        counts = {}
        counts["pricing"] = _detect_pricing_opportunities(cur, from_date, to_date)
        counts["ads"] = _detect_ads_opportunities(cur, from_date, to_date)
        counts["inventory"] = _detect_inventory_opportunities(cur, from_date, to_date)
        counts["content"] = _detect_content_opportunities(cur, from_date, to_date)
        counts["returns"] = _detect_return_opportunities(cur, from_date, to_date)
        counts["expansion"] = _detect_marketplace_expansion(cur, from_date, to_date)
        counts["family"] = _detect_family_repair(cur, from_date, to_date)
        counts["cost"] = _detect_cost_opportunities(cur, from_date, to_date)
        counts["bundles"] = _detect_bundle_opportunities(cur, from_date, to_date)
        counts["variant_exp"] = _detect_variant_expansion(cur, from_date, to_date)

        conn.commit()
        elapsed = round(time.time() - t0, 1)
        total = sum(counts.values())
        log.info("strategy.detection_done", total=total, elapsed=elapsed, **counts)
        return {"opportunities_found": total, "elapsed_sec": elapsed, "details": counts}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


# ═══════════════════════════════════════════════════════════════════
#  QUERY HELPERS  (consumed by the API layer)
# ═══════════════════════════════════════════════════════════════════

def get_strategy_overview() -> dict:
    """Aggregate KPIs + breakdowns for /strategy/overview."""
    conn = connect_acc()
    cur = conn.cursor()
    try:
        # Active opportunities
        cur.execute("""
            SELECT COUNT(*) total,
                   SUM(ISNULL(estimated_revenue_uplift, 0)) rev_up,
                   SUM(ISNULL(estimated_profit_uplift, 0)) prof_up,
                   SUM(CASE WHEN priority_score >= 90 THEN 1 ELSE 0 END) do_now,
                   SUM(CASE WHEN priority_score BETWEEN 75 AND 89.9 THEN 1 ELSE 0 END) this_week,
                   SUM(CASE WHEN priority_score BETWEEN 60 AND 74.9 THEN 1 ELSE 0 END) this_month,
                   SUM(CASE WHEN blocker_json IS NOT NULL AND blocker_json != '[]' AND blocker_json != 'null' THEN 1 ELSE 0 END) blocked
            FROM growth_opportunity
            WHERE status IN ('new', 'in_review', 'accepted')
        """)
        r = cur.fetchone()
        kpi = {
            "total_revenue_uplift": float(r[1] or 0),
            "total_profit_uplift": float(r[2] or 0),
            "total_opportunities": int(r[0] or 0),
            "do_now_count": int(r[3] or 0),
            "this_week_count": int(r[4] or 0),
            "this_month_count": int(r[5] or 0),
            "blocked_count": int(r[6] or 0),
        }

        # Completed in last 30d
        cur.execute("""
            SELECT COUNT(*), SUM(ISNULL(estimated_profit_uplift, 0))
            FROM growth_opportunity
            WHERE status = 'completed' AND updated_at >= DATEADD(day, -30, SYSUTCDATETIME())
        """)
        c = cur.fetchone()
        kpi["completed_30d"] = int(c[0] or 0)
        kpi["completed_impact_30d"] = float(c[1] or 0)

        # By type
        cur.execute("""
            SELECT opportunity_type, COUNT(*) cnt,
                   SUM(ISNULL(estimated_revenue_uplift, 0)),
                   SUM(ISNULL(estimated_profit_uplift, 0))
            FROM growth_opportunity WHERE status IN ('new','in_review','accepted')
            GROUP BY opportunity_type ORDER BY SUM(ISNULL(estimated_profit_uplift, 0)) DESC
        """)
        by_type = [{"opportunity_type": r[0], "count": int(r[1]), "revenue_uplift": float(r[2] or 0), "profit_uplift": float(r[3] or 0)} for r in cur.fetchall()]

        # By market
        cur.execute("""
            SELECT marketplace_id, COUNT(*) cnt,
                   SUM(ISNULL(estimated_revenue_uplift, 0)),
                   SUM(ISNULL(estimated_profit_uplift, 0))
            FROM growth_opportunity WHERE status IN ('new','in_review','accepted') AND marketplace_id IS NOT NULL
            GROUP BY marketplace_id ORDER BY SUM(ISNULL(estimated_profit_uplift, 0)) DESC
        """)
        by_mkt = [{"marketplace_id": r[0], "marketplace_code": MKT_CODE.get(r[0], r[0]), "count": int(r[1]), "revenue_uplift": float(r[2] or 0), "profit_uplift": float(r[3] or 0)} for r in cur.fetchall()]

        # By owner
        cur.execute("""
            SELECT ISNULL(owner_role, 'unassigned'), COUNT(*)
            FROM growth_opportunity WHERE status IN ('new','in_review','accepted')
            GROUP BY owner_role ORDER BY COUNT(*) DESC
        """)
        by_owner = [{"owner_role": r[0], "count": int(r[1])} for r in cur.fetchall()]

        # Top 10 by priority
        top = _fetch_opportunities(cur, where="status IN ('new','in_review','accepted')", order="priority_score DESC", limit=10)

        # Do now
        do_now = _fetch_opportunities(cur, where="status IN ('new','in_review','accepted') AND priority_score >= 90", order="priority_score DESC", limit=20)

        # This week
        this_week = _fetch_opportunities(cur, where="status IN ('new','in_review','accepted') AND priority_score BETWEEN 75 AND 89.9", order="priority_score DESC", limit=20)

        # Blocked
        blocked = _fetch_opportunities(cur, where="status IN ('new','in_review','accepted') AND blocker_json IS NOT NULL AND blocker_json != '[]' AND blocker_json != 'null'", order="priority_score DESC", limit=20)

        return {
            "kpi": kpi,
            "by_type": by_type,
            "by_market": by_mkt,
            "by_owner": by_owner,
            "top_priorities": top,
            "do_now": do_now,
            "this_week": this_week,
            "blocked": blocked,
        }
    finally:
        cur.close()
        conn.close()


def _fetch_opportunities(cur, *, where: str = "1=1", order: str = "priority_score DESC", limit: int = 50, offset: int = 0) -> list:
    """Fetch opportunity rows as dicts."""
    cur.execute(f"""
        SELECT id, opportunity_type, marketplace_id, sku, asin, parent_asin, family_id,
               title, description, root_cause, recommendation,
               priority_score, confidence_score,
               estimated_revenue_uplift, estimated_profit_uplift,
               estimated_margin_uplift, estimated_units_uplift,
               effort_score, owner_role, blocker_json, source_signals_json,
               status, created_at, updated_at
        FROM growth_opportunity
        WHERE {where}
        ORDER BY {order}
        OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
    """)
    rows = []
    for r in cur.fetchall():
        d = {
            "id": r[0], "opportunity_type": r[1], "marketplace_id": r[2],
            "marketplace_code": MKT_CODE.get(r[2], r[2]) if r[2] else None,
            "sku": r[3], "asin": r[4], "parent_asin": r[5], "family_id": r[6],
            "title": r[7], "description": r[8], "root_cause": r[9], "recommendation": r[10],
            "priority_score": float(r[11] or 0), "confidence_score": float(r[12] or 0),
            "priority_label": _priority_label(float(r[11] or 0)),
            "estimated_revenue_uplift": float(r[13]) if r[13] else None,
            "estimated_profit_uplift": float(r[14]) if r[14] else None,
            "estimated_margin_uplift": float(r[15]) if r[15] else None,
            "estimated_units_uplift": int(r[16]) if r[16] else None,
            "effort_score": float(r[17]) if r[17] else None,
            "owner_role": r[18], "status": r[21],
            "created_at": r[22].isoformat() if r[22] else None,
            "updated_at": r[23].isoformat() if r[23] else None,
        }
        # Parse JSON fields
        for jf, idx in [("blocker_json", 19), ("source_signals_json", 20)]:
            raw = r[idx]
            if raw:
                try:
                    d[jf] = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    d[jf] = raw
            else:
                d[jf] = None
        rows.append(d)
    return rows


def get_opportunities_page(
    *,
    page: int = 1,
    page_size: int = 50,
    marketplace_id: str | None = None,
    opportunity_type: str | None = None,
    status: str | None = None,
    owner_role: str | None = None,
    min_priority: float | None = None,
    max_priority: float | None = None,
    min_confidence: float | None = None,
    sku: str | None = None,
    sort: str = "priority_score",
    direction: str = "desc",
    quick_filter: str | None = None,
) -> dict:
    """Paginated filtered opportunity list."""
    conn = connect_acc()
    cur = conn.cursor()
    try:
        clauses = ["1=1"]
        if marketplace_id:
            clauses.append(f"marketplace_id = '{marketplace_id}'")
        if opportunity_type:
            clauses.append(f"opportunity_type = '{opportunity_type}'")
        if status:
            clauses.append(f"status = '{status}'")
        else:
            clauses.append("status IN ('new','in_review','accepted')")
        if owner_role:
            clauses.append(f"owner_role = '{owner_role}'")
        if min_priority is not None:
            clauses.append(f"priority_score >= {min_priority}")
        if max_priority is not None:
            clauses.append(f"priority_score <= {max_priority}")
        if min_confidence is not None:
            clauses.append(f"confidence_score >= {min_confidence}")
        if sku:
            clauses.append(f"sku LIKE '%{sku}%'")

        # Quick filters
        if quick_filter == "do_now":
            clauses.append("priority_score >= 90")
        elif quick_filter == "high_impact_low_effort":
            clauses.append("estimated_profit_uplift > 500 AND effort_score < 40")
        elif quick_filter == "pricing":
            clauses.append("opportunity_type IN ('PRICE_INCREASE','PRICE_DECREASE')")
        elif quick_filter == "inventory":
            clauses.append("opportunity_type IN ('STOCK_REPLENISH','STOCK_PROTECTION','LIQUIDATE_OR_PROMO')")
        elif quick_filter == "marketplace_expansion":
            clauses.append("opportunity_type = 'MARKETPLACE_EXPANSION'")
        elif quick_filter == "content":
            clauses.append("opportunity_type IN ('CONTENT_FIX','CONTENT_EXPANSION','SUPPRESSION_FIX')")
        elif quick_filter == "bundles":
            clauses.append("opportunity_type IN ('BUNDLE_CREATE','VARIANT_EXPANSION')")
        elif quick_filter == "family_repair":
            clauses.append("opportunity_type = 'FAMILY_REPAIR'")

        where = " AND ".join(clauses)
        allowed_sort = {"priority_score", "confidence_score", "estimated_profit_uplift", "estimated_revenue_uplift", "effort_score", "created_at"}
        sort_col = sort if sort in allowed_sort else "priority_score"
        sort_dir = "ASC" if direction.lower() == "asc" else "DESC"

        cur.execute(f"SELECT COUNT(*) FROM growth_opportunity WHERE {where}")
        total = cur.fetchone()[0]
        pages = math.ceil(total / page_size) if total > 0 else 0
        offset = (page - 1) * page_size

        items = _fetch_opportunities(cur, where=where, order=f"{sort_col} {sort_dir}", limit=page_size, offset=offset)
        return {"items": items, "total": total, "pages": pages}
    finally:
        cur.close()
        conn.close()


def get_opportunity_detail(opp_id: int) -> dict | None:
    """Fetch single opportunity + timeline."""
    conn = connect_acc()
    cur = conn.cursor()
    try:
        items = _fetch_opportunities(cur, where=f"id = {int(opp_id)}", limit=1)
        if not items:
            return None
        opp = items[0]
        cur.execute("""
            SELECT id, opportunity_id, action, actor, note, created_at
            FROM growth_opportunity_log
            WHERE opportunity_id = ? ORDER BY created_at
        """, (opp_id,))
        timeline = []
        for r in cur.fetchall():
            timeline.append({
                "id": r[0], "opportunity_id": r[1], "action": r[2],
                "actor": r[3], "note": r[4],
                "created_at": r[5].isoformat() if r[5] else None,
            })
        return {"opportunity": opp, "timeline": timeline}
    finally:
        cur.close()
        conn.close()


def change_opportunity_status(opp_id: int, new_status: str, *, actor: str = "system", note: str | None = None) -> dict:
    """Accept / reject / complete an opportunity."""
    valid = {"new", "in_review", "accepted", "rejected", "completed"}
    if new_status not in valid:
        raise ValueError(f"Invalid status: {new_status}")
    conn = connect_acc(autocommit=False)
    cur = conn.cursor()
    try:
        cur.execute("UPDATE growth_opportunity SET status=?, updated_at=SYSUTCDATETIME() WHERE id=?", (new_status, opp_id))
        cur.execute(
            "INSERT INTO growth_opportunity_log (opportunity_id, action, actor, note, created_at) VALUES (?,?,?,?,SYSUTCDATETIME())",
            (opp_id, new_status, actor, note),
        )
        conn.commit()
        cur.execute("SELECT id, status, updated_at FROM growth_opportunity WHERE id=?", (opp_id,))
        r = cur.fetchone()
        return {"id": r[0], "status": r[1], "updated_at": r[2].isoformat()}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


# ═══════════════════════════════════════════════════════════════════
#  MARKETPLACE EXPANSION DETAIL
# ═══════════════════════════════════════════════════════════════════

def get_market_expansion_items() -> list:
    """Return marketplace expansion opportunities with readiness assessment."""
    conn = connect_acc()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT TOP 200 id, marketplace_id, sku, asin, parent_asin, family_id,
                   estimated_revenue_uplift, estimated_profit_uplift,
                   confidence_score, source_signals_json, priority_score
            FROM growth_opportunity
            WHERE opportunity_type = 'MARKETPLACE_EXPANSION'
              AND status IN ('new','in_review','accepted')
              AND sku NOT LIKE 'amzn.gr.%%'
            ORDER BY priority_score DESC
        """)
        items = []
        for r in cur.fetchall():
            signals = {}
            if r[9]:
                try:
                    signals = json.loads(r[9])
                except (json.JSONDecodeError, TypeError):
                    pass

            # Compute readiness from source signals
            src_rev = signals.get("source_revenue", 0) or 0
            src_margin = signals.get("source_margin", 0) or 0
            missing_n = signals.get("missing_markets", 99) or 99
            confidence = float(r[8] or 0)

            readiness = 0.0
            missing = []

            # Revenue strength (max 30)
            if src_rev >= 5000:
                readiness += 30
            elif src_rev >= 1000:
                readiness += 20
            elif src_rev >= 500:
                readiness += 10
            else:
                missing.append("low_source_revenue")

            # Margin health (max 20)
            if src_margin >= 15:
                readiness += 20
            elif src_margin >= 8:
                readiness += 15
            elif src_margin >= 5:
                readiness += 10
            else:
                missing.append("low_margin")

            # Focused opportunity — fewer missing markets = higher readiness (max 25)
            if missing_n <= 1:
                readiness += 25
            elif missing_n <= 3:
                readiness += 15
            else:
                missing.append("many_markets_missing")

            # Data quality / confidence (max 25)
            if confidence >= 60:
                readiness += 25
            elif confidence >= 40:
                readiness += 15
            elif confidence >= 20:
                readiness += 10
            else:
                missing.append("low_confidence")

            if readiness >= 75:
                label = "launch_ready"
            elif readiness >= 60:
                label = "needs_content"
            elif readiness >= 40:
                label = "needs_family_fix"
            elif readiness >= 20:
                label = "needs_inventory"
            else:
                label = "not_viable"

            source_code = signals.get("source_market", "")
            source_mkt_id = CODE_MKT.get(source_code, "A1PA6795UKMFR9")
            items.append({
                "family_id": r[5],
                "parent_asin": r[4],
                "sku": r[2],
                "source_marketplace": source_mkt_id,
                "target_marketplace": r[1],
                "source_revenue": signals.get("source_revenue", 0),
                "source_profit": 0,
                "readiness_score": readiness,
                "readiness_label": label,
                "missing_components": missing or None,
                "estimated_revenue_uplift": float(r[6] or 0),
                "estimated_profit_uplift": float(r[7] or 0),
                "confidence": float(r[8] or 0),
            })
        return items
    finally:
        cur.close()
        conn.close()


# ═══════════════════════════════════════════════════════════════════
#  BUNDLE CANDIDATES
# ═══════════════════════════════════════════════════════════════════

def get_bundle_candidates() -> dict:
    """Detect bundle/variant opportunities from data."""
    conn = connect_acc()
    cur = conn.cursor()
    try:
        # Fetch existing BUNDLE_CREATE & VARIANT_EXPANSION opps
        cur.execute("""
            SELECT TOP 100 id, marketplace_id, sku, asin,
                   estimated_profit_uplift, confidence_score,
                   source_signals_json, opportunity_type, blocker_json,
                   estimated_revenue_uplift
            FROM growth_opportunity
            WHERE opportunity_type IN ('BUNDLE_CREATE','VARIANT_EXPANSION')
              AND status IN ('new','in_review','accepted')
            ORDER BY priority_score DESC
        """)
        bundles = []
        variant_gaps = []
        for r in cur.fetchall():
            signals = {}
            if r[6]:
                try:
                    signals = json.loads(r[6])
                except (json.JSONDecodeError, TypeError):
                    pass
            blockers = None
            if r[8]:
                try:
                    blockers = json.loads(r[8])
                except (json.JSONDecodeError, TypeError):
                    pass

            if r[7] == "BUNDLE_CREATE":
                bundles.append({
                    "id": r[0],
                    "sku_a": r[2] or "",
                    "sku_b": signals.get("sku_b"),
                    "proposed_bundle_sku": None,
                    "marketplace_id": r[1],
                    "est_margin": signals.get("avg_margin"),
                    "est_profit_uplift": float(r[4] or 0),
                    "confidence": float(r[5] or 0),
                    "blocker": blockers[0] if blockers else None,
                    "action": "review",
                })
            else:
                # VARIANT_EXPANSION → render as BundleCandidate shape for frontend
                bundles_on = signals.get("source_market", "")
                variant_gaps.append({
                    "id": r[0],
                    "sku_a": r[2] or "",
                    "sku_b": None,
                    "proposed_bundle_sku": None,
                    "marketplace_id": r[1],
                    "est_margin": signals.get("source_margin"),
                    "est_profit_uplift": float(r[4] or 0),
                    "confidence": float(r[5] or 0),
                    "blocker": None,
                    "action": f"expand from {bundles_on}" if bundles_on else "expand",
                })
        return {"bundles": bundles, "variant_gaps": variant_gaps}
    finally:
        cur.close()
        conn.close()


# ═══════════════════════════════════════════════════════════════════
#  EXPERIMENTS CRUD
# ═══════════════════════════════════════════════════════════════════

def get_experiments(*, status: str | None = None) -> list:
    conn = connect_acc()
    cur = conn.cursor()
    try:
        where = f"status = '{status}'" if status else "1=1"
        cur.execute(f"""
            SELECT id, opportunity_id, experiment_type, marketplace_id, sku, asin,
                   hypothesis, owner, status, start_date, end_date,
                   success_metric, baseline_value, result_value, result_summary,
                   created_at, updated_at
            FROM strategy_experiment
            WHERE {where}
            ORDER BY created_at DESC
        """)
        items = []
        for r in cur.fetchall():
            items.append({
                "id": r[0], "opportunity_id": r[1], "experiment_type": r[2],
                "marketplace_id": r[3], "sku": r[4], "asin": r[5],
                "hypothesis": r[6], "owner": r[7], "status": r[8],
                "start_date": r[9].isoformat() if r[9] else None,
                "end_date": r[10].isoformat() if r[10] else None,
                "success_metric": r[11],
                "baseline_value": float(r[12]) if r[12] else None,
                "result_value": float(r[13]) if r[13] else None,
                "result_summary": r[14],
                "created_at": r[15].isoformat() if r[15] else None,
                "updated_at": r[16].isoformat() if r[16] else None,
            })
        return items
    finally:
        cur.close()
        conn.close()


def create_experiment(data: dict) -> dict:
    conn = connect_acc(autocommit=False)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO strategy_experiment
                (opportunity_id, experiment_type, marketplace_id, sku, asin,
                 hypothesis, owner, success_metric, start_date, end_date,
                 status, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,'planned',SYSUTCDATETIME(),SYSUTCDATETIME())
        """, (
            data.get("opportunity_id"), data["experiment_type"],
            data.get("marketplace_id"), data.get("sku"), data.get("asin"),
            data["hypothesis"], data.get("owner"), data.get("success_metric"),
            data.get("start_date"), data.get("end_date"),
        ))
        cur.execute("SELECT SCOPE_IDENTITY()")
        new_id = int(cur.fetchone()[0])
        conn.commit()
        return {"id": new_id, "status": "planned"}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


# ═══════════════════════════════════════════════════════════════════
#  PLAYBOOKS  (static configuration, not DB-driven)
# ═══════════════════════════════════════════════════════════════════

PLAYBOOKS = [
    {
        "id": "high_sessions_low_cvr",
        "name": "High Sessions + Low CVR",
        "description": "Product gets traffic but doesn't convert. Likely content, pricing, or review issue.",
        "trigger_condition": "Sessions > 500/mo, CVR < 3%, in-stock, no suppression",
        "opportunity_types": ["CONTENT_FIX", "PRICE_DECREASE"],
        "steps": [
            {"seq": 1, "action": "Audit listing quality (title, bullets, images, A+)", "owner_role": "content_team"},
            {"seq": 2, "action": "Compare price vs competitors / Buy Box", "owner_role": "pricing_team"},
            {"seq": 3, "action": "Check review score and negative feedback", "owner_role": "operations_team"},
            {"seq": 4, "action": "Run A/B test on main image", "owner_role": "content_team"},
            {"seq": 5, "action": "Monitor CVR for 2 weeks", "owner_role": "analytics_team"},
        ],
        "metrics_to_monitor": ["CVR", "Sessions", "Orders", "BSR"],
        "expected_time_to_impact": "2-4 weeks",
    },
    {
        "id": "rising_demand_low_cover",
        "name": "Rising Demand + Low Inventory Cover",
        "description": "Demand is accelerating but stock won't last. Must replenish urgently.",
        "trigger_condition": "DOI < 14 days, daily sales increasing 20%+ WoW, FBA stock",
        "opportunity_types": ["STOCK_REPLENISH", "STOCK_PROTECTION"],
        "steps": [
            {"seq": 1, "action": "Calculate required units for 60-day cover", "owner_role": "supply_chain"},
            {"seq": 2, "action": "Create FBA shipment plan", "owner_role": "supply_chain"},
            {"seq": 3, "action": "Coordinate warehouse picking and dispatch", "owner_role": "warehouse_team"},
            {"seq": 4, "action": "Monitor inbound shipment status", "owner_role": "supply_chain"},
        ],
        "metrics_to_monitor": ["DOI", "Daily Sales", "Inbound Qty", "Stockout Date"],
        "expected_time_to_impact": "1-3 weeks (shipping lead time)",
    },
    {
        "id": "strong_de_weak_expansion",
        "name": "Strong DE Product + Weak/No Expansion",
        "description": "Product performs well in Germany but is absent or underperforming abroad.",
        "trigger_condition": "DE revenue >2000 PLN/mo, <2 other markets active, family mapping available",
        "opportunity_types": ["MARKETPLACE_EXPANSION"],
        "steps": [
            {"seq": 1, "action": "Verify family/variant mapping for target market", "owner_role": "catalog_team"},
            {"seq": 2, "action": "Translate and localise listing content", "owner_role": "content_team"},
            {"seq": 3, "action": "Set pricing strategy for target market", "owner_role": "pricing_team"},
            {"seq": 4, "action": "Ensure FBA stock availability", "owner_role": "supply_chain"},
            {"seq": 5, "action": "Launch with Sponsored Products campaign", "owner_role": "ads_team"},
        ],
        "metrics_to_monitor": ["Target Market Revenue", "Sessions", "BSR", "Reviews"],
        "expected_time_to_impact": "4-8 weeks",
    },
    {
        "id": "high_return_strong_traffic",
        "name": "High Return Rate + Strong Traffic",
        "description": "Product has demand but returns are eating margin. Root cause investigation needed.",
        "trigger_condition": "Return rate >10%, sessions >200/mo, revenue >500 PLN/mo",
        "opportunity_types": ["RETURN_REDUCTION"],
        "steps": [
            {"seq": 1, "action": "Analyse return reasons from Amazon reports", "owner_role": "operations_team"},
            {"seq": 2, "action": "Review listing accuracy (size chart, specifications)", "owner_role": "content_team"},
            {"seq": 3, "action": "Check packaging and product quality", "owner_role": "quality_team"},
            {"seq": 4, "action": "Update listing to set correct expectations", "owner_role": "content_team"},
            {"seq": 5, "action": "Monitor return rate for 4 weeks", "owner_role": "analytics_team"},
        ],
        "metrics_to_monitor": ["Return Rate", "Refund Amount", "Customer Feedback"],
        "expected_time_to_impact": "3-6 weeks",
    },
    {
        "id": "ads_high_spend_low_profit",
        "name": "Ads Spend High, Profit Low",
        "description": "Advertising budget is being burned with poor return. Optimise or cut.",
        "trigger_condition": "ACoS > 30%, ad spend >300 PLN/mo, margin <15%",
        "opportunity_types": ["ADS_CUT_WASTE"],
        "steps": [
            {"seq": 1, "action": "Audit campaign structure and targeting", "owner_role": "ads_team"},
            {"seq": 2, "action": "Pause unprofitable keywords/targets", "owner_role": "ads_team"},
            {"seq": 3, "action": "Lower bids on high-CPC low-conversion terms", "owner_role": "ads_team"},
            {"seq": 4, "action": "Test exact match vs broad match ROAS", "owner_role": "ads_team"},
            {"seq": 5, "action": "Review weekly for 4 weeks", "owner_role": "ads_team"},
        ],
        "metrics_to_monitor": ["ACoS", "ROAS", "TACoS", "Ad Profit Contribution"],
        "expected_time_to_impact": "1-2 weeks",
    },
    {
        "id": "family_broken_abroad",
        "name": "Family Broken Outside DE",
        "description": "Parent-child relationships are broken in non-DE markets, hurting discoverability.",
        "trigger_condition": "Family coverage <70% in target market, >2 missing children",
        "opportunity_types": ["FAMILY_REPAIR"],
        "steps": [
            {"seq": 1, "action": "Identify missing child ASINs via Family Mapper", "owner_role": "catalog_team"},
            {"seq": 2, "action": "Create variation relationships in Seller Central", "owner_role": "catalog_team"},
            {"seq": 3, "action": "Verify parent-child links after 48h", "owner_role": "catalog_team"},
        ],
        "metrics_to_monitor": ["Coverage %", "Missing Children", "Family Revenue"],
        "expected_time_to_impact": "1-2 weeks",
    },
    {
        "id": "bundle_complementary",
        "name": "Bundle Candidate from Complementary SKUs",
        "description": "Multiple products frequently bought together or in same category — bundle opportunity.",
        "trigger_condition": "2+ SKUs in same category, both profitable, stock available",
        "opportunity_types": ["BUNDLE_CREATE"],
        "steps": [
            {"seq": 1, "action": "Validate BOM and component stock levels", "owner_role": "supply_chain"},
            {"seq": 2, "action": "Calculate bundle pricing and margin", "owner_role": "pricing_team"},
            {"seq": 3, "action": "Create bundle listing with A+ content", "owner_role": "content_team"},
            {"seq": 4, "action": "Launch with targeted ads", "owner_role": "ads_team"},
        ],
        "metrics_to_monitor": ["Bundle CVR", "Bundle Margin", "Cannibalisation Rate"],
        "expected_time_to_impact": "2-4 weeks",
    },
]


def get_playbooks() -> list:
    return PLAYBOOKS
