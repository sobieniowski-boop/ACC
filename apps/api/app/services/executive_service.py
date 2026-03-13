"""
Executive Command Center — service layer.

Aggregates data from profitability rollups, inventory, ads, traffic
into executive_daily_metrics, computes health scores, detects
risks and growth opportunities.
"""
from __future__ import annotations

import math
import time
from datetime import date, timedelta
from typing import Any

import structlog

from app.core.config import MARKETPLACE_REGISTRY, RENEWED_SKU_SQL_FILTER
from app.core.db_connection import connect_acc
from app.services.growth_opportunity_access import (
    deactivate_by_types,
    insert_opportunity,
    priority_from_label,
    query_active,
)

log = structlog.get_logger(__name__)

_EXEC_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


def _exec_cache_get(key: str) -> dict[str, Any] | None:
    row = _EXEC_CACHE.get(key)
    if not row:
        return None
    exp, value = row
    if time.monotonic() > exp:
        _EXEC_CACHE.pop(key, None)
        return None
    return value


def _exec_cache_set(key: str, value: dict[str, Any], ttl_sec: int = 120) -> None:
    _EXEC_CACHE[key] = (time.monotonic() + ttl_sec, value)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _i(v: Any, default: int = 0) -> int:
    try:
        return int(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _mkt_code(marketplace_id: str | None) -> str:
    if not marketplace_id:
        return ""
    return MARKETPLACE_REGISTRY.get(marketplace_id, {}).get("code", marketplace_id[-2:])


def _fetchall_dict(cur) -> list[dict[str, Any]]:
    cols = [c[0] for c in cur.description] if cur.description else []
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]


def _health_label(score: float) -> dict:
    if score >= 90:
        return {"score": score, "label": "excellent", "color": "green"}
    if score >= 75:
        return {"score": score, "label": "healthy", "color": "blue"}
    if score >= 60:
        return {"score": score, "label": "watchlist", "color": "yellow"}
    if score >= 40:
        return {"score": score, "label": "risk", "color": "orange"}
    return {"score": score, "label": "critical", "color": "red"}


def _to_exec_opportunity(row: dict[str, Any]) -> dict[str, Any]:
    """Transform query_active() row → ExecOpportunity schema shape."""
    ps = _f(row.get("priority_score", 50))
    if ps >= 90:
        priority = "P1"
    elif ps >= 70:
        priority = "P2"
    else:
        priority = "P3"
    opp_type = row.get("opportunity_type", "")
    category = "risk" if "RISK" in opp_type else "growth"
    return {
        "id": row["id"],
        "opp_type": opp_type,
        "category": category,
        "priority": priority,
        "marketplace_id": row.get("marketplace_id"),
        "marketplace_code": row.get("marketplace_code"),
        "sku": row.get("sku"),
        "title": row.get("title", ""),
        "description": row.get("description"),
        "impact_estimate": row.get("impact_estimate"),
        "confidence": row.get("confidence_score"),
        "is_active": row.get("is_active", True),
        "created_at": row.get("created_at"),
    }


# ---------------------------------------------------------------------------
# 1) Executive Overview
# ---------------------------------------------------------------------------

def get_exec_overview(
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
) -> dict:
    """Top-level CEO view: KPIs, growth %, health score, risks, opportunities, best/worst SKUs."""
    cache_key = f"{date_from}:{date_to}:{marketplace_id or 'all'}"
    cached = _exec_cache_get(cache_key)
    if cached is not None:
        return cached
    conn = connect_acc(autocommit=False, timeout=20)
    try:
        cur = conn.cursor()
        mkt_clause = "AND m.marketplace_id = ?" if marketplace_id else ""
        params: list = [date_from, date_to]
        if marketplace_id:
            params.append(marketplace_id)

        # ── Current period KPIs ──
        cur.execute(f"""
            SELECT
                ISNULL(SUM(m.revenue_pln), 0),
                ISNULL(SUM(m.profit_pln), 0),
                ISNULL(SUM(m.orders), 0),
                ISNULL(SUM(m.units), 0),
                ISNULL(SUM(m.ad_spend_pln), 0),
                ISNULL(SUM(m.refund_pln), 0),
                ISNULL(SUM(m.sessions), 0),
                CASE WHEN SUM(m.units) > 0
                     THEN SUM(ISNULL(m.return_rate_pct, 0) * m.units / 100.0) * 100.0 / SUM(m.units)
                     ELSE NULL END,
                ISNULL(SUM(m.cm1_pln), 0),
                ISNULL(SUM(m.cm2_pln), 0)
            FROM dbo.executive_daily_metrics m WITH (NOLOCK)
            WHERE m.period_date >= ? AND m.period_date <= ?
            {mkt_clause}
        """, tuple(params))
        row = cur.fetchone()
        rev = _f(row[0]); prof = _f(row[1])
        orders = _i(row[2]); units = _i(row[3])
        ad = _f(row[4]); refund = _f(row[5]); sessions = _i(row[6])
        return_rate = _f(row[7]) if row[7] is not None else None
        cm1 = _f(row[8]); cm2 = _f(row[9])

        kpi = {
            "revenue_pln": round(rev, 2),
            "cm1_pln": round(cm1, 2),
            "cm2_pln": round(cm2, 2),
            "profit_pln": round(prof, 2),
            "profit_tier": "net_profit",  # SF-04: from rollup = Revenue minus ALL 9 cost categories
            "margin_pct": round(prof / rev * 100, 2) if rev else 0,
            "orders": orders,
            "units": units,
            "ad_spend_pln": round(ad, 2),
            "acos_pct": round(ad / rev * 100, 2) if rev else None,
            "return_rate_pct": round(return_rate, 2) if return_rate is not None else None,
            "revenue_growth_pct": None,
            "profit_growth_pct": None,
        }

        # ── Previous period (same length) for growth calculation ──
        period_days = (date_to - date_from).days + 1
        prev_to = date_from - timedelta(days=1)
        prev_from = prev_to - timedelta(days=period_days - 1)
        prev_params: list = [prev_from, prev_to]
        if marketplace_id:
            prev_params.append(marketplace_id)

        cur.execute(f"""
            SELECT ISNULL(SUM(m.revenue_pln), 0), ISNULL(SUM(m.profit_pln), 0),
                   ISNULL(SUM(m.cm1_pln), 0), ISNULL(SUM(m.cm2_pln), 0)
            FROM dbo.executive_daily_metrics m WITH (NOLOCK)
            WHERE m.period_date >= ? AND m.period_date <= ?
            {mkt_clause}
        """, tuple(prev_params))
        prev_row = cur.fetchone()
        prev_rev = _f(prev_row[0]); prev_prof = _f(prev_row[1])
        prev_cm1 = _f(prev_row[2]); prev_cm2 = _f(prev_row[3])

        kpi_prev = {
            "revenue_pln": round(prev_rev, 2),
            "cm1_pln": round(prev_cm1, 2),
            "cm2_pln": round(prev_cm2, 2),
            "profit_pln": round(prev_prof, 2),
            "margin_pct": round(prev_prof / prev_rev * 100, 2) if prev_rev else 0,
            "orders": 0, "units": 0, "ad_spend_pln": 0,
        }

        if prev_rev > 0:
            kpi["revenue_growth_pct"] = round((rev - prev_rev) / prev_rev * 100, 1)
        if prev_prof > 0:
            kpi["profit_growth_pct"] = round((prof - prev_prof) / prev_prof * 100, 1)

        # ── Latest health score ──
        cur.execute("""
            SELECT TOP 1 period_date, revenue_score, profit_score, demand_score,
                   inventory_score, operations_score, overall_score
            FROM dbo.executive_health_score WITH (NOLOCK)
            ORDER BY period_date DESC
        """)
        hs_row = cur.fetchone()
        health = None
        health_lbl = None
        if hs_row:
            health = {
                "period_date": str(hs_row[0]),
                "revenue_score": _f(hs_row[1]),
                "profit_score": _f(hs_row[2]),
                "demand_score": _f(hs_row[3]),
                "inventory_score": _f(hs_row[4]),
                "operations_score": _f(hs_row[5]),
                "overall_score": _f(hs_row[6]),
            }
            health_lbl = _health_label(_f(hs_row[6]))

        # ── Active risks (top 20) — from unified growth_opportunity ──
        _RISK_TYPES = [
            "EXEC_RISK_PROFIT_DECLINE", "EXEC_RISK_LOW_MARGIN",
            "EXEC_RISK_HIGH_RETURN", "EXEC_RISK_AD_INEFFICIENCY",
        ]
        risks = [
            _to_exec_opportunity(r)
            for r in query_active(
                cur,
                opportunity_types=_RISK_TYPES,
                marketplace_id=marketplace_id,
                limit=20,
                order="priority_score DESC",
            )
        ]

        # ── Active growth opportunities (top 20) — from unified growth_opportunity ──
        _GROWTH_TYPES = [
            "EXEC_MARGIN_OPTIMIZATION", "EXEC_MARKETPLACE_EXPANSION",
        ]
        growth = [
            _to_exec_opportunity(r)
            for r in query_active(
                cur,
                opportunity_types=_GROWTH_TYPES,
                marketplace_id=marketplace_id,
                limit=20,
                order="priority_score DESC",
            )
        ]

        # ── Best / Worst SKUs (from profitability rollup) ──
        sku_params: list = [date_from, date_to]
        if marketplace_id:
            sku_params.append(marketplace_id)
        sku_mkt = "AND r.marketplace_id = ?" if marketplace_id else ""

        cur.execute(f"""
            SELECT TOP 20 r.sku, MAX(r.asin), r.marketplace_id,
                SUM(r.revenue_pln), SUM(r.profit_pln),
                CASE WHEN SUM(r.revenue_pln)<>0 THEN SUM(r.profit_pln)/SUM(r.revenue_pln)*100 ELSE 0 END,
                SUM(r.units_sold),
                SUM(r.cm1_pln), SUM(r.cm2_pln)
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            WHERE r.period_date >= ? AND r.period_date <= ? {sku_mkt}
              AND r.sku NOT LIKE 'amzn.gr.%%' AND r.sku NOT LIKE 'amazon.found%%'
            GROUP BY r.sku, r.marketplace_id
            HAVING SUM(r.revenue_pln) > 0
            ORDER BY SUM(r.profit_pln) DESC
        """, tuple(sku_params))
        best_skus = [
            {"sku": r[0], "asin": r[1], "marketplace_id": r[2], "marketplace_code": _mkt_code(r[2]),
             "revenue_pln": round(_f(r[3]), 2), "profit_pln": round(_f(r[4]), 2),
             "margin_pct": round(_f(r[5]), 2), "units": _i(r[6]),
             "cm1_pln": round(_f(r[7]), 2), "cm2_pln": round(_f(r[8]), 2)}
            for r in cur.fetchall()
        ]

        cur.execute(f"""
            SELECT TOP 20 r.sku, MAX(r.asin), r.marketplace_id,
                SUM(r.revenue_pln), SUM(r.profit_pln),
                CASE WHEN SUM(r.revenue_pln)<>0 THEN SUM(r.profit_pln)/SUM(r.revenue_pln)*100 ELSE 0 END,
                SUM(r.units_sold),
                SUM(r.cm1_pln), SUM(r.cm2_pln)
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            WHERE r.period_date >= ? AND r.period_date <= ? {sku_mkt}
              AND r.sku NOT LIKE 'amzn.gr.%%' AND r.sku NOT LIKE 'amazon.found%%'
            GROUP BY r.sku, r.marketplace_id
            HAVING SUM(r.revenue_pln) > 0
            ORDER BY SUM(r.profit_pln) ASC
        """, tuple(sku_params))
        worst_skus = [
            {"sku": r[0], "asin": r[1], "marketplace_id": r[2], "marketplace_code": _mkt_code(r[2]),
             "revenue_pln": round(_f(r[3]), 2), "profit_pln": round(_f(r[4]), 2),
             "margin_pct": round(_f(r[5]), 2), "units": _i(r[6]),
             "cm1_pln": round(_f(r[7]), 2), "cm2_pln": round(_f(r[8]), 2)}
            for r in cur.fetchall()
        ]

        result = {
            "kpi": kpi,
            "kpi_prev": kpi_prev,
            "health": health,
            "health_label": health_lbl,
            "risks": risks,
            "growth": growth,
            "best_skus": best_skus,
            "worst_skus": worst_skus,
        }
        _exec_cache_set(cache_key, result, ttl_sec=120)
        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 2) Executive Products
# ---------------------------------------------------------------------------

def get_exec_products(
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
    sku: str | None = None,
    sort: str = "profit_pln",
    direction: str = "desc",
    page: int = 1,
    page_size: int = 50,
) -> dict:
    allowed_sorts = {
        "revenue_pln", "cm1_pln", "cm2_pln", "profit_pln", "margin_pct",
        "units", "return_rate_pct", "acos_pct", "sessions",
    }
    sort_col = sort if sort in allowed_sorts else "profit_pln"
    direction = "DESC" if direction.lower() != "asc" else "ASC"
    offset = (page - 1) * page_size

    conn = connect_acc(autocommit=False, timeout=20)
    try:
        cur = conn.cursor()
        where_parts = ["r.period_date >= ? AND r.period_date <= ?",
                       "r.sku NOT LIKE 'amzn.gr.%%' AND r.sku NOT LIKE 'amazon.found%%'"]
        params: list = [date_from, date_to]
        if marketplace_id:
            where_parts.append("r.marketplace_id = ?")
            params.append(marketplace_id)
        if sku:
            where_parts.append("r.sku LIKE ?")
            params.append(f"%{sku}%")
        where_sql = " AND ".join(where_parts)

        # Count
        cur.execute(f"""
            SELECT COUNT(DISTINCT CONCAT(r.sku, '|', r.marketplace_id))
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            WHERE {where_sql}
        """, tuple(params))
        total = _i(cur.fetchone()[0])
        pages = max(1, math.ceil(total / page_size))

        # Data
        cur.execute(f"""
            SELECT
                r.sku, MAX(r.asin), r.marketplace_id,
                SUM(r.units_sold) as units,
                SUM(r.revenue_pln) as revenue_pln,
                SUM(r.profit_pln) as profit_pln,
                CASE WHEN SUM(r.revenue_pln)<>0
                     THEN SUM(r.profit_pln)/SUM(r.revenue_pln)*100 ELSE 0 END as margin_pct,
                CASE WHEN SUM(r.revenue_pln)<>0
                     THEN SUM(r.ad_spend_pln)/SUM(r.revenue_pln)*100 ELSE NULL END as acos_pct,
                CASE WHEN SUM(r.units_sold)<>0
                     THEN SUM(r.refund_units)*100.0/SUM(r.units_sold) ELSE NULL END as return_rate_pct,
                MAX(t.sessions) as sessions,
                SUM(r.orders_count) as orders_count,
                CASE WHEN MAX(t.sessions) > 0
                     THEN SUM(r.orders_count)*100.0/MAX(t.sessions) ELSE NULL END as cvr_pct,
                SUM(r.cm1_pln) as cm1_pln,
                SUM(r.cm2_pln) as cm2_pln
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            LEFT JOIN dbo.acc_inv_traffic_rollup t WITH (NOLOCK)
              ON t.sku = r.sku AND t.marketplace_id = r.marketplace_id AND t.range_key = '30d'
            WHERE {where_sql}
            GROUP BY r.sku, r.marketplace_id
            ORDER BY {sort_col} {direction}
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, (*params, offset, page_size))

        items = [
            {
                "sku": r[0], "asin": r[1], "marketplace_id": r[2],
                "marketplace_code": _mkt_code(r[2]),
                "revenue_pln": round(_f(r[4]), 2),
                "cm1_pln": round(_f(r[12]), 2),
                "cm2_pln": round(_f(r[13]), 2),
                "profit_pln": round(_f(r[5]), 2),
                "margin_pct": round(_f(r[6]), 2),
                "units": _i(r[3]),
                "acos_pct": round(_f(r[7]), 2) if r[7] is not None else None,
                "return_rate_pct": round(_f(r[8]), 2) if r[8] is not None else None,
                "sessions": _i(r[9]) if r[9] else None,
                "cvr_pct": round(_f(r[11]), 2) if r[11] is not None else None,
                "inventory_risk": None,  # NOT COMPUTED — requires inventory model; null = unsupported
            }
            for r in cur.fetchall()
        ]
        return {"total": total, "page": page, "page_size": page_size, "pages": pages, "items": items}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 3) Executive Marketplaces
# ---------------------------------------------------------------------------

def get_exec_marketplaces(date_from: date, date_to: date) -> list[dict]:
    conn = connect_acc(autocommit=False, timeout=15)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                m.marketplace_id,
                SUM(m.revenue_pln),
                SUM(m.profit_pln),
                CASE WHEN SUM(m.revenue_pln)<>0 THEN SUM(m.profit_pln)/SUM(m.revenue_pln)*100 ELSE 0 END,
                SUM(m.orders), SUM(m.units),
                SUM(m.sessions),
                CASE WHEN SUM(m.sessions)>0 THEN SUM(m.orders)*100.0/SUM(m.sessions) ELSE NULL END,
                CASE WHEN SUM(m.revenue_pln)<>0 THEN SUM(m.ad_spend_pln)/SUM(m.revenue_pln)*100 ELSE NULL END,
                CASE WHEN SUM(m.units)>0 THEN SUM(m.refund_pln)/SUM(m.revenue_pln)*100 ELSE NULL END,
                SUM(m.cm1_pln),
                SUM(m.cm2_pln)
            FROM dbo.executive_daily_metrics m WITH (NOLOCK)
            WHERE m.period_date >= ? AND m.period_date <= ?
            GROUP BY m.marketplace_id
            ORDER BY SUM(m.revenue_pln) DESC
        """, (date_from, date_to))
        return [
            {
                "marketplace_id": r[0], "marketplace_code": _mkt_code(r[0]),
                "revenue_pln": round(_f(r[1]), 2),
                "cm1_pln": round(_f(r[10]), 2),
                "cm2_pln": round(_f(r[11]), 2),
                "profit_pln": round(_f(r[2]), 2),
                "margin_pct": round(_f(r[3]), 2),
                "orders": _i(r[4]), "units": _i(r[5]),
                "sessions": _i(r[6]) if r[6] else None,
                "cvr_pct": round(_f(r[7]), 2) if r[7] is not None else None,
                "acos_pct": round(_f(r[8]), 2) if r[8] is not None else None,
                "return_rate_pct": round(_f(r[9]), 2) if r[9] is not None else None,
                "health_score": None,
            }
            for r in cur.fetchall()
        ]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 4) Recompute daily metrics (aggregation job)
# ---------------------------------------------------------------------------

def recompute_executive_metrics(days_back: int = 7) -> dict:
    """
    Aggregate from acc_sku_profitability_rollup → executive_daily_metrics.
    MERGE upsert per (date, marketplace).
    """
    t0 = time.time()
    date_from = date.today() - timedelta(days=days_back)
    date_to = date.today()

    conn = connect_acc(autocommit=False, timeout=120)
    try:
        cur = conn.cursor()
        cur.execute("""
            MERGE dbo.executive_daily_metrics AS tgt
            USING (
                SELECT
                    r.period_date,
                    r.marketplace_id,
                    SUM(r.revenue_pln) as revenue_pln,
                    SUM(r.profit_pln) as profit_pln,
                    SUM(r.cm1_pln) as cm1_pln,
                    SUM(r.cm2_pln) as cm2_pln,
                    CASE WHEN SUM(r.revenue_pln)<>0
                         THEN SUM(r.profit_pln)/SUM(r.revenue_pln)*100 ELSE 0 END as margin_pct,
                    SUM(r.units_sold) as units,
                    SUM(r.orders_count) as orders,
                    SUM(r.ad_spend_pln) as ad_spend_pln,
                    CASE WHEN SUM(r.revenue_pln)<>0
                         THEN SUM(r.ad_spend_pln)/SUM(r.revenue_pln)*100 ELSE NULL END as acos_pct,
                    CASE WHEN SUM(r.units_sold)<>0
                         THEN SUM(r.refund_units)*100.0/SUM(r.units_sold) ELSE NULL END as return_rate_pct,
                    SUM(r.refund_pln) as refund_pln,
                    SUM(r.cogs_pln) as cogs_pln
                FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
                WHERE r.period_date >= ? AND r.period_date <= ?
                GROUP BY r.period_date, r.marketplace_id
            ) AS src
            ON tgt.period_date = src.period_date AND tgt.marketplace_id = src.marketplace_id
            WHEN MATCHED THEN UPDATE SET
                revenue_pln = src.revenue_pln, profit_pln = src.profit_pln,
                cm1_pln = src.cm1_pln, cm2_pln = src.cm2_pln,
                margin_pct = src.margin_pct, units = src.units, orders = src.orders,
                ad_spend_pln = src.ad_spend_pln, acos_pct = src.acos_pct,
                return_rate_pct = src.return_rate_pct, refund_pln = src.refund_pln,
                cogs_pln = src.cogs_pln, computed_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN INSERT (
                period_date, marketplace_id, revenue_pln, profit_pln, cm1_pln, cm2_pln,
                margin_pct, units, orders, ad_spend_pln, acos_pct, return_rate_pct,
                refund_pln, cogs_pln, computed_at
            ) VALUES (
                src.period_date, src.marketplace_id, src.revenue_pln, src.profit_pln,
                src.cm1_pln, src.cm2_pln,
                src.margin_pct, src.units, src.orders, src.ad_spend_pln, src.acos_pct,
                src.return_rate_pct, src.refund_pln, src.cogs_pln, SYSUTCDATETIME()
            );
        """, (date_from, date_to))
        metrics_rows = cur.rowcount
        conn.commit()

        elapsed = round(time.time() - t0, 1)
        log.info("executive.metrics_done", rows=metrics_rows, elapsed=elapsed)
        return {"metrics_rows": metrics_rows, "elapsed": elapsed}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 5) Compute Health Score
# ---------------------------------------------------------------------------

def compute_health_score(target_date: date | None = None) -> dict:
    """
    Compute CEO health score for a given date.

    Weights: profitability 30%, inventory 20%, demand 20%, operations 15%, ads 15%

    Each sub-score is 0-100 based on thresholds.
    """
    if target_date is None:
        target_date = date.today()

    # Compare last 7d vs prev 7d
    d_end = target_date
    d_start = target_date - timedelta(days=6)
    p_end = d_start - timedelta(days=1)
    p_start = p_end - timedelta(days=6)

    conn = connect_acc(autocommit=False, timeout=20)
    try:
        cur = conn.cursor()

        def _sums(df: date, dt: date):
            cur.execute("""
                SELECT
                    ISNULL(SUM(revenue_pln), 0), ISNULL(SUM(profit_pln), 0),
                    ISNULL(SUM(orders), 0), ISNULL(SUM(units), 0),
                    ISNULL(SUM(ad_spend_pln), 0), ISNULL(SUM(refund_pln), 0),
                    ISNULL(SUM(stockout_skus), 0), ISNULL(SUM(suppressed_skus), 0)
                FROM dbo.executive_daily_metrics WITH (NOLOCK)
                WHERE period_date >= ? AND period_date <= ?
            """, (df, dt))
            return cur.fetchone()

        c = _sums(d_start, d_end)
        p = _sums(p_start, p_end)

        c_rev, c_prof = _f(c[0]), _f(c[1])
        c_ord, c_units = _i(c[2]), _i(c[3])
        c_ad, c_ref = _f(c[4]), _f(c[5])
        c_stock, c_supp = _i(c[6]), _i(c[7])

        p_rev, p_prof = _f(p[0]), _f(p[1])

        # -- Profitability score (30%) --
        margin = (c_prof / c_rev * 100) if c_rev else 0
        profit_growth = ((c_prof - p_prof) / p_prof * 100) if p_prof > 0 else 0
        # Margin > 15% → 100, 10% → 80, 5% → 60, 0% → 40, <0 → 20
        if margin >= 15: ms = 100
        elif margin >= 10: ms = 80
        elif margin >= 5: ms = 60
        elif margin >= 0: ms = 40
        else: ms = 20
        # Growth bonus/penalty
        if profit_growth > 10: ms = min(100, ms + 10)
        elif profit_growth < -10: ms = max(0, ms - 15)
        profit_score = round(ms, 1)

        # -- Revenue/Demand score (20%) --
        rev_growth = ((c_rev - p_rev) / p_rev * 100) if p_rev > 0 else 0
        if rev_growth > 15: ds = 95
        elif rev_growth > 5: ds = 80
        elif rev_growth > 0: ds = 70
        elif rev_growth > -5: ds = 55
        elif rev_growth > -15: ds = 35
        else: ds = 20
        demand_score = round(ds, 1)

        # -- Revenue score (independent — based on absolute daily revenue level) --
        avg_daily_rev = c_rev / 7.0 if c_rev else 0
        if avg_daily_rev > 150_000: rs = 95
        elif avg_daily_rev > 100_000: rs = 85
        elif avg_daily_rev > 50_000: rs = 70
        elif avg_daily_rev > 20_000: rs = 55
        elif avg_daily_rev > 5_000: rs = 40
        else: rs = 20
        # Bonus for strong growth on top of healthy absolute level
        if rev_growth > 10 and rs >= 55: rs = min(100, rs + 10)
        elif rev_growth < -10: rs = max(0, rs - 10)
        revenue_score = round(rs, 1)

        # -- Inventory score (20%) --
        # Lower stockout / suppressed → better
        inv_penalty = c_stock * 2 + c_supp
        if inv_penalty == 0: inv_s = 95
        elif inv_penalty < 5: inv_s = 80
        elif inv_penalty < 15: inv_s = 60
        elif inv_penalty < 30: inv_s = 40
        else: inv_s = 20
        inventory_score = round(inv_s, 1)

        # -- Operations score (15%) --
        ret_rate = (c_ref / c_rev * 100) if c_rev else 0
        if ret_rate < 2: ops = 95
        elif ret_rate < 4: ops = 80
        elif ret_rate < 7: ops = 60
        elif ret_rate < 10: ops = 40
        else: ops = 20
        operations_score = round(ops, 1)

        # -- Overall --
        overall = round(
            profit_score * 0.30
            + inventory_score * 0.20
            + demand_score * 0.20
            + operations_score * 0.15
            + revenue_score * 0.15,
            1
        )

        # MERGE health score
        cur.execute("""
            MERGE dbo.executive_health_score AS tgt
            USING (SELECT ? as pd) AS src ON tgt.period_date = src.pd
            WHEN MATCHED THEN UPDATE SET
                revenue_score = ?, profit_score = ?, demand_score = ?,
                inventory_score = ?, operations_score = ?, overall_score = ?,
                computed_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN INSERT (
                period_date, revenue_score, profit_score, demand_score,
                inventory_score, operations_score, overall_score, computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
        """, (
            target_date,
            revenue_score, profit_score, demand_score,
            inventory_score, operations_score, overall,
            target_date,
            revenue_score, profit_score, demand_score,
            inventory_score, operations_score, overall,
        ))
        conn.commit()

        log.info("executive.health_score_done", date=str(target_date), overall=overall)

        def _tl(score: float) -> str:
            """Traffic light: green >= 70, amber >= 40, red < 40."""
            if score >= 70: return "green"
            if score >= 40: return "amber"
            return "red"

        return {
            "period_date": str(target_date),
            "revenue_score": revenue_score,
            "profit_score": profit_score,
            "demand_score": demand_score,
            "inventory_score": inventory_score,
            "operations_score": operations_score,
            "overall_score": overall,
            "traffic_light": {
                "revenue": _tl(revenue_score),
                "profitability": _tl(profit_score),
                "demand": _tl(demand_score),
                "inventory": _tl(inventory_score),
                "operations": _tl(operations_score),
                "overall": _tl(overall),
            },
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 6) Risk Detection Engine
# ---------------------------------------------------------------------------

def detect_risks(days_back: int = 7) -> list[dict]:
    """
    Scan data for business risks.  Writes to unified ``growth_opportunity``
    table via the growth-opportunity access layer (Sprint 8 – S8.1).

    P1: rapid profit decline, margin < 5%, high return rate > 10%
    P2: ad inefficiency
    """
    date_to = date.today()
    date_from = date_to - timedelta(days=days_back)
    prev_from = date_from - timedelta(days=days_back)
    prev_to = date_from - timedelta(days=1)

    conn = connect_acc(autocommit=False, timeout=30)
    found: list[dict] = []
    try:
        cur = conn.cursor()

        # Deactivate old executive risk opps before re-detection
        _RISK_TYPES = [
            "EXEC_RISK_PROFIT_DECLINE", "EXEC_RISK_LOW_MARGIN",
            "EXEC_RISK_HIGH_RETURN", "EXEC_RISK_AD_INEFFICIENCY",
        ]
        deactivate_by_types(cur, _RISK_TYPES)

        # --- P1: Profit decline > 20% per marketplace ---
        cur.execute("""
            SELECT c.marketplace_id,
                   SUM(c.profit_pln) as c_prof,
                   SUM(p.profit_pln) as p_prof
            FROM (
                SELECT marketplace_id, SUM(profit_pln) as profit_pln
                FROM dbo.executive_daily_metrics WITH (NOLOCK)
                WHERE period_date >= ? AND period_date <= ?
                GROUP BY marketplace_id
            ) c
            LEFT JOIN (
                SELECT marketplace_id, SUM(profit_pln) as profit_pln
                FROM dbo.executive_daily_metrics WITH (NOLOCK)
                WHERE period_date >= ? AND period_date <= ?
                GROUP BY marketplace_id
            ) p ON c.marketplace_id = p.marketplace_id
            WHERE p.profit_pln > 0
              AND (c.profit_pln - p.profit_pln) / p.profit_pln * 100 < -20
            GROUP BY c.marketplace_id
        """, (date_from, date_to, prev_from, prev_to))
        for r in cur.fetchall():
            decline = round((_f(r[1]) - _f(r[2])) / _f(r[2]) * 100, 1) if _f(r[2]) else 0
            impact = round(abs(_f(r[1]) - _f(r[2])), 2)
            insert_opportunity(
                cur,
                opportunity_type="EXEC_RISK_PROFIT_DECLINE",
                marketplace_id=r[0],
                title=f"Profit declined {decline:.0f}% on {_mkt_code(r[0])}",
                description=f"Current: {_f(r[1]):.0f} PLN vs prev: {_f(r[2]):.0f} PLN",
                root_cause="profit_decline",
                recommendation=f"Investigate {_mkt_code(r[0])} — check pricing, ad spend, returns for root cause of {abs(decline):.0f}% drop.",
                priority_score=priority_from_label("P1"),
                confidence_score=70,
                revenue_uplift=round(_f(r[2]) - _f(r[1]), 2),
                profit_uplift=impact,
                effort=40,
                owner_role="executive",
            )
            found.append({
                "opportunity_type": "EXEC_RISK_PROFIT_DECLINE",
                "marketplace_id": r[0],
                "title": f"Profit declined {decline:.0f}% on {_mkt_code(r[0])}",
                "impact_estimate": impact,
            })

        # --- P1: Low margin PARENT ASINs (<5% on >500 PLN revenue) ---
        cur.execute("""
            SELECT COALESCE(ls.parent_asin, r.asin, r.sku) AS group_key,
                   r.marketplace_id,
                   SUM(r.revenue_pln) as rev,
                   CASE WHEN SUM(r.revenue_pln)<>0 THEN SUM(r.profit_pln)/SUM(r.revenue_pln)*100 ELSE 0 END as m,
                   MAX(ls.title) as product_title
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            LEFT JOIN dbo.acc_listing_state ls WITH (NOLOCK)
              ON ls.seller_sku = r.sku AND ls.marketplace_id = r.marketplace_id
            WHERE r.period_date >= ? AND r.period_date <= ?
              AND r.sku NOT LIKE 'amzn.gr.%%' AND r.sku NOT LIKE 'amazon.found%%'
            GROUP BY COALESCE(ls.parent_asin, r.asin, r.sku), r.marketplace_id
            HAVING SUM(r.revenue_pln) > 500
               AND CASE WHEN SUM(r.revenue_pln)<>0 THEN SUM(r.profit_pln)/SUM(r.revenue_pln)*100 ELSE 0 END < 5
            ORDER BY SUM(r.revenue_pln) DESC
        """, (date_from, date_to))
        for r in cur.fetchall():
            impact = round(_f(r[2]) * 0.1, 2)
            prod_name = r[4] or r[0]
            short_name = (prod_name[:50] + "…") if len(prod_name) > 50 else prod_name
            insert_opportunity(
                cur,
                opportunity_type="EXEC_RISK_LOW_MARGIN",
                marketplace_id=r[1],
                parent_asin=r[0],
                title=f"Low margin {_f(r[3]):.1f}% — {short_name} ({_mkt_code(r[1])})",
                description=f"Revenue {_f(r[2]):.0f} PLN but margin only {_f(r[3]):.1f}%",
                root_cause="cost_problem",
                recommendation=f"Review COGS, logistics and ad spend. Target margin > 10%. Revenue {_f(r[2]):.0f} PLN at risk.",
                priority_score=priority_from_label("P1"),
                confidence_score=75,
                revenue_uplift=round(_f(r[2]), 2),
                profit_uplift=impact,
                effort=30,
                owner_role="executive",
            )
            found.append({
                "opportunity_type": "EXEC_RISK_LOW_MARGIN",
                "marketplace_id": r[1], "parent_asin": r[0],
                "title": f"Low margin {_f(r[3]):.1f}% — {short_name} ({_mkt_code(r[1])})",
                "impact_estimate": impact,
            })

        # --- P1: High return rate > 10% (grouped by parent ASIN) ---
        cur.execute("""
            SELECT COALESCE(ls.parent_asin, r.asin, r.sku) AS group_key,
                   r.marketplace_id,
                   SUM(r.units_sold) as u,
                   CASE WHEN SUM(r.units_sold)>0 THEN SUM(r.refund_units)*100.0/SUM(r.units_sold) ELSE 0 END as rr,
                   MAX(ls.title) as product_title
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            LEFT JOIN dbo.acc_listing_state ls WITH (NOLOCK)
              ON ls.seller_sku = r.sku AND ls.marketplace_id = r.marketplace_id
            WHERE r.period_date >= ? AND r.period_date <= ?
              AND r.sku NOT LIKE 'amzn.gr.%%' AND r.sku NOT LIKE 'amazon.found%%'
            GROUP BY COALESCE(ls.parent_asin, r.asin, r.sku), r.marketplace_id
            HAVING SUM(r.units_sold) >= 10
               AND CASE WHEN SUM(r.units_sold)>0 THEN SUM(r.refund_units)*100.0/SUM(r.units_sold) ELSE 0 END > 10
            ORDER BY SUM(r.refund_units)*100.0/SUM(r.units_sold) DESC
        """, (date_from, date_to))
        for r in cur.fetchall():
            prod_name = r[4] or r[0]
            short_name = (prod_name[:50] + "…") if len(prod_name) > 50 else prod_name
            insert_opportunity(
                cur,
                opportunity_type="EXEC_RISK_HIGH_RETURN",
                marketplace_id=r[1],
                parent_asin=r[0],
                title=f"Return rate {_f(r[3]):.1f}% — {short_name} ({_mkt_code(r[1])})",
                description=f"{_i(r[2])} units sold, return rate {_f(r[3]):.1f}%",
                root_cause="quality_problem",
                recommendation=f"Check listing accuracy, packaging, product quality. Return rate {_f(r[3]):.1f}% vs target <5%.",
                priority_score=priority_from_label("P1"),
                confidence_score=80,
                profit_uplift=round(_i(r[2]) * (_f(r[3]) - 5.0) / 100 * 15, 2),
                effort=50,
                owner_role="executive",
            )
            found.append({
                "opportunity_type": "EXEC_RISK_HIGH_RETURN",
                "marketplace_id": r[1], "parent_asin": r[0],
                "title": f"Return rate {_f(r[3]):.1f}% — {short_name} ({_mkt_code(r[1])})",
            })

        # --- P2: Ad inefficiency (ACOS > 30%, grouped by parent ASIN) ---
        cur.execute("""
            SELECT COALESCE(ls.parent_asin, r.asin, r.sku) AS group_key,
                   r.marketplace_id,
                   SUM(r.ad_spend_pln) as ad,
                   CASE WHEN SUM(r.revenue_pln)<>0 THEN SUM(r.ad_spend_pln)/SUM(r.revenue_pln)*100 ELSE 0 END as acos,
                   MAX(ls.title) as product_title
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            LEFT JOIN dbo.acc_listing_state ls WITH (NOLOCK)
              ON ls.seller_sku = r.sku AND ls.marketplace_id = r.marketplace_id
            WHERE r.period_date >= ? AND r.period_date <= ?
              AND r.sku NOT LIKE 'amzn.gr.%%' AND r.sku NOT LIKE 'amazon.found%%'
            GROUP BY COALESCE(ls.parent_asin, r.asin, r.sku), r.marketplace_id
            HAVING SUM(r.ad_spend_pln) > 50
               AND CASE WHEN SUM(r.revenue_pln)<>0 THEN SUM(r.ad_spend_pln)/SUM(r.revenue_pln)*100 ELSE 0 END > 30
            ORDER BY SUM(r.ad_spend_pln) DESC
        """, (date_from, date_to))
        for r in cur.fetchall():
            impact = round(_f(r[2]) * 0.3, 2)
            prod_name = r[4] or r[0]
            short_name = (prod_name[:50] + "…") if len(prod_name) > 50 else prod_name
            insert_opportunity(
                cur,
                opportunity_type="EXEC_RISK_AD_INEFFICIENCY",
                marketplace_id=r[1],
                parent_asin=r[0],
                title=f"High ACOS {_f(r[3]):.0f}% — {short_name} ({_mkt_code(r[1])})",
                description=f"Ad spend {_f(r[2]):.0f} PLN with ACOS {_f(r[3]):.1f}%",
                root_cause="ad_inefficiency",
                recommendation=f"Reduce bids or pause unprofitable keywords. ACOS {_f(r[3]):.1f}% vs target <20%. Potential savings {impact:.0f} PLN.",
                priority_score=priority_from_label("P2"),
                confidence_score=70,
                revenue_uplift=round(_f(r[2]), 2),
                profit_uplift=impact,
                effort=20,
                owner_role="executive",
            )
            found.append({
                "opportunity_type": "EXEC_RISK_AD_INEFFICIENCY",
                "marketplace_id": r[1], "parent_asin": r[0],
                "title": f"High ACOS {_f(r[3]):.0f}% — {short_name} ({_mkt_code(r[1])})",
                "impact_estimate": impact,
            })

        conn.commit()
        log.info("executive.risks_detected", count=len(found))
        return found
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 7) Growth Opportunity Detection
# ---------------------------------------------------------------------------

def detect_growth_opportunities(days_back: int = 7) -> list[dict]:
    """
    Detect growth opportunities:
    - High revenue SKUs with low margin (price optimization)
    - SKUs with strong demand but low availability
    """
    date_to = date.today()
    date_from = date_to - timedelta(days=days_back)

    conn = connect_acc(autocommit=False, timeout=30)
    found: list[dict] = []
    try:
        cur = conn.cursor()

        # Deactivate old executive growth opps before re-detection
        _GROWTH_TYPES = ["EXEC_MARGIN_OPTIMIZATION", "EXEC_MARKETPLACE_EXPANSION"]
        deactivate_by_types(cur, _GROWTH_TYPES)

        # --- High revenue + margin improvement potential (grouped by parent ASIN) ---
        cur.execute("""
            SELECT COALESCE(ls.parent_asin, r.asin, r.sku) AS group_key,
                   r.marketplace_id,
                   SUM(r.revenue_pln) as rev,
                   CASE WHEN SUM(r.revenue_pln)<>0 THEN SUM(r.profit_pln)/SUM(r.revenue_pln)*100 ELSE 0 END as m,
                   SUM(r.units_sold) as units,
                   MAX(ls.title) as product_title
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            LEFT JOIN dbo.acc_listing_state ls WITH (NOLOCK)
              ON ls.seller_sku = r.sku AND ls.marketplace_id = r.marketplace_id
            WHERE r.period_date >= ? AND r.period_date <= ?
              AND r.sku NOT LIKE 'amzn.gr.%%' AND r.sku NOT LIKE 'amazon.found%%'
            GROUP BY COALESCE(ls.parent_asin, r.asin, r.sku), r.marketplace_id
            HAVING SUM(r.revenue_pln) > 1000
               AND CASE WHEN SUM(r.revenue_pln)<>0 THEN SUM(r.profit_pln)/SUM(r.revenue_pln)*100 ELSE 0 END BETWEEN 5 AND 15
            ORDER BY SUM(r.revenue_pln) DESC
        """, (date_from, date_to))
        for r in cur.fetchall():
            potential = round(_f(r[2]) * 0.05, 2)
            prod_name = r[5] or r[0]
            short_name = (prod_name[:50] + "…") if len(prod_name) > 50 else prod_name
            insert_opportunity(
                cur,
                opportunity_type="EXEC_MARGIN_OPTIMIZATION",
                marketplace_id=r[1],
                parent_asin=r[0],
                title=f"Margin optimization — {short_name} ({_mkt_code(r[1])})",
                description=f"Revenue {_f(r[2]):.0f} PLN, margin {_f(r[3]):.1f}% — 5pp improvement = +{potential:.0f} PLN",
                root_cause="pricing_gap",
                recommendation=f"Test 3-5% price increase or negotiate lower COGS. Current margin {_f(r[3]):.1f}% leaves room for optimization.",
                priority_score=priority_from_label("P2"),
                confidence_score=70,
                revenue_uplift=round(_f(r[2]), 2),
                profit_uplift=potential,
                effort=25,
                owner_role="executive",
            )
            found.append({
                "opportunity_type": "EXEC_MARGIN_OPTIMIZATION",
                "marketplace_id": r[1], "parent_asin": r[0],
                "title": f"Margin optimization — {short_name} ({_mkt_code(r[1])})",
                "impact_estimate": potential,
            })

        # --- Top sellers by units (potential for expansion) ---
        cur.execute("""
            SELECT r.sku, r.marketplace_id,
                SUM(r.units_sold) as units,
                SUM(r.revenue_pln) as rev,
                SUM(r.profit_pln) as prof
            FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
            WHERE r.period_date >= ? AND r.period_date <= ?
              AND r.sku NOT LIKE 'amzn.gr.%%' AND r.sku NOT LIKE 'amazon.found%%'
            GROUP BY r.sku, r.marketplace_id
            HAVING SUM(r.units_sold) > 50
               AND SUM(r.profit_pln) > 100
            ORDER BY SUM(r.profit_pln) DESC
        """, (date_from, date_to))
        rows = cur.fetchall()

        # Check if these top SKUs are present on all marketplaces
        # Also check FBA inventory for physical presence (not just sales)
        asin_fba_presence: dict[str, set[str]] = {}
        try:
            cur.execute("""
                SELECT DISTINCT asin, marketplace_id
                FROM dbo.acc_fba_inventory_snapshot WITH (NOLOCK)
                WHERE snapshot_date = (
                    SELECT MAX(snapshot_date) FROM dbo.acc_fba_inventory_snapshot
                )
            """)
            for fba_row in cur.fetchall():
                if fba_row[0]:
                    asin_fba_presence.setdefault(fba_row[0], set()).add(fba_row[1])
        except Exception:
            pass  # table may not exist

        if rows:
            top_skus = list(set(r[0] for r in rows[:20]))
            for sku_val in top_skus:
                cur.execute("""
                    SELECT DISTINCT r.marketplace_id, r.asin
                    FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
                    WHERE r.sku = ? AND r.period_date >= ? AND r.period_date <= ?
                """, (sku_val, date_from, date_to))
                sku_rows = cur.fetchall()
                active_mkts = {r[0] for r in sku_rows}
                # Also consider FBA inventory presence via ASIN
                sku_asin = next((r[1] for r in sku_rows if r[1]), None)
                if sku_asin:
                    active_mkts |= asin_fba_presence.get(sku_asin, set())
                all_mkts = set(MARKETPLACE_REGISTRY.keys())
                missing = all_mkts - active_mkts
                if len(missing) >= 2:
                    codes = ", ".join(sorted(_mkt_code(m) for m in missing))
                    est_rev = round(sum(_f(r[3]) for r in rows if r[0] == sku_val) * 0.15, 2)
                    est_prof = round(est_rev * 0.1, 2)
                    insert_opportunity(
                        cur,
                        opportunity_type="EXEC_MARKETPLACE_EXPANSION",
                        parent_asin=sku_asin or sku_val,
                        sku=sku_val,
                        title=f"Expand {sku_val} to {codes}",
                        description=f"Strong seller on {len(active_mkts)} markets, missing from {codes}",
                        root_cause="expansion_gap",
                        recommendation=f"Launch on {codes}. Prepare listing translation and check FBA readiness.",
                        priority_score=priority_from_label("P2"),
                        confidence_score=50,
                        revenue_uplift=est_rev,
                        profit_uplift=est_prof,
                        effort=60,
                        owner_role="executive",
                    )
                    found.append({
                        "opportunity_type": "EXEC_MARKETPLACE_EXPANSION",
                        "sku": sku_val,
                        "title": f"Expand {sku_val} to {codes}",
                    })

        conn.commit()
        log.info("executive.growth_detected", count=len(found))
        return found
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 8) Full recompute orchestrator (called by scheduler)
# ---------------------------------------------------------------------------

def run_executive_pipeline(days_back: int = 7) -> dict:
    """Run the full executive pipeline: metrics → health → risks → growth."""
    m = recompute_executive_metrics(days_back=days_back)
    h = compute_health_score()
    risks = detect_risks(days_back=days_back)
    growth = detect_growth_opportunities(days_back=days_back)
    return {
        "metrics_rows": m["metrics_rows"],
        "health_computed": True,
        "opportunities_found": len(growth),
        "risks_found": len(risks),
    }
