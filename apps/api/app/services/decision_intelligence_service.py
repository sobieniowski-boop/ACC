"""Decision Intelligence — Feedback Loop service.

Pipeline:
  1. RECORD EXECUTION  — snapshot baseline when opportunity accepted/executed
  2. MONITOR OUTCOMES  — daily job evaluates outcomes at 7/14/30/60 day windows
  3. AGGREGATE LEARNING — weekly job builds per-type accuracy & ROI stats
  4. ADJUST MODELS     — monthly job tweaks confidence/priority weights

All SQL uses '?' placeholders (pymssql_compat converts to %s internally).
"""
from __future__ import annotations

import json
import math
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional

import structlog

from app.core.config import MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

# ── Marketplace codes (from central registry) ──────────
MKT_CODE = {mid: info.get("code", mid) for mid, info in MARKETPLACE_REGISTRY.items()}

# Monitoring windows by opportunity type (days)
MONITORING_WINDOWS: Dict[str, List[int]] = {
    "PRICE_INCREASE":        [14, 30],
    "PRICE_DECREASE":        [14, 30],
    "ADS_SCALE_UP":          [7, 14, 30],
    "ADS_CUT_WASTE":         [7, 14, 30],
    "CONTENT_FIX":           [30, 60],
    "STOCK_REPLENISH":       [14, 30],
    "MARKETPLACE_EXPANSION": [30, 60],
    "FAMILY_REPAIR":         [30, 60],
    "RETURN_REDUCTION":      [14, 30],
    "COST_RENEGOTIATION":    [30, 60],
    "CATEGORY_WINNER_SCALE": [14, 30],
    "BUNDLE_CREATE":         [30, 60],
    "VARIANT_EXPANSION":     [30, 60],
    # Executive-sourced types (unified via growth_opportunity_access — S8.1)
    "EXEC_RISK_PROFIT_DECLINE":  [7, 14],
    "EXEC_RISK_LOW_MARGIN":      [14, 30],
    "EXEC_RISK_HIGH_RETURN":     [14, 30],
    "EXEC_RISK_AD_INEFFICIENCY": [7, 14, 30],
    "EXEC_MARGIN_OPTIMIZATION":  [14, 30],
    "EXEC_MARKETPLACE_EXPANSION": [30, 60],
}
DEFAULT_WINDOWS = [14, 30]


def _f(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    return float(v)


def _success_label(score: float) -> str:
    if score >= 1.2:
        return "overperformed"
    if score >= 0.8:
        return "on_target"
    if score >= 0.4:
        return "partial_success"
    return "failure"


# ═══════════════════════════════════════════════════════════════════
#  1. RECORD EXECUTION  — called when opportunity is accepted/executed
# ═══════════════════════════════════════════════════════════════════

def record_execution(
    *,
    opportunity_id: int,
    entity_type: str | None = None,
    entity_id: str | None = None,
    action_type: str,
    executed_by: str | None = None,
    monitoring_days: int = 14,
) -> dict:
    """Create execution record with baseline snapshot from profitability rollup."""
    conn = connect_acc(autocommit=False)
    cur = conn.cursor()
    try:
        # Fetch opportunity details
        cur.execute("""
            SELECT opportunity_type, marketplace_id, sku, asin,
                   estimated_revenue_uplift, estimated_profit_uplift,
                   estimated_margin_uplift, estimated_units_uplift
            FROM growth_opportunity WHERE id = ?
        """, (opportunity_id,))
        opp = cur.fetchone()
        if not opp:
            raise ValueError(f"Opportunity {opportunity_id} not found")

        opp_type, mkt, sku, asin = opp[0], opp[1], opp[2], opp[3]
        entity_type = entity_type or ("sku" if sku else "asin" if asin else "marketplace")
        entity_id = entity_id or sku or asin or mkt

        # Determine monitoring window from type configuration
        windows = MONITORING_WINDOWS.get(opp_type, DEFAULT_WINDOWS)
        max_window = max(windows)
        monitoring_end_date = (date.today() + timedelta(days=max_window)).isoformat()

        # Build baseline metrics from last 30 days of profitability data
        baseline = _build_baseline(cur, mkt, sku, days=30)

        # Expected metrics from opportunity
        expected = {
            "expected_revenue_delta": _f(opp[4]),
            "expected_profit_delta": _f(opp[5]),
            "expected_margin_delta": _f(opp[6]),
            "expected_units_delta": int(opp[7] or 0),
        }

        cur.execute("""
            INSERT INTO opportunity_execution
                (opportunity_id, entity_type, entity_id, action_type, executed_by,
                 baseline_metrics_json, expected_metrics_json,
                 monitoring_start, monitoring_end, status)
            VALUES (?, ?, ?, ?, ?,
                    ?, ?,
                    SYSUTCDATETIME(), ?, 'monitoring')
        """, (
            opportunity_id, entity_type, entity_id, action_type, executed_by,
            json.dumps(baseline), json.dumps(expected),
            monitoring_end_date,
        ))
        cur.execute("SELECT SCOPE_IDENTITY()")
        exec_id = int(cur.fetchone()[0])

        # Log it in the opportunity timeline
        cur.execute("""
            INSERT INTO growth_opportunity_log
                (opportunity_id, action, actor, note, created_at)
            VALUES (?, 'executed', ?, ?, SYSUTCDATETIME())
        """, (opportunity_id, executed_by, f"Execution #{exec_id} started, monitoring {max_window}d"))

        conn.commit()
        log.info("decision_intel.execution_recorded", exec_id=exec_id, opp_id=opportunity_id)
        return {"execution_id": exec_id, "monitoring_days": max_window, "baseline": baseline, "expected": expected}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def _build_baseline(cur, marketplace_id: str | None, sku: str | None, *, days: int = 30) -> dict:
    """Snapshot baseline metrics from acc_sku_profitability_rollup."""
    if not sku:
        return {}
    from_date = (date.today() - timedelta(days=days)).isoformat()
    to_date = date.today().isoformat()
    if marketplace_id:
        cur.execute("""
            SELECT SUM(revenue_pln), SUM(profit_pln), AVG(margin_pct),
                   SUM(units_sold), SUM(orders_count),
                   AVG(acos_pct), AVG(return_rate_pct)
            FROM acc_sku_profitability_rollup
            WHERE sku = ? AND period_date BETWEEN ? AND ? AND marketplace_id = ?
        """, (sku, from_date, to_date, marketplace_id))
    else:
        cur.execute("""
            SELECT SUM(revenue_pln), SUM(profit_pln), AVG(margin_pct),
                   SUM(units_sold), SUM(orders_count),
                   AVG(acos_pct), AVG(return_rate_pct)
            FROM acc_sku_profitability_rollup
            WHERE sku = ? AND period_date BETWEEN ? AND ?
        """, (sku, from_date, to_date))
    r = cur.fetchone()
    if not r or r[0] is None:
        return {}
    return {
        "revenue_30d": _f(r[0]),
        "profit_30d": _f(r[1]),
        "margin_30d": _f(r[2]),
        "units_30d": int(r[3] or 0),
        "orders_30d": int(r[4] or 0),
        "acos_30d": _f(r[5]),
        "return_rate_30d": _f(r[6]),
    }


# ═══════════════════════════════════════════════════════════════════
#  2. DAILY OUTCOME MONITORING
# ═══════════════════════════════════════════════════════════════════

def run_outcome_monitoring() -> dict:
    """Daily job: evaluate outcomes for executions whose monitoring windows have matured."""
    t0 = time.time()
    conn = connect_acc(autocommit=False, timeout=120)
    cur = conn.cursor()
    evaluated = 0
    expired = 0
    try:
        # Find active executions with at least a 7-day window elapsed
        cur.execute("""
            SELECT e.id, e.opportunity_id, e.baseline_metrics_json, e.expected_metrics_json,
                   e.monitoring_start, e.monitoring_end, e.status,
                   g.opportunity_type, g.marketplace_id, g.sku,
                   g.estimated_profit_uplift
            FROM opportunity_execution e
            JOIN growth_opportunity g ON g.id = e.opportunity_id
            WHERE e.status = 'monitoring'
        """)
        executions = cur.fetchall()

        for ex in executions:
            exec_id = ex[0]
            opp_id = ex[1]
            baseline_raw = ex[2]
            expected_raw = ex[3]
            monitoring_start = ex[4]
            monitoring_end = ex[5]
            opp_type = ex[7]
            mkt = ex[8]
            sku = ex[9]
            expected_profit = _f(ex[10])

            if not monitoring_start:
                continue

            baseline = json.loads(baseline_raw) if baseline_raw else {}
            expected = json.loads(expected_raw) if expected_raw else {}

            # Determine which windows to evaluate
            windows = MONITORING_WINDOWS.get(opp_type, DEFAULT_WINDOWS)
            days_elapsed = (date.today() - monitoring_start.date() if hasattr(monitoring_start, 'date') else
                           (date.today() - date.fromisoformat(str(monitoring_start)[:10]))).days

            for window in windows:
                if days_elapsed < window:
                    continue
                # Check if we already evaluated this window
                cur.execute("""
                    SELECT COUNT(*) FROM opportunity_outcome
                    WHERE execution_id = ? AND monitoring_days = ?
                """, (exec_id, window))
                if cur.fetchone()[0] > 0:
                    continue

                # Build actual metrics for the monitoring period
                actual = _build_actual_metrics(cur, mkt, sku, monitoring_start, window)
                if not actual:
                    continue

                # Calculate deltas vs baseline
                delta = _calc_delta(baseline, actual)

                # Success score = actual profit delta / expected profit delta
                actual_profit_delta = actual.get("revenue_period", 0) * actual.get("margin_period", 0) / 100 - baseline.get("profit_30d", 0) * (window / 30)
                expected_profit_delta = expected.get("expected_profit_delta", 0) * (window / 30) if expected.get("expected_profit_delta", 0) else None

                success_score = None
                if expected_profit_delta and expected_profit_delta > 0:
                    success_score = round(actual_profit_delta / expected_profit_delta, 4)
                elif actual_profit_delta > 0:
                    success_score = 1.5  # positive outcome with no prediction

                # Impact score: normalized 0-100 based on profit delta magnitude
                impact_score = min(100, max(0, round(actual_profit_delta / 500 * 100, 1)))

                # Confidence adjustment
                conf_adj = _calc_confidence_adjustment(success_score)

                cur.execute("""
                    INSERT INTO opportunity_outcome
                        (execution_id, monitoring_days, actual_metrics_json,
                         expected_metrics_json, delta_json,
                         success_score, impact_score, confidence_adjustment)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    exec_id, window,
                    json.dumps(actual), json.dumps(expected), json.dumps(delta),
                    success_score, impact_score, conf_adj,
                ))
                evaluated += 1

            # If past monitoring end, mark as evaluated
            if monitoring_end:
                end_date = monitoring_end.date() if hasattr(monitoring_end, 'date') else date.fromisoformat(str(monitoring_end)[:10])
                if date.today() > end_date:
                    cur.execute("UPDATE opportunity_execution SET status='evaluated' WHERE id=?", (exec_id,))
                    expired += 1

        conn.commit()
        elapsed = round(time.time() - t0, 1)
        log.info("decision_intel.monitoring_done", evaluated=evaluated, expired=expired, elapsed=elapsed)
        return {"evaluated": evaluated, "expired": expired, "elapsed_sec": elapsed}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def _build_actual_metrics(cur, marketplace_id: str | None, sku: str | None,
                          start_date, window_days: int) -> dict | None:
    """Get actual performance during the monitoring window."""
    if not sku:
        return None
    if hasattr(start_date, 'date'):
        sd = start_date.date()
    else:
        sd = date.fromisoformat(str(start_date)[:10])
    end = sd + timedelta(days=window_days)
    if marketplace_id:
        cur.execute("""
            SELECT SUM(revenue_pln), SUM(profit_pln), AVG(margin_pct),
                   SUM(units_sold), SUM(orders_count),
                   AVG(acos_pct), AVG(return_rate_pct)
            FROM acc_sku_profitability_rollup
            WHERE sku = ? AND period_date BETWEEN ? AND ? AND marketplace_id = ?
        """, (sku, sd.isoformat(), end.isoformat(), marketplace_id))
    else:
        cur.execute("""
            SELECT SUM(revenue_pln), SUM(profit_pln), AVG(margin_pct),
                   SUM(units_sold), SUM(orders_count),
                   AVG(acos_pct), AVG(return_rate_pct)
            FROM acc_sku_profitability_rollup
            WHERE sku = ? AND period_date BETWEEN ? AND ?
        """, (sku, sd.isoformat(), end.isoformat()))
    r = cur.fetchone()
    if not r or r[0] is None:
        return None
    return {
        "revenue_period": _f(r[0]),
        "profit_period": _f(r[1]),
        "margin_period": _f(r[2]),
        "units_period": int(r[3] or 0),
        "orders_period": int(r[4] or 0),
        "acos_period": _f(r[5]),
        "return_rate_period": _f(r[6]),
    }


def _calc_delta(baseline: dict, actual: dict) -> dict:
    """Calculate deltas: actual - baseline (pro-rated)."""
    return {
        "revenue_delta": actual.get("revenue_period", 0) - baseline.get("revenue_30d", 0),
        "profit_delta": actual.get("profit_period", 0) - baseline.get("profit_30d", 0),
        "margin_delta": actual.get("margin_period", 0) - baseline.get("margin_30d", 0),
        "units_delta": actual.get("units_period", 0) - baseline.get("units_30d", 0),
    }


def _calc_confidence_adjustment(success_score: float | None) -> float:
    """Determine confidence adjustment based on success score."""
    if success_score is None:
        return 0.0
    if success_score >= 1.2:
        return 0.05   # model underestimates — boost confidence
    if success_score >= 0.8:
        return 0.02   # on target — small positive reinforcement
    if success_score >= 0.4:
        return -0.05  # partial — slight downgrade
    return -0.12      # failure — significant downgrade


# ═══════════════════════════════════════════════════════════════════
#  3. WEEKLY LEARNING AGGREGATION
# ═══════════════════════════════════════════════════════════════════

def run_learning_aggregation() -> dict:
    """Aggregate per-type statistics from outcome evaluations."""
    t0 = time.time()
    conn = connect_acc(autocommit=False, timeout=120)
    cur = conn.cursor()
    types_updated = 0
    try:
        # Get all evaluated outcomes grouped by opportunity type
        cur.execute("""
            SELECT g.opportunity_type,
                   COUNT(DISTINCT oc.id) sample_size,
                   AVG(g.estimated_profit_uplift) avg_expected,
                   AVG(oc.success_score) avg_success,
                   AVG(oc.confidence_adjustment) avg_conf_adj,
                   SUM(CASE WHEN oc.success_score >= 0.8 THEN 1 ELSE 0 END) wins,
                   COUNT(oc.id) total_outcomes
            FROM opportunity_outcome oc
            JOIN opportunity_execution ex ON ex.id = oc.execution_id
            JOIN growth_opportunity g ON g.id = ex.opportunity_id
            WHERE oc.monitoring_days = (
                SELECT MAX(oc2.monitoring_days) FROM opportunity_outcome oc2 WHERE oc2.execution_id = oc.execution_id
            )
            GROUP BY g.opportunity_type
        """)
        rows = cur.fetchall()

        for r in rows:
            opp_type = r[0]
            sample = int(r[1] or 0)
            avg_expected = _f(r[2])
            avg_success = _f(r[3])
            avg_conf_adj = _f(r[4])
            wins = int(r[5] or 0)
            total = int(r[6] or 1)
            win_rate = round(wins / max(total, 1), 4)

            # Calculate prediction accuracy and avg ROI from detailed outcomes
            cur.execute("""
                SELECT AVG(ABS(oc.success_score - 1.0))
                FROM opportunity_outcome oc
                JOIN opportunity_execution ex ON ex.id = oc.execution_id
                JOIN growth_opportunity g ON g.id = ex.opportunity_id
                WHERE g.opportunity_type = ? AND oc.success_score IS NOT NULL
            """, (opp_type,))
            avg_error = _f(cur.fetchone()[0])
            prediction_accuracy = round(max(0, 1.0 - avg_error), 4)

            # Avg actual profit from delta_json
            cur.execute("""
                SELECT oc.delta_json
                FROM opportunity_outcome oc
                JOIN opportunity_execution ex ON ex.id = oc.execution_id
                JOIN growth_opportunity g ON g.id = ex.opportunity_id
                WHERE g.opportunity_type = ? AND oc.delta_json IS NOT NULL
            """, (opp_type,))
            profit_deltas = []
            for dr in cur.fetchall():
                try:
                    d = json.loads(dr[0])
                    profit_deltas.append(d.get("profit_delta", 0))
                except (json.JSONDecodeError, TypeError, AttributeError):
                    pass
            avg_actual_profit = round(sum(profit_deltas) / max(len(profit_deltas), 1), 2)
            avg_roi = round(avg_actual_profit / max(avg_expected, 1), 4) if avg_expected > 0 else None

            # Upsert decision_learning
            cur.execute("SELECT id FROM decision_learning WHERE opportunity_type = ?", (opp_type,))
            existing = cur.fetchone()
            if existing:
                cur.execute("""
                    UPDATE decision_learning
                    SET sample_size=?, avg_expected_profit=?, avg_actual_profit=?,
                        prediction_accuracy=?, avg_success_score=?,
                        confidence_adjustment=?, win_rate=?, avg_roi=?,
                        last_updated=SYSUTCDATETIME()
                    WHERE opportunity_type=?
                """, (sample, avg_expected, avg_actual_profit,
                      prediction_accuracy, avg_success, avg_conf_adj,
                      win_rate, avg_roi, opp_type))
            else:
                cur.execute("""
                    INSERT INTO decision_learning
                        (opportunity_type, sample_size, avg_expected_profit, avg_actual_profit,
                         prediction_accuracy, avg_success_score, confidence_adjustment,
                         win_rate, avg_roi)
                    VALUES (?,?,?,?,?,?,?,?,?)
                """, (opp_type, sample, avg_expected, avg_actual_profit,
                      prediction_accuracy, avg_success, avg_conf_adj,
                      win_rate, avg_roi))
            types_updated += 1

        conn.commit()
        elapsed = round(time.time() - t0, 1)
        log.info("decision_intel.learning_done", types_updated=types_updated, elapsed=elapsed)
        return {"types_updated": types_updated, "elapsed_sec": elapsed}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


# ═══════════════════════════════════════════════════════════════════
#  4. MONTHLY MODEL RECALIBRATION
# ═══════════════════════════════════════════════════════════════════

def run_model_recalibration() -> dict:
    """Adjust confidence/priority weights based on learning data."""
    t0 = time.time()
    conn = connect_acc(autocommit=False, timeout=60)
    cur = conn.cursor()
    adjusted = 0
    try:
        cur.execute("""
            SELECT opportunity_type, prediction_accuracy, avg_success_score,
                   confidence_adjustment, win_rate, avg_roi, sample_size
            FROM decision_learning
            WHERE sample_size >= 5
        """)
        rows = cur.fetchall()

        for r in rows:
            opp_type = r[0]
            pred_acc = _f(r[1])
            avg_success = _f(r[2])
            conf_adj = _f(r[3])
            win_rate = _f(r[4])
            avg_roi = _f(r[5])
            sample = int(r[6] or 0)

            # Determine adjustments
            impact_adj = 0.0
            conf_weight_adj = 0.0
            prio_adj = 0.0
            reasons = []

            # If this type consistently overperforms → boost priority
            if avg_success >= 1.1 and win_rate >= 0.7:
                prio_adj = 0.08
                reasons.append(f"High performer: success={avg_success:.2f}, win_rate={win_rate:.0%}")
            elif avg_success < 0.5 and win_rate < 0.3:
                prio_adj = -0.10
                reasons.append(f"Underperformer: success={avg_success:.2f}, win_rate={win_rate:.0%}")

            # Confidence weight adjustment from prediction accuracy
            if pred_acc > 0.85:
                conf_weight_adj = 0.05
                reasons.append(f"Accurate predictions: {pred_acc:.0%}")
            elif pred_acc < 0.5:
                conf_weight_adj = -0.08
                reasons.append(f"Inaccurate predictions: {pred_acc:.0%}")

            # Impact weight from ROI
            if avg_roi and avg_roi > 1.2:
                impact_adj = 0.05
                reasons.append(f"Strong ROI: {avg_roi:.2f}x")
            elif avg_roi and avg_roi < 0.5:
                impact_adj = -0.05
                reasons.append(f"Weak ROI: {avg_roi:.2f}x")

            if not reasons:
                continue

            reason_str = "; ".join(reasons)

            # Upsert model adjustment
            cur.execute("SELECT id FROM opportunity_model_adjustments WHERE opportunity_type=?", (opp_type,))
            existing = cur.fetchone()
            if existing:
                cur.execute("""
                    UPDATE opportunity_model_adjustments
                    SET impact_weight_adjustment=?, confidence_weight_adjustment=?,
                        priority_weight_adjustment=?, reason=?, updated_at=SYSUTCDATETIME()
                    WHERE opportunity_type=?
                """, (impact_adj, conf_weight_adj, prio_adj, reason_str, opp_type))
            else:
                cur.execute("""
                    INSERT INTO opportunity_model_adjustments
                        (opportunity_type, impact_weight_adjustment, confidence_weight_adjustment,
                         priority_weight_adjustment, reason)
                    VALUES (?,?,?,?,?)
                """, (opp_type, impact_adj, conf_weight_adj, prio_adj, reason_str))
            adjusted += 1

        conn.commit()
        elapsed = round(time.time() - t0, 1)
        log.info("decision_intel.recalibration_done", adjusted=adjusted, elapsed=elapsed)
        return {"types_adjusted": adjusted, "elapsed_sec": elapsed}
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


# ═══════════════════════════════════════════════════════════════════
#  QUERY HELPERS  (consumed by the API layer)
# ═══════════════════════════════════════════════════════════════════

def get_outcomes_page(
    *,
    page: int = 1,
    page_size: int = 50,
    opportunity_type: str | None = None,
    marketplace_id: str | None = None,
    min_success: float | None = None,
    max_success: float | None = None,
    status: str | None = None,
) -> dict:
    """Paginated outcome list with joined opportunity info."""
    conn = connect_acc()
    cur = conn.cursor()
    try:
        clauses = ["1=1"]
        params: list = []
        if opportunity_type:
            clauses.append("g.opportunity_type = ?")
            params.append(opportunity_type)
        if marketplace_id:
            clauses.append("g.marketplace_id = ?")
            params.append(marketplace_id)
        if status:
            clauses.append("e.status = ?")
            params.append(status)
        if min_success is not None:
            clauses.append("oc.success_score >= ?")
            params.append(min_success)
        if max_success is not None:
            clauses.append("oc.success_score <= ?")
            params.append(max_success)
        where = " AND ".join(clauses)

        cur.execute(f"""
            SELECT COUNT(DISTINCT e.id)
            FROM opportunity_execution e
            JOIN growth_opportunity g ON g.id = e.opportunity_id
            LEFT JOIN opportunity_outcome oc ON oc.execution_id = e.id
            WHERE {where}
        """, params)
        total = int(cur.fetchone()[0] or 0)
        pages = math.ceil(total / page_size) if total > 0 else 0
        offset = (page - 1) * page_size

        cur.execute(f"""
            SELECT e.id, e.opportunity_id, e.entity_type, e.entity_id,
                   e.action_type, e.executed_by, e.executed_at,
                   e.monitoring_start, e.monitoring_end, e.status,
                   g.opportunity_type, g.marketplace_id, g.sku, g.title,
                   g.estimated_profit_uplift,
                   oc_latest.success_score, oc_latest.impact_score,
                   oc_latest.confidence_adjustment, oc_latest.monitoring_days,
                   oc_latest.evaluated_at,
                   oc_latest.actual_metrics_json, oc_latest.delta_json,
                   p.title
            FROM opportunity_execution e
            JOIN growth_opportunity g ON g.id = e.opportunity_id
            LEFT JOIN acc_product p ON p.sku = g.sku
            OUTER APPLY (
                SELECT TOP 1 * FROM opportunity_outcome
                WHERE execution_id = e.id
                ORDER BY monitoring_days DESC
            ) oc_latest
            WHERE {where}
            ORDER BY e.executed_at DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, params + [offset, page_size])
        items = []
        for r in cur.fetchall():
            actual_metrics = None
            delta = None
            if r[20]:
                try:
                    actual_metrics = json.loads(r[20])
                except (json.JSONDecodeError, TypeError):
                    pass
            if r[21]:
                try:
                    delta = json.loads(r[21])
                except (json.JSONDecodeError, TypeError):
                    pass

            items.append({
                "execution_id": r[0],
                "opportunity_id": r[1],
                "entity_type": r[2],
                "entity_id": r[3],
                "action_type": r[4],
                "executed_by": r[5],
                "executed_at": r[6].isoformat() if r[6] else None,
                "monitoring_start": r[7].isoformat() if r[7] else None,
                "monitoring_end": r[8].isoformat() if r[8] else None,
                "status": r[9],
                "opportunity_type": r[10],
                "marketplace_id": r[11],
                "marketplace_code": MKT_CODE.get(r[11], r[11]) if r[11] else None,
                "sku": r[12],
                "title": r[13],
                "product_title": r[22],
                "expected_profit": _f(r[14]),
                "success_score": _f(r[15]) if r[15] is not None else None,
                "success_label": _success_label(_f(r[15])) if r[15] is not None else None,
                "impact_score": _f(r[16]) if r[16] is not None else None,
                "confidence_adjustment": _f(r[17]) if r[17] is not None else None,
                "monitoring_days": int(r[18]) if r[18] else None,
                "evaluated_at": r[19].isoformat() if r[19] else None,
                "actual_metrics": actual_metrics,
                "delta": delta,
            })
        return {"items": items, "total": total, "pages": pages}
    finally:
        cur.close()
        conn.close()


def get_execution_detail(execution_id: int) -> dict | None:
    """Full execution detail with all outcome windows."""
    conn = connect_acc()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT e.id, e.opportunity_id, e.entity_type, e.entity_id,
                   e.action_type, e.executed_by, e.executed_at,
                   e.baseline_metrics_json, e.expected_metrics_json,
                   e.monitoring_start, e.monitoring_end, e.status,
                   g.opportunity_type, g.marketplace_id, g.sku, g.title,
                   p.title
            FROM opportunity_execution e
            JOIN growth_opportunity g ON g.id = e.opportunity_id
            LEFT JOIN acc_product p ON p.sku = g.sku
            WHERE e.id = ?
        """, (execution_id,))
        r = cur.fetchone()
        if not r:
            return None

        baseline = json.loads(r[7]) if r[7] else {}
        expected = json.loads(r[8]) if r[8] else {}

        execution = {
            "id": r[0], "opportunity_id": r[1], "entity_type": r[2], "entity_id": r[3],
            "action_type": r[4], "executed_by": r[5],
            "executed_at": r[6].isoformat() if r[6] else None,
            "baseline_metrics": baseline, "expected_metrics": expected,
            "monitoring_start": r[9].isoformat() if r[9] else None,
            "monitoring_end": r[10].isoformat() if r[10] else None,
            "status": r[11],
            "opportunity_type": r[12], "marketplace_id": r[13], "sku": r[14], "title": r[15],
            "product_title": r[16],
        }

        # All outcome windows
        cur.execute("""
            SELECT id, monitoring_days, actual_metrics_json, expected_metrics_json,
                   delta_json, success_score, impact_score, confidence_adjustment, evaluated_at
            FROM opportunity_outcome
            WHERE execution_id = ?
            ORDER BY monitoring_days
        """, (execution_id,))
        outcomes = []
        for o in cur.fetchall():
            outcomes.append({
                "id": o[0],
                "monitoring_days": o[1],
                "actual_metrics": json.loads(o[2]) if o[2] else None,
                "expected_metrics": json.loads(o[3]) if o[3] else None,
                "delta": json.loads(o[4]) if o[4] else None,
                "success_score": _f(o[5]) if o[5] is not None else None,
                "success_label": _success_label(_f(o[5])) if o[5] is not None else None,
                "impact_score": _f(o[6]) if o[6] is not None else None,
                "confidence_adjustment": _f(o[7]) if o[7] is not None else None,
                "evaluated_at": o[8].isoformat() if o[8] else None,
            })

        return {"execution": execution, "outcomes": outcomes}
    finally:
        cur.close()
        conn.close()


def get_learning_dashboard() -> dict:
    """Learning data + model adjustments for /strategy/learning."""
    conn = connect_acc()
    cur = conn.cursor()
    try:
        # Learning entries
        cur.execute("""
            SELECT opportunity_type, sample_size, avg_expected_profit, avg_actual_profit,
                   prediction_accuracy, avg_success_score, confidence_adjustment,
                   win_rate, avg_roi, last_updated
            FROM decision_learning
            ORDER BY sample_size DESC
        """)
        learning = []
        for r in cur.fetchall():
            learning.append({
                "opportunity_type": r[0],
                "sample_size": int(r[1] or 0),
                "avg_expected_profit": _f(r[2]),
                "avg_actual_profit": _f(r[3]),
                "prediction_accuracy": _f(r[4]),
                "avg_success_score": _f(r[5]),
                "confidence_adjustment": _f(r[6]),
                "win_rate": _f(r[7]),
                "avg_roi": _f(r[8]),
                "last_updated": r[9].isoformat() if r[9] else None,
            })

        # Model adjustments
        cur.execute("""
            SELECT opportunity_type, impact_weight_adjustment, confidence_weight_adjustment,
                   priority_weight_adjustment, reason, updated_at
            FROM opportunity_model_adjustments
            ORDER BY updated_at DESC
        """)
        adjustments = []
        for r in cur.fetchall():
            adjustments.append({
                "opportunity_type": r[0],
                "impact_weight_adjustment": _f(r[1]),
                "confidence_weight_adjustment": _f(r[2]),
                "priority_weight_adjustment": _f(r[3]),
                "reason": r[4],
                "updated_at": r[5].isoformat() if r[5] else None,
            })

        # Summary stats
        cur.execute("""
            SELECT COUNT(*), AVG(prediction_accuracy), AVG(win_rate),
                   SUM(sample_size), AVG(avg_roi)
            FROM decision_learning
        """)
        s = cur.fetchone()
        summary = {
            "types_tracked": int(s[0] or 0),
            "avg_prediction_accuracy": _f(s[1]),
            "avg_win_rate": _f(s[2]),
            "total_evaluations": int(s[3] or 0),
            "avg_roi": _f(s[4]),
        }

        return {"learning": learning, "adjustments": adjustments, "summary": summary}
    finally:
        cur.close()
        conn.close()


def get_opportunity_outcomes(opportunity_id: int) -> list:
    """Get all executions + outcomes for a specific opportunity (for detail drawer)."""
    conn = connect_acc()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT e.id, e.action_type, e.executed_by, e.executed_at,
                   e.baseline_metrics_json, e.expected_metrics_json,
                   e.monitoring_start, e.monitoring_end, e.status
            FROM opportunity_execution e
            WHERE e.opportunity_id = ?
            ORDER BY e.executed_at DESC
        """, (opportunity_id,))
        executions = []
        for r in cur.fetchall():
            exec_id = r[0]
            # Get outcomes for this execution
            cur.execute("""
                SELECT monitoring_days, success_score, impact_score,
                       confidence_adjustment, actual_metrics_json, delta_json, evaluated_at
                FROM opportunity_outcome
                WHERE execution_id = ?
                ORDER BY monitoring_days
            """, (exec_id,))
            outcomes = []
            for o in cur.fetchall():
                outcomes.append({
                    "monitoring_days": o[0],
                    "success_score": _f(o[1]) if o[1] is not None else None,
                    "success_label": _success_label(_f(o[1])) if o[1] is not None else None,
                    "impact_score": _f(o[2]) if o[2] is not None else None,
                    "confidence_adjustment": _f(o[3]) if o[3] is not None else None,
                    "actual_metrics": json.loads(o[4]) if o[4] else None,
                    "delta": json.loads(o[5]) if o[5] else None,
                    "evaluated_at": o[6].isoformat() if o[6] else None,
                })

            executions.append({
                "execution_id": r[0],
                "action_type": r[1],
                "executed_by": r[2],
                "executed_at": r[3].isoformat() if r[3] else None,
                "baseline_metrics": json.loads(r[4]) if r[4] else None,
                "expected_metrics": json.loads(r[5]) if r[5] else None,
                "monitoring_start": r[6].isoformat() if r[6] else None,
                "monitoring_end": r[7].isoformat() if r[7] else None,
                "status": r[8],
                "outcomes": outcomes,
            })
        return executions
    finally:
        cur.close()
        conn.close()


def get_weekly_report() -> dict:
    """Generate weekly decision performance report."""
    conn = connect_acc()
    cur = conn.cursor()
    try:
        period_end = date.today()
        period_start = period_end - timedelta(days=7)

        # Outcomes evaluated in last 7 days
        cur.execute("""
            SELECT g.opportunity_type, g.sku, g.marketplace_id, g.title,
                   oc.success_score, oc.impact_score, oc.delta_json,
                   g.estimated_profit_uplift, p.title
            FROM opportunity_outcome oc
            JOIN opportunity_execution ex ON ex.id = oc.execution_id
            JOIN growth_opportunity g ON g.id = ex.opportunity_id
            LEFT JOIN acc_product p ON p.sku = g.sku
            WHERE oc.evaluated_at >= ? AND oc.success_score IS NOT NULL
            ORDER BY oc.success_score DESC
        """, (period_start.isoformat(),))
        rows = cur.fetchall()

        all_items = []
        for r in rows:
            delta = json.loads(r[6]) if r[6] else {}
            all_items.append({
                "opportunity_type": r[0], "sku": r[1],
                "marketplace_code": MKT_CODE.get(r[2], r[2]) if r[2] else None,
                "title": r[3],
                "product_title": r[8],
                "success_score": _f(r[4]),
                "impact_score": _f(r[5]),
                "profit_delta": delta.get("profit_delta", 0),
                "expected_profit": _f(r[7]),
            })

        top = sorted(all_items, key=lambda x: x["success_score"], reverse=True)[:5]
        worst = sorted(all_items, key=lambda x: x["success_score"])[:5]

        total_evaluated = len(all_items)
        total_success = sum(1 for i in all_items if i["success_score"] >= 0.8)
        avg_accuracy = sum(max(0, 1 - abs(i["success_score"] - 1)) for i in all_items) / max(len(all_items), 1)

        # Generate insights
        insights = []
        if top:
            best_type = top[0]["opportunity_type"]
            insights.append(f"Best performing strategy: {best_type.replace('_', ' ')} (success {top[0]['success_score']:.0%})")
        if worst and worst[0]["success_score"] < 0.4:
            insights.append(f"⚠️ Underperforming: {worst[0]['opportunity_type'].replace('_', ' ')} — consider reviewing thresholds")
        if total_evaluated > 0:
            insights.append(f"Win rate this week: {total_success}/{total_evaluated} ({total_success/total_evaluated:.0%})")
        if not all_items:
            insights.append("No outcomes evaluated this week — ensure executions are being recorded")

        return {
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "top_performing": top,
            "worst_performing": worst,
            "prediction_accuracy": round(avg_accuracy, 4),
            "total_evaluated": total_evaluated,
            "total_success": total_success,
            "insights": insights,
        }
    finally:
        cur.close()
        conn.close()
