"""Runtime guardrails — silent-failure detection for ACC production pipelines.

Runs as a scheduled job and exposes results via API.  Each check returns a
:class:`GuardrailResult` with severity, detail, and the raw SQL/metric used
so operators can reproduce the check manually.

Design principles:
    - Read-only: never mutates business data.
    - Fail-open: a guardrail that cannot connect to SQL returns UNKNOWN, never
      blocks the pipeline it monitors.
    - Idempotent: safe to run at any frequency.
    - Observable: every result is logged with structlog and persisted in
      ``acc_guardrail_results`` for dashboard / trend analysis.
"""
from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any

import structlog

from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)


# ── Result model ────────────────────────────────────────────────────────────

class Severity(str, enum.Enum):
    OK = "ok"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass(slots=True)
class GuardrailResult:
    check_name: str
    severity: Severity
    message: str
    value: Any = None
    threshold: Any = None
    query_used: str = ""
    elapsed_ms: float = 0.0
    checked_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# ── Helpers ─────────────────────────────────────────────────────────────────

def _run_scalar(sql: str, *, timeout: int = 15) -> Any:
    """Execute *sql* and return the first column of the first row."""
    conn = connect_acc(timeout=timeout)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _run_rows(sql: str, *, timeout: int = 15) -> list[tuple]:
    conn = connect_acc(timeout=timeout)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def _timed(fn, *args, **kwargs) -> tuple[Any, float]:
    t0 = time.perf_counter()
    result = fn(*args, **kwargs)
    return result, round((time.perf_counter() - t0) * 1000, 1)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1 — Pipeline Freshness Checks
# ═══════════════════════════════════════════════════════════════════════════

_ORDER_SYNC_FRESHNESS_SQL = """\
SELECT DATEDIFF(MINUTE, MAX(updated_at), SYSUTCDATETIME())
FROM acc_order_sync_state WITH (NOLOCK)
"""

_FINANCE_FRESHNESS_SQL = """\
SELECT DATEDIFF(HOUR, MAX(posted_date), SYSUTCDATETIME())
FROM acc_finance_transaction WITH (NOLOCK)
WHERE posted_date >= DATEADD(DAY, -7, SYSUTCDATETIME())
"""

_INVENTORY_FRESHNESS_SQL = """\
SELECT DATEDIFF(HOUR,
       MAX(CAST(snapshot_date AS DATETIME)),
       SYSUTCDATETIME())
FROM acc_inventory_snapshot WITH (NOLOCK)
"""

_PROFITABILITY_FRESHNESS_SQL = """\
SELECT DATEDIFF(HOUR, MAX(computed_at), SYSUTCDATETIME())
FROM acc_sku_profitability_rollup WITH (NOLOCK)
"""

_FX_RATE_FRESHNESS_SQL = """\
SELECT DATEDIFF(HOUR,
       MAX(CAST(rate_date AS DATETIME)),
       SYSUTCDATETIME())
FROM acc_exchange_rate WITH (NOLOCK)
"""

_ADS_FRESHNESS_SQL = """\
SELECT DATEDIFF(HOUR, MAX(synced_at), SYSUTCDATETIME())
FROM acc_ads_campaign_day WITH (NOLOCK)
"""

_CONTENT_QUEUE_DEPTH_SQL = """\
SELECT COUNT(*)
FROM acc_co_publish_jobs WITH (NOLOCK)
WHERE status IN ('queued', 'running')
"""


def check_order_sync_freshness() -> GuardrailResult:
    """Order sync should complete every 30 min; alert if > 60 min stale."""
    sql = _ORDER_SYNC_FRESHNESS_SQL
    try:
        minutes, ms = _timed(_run_scalar, sql)
        if minutes is None:
            return GuardrailResult("order_sync_freshness", Severity.CRITICAL,
                                   "No order sync records found", query_used=sql, elapsed_ms=ms)
        sev = Severity.OK if minutes <= 60 else (Severity.WARNING if minutes <= 120 else Severity.CRITICAL)
        return GuardrailResult("order_sync_freshness", sev,
                               f"Last order sync {minutes} min ago",
                               value=minutes, threshold=60, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("order_sync_freshness", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_finance_freshness() -> GuardrailResult:
    """Finance transactions should arrive daily; alert if > 36 h stale."""
    sql = _FINANCE_FRESHNESS_SQL
    try:
        hours, ms = _timed(_run_scalar, sql)
        if hours is None:
            return GuardrailResult("finance_freshness", Severity.CRITICAL,
                                   "No finance transactions in last 7 days", query_used=sql, elapsed_ms=ms)
        sev = Severity.OK if hours <= 36 else (Severity.WARNING if hours <= 48 else Severity.CRITICAL)
        return GuardrailResult("finance_freshness", sev,
                               f"Latest finance transaction {hours}h ago",
                               value=hours, threshold=36, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("finance_freshness", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_inventory_freshness() -> GuardrailResult:
    """Inventory snapshots should be daily; alert if > 36 h stale."""
    sql = _INVENTORY_FRESHNESS_SQL
    try:
        hours, ms = _timed(_run_scalar, sql)
        if hours is None:
            return GuardrailResult("inventory_freshness", Severity.CRITICAL,
                                   "No inventory snapshots found", query_used=sql, elapsed_ms=ms)
        sev = Severity.OK if hours <= 36 else (Severity.WARNING if hours <= 48 else Severity.CRITICAL)
        return GuardrailResult("inventory_freshness", sev,
                               f"Latest inventory snapshot {hours}h ago",
                               value=hours, threshold=36, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("inventory_freshness", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_profitability_freshness() -> GuardrailResult:
    """Profitability rollup should run nightly; alert if > 36 h stale."""
    sql = _PROFITABILITY_FRESHNESS_SQL
    try:
        hours, ms = _timed(_run_scalar, sql)
        if hours is None:
            return GuardrailResult("profitability_freshness", Severity.CRITICAL,
                                   "No profitability rollup records found", query_used=sql, elapsed_ms=ms)
        sev = Severity.OK if hours <= 36 else (Severity.WARNING if hours <= 48 else Severity.CRITICAL)
        return GuardrailResult("profitability_freshness", sev,
                               f"Latest profitability rollup {hours}h ago",
                               value=hours, threshold=36, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("profitability_freshness", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_fx_rate_freshness() -> GuardrailResult:
    """FX rates should sync daily; warn > 36 h, critical > 7 days."""
    sql = _FX_RATE_FRESHNESS_SQL
    try:
        hours, ms = _timed(_run_scalar, sql)
        if hours is None:
            return GuardrailResult("fx_rate_freshness", Severity.CRITICAL,
                                   "No exchange rates found", query_used=sql, elapsed_ms=ms)
        sev = Severity.OK if hours <= 36 else (Severity.WARNING if hours <= 168 else Severity.CRITICAL)
        return GuardrailResult("fx_rate_freshness", sev,
                               f"Latest FX rate {hours}h ago",
                               value=hours, threshold=36, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("fx_rate_freshness", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_ads_freshness() -> GuardrailResult:
    """Ads data should sync daily; alert if > 48 h stale (24-48 h lag normal)."""
    sql = _ADS_FRESHNESS_SQL
    try:
        hours, ms = _timed(_run_scalar, sql)
        if hours is None:
            return GuardrailResult("ads_freshness", Severity.WARNING,
                                   "No ads data found", query_used=sql, elapsed_ms=ms)
        sev = Severity.OK if hours <= 48 else (Severity.WARNING if hours <= 72 else Severity.CRITICAL)
        return GuardrailResult("ads_freshness", sev,
                               f"Latest ads data {hours}h ago",
                               value=hours, threshold=48, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("ads_freshness", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_content_queue_depth() -> GuardrailResult:
    """Content publish queue should stay below 100; warn > 50, critical > 100."""
    sql = _CONTENT_QUEUE_DEPTH_SQL
    try:
        depth, ms = _timed(_run_scalar, sql)
        depth = int(depth or 0)
        sev = Severity.OK if depth <= 50 else (Severity.WARNING if depth <= 100 else Severity.CRITICAL)
        return GuardrailResult("content_queue_depth", sev,
                               f"Content publish queue depth: {depth}",
                               value=depth, threshold=50, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("content_queue_depth", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2 — Silent Financial Corruption Checks
# ═══════════════════════════════════════════════════════════════════════════

_UNKNOWN_FEE_TYPES_SQL = """\
SELECT charge_type, COUNT(*) AS cnt
FROM acc_finance_transaction WITH (NOLOCK)
WHERE charge_type NOT IN (
        'AmazonForAllFee','BuyerRecharge','Commission',
        'COMPENSATED_CLAWBACK','CustomerReturnHRRUnitFee',
        'DigitalServicesFee','DigitalServicesFeeFBA',
        'FBADisposalFee','FBAPerUnitFulfillmentFee',
        'FBAPerOrderFulfillmentFee','FBARemovalFee','FBAStorageFee',
        'FBAWeightBasedFee','FixedClosingFee',
        'GiftWrap','GiftwrapChargeback','GiftWrapTax',
        'Goodwill','GoodwillEvent','INCORRECT_FEES_ITEMS',
        'MiscAdjustment','Principal','Promotion','PromotionShipping',
        'RefundCommission','REMOVAL_ORDER_LOST',
        'ReserveCredit','ReserveDebit',
        'ReturnPostageBilling_Postage','ReturnPostageBilling_VAT',
        'ReturnShipping','REVERSAL_REIMBURSEMENT',
        'ShippingCharge','ShippingChargeback','ShippingHB',
        'ShippingPrice','ShippingTax','Subscription',
        'Tax','VariableClosingFee','VineFee',
        'WAREHOUSE_DAMAGE','WAREHOUSE_LOST','WAREHOUSE_LOST_MANUAL',
        'ItemWithheldTax',
        'MarketplaceFacilitatorTax-Principal',
        'MarketplaceFacilitatorTax-Shipping',
        'MarketplaceFacilitatorTax-Giftwrap',
        'MarketplaceFacilitatorVAT-Principal',
        'MarketplaceFacilitatorVAT-Shipping',
        'FBACustomerReturnPerUnitFee',
        'LowValueGoodsTax-Principal','LowValueGoodsTax-Shipping'
      )
  AND posted_date >= DATEADD(DAY, -7, SYSUTCDATETIME())
GROUP BY charge_type
"""

_FEE_CLASSIFICATION_COVERAGE_SQL = """\
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN charge_type IS NOT NULL
              AND charge_type != '' THEN 1 ELSE 0 END) AS classified
FROM acc_finance_transaction WITH (NOLOCK)
WHERE posted_date >= DATEADD(DAY, -30, SYSUTCDATETIME())
"""

_PROFIT_MARGIN_ANOMALY_SQL = """\
SELECT TOP 20
    sku, marketplace_id,
    revenue_pln,
    (ISNULL(cogs_pln,0) + ISNULL(amazon_fees_pln,0) + ISNULL(fba_fees_pln,0)
     + ISNULL(logistics_pln,0) + ISNULL(ad_spend_pln,0) + ISNULL(refund_pln,0)
     + ISNULL(storage_fee_pln,0) + ISNULL(other_fees_pln,0)) AS cost_total,
    margin_pct
FROM acc_sku_profitability_rollup WITH (NOLOCK)
WHERE period_date >= DATEADD(DAY, -30, SYSUTCDATETIME())
  AND ABS(revenue_pln) > 50
  AND (margin_pct > 80 OR margin_pct < -50)
ORDER BY ABS(revenue_pln) DESC
"""

_MISSING_FX_RATE_SQL = """\
SELECT DISTINCT o.currency
FROM acc_order o WITH (NOLOCK)
OUTER APPLY (
    SELECT TOP 1 fx.rate_to_pln
    FROM acc_exchange_rate fx WITH (NOLOCK)
    WHERE fx.currency = o.currency
      AND fx.rate_date <= CAST(o.purchase_date AS DATE)
    ORDER BY fx.rate_date DESC
) fx
WHERE o.purchase_date >= DATEADD(DAY, -7, SYSUTCDATETIME())
  AND o.currency != 'PLN'
  AND fx.rate_to_pln IS NULL
"""

_DUPLICATE_FINANCE_TRANSACTIONS_SQL = """\
SELECT
    CONCAT(ISNULL(amazon_order_id,''), '|', charge_type, '|',
           CAST(amount AS VARCHAR), '|',
           CONVERT(VARCHAR(23), posted_date, 126)) AS txn_key,
    COUNT(*) AS cnt,
    DATEDIFF(MINUTE, MIN(synced_at), MAX(synced_at)) AS sync_span_min
FROM acc_finance_transaction WITH (NOLOCK)
WHERE posted_date >= DATEADD(DAY, -7, SYSUTCDATETIME())
GROUP BY amazon_order_id, charge_type, amount,
         CONVERT(VARCHAR(23), posted_date, 126)
HAVING COUNT(*) > 1
   AND DATEDIFF(MINUTE, MIN(synced_at), MAX(synced_at)) > 5
"""

_ORDER_FINANCE_DRIFT_SQL = """\
SELECT
    COUNT(DISTINCT o.amazon_order_id) AS orders_without_finance,
    (SELECT COUNT(DISTINCT amazon_order_id) FROM acc_order WITH (NOLOCK)
     WHERE purchase_date >= DATEADD(DAY, -14, SYSUTCDATETIME())
       AND purchase_date <  DATEADD(DAY, -3, SYSUTCDATETIME())
       AND status NOT IN ('Canceled','Cancelled','Pending')) AS total_settled_orders
FROM acc_order o WITH (NOLOCK)
LEFT JOIN acc_finance_transaction ft WITH (NOLOCK)
    ON ft.amazon_order_id = o.amazon_order_id
WHERE o.purchase_date >= DATEADD(DAY, -14, SYSUTCDATETIME())
  AND o.purchase_date <  DATEADD(DAY, -3, SYSUTCDATETIME())
  AND o.status NOT IN ('Canceled','Cancelled','Pending')
  AND ft.id IS NULL
"""


def check_unknown_fee_types() -> GuardrailResult:
    """Detect Amazon charge_types not covered by fee taxonomy."""
    sql = _UNKNOWN_FEE_TYPES_SQL
    try:
        rows, ms = _timed(_run_rows, sql)
        total_unknown = sum(int(r[1]) for r in rows)
        if total_unknown == 0:
            return GuardrailResult("unknown_fee_types", Severity.OK,
                                   "All fee types classified", value=0, threshold=0,
                                   query_used=sql, elapsed_ms=ms)
        types_str = ", ".join(f"{r[0]}({r[1]})" for r in rows[:10])
        sev = Severity.WARNING if total_unknown < 50 else Severity.CRITICAL
        return GuardrailResult("unknown_fee_types", sev,
                               f"{total_unknown} unknown fees in 7d: {types_str}",
                               value=total_unknown, threshold=0, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("unknown_fee_types", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_fee_classification_coverage() -> GuardrailResult:
    """Fee classification should cover >= 90% of transactions."""
    sql = _FEE_CLASSIFICATION_COVERAGE_SQL
    try:
        row, ms = _timed(lambda: _run_rows(sql)[0] if _run_rows(sql) else (0, 0))
        # Re-run cleanly
        rows, ms = _timed(_run_rows, sql)
        if not rows:
            return GuardrailResult("fee_coverage", Severity.UNKNOWN,
                                   "No finance transactions in 30d", query_used=sql, elapsed_ms=ms)
        total, classified = int(rows[0][0]), int(rows[0][1])
        pct = round(classified / total * 100, 1) if total > 0 else 0.0
        sev = Severity.OK if pct >= 90.0 else (Severity.WARNING if pct >= 80.0 else Severity.CRITICAL)
        return GuardrailResult("fee_coverage", sev,
                               f"Fee classification: {pct}% ({classified}/{total})",
                               value=pct, threshold=90.0, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("fee_coverage", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_profit_margin_anomalies() -> GuardrailResult:
    """Flag SKUs with margin > 80% or < -50% (30-day window, revenue > 50 PLN)."""
    sql = _PROFIT_MARGIN_ANOMALY_SQL
    try:
        rows, ms = _timed(_run_rows, sql)
        count = len(rows)
        if count == 0:
            return GuardrailResult("profit_margin_anomalies", Severity.OK,
                                   "No extreme margin anomalies", value=0, threshold=25,
                                   query_used=sql, elapsed_ms=ms)
        sev = Severity.OK if count < 5 else (Severity.WARNING if count <= 25 else Severity.CRITICAL)
        sample = "; ".join(f"{r[0]}@{r[1]}={r[4]}%" for r in rows[:5])
        return GuardrailResult("profit_margin_anomalies", sev,
                               f"{count} SKUs with extreme margins: {sample}",
                               value=count, threshold=25, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("profit_margin_anomalies", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_missing_fx_rates() -> GuardrailResult:
    """Detect orders whose currency has no matching FX rate on the order date."""
    sql = _MISSING_FX_RATE_SQL
    try:
        rows, ms = _timed(_run_rows, sql)
        currencies = [str(r[0]) for r in rows]
        if not currencies:
            return GuardrailResult("missing_fx_rates", Severity.OK,
                                   "All order currencies have FX rates",
                                   value=0, threshold=0, query_used=sql, elapsed_ms=ms)
        sev = Severity.WARNING if len(currencies) <= 2 else Severity.CRITICAL
        return GuardrailResult("missing_fx_rates", sev,
                               f"Missing FX rates for: {', '.join(currencies)}",
                               value=len(currencies), threshold=0, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("missing_fx_rates", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_duplicate_finance_transactions() -> GuardrailResult:
    """Detect finance rows that were imported by multiple sync runs.

    Amazon legitimately returns per-unit fee lines (e.g. 25× FBARemovalFee
    for a 25-unit removal).  These are NOT duplicates.  To distinguish real
    sync bugs we check the *synced_at* span: rows from the same batch land
    within seconds; cross-batch duplicates have a gap > 5 minutes.
    """
    sql = _DUPLICATE_FINANCE_TRANSACTIONS_SQL
    try:
        rows, ms = _timed(_run_rows, sql)
        count = len(rows)
        if count == 0:
            return GuardrailResult("duplicate_finance_txn", Severity.OK,
                                   "No cross-batch duplicate finance rows",
                                   value=0, threshold=0, query_used=sql, elapsed_ms=ms)
        total_excess = sum(int(r[1]) - 1 for r in rows)
        sev = Severity.WARNING if total_excess < 50 else Severity.CRITICAL
        return GuardrailResult("duplicate_finance_txn", sev,
                               f"{count} cross-batch duplicate groups ({total_excess} excess rows)",
                               value=total_excess, threshold=0, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("duplicate_finance_txn", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_order_finance_drift() -> GuardrailResult:
    """Orders settled 3-14 days ago should have matching finance transactions.

    Thresholds set for bootstrap phase — tighten to 15/40 once
    settlement-report backfill covers all historical periods."""
    sql = _ORDER_FINANCE_DRIFT_SQL
    try:
        rows, ms = _timed(_run_rows, sql)
        if not rows:
            return GuardrailResult("order_finance_drift", Severity.UNKNOWN,
                                   "Could not compute drift", query_used=sql, elapsed_ms=ms)
        missing, total = int(rows[0][0] or 0), int(rows[0][1] or 0)
        pct = round(missing / total * 100, 1) if total > 0 else 0.0
        sev = Severity.OK if pct <= 50.0 else (Severity.WARNING if pct <= 85.0 else Severity.CRITICAL)
        return GuardrailResult("order_finance_drift", sev,
                               f"{missing}/{total} settled orders lack finance data ({pct}%)",
                               value=pct, threshold=50.0, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("order_finance_drift", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3 — Infrastructure & Service Checks
# ═══════════════════════════════════════════════════════════════════════════

async def check_scheduler_health() -> GuardrailResult:
    """Verify scheduler is running.  Checks APScheduler state first,
    then Redis leader lock (if available)."""
    try:
        from app.scheduler import scheduler as _sched
        if _sched.running:
            jobs = _sched.get_jobs()
            return GuardrailResult("scheduler_health", Severity.OK,
                                   f"APScheduler running, {len(jobs)} jobs registered",
                                   value=len(jobs), threshold=1)
        return GuardrailResult("scheduler_health", Severity.CRITICAL,
                               "APScheduler not running")
    except Exception as exc:
        return GuardrailResult("scheduler_health", Severity.UNKNOWN, str(exc)[:200])


async def check_circuit_breaker_state() -> GuardrailResult:
    """Check content publish circuit breaker."""
    try:
        from app.core.circuit_breaker import get_state
        state = await get_state()
        if state["state"] == "closed":
            return GuardrailResult("circuit_breaker", Severity.OK,
                                   f"Closed, {state['failures_in_window']} failures in window",
                                   value=state["failures_in_window"],
                                   threshold=10)
        remaining = state.get("cooldown_remaining_seconds", 0)
        return GuardrailResult("circuit_breaker", Severity.WARNING,
                               f"OPEN — {remaining}s remaining, {state['failures_in_window']} failures",
                               value=state["failures_in_window"],
                               threshold=10)
    except Exception as exc:
        msg = str(exc)[:200]
        if "connecting to" in msg and "6380" in msg:
            return GuardrailResult("circuit_breaker", Severity.OK,
                                   "Redis unavailable (dev mode) — circuit breaker skipped")
        return GuardrailResult("circuit_breaker", Severity.UNKNOWN, msg)


async def check_rate_limit_blocks() -> GuardrailResult:
    """Count currently blocked IPs from auth rate limiting."""
    try:
        from app.core.redis_client import get_redis
        redis = await get_redis()
        keys = []
        async for key in redis.scan_iter("auth:block:*"):
            keys.append(key)
        count = len(keys)
        sev = Severity.OK if count <= 3 else (Severity.WARNING if count <= 10 else Severity.CRITICAL)
        return GuardrailResult("rate_limit_blocks", sev,
                               f"{count} IPs currently blocked",
                               value=count, threshold=3)
    except Exception as exc:
        msg = str(exc)[:200]
        if "connecting to" in msg and "6380" in msg:
            return GuardrailResult("rate_limit_blocks", Severity.OK,
                                   "Redis unavailable (dev mode) — rate limiting skipped")
        return GuardrailResult("rate_limit_blocks", Severity.UNKNOWN, msg)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4 — Daily Integrity Cross-Checks
# ═══════════════════════════════════════════════════════════════════════════

_ORDERS_VS_FINANCE_TOTALS_SQL = """\
WITH order_totals AS (
    SELECT
        SUM(ol.item_price) AS order_revenue
    FROM acc_order_line ol WITH (NOLOCK)
    JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    WHERE o.purchase_date >= DATEADD(DAY, -7, SYSUTCDATETIME())
      AND o.status NOT IN ('Canceled','Cancelled','Pending')
),
finance_totals AS (
    SELECT
        SUM(CASE WHEN charge_type = 'Principal' THEN amount ELSE 0 END) AS finance_revenue
    FROM acc_finance_transaction WITH (NOLOCK)
    WHERE posted_date >= DATEADD(DAY, -7, SYSUTCDATETIME())
)
SELECT
    ot.order_revenue,
    ft.finance_revenue,
    CASE WHEN ABS(ot.order_revenue) > 0
         THEN ROUND(ABS(ot.order_revenue - ft.finance_revenue)
                     / ABS(ot.order_revenue) * 100, 1)
         ELSE 0 END AS drift_pct
FROM order_totals ot
CROSS JOIN finance_totals ft
"""

_INVENTORY_COUNT_VS_AMAZON_SQL = """\
SELECT
    COUNT(*) AS total_skus,
    SUM(CASE WHEN qty_fulfillable IS NULL THEN 1 ELSE 0 END) AS missing_qty,
    SUM(CASE WHEN qty_fulfillable < 0 THEN 1 ELSE 0 END) AS negative_qty
FROM acc_inventory_snapshot WITH (NOLOCK)
WHERE snapshot_date = (
    SELECT MAX(snapshot_date) FROM acc_inventory_snapshot WITH (NOLOCK)
)
"""

_ADS_SPEND_CONSISTENCY_SQL = """\
SELECT
    SUM(spend) AS total_spend,
    COUNT(*) AS record_count,
    COUNT(DISTINCT campaign_id) AS campaigns
FROM acc_ads_campaign_day WITH (NOLOCK)
WHERE report_date >= DATEADD(DAY, -7, SYSUTCDATETIME())
  AND spend > 0
"""

_SHIPPING_COST_GAPS_SQL = """\
WITH scoped AS (
    SELECT
        month_token,
        carrier,
        purchase_orders_universe,
        purchase_orders_missing_actual_cost,
        purchase_actual_cost_coverage_pct
    FROM dbo.acc_courier_monthly_kpi_snapshot WITH (NOLOCK)
    WHERE carrier IN ('DHL', 'GLS')
      AND month_start >= DATEADD(
            MONTH,
            -3,
            DATEFROMPARTS(YEAR(SYSUTCDATETIME()), MONTH(SYSUTCDATETIME()), 1)
          )
      AND (is_closed_by_buffer = 1 OR readiness = 'READY')
)
SELECT
    COUNT(*) AS carrier_months,
    SUM(purchase_orders_universe) AS total_orders,
    SUM(purchase_orders_missing_actual_cost) AS missing_cost,
    MIN(purchase_actual_cost_coverage_pct) AS min_coverage_pct
FROM scoped
"""

_PROFIT_CALC_COMPLETENESS_SQL = """\
SELECT
    COUNT(*) AS total_orders,
    SUM(CASE WHEN ol.cogs_pln IS NULL THEN 1 ELSE 0 END) AS missing_cm1,
    SUM(CASE WHEN ol.purchase_price_pln IS NULL OR ol.purchase_price_pln = 0
             THEN 1 ELSE 0 END) AS missing_cogs
FROM acc_order_line ol WITH (NOLOCK)
JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
WHERE o.purchase_date >= DATEADD(DAY, -7, SYSUTCDATETIME())
  AND o.status NOT IN ('Canceled','Cancelled','Pending')
"""


def check_order_finance_totals() -> GuardrailResult:
    """Compare 7-day order revenue vs finance principal.

    Thresholds set for bootstrap phase — tighten to 10/25 once
    settlement-report backfill covers all historical periods."""
    sql = _ORDERS_VS_FINANCE_TOTALS_SQL
    try:
        rows, ms = _timed(_run_rows, sql)
        if not rows or rows[0][0] is None:
            return GuardrailResult("order_finance_totals", Severity.UNKNOWN,
                                   "No data for comparison", query_used=sql, elapsed_ms=ms)
        order_rev, fin_rev, drift = float(rows[0][0] or 0), float(rows[0][1] or 0), float(rows[0][2] or 0)
        sev = Severity.OK if drift <= 40.0 else (Severity.WARNING if drift <= 70.0 else Severity.CRITICAL)
        return GuardrailResult("order_finance_totals", sev,
                               f"Order rev={order_rev:,.0f}, Finance rev={fin_rev:,.0f}, drift={drift}%",
                               value=drift, threshold=40.0, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("order_finance_totals", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_inventory_integrity() -> GuardrailResult:
    """Verify latest inventory snapshot has no negative/missing quantities."""
    sql = _INVENTORY_COUNT_VS_AMAZON_SQL
    try:
        rows, ms = _timed(_run_rows, sql)
        if not rows:
            return GuardrailResult("inventory_integrity", Severity.UNKNOWN,
                                   "No inventory snapshot", query_used=sql, elapsed_ms=ms)
        total, missing, negative = int(rows[0][0] or 0), int(rows[0][1] or 0), int(rows[0][2] or 0)
        issues = missing + negative
        sev = Severity.OK if issues == 0 else (Severity.WARNING if issues < 10 else Severity.CRITICAL)
        return GuardrailResult("inventory_integrity", sev,
                               f"{total} SKUs: {missing} missing qty, {negative} negative qty",
                               value=issues, threshold=0, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("inventory_integrity", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_ads_spend_consistency() -> GuardrailResult:
    """Verify ads spend data is present for last 7 days."""
    sql = _ADS_SPEND_CONSISTENCY_SQL
    try:
        rows, ms = _timed(_run_rows, sql)
        if not rows or rows[0][0] is None:
            return GuardrailResult("ads_spend", Severity.WARNING,
                                   "No ads spend data in 7d", query_used=sql, elapsed_ms=ms)
        spend, records, campaigns = float(rows[0][0] or 0), int(rows[0][1] or 0), int(rows[0][2] or 0)
        sev = Severity.OK if records > 0 and campaigns > 0 else Severity.WARNING
        return GuardrailResult("ads_spend", sev,
                               f"7d ads: {spend:,.2f} spend, {records} records, {campaigns} campaigns",
                               value=spend, threshold=0, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("ads_spend", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_shipping_cost_gaps() -> GuardrailResult:
    """Verify closed-month courier coverage from the canonical KPI snapshot."""
    sql = _SHIPPING_COST_GAPS_SQL
    try:
        rows, ms = _timed(_run_rows, sql)
        if not rows:
            return GuardrailResult("shipping_costs", Severity.UNKNOWN,
                                   "No courier snapshot rows returned", query_used=sql, elapsed_ms=ms)
        carrier_months = int(rows[0][0] or 0)
        total = int(rows[0][1] or 0)
        missing = int(rows[0][2] or 0)
        if carrier_months == 0 or total == 0:
            return GuardrailResult("shipping_costs", Severity.UNKNOWN,
                                   "No closed-month courier snapshot coverage to check",
                                   query_used=sql, elapsed_ms=ms)
        pct = round(missing / total * 100, 1) if total > 0 else 0.0
        sev = Severity.OK if pct <= 5.0 else (Severity.WARNING if pct <= 15.0 else Severity.CRITICAL)
        return GuardrailResult("shipping_costs", sev,
                               f"{missing}/{total} closed-month courier orders missing actual cost ({pct}%)",
                               value=pct, threshold=5.0, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("shipping_costs", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_profit_calc_completeness() -> GuardrailResult:
    """Verify order lines have profit CM1 and COGS within 7 days."""
    sql = _PROFIT_CALC_COMPLETENESS_SQL
    try:
        rows, ms = _timed(_run_rows, sql)
        if not rows or int(rows[0][0] or 0) == 0:
            return GuardrailResult("profit_completeness", Severity.UNKNOWN,
                                   "No order lines to check", query_used=sql, elapsed_ms=ms)
        total, missing_cm1, missing_cogs = int(rows[0][0]), int(rows[0][1] or 0), int(rows[0][2] or 0)
        cm1_pct = round(missing_cm1 / total * 100, 1) if total > 0 else 0.0
        cogs_pct = round(missing_cogs / total * 100, 1) if total > 0 else 0.0
        worst = max(cm1_pct, cogs_pct)
        sev = Severity.OK if worst <= 5.0 else (Severity.WARNING if worst <= 15.0 else Severity.CRITICAL)
        return GuardrailResult("profit_completeness", sev,
                               f"{total} lines: {cm1_pct}% missing CM1, {cogs_pct}% missing COGS",
                               value=worst, threshold=5.0, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("profit_completeness", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 5 — SP-API Throttle & Job Duplication Checks
# ═══════════════════════════════════════════════════════════════════════════

_SPAPI_THROTTLE_SQL = """\
SELECT
    COUNT(*) AS total_calls,
    SUM(CASE WHEN status_code = 429 THEN 1 ELSE 0 END) AS throttled,
    SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS server_errors
FROM acc_spapi_usage WITH (NOLOCK)
WHERE called_at >= DATEADD(HOUR, -1, SYSUTCDATETIME())
"""

_JOB_DUPLICATION_SQL = """\
SELECT job_type, COUNT(*) AS running
FROM acc_al_jobs WITH (NOLOCK)
WHERE status = 'running'
  AND started_at >= DATEADD(HOUR, -2, SYSUTCDATETIME())
GROUP BY job_type
HAVING COUNT(*) > 1
"""


def check_spapi_throttle_rate() -> GuardrailResult:
    """SP-API throttle rate in last hour; warn > 5%, critical > 15%."""
    sql = _SPAPI_THROTTLE_SQL
    try:
        rows, ms = _timed(_run_rows, sql)
        if not rows or int(rows[0][0] or 0) == 0:
            return GuardrailResult("spapi_throttle", Severity.OK,
                                   "No SP-API calls in last hour", query_used=sql, elapsed_ms=ms)
        total, throttled, errors = int(rows[0][0]), int(rows[0][1] or 0), int(rows[0][2] or 0)
        throttle_pct = round(throttled / total * 100, 1) if total > 0 else 0.0
        sev = Severity.OK if throttle_pct <= 5.0 else (
            Severity.WARNING if throttle_pct <= 15.0 else Severity.CRITICAL)
        return GuardrailResult("spapi_throttle", sev,
                               f"1h: {throttled}/{total} throttled ({throttle_pct}%), {errors} 5xx",
                               value=throttle_pct, threshold=5.0, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("spapi_throttle", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_job_duplication() -> GuardrailResult:
    """Detect concurrent running instances of the same job type."""
    sql = _JOB_DUPLICATION_SQL
    try:
        rows, ms = _timed(_run_rows, sql)
        if not rows:
            return GuardrailResult("job_duplication", Severity.OK,
                                   "No duplicate running jobs", value=0, threshold=0,
                                   query_used=sql, elapsed_ms=ms)
        dupes = [(str(r[0]), int(r[1])) for r in rows]
        desc = "; ".join(f"{t}={c}" for t, c in dupes)
        return GuardrailResult("job_duplication", Severity.WARNING,
                               f"Duplicate running jobs: {desc}",
                               value=len(dupes), threshold=0, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("job_duplication", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 5b — Intelligence Freshness Checks
# ═══════════════════════════════════════════════════════════════════════════

_STRATEGY_DETECTION_FRESHNESS_SQL = """\
SELECT DATEDIFF(HOUR, MAX(updated_at), SYSUTCDATETIME())
FROM growth_opportunity WITH (NOLOCK)
"""

_SEASONALITY_FRESHNESS_SQL = """\
SELECT DATEDIFF(HOUR, MAX(created_at), SYSUTCDATETIME())
FROM seasonality_monthly_metrics WITH (NOLOCK)
"""

_DECISION_EVAL_FRESHNESS_SQL = """\
SELECT DATEDIFF(HOUR, MAX(evaluated_at), SYSUTCDATETIME())
FROM opportunity_outcome WITH (NOLOCK)
"""

_EXECUTIVE_METRICS_FRESHNESS_SQL = """\
SELECT DATEDIFF(HOUR, MAX(computed_at), SYSUTCDATETIME())
FROM executive_daily_metrics WITH (NOLOCK)
"""


def check_strategy_detection_freshness() -> GuardrailResult:
    """Strategy detection should run daily; alert if > 36 h stale."""
    sql = _STRATEGY_DETECTION_FRESHNESS_SQL
    try:
        hours, ms = _timed(_run_scalar, sql)
        if hours is None:
            return GuardrailResult("strategy_detection_freshness", Severity.CRITICAL,
                                   "No strategy detections found", query_used=sql, elapsed_ms=ms)
        sev = Severity.OK if hours <= 36 else (Severity.WARNING if hours <= 72 else Severity.CRITICAL)
        return GuardrailResult("strategy_detection_freshness", sev,
                               f"Latest strategy detection {hours}h ago",
                               value=hours, threshold=36, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("strategy_detection_freshness", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_seasonality_freshness() -> GuardrailResult:
    """Seasonality metrics rebuilt monthly; warn if > 45 days stale."""
    sql = _SEASONALITY_FRESHNESS_SQL
    try:
        hours, ms = _timed(_run_scalar, sql)
        if hours is None:
            return GuardrailResult("seasonality_freshness", Severity.CRITICAL,
                                   "No seasonality metrics found", query_used=sql, elapsed_ms=ms)
        days = hours / 24
        sev = Severity.OK if days <= 45 else (Severity.WARNING if days <= 75 else Severity.CRITICAL)
        return GuardrailResult("seasonality_freshness", sev,
                               f"Latest seasonality metrics {days:.0f} days ago",
                               value=round(days, 1), threshold=45, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("seasonality_freshness", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_decision_eval_freshness() -> GuardrailResult:
    """Decision outcome evaluations should run daily; alert if > 48 h stale."""
    sql = _DECISION_EVAL_FRESHNESS_SQL
    try:
        hours, ms = _timed(_run_scalar, sql)
        if hours is None:
            return GuardrailResult("decision_eval_freshness", Severity.CRITICAL,
                                   "No outcome evaluations found", query_used=sql, elapsed_ms=ms)
        sev = Severity.OK if hours <= 48 else (Severity.WARNING if hours <= 96 else Severity.CRITICAL)
        return GuardrailResult("decision_eval_freshness", sev,
                               f"Latest outcome evaluation {hours}h ago",
                               value=hours, threshold=48, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("decision_eval_freshness", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


def check_executive_metrics_freshness() -> GuardrailResult:
    """Executive daily metrics should compute nightly; alert if > 36 h stale."""
    sql = _EXECUTIVE_METRICS_FRESHNESS_SQL
    try:
        hours, ms = _timed(_run_scalar, sql)
        if hours is None:
            return GuardrailResult("executive_metrics_freshness", Severity.CRITICAL,
                                   "No executive metrics found", query_used=sql, elapsed_ms=ms)
        sev = Severity.OK if hours <= 36 else (Severity.WARNING if hours <= 48 else Severity.CRITICAL)
        return GuardrailResult("executive_metrics_freshness", sev,
                               f"Latest executive metrics {hours}h ago",
                               value=hours, threshold=36, query_used=sql, elapsed_ms=ms)
    except Exception as exc:
        return GuardrailResult("executive_metrics_freshness", Severity.UNKNOWN, str(exc)[:200], query_used=sql)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 6 — Orchestrator
# ═══════════════════════════════════════════════════════════════════════════

# Sync checks (run in threadpool from async context)
_SYNC_CHECKS = [
    check_order_sync_freshness,
    check_finance_freshness,
    check_inventory_freshness,
    check_profitability_freshness,
    check_fx_rate_freshness,
    check_ads_freshness,
    check_content_queue_depth,
    check_unknown_fee_types,
    check_fee_classification_coverage,
    check_profit_margin_anomalies,
    check_missing_fx_rates,
    check_duplicate_finance_transactions,
    check_order_finance_drift,
    check_order_finance_totals,
    check_inventory_integrity,
    check_ads_spend_consistency,
    check_shipping_cost_gaps,
    check_profit_calc_completeness,
    check_spapi_throttle_rate,
    check_job_duplication,
    check_strategy_detection_freshness,
    check_seasonality_freshness,
    check_decision_eval_freshness,
    check_executive_metrics_freshness,
]

# Async checks (call directly in async context)
_ASYNC_CHECKS = [
    check_scheduler_health,
    check_circuit_breaker_state,
    check_rate_limit_blocks,
]


def run_all_sync_checks() -> list[GuardrailResult]:
    """Execute all synchronous guardrail checks.  Thread-safe."""
    results: list[GuardrailResult] = []
    for fn in _SYNC_CHECKS:
        try:
            results.append(fn())
        except Exception as exc:
            results.append(GuardrailResult(
                fn.__name__.replace("check_", ""),
                Severity.UNKNOWN,
                f"Guardrail crashed: {exc!s}"[:200],
            ))
    return results


async def run_all_async_checks() -> list[GuardrailResult]:
    """Execute all async guardrail checks (Redis-backed)."""
    results: list[GuardrailResult] = []
    for fn in _ASYNC_CHECKS:
        try:
            results.append(await fn())
        except Exception as exc:
            results.append(GuardrailResult(
                fn.__name__.replace("check_", ""),
                Severity.UNKNOWN,
                f"Guardrail crashed: {exc!s}"[:200],
            ))
    return results


def persist_results(results: list[GuardrailResult]) -> None:
    """Write guardrail results to acc_guardrail_results for trending."""
    try:
        conn = connect_acc(timeout=10)
        try:
            cur = conn.cursor()
            cur.execute("""\
                IF OBJECT_ID('dbo.acc_guardrail_results', 'U') IS NULL
                CREATE TABLE dbo.acc_guardrail_results (
                    id           BIGINT IDENTITY(1,1) PRIMARY KEY,
                    check_name   VARCHAR(100) NOT NULL,
                    severity     VARCHAR(20)  NOT NULL,
                    message      NVARCHAR(500),
                    value        FLOAT NULL,
                    threshold    FLOAT NULL,
                    elapsed_ms   FLOAT NULL,
                    checked_at   DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    INDEX ix_guardrail_name_date (check_name, checked_at)
                )
            """)
            conn.commit()

            for r in results:
                cur.execute("""\
                    INSERT INTO acc_guardrail_results
                        (check_name, severity, message, value, threshold, elapsed_ms, checked_at)
                    VALUES (?, ?, ?, ?, ?, ?, SYSUTCDATETIME())
                """, (
                    r.check_name,
                    r.severity.value,
                    r.message[:500],
                    float(r.value) if r.value is not None else None,
                    float(r.threshold) if r.threshold is not None else None,
                    r.elapsed_ms,
                ))
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        log.error("guardrails.persist_failed", error=str(exc))


async def run_guardrails(*, persist: bool = True) -> dict:
    """Run all guardrail checks and return structured report.

    Called by scheduler and by the API endpoint.
    """
    import asyncio
    from fastapi.concurrency import run_in_threadpool

    t0 = time.perf_counter()

    sync_results = await run_in_threadpool(run_all_sync_checks)
    async_results = await run_all_async_checks()
    all_results = sync_results + async_results

    if persist:
        await run_in_threadpool(persist_results, all_results)

    # Log summary
    by_sev = {}
    for r in all_results:
        by_sev[r.severity.value] = by_sev.get(r.severity.value, 0) + 1
    elapsed = round((time.perf_counter() - t0) * 1000, 1)

    for r in all_results:
        if r.severity in (Severity.WARNING, Severity.CRITICAL):
            log.warning("guardrail.alert",
                        check=r.check_name, severity=r.severity.value,
                        message=r.message, value=r.value)

    log.info("guardrails.complete", elapsed_ms=elapsed, summary=by_sev,
             total_checks=len(all_results))

    overall = "healthy"
    if by_sev.get("critical", 0) > 0:
        overall = "critical"
    elif by_sev.get("warning", 0) > 0:
        overall = "degraded"
    elif by_sev.get("unknown", 0) > 0:
        overall = "partial"

    return {
        "status": overall,
        "elapsed_ms": elapsed,
        "summary": by_sev,
        "total_checks": len(all_results),
        "checks": [asdict(r) for r in all_results],
    }
