"""Refund / Fee Anomaly Engine — Sprint 21-22.

Detects refund spikes, fee spikes, return rate spikes, identifies
serial returners, and manages automated reimbursement cases for
FBA lost/damaged inventory and fee overcharges.

Tables:
  acc_refund_anomaly      — Detected refund spike anomalies
  acc_serial_returner     — Identified serial returner patterns
  acc_reimbursement_case  — Reimbursement claim tracking

Capabilities:
  - Refund spike detection: week-over-week refund rate comparison per SKU
  - Fee spike detection: bridges fba_fee_audit to persist fee anomalies
  - Return rate spike detection: per-SKU return rate anomaly check
  - Serial returner identification: high-frequency returner pattern detection
  - Reimbursement case generation: FBA lost/damaged, fee overcharges
  - Detail and trend queries for drill-down analysis
  - CSV export support
  - Dashboard aggregations and KPI summaries
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

import structlog

from app.connectors.mssql import connect_acc

log = structlog.get_logger(__name__)

# ── Constants ────────────────────────────────────────────────────────

ANOMALY_TYPES = {"refund_spike", "fee_spike", "return_rate_spike"}
ANOMALY_SEVERITIES = {"critical", "high", "medium", "low"}
ANOMALY_STATUSES = {"open", "investigating", "resolved", "dismissed"}

RETURNER_RISK_TIERS = {"critical", "high", "medium", "low"}
RETURNER_STATUSES = {"flagged", "monitoring", "cleared", "blocked"}

CASE_TYPES = {"lost_inventory", "damaged_inbound", "fee_overcharge", "customer_return_not_received"}
CASE_STATUSES = {"identified", "filed", "accepted", "rejected", "paid"}

# Thresholds for anomaly detection
SPIKE_RATIO_CRITICAL = 3.0
SPIKE_RATIO_HIGH = 2.0
SPIKE_RATIO_MEDIUM = 1.5
MIN_ORDERS_FOR_SPIKE = 5  # Minimum orders in a period to flag

# Fee spike thresholds
FEE_SPIKE_RATIO_CRITICAL = 3.0
FEE_SPIKE_RATIO_HIGH = 2.0
FEE_SPIKE_RATIO_MEDIUM = 1.5

# Return rate spike thresholds
RETURN_RATE_SPIKE_CRITICAL = 3.0
RETURN_RATE_SPIKE_HIGH = 2.0
RETURN_RATE_SPIKE_MEDIUM = 1.5
MIN_UNITS_FOR_RETURN_SPIKE = 10  # Min units shipped to flag return rate

# Thresholds for serial returner detection
SERIAL_RETURN_MIN_COUNT = 3
SERIAL_RETURN_HIGH_RATE = 0.5  # 50% return rate
SERIAL_RETURN_CRITICAL_RATE = 0.7  # 70% return rate

# Reimbursement eligibility: days after lost/damaged event
REIMBURSEMENT_WINDOW_DAYS = 90


# ── Schema DDL ───────────────────────────────────────────────────────

_ANOMALY_SCHEMA: list[str] = [
    """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_refund_anomaly')
    CREATE TABLE dbo.acc_refund_anomaly (
        id                 INT IDENTITY(1,1) PRIMARY KEY,
        sku                NVARCHAR(60)   NOT NULL,
        asin               NVARCHAR(20)   NULL,
        marketplace_id     NVARCHAR(20)   NOT NULL,
        anomaly_type       NVARCHAR(40)   NOT NULL,
        detection_date     DATE           NOT NULL,
        period_start       DATE           NOT NULL,
        period_end         DATE           NOT NULL,
        baseline_rate      FLOAT          NOT NULL DEFAULT 0,
        current_rate       FLOAT          NOT NULL DEFAULT 0,
        spike_ratio        FLOAT          NOT NULL DEFAULT 0,
        refund_count       INT            NOT NULL DEFAULT 0,
        order_count        INT            NOT NULL DEFAULT 0,
        refund_amount_pln  FLOAT          NOT NULL DEFAULT 0,
        estimated_loss_pln FLOAT          NOT NULL DEFAULT 0,
        severity           NVARCHAR(20)   NOT NULL DEFAULT 'medium',
        status             NVARCHAR(20)   NOT NULL DEFAULT 'open',
        resolution_note    NVARCHAR(500)  NULL,
        resolved_by        NVARCHAR(60)   NULL,
        resolved_at        DATETIME2      NULL,
        created_at         DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at         DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_serial_returner')
    CREATE TABLE dbo.acc_serial_returner (
        id                 INT IDENTITY(1,1) PRIMARY KEY,
        buyer_identifier   NVARCHAR(120)  NOT NULL,
        marketplace_id     NVARCHAR(20)   NOT NULL,
        detection_date     DATE           NOT NULL,
        return_count       INT            NOT NULL DEFAULT 0,
        order_count        INT            NOT NULL DEFAULT 0,
        return_rate        FLOAT          NOT NULL DEFAULT 0,
        total_refund_pln   FLOAT          NOT NULL DEFAULT 0,
        avg_refund_pln     FLOAT          NOT NULL DEFAULT 0,
        first_return_date  DATE           NULL,
        last_return_date   DATE           NULL,
        top_skus           NVARCHAR(500)  NULL,
        risk_score         INT            NOT NULL DEFAULT 0,
        risk_tier          NVARCHAR(20)   NOT NULL DEFAULT 'low',
        status             NVARCHAR(20)   NOT NULL DEFAULT 'flagged',
        notes              NVARCHAR(500)  NULL,
        created_at         DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at         DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_reimbursement_case')
    CREATE TABLE dbo.acc_reimbursement_case (
        id                    INT IDENTITY(1,1) PRIMARY KEY,
        case_type             NVARCHAR(40)   NOT NULL,
        sku                   NVARCHAR(60)   NOT NULL,
        asin                  NVARCHAR(20)   NULL,
        marketplace_id        NVARCHAR(20)   NOT NULL,
        amazon_order_id       NVARCHAR(30)   NULL,
        fnsku                 NVARCHAR(20)   NULL,
        quantity              INT            NOT NULL DEFAULT 1,
        estimated_value_pln   FLOAT          NOT NULL DEFAULT 0,
        evidence_summary      NVARCHAR(1000) NULL,
        amazon_case_id        NVARCHAR(40)   NULL,
        status                NVARCHAR(20)   NOT NULL DEFAULT 'identified',
        filed_at              DATETIME2      NULL,
        resolved_at           DATETIME2      NULL,
        reimbursed_amount_pln FLOAT          NULL,
        resolution_note       NVARCHAR(500)  NULL,
        created_at            DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at            DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );
    """,
]


def ensure_anomaly_schema() -> None:
    """Create anomaly tables if needed (idempotent)."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        for ddl in _ANOMALY_SCHEMA:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


# ── Row mappers ──────────────────────────────────────────────────────

def _anomaly_row_to_dict(row: tuple) -> dict[str, Any]:
    """Map acc_refund_anomaly row (22 columns) to dict."""
    return {
        "id": row[0],
        "sku": row[1],
        "asin": row[2],
        "marketplace_id": row[3],
        "anomaly_type": row[4],
        "detection_date": str(row[5]) if row[5] else None,
        "period_start": str(row[6]) if row[6] else None,
        "period_end": str(row[7]) if row[7] else None,
        "baseline_rate": row[8],
        "current_rate": row[9],
        "spike_ratio": round(float(row[10]), 2) if row[10] else 0,
        "refund_count": row[11],
        "order_count": row[12],
        "refund_amount_pln": round(float(row[13]), 2) if row[13] else 0,
        "estimated_loss_pln": round(float(row[14]), 2) if row[14] else 0,
        "severity": row[15],
        "status": row[16],
        "resolution_note": row[17],
        "resolved_by": row[18],
        "resolved_at": str(row[19]) if row[19] else None,
        "created_at": str(row[20]) if row[20] else None,
        "updated_at": str(row[21]) if row[21] else None,
    }


def _returner_row_to_dict(row: tuple) -> dict[str, Any]:
    """Map acc_serial_returner row (18 columns) to dict."""
    return {
        "id": row[0],
        "buyer_identifier": row[1],
        "marketplace_id": row[2],
        "detection_date": str(row[3]) if row[3] else None,
        "return_count": row[4],
        "order_count": row[5],
        "return_rate": round(float(row[6]), 4) if row[6] else 0,
        "total_refund_pln": round(float(row[7]), 2) if row[7] else 0,
        "avg_refund_pln": round(float(row[8]), 2) if row[8] else 0,
        "first_return_date": str(row[9]) if row[9] else None,
        "last_return_date": str(row[10]) if row[10] else None,
        "top_skus": row[11],
        "risk_score": row[12],
        "risk_tier": row[13],
        "status": row[14],
        "notes": row[15],
        "created_at": str(row[16]) if row[16] else None,
        "updated_at": str(row[17]) if row[17] else None,
    }


def _case_row_to_dict(row: tuple) -> dict[str, Any]:
    """Map acc_reimbursement_case row (17 columns) to dict."""
    return {
        "id": row[0],
        "case_type": row[1],
        "sku": row[2],
        "asin": row[3],
        "marketplace_id": row[4],
        "amazon_order_id": row[5],
        "fnsku": row[6],
        "quantity": row[7],
        "estimated_value_pln": round(float(row[8]), 2) if row[8] else 0,
        "evidence_summary": row[9],
        "amazon_case_id": row[10],
        "status": row[11],
        "filed_at": str(row[12]) if row[12] else None,
        "resolved_at": str(row[13]) if row[13] else None,
        "reimbursed_amount_pln": round(float(row[14]), 2) if row[14] else 0,
        "resolution_note": row[15],
        "created_at": str(row[16]) if row[16] else None,
    }


# ── Refund spike detection ───────────────────────────────────────────

_ANOMALY_SELECT = """
    SELECT id, sku, asin, marketplace_id, anomaly_type,
           detection_date, period_start, period_end,
           baseline_rate, current_rate, spike_ratio,
           refund_count, order_count, refund_amount_pln, estimated_loss_pln,
           severity, status, resolution_note, resolved_by, resolved_at,
           created_at, updated_at
    FROM dbo.acc_refund_anomaly WITH (NOLOCK)
"""


def detect_refund_spikes(
    *,
    lookback_days: int = 28,
    comparison_days: int = 28,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    """Detect refund rate spikes per SKU by comparing recent period vs baseline.

    Compares refund_count/order_count ratio for recent `lookback_days`
    against the preceding `comparison_days` baseline.

    Returns dict with anomalies detected and saved count.
    """
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        today = date.today()
        recent_start = today - timedelta(days=lookback_days)
        baseline_start = recent_start - timedelta(days=comparison_days)
        baseline_end = recent_start - timedelta(days=1)

        mkt_filter = "AND o.marketplace_id = %s" if marketplace_id else ""
        mkt_params: list[Any] = [marketplace_id] if marketplace_id else []

        # Get per-SKU refund stats for recent period
        cur.execute(f"""
            SELECT
                ol.sku,
                MAX(ol.asin) AS asin,
                o.marketplace_id,
                COUNT(DISTINCT CASE WHEN o.is_refund = 1 THEN o.amazon_order_id END) AS refund_count,
                COUNT(DISTINCT o.amazon_order_id) AS order_count,
                ISNULL(SUM(CASE WHEN o.is_refund = 1 THEN o.refund_amount_pln END), 0) AS refund_amount_pln
            FROM dbo.acc_order o WITH (NOLOCK)
            JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
            WHERE o.purchase_date >= %s AND o.purchase_date < %s
                {mkt_filter}
            GROUP BY ol.sku, o.marketplace_id
            HAVING COUNT(DISTINCT o.amazon_order_id) >= %s
        """, [recent_start, today] + mkt_params + [MIN_ORDERS_FOR_SPIKE])
        recent_rows = cur.fetchall()

        # Get per-SKU baseline stats
        cur.execute(f"""
            SELECT
                ol.sku,
                o.marketplace_id,
                COUNT(DISTINCT CASE WHEN o.is_refund = 1 THEN o.amazon_order_id END) AS refund_count,
                COUNT(DISTINCT o.amazon_order_id) AS order_count
            FROM dbo.acc_order o WITH (NOLOCK)
            JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
            WHERE o.purchase_date >= %s AND o.purchase_date <= %s
                {mkt_filter}
            GROUP BY ol.sku, o.marketplace_id
            HAVING COUNT(DISTINCT o.amazon_order_id) >= %s
        """, [baseline_start, baseline_end] + mkt_params + [MIN_ORDERS_FOR_SPIKE])

        baseline_map: dict[str, tuple] = {}
        for br in cur.fetchall():
            key = f"{br[0]}|{br[1]}"
            baseline_map[key] = br

        anomalies_created = 0
        for row in recent_rows:
            sku, asin, mkt, refund_cnt, order_cnt, refund_amt = row
            current_rate = refund_cnt / order_cnt if order_cnt > 0 else 0

            key = f"{sku}|{mkt}"
            baseline = baseline_map.get(key)
            if baseline:
                bl_refund = baseline[2]
                bl_orders = baseline[3]
                baseline_rate = bl_refund / bl_orders if bl_orders > 0 else 0
            else:
                baseline_rate = 0

            if baseline_rate > 0:
                spike_ratio = current_rate / baseline_rate
            elif current_rate > 0:
                spike_ratio = 10.0  # No baseline, treat as large spike
            else:
                continue

            if spike_ratio < SPIKE_RATIO_MEDIUM:
                continue

            severity = _classify_severity(spike_ratio)
            estimated_loss = float(refund_amt) * 0.3  # estimated margin loss

            # Insert anomaly (deduplicate by sku + marketplace + period)
            cur.execute("""
                MERGE dbo.acc_refund_anomaly AS t
                USING (SELECT %s AS sku, %s AS marketplace_id, %s AS period_start) AS s
                    ON t.sku = s.sku AND t.marketplace_id = s.marketplace_id
                    AND t.period_start = s.period_start AND t.anomaly_type = 'refund_spike'
                WHEN MATCHED THEN UPDATE SET
                    current_rate = %s, baseline_rate = %s, spike_ratio = %s,
                    refund_count = %s, order_count = %s, refund_amount_pln = %s,
                    estimated_loss_pln = %s, severity = %s,
                    updated_at = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN INSERT
                    (sku, asin, marketplace_id, anomaly_type, detection_date,
                     period_start, period_end, baseline_rate, current_rate,
                     spike_ratio, refund_count, order_count, refund_amount_pln,
                     estimated_loss_pln, severity)
                VALUES (%s, %s, %s, 'refund_spike', %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s);
            """, (
                sku, mkt, str(recent_start),
                current_rate, baseline_rate, spike_ratio,
                refund_cnt, order_cnt, float(refund_amt),
                estimated_loss, severity,
                sku, asin, mkt, str(today), str(recent_start), str(today),
                baseline_rate, current_rate, spike_ratio,
                refund_cnt, order_cnt, float(refund_amt), estimated_loss, severity,
            ))
            anomalies_created += 1

        conn.commit()
        log.info("refund_spikes.detected", anomalies=anomalies_created)
        return {"anomalies_created": anomalies_created, "skus_analyzed": len(recent_rows)}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _classify_severity(spike_ratio: float) -> str:
    if spike_ratio >= SPIKE_RATIO_CRITICAL:
        return "critical"
    elif spike_ratio >= SPIKE_RATIO_HIGH:
        return "high"
    elif spike_ratio >= SPIKE_RATIO_MEDIUM:
        return "medium"
    return "low"


# ── Fee spike detection ──────────────────────────────────────────────

FEE_CHARGE_TYPES = (
    "FBAPerUnitFulfillmentFee",
    "FBAPerOrderFulfillmentFee",
    "FBAWeightBasedFee",
    "FBAPickAndPackFee",
)


def detect_fee_spikes(
    *,
    lookback_days: int = 28,
    comparison_days: int = 28,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    """Detect FBA fee spikes per SKU by comparing recent avg fee vs baseline.

    Computes average FBA fee per unit in the recent period and compares it
    against the preceding baseline period.  Results are persisted as
    anomaly_type='fee_spike' rows in acc_refund_anomaly.
    """
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        today = date.today()
        recent_start = today - timedelta(days=lookback_days)
        baseline_start = recent_start - timedelta(days=comparison_days)
        baseline_end = recent_start - timedelta(days=1)

        charge_placeholders = ", ".join(["%s"] * len(FEE_CHARGE_TYPES))
        mkt_filter = "AND ft.marketplace_id = %s" if marketplace_id else ""
        mkt_params: list[Any] = [marketplace_id] if marketplace_id else []

        # Recent period avg fee per SKU
        cur.execute(f"""
            SELECT
                ft.sku,
                MAX(p.asin) AS asin,
                ft.marketplace_id,
                AVG(ABS(ft.amount_pln)) AS avg_fee,
                COUNT(*) AS fee_count,
                SUM(ABS(ft.amount_pln)) AS total_fee
            FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
            LEFT JOIN dbo.acc_product p WITH (NOLOCK) ON p.sku = ft.sku
                AND p.marketplace_id = ft.marketplace_id
            WHERE ft.charge_type IN ({charge_placeholders})
                AND ft.posted_date >= %s AND ft.posted_date < %s
                {mkt_filter}
                AND ft.sku IS NOT NULL
            GROUP BY ft.sku, ft.marketplace_id
            HAVING COUNT(*) >= %s
        """, list(FEE_CHARGE_TYPES) + [recent_start, today] + mkt_params + [MIN_ORDERS_FOR_SPIKE])
        recent_rows = cur.fetchall()

        # Baseline period avg fee per SKU
        cur.execute(f"""
            SELECT
                ft.sku,
                ft.marketplace_id,
                AVG(ABS(ft.amount_pln)) AS avg_fee,
                COUNT(*) AS fee_count
            FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
            WHERE ft.charge_type IN ({charge_placeholders})
                AND ft.posted_date >= %s AND ft.posted_date <= %s
                {mkt_filter}
                AND ft.sku IS NOT NULL
            GROUP BY ft.sku, ft.marketplace_id
            HAVING COUNT(*) >= %s
        """, list(FEE_CHARGE_TYPES) + [baseline_start, baseline_end] + mkt_params + [MIN_ORDERS_FOR_SPIKE])

        baseline_map: dict[str, tuple] = {}
        for br in cur.fetchall():
            baseline_map[f"{br[0]}|{br[1]}"] = br

        anomalies_created = 0
        for row in recent_rows:
            sku, asin, mkt, avg_fee, fee_count, total_fee = row
            current_rate = float(avg_fee) if avg_fee else 0

            key = f"{sku}|{mkt}"
            baseline = baseline_map.get(key)
            if baseline:
                baseline_rate = float(baseline[2]) if baseline[2] else 0
            else:
                baseline_rate = 0

            if baseline_rate > 0:
                spike_ratio = current_rate / baseline_rate
            elif current_rate > 0:
                spike_ratio = 10.0
            else:
                continue

            if spike_ratio < FEE_SPIKE_RATIO_MEDIUM:
                continue

            severity = _classify_fee_severity(spike_ratio)
            estimated_overcharge = float(total_fee) - (baseline_rate * fee_count) if baseline_rate > 0 else 0
            estimated_loss = max(estimated_overcharge, 0)

            cur.execute("""
                MERGE dbo.acc_refund_anomaly AS t
                USING (SELECT %s AS sku, %s AS marketplace_id, %s AS period_start) AS s
                    ON t.sku = s.sku AND t.marketplace_id = s.marketplace_id
                    AND t.period_start = s.period_start AND t.anomaly_type = 'fee_spike'
                WHEN MATCHED THEN UPDATE SET
                    current_rate = %s, baseline_rate = %s, spike_ratio = %s,
                    refund_count = %s, order_count = %s, refund_amount_pln = %s,
                    estimated_loss_pln = %s, severity = %s,
                    updated_at = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN INSERT
                    (sku, asin, marketplace_id, anomaly_type, detection_date,
                     period_start, period_end, baseline_rate, current_rate,
                     spike_ratio, refund_count, order_count, refund_amount_pln,
                     estimated_loss_pln, severity)
                VALUES (%s, %s, %s, 'fee_spike', %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s);
            """, (
                sku, mkt, str(recent_start),
                current_rate, baseline_rate, spike_ratio,
                fee_count, fee_count, float(total_fee),
                estimated_loss, severity,
                sku, asin, mkt, str(today), str(recent_start), str(today),
                baseline_rate, current_rate, spike_ratio,
                fee_count, fee_count, float(total_fee), estimated_loss, severity,
            ))
            anomalies_created += 1

        conn.commit()
        log.info("fee_spikes.detected", anomalies=anomalies_created)
        return {"anomalies_created": anomalies_created, "skus_analyzed": len(recent_rows)}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _classify_fee_severity(spike_ratio: float) -> str:
    if spike_ratio >= FEE_SPIKE_RATIO_CRITICAL:
        return "critical"
    elif spike_ratio >= FEE_SPIKE_RATIO_HIGH:
        return "high"
    elif spike_ratio >= FEE_SPIKE_RATIO_MEDIUM:
        return "medium"
    return "low"


# ── Return rate spike detection ──────────────────────────────────────

def detect_return_rate_spikes(
    *,
    lookback_days: int = 28,
    comparison_days: int = 28,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    """Detect per-SKU return rate spikes (return count / units shipped).

    Compares the recent return rate against the preceding baseline period.
    Results are persisted as anomaly_type='return_rate_spike' rows.
    """
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        today = date.today()
        recent_start = today - timedelta(days=lookback_days)
        baseline_start = recent_start - timedelta(days=comparison_days)
        baseline_end = recent_start - timedelta(days=1)

        mkt_filter = "AND ri.marketplace_id = %s" if marketplace_id else ""
        mkt_params: list[Any] = [marketplace_id] if marketplace_id else []

        # Recent period: return count + units shipped per SKU
        cur.execute(f"""
            SELECT
                ri.sku,
                MAX(ri.asin) AS asin,
                ri.marketplace_id,
                COUNT(*) AS return_count,
                ISNULL(SUM(ri.quantity_returned), COUNT(*)) AS units_returned,
                ISNULL(SUM(ri.refund_amount_pln), 0) AS refund_amount_pln,
                (
                    SELECT COUNT(DISTINCT ol.order_id)
                    FROM dbo.acc_order_line ol WITH (NOLOCK)
                    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
                    WHERE ol.sku = ri.sku
                        AND o.marketplace_id = ri.marketplace_id
                        AND o.purchase_date >= %s AND o.purchase_date < %s
                ) AS order_count
            FROM dbo.acc_return_item ri WITH (NOLOCK)
            WHERE ri.refund_date >= %s AND ri.refund_date < %s
                {mkt_filter}
            GROUP BY ri.sku, ri.marketplace_id
        """, [recent_start, today, recent_start, today] + mkt_params)
        recent_rows = cur.fetchall()

        # Baseline period
        cur.execute(f"""
            SELECT
                ri.sku,
                ri.marketplace_id,
                COUNT(*) AS return_count,
                (
                    SELECT COUNT(DISTINCT ol.order_id)
                    FROM dbo.acc_order_line ol WITH (NOLOCK)
                    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
                    WHERE ol.sku = ri.sku
                        AND o.marketplace_id = ri.marketplace_id
                        AND o.purchase_date >= %s AND o.purchase_date <= %s
                ) AS order_count
            FROM dbo.acc_return_item ri WITH (NOLOCK)
            WHERE ri.refund_date >= %s AND ri.refund_date <= %s
                {mkt_filter}
            GROUP BY ri.sku, ri.marketplace_id
        """, [baseline_start, baseline_end, baseline_start, baseline_end] + mkt_params)

        baseline_map: dict[str, tuple] = {}
        for br in cur.fetchall():
            baseline_map[f"{br[0]}|{br[1]}"] = br

        anomalies_created = 0
        for row in recent_rows:
            sku, asin, mkt, return_count, units_returned, refund_amt, order_count = row
            if not order_count or order_count < MIN_UNITS_FOR_RETURN_SPIKE:
                continue

            current_rate = return_count / order_count if order_count > 0 else 0

            key = f"{sku}|{mkt}"
            baseline = baseline_map.get(key)
            if baseline:
                bl_returns = baseline[2]
                bl_orders = baseline[3]
                baseline_rate = bl_returns / bl_orders if bl_orders and bl_orders > 0 else 0
            else:
                baseline_rate = 0

            if baseline_rate > 0:
                spike_ratio = current_rate / baseline_rate
            elif current_rate > 0:
                spike_ratio = 10.0
            else:
                continue

            if spike_ratio < RETURN_RATE_SPIKE_MEDIUM:
                continue

            severity = _classify_return_severity(spike_ratio)
            estimated_loss = float(refund_amt) * 0.3

            cur.execute("""
                MERGE dbo.acc_refund_anomaly AS t
                USING (SELECT %s AS sku, %s AS marketplace_id, %s AS period_start) AS s
                    ON t.sku = s.sku AND t.marketplace_id = s.marketplace_id
                    AND t.period_start = s.period_start AND t.anomaly_type = 'return_rate_spike'
                WHEN MATCHED THEN UPDATE SET
                    current_rate = %s, baseline_rate = %s, spike_ratio = %s,
                    refund_count = %s, order_count = %s, refund_amount_pln = %s,
                    estimated_loss_pln = %s, severity = %s,
                    updated_at = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN INSERT
                    (sku, asin, marketplace_id, anomaly_type, detection_date,
                     period_start, period_end, baseline_rate, current_rate,
                     spike_ratio, refund_count, order_count, refund_amount_pln,
                     estimated_loss_pln, severity)
                VALUES (%s, %s, %s, 'return_rate_spike', %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s);
            """, (
                sku, mkt, str(recent_start),
                current_rate, baseline_rate, spike_ratio,
                return_count, order_count, float(refund_amt),
                estimated_loss, severity,
                sku, asin, mkt, str(today), str(recent_start), str(today),
                baseline_rate, current_rate, spike_ratio,
                return_count, order_count, float(refund_amt), estimated_loss, severity,
            ))
            anomalies_created += 1

        conn.commit()
        log.info("return_rate_spikes.detected", anomalies=anomalies_created)
        return {"anomalies_created": anomalies_created, "skus_analyzed": len(recent_rows)}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _classify_return_severity(spike_ratio: float) -> str:
    if spike_ratio >= RETURN_RATE_SPIKE_CRITICAL:
        return "critical"
    elif spike_ratio >= RETURN_RATE_SPIKE_HIGH:
        return "high"
    elif spike_ratio >= RETURN_RATE_SPIKE_MEDIUM:
        return "medium"
    return "low"


# ── Serial returner detection ────────────────────────────────────────

def detect_serial_returners(
    *,
    lookback_days: int = 90,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    """Identify buyers with abnormally high return rates.

    Groups orders by buyer shipping address pattern (city + postal code hash)
    and flags those exceeding the serial return thresholds.
    """
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        since = date.today() - timedelta(days=lookback_days)

        mkt_filter = "AND o.marketplace_id = %s" if marketplace_id else ""
        mkt_params: list[Any] = [marketplace_id] if marketplace_id else []

        # Group refunds by buyer pattern (ship_city + ship_postal_code)
        cur.execute(f"""
            SELECT
                CONCAT(ISNULL(o.ship_city, 'UNKNOWN'), '|', ISNULL(o.ship_postal_code, '00000')) AS buyer_key,
                o.marketplace_id,
                COUNT(DISTINCT CASE WHEN o.is_refund = 1 THEN o.amazon_order_id END) AS return_count,
                COUNT(DISTINCT o.amazon_order_id) AS order_count,
                ISNULL(SUM(CASE WHEN o.is_refund = 1 THEN o.refund_amount_pln END), 0) AS total_refund,
                MIN(CASE WHEN o.is_refund = 1 THEN o.refund_date END) AS first_return,
                MAX(CASE WHEN o.is_refund = 1 THEN o.refund_date END) AS last_return
            FROM dbo.acc_order o WITH (NOLOCK)
            WHERE o.purchase_date >= %s
                AND o.ship_city IS NOT NULL
                {mkt_filter}
            GROUP BY
                CONCAT(ISNULL(o.ship_city, 'UNKNOWN'), '|', ISNULL(o.ship_postal_code, '00000')),
                o.marketplace_id
            HAVING COUNT(DISTINCT CASE WHEN o.is_refund = 1 THEN o.amazon_order_id END) >= %s
        """, [since] + mkt_params + [SERIAL_RETURN_MIN_COUNT])
        rows = cur.fetchall()

        # Get top returned SKUs per buyer pattern
        flagged = 0
        for row in rows:
            buyer_key, mkt, return_cnt, order_cnt, total_refund, first_ret, last_ret = row
            return_rate = return_cnt / order_cnt if order_cnt > 0 else 0

            if return_rate < SPIKE_RATIO_MEDIUM / 10:  # At least 15% return rate
                continue

            risk_score = _compute_risk_score(return_count=return_cnt, return_rate=return_rate,
                                             total_refund=float(total_refund or 0))
            risk_tier = _classify_risk_tier(risk_score)

            # Get top SKUs for this buyer
            cur.execute(f"""
                SELECT TOP 5 ol.sku, COUNT(*) AS cnt
                FROM dbo.acc_order o WITH (NOLOCK)
                JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
                WHERE o.is_refund = 1 AND o.purchase_date >= %s
                    AND CONCAT(ISNULL(o.ship_city, 'UNKNOWN'), '|', ISNULL(o.ship_postal_code, '00000')) = %s
                    AND o.marketplace_id = %s
                GROUP BY ol.sku
                ORDER BY cnt DESC
            """, [since, buyer_key, mkt])
            top_skus = json.dumps([r[0] for r in cur.fetchall()])

            avg_refund = float(total_refund or 0) / return_cnt if return_cnt > 0 else 0

            cur.execute("""
                MERGE dbo.acc_serial_returner AS t
                USING (SELECT %s AS buyer_identifier, %s AS marketplace_id) AS s
                    ON t.buyer_identifier = s.buyer_identifier
                    AND t.marketplace_id = s.marketplace_id
                WHEN MATCHED THEN UPDATE SET
                    return_count = %s, order_count = %s, return_rate = %s,
                    total_refund_pln = %s, avg_refund_pln = %s,
                    first_return_date = %s, last_return_date = %s,
                    top_skus = %s, risk_score = %s, risk_tier = %s,
                    detection_date = %s, updated_at = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN INSERT
                    (buyer_identifier, marketplace_id, detection_date,
                     return_count, order_count, return_rate,
                     total_refund_pln, avg_refund_pln,
                     first_return_date, last_return_date,
                     top_skus, risk_score, risk_tier)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                buyer_key, mkt,
                return_cnt, order_cnt, return_rate, float(total_refund or 0), avg_refund,
                str(first_ret) if first_ret else None, str(last_ret) if last_ret else None,
                top_skus, risk_score, risk_tier, str(date.today()),
                buyer_key, mkt, str(date.today()),
                return_cnt, order_cnt, return_rate, float(total_refund or 0), avg_refund,
                str(first_ret) if first_ret else None, str(last_ret) if last_ret else None,
                top_skus, risk_score, risk_tier,
            ))
            flagged += 1

        conn.commit()
        log.info("serial_returners.detected", flagged=flagged)
        return {"returners_flagged": flagged, "buyers_analyzed": len(rows)}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _compute_risk_score(*, return_count: int, return_rate: float, total_refund: float) -> int:
    """Compute a 0-100 risk score for a serial returner."""
    score = 0
    # Return count contribution (max 30)
    score += min(return_count * 5, 30)
    # Return rate contribution (max 40)
    score += min(int(return_rate * 40), 40)
    # Refund value contribution (max 30)
    if total_refund > 1000:
        score += 30
    elif total_refund > 500:
        score += 20
    elif total_refund > 100:
        score += 10
    return min(score, 100)


def _classify_risk_tier(risk_score: int) -> str:
    if risk_score >= 80:
        return "critical"
    elif risk_score >= 60:
        return "high"
    elif risk_score >= 40:
        return "medium"
    return "low"


# ── Reimbursement case generation ────────────────────────────────────

def scan_reimbursement_opportunities(
    *,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    """Scan for FBA items eligible for reimbursement claims.

    Identifies:
    - Returns marked as damaged/lost but no reimbursement received
    - Items with customer_return_not_received status
    - Damaged inbound shipment items
    - Fee overcharge patterns from fee audit engine
    """
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cutoff = date.today() - timedelta(days=REIMBURSEMENT_WINDOW_DAYS)

        mkt_filter = "AND ri.marketplace_id = %s" if marketplace_id else ""
        mkt_params: list[Any] = [marketplace_id] if marketplace_id else []

        # ── 1. Lost/damaged return items ──
        cur.execute(f"""
            SELECT
                ri.sku, ri.asin, ri.marketplace_id,
                ri.amazon_order_id, ri.financial_status,
                ri.cogs_pln, ri.refund_amount_pln,
                COUNT(*) AS quantity
            FROM dbo.acc_return_item ri WITH (NOLOCK)
            WHERE ri.financial_status IN ('damaged_return', 'lost_in_transit')
                AND ri.refund_date >= %s
                {mkt_filter}
                AND NOT EXISTS (
                    SELECT 1 FROM dbo.acc_reimbursement_case rc WITH (NOLOCK)
                    WHERE rc.sku = ri.sku
                        AND rc.marketplace_id = ri.marketplace_id
                        AND rc.amazon_order_id = ri.amazon_order_id
                )
            GROUP BY ri.sku, ri.asin, ri.marketplace_id,
                     ri.amazon_order_id, ri.financial_status,
                     ri.cogs_pln, ri.refund_amount_pln
        """, [cutoff] + mkt_params)
        eligible_rows = cur.fetchall()

        cases_created = 0
        for row in eligible_rows:
            sku, asin, mkt, order_id, fin_status, cogs, refund_amt, qty = row
            case_type = "lost_inventory" if fin_status == "lost_in_transit" else "customer_return_not_received"
            estimated_value = float(cogs or 0) * qty
            evidence = f"Return status: {fin_status}. COGS per unit: {cogs}. Refund: {refund_amt}."

            cur.execute("""
                INSERT INTO dbo.acc_reimbursement_case
                    (case_type, sku, asin, marketplace_id, amazon_order_id,
                     quantity, estimated_value_pln, evidence_summary)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (case_type, sku, asin, mkt, order_id, qty, estimated_value, evidence))
            cases_created += 1

        # ── 2. Damaged inbound shipment items ──
        mkt_filter_di = "AND si.marketplace_id = %s" if marketplace_id else ""
        cur.execute(f"""
            SELECT
                si.sku, si.asin, si.marketplace_id,
                si.shipment_id, si.quantity_damaged,
                ISNULL(p.cogs_pln, 0) AS cogs_pln
            FROM dbo.acc_fba_inbound_item si WITH (NOLOCK)
            LEFT JOIN dbo.acc_product p WITH (NOLOCK)
                ON p.sku = si.sku AND p.marketplace_id = si.marketplace_id
            WHERE si.quantity_damaged > 0
                AND si.received_date >= %s
                {mkt_filter_di}
                AND NOT EXISTS (
                    SELECT 1 FROM dbo.acc_reimbursement_case rc WITH (NOLOCK)
                    WHERE rc.sku = si.sku
                        AND rc.marketplace_id = si.marketplace_id
                        AND rc.case_type = 'damaged_inbound'
                        AND rc.evidence_summary LIKE '%%shipment:' + si.shipment_id + '%%'
                )
        """, [cutoff] + (mkt_params if marketplace_id else []))
        damaged_inbound_rows = cur.fetchall()

        for row in damaged_inbound_rows:
            sku, asin, mkt, shipment_id, qty_damaged, cogs = row
            estimated_value = float(cogs or 0) * int(qty_damaged or 0)
            evidence = f"Damaged inbound shipment:{shipment_id}. Qty damaged: {qty_damaged}. COGS/unit: {cogs}."

            cur.execute("""
                INSERT INTO dbo.acc_reimbursement_case
                    (case_type, sku, asin, marketplace_id,
                     quantity, estimated_value_pln, evidence_summary)
                VALUES ('damaged_inbound', %s, %s, %s, %s, %s, %s)
            """, (sku, asin, mkt, int(qty_damaged or 0), estimated_value, evidence))
            cases_created += 1

        # ── 3. Fee overcharge cases ──
        charge_placeholders = ", ".join(["%s"] * len(FEE_CHARGE_TYPES))
        mkt_filter_fee = "AND ft.marketplace_id = %s" if marketplace_id else ""
        mkt_params_fee: list[Any] = [marketplace_id] if marketplace_id else []

        cur.execute(f"""
            WITH fee_stats AS (
                SELECT
                    ft.sku,
                    MAX(p.asin) AS asin,
                    ft.marketplace_id,
                    AVG(ABS(ft.amount_pln)) AS avg_fee,
                    MAX(ABS(ft.amount_pln)) AS max_fee,
                    COUNT(*) AS fee_count,
                    STDEV(ABS(ft.amount_pln)) AS fee_stdev
                FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
                LEFT JOIN dbo.acc_product p WITH (NOLOCK)
                    ON p.sku = ft.sku AND p.marketplace_id = ft.marketplace_id
                WHERE ft.charge_type IN ({charge_placeholders})
                    AND ft.posted_date >= %s
                    {mkt_filter_fee}
                    AND ft.sku IS NOT NULL
                GROUP BY ft.sku, ft.marketplace_id
                HAVING COUNT(*) >= 5 AND STDEV(ABS(ft.amount_pln)) > 0
            )
            SELECT sku, asin, marketplace_id, avg_fee, max_fee, fee_count, fee_stdev
            FROM fee_stats
            WHERE max_fee > avg_fee + 2 * fee_stdev
                AND NOT EXISTS (
                    SELECT 1 FROM dbo.acc_reimbursement_case rc WITH (NOLOCK)
                    WHERE rc.sku = fee_stats.sku
                        AND rc.marketplace_id = fee_stats.marketplace_id
                        AND rc.case_type = 'fee_overcharge'
                        AND rc.created_at >= DATEADD(DAY, -7, SYSUTCDATETIME())
                )
        """, list(FEE_CHARGE_TYPES) + [cutoff] + mkt_params_fee)
        overcharge_rows = cur.fetchall()

        for row in overcharge_rows:
            sku, asin, mkt, avg_fee, max_fee, fee_count, fee_stdev = row
            estimated_overcharge = (float(max_fee) - float(avg_fee)) * int(fee_count)
            evidence = (
                f"Avg FBA fee: {avg_fee:.2f} PLN. Max fee: {max_fee:.2f} PLN. "
                f"Stdev: {fee_stdev:.2f}. Count: {fee_count}. "
                f"Estimated overcharge: {estimated_overcharge:.2f} PLN."
            )
            cur.execute("""
                INSERT INTO dbo.acc_reimbursement_case
                    (case_type, sku, asin, marketplace_id,
                     quantity, estimated_value_pln, evidence_summary)
                VALUES ('fee_overcharge', %s, %s, %s, %s, %s, %s)
            """, (sku, asin, mkt, fee_count, estimated_overcharge, evidence))
            cases_created += 1

        conn.commit()
        log.info("reimbursement.scan.done", cases_created=cases_created)
        return {
            "cases_created": cases_created,
            "items_scanned": len(eligible_rows) + len(damaged_inbound_rows) + len(overcharge_rows),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Anomaly CRUD / queries ───────────────────────────────────────────

def get_anomalies(
    *,
    anomaly_type: str | None = None,
    severity: str | None = None,
    status: str | None = None,
    marketplace_id: str | None = None,
    sku: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """List refund anomalies with filters."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        where: list[str] = []
        params: list[Any] = []
        if anomaly_type:
            where.append("anomaly_type = %s")
            params.append(anomaly_type)
        if severity:
            where.append("severity = %s")
            params.append(severity)
        if status:
            where.append("status = %s")
            params.append(status)
        if marketplace_id:
            where.append("marketplace_id = %s")
            params.append(marketplace_id)
        if sku:
            where.append("sku = %s")
            params.append(sku)

        where_sql = " AND ".join(where) if where else "1=1"

        cur.execute(f"SELECT COUNT(*) FROM dbo.acc_refund_anomaly WITH (NOLOCK) WHERE {where_sql}", params)
        total = cur.fetchone()[0] or 0

        cur.execute(f"""
            {_ANOMALY_SELECT}
            WHERE {where_sql}
            ORDER BY detection_date DESC, severity
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
        """, params + [offset, limit])
        items = [_anomaly_row_to_dict(r) for r in cur.fetchall()]
        return {"items": items, "total": total, "limit": limit, "offset": offset}
    finally:
        conn.close()


def update_anomaly_status(
    anomaly_id: int,
    *,
    status: str,
    resolution_note: str | None = None,
    resolved_by: str | None = None,
) -> dict[str, Any]:
    """Update anomaly status (e.g., investigating → resolved)."""
    if status not in ANOMALY_STATUSES:
        raise ValueError(f"Invalid status '{status}'. Must be one of: {sorted(ANOMALY_STATUSES)}")
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        resolved_at = "SYSUTCDATETIME()" if status in ("resolved", "dismissed") else "NULL"
        cur.execute(f"""
            UPDATE dbo.acc_refund_anomaly
            SET status = %s, resolution_note = %s, resolved_by = %s,
                resolved_at = {resolved_at}, updated_at = SYSUTCDATETIME()
            WHERE id = %s
        """, (status, resolution_note, resolved_by, anomaly_id))
        conn.commit()
        return {"id": anomaly_id, "status": status, "updated": True}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Serial returner queries ──────────────────────────────────────────

_RETURNER_SELECT = """
    SELECT id, buyer_identifier, marketplace_id, detection_date,
           return_count, order_count, return_rate,
           total_refund_pln, avg_refund_pln,
           first_return_date, last_return_date, top_skus,
           risk_score, risk_tier, status, notes,
           created_at, updated_at
    FROM dbo.acc_serial_returner WITH (NOLOCK)
"""


def get_serial_returners(
    *,
    risk_tier: str | None = None,
    status: str | None = None,
    marketplace_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """List serial returners with filters."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        where: list[str] = []
        params: list[Any] = []
        if risk_tier:
            where.append("risk_tier = %s")
            params.append(risk_tier)
        if status:
            where.append("status = %s")
            params.append(status)
        if marketplace_id:
            where.append("marketplace_id = %s")
            params.append(marketplace_id)

        where_sql = " AND ".join(where) if where else "1=1"

        cur.execute(f"SELECT COUNT(*) FROM dbo.acc_serial_returner WITH (NOLOCK) WHERE {where_sql}", params)
        total = cur.fetchone()[0] or 0

        cur.execute(f"""
            {_RETURNER_SELECT}
            WHERE {where_sql}
            ORDER BY risk_score DESC
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
        """, params + [offset, limit])
        items = [_returner_row_to_dict(r) for r in cur.fetchall()]
        return {"items": items, "total": total, "limit": limit, "offset": offset}
    finally:
        conn.close()


def update_returner_status(
    returner_id: int,
    *,
    status: str,
    notes: str | None = None,
) -> dict[str, Any]:
    """Update serial returner status."""
    if status not in RETURNER_STATUSES:
        raise ValueError(f"Invalid status '{status}'. Must be one of: {sorted(RETURNER_STATUSES)}")
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE dbo.acc_serial_returner
            SET status = %s, notes = %s, updated_at = SYSUTCDATETIME()
            WHERE id = %s
        """, (status, notes, returner_id))
        conn.commit()
        return {"id": returner_id, "status": status, "updated": True}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Reimbursement case queries ───────────────────────────────────────

_CASE_SELECT = """
    SELECT id, case_type, sku, asin, marketplace_id,
           amazon_order_id, fnsku, quantity, estimated_value_pln,
           evidence_summary, amazon_case_id, status,
           filed_at, resolved_at, reimbursed_amount_pln,
           resolution_note, created_at
    FROM dbo.acc_reimbursement_case WITH (NOLOCK)
"""


def get_reimbursement_cases(
    *,
    case_type: str | None = None,
    status: str | None = None,
    marketplace_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """List reimbursement cases with filters."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        where: list[str] = []
        params: list[Any] = []
        if case_type:
            where.append("case_type = %s")
            params.append(case_type)
        if status:
            where.append("status = %s")
            params.append(status)
        if marketplace_id:
            where.append("marketplace_id = %s")
            params.append(marketplace_id)

        where_sql = " AND ".join(where) if where else "1=1"

        cur.execute(f"SELECT COUNT(*) FROM dbo.acc_reimbursement_case WITH (NOLOCK) WHERE {where_sql}", params)
        total = cur.fetchone()[0] or 0

        cur.execute(f"""
            {_CASE_SELECT}
            WHERE {where_sql}
            ORDER BY created_at DESC
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
        """, params + [offset, limit])
        items = [_case_row_to_dict(r) for r in cur.fetchall()]
        return {"items": items, "total": total, "limit": limit, "offset": offset}
    finally:
        conn.close()


def update_case_status(
    case_id: int,
    *,
    status: str,
    amazon_case_id: str | None = None,
    reimbursed_amount_pln: float | None = None,
    resolution_note: str | None = None,
) -> dict[str, Any]:
    """Update reimbursement case status."""
    if status not in CASE_STATUSES:
        raise ValueError(f"Invalid status '{status}'. Must be one of: {sorted(CASE_STATUSES)}")
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        sets = ["status = %s", "updated_at = SYSUTCDATETIME()"]
        params: list[Any] = [status]
        if amazon_case_id is not None:
            sets.append("amazon_case_id = %s")
            params.append(amazon_case_id)
        if reimbursed_amount_pln is not None:
            sets.append("reimbursed_amount_pln = %s")
            params.append(reimbursed_amount_pln)
        if resolution_note is not None:
            sets.append("resolution_note = %s")
            params.append(resolution_note)
        if status == "filed":
            sets.append("filed_at = SYSYTCDATETIME()" if False else "filed_at = SYSUTCDATETIME()")
        if status in ("accepted", "rejected", "paid"):
            sets.append("resolved_at = SYSUTCDATETIME()")
        params.append(case_id)
        cur.execute(f"UPDATE dbo.acc_reimbursement_case SET {', '.join(sets)} WHERE id = %s", params)
        conn.commit()
        return {"id": case_id, "status": status, "updated": True}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Dashboard / summary ──────────────────────────────────────────────

def get_anomaly_dashboard() -> dict[str, Any]:
    """Get anomaly engine dashboard KPIs."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()

        # Anomaly summary
        cur.execute("""
            SELECT
                COUNT(*) AS total_anomalies,
                SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_anomalies,
                SUM(CASE WHEN severity = 'critical' AND status = 'open' THEN 1 ELSE 0 END) AS critical_open,
                SUM(CASE WHEN severity = 'high' AND status = 'open' THEN 1 ELSE 0 END) AS high_open,
                SUM(estimated_loss_pln) AS total_estimated_loss,
                SUM(CASE WHEN status = 'open' THEN estimated_loss_pln ELSE 0 END) AS open_estimated_loss
            FROM dbo.acc_refund_anomaly WITH (NOLOCK)
        """)
        anom_row = cur.fetchone()

        # Serial returner summary
        cur.execute("""
            SELECT
                COUNT(*) AS total_returners,
                SUM(CASE WHEN risk_tier = 'critical' THEN 1 ELSE 0 END) AS critical_returners,
                SUM(CASE WHEN risk_tier = 'high' THEN 1 ELSE 0 END) AS high_returners,
                SUM(total_refund_pln) AS total_refund_exposure
            FROM dbo.acc_serial_returner WITH (NOLOCK)
            WHERE status IN ('flagged', 'monitoring')
        """)
        ret_row = cur.fetchone()

        # Reimbursement summary
        cur.execute("""
            SELECT
                COUNT(*) AS total_cases,
                SUM(CASE WHEN status = 'identified' THEN 1 ELSE 0 END) AS pending_cases,
                SUM(CASE WHEN status = 'filed' THEN 1 ELSE 0 END) AS filed_cases,
                SUM(CASE WHEN status = 'paid' THEN 1 ELSE 0 END) AS paid_cases,
                SUM(estimated_value_pln) AS total_estimated_value,
                ISNULL(SUM(reimbursed_amount_pln), 0) AS total_reimbursed
            FROM dbo.acc_reimbursement_case WITH (NOLOCK)
        """)
        case_row = cur.fetchone()

        return {
            "anomalies": {
                "total": anom_row[0] or 0 if anom_row else 0,
                "open": anom_row[1] or 0 if anom_row else 0,
                "critical_open": anom_row[2] or 0 if anom_row else 0,
                "high_open": anom_row[3] or 0 if anom_row else 0,
                "total_estimated_loss_pln": round(float(anom_row[4] or 0), 2) if anom_row else 0,
                "open_estimated_loss_pln": round(float(anom_row[5] or 0), 2) if anom_row else 0,
            },
            "serial_returners": {
                "total_active": ret_row[0] or 0 if ret_row else 0,
                "critical": ret_row[1] or 0 if ret_row else 0,
                "high": ret_row[2] or 0 if ret_row else 0,
                "total_refund_exposure_pln": round(float(ret_row[3] or 0), 2) if ret_row else 0,
            },
            "reimbursements": {
                "total_cases": case_row[0] or 0 if case_row else 0,
                "pending": case_row[1] or 0 if case_row else 0,
                "filed": case_row[2] or 0 if case_row else 0,
                "paid": case_row[3] or 0 if case_row else 0,
                "total_estimated_value_pln": round(float(case_row[4] or 0), 2) if case_row else 0,
                "total_reimbursed_pln": round(float(case_row[5] or 0), 2) if case_row else 0,
            },
        }
    finally:
        conn.close()


def run_full_scan(*, marketplace_id: str | None = None) -> dict[str, Any]:
    """Run complete anomaly detection: all spike types + returners + reimbursement scan."""
    results: dict[str, Any] = {}
    try:
        results["refund_spikes"] = detect_refund_spikes(marketplace_id=marketplace_id)
    except Exception as exc:
        log.error("scan.refund_spikes.failed", error=str(exc))
        results["refund_spikes"] = {"error": str(exc)}

    try:
        results["fee_spikes"] = detect_fee_spikes(marketplace_id=marketplace_id)
    except Exception as exc:
        log.error("scan.fee_spikes.failed", error=str(exc))
        results["fee_spikes"] = {"error": str(exc)}

    try:
        results["return_rate_spikes"] = detect_return_rate_spikes(marketplace_id=marketplace_id)
    except Exception as exc:
        log.error("scan.return_rate_spikes.failed", error=str(exc))
        results["return_rate_spikes"] = {"error": str(exc)}

    try:
        results["serial_returners"] = detect_serial_returners(marketplace_id=marketplace_id)
    except Exception as exc:
        log.error("scan.serial_returners.failed", error=str(exc))
        results["serial_returners"] = {"error": str(exc)}

    try:
        results["reimbursement"] = scan_reimbursement_opportunities(marketplace_id=marketplace_id)
    except Exception as exc:
        log.error("scan.reimbursement.failed", error=str(exc))
        results["reimbursement"] = {"error": str(exc)}

    return results


# ── Detail lookups ───────────────────────────────────────────────────

def get_anomaly_by_id(anomaly_id: int) -> dict[str, Any] | None:
    """Get a single anomaly by ID."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute(f"{_ANOMALY_SELECT} WHERE id = %s", [anomaly_id])
        row = cur.fetchone()
        return _anomaly_row_to_dict(row) if row else None
    finally:
        conn.close()


def get_returner_by_id(returner_id: int) -> dict[str, Any] | None:
    """Get a single serial returner by ID."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute(f"{_RETURNER_SELECT} WHERE id = %s", [returner_id])
        row = cur.fetchone()
        return _returner_row_to_dict(row) if row else None
    finally:
        conn.close()


def get_case_by_id(case_id: int) -> dict[str, Any] | None:
    """Get a single reimbursement case by ID."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute(f"{_CASE_SELECT} WHERE id = %s", [case_id])
        row = cur.fetchone()
        return _case_row_to_dict(row) if row else None
    finally:
        conn.close()


# ── Trend / history queries ──────────────────────────────────────────

def get_anomaly_trends(
    *,
    days: int = 90,
    anomaly_type: str | None = None,
    marketplace_id: str | None = None,
) -> list[dict[str, Any]]:
    """Get anomaly count grouped by week for trend charts."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        since = date.today() - timedelta(days=days)
        where = ["detection_date >= %s"]
        params: list[Any] = [since]
        if anomaly_type:
            where.append("anomaly_type = %s")
            params.append(anomaly_type)
        if marketplace_id:
            where.append("marketplace_id = %s")
            params.append(marketplace_id)
        where_sql = " AND ".join(where)

        cur.execute(f"""
            SELECT
                DATEADD(WEEK, DATEDIFF(WEEK, 0, detection_date), 0) AS week_start,
                anomaly_type,
                COUNT(*) AS count,
                SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_count,
                SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) AS high_count,
                SUM(estimated_loss_pln) AS total_loss
            FROM dbo.acc_refund_anomaly WITH (NOLOCK)
            WHERE {where_sql}
            GROUP BY
                DATEADD(WEEK, DATEDIFF(WEEK, 0, detection_date), 0),
                anomaly_type
            ORDER BY week_start
        """, params)

        return [
            {
                "week_start": str(r[0]) if r[0] else None,
                "anomaly_type": r[1],
                "count": r[2] or 0,
                "critical_count": r[3] or 0,
                "high_count": r[4] or 0,
                "total_loss_pln": round(float(r[5] or 0), 2),
            }
            for r in cur.fetchall()
        ]
    finally:
        conn.close()


# ── CSV export helpers ───────────────────────────────────────────────

def export_anomalies_csv(
    *,
    anomaly_type: str | None = None,
    severity: str | None = None,
    status: str | None = None,
    marketplace_id: str | None = None,
) -> list[dict[str, Any]]:
    """Export all matching anomalies as list of dicts (for CSV serialization)."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        where: list[str] = []
        params: list[Any] = []
        if anomaly_type:
            where.append("anomaly_type = %s")
            params.append(anomaly_type)
        if severity:
            where.append("severity = %s")
            params.append(severity)
        if status:
            where.append("status = %s")
            params.append(status)
        if marketplace_id:
            where.append("marketplace_id = %s")
            params.append(marketplace_id)

        where_sql = " AND ".join(where) if where else "1=1"
        cur.execute(f"{_ANOMALY_SELECT} WHERE {where_sql} ORDER BY detection_date DESC", params)
        return [_anomaly_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def export_returners_csv(
    *,
    risk_tier: str | None = None,
    status: str | None = None,
    marketplace_id: str | None = None,
) -> list[dict[str, Any]]:
    """Export all matching serial returners as list of dicts."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        where: list[str] = []
        params: list[Any] = []
        if risk_tier:
            where.append("risk_tier = %s")
            params.append(risk_tier)
        if status:
            where.append("status = %s")
            params.append(status)
        if marketplace_id:
            where.append("marketplace_id = %s")
            params.append(marketplace_id)

        where_sql = " AND ".join(where) if where else "1=1"
        cur.execute(f"{_RETURNER_SELECT} WHERE {where_sql} ORDER BY risk_score DESC", params)
        return [_returner_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def export_cases_csv(
    *,
    case_type: str | None = None,
    status: str | None = None,
    marketplace_id: str | None = None,
) -> list[dict[str, Any]]:
    """Export all matching reimbursement cases as list of dicts."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        where: list[str] = []
        params: list[Any] = []
        if case_type:
            where.append("case_type = %s")
            params.append(case_type)
        if status:
            where.append("status = %s")
            params.append(status)
        if marketplace_id:
            where.append("marketplace_id = %s")
            params.append(marketplace_id)

        where_sql = " AND ".join(where) if where else "1=1"
        cur.execute(f"{_CASE_SELECT} WHERE {where_sql} ORDER BY created_at DESC", params)
        return [_case_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
