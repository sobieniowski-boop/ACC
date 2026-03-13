"""
Order pipeline — runs every 15 min.

Steps:
  1. Fetch recent orders from SP-API (raw pyodbc to avoid MARS/hstmt)
     - Use LastUpdatedAfter (30-min window) to catch new + changed orders
     - Re-fetch items for orders whose last_update_date changed
     - Inline COGS stamp when product price is available
  2. Backfill missing acc_product rows from new SKU/ASIN combos
  3. Link acc_order_line.product_id where NULL
  4. Map internal_sku for unmapped products (Ergonode → GSheet → Baselinker → ASIN)
  5. Stamp purchase_price_pln + cogs_pln on unstamped order lines
  5.8  Sync FX rates + Finance transactions
  5.9  Bridge finance fees → order line fee columns
  5.95 Sync courier costs for FBM (DHL / GLS → logistics_pln)
  6. Recalculate profit / contribution margin

Uses raw pyodbc for DB writes (compatible with old "SQL Server" ODBC driver).
SP-API calls use the existing async OrdersClient.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import pyodbc
import structlog

from app.core.config import settings, MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc
from app.services.amazon_listing_registry import lookup_listing_registry_context

log = structlog.get_logger(__name__)

# Statuses we fetch from SP-API.  Canceled included to track cancellations.
ORDER_STATUSES = ["Shipped", "Unshipped", "PartiallyShipped", "Canceled"]

# How far back LastUpdatedAfter looks (2× the 15-min schedule for safety)
UPDATE_WINDOW_MINUTES = 30
ORDER_SYNC_OVERLAP_MINUTES = 15
ORDER_SYNC_SAFETY_LAG_MINUTES = 2
ORDER_SYNC_INITIAL_LOOKBACK_HOURS = 24
ORDER_SYNC_GAP_ALERT_MINUTES = 45

ORDER_SYNC_PROFILES: dict[str, dict[str, object]] = {
    # Main production profile for finance/profit/order-line completeness.
    "core_sync": {
        "fetch_items": True,
        "statuses": ORDER_STATUSES,
    },
    # Lightweight operational profile; keeps headers fresh without item calls.
    "ops_tracking": {
        "fetch_items": False,
        "statuses": ORDER_STATUSES,
    },
    # Reserved for explicit support workflows requiring PII-related follow-ups.
    "pii_support": {
        "fetch_items": False,
        "statuses": ORDER_STATUSES,
    },
}


# Safety: 120s query timeout prevents runaway queries from blocking SQL
QUERY_TIMEOUT_SECONDS = 120
ORDER_SYNC_COMMIT_EVERY = 5


def _db_conn(timeout: int = QUERY_TIMEOUT_SECONDS):
    conn = connect_acc(autocommit=False, timeout=timeout)
    conn.timeout = timeout
    return conn


def _db_conn_finance(timeout: int = QUERY_TIMEOUT_SECONDS):
    """Connection for finance writes — explicit READ COMMITTED isolation."""
    conn = connect_acc(autocommit=False, timeout=timeout, isolation_level="READ COMMITTED")
    conn.timeout = timeout
    return conn


def _clean_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _ensure_order_sync_state_schema(cur) -> None:
    cur.execute(
        """
IF OBJECT_ID('dbo.acc_order_sync_state', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_order_sync_state (
        marketplace_id NVARCHAR(32) NOT NULL PRIMARY KEY,
        last_mode NVARCHAR(32) NULL,
        last_status NVARCHAR(20) NULL,
        last_started_at DATETIME2 NULL,
        last_finished_at DATETIME2 NULL,
        last_window_from DATETIME2 NULL,
        last_window_to DATETIME2 NULL,
        last_successful_window_from DATETIME2 NULL,
        last_successful_window_to DATETIME2 NULL,
        last_orders_count INT NOT NULL DEFAULT 0,
        last_error NVARCHAR(2000) NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END

IF COL_LENGTH('dbo.acc_order', 'sync_payload_hash') IS NULL
BEGIN
    ALTER TABLE dbo.acc_order ADD sync_payload_hash NVARCHAR(64) NULL;
END

IF COL_LENGTH('dbo.acc_order', 'shipping_surcharge_pln') IS NULL
BEGIN
    ALTER TABLE dbo.acc_order ADD shipping_surcharge_pln DECIMAL(18,4) NULL;
END

IF COL_LENGTH('dbo.acc_order', 'promo_order_fee_pln') IS NULL
BEGIN
    ALTER TABLE dbo.acc_order ADD promo_order_fee_pln DECIMAL(18,4) NULL;
END

IF COL_LENGTH('dbo.acc_order', 'refund_commission_pln') IS NULL
BEGIN
    ALTER TABLE dbo.acc_order ADD refund_commission_pln DECIMAL(18,4) NULL;
END
        """
    )


def _as_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None) is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _load_order_sync_state(cur, marketplace_id: str) -> dict[str, object] | None:
    cur.execute(
        """
        SELECT
            marketplace_id,
            last_mode,
            last_status,
            last_started_at,
            last_finished_at,
            last_window_from,
            last_window_to,
            last_successful_window_from,
            last_successful_window_to,
            last_orders_count,
            last_error
        FROM dbo.acc_order_sync_state WITH (NOLOCK)
        WHERE marketplace_id = ?
        """,
        (marketplace_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "marketplace_id": str(row[0] or ""),
        "last_mode": str(row[1] or ""),
        "last_status": str(row[2] or ""),
        "last_started_at": row[3],
        "last_finished_at": row[4],
        "last_window_from": row[5],
        "last_window_to": row[6],
        "last_successful_window_from": row[7],
        "last_successful_window_to": row[8],
        "last_orders_count": int(row[9] or 0),
        "last_error": str(row[10] or "") or None,
    }


def _upsert_order_sync_state(
    cur,
    *,
    marketplace_id: str,
    last_mode: str,
    last_status: str,
    last_started_at: datetime | None,
    last_finished_at: datetime | None,
    last_window_from: datetime | None,
    last_window_to: datetime | None,
    last_successful_window_from: datetime | None,
    last_successful_window_to: datetime | None,
    last_orders_count: int,
    last_error: str | None,
) -> None:
    cur.execute(
        """
        MERGE dbo.acc_order_sync_state AS tgt
        USING (SELECT ? AS marketplace_id) AS src
        ON tgt.marketplace_id = src.marketplace_id
        WHEN MATCHED THEN
            UPDATE SET
                last_mode = ?,
                last_status = ?,
                last_started_at = ?,
                last_finished_at = ?,
                last_window_from = ?,
                last_window_to = ?,
                last_successful_window_from = ?,
                last_successful_window_to = ?,
                last_orders_count = ?,
                last_error = ?,
                updated_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (
                marketplace_id, last_mode, last_status, last_started_at, last_finished_at,
                last_window_from, last_window_to, last_successful_window_from, last_successful_window_to,
                last_orders_count, last_error, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
        """,
        (
            marketplace_id,
            last_mode,
            last_status,
            last_started_at,
            last_finished_at,
            last_window_from,
            last_window_to,
            last_successful_window_from,
            last_successful_window_to,
            last_orders_count,
            last_error,
            marketplace_id,
            last_mode,
            last_status,
            last_started_at,
            last_finished_at,
            last_window_from,
            last_window_to,
            last_successful_window_from,
            last_successful_window_to,
            last_orders_count,
            last_error,
        ),
    )


def _compute_incremental_order_window(cur, marketplace_id: str, now_utc: datetime) -> tuple[datetime, datetime, dict[str, object] | None]:
    state = _load_order_sync_state(cur, marketplace_id)
    upper_bound = now_utc - timedelta(minutes=ORDER_SYNC_SAFETY_LAG_MINUTES)
    overlap = timedelta(minutes=ORDER_SYNC_OVERLAP_MINUTES)
    last_success_to = _as_utc(state.get("last_successful_window_to")) if state else None
    if last_success_to:
        window_from = last_success_to - overlap
    else:
        window_from = upper_bound - timedelta(hours=ORDER_SYNC_INITIAL_LOOKBACK_HOURS)
    if window_from >= upper_bound:
        window_from = upper_bound - overlap
    return window_from, upper_bound, state


def _collect_order_sync_health(stale_minutes: int = ORDER_SYNC_GAP_ALERT_MINUTES) -> dict[str, object]:
    conn = _db_conn()
    try:
        cur = conn.cursor()
        _ensure_order_sync_state_schema(cur)
        cur.execute(
            """
            SELECT
                marketplace_id,
                last_status,
                last_finished_at,
                last_successful_window_from,
                last_successful_window_to,
                last_orders_count,
                last_error
            FROM dbo.acc_order_sync_state WITH (NOLOCK)
            ORDER BY marketplace_id
            """
        )
        now_utc = datetime.now(timezone.utc)
        items = []
        overall_status = "healthy"
        for row in cur.fetchall():
            marketplace_id = str(row[0] or "")
            last_status = str(row[1] or "")
            last_finished_at = _as_utc(row[2])
            gap_minutes = None
            if last_finished_at:
                gap_minutes = round((now_utc - last_finished_at).total_seconds() / 60.0, 1)
            status = "ok"
            if last_status.lower() != "success":
                status = "failed"
            elif gap_minutes is None or gap_minutes > stale_minutes:
                status = "gap"
            if status != "ok":
                overall_status = "degraded"
            items.append(
                {
                    "marketplace_id": marketplace_id,
                    "marketplace_code": MARKETPLACE_REGISTRY.get(marketplace_id, {}).get("code", marketplace_id),
                    "status": status,
                    "gap_minutes": gap_minutes,
                    "last_finished_at": row[2],
                    "last_successful_window_from": row[3],
                    "last_successful_window_to": row[4],
                    "last_orders_count": int(row[5] or 0),
                    "last_error": str(row[6] or "") or None,
                }
            )
        return {
            "status": overall_status,
            "stale_minutes": stale_minutes,
            "items": items,
        }
    finally:
        conn.close()


def evaluate_order_sync_gap_alerts(stale_minutes: int = ORDER_SYNC_GAP_ALERT_MINUTES) -> dict[str, int | str]:
    from app.connectors.mssql import ensure_v2_schema

    ensure_v2_schema()
    health = _collect_order_sync_health(stale_minutes=stale_minutes)
    degraded = [item for item in health.get("items", []) if str(item.get("status")) != "ok"]
    conn = _db_conn()
    created = 0
    updated = 0
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 1 CAST(id AS NVARCHAR(40))
            FROM dbo.acc_al_alert_rules WITH (NOLOCK)
            WHERE name = ? AND rule_type = ?
            ORDER BY created_at DESC
            """,
            ("Order sync gap", "system_health"),
        )
        row = cur.fetchone()
        if row:
            rule_id = str(row[0])
        else:
            rule_id = str(uuid.uuid4())
            cur.execute(
                """
                INSERT INTO dbo.acc_al_alert_rules
                (
                    id, name, description, rule_type, severity, is_active, created_by
                )
                VALUES
                (
                    CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, 1, ?
                )
                """,
                (
                    rule_id,
                    "Order sync gap",
                    "Order sync last-success watermark is older than the accepted threshold.",
                    "system_health",
                    "warning",
                    settings.DEFAULT_ACTOR,
                ),
            )

        def _upsert_alert(marketplace_id: str, title: str, detail: str, current_value: float, context_json: str) -> None:
            nonlocal created, updated
            cur.execute(
                """
                SELECT TOP 1 CAST(id AS NVARCHAR(40))
                FROM dbo.acc_al_alerts WITH (UPDLOCK, ROWLOCK)
                WHERE rule_id = CAST(? AS UNIQUEIDENTIFIER)
                  AND marketplace_id = ?
                  AND is_resolved = 0
                ORDER BY triggered_at DESC
                """,
                (rule_id, marketplace_id),
            )
            alert_row = cur.fetchone()
            if alert_row:
                cur.execute(
                    """
                    UPDATE dbo.acc_al_alerts
                    SET title = ?, detail = ?, severity = ?, current_value = ?, context_json = ?, triggered_at = SYSUTCDATETIME()
                    WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                    """,
                    (title, detail, "warning", current_value, context_json, str(alert_row[0])),
                )
                updated += 1
            else:
                cur.execute(
                    """
                    INSERT INTO dbo.acc_al_alerts
                    (
                        id, rule_id, marketplace_id, title, detail, severity, current_value, context_json
                    )
                    VALUES
                    (
                        CAST(? AS UNIQUEIDENTIFIER), CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?
                    )
                    """,
                    (str(uuid.uuid4()), rule_id, marketplace_id, title, detail, "warning", current_value, context_json),
                )
                created += 1

        active_marketplaces = {str(item.get("marketplace_id")) for item in degraded if item.get("marketplace_id")}
        for item in degraded:
            marketplace_id = str(item.get("marketplace_id") or "")
            marketplace_code = str(item.get("marketplace_code") or marketplace_id)
            gap_minutes = float(item.get("gap_minutes") or 0.0)
            title = f"Order sync gap: {marketplace_code}"
            detail = (
                f"Last successful order sync for {marketplace_code} is older than {stale_minutes} min "
                f"(current gap {gap_minutes:.1f} min). Fresh finance rows may not match acc_order yet."
            )
            context = json.dumps(
                {
                    "source": "order_sync_gap",
                    "route": "/finance/dashboard",
                    "marketplace_id": marketplace_id,
                    "marketplace_code": marketplace_code,
                    "gap_minutes": gap_minutes,
                    "last_finished_at": item.get("last_finished_at").isoformat() if hasattr(item.get("last_finished_at"), "isoformat") else item.get("last_finished_at"),
                    "window_to": item.get("last_successful_window_to").isoformat() if hasattr(item.get("last_successful_window_to"), "isoformat") else item.get("last_successful_window_to"),
                },
                ensure_ascii=True,
            )
            _upsert_alert(marketplace_id, title, detail, gap_minutes, context)

        cur.execute(
            """
            SELECT CAST(id AS NVARCHAR(40)), marketplace_id
            FROM dbo.acc_al_alerts WITH (UPDLOCK, ROWLOCK)
            WHERE rule_id = CAST(? AS UNIQUEIDENTIFIER)
              AND is_resolved = 0
            """,
            (rule_id,),
        )
        for alert_id, marketplace_id in cur.fetchall():
            if str(marketplace_id or "") not in active_marketplaces:
                cur.execute(
                    """
                    UPDATE dbo.acc_al_alerts
                    SET is_resolved = 1, resolved_at = SYSUTCDATETIME()
                    WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                    """,
                    (str(alert_id),),
                )
        conn.commit()
        return {"status": str(health.get("status") or "unknown"), "created": created, "updated": updated}
    finally:
        conn.close()


def _ensure_finance_group_sync_schema(cur) -> None:
    cur.execute(
        """
IF OBJECT_ID('dbo.acc_fin_event_group_sync', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fin_event_group_sync (
        financial_event_group_id NVARCHAR(120) NOT NULL PRIMARY KEY,
        marketplace_id NVARCHAR(32) NULL,
        processing_status NVARCHAR(40) NULL,
        fund_transfer_status NVARCHAR(40) NULL,
        group_start DATETIME2 NULL,
        group_end DATETIME2 NULL,
        original_currency NVARCHAR(8) NULL,
        original_amount DECIMAL(18,4) NULL,
        last_row_count INT NOT NULL DEFAULT 0,
        event_type_counts_json NVARCHAR(MAX) NULL,
        payload_signature NVARCHAR(64) NULL,
        first_posted_at DATETIME2 NULL,
        last_posted_at DATETIME2 NULL,
        open_refresh_after DATETIME2 NULL,
        last_synced_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END

IF COL_LENGTH('dbo.acc_fin_event_group_sync', 'event_type_counts_json') IS NULL
BEGIN
    ALTER TABLE dbo.acc_fin_event_group_sync ADD event_type_counts_json NVARCHAR(MAX) NULL;
END

IF COL_LENGTH('dbo.acc_fin_event_group_sync', 'payload_signature') IS NULL
BEGIN
    ALTER TABLE dbo.acc_fin_event_group_sync ADD payload_signature NVARCHAR(64) NULL;
END

IF COL_LENGTH('dbo.acc_fin_event_group_sync', 'first_posted_at') IS NULL
BEGIN
    ALTER TABLE dbo.acc_fin_event_group_sync ADD first_posted_at DATETIME2 NULL;
END

IF COL_LENGTH('dbo.acc_fin_event_group_sync', 'last_posted_at') IS NULL
BEGIN
    ALTER TABLE dbo.acc_fin_event_group_sync ADD last_posted_at DATETIME2 NULL;
END

IF COL_LENGTH('dbo.acc_fin_event_group_sync', 'open_refresh_after') IS NULL
BEGIN
    ALTER TABLE dbo.acc_fin_event_group_sync ADD open_refresh_after DATETIME2 NULL;
END

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_acc_fin_event_group_sync_open_refresh'
      AND object_id = OBJECT_ID('dbo.acc_fin_event_group_sync')
)
BEGIN
    CREATE INDEX IX_acc_fin_event_group_sync_open_refresh
    ON dbo.acc_fin_event_group_sync(marketplace_id, open_refresh_after, last_synced_at);
END
        """
    )


def _map_marketplace_name_to_id(name: str | None) -> str | None:
    if not name:
        return None
    normalized = str(name).strip().lower()
    for mkt_id, info in MARKETPLACE_REGISTRY.items():
        full_name = str(info.get("name") or "").strip().lower()
        code = str(info.get("code") or "").strip().lower()
        if normalized == full_name:
            return mkt_id
        if normalized.endswith(f".{code}") or normalized == f"amazon.{code}":
            return mkt_id
    return None


def _infer_legacy_marketplace_id(legacy_events: dict[str, list], default_marketplace_id: str | None = None) -> str | None:
    for event_list in legacy_events.values():
        for event in event_list[:10]:
            inferred = _map_marketplace_name_to_id(event.get("MarketplaceName") or event.get("StoreName"))
            if inferred:
                return inferred
    return default_marketplace_id


def _should_resync_group(
    cur,
    group_id: str,
    processing_status: str | None,
    fund_transfer_status: str | None,
    group_end: str | None,
    payload_signature: str | None = None,
    cooldown_minutes: int = 15,
) -> bool:
    cur.execute(
        """
        SELECT processing_status, fund_transfer_status, group_end, last_synced_at, payload_signature, open_refresh_after
        FROM dbo.acc_fin_event_group_sync WITH (NOLOCK)
        WHERE financial_event_group_id = ?
        """,
        (group_id,),
    )
    row = cur.fetchone()
    if not row:
        return True
    old_processing = str(row[0] or "")
    old_fund = str(row[1] or "")
    old_group_end = row[2]
    last_synced_at = row[3]
    old_signature = str(row[4] or "")
    open_refresh_after = row[5]
    new_processing = str(processing_status or "")
    new_fund = str(fund_transfer_status or "")
    new_group_end = str(group_end or "")
    old_group_end_str = old_group_end.isoformat() if hasattr(old_group_end, "isoformat") else str(old_group_end or "")
    is_terminal = new_processing.lower() == "closed" and new_fund.lower() in {"succeeded", "transferred", "transfered"}
    is_unchanged = old_processing == new_processing and old_fund == new_fund and old_group_end_str == new_group_end
    if not is_unchanged:
        return True
    if payload_signature is not None and old_signature and old_signature != str(payload_signature):
        return True
    if is_terminal:
        return False
    if open_refresh_after and hasattr(open_refresh_after, "__sub__"):
        try:
            if open_refresh_after > datetime.now(timezone.utc):
                return False
        except Exception:
            pass
    if last_synced_at and hasattr(last_synced_at, "__sub__"):
        try:
            recent_cutoff = datetime.now(timezone.utc) - timedelta(minutes=cooldown_minutes)
            if last_synced_at >= recent_cutoff:
                return False
        except Exception:
            pass
    return True


def _get_group_sync_snapshot(cur, group_id: str) -> dict[str, object] | None:
    cur.execute(
        """
        SELECT
            marketplace_id,
            processing_status,
            fund_transfer_status,
            group_end,
            last_synced_at,
            payload_signature,
            open_refresh_after,
            last_row_count
        FROM dbo.acc_fin_event_group_sync WITH (NOLOCK)
        WHERE financial_event_group_id = ?
        """,
        (group_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "marketplace_id": str(row[0] or "") or None,
        "processing_status": str(row[1] or ""),
        "fund_transfer_status": str(row[2] or ""),
        "group_end": row[3],
        "last_synced_at": row[4],
        "payload_signature": str(row[5] or ""),
        "open_refresh_after": row[6],
        "last_row_count": int(row[7] or 0),
    }


def _get_finance_group_persisted_row_count(cur, group_id: str) -> int:
    cur.execute(
        """
        SELECT COUNT(*)
        FROM dbo.acc_finance_transaction WITH (NOLOCK)
        WHERE financial_event_group_id = ? OR settlement_id = ?
        """,
        (group_id, group_id),
    )
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def _load_order_marketplace_map(cur, amazon_order_ids: list[str]) -> dict[str, str]:
    if not amazon_order_ids:
        return {}
    unique_ids = [str(item).strip() for item in amazon_order_ids if str(item).strip()]
    if not unique_ids:
        return {}
    mapping: dict[str, str] = {}
    for offset in range(0, len(unique_ids), 200):
        batch = unique_ids[offset: offset + 200]
        placeholders = ",".join("?" for _ in batch)
        cur.execute(
            f"""
            SELECT amazon_order_id, marketplace_id
            FROM dbo.acc_order WITH (NOLOCK)
            WHERE amazon_order_id IN ({placeholders})
            """,
            tuple(batch),
        )
        for row in cur.fetchall():
            order_id = str(row[0] or "").strip()
            marketplace_id = str(row[1] or "").strip()
            if order_id and marketplace_id:
                mapping[order_id] = marketplace_id
    return mapping


def _compute_open_refresh_after(
    *,
    processing_status: str | None,
    row_count: int,
    unchanged_payload: bool = False,
) -> datetime | None:
    if str(processing_status or "").lower() == "closed":
        return None
    if unchanged_payload:
        if row_count <= 0:
            minutes = 240
        elif row_count < 10:
            minutes = 120
        elif row_count < 100:
            minutes = 60
        else:
            minutes = 30
    else:
        if row_count <= 0:
            minutes = 60
        elif row_count < 10:
            minutes = 45
        else:
            minutes = 20
    return datetime.now(timezone.utc) + timedelta(minutes=minutes)


def _touch_group_refresh_after(cur, group_id: str, refresh_after: datetime | None) -> None:
    cur.execute(
        """
        UPDATE dbo.acc_fin_event_group_sync
        SET
            open_refresh_after = ?,
            last_synced_at = SYSUTCDATETIME()
        WHERE financial_event_group_id = ?
        """,
        (refresh_after, group_id),
    )


def _upsert_group_sync(
    cur,
    *,
    group_id: str,
    marketplace_id: str | None,
    processing_status: str | None,
    fund_transfer_status: str | None,
    group_start: str | None,
    group_end: str | None,
    original_currency: str | None,
    original_amount: float | None,
    row_count: int,
    event_type_counts_json: str | None,
    payload_signature: str | None,
    first_posted_at: datetime | None,
    last_posted_at: datetime | None,
    open_refresh_after: datetime | None,
) -> None:
    cur.execute(
        """
        MERGE dbo.acc_fin_event_group_sync AS tgt
        USING (SELECT ? AS financial_event_group_id) AS src
        ON tgt.financial_event_group_id = src.financial_event_group_id
        WHEN MATCHED THEN
            UPDATE SET
                marketplace_id = ?,
                processing_status = ?,
                fund_transfer_status = ?,
                group_start = ?,
                group_end = ?,
                original_currency = ?,
                original_amount = ?,
                last_row_count = ?,
                event_type_counts_json = ?,
                payload_signature = ?,
                first_posted_at = ?,
                last_posted_at = ?,
                open_refresh_after = ?,
                last_synced_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (
                financial_event_group_id, marketplace_id, processing_status, fund_transfer_status,
                group_start, group_end, original_currency, original_amount, last_row_count,
                event_type_counts_json, payload_signature, first_posted_at, last_posted_at, open_refresh_after,
                last_synced_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
        """,
        (
            group_id,
            marketplace_id,
            processing_status,
            fund_transfer_status,
            group_start,
            group_end,
            original_currency,
            original_amount,
            row_count,
            event_type_counts_json,
            payload_signature,
            first_posted_at,
            last_posted_at,
            open_refresh_after,
            group_id,
            marketplace_id,
            processing_status,
            fund_transfer_status,
            group_start,
            group_end,
            original_currency,
            original_amount,
            row_count,
            event_type_counts_json,
            payload_signature,
            first_posted_at,
            last_posted_at,
            open_refresh_after,
        ),
    )


def _summarize_legacy_events(legacy_events: dict[str, list]) -> tuple[str, str, datetime | None, datetime | None]:
    event_type_counts: dict[str, int] = {}
    first_posted_at: datetime | None = None
    last_posted_at: datetime | None = None
    total_rows = 0
    for event_type, event_list in legacy_events.items():
        count = len(event_list or [])
        event_type_counts[event_type] = count
        total_rows += count
        for event in event_list:
            posted_raw = event.get("PostedDate")
            if not posted_raw:
                continue
            try:
                posted_at = datetime.fromisoformat(str(posted_raw).replace("Z", "+00:00"))
            except Exception:
                continue
            if first_posted_at is None or posted_at < first_posted_at:
                first_posted_at = posted_at
            if last_posted_at is None or posted_at > last_posted_at:
                last_posted_at = posted_at
    signature_payload = {
        "event_type_counts": event_type_counts,
        "total_rows": total_rows,
        "first_posted_at": first_posted_at.isoformat() if first_posted_at else None,
        "last_posted_at": last_posted_at.isoformat() if last_posted_at else None,
    }
    signature_json = json.dumps(signature_payload, sort_keys=True, separators=(",", ":"))
    signature_hash = hashlib.sha256(signature_json.encode("utf-8")).hexdigest()
    return signature_json, signature_hash, first_posted_at, last_posted_at


def _effective_group_window_start(cur, requested_start: datetime, marketplace_ids: set[str] | None) -> datetime:
    overlap = timedelta(days=2)
    params: list[object] = []
    where_sql = ""
    market_list = [m for m in (marketplace_ids or set()) if m]
    if market_list:
        placeholders = ",".join("?" for _ in market_list)
        where_sql = f"WHERE marketplace_id IN ({placeholders})"
        params.extend(market_list)
    cur.execute(
        f"""
        SELECT MAX(COALESCE(group_end, last_posted_at, group_start))
        FROM dbo.acc_fin_event_group_sync WITH (NOLOCK)
        {where_sql}
        """,
        tuple(params),
    )
    row = cur.fetchone()
    watermark = row[0] if row and row[0] is not None else None
    if not watermark:
        return requested_start
    if getattr(watermark, "tzinfo", None) is None:
        watermark = watermark.replace(tzinfo=timezone.utc)
    effective = watermark - overlap
    return effective if effective > requested_start else requested_start


def _load_due_open_group_ids(cur, marketplace_ids: set[str] | None, limit: int = 25) -> list[str]:
    params: list[object] = []
    market_list = [m for m in (marketplace_ids or set()) if m]
    where_parts = [
        "ISNULL(processing_status, '') <> 'Closed'",
        "(open_refresh_after IS NULL OR open_refresh_after <= SYSUTCDATETIME())",
    ]
    if market_list:
        placeholders = ",".join("?" for _ in market_list)
        where_parts.append(f"marketplace_id IN ({placeholders})")
        params.extend(market_list)
    cur.execute(
        f"""
        SELECT TOP {limit} financial_event_group_id
        FROM dbo.acc_fin_event_group_sync WITH (NOLOCK)
        WHERE {" AND ".join(where_parts)}
        ORDER BY COALESCE(open_refresh_after, last_synced_at) ASC, last_synced_at ASC
        """,
        tuple(params),
    )
    return [str(r[0]) for r in cur.fetchall() if r and r[0]]


def _update_finance_job_progress(
    job_id: str | None,
    *,
    progress_pct: int,
    progress_message: str,
    records_processed: int | None = None,
) -> None:
    if not job_id:
        return
    try:
        conn = _db_conn()
        try:
            cur = conn.cursor()
            if records_processed is None:
                cur.execute(
                    """
                    UPDATE dbo.acc_al_jobs
                    SET progress_pct = ?, progress_message = ?, last_heartbeat_at = SYSUTCDATETIME()
                    WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                    """,
                    (progress_pct, progress_message[:300], job_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE dbo.acc_al_jobs
                    SET progress_pct = ?, progress_message = ?, records_processed = ?, last_heartbeat_at = SYSUTCDATETIME()
                    WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                    """,
                    (progress_pct, progress_message[:300], records_processed, job_id),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass



def _fx_case(currency_col: str = "o.currency") -> str:
    """Build SQL CASE expression for FX fallback using DB-sourced rates."""
    from app.core.fx_service import build_fx_case_sql
    return build_fx_case_sql(currency_col)


def _load_fx_cache(cur) -> dict[str, list[tuple[str, float]]]:
    """Load exchange rates into dict keyed by currency → sorted [(date_str, rate)] list."""
    try:
        cur.execute(
            "SELECT currency, CONVERT(VARCHAR(10), rate_date, 120), rate_to_pln "
            "FROM acc_exchange_rate ORDER BY currency, rate_date"
        )
        cache: dict[str, list[tuple[str, float]]] = {}
        for r in cur.fetchall():
            cache.setdefault(str(r[0]), []).append((str(r[1]), float(r[2])))
        return cache
    except Exception:
        return {}


def _get_fx_rate(fx_cache: dict[str, list[tuple[str, float]]], currency: str, posted_date_str: str) -> float:
    """Get closest FX rate <= posted_date from cache using binary search."""
    import bisect
    if not currency or currency == "PLN":
        return 1.0
    date_str = posted_date_str[:10] if posted_date_str else ""
    rates = fx_cache.get(currency, [])
    if not rates:
        from app.core.fx_service import get_rate_safe
        return get_rate_safe(currency, date_str)
    # Binary search for largest date <= date_str
    idx = bisect.bisect_right(rates, (date_str, float("inf"))) - 1
    if idx >= 0:
        return rates[idx][1]
    from app.core.fx_service import get_rate_safe
    return get_rate_safe(currency, date_str)


def _insert_finance_rows(cur, rows: list[tuple], chunk_size: int = 500) -> int:
    """Insert finance transaction rows with amount_pln computed via FX rates."""
    if not rows:
        return 0

    # Preload FX cache for PLN conversion
    fx_cache = _load_fx_cache(cur)

    inserted = 0
    sql = (
        "INSERT INTO acc_finance_transaction "
        "(id, marketplace_id, transaction_type, "
        " amazon_order_id, shipment_id, sku, "
        " posted_date, settlement_id, financial_event_group_id, "
        " amount, currency, charge_type, "
        " amount_pln, exchange_rate, synced_at) "
        "VALUES (NEWID(),?,?,?,?,?,?,?,?,?,?,?,?,?,GETUTCDATE())"
    )
    for offset in range(0, len(rows), chunk_size):
        chunk = rows[offset: offset + chunk_size]
        enriched = []
        for row in chunk:
            # row tuple: (marketplace_id, transaction_type, amazon_order_id,
            #             shipment_id, sku, posted_date, settlement_id,
            #             financial_event_group_id, amount, currency, charge_type)
            amount = float(row[8]) if row[8] is not None else 0.0
            currency = str(row[9] or "EUR")
            posted_date = str(row[5] or "")
            fx_rate = _get_fx_rate(fx_cache, currency, posted_date)
            amount_pln = round(amount * fx_rate, 4)
            enriched.append((*row, amount_pln, fx_rate))
        cur.executemany(sql, enriched)
        inserted += len(chunk)
    return inserted


# ──────────────────────────────────────────────────────────────────
# Helpers for Step 1
# ──────────────────────────────────────────────────────────────────

def _order_payload_hash(raw: dict) -> str:
    """Deterministic hash for order payload change detection.

    Excludes LastUpdateDate so we can skip "timestamp-only" refreshes.
    """
    order_total = raw.get("OrderTotal") or {}
    shipping = raw.get("ShippingAddress") or {}
    payload = {
        "amazon_order_id": raw.get("AmazonOrderId"),
        "status": raw.get("OrderStatus"),
        "fulfillment_channel": raw.get("FulfillmentChannel"),
        "sales_channel": raw.get("SalesChannel"),
        "purchase_date": raw.get("PurchaseDate"),
        "order_total_amount": str(order_total.get("Amount")) if order_total else None,
        "order_total_currency": order_total.get("CurrencyCode") if order_total else None,
        "ship_country": shipping.get("CountryCode"),
    }
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _parse_sp_order(raw: dict, mkt_id: str) -> dict:
    """Parse raw SP-API order dict into a flat dict of DB fields."""
    pd_str = raw.get("PurchaseDate", "")
    lu_str = raw.get("LastUpdateDate", "")
    ot = raw.get("OrderTotal", {})
    sa = raw.get("ShippingAddress", {})
    return {
        "amazon_id": raw.get("AmazonOrderId", ""),
        "mkt_id": mkt_id,
        "status": raw.get("OrderStatus", ""),
        "fc": raw.get("FulfillmentChannel", "FBA"),
        "sc": raw.get("SalesChannel"),
        "purchase_date": (
            datetime.fromisoformat(pd_str.replace("Z", "+00:00")) if pd_str else None
        ),
        "last_update": (
            datetime.fromisoformat(lu_str.replace("Z", "+00:00")) if lu_str else None
        ),
        "order_total": Decimal(str(ot.get("Amount", 0))) if ot else None,
        "currency": ot.get("CurrencyCode", "EUR") if ot else "EUR",
        "ship_country": sa.get("CountryCode") if sa else None,
        "payload_hash": _order_payload_hash(raw),
    }


def _upsert_order(cur, o: dict) -> tuple[str, bool, bool]:
    """Upsert acc_order. Returns (order_id, is_new, status_changed)."""
    cur.execute(
        "SELECT CAST(id AS VARCHAR(36)), status, last_update_date, sync_payload_hash "
        "FROM acc_order WHERE amazon_order_id = ?",
        o["amazon_id"],
    )
    row = cur.fetchone()
    synced_at = datetime.now(timezone.utc)

    if row is None:
        order_id = str(uuid.uuid4())
        cur.execute(
            "INSERT INTO acc_order "
            "(id, amazon_order_id, marketplace_id, status, "
            " fulfillment_channel, sales_channel, purchase_date, "
            " last_update_date, order_total, currency, "
            " ship_country, buyer_country, sync_payload_hash, synced_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            order_id, o["amazon_id"], o["mkt_id"], o["status"],
            o["fc"], o["sc"], o["purchase_date"], o["last_update"],
            o["order_total"], o["currency"],
            o["ship_country"], o["ship_country"], o.get("payload_hash"), synced_at,
        )
        return order_id, True, False

    # Existing order — detect meaningful changes
    order_id = row[0]
    old_status = row[1] or ""
    old_hash = str(row[3] or "")
    new_hash = str(o.get("payload_hash") or "")
    status_changed = old_status != o["status"]
    payload_changed = (not old_hash) or (not new_hash) or old_hash != new_hash

    # Hash-skip: order came in window, but payload is effectively unchanged.
    if not payload_changed:
        return order_id, False, False

    cur.execute(
        "UPDATE acc_order SET status=?, fulfillment_channel=?, "
        "sales_channel=?, purchase_date=?, last_update_date=?, "
        "order_total=?, currency=?, ship_country=?, buyer_country=?, "
        "sync_payload_hash=?, synced_at=? WHERE id=?",
        o["status"], o["fc"], o["sc"], o["purchase_date"], o["last_update"],
        o["order_total"], o["currency"], o["ship_country"], o["ship_country"],
        new_hash, synced_at, order_id,
    )
    return order_id, False, status_changed or payload_changed


def _upsert_items(cur, raw_items: list[dict], order_id: str,
                  currency: str, stats: dict) -> None:
    """
    Upsert order items: insert new, update changed.
    Inline-stamps COGS when product has a price.
    """
    for ri in raw_items:
        iid = ri.get("OrderItemId", "")
        if not iid:
            continue

        sku = ri.get("SellerSKU")
        asin = ri.get("ASIN")
        title = (ri.get("Title") or "")[:500]

        # Lookup product + price for inline COGS stamp
        prod_id = None
        product_price = None
        if sku:
            cur.execute(
                "SELECT CAST(id AS VARCHAR(36)), netto_purchase_price_pln "
                "FROM acc_product WHERE sku=?",
                sku,
            )
            pr = cur.fetchone()
            if pr:
                prod_id = pr[0]
                product_price = float(pr[1]) if pr[1] is not None else None

        ip = ri.get("ItemPrice", {})
        item_price = Decimal(str(ip.get("Amount", 0))) if ip else None
        line_cur = ip.get("CurrencyCode", currency) if ip else currency

        it = ri.get("ItemTax", {})
        item_tax = Decimal(str(it.get("Amount", 0))) if it else None

        pd_d = ri.get("PromotionDiscount", {})
        promo = Decimal(str(pd_d.get("Amount", 0))) if pd_d else None

        qty_ordered = ri.get("QuantityOrdered", 1)
        qty_shipped = ri.get("QuantityShipped", 0)

        # Check if line already exists
        cur.execute(
            "SELECT id FROM acc_order_line WHERE amazon_order_item_id = ?", iid
        )
        existing_line = cur.fetchone()

        if existing_line:
            # Update quantities, prices, status
            cur.execute(
                "UPDATE acc_order_line "
                "SET quantity_ordered=?, quantity_shipped=?, "
                "    item_price=?, item_tax=?, promotion_discount=?, "
                "    product_id = COALESCE(product_id, ?) "
                "WHERE amazon_order_item_id=?",
                qty_ordered, qty_shipped,
                item_price, item_tax, promo,
                prod_id, iid,
            )
            stats["items_updated"] += 1
        else:
            # Insert new line
            purchase_price = None
            cogs = None
            price_source = None
            if product_price is not None:
                purchase_price = product_price
                cogs = round(product_price * (qty_ordered or 1), 4)
                price_source = "auto"
                stats["items_stamped_inline"] += 1

            cur.execute(
                "INSERT INTO acc_order_line "
                "(id, order_id, product_id, "
                " amazon_order_item_id, sku, asin, title, "
                " quantity_ordered, quantity_shipped, "
                " item_price, item_tax, promotion_discount, "
                " currency, purchase_price_pln, cogs_pln, price_source) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                str(uuid.uuid4()), order_id, prod_id,
                iid, sku, asin, title,
                qty_ordered, qty_shipped,
                item_price, item_tax, promo, line_cur,
                purchase_price, cogs, price_source,
            )
            stats["items_new"] += 1


# ──────────────────────────────────────────────────────────────────
# Step 1: Sync orders from SP-API
# ──────────────────────────────────────────────────────────────────
async def step_sync_orders(
    days_back: int = 1,
    max_results: int | None = None,
    marketplace_id: str | None = None,
    created_after_override: datetime | None = None,
    created_before_override: datetime | None = None,
    use_watermark: bool = True,
    sync_profile: str = "core_sync",
) -> dict:
    """
    Fetch recent orders from SP-API → upsert into acc_order + acc_order_line.

    Strategy:
      - For scheduled 15-min runs (days_back ≤ 1):
          Use LastUpdatedAfter (30-min window) to catch both NEW orders
          and STATUS CHANGES on existing orders.
      - For manual backfill runs (days_back > 1):
          Use CreatedAfter for broad historical fetch.

    Items are re-fetched for existing orders whose last_update_date changed,
    catching quantity changes, cancellations, new items added, etc.

    Returns {"orders": N, "items_new": N, "items_updated": N, "new_orders": N,
             "updated_orders": N, "items_stamped_inline": N, "orders_hash_skipped": N}.
    """
    from app.connectors.amazon_sp_api.orders import OrdersClient

    profile_key = (sync_profile or "core_sync").strip().lower()
    profile_cfg = ORDER_SYNC_PROFILES.get(profile_key, ORDER_SYNC_PROFILES["core_sync"])
    fetch_items = bool(profile_cfg.get("fetch_items", True))
    statuses = list(profile_cfg.get("statuses", ORDER_STATUSES))
    use_watermark_effective = bool(use_watermark and profile_key == "core_sync")

    # Decide fetch strategy
    now_utc = datetime.now(timezone.utc)
    created_after = created_after_override
    created_before = created_before_override
    last_updated_after = None
    last_updated_before = None
    sync_mode = "created_after"
    schema_conn = _db_conn()
    try:
        schema_cur = schema_conn.cursor()
        _ensure_order_sync_state_schema(schema_cur)
        schema_conn.commit()
    finally:
        schema_cur.close()
        schema_conn.close()

    if created_after_override or created_before_override or days_back > 1 or not use_watermark_effective:
        if created_after is None:
            created_after = now_utc - timedelta(days=days_back)
        sync_mode = "created_after"
        log.info(
            "pipeline.step1_strategy",
            mode=sync_mode,
            sync_profile=profile_key,
            created_after=created_after.isoformat(),
            created_before=created_before.isoformat() if created_before else None,
        )
    else:
        sync_mode = "last_updated_watermark"
        log.info(
            "pipeline.step1_strategy",
            mode=sync_mode,
            sync_profile=profile_key,
            overlap_min=ORDER_SYNC_OVERLAP_MINUTES,
            safety_lag_min=ORDER_SYNC_SAFETY_LAG_MINUTES,
        )

    stats = {
        "orders": 0,
        "new_orders": 0,
        "updated_orders": 0,
        "items_new": 0,
        "items_updated": 0,
        "items_stamped_inline": 0,
        "orders_hash_skipped": 0,
        "window_marketplaces": {},
        "sync_profile": profile_key,
    }
    t0 = time.time()
    target_marketplaces = (
        [(marketplace_id, MARKETPLACE_REGISTRY[marketplace_id])]
        if marketplace_id
        else list(MARKETPLACE_REGISTRY.items())
    )

    for idx, (mkt_id, info) in enumerate(target_marketplaces):
        code = info["code"]
        if idx > 0:
            await asyncio.sleep(1)  # throttle between marketplaces

        for attempt in range(2):  # 1 retry on deadlock
            conn = None
            cur = None
            try:
                state_conn = _db_conn()
                state_cur = state_conn.cursor()
                window_from = created_after
                window_to = created_before
                state_snapshot = None
                if sync_mode == "last_updated_watermark":
                    window_from, window_to, state_snapshot = _compute_incremental_order_window(state_cur, mkt_id, now_utc)
                    _upsert_order_sync_state(
                        state_cur,
                        marketplace_id=mkt_id,
                        last_mode=sync_mode,
                        last_status="running",
                        last_started_at=now_utc,
                        last_finished_at=None,
                        last_window_from=window_from,
                        last_window_to=window_to,
                        last_successful_window_from=_as_utc(state_snapshot.get("last_successful_window_from")) if state_snapshot else None,
                        last_successful_window_to=_as_utc(state_snapshot.get("last_successful_window_to")) if state_snapshot else None,
                        last_orders_count=int(state_snapshot.get("last_orders_count") or 0) if state_snapshot else 0,
                        last_error=None,
                    )
                    state_conn.commit()
                state_cur.close()
                state_conn.close()

                client = OrdersClient(marketplace_id=mkt_id, sync_profile=profile_key)
                raw_orders = await client.get_orders(
                    created_after=window_from if sync_mode == "created_after" else None,
                    created_before=window_to if sync_mode == "created_after" else None,
                    last_updated_after=window_from if sync_mode == "last_updated_watermark" else None,
                    last_updated_before=window_to if sync_mode == "last_updated_watermark" else None,
                    statuses=statuses,
                    max_results=max_results,
                )
                stats["window_marketplaces"][code] = {
                    "mode": sync_mode,
                    "from": window_from.isoformat() if window_from else None,
                    "to": window_to.isoformat() if window_to else None,
                    "orders": len(raw_orders),
                }
                if not raw_orders:
                    if sync_mode == "last_updated_watermark":
                        state_conn = _db_conn()
                        state_cur = state_conn.cursor()
                        _upsert_order_sync_state(
                            state_cur,
                            marketplace_id=mkt_id,
                            last_mode=sync_mode,
                            last_status="success",
                            last_started_at=now_utc,
                            last_finished_at=datetime.now(timezone.utc),
                            last_window_from=window_from,
                            last_window_to=window_to,
                            last_successful_window_from=window_from,
                            last_successful_window_to=window_to,
                            last_orders_count=0,
                            last_error=None,
                        )
                        state_conn.commit()
                        state_cur.close()
                        state_conn.close()
                    break

                conn = _db_conn()
                cur = conn.cursor()

                for i, raw in enumerate(raw_orders):
                    parsed = _parse_sp_order(raw, mkt_id)
                    if not parsed["amazon_id"]:
                        continue

                    order_id, is_new, changed = _upsert_order(cur, parsed)
                    stats["orders"] += 1

                    if is_new:
                        stats["new_orders"] += 1
                    elif changed:
                        stats["updated_orders"] += 1
                    else:
                        stats["orders_hash_skipped"] += 1

                    # Fetch items for NEW orders or CHANGED existing orders
                    if fetch_items and (is_new or changed):
                        try:
                            raw_items = await client.get_order_items(
                                parsed["amazon_id"]
                            )
                            _upsert_items(
                                cur, raw_items, order_id,
                                parsed["currency"], stats,
                            )
                        except BaseException as e:
                            log.warning(
                                "pipeline.items_err",
                                order=parsed["amazon_id"],
                                error=str(e),
                            )
                            try:
                                conn.rollback()
                                cur.close()
                                conn.close()
                            except Exception:
                                pass
                            conn = _db_conn()
                            cur = conn.cursor()
                            try:
                                _upsert_order(cur, parsed)
                                conn.commit()
                            except Exception as recovery_exc:
                                log.warning(
                                    "pipeline.items_recover_err",
                                    order=parsed["amazon_id"],
                                    error=str(recovery_exc),
                                )
                                try:
                                    conn.rollback()
                                except Exception:
                                    pass
                                cur.close()
                                conn.close()
                                conn = _db_conn()
                                cur = conn.cursor()

                    # Keep transactions short to reduce lock contention and rollback blast radius.
                    if (i + 1) % ORDER_SYNC_COMMIT_EVERY == 0:
                        conn.commit()

                conn.commit()
                cur.close()
                conn.close()
                if sync_mode == "last_updated_watermark":
                    state_conn = _db_conn()
                    state_cur = state_conn.cursor()
                    _upsert_order_sync_state(
                        state_cur,
                        marketplace_id=mkt_id,
                        last_mode=sync_mode,
                        last_status="success",
                        last_started_at=now_utc,
                        last_finished_at=datetime.now(timezone.utc),
                        last_window_from=window_from,
                        last_window_to=window_to,
                        last_successful_window_from=window_from,
                        last_successful_window_to=window_to,
                        last_orders_count=len(raw_orders),
                        last_error=None,
                    )
                    state_conn.commit()
                    state_cur.close()
                    state_conn.close()
                log.info(
                    "pipeline.step1_mkt", mkt=code,
                    orders=stats["orders"], new=stats["new_orders"],
                    updated=stats["updated_orders"], mode=sync_mode,
                    sync_profile=profile_key,
                    fetch_items=fetch_items,
                    hash_skipped=stats["orders_hash_skipped"],
                    window_from=window_from.isoformat() if window_from else None,
                    window_to=window_to.isoformat() if window_to else None,
                )
                break  # success, no retry needed

            except pyodbc.Error as db_err:
                try:
                    if conn:
                        conn.rollback()
                except Exception:
                    pass
                try:
                    if cur:
                        cur.close()
                except Exception:
                    pass
                try:
                    if conn:
                        conn.close()
                except Exception:
                    pass
                if sync_mode == "last_updated_watermark":
                    try:
                        state_conn = _db_conn()
                        state_cur = state_conn.cursor()
                        _upsert_order_sync_state(
                            state_cur,
                            marketplace_id=mkt_id,
                            last_mode=sync_mode,
                            last_status="failure",
                            last_started_at=now_utc,
                            last_finished_at=datetime.now(timezone.utc),
                            last_window_from=window_from,
                            last_window_to=window_to,
                            last_successful_window_from=_as_utc(state_snapshot.get("last_successful_window_from")) if state_snapshot else None,
                            last_successful_window_to=_as_utc(state_snapshot.get("last_successful_window_to")) if state_snapshot else None,
                            last_orders_count=int(state_snapshot.get("last_orders_count") or 0) if state_snapshot else 0,
                            last_error=str(db_err)[:2000],
                        )
                        state_conn.commit()
                        state_cur.close()
                        state_conn.close()
                    except Exception:
                        pass
                err_code = db_err.args[0] if db_err.args else ""
                if err_code in ("40001", "24000") and attempt == 0:
                    log.warning(
                        "pipeline.step1_mkt_deadlock", mkt=code,
                        error=str(db_err), attempt="retry_in_5s",
                    )
                    await asyncio.sleep(5)
                    continue  # retry
                log.error("pipeline.step1_mkt_err", mkt=code, error=str(db_err))
                break
            except Exception as e:
                try:
                    if conn:
                        conn.rollback()
                except Exception:
                    pass
                try:
                    if cur:
                        cur.close()
                except Exception:
                    pass
                try:
                    if conn:
                        conn.close()
                except Exception:
                    pass
                if sync_mode == "last_updated_watermark":
                    try:
                        state_conn = _db_conn()
                        state_cur = state_conn.cursor()
                        _upsert_order_sync_state(
                            state_cur,
                            marketplace_id=mkt_id,
                            last_mode=sync_mode,
                            last_status="failure",
                            last_started_at=now_utc,
                            last_finished_at=datetime.now(timezone.utc),
                            last_window_from=window_from,
                            last_window_to=window_to,
                            last_successful_window_from=_as_utc(state_snapshot.get("last_successful_window_from")) if state_snapshot else None,
                            last_successful_window_to=_as_utc(state_snapshot.get("last_successful_window_to")) if state_snapshot else None,
                            last_orders_count=int(state_snapshot.get("last_orders_count") or 0) if state_snapshot else 0,
                            last_error=str(e)[:2000],
                        )
                        state_conn.commit()
                        state_cur.close()
                        state_conn.close()
                    except Exception:
                        pass
                log.error("pipeline.step1_mkt_err", mkt=code, error=str(e))
                break

    dt = time.time() - t0
    log.info("pipeline.step1_done", **stats, seconds=round(dt, 1))

    # Emit domain event for downstream triggers
    if stats["orders"] > 0:
        try:
            from app.services.event_backbone import emit_domain_event
            emit_domain_event(
                "orders", "synced",
                {"orders": stats["orders"], "new": stats["new_orders"],
                 "updated": stats["updated_orders"], "days_back": days_back},
            )
        except Exception:
            pass  # non-critical

    return stats


async def backfill_orders_range(
    *,
    date_from: datetime,
    date_to: datetime,
    chunk_days: int = 7,
    marketplace_id: str | None = None,
) -> dict:
    if date_to <= date_from:
        raise ValueError("date_to must be greater than date_from")
    total = {
        "windows": 0,
        "orders": 0,
        "new_orders": 0,
        "updated_orders": 0,
        "items_new": 0,
        "items_updated": 0,
        "items_stamped_inline": 0,
    }
    cursor = date_from
    while cursor < date_to:
        window_end = min(cursor + timedelta(days=chunk_days), date_to)
        log.info("orders.backfill.window_start", window_from=cursor.isoformat(), window_to=window_end.isoformat(), marketplace_id=marketplace_id)
        result = await step_sync_orders(
            days_back=0,
            marketplace_id=marketplace_id,
            created_after_override=cursor,
            created_before_override=window_end,
            use_watermark=False,
            max_results=None,
        )
        log.info("orders.backfill.window_done", window_from=cursor.isoformat(), window_to=window_end.isoformat(), result=result)
        total["windows"] += 1
        for key in ("orders", "new_orders", "updated_orders", "items_new", "items_updated", "items_stamped_inline"):
            total[key] += int(result.get(key, 0) or 0)
        await asyncio.sleep(1)
        cursor = window_end
    return total


# ──────────────────────────────────────────────────────────────────
# Step 2: Backfill missing products
# ──────────────────────────────────────────────────────────────────
def step_backfill_products() -> int:
    """
    Create acc_product rows for SKU/ASIN combos in acc_order_line
    that don't have a matching product yet.
    Returns number of products created.
    """
    conn = _db_conn()
    cur = conn.cursor()

    # Find distinct SKU/ASIN pairs with no matching product
    cur.execute("""
        SELECT DISTINCT ol.sku, ol.asin
        FROM acc_order_line ol
        WHERE ol.product_id IS NULL
          AND ol.sku IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM acc_product p WHERE p.sku = ol.sku
          )
    """)
    missing = cur.fetchall()

    created = 0
    for row in missing:
        sku, asin = row[0], row[1]
        try:
            registry = lookup_listing_registry_context(cur, sku=sku, asin=asin)
            product_sku = _clean_text(sku)
            product_asin = _clean_text((registry or {}).get("asin")) or _clean_text(asin)
            title = _clean_text((registry or {}).get("product_name"))
            brand = _clean_text((registry or {}).get("brand"))
            internal_sku = _clean_text((registry or {}).get("internal_sku"))
            ean = _clean_text((registry or {}).get("ean"))
            parent_asin = _clean_text((registry or {}).get("parent_asin"))
            category = _clean_text((registry or {}).get("category_1")) or _clean_text((registry or {}).get("category_2"))
            listing_role = _clean_text((registry or {}).get("listing_role")) or ""
            cur.execute(
                """
                INSERT INTO acc_product (
                    id, sku, asin, ean, brand, category, title,
                    is_parent, parent_asin, internal_sku, mapping_source
                )
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM acc_product p 
                    WHERE (p.sku = ? OR (? IS NOT NULL AND p.asin = ?))
                )
                """,
                (
                    str(uuid.uuid4()),
                    product_sku,
                    product_asin,
                    ean,
                    brand,
                    category,
                    title,
                    1 if listing_role.lower() == "parent" else 0,
                    parent_asin,
                    internal_sku,
                    "amazon_listing_registry" if registry else "order_pipeline",
                    product_sku,
                    product_asin,
                    product_asin,
                ),
            )
            created += 1
        except Exception as e:
            log.warning("pipeline.backfill_err", sku=sku, error=str(e))

    conn.commit()
    cur.close()
    conn.close()
    log.info("pipeline.step2_done", created=created)
    return created


def step_enrich_products_from_registry() -> int:
    conn = _db_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE p
            SET
                p.asin = COALESCE(NULLIF(p.asin, ''), rg.asin),
                p.ean = COALESCE(NULLIF(p.ean, ''), rg.ean),
                p.brand = COALESCE(NULLIF(p.brand, ''), rg.brand),
                p.category = COALESCE(NULLIF(p.category, ''), COALESCE(rg.category_1, rg.category_2)),
                p.title = COALESCE(NULLIF(p.title, ''), rg.product_name),
                p.parent_asin = COALESCE(NULLIF(p.parent_asin, ''), rg.parent_asin),
                p.internal_sku = COALESCE(NULLIF(p.internal_sku, ''), rg.internal_sku),
                p.mapping_source = CASE
                    WHEN ISNULL(p.mapping_source, '') = '' AND rg.internal_sku IS NOT NULL THEN 'amazon_listing_registry'
                    ELSE p.mapping_source
                END,
                p.updated_at = GETUTCDATE()
            FROM acc_product p
            CROSS APPLY (
                SELECT TOP 1
                    r.asin,
                    r.ean,
                    r.brand,
                    r.product_name,
                    r.parent_asin,
                    r.internal_sku,
                    r.category_1,
                    r.category_2
                FROM dbo.acc_amazon_listing_registry r WITH (NOLOCK)
                WHERE (p.sku IS NOT NULL AND (r.merchant_sku = p.sku OR r.merchant_sku_alt = p.sku))
                   OR (p.asin IS NOT NULL AND r.asin = p.asin)
                ORDER BY
                    CASE
                        WHEN p.sku IS NOT NULL AND r.merchant_sku = p.sku THEN 0
                        WHEN p.sku IS NOT NULL AND r.merchant_sku_alt = p.sku THEN 1
                        WHEN p.asin IS NOT NULL AND r.asin = p.asin THEN 2
                        ELSE 9
                    END,
                    r.updated_at DESC
            ) rg
            WHERE
                ISNULL(p.internal_sku, '') = ''
                OR ISNULL(p.title, '') = ''
                OR ISNULL(p.ean, '') = ''
                OR ISNULL(p.parent_asin, '') = ''
                OR ISNULL(p.brand, '') = ''
                OR ISNULL(p.category, '') = ''
            """
        )
        updated = int(cur.rowcount or 0)
        conn.commit()
        log.info("pipeline.step2_5_done", enriched=updated)
        return updated
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────
# Step 3: Link order lines to products
# ──────────────────────────────────────────────────────────────────
def step_link_order_lines() -> int:
    """
    SET acc_order_line.product_id where it's NULL but SKU exists in acc_product.
    Returns number of lines linked.
    """
    conn = _db_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE ol
        SET ol.product_id = p.id
        FROM acc_order_line ol
        INNER JOIN acc_product p ON p.sku = ol.sku
        WHERE ol.product_id IS NULL
          AND ol.sku IS NOT NULL
    """)
    linked = int(cur.rowcount or 0)
    conn.commit()

    cur.execute(
        """
        UPDATE ol
        SET ol.product_id = p.id
        FROM acc_order_line ol
        CROSS APPLY (
            SELECT TOP 1
                r.internal_sku,
                r.asin
            FROM dbo.acc_amazon_listing_registry r WITH (NOLOCK)
            WHERE (ol.sku IS NOT NULL AND (r.merchant_sku = ol.sku OR r.merchant_sku_alt = ol.sku))
               OR (ol.asin IS NOT NULL AND r.asin = ol.asin)
            ORDER BY
                CASE
                    WHEN ol.sku IS NOT NULL AND r.merchant_sku = ol.sku THEN 0
                    WHEN ol.sku IS NOT NULL AND r.merchant_sku_alt = ol.sku THEN 1
                    WHEN ol.asin IS NOT NULL AND r.asin = ol.asin THEN 2
                    ELSE 9
                END,
                r.updated_at DESC
        ) rg
        CROSS APPLY (
            SELECT TOP 1 p2.id
            FROM acc_product p2
            WHERE (rg.internal_sku IS NOT NULL AND p2.internal_sku = rg.internal_sku)
               OR (rg.asin IS NOT NULL AND p2.asin = rg.asin)
            ORDER BY
                CASE
                    WHEN rg.internal_sku IS NOT NULL AND p2.internal_sku = rg.internal_sku THEN 0
                    WHEN rg.asin IS NOT NULL AND p2.asin = rg.asin THEN 1
                    ELSE 9
                END,
                p2.updated_at DESC
        ) p
        WHERE ol.product_id IS NULL
          AND (ol.sku IS NOT NULL OR ol.asin IS NOT NULL)
        """
    )
    linked += int(cur.rowcount or 0)
    conn.commit()
    cur.close()
    conn.close()
    log.info("pipeline.step3_done", linked=linked)
    return linked


# ──────────────────────────────────────────────────────────────────
# Step 4: Map internal SKU (canonical → legacy fallback)
# ──────────────────────────────────────────────────────────────────
async def step_map_products() -> int:
    """Map unmapped products to internal SKU.

    Strategy:
      1. Try canonical mapping via ``acc_canonical_product`` / ``acc_marketplace_presence``.
      2. Fall back to legacy ``sync_product_mapping`` for any remaining unmapped products.
      Both paths are logged for comparison during the canonical migration period.
    """
    from app.domain.marketplace_mapping import resolve_product
    from app.services.sync_service import sync_product_mapping

    # --- Phase 1: Canonical mapping ---
    canonical_mapped = 0
    try:
        canonical_mapped = _step_map_products_canonical(resolve_product)
        log.info("pipeline.step4_canonical", mapped=canonical_mapped)
    except Exception as e:
        log.warning("pipeline.step4_canonical_err", error=str(e))

    # --- Phase 2: Legacy fallback for remaining unmapped ---
    legacy_mapped = 0
    try:
        legacy_mapped = await sync_product_mapping(only_unmapped=True)
        log.info("pipeline.step4_legacy", mapped=legacy_mapped)
    except Exception as e:
        log.error("pipeline.step4_legacy_err", error=str(e))

    total = canonical_mapped + legacy_mapped
    log.info("pipeline.step4_done", mapped=total,
             canonical=canonical_mapped, legacy=legacy_mapped)
    return total


def _step_map_products_canonical(resolve_fn) -> int:
    """Try to map unmapped acc_product rows via canonical lookup.

    Returns number of products successfully mapped.
    """
    conn = connect_acc(timeout=15)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT p.id, p.sku, p.asin, p.ean,
                   o.marketplace_id
            FROM dbo.acc_product p WITH (NOLOCK)
            LEFT JOIN (
                SELECT DISTINCT product_id, marketplace_id
                FROM dbo.acc_offer WITH (NOLOCK)
            ) o ON o.product_id = p.id
            WHERE p.internal_sku IS NULL
            """
        )
        rows = cur.fetchall()
        if not rows:
            return 0

        mapped = 0
        for row in rows:
            pid, sku, asin, ean, marketplace_id = row[0], row[1], row[2], row[3], row[4]
            match = resolve_fn(
                sku=sku, asin=asin, ean=ean,
                marketplace_id=marketplace_id,
            )
            if match and match.internal_sku:
                cur.execute(
                    """
                    UPDATE dbo.acc_product
                    SET internal_sku = ?,
                        k_number = ?,
                        ergonode_id = ?,
                        mapping_source = ?,
                        updated_at = SYSUTCDATETIME()
                    WHERE id = ? AND internal_sku IS NULL
                    """,
                    (
                        match.internal_sku,
                        match.k_number,
                        match.ergonode_id,
                        f"canonical_{match.mapping_source or 'direct'}",
                        pid,
                    ),
                )
                mapped += 1
        conn.commit()
        return mapped
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────
# Step 5: Stamp purchase prices on new order lines
# ──────────────────────────────────────────────────────────────────
def step_stamp_purchase_prices() -> int:
    """
    Fill acc_order_line.purchase_price_pln + cogs_pln for lines where it's NULL.

    Price lookup cascade:
      1. acc_product.netto_purchase_price_pln  (legacy, fast)
      2. acc_purchase_price (latest by valid_from for the internal_sku)

    Returns total number of lines stamped.
    """
    conn = _db_conn()
    cur = conn.cursor()

    # --- Pass 1: from acc_product.netto_purchase_price_pln (legacy) ---
    # Cap at 2000 PLN to filter out garbage prices from xlsx imports
    cur.execute("""
        UPDATE ol
        SET ol.purchase_price_pln = p.netto_purchase_price_pln,
            ol.cogs_pln = p.netto_purchase_price_pln
                          * ISNULL(ol.quantity_ordered, 1),
            ol.price_source = 'auto'
        FROM acc_order_line ol
        INNER JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        INNER JOIN acc_product p ON p.id = ol.product_id
        WHERE ol.purchase_price_pln IS NULL
          AND p.netto_purchase_price_pln IS NOT NULL
          AND p.netto_purchase_price_pln > 0
          AND p.netto_purchase_price_pln <= 2000
          AND ol.quantity_ordered > 0
          AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
          AND o.amazon_order_id NOT LIKE 'S02-%'
    """)
    stamped_legacy = cur.rowcount
    conn.commit()

    # --- Pass 2: from acc_purchase_price table (covers all sources) ---
    # Source priority: manual > xlsx_oficjalne > holding > erp_holding > others
    # Holding prices are systematically ~4% below official xlsx → apply 1.04 multiplier
    HOLDING_MULTIPLIER = 1.04
    cur.execute(f"""
        UPDATE ol
        SET ol.purchase_price_pln = pp.netto_price_pln
                * CASE WHEN pp.source IN ('holding','erp_holding') THEN {HOLDING_MULTIPLIER} ELSE 1.0 END,
            ol.cogs_pln = pp.netto_price_pln
                * CASE WHEN pp.source IN ('holding','erp_holding') THEN {HOLDING_MULTIPLIER} ELSE 1.0 END
                          * ISNULL(ol.quantity_ordered, 1),
            ol.price_source = pp.source
        FROM acc_order_line ol
        INNER JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        INNER JOIN acc_product p ON p.id = ol.product_id
        CROSS APPLY (
            SELECT TOP 1 pp2.netto_price_pln, pp2.source
            FROM acc_purchase_price pp2 WITH (NOLOCK)
            WHERE pp2.internal_sku = p.internal_sku
              AND pp2.netto_price_pln > 0
              AND pp2.netto_price_pln <= 2000
            ORDER BY
                CASE pp2.source
                    WHEN 'manual'         THEN 1
                    WHEN 'import_xlsx'    THEN 2
                    WHEN 'xlsx_oficjalne' THEN 3
                    WHEN 'holding'        THEN 4
                    WHEN 'erp_holding'    THEN 5
                    WHEN 'import_csv'     THEN 6
                    WHEN 'cogs_xlsx'      THEN 7
                    WHEN 'acc_product'    THEN 8
                    ELSE 9
                END,
                pp2.valid_from DESC
        ) pp
        WHERE (ol.purchase_price_pln IS NULL OR ol.purchase_price_pln = 0)
          AND p.internal_sku IS NOT NULL
          AND ol.quantity_ordered > 0
          AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
          AND o.amazon_order_id NOT LIKE 'S02-%'
    """)
    stamped_pp = cur.rowcount
    conn.commit()

    total = stamped_legacy + stamped_pp
    cur.close()
    conn.close()
    log.info("pipeline.step5_done", stamped_legacy=stamped_legacy, stamped_pp=stamped_pp, total=total)
    return total


# ──────────────────────────────────────────────────────────────────
# Step 5.5b: Sync financial transactions from SP-API (Finances v2024-06-19)
# ──────────────────────────────────────────────────────────────────
async def step_sync_finances(days_back: int = 3, marketplace_id: str | None = None, job_id: str | None = None) -> dict:
    """
    Fetch financial transactions from SP-API Finances v2024-06-19
    → upsert into acc_finance_transaction.

    Handles the 180-day API window constraint by chunking automatically.
    For daily cron use days_back=3 (covers 48 h lag + safety margin).
    For historical backfill use days_back=730 (2 years).

    Returns {"transactions": N, "fee_rows": N, "marketplaces": N}.
    """
    from app.connectors.amazon_sp_api.finances import FinancesClient

    stats = {"transactions": 0, "fee_rows": 0, "marketplaces": 0, "errors": 0, "groups_synced": 0, "groups_skipped": 0}
    request_cutoff = datetime.now(timezone.utc) - timedelta(minutes=3)
    start = request_cutoff - timedelta(days=days_back)
    use_group_watermark = days_back <= 30

    # Build 180-day windows (API limit)
    windows: list[tuple[datetime, datetime]] = []
    win_start = start
    while win_start < request_cutoff:
        win_end = min(win_start + timedelta(days=179), request_cutoff)
        windows.append((win_start, win_end))
        win_start = win_end
    # Newest windows first so the most recent data is backfilled first
    windows.reverse()
    log.info("finances_sync.plan", windows=len(windows), days_back=days_back, request_cutoff=request_cutoff.isoformat())
    _update_finance_job_progress(
        job_id,
        progress_pct=10,
        progress_message=f"Planning finance sync windows={len(windows)}",
        records_processed=0,
    )

    conn = _db_conn_finance()
    cur = conn.cursor()
    cur.execute(
        """
        DECLARE @result INT;
        EXEC @result = sp_getapplock
            @Resource = 'acc_finance_sync_transactions',
            @LockMode = 'Exclusive',
            @LockOwner = 'Session',
            @LockTimeout = 1000;
        SELECT @result;
        """
    )
    lock_row = cur.fetchone()
    lock_result = int(lock_row[0]) if lock_row and lock_row[0] is not None else -999
    if lock_result < 0:
        cur.close()
        conn.close()
        raise RuntimeError("finance_sync_transactions is already running in another session")
    _ensure_finance_group_sync_schema(cur)
    conn.commit()

    fallback_markets: set[str] = set()

    try:
        for mkt_id, info in MARKETPLACE_REGISTRY.items():
            if marketplace_id and mkt_id != marketplace_id:
                continue
            code = info["code"]
            mkt_total = 0
            try:
                client = FinancesClient(marketplace_id=mkt_id)
                for win_start, win_end in windows:
                    _update_finance_job_progress(
                        job_id,
                        progress_pct=min(40, 12 + stats["marketplaces"] * 3),
                        progress_message=f"Market {code} fetching direct window {win_start.date()}->{win_end.date()}",
                        records_processed=stats["fee_rows"] + mkt_total,
                    )
                    txns = await client.list_transactions(
                        posted_after=win_start,
                        posted_before=win_end,
                        marketplace_id=mkt_id,
                    )
                    stats["transactions"] += len(txns)

                    batch_rows = 0
                    if txns:
                        cur.execute(
                            "DELETE FROM acc_finance_transaction WITH (ROWLOCK) "
                            "WHERE marketplace_id = ? "
                            "  AND posted_date >= ? AND posted_date < ?",
                            mkt_id,
                            win_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            win_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        )
                        conn.commit()
                        batch_params: list[tuple] = []
                        for txn in txns:
                            fee_rows = FinancesClient.parse_transaction_fees(txn)
                            for row in fee_rows:
                                row_marketplace_id = row.get("marketplace_id") or mkt_id
                                batch_params.append(
                                    (
                                        row_marketplace_id,
                                        row["transaction_type"],
                                        row["amazon_order_id"],
                                        row.get("shipment_id"),
                                        row["sku"],
                                        row["posted_date"],
                                        row.get("settlement_id") or row.get("financial_event_group_id"),
                                        row.get("financial_event_group_id"),
                                        row["amount"],
                                        row["currency"],
                                        row["charge_type"],
                                    )
                                )
                        batch_rows += _insert_finance_rows(cur, batch_params)
                        conn.commit()
                        mkt_total += batch_rows
                        log.info(
                            "finances_sync.window",
                            mkt=code,
                            win=f"{win_start.date()}->{win_end.date()}",
                            txns=len(txns),
                            rows=batch_rows,
                            source="v2024_06_19",
                        )
                        _update_finance_job_progress(
                            job_id,
                            progress_pct=min(55, 15 + stats["marketplaces"] * 3),
                            progress_message=f"Market {code} direct feed rows={batch_rows}",
                            records_processed=stats["fee_rows"] + mkt_total,
                        )
                    else:
                        fallback_markets.add(mkt_id)

                stats["fee_rows"] += mkt_total
                stats["marketplaces"] += 1
                log.info("finances_sync.mkt_done", mkt=code, rows=mkt_total)

            except Exception as e:
                stats["errors"] += 1
                log.error("finances_sync.mkt_error", mkt=code, error=str(e))
                conn.rollback()

            await asyncio.sleep(1)

        if fallback_markets:
            anchor_marketplace = marketplace_id or settings.SP_API_PRIMARY_MARKETPLACE or next(iter(fallback_markets))
            group_client = FinancesClient(marketplace_id=anchor_marketplace)
            for win_start, win_end in windows:
                effective_start = (
                    _effective_group_window_start(cur, win_start, fallback_markets)
                    if use_group_watermark
                    else win_start
                )
                groups = await group_client.list_financial_event_groups(
                    started_after=effective_start,
                    started_before=win_end,
                    max_pages=20,
                )
                due_open_group_ids = _load_due_open_group_ids(cur, fallback_markets, limit=25)
                seen_group_ids: set[str] = set()
                for group in groups:
                    gid = str(group.get("FinancialEventGroupId") or "")
                    if gid:
                        seen_group_ids.add(gid)
                for due_group_id in due_open_group_ids:
                    if due_group_id not in seen_group_ids:
                        groups.append(
                            {
                                "FinancialEventGroupId": due_group_id,
                                "ProcessingStatus": "Open",
                                "FundTransferStatus": None,
                                "FinancialEventGroupEnd": None,
                                "FinancialEventGroupStart": effective_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                "OriginalTotal": {},
                            }
                        )
                if not use_group_watermark:
                    groups.sort(
                        key=lambda group: str(
                            group.get("FinancialEventGroupStart")
                            or group.get("FinancialEventGroupEnd")
                            or ""
                        ),
                        reverse=True,
                    )
                window_rows = 0
                total_groups = len(groups) or 1
                for group in groups:
                    group_id = str(group.get("FinancialEventGroupId") or "")
                    if not group_id:
                        continue
                    processed_groups = stats["groups_synced"] + stats["groups_skipped"]
                    progress_pct = min(95, 55 + int((processed_groups / total_groups) * 35))
                    _update_finance_job_progress(
                        job_id,
                        progress_pct=progress_pct,
                        progress_message=f"Fetching group {group_id[:18]} synced={stats['groups_synced']} skipped={stats['groups_skipped']}",
                        records_processed=stats["fee_rows"],
                    )
                    processing_status = group.get("ProcessingStatus")
                    fund_transfer_status = group.get("FundTransferStatus")
                    group_end = group.get("FinancialEventGroupEnd")
                    existing_group = _get_group_sync_snapshot(cur, group_id)
                    persisted_group_rows = _get_finance_group_persisted_row_count(cur, group_id)
                    force_repair_missing_rows = bool(
                        existing_group
                        and int(existing_group.get("last_row_count") or 0) > 0
                        and persisted_group_rows == 0
                    )
                    if not force_repair_missing_rows and not _should_resync_group(cur, group_id, processing_status, fund_transfer_status, group_end):
                        stats["groups_skipped"] += 1
                        continue
                    legacy_events = await group_client.list_financial_events_by_group_id(
                        event_group_id=group_id,
                        posted_after=win_start,
                        posted_before=win_end,
                        max_results=5000,
                    )
                    event_type_counts_json, payload_signature, first_posted_at, last_posted_at = _summarize_legacy_events(legacy_events)
                    current_row_count = sum(len(v or []) for v in legacy_events.values())
                    unchanged_payload = bool(
                        existing_group
                        and str(existing_group.get("payload_signature") or "") == str(payload_signature or "")
                        and int(existing_group.get("last_row_count") or 0) == current_row_count
                        and str(existing_group.get("processing_status") or "") == str(processing_status or "")
                        and str(existing_group.get("fund_transfer_status") or "") == str(fund_transfer_status or "")
                    )
                    if not force_repair_missing_rows and not _should_resync_group(
                        cur,
                        group_id,
                        processing_status,
                        fund_transfer_status,
                        group_end,
                        payload_signature=payload_signature,
                    ):
                        if unchanged_payload:
                            _touch_group_refresh_after(
                                cur,
                                group_id,
                                _compute_open_refresh_after(
                                    processing_status=processing_status,
                                    row_count=current_row_count,
                                    unchanged_payload=True,
                                ),
                            )
                            conn.commit()
                        stats["groups_skipped"] += 1
                        continue
                    actual_marketplace_id = _infer_legacy_marketplace_id(
                        legacy_events,
                        default_marketplace_id=(
                            (existing_group or {}).get("marketplace_id")
                            or marketplace_id
                        ),
                    )
                    if actual_marketplace_id and actual_marketplace_id not in fallback_markets:
                        stats["groups_skipped"] += 1
                        continue

                    cur.execute(
                        "DELETE FROM acc_finance_transaction WITH (ROWLOCK) WHERE financial_event_group_id = ? OR settlement_id = ?",
                        (group_id, group_id),
                    )
                    conn.commit()

                    group_rows = 0
                    default_currency = (
                        MARKETPLACE_REGISTRY.get(actual_marketplace_id or "", {}).get("currency")
                        or group.get("OriginalTotal", {}).get("CurrencyCode")
                        or "EUR"
                    )
                    order_marketplace_map = _load_order_marketplace_map(
                        cur,
                        [
                            str(event.get("AmazonOrderId") or "").strip()
                            for event_list in legacy_events.values()
                            for event in event_list
                            if event.get("AmazonOrderId")
                        ],
                    )
                    batch_params: list[tuple] = []
                    for event_type, event_list in legacy_events.items():
                        for event in event_list:
                            fee_rows = FinancesClient.parse_legacy_event_rows(
                                event_type,
                                event,
                                default_currency=default_currency,
                                financial_event_group_id=group_id,
                            )
                            event_marketplace_id = (
                                _map_marketplace_name_to_id(event.get("MarketplaceName") or event.get("StoreName"))
                                or order_marketplace_map.get(str(event.get("AmazonOrderId") or "").strip())
                                or actual_marketplace_id
                                or (existing_group or {}).get("marketplace_id")
                                or marketplace_id
                            )
                            if event_marketplace_id and event_marketplace_id not in fallback_markets:
                                continue
                            for row in fee_rows:
                                batch_params.append(
                                    (
                                        event_marketplace_id,
                                        row["transaction_type"],
                                        row["amazon_order_id"],
                                        row.get("shipment_id"),
                                        row["sku"],
                                        row["posted_date"],
                                        row.get("settlement_id") or row.get("financial_event_group_id"),
                                        row.get("financial_event_group_id"),
                                        row["amount"],
                                        row["currency"],
                                        row["charge_type"],
                                    )
                                )

                    group_rows += _insert_finance_rows(cur, batch_params, chunk_size=250)

                    _upsert_group_sync(
                        cur,
                        group_id=group_id,
                        marketplace_id=actual_marketplace_id,
                        processing_status=processing_status,
                        fund_transfer_status=fund_transfer_status,
                        group_start=group.get("FinancialEventGroupStart"),
                        group_end=group_end,
                        original_currency=(group.get("OriginalTotal") or {}).get("CurrencyCode"),
                        original_amount=(group.get("OriginalTotal") or {}).get("CurrencyAmount"),
                        row_count=group_rows,
                        event_type_counts_json=event_type_counts_json,
                        payload_signature=payload_signature,
                        first_posted_at=first_posted_at,
                        last_posted_at=last_posted_at,
                        open_refresh_after=_compute_open_refresh_after(
                            processing_status=processing_status,
                            row_count=group_rows,
                            unchanged_payload=False,
                        ),
                    )
                    conn.commit()
                    window_rows += group_rows
                    stats["fee_rows"] += group_rows
                    stats["groups_synced"] += 1
                    processed_groups = stats["groups_synced"] + stats["groups_skipped"]
                    progress_pct = min(95, 55 + int((processed_groups / total_groups) * 35))
                    _update_finance_job_progress(
                        job_id,
                        progress_pct=progress_pct,
                        progress_message=f"Groups synced={stats['groups_synced']} skipped={stats['groups_skipped']} current={group_id[:18]}",
                        records_processed=stats["fee_rows"],
                    )

                log.info(
                    "finances_sync.window",
                    mkt="account-wide",
                    win=f"{win_start.date()}->{win_end.date()}",
                    txns=0,
                    rows=window_rows,
                    source="v0_fallback_grouped",
                    groups_synced=stats["groups_synced"],
                    groups_skipped=stats["groups_skipped"],
                )
    finally:
        try:
            cur.execute(
                """
                DECLARE @result INT;
                EXEC @result = sp_releaseapplock
                    @Resource = 'acc_finance_sync_transactions',
                    @LockOwner = 'Session';
                """
            )
        except Exception:
            pass
        cur.execute(
            "SELECT 1"
        )
        cur.close()
        conn.close()
    _update_finance_job_progress(
        job_id,
        progress_pct=99,
        progress_message=(
            f"Finance sync done groups_synced={stats['groups_synced']} "
            f"groups_skipped={stats['groups_skipped']}"
        ),
        records_processed=stats["fee_rows"],
    )
    log.info("finances_sync.done", **stats)
    return stats


# ──────────────────────────────────────────────────────────────────
# Step 5.9: Bridge finance fees → order line fee columns
# ──────────────────────────────────────────────────────────────────

# Charge types bridged from acc_finance_transaction → order_line.
# Must stay in sync with FeeCategory.FBA_FEE / REFERRAL_FEE entries
# in fee_taxonomy.FEE_REGISTRY.
_BRIDGE_FBA_CHARGE_TYPES: tuple[str, ...] = (
    "FBAPerUnitFulfillmentFee",
    "FBAPerOrderFulfillmentFee",
    "FBAWeightBasedFee",
    "FBAPickAndPackFee",
    "FBAWeightHandlingFee",
    "FBAOrderHandlingFee",
    "FBAPerUnitFulfillment",
    "FBADeliveryServicesFee",
)
_BRIDGE_REF_CHARGE_TYPES: tuple[str, ...] = (
    "Commission",
    "ReferralFee",
    "VariableClosingFee",
    "FixedClosingFee",
)
_BRIDGE_ALL_CHARGE_TYPES: tuple[str, ...] = _BRIDGE_FBA_CHARGE_TYPES + _BRIDGE_REF_CHARGE_TYPES

# Direct-order CM1 costs live on acc_order, not acc_order_line.
_BRIDGE_ORDER_SHIPPING_SURCHARGE_TYPES: tuple[str, ...] = (
    "ShippingHB",
    "ShippingChargeback",
    "FBAOverSizeSurcharge",
)
_BRIDGE_ORDER_PROMO_CHARGE_TYPES: tuple[str, ...] = (
    "CouponRedemptionFee",
    "PrimeExclusiveDiscountFee",
    "SubscribeAndSavePerformanceFee",
)
_BRIDGE_ORDER_REFUND_COMMISSION_TYPES: tuple[str, ...] = (
    "RefundCommission",
)
_BRIDGE_ORDER_ALL_CHARGE_TYPES: tuple[str, ...] = (
    _BRIDGE_ORDER_SHIPPING_SURCHARGE_TYPES
    + _BRIDGE_ORDER_PROMO_CHARGE_TYPES
    + _BRIDGE_ORDER_REFUND_COMMISSION_TYPES
)


def _bridge_in_clause(charge_types: tuple[str, ...]) -> str:
    return ",".join(f"'{ct}'" for ct in charge_types)


def _bridge_fba_in_clause() -> str:
    """SQL IN-clause values for FBA fee charge types."""
    return _bridge_in_clause(_BRIDGE_FBA_CHARGE_TYPES)


def _bridge_ref_in_clause() -> str:
    """SQL IN-clause values for referral fee charge types."""
    return _bridge_in_clause(_BRIDGE_REF_CHARGE_TYPES)


def _bridge_all_in_clause() -> str:
    """SQL IN-clause values for all bridged charge types."""
    return _bridge_in_clause(_BRIDGE_ALL_CHARGE_TYPES)


def _bridge_order_shipping_in_clause() -> str:
    return _bridge_in_clause(_BRIDGE_ORDER_SHIPPING_SURCHARGE_TYPES)


def _bridge_order_promo_in_clause() -> str:
    return _bridge_in_clause(_BRIDGE_ORDER_PROMO_CHARGE_TYPES)


def _bridge_order_refund_commission_in_clause() -> str:
    return _bridge_in_clause(_BRIDGE_ORDER_REFUND_COMMISSION_TYPES)


def _bridge_order_all_in_clause() -> str:
    return _bridge_in_clause(_BRIDGE_ORDER_ALL_CHARGE_TYPES)


def step_bridge_fees(timeout: int = 300) -> int:
    """
        Aggregate order-matchable Amazon fees from acc_finance_transaction.

        Line-level fees:
            acc_order_line.fba_fee_pln / referral_fee_pln

        Order-level direct CM1 fees:
            acc_order.shipping_surcharge_pln / promo_order_fee_pln /
            refund_commission_pln

        Total fees:
            acc_order.amazon_fees_pln.

    Only processes lines where fba_fee_pln IS NULL and we have
    matching finance transactions.

    Uses temp-table with ROW_NUMBER dedup to handle duplicate
    transactions across financial event groups (dual-sync path).
    Proportionally distributes fees across multiple lines with
    the same SKU within an order.

    Returns number of order lines updated.
    """
    conn = _db_conn_finance(timeout=timeout)
    cur = conn.cursor()

    # Step A: Build temp table with deduped + aggregated fees per order+SKU
    # Finance transactions can appear in multiple financial_event_group_ids
    # (dual-sync path). ROW_NUMBER deduplicates exact matches.
    cur.execute("IF OBJECT_ID('tempdb..#fee_agg') IS NOT NULL DROP TABLE #fee_agg")
    conn.commit()

    fba_in = _bridge_fba_in_clause()
    ref_in = _bridge_ref_in_clause()
    all_in = _bridge_all_in_clause()

    cur.execute(f"""
        SELECT
            df.amazon_order_id,
            df.sku,
            SUM(CASE WHEN df.charge_type IN ({fba_in})
                THEN ABS(df.amount) * ISNULL(fx.rate_to_pln,
                    {_fx_case('df.currency')})
                ELSE 0 END) AS fba_pln,
            SUM(CASE WHEN df.charge_type IN ({ref_in})
                THEN ABS(df.amount) * ISNULL(fx.rate_to_pln,
                    {_fx_case('df.currency')})
                ELSE 0 END) AS ref_pln
        INTO #fee_agg
        FROM (
            SELECT
                ft.amazon_order_id,
                ft.sku,
                ft.charge_type,
                ft.amount,
                ft.currency,
                ft.posted_date,
                ROW_NUMBER() OVER (
                    PARTITION BY ft.amazon_order_id, ft.sku,
                                 ft.charge_type, ft.amount, ft.posted_date
                    ORDER BY ft.synced_at DESC
                ) AS rn
            FROM acc_finance_transaction ft WITH (NOLOCK)
            WHERE ft.charge_type IN ({all_in})
              AND ft.amazon_order_id IS NOT NULL
              AND ft.sku IS NOT NULL
        ) df
        OUTER APPLY (
            SELECT TOP 1 rate_to_pln FROM acc_exchange_rate er WITH (NOLOCK)
            WHERE er.currency = df.currency
              AND er.rate_date <= CAST(df.posted_date AS DATE)
            ORDER BY er.rate_date DESC
        ) fx
        WHERE df.rn = 1
        GROUP BY df.amazon_order_id, df.sku
        HAVING SUM(CASE WHEN df.charge_type IN ({fba_in})
                THEN ABS(df.amount) ELSE 0 END) > 0
            OR SUM(CASE WHEN df.charge_type IN ({ref_in})
                THEN ABS(df.amount) ELSE 0 END) > 0
    """)
    conn.commit()

    cur.execute("CREATE CLUSTERED INDEX IX_fee_agg ON #fee_agg (amazon_order_id, sku)")
    conn.commit()

    cur.execute("IF OBJECT_ID('tempdb..#fee_order_agg') IS NOT NULL DROP TABLE #fee_order_agg")
    conn.commit()

    cur.execute(f"""
        SELECT
            df.amazon_order_id,
            SUM(CASE WHEN df.charge_type IN ({fba_in})
                THEN ABS(df.amount) * ISNULL(fx.rate_to_pln,
                    {_fx_case('df.currency')})
                ELSE 0 END) AS fba_pln,
            SUM(CASE WHEN df.charge_type IN ({ref_in})
                THEN ABS(df.amount) * ISNULL(fx.rate_to_pln,
                    {_fx_case('df.currency')})
                ELSE 0 END) AS ref_pln
        INTO #fee_order_agg
        FROM (
            SELECT
                ft.amazon_order_id,
                ft.charge_type,
                ft.amount,
                ft.currency,
                ft.posted_date,
                ROW_NUMBER() OVER (
                    PARTITION BY ft.amazon_order_id, ft.sku,
                                 ft.charge_type, ft.amount, ft.posted_date
                    ORDER BY ft.synced_at DESC
                ) AS rn
            FROM acc_finance_transaction ft WITH (NOLOCK)
            WHERE ft.charge_type IN ({all_in})
              AND ft.amazon_order_id IS NOT NULL
        ) df
        OUTER APPLY (
            SELECT TOP 1 rate_to_pln FROM acc_exchange_rate er WITH (NOLOCK)
            WHERE er.currency = df.currency
              AND er.rate_date <= CAST(df.posted_date AS DATE)
            ORDER BY er.rate_date DESC
        ) fx
        WHERE df.rn = 1
        GROUP BY df.amazon_order_id
        HAVING SUM(CASE WHEN df.charge_type IN ({fba_in})
                THEN ABS(df.amount) ELSE 0 END) > 0
            OR SUM(CASE WHEN df.charge_type IN ({ref_in})
                THEN ABS(df.amount) ELSE 0 END) > 0
    """)
    conn.commit()

    cur.execute("CREATE CLUSTERED INDEX IX_fee_order_agg ON #fee_order_agg (amazon_order_id)")
    conn.commit()

    # Step B: Stamp fees on order lines (only where currently NULL)
    # Distributes proportionally among multiple lines with the same SKU
    cur.execute("""
        UPDATE ol
        SET ol.fba_fee_pln = CASE
                WHEN fa.fba_pln > 0
                THEN ROUND(fa.fba_pln
                     * (CAST(ol.quantity_ordered AS DECIMAL(18,6))
                        / NULLIF(sku_qty.total_qty, 0)), 4)
                ELSE NULL
            END,
            ol.referral_fee_pln = CASE
                WHEN fa.ref_pln > 0
                THEN ROUND(fa.ref_pln
                     * (CAST(ol.quantity_ordered AS DECIMAL(18,6))
                        / NULLIF(sku_qty.total_qty, 0)), 4)
                ELSE NULL
            END
        FROM acc_order_line ol
        INNER JOIN acc_order o ON o.id = ol.order_id
        INNER JOIN #fee_agg fa
            ON fa.amazon_order_id = o.amazon_order_id AND fa.sku = ol.sku
        CROSS APPLY (
            SELECT SUM(ol2.quantity_ordered) AS total_qty
            FROM acc_order_line ol2 WITH (NOLOCK)
            WHERE ol2.order_id = o.id
              AND ol2.sku = ol.sku
              AND ol2.quantity_ordered > 0
        ) sku_qty
        WHERE ol.fba_fee_pln IS NULL
          AND ol.quantity_ordered > 0
          AND sku_qty.total_qty > 0
          AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
          AND o.amazon_order_id NOT LIKE 'S02-%'
    """)
    lines_updated = cur.rowcount or 0
    conn.commit()

    # Step B2: Registry/internal_sku fallback for finance rows where sku differs
    cur.execute("IF OBJECT_ID('tempdb..#fee_agg_registry') IS NOT NULL DROP TABLE #fee_agg_registry")
    conn.commit()

    cur.execute("""
        SELECT
            fa.amazon_order_id,
            rg.internal_sku,
            SUM(fa.fba_pln) AS fba_pln,
            SUM(fa.ref_pln) AS ref_pln
        INTO #fee_agg_registry
        FROM #fee_agg fa
        OUTER APPLY (
            SELECT TOP 1 r.internal_sku
            FROM dbo.acc_amazon_listing_registry r WITH (NOLOCK)
            WHERE fa.sku IS NOT NULL
              AND (r.merchant_sku = fa.sku OR r.merchant_sku_alt = fa.sku)
              AND ISNULL(r.internal_sku, '') <> ''
            ORDER BY CASE
                WHEN r.merchant_sku = fa.sku THEN 0
                WHEN r.merchant_sku_alt = fa.sku THEN 1
                ELSE 2
            END
        ) rg
        WHERE ISNULL(rg.internal_sku, '') <> ''
        GROUP BY fa.amazon_order_id, rg.internal_sku
    """)
    conn.commit()

    cur.execute("CREATE CLUSTERED INDEX IX_fee_agg_registry ON #fee_agg_registry (amazon_order_id, internal_sku)")
    conn.commit()

    cur.execute("""
        UPDATE ol
        SET ol.fba_fee_pln = CASE
                WHEN (ol.fba_fee_pln IS NULL OR ol.fba_fee_pln = 0) AND fr.fba_pln > 0
                THEN ROUND(fr.fba_pln
                     * (CAST(ol.quantity_ordered AS DECIMAL(18,6))
                        / NULLIF(sku_qty.total_qty, 0)), 4)
                ELSE ol.fba_fee_pln
            END,
            ol.referral_fee_pln = CASE
                WHEN (ol.referral_fee_pln IS NULL OR ol.referral_fee_pln = 0) AND fr.ref_pln > 0
                THEN ROUND(fr.ref_pln
                     * (CAST(ol.quantity_ordered AS DECIMAL(18,6))
                        / NULLIF(sku_qty.total_qty, 0)), 4)
                ELSE ol.referral_fee_pln
            END
        FROM acc_order_line ol
        INNER JOIN acc_order o ON o.id = ol.order_id
        INNER JOIN acc_product p ON p.id = ol.product_id
        INNER JOIN #fee_agg_registry fr
            ON fr.amazon_order_id = o.amazon_order_id
           AND fr.internal_sku = p.internal_sku
        CROSS APPLY (
            SELECT SUM(ol2.quantity_ordered) AS total_qty
            FROM acc_order_line ol2 WITH (NOLOCK)
            INNER JOIN acc_product p2 WITH (NOLOCK) ON p2.id = ol2.product_id
            WHERE ol2.order_id = o.id
              AND p2.internal_sku = p.internal_sku
              AND ol2.quantity_ordered > 0
        ) sku_qty
        WHERE ((ol.fba_fee_pln IS NULL OR ol.fba_fee_pln = 0)
           OR (ol.referral_fee_pln IS NULL OR ol.referral_fee_pln = 0))
          AND ol.quantity_ordered > 0
          AND sku_qty.total_qty > 0
          AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
          AND o.amazon_order_id NOT LIKE 'S02-%'
    """)
    lines_updated += cur.rowcount or 0
    conn.commit()

    # Step B3: Order-level residual allocation for finance rows without usable sku
    cur.execute("""
        WITH residuals AS (
            SELECT
                oa.amazon_order_id,
                ROUND(oa.fba_pln - ISNULL(stamped.fba_pln, 0), 4) AS residual_fba_pln,
                ROUND(oa.ref_pln - ISNULL(stamped.ref_pln, 0), 4) AS residual_ref_pln
            FROM #fee_order_agg oa
            OUTER APPLY (
                SELECT
                    SUM(ISNULL(ol.fba_fee_pln, 0)) AS fba_pln,
                    SUM(ISNULL(ol.referral_fee_pln, 0)) AS ref_pln
                FROM acc_order_line ol WITH (NOLOCK)
                INNER JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
                WHERE o.amazon_order_id = oa.amazon_order_id
            ) stamped
        )
        UPDATE ol
        SET ol.fba_fee_pln = CASE
                WHEN (ol.fba_fee_pln IS NULL OR ol.fba_fee_pln = 0)
                 AND resid.residual_fba_pln > 0
                 AND ISNULL(o.fulfillment_channel, '') = 'AFN'
                THEN ROUND(
                    resid.residual_fba_pln
                    * (
                        CASE
                            WHEN weights.total_weight > 0
                                THEN weights.line_weight / weights.total_weight
                            ELSE CAST(ol.quantity_ordered AS DECIMAL(18,6)) / NULLIF(weights.total_qty, 0)
                        END
                    ),
                    4
                )
                ELSE ol.fba_fee_pln
            END,
            ol.referral_fee_pln = CASE
                WHEN (ol.referral_fee_pln IS NULL OR ol.referral_fee_pln = 0)
                 AND resid.residual_ref_pln > 0
                THEN ROUND(
                    resid.residual_ref_pln
                    * (
                        CASE
                            WHEN weights.total_weight > 0
                                THEN weights.line_weight / weights.total_weight
                            ELSE CAST(ol.quantity_ordered AS DECIMAL(18,6)) / NULLIF(weights.total_qty, 0)
                        END
                    ),
                    4
                )
                ELSE ol.referral_fee_pln
            END
        FROM acc_order_line ol
        INNER JOIN acc_order o ON o.id = ol.order_id
        INNER JOIN residuals resid ON resid.amazon_order_id = o.amazon_order_id
        CROSS APPLY (
            SELECT
                CASE
                    WHEN ISNULL(ol.item_price, 0) > 0
                        THEN CAST(ISNULL(ol.item_price, 0) * CAST(ol.quantity_ordered AS DECIMAL(18,6)) AS DECIMAL(18,6))
                    ELSE CAST(ol.quantity_ordered AS DECIMAL(18,6))
                END AS line_weight,
                SUM(CASE
                        WHEN ISNULL(ol2.item_price, 0) > 0
                            THEN CAST(ISNULL(ol2.item_price, 0) * CAST(ol2.quantity_ordered AS DECIMAL(18,6)) AS DECIMAL(18,6))
                        ELSE CAST(ol2.quantity_ordered AS DECIMAL(18,6))
                    END) AS total_weight,
                SUM(CAST(ol2.quantity_ordered AS DECIMAL(18,6))) AS total_qty
            FROM acc_order_line ol2 WITH (NOLOCK)
            WHERE ol2.order_id = o.id
              AND ol2.quantity_ordered > 0
              AND (
                    (ol2.referral_fee_pln IS NULL OR ol2.referral_fee_pln = 0)
                 OR (ISNULL(o.fulfillment_channel, '') = 'AFN' AND (ol2.fba_fee_pln IS NULL OR ol2.fba_fee_pln = 0))
              )
        ) weights
        WHERE ol.quantity_ordered > 0
          AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
          AND o.amazon_order_id NOT LIKE 'S02-%'
          AND (
                ((ol.referral_fee_pln IS NULL OR ol.referral_fee_pln = 0) AND resid.residual_ref_pln > 0)
             OR ((ol.fba_fee_pln IS NULL OR ol.fba_fee_pln = 0) AND resid.residual_fba_pln > 0 AND ISNULL(o.fulfillment_channel, '') = 'AFN')
          )
          AND weights.total_qty > 0
    """)
    lines_updated += cur.rowcount or 0
    conn.commit()

    # ── Step B3.5: Rate-based estimation for lines still missing fees ───
    # When finance transactions are missing for a marketplace (common for
    # NL / PL / SE / BE), impute fees using observed rates from lines that
    # DO have fees.  Priority: per (marketplace, product) → marketplace avg.
    fx_sql = _fx_case("o.currency")

    # ── 1) Referral rate per (marketplace, product) ──
    cur.execute("IF OBJECT_ID('tempdb..#est_ref_prod') IS NOT NULL DROP TABLE #est_ref_prod")
    cur.execute("IF OBJECT_ID('tempdb..#est_ref_mkt') IS NOT NULL DROP TABLE #est_ref_mkt")
    cur.execute("IF OBJECT_ID('tempdb..#est_fba_prod') IS NOT NULL DROP TABLE #est_fba_prod")
    cur.execute("IF OBJECT_ID('tempdb..#est_fba_mkt') IS NOT NULL DROP TABLE #est_fba_mkt")
    conn.commit()

    cur.execute(f"""
        SELECT
            o.marketplace_id,
            ol.product_id,
            CASE WHEN COUNT(*) >= 3
                 THEN AVG(
                    ol.referral_fee_pln * 1.0
                    / NULLIF(
                        (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0))
                        * ol.quantity_ordered
                        * {fx_sql}, 0)
                 )
                 ELSE NULL
            END AS ref_rate
        INTO #est_ref_prod
        FROM acc_order_line ol WITH (NOLOCK)
        JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        WHERE ol.referral_fee_pln > 0
          AND (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0)) > 0
          AND ol.quantity_ordered > 0
          AND o.purchase_date >= DATEADD(month, -6, GETDATE())
          AND o.status = 'Shipped'
        GROUP BY o.marketplace_id, ol.product_id
    """)
    conn.commit()

    cur.execute("CREATE NONCLUSTERED INDEX IX_erp ON #est_ref_prod (marketplace_id, product_id)")
    conn.commit()

    # ── 2) Marketplace-level referral rate fallback ──
    cur.execute(f"""
        SELECT
            o.marketplace_id,
            AVG(
                ol.referral_fee_pln * 1.0
                / NULLIF(
                    (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0))
                    * ol.quantity_ordered
                    * {fx_sql}, 0)
            ) AS ref_rate_mkt
        INTO #est_ref_mkt
        FROM acc_order_line ol WITH (NOLOCK)
        JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        WHERE ol.referral_fee_pln > 0
          AND (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0)) > 0
          AND ol.quantity_ordered > 0
          AND o.purchase_date >= DATEADD(month, -6, GETDATE())
          AND o.status = 'Shipped'
        GROUP BY o.marketplace_id
    """)
    conn.commit()

    cur.execute("CREATE NONCLUSTERED INDEX IX_erm ON #est_ref_mkt (marketplace_id)")
    conn.commit()

    # ── 3) FBA fee per unit per (marketplace, product) ──
    cur.execute("""
        SELECT
            o.marketplace_id,
            ol.product_id,
            CASE WHEN COUNT(*) >= 3
                 THEN AVG(ol.fba_fee_pln * 1.0 / NULLIF(ol.quantity_ordered, 0))
                 ELSE NULL
            END AS fba_per_unit
        INTO #est_fba_prod
        FROM acc_order_line ol WITH (NOLOCK)
        JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        WHERE ol.fba_fee_pln > 0
          AND ISNULL(o.fulfillment_channel, '') = 'AFN'
          AND ol.quantity_ordered > 0
          AND o.purchase_date >= DATEADD(month, -6, GETDATE())
          AND o.status = 'Shipped'
        GROUP BY o.marketplace_id, ol.product_id
    """)
    conn.commit()

    cur.execute("CREATE NONCLUSTERED INDEX IX_efp ON #est_fba_prod (marketplace_id, product_id)")
    conn.commit()

    # ── 4) Marketplace-level FBA fee per unit fallback ──
    cur.execute("""
        SELECT
            o.marketplace_id,
            AVG(ol.fba_fee_pln * 1.0 / NULLIF(ol.quantity_ordered, 0)) AS fba_per_unit_mkt
        INTO #est_fba_mkt
        FROM acc_order_line ol WITH (NOLOCK)
        JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        WHERE ol.fba_fee_pln > 0
          AND ISNULL(o.fulfillment_channel, '') = 'AFN'
          AND ol.quantity_ordered > 0
          AND o.purchase_date >= DATEADD(month, -6, GETDATE())
          AND o.status = 'Shipped'
        GROUP BY o.marketplace_id
    """)
    conn.commit()

    cur.execute("CREATE NONCLUSTERED INDEX IX_efm ON #est_fba_mkt (marketplace_id)")
    conn.commit()

    # ── 5) Apply referral fee estimation ──
    cur.execute(f"""
        UPDATE ol
        SET ol.referral_fee_pln = ROUND(
            (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0))
            * ol.quantity_ordered
            * {fx_sql}
            * COALESCE(erp.ref_rate, erm.ref_rate_mkt, 0.15),
            4
        )
        FROM acc_order_line ol
        JOIN acc_order o ON o.id = ol.order_id
        LEFT JOIN #est_ref_prod erp
            ON erp.marketplace_id = o.marketplace_id
           AND erp.product_id = ol.product_id
        LEFT JOIN #est_ref_mkt erm
            ON erm.marketplace_id = o.marketplace_id
        WHERE (ol.referral_fee_pln IS NULL OR ol.referral_fee_pln = 0)
          AND (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0)) > 0
          AND ol.quantity_ordered > 0
          AND o.status = 'Shipped'
          AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
          AND o.amazon_order_id NOT LIKE 'S02-%'
    """)
    est_ref_updated = cur.rowcount or 0
    conn.commit()

    # ── 6) Apply FBA fee estimation (AFN only) ──
    cur.execute("""
        UPDATE ol
        SET ol.fba_fee_pln = ROUND(
            ol.quantity_ordered
            * COALESCE(efp.fba_per_unit, efm.fba_per_unit_mkt),
            4
        )
        FROM acc_order_line ol
        JOIN acc_order o ON o.id = ol.order_id
        LEFT JOIN #est_fba_prod efp
            ON efp.marketplace_id = o.marketplace_id
           AND efp.product_id = ol.product_id
        LEFT JOIN #est_fba_mkt efm
            ON efm.marketplace_id = o.marketplace_id
        WHERE (ol.fba_fee_pln IS NULL OR ol.fba_fee_pln = 0)
          AND ISNULL(o.fulfillment_channel, '') = 'AFN'
          AND ol.quantity_ordered > 0
          AND o.status = 'Shipped'
          AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
          AND o.amazon_order_id NOT LIKE 'S02-%'
          AND COALESCE(efp.fba_per_unit, efm.fba_per_unit_mkt) IS NOT NULL
    """)
    est_fba_updated = cur.rowcount or 0
    conn.commit()
    lines_updated += est_ref_updated + est_fba_updated

    log.info(
        "pipeline.step5_1_rate_estimation",
        est_ref_lines=est_ref_updated,
        est_fba_lines=est_fba_updated,
    )

    # Cleanup estimation temp tables
    cur.execute("IF OBJECT_ID('tempdb..#est_ref_prod') IS NOT NULL DROP TABLE #est_ref_prod")
    cur.execute("IF OBJECT_ID('tempdb..#est_ref_mkt') IS NOT NULL DROP TABLE #est_ref_mkt")
    cur.execute("IF OBJECT_ID('tempdb..#est_fba_prod') IS NOT NULL DROP TABLE #est_fba_prod")
    cur.execute("IF OBJECT_ID('tempdb..#est_fba_mkt') IS NOT NULL DROP TABLE #est_fba_mkt")
    conn.commit()

    # Step B4: Persist direct-order CM1 costs on acc_order exactly once.
    cur.execute("IF OBJECT_ID('tempdb..#cm1_order_fee_agg') IS NOT NULL DROP TABLE #cm1_order_fee_agg")
    conn.commit()

    ship_in = _bridge_order_shipping_in_clause()
    promo_in = _bridge_order_promo_in_clause()
    refund_commission_in = _bridge_order_refund_commission_in_clause()
    direct_all_in = _bridge_order_all_in_clause()

    cur.execute(f"""
        SELECT
            df.amazon_order_id,
            SUM(CASE WHEN df.charge_type IN ({ship_in})
                THEN ABS(df.amount) * ISNULL(fx.rate_to_pln,
                    {_fx_case('df.currency')})
                ELSE 0 END) AS shipping_surcharge_pln,
            SUM(CASE WHEN df.charge_type IN ({promo_in})
                THEN ABS(df.amount) * ISNULL(fx.rate_to_pln,
                    {_fx_case('df.currency')})
                ELSE 0 END) AS promo_order_fee_pln,
            SUM(CASE WHEN df.charge_type IN ({refund_commission_in})
                THEN ABS(df.amount) * ISNULL(fx.rate_to_pln,
                    {_fx_case('df.currency')})
                ELSE 0 END) AS refund_commission_pln
        INTO #cm1_order_fee_agg
        FROM (
            SELECT
                ft.amazon_order_id,
                ft.sku,
                ft.charge_type,
                ft.amount,
                ft.currency,
                ft.posted_date,
                ROW_NUMBER() OVER (
                    PARTITION BY ft.amazon_order_id, ft.sku,
                                 ft.charge_type, ft.amount, ft.posted_date
                    ORDER BY ft.synced_at DESC
                ) AS rn
            FROM acc_finance_transaction ft WITH (NOLOCK)
            WHERE ft.charge_type IN ({direct_all_in})
              AND ft.amazon_order_id IS NOT NULL
        ) df
        OUTER APPLY (
            SELECT TOP 1 rate_to_pln FROM acc_exchange_rate er WITH (NOLOCK)
            WHERE er.currency = df.currency
              AND er.rate_date <= CAST(df.posted_date AS DATE)
            ORDER BY er.rate_date DESC
        ) fx
        WHERE df.rn = 1
        GROUP BY df.amazon_order_id
        HAVING SUM(CASE WHEN df.charge_type IN ({ship_in})
                THEN ABS(df.amount) ELSE 0 END) > 0
            OR SUM(CASE WHEN df.charge_type IN ({promo_in})
                THEN ABS(df.amount) ELSE 0 END) > 0
            OR SUM(CASE WHEN df.charge_type IN ({refund_commission_in})
                THEN ABS(df.amount) ELSE 0 END) > 0
    """)
    conn.commit()

    cur.execute("CREATE CLUSTERED INDEX IX_cm1_order_fee_agg ON #cm1_order_fee_agg (amazon_order_id)")
    conn.commit()

    cur.execute("""
        UPDATE o
        SET o.shipping_surcharge_pln = ROUND(ISNULL(agg.shipping_surcharge_pln, 0), 2),
            o.promo_order_fee_pln = ROUND(ISNULL(agg.promo_order_fee_pln, 0), 2),
            o.refund_commission_pln = ROUND(ISNULL(agg.refund_commission_pln, 0), 2)
        FROM acc_order o
        INNER JOIN #cm1_order_fee_agg agg
            ON agg.amazon_order_id = o.amazon_order_id
        WHERE ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
          AND o.amazon_order_id NOT LIKE 'S02-%'
          AND (
                ISNULL(o.shipping_surcharge_pln, 0) != ROUND(ISNULL(agg.shipping_surcharge_pln, 0), 2)
             OR ISNULL(o.promo_order_fee_pln, 0) != ROUND(ISNULL(agg.promo_order_fee_pln, 0), 2)
             OR ISNULL(o.refund_commission_pln, 0) != ROUND(ISNULL(agg.refund_commission_pln, 0), 2)
          )
    """)
    orders_direct_updated = cur.rowcount or 0
    conn.commit()

    cur.execute("IF OBJECT_ID('tempdb..#fee_agg') IS NOT NULL DROP TABLE #fee_agg")
    cur.execute("IF OBJECT_ID('tempdb..#fee_agg_registry') IS NOT NULL DROP TABLE #fee_agg_registry")
    cur.execute("IF OBJECT_ID('tempdb..#fee_order_agg') IS NOT NULL DROP TABLE #fee_order_agg")
    cur.execute("IF OBJECT_ID('tempdb..#cm1_order_fee_agg') IS NOT NULL DROP TABLE #cm1_order_fee_agg")
    conn.commit()

    # Step C: aggregate line fees + direct order CM1 fees → order amazon_fees_pln
    cur.execute("""
        UPDATE o
        SET o.amazon_fees_pln = calc.total_fees
        FROM acc_order o
        OUTER APPLY (
            SELECT SUM(ISNULL(ol.fba_fee_pln, 0) + ISNULL(ol.referral_fee_pln, 0))
                   AS total_fees
            FROM acc_order_line ol WHERE ol.order_id = o.id
        ) agg
        CROSS APPLY (
            SELECT ROUND(
                ISNULL(agg.total_fees, 0)
                + ISNULL(o.shipping_surcharge_pln, 0)
                + ISNULL(o.promo_order_fee_pln, 0)
                + ISNULL(o.refund_commission_pln, 0),
                2
            ) AS total_fees
        ) calc
        WHERE calc.total_fees > 0
          AND (o.amazon_fees_pln IS NULL
               OR o.amazon_fees_pln = 0
               OR o.amazon_fees_pln != calc.total_fees)
    """)
    orders_updated = cur.rowcount or 0
    conn.commit()

    cur.close()
    conn.close()
    log.info(
        "pipeline.step5_1_done",
        lines=lines_updated,
        orders=orders_updated,
        direct_orders=orders_direct_updated,
    )
    return lines_updated


# ──────────────────────────────────────────────────────────────────
# Step 5.95: Sync courier costs for FBM (MFN) orders
# ──────────────────────────────────────────────────────────────────

# DHL_Costs batch size — never send more than this many JJD numbers
# against the unindexed 13M-row ITJK_DHL_Costs table in one query.
_DHL_BATCH_SIZE = 50


def step_sync_courier_costs(days_back: int = 30) -> dict:
    """
    Map FBM courier costs (DHL / GLS) → acc_order.logistics_pln.

    SAFETY: Uses temp table approach instead of CROSS APPLY to avoid
    runaway full-table scans.  DHL JJD lookups are batched (50 at a
    time) because ITJK_DHL_Costs has 13M rows and NO indexes.

    Matching chain:
      acc_order.amazon_order_id
      → ITJK_ZamowieniaBaselinkerAPI.external_order_id  (BL order / tracking)
      → tracking number (delivery_package_nr)
      → courier invoice cost

    GLS path  – direct:  parcel_num = tracking in CouriersInvoicesDetails
    GLS alt   – reverse: note1 = BL order_id    in CouriersInvoicesDetails
    DHL path  – short:   parcel_num = tracking  in CouriersInvoicesDetails
    DHL path  – JJD:     parcel_num_other = tracking in DHL_Costs (batched)
    """
    log.info("pipeline.courier_costs.removed")
    return {"removed": True, "reason": "legacy_courier_cost_mapping_removed"}

    from datetime import date as _date

    stats = {"gls_direct": 0, "gls_note1": 0, "dhl_short": 0, "dhl_jjd": 0,
             "total": 0, "errors": 0, "bl_matched": 0}

    conn = _db_conn(timeout=QUERY_TIMEOUT_SECONDS)
    cur = conn.cursor()
    # Use isoformat() string for date — avoids ODBC driver parameter
    # binding issues with the old {SQL Server} driver.
    date_cutoff = (_date.today() - timedelta(days=days_back)).isoformat()

    try:
        # ── Build temp table: BL tracking for unmatched MFN orders ──
        cur.execute("IF OBJECT_ID('tempdb..#bl_track') IS NOT NULL DROP TABLE #bl_track")
        cur.execute(f"""
            SELECT
                o.id            AS acc_id,
                bl.delivery_package_nr AS tracking,
                bl.order_id     AS bl_id
            INTO #bl_track
            FROM acc_order o WITH (NOLOCK)
            JOIN (
                SELECT external_order_id,
                       MIN(delivery_package_nr) AS delivery_package_nr,
                       MIN(order_id)            AS order_id
                FROM ITJK_ZamowieniaBaselinkerAPI WITH (NOLOCK)
                WHERE order_source = 'amazon'
                  AND delivery_package_nr IS NOT NULL
                  AND delivery_package_nr != ''
                GROUP BY external_order_id
            ) bl ON bl.external_order_id = o.amazon_order_id
            WHERE o.fulfillment_channel = 'MFN'
              AND o.logistics_pln IS NULL
              AND CAST(o.purchase_date AS DATE) >= '{date_cutoff}'
        """)
        conn.commit()

        cur.execute("CREATE INDEX ix_bt_track ON #bl_track(tracking)")
        cur.execute("CREATE INDEX ix_bt_blid  ON #bl_track(bl_id)")
        cur.execute("CREATE INDEX ix_bt_accid ON #bl_track(acc_id)")
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM #bl_track")
        stats["bl_matched"] = cur.fetchone()[0]
        log.info("courier_costs.temp_table", bl_matched=stats["bl_matched"])

        if stats["bl_matched"] == 0:
            log.info("courier_costs.skip_no_bl_matches")
            cur.execute("DROP TABLE #bl_track")
            conn.commit()
            return stats

        # ── Pass 1: GLS direct (parcel_num = tracking) ─────────────
        cur.execute("""
            UPDATE o
            SET o.logistics_pln = ci_agg.total_cost
            FROM acc_order o
            JOIN #bl_track bt ON bt.acc_id = o.id
            JOIN (
                SELECT ci.parcel_num,
                       SUM(ci.netto + ci.toll + ci.fuel_surcharge) AS total_cost
                FROM ITJK_CouriersInvoicesDetails ci WITH (NOLOCK)
                WHERE ci.Courier = 'GLS'
                GROUP BY ci.parcel_num
            ) ci_agg ON ci_agg.parcel_num = bt.tracking
            WHERE ci_agg.total_cost > 0
        """)
        stats["gls_direct"] = cur.rowcount or 0
        conn.commit()
        log.info("courier_costs.gls_direct", matched=stats["gls_direct"])

        # ── Pass 2: GLS note1 fallback (note1 = BL order_id) ──────
        cur.execute("""
            UPDATE o
            SET o.logistics_pln = ci_agg.total_cost
            FROM acc_order o
            JOIN #bl_track bt ON bt.acc_id = o.id
            JOIN (
                SELECT ci.note1,
                       SUM(ci.netto + ci.toll + ci.fuel_surcharge) AS total_cost
                FROM ITJK_CouriersInvoicesDetails ci WITH (NOLOCK)
                WHERE ci.Courier = 'GLS'
                  AND ci.note1 IS NOT NULL AND ci.note1 != ''
                GROUP BY ci.note1
            ) ci_agg ON ci_agg.note1 = CAST(bt.bl_id AS VARCHAR(20))
            WHERE o.logistics_pln IS NULL
              AND ci_agg.total_cost > 0
        """)
        stats["gls_note1"] = cur.rowcount or 0
        conn.commit()
        log.info("courier_costs.gls_note1", matched=stats["gls_note1"])

        # ── Pass 3: DHL short (non-JJD tracking in CouriersInvoicesDetails) ─
        cur.execute("""
            UPDATE o
            SET o.logistics_pln = ci_agg.total_cost
            FROM acc_order o
            JOIN #bl_track bt ON bt.acc_id = o.id
            JOIN (
                SELECT ci.parcel_num,
                       SUM(ci.netto + ci.toll + ci.fuel_surcharge) AS total_cost
                FROM ITJK_CouriersInvoicesDetails ci WITH (NOLOCK)
                WHERE ci.Courier = 'DHL'
                GROUP BY ci.parcel_num
            ) ci_agg ON ci_agg.parcel_num = bt.tracking
            WHERE o.logistics_pln IS NULL
              AND bt.tracking NOT LIKE 'JJD%'
              AND ci_agg.total_cost > 0
        """)
        stats["dhl_short"] = cur.rowcount or 0
        conn.commit()
        log.info("courier_costs.dhl_short", matched=stats["dhl_short"])

        # ── Pass 4: DHL JJD — BATCHED (ITJK_DHL_Costs is 13M, no index) ─
        # Collect JJD tracking numbers still unmatched, process in small
        # batches to never overload the unindexed table.
        cur.execute("""
            SELECT bt.acc_id, bt.tracking
            FROM #bl_track bt
            JOIN acc_order o WITH (NOLOCK) ON o.id = bt.acc_id
            WHERE o.logistics_pln IS NULL
              AND bt.tracking LIKE 'JJD%'
        """)
        jjd_rows = cur.fetchall()
        log.info("courier_costs.dhl_jjd_pending", count=len(jjd_rows))

        jjd_updated = 0
        for i in range(0, len(jjd_rows), _DHL_BATCH_SIZE):
            batch = jjd_rows[i:i + _DHL_BATCH_SIZE]
            # Build a temp VALUES list for this batch
            placeholders = ",".join(["(?)"] * len(batch))
            tracking_vals = [r[1] for r in batch]

            # Look up costs for this batch from DHL_Costs
            sql = f"""
                SELECT dc.parcel_num_other,
                       SUM(dc.netto + dc.toll + dc.fuel_surcharge) AS total_cost
                FROM ITJK_DHL_Costs dc WITH (NOLOCK)
                WHERE dc.parcel_num_other IN ({','.join(['?'] * len(batch))})
                GROUP BY dc.parcel_num_other
                HAVING SUM(dc.netto + dc.toll + dc.fuel_surcharge) > 0
            """
            try:
                cur.execute(sql, *tracking_vals)
                cost_map = {row[0]: float(row[1]) for row in cur.fetchall()}
            except Exception as e:
                log.warning("courier_costs.dhl_jjd_batch_err",
                            batch_start=i, error=str(e))
                continue

            # Update matched orders one by one (safe, controlled)
            for acc_id, tracking in batch:
                if tracking in cost_map:
                    cur.execute(
                        "UPDATE acc_order SET logistics_pln = ? "
                        "WHERE id = ? AND logistics_pln IS NULL",
                        cost_map[tracking], acc_id,
                    )
                    jjd_updated += cur.rowcount or 0
            conn.commit()

        stats["dhl_jjd"] = jjd_updated
        log.info("courier_costs.dhl_jjd", matched=stats["dhl_jjd"])

        # Cleanup temp table
        cur.execute("DROP TABLE #bl_track")
        conn.commit()

        stats["total"] = (stats["gls_direct"] + stats["gls_note1"]
                          + stats["dhl_short"] + stats["dhl_jjd"])

    except pyodbc.Error as e:
        stats["errors"] += 1
        log.error("courier_costs.sql_error", error=str(e),
                  sqlstate=getattr(e, 'args', [None])[0] if e.args else None)
        try:
            conn.rollback()
        except Exception:
            pass
    except Exception as e:
        stats["errors"] += 1
        log.error("courier_costs.error", error=str(e))
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        try:
            cur.execute("IF OBJECT_ID('tempdb..#bl_track') IS NOT NULL DROP TABLE #bl_track")
            conn.commit()
        except Exception:
            pass
        cur.close()
        conn.close()

    log.info("courier_costs.done", **stats)
    return stats


# ──────────────────────────────────────────────────────────────────
# Step 5.97: Detect refunded orders from finance transactions
# ──────────────────────────────────────────────────────────────────
def step_detect_refunds(timeout: int = 300) -> dict:
    """
    Detect refunded orders by analysing acc_finance_transaction.

    Logic:
      1. Find all orders that have RefundEventList transactions
         but are NOT yet flagged (is_refund IS NULL or is_refund = 0).
      2. For each, compute net principal = SUM(Principal across all
         ShipmentEventList + RefundEventList).
      3. If |net_principal| < 0.05 → 'full' refund.
         Otherwise → 'partial' refund.
      4. Store refund_amount_eur (the total refunded Principal, negative),
         refund_amount_pln (× exchange_rate), refund_date (MAX posted_date
         of refund txns), and refund_type.

    Also marks orders with status='Return' as is_refund=1 even if
    no finance transactions exist yet (Amazon may set Return status
    before we sync the finance event).

    Returns {"flagged": N, "full": N, "partial": N, "status_return": N}.
    """
    conn = _db_conn(timeout=timeout)
    cur = conn.cursor()

    stats = {"flagged": 0, "full": 0, "partial": 0, "status_return": 0}

    try:
        # ── Pass 1: Flag orders with RefundEventList finance transactions ──
        # Uses a single UPDATE … FROM pattern for efficiency.
        cur.execute(f"""
            SET LOCK_TIMEOUT 30000;

            WITH refund_summary AS (
                SELECT
                    ft.amazon_order_id,
                    -- net principal across shipment + refund events
                    SUM(CASE WHEN ft.charge_type = 'Principal'
                             THEN ft.amount ELSE 0 END)         AS net_principal_eur,
                    -- total refunded principal only (negative)
                    SUM(CASE WHEN ft.transaction_type = 'RefundEventList'
                              AND ft.charge_type = 'Principal'
                             THEN ft.amount ELSE 0 END)         AS refund_principal_eur,
                    -- refund commission cost (negative)
                    SUM(CASE WHEN ft.transaction_type = 'RefundEventList'
                              AND ft.charge_type = 'RefundCommission'
                             THEN ft.amount ELSE 0 END)         AS refund_commission_eur,
                    -- latest refund date
                    MAX(CASE WHEN ft.transaction_type = 'RefundEventList'
                             THEN ft.posted_date END)           AS refund_date,
                    -- average exchange rate from refund transactions
                    AVG(CASE WHEN ft.transaction_type = 'RefundEventList'
                              AND ft.exchange_rate IS NOT NULL
                              AND ft.exchange_rate > 0
                             THEN ft.exchange_rate END)         AS avg_fx
                FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
                WHERE ft.transaction_type IN ('ShipmentEventList', 'RefundEventList')
                  AND ft.amazon_order_id IN (
                      SELECT DISTINCT amazon_order_id
                      FROM dbo.acc_finance_transaction WITH (NOLOCK)
                      WHERE transaction_type = 'RefundEventList'
                  )
                GROUP BY ft.amazon_order_id
            )
            UPDATE o
            SET o.is_refund = 1,
                o.refund_amount_eur = rs.refund_principal_eur,
                o.refund_amount_pln = rs.refund_principal_eur
                    * ISNULL(rs.avg_fx, ISNULL(
                        (SELECT TOP 1 er.rate_to_pln
                         FROM dbo.acc_exchange_rate er WITH (NOLOCK)
                         WHERE er.currency = o.currency
                           AND er.rate_date <= CAST(rs.refund_date AS DATE)
                         ORDER BY er.rate_date DESC),
                        {_fx_case('o.currency')})),
                o.refund_date = rs.refund_date,
                o.refund_type = CASE
                    WHEN ABS(rs.net_principal_eur) < 0.05 THEN 'full'
                    ELSE 'partial'
                END
            FROM dbo.acc_order o
            INNER JOIN refund_summary rs
                ON rs.amazon_order_id = o.amazon_order_id
            WHERE ISNULL(o.is_refund, 0) = 0
        """)
        flagged_finance = cur.rowcount or 0
        conn.commit()
        stats["flagged"] += flagged_finance
        log.info("detect_refunds.pass1_finance", flagged=flagged_finance)

        # Count full vs partial among newly flagged
        cur.execute("""
            SELECT refund_type, COUNT(*)
            FROM dbo.acc_order WITH (NOLOCK)
            WHERE is_refund = 1 AND refund_type IS NOT NULL
            GROUP BY refund_type
        """)
        for row in cur.fetchall():
            if row[0] == 'full':
                stats["full"] = row[1]
            elif row[0] == 'partial':
                stats["partial"] = row[1]

        # ── Pass 2: Flag orders with status='Return' that lack finance txns ──
        cur.execute("""
            UPDATE dbo.acc_order
            SET is_refund = 1,
                refund_type = ISNULL(refund_type, 'full'),
                refund_date = ISNULL(refund_date, last_update_date)
            WHERE status = 'Return'
              AND ISNULL(is_refund, 0) = 0
        """)
        status_flagged = cur.rowcount or 0
        conn.commit()
        stats["status_return"] = status_flagged
        stats["flagged"] += status_flagged
        log.info("detect_refunds.pass2_status", flagged=status_flagged)

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    log.info("detect_refunds.done", **stats)
    return stats


# ──────────────────────────────────────────────────────────────────
# Step 6: Recalculate profit / contribution margin
# ──────────────────────────────────────────────────────────────────
async def step_calc_profit(days_back: int = 7) -> int:
    """
    Recalculate CM1 for recent orders.
    Uses wider window (7 days) to catch orders whose prices
    were stamped after the original calc_profit ran.
    """
    from app.connectors.mssql import recalc_profit_orders
    from datetime import date as _date

    date_to = _date.today()
    date_from = date_to - timedelta(days=days_back)

    count = recalc_profit_orders(date_from=date_from, date_to=date_to)

    log.info("pipeline.step6_done", profit_recalc=count)
    return count


# ──────────────────────────────────────────────────────────────────
# Pipeline orchestrator
# ──────────────────────────────────────────────────────────────────
async def run_order_pipeline(days_back: int = 1, sync_profile: str = "core_sync") -> dict:
    """
    Run the full 6-step order pipeline.
    Designed to run every 15 min via APScheduler.
    """
    t0 = time.time()
    results = {}

    # Step 1 — Sync orders (async — SP-API calls)
    try:
        results["sync_orders"] = await step_sync_orders(days_back=days_back, sync_profile=sync_profile)
    except Exception as e:
        log.error("pipeline.step1_fatal", error=str(e))
        results["sync_orders"] = {"error": str(e)}

    # Step 2 — Backfill products (sync)
    try:
        results["backfill_products"] = step_backfill_products()
    except Exception as e:
        log.error("pipeline.step2_fatal", error=str(e))
        results["backfill_products"] = {"error": str(e)}

    # Step 2.5 — Enrich product registry fields (sync)
    try:
        results["enrich_products_registry"] = step_enrich_products_from_registry()
    except Exception as e:
        log.error("pipeline.step2_5_fatal", error=str(e))
        results["enrich_products_registry"] = {"error": str(e)}

    # Step 3 — Link order lines (sync)
    try:
        results["link_order_lines"] = step_link_order_lines()
    except Exception as e:
        log.error("pipeline.step3_fatal", error=str(e))
        results["link_order_lines"] = {"error": str(e)}

    # Step 4 — Map internal SKU (async — Ergonode/GSheet API calls)
    try:
        results["map_products"] = await step_map_products()
    except Exception as e:
        log.error("pipeline.step4_fatal", error=str(e))
        results["map_products"] = {"error": str(e)}

    # Step 5 — Stamp purchase prices (sync)
    try:
        results["stamp_prices"] = step_stamp_purchase_prices()
    except Exception as e:
        log.error("pipeline.step5_fatal", error=str(e))
        results["stamp_prices"] = {"error": str(e)}

    # Step 5.5 — Quick COGS validation (sync)
    try:
        from app.services.cogs_audit import validate_after_stamp
        stamp_count = results.get("stamp_prices", 0)
        if isinstance(stamp_count, dict):
            stamp_count = 0
        warnings = validate_after_stamp(stamp_count)
        if warnings:
            results["audit_warnings"] = warnings
    except Exception as e:
        log.warning("pipeline.audit_skip", error=str(e))

    # Step 5.8 — Sync FX rates (quick, dedup-safe)
    try:
        from app.services.sync_service import sync_exchange_rates
        results["exchange_rates"] = await sync_exchange_rates(days_back=7)
    except Exception as e:
        log.warning("pipeline.fx_sync_skip", error=str(e))
        results["exchange_rates"] = {"error": str(e)}

    # Step 5.8b — Sync finance transactions (Finances API v2024-06-19)
    try:
        results["sync_finances"] = await step_sync_finances(days_back=3)
    except Exception as e:
        log.warning("pipeline.sync_finances_skip", error=str(e))
        results["sync_finances"] = {"error": str(e)}

    # Step 5.9 — Bridge finance fees → order line + order fee columns
    try:
        results["bridge_fees"] = step_bridge_fees()
    except Exception as e:
        log.warning("pipeline.step5_9_skip", error=str(e))
        results["bridge_fees"] = {"error": str(e)}

    # Step 5.95 — Sync courier costs for FBM (MFN) orders
    results["courier_costs"] = {
        "removed": True,
        "reason": "legacy_courier_cost_mapping_removed",
    }

    # Step 5.97 — Detect refunded orders from finance transactions
    try:
        results["detect_refunds"] = step_detect_refunds()
    except Exception as e:
        log.warning("pipeline.detect_refunds_skip", error=str(e))
        results["detect_refunds"] = {"error": str(e)}

    # Step 6 — Recalculate profit / CM1 (async)
    try:
        results["calc_profit"] = await step_calc_profit(days_back=max(days_back, 7))
    except Exception as e:
        log.error("pipeline.step6_fatal", error=str(e))
        results["calc_profit"] = {"error": str(e)}

    dt = time.time() - t0
    log.info("pipeline.complete", results=results, seconds=round(dt, 1))
    return results
