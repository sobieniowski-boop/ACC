from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import threading
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pyodbc
import structlog

from app.connectors.mssql import enqueue_job, get_job, list_jobs, set_job_progress
from app.core.config import MARKETPLACE_REGISTRY, settings
from app.core.db_connection import connect_acc
from app.services.finance_center.mappers.amazon_to_ledger import (
    LedgerMappingRule,
    build_entry_hash,
    resolve_mapping_rule,
)
from app.services.order_pipeline import step_bridge_fees, step_sync_finances


def _connect() -> pyodbc.Connection:
    return connect_acc(autocommit=False, timeout=20, isolation_level="READ COMMITTED")


def _fetchall_dict(cur: pyodbc.Cursor) -> list[dict[str, Any]]:
    cols = [c[0] for c in cur.description] if cur.description else []
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]


def _fetchone_dict(cur: pyodbc.Cursor) -> dict[str, Any] | None:
    row = cur.fetchone()
    if not row or not cur.description:
        return None
    cols = [c[0] for c in cur.description]
    return {cols[i]: row[i] for i in range(len(cols))}


log = structlog.get_logger(__name__)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def _marketplace_code(marketplace_id: str | None) -> str:
    info = MARKETPLACE_REGISTRY.get(str(marketplace_id or ""))
    return str(info.get("code")) if info else str(marketplace_id or "")


def _marketplace_country(marketplace_id: str | None) -> str | None:
    code = _marketplace_code(marketplace_id)
    return code or None


def _actor(user_id: str | None = None) -> str:
    return str(user_id or settings.DEFAULT_ACTOR or "system")


_FIN_DASH_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


def _fin_dash_cache_get(key: str) -> dict[str, Any] | None:
    row = _FIN_DASH_CACHE.get(key)
    if not row:
        return None
    exp, value = row
    if time.monotonic() > exp:
        _FIN_DASH_CACHE.pop(key, None)
        return None
    return value


def _fin_dash_cache_set(key: str, value: dict[str, Any], ttl_sec: int = 120) -> None:
    _FIN_DASH_CACHE[key] = (time.monotonic() + ttl_sec, value)


def _cleanup_stale_finance_jobs(max_age_minutes: int = 45) -> int:
    ensure_finance_center_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE dbo.acc_al_jobs
            SET
                status = 'failure',
                progress_pct = 100,
                progress_message = 'Failed',
                error_message = COALESCE(error_message + ' | ', '') + ?,
                finished_at = SYSUTCDATETIME(),
                duration_seconds = DATEDIFF(second, COALESCE(started_at, created_at), SYSUTCDATETIME())
            WHERE
                job_type IN ('finance_sync_transactions', 'finance_prepare_settlements', 'finance_generate_ledger', 'finance_reconcile_payouts')
                AND status IN ('pending', 'running')
                AND COALESCE(last_heartbeat_at, started_at, created_at) < DATEADD(minute, -?, SYSUTCDATETIME())
            """,
            ("Marked stale by finance_center cleanup", max_age_minutes),
        )
        affected = int(cur.rowcount or 0)
        conn.commit()
        return affected
    finally:
        conn.close()


def _find_active_finance_job(job_type: str) -> dict[str, Any] | None:
    ensure_finance_center_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 1
                CAST(id AS NVARCHAR(40)) AS id,
                job_type,
                marketplace_id,
                status,
                progress_pct,
                progress_message,
                created_at
            FROM dbo.acc_al_jobs WITH (NOLOCK)
            WHERE job_type = ?
              AND status IN ('pending', 'running')
            ORDER BY created_at DESC
            """,
            (job_type,),
        )
        return _fetchone_dict(cur)
    finally:
        conn.close()


def ensure_finance_center_schema() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
IF OBJECT_ID('dbo.acc_fin_chart_of_accounts', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fin_chart_of_accounts (
        account_code NVARCHAR(32) NOT NULL PRIMARY KEY,
        name NVARCHAR(200) NOT NULL,
        account_type NVARCHAR(32) NOT NULL,
        parent_code NVARCHAR(32) NULL,
        is_active BIT NOT NULL DEFAULT 1,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

IF OBJECT_ID('dbo.acc_fin_tax_code', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fin_tax_code (
        code NVARCHAR(32) NOT NULL PRIMARY KEY,
        vat_rate DECIMAL(9,4) NOT NULL DEFAULT 0,
        oss_flag BIT NOT NULL DEFAULT 0,
        country NVARCHAR(8) NULL,
        description NVARCHAR(200) NULL,
        is_active BIT NOT NULL DEFAULT 1,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

IF COL_LENGTH('dbo.acc_finance_transaction', 'financial_event_group_id') IS NULL
BEGIN
    ALTER TABLE dbo.acc_finance_transaction ADD financial_event_group_id NVARCHAR(120) NULL;
END;

IF EXISTS (
    SELECT 1
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'acc_finance_transaction'
      AND COLUMN_NAME = 'marketplace_id'
      AND IS_NULLABLE = 'NO'
)
BEGIN
    IF EXISTS (
        SELECT 1
        FROM sys.foreign_keys
        WHERE name = 'FK_acc_finance_tx_marketplace'
          AND parent_object_id = OBJECT_ID('dbo.acc_finance_transaction')
    )
    BEGIN
        ALTER TABLE dbo.acc_finance_transaction DROP CONSTRAINT FK_acc_finance_tx_marketplace;
    END;
    ALTER TABLE dbo.acc_finance_transaction ALTER COLUMN marketplace_id NVARCHAR(32) NULL;
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_acc_finance_transaction_group'
      AND object_id = OBJECT_ID('dbo.acc_finance_transaction')
)
BEGIN
    CREATE INDEX IX_acc_finance_transaction_group
    ON dbo.acc_finance_transaction(financial_event_group_id, marketplace_id, posted_date);
END;

IF OBJECT_ID('dbo.acc_fin_mapping_rule', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fin_mapping_rule (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
        source_system NVARCHAR(32) NOT NULL,
        source_type NVARCHAR(120) NOT NULL,
        account_code NVARCHAR(32) NOT NULL,
        tax_code NVARCHAR(32) NULL,
        sign_multiplier DECIMAL(9,4) NOT NULL DEFAULT 1,
        notes NVARCHAR(500) NULL,
        is_active BIT NOT NULL DEFAULT 1,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE UNIQUE INDEX UX_acc_fin_mapping_rule_source ON dbo.acc_fin_mapping_rule(source_system, source_type);
END;

IF OBJECT_ID('dbo.acc_fin_settlement_summary', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fin_settlement_summary (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
        settlement_id NVARCHAR(80) NOT NULL,
        financial_event_group_id NVARCHAR(120) NULL,
        marketplace_id NVARCHAR(32) NULL,
        currency NVARCHAR(8) NOT NULL,
        total_amount DECIMAL(18,4) NOT NULL DEFAULT 0,
        total_amount_base DECIMAL(18,4) NOT NULL DEFAULT 0,
        transaction_count INT NOT NULL DEFAULT 0,
        posted_from DATE NULL,
        posted_to DATE NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE UNIQUE INDEX UX_acc_fin_settlement_summary_settlement ON dbo.acc_fin_settlement_summary(settlement_id);
END;

IF COL_LENGTH('dbo.acc_fin_settlement_summary', 'financial_event_group_id') IS NULL
BEGIN
    ALTER TABLE dbo.acc_fin_settlement_summary ADD financial_event_group_id NVARCHAR(120) NULL;
END;

IF OBJECT_ID('dbo.acc_fin_bank_line', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fin_bank_line (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
        bank_date DATE NOT NULL,
        amount DECIMAL(18,4) NOT NULL,
        currency NVARCHAR(8) NOT NULL,
        description NVARCHAR(500) NULL,
        reference NVARCHAR(200) NULL,
        line_hash NVARCHAR(64) NOT NULL,
        imported_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE UNIQUE INDEX UX_acc_fin_bank_line_hash ON dbo.acc_fin_bank_line(line_hash);
END;

IF OBJECT_ID('dbo.acc_fin_ledger_entry', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fin_ledger_entry (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
        entry_date DATE NOT NULL,
        source NVARCHAR(32) NOT NULL,
        source_ref NVARCHAR(120) NOT NULL,
        source_line_hash NVARCHAR(64) NOT NULL,
        marketplace_id NVARCHAR(32) NULL,
        settlement_id NVARCHAR(80) NULL,
        financial_event_group_id NVARCHAR(120) NULL,
        amazon_order_id NVARCHAR(80) NULL,
        transaction_type NVARCHAR(120) NULL,
        charge_type NVARCHAR(120) NULL,
        currency NVARCHAR(8) NOT NULL,
        amount DECIMAL(18,4) NOT NULL,
        fx_rate DECIMAL(18,6) NOT NULL DEFAULT 1,
        amount_base DECIMAL(18,4) NOT NULL,
        base_currency NVARCHAR(8) NOT NULL DEFAULT 'PLN',
        account_code NVARCHAR(32) NOT NULL,
        tax_code NVARCHAR(32) NULL,
        country NVARCHAR(8) NULL,
        sku NVARCHAR(120) NULL,
        asin NVARCHAR(40) NULL,
        description NVARCHAR(500) NULL,
        tags_json NVARCHAR(MAX) NULL,
        reversed_entry_id UNIQUEIDENTIFIER NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE UNIQUE INDEX UX_acc_fin_ledger_entry_hash ON dbo.acc_fin_ledger_entry(source, source_line_hash);
    CREATE INDEX IX_acc_fin_ledger_entry_filters ON dbo.acc_fin_ledger_entry(entry_date, marketplace_id, account_code, sku, country, source);
END;

IF COL_LENGTH('dbo.acc_fin_ledger_entry', 'financial_event_group_id') IS NULL
BEGIN
    ALTER TABLE dbo.acc_fin_ledger_entry ADD financial_event_group_id NVARCHAR(120) NULL;
END;

IF OBJECT_ID('dbo.acc_fin_reconciliation_payout', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fin_reconciliation_payout (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
        settlement_id NVARCHAR(80) NOT NULL,
        financial_event_group_id NVARCHAR(120) NULL,
        bank_line_id UNIQUEIDENTIFIER NULL,
        status NVARCHAR(32) NOT NULL DEFAULT 'unmatched',
        matched_amount DECIMAL(18,4) NOT NULL DEFAULT 0,
        diff_amount DECIMAL(18,4) NOT NULL DEFAULT 0,
        notes NVARCHAR(500) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE UNIQUE INDEX UX_acc_fin_reconciliation_payout_settlement ON dbo.acc_fin_reconciliation_payout(settlement_id);
END;

IF COL_LENGTH('dbo.acc_fin_reconciliation_payout', 'financial_event_group_id') IS NULL
BEGIN
    ALTER TABLE dbo.acc_fin_reconciliation_payout ADD financial_event_group_id NVARCHAR(120) NULL;
END;

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
END;

IF COL_LENGTH('dbo.acc_fin_event_group_sync', 'event_type_counts_json') IS NULL
BEGIN
    ALTER TABLE dbo.acc_fin_event_group_sync ADD event_type_counts_json NVARCHAR(MAX) NULL;
END;

IF COL_LENGTH('dbo.acc_fin_event_group_sync', 'payload_signature') IS NULL
BEGIN
    ALTER TABLE dbo.acc_fin_event_group_sync ADD payload_signature NVARCHAR(64) NULL;
END;

IF COL_LENGTH('dbo.acc_fin_event_group_sync', 'first_posted_at') IS NULL
BEGIN
    ALTER TABLE dbo.acc_fin_event_group_sync ADD first_posted_at DATETIME2 NULL;
END;

IF COL_LENGTH('dbo.acc_fin_event_group_sync', 'last_posted_at') IS NULL
BEGIN
    ALTER TABLE dbo.acc_fin_event_group_sync ADD last_posted_at DATETIME2 NULL;
END;

IF COL_LENGTH('dbo.acc_fin_event_group_sync', 'open_refresh_after') IS NULL
BEGIN
    ALTER TABLE dbo.acc_fin_event_group_sync ADD open_refresh_after DATETIME2 NULL;
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_acc_fin_event_group_sync_open_refresh'
      AND object_id = OBJECT_ID('dbo.acc_fin_event_group_sync')
)
BEGIN
    CREATE INDEX IX_acc_fin_event_group_sync_open_refresh
    ON dbo.acc_fin_event_group_sync(marketplace_id, open_refresh_after, last_synced_at);
END;
            """
        )

        seed_accounts = [
            ("100", "Bank", "asset", None),
            ("220", "VAT Payable", "liability", None),
            ("300", "Amazon Receivable", "asset", None),
            ("520", "Amazon Fees Expense", "expense", None),
            ("530", "FBA Fulfillment Expense", "expense", None),
            ("540", "Storage Fees", "expense", None),
            ("550", "Advertising Expense", "expense", None),
            ("580", "Amazon Other Adjustments", "expense", None),
            ("700", "Sales Revenue", "revenue", None),
        ]
        for account_code, name, account_type, parent_code in seed_accounts:
            cur.execute(
                """
IF NOT EXISTS (SELECT 1 FROM dbo.acc_fin_chart_of_accounts WHERE account_code = ?)
BEGIN
    INSERT INTO dbo.acc_fin_chart_of_accounts(account_code, name, account_type, parent_code)
    VALUES (?, ?, ?, ?)
END
                """,
                (account_code, account_code, name, account_type, parent_code),
            )

        seed_tax_codes = [
            ("VAT23", 23.0, 0, "PL", "Standard VAT 23%"),
            ("VAT0", 0.0, 0, None, "Zero VAT / out of scope"),
            ("OSS-EU", 0.0, 1, "EU", "OSS placeholder"),
        ]
        for code, vat_rate, oss_flag, country, description in seed_tax_codes:
            cur.execute(
                """
IF NOT EXISTS (SELECT 1 FROM dbo.acc_fin_tax_code WHERE code = ?)
BEGIN
    INSERT INTO dbo.acc_fin_tax_code(code, vat_rate, oss_flag, country, description)
    VALUES (?, ?, ?, ?, ?)
END
                """,
                (code, code, vat_rate, oss_flag, country, description),
            )

        seed_rules = [
            ("amazon", "Principal", "700", "VAT0", 1.0, "Revenue"),
            ("amazon", "Tax", "220", "VAT23", 1.0, "VAT component"),
            ("amazon", "Commission", "520", None, 1.0, "Referral fee"),
            ("amazon", "VariableClosingFee", "520", None, 1.0, "Closing fee"),
            ("amazon", "FixedClosingFee", "520", None, 1.0, "Closing fee"),
            ("amazon", "FBAPerUnitFulfillmentFee", "530", None, 1.0, "FBA fee"),
            ("amazon", "FBAPerOrderFulfillmentFee", "530", None, 1.0, "FBA fee"),
            ("amazon", "FBAWeightBasedFee", "530", None, 1.0, "FBA fee"),
            ("amazon", "FBAPickAndPackFee", "530", None, 1.0, "FBA fee"),
            ("amazon", "StorageFee", "540", None, 1.0, "Storage fee"),
            ("amazon", "CostOfAdvertising", "550", None, 1.0, "Ads fee"),
        ]
        for source_system, source_type, account_code, tax_code, sign_multiplier, notes in seed_rules:
            cur.execute(
                """
IF NOT EXISTS (
    SELECT 1 FROM dbo.acc_fin_mapping_rule
    WHERE source_system = ? AND source_type = ?
)
BEGIN
    INSERT INTO dbo.acc_fin_mapping_rule(source_system, source_type, account_code, tax_code, sign_multiplier, notes)
    VALUES (?, ?, ?, ?, ?, ?)
END
                """,
                (source_system, source_type, source_system, source_type, account_code, tax_code, sign_multiplier, notes),
            )
        conn.commit()
    finally:
        conn.close()


def _load_mapping_rules(cur: pyodbc.Cursor) -> dict[str, LedgerMappingRule]:
    cur.execute(
        """
        SELECT source_type, account_code, tax_code, sign_multiplier
        FROM dbo.acc_fin_mapping_rule WITH (NOLOCK)
        WHERE source_system = 'amazon' AND is_active = 1
        """
    )
    rules: dict[str, LedgerMappingRule] = {}
    for row in _fetchall_dict(cur):
        rules[str(row["source_type"])] = LedgerMappingRule(
            account_code=str(row["account_code"]),
            tax_code=str(row.get("tax_code")) if row.get("tax_code") else None,
            sign_multiplier=_to_float(row.get("sign_multiplier"), 1.0),
        )
    return rules


def _lookup_fx_rate(cur: pyodbc.Cursor, currency: str, rate_date: date) -> float:
    if not currency or currency.upper() == "PLN":
        return 1.0
    cur.execute(
        """
        SELECT TOP 1 rate_to_pln
        FROM dbo.acc_exchange_rate WITH (NOLOCK)
        WHERE currency = ? AND rate_date <= ?
        ORDER BY rate_date DESC
        """,
        (currency.upper(), rate_date),
    )
    row = cur.fetchone()
    if row:
        return _to_float(row[0], 1.0)
    log.warning("finance_center.fx_rate_missing",
                currency=currency, rate_date=str(rate_date),
                msg="No FX rate found for currency — financial data may be inaccurate (SF-02)")
    return 1.0  # TODO: raise once all callers handle missing rates


def queue_finance_job(job_type: str, params: dict[str, Any] | None = None, marketplace_id: str | None = None) -> dict[str, Any]:
    _cleanup_stale_finance_jobs()
    active_job = _find_active_finance_job(job_type)
    if active_job:
        raise RuntimeError(f"{job_type} already active: {active_job.get('id')}")
    return enqueue_job(
        job_type=job_type,
        marketplace_id=marketplace_id,
        trigger_source="manual",
        triggered_by=settings.DEFAULT_ACTOR,
        params=params or {},
    )


def list_finance_jobs(page: int = 1, page_size: int = 30) -> dict[str, Any]:
    _cleanup_stale_finance_jobs()
    response = list_jobs(page=page, page_size=200)
    allowed = {
        "finance_sync_transactions",
        "finance_prepare_settlements",
        "finance_generate_ledger",
        "finance_reconcile_payouts",
    }
    items = [item for item in response.get("items", []) if item.get("job_type") in allowed]
    start = (page - 1) * page_size
    return {"total": len(items), "items": items[start:start + page_size]}


def get_finance_job(job_id: str) -> dict[str, Any] | None:
    return get_job(job_id)


def get_finance_sync_diagnostics(limit: int = 30, marketplace_id: str | None = None) -> dict[str, Any]:
    ensure_finance_center_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        where_parts: list[str] = []
        params: list[Any] = []
        if marketplace_id:
            where_parts.append("marketplace_id = ?")
            params.append(marketplace_id)
        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

        cur.execute(
            f"""
            SELECT
                MAX(COALESCE(group_end, last_posted_at, group_start)) AS latest_watermark_from,
                SUM(CASE WHEN ISNULL(processing_status, '') <> 'Closed' THEN 1 ELSE 0 END) AS tracked_open_groups
            FROM dbo.acc_fin_event_group_sync WITH (NOLOCK)
            {where_sql}
            """,
            tuple(params),
        )
        summary = _fetchone_dict(cur) or {}

        cur.execute(
            f"""
            SELECT TOP ({int(limit)})
                financial_event_group_id,
                marketplace_id,
                processing_status,
                fund_transfer_status,
                group_start,
                group_end,
                first_posted_at,
                last_posted_at,
                last_row_count,
                payload_signature,
                event_type_counts_json,
                last_synced_at,
                open_refresh_after,
                CASE
                    WHEN ISNULL(processing_status, '') <> 'Closed'
                         AND (open_refresh_after IS NULL OR open_refresh_after <= SYSUTCDATETIME()) THEN 'open_due'
                    WHEN ISNULL(processing_status, '') <> 'Closed' THEN 'open_cooldown'
                    ELSE 'closed'
                END AS sync_state
            FROM dbo.acc_fin_event_group_sync WITH (NOLOCK)
            {where_sql}
            ORDER BY
                CASE
                    WHEN ISNULL(processing_status, '') <> 'Closed'
                         AND (open_refresh_after IS NULL OR open_refresh_after <= SYSUTCDATETIME()) THEN 0
                    WHEN ISNULL(processing_status, '') <> 'Closed' THEN 1
                    ELSE 2
                END,
                last_row_count DESC,
                last_synced_at ASC
            """,
            tuple(params),
        )
        items: list[dict[str, Any]] = []
        now_utc = datetime.now(timezone.utc)
        for row in _fetchall_dict(cur):
            counts = row.get("event_type_counts_json")
            try:
                parsed_counts = json.loads(str(counts)) if counts else {}
            except Exception:
                parsed_counts = {}
            group_start = row.get("group_start")
            if group_start and getattr(group_start, "tzinfo", None) is None:
                group_start = group_start.replace(tzinfo=timezone.utc)
            open_age_hours = 0.0
            if group_start:
                try:
                    open_age_hours = max(0.0, round((now_utc - group_start).total_seconds() / 3600.0, 1))
                except Exception:
                    open_age_hours = 0.0
            row_count = _to_int(row.get("last_row_count"))
            cost_score = round(open_age_hours * 2.0 + min(row_count, 5000) / 50.0 + (15.0 if row_count == 0 and open_age_hours >= 6 else 0.0), 1)
            items.append(
                {
                    "financial_event_group_id": row["financial_event_group_id"],
                    "marketplace_id": row.get("marketplace_id"),
                    "marketplace_code": _marketplace_code(row.get("marketplace_id")),
                    "processing_status": row.get("processing_status"),
                    "fund_transfer_status": row.get("fund_transfer_status"),
                    "group_start": row.get("group_start"),
                    "group_end": row.get("group_end"),
                    "first_posted_at": row.get("first_posted_at"),
                    "last_posted_at": row.get("last_posted_at"),
                    "last_row_count": row_count,
                    "payload_signature": row.get("payload_signature"),
                    "event_type_counts_json": parsed_counts.get("event_type_counts", parsed_counts if isinstance(parsed_counts, dict) else {}),
                    "last_synced_at": row.get("last_synced_at"),
                    "open_refresh_after": row.get("open_refresh_after"),
                    "open_age_hours": open_age_hours,
                    "cost_score": cost_score,
                    "sync_state": row.get("sync_state") or "closed",
                }
            )
        return {
            "latest_watermark_from": summary.get("latest_watermark_from"),
            "tracked_open_groups": _to_int(summary.get("tracked_open_groups")),
            "items": items,
        }
    finally:
        conn.close()


def get_finance_data_completeness(days_back: int = 30) -> dict[str, Any]:
    ensure_finance_center_schema()
    date_to = date.today()
    date_from = date_to - timedelta(days=max(0, days_back - 1))
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            WITH order_days AS (
                SELECT
                    o.marketplace_id,
                    CAST(o.purchase_date AS DATE) AS day_key,
                    COUNT(DISTINCT o.amazon_order_id) AS orders_total
                FROM dbo.acc_order o WITH (NOLOCK)
                WHERE o.status = 'Shipped'
                  AND CAST(o.purchase_date AS DATE) >= ?
                  AND CAST(o.purchase_date AS DATE) <= ?
                GROUP BY o.marketplace_id, CAST(o.purchase_date AS DATE)
            ),
            finance_days AS (
                SELECT
                    marketplace_id,
                    CAST(posted_date AS DATE) AS day_key,
                    COUNT(DISTINCT amazon_order_id) AS orders_with_finance
                FROM dbo.acc_finance_transaction WITH (NOLOCK)
                WHERE CAST(posted_date AS DATE) >= ?
                  AND CAST(posted_date AS DATE) <= ?
                GROUP BY marketplace_id, CAST(posted_date AS DATE)
            )
            SELECT
                od.marketplace_id,
                COUNT(*) AS order_days,
                SUM(CASE WHEN fd.day_key IS NOT NULL THEN 1 ELSE 0 END) AS finance_days,
                SUM(od.orders_total) AS orders_total,
                SUM(ISNULL(fd.orders_with_finance, 0)) AS orders_with_finance
            FROM order_days od
            LEFT JOIN finance_days fd
              ON fd.marketplace_id = od.marketplace_id
             AND fd.day_key = od.day_key
            GROUP BY od.marketplace_id
            ORDER BY od.marketplace_id
            """,
            (date_from.isoformat(), date_to.isoformat(), date_from.isoformat(), date_to.isoformat()),
        )
        rows = _fetchall_dict(cur)
        items: list[dict[str, Any]] = []
        for row in rows:
            marketplace_id = str(row.get("marketplace_id") or "")
            order_days = _to_int(row.get("order_days"))
            finance_days = _to_int(row.get("finance_days"))
            orders_total = _to_int(row.get("orders_total"))
            orders_with_finance = _to_int(row.get("orders_with_finance"))
            day_cov = round((finance_days / order_days) * 100, 1) if order_days else 0.0
            order_cov = round((orders_with_finance / orders_total) * 100, 1) if orders_total else 0.0
            if day_cov >= 95 and order_cov >= 95:
                status = "complete"
                note = "Feed coverage looks production-safe for this marketplace."
            elif day_cov >= 70 or order_cov >= 70:
                status = "partial"
                note = "Feed is incomplete; operational and financial decisions should be treated cautiously."
            else:
                status = "critical"
                note = "Feed coverage is too low for production-trust decisions."
            items.append(
                {
                    "marketplace_id": marketplace_id,
                    "marketplace_code": _marketplace_code(marketplace_id),
                    "order_days": order_days,
                    "finance_days": finance_days,
                    "day_coverage_pct": day_cov,
                    "orders_total": orders_total,
                    "orders_with_finance": orders_with_finance,
                    "order_coverage_pct": order_cov,
                    "status": status,
                    "note": note,
                }
            )
        overall_status = "complete"
        if any(item["status"] == "critical" for item in items):
            overall_status = "critical"
        elif any(item["status"] == "partial" for item in items):
            overall_status = "partial"
        partial = overall_status != "complete"
        note = (
            "Finance dashboard contains partial data. Metrics derived from Amazon fees/ledger should not be treated as complete."
            if partial
            else "Finance feed completeness meets production threshold for the observed range."
        )
        return {
            "date_from": date_from,
            "date_to": date_to,
            "overall_status": overall_status,
            "partial": partial,
            "note": note,
            "marketplaces": items,
        }
    finally:
        conn.close()


def evaluate_finance_completeness_alerts(days_back: int = 30) -> dict[str, Any]:
    ensure_finance_center_schema()
    summary = get_finance_data_completeness(days_back)
    items = [dict(item) for item in summary.get("marketplaces") or []]
    critical_items = [item for item in items if str(item.get("status") or "").lower() == "critical"]
    partial_items = [item for item in items if str(item.get("status") or "").lower() == "partial"]
    coverage_values = [float(item.get("order_coverage_pct") or 0.0) for item in items]
    min_coverage = round(min(coverage_values), 1) if coverage_values else 0.0

    conn = _connect()
    try:
        cur = conn.cursor()
        def _ensure_rule(name: str, rule_type: str, severity: str, description: str) -> str:
            cur.execute(
                """
                SELECT TOP 1 CAST(id AS NVARCHAR(40))
                FROM dbo.acc_al_alert_rules WITH (NOLOCK)
                WHERE name = ? AND rule_type = ?
                ORDER BY created_at DESC
                """,
                (name, rule_type),
            )
            row = cur.fetchone()
            if row and row[0]:
                return str(row[0])
            new_rule_id = str(uuid.uuid4())
            cur.execute(
                """
                INSERT INTO dbo.acc_al_alert_rules
                (
                    id, name, description, rule_type, severity, is_active, created_by
                )
                VALUES
                (
                    CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, 1, 'system'
                )
                """,
                (new_rule_id, name, description, rule_type, severity),
            )
            return new_rule_id

        critical_rule_id = _ensure_rule(
            "Finance completeness critical",
            "finance_completeness_critical",
            "critical",
            "Finance feed completeness fell below production-safe thresholds.",
        )
        partial_rule_id = _ensure_rule(
            "Finance completeness partial",
            "finance_completeness_partial",
            "warning",
            "Finance feed completeness is degraded and should be monitored before it becomes critical.",
        )

        def _upsert_alert(
            *,
            rule_id: str,
            severity: str,
            title: str,
            detail: str,
            detail_json: dict[str, Any],
            context_json: dict[str, Any],
            current_value: float,
        ) -> dict[str, int]:
            cur.execute(
                """
                SELECT TOP 1 CAST(id AS NVARCHAR(40))
                FROM dbo.acc_al_alerts WITH (UPDLOCK, ROWLOCK)
                WHERE rule_id = CAST(? AS UNIQUEIDENTIFIER)
                  AND is_resolved = 0
                ORDER BY triggered_at DESC
                """,
                (rule_id,),
            )
            active_row = cur.fetchone()
            if active_row and active_row[0]:
                cur.execute(
                    """
                    UPDATE dbo.acc_al_alerts
                    SET
                        title = ?,
                        detail = ?,
                        detail_json = ?,
                        context_json = ?,
                        severity = ?,
                        current_value = ?,
                        is_read = 0
                    WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                    """,
                    (
                        title,
                        detail,
                        json.dumps(detail_json, ensure_ascii=True),
                        json.dumps(context_json, ensure_ascii=True),
                        severity,
                        current_value,
                        str(active_row[0]),
                    ),
                )
                return {"created": 0, "updated": 1}

            cur.execute(
                """
                INSERT INTO dbo.acc_al_alerts
                (
                    id, rule_id, marketplace_id, sku, title, detail, detail_json, context_json,
                    severity, current_value, is_read, is_resolved, triggered_at
                )
                VALUES
                (
                    CAST(? AS UNIQUEIDENTIFIER), CAST(? AS UNIQUEIDENTIFIER), NULL, NULL, ?, ?, ?, ?,
                    ?, ?, 0, 0, SYSUTCDATETIME()
                )
                """,
                (
                    str(uuid.uuid4()),
                    rule_id,
                    title,
                    detail,
                    json.dumps(detail_json, ensure_ascii=True),
                    json.dumps(context_json, ensure_ascii=True),
                    severity,
                    current_value,
                ),
            )
            return {"created": 1, "updated": 0}

        def _resolve_rule(rule_id: str) -> int:
            cur.execute(
                """
                UPDATE dbo.acc_al_alerts
                SET
                    is_resolved = 1,
                    resolved_at = SYSUTCDATETIME(),
                    resolved_by = 'system'
                WHERE rule_id = CAST(? AS UNIQUEIDENTIFIER)
                  AND is_resolved = 0
                """,
                (rule_id,),
            )
            return int(cur.rowcount or 0)

        status = str(summary.get("overall_status") or "unknown").lower()
        if status == "critical":
            top_critical = sorted(
                critical_items,
                key=lambda item: (
                    float(item.get("order_coverage_pct") or 0.0),
                    float(item.get("day_coverage_pct") or 0.0),
                ),
            )[:5]
            top_codes = [str(item.get("marketplace_code") or item.get("marketplace_id") or "?") for item in top_critical]
            detail_json = {
                "date_from": str(summary.get("date_from")),
                "date_to": str(summary.get("date_to")),
                "overall_status": status,
                "critical_marketplaces": critical_items,
                "partial_marketplaces": partial_items,
                "top_critical_codes": top_codes,
            }
            context_json = {
                "route": "/finance/dashboard",
                "query": {"completeness_status": "critical"},
                "source": "finance_completeness",
                "days_back": days_back,
            }
            title = (
                f"Finance completeness critical: {len(critical_items)} MP below threshold"
                if len(critical_items) == 1
                else f"Finance completeness critical: {len(critical_items)} MPs below threshold"
            )
            detail = (
                f"Finance feed is incomplete for {len(critical_items)} critical marketplace(s) "
                f"and {len(partial_items)} partial marketplace(s). "
                f"Worst coverage: {', '.join(top_codes) if top_codes else 'n/a'}. "
                f"Minimum order coverage={min_coverage:.1f}%."
            )

            stats = _upsert_alert(
                rule_id=critical_rule_id,
                severity="critical",
                title=title,
                detail=detail,
                detail_json=detail_json,
                context_json=context_json,
                current_value=float(len(critical_items)),
            )
            resolved = _resolve_rule(partial_rule_id)
            conn.commit()
            return {
                "status": status,
                "critical_marketplaces": len(critical_items),
                "partial_marketplaces": len(partial_items),
                "created": int(stats["created"]),
                "updated": int(stats["updated"]),
                "resolved": resolved,
            }

        if status == "partial":
            top_partial = sorted(
                partial_items,
                key=lambda item: (
                    float(item.get("order_coverage_pct") or 0.0),
                    float(item.get("day_coverage_pct") or 0.0),
                ),
            )[:5]
            top_codes = [str(item.get("marketplace_code") or item.get("marketplace_id") or "?") for item in top_partial]
            detail_json = {
                "date_from": str(summary.get("date_from")),
                "date_to": str(summary.get("date_to")),
                "overall_status": status,
                "critical_marketplaces": critical_items,
                "partial_marketplaces": partial_items,
                "top_partial_codes": top_codes,
            }
            context_json = {
                "route": "/finance/dashboard",
                "query": {"completeness_status": "partial"},
                "source": "finance_completeness",
                "days_back": days_back,
            }
            title = (
                f"Finance completeness partial: {len(partial_items)} MP below target"
                if len(partial_items) == 1
                else f"Finance completeness partial: {len(partial_items)} MPs below target"
            )
            detail = (
                f"Finance feed is degraded for {len(partial_items)} partial marketplace(s). "
                f"Lowest partial coverage: {', '.join(top_codes) if top_codes else 'n/a'}. "
                f"Minimum order coverage={min_coverage:.1f}%."
            )
            stats = _upsert_alert(
                rule_id=partial_rule_id,
                severity="warning",
                title=title,
                detail=detail,
                detail_json=detail_json,
                context_json=context_json,
                current_value=float(len(partial_items)),
            )
            resolved = _resolve_rule(critical_rule_id)
            conn.commit()
            return {
                "status": status,
                "critical_marketplaces": len(critical_items),
                "partial_marketplaces": len(partial_items),
                "created": int(stats["created"]),
                "updated": int(stats["updated"]),
                "resolved": resolved,
            }

        resolved = _resolve_rule(critical_rule_id) + _resolve_rule(partial_rule_id)
        conn.commit()
        return {
            "status": status,
            "critical_marketplaces": len(critical_items),
            "partial_marketplaces": len(partial_items),
            "created": 0,
            "updated": 0,
            "resolved": resolved,
        }
    finally:
        conn.close()


def get_finance_marketplace_gap_diagnostics(days_back: int = 30) -> dict[str, Any]:
    ensure_finance_center_schema()
    date_to = date.today()
    date_from = date_to - timedelta(days=max(0, days_back - 1))
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            WITH orders_scope AS (
                SELECT
                    marketplace_id,
                    COUNT(DISTINCT amazon_order_id) AS orders_total,
                    COUNT(DISTINCT CAST(purchase_date AS DATE)) AS order_days
                FROM dbo.acc_order WITH (NOLOCK)
                WHERE status = 'Shipped'
                  AND CAST(purchase_date AS DATE) >= ?
                  AND CAST(purchase_date AS DATE) <= ?
                GROUP BY marketplace_id
            ),
            finance_rows AS (
                SELECT
                    ft.marketplace_id,
                    ft.amazon_order_id,
                    CAST(ft.posted_date AS DATE) AS posted_day,
                    CASE WHEN ft.amazon_order_id IS NULL OR LTRIM(RTRIM(COALESCE(ft.amazon_order_id, ''))) = '' THEN 1 ELSE 0 END AS is_unmapped,
                    CASE WHEN ft.amazon_order_id IS NOT NULL AND o.amazon_order_id IS NULL THEN 1 ELSE 0 END AS is_missing_order
                FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
                LEFT JOIN dbo.acc_order o WITH (NOLOCK)
                  ON o.amazon_order_id = ft.amazon_order_id
                WHERE CAST(ft.posted_date AS DATE) >= ?
                  AND CAST(ft.posted_date AS DATE) <= ?
            ),
            finance_scope AS (
                SELECT
                    marketplace_id,
                    COUNT(DISTINCT amazon_order_id) AS imported_orders,
                    COUNT(DISTINCT posted_day) AS finance_days,
                    COUNT(*) AS imported_rows,
                    SUM(is_unmapped) AS unmapped_rows,
                    SUM(is_missing_order) AS missing_order_rows,
                    COUNT(DISTINCT CASE WHEN is_missing_order = 1 THEN amazon_order_id END) AS missing_order_distinct_orders
                FROM finance_rows
                GROUP BY marketplace_id
            ),
            group_scope AS (
                SELECT
                    marketplace_id,
                    COUNT(*) AS tracked_groups,
                    SUM(CASE WHEN ISNULL(last_row_count, 0) > 0 THEN 1 ELSE 0 END) AS groups_with_rows,
                    MIN(CAST(group_start AS DATETIME2)) AS first_group_start,
                    MAX(CAST(COALESCE(group_end, last_posted_at, group_start) AS DATETIME2)) AS last_group_end
                FROM dbo.acc_fin_event_group_sync WITH (NOLOCK)
                WHERE CAST(COALESCE(group_start, last_posted_at, last_synced_at) AS DATE) >= ?
                  AND CAST(COALESCE(group_start, last_posted_at, last_synced_at) AS DATE) <= ?
                GROUP BY marketplace_id
            )
            SELECT
                COALESCE(os.marketplace_id, fs.marketplace_id, gs.marketplace_id) AS marketplace_id,
                ISNULL(os.order_days, 0) AS order_days,
                ISNULL(os.orders_total, 0) AS orders_total,
                ISNULL(fs.imported_orders, 0) AS imported_orders,
                ISNULL(fs.finance_days, 0) AS finance_days,
                ISNULL(fs.imported_rows, 0) AS imported_rows,
                ISNULL(fs.unmapped_rows, 0) AS unmapped_rows,
                ISNULL(fs.missing_order_rows, 0) AS missing_order_rows,
                ISNULL(fs.missing_order_distinct_orders, 0) AS missing_order_distinct_orders,
                ISNULL(gs.tracked_groups, 0) AS tracked_groups,
                ISNULL(gs.groups_with_rows, 0) AS groups_with_rows,
                gs.first_group_start,
                gs.last_group_end
            FROM orders_scope os
            FULL OUTER JOIN finance_scope fs
              ON fs.marketplace_id = os.marketplace_id
            FULL OUTER JOIN group_scope gs
              ON gs.marketplace_id = COALESCE(os.marketplace_id, fs.marketplace_id)
            WHERE COALESCE(os.marketplace_id, fs.marketplace_id, gs.marketplace_id) IS NOT NULL
            ORDER BY COALESCE(os.marketplace_id, fs.marketplace_id, gs.marketplace_id)
            """,
            (
                date_from.isoformat(),
                date_to.isoformat(),
                date_from.isoformat(),
                date_to.isoformat(),
                date_from.isoformat(),
                date_to.isoformat(),
                date_from.isoformat(),
                date_to.isoformat(),
            ),
        )
        rows = _fetchall_dict(cur)

        cur.execute(
            """
            SELECT
                marketplace_id,
                event_type_counts_json
            FROM dbo.acc_fin_event_group_sync WITH (NOLOCK)
            WHERE CAST(COALESCE(group_start, last_posted_at, last_synced_at) AS DATE) >= ?
              AND CAST(COALESCE(group_start, last_posted_at, last_synced_at) AS DATE) <= ?
            """,
            (date_from.isoformat(), date_to.isoformat()),
        )
        event_rows = _fetchall_dict(cur)
        event_counts_by_marketplace: dict[str, dict[str, int]] = {}
        for row in event_rows:
            marketplace_id = str(row.get("marketplace_id") or "")
            if not marketplace_id:
                continue
            bucket = event_counts_by_marketplace.setdefault(marketplace_id, {})
            raw_payload = row.get("event_type_counts_json")
            try:
                payload = json.loads(str(raw_payload)) if raw_payload else {}
            except Exception:
                payload = {}
            counts = payload.get("event_type_counts", payload) if isinstance(payload, dict) else {}
            if not isinstance(counts, dict):
                counts = {}
            for key, value in counts.items():
                bucket[str(key)] = bucket.get(str(key), 0) + _to_int(value)

        cur.execute(
            """
            WITH order_scope AS (
                SELECT
                    marketplace_id,
                    amazon_order_id,
                    CASE
                        WHEN CAST(purchase_date AS DATE) >= DATEADD(day, -6, CAST(GETDATE() AS DATE)) THEN '0_6d'
                        WHEN CAST(purchase_date AS DATE) >= DATEADD(day, -13, CAST(GETDATE() AS DATE)) THEN '7_13d'
                        ELSE '14_29d'
                    END AS age_bucket
                FROM dbo.acc_order WITH (NOLOCK)
                WHERE status = 'Shipped'
                  AND CAST(purchase_date AS DATE) >= ?
                  AND CAST(purchase_date AS DATE) <= ?
            ),
            finance_orders AS (
                SELECT DISTINCT marketplace_id, amazon_order_id
                FROM dbo.acc_finance_transaction WITH (NOLOCK)
                WHERE CAST(posted_date AS DATE) >= ?
                  AND CAST(posted_date AS DATE) <= ?
                  AND amazon_order_id IS NOT NULL
                  AND LTRIM(RTRIM(amazon_order_id)) <> ''
            )
            SELECT
                o.marketplace_id,
                o.age_bucket,
                COUNT(*) AS orders_total,
                SUM(CASE WHEN f.amazon_order_id IS NOT NULL THEN 1 ELSE 0 END) AS orders_with_finance
            FROM order_scope o
            LEFT JOIN finance_orders f
              ON f.marketplace_id = o.marketplace_id
             AND f.amazon_order_id = o.amazon_order_id
            GROUP BY o.marketplace_id, o.age_bucket
            ORDER BY o.marketplace_id, o.age_bucket
            """,
            (
                date_from.isoformat(),
                date_to.isoformat(),
                date_from.isoformat(),
                date_to.isoformat(),
            ),
        )
        age_rows = _fetchall_dict(cur)
        age_breakdown_by_marketplace: dict[str, list[dict[str, Any]]] = {}
        for row in age_rows:
            marketplace_id = str(row.get("marketplace_id") or "")
            if not marketplace_id:
                continue
            orders_total = _to_int(row.get("orders_total"))
            orders_with_finance = _to_int(row.get("orders_with_finance"))
            age_breakdown_by_marketplace.setdefault(marketplace_id, []).append(
                {
                    "key": str(row.get("age_bucket") or ""),
                    "orders_total": orders_total,
                    "orders_with_finance": orders_with_finance,
                    "coverage_pct": round((orders_with_finance / orders_total) * 100, 1) if orders_total else 0.0,
                }
            )

        cur.execute(
            """
            WITH missing_rows AS (
                SELECT
                    ft.marketplace_id,
                    ft.amazon_order_id,
                    ft.transaction_type,
                    CASE
                        WHEN CAST(ft.posted_date AS DATE) >= DATEADD(day, -6, CAST(GETDATE() AS DATE)) THEN '0_6d'
                        WHEN CAST(ft.posted_date AS DATE) >= DATEADD(day, -13, CAST(GETDATE() AS DATE)) THEN '7_13d'
                        ELSE '14_29d'
                    END AS age_bucket
                FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
                LEFT JOIN dbo.acc_order o WITH (NOLOCK)
                  ON o.amazon_order_id = ft.amazon_order_id
                WHERE CAST(ft.posted_date AS DATE) >= ?
                  AND CAST(ft.posted_date AS DATE) <= ?
                  AND ft.amazon_order_id IS NOT NULL
                  AND LTRIM(RTRIM(ft.amazon_order_id)) <> ''
                  AND o.amazon_order_id IS NULL
            )
            SELECT marketplace_id, age_bucket, COUNT(*) AS row_count
            FROM missing_rows
            GROUP BY marketplace_id, age_bucket
            ORDER BY marketplace_id, age_bucket
            """,
            (date_from.isoformat(), date_to.isoformat()),
        )
        missing_age_rows = _fetchall_dict(cur)
        missing_age_counts_by_marketplace: dict[str, dict[str, int]] = {}
        for row in missing_age_rows:
            marketplace_id = str(row.get("marketplace_id") or "")
            if not marketplace_id:
                continue
            missing_age_counts_by_marketplace.setdefault(marketplace_id, {})[str(row.get("age_bucket") or "")] = _to_int(
                row.get("row_count")
            )

        cur.execute(
            """
            WITH missing_rows AS (
                SELECT
                    ft.marketplace_id,
                    ft.transaction_type
                FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
                LEFT JOIN dbo.acc_order o WITH (NOLOCK)
                  ON o.amazon_order_id = ft.amazon_order_id
                WHERE CAST(ft.posted_date AS DATE) >= ?
                  AND CAST(ft.posted_date AS DATE) <= ?
                  AND ft.amazon_order_id IS NOT NULL
                  AND LTRIM(RTRIM(ft.amazon_order_id)) <> ''
                  AND o.amazon_order_id IS NULL
            )
            SELECT marketplace_id, transaction_type, COUNT(*) AS row_count
            FROM missing_rows
            GROUP BY marketplace_id, transaction_type
            ORDER BY marketplace_id, row_count DESC
            """,
            (date_from.isoformat(), date_to.isoformat()),
        )
        missing_type_rows = _fetchall_dict(cur)
        missing_type_counts_by_marketplace: dict[str, dict[str, int]] = {}
        for row in missing_type_rows:
            marketplace_id = str(row.get("marketplace_id") or "")
            if not marketplace_id:
                continue
            missing_type_counts_by_marketplace.setdefault(marketplace_id, {})[str(row.get("transaction_type") or "")] = _to_int(
                row.get("row_count")
            )

        cur.execute(
            """
            WITH order_scope AS (
                SELECT marketplace_id, amazon_order_id, fulfillment_channel
                FROM dbo.acc_order WITH (NOLOCK)
                WHERE status = 'Shipped'
                  AND CAST(purchase_date AS DATE) >= ?
                  AND CAST(purchase_date AS DATE) <= ?
            ),
            finance_orders AS (
                SELECT DISTINCT marketplace_id, amazon_order_id
                FROM dbo.acc_finance_transaction WITH (NOLOCK)
                WHERE CAST(posted_date AS DATE) >= ?
                  AND CAST(posted_date AS DATE) <= ?
                  AND amazon_order_id IS NOT NULL
                  AND LTRIM(RTRIM(amazon_order_id)) <> ''
            )
            SELECT
                o.marketplace_id,
                o.fulfillment_channel,
                COUNT(*) AS orders_total,
                SUM(CASE WHEN f.amazon_order_id IS NOT NULL THEN 1 ELSE 0 END) AS orders_with_finance
            FROM order_scope o
            LEFT JOIN finance_orders f
              ON f.marketplace_id = o.marketplace_id
             AND f.amazon_order_id = o.amazon_order_id
            GROUP BY o.marketplace_id, o.fulfillment_channel
            ORDER BY o.marketplace_id, o.fulfillment_channel
            """,
            (
                date_from.isoformat(),
                date_to.isoformat(),
                date_from.isoformat(),
                date_to.isoformat(),
            ),
        )
        fulfillment_rows = _fetchall_dict(cur)
        fulfillment_breakdown_by_marketplace: dict[str, list[dict[str, Any]]] = {}
        for row in fulfillment_rows:
            marketplace_id = str(row.get("marketplace_id") or "")
            if not marketplace_id:
                continue
            orders_total = _to_int(row.get("orders_total"))
            orders_with_finance = _to_int(row.get("orders_with_finance"))
            fulfillment_breakdown_by_marketplace.setdefault(marketplace_id, []).append(
                {
                    "key": str(row.get("fulfillment_channel") or "?"),
                    "orders_total": orders_total,
                    "orders_with_finance": orders_with_finance,
                    "coverage_pct": round((orders_with_finance / orders_total) * 100, 1) if orders_total else 0.0,
                }
            )

        cur.execute(
            """
            SELECT marketplace_id, transaction_type, COUNT(*) AS row_count
            FROM dbo.acc_finance_transaction WITH (NOLOCK)
            WHERE CAST(posted_date AS DATE) >= ?
              AND CAST(posted_date AS DATE) <= ?
            GROUP BY marketplace_id, transaction_type
            """,
            (date_from.isoformat(), date_to.isoformat()),
        )
        imported_type_rows = _fetchall_dict(cur)
        imported_type_counts_by_marketplace: dict[str, dict[str, int]] = {}
        for row in imported_type_rows:
            marketplace_id = str(row.get("marketplace_id") or "")
            if not marketplace_id:
                continue
            imported_type_counts_by_marketplace.setdefault(marketplace_id, {})[str(row.get("transaction_type") or "")] = _to_int(
                row.get("row_count")
            )

        def _breakdown_to_map(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
            return {str(item.get("key") or ""): item for item in items}

        def _infer_gap_driver(
            *,
            age_breakdown: list[dict[str, Any]],
            fulfillment_breakdown: list[dict[str, Any]],
            imported_rows: int,
            unmapped_rows: int,
            missing_order_rows: int,
        ) -> str | None:
            if imported_rows <= 0:
                return None
            if missing_order_rows > 0:
                return "missing_orders_in_acc"
            if unmapped_rows > 0:
                return "unmapped_finance_rows"
            age_map = _breakdown_to_map(age_breakdown)
            recent_pct = float(age_map.get("0_6d", {}).get("coverage_pct") or 0.0)
            mid_pct = float(age_map.get("7_13d", {}).get("coverage_pct") or 0.0)
            old_pct = float(age_map.get("14_29d", {}).get("coverage_pct") or 0.0)
            if recent_pct >= 20 and old_pct <= 5 and old_pct + 10 < recent_pct:
                return "older_orders_not_backfilled"
            if mid_pct <= 5 and recent_pct >= 15:
                return "mid_window_gap"
            fulfillment_map = _breakdown_to_map(fulfillment_breakdown)
            afn_pct = float(fulfillment_map.get("AFN", {}).get("coverage_pct") or 0.0)
            mfn_pct = float(fulfillment_map.get("MFN", {}).get("coverage_pct") or 0.0)
            if afn_pct >= mfn_pct + 8:
                return "mfn_undercovered"
            if mfn_pct >= afn_pct + 8:
                return "afn_undercovered"
            return "general_coverage_gap"

        def _infer_missing_order_cause(
            *,
            missing_order_rows: int,
            missing_age_counts: dict[str, int],
            missing_type_counts: dict[str, int],
        ) -> str | None:
            if missing_order_rows <= 0:
                return None
            recent_rows = _to_int(missing_age_counts.get("0_6d"))
            mid_rows = _to_int(missing_age_counts.get("7_13d"))
            old_rows = _to_int(missing_age_counts.get("14_29d"))
            shipment_rows = _to_int(missing_type_counts.get("ShipmentEventList"))
            if recent_rows >= int(missing_order_rows * 0.7) and shipment_rows >= int(missing_order_rows * 0.6):
                return "recent_order_sync_lag"
            if old_rows >= int(missing_order_rows * 0.5):
                return "historical_order_backfill_gap"
            if mid_rows + old_rows >= int(missing_order_rows * 0.6):
                return "older_order_coverage_gap"
            if shipment_rows >= int(missing_order_rows * 0.6):
                return "shipment_events_without_orders"
            return "mixed_missing_orders"

        items: list[dict[str, Any]] = []
        for row in rows:
            marketplace_id = str(row.get("marketplace_id") or "")
            order_days = _to_int(row.get("order_days"))
            finance_days = _to_int(row.get("finance_days"))
            orders_total = _to_int(row.get("orders_total"))
            imported_orders = _to_int(row.get("imported_orders"))
            tracked_groups = _to_int(row.get("tracked_groups"))
            groups_with_rows = _to_int(row.get("groups_with_rows"))
            imported_rows = _to_int(row.get("imported_rows"))
            unmapped_rows = _to_int(row.get("unmapped_rows"))
            missing_order_rows = _to_int(row.get("missing_order_rows"))
            missing_order_distinct_orders = _to_int(row.get("missing_order_distinct_orders"))
            age_breakdown = age_breakdown_by_marketplace.get(marketplace_id, [])
            fulfillment_breakdown = fulfillment_breakdown_by_marketplace.get(marketplace_id, [])
            imported_type_counts = imported_type_counts_by_marketplace.get(marketplace_id, {})
            missing_age_counts = missing_age_counts_by_marketplace.get(marketplace_id, {})
            missing_type_counts = missing_type_counts_by_marketplace.get(marketplace_id, {})
            day_coverage_pct = round((finance_days / order_days) * 100, 1) if order_days else 0.0
            order_coverage_pct = round((imported_orders / orders_total) * 100, 1) if orders_total else 0.0
            likely_gap_driver = _infer_gap_driver(
                age_breakdown=age_breakdown,
                fulfillment_breakdown=fulfillment_breakdown,
                imported_rows=imported_rows,
                unmapped_rows=unmapped_rows,
                missing_order_rows=missing_order_rows,
            )
            missing_order_likely_cause = _infer_missing_order_cause(
                missing_order_rows=missing_order_rows,
                missing_age_counts=missing_age_counts,
                missing_type_counts=missing_type_counts,
            )

            if tracked_groups == 0:
                gap_reason = "no_groups_tracked"
                note = "No financial event groups were attributed to this marketplace in the requested window."
            elif groups_with_rows == 0:
                gap_reason = "groups_without_rows"
                note = "Groups were tracked, but no finance rows were persisted after event fetch."
            elif groups_with_rows > 0 and imported_rows == 0:
                gap_reason = "rows_not_attributed_to_marketplace"
                note = (
                    "Group fetch returned rows, but none were attributed to this marketplace after import. "
                    "Check marketplace inference and event mapping."
                )
            elif imported_rows > 0 and order_coverage_pct < 70:
                gap_reason = "coverage_gap_after_import"
                note = "Finance rows exist, but coverage against shipped orders is still below target."
            elif missing_order_rows > 0:
                gap_reason = "imported_rows_missing_orders"
                note = "Imported finance rows reference orders that are not present in acc_order."
            elif unmapped_rows > 0:
                gap_reason = "unmapped_finance_rows"
                note = "Some finance rows were imported without order identifiers and need manual interpretation."
            else:
                gap_reason = "ok"
                note = "Tracked groups and imported rows look internally consistent."

            if missing_order_rows > 0:
                cause_label = {
                    "recent_order_sync_lag": "Most missing rows are fresh 0-6d shipment/refund events, so this looks like order sync lag.",
                    "historical_order_backfill_gap": "Missing orders are concentrated in older finance rows, which points to a backfill gap in acc_order.",
                    "older_order_coverage_gap": "Missing orders are concentrated outside the freshest bucket, which suggests an older order coverage gap.",
                    "shipment_events_without_orders": "Missing rows are dominated by shipment events without matching orders in acc_order.",
                    "mixed_missing_orders": "Missing finance rows reference orders absent from acc_order, but the pattern is mixed.",
                }.get(missing_order_likely_cause or "", "Imported finance rows reference orders that are not present in acc_order.")
                note = (
                    f"{cause_label} Missing rows={missing_order_rows}, distinct orders={missing_order_distinct_orders}, "
                    f"unmapped rows={unmapped_rows}."
                )

            items.append(
                {
                    "marketplace_id": marketplace_id,
                    "marketplace_code": _marketplace_code(marketplace_id),
                    "tracked_groups": tracked_groups,
                    "groups_with_rows": groups_with_rows,
                    "imported_rows": imported_rows,
                    "imported_orders": imported_orders,
                    "unmapped_rows": unmapped_rows,
                    "missing_order_rows": missing_order_rows,
                    "missing_order_distinct_orders": missing_order_distinct_orders,
                    "event_type_counts": event_counts_by_marketplace.get(marketplace_id, {}),
                    "first_group_start": row.get("first_group_start"),
                    "last_group_end": row.get("last_group_end"),
                    "order_days": order_days,
                    "finance_days": finance_days,
                    "day_coverage_pct": day_coverage_pct,
                    "order_coverage_pct": order_coverage_pct,
                    "imported_transaction_type_counts": imported_type_counts,
                    "by_age_bucket": age_breakdown,
                    "by_fulfillment_channel": fulfillment_breakdown,
                    "missing_order_age_bucket_counts": missing_age_counts,
                    "missing_order_transaction_type_counts": missing_type_counts,
                    "missing_order_likely_cause": missing_order_likely_cause,
                    "likely_gap_driver": likely_gap_driver,
                    "gap_reason": gap_reason,
                    "note": note,
                }
            )

        return {
            "date_from": date_from,
            "date_to": date_to,
            "note": (
                "Amazon financialEventGroups is account-wide in v0. Marketplace attribution shown here is inferred "
                "after importing group events, not returned natively by the group list endpoint."
            ),
            "marketplaces": items,
        }
    finally:
        conn.close()


def get_order_revenue_integrity(date_from: date, date_to: date) -> dict[str, Any]:
    ensure_finance_center_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            WITH order_base AS (
                SELECT
                    o.id,
                    o.status,
                    o.amazon_order_id,
                    o.marketplace_id,
                    CASE WHEN o.revenue_pln IS NULL THEN 1 ELSE 0 END AS is_missing_revenue,
                    CASE WHEN o.order_total IS NULL OR o.order_total = 0 THEN 1 ELSE 0 END AS is_missing_order_total,
                    CASE WHEN o.status IN ('Canceled', 'Cancelled') THEN 1 ELSE 0 END AS is_canceled,
                    CASE WHEN o.status = 'Shipped' THEN 1 ELSE 0 END AS is_shipped,
                    CASE WHEN o.status = 'Unshipped' THEN 1 ELSE 0 END AS is_unshipped
                FROM dbo.acc_order o WITH (NOLOCK)
                WHERE CAST(o.purchase_date AS DATE) >= ?
                  AND CAST(o.purchase_date AS DATE) <= ?
            ),
            line_stats AS (
                SELECT
                    ol.order_id,
                    COUNT(*) AS line_count
                FROM dbo.acc_order_line ol WITH (NOLOCK)
                GROUP BY ol.order_id
            )
            SELECT
                COUNT(*) AS total_orders,
                SUM(CASE WHEN is_canceled = 0 THEN 1 ELSE 0 END) AS active_orders,
                SUM(is_canceled) AS canceled_orders,
                SUM(is_missing_revenue) AS missing_revenue_total,
                SUM(CASE WHEN is_canceled = 0 AND is_missing_revenue = 1 THEN 1 ELSE 0 END) AS missing_revenue_active,
                SUM(CASE WHEN is_shipped = 1 AND is_missing_revenue = 1 THEN 1 ELSE 0 END) AS missing_revenue_shipped,
                SUM(CASE WHEN is_unshipped = 1 AND is_missing_revenue = 1 THEN 1 ELSE 0 END) AS missing_revenue_unshipped,
                SUM(is_missing_order_total) AS missing_order_total_total,
                SUM(CASE WHEN is_canceled = 0 AND is_missing_order_total = 1 THEN 1 ELSE 0 END) AS missing_order_total_active,
                SUM(CASE WHEN is_shipped = 1 AND is_missing_order_total = 1 THEN 1 ELSE 0 END) AS missing_order_total_shipped,
                SUM(CASE WHEN is_unshipped = 1 AND is_missing_order_total = 1 THEN 1 ELSE 0 END) AS missing_order_total_unshipped,
                SUM(CASE WHEN is_shipped = 1 AND is_missing_revenue = 1 AND ISNULL(ls.line_count, 0) = 0 THEN 1 ELSE 0 END) AS shipped_missing_revenue_zero_line_headers,
                SUM(CASE WHEN is_unshipped = 1 AND is_missing_revenue = 1 AND ISNULL(ls.line_count, 0) = 0 THEN 1 ELSE 0 END) AS unshipped_missing_revenue_zero_line_headers
            FROM order_base ob
            LEFT JOIN line_stats ls
              ON ls.order_id = ob.id
            """,
            (date_from.isoformat(), date_to.isoformat()),
        )
        summary = _fetchone_dict(cur) or {}

        cur.execute(
            """
            SELECT
                status,
                SUM(CASE WHEN revenue_pln IS NULL THEN 1 ELSE 0 END) AS missing_revenue,
                SUM(CASE WHEN order_total IS NULL OR order_total = 0 THEN 1 ELSE 0 END) AS missing_order_total
            FROM dbo.acc_order WITH (NOLOCK)
            WHERE CAST(purchase_date AS DATE) >= ?
              AND CAST(purchase_date AS DATE) <= ?
            GROUP BY status
            ORDER BY status
            """,
            (date_from.isoformat(), date_to.isoformat()),
        )
        status_rows = _fetchall_dict(cur)
        missing_revenue_by_status: dict[str, int] = {}
        missing_order_total_by_status: dict[str, int] = {}
        for row in status_rows:
            status = str(row.get("status") or "?")
            missing_revenue_by_status[status] = _to_int(row.get("missing_revenue"))
            missing_order_total_by_status[status] = _to_int(row.get("missing_order_total"))

        note = (
            "Headline integrity excludes Canceled/Cancelled so zero-value cancellations do not inflate the risk signal. "
            "Shipped/Unshipped anomalies remain visible separately for data hygiene review."
        )
        if _to_int(summary.get("missing_revenue_unshipped")) > 0:
            note += " Unshipped zero-value headers should be reviewed separately; they do not look like normal completed sales."

        return {
            "date_from": date_from,
            "date_to": date_to,
            "total_orders": _to_int(summary.get("total_orders")),
            "active_orders": _to_int(summary.get("active_orders")),
            "canceled_orders": _to_int(summary.get("canceled_orders")),
            "missing_revenue_total": _to_int(summary.get("missing_revenue_total")),
            "missing_revenue_active": _to_int(summary.get("missing_revenue_active")),
            "missing_revenue_shipped": _to_int(summary.get("missing_revenue_shipped")),
            "missing_revenue_unshipped": _to_int(summary.get("missing_revenue_unshipped")),
            "missing_order_total_total": _to_int(summary.get("missing_order_total_total")),
            "missing_order_total_active": _to_int(summary.get("missing_order_total_active")),
            "missing_order_total_shipped": _to_int(summary.get("missing_order_total_shipped")),
            "missing_order_total_unshipped": _to_int(summary.get("missing_order_total_unshipped")),
            "shipped_missing_revenue_zero_line_headers": _to_int(summary.get("shipped_missing_revenue_zero_line_headers")),
            "unshipped_missing_revenue_zero_line_headers": _to_int(summary.get("unshipped_missing_revenue_zero_line_headers")),
            "missing_revenue_by_status": missing_revenue_by_status,
            "missing_order_total_by_status": missing_order_total_by_status,
            "note": note,
        }
    finally:
        conn.close()


def _summarize_finance_group_events(legacy_events: dict[str, list]) -> tuple[str, str, datetime | None, datetime | None]:
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
    payload = {
        "event_type_counts": event_type_counts,
        "total_rows": total_rows,
        "first_posted_at": first_posted_at.isoformat() if first_posted_at else None,
        "last_posted_at": last_posted_at.isoformat() if last_posted_at else None,
    }
    signature_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    signature_hash = hashlib.sha256(signature_json.encode("utf-8")).hexdigest()
    return signature_json, signature_hash, first_posted_at, last_posted_at


def backfill_finance_group_signatures(limit: int = 20, marketplace_id: str | None = None) -> dict[str, Any]:
    ensure_finance_center_schema()
    from app.connectors.amazon_sp_api.finances import FinancesClient

    conn = _connect()
    updated = 0
    failed = 0
    try:
        cur = conn.cursor()
        params: list[Any] = []
        where_parts = ["(payload_signature IS NULL OR payload_signature = '')"]
        if marketplace_id:
            where_parts.append("marketplace_id = ?")
            params.append(marketplace_id)
        cur.execute(
            f"""
            SELECT TOP ({int(limit)})
                financial_event_group_id,
                marketplace_id,
                processing_status,
                group_start,
                group_end
            FROM dbo.acc_fin_event_group_sync WITH (NOLOCK)
            WHERE {" AND ".join(where_parts)}
            ORDER BY last_synced_at ASC
            """,
            tuple(params),
        )
        rows = _fetchall_dict(cur)
        if not rows:
            return {"selected": 0, "updated": 0, "failed": 0}

        for row in rows:
            group_id = str(row.get("financial_event_group_id") or "")
            if not group_id:
                continue
            try:
                client_marketplace = row.get("marketplace_id") or settings.SP_API_PRIMARY_MARKETPLACE
                client = FinancesClient(marketplace_id=client_marketplace)
                legacy_events = asyncio.run(
                    client.list_financial_events_by_group_id(
                        event_group_id=group_id,
                        posted_after=row.get("group_start"),
                        posted_before=row.get("group_end"),
                        max_results=5000,
                    )
                )
                signature_json, signature_hash, first_posted_at, last_posted_at = _summarize_finance_group_events(legacy_events)
                open_refresh_after = None
                if str(row.get("processing_status") or "").lower() != "closed":
                    open_refresh_after = datetime.now(timezone.utc) + timedelta(minutes=20)
                cur.execute(
                    """
                    UPDATE dbo.acc_fin_event_group_sync
                    SET
                        event_type_counts_json = ?,
                        payload_signature = ?,
                        first_posted_at = ?,
                        last_posted_at = ?,
                        open_refresh_after = ?,
                        last_synced_at = SYSUTCDATETIME()
                    WHERE financial_event_group_id = ?
                    """,
                    (
                        signature_json,
                        signature_hash,
                        first_posted_at,
                        last_posted_at,
                        open_refresh_after,
                        group_id,
                    ),
                )
                conn.commit()
                updated += 1
            except Exception:
                conn.rollback()
                failed += 1
        return {"selected": len(rows), "updated": updated, "failed": failed}
    finally:
        conn.close()


def import_amazon_transactions(
    days_back: int = 30,
    marketplace_id: str | None = None,
    job_id: str | None = None,
) -> dict[str, Any]:
    ensure_finance_center_schema()
    result = asyncio.run(
        step_sync_finances(
            days_back=days_back,
            marketplace_id=marketplace_id,
            job_id=job_id,
        )
    )
    bridged_lines = 0
    if int(result.get("fee_rows", 0) or result.get("transactions", 0) or 0) > 0:
        bridged_lines = int(step_bridge_fees() or 0)
    alert_result = evaluate_finance_completeness_alerts(days_back=min(max(int(days_back or 30), 1), 30))
    return {
        "days_back": days_back,
        "marketplace_id": marketplace_id,
        "bridged_lines": bridged_lines,
        "completeness_alert": alert_result,
        **result,
    }


def build_settlement_summaries(job_id: str | None = None) -> dict[str, Any]:
    ensure_finance_center_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        if job_id:
            set_job_progress(job_id, progress_pct=15, message="Rebuilding settlement summaries", records_processed=0)
        cur.execute("DELETE FROM dbo.acc_fin_reconciliation_payout")
        cur.execute("DELETE FROM dbo.acc_fin_settlement_summary")
        cur.execute(
            """
            SELECT
                COALESCE(NULLIF(financial_event_group_id, ''), NULLIF(settlement_id, '')) AS payout_group_id,
                MAX(NULLIF(settlement_id, '')) AS settlement_id,
                marketplace_id,
                currency,
                MIN(CAST(posted_date AS DATE)) AS posted_from,
                MAX(CAST(posted_date AS DATE)) AS posted_to,
                COUNT(*) AS transaction_count,
                SUM(CAST(amount AS DECIMAL(18,4))) AS total_amount,
                SUM(CAST(COALESCE(amount_pln, amount * exchange_rate) AS DECIMAL(18,4))) AS total_amount_base
            FROM dbo.acc_finance_transaction WITH (NOLOCK)
            WHERE COALESCE(NULLIF(financial_event_group_id, ''), NULLIF(settlement_id, '')) IS NOT NULL
            GROUP BY COALESCE(NULLIF(financial_event_group_id, ''), NULLIF(settlement_id, '')), marketplace_id, currency
            """
        )
        rows = _fetchall_dict(cur)
        total = len(rows) or 1
        for idx, row in enumerate(rows, start=1):
            cur.execute(
                """
                MERGE dbo.acc_fin_settlement_summary AS tgt
                USING (SELECT ? AS financial_event_group_id) AS src
                ON tgt.financial_event_group_id = src.financial_event_group_id
                WHEN MATCHED THEN
                    UPDATE SET
                        settlement_id = ?,
                        financial_event_group_id = ?,
                        marketplace_id = ?,
                        currency = ?,
                        total_amount = ?,
                        total_amount_base = ?,
                        transaction_count = ?,
                        posted_from = ?,
                        posted_to = ?,
                        updated_at = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN
                    INSERT (settlement_id, financial_event_group_id, marketplace_id, currency, total_amount, total_amount_base, transaction_count, posted_from, posted_to)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    row["payout_group_id"],
                    row.get("settlement_id") or row["payout_group_id"],
                    row["payout_group_id"],
                    row.get("marketplace_id"),
                    row.get("currency") or "PLN",
                    row.get("total_amount") or 0,
                    row.get("total_amount_base") or 0,
                    row.get("transaction_count") or 0,
                    row.get("posted_from"),
                    row.get("posted_to"),
                    row.get("settlement_id") or row["payout_group_id"],
                    row["payout_group_id"],
                    row.get("marketplace_id"),
                    row.get("currency") or "PLN",
                    row.get("total_amount") or 0,
                    row.get("total_amount_base") or 0,
                    row.get("transaction_count") or 0,
                    row.get("posted_from"),
                    row.get("posted_to"),
                ),
            )
            if job_id and (idx == total or idx % 10 == 0):
                progress = min(95, 15 + int((idx / total) * 80))
                set_job_progress(
                    job_id,
                    progress_pct=progress,
                    message=f"Settlement summaries {idx}/{total}",
                    records_processed=idx,
                )
        conn.commit()
        return {"settlements": len(rows)}
    finally:
        conn.close()


def generate_ledger_from_amazon(days_back: int = 90, job_id: str | None = None) -> dict[str, Any]:
    ensure_finance_center_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cutoff = date.today() - timedelta(days=abs(days_back))
        if job_id:
            set_job_progress(job_id, progress_pct=10, message=f"Rebuilding Amazon ledger since {cutoff.isoformat()}", records_processed=0)
        cur.execute(
            """
            DELETE FROM dbo.acc_fin_ledger_entry
            WHERE source = 'amazon' AND entry_date >= ?
            """,
            (cutoff,),
        )
        conn.commit()
        mapping_rules = _load_mapping_rules(cur)
        cur.execute(
            """
            SELECT
                ft.id,
                ft.marketplace_id,
                ft.transaction_type,
                ft.amazon_order_id,
                ft.shipment_id,
                ft.sku,
                ft.posted_date,
                ft.settlement_id,
                ft.financial_event_group_id,
                ft.amount,
                ft.currency,
                ft.charge_type,
                ft.amount_pln,
                ft.exchange_rate,
                COALESCE(p.asin, rg.asin) AS asin,
                COALESCE(p.internal_sku, rg.internal_sku) AS internal_sku_hint,
                COALESCE(p.ean, rg.ean) AS ean_hint,
                COALESCE(ip.nazwa_pelna, rg.product_name, p.title, ft.sku) AS title_preferred,
                rg.parent_asin,
                rg.listing_role
            FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
            LEFT JOIN dbo.acc_product p WITH (NOLOCK)
                ON p.sku = ft.sku
            OUTER APPLY (
                SELECT TOP 1
                    r.internal_sku,
                    r.ean,
                    r.asin,
                    r.parent_asin,
                    r.product_name,
                    r.listing_role
                FROM dbo.acc_amazon_listing_registry r WITH (NOLOCK)
                WHERE (ft.sku IS NOT NULL AND (r.merchant_sku = ft.sku OR r.merchant_sku_alt = ft.sku))
                   OR (p.asin IS NOT NULL AND r.asin = p.asin)
                ORDER BY
                    CASE
                        WHEN ft.sku IS NOT NULL AND r.merchant_sku = ft.sku THEN 0
                        WHEN ft.sku IS NOT NULL AND r.merchant_sku_alt = ft.sku THEN 1
                        WHEN p.asin IS NOT NULL AND r.asin = p.asin THEN 2
                        ELSE 9
                    END,
                    r.updated_at DESC
            ) rg
            OUTER APPLY (
                SELECT TOP 1 ipm.nazwa_pelna
                FROM dbo.acc_import_products ipm WITH (NOLOCK)
                WHERE ipm.sku = ft.sku
                   OR (p.ean IS NOT NULL AND ipm.sku = p.ean)
                   OR (rg.ean IS NOT NULL AND ipm.sku = rg.ean)
                   OR (rg.internal_sku IS NOT NULL AND ipm.sku = rg.internal_sku)
                ORDER BY ipm.id DESC
            ) ip
            WHERE CAST(ft.posted_date AS DATE) >= ?
            """,
            (cutoff,),
        )
        rows = _fetchall_dict(cur)
        inserted = 0
        skipped = 0
        total = len(rows) or 1
        for idx, row in enumerate(rows, start=1):
            posted_date = row.get("posted_date")
            entry_date = posted_date.date() if isinstance(posted_date, datetime) else date.today()
            currency = str(row.get("currency") or "PLN").upper()
            amount = _to_float(row.get("amount"))
            fx_rate = _to_float(row.get("exchange_rate")) or _lookup_fx_rate(cur, currency, entry_date)
            amount_base = _to_float(row.get("amount_pln")) if row.get("amount_pln") is not None else round(amount * fx_rate, 4)
            rule = resolve_mapping_rule(row.get("charge_type"), mapping_rules)
            signed_amount = round(amount * rule.sign_multiplier, 4)
            signed_amount_base = round(amount_base * rule.sign_multiplier, 4)
            source_ref = str(row["id"])
            line_hash = build_entry_hash("amazon", source_ref, row.get("charge_type"), signed_amount, currency)
            cur.execute(
                "SELECT 1 FROM dbo.acc_fin_ledger_entry WITH (NOLOCK) WHERE source = 'amazon' AND source_line_hash = ?",
                (line_hash,),
            )
            if cur.fetchone():
                skipped += 1
                continue
            tags = {
                "marketplace_code": _marketplace_code(row.get("marketplace_id")),
                "transaction_type": row.get("transaction_type"),
                "charge_type": row.get("charge_type"),
                "shipment_id": row.get("shipment_id"),
                "financial_event_group_id": row.get("financial_event_group_id") or row.get("settlement_id"),
                "title_preferred": row.get("title_preferred"),
                "internal_sku_hint": row.get("internal_sku_hint"),
                "ean_hint": row.get("ean_hint"),
                "parent_asin": row.get("parent_asin"),
                "listing_role": row.get("listing_role"),
            }
            cur.execute(
                """
                INSERT INTO dbo.acc_fin_ledger_entry
                (
                    id, entry_date, source, source_ref, source_line_hash,
                    marketplace_id, settlement_id, financial_event_group_id, amazon_order_id, transaction_type, charge_type,
                    currency, amount, fx_rate, amount_base, base_currency,
                    account_code, tax_code, country, sku, asin, description, tags_json
                )
                VALUES
                (
                    NEWID(), ?, 'amazon', ?, ?,
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, 'PLN',
                    ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    entry_date,
                    source_ref,
                    line_hash,
                    row.get("marketplace_id"),
                    row.get("settlement_id") or row.get("financial_event_group_id"),
                    row.get("financial_event_group_id") or row.get("settlement_id"),
                    row.get("amazon_order_id"),
                    row.get("transaction_type"),
                    row.get("charge_type"),
                    currency,
                    Decimal(str(signed_amount)),
                    Decimal(str(fx_rate)),
                    Decimal(str(signed_amount_base)),
                    rule.account_code,
                    rule.tax_code,
                    _marketplace_country(row.get("marketplace_id")),
                    row.get("sku"),
                    row.get("asin"),
                    row.get("title_preferred") or row.get("charge_type") or row.get("transaction_type"),
                    json.dumps(tags, ensure_ascii=True),
                ),
            )
            inserted += 1
            if job_id and (idx == total or idx % 1000 == 0):
                progress = min(95, 10 + int((idx / total) * 85))
                set_job_progress(
                    job_id,
                    progress_pct=progress,
                    message=f"Ledger rows {idx}/{total}",
                    records_processed=inserted,
                )
        conn.commit()
        return {"inserted": inserted, "skipped": skipped}
    finally:
        conn.close()


def import_bank_csv(content: bytes, filename: str) -> dict[str, Any]:
    ensure_finance_center_schema()
    text = content.decode("utf-8-sig", errors="replace")
    try:
        dialect = csv.Sniffer().sniff(text[:2048], delimiters=";,")
    except Exception:
        dialect = csv.excel
        dialect.delimiter = ";"
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)

    def _pick(row: dict[str, Any], *candidates: str) -> str:
        lowered = {str(k).strip().lower(): v for k, v in row.items()}
        for candidate in candidates:
            if candidate in lowered and lowered[candidate] not in (None, ""):
                return str(lowered[candidate]).strip()
        return ""

    conn = _connect()
    try:
        cur = conn.cursor()
        inserted = 0
        skipped = 0
        for row in reader:
            bank_date = _pick(row, "date", "bank_date", "data", "booking_date")
            amount_raw = _pick(row, "amount", "kwota")
            currency = (_pick(row, "currency", "waluta") or "PLN").upper()
            description = _pick(row, "description", "opis", "title")
            reference = _pick(row, "reference", "ref", "tytul", "transaction_id")
            if not bank_date or not amount_raw:
                skipped += 1
                continue
            amount = Decimal(amount_raw.replace(" ", "").replace(",", "."))
            line_hash = build_entry_hash(bank_date, amount, currency, description, reference)
            cur.execute("SELECT 1 FROM dbo.acc_fin_bank_line WITH (NOLOCK) WHERE line_hash = ?", (line_hash,))
            if cur.fetchone():
                skipped += 1
                continue
            cur.execute(
                """
                INSERT INTO dbo.acc_fin_bank_line(id, bank_date, amount, currency, description, reference, line_hash)
                VALUES (NEWID(), ?, ?, ?, ?, ?, ?)
                """,
                (date.fromisoformat(bank_date), amount, currency, description or None, reference or None, line_hash),
            )
            inserted += 1
        conn.commit()
        return {"filename": filename, "inserted": inserted, "skipped": skipped}
    finally:
        conn.close()


def auto_match_payouts() -> dict[str, Any]:
    ensure_finance_center_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT settlement_id, financial_event_group_id, marketplace_id, currency, total_amount, total_amount_base, posted_to
            FROM dbo.acc_fin_settlement_summary WITH (NOLOCK)
            """
        )
        settlements = _fetchall_dict(cur)
        matched = 0
        for settlement in settlements:
            settlement_id = str(settlement["settlement_id"])
            financial_event_group_id = str(settlement.get("financial_event_group_id") or settlement_id)
            cur.execute(
                """
                SELECT TOP 1 id, amount, currency, bank_date
                FROM dbo.acc_fin_bank_line WITH (NOLOCK)
                WHERE id NOT IN (
                    SELECT bank_line_id
                    FROM dbo.acc_fin_reconciliation_payout WITH (NOLOCK)
                    WHERE bank_line_id IS NOT NULL AND status IN ('matched', 'partial')
                )
                ORDER BY ABS(
                    CASE
                        WHEN currency = ? THEN amount - ?
                        ELSE amount - ?
                    END
                ) ASC,
                ABS(DATEDIFF(day, bank_date, ?)) ASC
                """,
                (
                    settlement.get("currency") or "PLN",
                    settlement.get("total_amount") or 0,
                    settlement.get("total_amount_base") or 0,
                    settlement.get("posted_to") or date.today(),
                ),
            )
            bank_row = _fetchone_dict(cur)
            if not bank_row:
                cur.execute(
                    """
                    MERGE dbo.acc_fin_reconciliation_payout AS tgt
                    USING (SELECT ? AS financial_event_group_id) AS src
                    ON tgt.financial_event_group_id = src.financial_event_group_id
                    WHEN NOT MATCHED THEN
                        INSERT (settlement_id, financial_event_group_id, status, matched_amount, diff_amount, notes)
                        VALUES (?, ?, 'unmatched', 0, ?, ?);
                    """,
                    (
                        financial_event_group_id,
                        settlement_id,
                        financial_event_group_id,
                        settlement.get("total_amount_base") or 0,
                        "No matching bank line found",
                    ),
                )
                continue
            expected = _to_float(
                settlement.get("total_amount") if bank_row.get("currency") == settlement.get("currency") else settlement.get("total_amount_base")
            )
            actual = _to_float(bank_row.get("amount"))
            diff = round(actual - expected, 4)
            status = "matched" if abs(diff) <= 0.05 else "partial"
            cur.execute(
                """
                MERGE dbo.acc_fin_reconciliation_payout AS tgt
                USING (SELECT ? AS financial_event_group_id) AS src
                ON tgt.financial_event_group_id = src.financial_event_group_id
                WHEN MATCHED THEN
                    UPDATE SET
                        settlement_id = ?,
                        financial_event_group_id = ?,
                        bank_line_id = ?,
                        status = ?,
                        matched_amount = ?,
                        diff_amount = ?,
                        notes = ?,
                        updated_at = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN
                    INSERT (settlement_id, financial_event_group_id, bank_line_id, status, matched_amount, diff_amount, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    financial_event_group_id,
                    settlement_id,
                    financial_event_group_id,
                    bank_row.get("id"),
                    status,
                    expected,
                    diff,
                    f"Auto-match from bank import | settlement={settlement_id}",
                    settlement_id,
                    financial_event_group_id,
                    bank_row.get("id"),
                    status,
                    expected,
                    diff,
                    f"Auto-match from bank import | settlement={settlement_id}",
                ),
            )
            matched += 1
        conn.commit()
        return {"matched": matched, "settlements": len(settlements)}
    finally:
        conn.close()


def list_accounts() -> list[dict[str, Any]]:
    ensure_finance_center_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT account_code, name, account_type, parent_code, is_active
            FROM dbo.acc_fin_chart_of_accounts WITH (NOLOCK)
            ORDER BY account_code
            """
        )
        rows = _fetchall_dict(cur)
        for row in rows:
            row["is_active"] = bool(row.get("is_active"))
        return rows
    finally:
        conn.close()


def upsert_account(account_code: str, name: str, account_type: str, parent_code: str | None = None) -> dict[str, Any]:
    ensure_finance_center_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            MERGE dbo.acc_fin_chart_of_accounts AS tgt
            USING (SELECT ? AS account_code) AS src
            ON tgt.account_code = src.account_code
            WHEN MATCHED THEN
                UPDATE SET name = ?, account_type = ?, parent_code = ?, updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (account_code, name, account_type, parent_code)
                VALUES (?, ?, ?, ?);
            """,
            (account_code, name, account_type, parent_code, account_code, name, account_type, parent_code),
        )
        conn.commit()
        return {"account_code": account_code, "name": name, "account_type": account_type, "parent_code": parent_code}
    finally:
        conn.close()


def list_tax_codes() -> list[dict[str, Any]]:
    ensure_finance_center_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT code, vat_rate, oss_flag, country, description, is_active
            FROM dbo.acc_fin_tax_code WITH (NOLOCK)
            ORDER BY code
            """
        )
        rows = _fetchall_dict(cur)
        for row in rows:
            row["oss_flag"] = bool(row.get("oss_flag"))
            row["is_active"] = bool(row.get("is_active"))
        return rows
    finally:
        conn.close()


def upsert_tax_code(code: str, vat_rate: float, oss_flag: bool, country: str | None = None, description: str | None = None) -> dict[str, Any]:
    ensure_finance_center_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            MERGE dbo.acc_fin_tax_code AS tgt
            USING (SELECT ? AS code) AS src
            ON tgt.code = src.code
            WHEN MATCHED THEN
                UPDATE SET vat_rate = ?, oss_flag = ?, country = ?, description = ?, updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (code, vat_rate, oss_flag, country, description)
                VALUES (?, ?, ?, ?, ?);
            """,
            (code, vat_rate, 1 if oss_flag else 0, country, description, code, vat_rate, 1 if oss_flag else 0, country, description),
        )
        conn.commit()
        return {"code": code, "vat_rate": vat_rate, "oss_flag": oss_flag, "country": country, "description": description}
    finally:
        conn.close()


def get_finance_dashboard(date_from: date | None = None, date_to: date | None = None) -> dict[str, Any]:
    cache_key = f"finance_dashboard:{date_from}:{date_to}"
    cached = _fin_dash_cache_get(cache_key)
    if cached is not None:
        return cached
    ensure_finance_center_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        ledger_where = ["1=1"]
        ledger_params: list[Any] = []
        settlement_where = ["1=1"]
        settlement_params: list[Any] = []
        if date_from:
            ledger_where.append("entry_date >= ?")
            ledger_params.append(date_from)
            settlement_where.append("ISNULL(posted_to, posted_from) >= ?")
            settlement_params.append(date_from)
        if date_to:
            ledger_where.append("entry_date <= ?")
            ledger_params.append(date_to)
            settlement_where.append("ISNULL(posted_from, posted_to) <= ?")
            settlement_params.append(date_to)

        cur.execute(
            f"""
            SELECT
                SUM(CASE WHEN account_code = '700' THEN amount_base ELSE 0 END) AS revenue_base,
                SUM(CASE WHEN account_code IN ('520','530','540','550','580') THEN ABS(amount_base) ELSE 0 END) AS fees_base,
                SUM(CASE WHEN account_code = '220' THEN amount_base ELSE 0 END) AS vat_base,
                COUNT(*) AS ledger_rows
            FROM dbo.acc_fin_ledger_entry WITH (NOLOCK)
            WHERE {' AND '.join(ledger_where)}
            """,
            ledger_params,
        )
        ledger_summary = _fetchone_dict(cur) or {}

        cur.execute(
            f"""
            SELECT
                COUNT(*) AS settlement_rows,
                SUM(CASE WHEN financial_event_group_id IS NOT NULL AND LTRIM(RTRIM(financial_event_group_id)) <> '' THEN 1 ELSE 0 END) AS payout_rows
            FROM dbo.acc_fin_settlement_summary WITH (NOLOCK)
            WHERE {' AND '.join(settlement_where)}
            """,
            settlement_params,
        )
        settlement_summary = _fetchone_dict(cur) or {}

        cur.execute(
            """
            SELECT
                COUNT(*) AS bank_line_rows
            FROM dbo.acc_fin_bank_line WITH (NOLOCK)
            """
        )
        bank_summary = _fetchone_dict(cur) or {}

        cur.execute(
            """
            SELECT
                COUNT(*) AS unmatched_payouts
            FROM dbo.acc_fin_reconciliation_payout WITH (NOLOCK)
            WHERE ISNULL(status, 'unmatched') = 'unmatched'
            """
        )
        payout_summary = _fetchone_dict(cur) or {}

        if date_from and date_to:
            days_back = max(1, min(365, (date_to - date_from).days + 1))
        else:
            days_back = 30
        completeness = get_finance_data_completeness(days_back)
        gap_diagnostics = get_finance_marketplace_gap_diagnostics(days_back)
        sync_diagnostics = get_finance_sync_diagnostics(limit=12)
        recent_jobs = list_finance_jobs(1, 12)
        payout_reconciliation = list_payout_reconciliation()
        order_revenue_integrity = get_order_revenue_integrity(date(2025, 1, 1), date(2025, 12, 31))
        from app.services.order_pipeline import _collect_order_sync_health

        order_sync = _collect_order_sync_health()

        ledger_rows = _to_int(ledger_summary.get("ledger_rows"))
        settlement_rows = _to_int(settlement_summary.get("settlement_rows"))
        payout_rows = _to_int(settlement_summary.get("payout_rows"))
        bank_line_rows = _to_int(bank_summary.get("bank_line_rows"))
        revenue_base = _to_float(ledger_summary.get("revenue_base"))
        fees_base = _to_float(ledger_summary.get("fees_base"))
        vat_base = _to_float(ledger_summary.get("vat_base"))
        unmatched_payouts = _to_int(payout_summary.get("unmatched_payouts"))
        completeness_status = str(completeness.get("overall_status") or "unknown")
        is_partial = bool(completeness.get("partial"))

        def _section_status(*, has_data: bool, blocked_by_bank: bool = False, partial_hint: bool = False) -> str:
            if blocked_by_bank:
                return "blocked_by_missing_bank_import"
            if not has_data:
                return "no_data"
            if partial_hint:
                return "partial"
            return "real_data"

        sections = [
            {
                "key": "finance_feed",
                "label": "Finance feed",
                "status": _section_status(has_data=ledger_rows > 0, partial_hint=is_partial or completeness_status != "complete"),
                "note": completeness.get("note"),
            },
            {
                "key": "ledger",
                "label": "Ledger",
                "status": _section_status(has_data=ledger_rows > 0, partial_hint=is_partial),
                "note": f"rows={ledger_rows}" if ledger_rows > 0 else "No ledger rows yet",
            },
            {
                "key": "settlements",
                "label": "Settlements",
                "status": _section_status(has_data=settlement_rows > 0, partial_hint=payout_rows == 0 and settlement_rows > 0),
                "note": f"rows={settlement_rows}, payout_groups={payout_rows}" if settlement_rows > 0 else "No payout groups prepared yet",
            },
            {
                "key": "reconciliation",
                "label": "Payout reconciliation",
                "status": _section_status(has_data=payout_rows > 0, blocked_by_bank=payout_rows > 0 and bank_line_rows == 0),
                "note": (
                    "Payout groups exist, but no bank lines imported yet"
                    if payout_rows > 0 and bank_line_rows == 0
                    else (f"unmatched={unmatched_payouts}" if payout_rows > 0 else "No payout groups prepared yet")
                ),
            },
            {
                "key": "bank_import",
                "label": "Bank import",
                "status": _section_status(has_data=bank_line_rows > 0, blocked_by_bank=bank_line_rows == 0),
                "note": f"rows={bank_line_rows}" if bank_line_rows > 0 else "No bank lines imported",
            },
        ]

        recent_job_items = [
            {
                "id": str(item.get("id") or ""),
                "job_type": str(item.get("job_type") or ""),
                "status": str(item.get("status") or ""),
                "progress_pct": _to_float(item.get("progress_pct")),
                "progress_message": item.get("progress_message"),
                "started_at": item.get("started_at"),
                "finished_at": item.get("finished_at"),
                "records_processed": _to_int(item.get("records_processed")) if item.get("records_processed") is not None else None,
            }
            for item in (recent_jobs.get("items") or [])
        ]

        result = {
            "date_from": date_from,
            "date_to": date_to,
            "revenue_base": revenue_base,
            "fees_base": fees_base,
            "vat_base": vat_base,
            "profit_proxy": revenue_base - fees_base,
            "unmatched_payouts": unmatched_payouts,
            "ledger_rows": ledger_rows,
            "settlement_rows": settlement_rows,
            "payout_rows": payout_rows,
            "bank_line_rows": bank_line_rows,
            "completeness_status": completeness_status,
            "partial": is_partial,
            "note": completeness.get("note"),
            "sections": sections,
            "recent_jobs": recent_job_items,
            "completeness": completeness,
            "gap_diagnostics": gap_diagnostics,
            "order_revenue_integrity": order_revenue_integrity,
            "sync_diagnostics": sync_diagnostics,
            "payout_reconciliation": payout_reconciliation,
            "order_sync": order_sync,
        }
        _fin_dash_cache_set(cache_key, result, ttl_sec=120)
        return result
    finally:
        conn.close()


def list_ledger(
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    marketplace_id: str | None = None,
    account_code: str | None = None,
    sku: str | None = None,
    country: str | None = None,
    source: str | None = None,
) -> dict[str, Any]:
    ensure_finance_center_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        where = ["1=1"]
        params: list[Any] = []
        if date_from:
            where.append("entry_date >= ?")
            params.append(date_from)
        if date_to:
            where.append("entry_date <= ?")
            params.append(date_to)
        if marketplace_id:
            where.append("marketplace_id = ?")
            params.append(marketplace_id)
        if account_code:
            where.append("account_code = ?")
            params.append(account_code)
        if sku:
            where.append("sku = ?")
            params.append(sku)
        if country:
            where.append("country = ?")
            params.append(country)
        if source:
            where.append("source = ?")
            params.append(source)
        cur.execute(
            f"""
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                entry_date, source, source_ref, marketplace_id, settlement_id, financial_event_group_id, amazon_order_id,
                transaction_type, charge_type, currency, amount, fx_rate, amount_base, base_currency,
                account_code, tax_code, country, sku, asin, description, tags_json,
                CAST(reversed_entry_id AS NVARCHAR(40)) AS reversed_entry_id
            FROM dbo.acc_fin_ledger_entry WITH (NOLOCK)
            WHERE {' AND '.join(where)}
            ORDER BY entry_date DESC, created_at DESC
            """,
            params,
        )
        items = _fetchall_dict(cur)
        for item in items:
            try:
                item["tags_json"] = json.loads(item.get("tags_json")) if item.get("tags_json") else {}
            except Exception:
                item["tags_json"] = {}
            item["marketplace_code"] = _marketplace_code(item.get("marketplace_id"))
        return {"items": items, "total": len(items)}
    finally:
        conn.close()


def create_manual_ledger_entry(payload: dict[str, Any], actor: str | None = None) -> dict[str, Any]:
    ensure_finance_center_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        entry_id = str(uuid.uuid4())
        entry_date = payload.get("entry_date") or date.today()
        amount = Decimal(str(payload.get("amount") or 0))
        fx_rate = Decimal(str(payload.get("fx_rate") or 1))
        amount_base = Decimal(str(payload.get("amount_base") or (amount * fx_rate)))
        source_ref = str(payload.get("source_ref") or entry_id)
        line_hash = build_entry_hash("manual", source_ref, amount, payload.get("account_code"), payload.get("sku"))
        cur.execute(
            """
            INSERT INTO dbo.acc_fin_ledger_entry
            (
                id, entry_date, source, source_ref, source_line_hash, marketplace_id, settlement_id, financial_event_group_id,
                amazon_order_id, transaction_type, charge_type, currency, amount, fx_rate, amount_base,
                base_currency, account_code, tax_code, country, sku, asin, description, tags_json
            )
            VALUES
            (
                CAST(? AS UNIQUEIDENTIFIER), ?, 'manual', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PLN', ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                entry_id,
                entry_date,
                source_ref,
                line_hash,
                payload.get("marketplace_id"),
                payload.get("settlement_id"),
                payload.get("financial_event_group_id") or payload.get("settlement_id"),
                payload.get("amazon_order_id"),
                payload.get("transaction_type"),
                payload.get("charge_type"),
                payload.get("currency") or "PLN",
                amount,
                fx_rate,
                amount_base,
                payload.get("account_code"),
                payload.get("tax_code"),
                payload.get("country"),
                payload.get("sku"),
                payload.get("asin"),
                payload.get("description"),
                json.dumps({"actor": _actor(actor), **(payload.get("tags_json") or {})}, ensure_ascii=True),
            ),
        )
        conn.commit()
        return {"id": entry_id}
    finally:
        conn.close()


def reverse_ledger_entry(entry_id: str, actor: str | None = None) -> dict[str, Any]:
    ensure_finance_center_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                entry_date, source, source_ref, marketplace_id, settlement_id, financial_event_group_id, amazon_order_id,
                transaction_type, charge_type, currency, amount, fx_rate, amount_base, account_code,
                tax_code, country, sku, asin, description, tags_json
            FROM dbo.acc_fin_ledger_entry WITH (NOLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (entry_id,),
        )
        row = _fetchone_dict(cur)
        if not row:
            raise ValueError("ledger entry not found")
        new_id = str(uuid.uuid4())
        new_hash = build_entry_hash("reverse", entry_id, row.get("amount"), row.get("account_code"))
        tags = json.loads(row.get("tags_json")) if row.get("tags_json") else {}
        tags["reversed_by"] = _actor(actor)
        cur.execute(
            """
            INSERT INTO dbo.acc_fin_ledger_entry
            (
                id, entry_date, source, source_ref, source_line_hash, marketplace_id, settlement_id, financial_event_group_id,
                amazon_order_id, transaction_type, charge_type, currency, amount, fx_rate, amount_base,
                base_currency, account_code, tax_code, country, sku, asin, description, tags_json, reversed_entry_id
            )
            VALUES
            (
                CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PLN', ?, ?, ?, ?, ?, ?, ?, CAST(? AS UNIQUEIDENTIFIER)
            )
            """,
            (
                new_id,
                row["entry_date"],
                row["source"],
                f"{row['source_ref']}:reversal",
                new_hash,
                row.get("marketplace_id"),
                row.get("settlement_id"),
                row.get("financial_event_group_id") or row.get("settlement_id"),
                row.get("amazon_order_id"),
                row.get("transaction_type"),
                row.get("charge_type"),
                row.get("currency"),
                Decimal(str(-_to_float(row.get("amount")))),
                Decimal(str(_to_float(row.get("fx_rate"), 1))),
                Decimal(str(-_to_float(row.get("amount_base")))),
                row.get("account_code"),
                row.get("tax_code"),
                row.get("country"),
                row.get("sku"),
                row.get("asin"),
                f"Reversal of {entry_id}",
                json.dumps(tags, ensure_ascii=True),
                entry_id,
            ),
        )
        conn.commit()
        return {"id": new_id, "reversed_entry_id": entry_id}
    finally:
        conn.close()


def list_payout_reconciliation(status: str | None = None) -> dict[str, Any]:
    ensure_finance_center_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        where = ["1=1"]
        params: list[Any] = []
        if status:
            where.append("rp.status = ?")
            params.append(status)
        cur.execute(
            f"""
            SELECT
                ss.settlement_id,
                ss.financial_event_group_id,
                ss.marketplace_id,
                ss.currency,
                ss.total_amount,
                ss.total_amount_base,
                ss.transaction_count,
                ss.posted_from,
                ss.posted_to,
                CAST(rp.id AS NVARCHAR(40)) AS id,
                rp.status,
                CAST(rp.bank_line_id AS NVARCHAR(40)) AS bank_line_id,
                rp.matched_amount,
                rp.diff_amount,
                rp.notes,
                bl.bank_date,
                bl.amount AS bank_amount,
                bl.currency AS bank_currency,
                bl.reference
            FROM dbo.acc_fin_settlement_summary ss WITH (NOLOCK)
            LEFT JOIN dbo.acc_fin_reconciliation_payout rp WITH (NOLOCK)
                ON rp.financial_event_group_id = ss.financial_event_group_id
            LEFT JOIN dbo.acc_fin_bank_line bl WITH (NOLOCK)
                ON bl.id = rp.bank_line_id
            WHERE {' AND '.join(where)}
            ORDER BY ss.posted_to DESC, ss.settlement_id DESC
            """,
            params,
        )
        items = _fetchall_dict(cur)
        for item in items:
            item["marketplace_code"] = _marketplace_code(item.get("marketplace_id"))
            item["status"] = item.get("status") or "unmatched"
        return {"items": items, "total": len(items)}
    finally:
        conn.close()
