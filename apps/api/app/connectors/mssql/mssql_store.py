"""MSSQL store — Amazon Command Center data layer.

Reads order/profit data from acc_order + acc_order_line tables.
Manages alerts, jobs, and planning via acc_al_* tables.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
import math
from pathlib import Path
import tempfile
import threading
import time
import uuid
from typing import Any

import pyodbc
import structlog

from app.core.config import settings, MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc
from app.services.order_logistics_source import profit_logistics_join_sql, profit_logistics_value_sql

log = structlog.get_logger(__name__)


def _fx_case(currency_col: str = "o.currency") -> str:
    """Build SQL CASE expression for FX fallback using DB-sourced rates."""
    from app.core.fx_service import build_fx_case_sql
    return build_fx_case_sql(currency_col)


# ---------------------------------------------------------------------------
# In-memory TTL cache for heavy queries
# ---------------------------------------------------------------------------
_RESULT_CACHE: dict[str, tuple[float, Any]] = {}
_RESULT_CACHE_MAX = 30


def _rcache_get(key: str) -> Any | None:
    entry = _RESULT_CACHE.get(key)
    if entry is None:
        return None
    expires_at, value = entry
    if time.monotonic() > expires_at:
        _RESULT_CACHE.pop(key, None)
        return None
    return value


def _rcache_set(key: str, value: Any, ttl: int = 120) -> None:
    if len(_RESULT_CACHE) > _RESULT_CACHE_MAX:
        now = time.monotonic()
        expired = [k for k, (exp, _) in _RESULT_CACHE.items() if now > exp]
        for k in expired:
            _RESULT_CACHE.pop(k, None)
    _RESULT_CACHE[key] = (time.monotonic() + ttl, value)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALLOWED_JOB_TYPES = {
    "poll_sqs_notifications",
    "process_notification_events",
    "sync_orders",
    "sync_finances",
    "sync_inventory",
    "sync_ads",
    "sync_fba_inventory",
    "sync_fba_inbound",
    "sync_pricing",
    "sync_offer_fee_estimates",
    "sync_tkl_cache",
    "sync_purchase_prices",
    "sync_product_mapping",
    "sync_listings_to_products",
    "sync_amazon_listing_registry",
    "sync_taxonomy",
    "inventory_taxonomy_refresh",
    "inventory_sync_listings",
    "inventory_sync_snapshots",
    "inventory_sync_sales_traffic",
    "inventory_compute_rollups",
    "inventory_run_alerts",
    "inventory_apply_draft",
    "inventory_rollback_draft",
    "order_pipeline",
    "run_fba_alerts",
    "recompute_fba_replenishment",
    "family_sync_marketplace_listings",
    "family_matching_pipeline",
    "family_recompute_coverage",
    "finance_sync_transactions",
    "finance_prepare_settlements",
    "finance_generate_ledger",
    "finance_reconcile_payouts",
    "returns_seed_items",
    "returns_reconcile",
    "returns_rebuild_summary",
    "returns_sync_fba",
    "returns_backfill_fba",
    "fee_gap_watch_seed",
    "fee_gap_watch_recheck",
    "dhl_backfill_shipments",
    "dhl_sync_tracking_events",
    "dhl_import_billing_files",
    "dhl_seed_shipments_from_staging",
    "dhl_sync_costs",
    "dhl_aggregate_logistics",
    "dhl_shadow_logistics",
    "gls_import_billing_files",
    "gls_seed_shipments_from_staging",
    "gls_sync_costs",
    "gls_aggregate_logistics",
    "gls_shadow_logistics",
    "sync_bl_distribution_order_cache",
    "courier_backfill_identifier_sources",
    "courier_refresh_order_relations",
    "courier_order_universe_linking",
    "courier_refresh_shipment_outcomes",
    "courier_evaluate_alerts",
    "courier_verify_billing_completeness",
    "courier_refresh_monthly_kpis",
    "courier_estimate_preinvoice_costs",
    "courier_reconcile_estimated_costs",
    "courier_compute_estimation_kpis",
    "import_products_upload",
    "planning_refresh_actuals",
    "profit_ai_match_run",
    "content_apply_publish_mapping_suggestions",
    "content_refresh_product_type_definition",
    "cogs_import",
    "calc_profit",
    "generate_ai_report",
    "evaluate_alerts",
    "sync_exchange_rates",
    "sync_ecb_exchange_rates",
    "sync_catalog",
    "full_sync",
}

_MONTH_NAMES_PL = [
    "",
    "Styczeń",
    "Luty",
    "Marzec",
    "Kwiecień",
    "Maj",
    "Czerwiec",
    "Lipiec",
    "Sierpień",
    "Wrzesień",
    "Październik",
    "Listopad",
    "Grudzień",
]

_DEFAULT_ACTOR = "system"
_UNSET = object()
_RETRY_BACKOFF_STANDARD_MINUTES = (1, 5, 15, 60)
_ACTIVE_JOB_STATUSES = ("pending", "running", "retrying")
_AUTO_RETRY_DISABLED_JOB_TYPES = {
    "inventory_apply_draft",
    "inventory_rollback_draft",
    "content_apply_publish_mapping_suggestions",
    "courier_order_universe_linking",
}
_SINGLE_FLIGHT_JOB_TYPES = {
    "courier_order_universe_linking",
    "courier_refresh_monthly_kpis",
    "sync_ads",
    # Global manual jobs where duplicate concurrent execution creates UI noise,
    # wasted work, or conflicts in downstream writes.
    "calc_profit",
    "cogs_import",
    "content_apply_publish_mapping_suggestions",
    "content_refresh_product_type_definition",
    "family_matching_pipeline",
    "family_sync_marketplace_listings",
    "import_products_upload",
    "inventory_apply_draft",
    "inventory_taxonomy_refresh",
    "planning_refresh_actuals",
    "profit_ai_match_run",
    "recompute_fba_replenishment",
    "returns_backfill_fba",
    "returns_sync_fba",
    "sync_fba_inbound",
    "sync_fba_inventory",
    "sync_finances",
    "sync_inventory",
    "sync_listings_to_products",
    "sync_offer_fee_estimates",
    "sync_orders",
    "sync_pricing",
    "sync_tkl_cache",
}
_QUEUE_COURIER_HEAVY = "courier.heavy"
_QUEUE_INVENTORY_HEAVY = "inventory.heavy"
_QUEUE_FINANCE_HEAVY = "finance.heavy"
_QUEUE_FBA_MEDIUM = "fba.medium"
_QUEUE_CORE_MEDIUM = "core.medium"
_QUEUE_LIGHT_DEFAULT = "light.default"
_QUEUE_HEAVY_SET = {
    _QUEUE_COURIER_HEAVY,
    _QUEUE_INVENTORY_HEAVY,
    _QUEUE_FINANCE_HEAVY,
}
_JOB_TYPES_COURIER_HEAVY = {
    "courier_refresh_order_relations",
    "courier_order_universe_linking",
    "courier_refresh_shipment_outcomes",
    "courier_refresh_monthly_kpis",
}
_JOB_TYPES_INVENTORY_HEAVY = {
    "inventory_sync_listings",
    "inventory_sync_snapshots",
    "inventory_sync_sales_traffic",
    "inventory_compute_rollups",
}
_JOB_TYPES_FINANCE_HEAVY = {
    "finance_sync_transactions",
    "finance_prepare_settlements",
    "finance_generate_ledger",
    "finance_reconcile_payouts",
}
_JOB_TYPES_FBA_MEDIUM = {
    "sync_fba_inventory",
    "sync_fba_inbound",
    "sync_fba_reconciliation",
    "run_fba_alerts",
    "recompute_fba_replenishment",
}
_JOB_TYPES_CORE_MEDIUM = {
    "sync_orders",
    "sync_finances",
    "sync_inventory",
    "sync_purchase_prices",
    "calc_profit",
}
_JOB_TYPES_CANARY_COURIER_FBA = _JOB_TYPES_COURIER_HEAVY | _JOB_TYPES_FBA_MEDIUM
_TRANSIENT_SQLSTATE_CODES = {
    "08S01",
    "08001",
    "08007",
    "40001",
    "HYT00",
    "HYT01",
}
_TRANSIENT_ERROR_PATTERNS = (
    "timed out",
    "timeout",
    "deadlock",
    "lock request time out",
    "temporarily unavailable",
    "temporary failure",
    "temporary error",
    "connection reset",
    "connection aborted",
    "connection refused",
    "could not connect",
    "server is busy",
    "transport-level error",
    "network-related",
    "try again",
    "too many requests",
    "rate exceeded",
    "429",
    "502",
    "503",
    "504",
)


class JobTransientError(RuntimeError):
    """Explicit marker for retryable job failures."""


class JobPermanentError(RuntimeError):
    """Explicit marker for non-retryable job failures."""


_SCHEDULER_LOCK_CONN: Any | None = None

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _assert_mssql_enabled() -> None:
    if not settings.mssql_enabled:
        raise RuntimeError("MSSQL is not configured (missing MSSQL_USER/MSSQL_PASSWORD).")


def _connect() -> pyodbc.Connection:
    _assert_mssql_enabled()
    return connect_acc(autocommit=False, timeout=20)


def _fetchall_dict(cur: pyodbc.Cursor) -> list[dict[str, Any]]:
    cols = [c[0] for c in cur.description] if cur.description else []
    out: list[dict[str, Any]] = []
    for row in cur.fetchall():
        out.append({cols[i]: row[i] for i in range(len(cols))})
    return out


def _to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    text = str(value).strip()
    if not text:
        return None
    for candidate in (text, text[:19], text[:10]):
        try:
            dt = datetime.fromisoformat(candidate)
            if isinstance(dt, datetime):
                return dt
        except ValueError:
            continue
    return None


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


def _to_int_list(value: Any) -> list[int]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raw_items = [item.strip() for item in str(value).split(",")]
    out: list[int] = []
    for item in raw_items:
        if item is None:
            continue
        text = str(item).strip()
        if not text:
            continue
        try:
            out.append(int(float(text)))
        except Exception:
            continue
    return out


def _to_text_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raw_items = [item.strip() for item in str(value).split(",")]
    out: list[str] = []
    for item in raw_items:
        text = str(item or "").strip()
        if text:
            out.append(text)
    return out


def _month_label(year: int, month: int) -> str:
    if month < 1 or month > 12:
        return f"{year}-{month:02d}"
    return f"{_MONTH_NAMES_PL[month]} {year}"


def _marketplace_code(marketplace_id: str | None) -> str:
    """Resolve marketplace_id (e.g. 'A1PA6795UKMFR9') to code (e.g. 'DE')."""
    if not marketplace_id:
        return ""
    info = MARKETPLACE_REGISTRY.get(marketplace_id)
    return info["code"] if info else marketplace_id[:6]


def _actor(value: str | None) -> str:
    """Return actor name, falling back to DEFAULT_ACTOR from settings or constant."""
    if value:
        return value
    return getattr(settings, "DEFAULT_ACTOR", _DEFAULT_ACTOR)


def _default_retry_policy(job_type: str) -> str:
    return "none" if job_type in _AUTO_RETRY_DISABLED_JOB_TYPES else "standard"


def _default_job_queue(job_type: str) -> str:
    if job_type in _JOB_TYPES_COURIER_HEAVY:
        return _QUEUE_COURIER_HEAVY
    if job_type in _JOB_TYPES_INVENTORY_HEAVY:
        return _QUEUE_INVENTORY_HEAVY
    if job_type in _JOB_TYPES_FINANCE_HEAVY:
        return _QUEUE_FINANCE_HEAVY
    if job_type in _JOB_TYPES_FBA_MEDIUM:
        return _QUEUE_FBA_MEDIUM
    if job_type in _JOB_TYPES_CORE_MEDIUM:
        return _QUEUE_CORE_MEDIUM
    return _QUEUE_LIGHT_DEFAULT


def resolve_job_queue(job_type: str) -> str:
    routing = settings.job_queue_routing
    if job_type in routing:
        return routing[job_type]
    return _default_job_queue(job_type)


def _canary_mode() -> str:
    raw = str(settings.JOB_CANARY_MODE or "").strip().lower()
    if raw in {"off", "courier_fba", "all"}:
        return raw
    return "courier_fba"


def _should_dispatch_via_worker(job_type: str) -> bool:
    mode = _canary_mode()
    if mode == "off":
        return False
    if mode == "all":
        return True
    return job_type in _JOB_TYPES_CANARY_COURIER_FBA


def _normalize_retry_policy(job_type: str, params: dict[str, Any] | None = None) -> str:
    default_policy = _default_retry_policy(job_type)
    raw_policy = str((params or {}).get("retry_policy") or default_policy).strip().lower()
    if default_policy == "none":
        return "none"
    if raw_policy not in {"none", "standard"}:
        return default_policy
    return raw_policy


def _default_max_retries(job_type: str, retry_policy: str | None = None) -> int:
    policy = retry_policy or _default_retry_policy(job_type)
    if policy == "none":
        return 0
    return len(_RETRY_BACKOFF_STANDARD_MINUTES)


def _normalize_max_retries(job_type: str, params: dict[str, Any] | None = None, retry_policy: str | None = None) -> int:
    policy = retry_policy or _normalize_retry_policy(job_type, params)
    if policy == "none":
        return 0
    raw_value = (params or {}).get("max_retries")
    try:
        requested = int(raw_value) if raw_value is not None else _default_max_retries(job_type, policy)
    except Exception:
        requested = _default_max_retries(job_type, policy)
    return max(1, min(requested, len(_RETRY_BACKOFF_STANDARD_MINUTES)))


def _retry_backoff_minutes(retry_count: int, retry_policy: str = "standard") -> int:
    if retry_policy != "standard":
        return 0
    idx = min(max(int(retry_count or 1), 1), len(_RETRY_BACKOFF_STANDARD_MINUTES)) - 1
    return int(_RETRY_BACKOFF_STANDARD_MINUTES[idx])


def _extract_error_code(exc: BaseException | str | None) -> str | None:
    if exc is None:
        return None
    if isinstance(exc, str):
        return None
    args = getattr(exc, "args", ()) or ()
    for candidate in args:
        text = str(candidate).strip()
        if not text:
            continue
        token = text.split()[0].strip("[](),")
        if token:
            return token[:120]
    return exc.__class__.__name__[:120]


def _classify_job_error(exc: BaseException | str) -> tuple[str, str | None]:
    if isinstance(exc, JobTransientError):
        return "transient", _extract_error_code(exc) or "job_transient"
    if isinstance(exc, JobPermanentError):
        return "permanent", _extract_error_code(exc) or "job_permanent"
    if isinstance(exc, (TimeoutError, ConnectionError)):
        return "transient", _extract_error_code(exc) or exc.__class__.__name__
    if isinstance(exc, pyodbc.Error):
        code = (_extract_error_code(exc) or "").upper()
        if code in _TRANSIENT_SQLSTATE_CODES:
            return "transient", code
    message = str(exc).strip().lower()
    if any(pattern in message for pattern in _TRANSIENT_ERROR_PATTERNS):
        return "transient", _extract_error_code(exc) or "transient_error"
    return "permanent", _extract_error_code(exc) or type(exc).__name__[:120]


# ---------------------------------------------------------------------------
# Schema bootstrap — acc_al_* helper tables
# ---------------------------------------------------------------------------


def ensure_v2_schema() -> None:
    _assert_mssql_enabled()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
IF OBJECT_ID('dbo.acc_al_alert_rules', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_alert_rules (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        name NVARCHAR(200) NOT NULL,
        description NVARCHAR(500) NULL,
        rule_type NVARCHAR(80) NOT NULL,
        marketplace_id NVARCHAR(160) NULL,
        sku NVARCHAR(120) NULL,
        category NVARCHAR(120) NULL,
        threshold_value DECIMAL(18,4) NULL,
        threshold_operator NVARCHAR(8) NULL,
        severity NVARCHAR(20) NOT NULL DEFAULT 'warning',
        is_active BIT NOT NULL DEFAULT 1,
        created_by NVARCHAR(120) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_al_alert_rules_type ON dbo.acc_al_alert_rules(rule_type, is_active);
END

IF OBJECT_ID('dbo.acc_al_alerts', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_alerts (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        rule_id UNIQUEIDENTIFIER NOT NULL,
        marketplace_id NVARCHAR(160) NULL,
        sku NVARCHAR(120) NULL,
        title NVARCHAR(300) NOT NULL,
        detail NVARCHAR(MAX) NULL,
        detail_json NVARCHAR(MAX) NULL,
        context_json NVARCHAR(MAX) NULL,
        severity NVARCHAR(20) NOT NULL,
        current_value DECIMAL(18,4) NULL,
        is_read BIT NOT NULL DEFAULT 0,
        is_resolved BIT NOT NULL DEFAULT 0,
        triggered_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        resolved_at DATETIME2 NULL,
        resolved_by NVARCHAR(120) NULL
    );
    CREATE INDEX IX_acc_al_alerts_state ON dbo.acc_al_alerts(is_resolved, severity, triggered_at);
    CREATE INDEX IX_acc_al_alerts_rule ON dbo.acc_al_alerts(rule_id, is_resolved);
END

IF COL_LENGTH('dbo.acc_al_alerts', 'detail_json') IS NULL
BEGIN
    ALTER TABLE dbo.acc_al_alerts ADD detail_json NVARCHAR(MAX) NULL;
END

IF COL_LENGTH('dbo.acc_al_alerts', 'context_json') IS NULL
BEGIN
    ALTER TABLE dbo.acc_al_alerts ADD context_json NVARCHAR(MAX) NULL;
END

IF OBJECT_ID('dbo.acc_al_jobs', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_jobs (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        celery_task_id NVARCHAR(80) NULL,
        job_type NVARCHAR(80) NOT NULL,
        marketplace_id NVARCHAR(160) NULL,
        trigger_source NVARCHAR(20) NOT NULL DEFAULT 'manual',
        triggered_by NVARCHAR(120) NULL,
        status NVARCHAR(20) NOT NULL DEFAULT 'pending',
        progress_pct INT NOT NULL DEFAULT 0,
        progress_message NVARCHAR(300) NULL,
        records_processed INT NULL,
        error_message NVARCHAR(MAX) NULL,
        retry_count INT NOT NULL DEFAULT 0,
        max_retries INT NOT NULL DEFAULT 0,
        next_retry_at DATETIME2 NULL,
        last_error_code NVARCHAR(120) NULL,
        last_error_kind NVARCHAR(20) NULL,
        retry_policy NVARCHAR(20) NOT NULL DEFAULT 'none',
        started_at DATETIME2 NULL,
        last_heartbeat_at DATETIME2 NULL,
        finished_at DATETIME2 NULL,
        duration_seconds FLOAT NULL,
        params_json NVARCHAR(MAX) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_al_jobs_main ON dbo.acc_al_jobs(job_type, status, created_at);
END

IF COL_LENGTH('dbo.acc_al_jobs', 'last_heartbeat_at') IS NULL
BEGIN
    ALTER TABLE dbo.acc_al_jobs ADD last_heartbeat_at DATETIME2 NULL;
END

IF COL_LENGTH('dbo.acc_al_jobs', 'retry_count') IS NULL
BEGIN
    ALTER TABLE dbo.acc_al_jobs ADD retry_count INT NOT NULL DEFAULT 0;
END

IF COL_LENGTH('dbo.acc_al_jobs', 'max_retries') IS NULL
BEGIN
    ALTER TABLE dbo.acc_al_jobs ADD max_retries INT NOT NULL DEFAULT 0;
END

IF COL_LENGTH('dbo.acc_al_jobs', 'next_retry_at') IS NULL
BEGIN
    ALTER TABLE dbo.acc_al_jobs ADD next_retry_at DATETIME2 NULL;
END

IF COL_LENGTH('dbo.acc_al_jobs', 'last_error_code') IS NULL
BEGIN
    ALTER TABLE dbo.acc_al_jobs ADD last_error_code NVARCHAR(120) NULL;
END

IF COL_LENGTH('dbo.acc_al_jobs', 'last_error_kind') IS NULL
BEGIN
    ALTER TABLE dbo.acc_al_jobs ADD last_error_kind NVARCHAR(20) NULL;
END

IF COL_LENGTH('dbo.acc_al_jobs', 'retry_policy') IS NULL
BEGIN
    ALTER TABLE dbo.acc_al_jobs ADD retry_policy NVARCHAR(20) NOT NULL DEFAULT 'none';
END

IF COL_LENGTH('dbo.acc_al_jobs', 'lease_owner') IS NULL
BEGIN
    ALTER TABLE dbo.acc_al_jobs ADD lease_owner NVARCHAR(160) NULL;
END

IF COL_LENGTH('dbo.acc_al_jobs', 'lease_expires_at') IS NULL
BEGIN
    ALTER TABLE dbo.acc_al_jobs ADD lease_expires_at DATETIME2 NULL;
END

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_acc_al_jobs_retry'
      AND object_id = OBJECT_ID('dbo.acc_al_jobs')
)
BEGIN
    CREATE INDEX IX_acc_al_jobs_retry
        ON dbo.acc_al_jobs(status, next_retry_at, created_at DESC);
END

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_acc_al_jobs_lease'
      AND object_id = OBJECT_ID('dbo.acc_al_jobs')
)
BEGIN
    CREATE INDEX IX_acc_al_jobs_lease
        ON dbo.acc_al_jobs(status, lease_expires_at, job_type, created_at DESC);
END

IF OBJECT_ID('dbo.acc_al_job_semaphore', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_job_semaphore (
        semaphore_key NVARCHAR(64) NOT NULL,
        slot_no INT NOT NULL,
        holder_job_id UNIQUEIDENTIFIER NULL,
        lease_expires_at DATETIME2 NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_acc_al_job_semaphore PRIMARY KEY (semaphore_key, slot_no)
    );
    CREATE INDEX IX_acc_al_job_semaphore_holder ON dbo.acc_al_job_semaphore(holder_job_id, lease_expires_at);
END

IF COL_LENGTH('dbo.acc_al_job_semaphore', 'holder_job_id') IS NULL
BEGIN
    ALTER TABLE dbo.acc_al_job_semaphore ADD holder_job_id UNIQUEIDENTIFIER NULL;
END

IF COL_LENGTH('dbo.acc_al_job_semaphore', 'lease_expires_at') IS NULL
BEGIN
    ALTER TABLE dbo.acc_al_job_semaphore ADD lease_expires_at DATETIME2 NULL;
END

IF COL_LENGTH('dbo.acc_al_job_semaphore', 'updated_at') IS NULL
BEGIN
    ALTER TABLE dbo.acc_al_job_semaphore ADD updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME();
END

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_acc_al_job_semaphore_holder'
      AND object_id = OBJECT_ID('dbo.acc_al_job_semaphore')
)
BEGIN
    CREATE INDEX IX_acc_al_job_semaphore_holder ON dbo.acc_al_job_semaphore(holder_job_id, lease_expires_at);
END

IF OBJECT_ID('dbo.acc_al_plans', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_plans (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [year] INT NOT NULL,
        [month] INT NOT NULL,
        status NVARCHAR(20) NOT NULL DEFAULT 'draft',
        created_by NVARCHAR(120) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_acc_al_plan_month UNIQUE([year], [month])
    );
END

IF OBJECT_ID('dbo.acc_al_plan_lines', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_plan_lines (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        plan_id INT NOT NULL,
        marketplace_id NVARCHAR(160) NOT NULL,
        target_revenue_pln DECIMAL(18,2) NOT NULL DEFAULT 0,
        target_orders INT NOT NULL DEFAULT 0,
        target_acos_pct DECIMAL(9,2) NOT NULL DEFAULT 0,
        target_cm_pct DECIMAL(9,2) NOT NULL DEFAULT 0,
        budget_ads_pln DECIMAL(18,2) NOT NULL DEFAULT 0,
        actual_revenue_pln DECIMAL(18,2) NULL,
        actual_orders INT NULL,
        actual_acos_pct DECIMAL(9,2) NULL,
        actual_cm_pct DECIMAL(9,2) NULL,
        CONSTRAINT FK_acc_al_plan_lines_plan FOREIGN KEY (plan_id) REFERENCES dbo.acc_al_plans(id)
    );
    CREATE INDEX IX_acc_al_plan_lines_plan ON dbo.acc_al_plan_lines(plan_id, marketplace_id);
END

IF OBJECT_ID('dbo.acc_al_profit_snapshot', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_profit_snapshot (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        sales_date DATE NOT NULL,
        order_number NVARCHAR(180) NULL,
        sku NVARCHAR(120) NULL,
        title NVARCHAR(300) NULL,
        quantity FLOAT NOT NULL DEFAULT 0,
        revenue_net DECIMAL(18,2) NOT NULL DEFAULT 0,
        revenue_gross DECIMAL(18,2) NOT NULL DEFAULT 0,
        cogs DECIMAL(18,2) NOT NULL DEFAULT 0,
        transport DECIMAL(18,2) NOT NULL DEFAULT 0,
        channel NVARCHAR(180) NULL,
        source_table NVARCHAR(180) NOT NULL,
        synced_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_al_profit_snapshot_date ON dbo.acc_al_profit_snapshot(sales_date, channel);
END

IF OBJECT_ID('dbo.acc_audit_log', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_audit_log (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        audit_date DATE NOT NULL,
        overall_status NVARCHAR(20) NOT NULL,
        cogs_coverage_pct DECIMAL(9,2) NULL,
        mapping_coverage_pct DECIMAL(9,2) NULL,
        total_issues INT NOT NULL DEFAULT 0,
        loss_lines INT NULL,
        avg_cogs_pct DECIMAL(9,2) NULL,
        issues_json NVARCHAR(MAX) NULL,
        checks_json NVARCHAR(MAX) NULL,
        trigger_source NVARCHAR(20) NOT NULL DEFAULT 'scheduler',
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE UNIQUE INDEX IX_acc_audit_log_date
        ON dbo.acc_audit_log(audit_date, trigger_source);
END

IF OBJECT_ID('dbo.acc_al_product_tasks', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_product_tasks (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        task_type NVARCHAR(40) NOT NULL,            -- pricing | content | watchlist
        sku NVARCHAR(120) NOT NULL,
        marketplace_id NVARCHAR(160) NULL,
        status NVARCHAR(30) NOT NULL DEFAULT 'open',
        title NVARCHAR(300) NULL,
        note NVARCHAR(MAX) NULL,
        owner NVARCHAR(120) NULL,
        source_page NVARCHAR(80) NULL,
        payload_json NVARCHAR(MAX) NULL,
        created_by NVARCHAR(120) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_al_product_tasks_main
        ON dbo.acc_al_product_tasks(task_type, status, created_at);
    CREATE INDEX IX_acc_al_product_tasks_sku
        ON dbo.acc_al_product_tasks(sku, marketplace_id, created_at);
END

IF COL_LENGTH('dbo.acc_al_product_tasks', 'owner') IS NULL
BEGIN
    ALTER TABLE dbo.acc_al_product_tasks ADD owner NVARCHAR(120) NULL;
END

IF OBJECT_ID('dbo.acc_al_product_task_comments', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_product_task_comments (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        task_id UNIQUEIDENTIFIER NOT NULL,
        comment NVARCHAR(MAX) NOT NULL,
        author NVARCHAR(120) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_al_product_task_comments_task
        ON dbo.acc_al_product_task_comments(task_id, created_at DESC);
END

IF OBJECT_ID('dbo.acc_al_task_owner_rules', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_task_owner_rules (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        is_active BIT NOT NULL DEFAULT 1,
        priority INT NOT NULL DEFAULT 100,
        task_type NVARCHAR(40) NULL,
        marketplace_id NVARCHAR(160) NULL,
        brand NVARCHAR(120) NULL,
        owner NVARCHAR(120) NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_al_task_owner_rules_match
        ON dbo.acc_al_task_owner_rules(is_active, task_type, marketplace_id, brand, priority);
END

IF OBJECT_ID('dbo.acc_co_tasks', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_co_tasks (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        task_type NVARCHAR(40) NOT NULL,
        sku NVARCHAR(120) NOT NULL,
        asin NVARCHAR(40) NULL,
        marketplace_id NVARCHAR(160) NULL,
        priority NVARCHAR(10) NOT NULL DEFAULT 'p1',
        owner NVARCHAR(120) NULL,
        due_date DATETIME2 NULL,
        status NVARCHAR(30) NOT NULL DEFAULT 'open',
        tags_json NVARCHAR(MAX) NULL,
        title NVARCHAR(300) NULL,
        note NVARCHAR(MAX) NULL,
        source_page NVARCHAR(80) NULL,
        created_by NVARCHAR(120) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_co_tasks_main
        ON dbo.acc_co_tasks(task_type, status, priority, created_at);
    CREATE INDEX IX_acc_co_tasks_owner_due
        ON dbo.acc_co_tasks(owner, due_date, status);
    CREATE INDEX IX_acc_co_tasks_sku
        ON dbo.acc_co_tasks(sku, marketplace_id, created_at);
END

IF OBJECT_ID('dbo.acc_co_versions', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_co_versions (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        sku NVARCHAR(120) NOT NULL,
        asin NVARCHAR(40) NULL,
        marketplace_id NVARCHAR(160) NOT NULL,
        version_no INT NOT NULL,
        status NVARCHAR(30) NOT NULL DEFAULT 'draft',
        fields_json NVARCHAR(MAX) NOT NULL,
        compliance_notes NVARCHAR(MAX) NULL,
        created_by NVARCHAR(120) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        approved_by NVARCHAR(120) NULL,
        approved_at DATETIME2 NULL,
        published_at DATETIME2 NULL,
        parent_version_id UNIQUEIDENTIFIER NULL
    );
    CREATE UNIQUE INDEX IX_acc_co_versions_unique
        ON dbo.acc_co_versions(sku, marketplace_id, version_no);
    CREATE INDEX IX_acc_co_versions_lookup
        ON dbo.acc_co_versions(sku, marketplace_id, status, created_at DESC);
END

IF OBJECT_ID('dbo.acc_co_policy_rules', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_co_policy_rules (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        name NVARCHAR(200) NOT NULL,
        pattern NVARCHAR(1000) NOT NULL,
        severity NVARCHAR(20) NOT NULL,
        applies_to_json NVARCHAR(MAX) NULL,
        is_active BIT NOT NULL DEFAULT 1,
        created_by NVARCHAR(120) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_co_policy_rules_active
        ON dbo.acc_co_policy_rules(is_active, severity, created_at);
END

IF OBJECT_ID('dbo.acc_co_policy_checks', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_co_policy_checks (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        version_id UNIQUEIDENTIFIER NOT NULL,
        results_json NVARCHAR(MAX) NOT NULL,
        passed BIT NOT NULL,
        checked_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        checker_version NVARCHAR(80) NULL
    );
    CREATE INDEX IX_acc_co_policy_checks_version
        ON dbo.acc_co_policy_checks(version_id, checked_at DESC);
END

IF OBJECT_ID('dbo.acc_co_assets', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_co_assets (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        filename NVARCHAR(260) NOT NULL,
        mime NVARCHAR(120) NOT NULL,
        content_hash NVARCHAR(120) NOT NULL,
        storage_path NVARCHAR(500) NOT NULL,
        metadata_json NVARCHAR(MAX) NULL,
        status NVARCHAR(30) NOT NULL DEFAULT 'approved',
        uploaded_by NVARCHAR(120) NULL,
        uploaded_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE UNIQUE INDEX IX_acc_co_assets_hash
        ON dbo.acc_co_assets(content_hash);
END

IF OBJECT_ID('dbo.acc_co_asset_links', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_co_asset_links (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        asset_id UNIQUEIDENTIFIER NOT NULL,
        sku NVARCHAR(120) NOT NULL,
        asin NVARCHAR(40) NULL,
        marketplace_id NVARCHAR(160) NULL,
        role NVARCHAR(40) NOT NULL,
        status NVARCHAR(30) NOT NULL DEFAULT 'approved',
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_co_asset_links_lookup
        ON dbo.acc_co_asset_links(sku, marketplace_id, role, created_at DESC);
END

IF OBJECT_ID('dbo.acc_co_publish_jobs', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_co_publish_jobs (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        job_type NVARCHAR(40) NOT NULL,
        marketplaces_json NVARCHAR(MAX) NULL,
        selection_mode NVARCHAR(20) NOT NULL,
        status NVARCHAR(20) NOT NULL DEFAULT 'pending',
        progress_pct DECIMAL(6,2) NOT NULL DEFAULT 0,
        log_json NVARCHAR(MAX) NULL,
        artifact_url NVARCHAR(500) NULL,
        created_by NVARCHAR(120) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        finished_at DATETIME2 NULL
    );
    CREATE INDEX IX_acc_co_publish_jobs_main
        ON dbo.acc_co_publish_jobs(status, created_at DESC);
END

IF COL_LENGTH('dbo.acc_co_publish_jobs', 'idempotency_key') IS NULL
BEGIN
    ALTER TABLE dbo.acc_co_publish_jobs ADD idempotency_key NVARCHAR(160) NULL;
END

IF COL_LENGTH('dbo.acc_co_publish_jobs', 'retry_count') IS NULL
BEGIN
    ALTER TABLE dbo.acc_co_publish_jobs ADD retry_count INT NOT NULL DEFAULT 0;
END

IF COL_LENGTH('dbo.acc_co_publish_jobs', 'max_retries') IS NULL
BEGIN
    ALTER TABLE dbo.acc_co_publish_jobs ADD max_retries INT NOT NULL DEFAULT 3;
END

IF COL_LENGTH('dbo.acc_co_publish_jobs', 'next_retry_at') IS NULL
BEGIN
    ALTER TABLE dbo.acc_co_publish_jobs ADD next_retry_at DATETIME2 NULL;
END

IF COL_LENGTH('dbo.acc_co_publish_jobs', 'last_error') IS NULL
BEGIN
    ALTER TABLE dbo.acc_co_publish_jobs ADD last_error NVARCHAR(MAX) NULL;
END

IF COL_LENGTH('dbo.acc_co_publish_jobs', 'heartbeat_at') IS NULL
BEGIN
    ALTER TABLE dbo.acc_co_publish_jobs ADD heartbeat_at DATETIME2 NULL;
END

IF COL_LENGTH('dbo.acc_co_publish_jobs', 'manual_retry_owner') IS NULL
BEGIN
    ALTER TABLE dbo.acc_co_publish_jobs ADD manual_retry_owner NVARCHAR(120) NULL;
END

IF COL_LENGTH('dbo.acc_co_publish_jobs', 'manual_retry_marketplace') IS NULL
BEGIN
    ALTER TABLE dbo.acc_co_publish_jobs ADD manual_retry_marketplace NVARCHAR(160) NULL;
END

IF COL_LENGTH('dbo.acc_co_publish_jobs', 'retry_source_job_id') IS NULL
BEGIN
    ALTER TABLE dbo.acc_co_publish_jobs ADD retry_source_job_id UNIQUEIDENTIFIER NULL;
END

IF COL_LENGTH('dbo.acc_co_publish_jobs', 'retry_source_sku') IS NULL
BEGIN
    ALTER TABLE dbo.acc_co_publish_jobs ADD retry_source_sku NVARCHAR(120) NULL;
END

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_acc_co_publish_jobs_idempotency'
      AND object_id = OBJECT_ID('dbo.acc_co_publish_jobs')
)
BEGIN
    CREATE INDEX IX_acc_co_publish_jobs_idempotency
        ON dbo.acc_co_publish_jobs(idempotency_key, created_at DESC);
END

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_acc_co_publish_jobs_retry'
      AND object_id = OBJECT_ID('dbo.acc_co_publish_jobs')
)
BEGIN
    CREATE INDEX IX_acc_co_publish_jobs_retry
        ON dbo.acc_co_publish_jobs(status, next_retry_at, created_at DESC);
END

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_acc_co_publish_jobs_manual_retry'
      AND object_id = OBJECT_ID('dbo.acc_co_publish_jobs')
)
BEGIN
    CREATE INDEX IX_acc_co_publish_jobs_manual_retry
        ON dbo.acc_co_publish_jobs(manual_retry_owner, manual_retry_marketplace, created_at DESC);
END

IF OBJECT_ID('dbo.acc_co_retry_policy', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_co_retry_policy (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        owner NVARCHAR(120) NULL,
        marketplace_id NVARCHAR(160) NULL,
        max_manual_retries_24h INT NOT NULL DEFAULT 5,
        is_active BIT NOT NULL DEFAULT 1,
        created_by NVARCHAR(120) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_co_retry_policy_lookup
        ON dbo.acc_co_retry_policy(is_active, owner, marketplace_id, updated_at DESC);
END

IF OBJECT_ID('dbo.acc_co_ai_cache', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_co_ai_cache (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        context_hash NVARCHAR(128) NOT NULL,
        mode NVARCHAR(40) NOT NULL,
        marketplace_id NVARCHAR(160) NULL,
        input_json NVARCHAR(MAX) NULL,
        output_json NVARCHAR(MAX) NULL,
        model NVARCHAR(80) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        expires_at DATETIME2 NULL
    );
    CREATE UNIQUE INDEX IX_acc_co_ai_cache_hash
        ON dbo.acc_co_ai_cache(context_hash, mode, marketplace_id);
END

IF OBJECT_ID('dbo.acc_co_product_type_map', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_co_product_type_map (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        marketplace_id NVARCHAR(160) NULL,
        brand NVARCHAR(120) NULL,
        category NVARCHAR(200) NULL,
        subcategory NVARCHAR(200) NULL,
        product_type NVARCHAR(80) NOT NULL,
        required_attrs_json NVARCHAR(MAX) NULL,
        priority INT NOT NULL DEFAULT 100,
        is_active BIT NOT NULL DEFAULT 1,
        created_by NVARCHAR(120) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_co_product_type_map_match
        ON dbo.acc_co_product_type_map(is_active, marketplace_id, brand, category, subcategory, priority);
END

IF OBJECT_ID('dbo.acc_co_product_type_defs', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_co_product_type_defs (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        marketplace_id NVARCHAR(160) NOT NULL,
        marketplace_code NVARCHAR(20) NOT NULL,
        product_type NVARCHAR(80) NOT NULL,
        requirements_json NVARCHAR(MAX) NULL,
        required_attrs_json NVARCHAR(MAX) NULL,
        source NVARCHAR(80) NOT NULL DEFAULT 'sp_api_definitions',
        refreshed_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE UNIQUE INDEX IX_acc_co_product_type_defs_unique
        ON dbo.acc_co_product_type_defs(marketplace_id, product_type);
    CREATE INDEX IX_acc_co_product_type_defs_lookup
        ON dbo.acc_co_product_type_defs(refreshed_at DESC, marketplace_id, product_type);
END

IF OBJECT_ID('dbo.acc_co_impact_snapshots', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_co_impact_snapshots (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        sku NVARCHAR(120) NOT NULL,
        marketplace_id NVARCHAR(160) NOT NULL,
        version_id UNIQUEIDENTIFIER NULL,
        range_days INT NOT NULL,
        metrics_json NVARCHAR(MAX) NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_co_impact_snapshots_lookup
        ON dbo.acc_co_impact_snapshots(sku, marketplace_id, range_days, created_at DESC);
END

IF OBJECT_ID('dbo.acc_co_attribute_map', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_co_attribute_map (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        marketplace_id NVARCHAR(160) NULL,
        product_type NVARCHAR(80) NULL,
        source_field NVARCHAR(200) NOT NULL,
        target_attribute NVARCHAR(200) NOT NULL,
        transform NVARCHAR(40) NOT NULL DEFAULT 'identity',
        priority INT NOT NULL DEFAULT 100,
        is_active BIT NOT NULL DEFAULT 1,
        created_by NVARCHAR(120) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_co_attribute_map_lookup
        ON dbo.acc_co_attribute_map(is_active, marketplace_id, product_type, priority);
END
            """
        )
        conn.commit()
    finally:
        conn.close()


def test_connection() -> bool:
    try:
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            return True
        finally:
            conn.close()
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Profit — read from acc_order + acc_order_line
# ---------------------------------------------------------------------------


def get_profit_orders(
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
    sku: str | None = None,
    fulfillment_channel: str | None = None,
    min_cm_pct: float | None = None,
    max_cm_pct: float | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """Return paginated profit-per-order from acc_order + acc_order_line."""
    cache_key = f"profit_orders:{date_from}:{date_to}:{marketplace_id}:{sku}:{fulfillment_channel}:{min_cm_pct}:{max_cm_pct}:{page}:{page_size}"
    cached = _rcache_get(cache_key)
    if cached is not None:
        return cached

    conn = _connect()
    try:
        cur = conn.cursor()
        logistics_join_sql = profit_logistics_join_sql(order_alias="o", fact_alias="olf")
        logistics_value_sql = profit_logistics_value_sql(order_alias="o", fact_alias="olf")
        cm_value_sql = (
            "("
            "ISNULL(o.revenue_pln, 0) - ISNULL(o.cogs_pln, 0) "
            "- ISNULL(o.amazon_fees_pln, 0) - ISNULL(o.ads_cost_pln, 0) "
            f"- {logistics_value_sql}"
            ")"
        )
        cm_pct_sql = (
            "CASE "
            "WHEN ISNULL(o.revenue_pln, 0) > 0 THEN "
            f"(({cm_value_sql}) / NULLIF(o.revenue_pln, 0)) * 100.0 "
            "ELSE 0 END"
        )

        # --- build WHERE clause ---
        where_parts = [
            "o.purchase_date >= CAST(? AS DATE)",
            "o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))",
            # Exclude noise: Canceled (no revenue), Pending (not paid), Non-Amazon (liquidation)
            "ISNULL(o.status, '') NOT IN ('Cancelled', 'Canceled', 'Pending')",
            "ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'",
            # Exclude Amazon Renewed (used) products
            "o.id NOT IN (SELECT ol_r.order_id FROM dbo.acc_order_line ol_r WHERE ol_r.sku LIKE 'amzn.gr.%%')",
        ]
        params: list[Any] = [date_from.isoformat(), date_to.isoformat()]

        if marketplace_id:
            where_parts.append("o.marketplace_id = ?")
            params.append(marketplace_id)
        if sku:
            where_parts.append(
                "o.id IN (SELECT ol2.order_id FROM dbo.acc_order_line ol2 WHERE ol2.sku = ?)"
            )
            params.append(sku)
        if fulfillment_channel:
            where_parts.append("o.fulfillment_channel = ?")
            params.append(fulfillment_channel)
        if min_cm_pct is not None:
            where_parts.append(f"({cm_pct_sql}) >= ?")
            params.append(min_cm_pct)
        if max_cm_pct is not None:
            where_parts.append(f"({cm_pct_sql}) <= ?")
            params.append(max_cm_pct)

        where_sql = " AND ".join(where_parts)

        # --- count ---
        cur.execute(
            f"SELECT COUNT(*) FROM dbo.acc_order o WITH (NOLOCK) {logistics_join_sql} WHERE {where_sql}",
            params,
        )
        total = _to_int(cur.fetchone()[0], 0)

        # --- paginated orders ---
        offset = (max(1, page) - 1) * max(1, page_size)
        order_sql = f"""
            SELECT
                CAST(o.id AS NVARCHAR(40))  AS id,
                o.amazon_order_id,
                o.marketplace_id,
                o.purchase_date,
                o.status,
                o.fulfillment_channel,
                o.order_total,
                o.currency,
                ISNULL(o.revenue_pln, 0)              AS revenue_pln,
                ISNULL(o.cogs_pln, 0)                  AS cogs_pln,
                ISNULL(o.amazon_fees_pln, 0)           AS amazon_fees_pln,
                ISNULL(o.ads_cost_pln, 0)              AS ads_cost_pln,
                {logistics_value_sql}                  AS logistics_pln,
                CAST({cm_value_sql} AS FLOAT)          AS contribution_margin_pln,
                CAST({cm_pct_sql} AS FLOAT)            AS cm_percent
            FROM dbo.acc_order o WITH (NOLOCK)
            {logistics_join_sql}
            WHERE {where_sql}
            ORDER BY o.purchase_date DESC, o.amazon_order_id DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """
        cur.execute(order_sql, (*params, offset, max(1, page_size)))
        order_rows = _fetchall_dict(cur)

        if not order_rows:
            pages = math.ceil(total / max(1, page_size)) if total else 0
            return {
                "total": total,
                "page": max(1, page),
                "page_size": max(1, page_size),
                "pages": pages,
                "items": [],
            }

        # --- fetch lines for returned orders ---
        order_ids = [r["id"] for r in order_rows]
        placeholders = ",".join(["?"] * len(order_ids))
        line_sql = f"""
            SELECT
                CAST(ol.order_id AS NVARCHAR(40)) AS order_id,
                ol.sku,
                ol.asin,
                ol.title,
                p.title                         AS title_pl,
                ISNULL(ol.quantity_ordered, 0)   AS quantity,
                ol.item_price,
                ol.currency,
                ISNULL(ol.purchase_price_pln, 0) AS purchase_price_pln,
                ISNULL(ol.cogs_pln, 0)           AS cogs_pln,
                ISNULL(ol.fba_fee_pln, 0)        AS fba_fee_pln,
                ISNULL(ol.referral_fee_pln, 0)   AS referral_fee_pln
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            LEFT JOIN dbo.acc_product p WITH (NOLOCK) ON p.id = ol.product_id
            WHERE CAST(ol.order_id AS NVARCHAR(40)) IN ({placeholders})
            ORDER BY ol.sku
        """
        cur.execute(line_sql, order_ids)
        line_rows = _fetchall_dict(cur)

        # group lines by order_id
        lines_by_order: dict[str, list[dict[str, Any]]] = {}
        for lr in line_rows:
            oid = lr.pop("order_id", "")
            lines_by_order.setdefault(oid, []).append(lr)

        # --- build response items ---
        items: list[dict[str, Any]] = []
        for row in order_rows:
            oid = row["id"]
            purchase_dt = _to_datetime(row.get("purchase_date"))
            items.append(
                {
                    "id": oid,
                    "amazon_order_id": row.get("amazon_order_id") or oid,
                    "marketplace_id": row.get("marketplace_id") or "",
                    "marketplace_code": _marketplace_code(row.get("marketplace_id")),
                    "purchase_date": purchase_dt or datetime.now(timezone.utc),
                    "status": row.get("status") or "Unknown",
                    "fulfillment_channel": row.get("fulfillment_channel") or "Amazon",
                    "order_total": _to_float(row.get("order_total")),
                    "currency": row.get("currency") or "PLN",
                    "revenue_pln": _to_float(row.get("revenue_pln")),
                    "cogs_pln": _to_float(row.get("cogs_pln")),
                    "amazon_fees_pln": _to_float(row.get("amazon_fees_pln")),
                    "ads_cost_pln": _to_float(row.get("ads_cost_pln")),
                    "logistics_pln": _to_float(row.get("logistics_pln")),
                    "contribution_margin_pln": _to_float(row.get("contribution_margin_pln")),
                    "cm_percent": _to_float(row.get("cm_percent")),
                    "lines": lines_by_order.get(oid, []),
                }
            )

        pages = math.ceil(total / max(1, page_size)) if total else 0
        result = {
            "total": total,
            "page": max(1, page),
            "page_size": max(1, page_size),
            "pages": pages,
            "items": items,
        }
        _rcache_set(cache_key, result, ttl=120)
        return result
    finally:
        conn.close()


def get_profit_by_sku(
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    """Aggregate profit metrics grouped by SKU from acc_order + acc_order_line."""
    conn = _connect()
    try:
        cur = conn.cursor()
        logistics_join_sql = profit_logistics_join_sql(order_alias="o", fact_alias="olf")
        logistics_value_sql = profit_logistics_value_sql(order_alias="o", fact_alias="olf")

        where_parts = [
            "o.purchase_date >= CAST(? AS DATE)",
            "o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))",
            # Exclude Amazon Renewed (used) products
            "ol.sku NOT LIKE 'amzn.gr.%%'",
        ]
        params: list[Any] = [date_from.isoformat(), date_to.isoformat()]

        if marketplace_id:
            where_parts.append("o.marketplace_id = ?")
            params.append(marketplace_id)

        where_sql = " AND ".join(where_parts)

        sql = f"""
            SELECT
                ISNULL(ol.sku, '')          AS sku,
                MIN(ol.asin)                AS asin,
                MIN(ol.title)               AS title,
                SUM(ISNULL(ol.quantity_ordered, 0))  AS units,
                SUM(ROUND(
                    (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                    * ISNULL(fx.rate_to_pln,
                        {_fx_case('o.currency')}), 2))            AS revenue_pln,
                SUM(ISNULL(ol.cogs_pln, 0))          AS cogs_pln,
                SUM(
                    ISNULL(ol.fba_fee_pln, 0)
                    + ISNULL(ol.referral_fee_pln, 0)
                    + (
                        ISNULL(o.shipping_surcharge_pln, 0)
                        + ISNULL(o.promo_order_fee_pln, 0)
                        + ISNULL(o.refund_commission_pln, 0)
                    ) * CASE
                        WHEN ISNULL(olt.order_line_total, 0) > 0
                            THEN (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0)) / NULLIF(olt.order_line_total, 0)
                        WHEN ISNULL(olt.order_units_total, 0) > 0
                            THEN ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                        ELSE 0
                    END
                ) AS amazon_fees_pln,
                SUM(
                    {logistics_value_sql} * CASE
                        WHEN ISNULL(olt.order_line_total, 0) > 0
                            THEN (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0)) / NULLIF(olt.order_line_total, 0)
                        WHEN ISNULL(olt.order_units_total, 0) > 0
                            THEN ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                        ELSE 0
                    END
                ) AS logistics_pln,
                COUNT(DISTINCT o.id)                  AS orders
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            {logistics_join_sql}
            OUTER APPLY (
                SELECT
                    SUM(ISNULL(ol2.item_price, 0) - ISNULL(ol2.item_tax, 0) - ISNULL(ol2.promotion_discount, 0)) AS order_line_total,
                    SUM(ISNULL(ol2.quantity_ordered, 0)) AS order_units_total
                FROM dbo.acc_order_line ol2 WITH (NOLOCK)
                WHERE ol2.order_id = o.id
            ) olt
            OUTER APPLY (
                SELECT TOP 1 rate_to_pln
                FROM dbo.acc_exchange_rate er WITH (NOLOCK)
                WHERE er.currency = o.currency
                  AND er.rate_date <= CAST(o.purchase_date AS DATE)
                ORDER BY er.rate_date DESC
            ) fx
            WHERE {where_sql}
            GROUP BY ol.sku
            ORDER BY SUM(ROUND(
                    (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                    * ISNULL(fx.rate_to_pln,
                        {_fx_case('o.currency')}), 2)) DESC
        """
        cur.execute(sql, params)
        rows = _fetchall_dict(cur)

        items = []
        for row in rows:
            revenue = _to_float(row.get("revenue_pln"))
            cogs = _to_float(row.get("cogs_pln"))
            fees = _to_float(row.get("amazon_fees_pln"))
            logistics = _to_float(row.get("logistics_pln"))
            cm = revenue - cogs - fees - logistics
            items.append(
                {
                    "sku": str(row.get("sku") or ""),
                    "asin": row.get("asin"),
                    "title": row.get("title"),
                    "units": _to_int(row.get("units")),
                    "revenue_pln": revenue,
                    "cogs_pln": cogs,
                    "amazon_fees_pln": fees,
                    "logistics_pln": round(logistics, 2),
                    "contribution_margin_pln": round(cm, 2),
                    "cm_percent": round((cm * 100.0 / revenue), 2) if revenue else 0.0,
                    "orders": _to_int(row.get("orders")),
                }
            )

        return {
            "date_from": date_from,
            "date_to": date_to,
            "marketplace_id": marketplace_id,
            "total_skus": len(items),
            "items": items,
        }
    finally:
        conn.close()


def recalc_profit_orders(*, date_from: date | None = None, date_to: date | None = None) -> int:
    """Recalculate COGS + CM1 for orders in the given date range.

    Steps:
      1. Stamp cogs_pln on order lines (purchase_price * qty) where missing
      2. Aggregate line-level COGS to order level
      3. Calculate netto revenue_pln and vat_pln using exchange rates
         revenue = (order_total - SUM(item_tax)) × fx_rate
         vat_pln = SUM(item_tax) × fx_rate
      4. Calculate contribution_margin_pln and cm_percent

    Returns number of orders updated.
    """
    if date_to is None:
        date_to = date.today()
    if date_from is None:
        date_from = date_to - timedelta(days=30)

    conn = _connect()
    try:
        cur = conn.cursor()
        # Reduce lock contention with running backfill
        cur.execute("SET DEADLOCK_PRIORITY LOW")

        d_from = date_from.isoformat()
        d_to = date_to.isoformat()

        # Step 0: Stamp purchase_price_pln from acc_purchase_price for lines
        # that have a product mapping but no purchase price yet
        cur.execute("""
            UPDATE ol
            SET ol.purchase_price_pln = pp.netto_price_pln,
                ol.price_source = 'purchase_price_tbl'
            FROM acc_order_line ol
            INNER JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            INNER JOIN acc_product p WITH (NOLOCK) ON p.id = ol.product_id
            CROSS APPLY (
                SELECT TOP 1 netto_price_pln
                FROM acc_purchase_price pp2 WITH (NOLOCK)
                WHERE pp2.internal_sku = p.internal_sku
                  AND pp2.netto_price_pln > 0
                  AND pp2.netto_price_pln <= 2000
                ORDER BY pp2.valid_from DESC
            ) pp
            WHERE (ol.purchase_price_pln IS NULL OR ol.purchase_price_pln = 0)
              AND p.internal_sku IS NOT NULL
              AND o.purchase_date >= CAST(? AS DATE)
              AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
              AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
              AND o.amazon_order_id NOT LIKE 'S02-%'
              AND ol.quantity_ordered > 0
        """, d_from, d_to)
        conn.commit()

        # Step 1: Stamp cogs_pln on lines
        # cogs_pln on line = netto purchase price × qty (VAT is recoverable)
        cur.execute("""
            UPDATE ol
            SET ol.cogs_pln = ROUND(
                ol.purchase_price_pln * ISNULL(ol.quantity_ordered, 1), 4)
            FROM acc_order_line ol
            INNER JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            WHERE ol.purchase_price_pln IS NOT NULL
              AND ol.purchase_price_pln > 0
              AND (ol.cogs_pln IS NULL OR ol.cogs_pln = 0)
              AND o.purchase_date >= CAST(? AS DATE)
              AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
              AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
              AND o.amazon_order_id NOT LIKE 'S02-%'
              AND ol.quantity_ordered > 0
        """, d_from, d_to)
        conn.commit()

        # Step 2: Aggregate line COGS to order level
        # ol.cogs_pln already = purchase_price * qty, just SUM (no extra * qty)
        cur.execute("""
            UPDATE o
            SET o.cogs_pln = ISNULL(agg.total_cogs, 0)
            FROM acc_order o
            CROSS APPLY (
                SELECT SUM(ISNULL(ol.cogs_pln, 0)) AS total_cogs
                FROM acc_order_line ol WITH (NOLOCK)
                WHERE ol.order_id = o.id
            ) agg
            WHERE o.purchase_date >= CAST(? AS DATE)
              AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
              AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
        """, d_from, d_to)
        conn.commit()

        # Step 3: Calculate netto revenue_pln and vat_pln using exchange rates
        # revenue = (order_total - SUM(item_tax)) × fx_rate  (netto)
        # vat_pln = SUM(item_tax) × fx_rate
        # item_price is BRUTTO (VAT incl), item_tax is VAT inside the price
        # B2B orders have item_tax=0 → item_price is already netto → formula works
        cur.execute(f"""
            UPDATE o
            SET o.vat_pln = ROUND(
                    ISNULL(tax_agg.total_tax, 0) * ISNULL(fx.rate_to_pln,
                        {_fx_case('o.currency')}
                    ), 2),
                o.revenue_pln = ROUND(
                    (ISNULL(o.order_total, 0) - ISNULL(tax_agg.total_tax, 0))
                    * ISNULL(fx.rate_to_pln,
                        {_fx_case('o.currency')}
                    ), 2)
            FROM acc_order o
            CROSS APPLY (
                SELECT ISNULL(SUM(ISNULL(ol.item_tax, 0)), 0) AS total_tax
                FROM acc_order_line ol WITH (NOLOCK)
                WHERE ol.order_id = o.id
            ) tax_agg
            OUTER APPLY (
                SELECT TOP 1 rate_to_pln
                FROM acc_exchange_rate er WITH (NOLOCK)
                WHERE er.currency = o.currency
                  AND er.rate_date < o.purchase_date
                ORDER BY er.rate_date DESC
            ) fx
            WHERE o.purchase_date >= CAST(? AS DATE)
              AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
              AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
        """, d_from, d_to)
        conn.commit()

        # Step 4: Calculate CM1
        logistics_join_sql = profit_logistics_join_sql(order_alias="o", fact_alias="olf")
        logistics_value_sql = profit_logistics_value_sql(order_alias="o", fact_alias="olf")
        cur.execute(f"""
            UPDATE o
            SET contribution_margin_pln = ROUND(
                    ISNULL(revenue_pln, 0) - ISNULL(cogs_pln, 0)
                    - ISNULL(amazon_fees_pln, 0) - ISNULL(ads_cost_pln, 0)
                    - {logistics_value_sql}, 2),
                cm_percent = CASE
                    WHEN ISNULL(revenue_pln, 0) > 0 THEN
                        ROUND(
                            (ISNULL(revenue_pln, 0) - ISNULL(cogs_pln, 0)
                             - ISNULL(amazon_fees_pln, 0) - ISNULL(ads_cost_pln, 0)
                             - {logistics_value_sql})
                            / NULLIF(revenue_pln, 0) * 100, 4)
                    ELSE 0
                END
            FROM acc_order o
            {logistics_join_sql}
            WHERE o.purchase_date >= CAST(? AS DATE)
              AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
              AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
        """, d_from, d_to)
        updated = _to_int(cur.rowcount, 0)
        conn.commit()

        log.info("recalc_profit.done", orders=updated,
                 date_from=d_from, date_to=d_to)
        return updated
    finally:
        conn.close()


def sync_profit_snapshot(*, date_from: date, date_to: date) -> int:
    """Sync profit snapshot from acc_order + acc_order_line for the given date range.

    Populates acc_al_profit_snapshot with denormalised order-line data
    so planning and alerts can reference a stable snapshot.
    """
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        logistics_join_sql = profit_logistics_join_sql(order_alias="o", fact_alias="olf")
        logistics_value_sql = profit_logistics_value_sql(order_alias="o", fact_alias="olf")

        # Clear existing snapshot rows for this range
        cur.execute(
            """
            DELETE FROM dbo.acc_al_profit_snapshot
            WHERE sales_date >= ? AND sales_date <= ? AND source_table = 'acc_order'
            """,
            (date_from.isoformat(), date_to.isoformat()),
        )
        conn.commit()

        # Insert from acc_order + acc_order_line
        insert_sql = f"""
            INSERT INTO dbo.acc_al_profit_snapshot
            (
                sales_date,
                order_number,
                sku,
                title,
                quantity,
                revenue_net,
                revenue_gross,
                cogs,
                transport,
                channel,
                source_table
            )
            SELECT
                CAST(o.purchase_date AS date)        AS sales_date,
                o.amazon_order_id                    AS order_number,
                ol.sku                               AS sku,
                ol.title                             AS title,
                ISNULL(ol.quantity_ordered, 0)        AS quantity,
                ROUND(
                    (
                        (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                        * ISNULL(fx.rate_to_pln, {_fx_case('o.currency')})
                    ) + (
                        ISNULL(fin.shipping_charge_net_pln, 0) * CASE
                            WHEN ISNULL(olt.order_net, 0) > 0 THEN
                                (
                                    (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                                    / NULLIF(olt.order_net, 0)
                                )
                            WHEN ISNULL(olt.order_units_total, 0) > 0 THEN
                                ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                            ELSE 0
                        END
                    ),
                    2
                )                                    AS revenue_net,
                ROUND(
                    (
                        (ISNULL(ol.item_price, 0) - ISNULL(ol.promotion_discount, 0))
                        * ISNULL(fx.rate_to_pln, {_fx_case('o.currency')})
                    ) + (
                        ISNULL(fin.shipping_charge_gross_pln, 0) * CASE
                            WHEN ISNULL(olt.order_net, 0) > 0 THEN
                                (
                                    (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                                    / NULLIF(olt.order_net, 0)
                                )
                            WHEN ISNULL(olt.order_units_total, 0) > 0 THEN
                                ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                            ELSE 0
                        END
                    ),
                    2
                )                                    AS revenue_gross,
                ISNULL(ol.cogs_pln, ISNULL(ol.purchase_price_pln, 0) * ISNULL(ol.quantity_ordered, 0))  AS cogs,
                ROUND(
                    {logistics_value_sql} * CASE
                        WHEN ISNULL(olt.order_net, 0) > 0 THEN
                            (
                                (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
                                / NULLIF(olt.order_net, 0)
                            )
                        WHEN ISNULL(olt.order_units_total, 0) > 0 THEN
                            ISNULL(ol.quantity_ordered, 0) * 1.0 / NULLIF(olt.order_units_total, 0)
                        ELSE 0
                    END,
                    2
                )                                    AS transport,
                o.marketplace_id                     AS channel,
                'acc_order'                          AS source_table
            FROM dbo.acc_order o
            JOIN dbo.acc_order_line ol ON ol.order_id = o.id
            {logistics_join_sql}
            OUTER APPLY (
                SELECT TOP 1 rate_to_pln
                FROM dbo.acc_exchange_rate er WITH (NOLOCK)
                WHERE er.currency = o.currency
                  AND er.rate_date <= CAST(o.purchase_date AS DATE)
                ORDER BY er.rate_date DESC
            ) fx
            OUTER APPLY (
                SELECT
                    ISNULL(SUM(
                        ISNULL(ol2.item_price, 0)
                        - ISNULL(ol2.item_tax, 0)
                        - ISNULL(ol2.promotion_discount, 0)
                    ), 0) AS order_net,
                    ISNULL(SUM(ISNULL(ol2.quantity_ordered, 0)), 0) AS order_units_total
                FROM dbo.acc_order_line ol2 WITH (NOLOCK)
                WHERE ol2.order_id = o.id
            ) olt
            OUTER APPLY (
                SELECT
                    SUM(
                        CASE
                            WHEN ft.charge_type IN ('ShippingCharge', 'ShippingTax') THEN
                                ISNULL(
                                    ft.amount_pln,
                                    ft.amount * {_fx_case("ISNULL(ft.currency, 'EUR')")}
                                )
                            ELSE 0
                        END
                    ) AS shipping_charge_net_pln,
                    SUM(
                        CASE
                            WHEN ft.charge_type = 'ShippingCharge' AND ft.amount > 0 THEN
                                ISNULL(
                                    ft.amount_pln,
                                    ft.amount * {_fx_case("ISNULL(ft.currency, 'EUR')")}
                                )
                            ELSE 0
                        END
                    ) AS shipping_charge_gross_pln
                FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
                WHERE ft.amazon_order_id = o.amazon_order_id
                  AND (
                      ft.marketplace_id = o.marketplace_id
                      OR ft.marketplace_id IS NULL
                      OR o.marketplace_id IS NULL
                  )
            ) fin
            WHERE
                o.purchase_date >= CAST(? AS DATE)
                AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
                AND ol.sku NOT LIKE 'amzn.gr.%%'
        """
        cur.execute(insert_sql, (date_from.isoformat(), date_to.isoformat()))
        inserted = _to_int(cur.rowcount, 0)
        conn.commit()
        return inserted
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------


def compare_value(operator: str | None, current_value: float, threshold_value: float | None) -> bool:
    if threshold_value is None:
        return False
    op = (operator or "<=").strip().lower()
    if op in {"<", "lt"}:
        return current_value < threshold_value
    if op in {"<=", "lte"}:
        return current_value <= threshold_value
    if op in {">", "gt"}:
        return current_value > threshold_value
    if op in {">=", "gte"}:
        return current_value >= threshold_value
    if op in {"=", "==", "eq"}:
        return current_value == threshold_value
    if op in {"!=", "<>", "ne"}:
        return current_value != threshold_value
    return current_value <= threshold_value


def _ensure_system_alert_rule(
    cur: pyodbc.Cursor,
    *,
    rule_name: str,
    rule_type: str,
    severity: str,
    description: str,
) -> str:
    """Ensure system alert rule exists and return its UUID string."""
    cur.execute(
        """
        SELECT TOP 1 CAST(id AS NVARCHAR(40))
        FROM dbo.acc_al_alert_rules WITH (NOLOCK)
        WHERE name = ? AND rule_type = ?
        ORDER BY created_at DESC
        """,
        (rule_name, rule_type),
    )
    row = cur.fetchone()
    if row and row[0]:
        return str(row[0])

    rule_id = str(uuid.uuid4())
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
        (rule_id, rule_name, description, rule_type, severity),
    )
    return rule_id


def _evaluate_task_sla_alerts(cur: pyodbc.Cursor) -> int:
    """
    Evaluate Product/Content Task SLA alerts:
      - open > 48h (critical)
      - investigating > 72h (warning)
    """
    rules = [
        {
            "name": "Task SLA Open > 48h",
            "rule_type": "task_sla_open",
            "status": "open",
            "hours": 48,
            "severity": "critical",
            "description": "Product task is still open after 48 hours.",
        },
        {
            "name": "Task SLA Investigating > 72h",
            "rule_type": "task_sla_investigating",
            "status": "investigating",
            "hours": 72,
            "severity": "warning",
            "description": "Product task is still investigating after 72 hours.",
        },
    ]

    created = 0
    for cfg in rules:
        rule_id = _ensure_system_alert_rule(
            cur,
            rule_name=cfg["name"],
            rule_type=cfg["rule_type"],
            severity=cfg["severity"],
            description=cfg["description"],
        )

        cur.execute(
            f"""
            SELECT
                CAST(t.id AS NVARCHAR(40)) AS task_id,
                t.task_type,
                t.sku,
                t.marketplace_id,
                t.owner,
                'product' AS task_domain,
                DATEDIFF(hour, t.created_at, SYSUTCDATETIME()) AS age_h
            FROM dbo.acc_al_product_tasks t WITH (NOLOCK)
            WHERE t.status = ?
              AND DATEDIFF(hour, t.created_at, SYSUTCDATETIME()) > ?
            UNION ALL
            SELECT
                CAST(ct.id AS NVARCHAR(40)) AS task_id,
                ct.task_type,
                ct.sku,
                ct.marketplace_id,
                ct.owner,
                'content' AS task_domain,
                DATEDIFF(hour, ct.created_at, SYSUTCDATETIME()) AS age_h
            FROM dbo.acc_co_tasks ct WITH (NOLOCK)
            WHERE ct.status = ?
              AND DATEDIFF(hour, ct.created_at, SYSUTCDATETIME()) > ?
            """,
            (cfg["status"], cfg["hours"], cfg["status"], cfg["hours"]),
        )
        overdue_tasks = _fetchall_dict(cur)

        for task in overdue_tasks:
            dedupe_sku = f"{task.get('sku') or ''}|{task.get('task_id') or ''}"
            cur.execute(
                """
                SELECT COUNT(*)
                FROM dbo.acc_al_alerts
                WHERE rule_id = CAST(? AS UNIQUEIDENTIFIER)
                  AND is_resolved = 0
                  AND sku = ?
                  AND triggered_at >= DATEADD(hour, -24, SYSUTCDATETIME())
                """,
                (rule_id, dedupe_sku),
            )
            if _to_int(cur.fetchone()[0], 0) > 0:
                continue

            alert_id = str(uuid.uuid4())
            owner_text = task.get("owner") or "unassigned"
            title = f"Task SLA breach [{task.get('task_domain')}]: {task.get('task_type')} {task.get('sku')}"
            detail = (
                f"Task {task.get('task_id')} is '{cfg['status']}' for {task.get('age_h')}h "
                f"(threshold {cfg['hours']}h). Owner={owner_text}."
            )
            cur.execute(
                """
                INSERT INTO dbo.acc_al_alerts
                (
                    id, rule_id, marketplace_id, sku, title, detail, severity, current_value
                )
                VALUES
                (
                    CAST(? AS UNIQUEIDENTIFIER), CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    alert_id,
                    rule_id,
                    task.get("marketplace_id"),
                    dedupe_sku,
                    title,
                    detail,
                    cfg["severity"],
                    float(task.get("age_h") or 0),
                ),
            )
            created += 1

    return created


def list_alert_rules() -> list[dict[str, Any]]:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                name,
                description,
                rule_type,
                marketplace_id,
                sku,
                category,
                threshold_value,
                threshold_operator,
                severity,
                is_active,
                created_by,
                created_at
            FROM dbo.acc_al_alert_rules
            ORDER BY name
            """
        )
        rows = _fetchall_dict(cur)
        for row in rows:
            row["is_active"] = bool(row.get("is_active"))
        return rows
    finally:
        conn.close()


def create_alert_rule(payload: dict[str, Any], actor: str | None = None) -> dict[str, Any]:
    ensure_v2_schema()
    rule_id = str(uuid.uuid4())
    created_by = _actor(actor)
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO dbo.acc_al_alert_rules
            (
                id, name, description, rule_type, marketplace_id, sku, category,
                threshold_value, threshold_operator, severity, is_active, created_by
            )
            VALUES
            (
                CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?
            )
            """,
            (
                rule_id,
                payload.get("name"),
                payload.get("description"),
                payload.get("rule_type"),
                payload.get("marketplace_id"),
                payload.get("sku"),
                payload.get("category"),
                payload.get("threshold_value"),
                payload.get("threshold_operator"),
                payload.get("severity", "warning"),
                1 if payload.get("is_active", True) else 0,
                created_by,
            ),
        )
        conn.commit()
        cur.execute(
            """
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                name, description, rule_type, marketplace_id, sku, category,
                threshold_value, threshold_operator, severity, is_active, created_by, created_at
            FROM dbo.acc_al_alert_rules
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (rule_id,),
        )
        row = _fetchall_dict(cur)[0]
        row["is_active"] = bool(row.get("is_active"))
        return row
    finally:
        conn.close()


def delete_alert_rule(rule_id: str) -> bool:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM dbo.acc_al_alert_rules WHERE id = CAST(? AS UNIQUEIDENTIFIER)", (rule_id,))
        deleted = _to_int(cur.rowcount, 0) > 0
        conn.commit()
        return deleted
    finally:
        conn.close()


def _metric_for_rule(
    cur: pyodbc.Cursor,
    *,
    rule_type: str,
    marketplace_id: str | None,
    sku: str | None,
    days: int,
) -> float:
    """Compute a single metric from acc_order for alert evaluation."""
    since = (date.today() - timedelta(days=max(1, days))).isoformat()
    until = date.today().isoformat()

    where = ["o.purchase_date >= CAST(? AS DATE)", "o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))"]
    params: list[Any] = [since, until]

    if marketplace_id:
        where.append("o.marketplace_id = ?")
        params.append(marketplace_id)
    if sku:
        where.append(
            "o.id IN (SELECT ol2.order_id FROM dbo.acc_order_line ol2 WHERE ol2.sku = ?)"
        )
        params.append(sku)

    where_sql = " AND ".join(where)
    rt = (rule_type or "").lower()
    logistics_join_sql = profit_logistics_join_sql(order_alias="o", fact_alias="olf")
    logistics_value_sql = profit_logistics_value_sql(order_alias="o", fact_alias="olf")
    cm_value_sql = (
        "("
        "ISNULL(o.revenue_pln, 0) - ISNULL(o.cogs_pln, 0) "
        "- ISNULL(o.amazon_fees_pln, 0) - ISNULL(o.ads_cost_pln, 0) "
        f"- {logistics_value_sql}"
        ")"
    )

    if "order" in rt:
        measure = "COUNT(*)"
    elif "transport" in rt or "logistics" in rt:
        measure = (
            "CASE WHEN NULLIF(SUM(ISNULL(o.revenue_pln, 0)), 0) IS NULL THEN 0.0 "
            f"ELSE SUM({logistics_value_sql}) * 100.0 "
            "/ NULLIF(SUM(ISNULL(o.revenue_pln, 0)), 0) END"
        )
    elif "cm" in rt:
        measure = (
            "CASE WHEN NULLIF(SUM(ISNULL(o.revenue_pln, 0)), 0) IS NULL THEN 0.0 "
            f"ELSE SUM({cm_value_sql}) * 100.0 "
            "/ NULLIF(SUM(ISNULL(o.revenue_pln, 0)), 0) END"
        )
    else:
        measure = "SUM(ISNULL(o.revenue_pln, 0))"

    sql = f"SELECT {measure} AS metric FROM dbo.acc_order o WITH (NOLOCK) {logistics_join_sql} WHERE {where_sql}"
    cur.execute(sql, params)
    row = cur.fetchone()
    return _to_float(row[0] if row else 0.0)


def evaluate_alert_rules(days: int = 7) -> int:
    """Evaluate all active alert rules against acc_order data. Returns count of new alerts."""
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                name,
                rule_type,
                marketplace_id,
                sku,
                threshold_value,
                threshold_operator,
                severity
            FROM dbo.acc_al_alert_rules
            WHERE is_active = 1
            """
        )
        rules = _fetchall_dict(cur)
        created = 0

        for rule in rules:
            current = _metric_for_rule(
                cur,
                rule_type=str(rule.get("rule_type") or ""),
                marketplace_id=rule.get("marketplace_id"),
                sku=rule.get("sku"),
                days=days,
            )
            threshold = rule.get("threshold_value")
            op = rule.get("threshold_operator") or "<="
            threshold_num = _to_float(threshold) if threshold is not None else None
            if not compare_value(str(op), current, threshold_num):
                continue

            # Deduplicate — skip if unresolved alert from same rule in last 24h
            cur.execute(
                """
                SELECT COUNT(*)
                FROM dbo.acc_al_alerts
                WHERE rule_id = CAST(? AS UNIQUEIDENTIFIER)
                  AND is_resolved = 0
                  AND triggered_at >= DATEADD(hour, -24, SYSUTCDATETIME())
                """,
                (rule["id"],),
            )
            if _to_int(cur.fetchone()[0], 0) > 0:
                continue

            alert_id = str(uuid.uuid4())
            threshold_text = f"{threshold}" if threshold is not None else "n/a"
            title = f"Alert: {rule.get('name')}"
            detail = (
                f"Rule '{rule.get('name')}' triggered. "
                f"Current value={round(current, 4)}, operator={op}, threshold={threshold_text}."
            )
            cur.execute(
                """
                INSERT INTO dbo.acc_al_alerts
                (
                    id, rule_id, marketplace_id, sku, title, detail, severity, current_value
                )
                VALUES
                (
                    CAST(? AS UNIQUEIDENTIFIER), CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    alert_id,
                    rule["id"],
                    rule.get("marketplace_id"),
                    rule.get("sku"),
                    title,
                    detail,
                    rule.get("severity") or "warning",
                    current,
                ),
            )
            created += 1

        # System SLA alerts for Product Tasks (independent of manual rules)
        created += _evaluate_task_sla_alerts(cur)

        conn.commit()
        return created
    finally:
        conn.close()


def list_alerts(
    *,
    is_resolved: bool | None = False,
    severity: str | None = None,
    marketplace_id: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        where_parts = ["1=1"]
        params: list[Any] = []

        if is_resolved is not None:
            where_parts.append("a.is_resolved = ?")
            params.append(1 if is_resolved else 0)
        if severity:
            where_parts.append("a.severity = ?")
            params.append(severity)
        if marketplace_id:
            where_parts.append("a.marketplace_id = ?")
            params.append(marketplace_id)

        where_sql = " AND ".join(where_parts)
        base_sql = f"""
            SELECT
                CAST(a.id AS NVARCHAR(40)) AS id,
                CAST(a.rule_id AS NVARCHAR(40)) AS rule_id,
                r.rule_type AS rule_type,
                a.marketplace_id,
                a.sku,
                a.title,
                a.detail,
                a.detail_json,
                a.context_json,
                a.severity,
                a.current_value,
                a.is_read,
                a.is_resolved,
                a.triggered_at
            FROM dbo.acc_al_alerts a
            LEFT JOIN dbo.acc_al_alert_rules r ON r.id = a.rule_id
            WHERE {where_sql}
        """

        cur.execute(f"SELECT COUNT(*) FROM ({base_sql}) q", params)
        total = _to_int(cur.fetchone()[0], 0)

        cur.execute(
            "SELECT COUNT(*) FROM dbo.acc_al_alerts WHERE is_read = 0 AND is_resolved = 0"
        )
        unread = _to_int(cur.fetchone()[0], 0)
        cur.execute(
            "SELECT COUNT(*) FROM dbo.acc_al_alerts WHERE severity = 'critical' AND is_resolved = 0"
        )
        critical = _to_int(cur.fetchone()[0], 0)

        start_row = (max(1, page) - 1) * max(1, page_size) + 1
        end_row = start_row + max(1, page_size) - 1
        paged_sql = f"""
            SELECT *
            FROM (
                SELECT
                    ROW_NUMBER() OVER (ORDER BY triggered_at DESC) AS rn,
                    q.*
                FROM ({base_sql}) q
            ) x
            WHERE x.rn BETWEEN ? AND ?
            ORDER BY x.rn
        """
        cur.execute(paged_sql, (*params, start_row, end_row))
        rows = _fetchall_dict(cur)
        for row in rows:
            row["is_read"] = bool(row.get("is_read"))
            row["is_resolved"] = bool(row.get("is_resolved"))
            try:
                row["detail_json"] = json.loads(row.get("detail_json")) if row.get("detail_json") else {}
            except Exception:
                row["detail_json"] = {}
            try:
                row["context_json"] = json.loads(row.get("context_json")) if row.get("context_json") else {}
            except Exception:
                row["context_json"] = {}

        return {
            "total": total,
            "unread": unread,
            "critical_count": critical,
            "items": rows,
        }
    finally:
        conn.close()


def mark_alert_read(alert_id: str) -> bool:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE dbo.acc_al_alerts
            SET is_read = 1
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (alert_id,),
        )
        changed = _to_int(cur.rowcount, 0) > 0
        conn.commit()
        return changed
    finally:
        conn.close()


def resolve_alert(alert_id: str, resolved_by: str | None = None) -> bool:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE dbo.acc_al_alerts
            SET
                is_resolved = 1,
                resolved_at = SYSUTCDATETIME(),
                resolved_by = ?
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (_actor(resolved_by), alert_id),
        )
        changed = _to_int(cur.rowcount, 0) > 0
        conn.commit()
        return changed
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


def create_job(
    *,
    job_type: str,
    marketplace_id: str | None = None,
    trigger_source: str = "manual",
    triggered_by: str | None = None,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_v2_schema()
    if job_type not in ALLOWED_JOB_TYPES:
        raise RuntimeError(f"Unknown job_type '{job_type}'. Allowed: {sorted(ALLOWED_JOB_TYPES)}")

    job_id = str(uuid.uuid4())
    payload = params or {}
    retry_policy = _normalize_retry_policy(job_type, payload)
    max_retries = _normalize_max_retries(job_type, payload, retry_policy)
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO dbo.acc_al_jobs
            (
                id, celery_task_id, job_type, marketplace_id, trigger_source, triggered_by,
                status, progress_pct, progress_message, params_json, last_heartbeat_at,
                retry_count, max_retries, next_retry_at, last_error_code, last_error_kind, retry_policy
            )
            VALUES
            (
                CAST(? AS UNIQUEIDENTIFIER), NULL, ?, ?, ?, ?,
                'pending', 0, 'Queued', ?, SYSUTCDATETIME(),
                0, ?, NULL, NULL, NULL, ?
            )
            """,
            (
                job_id,
                job_type,
                marketplace_id,
                trigger_source,
                _actor(triggered_by),
                json.dumps(payload, ensure_ascii=True),
                max_retries,
                retry_policy,
            ),
        )
        conn.commit()
        return get_job(job_id) or {}
    finally:
        conn.close()


def acquire_scheduler_leader_lock(lock_timeout_ms: int = 1000) -> bool:
    """Acquire a DB-backed singleton lock for scheduler leader election."""
    global _SCHEDULER_LOCK_CONN
    ensure_v2_schema()
    if _SCHEDULER_LOCK_CONN is not None:
        return True
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            DECLARE @res INT;
            EXEC @res = sp_getapplock
                @Resource = 'acc_scheduler_leader',
                @LockMode = 'Exclusive',
                @LockOwner = 'Session',
                @LockTimeout = ?;
            SELECT @res;
            """,
            (max(0, int(lock_timeout_ms)),),
        )
        row = cur.fetchone()
        res = _to_int(row[0], default=-999) if row else -999
        if res >= 0:
            _SCHEDULER_LOCK_CONN = conn
            return True
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass
    return False


def release_scheduler_leader_lock() -> None:
    global _SCHEDULER_LOCK_CONN
    conn = _SCHEDULER_LOCK_CONN
    _SCHEDULER_LOCK_CONN = None
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            """
            DECLARE @res INT;
            EXEC @res = sp_releaseapplock
                @Resource = 'acc_scheduler_leader',
                @LockOwner = 'Session';
            SELECT @res;
            """
        )
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _acquire_job_single_flight_lock(job_type: str, lock_timeout_ms: int = 2000) -> Any:
    """Serialize enqueue for selected job types across processes via applock."""
    ensure_v2_schema()
    conn = _connect()
    resource = f"acc_job_single_flight:{job_type}"
    try:
        cur = conn.cursor()
        cur.execute(
            """
            DECLARE @res INT;
            EXEC @res = sp_getapplock
                @Resource = ?,
                @LockMode = 'Exclusive',
                @LockOwner = 'Session',
                @LockTimeout = ?;
            SELECT @res;
            """,
            (resource, max(0, int(lock_timeout_ms))),
        )
        row = cur.fetchone()
        res = _to_int(row[0], default=-999) if row else -999
        if res >= 0:
            return conn
        raise RuntimeError(f"{job_type} enqueue lock unavailable")
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        raise


def _release_job_single_flight_lock(lock_conn: Any | None, job_type: str) -> None:
    if lock_conn is None:
        return
    resource = f"acc_job_single_flight:{job_type}"
    try:
        cur = lock_conn.cursor()
        cur.execute(
            """
            DECLARE @res INT;
            EXEC @res = sp_releaseapplock
                @Resource = ?,
                @LockOwner = 'Session';
            SELECT @res;
            """,
            (resource,),
        )
    except Exception:
        pass
    finally:
        try:
            lock_conn.close()
        except Exception:
            pass


def _find_active_job_by_type(job_type: str) -> dict[str, Any] | None:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 1
                CAST(id AS NVARCHAR(40)) AS id,
                job_type,
                status,
                progress_pct,
                progress_message,
                created_at
            FROM dbo.acc_al_jobs WITH (NOLOCK)
            WHERE job_type = ?
              AND status IN ('pending', 'running', 'retrying')
            ORDER BY created_at DESC
            """,
            (job_type,),
        )
        row = cur.fetchone()
        if not row:
            return None
        cols = [c[0] for c in cur.description] if cur.description else []
        return {cols[i]: row[i] for i in range(len(cols))}
    finally:
        conn.close()


def enqueue_job(
    *,
    job_type: str,
    marketplace_id: str | None = None,
    trigger_source: str = "manual",
    triggered_by: str | None = None,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = params or {}
    lock_conn = None
    try:
        if job_type in _SINGLE_FLIGHT_JOB_TYPES:
            lock_conn = _acquire_job_single_flight_lock(job_type)
            active = _find_active_job_by_type(job_type)
            if active:
                active_id = str(active.get("id") or "")
                active_status = str(active.get("status") or "")
                log.info(
                    "jobs.enqueue.single_flight_coalesced",
                    job_type=job_type,
                    active_id=active_id,
                    active_status=active_status,
                )
                return get_job(active_id) or active
        job = create_job(
            job_type=job_type,
            marketplace_id=marketplace_id,
            trigger_source=trigger_source,
            triggered_by=triggered_by,
            params=payload,
        )
    finally:
        if lock_conn is not None:
            _release_job_single_flight_lock(lock_conn, job_type)
    _dispatch_job_runner(str(job["id"]), job_type, payload)
    return get_job(str(job["id"])) or job


def _spawn_job_runner(job_id: str, job_type: str, payload: dict[str, Any] | None = None) -> None:
    runner_payload = payload or {}

    def _runner() -> None:
        try:
            run_job_type(job_id, runner_payload)
        except BaseException as exc:
            log.error("jobs.enqueue.background_error", job_type=job_type, job_id=job_id, error=str(exc))
            try:
                set_job_failure(job_id, exc, job_type=job_type)
            except Exception:
                pass

    thread = threading.Thread(
        target=_runner,
        name=f"acc-job-{job_type}-{job_id[:8]}",
        daemon=True,
    )
    thread.start()


def _enqueue_celery_job(job_id: str, job_type: str) -> None:
    from app.jobs.acc_job import run_acc_job_task

    queue = resolve_job_queue(job_type)
    task_id = f"acc-job-{job_id}"
    run_acc_job_task.apply_async(
        kwargs={"job_id": job_id},
        queue=queue,
        routing_key=queue,
        task_id=task_id,
    )
    _update_job(job_id, celery_task_id=task_id, progress_message=f"Queued on {queue}")


def _dispatch_job_runner(job_id: str, job_type: str, payload: dict[str, Any] | None = None) -> None:
    runner_payload = payload or {}
    if _should_dispatch_via_worker(job_type):
        _enqueue_celery_job(job_id, job_type)
        return
    _spawn_job_runner(job_id, job_type, runner_payload)


def _update_job(
    job_id: str,
    *,
    celery_task_id: str | None | object = _UNSET,
    status: str | None = None,
    progress_pct: int | None = None,
    progress_message: str | None = None,
    records_processed: int | None = None,
    error_message: str | None = None,
    retry_count: int | None = None,
    max_retries: int | None = None,
    next_retry_at: datetime | None | object = _UNSET,
    last_error_code: str | None | object = _UNSET,
    last_error_kind: str | None | object = _UNSET,
    retry_policy: str | None = None,
    lease_owner: str | None | object = _UNSET,
    lease_expires_at: datetime | None | object = _UNSET,
    clear_error_fields: bool = False,
    started_at_now: bool = False,
    finished_at_now: bool = False,
) -> bool:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        set_parts: list[str] = []
        params: list[Any] = []
        if celery_task_id is not _UNSET:
            set_parts.append("celery_task_id = ?")
            params.append(celery_task_id)
        if status is not None:
            set_parts.append("status = ?")
            params.append(status)
        if progress_pct is not None:
            set_parts.append("progress_pct = ?")
            params.append(progress_pct)
        if progress_message is not None:
            set_parts.append("progress_message = ?")
            params.append(progress_message)
        if records_processed is not None:
            set_parts.append("records_processed = ?")
            params.append(records_processed)
        if error_message is not None:
            set_parts.append("error_message = ?")
            params.append(error_message)
        if retry_count is not None:
            set_parts.append("retry_count = ?")
            params.append(retry_count)
        if max_retries is not None:
            set_parts.append("max_retries = ?")
            params.append(max_retries)
        if next_retry_at is not _UNSET:
            set_parts.append("next_retry_at = ?")
            params.append(next_retry_at)
        if last_error_code is not _UNSET:
            set_parts.append("last_error_code = ?")
            params.append(last_error_code)
        if last_error_kind is not _UNSET:
            set_parts.append("last_error_kind = ?")
            params.append(last_error_kind)
        if retry_policy is not None:
            set_parts.append("retry_policy = ?")
            params.append(retry_policy)
        if lease_owner is not _UNSET:
            set_parts.append("lease_owner = ?")
            params.append(lease_owner)
        if lease_expires_at is not _UNSET:
            set_parts.append("lease_expires_at = ?")
            params.append(lease_expires_at)
        if clear_error_fields:
            set_parts.append("error_message = NULL")
            set_parts.append("last_error_code = NULL")
            set_parts.append("last_error_kind = NULL")
        set_parts.append("last_heartbeat_at = SYSUTCDATETIME()")
        if started_at_now:
            set_parts.append("started_at = ISNULL(started_at, SYSUTCDATETIME())")
        if finished_at_now:
            set_parts.append("finished_at = SYSUTCDATETIME()")
            set_parts.append(
                "duration_seconds = CASE WHEN started_at IS NULL THEN NULL ELSE DATEDIFF(second, started_at, SYSUTCDATETIME()) END"
            )
        if not set_parts:
            return False

        sql = "UPDATE dbo.acc_al_jobs SET " + ", ".join(set_parts) + " WHERE id = CAST(? AS UNIQUEIDENTIFIER)"
        params.append(job_id)
        cur.execute(sql, params)
        changed = _to_int(cur.rowcount, 0) > 0
        conn.commit()
        return changed
    finally:
        conn.close()


def set_job_running(job_id: str, message: str | None = None) -> bool:
    return _update_job(
        job_id,
        status="running",
        progress_pct=10,
        progress_message=message or "Running",
        next_retry_at=None,
        clear_error_fields=True,
        started_at_now=True,
    )


def set_job_progress(
    job_id: str,
    *,
    progress_pct: int | None = None,
    message: str | None = None,
    records_processed: int | None = None,
) -> bool:
    return _update_job(
        job_id,
        progress_pct=progress_pct,
        progress_message=message,
        records_processed=records_processed,
    )


def set_job_success(job_id: str, records_processed: int = 0, message: str | None = None) -> bool:
    return _update_job(
        job_id,
        status="completed",
        progress_pct=100,
        progress_message=message or "Completed",
        records_processed=records_processed,
        next_retry_at=None,
        lease_owner=None,
        lease_expires_at=None,
        clear_error_fields=True,
        finished_at_now=True,
    )


def _mark_job_failure_terminal(
    job_id: str,
    error_message: str,
    *,
    error_code: str | None = None,
    error_kind: str | None = None,
) -> bool:
    return _update_job(
        job_id,
        status="failure",
        progress_pct=100,
        progress_message="Failed",
        error_message=error_message[:4000],
        next_retry_at=None,
        lease_owner=None,
        lease_expires_at=None,
        last_error_code=error_code[:120] if error_code else None,
        last_error_kind=error_kind[:20] if error_kind else None,
        finished_at_now=True,
    )


def schedule_job_retry(
    job_id: str,
    *,
    error_message: str,
    retry_count: int,
    max_retries: int,
    retry_policy: str,
    error_code: str | None = None,
    error_kind: str = "transient",
) -> bool:
    backoff_minutes = _retry_backoff_minutes(retry_count, retry_policy)
    message = (
        f"Retry scheduled {retry_count}/{max_retries} in {backoff_minutes} min"
        if backoff_minutes > 0
        else f"Retry scheduled {retry_count}/{max_retries}"
    )
    next_retry_at = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=backoff_minutes)
    return _update_job(
        job_id,
        status="retrying",
        progress_message=message,
        error_message=error_message[:4000],
        retry_count=retry_count,
        max_retries=max_retries,
        next_retry_at=next_retry_at,
        last_error_code=error_code[:120] if error_code else None,
        last_error_kind=error_kind[:20] if error_kind else None,
        retry_policy=retry_policy,
        lease_owner=None,
        lease_expires_at=None,
    )


def handle_job_failure(
    job_id: str,
    error: BaseException | str,
    *,
    job_type: str | None = None,
    allow_retry: bool = True,
) -> dict[str, Any]:
    job = get_job(job_id, include_params=True) or {}
    resolved_job_type = str(job_type or job.get("job_type") or "")
    retry_policy = str(job.get("retry_policy") or _default_retry_policy(resolved_job_type))
    max_retries = int(job.get("max_retries") or _default_max_retries(resolved_job_type, retry_policy))
    retry_count = int(job.get("retry_count") or 0)
    error_message = str(error).strip() or "Unknown job failure"
    error_kind, error_code = _classify_job_error(error)
    should_retry = (
        allow_retry
        and retry_policy != "none"
        and error_kind == "transient"
        and retry_count < max_retries
    )
    if should_retry:
        next_retry_count = retry_count + 1
        schedule_job_retry(
            job_id,
            error_message=error_message,
            retry_count=next_retry_count,
            max_retries=max_retries,
            retry_policy=retry_policy,
            error_code=error_code,
            error_kind=error_kind,
        )
        return {
            "status": "retrying",
            "retry_count": next_retry_count,
            "max_retries": max_retries,
            "retry_policy": retry_policy,
            "error_kind": error_kind,
            "error_code": error_code,
        }
    _mark_job_failure_terminal(
        job_id,
        error_message,
        error_code=error_code,
        error_kind=error_kind,
    )
    return {
        "status": "failure",
        "retry_count": retry_count,
        "max_retries": max_retries,
        "retry_policy": retry_policy,
        "error_kind": error_kind,
        "error_code": error_code,
    }


def set_job_failure(job_id: str, error_message: BaseException | str, *, job_type: str | None = None, allow_retry: bool = True) -> bool:
    result = handle_job_failure(job_id, error_message, job_type=job_type, allow_retry=allow_retry)
    return result.get("status") in {"retrying", "failure"}


def get_job(job_id: str, *, include_params: bool = False) -> dict[str, Any] | None:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        extra_select = ", params_json" if include_params else ""
        cur.execute(
            f"""
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                celery_task_id,
                job_type,
                marketplace_id,
                trigger_source,
                status,
                progress_pct,
                progress_message,
                records_processed,
                error_message,
                retry_count,
                max_retries,
                next_retry_at,
                last_error_code,
                last_error_kind,
                retry_policy,
                lease_owner,
                lease_expires_at,
                started_at,
                finished_at,
                duration_seconds,
                created_at
                {extra_select}
            FROM dbo.acc_al_jobs
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (job_id,),
        )
        rows = _fetchall_dict(cur)
        if not rows:
            return None
        row = rows[0]
        if include_params:
            try:
                row["params"] = json.loads(row.get("params_json")) if row.get("params_json") else {}
            except Exception:
                row["params"] = {}
            row.pop("params_json", None)
        return row
    finally:
        conn.close()


def list_jobs(
    *,
    job_type: str | None = None,
    status: str | None = None,
    page: int = 1,
    page_size: int = 30,
) -> dict[str, Any]:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        where = ["1=1"]
        params: list[Any] = []
        if job_type:
            where.append("job_type = ?")
            params.append(job_type)
        if status:
            where.append("status = ?")
            params.append(status)
        where_sql = " AND ".join(where)

        base_sql = f"""
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                celery_task_id,
                job_type,
                marketplace_id,
                trigger_source,
                status,
                progress_pct,
                progress_message,
                records_processed,
                error_message,
                retry_count,
                max_retries,
                next_retry_at,
                last_error_code,
                last_error_kind,
                retry_policy,
                lease_owner,
                lease_expires_at,
                started_at,
                finished_at,
                duration_seconds,
                created_at
            FROM dbo.acc_al_jobs
            WHERE {where_sql}
        """

        cur.execute(f"SELECT COUNT(*) FROM ({base_sql}) q", params)
        total = _to_int(cur.fetchone()[0], 0)

        start_row = (max(1, page) - 1) * max(1, page_size) + 1
        end_row = start_row + max(1, page_size) - 1
        paged_sql = f"""
            SELECT *
            FROM (
                SELECT ROW_NUMBER() OVER (ORDER BY created_at DESC) AS rn, q.*
                FROM ({base_sql}) q
            ) x
            WHERE x.rn BETWEEN ? AND ?
            ORDER BY x.rn
        """
        cur.execute(paged_sql, (*params, start_row, end_row))
        rows = _fetchall_dict(cur)
        return {"total": total, "items": rows}
    finally:
        conn.close()


def dispatch_due_retry_jobs(limit: int = 5) -> dict[str, Any]:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            ;WITH due AS (
                SELECT TOP (?) *
                FROM dbo.acc_al_jobs WITH (UPDLOCK, READPAST, ROWLOCK)
                WHERE status = 'retrying'
                  AND next_retry_at IS NOT NULL
                  AND next_retry_at <= SYSUTCDATETIME()
                ORDER BY next_retry_at ASC, created_at ASC
            )
            UPDATE due
            SET
                status = 'pending',
                progress_message = 'Retry queued',
                last_heartbeat_at = SYSUTCDATETIME()
            OUTPUT
                CAST(inserted.id AS NVARCHAR(40)) AS id,
                inserted.job_type,
                inserted.params_json
            ;
            """,
            (max(1, int(limit or 1)),),
        )
        rows = _fetchall_dict(cur)
        conn.commit()
    finally:
        conn.close()

    dispatched: list[str] = []
    for row in rows:
        try:
            params = json.loads(row.get("params_json")) if row.get("params_json") else {}
        except Exception:
            params = {}
        _dispatch_job_runner(str(row.get("id") or ""), str(row.get("job_type") or ""), params)
        dispatched.append(str(row.get("id") or ""))
    return {"dispatched": len(dispatched), "job_ids": dispatched}


def claim_job_lease(job_id: str, worker_id: str, lease_seconds: int = 600) -> bool:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE dbo.acc_al_jobs
            SET
                status = 'running',
                progress_pct = CASE WHEN progress_pct < 10 THEN 10 ELSE progress_pct END,
                progress_message = CASE
                    WHEN progress_message IS NULL OR progress_message IN ('Queued', 'Retry queued')
                    THEN 'Running'
                    ELSE progress_message
                END,
                started_at = ISNULL(started_at, SYSUTCDATETIME()),
                lease_owner = ?,
                lease_expires_at = DATEADD(second, ?, SYSUTCDATETIME()),
                last_heartbeat_at = SYSUTCDATETIME()
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
              AND status IN ('pending', 'retrying', 'running')
              AND (lease_owner IS NULL OR lease_owner = ? OR lease_expires_at IS NULL OR lease_expires_at < SYSUTCDATETIME())
            """,
            (worker_id, max(60, int(lease_seconds)), job_id, worker_id),
        )
        ok = _to_int(cur.rowcount, 0) > 0
        conn.commit()
        return ok
    finally:
        conn.close()


def _ensure_db_heavy_slots(cur: pyodbc.Cursor, slots: int) -> None:
    cur.execute(
        "SELECT COUNT(*) FROM dbo.acc_al_job_semaphore WITH (UPDLOCK, HOLDLOCK) WHERE semaphore_key = ?",
        ("db_heavy",),
    )
    existing = _to_int(cur.fetchone()[0], 0)
    for slot_no in range(existing + 1, max(1, int(slots)) + 1):
        cur.execute(
            """
            INSERT INTO dbo.acc_al_job_semaphore(semaphore_key, slot_no, holder_job_id, lease_expires_at)
            VALUES ('db_heavy', ?, NULL, NULL)
            """,
            (slot_no,),
        )


def acquire_db_heavy_slot(job_id: str, lease_seconds: int = 14400) -> bool:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        slots = max(1, int(settings.WORKER_DB_HEAVY_MAX or 3))
        _ensure_db_heavy_slots(cur, slots)
        cur.execute(
            """
            UPDATE dbo.acc_al_job_semaphore
            SET holder_job_id = NULL,
                lease_expires_at = NULL,
                updated_at = SYSUTCDATETIME()
            WHERE semaphore_key = 'db_heavy'
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at < SYSUTCDATETIME()
            """
        )
        cur.execute(
            """
            ;WITH candidate AS (
                SELECT TOP 1 semaphore_key, slot_no, holder_job_id, lease_expires_at, updated_at
                FROM dbo.acc_al_job_semaphore WITH (UPDLOCK, READPAST, ROWLOCK)
                WHERE semaphore_key = 'db_heavy'
                  AND holder_job_id IS NULL
                ORDER BY slot_no ASC
            )
            UPDATE candidate
            SET holder_job_id = CAST(? AS UNIQUEIDENTIFIER),
                lease_expires_at = DATEADD(second, ?, SYSUTCDATETIME()),
                updated_at = SYSUTCDATETIME()
            """,
            (job_id, max(300, int(lease_seconds))),
        )
        ok = _to_int(cur.rowcount, 0) > 0
        conn.commit()
        return ok
    finally:
        conn.close()


def release_db_heavy_slot(job_id: str) -> None:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE dbo.acc_al_job_semaphore
            SET holder_job_id = NULL,
                lease_expires_at = NULL,
                updated_at = SYSUTCDATETIME()
            WHERE semaphore_key = 'db_heavy'
              AND holder_job_id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (job_id,),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Job dispatch — delegated to platform/job_dispatch.py  (Sprint 7 S7.1)
# Backward-compat re-exports so existing callers keep working.
# ---------------------------------------------------------------------------
from app.platform.job_dispatch import run_job_type  # noqa: F401
from app.platform.job_dispatch import _run_async_job  # noqa: F401,E501
from app.platform.job_dispatch import _cleanup_staged_job_file  # noqa: F401,E501


_REMOVED_RUN_JOB_TYPE = True  # marker: original code lived here L3105-4334


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


def _plan_lines_for_month(cur: pyodbc.Cursor, plan_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
            id,
            plan_id,
            marketplace_id,
            target_revenue_pln,
            target_orders,
            target_acos_pct,
            target_cm_pct,
            budget_ads_pln,
            actual_revenue_pln,
            actual_orders,
            actual_acos_pct,
            actual_cm_pct
        FROM dbo.acc_al_plan_lines
        WHERE plan_id = ?
        ORDER BY marketplace_id
        """,
        (plan_id,),
    )
    rows = _fetchall_dict(cur)
    out: list[dict[str, Any]] = []
    for row in rows:
        target_rev = _to_float(row.get("target_revenue_pln"))
        actual_rev = _to_float(row.get("actual_revenue_pln"), default=0.0) if row.get("actual_revenue_pln") is not None else None
        out.append(
            {
                "id": _to_int(row.get("id")),
                "plan_id": _to_int(row.get("plan_id")),
                "marketplace_id": row.get("marketplace_id"),
                "marketplace_code": _marketplace_code(row.get("marketplace_id")),
                "target_revenue_pln": target_rev,
                "target_orders": _to_int(row.get("target_orders")),
                "target_acos_pct": _to_float(row.get("target_acos_pct")),
                "target_cm_pct": _to_float(row.get("target_cm_pct")),
                "budget_ads_pln": _to_float(row.get("budget_ads_pln")),
                "actual_revenue_pln": actual_rev,
                "actual_orders": _to_int(row.get("actual_orders")) if row.get("actual_orders") is not None else None,
                "actual_acos_pct": _to_float(row.get("actual_acos_pct")) if row.get("actual_acos_pct") is not None else None,
                "actual_cm_pct": _to_float(row.get("actual_cm_pct")) if row.get("actual_cm_pct") is not None else None,
                "revenue_attainment_pct": (round((actual_rev * 100.0 / target_rev), 1) if actual_rev is not None and target_rev else None),
            }
        )
    return out


def create_plan_month(payload: dict[str, Any], actor: str | None = None) -> dict[str, Any]:
    ensure_v2_schema()
    year = int(payload["year"])
    month = int(payload["month"])
    lines = payload.get("lines") or []
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM dbo.acc_al_plans WHERE [year] = ? AND [month] = ?", (year, month))
        exists = cur.fetchone()
        if exists:
            raise RuntimeError(f"Plan for {year}/{month:02d} already exists")

        cur.execute(
            """
            INSERT INTO dbo.acc_al_plans([year], [month], status, created_by)
            VALUES(?, ?, 'draft', ?)
            """,
            (year, month, _actor(actor)),
        )
        cur.execute("SELECT CAST(SCOPE_IDENTITY() AS INT)")
        plan_id = _to_int(cur.fetchone()[0], 0)

        for line in lines:
            cur.execute(
                """
                INSERT INTO dbo.acc_al_plan_lines
                (
                    plan_id, marketplace_id, target_revenue_pln, target_orders,
                    target_acos_pct, target_cm_pct, budget_ads_pln
                )
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    plan_id,
                    line.get("marketplace_id"),
                    _to_float(line.get("target_revenue_pln")),
                    _to_int(line.get("target_orders")),
                    _to_float(line.get("target_acos_pct")),
                    _to_float(line.get("target_cm_pct")),
                    _to_float(line.get("budget_ads_pln")),
                ),
            )
        conn.commit()

        refresh_plan_actuals(plan_id=plan_id)
        plans = list_plan_months(year=year)
        for plan in plans:
            if plan["id"] == plan_id:
                return plan
        raise RuntimeError("Created plan was not found")
    finally:
        conn.close()


def update_plan_status(plan_id: int, status: str) -> dict[str, Any]:
    ensure_v2_schema()
    if status not in {"draft", "approved", "locked"}:
        raise RuntimeError("Status must be one of: draft, approved, locked")

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT [year], [month], status FROM dbo.acc_al_plans WHERE id = ?", (plan_id,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError("Plan not found")
        existing_status = str(row[2] or "")
        if existing_status == "locked" and status != "locked":
            raise RuntimeError("Locked plans cannot be modified")
        cur.execute("UPDATE dbo.acc_al_plans SET status = ? WHERE id = ?", (status, plan_id))
        conn.commit()
    finally:
        conn.close()

    year = _to_int(row[0], 0)
    plans = list_plan_months(year=year)
    for plan in plans:
        if plan["id"] == plan_id:
            return plan
    raise RuntimeError("Plan not found after status update")


def delete_plan_month(plan_id: int) -> bool:
    """Delete a plan month and its lines. Only draft plans can be deleted."""
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT status FROM dbo.acc_al_plans WHERE id = ?", (plan_id,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError("Plan not found")
        if str(row[0] or "") == "locked":
            raise RuntimeError("Locked plans cannot be deleted")
        cur.execute("DELETE FROM dbo.acc_al_plan_lines WHERE plan_id = ?", (plan_id,))
        cur.execute("DELETE FROM dbo.acc_al_plans WHERE id = ?", (plan_id,))
        conn.commit()
        return True
    finally:
        conn.close()


def refresh_plan_actuals(*, plan_id: int | None = None, year: int | None = None) -> int:
    """Refresh plan line actuals from acc_order data."""
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()

        filter_parts = ["1=1"]
        params: list[Any] = []
        if plan_id is not None:
            filter_parts.append("p.id = ?")
            params.append(plan_id)
        if year is not None:
            filter_parts.append("p.[year] = ?")
            params.append(year)

        sql = f"""
            SELECT
                l.id AS line_id,
                l.marketplace_id,
                p.[year],
                p.[month]
            FROM dbo.acc_al_plan_lines l
            JOIN dbo.acc_al_plans p ON p.id = l.plan_id
            WHERE {' AND '.join(filter_parts)}
        """
        cur.execute(sql, params)
        rows = _fetchall_dict(cur)
        updated = 0

        for row in rows:
            y = _to_int(row["year"])
            m = _to_int(row["month"])
            start = date(y, m, 1)
            if m == 12:
                end = date(y + 1, 1, 1) - timedelta(days=1)
            else:
                end = date(y, m + 1, 1) - timedelta(days=1)

            market = str(row.get("marketplace_id") or "")

            # Query acc_order for actuals
            logistics_join_sql = profit_logistics_join_sql(order_alias="o", fact_alias="olf")
            logistics_value_sql = profit_logistics_value_sql(order_alias="o", fact_alias="olf")
            cm_value_sql = (
                "("
                "ISNULL(o.revenue_pln, 0) - ISNULL(o.cogs_pln, 0) "
                "- ISNULL(o.amazon_fees_pln, 0) - ISNULL(o.ads_cost_pln, 0) "
                f"- {logistics_value_sql}"
                ")"
            )
            metric_sql = f"""
                SELECT
                    SUM(ISNULL(o.revenue_pln, 0))                    AS revenue,
                    COUNT(*)                                          AS orders_cnt,
                    CASE
                        WHEN NULLIF(SUM(ISNULL(o.revenue_pln, 0)), 0) IS NULL THEN 0.0
                        ELSE SUM({cm_value_sql}) * 100.0
                             / NULLIF(SUM(ISNULL(o.revenue_pln, 0)), 0)
                    END                                               AS cm_pct
                FROM dbo.acc_order o WITH (NOLOCK)
                {logistics_join_sql}
                WHERE
                    o.purchase_date >= CAST(? AS DATE)
                    AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
                    AND (o.marketplace_id = ? OR ? = '')
            """
            cur.execute(metric_sql, (start.isoformat(), end.isoformat(), market, market))
            met = cur.fetchone()
            revenue = _to_float(met[0] if met else 0.0)
            orders_cnt = _to_int(met[1] if met else 0)
            cm_pct = _to_float(met[2] if met else 0.0)

            cur.execute(
                """
                UPDATE dbo.acc_al_plan_lines
                SET
                    actual_revenue_pln = ?,
                    actual_orders = ?,
                    actual_acos_pct = NULL,
                    actual_cm_pct = ?
                WHERE id = ?
                """,
                (revenue, orders_cnt, cm_pct, _to_int(row["line_id"])),
            )
            updated += 1

        conn.commit()
        return updated
    finally:
        conn.close()


def list_plan_months(year: int | None = None) -> list[dict[str, Any]]:
    ensure_v2_schema()
    if year is not None:
        refresh_plan_actuals(year=year)
    else:
        refresh_plan_actuals()

    conn = _connect()
    try:
        cur = conn.cursor()
        if year is None:
            cur.execute(
                """
                SELECT id, [year], [month], status, created_by, created_at
                FROM dbo.acc_al_plans
                ORDER BY [year] DESC, [month] DESC
                """
            )
        else:
            cur.execute(
                """
                SELECT id, [year], [month], status, created_by, created_at
                FROM dbo.acc_al_plans
                WHERE [year] = ?
                ORDER BY [month] DESC
                """,
                (year,),
            )
        months = _fetchall_dict(cur)
        out: list[dict[str, Any]] = []
        for m in months:
            plan_id = _to_int(m["id"])
            lines = _plan_lines_for_month(cur, plan_id)
            total_target = sum(_to_float(line.get("target_revenue_pln")) for line in lines)
            total_budget = sum(_to_float(line.get("budget_ads_pln")) for line in lines)
            total_actual = sum(_to_float(line.get("actual_revenue_pln")) for line in lines if line.get("actual_revenue_pln") is not None)
            out.append(
                {
                    "id": plan_id,
                    "year": _to_int(m.get("year")),
                    "month": _to_int(m.get("month")),
                    "month_label": _month_label(_to_int(m.get("year")), _to_int(m.get("month"))),
                    "status": m.get("status"),
                    "total_target_revenue_pln": round(total_target, 2),
                    "total_target_budget_ads_pln": round(total_budget, 2),
                    "total_actual_revenue_pln": round(total_actual, 2) if total_actual else 0.0,
                    "revenue_attainment_pct": (round(total_actual * 100.0 / total_target, 1) if total_target else None),
                    "lines": lines,
                    "created_by": m.get("created_by"),
                    "created_at": _to_datetime(m.get("created_at")) or datetime.now(timezone.utc),
                }
            )
        return out
    finally:
        conn.close()


def get_plan_vs_actual(year: int) -> dict[str, Any]:
    plans = list_plan_months(year=year)
    rows: list[dict[str, Any]] = []
    ytd_target = 0.0
    ytd_actual = 0.0
    for m in sorted(plans, key=lambda x: x["month"]):
        lines = m.get("lines") or []
        target_revenue = sum(_to_float(line.get("target_revenue_pln")) for line in lines)
        actual_revenue = sum(_to_float(line.get("actual_revenue_pln")) for line in lines if line.get("actual_revenue_pln") is not None)
        t_cm = (
            sum(_to_float(line.get("target_cm_pct")) for line in lines) / len(lines)
            if lines
            else 0.0
        )
        a_cm = (
            sum(_to_float(line.get("actual_cm_pct")) for line in lines if line.get("actual_cm_pct") is not None) / len(lines)
            if lines
            else 0.0
        )
        t_acos = (
            sum(_to_float(line.get("target_acos_pct")) for line in lines) / len(lines)
            if lines
            else 0.0
        )
        a_acos = (
            sum(_to_float(line.get("actual_acos_pct")) for line in lines if line.get("actual_acos_pct") is not None) / len(lines)
            if lines
            else 0.0
        )
        ytd_target += target_revenue
        ytd_actual += actual_revenue
        rows.append(
            {
                "month_label": m["month_label"],
                "target_revenue_pln": round(target_revenue, 2),
                "actual_revenue_pln": round(actual_revenue, 2),
                "revenue_attainment_pct": round(actual_revenue * 100.0 / target_revenue, 1) if target_revenue else 0.0,
                "target_cm_pct": round(t_cm, 2),
                "actual_cm_pct": round(a_cm, 2),
                "target_acos_pct": round(t_acos, 2),
                "actual_acos_pct": round(a_acos, 2),
            }
        )
    return {
        "rows": rows,
        "ytd_target_revenue_pln": round(ytd_target, 2),
        "ytd_actual_revenue_pln": round(ytd_actual, 2),
        "ytd_attainment_pct": round(ytd_actual * 100.0 / ytd_target, 1) if ytd_target else 0.0,
    }
