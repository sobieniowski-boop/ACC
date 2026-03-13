from __future__ import annotations

import json
import math
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from statistics import median
from typing import Any

import pyodbc
import structlog

from app.connectors.amazon_sp_api.reports import ReportType, parse_tsv_report
from app.core.config import MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc, connect_netfox
from app.services.amazon_listing_registry import lookup_listing_registry_context

log = structlog.get_logger(__name__)

# ──────────── In-memory cache ────────────

_FBA_CACHE: dict[str, tuple[float, Any]] = {}

def _fba_cache_get(key: str) -> Any | None:
    entry = _FBA_CACHE.get(key)
    if entry is None:
        return None
    exp, val = entry
    if time.monotonic() > exp:
        _FBA_CACHE.pop(key, None)
        return None
    return val

def _fba_cache_set(key: str, value: Any, ttl: int = 600) -> None:
    _FBA_CACHE[key] = (time.monotonic() + ttl, value)

# ──────────── Constants ────────────

PLANNING_REPORT_PROBLEM_MARKETPLACES = {
    "A28R8C7NBKEWEA",   # IE
    "A2NODRKZP88ZB9",   # SE
    "A1805IZSGTT6HS",   # NL
    "A1C3SOZRARQ6R3",   # PL
    "AMEN7PMS3EDWL",    # BE
}
PLANNING_REPORT_FATAL_THRESHOLD = 3
PLANNING_REPORT_FATAL_WINDOW_HOURS = 12

# ──────────── Database helpers ────────────

def _connect() -> pyodbc.Connection:
    return connect_acc(autocommit=False, timeout=120)


def _fetchall_dict(cur: pyodbc.Cursor) -> list[dict[str, Any]]:
    cols = [c[0] for c in cur.description] if cur.description else []
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]


def _fetchone_dict(cur: pyodbc.Cursor) -> dict[str, Any] | None:
    row = cur.fetchone()
    if not row or not cur.description:
        return None
    cols = [c[0] for c in cur.description]
    return {cols[i]: row[i] for i in range(len(cols))}

# ──────────── Conversion / parsing helpers ────────────

def _marketplace_code(marketplace_id: str | None) -> str:
    if not marketplace_id:
        return ""
    info = MARKETPLACE_REGISTRY.get(str(marketplace_id))
    return str(info.get("code")) if info else str(marketplace_id)


def _is_amzn_grade_sku(sku: Any) -> bool:
    return str(sku or "").strip().lower().startswith("amzn.gr.")


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


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _parse_quarter(quarter: str) -> tuple[date, date]:
    try:
        year_raw, quarter_raw = quarter.upper().split("-Q", 1)
        year = int(year_raw)
        quarter_no = int(quarter_raw)
        if quarter_no < 1 or quarter_no > 4:
            raise ValueError
    except Exception as exc:
        raise ValueError(f"invalid quarter format: {quarter}") from exc
    start_month = (quarter_no - 1) * 3 + 1
    start_date = date(year, start_month, 1)
    if quarter_no == 4:
        end_date = date(year, 12, 31)
    else:
        end_date = date(year, start_month + 3, 1) - timedelta(days=1)
    return start_date, end_date


def _quarter_day_count(start_date: date, end_date: date) -> int:
    return (end_date - start_date).days + 1


def _factor_lower_is_better(actual: float | None, alarm: float | None, target: float | None, good: float | None) -> float:
    if actual is None or alarm is None or target is None or good is None:
        return 0.0
    if actual <= target:
        if target <= good:
            return 1.0
        span = max(target - good, 0.0001)
        return _clamp(1.0 + ((target - actual) / span) * 0.20, 0.0, 1.20)
    span = max(alarm - target, 0.0001)
    return _clamp(1.0 - ((actual - target) / span), 0.0, 1.0)


def _factor_higher_is_better(actual: float | None, alarm: float | None, target: float | None, good: float | None) -> float:
    if actual is None or alarm is None or target is None or good is None:
        return 0.0
    if actual >= target:
        span = max(good - target, 0.0001)
        return _clamp(1.0 + ((actual - target) / span) * 0.20, 0.0, 1.20)
    span = max(target - alarm, 0.0001)
    return _clamp((actual - alarm) / span, 0.0, 1.0)


def _business_day_diff(start_dt: date | None, end_dt: date | None) -> int | None:
    if not start_dt or not end_dt:
        return None
    if end_dt < start_dt:
        return -_business_day_diff(end_dt, start_dt)  # type: ignore[arg-type]
    current = start_dt
    days = 0
    while current <= end_dt:
        if current.weekday() < 5:
            days += 1
        current += timedelta(days=1)
    return days - 1


def _parse_json(value: Any, default: dict[str, Any] | list[Any] | None = None) -> Any:
    if value in (None, ""):
        return {} if default is None else default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return {} if default is None else default


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_key(value: Any) -> str:
    text = _normalize_text(value).lower()
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_")


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _first_value(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        if key in row and str(row[key]).strip():
            return str(row[key]).strip()
    return ""


def _first_int(row: dict[str, Any], *keys: str) -> int:
    for key in keys:
        if key in row and str(row[key]).strip():
            return _to_int(row[key])
    return 0


def _latest_inventory_snapshot_date(cur: pyodbc.Cursor) -> date | None:
    cur.execute("SELECT MAX(snapshot_date) FROM dbo.acc_fba_inventory_snapshot WITH (NOLOCK)")
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def _latest_raw_inventory_snapshot_date(cur: pyodbc.Cursor) -> date | None:
    cur.execute("SELECT MAX(snapshot_date) FROM dbo.acc_inventory_snapshot WITH (NOLOCK)")
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def _recent_report_failures(
    marketplace_id: str,
    report_type: str,
    *,
    lookback_hours: int,
    request_status: str = "FATAL",
) -> dict[str, Any]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) AS failure_count, MAX(created_at) AS last_failure_at
            FROM dbo.acc_fba_report_diagnostic WITH (NOLOCK)
            WHERE sync_scope = 'inventory'
              AND marketplace_id = ?
              AND report_type = ?
              AND UPPER(ISNULL(request_status, '')) = ?
              AND created_at >= DATEADD(HOUR, ?, SYSUTCDATETIME())
            """,
            (marketplace_id, report_type, request_status.upper(), -abs(int(lookback_hours))),
        )
        row = cur.fetchone()
        return {
            "failure_count": _to_int(row[0]) if row else 0,
            "last_failure_at": row[1] if row else None,
        }
    finally:
        conn.close()


def _planning_report_cooldown_state(marketplace_id: str) -> dict[str, Any]:
    recent = _recent_report_failures(
        marketplace_id,
        ReportType.FBA_INVENTORY_PLANNING,
        lookback_hours=PLANNING_REPORT_FATAL_WINDOW_HOURS,
    )
    failure_count = _to_int(recent.get("failure_count"))
    last_failure_at = recent.get("last_failure_at")
    if marketplace_id in PLANNING_REPORT_PROBLEM_MARKETPLACES and last_failure_at:
        return {
            "active": True,
            "reason": "known_marketplace_recent_fatal",
            "failure_count": failure_count,
            "last_failure_at": last_failure_at,
        }
    if failure_count >= PLANNING_REPORT_FATAL_THRESHOLD:
        return {
            "active": True,
            "reason": "persistent_recent_fatal",
            "failure_count": failure_count,
            "last_failure_at": last_failure_at,
        }
    return {
        "active": False,
        "reason": None,
        "failure_count": failure_count,
        "last_failure_at": last_failure_at,
    }


# ──────────── Config helpers ────────────

def _get_defaults(cur: pyodbc.Cursor) -> dict[str, Any]:
    cur.execute("SELECT value_json FROM dbo.acc_fba_config WITH (NOLOCK) WHERE [key] = 'defaults'")
    row = cur.fetchone()
    if not row or not row[0]:
        return {"target_days": 45, "safety_stock_days": 14, "lead_time_days": 21, "inbound_stuck_days": 7}
    try:
        return json.loads(row[0])
    except Exception:
        return {"target_days": 45, "safety_stock_days": 14, "lead_time_days": 21, "inbound_stuck_days": 7}


def _get_json_config(cur: pyodbc.Cursor, key: str, fallback: dict[str, Any]) -> dict[str, Any]:
    cur.execute("SELECT value_json FROM dbo.acc_fba_config WITH (NOLOCK) WHERE [key] = ?", (key,))
    row = cur.fetchone()
    if not row or not row[0]:
        return fallback
    try:
        parsed = json.loads(row[0])
        return parsed if isinstance(parsed, dict) else fallback
    except Exception:
        return fallback


# ──────────── Report normalization ────────────

def _report_rows_to_normalized(content: str) -> list[dict[str, Any]]:
    rows = parse_tsv_report(content)
    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append({_normalize_key(k): v for k, v in row.items()})
    return normalized


def _inventory_api_rows_to_normalized(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in items:
        details = item.get("inventoryDetails") or {}
        reserved = details.get("reservedQuantity") or {}
        normalized.append(
            {
                "sku": item.get("sellerSku") or "",
                "asin": item.get("asin") or "",
                "available": details.get("fulfillableQuantity", item.get("totalQuantity", 0)),
                "inbound_working": details.get("inboundWorkingQuantity", 0),
                "inbound_shipped": details.get("inboundShippedQuantity", 0),
                "inbound_received": details.get("inboundReceivingQuantity", 0),
                "inbound_quantity": details.get("futureSupplyQuantity", 0),
                "total_reserved_quantity": reserved.get("totalReservedQuantity", 0),
                "unfulfillable_quantity": details.get("unfulfillableQuantity", 0),
                "researching_quantity": details.get("researchingQuantity", 0),
                "product_name": item.get("productName") or "",
                "source": "inventory_api_summaries",
            }
        )
    return normalized


# ──────────── Scorecard component builder ────────────

def _build_component(
    *,
    key: str,
    actual: float | None,
    data_ready: bool,
    note: str | None,
    config: dict[str, Any],
) -> dict[str, Any]:
    direction = str(config.get("direction") or "lower")
    alarm = _to_float(config.get("alarm")) if config.get("alarm") is not None else None
    target = _to_float(config.get("target")) if config.get("target") is not None else None
    good = _to_float(config.get("good")) if config.get("good") is not None else None
    if direction == "higher":
        factor = _factor_higher_is_better(actual, alarm, target, good) if data_ready else 0.0
    else:
        factor = _factor_lower_is_better(actual, alarm, target, good) if data_ready else 0.0
    weight = _to_float(config.get("weight"))
    return {
        "key": key,
        "label": str(config.get("label") or key),
        "unit": str(config.get("unit") or ""),
        "direction": direction,
        "weight": weight,
        "actual": round(actual, 4) if actual is not None else None,
        "alarm": round(alarm, 4) if alarm is not None else None,
        "target": round(target, 4) if target is not None else None,
        "good": round(good, 4) if good is not None else None,
        "factor": round(factor, 4),
        "score_contribution": round(weight * factor, 4),
        "data_ready": data_ready,
        "note": note,
    }


# ──────────── Product context lookup ────────────

def _truncate_text(value: str | None, max_len: int = 80) -> str:
    text = str(value or "").strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "\u2026"


def _sku_candidate_ean(sku: str | None) -> str | None:
    text = str(sku or "").strip()
    if text.upper().startswith("FBA_"):
        candidate = text[4:]
        if candidate.isdigit():
            return candidate
    return None


def _lookup_netfox_product_context(*, sku: str | None, ean: str | None = None) -> dict[str, Any]:
    if not sku and not ean:
        return {}
    conn = None
    try:
        conn = connect_netfox(autocommit=True, timeout=10)
        cur = conn.cursor()
        where_parts: list[str] = []
        params: list[Any] = []
        if sku:
            where_parts.append("k.Symbol = ?")
            params.append(sku)
        if ean:
            where_parts.append("k.EAN = ?")
            params.append(ean)
        cur.execute(
            f"""
            SELECT TOP 1
                k.Symbol AS sku,
                k.EAN AS ean,
                k.Nazwa AS product_name
            FROM dbo.Kartoteki k
            WHERE {' OR '.join(where_parts)}
            """,
            tuple(params),
        )
        row = cur.fetchone()
        if not row:
            return {}
        return {
            "netfox_sku": str(row[0]).strip() if row[0] else None,
            "ean": str(row[1]).strip() if row[1] else None,
            "title_preferred": str(row[2]).strip() if row[2] else None,
            "title_netfox": str(row[2]).strip() if row[2] else None,
        }
    except Exception:
        return {}
    finally:
        if conn is not None:
            conn.close()


def _lookup_product_context(cur: pyodbc.Cursor, *, sku: str | None, asin: str | None = None) -> dict[str, Any]:
    if not sku and not asin:
        return {}
    candidate_ean = _sku_candidate_ean(sku)
    registry = lookup_listing_registry_context(cur, sku=sku, asin=asin, ean=candidate_ean)
    cur.execute(
        """
        SELECT TOP 1
            p.ean,
            p.sku,
            p.asin,
            p.brand,
            p.category,
            p.title,
            p.internal_sku,
            p.k_number,
            ip.nazwa_pelna AS title_pl
        FROM dbo.acc_product p WITH (NOLOCK)
        OUTER APPLY (
            SELECT TOP 1 ip.nazwa_pelna
            FROM dbo.acc_import_products ip WITH (NOLOCK)
            WHERE (p.internal_sku IS NOT NULL AND ip.sku = p.internal_sku)
               OR (p.sku IS NOT NULL AND ip.sku = p.sku)
               OR (p.ean IS NOT NULL AND ip.sku = p.ean)
               OR (p.k_number IS NOT NULL AND ip.kod_k = p.k_number)
            ORDER BY
                CASE
                    WHEN p.internal_sku IS NOT NULL AND ip.sku = p.internal_sku THEN 0
                    WHEN p.sku IS NOT NULL AND ip.sku = p.sku THEN 1
                    WHEN p.ean IS NOT NULL AND ip.sku = p.ean THEN 2
                    WHEN p.k_number IS NOT NULL AND ip.kod_k = p.k_number THEN 3
                    ELSE 4
                END,
                ip.id DESC
        ) ip
        WHERE (? IS NOT NULL AND p.sku = ?)
           OR (? IS NOT NULL AND p.asin = ?)
           OR (? IS NOT NULL AND p.ean = ?)
        """,
        (sku, sku, asin, asin, candidate_ean, candidate_ean),
    )
    row = _fetchone_dict(cur) or {}
    if not row and sku:
        cur.execute(
            """
            SELECT TOP 1 title
            FROM dbo.acc_order_line WITH (NOLOCK)
            WHERE sku = ?
              AND title IS NOT NULL
              AND LTRIM(RTRIM(title)) <> ''
            """,
            (sku,),
        )
        fallback = cur.fetchone()
        if fallback and fallback[0]:
            result = {"title_preferred": str(fallback[0]).strip(), "title": str(fallback[0]).strip()}
            if registry:
                result.update(
                    {
                        "internal_sku": registry.get("internal_sku"),
                        "ean": registry.get("ean"),
                        "parent_asin": registry.get("parent_asin"),
                    }
                )
            return result
        netfox = _lookup_netfox_product_context(sku=sku, ean=candidate_ean)
        result = dict(netfox)
        if registry:
            result.setdefault("title_preferred", registry.get("product_name"))
            result.setdefault("title", registry.get("product_name"))
            result["internal_sku"] = result.get("internal_sku") or registry.get("internal_sku")
            result["ean"] = result.get("ean") or registry.get("ean")
            result["parent_asin"] = result.get("parent_asin") or registry.get("parent_asin")
            result["brand"] = result.get("brand") or registry.get("brand")
            result["category"] = result.get("category") or registry.get("category_1") or registry.get("category_2")
        return result
    preferred_title = str(row.get("title_pl") or registry.get("product_name") or row.get("title") or "").strip()
    result = {
        "title_preferred": preferred_title or None,
        "title_pl": str(row.get("title_pl") or "").strip() or None,
        "title": str(row.get("title") or registry.get("product_name") or "").strip() or None,
        "brand": str(row.get("brand") or registry.get("brand") or "").strip() or None,
        "category": str(row.get("category") or registry.get("category_1") or registry.get("category_2") or "").strip() or None,
        "internal_sku": str(row.get("internal_sku") or registry.get("internal_sku") or "").strip() or None,
        "ean": str(row.get("ean") or registry.get("ean") or "").strip() or None,
        "parent_asin": str(registry.get("parent_asin") or "").strip() or None,
        "k_number": str(row.get("k_number") or "").strip() or None,
    }
    if not result.get("title_preferred"):
        netfox = _lookup_netfox_product_context(sku=sku, ean=candidate_ean)
        if netfox.get("title_preferred"):
            result.update({k: v for k, v in netfox.items() if v and not result.get(k)})
            result["title_preferred"] = netfox["title_preferred"]
    return result


# ──────────── Schema DDL ────────────

def ensure_fba_schema() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
IF OBJECT_ID('dbo.acc_fba_inventory_snapshot', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fba_inventory_snapshot (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        marketplace_id NVARCHAR(20) NOT NULL,
        sku NVARCHAR(120) NOT NULL,
        asin NVARCHAR(40) NULL,
        on_hand INT NOT NULL DEFAULT 0,
        inbound INT NOT NULL DEFAULT 0,
        reserved INT NOT NULL DEFAULT 0,
        stranded_units INT NOT NULL DEFAULT 0,
        aged_0_30 INT NOT NULL DEFAULT 0,
        aged_31_60 INT NOT NULL DEFAULT 0,
        aged_61_90 INT NOT NULL DEFAULT 0,
        aged_90_plus INT NOT NULL DEFAULT 0,
        excess_units INT NOT NULL DEFAULT 0,
        snapshot_date DATE NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_acc_fba_inventory_snapshot UNIQUE (marketplace_id, sku, snapshot_date)
    );
    CREATE INDEX IX_acc_fba_inventory_snapshot_market_date ON dbo.acc_fba_inventory_snapshot(marketplace_id, snapshot_date);
    CREATE INDEX IX_acc_fba_inventory_snapshot_sku_date ON dbo.acc_fba_inventory_snapshot(sku, snapshot_date);
END

IF COL_LENGTH('dbo.acc_fba_inventory_snapshot', 'excess_units') IS NULL
BEGIN
    ALTER TABLE dbo.acc_fba_inventory_snapshot
    ADD excess_units INT NOT NULL CONSTRAINT DF_acc_fba_inventory_snapshot_excess_units DEFAULT 0;
END

IF OBJECT_ID('dbo.acc_fba_inbound_shipment', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fba_inbound_shipment (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        marketplace_id NVARCHAR(20) NULL,
        shipment_id NVARCHAR(120) NOT NULL,
        shipment_name NVARCHAR(200) NULL,
        status NVARCHAR(40) NOT NULL,
        created_at DATETIME2 NULL,
        last_update_at DATETIME2 NULL,
        from_warehouse NVARCHAR(120) NULL,
        units_planned INT NOT NULL DEFAULT 0,
        units_received INT NOT NULL DEFAULT 0,
        first_receive_at DATETIME2 NULL,
        closed_at DATETIME2 NULL,
        owner NVARCHAR(120) NULL,
        payload_json NVARCHAR(MAX) NULL,
        CONSTRAINT UQ_acc_fba_inbound_shipment UNIQUE (shipment_id)
    );
    CREATE INDEX IX_acc_fba_inbound_shipment_market_status ON dbo.acc_fba_inbound_shipment(marketplace_id, status);
END

IF OBJECT_ID('dbo.acc_fba_inbound_shipment_line', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fba_inbound_shipment_line (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        shipment_id NVARCHAR(120) NOT NULL,
        sku NVARCHAR(120) NOT NULL,
        asin NVARCHAR(40) NULL,
        qty_planned INT NOT NULL DEFAULT 0,
        qty_received INT NOT NULL DEFAULT 0,
        payload_json NVARCHAR(MAX) NULL
    );
    CREATE INDEX IX_acc_fba_inbound_shipment_line_shipment ON dbo.acc_fba_inbound_shipment_line(shipment_id);
    CREATE INDEX IX_acc_fba_inbound_shipment_line_sku ON dbo.acc_fba_inbound_shipment_line(sku);
END

IF OBJECT_ID('dbo.acc_fba_bundle', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fba_bundle (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        bundle_sku NVARCHAR(120) NOT NULL,
        name NVARCHAR(240) NOT NULL,
        status_stage NVARCHAR(40) NOT NULL DEFAULT 'idea',
        marketplaces_json NVARCHAR(MAX) NULL,
        bom_json NVARCHAR(MAX) NULL,
        expected_cm1 DECIMAL(18,4) NULL,
        expected_np DECIMAL(18,4) NULL,
        owner NVARCHAR(120) NULL,
        due_date DATE NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END

IF OBJECT_ID('dbo.acc_fba_bundle_event', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fba_bundle_event (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        bundle_id UNIQUEIDENTIFIER NOT NULL,
        event_type NVARCHAR(60) NOT NULL,
        at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        payload_json NVARCHAR(MAX) NULL
    );
    CREATE INDEX IX_acc_fba_bundle_event_bundle ON dbo.acc_fba_bundle_event(bundle_id, at DESC);
END

IF OBJECT_ID('dbo.acc_fba_kpi_snapshot', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fba_kpi_snapshot (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        quarter NVARCHAR(16) NOT NULL,
        marketplace_id NVARCHAR(20) NULL,
        ipi DECIMAL(18,4) NULL,
        oos_top100 DECIMAL(18,4) NULL,
        aged_90plus_value_pct DECIMAL(18,4) NULL,
        stranded_value DECIMAL(18,4) NULL,
        bundles_created INT NOT NULL DEFAULT 0,
        bundles_breakeven_90d_pct DECIMAL(18,4) NULL,
        score_json NVARCHAR(MAX) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_fba_kpi_snapshot_quarter ON dbo.acc_fba_kpi_snapshot(quarter, marketplace_id);
END

IF OBJECT_ID('dbo.acc_fba_sku_status', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fba_sku_status (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        marketplace_id NVARCHAR(20) NULL,
        sku NVARCHAR(120) NOT NULL,
        is_excluded BIT NOT NULL DEFAULT 0,
        is_retired BIT NOT NULL DEFAULT 0,
        notes NVARCHAR(500) NULL,
        effective_from DATE NULL,
        effective_to DATE NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_fba_sku_status_sku_market ON dbo.acc_fba_sku_status(sku, marketplace_id, effective_from, effective_to);
END

IF OBJECT_ID('dbo.acc_fba_shipment_plan', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fba_shipment_plan (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        quarter NVARCHAR(16) NOT NULL,
        marketplace_id NVARCHAR(20) NULL,
        shipment_id NVARCHAR(120) NULL,
        plan_week_start DATE NOT NULL,
        planned_ship_date DATE NULL,
        planned_units INT NOT NULL DEFAULT 0,
        actual_ship_date DATE NULL,
        actual_units INT NULL,
        tolerance_pct DECIMAL(18,4) NOT NULL DEFAULT 0.10,
        status NVARCHAR(40) NOT NULL DEFAULT 'planned',
        owner NVARCHAR(120) NULL,
        notes_json NVARCHAR(MAX) NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_fba_shipment_plan_quarter ON dbo.acc_fba_shipment_plan(quarter, marketplace_id, status);
END

IF OBJECT_ID('dbo.acc_fba_case', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fba_case (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        case_type NVARCHAR(60) NOT NULL,
        marketplace_id NVARCHAR(20) NULL,
        entity_type NVARCHAR(40) NULL,
        entity_id NVARCHAR(120) NULL,
        sku NVARCHAR(120) NULL,
        detected_date DATE NOT NULL,
        close_date DATE NULL,
        owner NVARCHAR(120) NULL,
        status NVARCHAR(40) NOT NULL DEFAULT 'open',
        root_cause NVARCHAR(240) NULL,
        payload_json NVARCHAR(MAX) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_fba_case_status ON dbo.acc_fba_case(status, detected_date, close_date);
END

IF OBJECT_ID('dbo.acc_fba_case_event', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fba_case_event (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        case_id UNIQUEIDENTIFIER NOT NULL,
        event_type NVARCHAR(60) NOT NULL,
        event_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        actor NVARCHAR(120) NULL,
        payload_json NVARCHAR(MAX) NULL
    );
    CREATE INDEX IX_acc_fba_case_event_case ON dbo.acc_fba_case_event(case_id, event_at DESC);
END

IF OBJECT_ID('dbo.acc_fba_report_diagnostic', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fba_report_diagnostic (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        sync_scope NVARCHAR(40) NOT NULL,
        marketplace_id NVARCHAR(20) NULL,
        report_type NVARCHAR(120) NOT NULL,
        fetch_mode NVARCHAR(60) NOT NULL,
        request_report_id NVARCHAR(80) NULL,
        request_status NVARCHAR(40) NULL,
        selected_report_id NVARCHAR(80) NULL,
        selected_status NVARCHAR(40) NULL,
        selected_document_id NVARCHAR(200) NULL,
        fallback_source NVARCHAR(80) NULL,
        detail_json NVARCHAR(MAX) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_fba_report_diagnostic_scope ON dbo.acc_fba_report_diagnostic(sync_scope, created_at DESC);
    CREATE INDEX IX_acc_fba_report_diagnostic_market_report ON dbo.acc_fba_report_diagnostic(marketplace_id, report_type, created_at DESC);
END

IF OBJECT_ID('dbo.acc_fba_receiving_reconciliation', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fba_receiving_reconciliation (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        shipment_id NVARCHAR(120) NULL,
        marketplace_id NVARCHAR(20) NULL,
        sku NVARCHAR(120) NULL,
        event_date DATE NOT NULL,
        shipped_units INT NOT NULL DEFAULT 0,
        shortage_units INT NOT NULL DEFAULT 0,
        damage_units INT NOT NULL DEFAULT 0,
        reimbursement_units INT NOT NULL DEFAULT 0,
        notes_json NVARCHAR(MAX) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_fba_receiving_reconciliation_date ON dbo.acc_fba_receiving_reconciliation(event_date, marketplace_id);
END

IF OBJECT_ID('dbo.acc_fba_launch', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fba_launch (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        quarter NVARCHAR(16) NOT NULL,
        launch_type NVARCHAR(40) NOT NULL DEFAULT 'new_sku',
        sku NVARCHAR(120) NULL,
        bundle_id UNIQUEIDENTIFIER NULL,
        marketplace_id NVARCHAR(20) NULL,
        planned_go_live_date DATE NULL,
        actual_go_live_date DATE NULL,
        live_stable_at DATE NULL,
        incident_free BIT NOT NULL DEFAULT 1,
        vine_eligible BIT NOT NULL DEFAULT 0,
        vine_eligible_at DATE NULL,
        vine_submitted_at DATE NULL,
        owner NVARCHAR(120) NULL,
        status NVARCHAR(40) NOT NULL DEFAULT 'planned',
        payload_json NVARCHAR(MAX) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_fba_launch_quarter ON dbo.acc_fba_launch(quarter, marketplace_id, status);
END

IF OBJECT_ID('dbo.acc_fba_initiative', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fba_initiative (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        quarter NVARCHAR(16) NOT NULL,
        initiative_type NVARCHAR(40) NOT NULL,
        title NVARCHAR(240) NOT NULL,
        sku NVARCHAR(120) NULL,
        bundle_id UNIQUEIDENTIFIER NULL,
        owner NVARCHAR(120) NULL,
        status NVARCHAR(40) NOT NULL DEFAULT 'planned',
        planned BIT NOT NULL DEFAULT 1,
        approved BIT NOT NULL DEFAULT 1,
        live_stable_at DATE NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_fba_initiative_quarter ON dbo.acc_fba_initiative(quarter, status, planned, approved);
END

IF OBJECT_ID('dbo.acc_fba_config', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_fba_config (
        [key] NVARCHAR(100) NOT NULL PRIMARY KEY,
        value_json NVARCHAR(MAX) NOT NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END

IF NOT EXISTS (SELECT 1 FROM dbo.acc_fba_config WHERE [key] = 'defaults')
BEGIN
    INSERT INTO dbo.acc_fba_config ([key], value_json)
    VALUES ('defaults', '{"target_days":45,"safety_stock_days":14,"lead_time_days":21,"inbound_stuck_days":7}');
END

IF NOT EXISTS (SELECT 1 FROM dbo.acc_fba_config WHERE [key] = 'scorecard_defaults')
BEGIN
    INSERT INTO dbo.acc_fba_config ([key], value_json)
    VALUES (
        'scorecard_defaults',
        '{
            "top100_availability":{"label":"Top 100 Availability","unit":"%","direction":"higher","weight":0.22,"alarm":70.0,"target":90.0,"good":98.0},
            "shipment_plan_adherence":{"label":"Shipment Plan Adherence","unit":"%","direction":"higher","weight":0.14,"alarm":70.0,"target":90.0,"good":97.0},
            "stranded_inventory_value_pct":{"label":"Stranded Inventory Value","unit":"%","direction":"lower","weight":0.10,"alarm":5.0,"target":2.0,"good":1.0},
            "median_resolve_days":{"label":"Median Resolve Days","unit":"days","direction":"lower","weight":0.10,"alarm":14.0,"target":5.0,"good":2.0},
            "aging_excess_share":{"label":"Aging / Excess Share","unit":"%","direction":"lower","weight":0.12,"alarm":20.0,"target":10.0,"good":5.0},
            "fc_discrepancy_rate":{"label":"FC Discrepancy Rate","unit":"%","direction":"lower","weight":0.08,"alarm":3.0,"target":1.0,"good":0.3},
            "on_time_launch_rate":{"label":"On-time Launch Rate","unit":"%","direction":"higher","weight":0.08,"alarm":70.0,"target":90.0,"good":100.0},
            "vine_coverage_rate":{"label":"Vine Coverage Rate","unit":"%","direction":"higher","weight":0.06,"alarm":50.0,"target":85.0,"good":100.0},
            "initiatives_completion_rate":{"label":"Initiatives Completion Rate","unit":"%","direction":"higher","weight":0.10,"alarm":60.0,"target":100.0,"good":120.0}
        }'
    );
END
"""
        )
        conn.commit()
    finally:
        conn.close()
