from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

log = logging.getLogger(__name__)

_INV_OVERVIEW_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


def _inv_cache_key(marketplace_ids: list[str] | None) -> str:
    if not marketplace_ids:
        return "all"
    return ",".join(sorted(str(x) for x in marketplace_ids if str(x).strip()))


def _inv_overview_cache_get(key: str) -> dict[str, Any] | None:
    row = _INV_OVERVIEW_CACHE.get(key)
    if not row:
        return None
    exp, value = row
    if time.monotonic() > exp:
        _INV_OVERVIEW_CACHE.pop(key, None)
        return None
    return value


def _inv_overview_cache_set(key: str, value: dict[str, Any], ttl_sec: int = 180) -> None:
    _INV_OVERVIEW_CACHE[key] = (time.monotonic() + ttl_sec, value)

from app.connectors.mssql import enqueue_job, list_jobs
from app.core.config import MARKETPLACE_REGISTRY, settings
from app.core.db_connection import connect_acc


def _connect():
    return connect_acc(autocommit=False, timeout=20)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _chunked(values: Iterable[str], size: int = 400) -> list[list[str]]:
    items = [str(v) for v in values if str(v or "").strip()]
    return [items[i:i + size] for i in range(0, len(items), size)]


def _seed_temp_values(
    cur,
    table_name: str,
    column_name: str,
    sql_type: str,
    values: Iterable[str],
) -> list[str]:
    cleaned = sorted({str(v).strip() for v in values if str(v or "").strip()})
    cur.execute(f"IF OBJECT_ID('tempdb..{table_name}') IS NOT NULL DROP TABLE {table_name}")
    cur.execute(f"CREATE TABLE {table_name} ({column_name} {sql_type} NOT NULL PRIMARY KEY)")
    if cleaned:
        rows = [(value,) for value in cleaned]
        if hasattr(cur, "fast_executemany"):
            cur.fast_executemany = True
        cur.executemany(f"INSERT INTO {table_name} ({column_name}) VALUES (?)", rows)
        if hasattr(cur, "fast_executemany"):
            cur.fast_executemany = False
    return cleaned


def _seed_marketplace_code_map(cur, table_name: str = "#tmp_inv_marketplace_codes") -> None:
    cur.execute(f"IF OBJECT_ID('tempdb..{table_name}') IS NOT NULL DROP TABLE {table_name}")
    cur.execute(
        f"""
        CREATE TABLE {table_name} (
            marketplace_id NVARCHAR(32) NOT NULL PRIMARY KEY,
            marketplace_code NVARCHAR(10) NOT NULL
        )
        """
    )
    rows = [
        (str(marketplace_id), str(info.get("code") or ""))
        for marketplace_id, info in MARKETPLACE_REGISTRY.items()
        if str(info.get("code") or "").strip()
    ]
    if rows:
        if hasattr(cur, "fast_executemany"):
            cur.fast_executemany = True
        cur.executemany(
            f"INSERT INTO {table_name} (marketplace_id, marketplace_code) VALUES (?, ?)",
            rows,
        )
        if hasattr(cur, "fast_executemany"):
            cur.fast_executemany = False


def _marketplace_code(marketplace_id: str | None) -> str:
    if not marketplace_id:
        return ""
    info = MARKETPLACE_REGISTRY.get(str(marketplace_id))
    return info["code"] if info else str(marketplace_id)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def _json_loads(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return default


def _status_from_pct(pct: float, *, good: float = 95.0, warn: float = 70.0) -> str:
    if pct >= good:
        return "ok"
    if pct >= warn:
        return "warning"
    return "critical"


def _is_amzn_grade_sku(sku: Any) -> bool:
    return str(sku or "").strip().lower().startswith("amzn.gr.")


def _overview_group_key(item: dict[str, Any]) -> str:
    key = (
        str(item.get("local_parent_asin") or "").strip()
        or str(item.get("parent_asin") or "").strip()
        or str(item.get("asin") or "").strip()
    )
    if key:
        return key
    return str(item.get("sku") or "").strip()


_DEFAULT_SETTINGS = {
    "thresholds": {
        "high_sessions_threshold": 100,
        "high_units_threshold": 5,
        "stockout_days_critical": 7,
        "stockout_days_warning": 14,
        "overstock_days": 90,
        "cvr_crash_pct": -25.0,
        "traffic_drop_pct": -30.0,
    },
    "theme_requirements": {
        "color": ["color"],
        "size": ["size"],
        "size_color": ["size", "color"],
    },
    "apply_safety": {
        "block_conflicts": True,
        "block_missing_attrs": True,
        "auto_propose_confidence": 75,
        "safe_auto_confidence": 90,
    },
    "traffic_schedule": {
        "incremental_days": 90,
        "nightly_hour_utc": 2,
    },
    "saved_views_enabled": True,
}


def ensure_manage_inventory_schema() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
IF OBJECT_ID('dbo.acc_inv_traffic_sku_daily', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_inv_traffic_sku_daily (
        marketplace_id NVARCHAR(32) NOT NULL,
        sku NVARCHAR(120) NOT NULL,
        report_date DATE NOT NULL,
        asin NVARCHAR(40) NULL,
        sessions INT NULL,
        page_views INT NULL,
        units_ordered INT NULL,
        orders_count INT NULL,
        revenue DECIMAL(18,4) NULL,
        unit_session_pct DECIMAL(18,6) NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_acc_inv_traffic_sku_daily PRIMARY KEY (marketplace_id, sku, report_date)
    );
    CREATE INDEX IX_acc_inv_traffic_sku_daily_date ON dbo.acc_inv_traffic_sku_daily(report_date, marketplace_id);
END

IF OBJECT_ID('dbo.acc_inv_traffic_asin_daily', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_inv_traffic_asin_daily (
        marketplace_id NVARCHAR(32) NOT NULL,
        asin NVARCHAR(40) NOT NULL,
        report_date DATE NOT NULL,
        sessions INT NULL,
        page_views INT NULL,
        units_ordered INT NULL,
        orders_count INT NULL,
        revenue DECIMAL(18,4) NULL,
        unit_session_pct DECIMAL(18,6) NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_acc_inv_traffic_asin_daily PRIMARY KEY (marketplace_id, asin, report_date)
    );
    CREATE INDEX IX_acc_inv_traffic_asin_daily_date ON dbo.acc_inv_traffic_asin_daily(report_date, marketplace_id);
END

IF OBJECT_ID('dbo.acc_inv_traffic_rollup', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_inv_traffic_rollup (
        marketplace_id NVARCHAR(32) NOT NULL,
        sku NVARCHAR(120) NULL,
        asin NVARCHAR(40) NULL,
        range_key NVARCHAR(10) NOT NULL,
        sessions INT NULL,
        page_views INT NULL,
        units INT NULL,
        orders_count INT NULL,
        revenue DECIMAL(18,4) NULL,
        unit_session_pct DECIMAL(18,6) NULL,
        sessions_delta_pct DECIMAL(18,6) NULL,
        cvr_delta_pct DECIMAL(18,6) NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE UNIQUE INDEX UX_acc_inv_traffic_rollup
        ON dbo.acc_inv_traffic_rollup(marketplace_id, range_key, sku, asin);
END

IF OBJECT_ID('dbo.acc_inv_item_cache', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_inv_item_cache (
        marketplace_id NVARCHAR(32) NOT NULL,
        sku NVARCHAR(120) NOT NULL,
        asin NVARCHAR(40) NULL,
        snapshot_date DATE NULL,
        title_preferred NVARCHAR(400) NULL,
        listing_status NVARCHAR(32) NULL,
        stockout_risk_badge NVARCHAR(32) NULL,
        overstock_risk_badge NVARCHAR(32) NULL,
        family_health NVARCHAR(40) NULL,
        global_family_status NVARCHAR(40) NULL,
        days_cover DECIMAL(18,4) NULL,
        sessions_7d INT NULL,
        orders_7d INT NULL,
        units_ordered_7d INT NULL,
        unit_session_pct_7d DECIMAL(18,6) NULL,
        cvr_delta_pct DECIMAL(18,6) NULL,
        sessions_delta_pct DECIMAL(18,6) NULL,
        traffic_coverage_flag BIT NOT NULL DEFAULT 1,
        internal_sku NVARCHAR(80) NULL,
        ean NVARCHAR(80) NULL,
        payload_json NVARCHAR(MAX) NOT NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_acc_inv_item_cache PRIMARY KEY (marketplace_id, sku)
    );
    CREATE INDEX IX_acc_inv_item_cache_filters
        ON dbo.acc_inv_item_cache(marketplace_id, listing_status, stockout_risk_badge, family_health, traffic_coverage_flag);
END

IF OBJECT_ID('dbo.acc_inv_change_draft', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_inv_change_draft (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        draft_type NVARCHAR(40) NOT NULL,
        marketplace_id NVARCHAR(32) NULL,
        affected_parent_asin NVARCHAR(40) NULL,
        affected_sku NVARCHAR(120) NULL,
        payload_json NVARCHAR(MAX) NOT NULL,
        snapshot_before_json NVARCHAR(MAX) NULL,
        snapshot_after_json NVARCHAR(MAX) NULL,
        validation_status NVARCHAR(20) NOT NULL DEFAULT 'pending',
        validation_errors_json NVARCHAR(MAX) NULL,
        approval_status NVARCHAR(20) NOT NULL DEFAULT 'draft',
        apply_status NVARCHAR(20) NOT NULL DEFAULT 'pending',
        created_by NVARCHAR(120) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        approved_by NVARCHAR(120) NULL,
        approved_at DATETIME2 NULL,
        apply_started_at DATETIME2 NULL,
        applied_at DATETIME2 NULL,
        rolled_back_at DATETIME2 NULL
    );
    CREATE INDEX IX_acc_inv_change_draft_status
        ON dbo.acc_inv_change_draft(validation_status, approval_status, apply_status, created_at);
END

IF OBJECT_ID('dbo.acc_inv_change_event', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_inv_change_event (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        draft_id UNIQUEIDENTIFIER NOT NULL,
        event_type NVARCHAR(40) NOT NULL,
        actor NVARCHAR(120) NULL,
        payload_json NVARCHAR(MAX) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_inv_change_event_main ON dbo.acc_inv_change_event(draft_id, created_at DESC);
END

IF OBJECT_ID('dbo.acc_inv_settings', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_inv_settings (
        [key] NVARCHAR(80) NOT NULL PRIMARY KEY,
        value_json NVARCHAR(MAX) NOT NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END

IF OBJECT_ID('dbo.acc_inv_category_cvr_baseline', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_inv_category_cvr_baseline (
        marketplace_id NVARCHAR(32) NOT NULL,
        category NVARCHAR(200) NOT NULL,
        p25 DECIMAL(18,6) NULL,
        p50 DECIMAL(18,6) NULL,
        p75 DECIMAL(18,6) NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_acc_inv_category_cvr_baseline PRIMARY KEY (marketplace_id, category)
    );
END
            """
        )
        cur.execute("SELECT COUNT(*) FROM dbo.acc_inv_settings WHERE [key] = 'default'")
        if _safe_int(cur.fetchone()[0]) == 0:
            cur.execute(
                """
                INSERT INTO dbo.acc_inv_settings ([key], value_json, updated_at)
                VALUES ('default', ?, SYSUTCDATETIME())
                """,
                (json.dumps(_DEFAULT_SETTINGS, ensure_ascii=False),),
            )

        # ---- tables queried by helpers but created by other migrations ----
        cur.execute("""
IF OBJECT_ID('dbo.acc_amazon_listing_registry', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_amazon_listing_registry (
        id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        merchant_sku NVARCHAR(255) NULL,
        merchant_sku_alt NVARCHAR(255) NULL,
        internal_sku NVARCHAR(64) NULL,
        ean NVARCHAR(64) NULL,
        asin NVARCHAR(64) NULL,
        parent_asin NVARCHAR(64) NULL,
        brand NVARCHAR(128) NULL,
        product_name NVARCHAR(512) NULL,
        listing_role NVARCHAR(32) NULL,
        priority_label NVARCHAR(64) NULL,
        launch_type NVARCHAR(64) NULL,
        category_1 NVARCHAR(255) NULL,
        category_2 NVARCHAR(255) NULL,
        source_gid NVARCHAR(32) NOT NULL DEFAULT 'manual',
        row_hash NVARCHAR(64) NOT NULL DEFAULT '',
        raw_json NVARCHAR(MAX) NULL,
        synced_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_alr_sku ON dbo.acc_amazon_listing_registry(merchant_sku);
    CREATE INDEX IX_acc_alr_asin ON dbo.acc_amazon_listing_registry(asin);
    CREATE INDEX IX_acc_alr_ean ON dbo.acc_amazon_listing_registry(ean);
END

IF OBJECT_ID('dbo.global_family', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.global_family (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        de_parent_asin NVARCHAR(20) NOT NULL,
        brand NVARCHAR(120) NULL,
        category NVARCHAR(200) NULL,
        product_type NVARCHAR(120) NULL,
        variation_theme_de NVARCHAR(120) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE UNIQUE INDEX UX_global_family_de_parent_asin ON dbo.global_family(de_parent_asin);
END

IF OBJECT_ID('dbo.global_family_child', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.global_family_child (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        global_family_id INT NOT NULL,
        master_key NVARCHAR(120) NOT NULL,
        key_type NVARCHAR(20) NOT NULL,
        de_child_asin NVARCHAR(20) NOT NULL,
        sku_de NVARCHAR(80) NULL,
        ean_de NVARCHAR(20) NULL,
        attributes_json NVARCHAR(MAX) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE UNIQUE INDEX UX_gfc_family_master ON dbo.global_family_child(global_family_id, master_key);
    CREATE INDEX IX_gfc_de_child_asin ON dbo.global_family_child(de_child_asin);
END

IF OBJECT_ID('dbo.marketplace_listing_child', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.marketplace_listing_child (
        marketplace NVARCHAR(10) NOT NULL,
        asin NVARCHAR(20) NOT NULL,
        sku NVARCHAR(80) NULL,
        ean NVARCHAR(20) NULL,
        current_parent_asin NVARCHAR(20) NULL,
        variation_theme NVARCHAR(120) NULL,
        attributes_json NVARCHAR(MAX) NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_mlc PRIMARY KEY (marketplace, asin)
    );
    CREATE INDEX IX_mlc_mp_sku ON dbo.marketplace_listing_child(marketplace, sku);
END

IF OBJECT_ID('dbo.global_family_market_link', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.global_family_market_link (
        global_family_id INT NOT NULL,
        marketplace NVARCHAR(10) NOT NULL,
        target_parent_asin NVARCHAR(20) NULL,
        status NVARCHAR(20) NOT NULL DEFAULT 'needs_review',
        confidence_avg INT NOT NULL DEFAULT 0,
        notes NVARCHAR(MAX) NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_gfml PRIMARY KEY (global_family_id, marketplace)
    );
END
        """)

        conn.commit()
    finally:
        conn.close()


def _load_settings(cur) -> tuple[dict[str, Any], datetime | None]:
    cur.execute("SELECT TOP 1 value_json, updated_at FROM dbo.acc_inv_settings WHERE [key] = 'default'")
    row = cur.fetchone()
    if not row:
        return dict(_DEFAULT_SETTINGS), None
    return _json_loads(row[0], dict(_DEFAULT_SETTINGS)), row[1]


def _fetch_inventory_base_rows(
    cur,
    *,
    marketplace_ids: list[str] | None = None,
    search_terms: list[str] | None = None,
) -> tuple[date | None, list[dict[str, Any]]]:
    cur.execute("SELECT MAX(snapshot_date) FROM dbo.acc_fba_inventory_snapshot WITH (NOLOCK)")
    latest_row = cur.fetchone()
    snapshot_date = latest_row[0] if latest_row and latest_row[0] else None
    if snapshot_date is None:
        return None, []
    params: list[Any] = [snapshot_date]
    where_parts = ["snapshot_date = ?"]
    if marketplace_ids:
        _seed_temp_values(cur, "#tmp_inv_base_markets", "marketplace_id", "NVARCHAR(32)", marketplace_ids)
        where_parts.append("marketplace_id IN (SELECT marketplace_id FROM #tmp_inv_base_markets)")
    if search_terms:
        normalized = sorted({term.strip().upper() for term in search_terms if term and term.strip()})
        if normalized:
            _seed_temp_values(cur, "#tmp_inv_search", "term", "NVARCHAR(120)", normalized)
            where_parts.append(
                "(UPPER(ISNULL(sku, '')) IN (SELECT term FROM #tmp_inv_search) "
                "OR UPPER(ISNULL(asin, '')) IN (SELECT term FROM #tmp_inv_search))"
            )
    cur.execute(
        f"""
        SELECT
            marketplace_id, sku, asin, on_hand, inbound, reserved,
            stranded_units, aged_90_plus, excess_units, snapshot_date
        FROM dbo.acc_fba_inventory_snapshot WITH (NOLOCK)
        WHERE {" AND ".join(where_parts)}
        """,
        tuple(params),
    )
    cols = [d[0] for d in cur.description]
    return snapshot_date, [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]


def _fetch_product_maps(cur, skus: list[str], asins: list[str]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_sku: dict[str, dict[str, Any]] = {}
    by_asin: dict[str, dict[str, Any]] = {}
    if skus:
        _seed_temp_values(cur, "#tmp_inv_product_skus", "sku", "NVARCHAR(120)", skus)
        cur.execute(
            """
            SELECT id, asin, ean, sku, brand, category, subcategory, title,
                   is_parent, parent_asin, internal_sku, mapping_source, netto_purchase_price_pln
            FROM dbo.acc_product WITH (NOLOCK)
            WHERE sku IN (SELECT sku FROM #tmp_inv_product_skus)
            """
        )
        cols = [d[0] for d in cur.description]
        for row in cur.fetchall():
            item = {cols[i]: row[i] for i in range(len(cols))}
            if item.get("sku"):
                by_sku[str(item["sku"])] = item
            if item.get("asin"):
                by_asin[str(item["asin"])] = item
    missing_asins = [asin for asin in set(asins) if asin and asin not in by_asin]
    if missing_asins:
        _seed_temp_values(cur, "#tmp_inv_product_asins", "asin", "NVARCHAR(40)", missing_asins)
        cur.execute(
            """
            SELECT id, asin, ean, sku, brand, category, subcategory, title,
                   is_parent, parent_asin, internal_sku, mapping_source, netto_purchase_price_pln
            FROM dbo.acc_product WITH (NOLOCK)
            WHERE asin IN (SELECT asin FROM #tmp_inv_product_asins)
            """
        )
        cols = [d[0] for d in cur.description]
        for row in cur.fetchall():
            item = {cols[i]: row[i] for i in range(len(cols))}
            if item.get("sku"):
                by_sku.setdefault(str(item["sku"]), item)
            if item.get("asin"):
                by_asin[str(item["asin"])] = item
    return by_sku, by_asin


def _fetch_registry_maps(
    cur,
    skus: list[str],
    asins: list[str],
    eans: list[str],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_sku: dict[str, dict[str, Any]] = {}
    by_asin: dict[str, dict[str, Any]] = {}
    by_ean: dict[str, dict[str, Any]] = {}
    cols: list[str] | None = None

    def _consume_rows() -> None:
        nonlocal cols
        cols = cols or [d[0] for d in cur.description]
        for row in cur.fetchall():
            item = {cols[i]: row[i] for i in range(len(cols))}
            for key in (item.get("merchant_sku"), item.get("merchant_sku_alt")):
                if key:
                    by_sku[str(key)] = item
            if item.get("asin"):
                by_asin[str(item["asin"])] = item
            if item.get("ean"):
                by_ean[str(item["ean"])] = item

    if skus:
        _seed_temp_values(cur, "#tmp_inv_registry_skus", "sku", "NVARCHAR(160)", skus)
        cur.execute(
            """
            SELECT merchant_sku, merchant_sku_alt, internal_sku, ean, asin, parent_asin, brand,
                   product_name, listing_role, priority_label, launch_type, category_1, category_2,
                   source_gid, row_hash, synced_at, updated_at
            FROM dbo.acc_amazon_listing_registry WITH (NOLOCK)
            WHERE merchant_sku IN (SELECT sku FROM #tmp_inv_registry_skus)
               OR merchant_sku_alt IN (SELECT sku FROM #tmp_inv_registry_skus)
            """
        )
        _consume_rows()

    missing_asins = [asin for asin in set(asins) if asin and asin not in by_asin]
    if missing_asins:
        _seed_temp_values(cur, "#tmp_inv_registry_asins", "asin", "NVARCHAR(40)", missing_asins)
        cur.execute(
            """
            SELECT merchant_sku, merchant_sku_alt, internal_sku, ean, asin, parent_asin, brand,
                   product_name, listing_role, priority_label, launch_type, category_1, category_2,
                   source_gid, row_hash, synced_at, updated_at
            FROM dbo.acc_amazon_listing_registry WITH (NOLOCK)
            WHERE asin IN (SELECT asin FROM #tmp_inv_registry_asins)
            """
        )
        _consume_rows()

    missing_eans = [ean for ean in set(eans) if ean and ean not in by_ean]
    if missing_eans:
        _seed_temp_values(cur, "#tmp_inv_registry_eans", "ean", "NVARCHAR(80)", missing_eans)
        cur.execute(
            """
            SELECT merchant_sku, merchant_sku_alt, internal_sku, ean, asin, parent_asin, brand,
                   product_name, listing_role, priority_label, launch_type, category_1, category_2,
                   source_gid, row_hash, synced_at, updated_at
            FROM dbo.acc_amazon_listing_registry WITH (NOLOCK)
            WHERE ean IN (SELECT ean FROM #tmp_inv_registry_eans)
            """
        )
        _consume_rows()
    return by_sku, by_asin, by_ean


def _fetch_listing_maps(
    cur,
    skus: list[str],
    asins: list[str],
    marketplace_codes: list[str] | None = None,
) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
    by_market_sku: dict[tuple[str, str], dict[str, Any]] = {}
    by_market_asin: dict[tuple[str, str], dict[str, Any]] = {}
    wanted = list(set(skus) | set(asins))
    if not wanted:
        return by_market_sku, by_market_asin
    cur.execute("SELECT TOP 1 1 FROM dbo.marketplace_listing_child WITH (NOLOCK)")
    if not cur.fetchone():
        return by_market_sku, by_market_asin

    where_mp = ""
    if marketplace_codes:
        _seed_temp_values(cur, "#tmp_inv_listing_marketplaces", "marketplace", "NVARCHAR(10)", marketplace_codes)
        where_mp = " AND marketplace IN (SELECT marketplace FROM #tmp_inv_listing_marketplaces)"

    if skus:
        _seed_temp_values(cur, "#tmp_inv_listing_skus", "sku", "NVARCHAR(120)", skus)
        cur.execute(
            f"""
            SELECT marketplace, asin, sku, ean, current_parent_asin, variation_theme, attributes_json, updated_at
            FROM dbo.marketplace_listing_child WITH (NOLOCK)
            WHERE sku IN (SELECT sku FROM #tmp_inv_listing_skus){where_mp}
            """
        )
        cols = [d[0] for d in cur.description]
        for row in cur.fetchall():
            item = {cols[i]: row[i] for i in range(len(cols))}
            mp = str(item.get("marketplace") or "")
            if item.get("sku"):
                by_market_sku[(mp, str(item["sku"]))] = item
            if item.get("asin"):
                by_market_asin[(mp, str(item["asin"]))] = item

    missing_asins = [asin for asin in set(asins) if asin and all(key[1] != asin for key in by_market_asin.keys())]
    if missing_asins:
        _seed_temp_values(cur, "#tmp_inv_listing_asins", "asin", "NVARCHAR(40)", missing_asins)
        cur.execute(
            f"""
            SELECT marketplace, asin, sku, ean, current_parent_asin, variation_theme, attributes_json, updated_at
            FROM dbo.marketplace_listing_child WITH (NOLOCK)
            WHERE asin IN (SELECT asin FROM #tmp_inv_listing_asins){where_mp}
            """
        )
        cols = [d[0] for d in cur.description]
        for row in cur.fetchall():
            item = {cols[i]: row[i] for i in range(len(cols))}
            mp = str(item.get("marketplace") or "")
            if item.get("sku"):
                by_market_sku.setdefault((mp, str(item["sku"])), item)
            if item.get("asin"):
                by_market_asin[(mp, str(item["asin"]))] = item
    return by_market_sku, by_market_asin


def _fetch_global_family_maps(cur, asins: list[str], eans: list[str]) -> tuple[set[str], set[str]]:
    asin_set: set[str] = set()
    ean_set: set[str] = set()
    if asins:
        _seed_temp_values(cur, "#tmp_inv_family_asins", "asin", "NVARCHAR(40)", asins)
        cur.execute(
            """
            SELECT de_child_asin
            FROM dbo.global_family_child WITH (NOLOCK)
            WHERE de_child_asin IN (SELECT asin FROM #tmp_inv_family_asins)
            """
        )
        for row in cur.fetchall():
            if row[0]:
                asin_set.add(str(row[0]))
    if eans:
        _seed_temp_values(cur, "#tmp_inv_family_eans", "ean", "NVARCHAR(80)", eans)
        cur.execute(
            """
            SELECT ean_de
            FROM dbo.global_family_child WITH (NOLOCK)
            WHERE ean_de IN (SELECT ean FROM #tmp_inv_family_eans)
            """
        )
        for row in cur.fetchall():
            if row[0]:
                ean_set.add(str(row[0]))
    return asin_set, ean_set


def _fetch_sales_aggregates(cur, marketplace_ids: list[str], skus: list[str]) -> dict[tuple[str, str], dict[str, float]]:
    if not marketplace_ids or not skus:
        return {}
    metrics: dict[tuple[str, str], dict[str, float]] = {}
    date_30 = date.today() - timedelta(days=30)
    date_7 = date.today() - timedelta(days=7)
    _seed_temp_values(cur, "#tmp_inv_marketplaces", "marketplace_id", "NVARCHAR(32)", marketplace_ids)
    _seed_temp_values(cur, "#tmp_inv_skus", "sku", "NVARCHAR(120)", skus)
    cur.execute(
        """
        SELECT
            o.marketplace_id,
            ol.sku,
            SUM(CASE WHEN CAST(o.purchase_date AS date) >= ?
                     THEN ISNULL(NULLIF(ol.quantity_shipped, 0), ol.quantity_ordered) ELSE 0 END) AS units_7d,
            SUM(CASE WHEN CAST(o.purchase_date AS date) >= ?
                     THEN ISNULL(NULLIF(ol.quantity_shipped, 0), ol.quantity_ordered) ELSE 0 END) AS units_30d,
            COUNT(DISTINCT CASE WHEN CAST(o.purchase_date AS date) >= ? THEN o.amazon_order_id END) AS orders_7d
        FROM dbo.acc_order o WITH (NOLOCK)
        JOIN #tmp_inv_marketplaces mp ON mp.marketplace_id = o.marketplace_id
        JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
        JOIN #tmp_inv_skus sku ON sku.sku = ol.sku
        WHERE o.status NOT IN ('Canceled', 'Cancelled')
          AND CAST(o.purchase_date AS date) >= ?
          AND ol.sku NOT LIKE 'amzn.gr.%%'
        GROUP BY o.marketplace_id, ol.sku
        """,
        (date_7, date_30, date_7, date_30),
    )
    for mp_id, sku, units_7d, units_30d, orders_7d in cur.fetchall():
        metrics[(str(mp_id), str(sku))] = {
            "units_7d": _safe_float(units_7d),
            "units_30d": _safe_float(units_30d),
            "orders_7d": _safe_float(orders_7d),
        }
    return metrics


def _fetch_traffic_rollups(cur, marketplace_ids: list[str], skus: list[str], asins: list[str]) -> dict[tuple[str, str], dict[str, Any]]:
    metrics: dict[tuple[str, str], dict[str, Any]] = {}
    wanted_markets = list(set(marketplace_ids))
    wanted_skus = list(set(skus))
    wanted_asins = list(set(asins))
    if not wanted_markets:
        return metrics
    cur.execute("SELECT TOP 1 1 FROM dbo.acc_inv_traffic_rollup WITH (NOLOCK)")
    if not cur.fetchone():
        return metrics
    _seed_temp_values(cur, "#tmp_inv_rollup_markets", "marketplace_id", "NVARCHAR(32)", wanted_markets)
    _seed_temp_values(cur, "#tmp_inv_rollup_skus", "sku", "NVARCHAR(120)", wanted_skus)
    _seed_temp_values(cur, "#tmp_inv_rollup_asins", "asin", "NVARCHAR(40)", wanted_asins)
    cur.execute(
        """
        SELECT r.marketplace_id, r.sku, r.asin, r.range_key, r.sessions, r.page_views, r.units, r.orders_count,
               r.revenue, r.unit_session_pct, r.sessions_delta_pct, r.cvr_delta_pct
        FROM dbo.acc_inv_traffic_rollup r WITH (NOLOCK)
        JOIN #tmp_inv_rollup_markets mp ON mp.marketplace_id = r.marketplace_id
        WHERE r.range_key IN ('7d', '30d')
          AND (
                (r.sku IS NOT NULL AND r.sku IN (SELECT sku FROM #tmp_inv_rollup_skus))
             OR (r.asin IS NOT NULL AND r.asin IN (SELECT asin FROM #tmp_inv_rollup_asins))
          )
        """
    )
    for row in cur.fetchall():
        mp_id, sku, asin, range_key, sessions, page_views, units, orders_count, revenue, unit_session_pct, sessions_delta_pct, cvr_delta_pct = row
        key = (str(mp_id), str(sku or asin or ""))
        bucket = metrics.setdefault(key, {})
        bucket[str(range_key)] = {
            "sessions": _safe_int(sessions, default=0) if sessions is not None else None,
            "page_views": _safe_int(page_views, default=0) if page_views is not None else None,
            "units": _safe_int(units, default=0) if units is not None else None,
            "orders_count": _safe_int(orders_count, default=0) if orders_count is not None else None,
            "revenue": _safe_float(revenue, default=0.0) if revenue is not None else None,
            "unit_session_pct": _safe_float(unit_session_pct) if unit_session_pct is not None else None,
            "sessions_delta_pct": _safe_float(sessions_delta_pct) if sessions_delta_pct is not None else None,
            "cvr_delta_pct": _safe_float(cvr_delta_pct) if cvr_delta_pct is not None else None,
        }
    return metrics


def _estimate_value(units: int, price_netto: float | None) -> float:
    if not price_netto or units <= 0:
        return 0.0
    return round(units * price_netto * 1.23, 2)


def _build_item(
    row: dict[str, Any],
    *,
    product_by_sku: dict[str, dict[str, Any]],
    product_by_asin: dict[str, dict[str, Any]],
    registry_by_sku: dict[str, dict[str, Any]],
    registry_by_asin: dict[str, dict[str, Any]],
    registry_by_ean: dict[str, dict[str, Any]],
    listing_by_market_sku: dict[tuple[str, str], dict[str, Any]],
    listing_by_market_asin: dict[tuple[str, str], dict[str, Any]],
    global_family_asins: set[str],
    global_family_eans: set[str],
    sales_map: dict[tuple[str, str], dict[str, float]],
    traffic_map: dict[tuple[str, str], dict[str, Any]],
    taxonomy_by_sku: dict[str, dict[str, Any]],
    taxonomy_by_asin: dict[str, dict[str, Any]],
    taxonomy_by_ean: dict[str, dict[str, Any]],
    settings: dict[str, Any],
) -> dict[str, Any]:
    sku = str(row.get("sku") or "")
    asin = str(row.get("asin") or "") or None
    marketplace_id = str(row.get("marketplace_id") or "")
    marketplace_code = _marketplace_code(marketplace_id)
    product = product_by_sku.get(sku) or (product_by_asin.get(asin) if asin else None) or {}
    registry = registry_by_sku.get(sku) or (registry_by_asin.get(asin) if asin else None) or {}
    if not registry and product.get("ean"):
        registry = registry_by_ean.get(str(product.get("ean"))) or {}
    taxonomy = taxonomy_by_sku.get(sku) or (taxonomy_by_asin.get(asin) if asin else None) or {}
    if not taxonomy and product.get("ean"):
        taxonomy = taxonomy_by_ean.get(str(product.get("ean"))) or {}
    listing = listing_by_market_sku.get((marketplace_code, sku)) or (listing_by_market_asin.get((marketplace_code, asin)) if asin else None) or {}
    sales = sales_map.get((marketplace_id, sku), {})
    traffic = traffic_map.get((marketplace_id, sku)) or traffic_map.get((marketplace_id, asin or "")) or {}
    traffic_7d = traffic.get("7d", {})
    traffic_30d = traffic.get("30d", {})

    brand = registry.get("brand") or product.get("brand") or taxonomy.get("brand")
    category = registry.get("category_1") or product.get("category") or taxonomy.get("category")
    product_type = registry.get("category_2") or product.get("subcategory") or taxonomy.get("product_type")
    internal_sku = registry.get("internal_sku") or product.get("internal_sku")
    ean = registry.get("ean") or product.get("ean")
    parent_asin = registry.get("parent_asin") or product.get("parent_asin")
    title_preferred = registry.get("product_name") or product.get("title") or sku
    local_parent_asin = listing.get("current_parent_asin") or parent_asin
    local_theme = listing.get("variation_theme")
    listing_status = "active" if (listing or registry or product) else "inactive"
    family_health = "ok" if (local_parent_asin and local_theme) else ("missing_theme" if local_parent_asin else "missing_parent")
    global_family_status = "ok" if ((asin and asin in global_family_asins) or (ean and ean in global_family_eans)) else "missing"

    on_hand = _safe_int(row.get("on_hand"))
    inbound = _safe_int(row.get("inbound"))
    reserved = _safe_int(row.get("reserved"))
    available = max(0, on_hand - reserved)
    units_7d = _safe_float(sales.get("units_7d"))
    units_30d = _safe_float(sales.get("units_30d"))
    velocity_7d_units = round(units_7d / 7.0, 2) if units_7d > 0 else 0.0
    velocity_30d_units = round(units_30d / 30.0, 2) if units_30d > 0 else 0.0
    days_cover = round(available / velocity_30d_units, 1) if velocity_30d_units > 0 else None

    thresholds = settings.get("thresholds", {})
    stockout_days_critical = int(thresholds.get("stockout_days_critical", 7))
    stockout_days_warning = int(thresholds.get("stockout_days_warning", 14))
    overstock_days = int(thresholds.get("overstock_days", 90))
    high_sessions_threshold = int(thresholds.get("high_sessions_threshold", 100))
    high_units_threshold = int(thresholds.get("high_units_threshold", 5))
    cvr_crash_pct = float(thresholds.get("cvr_crash_pct", -25.0))
    traffic_drop_pct = float(thresholds.get("traffic_drop_pct", -30.0))

    if days_cover is None:
        stockout_risk = "unknown"
        overstock_risk = "unknown"
    else:
        stockout_risk = "critical" if days_cover < stockout_days_critical else ("warning" if days_cover < stockout_days_warning else "ok")
        overstock_risk = "warning" if days_cover > overstock_days else "ok"

    sessions_7d = traffic_7d.get("sessions")
    sessions_30d = traffic_30d.get("sessions")
    page_views_7d = traffic_7d.get("page_views")
    page_views_30d = traffic_30d.get("page_views")
    orders_7d = _safe_int(traffic_7d.get("orders_count")) if traffic_7d.get("orders_count") is not None else _safe_int(sales.get("orders_7d"))
    units_ordered_7d = _safe_int(traffic_7d.get("units")) if traffic_7d.get("units") is not None else _safe_int(units_7d)
    unit_session_pct_7d = traffic_7d.get("unit_session_pct")
    unit_session_pct_30d = traffic_30d.get("unit_session_pct")
    sessions_delta_pct = traffic_7d.get("sessions_delta_pct")
    cvr_delta_pct = traffic_7d.get("cvr_delta_pct")
    traffic_coverage_flag = sessions_7d is None or sessions_30d is None

    if listing_status == "suppressed" and (sessions_7d or 0) >= high_sessions_threshold:
        demand_badge = "Fix suppression NOW"
    elif not traffic_coverage_flag and (sessions_7d or 0) >= high_sessions_threshold and days_cover is not None and days_cover < stockout_days_warning:
        demand_badge = "Replenish NOW"
    elif not traffic_coverage_flag and sessions_delta_pct is not None and cvr_delta_pct is not None and abs(sessions_delta_pct) <= 15 and cvr_delta_pct <= cvr_crash_pct:
        demand_badge = "Listing/Price issue"
    elif not traffic_coverage_flag and sessions_delta_pct is not None and sessions_delta_pct <= traffic_drop_pct and available > 0:
        demand_badge = "Traffic issue"
    elif traffic_coverage_flag and days_cover is not None and days_cover < stockout_days_critical and units_ordered_7d >= high_units_threshold:
        demand_badge = "Replenish NOW (partial)"
    else:
        demand_badge = "Traffic missing" if traffic_coverage_flag else "OK"

    price_netto = _safe_float(product.get("netto_purchase_price_pln"), default=0.0)
    stranded_units = _safe_int(row.get("stranded_units"))
    aged_90_plus_units = _safe_int(row.get("aged_90_plus"))
    return {
        "sku": sku,
        "asin": asin,
        "marketplace_id": marketplace_id,
        "marketplace_code": marketplace_code,
        "title_preferred": title_preferred,
        "brand": brand,
        "category": category,
        "product_type": product_type,
        "fulfillment_badge": "FBA",
        "listing_status": listing_status,
        "suppression_reason": None,
        "local_parent_asin": local_parent_asin,
        "local_theme": local_theme,
        "family_health": family_health,
        "global_family_status": global_family_status,
        "fba_on_hand": on_hand,
        "fba_available": available,
        "inbound": inbound,
        "reserved": reserved,
        "fbm_on_hand": 0,
        "velocity_7d_units": velocity_7d_units,
        "velocity_30d_units": velocity_30d_units,
        "days_cover": days_cover,
        "stockout_risk_badge": stockout_risk,
        "overstock_risk_badge": overstock_risk,
        "stranded_units": stranded_units,
        "stranded_value_pln": _estimate_value(stranded_units, price_netto),
        "aged_90_plus_units": aged_90_plus_units,
        "aged_90_plus_value_pln": _estimate_value(aged_90_plus_units, price_netto),
        "sessions_7d": sessions_7d,
        "sessions_30d": sessions_30d,
        "page_views_7d": page_views_7d,
        "page_views_30d": page_views_30d,
        "orders_7d": orders_7d,
        "units_ordered_7d": units_ordered_7d,
        "unit_session_pct_7d": unit_session_pct_7d,
        "unit_session_pct_30d": unit_session_pct_30d,
        "sessions_delta_pct": sessions_delta_pct,
        "cvr_delta_pct": cvr_delta_pct,
        "demand_vs_supply_badge": demand_badge,
        "traffic_coverage_flag": traffic_coverage_flag,
        "inventory_freshness": row.get("snapshot_date"),
        "last_change_at": listing.get("updated_at") or registry.get("updated_at"),
        "notes_indicator": False,
        "internal_sku": internal_sku,
        "ean": ean,
        "parent_asin": parent_asin,
    }


def _load_inventory_items(
    *,
    marketplace_ids: list[str] | None = None,
    search_terms: list[str] | None = None,
) -> tuple[date | None, dict[str, Any], list[dict[str, Any]]]:
    ensure_manage_inventory_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        settings, _ = _load_settings(cur)
        search = [s.strip() for s in (search_terms or []) if s and s.strip()]
        snapshot_date, base_rows = _fetch_inventory_base_rows(
            cur,
            marketplace_ids=marketplace_ids,
            search_terms=search,
        )
        if not base_rows:
            return None, settings, []

        skus = [str(row.get("sku") or "") for row in base_rows if row.get("sku")]
        asins = [str(row.get("asin") or "") for row in base_rows if row.get("asin")]
        mp_ids = [str(row.get("marketplace_id") or "") for row in base_rows if row.get("marketplace_id")]
        mp_codes = [_marketplace_code(mp_id) for mp_id in mp_ids if mp_id]

        try:
            product_by_sku, product_by_asin = _fetch_product_maps(cur, skus, asins)
        except Exception:
            product_by_sku, product_by_asin = {}, {}
        eans = [str(item.get("ean")) for item in product_by_sku.values() if item.get("ean")]
        try:
            registry_by_sku, registry_by_asin, registry_by_ean = _fetch_registry_maps(cur, skus, asins, eans)
        except Exception:
            registry_by_sku, registry_by_asin, registry_by_ean = {}, {}, {}
        try:
            listing_by_market_sku, listing_by_market_asin = _fetch_listing_maps(cur, skus, asins, mp_codes)
        except Exception:
            listing_by_market_sku, listing_by_market_asin = {}, {}
        try:
            global_family_asins, global_family_eans = _fetch_global_family_maps(
                cur,
                asins,
                eans + [str(item.get("ean")) for item in registry_by_sku.values() if item.get("ean")],
            )
        except Exception:
            global_family_asins, global_family_eans = set(), set()
        try:
            sales_map = _fetch_sales_aggregates(cur, mp_ids, skus)
        except Exception:
            sales_map = {}
        try:
            traffic_map = _fetch_traffic_rollups(cur, mp_ids, skus, asins)
        except Exception:
            traffic_map = {}
        try:
            from app.services.taxonomy import ensure_taxonomy_schema, load_taxonomy_lookup

            ensure_taxonomy_schema()
            taxonomy_by_sku, taxonomy_by_asin, taxonomy_by_ean = load_taxonomy_lookup(
                cur,
                skus=skus,
                asins=asins,
                eans=eans + [str(item.get("ean")) for item in registry_by_sku.values() if item.get("ean")],
                min_confidence=0.75,
            )
        except Exception:
            taxonomy_by_sku, taxonomy_by_asin, taxonomy_by_ean = {}, {}, {}

        items = [
            _build_item(
                row,
                product_by_sku=product_by_sku,
                product_by_asin=product_by_asin,
                registry_by_sku=registry_by_sku,
                registry_by_asin=registry_by_asin,
                registry_by_ean=registry_by_ean,
                listing_by_market_sku=listing_by_market_sku,
                listing_by_market_asin=listing_by_market_asin,
                global_family_asins=global_family_asins,
                global_family_eans=global_family_eans,
                sales_map=sales_map,
                traffic_map=traffic_map,
                taxonomy_by_sku=taxonomy_by_sku,
                taxonomy_by_asin=taxonomy_by_asin,
                taxonomy_by_ean=taxonomy_by_ean,
                settings=settings,
            )
            for row in base_rows
        ]
        return snapshot_date, settings, items
    finally:
        conn.close()


def _rebuild_inventory_item_cache(*, marketplace_ids: list[str] | None = None) -> dict[str, Any]:
    snapshot_date, settings, items = _load_inventory_items(marketplace_ids=marketplace_ids)
    conn = _connect()
    try:
        cur = conn.cursor()
        if marketplace_ids:
            _seed_temp_values(cur, "#tmp_inv_cache_markets", "marketplace_id", "NVARCHAR(32)", marketplace_ids)
            cur.execute("DELETE FROM dbo.acc_inv_item_cache WHERE marketplace_id IN (SELECT marketplace_id FROM #tmp_inv_cache_markets)")
        else:
            cur.execute("DELETE FROM dbo.acc_inv_item_cache")
        rows = [
            (
                str(item.get("marketplace_id") or ""),
                str(item.get("sku") or ""),
                item.get("asin"),
                snapshot_date,
                item.get("title_preferred"),
                item.get("listing_status"),
                item.get("stockout_risk_badge"),
                item.get("overstock_risk_badge"),
                item.get("family_health"),
                item.get("global_family_status"),
                item.get("days_cover"),
                item.get("sessions_7d"),
                item.get("orders_7d"),
                item.get("units_ordered_7d"),
                item.get("unit_session_pct_7d"),
                item.get("cvr_delta_pct"),
                item.get("sessions_delta_pct"),
                1 if item.get("traffic_coverage_flag") else 0,
                item.get("internal_sku"),
                item.get("ean"),
                json.dumps(item, ensure_ascii=False, default=str),
            )
            for item in items
            if item.get("marketplace_id") and item.get("sku")
        ]
        if rows:
            if hasattr(cur, "fast_executemany"):
                cur.fast_executemany = True
            cur.executemany(
                """
                INSERT INTO dbo.acc_inv_item_cache (
                    marketplace_id, sku, asin, snapshot_date, title_preferred, listing_status,
                    stockout_risk_badge, overstock_risk_badge, family_health, global_family_status,
                    days_cover, sessions_7d, orders_7d, units_ordered_7d, unit_session_pct_7d,
                    cvr_delta_pct, sessions_delta_pct, traffic_coverage_flag, internal_sku, ean,
                    payload_json, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME())
                """,
                rows,
            )
            if hasattr(cur, "fast_executemany"):
                cur.fast_executemany = False
        conn.commit()
        return {
            "status": "ok",
            "rows": len(rows),
            "snapshot_date": snapshot_date,
            "traffic_partial": sum(1 for item in items if item.get("traffic_coverage_flag")),
            "settings_updated_at": settings.get("updated_at"),
        }
    finally:
        conn.close()


def _load_cached_inventory_items(
    *,
    marketplace_ids: list[str] | None = None,
    search_terms: list[str] | None = None,
) -> tuple[date | None, dict[str, Any], list[dict[str, Any]]]:
    ensure_manage_inventory_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        settings, _ = _load_settings(cur)
        cur.execute("SELECT TOP 1 1 FROM dbo.acc_inv_item_cache WITH (NOLOCK)")
        if not cur.fetchone():
            return _load_inventory_items(marketplace_ids=marketplace_ids, search_terms=search_terms)
        cur.execute("SELECT MAX(snapshot_date) FROM dbo.acc_fba_inventory_snapshot WITH (NOLOCK)")
        latest_snapshot = cur.fetchone()[0]
        if latest_snapshot is None:
            return None, settings, []
        if marketplace_ids:
            _seed_temp_values(cur, "#tmp_inv_cache_guard_markets", "marketplace_id", "NVARCHAR(32)", marketplace_ids)
            cur.execute(
                """
                SELECT COUNT(*)
                FROM dbo.acc_fba_inventory_snapshot WITH (NOLOCK)
                WHERE snapshot_date = ?
                  AND marketplace_id IN (SELECT marketplace_id FROM #tmp_inv_cache_guard_markets)
                """,
                (latest_snapshot,),
            )
            expected_count = _safe_int(cur.fetchone()[0])
            cur.execute(
                """
                SELECT COUNT(*)
                FROM dbo.acc_inv_item_cache WITH (NOLOCK)
                WHERE snapshot_date = ?
                  AND marketplace_id IN (SELECT marketplace_id FROM #tmp_inv_cache_guard_markets)
                """,
                (latest_snapshot,),
            )
            cache_count = _safe_int(cur.fetchone()[0])
        else:
            cur.execute("SELECT COUNT(*) FROM dbo.acc_fba_inventory_snapshot WITH (NOLOCK) WHERE snapshot_date = ?", (latest_snapshot,))
            expected_count = _safe_int(cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM dbo.acc_inv_item_cache WITH (NOLOCK) WHERE snapshot_date = ?", (latest_snapshot,))
            cache_count = _safe_int(cur.fetchone()[0])
        if expected_count <= 0 or cache_count < expected_count:
            # Cache is stale – if we have *any* cached rows, serve them instead
            # of falling back to the very slow _load_inventory_items() path.
            # The scheduler will rebuild the cache in the background.
            if cache_count > 0:
                log.warning(
                    "inventory_cache.stale_but_serving cache=%d expected=%d snapshot=%s",
                    cache_count, expected_count, latest_snapshot,
                )
                # fall through to read from cache below
            else:
                return _load_inventory_items(marketplace_ids=marketplace_ids, search_terms=search_terms)

        params: list[Any] = []
        where_parts = ["1 = 1"]
        if marketplace_ids:
            _seed_temp_values(cur, "#tmp_inv_cache_filter_markets", "marketplace_id", "NVARCHAR(32)", marketplace_ids)
            where_parts.append("marketplace_id IN (SELECT marketplace_id FROM #tmp_inv_cache_filter_markets)")
        normalized = sorted({term.strip().upper() for term in (search_terms or []) if term and term.strip()})
        if normalized:
            _seed_temp_values(cur, "#tmp_inv_cache_search", "term", "NVARCHAR(120)", normalized)
            where_parts.append(
                "(UPPER(ISNULL(sku, '')) IN (SELECT term FROM #tmp_inv_cache_search) "
                "OR UPPER(ISNULL(asin, '')) IN (SELECT term FROM #tmp_inv_cache_search) "
                "OR UPPER(ISNULL(ean, '')) IN (SELECT term FROM #tmp_inv_cache_search))"
            )
        cur.execute(
            f"""
            SELECT snapshot_date, payload_json
            FROM dbo.acc_inv_item_cache WITH (NOLOCK)
            WHERE {" AND ".join(where_parts)}
            """,
            tuple(params),
        )
        snapshot_date = None
        items: list[dict[str, Any]] = []
        for row in cur.fetchall():
            if row[0] and snapshot_date is None:
                snapshot_date = row[0]
            payload = _json_loads(row[1], {})
            if isinstance(payload, dict):
                items.append(payload)
        return snapshot_date, settings, items
    finally:
        conn.close()


def _coverage_from_items(snapshot_date: date | None, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    total = len(items)
    if total == 0:
        return [
            {"key": "inventory", "label": "Inventory truth", "pct": 0.0, "status": "critical", "note": "Brak snapshotu inventory"},
            {"key": "traffic", "label": "Traffic truth", "pct": 0.0, "status": "critical", "note": "Brak traffic rows"},
            {"key": "listing", "label": "Listing truth", "pct": 0.0, "status": "critical", "note": "Brak listing context"},
            {"key": "family", "label": "Family truth", "pct": 0.0, "status": "critical", "note": "Brak family context"},
        ]

    inventory_pct = 100.0 if snapshot_date is not None else 0.0
    traffic_pct = round(sum(1 for item in items if not item["traffic_coverage_flag"]) * 100.0 / total, 2)
    listing_pct = round(sum(1 for item in items if item["listing_status"] != "inactive") * 100.0 / total, 2)
    family_pct = round(sum(1 for item in items if item["global_family_status"] == "ok") * 100.0 / total, 2)
    return [
        {
            "key": "inventory",
            "label": "Inventory truth",
            "pct": inventory_pct,
            "status": _status_from_pct(inventory_pct, good=99.0, warn=70.0),
            "note": f"Latest snapshot {snapshot_date}" if snapshot_date else "Brak snapshotu inventory",
        },
        {
            "key": "traffic",
            "label": "Traffic truth",
            "pct": traffic_pct,
            "status": _status_from_pct(traffic_pct, good=90.0, warn=40.0),
            "note": "Sessions / CVR sa partial dopoki nie ma pelnego traffic feedu",
        },
        {
            "key": "listing",
            "label": "Listing truth",
            "pct": listing_pct,
            "status": _status_from_pct(listing_pct, good=95.0, warn=60.0),
            "note": "Listing status korzysta z registry + marketplace listing context",
        },
        {
            "key": "family",
            "label": "Family truth",
            "pct": family_pct,
            "status": _status_from_pct(family_pct, good=90.0, warn=50.0),
            "note": "Global family jest read-only i opiera sie na Family Mapper",
        },
    ]


def get_inventory_overview(*, marketplace_ids: list[str] | None = None) -> dict[str, Any]:
    cache_key = _inv_cache_key(marketplace_ids)
    cached = _inv_overview_cache_get(cache_key)
    if cached is not None:
        return cached
    snapshot_date, _settings, items = _load_cached_inventory_items(marketplace_ids=marketplace_ids)
    coverage = _coverage_from_items(snapshot_date, items)
    sessions_7d_total = sum(item["sessions_7d"] or 0 for item in items)
    orders_7d_total = sum(item["orders_7d"] for item in items)
    units_7d_total = sum(item["units_ordered_7d"] for item in items)
    buyable = sum(1 for item in items if item["fba_available"] > 0)
    stockout_7 = sum(1 for item in items if item["days_cover"] is not None and item["days_cover"] < 7)
    stockout_14 = sum(1 for item in items if item["days_cover"] is not None and item["days_cover"] < 14)
    stranded_value = round(sum(item["stranded_value_pln"] for item in items), 2)
    aged_value = round(sum(item["aged_90_plus_value_pln"] for item in items), 2)
    cvr_values = [item["unit_session_pct_7d"] for item in items if item["unit_session_pct_7d"] is not None]
    cvr_avg = round(sum(cvr_values) / len(cvr_values), 4) if cvr_values else None

    metrics = [
        {"label": "Buyable SKUs", "value": buyable, "status": "ok"},
        {"label": "Stockout risk <7d", "value": stockout_7, "status": "critical" if stockout_7 else "ok"},
        {"label": "Stockout risk <14d", "value": stockout_14, "status": "warning" if stockout_14 else "ok"},
        {"label": "Stranded value", "value": stranded_value, "unit": "PLN", "status": "warning" if stranded_value > 0 else "ok"},
        {"label": "Aged 90+ value", "value": aged_value, "unit": "PLN", "status": "warning" if aged_value > 0 else "ok"},
        {"label": "Sessions 7d", "value": sessions_7d_total, "status": "ok" if sessions_7d_total > 0 else "warning"},
        {"label": "Orders 7d", "value": orders_7d_total, "status": "ok"},
        {"label": "Units 7d", "value": units_7d_total, "status": "ok"},
        {"label": "Unit Session % 7d", "value": cvr_avg or 0.0, "unit": "ratio", "status": "ok" if cvr_avg is not None else "warning"},
    ]

    grouped_high_demand: dict[tuple[str, str], dict[str, Any]] = {}
    for item in items:
        if _is_amzn_grade_sku(item.get("sku")):
            continue
        group_key = _overview_group_key(item)
        if not group_key:
            continue
        mp_id = str(item.get("marketplace_id") or "")
        agg_key = (mp_id, group_key)
        sessions = _safe_int(item.get("sessions_7d"), 0)
        orders = _safe_int(item.get("orders_7d"), 0)
        units = _safe_int(item.get("units_ordered_7d"), 0)
        fba_available = _safe_int(item.get("fba_available"), 0)
        velocity_30d = _safe_float(item.get("velocity_30d_units"), 0.0)
        if agg_key not in grouped_high_demand:
            base = dict(item)
            base["sku"] = group_key
            base["parent_asin"] = group_key
            base["local_parent_asin"] = group_key
            base["sessions_7d"] = 0
            base["orders_7d"] = 0
            base["units_ordered_7d"] = 0
            base["fba_available"] = 0
            base["velocity_30d_units"] = 0.0
            grouped_high_demand[agg_key] = base
        grouped = grouped_high_demand[agg_key]
        grouped["sessions_7d"] = _safe_int(grouped.get("sessions_7d"), 0) + sessions
        grouped["orders_7d"] = _safe_int(grouped.get("orders_7d"), 0) + orders
        grouped["units_ordered_7d"] = _safe_int(grouped.get("units_ordered_7d"), 0) + units
        grouped["fba_available"] = _safe_int(grouped.get("fba_available"), 0) + fba_available
        grouped["velocity_30d_units"] = _safe_float(grouped.get("velocity_30d_units"), 0.0) + velocity_30d

    top_high_demand_low_supply: list[dict[str, Any]] = []
    for grouped in grouped_high_demand.values():
        demand_score = _safe_int(grouped.get("sessions_7d"), 0) or _safe_int(grouped.get("orders_7d"), 0)
        velocity_30d = _safe_float(grouped.get("velocity_30d_units"), 0.0)
        fba_available = _safe_int(grouped.get("fba_available"), 0)
        if velocity_30d > 0:
            grouped["days_cover"] = round(fba_available / velocity_30d, 1)
        else:
            grouped["days_cover"] = None
        if demand_score > 0 and grouped["days_cover"] is not None and grouped["days_cover"] < 14:
            top_high_demand_low_supply.append(grouped)
    top_high_demand_low_supply.sort(
        key=lambda item: (
            -(_safe_int(item.get("sessions_7d"), 0) or _safe_int(item.get("orders_7d"), 0)),
            item.get("days_cover") if item.get("days_cover") is not None else 9999,
        )
    )

    top_cvr_crash = [
        item for item in items
        if not _is_amzn_grade_sku(item.get("sku")) and item["cvr_delta_pct"] is not None and item["cvr_delta_pct"] <= -25.0
    ]
    top_cvr_crash.sort(key=lambda item: item["cvr_delta_pct"])

    top_suppressed_high_sessions = [
        item for item in items
        if not _is_amzn_grade_sku(item.get("sku")) and item["listing_status"] == "suppressed" and (item["sessions_7d"] or 0) > 0
    ]
    top_suppressed_high_sessions.sort(key=lambda item: -(item["sessions_7d"] or 0))

    recent_families = _list_recent_inventory_family_changes(limit=20)
    result = {
        "metrics": metrics,
        "coverage": coverage,
        "top_high_demand_low_supply": top_high_demand_low_supply[:20],
        "top_cvr_crash": top_cvr_crash[:20],
        "top_suppressed_high_sessions": top_suppressed_high_sessions[:20],
        "recently_changed_families": recent_families,
        "generated_at": _utcnow(),
    }
    _inv_overview_cache_set(cache_key, result, ttl_sec=180)
    return result


def list_manage_inventory(
    *,
    marketplace_ids: list[str] | None = None,
    search: str | None = None,
    risk_type: str | None = None,
    listing_status: str | None = None,
) -> dict[str, Any]:
    terms = [part.strip() for part in str(search or "").replace("\r", "\n").split("\n") if part.strip()]
    snapshot_date, _settings, items = _load_cached_inventory_items(marketplace_ids=marketplace_ids, search_terms=terms)

    if listing_status and listing_status != "all":
        items = [item for item in items if item["listing_status"] == listing_status]

    if risk_type and risk_type != "all":
        def _matches(item: dict[str, Any]) -> bool:
            if risk_type == "stockout":
                return item["stockout_risk_badge"] in {"critical", "warning"}
            if risk_type == "overstock":
                return item["overstock_risk_badge"] == "warning"
            if risk_type == "stranded":
                return item["stranded_units"] > 0
            if risk_type == "aged":
                return item["aged_90_plus_units"] > 0
            if risk_type == "broken_family":
                return item["family_health"] not in {"ok", "unknown"}
            if risk_type == "missing_attrs":
                return item["family_health"] == "missing_theme"
            if risk_type == "cvr_crash":
                return item["cvr_delta_pct"] is not None and item["cvr_delta_pct"] <= -25.0
            if risk_type == "traffic_drop":
                return item["sessions_delta_pct"] is not None and item["sessions_delta_pct"] <= -30.0
            return True
        items = [item for item in items if _matches(item)]

    items.sort(key=lambda item: ((item["days_cover"] if item["days_cover"] is not None else 9999), -(item["sessions_7d"] or item["orders_7d"] or 0)))
    return {
        "items": items,
        "total": len(items),
        "snapshot_date": snapshot_date,
        "coverage": _coverage_from_items(snapshot_date, items),
    }


def get_inventory_sku_detail(*, sku: str, marketplace_id: str | None = None) -> dict[str, Any]:
    result = list_manage_inventory(marketplace_ids=[marketplace_id] if marketplace_id else None, search=sku)
    item = next((row for row in result["items"] if row["sku"] == sku and (marketplace_id is None or row["marketplace_id"] == marketplace_id)), None)
    if not item:
        raise ValueError("SKU not found")

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT snapshot_date, on_hand, inbound, reserved
            FROM dbo.acc_fba_inventory_snapshot WITH (NOLOCK)
            WHERE sku = ?
              AND (? IS NULL OR marketplace_id = ?)
            ORDER BY snapshot_date DESC
            """,
            (sku, marketplace_id, marketplace_id),
        )
        inventory_timeline = [
            {
                "date": row[0],
                "on_hand": _safe_int(row[1]),
                "available": max(0, _safe_int(row[1]) - _safe_int(row[3])),
                "inbound": _safe_int(row[2]),
            }
            for row in cur.fetchall()[:30]
        ]

        cur.execute(
            """
            SELECT
                CAST(o.purchase_date AS date) AS day,
                SUM(ISNULL(NULLIF(ol.quantity_shipped, 0), ol.quantity_ordered)) AS units,
                COUNT(DISTINCT o.amazon_order_id) AS orders
            FROM dbo.acc_order o WITH (NOLOCK)
            JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
            WHERE ol.sku = ?
              AND (? IS NULL OR o.marketplace_id = ?)
              AND CAST(o.purchase_date AS date) >= ?
              AND o.status NOT IN ('Canceled', 'Cancelled')
            GROUP BY CAST(o.purchase_date AS date)
            ORDER BY day DESC
            """,
            (sku, marketplace_id, marketplace_id, date.today() - timedelta(days=90)),
        )
        traffic_timeline = [
            {
                "date": row[0],
                "units": _safe_int(row[1]),
                "orders": _safe_int(row[2]),
            }
            for row in cur.fetchall()
        ]

        cur.execute(
            """
            SELECT TOP 50
                event_type, actor, payload_json, created_at
            FROM dbo.acc_inv_change_event WITH (NOLOCK)
            WHERE draft_id IN (
                SELECT id FROM dbo.acc_inv_change_draft WITH (NOLOCK) WHERE affected_sku = ?
            )
            ORDER BY created_at DESC
            """,
            (sku,),
        )
        change_history = [
            {
                "event_type": row[0],
                "actor": row[1],
                "payload_json": _json_loads(row[2], {}),
                "created_at": row[3],
            }
            for row in cur.fetchall()
        ]
    finally:
        conn.close()

    issues: list[str] = []
    if item["traffic_coverage_flag"]:
        issues.append("Traffic coverage partial — brak Sessions/Page Views/CVR dla tego SKU.")
    if item["family_health"] != "ok":
        issues.append(f"Family health: {item['family_health']}")
    if item["stockout_risk_badge"] in {"critical", "warning"}:
        issues.append(f"Stockout risk: {item['stockout_risk_badge']}")

    family_context = {
        "local_parent_asin": item["local_parent_asin"],
        "local_theme": item["local_theme"],
        "global_family_status": item["global_family_status"],
        "parent_asin": item["parent_asin"],
    }
    return {
        "item": item,
        "inventory_timeline": inventory_timeline,
        "traffic_timeline": traffic_timeline,
        "family_context": family_context,
        "issues": issues,
        "change_history": change_history,
        "coverage": result["coverage"],
    }


def _list_recent_inventory_family_changes(limit: int = 20) -> list[dict[str, Any]]:
    conn = _connect()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT TOP {int(limit)}
                    gfml.marketplace,
                    COALESCE(gfml.target_parent_asin, gf.de_parent_asin) AS parent_asin,
                    cc.children_count,
                    gf.variation_theme_de,
                    CAST(ISNULL(gfml.confidence_avg, 0) AS FLOAT) AS confidence_avg,
                    ISNULL(gfml.status, 'needs_review') AS status,
                    gfml.updated_at
                FROM dbo.global_family_market_link gfml WITH (NOLOCK)
                JOIN dbo.global_family gf WITH (NOLOCK) ON gf.id = gfml.global_family_id
                OUTER APPLY (
                    SELECT COUNT(*) AS children_count
                    FROM dbo.global_family_child gfc WITH (NOLOCK)
                    WHERE gfc.global_family_id = gf.id
                ) cc
                ORDER BY gfml.updated_at DESC
                """
            )
            return [
                {
                    "marketplace_code": row[0],
                    "parent_asin": row[1],
                    "children_count": _safe_int(row[2]),
                    "theme": row[3],
                    "coverage_vs_de_pct": float(row[4]) if row[4] is not None else None,
                    "missing_children": 0,
                    "extra_children": 0,
                    "conflicts_count": 0 if row[5] in {None, "ok", "mapped"} else 1,
                    "missing_required_attrs_count": 0,
                    "confidence_avg": float(row[4]) if row[4] is not None else None,
                    "status": row[5] or "needs_review",
                    "updated_at": row[6],
                }
                for row in cur.fetchall()
            ]
        except Exception:
            return []
    finally:
        conn.close()


def list_inventory_families(*, marketplace: str | None = None, limit: int = 200) -> dict[str, Any]:
    ensure_manage_inventory_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        try:
            params: list[Any] = []
            marketplace_where = ""
            if marketplace:
                marketplace_where = "WHERE gfml.marketplace = ?"
                params.append(marketplace)
            cur.execute(
                f"""
                SELECT TOP {int(limit)}
                    COALESCE(gfml.marketplace, 'DE') AS marketplace_code,
                    COALESCE(gfml.target_parent_asin, gf.de_parent_asin) AS parent_asin,
                    cc.children_count,
                    gf.variation_theme_de AS theme,
                    CAST(CASE WHEN cc.children_count > 0 THEN ISNULL(gfml.confidence_avg, 0) ELSE 0 END AS FLOAT) AS confidence_avg,
                    gfml.status,
                    gfml.updated_at
                FROM dbo.global_family gf WITH (NOLOCK)
                LEFT JOIN dbo.global_family_market_link gfml WITH (NOLOCK) ON gfml.global_family_id = gf.id
                OUTER APPLY (
                    SELECT COUNT(*) AS children_count
                    FROM dbo.global_family_child gfc WITH (NOLOCK)
                    WHERE gfc.global_family_id = gf.id
                ) cc
                {marketplace_where}
                ORDER BY ISNULL(gfml.updated_at, gf.created_at) DESC
                """,
                tuple(params),
            )
            items = []
            for row in cur.fetchall():
                items.append(
                    {
                        "marketplace_code": row[0],
                        "parent_asin": row[1],
                        "children_count": _safe_int(row[2]),
                        "theme": row[3],
                        "coverage_vs_de_pct": float(row[4]) if row[4] is not None else None,
                        "missing_children": 0,
                        "extra_children": 0,
                        "conflicts_count": 0 if row[5] in {None, "ok", "mapped"} else 1,
                        "missing_required_attrs_count": 0,
                        "confidence_avg": float(row[4]) if row[4] is not None else None,
                        "status": row[5] or "needs_review",
                        "updated_at": row[6],
                    }
                )
            return {"items": items, "total": len(items)}
        except Exception:
            return {"items": [], "total": 0}
    finally:
        conn.close()


def get_inventory_family_detail(*, parent_asin: str, marketplace: str | None = None) -> dict[str, Any]:
    ensure_manage_inventory_schema()
    marketplace_code = (marketplace or "DE").upper()
    conn = _connect()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT TOP 1 gf.id, gf.variation_theme_de, gf.brand, gf.category
                FROM dbo.global_family gf WITH (NOLOCK)
                WHERE gf.de_parent_asin = ?
                """,
                (parent_asin,),
            )
            family = cur.fetchone()
        except Exception:
            family = None
        if not family:
            raise ValueError("Family not found")

        cur.execute(
            """
            SELECT master_key, key_type, de_child_asin, sku_de, ean_de, attributes_json
            FROM dbo.global_family_child WITH (NOLOCK)
            WHERE global_family_id = ?
            ORDER BY de_child_asin
            """,
            (family[0],),
        )
        current_children = [
            {
                "child_asin": row[2],
                "child_sku": row[3],
                "master_key": row[0],
                "key_type": row[1],
                "variant_attributes": _json_loads(row[5], {}),
                "current_parent_asin": parent_asin if marketplace_code == "DE" else None,
                "proposed_parent_asin": parent_asin if marketplace_code == "DE" else None,
                "match_type": "canonical",
                "confidence": 100.0 if marketplace_code == "DE" else None,
                "warnings": [],
            }
            for row in cur.fetchall()
        ]

        cur.execute(
            """
            SELECT TOP 1 target_parent_asin, status, confidence_avg
            FROM dbo.global_family_market_link WITH (NOLOCK)
            WHERE global_family_id = ? AND marketplace = ?
            """,
            (family[0], marketplace_code),
        )
        mp_link = cur.fetchone()
    finally:
        conn.close()

    return {
        "marketplace_code": marketplace_code,
        "parent_asin": parent_asin,
        "theme": family[1],
        "status": mp_link[1] if mp_link else "needs_review",
        "current_children": current_children,
        "proposed_children": current_children,
        "coverage_vs_de_pct": float(mp_link[2]) if mp_link and mp_link[2] is not None else None,
        "issues": [],
    }


def _append_draft_event(cur, draft_id: str, event_type: str, actor: str | None, payload: dict[str, Any] | None = None) -> None:
    cur.execute(
        """
        INSERT INTO dbo.acc_inv_change_event (draft_id, event_type, actor, payload_json, created_at)
        VALUES (?, ?, ?, ?, SYSUTCDATETIME())
        """,
        (draft_id, event_type, actor, json.dumps(payload or {}, ensure_ascii=False)),
    )


def _get_draft(cur, draft_id: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT TOP 1
            id, draft_type, marketplace_id, affected_parent_asin, affected_sku,
            payload_json, snapshot_before_json, snapshot_after_json,
            validation_status, validation_errors_json, approval_status, apply_status,
            created_by, created_at, approved_by, approved_at, apply_started_at, applied_at, rolled_back_at
        FROM dbo.acc_inv_change_draft WITH (NOLOCK)
        WHERE id = ?
        """,
        (draft_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    mp_id = row[2]
    return {
        "id": str(row[0]),
        "draft_type": row[1],
        "marketplace_id": mp_id,
        "marketplace_code": _marketplace_code(mp_id),
        "affected_parent_asin": row[3],
        "affected_sku": row[4],
        "payload_json": _json_loads(row[5], {}),
        "snapshot_before_json": _json_loads(row[6], {}),
        "snapshot_after_json": _json_loads(row[7], {}),
        "validation_status": row[8],
        "validation_errors": _json_loads(row[9], []),
        "approval_status": row[10],
        "apply_status": row[11],
        "created_by": row[12],
        "created_at": row[13],
        "approved_by": row[14],
        "approved_at": row[15],
        "apply_started_at": row[16],
        "applied_at": row[17],
        "rolled_back_at": row[18],
    }


def _inventory_issue_locale(marketplace_id: str | None) -> str:
    code = _marketplace_code(marketplace_id or "")
    if code == "GB":
        return "en_GB"
    return "en_GB"


def _build_inventory_feed_messages(draft: dict[str, Any], *, use_snapshot_before: bool = False) -> tuple[str, list[dict[str, Any]]]:
    payload = dict(draft.get("snapshot_before_json") or {}) if use_snapshot_before else dict(draft.get("payload_json") or {})
    marketplace_id = str(payload.get("marketplace_id") or draft.get("marketplace_id") or "").strip()
    if not marketplace_id:
        raise ValueError("marketplace_id is required for apply")

    explicit_messages = payload.get("messages")
    if isinstance(explicit_messages, list) and explicit_messages:
        return marketplace_id, explicit_messages

    draft_type = str(draft.get("draft_type") or "").strip().lower()
    message_id = 1
    product_type = str(payload.get("product_type") or "").strip()
    sku = str(payload.get("child_sku") or payload.get("sku") or draft.get("affected_sku") or "").strip()
    theme = str(payload.get("variation_theme") or payload.get("theme") or "").strip()

    if draft_type == "reparent":
        parent_sku = str(payload.get("target_parent_sku") or payload.get("parent_sku") or "").strip()
        if not sku or not parent_sku or not product_type:
            raise ValueError("reparent apply requires child_sku/affected_sku, target_parent_sku and product_type")
        patches: list[dict[str, Any]] = [
            {
                "op": "replace",
                "path": "/attributes/parentage_level",
                "value": [{"value": "child", "marketplace_id": marketplace_id}],
            },
            {
                "op": "replace",
                "path": "/attributes/child_parent_sku_relationship",
                "value": [{
                    "parent_sku": parent_sku,
                    "child_relationship_type": "variation",
                    "marketplace_id": marketplace_id,
                }],
            },
        ]
        if theme:
            patches.append(
                {
                    "op": "replace",
                    "path": "/attributes/variation_theme",
                    "value": [{"name": theme, "marketplace_id": marketplace_id}],
                }
            )
        return marketplace_id, [{
            "messageId": message_id,
            "sku": sku,
            "operationType": "PATCH",
            "productType": product_type,
            "patches": patches,
        }]

    if draft_type == "update_theme":
        if not sku or not theme or not product_type:
            raise ValueError("update_theme apply requires sku, variation_theme and product_type")
        return marketplace_id, [{
            "messageId": message_id,
            "sku": sku,
            "operationType": "PATCH",
            "productType": product_type,
            "patches": [
                {
                    "op": "replace",
                    "path": "/attributes/variation_theme",
                    "value": [{"name": theme, "marketplace_id": marketplace_id}],
                }
            ],
        }]

    if draft_type == "create_parent":
        raise ValueError("create_parent requires explicit payload_json.messages with full parent attributes")
    if draft_type == "detach":
        raise ValueError("detach requires explicit payload_json.messages because safe detach depends on listing state")
    raise ValueError(f"unsupported draft_type for apply: {draft_type}")


def _submit_inventory_draft_feed(draft: dict[str, Any], *, use_snapshot_before: bool = False) -> dict[str, Any]:
    from app.connectors.amazon_sp_api.feeds import FeedsClient

    marketplace_id, messages = _build_inventory_feed_messages(draft, use_snapshot_before=use_snapshot_before)
    feed_payload = {
        "header": {
            "sellerId": settings.SP_API_SELLER_ID,
            "version": "2.0",
            "issueLocale": _inventory_issue_locale(marketplace_id),
        },
        "messages": messages,
    }

    async def _run() -> dict[str, Any]:
        client = FeedsClient(marketplace_id=marketplace_id)
        submitted = await client.submit_json_listings_feed(
            marketplace_ids=[marketplace_id],
            feed_payload=feed_payload,
        )
        feed_id = str(submitted.get("feedId") or "")
        if not feed_id:
            return {
                "apply_status": "failed",
                "marketplace_id": marketplace_id,
                "messages_count": len(messages),
                "error": "feedId missing from createFeed response",
            }
        feed_state = await client.wait_for_feed(feed_id, poll_interval=15.0, max_wait=90.0)
        processing_status = str(feed_state.get("processingStatus") or "")
        if processing_status == "DONE":
            return {
                "apply_status": "success",
                "marketplace_id": marketplace_id,
                "messages_count": len(messages),
                "feed_id": feed_id,
                "feed_document_id": submitted.get("feedDocumentId"),
                "feed_status": processing_status,
            }
        if processing_status in {"CANCELLED", "FATAL"}:
            return {
                "apply_status": "failed",
                "marketplace_id": marketplace_id,
                "messages_count": len(messages),
                "feed_id": feed_id,
                "feed_document_id": submitted.get("feedDocumentId"),
                "feed_status": processing_status,
            }
        return {
            "apply_status": "queued",
            "marketplace_id": marketplace_id,
            "messages_count": len(messages),
            "feed_id": feed_id,
            "feed_document_id": submitted.get("feedDocumentId"),
            "feed_status": processing_status or "IN_PROGRESS",
        }

    return __import__("asyncio").run(_run())


def list_inventory_draft_events(cur, draft_id: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT event_type, actor, payload_json, created_at
        FROM dbo.acc_inv_change_event WITH (NOLOCK)
        WHERE draft_id = ?
        ORDER BY created_at DESC
        """,
        (draft_id,),
    )
    return [
        {"event_type": row[0], "actor": row[1], "payload_json": _json_loads(row[2], {}), "created_at": row[3]}
        for row in cur.fetchall()
    ]


def list_inventory_drafts() -> dict[str, Any]:
    ensure_manage_inventory_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 200 id
            FROM dbo.acc_inv_change_draft WITH (NOLOCK)
            ORDER BY created_at DESC
            """
        )
        ids = [str(row[0]) for row in cur.fetchall()]
        items = [_get_draft(cur, record_id) for record_id in ids]
        return {"items": [item for item in items if item], "total": len(ids)}
    finally:
        conn.close()


def create_inventory_draft(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_manage_inventory_schema()
    draft_id = str(uuid.uuid4())
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO dbo.acc_inv_change_draft (
                id, draft_type, marketplace_id, affected_parent_asin, affected_sku,
                payload_json, snapshot_before_json, validation_status, approval_status,
                apply_status, created_by, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 'draft', 'pending', ?, SYSUTCDATETIME())
            """,
            (
                draft_id,
                payload.get("draft_type"),
                payload.get("marketplace_id"),
                payload.get("affected_parent_asin"),
                payload.get("affected_sku"),
                json.dumps(payload.get("payload_json") or {}, ensure_ascii=False),
                json.dumps(payload.get("snapshot_before_json") or {}, ensure_ascii=False),
                payload.get("created_by"),
            ),
        )
        _append_draft_event(cur, draft_id, "created", payload.get("created_by"), payload.get("payload_json") or {})
        conn.commit()
        return _get_draft(cur, draft_id) or {}
    finally:
        conn.close()


def validate_inventory_draft(draft_id: str, actor: str | None = None) -> dict[str, Any]:
    ensure_manage_inventory_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        draft = _get_draft(cur, draft_id)
        if not draft:
            raise ValueError("Draft not found")
        errors: list[str] = []
        payload = draft.get("payload_json") or {}
        if not payload:
            errors.append("payload_json is empty")
        if draft.get("draft_type") in {"reparent", "create_parent"} and not (draft.get("affected_parent_asin") or payload.get("target_parent_asin")):
            errors.append("target parent asin is missing")
        try:
            _normalize_inventory_feed_messages(draft)
        except ValueError as exc:
            errors.append(str(exc))
        status = "failed" if errors else "passed"
        cur.execute(
            """
            UPDATE dbo.acc_inv_change_draft
            SET validation_status = ?,
                validation_errors_json = ?
            WHERE id = ?
            """,
            (status, json.dumps(errors, ensure_ascii=False), draft_id),
        )
        _append_draft_event(cur, draft_id, "validated", actor, {"status": status, "errors": errors})
        conn.commit()
        events = list_inventory_draft_events(cur, draft_id)
        return {"draft": _get_draft(cur, draft_id), "events": events}
    finally:
        conn.close()


def approve_inventory_draft(draft_id: str, actor: str | None = None) -> dict[str, Any]:
    ensure_manage_inventory_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        draft = _get_draft(cur, draft_id)
        if not draft:
            raise ValueError("Draft not found")
        if draft.get("validation_status") != "passed":
            raise ValueError("Draft must pass validation before approval")
        cur.execute(
            """
            UPDATE dbo.acc_inv_change_draft
            SET approval_status = 'approved',
                approved_by = ?,
                approved_at = SYSUTCDATETIME()
            WHERE id = ?
            """,
            (actor, draft_id),
        )
        _append_draft_event(cur, draft_id, "approved", actor, {})
        conn.commit()
        return {"draft": _get_draft(cur, draft_id), "events": list_inventory_draft_events(cur, draft_id)}
    finally:
        conn.close()


def apply_inventory_draft(draft_id: str, actor: str | None = None) -> dict[str, Any]:
    ensure_manage_inventory_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        draft = _get_draft(cur, draft_id)
        if not draft:
            raise ValueError("Draft not found")
        if draft.get("approval_status") != "approved":
            raise ValueError("Draft must be approved before apply")
        apply_result = _submit_inventory_draft_feed(draft)
        apply_status = str(apply_result.get("apply_status") or "queued")
        snapshot_after = {
            "feed_result": apply_result,
            "submitted_at": _utcnow().isoformat(),
        }
        cur.execute(
            """
            UPDATE dbo.acc_inv_change_draft
            SET apply_status = ?,
                apply_started_at = ISNULL(apply_started_at, SYSUTCDATETIME()),
                applied_at = CASE WHEN ? = 'success' THEN SYSUTCDATETIME() ELSE applied_at END,
                snapshot_after_json = ?
            WHERE id = ?
            """,
            (apply_status, apply_status, json.dumps(snapshot_after, ensure_ascii=False), draft_id),
        )
        _append_draft_event(cur, draft_id, "applied", actor, snapshot_after)
        conn.commit()
        return {"draft": _get_draft(cur, draft_id), "events": list_inventory_draft_events(cur, draft_id)}
    finally:
        conn.close()


def rollback_inventory_draft(draft_id: str, actor: str | None = None) -> dict[str, Any]:
    ensure_manage_inventory_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        draft = _get_draft(cur, draft_id)
        if not draft:
            raise ValueError("Draft not found")
        rollback_result = _submit_inventory_draft_feed(draft, use_snapshot_before=True)
        apply_status = str(rollback_result.get("apply_status") or "queued")
        cur.execute(
            """
            UPDATE dbo.acc_inv_change_draft
            SET apply_status = CASE WHEN ? = 'success' THEN 'rolled_back' ELSE apply_status END,
                rolled_back_at = CASE WHEN ? = 'success' THEN SYSUTCDATETIME() ELSE rolled_back_at END,
                snapshot_after_json = ?
            WHERE id = ?
            """,
            (apply_status, apply_status, json.dumps({"rollback_feed_result": rollback_result}, ensure_ascii=False), draft_id),
        )
        _append_draft_event(cur, draft_id, "rolled_back", actor, {"restored": draft.get("snapshot_before_json") or {}, "feed_result": rollback_result})
        conn.commit()
        return {"draft": _get_draft(cur, draft_id), "events": list_inventory_draft_events(cur, draft_id)}
    finally:
        conn.close()


def get_inventory_jobs() -> dict[str, Any]:
    jobs = list_jobs(page=1, page_size=100, job_type=None, status=None)
    filtered = [
        item
        for item in jobs.get("items", [])
        if str(item.get("job_type") or "").startswith("inventory_")
    ]
    job_order = {
        "inventory_sync_listings": 0,
        "inventory_sync_snapshots": 1,
        "inventory_sync_sales_traffic": 2,
        "inventory_compute_rollups": 3,
        "inventory_run_alerts": 4,
    }
    latest_by_type: dict[str, dict[str, Any]] = {}
    for item in filtered:
        job_type = str(item.get("job_type") or "")
        if job_type and job_type not in latest_by_type:
            latest_by_type[job_type] = item
    latest_items = sorted(
        latest_by_type.values(),
        key=lambda item: job_order.get(str(item.get("job_type") or ""), 999),
    )
    return {"items": filtered[:20], "total": len(filtered), "latest_by_type": latest_items}


def run_inventory_job(job_type: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    return enqueue_job(
        job_type=job_type,
        trigger_source="manual",
        triggered_by="system",
        params=params or {},
    )


def get_inventory_settings() -> dict[str, Any]:
    ensure_manage_inventory_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        settings, updated_at = _load_settings(cur)
        settings["updated_at"] = updated_at
        return settings
    finally:
        conn.close()


def update_inventory_settings(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_manage_inventory_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        current, _ = _load_settings(cur)
        for key in ("thresholds", "theme_requirements", "apply_safety", "traffic_schedule"):
            if payload.get(key) is not None:
                current[key] = payload[key]
        if payload.get("saved_views_enabled") is not None:
            current["saved_views_enabled"] = bool(payload["saved_views_enabled"])
        cur.execute(
            """
            UPDATE dbo.acc_inv_settings
            SET value_json = ?, updated_at = SYSUTCDATETIME()
            WHERE [key] = 'default'
            """,
            (json.dumps(current, ensure_ascii=False),),
        )
        conn.commit()
        return get_inventory_settings()
    finally:
        conn.close()


def compute_inventory_rollups(
    *,
    job_id: str | None = None,
    marketplace_ids: list[str] | None = None,
) -> dict[str, Any]:
    ensure_manage_inventory_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM dbo.acc_inv_traffic_sku_daily WITH (NOLOCK))
              + (SELECT COUNT(*) FROM dbo.acc_inv_traffic_asin_daily WITH (NOLOCK))
            """
        )
        raw_count = _safe_int(cur.fetchone()[0])
        cur.execute("DELETE FROM dbo.acc_inv_traffic_rollup")
        if raw_count <= 0:
            conn.commit()
            item_cache = _rebuild_inventory_item_cache(marketplace_ids=marketplace_ids)
            return {"status": "no_source_data", "rows": 0, "item_cache_rows": item_cache.get("rows", 0)}
        cur.execute("IF OBJECT_ID('tempdb..#tmp_inv_rollup_source') IS NOT NULL DROP TABLE #tmp_inv_rollup_source")
        cur.execute(
            """
            CREATE TABLE #tmp_inv_rollup_source (
                marketplace_id NVARCHAR(32) NOT NULL,
                sku NVARCHAR(120) NULL,
                asin NVARCHAR(40) NULL,
                report_date DATE NOT NULL,
                sessions INT NULL,
                page_views INT NULL,
                units_ordered INT NULL,
                orders_count INT NULL,
                revenue DECIMAL(18,4) NULL,
                unit_session_pct DECIMAL(18,6) NULL
            )
            """
        )
        cur.execute(
            """
            INSERT INTO #tmp_inv_rollup_source (
                marketplace_id, sku, asin, report_date, sessions, page_views,
                units_ordered, orders_count, revenue, unit_session_pct
            )
            SELECT
                marketplace_id, sku, asin, report_date, sessions, page_views,
                units_ordered, orders_count, revenue, unit_session_pct
            FROM dbo.acc_inv_traffic_sku_daily WITH (NOLOCK)
            """
        )
        _seed_marketplace_code_map(cur)
        cur.execute(
            """
            INSERT INTO #tmp_inv_rollup_source (
                marketplace_id, sku, asin, report_date, sessions, page_views,
                units_ordered, orders_count, revenue, unit_session_pct
            )
            SELECT
                src.marketplace_id,
                mapped.sku,
                src.asin,
                src.report_date,
                src.sessions,
                src.page_views,
                src.units_ordered,
                src.orders_count,
                src.revenue,
                src.unit_session_pct
            FROM dbo.acc_inv_traffic_asin_daily src WITH (NOLOCK)
            LEFT JOIN #tmp_inv_marketplace_codes mpc
              ON mpc.marketplace_id = src.marketplace_id
            OUTER APPLY (
                SELECT TOP 1
                    COALESCE(mlc.sku, rg.merchant_sku, rg.merchant_sku_alt, p.sku) AS sku
                FROM (SELECT 1 AS anchor) seed
                OUTER APPLY (
                    SELECT TOP 1 mlc.sku
                    FROM dbo.marketplace_listing_child mlc WITH (NOLOCK)
                    WHERE mlc.asin = src.asin
                      AND mlc.marketplace = mpc.marketplace_code
                    ORDER BY mlc.updated_at DESC
                ) mlc
                OUTER APPLY (
                    SELECT TOP 1 merchant_sku, merchant_sku_alt
                    FROM dbo.acc_amazon_listing_registry rg WITH (NOLOCK)
                    WHERE rg.asin = src.asin
                    ORDER BY rg.updated_at DESC
                ) rg
                OUTER APPLY (
                    SELECT TOP 1 p.sku
                    FROM dbo.acc_product p WITH (NOLOCK)
                    WHERE p.asin = src.asin
                    ORDER BY p.updated_at DESC
                ) p
            ) mapped
            WHERE mapped.sku IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM #tmp_inv_rollup_source existing
                  WHERE existing.marketplace_id = src.marketplace_id
                    AND existing.sku = mapped.sku
                    AND existing.report_date = src.report_date
              )
            """
        )
        windows = [
            ("7d", 7, 14),
            ("30d", 30, 60),
        ]
        inserted = 0
        today = date.today()
        for range_key, current_days, previous_days in windows:
            current_from = today - timedelta(days=current_days)
            previous_from = today - timedelta(days=previous_days)
            previous_to = current_from
            cur.execute(
                """
                INSERT INTO dbo.acc_inv_traffic_rollup (
                    marketplace_id, sku, asin, range_key,
                    sessions, page_views, units, orders_count, revenue,
                    unit_session_pct, sessions_delta_pct, cvr_delta_pct, updated_at
                )
                SELECT
                    curr.marketplace_id,
                    curr.sku,
                    MAX(curr.asin) AS asin,
                    ? AS range_key,
                    SUM(ISNULL(curr.sessions, 0)) AS sessions,
                    SUM(ISNULL(curr.page_views, 0)) AS page_views,
                    SUM(ISNULL(curr.units_ordered, 0)) AS units,
                    SUM(ISNULL(curr.orders_count, 0)) AS orders_count,
                    SUM(ISNULL(curr.revenue, 0)) AS revenue,
                    CASE
                        WHEN NULLIF(SUM(ISNULL(curr.sessions, 0)), 0) IS NULL THEN NULL
                        ELSE CAST(SUM(ISNULL(curr.units_ordered, 0)) * 100.0 / NULLIF(SUM(ISNULL(curr.sessions, 0)), 0) AS DECIMAL(18,6))
                    END AS unit_session_pct,
                    CASE
                        WHEN NULLIF(prev.prev_sessions, 0) IS NULL THEN NULL
                        ELSE CAST((SUM(ISNULL(curr.sessions, 0)) - prev.prev_sessions) * 100.0 / NULLIF(prev.prev_sessions, 0) AS DECIMAL(18,6))
                    END AS sessions_delta_pct,
                    CASE
                        WHEN prev.prev_cvr IS NULL THEN NULL
                        ELSE CAST(
                            (
                                CASE
                                    WHEN NULLIF(SUM(ISNULL(curr.sessions, 0)), 0) IS NULL THEN NULL
                                    ELSE SUM(ISNULL(curr.units_ordered, 0)) * 100.0 / NULLIF(SUM(ISNULL(curr.sessions, 0)), 0)
                                END
                                - prev.prev_cvr
                            ) * 100.0 / NULLIF(prev.prev_cvr, 0)
                            AS DECIMAL(18,6)
                        )
                    END AS cvr_delta_pct,
                    SYSUTCDATETIME()
                FROM #tmp_inv_rollup_source curr
                LEFT JOIN (
                    SELECT
                        marketplace_id,
                        sku,
                        SUM(ISNULL(sessions, 0)) AS prev_sessions,
                        CASE
                            WHEN NULLIF(SUM(ISNULL(sessions, 0)), 0) IS NULL THEN NULL
                            ELSE SUM(ISNULL(units_ordered, 0)) * 100.0 / NULLIF(SUM(ISNULL(sessions, 0)), 0)
                        END AS prev_cvr
                    FROM #tmp_inv_rollup_source
                    WHERE report_date >= ? AND report_date < ?
                      AND sku IS NOT NULL
                    GROUP BY marketplace_id, sku
                ) prev
                    ON prev.marketplace_id = curr.marketplace_id
                   AND prev.sku = curr.sku
                WHERE curr.report_date >= ? AND curr.report_date < ?
                  AND curr.sku IS NOT NULL
                GROUP BY curr.marketplace_id, curr.sku, prev.prev_sessions, prev.prev_cvr
                """,
                (
                    range_key,
                    previous_from,
                    previous_to,
                    current_from,
                    today,
                ),
            )
            affected = _safe_int(cur.rowcount, 0)
            if affected > 0:
                inserted += affected
            cur.execute(
                """
                INSERT INTO dbo.acc_inv_traffic_rollup (
                    marketplace_id, sku, asin, range_key,
                    sessions, page_views, units, orders_count, revenue,
                    unit_session_pct, sessions_delta_pct, cvr_delta_pct, updated_at
                )
                SELECT
                    curr.marketplace_id,
                    NULL AS sku,
                    curr.asin,
                    ? AS range_key,
                    SUM(ISNULL(curr.sessions, 0)) AS sessions,
                    SUM(ISNULL(curr.page_views, 0)) AS page_views,
                    SUM(ISNULL(curr.units_ordered, 0)) AS units,
                    SUM(ISNULL(curr.orders_count, 0)) AS orders_count,
                    SUM(ISNULL(curr.revenue, 0)) AS revenue,
                    CASE
                        WHEN NULLIF(SUM(ISNULL(curr.sessions, 0)), 0) IS NULL THEN NULL
                        ELSE CAST(SUM(ISNULL(curr.units_ordered, 0)) * 100.0 / NULLIF(SUM(ISNULL(curr.sessions, 0)), 0) AS DECIMAL(18,6))
                    END AS unit_session_pct,
                    CASE
                        WHEN NULLIF(prev.prev_sessions, 0) IS NULL THEN NULL
                        ELSE CAST((SUM(ISNULL(curr.sessions, 0)) - prev.prev_sessions) * 100.0 / NULLIF(prev.prev_sessions, 0) AS DECIMAL(18,6))
                    END AS sessions_delta_pct,
                    CASE
                        WHEN prev.prev_cvr IS NULL THEN NULL
                        ELSE CAST(
                            (
                                CASE
                                    WHEN NULLIF(SUM(ISNULL(curr.sessions, 0)), 0) IS NULL THEN NULL
                                    ELSE SUM(ISNULL(curr.units_ordered, 0)) * 100.0 / NULLIF(SUM(ISNULL(curr.sessions, 0)), 0)
                                END
                                - prev.prev_cvr
                            ) * 100.0 / NULLIF(prev.prev_cvr, 0)
                            AS DECIMAL(18,6)
                        )
                    END AS cvr_delta_pct,
                    SYSUTCDATETIME()
                FROM dbo.acc_inv_traffic_asin_daily curr WITH (NOLOCK)
                LEFT JOIN (
                    SELECT
                        marketplace_id,
                        asin,
                        SUM(ISNULL(sessions, 0)) AS prev_sessions,
                        CASE
                            WHEN NULLIF(SUM(ISNULL(sessions, 0)), 0) IS NULL THEN NULL
                            ELSE SUM(ISNULL(units_ordered, 0)) * 100.0 / NULLIF(SUM(ISNULL(sessions, 0)), 0)
                        END AS prev_cvr
                    FROM dbo.acc_inv_traffic_asin_daily WITH (NOLOCK)
                    WHERE report_date >= ? AND report_date < ?
                    GROUP BY marketplace_id, asin
                ) prev
                    ON prev.marketplace_id = curr.marketplace_id
                   AND prev.asin = curr.asin
                WHERE curr.report_date >= ? AND curr.report_date < ?
                GROUP BY curr.marketplace_id, curr.asin, prev.prev_sessions, prev.prev_cvr
                """,
                (
                    range_key,
                    previous_from,
                    previous_to,
                    current_from,
                    today,
                ),
            )
            affected = _safe_int(cur.rowcount, 0)
            if affected > 0:
                inserted += affected
        if inserted <= 0:
            cur.execute("SELECT COUNT(*) FROM dbo.acc_inv_traffic_rollup WITH (NOLOCK)")
            inserted = _safe_int(cur.fetchone()[0])
        conn.commit()
        item_cache = _rebuild_inventory_item_cache(marketplace_ids=marketplace_ids)
        return {"status": "ok", "rows": inserted, "item_cache_rows": item_cache.get("rows", 0)}
    finally:
        conn.close()


def _parse_report_date(value: Any) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except Exception:
        return None


def _money_amount(payload: dict[str, Any], *keys: str) -> float:
    for key in keys:
        raw = payload.get(key)
        if isinstance(raw, dict) and raw.get("amount") is not None:
            return _safe_float(raw.get("amount"))
        if raw is not None and not isinstance(raw, (dict, list)):
            return _safe_float(raw)
    return 0.0


def _metric_value(payload: dict[str, Any], *keys: str) -> int:
    for key in keys:
        if payload.get(key) is not None:
            return _safe_int(payload.get(key))
    return 0


def _extract_sales_traffic_rows(content: str, marketplace_id: str) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    content = (content or "").strip()
    if not content:
        return [], []
    sku_rows: list[tuple[Any, ...]] = []
    asin_rows: list[tuple[Any, ...]] = []
    if content.startswith("{") or content.startswith("["):
        payload = _json_loads(content, {})
        default_report_date = None
        if isinstance(payload, dict):
            spec = payload.get("reportSpecification")
            if isinstance(spec, dict):
                default_report_date = _parse_report_date(spec.get("dataStartTime") or spec.get("dataEndTime"))
        asin_items = payload.get("salesAndTrafficByAsin") if isinstance(payload, dict) else None
        if not isinstance(asin_items, list):
            return [], []
        for item in asin_items:
            if not isinstance(item, dict):
                continue
            report_date = _parse_report_date(item.get("date") or item.get("startDate") or item.get("endDate")) or default_report_date
            if report_date is None:
                continue
            sku = str(item.get("sku") or "").strip()
            asin = str(item.get("childAsin") or item.get("asin") or item.get("parentAsin") or "").strip()
            sales_block = item.get("salesByAsin") if isinstance(item.get("salesByAsin"), dict) else {}
            traffic_block = item.get("trafficByAsin") if isinstance(item.get("trafficByAsin"), dict) else {}
            sessions = _metric_value(traffic_block, "sessions", "browserSessions", "glanceViews")
            if sessions == 0:
                sessions = _safe_int(traffic_block.get("browserSessions")) + _safe_int(traffic_block.get("mobileAppSessions"))
            page_views = _metric_value(traffic_block, "pageViews", "browserPageViews")
            if page_views == 0:
                page_views = _safe_int(traffic_block.get("browserPageViews")) + _safe_int(traffic_block.get("mobileAppPageViews"))
            units_ordered = _metric_value(sales_block, "unitsOrdered", "unitsOrderedB2B", "orderedUnits")
            orders_count = _metric_value(sales_block, "totalOrderItems", "ordersCount", "orderItemCount")
            revenue = _money_amount(sales_block, "orderedProductSales", "orderedProductSalesB2B", "revenue")
            unit_session_pct = traffic_block.get("unitSessionPercentage")
            if unit_session_pct is None and sessions > 0:
                unit_session_pct = round(units_ordered * 100.0 / sessions, 6)
            if sku:
                sku_rows.append((
                    marketplace_id,
                    sku,
                    report_date,
                    asin or None,
                    sessions,
                    page_views,
                    units_ordered,
                    orders_count,
                    revenue,
                    _safe_float(unit_session_pct) if unit_session_pct is not None else None,
                ))
            if asin:
                asin_rows.append((
                    marketplace_id,
                    asin,
                    report_date,
                    sessions,
                    page_views,
                    units_ordered,
                    orders_count,
                    revenue,
                    _safe_float(unit_session_pct) if unit_session_pct is not None else None,
                ))
        return sku_rows, asin_rows
    return [], []


def _upsert_traffic_sku_rows(cur, rows: list[tuple[Any, ...]]) -> int:
    if not rows:
        return 0
    cur.execute("IF OBJECT_ID('tempdb..#tmp_inv_traffic_sku_stage') IS NOT NULL DROP TABLE #tmp_inv_traffic_sku_stage")
    cur.execute(
        """
        CREATE TABLE #tmp_inv_traffic_sku_stage (
            marketplace_id NVARCHAR(32) NOT NULL,
            sku NVARCHAR(120) NOT NULL,
            report_date DATE NOT NULL,
            asin NVARCHAR(40) NULL,
            sessions INT NULL,
            page_views INT NULL,
            units_ordered INT NULL,
            orders_count INT NULL,
            revenue DECIMAL(18,4) NULL,
            unit_session_pct DECIMAL(18,6) NULL
        )
        """
    )
    if hasattr(cur, "fast_executemany"):
        cur.fast_executemany = True
    cur.executemany(
        """
        INSERT INTO #tmp_inv_traffic_sku_stage (
            marketplace_id, sku, report_date, asin, sessions, page_views,
            units_ordered, orders_count, revenue, unit_session_pct
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    if hasattr(cur, "fast_executemany"):
        cur.fast_executemany = False
    cur.execute(
        """
        UPDATE tgt
        SET asin = src.asin,
            sessions = src.sessions,
            page_views = src.page_views,
            units_ordered = src.units_ordered,
            orders_count = src.orders_count,
            revenue = src.revenue,
            unit_session_pct = src.unit_session_pct,
            updated_at = SYSUTCDATETIME()
        FROM dbo.acc_inv_traffic_sku_daily tgt
        JOIN #tmp_inv_traffic_sku_stage src
          ON src.marketplace_id = tgt.marketplace_id
         AND src.sku = tgt.sku
         AND src.report_date = tgt.report_date
        """
    )
    cur.execute(
        """
        INSERT INTO dbo.acc_inv_traffic_sku_daily (
            marketplace_id, sku, report_date, asin, sessions, page_views,
            units_ordered, orders_count, revenue, unit_session_pct, updated_at
        )
        SELECT src.marketplace_id, src.sku, src.report_date, src.asin, src.sessions, src.page_views,
               src.units_ordered, src.orders_count, src.revenue, src.unit_session_pct, SYSUTCDATETIME()
        FROM #tmp_inv_traffic_sku_stage src
        LEFT JOIN dbo.acc_inv_traffic_sku_daily tgt
          ON src.marketplace_id = tgt.marketplace_id
         AND src.sku = tgt.sku
         AND src.report_date = tgt.report_date
        WHERE tgt.marketplace_id IS NULL
        """
    )
    return len(rows)


def _upsert_traffic_asin_rows(cur, rows: list[tuple[Any, ...]]) -> int:
    if not rows:
        return 0
    cur.execute("IF OBJECT_ID('tempdb..#tmp_inv_traffic_asin_stage') IS NOT NULL DROP TABLE #tmp_inv_traffic_asin_stage")
    cur.execute(
        """
        CREATE TABLE #tmp_inv_traffic_asin_stage (
            marketplace_id NVARCHAR(32) NOT NULL,
            asin NVARCHAR(40) NOT NULL,
            report_date DATE NOT NULL,
            sessions INT NULL,
            page_views INT NULL,
            units_ordered INT NULL,
            orders_count INT NULL,
            revenue DECIMAL(18,4) NULL,
            unit_session_pct DECIMAL(18,6) NULL
        )
        """
    )
    if hasattr(cur, "fast_executemany"):
        cur.fast_executemany = True
    cur.executemany(
        """
        INSERT INTO #tmp_inv_traffic_asin_stage (
            marketplace_id, asin, report_date, sessions, page_views,
            units_ordered, orders_count, revenue, unit_session_pct
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    if hasattr(cur, "fast_executemany"):
        cur.fast_executemany = False
    cur.execute(
        """
        UPDATE tgt
        SET sessions = src.sessions,
            page_views = src.page_views,
            units_ordered = src.units_ordered,
            orders_count = src.orders_count,
            revenue = src.revenue,
            unit_session_pct = src.unit_session_pct,
            updated_at = SYSUTCDATETIME()
        FROM dbo.acc_inv_traffic_asin_daily tgt
        JOIN #tmp_inv_traffic_asin_stage src
          ON src.marketplace_id = tgt.marketplace_id
         AND src.asin = tgt.asin
         AND src.report_date = tgt.report_date
        """
    )
    cur.execute(
        """
        INSERT INTO dbo.acc_inv_traffic_asin_daily (
            marketplace_id, asin, report_date, sessions, page_views,
            units_ordered, orders_count, revenue, unit_session_pct, updated_at
        )
        SELECT src.marketplace_id, src.asin, src.report_date, src.sessions, src.page_views,
               src.units_ordered, src.orders_count, src.revenue, src.unit_session_pct, SYSUTCDATETIME()
        FROM #tmp_inv_traffic_asin_stage src
        LEFT JOIN dbo.acc_inv_traffic_asin_daily tgt
          ON src.marketplace_id = tgt.marketplace_id
         AND src.asin = tgt.asin
         AND src.report_date = tgt.report_date
        WHERE tgt.marketplace_id IS NULL
        """
    )
    return len(rows)


def sync_inventory_sales_traffic(
    *,
    days_back: int = 90,
    job_id: str | None = None,
    marketplace_ids: list[str] | None = None,
) -> dict[str, Any]:
    ensure_manage_inventory_schema()
    from app.connectors.amazon_sp_api.reports import ReportType, ReportsClient
    from app.connectors.mssql.mssql_store import set_job_progress

    async def _run() -> dict[str, Any]:
        wanted_marketplace_ids = marketplace_ids or list(MARKETPLACE_REGISTRY.keys())
        total_sku_rows = 0
        total_asin_rows = 0
        per_marketplace: list[dict[str, Any]] = []
        end_dt = datetime.now(timezone.utc) - timedelta(hours=6)
        start_dt = end_dt - timedelta(days=max(1, int(days_back)))
        conn = _connect()
        try:
            cur = conn.cursor()
            for idx, marketplace_id in enumerate(wanted_marketplace_ids, start=1):
                if job_id:
                    set_job_progress(
                        job_id,
                        progress_pct=min(95, int((idx - 1) * 100 / max(len(wanted_marketplace_ids), 1))),
                        message=f"Traffic report {idx}/{len(wanted_marketplace_ids)} {_marketplace_code(marketplace_id)}",
                        records_processed=total_sku_rows + total_asin_rows,
                    )
                client = ReportsClient(marketplace_id=marketplace_id)
                try:
                    content = await client.request_and_download_reuse_recent(
                        report_type=ReportType.SALES_TRAFFIC_BUSINESS,
                        marketplace_ids=[marketplace_id],
                        max_age_minutes=240,
                        data_start_time=start_dt,
                        data_end_time=end_dt,
                        report_options={
                            "dateGranularity": "DAY",
                            "asinGranularity": "CHILD",
                        },
                        poll_interval=20.0,
                    )
                    sku_rows, asin_rows = _extract_sales_traffic_rows(content, marketplace_id)
                    total_sku_rows += _upsert_traffic_sku_rows(cur, sku_rows)
                    total_asin_rows += _upsert_traffic_asin_rows(cur, asin_rows)
                    conn.commit()
                    per_marketplace.append(
                        {
                            "marketplace_id": marketplace_id,
                            "marketplace_code": _marketplace_code(marketplace_id),
                            "sku_rows": len(sku_rows),
                            "asin_rows": len(asin_rows),
                            "status": "ok",
                        }
                    )
                except Exception as exc:
                    conn.rollback()
                    per_marketplace.append(
                        {
                            "marketplace_id": marketplace_id,
                            "marketplace_code": _marketplace_code(marketplace_id),
                            "sku_rows": 0,
                            "asin_rows": 0,
                            "status": "failed",
                            "error": str(exc),
                        }
                    )
            rollup_result = compute_inventory_rollups(job_id=job_id, marketplace_ids=wanted_marketplace_ids)
            return {
                "status": "ok",
                "rows": total_sku_rows,
                "asin_rows": total_asin_rows,
                "days_back": int(days_back),
                "marketplaces": per_marketplace,
                "rollups": rollup_result,
            }
        finally:
            conn.close()

    return __import__("asyncio").run(_run())


def evaluate_inventory_alerts() -> dict[str, Any]:
    overview = get_inventory_overview()
    candidates = 0
    for item in overview["top_high_demand_low_supply"]:
        if item["demand_vs_supply_badge"] in {"Replenish NOW", "Replenish NOW (partial)"}:
            candidates += 1
    return {"status": "ok", "candidates": candidates}
