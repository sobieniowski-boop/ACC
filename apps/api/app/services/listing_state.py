"""ACC Listing State — canonical persisted state for listing health.

Provides a durable, marketplace-scoped representation of every Amazon
listing's current operational status.  Key design decisions:

* Composite unique key: ``(seller_sku, marketplace_id)``
* NOT a transient cache — survives restarts, meant for operational monitoring
* Fed by three ingestion paths:
    1. Daily listing report sweep  (``sync_listings_to_products`` at 01:00)
    2. Real-time SP-API notification events via Event Backbone
    3. On-demand Listings Items API call (per-SKU refresh)
* Links back to ``acc_product`` via ``product_id`` for cross-referencing
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import structlog

from app.core.config import settings, MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
#  Schema DDL
# ═══════════════════════════════════════════════════════════════════════════

_SCHEMA_STATEMENTS: list[str] = [
    """
    IF OBJECT_ID('dbo.acc_listing_state', 'U') IS NULL
    CREATE TABLE dbo.acc_listing_state (
        id                   BIGINT IDENTITY(1,1) PRIMARY KEY,
        seller_sku           NVARCHAR(100)  NOT NULL,
        asin                 VARCHAR(20)    NULL,
        marketplace_id       VARCHAR(20)    NOT NULL,

        -- Classification
        product_type         VARCHAR(100)   NULL,
        listing_status       VARCHAR(30)    NOT NULL DEFAULT 'UNKNOWN',
        fulfillment_channel  VARCHAR(20)    NULL,
        condition_type       VARCHAR(30)    NULL,

        -- Issues / suppression
        has_issues           BIT            NOT NULL DEFAULT 0,
        issues_severity      VARCHAR(20)    NULL,
        issues_count_error   INT            NOT NULL DEFAULT 0,
        issues_count_warning INT            NOT NULL DEFAULT 0,
        issues_snapshot      NVARCHAR(MAX)  NULL,
        is_suppressed        BIT            NOT NULL DEFAULT 0,
        suppression_reasons  NVARCHAR(MAX)  NULL,

        -- Content identity snapshot
        title                NVARCHAR(500)  NULL,
        image_url            NVARCHAR(500)  NULL,
        brand                NVARCHAR(100)  NULL,

        -- Pricing snapshot
        current_price        DECIMAL(12,2)  NULL,
        currency_code        VARCHAR(5)     NULL,

        -- Parent / child
        parent_asin          VARCHAR(20)    NULL,
        variation_theme      VARCHAR(120)   NULL,

        -- Tracking
        sync_source          VARCHAR(50)    NOT NULL DEFAULT 'unknown',
        last_synced_at       DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        last_status_change   DATETIME2      NULL,
        last_issues_change   DATETIME2      NULL,

        -- ACC internal link
        product_id           UNIQUEIDENTIFIER NULL,

        created_at           DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at           DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT uq_listing_state_sku_mkt UNIQUE (seller_sku, marketplace_id)
    )
    """,

    # Health dashboard queries
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ls_mkt_status')
    CREATE INDEX ix_ls_mkt_status
        ON dbo.acc_listing_state (marketplace_id, listing_status)
    """,
    # Cross-marketplace ASIN lookups
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ls_asin')
    CREATE INDEX ix_ls_asin
        ON dbo.acc_listing_state (asin) WHERE asin IS NOT NULL
    """,
    # Suppression monitoring
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ls_suppressed')
    CREATE INDEX ix_ls_suppressed
        ON dbo.acc_listing_state (is_suppressed) WHERE is_suppressed = 1
    """,
    # Issues monitoring
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ls_issues')
    CREATE INDEX ix_ls_issues
        ON dbo.acc_listing_state (has_issues) WHERE has_issues = 1
    """,
    # Stale state detection
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ls_synced')
    CREATE INDEX ix_ls_synced
        ON dbo.acc_listing_state (last_synced_at)
    """,
    # Product link
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ls_product')
    CREATE INDEX ix_ls_product
        ON dbo.acc_listing_state (product_id) WHERE product_id IS NOT NULL
    """,
    # ── Listing state history ──
    """
    IF OBJECT_ID('dbo.acc_listing_state_history', 'U') IS NULL
    CREATE TABLE dbo.acc_listing_state_history (
        id                BIGINT IDENTITY(1,1) PRIMARY KEY,
        seller_sku        NVARCHAR(100)  NOT NULL,
        marketplace_id    VARCHAR(20)    NOT NULL,
        asin              VARCHAR(20)    NULL,
        previous_status   VARCHAR(30)    NULL,
        new_status        VARCHAR(30)    NOT NULL,
        issue_code        NVARCHAR(200)  NULL,
        issue_severity    VARCHAR(20)    NULL,
        changed_at        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        change_source     VARCHAR(50)    NOT NULL DEFAULT 'unknown',
        INDEX ix_lsh_sku_mkt_changed (seller_sku, marketplace_id, changed_at)
    )
    """,
]


def ensure_listing_state_schema() -> None:
    """Create listing state table + indexes if they don't exist."""
    conn = connect_acc(autocommit=True)
    try:
        cur = conn.cursor()
        for stmt in _SCHEMA_STATEMENTS:
            cur.execute(stmt)
        cur.close()
        log.info("listing_state.schema_ensured")
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Core upsert — single listing
# ═══════════════════════════════════════════════════════════════════════════

def upsert_listing_state(
    seller_sku: str,
    marketplace_id: str,
    *,
    asin: str | None = None,
    product_type: str | None = None,
    listing_status: str | None = None,
    fulfillment_channel: str | None = None,
    condition_type: str | None = None,
    has_issues: bool | None = None,
    issues_severity: str | None = None,
    issues_count_error: int | None = None,
    issues_count_warning: int | None = None,
    issues_snapshot: str | None = None,
    is_suppressed: bool | None = None,
    suppression_reasons: str | None = None,
    title: str | None = None,
    image_url: str | None = None,
    brand: str | None = None,
    current_price: float | None = None,
    currency_code: str | None = None,
    parent_asin: str | None = None,
    variation_theme: str | None = None,
    sync_source: str = "unknown",
    product_id: str | None = None,
    last_status_change: str | None = None,
    last_issues_change: str | None = None,
    conn: Any = None,
) -> str:
    """Insert or update a single listing state row.

    Uses MERGE for atomic upsert keyed on (seller_sku, marketplace_id).
    Only non-None fields are updated on conflict (preserves existing data).
    Returns 'created' or 'updated'.
    """
    own_conn = conn is None
    if own_conn:
        conn = connect_acc()
    try:
        cur = conn.cursor()

        # Check existence + current status for history & diff tracking
        cur.execute(
            "SELECT id, listing_status, title, image_url, brand, "
            "       current_price, is_suppressed, has_issues, "
            "       issues_severity, fulfillment_channel "
            "FROM dbo.acc_listing_state WITH (NOLOCK) "
            "WHERE seller_sku = ? AND marketplace_id = ?",
            (seller_sku, marketplace_id),
        )
        existing = cur.fetchone()

        if existing:
            # ── Field-diff detection (S9.2) ──
            _old_vals = {
                "listing_status": existing[1],
                "title": existing[2],
                "image_url": existing[3],
                "brand": existing[4],
                "current_price": existing[5],
                "is_suppressed": existing[6],
                "has_issues": existing[7],
                "issues_severity": existing[8],
                "fulfillment_channel": existing[9],
            }
            _new_vals = {
                "listing_status": listing_status,
                "title": title,
                "image_url": image_url,
                "brand": brand,
                "current_price": current_price,
                "is_suppressed": is_suppressed,
                "has_issues": has_issues,
                "issues_severity": issues_severity,
                "fulfillment_channel": fulfillment_channel,
            }
            try:
                from app.intelligence.catalog_health import detect_and_record_diffs
                detect_and_record_diffs(
                    cur, seller_sku, marketplace_id,
                    _old_vals, _new_vals,
                    change_source=sync_source,
                )
            except Exception as exc:
                log.debug("listing_state.diff_tracking_skipped", error=str(exc))

            # Build dynamic UPDATE — only set non-None fields
            updates: list[str] = []
            params: list[Any] = []

            def _add(col: str, val: Any) -> None:
                if val is not None:
                    updates.append(f"{col} = ?")
                    params.append(val)

            _add("asin", asin)
            _add("product_type", product_type)
            _add("listing_status", listing_status)
            _add("fulfillment_channel", fulfillment_channel)
            _add("condition_type", condition_type)
            if has_issues is not None:
                updates.append("has_issues = ?")
                params.append(1 if has_issues else 0)
            _add("issues_severity", issues_severity)
            _add("issues_count_error", issues_count_error)
            _add("issues_count_warning", issues_count_warning)
            _add("issues_snapshot", issues_snapshot)
            if is_suppressed is not None:
                updates.append("is_suppressed = ?")
                params.append(1 if is_suppressed else 0)
            _add("suppression_reasons", suppression_reasons)
            _add("title", title)
            _add("image_url", image_url)
            _add("brand", brand)
            _add("current_price", current_price)
            _add("currency_code", currency_code)
            _add("parent_asin", parent_asin)
            _add("variation_theme", variation_theme)
            _add("sync_source", sync_source)
            _add("product_id", product_id)
            _add("last_status_change", last_status_change)
            _add("last_issues_change", last_issues_change)

            # Always touch timestamps
            updates.append("last_synced_at = SYSUTCDATETIME()")
            updates.append("updated_at = SYSUTCDATETIME()")

            if updates:
                sql = (
                    f"UPDATE dbo.acc_listing_state SET {', '.join(updates)} "
                    f"WHERE seller_sku = ? AND marketplace_id = ?"
                )
                params.extend([seller_sku, marketplace_id])
                cur.execute(sql, tuple(params))

            # ── History tracking: record status transitions ──
            old_status = existing[1] if existing else None
            if listing_status is not None and old_status != listing_status:
                _insert_state_history(
                    cur,
                    seller_sku=seller_sku,
                    marketplace_id=marketplace_id,
                    asin=asin,
                    previous_status=old_status,
                    new_status=listing_status,
                    issue_code=_extract_top_issue_code(issues_snapshot),
                    issue_severity=issues_severity,
                    change_source=sync_source,
                )

            if own_conn:
                conn.commit()
            cur.close()
            return "updated"
        else:
            # INSERT
            cur.execute(
                """
                INSERT INTO dbo.acc_listing_state (
                    seller_sku, asin, marketplace_id, product_type,
                    listing_status, fulfillment_channel, condition_type,
                    has_issues, issues_severity, issues_count_error, issues_count_warning,
                    issues_snapshot, is_suppressed, suppression_reasons,
                    title, image_url, brand,
                    current_price, currency_code,
                    parent_asin, variation_theme,
                    sync_source, product_id,
                    last_status_change, last_issues_change,
                    last_synced_at, created_at, updated_at
                ) VALUES (
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    SYSUTCDATETIME(), SYSUTCDATETIME(), SYSUTCDATETIME()
                )
                """,
                (
                    seller_sku, asin, marketplace_id, product_type,
                    listing_status or "UNKNOWN", fulfillment_channel, condition_type,
                    1 if has_issues else 0, issues_severity,
                    issues_count_error or 0, issues_count_warning or 0,
                    issues_snapshot, 1 if is_suppressed else 0, suppression_reasons,
                    title, image_url, brand,
                    current_price, currency_code,
                    parent_asin, variation_theme,
                    sync_source, product_id,
                    last_status_change, last_issues_change,
                ),
            )
            if own_conn:
                conn.commit()
            cur.close()
            return "created"
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  State history helpers
# ═══════════════════════════════════════════════════════════════════════════

def _insert_state_history(
    cur: Any,
    *,
    seller_sku: str,
    marketplace_id: str,
    asin: str | None,
    previous_status: str | None,
    new_status: str,
    issue_code: str | None,
    issue_severity: str | None,
    change_source: str,
) -> None:
    """Insert a row into acc_listing_state_history (uses caller's cursor/txn)."""
    cur.execute(
        """
        INSERT INTO dbo.acc_listing_state_history (
            seller_sku, marketplace_id, asin,
            previous_status, new_status,
            issue_code, issue_severity,
            changed_at, change_source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), ?)
        """,
        (
            seller_sku, marketplace_id, asin,
            previous_status, new_status,
            (issue_code or "")[:200] or None, issue_severity,
            change_source,
        ),
    )
    log.debug(
        "listing_state.history_recorded",
        seller_sku=seller_sku,
        marketplace_id=marketplace_id,
        previous_status=previous_status,
        new_status=new_status,
        change_source=change_source,
    )


def _extract_top_issue_code(issues_snapshot: str | None) -> str | None:
    """Extract the first issue code from a JSON issues snapshot, if any."""
    if not issues_snapshot:
        return None
    try:
        issues = json.loads(issues_snapshot)
        if isinstance(issues, list) and issues:
            return str(issues[0].get("code", issues[0].get("issueType", "")))[:200] or None
    except (json.JSONDecodeError, AttributeError, IndexError):
        pass
    return None


# ═══════════════════════════════════════════════════════════════════════════
#  Bulk ingestion — from listing report sweep
# ═══════════════════════════════════════════════════════════════════════════

def upsert_from_listing_report(
    listings: list[dict[str, Any]],
    marketplace_id: str,
) -> dict[str, int]:
    """Bulk-upsert listing state from GET_MERCHANT_LISTINGS_ALL_DATA rows.

    Called by ``sync_listings_to_products`` after parsing TSV report.
    Each row should have: sku, asin, title, image_url, status, fulfillment_channel.

    Returns ``{"created": N, "updated": N, "skipped": N}``.
    """
    if not listings:
        return {"created": 0, "updated": 0, "skipped": 0}

    # Resolve currency from marketplace
    mkt_info = MARKETPLACE_REGISTRY.get(marketplace_id, {})
    currency = mkt_info.get("currency")

    # Lookup product_ids for linking
    conn = connect_acc()
    try:
        cur = conn.cursor()

        # Batch-fetch existing product IDs
        skus = [r["sku"] for r in listings if r.get("sku")]
        product_map: dict[str, str] = {}
        chunk_size = 500
        for offset in range(0, len(skus), chunk_size):
            chunk = skus[offset : offset + chunk_size]
            placeholders = ",".join(["?"] * len(chunk))
            cur.execute(
                f"SELECT sku, CAST(id AS VARCHAR(36)) FROM dbo.acc_product WITH (NOLOCK) "
                f"WHERE sku IN ({placeholders})",
                tuple(chunk),
            )
            for row in cur.fetchall():
                product_map[row[0]] = row[1]
        cur.close()

        created = 0
        updated = 0
        skipped = 0

        for item in listings:
            sku = item.get("sku")
            if not sku:
                skipped += 1
                continue

            status_raw = (item.get("status") or "").strip().upper()
            listing_status = status_raw if status_raw else "UNKNOWN"
            is_suppressed = listing_status == "SUPPRESSED"

            try:
                result = upsert_listing_state(
                    seller_sku=sku,
                    marketplace_id=marketplace_id,
                    asin=item.get("asin"),
                    listing_status=listing_status,
                    fulfillment_channel=item.get("fulfillment_channel"),
                    title=item.get("title"),
                    image_url=item.get("image_url"),
                    is_suppressed=is_suppressed,
                    currency_code=currency,
                    sync_source="listing_report",
                    product_id=product_map.get(sku),
                    conn=conn,
                )
                if result == "created":
                    created += 1
                else:
                    updated += 1
            except Exception as exc:
                log.warning("listing_state.report_upsert_failed", sku=sku, error=str(exc))
                skipped += 1

        conn.commit()
        log.info(
            "listing_state.report_batch_done",
            marketplace_id=marketplace_id,
            created=created, updated=updated, skipped=skipped,
        )
        return {"created": created, "updated": updated, "skipped": skipped}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Event backbone handlers
# ═══════════════════════════════════════════════════════════════════════════

def handle_listing_status_event(event: dict) -> dict:
    """Event backbone handler for LISTINGS_ITEM_STATUS_CHANGE.

    Updates listing_status, is_suppressed, last_status_change.
    """
    norm = event.get("payload_normalized")
    if isinstance(norm, str):
        norm = json.loads(norm)
    if not norm:
        return {"status": "skipped", "reason": "no normalized payload"}

    sku = norm.get("sku") or event.get("sku")
    marketplace_id = norm.get("marketplace_id") or event.get("marketplace_id")
    if not sku or not marketplace_id:
        return {"status": "skipped", "reason": "missing sku or marketplace_id"}

    status = norm.get("status", "UNKNOWN").upper()
    is_suppressed = status in ("SUPPRESSED", "DELETED")

    result = upsert_listing_state(
        seller_sku=sku,
        marketplace_id=marketplace_id,
        asin=norm.get("asin") or event.get("asin"),
        listing_status=status,
        is_suppressed=is_suppressed,
        sync_source="sp_api_event",
        last_status_change=event.get("received_at"),
    )
    log.info(
        "listing_state.status_event_applied",
        sku=sku, marketplace_id=marketplace_id,
        status=status, result=result,
    )
    return {"status": "ok", "result": result, "listing_status": status}


def handle_listing_issues_event(event: dict) -> dict:
    """Event backbone handler for LISTINGS_ITEM_ISSUES_CHANGE.

    Updates has_issues, issues_severity, issues_count_*, last_issues_change.
    """
    norm = event.get("payload_normalized")
    if isinstance(norm, str):
        norm = json.loads(norm)
    if not norm:
        return {"status": "skipped", "reason": "no normalized payload"}

    sku = norm.get("sku") or event.get("sku")
    marketplace_id = norm.get("marketplace_id") or event.get("marketplace_id")
    if not sku or not marketplace_id:
        return {"status": "skipped", "reason": "missing sku or marketplace_id"}

    severity_counts = norm.get("severity_counts") or {}
    error_count = severity_counts.get("ERROR", 0)
    warning_count = severity_counts.get("WARNING", 0)
    has_issues = (error_count + warning_count) > 0

    if error_count > 0:
        issues_severity = "ERROR"
    elif warning_count > 0:
        issues_severity = "WARNING"
    else:
        issues_severity = None

    result = upsert_listing_state(
        seller_sku=sku,
        marketplace_id=marketplace_id,
        asin=norm.get("asin") or event.get("asin"),
        has_issues=has_issues,
        issues_severity=issues_severity,
        issues_count_error=error_count,
        issues_count_warning=warning_count,
        sync_source="sp_api_event",
        last_issues_change=event.get("received_at"),
    )
    log.info(
        "listing_state.issues_event_applied",
        sku=sku, marketplace_id=marketplace_id,
        errors=error_count, warnings=warning_count, result=result,
    )
    return {"status": "ok", "result": result, "has_issues": has_issues}


def register_backbone_handlers() -> None:
    """Register listing-domain event handlers with the event backbone."""
    from app.services.event_backbone import register_handler

    register_handler(
        "listing",
        "listing_status_changed",
        handler_name="listing_state.status",
        handler_fn=handle_listing_status_event,
    )
    register_handler(
        "listing",
        "listing_issues_changed",
        handler_name="listing_state.issues",
        handler_fn=handle_listing_issues_event,
    )
    log.info("listing_state.backbone_handlers_registered")


# ═══════════════════════════════════════════════════════════════════════════
#  On-demand refresh — SP-API Listings Items API
# ═══════════════════════════════════════════════════════════════════════════

async def refresh_from_sp_api(
    seller_sku: str,
    marketplace_id: str,
) -> dict[str, Any]:
    """Fetch live listing data from SP-API and update listing state.

    Calls ListingsItems getListingsItem with summaries+attributes+issues.
    """
    from app.connectors.amazon_sp_api.listings import ListingsClient

    client = ListingsClient(marketplace_id=marketplace_id)
    data = await client.get_listings_item(
        seller_id=settings.SP_API_SELLER_ID,
        sku=seller_sku,
        included_data="summaries,attributes,issues",
    )

    # Parse summaries
    summaries = data.get("summaries") or []
    summary = summaries[0] if summaries else {}

    asin = summary.get("asin")
    product_type = summary.get("productType")
    status = summary.get("status")
    condition_type = summary.get("conditionType")
    title = summary.get("itemName")
    brand_attr = (data.get("attributes") or {}).get("brand")
    brand = None
    if isinstance(brand_attr, list) and brand_attr:
        brand = brand_attr[0].get("value")

    # Parse issues
    issues = data.get("issues") or []
    error_count = sum(1 for i in issues if i.get("severity") == "ERROR")
    warning_count = sum(1 for i in issues if i.get("severity") == "WARNING")
    has_issues = len(issues) > 0
    issues_snapshot = json.dumps(issues, default=str, ensure_ascii=False) if issues else None

    # Price from summaries
    main_image = summary.get("mainImage", {})
    image_url = main_image.get("link") if isinstance(main_image, dict) else None

    result = upsert_listing_state(
        seller_sku=seller_sku,
        marketplace_id=marketplace_id,
        asin=asin,
        product_type=product_type,
        listing_status=(status or "UNKNOWN").upper(),
        condition_type=condition_type,
        has_issues=has_issues,
        issues_severity="ERROR" if error_count > 0 else ("WARNING" if warning_count > 0 else None),
        issues_count_error=error_count,
        issues_count_warning=warning_count,
        issues_snapshot=issues_snapshot,
        is_suppressed=(status or "").upper() in ("SUPPRESSED", "DELETED"),
        title=title,
        image_url=image_url,
        brand=brand,
        sync_source="sp_api_direct",
    )

    log.info(
        "listing_state.sp_api_refresh",
        sku=seller_sku, marketplace_id=marketplace_id,
        status=status, issues=len(issues), result=result,
    )
    return {
        "result": result,
        "asin": asin,
        "listing_status": status,
        "product_type": product_type,
        "issues_count": len(issues),
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Query helpers
# ═══════════════════════════════════════════════════════════════════════════

def get_listing_state(seller_sku: str, marketplace_id: str) -> dict | None:
    """Get a single listing state row."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM dbo.acc_listing_state WITH (NOLOCK) "
            "WHERE seller_sku = ? AND marketplace_id = ?",
            (seller_sku, marketplace_id),
        )
        row = cur.fetchone()
        if not row:
            return None
        columns = [desc[0] for desc in cur.description]
        cur.close()
        return dict(zip(columns, row))
    finally:
        conn.close()


def get_listing_history(
    seller_sku: str,
    marketplace_id: str,
    *,
    limit: int = 100,
) -> list[dict]:
    """Return status-change history for a SKU, most recent first."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT TOP (?)
                id, seller_sku, marketplace_id, asin,
                previous_status, new_status,
                issue_code, issue_severity,
                changed_at, change_source
            FROM dbo.acc_listing_state_history WITH (NOLOCK)
            WHERE seller_sku = ? AND marketplace_id = ?
            ORDER BY changed_at DESC
        """, (limit, seller_sku, marketplace_id))
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, r)) for r in cur.fetchall()]
        # Serialize datetimes
        for row in rows:
            if row.get("changed_at"):
                row["changed_at"] = row["changed_at"].isoformat()
        cur.close()
        return rows
    finally:
        conn.close()


def get_listing_states(
    *,
    marketplace_id: str | None = None,
    listing_status: str | None = None,
    has_issues: bool | None = None,
    is_suppressed: bool | None = None,
    asin: str | None = None,
    sku_search: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict:
    """Paginated listing state query with optional filters."""
    conn = connect_acc()
    try:
        cur = conn.cursor()

        where_parts: list[str] = []
        params: list[Any] = []

        if marketplace_id:
            where_parts.append("marketplace_id = ?")
            params.append(marketplace_id)
        if listing_status:
            where_parts.append("listing_status = ?")
            params.append(listing_status.upper())
        if has_issues is not None:
            where_parts.append("has_issues = ?")
            params.append(1 if has_issues else 0)
        if is_suppressed is not None:
            where_parts.append("is_suppressed = ?")
            params.append(1 if is_suppressed else 0)
        if asin:
            where_parts.append("asin = ?")
            params.append(asin)
        if sku_search:
            where_parts.append("seller_sku LIKE ?")
            params.append(f"%{sku_search}%")

        where_clause = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

        # Count
        cur.execute(
            f"SELECT COUNT(*) FROM dbo.acc_listing_state WITH (NOLOCK){where_clause}",
            tuple(params) if params else None,
        )
        total = cur.fetchone()[0]

        # Page — offset/fetch must be inlined (pymssql can't parameterize them)
        offset = (page - 1) * page_size
        cur.execute(
            f"SELECT * FROM dbo.acc_listing_state WITH (NOLOCK){where_clause} "
            f"ORDER BY updated_at DESC "
            f"OFFSET {int(offset)} ROWS FETCH NEXT {int(page_size)} ROWS ONLY",
            tuple(params) if params else None,
        )
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()

        items = [dict(zip(columns, r)) for r in rows]
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": max(1, -(-total // page_size)),
            "items": items,
        }
    finally:
        conn.close()


def get_listing_health_summary(marketplace_id: str | None = None) -> dict:
    """Aggregated listing health summary.

    Returns counts by status, issue severity, suppression, and staleness.
    """
    conn = connect_acc()
    try:
        cur = conn.cursor()

        mkt_filter = ""
        params: tuple = ()
        if marketplace_id:
            mkt_filter = " WHERE marketplace_id = ?"
            params = (marketplace_id,)

        # Status distribution
        cur.execute(
            f"SELECT listing_status, COUNT(*) AS cnt "
            f"FROM dbo.acc_listing_state WITH (NOLOCK){mkt_filter} "
            f"GROUP BY listing_status",
            params,
        )
        status_counts = {r[0]: r[1] for r in cur.fetchall()}

        # Marketplace distribution
        cur.execute(
            "SELECT marketplace_id, COUNT(*) AS cnt "
            "FROM dbo.acc_listing_state WITH (NOLOCK) "
            "GROUP BY marketplace_id",
        )
        mkt_counts = {r[0]: r[1] for r in cur.fetchall()}

        # Issue / suppression totals
        cur.execute(
            f"SELECT "
            f"  SUM(CASE WHEN has_issues = 1 THEN 1 ELSE 0 END) AS with_issues, "
            f"  SUM(CASE WHEN is_suppressed = 1 THEN 1 ELSE 0 END) AS suppressed, "
            f"  SUM(CASE WHEN issues_severity = 'ERROR' THEN 1 ELSE 0 END) AS critical_issues, "
            f"  COUNT(*) AS total "
            f"FROM dbo.acc_listing_state WITH (NOLOCK){mkt_filter}",
            params,
        )
        row = cur.fetchone()
        totals = {
            "total_listings": row[3],
            "with_issues": row[0],
            "suppressed": row[1],
            "critical_issues": row[2],
        }

        # Stale detection (not synced in >48h)
        cur.execute(
            f"SELECT COUNT(*) FROM dbo.acc_listing_state WITH (NOLOCK) "
            f"WHERE last_synced_at < DATEADD(HOUR, -48, SYSUTCDATETIME())"
            f"{' AND marketplace_id = ?' if marketplace_id else ''}",
            params,
        )
        totals["stale_48h"] = cur.fetchone()[0]

        cur.close()
        return {
            "totals": totals,
            "by_status": status_counts,
            "by_marketplace": mkt_counts,
        }
    finally:
        conn.close()
