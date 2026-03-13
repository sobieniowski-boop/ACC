"""Pricing State — persistent snapshot / history + recommendation layer.

Tables:
  ``acc_pricing_snapshot``   — point-in-time price observation (our price, buybox, competitive)
  ``acc_pricing_rule``       — per-SKU guardrails (min/max, min margin, strategy)
  ``acc_pricing_recommendation`` — generated recommendations (detect→store→recommend, never auto-write)
  ``acc_pricing_sync_state`` — per-marketplace sync tracking

Design:
  • Every sync cycle or real-time event appends a row to ``acc_pricing_snapshot``.
  • Rules are evaluated against the latest snapshot; violations produce recommendations.
  • Recommendations have lifecycle: pending → accepted / dismissed / expired.
  • The system detects, stores, and recommends. It does NOT auto-change prices.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

import structlog

from app.core.config import settings, MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)


def _connect():
    return connect_acc(autocommit=False, timeout=30)


# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

def ensure_pricing_state_schema() -> None:
    """Create pricing-state tables (idempotent)."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            -- 1. Pricing Snapshot (history of observations)
            IF OBJECT_ID('dbo.acc_pricing_snapshot', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.acc_pricing_snapshot (
                    id                   BIGINT IDENTITY(1,1) PRIMARY KEY,
                    seller_sku           NVARCHAR(100)   NOT NULL,
                    asin                 VARCHAR(20)     NULL,
                    marketplace_id       VARCHAR(20)     NOT NULL,

                    -- Our price
                    our_price            DECIMAL(12,2)   NULL,
                    our_currency         VARCHAR(5)      NOT NULL DEFAULT 'EUR',
                    fulfillment_channel  VARCHAR(10)     NULL,       -- FBA / FBM

                    -- Buy Box / Featured Offer context
                    buybox_price         DECIMAL(12,2)   NULL,
                    buybox_landed_price  DECIMAL(12,2)   NULL,       -- including shipping
                    has_buybox           BIT             NOT NULL DEFAULT 0,
                    is_featured_merchant BIT             NOT NULL DEFAULT 0,
                    buybox_seller_id     VARCHAR(20)     NULL,       -- who owns BB (if not us)

                    -- Competitive context
                    lowest_price_new     DECIMAL(12,2)   NULL,
                    num_offers_new       INT             NULL,
                    num_offers_used      INT             NULL,
                    bsr_rank             INT             NULL,
                    bsr_category         NVARCHAR(200)   NULL,

                    -- Price gap analytics
                    price_vs_buybox_pct  AS CASE
                        WHEN buybox_price > 0 AND our_price IS NOT NULL
                        THEN CAST(((our_price - buybox_price) / buybox_price * 100) AS DECIMAL(8,2))
                        ELSE NULL END PERSISTED,

                    -- Source / provenance
                    source               VARCHAR(30)     NOT NULL DEFAULT 'competitive_pricing_api',
                    observed_at          DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
                    created_at           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
                );

                CREATE INDEX IX_pricing_snap_sku_mkt
                    ON dbo.acc_pricing_snapshot (seller_sku, marketplace_id, observed_at DESC);
                CREATE INDEX IX_pricing_snap_asin
                    ON dbo.acc_pricing_snapshot (asin, marketplace_id, observed_at DESC)
                    WHERE asin IS NOT NULL;
                CREATE INDEX IX_pricing_snap_observed
                    ON dbo.acc_pricing_snapshot (observed_at DESC);
                CREATE INDEX IX_pricing_snap_buybox
                    ON dbo.acc_pricing_snapshot (marketplace_id, has_buybox)
                    INCLUDE (seller_sku, our_price, buybox_price);
            END

            -- 2. Pricing Rules (per-SKU or global guardrails)
            IF OBJECT_ID('dbo.acc_pricing_rule', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.acc_pricing_rule (
                    id                   INT IDENTITY(1,1) PRIMARY KEY,
                    seller_sku           NVARCHAR(100)   NULL,       -- NULL = global default
                    marketplace_id       VARCHAR(20)     NULL,       -- NULL = all marketplaces
                    rule_type            VARCHAR(30)     NOT NULL,   -- min_margin | max_deviation | floor_price | ceiling_price

                    -- Guardrail parameters
                    min_margin_pct       DECIMAL(6,2)    NULL,       -- e.g. 15.0 = 15% minimum margin
                    max_price_deviation_pct DECIMAL(6,2) NULL,       -- max % deviation from buybox
                    floor_price          DECIMAL(12,2)   NULL,       -- absolute minimum price
                    ceiling_price        DECIMAL(12,2)   NULL,       -- absolute maximum price
                    target_margin_pct    DECIMAL(6,2)    NULL,       -- target margin for recommendations

                    -- Strategy
                    strategy             VARCHAR(30)     NOT NULL DEFAULT 'monitor',
                        -- monitor | buybox_match | margin_protect | competitive

                    is_active            BIT             NOT NULL DEFAULT 1,
                    priority             INT             NOT NULL DEFAULT 100,  -- lower = higher priority
                    created_at           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
                    updated_at           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),

                    CONSTRAINT uq_pricing_rule_sku_mkt_type
                        UNIQUE (seller_sku, marketplace_id, rule_type)
                );

                CREATE INDEX IX_pricing_rule_active
                    ON dbo.acc_pricing_rule (is_active, priority)
                    WHERE is_active = 1;
            END

            -- 3. Pricing Recommendations (detect + recommend, never auto-write)
            IF OBJECT_ID('dbo.acc_pricing_recommendation', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.acc_pricing_recommendation (
                    id                   BIGINT IDENTITY(1,1) PRIMARY KEY,
                    seller_sku           NVARCHAR(100)   NOT NULL,
                    asin                 VARCHAR(20)     NULL,
                    marketplace_id       VARCHAR(20)     NOT NULL,

                    -- What we detected
                    current_price        DECIMAL(12,2)   NULL,
                    recommended_price    DECIMAL(12,2)   NOT NULL,
                    buybox_price         DECIMAL(12,2)   NULL,

                    -- Impact estimate
                    price_delta          AS (recommended_price - current_price) PERSISTED,
                    price_delta_pct      AS CASE
                        WHEN current_price > 0
                        THEN CAST(((recommended_price - current_price) / current_price * 100) AS DECIMAL(8,2))
                        ELSE NULL END PERSISTED,

                    -- Reason / confidence
                    reason_code          VARCHAR(50)     NOT NULL,
                        -- buybox_lost | margin_below_min | price_above_ceiling
                        -- price_below_floor | deviation_too_high | competitive_gap
                    reason_text          NVARCHAR(500)   NULL,
                    confidence           DECIMAL(5,2)    NOT NULL DEFAULT 50.0,  -- 0-100

                    -- Rule that triggered
                    rule_id              INT             NULL,
                    snapshot_id          BIGINT          NULL,

                    -- Lifecycle
                    status               VARCHAR(20)     NOT NULL DEFAULT 'pending',
                        -- pending | accepted | dismissed | expired | superseded
                    decided_at           DATETIME2       NULL,
                    decided_by           NVARCHAR(100)   NULL,

                    created_at           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
                    expires_at           DATETIME2       NULL,

                    CONSTRAINT FK_rec_rule FOREIGN KEY (rule_id)
                        REFERENCES dbo.acc_pricing_rule(id),
                    CONSTRAINT FK_rec_snapshot FOREIGN KEY (snapshot_id)
                        REFERENCES dbo.acc_pricing_snapshot(id)
                );

                CREATE INDEX IX_pricing_rec_sku_mkt
                    ON dbo.acc_pricing_recommendation (seller_sku, marketplace_id, created_at DESC);
                CREATE INDEX IX_pricing_rec_status
                    ON dbo.acc_pricing_recommendation (status, created_at DESC)
                    WHERE status = 'pending';
            END

            -- 4. Sync state
            IF OBJECT_ID('dbo.acc_pricing_sync_state', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.acc_pricing_sync_state (
                    marketplace_id       VARCHAR(20)     NOT NULL PRIMARY KEY,
                    last_snapshot_at     DATETIME2       NULL,
                    last_rule_eval_at    DATETIME2       NULL,
                    snapshots_count      INT             NOT NULL DEFAULT 0,
                    recommendations_count INT            NOT NULL DEFAULT 0,
                    last_error           NVARCHAR(500)   NULL,
                    updated_at           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
                );
            END

            -- 5. Pricing Snapshot Archive (same schema, no computed column)
            IF OBJECT_ID('dbo.acc_pricing_snapshot_archive', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.acc_pricing_snapshot_archive (
                    id                   BIGINT          NOT NULL,
                    seller_sku           NVARCHAR(100)   NOT NULL,
                    asin                 VARCHAR(20)     NULL,
                    marketplace_id       VARCHAR(20)     NOT NULL,
                    our_price            DECIMAL(12,2)   NULL,
                    our_currency         VARCHAR(5)      NOT NULL DEFAULT 'EUR',
                    fulfillment_channel  VARCHAR(10)     NULL,
                    buybox_price         DECIMAL(12,2)   NULL,
                    buybox_landed_price  DECIMAL(12,2)   NULL,
                    has_buybox           BIT             NOT NULL DEFAULT 0,
                    is_featured_merchant BIT             NOT NULL DEFAULT 0,
                    buybox_seller_id     VARCHAR(20)     NULL,
                    lowest_price_new     DECIMAL(12,2)   NULL,
                    num_offers_new       INT             NULL,
                    num_offers_used      INT             NULL,
                    bsr_rank             INT             NULL,
                    bsr_category         NVARCHAR(200)   NULL,
                    source               VARCHAR(30)     NOT NULL DEFAULT 'competitive_pricing_api',
                    observed_at          DATETIME2       NOT NULL,
                    created_at           DATETIME2       NOT NULL,
                    archived_at          DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
                    CONSTRAINT PK_pricing_snap_archive PRIMARY KEY (id)
                );

                CREATE INDEX IX_pricing_snap_archive_observed
                    ON dbo.acc_pricing_snapshot_archive (observed_at DESC);
                CREATE INDEX IX_pricing_snap_archive_sku_mkt
                    ON dbo.acc_pricing_snapshot_archive (seller_sku, marketplace_id, observed_at DESC);
            END
        """)
        conn.commit()
        log.info("pricing_state.schema_ensured")
    finally:
        conn.close()


def _ensure_internal_sku_column() -> None:
    """Add internal_sku column to pricing snapshot tables (idempotent, S8.3)."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'acc_pricing_snapshot'
                  AND COLUMN_NAME = 'internal_sku'
            )
            ALTER TABLE dbo.acc_pricing_snapshot
                ADD internal_sku NVARCHAR(64) NULL;
        """)
        cur.execute("""
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'acc_pricing_snapshot_archive'
                  AND COLUMN_NAME = 'internal_sku'
            )
            ALTER TABLE dbo.acc_pricing_snapshot_archive
                ADD internal_sku NVARCHAR(64) NULL;
        """)
        conn.commit()
    except Exception as exc:
        log.debug("pricing_state.internal_sku_col_skip", error=str(exc))
    finally:
        conn.close()


_INTERNAL_SKU_COL_ENSURED = False


def _ensure_internal_sku_col_once() -> None:
    global _INTERNAL_SKU_COL_ENSURED
    if _INTERNAL_SKU_COL_ENSURED:
        return
    _ensure_internal_sku_column()
    _INTERNAL_SKU_COL_ENSURED = True


# ---------------------------------------------------------------------------
# Snapshot operations
# ---------------------------------------------------------------------------

def record_snapshot(
    seller_sku: str,
    marketplace_id: str,
    *,
    asin: str | None = None,
    our_price: float | None = None,
    our_currency: str = "EUR",
    fulfillment_channel: str | None = None,
    buybox_price: float | None = None,
    buybox_landed_price: float | None = None,
    has_buybox: bool = False,
    is_featured_merchant: bool = False,
    buybox_seller_id: str | None = None,
    lowest_price_new: float | None = None,
    num_offers_new: int | None = None,
    num_offers_used: int | None = None,
    bsr_rank: int | None = None,
    bsr_category: str | None = None,
    source: str = "competitive_pricing_api",
) -> int:
    """Record a single pricing snapshot. Returns the snapshot id."""
    _ensure_internal_sku_col_once()
    conn = _connect()
    try:
        cur = conn.cursor()
        # Resolve internal_sku via marketplace presence bridge (S8.3)
        internal_sku = None
        try:
            cur.execute("""
                SELECT TOP 1 internal_sku
                FROM dbo.acc_marketplace_presence WITH (NOLOCK)
                WHERE seller_sku = ? AND marketplace_id = ?
            """, (seller_sku, marketplace_id))
            isk_row = cur.fetchone()
            internal_sku = isk_row[0] if isk_row else None
        except Exception:
            pass

        cur.execute("""
            SET LOCK_TIMEOUT 30000;
            INSERT INTO dbo.acc_pricing_snapshot (
                seller_sku, asin, marketplace_id, internal_sku,
                our_price, our_currency, fulfillment_channel,
                buybox_price, buybox_landed_price,
                has_buybox, is_featured_merchant, buybox_seller_id,
                lowest_price_new, num_offers_new, num_offers_used,
                bsr_rank, bsr_category,
                source, observed_at
            ) VALUES (
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, SYSUTCDATETIME()
            );
            SELECT SCOPE_IDENTITY();
        """, (
            seller_sku, asin, marketplace_id, internal_sku,
            our_price, our_currency, fulfillment_channel,
            buybox_price, buybox_landed_price,
            1 if has_buybox else 0, 1 if is_featured_merchant else 0, buybox_seller_id,
            lowest_price_new, num_offers_new, num_offers_used,
            bsr_rank, bsr_category,
            source,
        ))
        row = cur.fetchone()
        snap_id = int(row[0]) if row else 0
        conn.commit()
        return snap_id
    finally:
        conn.close()


def record_snapshots_batch(rows: list[dict], marketplace_id: str, source: str) -> int:
    """Batch-insert multiple snapshots. Returns count inserted."""
    if not rows:
        return 0
    _ensure_internal_sku_col_once()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 30000")
        count = 0
        for r in rows:
            cur.execute("""
                INSERT INTO dbo.acc_pricing_snapshot (
                    seller_sku, asin, marketplace_id,
                    our_price, our_currency, fulfillment_channel,
                    buybox_price, buybox_landed_price,
                    has_buybox, is_featured_merchant,
                    lowest_price_new, num_offers_new, num_offers_used,
                    bsr_rank, bsr_category,
                    source, observed_at
                ) VALUES (
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, SYSUTCDATETIME()
                )
            """, (
                r.get("seller_sku") or r.get("sku", ""),
                r.get("asin"),
                marketplace_id,
                r.get("our_price") or r.get("price"),
                r.get("currency", "EUR"),
                r.get("fulfillment_channel"),
                r.get("buybox_price"),
                r.get("buybox_landed_price"),
                1 if r.get("has_buybox") else 0,
                1 if r.get("is_featured_merchant") else 0,
                r.get("lowest_price_new"),
                r.get("num_offers_new"),
                r.get("num_offers_used"),
                r.get("bsr_rank"),
                r.get("bsr_category"),
                source,
            ))
            count += 1

        # Batch-resolve internal_sku from marketplace presence bridge (S8.3)
        try:
            cur.execute("""
                UPDATE s
                SET s.internal_sku = mp.internal_sku
                FROM dbo.acc_pricing_snapshot s
                JOIN dbo.acc_marketplace_presence mp
                    ON mp.seller_sku = s.seller_sku AND mp.marketplace_id = s.marketplace_id
                WHERE s.internal_sku IS NULL
                  AND s.marketplace_id = ?
                  AND s.observed_at >= DATEADD(minute, -5, SYSUTCDATETIME())
            """, (marketplace_id,))
        except Exception:
            pass

        conn.commit()
        log.info("pricing_state.snapshots_batch", marketplace_id=marketplace_id, count=count)

        # Emit domain event for downstream triggers
        if count > 0:
            try:
                from app.services.event_backbone import emit_domain_event
                emit_domain_event(
                    "pricing", "captured",
                    {"marketplace_id": marketplace_id, "snapshots": count, "source": source},
                    marketplace_id=marketplace_id,
                )
            except Exception:
                pass  # non-critical — don't break pricing on event emission failure

        return count
    finally:
        conn.close()


def get_latest_snapshot(
    seller_sku: str,
    marketplace_id: str,
) -> dict | None:
    """Get the most recent pricing snapshot for a SKU."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT TOP 1
                id, seller_sku, asin, marketplace_id,
                our_price, our_currency, fulfillment_channel,
                buybox_price, buybox_landed_price,
                has_buybox, is_featured_merchant, buybox_seller_id,
                lowest_price_new, num_offers_new, num_offers_used,
                bsr_rank, bsr_category, price_vs_buybox_pct,
                source, observed_at, internal_sku
            FROM dbo.acc_pricing_snapshot WITH (NOLOCK)
            WHERE seller_sku = ? AND marketplace_id = ?
            ORDER BY observed_at DESC
        """, (seller_sku, marketplace_id))
        row = cur.fetchone()
        if not row:
            return None
        return _snap_row_to_dict(row)
    finally:
        conn.close()


def get_snapshot_history(
    seller_sku: str,
    marketplace_id: str,
    *,
    limit: int = 100,
) -> list[dict]:
    """Get pricing history for a SKU (most recent first)."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT TOP (?)
                id, seller_sku, asin, marketplace_id,
                our_price, our_currency, fulfillment_channel,
                buybox_price, buybox_landed_price,
                has_buybox, is_featured_merchant, buybox_seller_id,
                lowest_price_new, num_offers_new, num_offers_used,
                bsr_rank, bsr_category, price_vs_buybox_pct,
                source, observed_at, internal_sku
            FROM dbo.acc_pricing_snapshot WITH (NOLOCK)
            WHERE seller_sku = ? AND marketplace_id = ?
            ORDER BY observed_at DESC
        """, (limit, seller_sku, marketplace_id))
        return [_snap_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_buybox_overview(marketplace_id: str | None = None) -> list[dict]:
    """Latest snapshot per SKU with buybox status for dashboard."""
    conn = _connect()
    try:
        cur = conn.cursor()
        where = ""
        params: list = []
        if marketplace_id:
            where = "WHERE s.marketplace_id = ?"
            params.append(marketplace_id)

        cur.execute(f"""
            WITH latest AS (
                SELECT seller_sku, marketplace_id,
                       MAX(observed_at) AS max_obs
                FROM dbo.acc_pricing_snapshot WITH (NOLOCK)
                GROUP BY seller_sku, marketplace_id
            )
            SELECT s.id, s.seller_sku, s.asin, s.marketplace_id,
                   s.our_price, s.our_currency, s.fulfillment_channel,
                   s.buybox_price, s.has_buybox, s.is_featured_merchant,
                   s.lowest_price_new, s.num_offers_new,
                   s.price_vs_buybox_pct, s.source, s.observed_at,
                   s.internal_sku
            FROM dbo.acc_pricing_snapshot s WITH (NOLOCK)
            JOIN latest l ON s.seller_sku = l.seller_sku
              AND s.marketplace_id = l.marketplace_id
              AND s.observed_at = l.max_obs
            {where}
            ORDER BY s.has_buybox ASC, s.price_vs_buybox_pct DESC
        """, params)

        results = []
        for r in cur.fetchall():
            results.append({
                "id": r[0],
                "seller_sku": r[1],
                "asin": r[2],
                "marketplace_id": r[3],
                "our_price": float(r[4]) if r[4] else None,
                "our_currency": r[5],
                "fulfillment_channel": r[6],
                "buybox_price": float(r[7]) if r[7] else None,
                "has_buybox": bool(r[8]),
                "is_featured_merchant": bool(r[9]),
                "lowest_price_new": float(r[10]) if r[10] else None,
                "num_offers_new": r[11],
                "price_vs_buybox_pct": float(r[12]) if r[12] is not None else None,
                "source": r[13],
                "observed_at": str(r[14]),
                "internal_sku": r[15] if r[15] else None,
            })
        return results
    finally:
        conn.close()


def _snap_row_to_dict(row) -> dict:
    return {
        "id": row[0],
        "seller_sku": row[1],
        "asin": row[2],
        "marketplace_id": row[3],
        "our_price": float(row[4]) if row[4] else None,
        "our_currency": row[5],
        "fulfillment_channel": row[6],
        "buybox_price": float(row[7]) if row[7] else None,
        "buybox_landed_price": float(row[8]) if row[8] else None,
        "has_buybox": bool(row[9]),
        "is_featured_merchant": bool(row[10]),
        "buybox_seller_id": row[11],
        "lowest_price_new": float(row[12]) if row[12] else None,
        "num_offers_new": row[13],
        "num_offers_used": row[14],
        "bsr_rank": row[15],
        "bsr_category": row[16],
        "price_vs_buybox_pct": float(row[17]) if row[17] is not None else None,
        "source": row[18],
        "observed_at": str(row[19]),
        "internal_sku": row[20] if len(row) > 20 else None,
    }


# ---------------------------------------------------------------------------
# Snapshot capture from SP-API (plugs into existing PricingClient)
# ---------------------------------------------------------------------------

async def capture_pricing_snapshots(
    marketplace_id: str,
    *,
    asin_limit: int = 500,
) -> dict[str, Any]:
    """Fetch competitive pricing from SP-API and record snapshots.

    Reads active ASINs from acc_offer, calls PricingClient, records history.
    """
    import asyncio
    from app.connectors.amazon_sp_api.pricing_api import (
        PricingClient,
        parse_competitive_pricing,
    )

    # Get active ASINs for this marketplace from acc_offer
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT o.sku, o.asin, o.price, o.currency, o.fulfillment_channel
            FROM dbo.acc_offer o WITH (NOLOCK)
            WHERE o.marketplace_id = ?
              AND o.asin IS NOT NULL AND o.asin != ''
              AND o.status = 'Active'
        """, (marketplace_id,))
        offers = []
        for r in cur.fetchall():
            offers.append({
                "sku": r[0], "asin": r[1],
                "price": float(r[2]) if r[2] else None,
                "currency": r[3] or "EUR",
                "fc": r[4],
            })
    finally:
        conn.close()

    if not offers:
        return {"marketplace_id": marketplace_id, "snapshots": 0, "status": "no_active_offers"}

    # Build ASIN → SKU/price lookup
    asin_to_offer: dict[str, dict] = {}
    for o in offers:
        asin_to_offer[o["asin"]] = o

    unique_asins = list(asin_to_offer.keys())[:asin_limit]

    # Fetch competitive pricing in batches
    client = PricingClient(marketplace_id=marketplace_id)
    pricing_results = await client.get_competitive_pricing_batch(unique_asins)

    # Parse and record snapshots
    snapshot_rows = []
    for pr in pricing_results:
        parsed = parse_competitive_pricing(pr)
        asin = parsed.get("asin", "")
        offer_info = asin_to_offer.get(asin, {})

        snapshot_rows.append({
            "seller_sku": offer_info.get("sku", ""),
            "asin": asin,
            "our_price": offer_info.get("price"),
            "currency": offer_info.get("currency", "EUR"),
            "fulfillment_channel": offer_info.get("fc"),
            "buybox_price": parsed.get("buybox_price"),
            "has_buybox": parsed.get("has_buybox", False),
            "num_offers_new": parsed.get("num_offers_new", 0),
            "bsr_rank": parsed.get("bsr_rank"),
            "bsr_category": parsed.get("bsr_category"),
        })

    count = record_snapshots_batch(
        snapshot_rows, marketplace_id, source="competitive_pricing_api",
    )

    # Step 4: Capture competitor offers for top ASINs (Sprint 12)
    competitor_result: dict = {}
    try:
        from app.intelligence.buybox_radar import capture_competitor_offers
        competitor_result = await capture_competitor_offers(
            marketplace_id, asin_limit=min(30, len(unique_asins)),
        )
    except Exception as exc:
        log.warning("pricing_state.competitor_capture_error",
                    marketplace_id=marketplace_id, error=str(exc))

    # Update sync state
    _update_sync_state(marketplace_id, snapshots_count=count)

    return {
        "marketplace_id": marketplace_id,
        "snapshots": count,
        "competitor_offers": competitor_result.get("offers_recorded", 0),
        "status": "ok",
    }


async def capture_all_marketplaces(*, asin_limit: int = 500) -> dict[str, Any]:
    """Capture pricing snapshots for all active marketplaces."""
    totals = {"marketplaces": 0, "snapshots": 0, "errors": 0}
    for mkt_id in MARKETPLACE_REGISTRY:
        try:
            result = await capture_pricing_snapshots(mkt_id, asin_limit=asin_limit)
            totals["marketplaces"] += 1
            totals["snapshots"] += result.get("snapshots", 0)
        except Exception as exc:
            log.warning("pricing_state.capture_error",
                        marketplace_id=mkt_id, error=str(exc))
            totals["errors"] += 1
    return totals


def record_snapshot_from_notification(
    asin: str,
    marketplace_id: str,
    notification_payload: dict,
) -> int | None:
    """Record a snapshot from an ANY_OFFER_CHANGED notification.

    Called by the Event Backbone handler.
    """
    buy_box_prices = notification_payload.get("buy_box_prices") or []
    number_of_offers = notification_payload.get("number_of_offers") or []

    buybox_price = None
    for bbp in buy_box_prices:
        if bbp.get("condition") == "New":
            buybox_price = bbp.get("Price", {}).get("LandedPrice", {}).get("Amount")
            break

    num_new = 0
    for noo in number_of_offers:
        if noo.get("condition") == "New":
            num_new = noo.get("OfferCount", 0)
            break

    # Look up our SKU + price for this ASIN
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT sku, price, currency, fulfillment_channel
            FROM dbo.acc_offer WITH (NOLOCK)
            WHERE marketplace_id = ? AND asin = ?
        """, (marketplace_id, asin))
        row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        log.debug("pricing_state.notification_no_offer", asin=asin, marketplace_id=marketplace_id)
        return None

    snapshot_id = record_snapshot(
        seller_sku=row[0],
        marketplace_id=marketplace_id,
        asin=asin,
        our_price=float(row[1]) if row[1] else None,
        our_currency=row[2] or "EUR",
        fulfillment_channel=row[3],
        buybox_price=float(buybox_price) if buybox_price else None,
        num_offers_new=num_new,
        source="any_offer_changed_notification",
    )

    # Sprint 12: Record competitor offers from notification payload
    try:
        from app.intelligence.buybox_radar import record_competitor_offers_from_notification
        record_competitor_offers_from_notification(
            asin, marketplace_id, notification_payload,
        )
    except Exception as exc:
        log.debug("pricing_state.notification_competitor_error",
                  asin=asin, error=str(exc))

    return snapshot_id


# ---------------------------------------------------------------------------
# Sync state helper
# ---------------------------------------------------------------------------

def _update_sync_state(
    marketplace_id: str,
    *,
    snapshots_count: int = 0,
    recommendations_count: int = 0,
    error: str | None = None,
) -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SET LOCK_TIMEOUT 30000;
            MERGE dbo.acc_pricing_sync_state AS tgt
            USING (SELECT ? AS marketplace_id) AS src
            ON tgt.marketplace_id = src.marketplace_id
            WHEN MATCHED THEN
                UPDATE SET last_snapshot_at = SYSUTCDATETIME(),
                           snapshots_count = snapshots_count + ?,
                           recommendations_count = recommendations_count + ?,
                           last_error = ?,
                           updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (marketplace_id, last_snapshot_at, snapshots_count,
                        recommendations_count, last_error, updated_at)
                VALUES (?, SYSUTCDATETIME(), ?, ?, ?, SYSUTCDATETIME());
        """, (
            marketplace_id,
            snapshots_count, recommendations_count, error,
            marketplace_id,
            snapshots_count, recommendations_count, error,
        ))
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Event Backbone integration
# ---------------------------------------------------------------------------

def _handle_offer_changed(event_row: dict) -> dict | None:
    """Handle ANY_OFFER_CHANGED events from the Event Backbone.

    Records a pricing snapshot from the real-time notification payload.
    """
    payload = event_row.get("payload_normalized") or {}
    asin = event_row.get("asin") or payload.get("asin")
    marketplace_id = event_row.get("marketplace_id") or payload.get("marketplace_id")

    if not asin or not marketplace_id:
        return {"status": "skipped", "reason": "missing_asin_or_marketplace"}

    snap_id = record_snapshot_from_notification(asin, marketplace_id, payload)
    if snap_id:
        log.info("pricing_state.notification_snapshot",
                 asin=asin, marketplace_id=marketplace_id, snapshot_id=snap_id)
        return {"status": "ok", "snapshot_id": snap_id}
    return {"status": "skipped", "reason": "no_matching_offer"}


def register_pricing_backbone_handler() -> None:
    """Register pricing-domain event handler with the event backbone."""
    from app.services.event_backbone import register_handler

    register_handler(
        "pricing",
        "offer_changed",
        handler_name="pricing_state.offer_changed",
        handler_fn=_handle_offer_changed,
    )
    log.info("pricing_state.backbone_handler_registered")


# ---------------------------------------------------------------------------
# Snapshot retention / archival
# ---------------------------------------------------------------------------

DEFAULT_ARCHIVE_DAYS: int = 30
ARCHIVE_BATCH_SIZE: int = 10_000


def archive_old_snapshots(days: int = DEFAULT_ARCHIVE_DAYS) -> dict:
    """Move pricing snapshots older than *days* to the archive table.

    Operates in batches to avoid long-running transactions and
    excessive lock escalation.

    Returns ``{"archived": int, "batches": int}``.
    """
    conn = _connect()
    total_archived = 0
    batches = 0

    try:
        cur = conn.cursor()

        while True:
            # 1) INSERT batch into archive
            cur.execute(
                f"""
                SET LOCK_TIMEOUT 30000;
                INSERT INTO dbo.acc_pricing_snapshot_archive
                    (id, seller_sku, asin, marketplace_id,
                     our_price, our_currency, fulfillment_channel,
                     buybox_price, buybox_landed_price,
                     has_buybox, is_featured_merchant, buybox_seller_id,
                     lowest_price_new, num_offers_new, num_offers_used,
                     bsr_rank, bsr_category,
                     source, observed_at, created_at, archived_at)
                SELECT TOP({ARCHIVE_BATCH_SIZE})
                    id, seller_sku, asin, marketplace_id,
                    our_price, our_currency, fulfillment_channel,
                    buybox_price, buybox_landed_price,
                    has_buybox, is_featured_merchant, buybox_seller_id,
                    lowest_price_new, num_offers_new, num_offers_used,
                    bsr_rank, bsr_category,
                    source, observed_at, created_at, SYSUTCDATETIME()
                FROM dbo.acc_pricing_snapshot WITH (NOLOCK)
                WHERE observed_at < DATEADD(DAY, -?, GETUTCDATE())
                  AND id NOT IN (
                      SELECT id FROM dbo.acc_pricing_snapshot_archive WITH (NOLOCK)
                  )
                ORDER BY observed_at ASC
                """,
                (days,),
            )
            inserted = cur.rowcount
            conn.commit()

            if inserted == 0:
                break

            # 2) DELETE the same rows from main table
            cur.execute(
                f"""
                SET LOCK_TIMEOUT 30000;
                DELETE TOP({ARCHIVE_BATCH_SIZE}) FROM dbo.acc_pricing_snapshot
                WHERE observed_at < DATEADD(DAY, -?, GETUTCDATE())
                  AND id IN (
                      SELECT TOP({ARCHIVE_BATCH_SIZE}) id
                      FROM dbo.acc_pricing_snapshot_archive WITH (NOLOCK)
                  )
                """,
                (days,),
            )
            deleted = cur.rowcount
            conn.commit()

            total_archived += deleted
            batches += 1

            log.info(
                "pricing_state.archive_batch",
                inserted=inserted,
                deleted=deleted,
                batch=batches,
            )

            if inserted < ARCHIVE_BATCH_SIZE:
                break  # last batch

        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    log.info(
        "pricing_state.archive_done",
        total_archived=total_archived,
        batches=batches,
        retention_days=days,
    )
    return {"archived": total_archived, "batches": batches, "retention_days": days}

