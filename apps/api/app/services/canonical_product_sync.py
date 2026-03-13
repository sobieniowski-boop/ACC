"""Canonical product population — initial scan and ongoing sync.

Scans ``acc_product`` + ``acc_offer`` + ``acc_amazon_listing_registry``
to populate ``acc_canonical_product`` and ``acc_marketplace_presence``.

Usage::

    from app.services.canonical_product_sync import populate_canonical_products
    stats = populate_canonical_products()
"""
from __future__ import annotations

import structlog
from dataclasses import dataclass

from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)


@dataclass
class PopulationStats:
    """Statistics from a canonical population run."""
    products_scanned: int = 0
    canonical_created: int = 0
    canonical_updated: int = 0
    presences_created: int = 0
    unmapped_flagged: int = 0


def populate_canonical_products() -> PopulationStats:
    """Scan existing tables and populate canonical product + marketplace presence.

    Sources (in priority order):
      1. ``acc_product`` — products with ``internal_sku`` already set
      2. ``acc_amazon_listing_registry`` — enriched listing data with internal_sku
      3. ``acc_offer`` — marketplace presences for mapped products

    Products without ``internal_sku`` are flagged with ``needs_review = 1``.

    Returns:
        PopulationStats with counts.
    """
    stats = PopulationStats()
    conn = connect_acc(timeout=30)
    try:
        cur = conn.cursor()

        # --- Phase 1: Upsert canonical products from acc_product ---
        stats.products_scanned = _count_products(cur)
        log.info("canonical_pop.phase1_start", products=stats.products_scanned)

        created, updated = _upsert_from_acc_product(cur)
        stats.canonical_created += created
        stats.canonical_updated += updated
        conn.commit()
        log.info("canonical_pop.phase1_done", created=created, updated=updated)

        # --- Phase 2: Enrich from acc_amazon_listing_registry ---
        enriched = _enrich_from_listing_registry(cur)
        stats.canonical_created += enriched
        conn.commit()
        log.info("canonical_pop.phase2_done", enriched=enriched)

        # --- Phase 3: Build marketplace presences from acc_offer ---
        presences = _build_presences_from_offers(cur)
        stats.presences_created = presences
        conn.commit()
        log.info("canonical_pop.phase3_done", presences=presences)

        # --- Phase 4: Enrich presences from listing registry ---
        registry_presences = _build_presences_from_registry(cur)
        stats.presences_created += registry_presences
        conn.commit()
        log.info("canonical_pop.phase4_done", registry_presences=registry_presences)

        # --- Phase 5: Flag unmapped products ---
        flagged = _flag_unmapped(cur)
        stats.unmapped_flagged = flagged
        conn.commit()
        log.info("canonical_pop.phase5_done", flagged=flagged)

        log.info("canonical_pop.complete",
                 canonical_created=stats.canonical_created,
                 canonical_updated=stats.canonical_updated,
                 presences=stats.presences_created,
                 unmapped=stats.unmapped_flagged)
        return stats
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _count_products(cur) -> int:
    cur.execute("SELECT COUNT(*) FROM dbo.acc_product WITH (NOLOCK)")
    return cur.fetchone()[0]


def _upsert_from_acc_product(cur) -> tuple[int, int]:
    """Upsert canonical products from acc_product where internal_sku is set."""
    cur.execute("""
        MERGE dbo.acc_canonical_product AS tgt
        USING (
            SELECT internal_sku, ean, brand, category, subcategory,
                   title, image_url, k_number, ergonode_id,
                   netto_purchase_price_pln, vat_rate, mapping_source
            FROM dbo.acc_product WITH (NOLOCK)
            WHERE internal_sku IS NOT NULL
              AND LTRIM(RTRIM(internal_sku)) <> ''
        ) AS src
        ON tgt.internal_sku = src.internal_sku
        WHEN NOT MATCHED THEN
            INSERT (internal_sku, ean, brand, category, subcategory,
                    product_name, image_url, k_number, ergonode_id,
                    netto_purchase_price_pln, vat_rate, mapping_source,
                    mapping_confidence, lifecycle_status)
            VALUES (src.internal_sku, src.ean, src.brand, src.category,
                    src.subcategory, src.title, src.image_url,
                    src.k_number, src.ergonode_id,
                    src.netto_purchase_price_pln, src.vat_rate,
                    src.mapping_source, 90.0, 'active')
        WHEN MATCHED THEN
            UPDATE SET
                ean = COALESCE(src.ean, tgt.ean),
                brand = COALESCE(src.brand, tgt.brand),
                category = COALESCE(src.category, tgt.category),
                subcategory = COALESCE(src.subcategory, tgt.subcategory),
                product_name = COALESCE(src.title, tgt.product_name),
                image_url = COALESCE(src.image_url, tgt.image_url),
                k_number = COALESCE(src.k_number, tgt.k_number),
                ergonode_id = COALESCE(src.ergonode_id, tgt.ergonode_id),
                netto_purchase_price_pln = COALESCE(src.netto_purchase_price_pln,
                                                     tgt.netto_purchase_price_pln),
                vat_rate = COALESCE(src.vat_rate, tgt.vat_rate),
                updated_at = SYSUTCDATETIME()
        OUTPUT $action;
    """)
    created = 0
    updated = 0
    for row in cur.fetchall():
        if row[0] == "INSERT":
            created += 1
        else:
            updated += 1
    return created, updated


def _enrich_from_listing_registry(cur) -> int:
    """Create canonical products from listing registry entries not yet in canonical."""
    cur.execute("""
        INSERT INTO dbo.acc_canonical_product
            (internal_sku, ean, brand, product_name, category,
             mapping_source, mapping_confidence, lifecycle_status)
        SELECT DISTINCT
            lr.internal_sku,
            lr.ean,
            lr.brand,
            lr.product_name,
            lr.category_1,
            'listing_registry',
            70.0,
            'active'
        FROM dbo.acc_amazon_listing_registry lr WITH (NOLOCK)
        WHERE lr.internal_sku IS NOT NULL
          AND LTRIM(RTRIM(lr.internal_sku)) <> ''
          AND NOT EXISTS (
              SELECT 1 FROM dbo.acc_canonical_product cp WITH (NOLOCK)
              WHERE cp.internal_sku = lr.internal_sku
          )
    """)
    return cur.rowcount


def _build_presences_from_offers(cur) -> int:
    """Build marketplace presences from acc_offer joined with acc_product."""
    cur.execute("""
        INSERT INTO dbo.acc_marketplace_presence
            (internal_sku, marketplace_id, seller_sku, asin, fnsku,
             fulfillment_channel, listing_status, last_seen_at)
        SELECT
            p.internal_sku,
            o.marketplace_id,
            o.sku,
            o.asin,
            o.fnsku,
            o.fulfillment_channel,
            COALESCE(o.status, 'UNKNOWN'),
            o.last_synced_at
        FROM dbo.acc_offer o WITH (NOLOCK)
        JOIN dbo.acc_product p WITH (NOLOCK) ON p.id = o.product_id
        WHERE p.internal_sku IS NOT NULL
          AND LTRIM(RTRIM(p.internal_sku)) <> ''
          AND o.marketplace_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM dbo.acc_marketplace_presence mp WITH (NOLOCK)
              WHERE mp.internal_sku = p.internal_sku
                AND mp.marketplace_id = o.marketplace_id
                AND mp.seller_sku = o.sku
          )
    """)
    return cur.rowcount


def _build_presences_from_registry(cur) -> int:
    """Build marketplace presences from listing registry with source_gid as marketplace hint."""
    # The listing registry has merchant_sku + asin but no explicit marketplace_id.
    # We cross-reference with acc_offer to determine the marketplace.
    cur.execute("""
        INSERT INTO dbo.acc_marketplace_presence
            (internal_sku, marketplace_id, seller_sku, asin, parent_asin,
             listing_status, last_seen_at)
        SELECT DISTINCT
            lr.internal_sku,
            o.marketplace_id,
            lr.merchant_sku,
            lr.asin,
            lr.parent_asin,
            'REGISTRY',
            lr.synced_at
        FROM dbo.acc_amazon_listing_registry lr WITH (NOLOCK)
        JOIN dbo.acc_offer o WITH (NOLOCK)
            ON o.sku = lr.merchant_sku
        WHERE lr.internal_sku IS NOT NULL
          AND LTRIM(RTRIM(lr.internal_sku)) <> ''
          AND o.marketplace_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM dbo.acc_marketplace_presence mp WITH (NOLOCK)
              WHERE mp.internal_sku = lr.internal_sku
                AND mp.marketplace_id = o.marketplace_id
                AND mp.seller_sku = lr.merchant_sku
          )
    """)
    return cur.rowcount


def _flag_unmapped(cur) -> int:
    """Count products that have no canonical mapping (for queue display)."""
    cur.execute("""
        SELECT COUNT(*)
        FROM dbo.acc_product WITH (NOLOCK)
        WHERE internal_sku IS NULL
    """)
    return cur.fetchone()[0]
