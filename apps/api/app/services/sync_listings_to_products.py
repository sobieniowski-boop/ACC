"""
Sync full Amazon listings → acc_product.

Uses SP-API report GET_MERCHANT_LISTINGS_ALL_DATA to discover ALL active
SKUs across all marketplaces, not just those that appeared in orders.
Creates acc_product rows for unknown SKUs and enriches existing ones
with data from the report (title, ASIN, price, status, etc.).

Designed to run daily (scheduler) or on-demand (POST /api/v1/jobs/sync-listings).
"""
from __future__ import annotations

import asyncio
import uuid
from typing import Any

import structlog

from app.connectors.amazon_sp_api.reports import ReportsClient, ReportType, parse_tsv_report
from app.core.config import MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc
from app.services.amazon_listing_registry import lookup_listing_registry_context

log = structlog.get_logger(__name__)


def _db_conn():
    return connect_acc(autocommit=False, timeout=30)


def _clean(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


# ---------------------------------------------------------------------------
# TSV column mapping for GET_MERCHANT_LISTINGS_ALL_DATA
# columns: item-name, item-description, listing-id, seller-sku, price,
#           quantity, open-date, image-url, item-is-marketplace, product-id-type,
#           zshop-shipping-fee, item-note, item-condition, zshop-category1,
#           zshop-browse-path, zshop-storefront-feature, asin1, asin2, asin3,
#           will-ship-internationally, expedited-shipping, zshop-boldface,
#           product-id, bid-for-featured-placement, add-delete, pending-quantity,
#           fulfillment-channel, merchant-shipping-group, status
# ---------------------------------------------------------------------------


def _parse_listing_row(row: dict[str, str]) -> dict[str, Any] | None:
    """Extract relevant fields from a single TSV row."""
    sku = _clean(row.get("seller-sku"))
    if not sku:
        return None

    asin = _clean(row.get("asin1"))
    title = _clean(row.get("item-name"))
    image_url = _clean(row.get("image-url"))
    status = _clean(row.get("status"))
    fulfillment = _clean(row.get("fulfillment-channel"))

    return {
        "sku": sku,
        "asin": asin,
        "title": title[:500] if title else None,
        "image_url": image_url,
        "status": status,
        "fulfillment_channel": fulfillment,
    }


def _upsert_products_from_listings(
    listings: list[dict[str, Any]],
    marketplace_id: str,
) -> dict[str, int]:
    """
    Create missing acc_product rows and enrich existing ones.
    Returns {"created": N, "enriched": N, "skipped": N}.
    """
    if not listings:
        return {"created": 0, "enriched": 0, "skipped": 0}

    conn = _db_conn()
    try:
        cur = conn.cursor()

        # Load existing SKUs in one shot
        skus = [row["sku"] for row in listings if row.get("sku")]
        existing: dict[str, dict] = {}
        chunk_size = 500
        for offset in range(0, len(skus), chunk_size):
            chunk = skus[offset : offset + chunk_size]
            placeholders = ",".join(["?"] * len(chunk))
            cur.execute(
                f"SELECT sku, asin, title, image_url, internal_sku "
                f"FROM dbo.acc_product WITH (NOLOCK) "
                f"WHERE sku IN ({placeholders})",
                chunk,
            )
            for r in cur.fetchall():
                existing[r[0]] = {
                    "asin": r[1],
                    "title": r[2],
                    "image_url": r[3],
                    "internal_sku": r[4],
                }

        created = 0
        enriched = 0
        skipped = 0

        for item in listings:
            sku = item["sku"]
            if not sku:
                skipped += 1
                continue

            if sku not in existing:
                # New product — create it
                registry = lookup_listing_registry_context(cur, sku=sku, asin=item.get("asin"))
                internal_sku = _clean((registry or {}).get("internal_sku"))
                ean = _clean((registry or {}).get("ean"))
                brand = _clean((registry or {}).get("brand"))
                parent_asin = _clean((registry or {}).get("parent_asin"))
                category = _clean((registry or {}).get("category_1")) or _clean((registry or {}).get("category_2"))
                listing_role = _clean((registry or {}).get("listing_role")) or ""
                mapping_source = "amazon_listing_registry" if registry else "spapi_listing_report"

                cur.execute(
                    """
                    INSERT INTO dbo.acc_product (
                        id, sku, asin, ean, brand, category, title,
                        image_url, is_parent, parent_asin,
                        internal_sku, mapping_source, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME())
                    """,
                    (
                        str(product_id_new := uuid.uuid4()),
                        sku,
                        item.get("asin") or _clean((registry or {}).get("asin")),
                        ean,
                        brand,
                        category,
                        item.get("title") or _clean((registry or {}).get("product_name")),
                        item.get("image_url"),
                        1 if listing_role.lower() == "parent" else 0,
                        parent_asin,
                        internal_sku,
                        mapping_source,
                    ),
                )
                # --- Controlling: log initial mapping ---
                if internal_sku:
                    try:
                        from app.services.controlling import log_mapping_change
                        log_mapping_change(
                            conn,
                            product_id=str(product_id_new),
                            sku=sku,
                            asin=item.get("asin"),
                            old_internal_sku=None,
                            new_internal_sku=internal_sku,
                            old_source=None,
                            new_source=mapping_source,
                            change_type="set",
                            reason="new product from listings sync",
                        )
                    except Exception:
                        pass  # controlling is non-blocking
                created += 1
                existing[sku] = {"asin": item.get("asin"), "title": item.get("title")}

            else:
                # Existing product — fill in blanks
                ex = existing[sku]
                updates = []
                params: list[Any] = []
                if not ex.get("title") and item.get("title"):
                    updates.append("title = ?")
                    params.append(item["title"])
                if not ex.get("asin") and item.get("asin"):
                    updates.append("asin = ?")
                    params.append(item["asin"])
                if not ex.get("image_url") and item.get("image_url"):
                    updates.append("image_url = ?")
                    params.append(item["image_url"])

                if updates:
                    updates.append("updated_at = SYSUTCDATETIME()")
                    params.append(sku)
                    cur.execute(
                        f"UPDATE dbo.acc_product SET {', '.join(updates)} WHERE sku = ?",
                        params,
                    )
                    enriched += 1
                else:
                    skipped += 1

        conn.commit()
        return {"created": created, "enriched": enriched, "skipped": skipped}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


async def sync_listings_to_products(
    marketplace_ids: list[str] | None = None,
    job_id: str | None = None,
) -> dict[str, Any]:
    """
    Fetch GET_MERCHANT_LISTINGS_ALL_DATA for each marketplace and upsert
    into acc_product.

    Args:
        marketplace_ids: Specific marketplaces (default: all from MARKETPLACE_REGISTRY).
        job_id: Optional JobRun ID for tracking.

    Returns:
        Summary dict with per-marketplace and total stats.
    """
    targets = marketplace_ids or list(MARKETPLACE_REGISTRY.keys())
    results: dict[str, Any] = {"marketplaces": {}, "totals": {"created": 0, "enriched": 0, "skipped": 0, "errors": 0}}

    for mkt_id in targets:
        mkt_code = MARKETPLACE_REGISTRY.get(mkt_id, {}).get("code", mkt_id)
        log.info("sync_listings.start", marketplace=mkt_code, marketplace_id=mkt_id)

        try:
            client = ReportsClient(marketplace_id=mkt_id)
            content = await client.request_and_download_reuse_recent(
                report_type=ReportType.ACTIVE_LISTINGS,
                marketplace_ids=[mkt_id],
                max_age_minutes=720,  # reuse report if < 12h old
                poll_interval=20.0,
            )

            rows = parse_tsv_report(content)
            listings = [r for r in (_parse_listing_row(row) for row in rows) if r]

            log.info("sync_listings.parsed", marketplace=mkt_code, raw_rows=len(rows), valid_listings=len(listings))

            stats = await asyncio.to_thread(
                _upsert_products_from_listings, listings, mkt_id,
            )

            # Update listing state table (Digital Twin layer)
            try:
                from app.services.listing_state import upsert_from_listing_report
                ls_stats = await asyncio.to_thread(
                    upsert_from_listing_report, listings, mkt_id,
                )
                log.info("sync_listings.listing_state_updated", marketplace=mkt_code, **ls_stats)
            except Exception as ls_exc:
                log.warning("sync_listings.listing_state_failed", marketplace=mkt_code, error=str(ls_exc))

            results["marketplaces"][mkt_code] = {
                "listings_found": len(listings),
                **stats,
            }
            results["totals"]["created"] += stats["created"]
            results["totals"]["enriched"] += stats["enriched"]
            results["totals"]["skipped"] += stats["skipped"]

            log.info("sync_listings.done", marketplace=mkt_code, **stats)

        except Exception as exc:
            log.error("sync_listings.error", marketplace=mkt_code, error=str(exc))
            results["marketplaces"][mkt_code] = {"error": str(exc)}
            results["totals"]["errors"] += 1

    log.info("sync_listings.complete", totals=results["totals"])
    return results
