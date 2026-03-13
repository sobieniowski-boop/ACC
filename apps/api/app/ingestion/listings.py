"""Unified listing ingestion: registry, events, and report-based paths.

Ensures one canonical listing state per ``(seller_sku, marketplace_id)``
regardless of which data source triggers the update.

Three upstream paths:
  1. **Registry** — ``sync_amazon_listing_registry`` (batch from external source)
  2. **Events** — SP-API LISTINGS_ITEM_* events via event backbone → ``listing_state``
  3. **Reports** — ``sync_listings_to_products`` (SP-API ``GET_MERCHANT_LISTINGS`` report)

All three ultimately converge on ``dbo.acc_listing_state`` as the single
source-of-truth for listing status per (sku, marketplace).  History is
preserved in ``dbo.acc_listing_state_history`` (written by ``upsert_listing_state``).
"""
from __future__ import annotations

import asyncio
from typing import Any

import structlog

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def sync_listing_registry(
    *,
    force: bool = False,
    job_id: str | None = None,
) -> dict[str, Any]:
    """Step 1: import external listing registry → ``acc_amazon_listing_registry``.

    Also patches ``acc_listing_state`` for any new/changed SKUs so the
    canonical table stays in sync.
    """
    from app.services.amazon_listing_registry import sync_amazon_listing_registry

    result = sync_amazon_listing_registry(force=force, job_id=job_id)
    patched = _patch_listing_state_from_registry(result)
    result["listing_state_patched"] = patched
    return result


async def sync_listings_from_report(
    *,
    marketplace_ids: list[str] | None = None,
    job_id: str | None = None,
) -> dict[str, Any]:
    """Step 2: SP-API merchant-listing report → ``acc_product`` + ``acc_listing_state``.

    Delegates the report download + product upsert to the existing service,
    then patches ``acc_listing_state`` for any new/changed SKUs.
    """
    from app.services.sync_listings_to_products import sync_listings_to_products

    result = await sync_listings_to_products(
        marketplace_ids=marketplace_ids,
        job_id=job_id,
    )
    patched = _patch_listing_state_from_products(result)
    result["listing_state_patched"] = patched
    return result


async def sync_marketplace_listings(
    *,
    marketplace_ids: list[str] | None = None,
    family_ids: list[int] | None = None,
) -> dict[str, Any]:
    """Step 3: cross-marketplace child ASIN sync → ``marketplace_listing_child``.

    Thin delegation — this path produces a separate table used by the
    family mapper and does not write to ``acc_listing_state``.
    """
    from app.services.family_mapper.marketplace_sync import (
        sync_marketplace_listings as _sync_mp,
    )

    return await _sync_mp(
        marketplace_ids=marketplace_ids,
        family_ids=family_ids,
    )


def upsert_listing_state(
    seller_sku: str,
    marketplace_id: str,
    **kwargs: Any,
) -> str:
    """Canonical single-row upsert — delegates to ``listing_state.upsert_listing_state``.

    All callers should use this function (or the underlying service
    directly) so there is exactly one write path to ``acc_listing_state``.
    """
    from app.services.listing_state import upsert_listing_state as _upsert
    return _upsert(seller_sku, marketplace_id, **kwargs)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _patch_listing_state_from_registry(registry_result: dict[str, Any]) -> int:
    """After registry sync, ensure ``acc_listing_state`` has an entry for
    every (sku, marketplace) that appeared in the registry."""
    from app.core.db_connection import connect_acc

    row_count = int(registry_result.get("row_count", 0) or 0)
    if row_count == 0:
        return 0

    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            MERGE dbo.acc_listing_state AS t
            USING (
                SELECT DISTINCT merchant_sku AS seller_sku,
                       'A1PA6795UKMFR9' AS marketplace_id,
                       asin, brand, product_name AS title
                FROM dbo.acc_amazon_listing_registry WITH (NOLOCK)
            ) AS s
            ON t.seller_sku = s.seller_sku AND t.marketplace_id = s.marketplace_id
            WHEN NOT MATCHED THEN
                INSERT (id, seller_sku, marketplace_id, asin, brand, title,
                        sync_source, created_at, updated_at)
                VALUES (NEWID(), s.seller_sku, s.marketplace_id, s.asin,
                        s.brand, s.title, 'registry', SYSUTCDATETIME(), SYSUTCDATETIME());
            """,
        )
        patched = cur.rowcount
        conn.commit()
        return patched
    finally:
        conn.close()


def _patch_listing_state_from_products(report_result: dict[str, Any]) -> int:
    """After product report sync, ensure new products are reflected in
    ``acc_listing_state``."""
    from app.core.db_connection import connect_acc

    totals = report_result.get("totals", {}) if isinstance(report_result, dict) else {}
    created = int(totals.get("created", 0) or 0)
    if created == 0:
        return 0

    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            MERGE dbo.acc_listing_state AS t
            USING (
                SELECT p.sku AS seller_sku,
                       mp.marketplace_id,
                       p.asin, p.brand, p.title
                FROM dbo.acc_product p WITH (NOLOCK)
                CROSS APPLY (
                    SELECT DISTINCT marketplace_id
                    FROM dbo.acc_inventory_snapshot s WITH (NOLOCK)
                    WHERE s.sku = p.sku
                ) mp
            ) AS s
            ON t.seller_sku = s.seller_sku AND t.marketplace_id = s.marketplace_id
            WHEN NOT MATCHED THEN
                INSERT (id, seller_sku, marketplace_id, asin, brand, title,
                        sync_source, created_at, updated_at)
                VALUES (NEWID(), s.seller_sku, s.marketplace_id, s.asin,
                        s.brand, s.title, 'report', SYSUTCDATETIME(), SYSUTCDATETIME());
            """,
        )
        patched = cur.rowcount
        conn.commit()
        return patched
    finally:
        conn.close()
