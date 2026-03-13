"""Unified inventory ingestion: SP-API base snapshots + FBA enrichment.

All inventory snapshot writes go through this module so there is exactly
one insertion path and zero duplicate snapshots per (marketplace, sku, date).
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from typing import Any

import structlog

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def ingest_inventory(
    *,
    marketplace_id: str | None = None,
    enrich: bool = True,
    return_meta: bool = False,
) -> dict[str, Any]:
    """Unified entry-point for inventory snapshot ingestion.

    1. Fetch base inventory from SP-API Inventory Summaries → ``acc_inventory_snapshot``
    2. (optional) Enrich with FBA Planning + Stranded reports → ``acc_fba_inventory_snapshot``

    Returns dict with keys: ``raw_total``, ``enrichment`` (if *enrich*).
    """
    raw_total = await _ingest_raw_snapshots(marketplace_id=marketplace_id)

    enrichment: dict[str, Any] = {}
    if enrich:
        enrichment = await _enrich_with_fba_reports(return_meta=return_meta)

    return {
        "raw_total": raw_total,
        "enrichment": enrichment,
    }


def ingest_inventory_sync(
    *,
    marketplace_id: str | None = None,
    enrich: bool = True,
    return_meta: bool = False,
) -> dict[str, Any]:
    """Synchronous wrapper around :func:`ingest_inventory`."""
    return asyncio.run(
        ingest_inventory(
            marketplace_id=marketplace_id,
            enrich=enrich,
            return_meta=return_meta,
        )
    )


# ---------------------------------------------------------------------------
# Step 1 — raw SP-API inventory → acc_inventory_snapshot
# ---------------------------------------------------------------------------

async def _ingest_raw_snapshots(*, marketplace_id: str | None = None) -> int:
    """Fetch Inventory Summaries from SP-API and upsert ``acc_inventory_snapshot``."""
    from app.core.config import MARKETPLACE_REGISTRY
    from app.connectors.amazon_sp_api.inventory import InventoryClient
    from app.core.db_connection import connect_acc

    today = date.today()
    now = datetime.now(timezone.utc)
    mkts = [marketplace_id] if marketplace_id else sorted(MARKETPLACE_REGISTRY.keys())
    total = 0

    for mkt_id in mkts:
        try:
            client = InventoryClient(marketplace_id=mkt_id)
            summaries = await client.get_inventory_summaries()

            conn = connect_acc()
            try:
                cur = conn.cursor()
                for s in summaries:
                    sku = s.get("sellerSku", "")
                    fnsku = s.get("fnSku")
                    asin = s.get("asin")
                    inv = s.get("inventoryDetails", {})
                    qty_ful = int(inv.get("fulfillableQuantity", 0) or 0)
                    qty_res = int(
                        (inv.get("reservedQuantity") or {}).get("totalReservedQuantity", 0) or 0
                    )
                    qty_inb = int(inv.get("inboundWorkingQuantity", 0) or 0) + int(
                        inv.get("inboundShippedQuantity", 0) or 0
                    )
                    qty_unf = int(
                        (inv.get("unfulfillableQuantity") or {}).get("totalUnfulfillableQuantity", 0) or 0
                    )

                    cur.execute(
                        """
                        MERGE dbo.acc_inventory_snapshot AS target
                        USING (
                            SELECT ? AS marketplace_id, ? AS sku, ? AS snapshot_date
                        ) AS source
                        ON target.marketplace_id = source.marketplace_id
                           AND target.sku = source.sku
                           AND target.snapshot_date = source.snapshot_date
                        WHEN MATCHED THEN
                            UPDATE SET fnsku = ?, asin = ?,
                                       qty_fulfillable = ?, qty_reserved = ?,
                                       qty_inbound = ?, qty_unfulfillable = ?,
                                       synced_at = ?
                        WHEN NOT MATCHED THEN
                            INSERT (id, product_id, marketplace_id, snapshot_date,
                                    sku, fnsku, asin,
                                    qty_fulfillable, qty_reserved,
                                    qty_inbound, qty_unfulfillable, synced_at)
                            VALUES (NEWID(), NULL, ?, ?, ?, ?, ?,
                                    ?, ?, ?, ?, ?);
                        """,
                        (
                            mkt_id, sku, today,
                            # WHEN MATCHED
                            fnsku, asin, qty_ful, qty_res, qty_inb, qty_unf, now,
                            # WHEN NOT MATCHED
                            mkt_id, today, sku, fnsku, asin,
                            qty_ful, qty_res, qty_inb, qty_unf, now,
                        ),
                    )
                    total += 1
                conn.commit()
            finally:
                conn.close()
            log.info("ingestion.inventory.raw_done", mkt=mkt_id, rows=len(summaries))
        except Exception as exc:
            log.error("ingestion.inventory.raw_error", mkt=mkt_id, error=str(exc))

    return total


# ---------------------------------------------------------------------------
# Step 2 — FBA report enrichment → acc_fba_inventory_snapshot
# ---------------------------------------------------------------------------

async def _enrich_with_fba_reports(*, return_meta: bool = False) -> dict[str, Any]:
    """Delegate enrichment to ``fba_ops`` existing implementation.

    Keeps the SP-API report-download + merge logic in fba_ops to avoid
    duplicating complex report parsing.  The important invariant is that
    *this module* is the only entry-point callers use.
    """
    from app.services.fba_ops.inbound import _sync_inventory_cache_async

    result = await _sync_inventory_cache_async(return_meta=return_meta)
    if isinstance(result, int):
        return {"rows": result}
    return result
