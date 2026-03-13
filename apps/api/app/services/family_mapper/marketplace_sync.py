"""
Marketplace sync — pull child listings for non-DE marketplaces and
populate marketplace_listing_child.

Uses SP-API Catalog Items to fetch ASIN-level details per marketplace,
extracting variation relationships, SKU, EAN, and attributes.
"""
from __future__ import annotations

import asyncio
import json
from typing import Optional

import structlog

from app.connectors.amazon_sp_api.catalog import CatalogClient
from app.core.config import MARKETPLACE_REGISTRY, settings
from app.core.db_connection import connect_acc
from app.services.family_mapper.master_key import build_master_key

log = structlog.get_logger(__name__)

DE_MARKETPLACE = settings.SP_API_PRIMARY_MARKETPLACE
NON_DE_MARKETPLACES = [
    mp_id for mp_id in MARKETPLACE_REGISTRY
    if mp_id != DE_MARKETPLACE
]

_INCLUDE = "summaries,relationships,identifiers,attributes"


# ---------------------------------------------------------------------------
# DB helpers (sync pyodbc)
# ---------------------------------------------------------------------------

def _connect():
    return connect_acc(autocommit=True)


def _upsert_listing_child(cur, marketplace: str, asin: str,
                           sku: str | None, ean: str | None,
                           parent_asin: str | None,
                           variation_theme: str | None,
                           attributes_json: str | None) -> None:
    """MERGE marketplace_listing_child on (marketplace, asin)."""
    cur.execute("""
        MERGE dbo.marketplace_listing_child AS tgt
        USING (SELECT ? AS marketplace, ? AS asin) AS src
            ON tgt.marketplace = src.marketplace AND tgt.asin = src.asin
        WHEN MATCHED THEN
            UPDATE SET sku = ?, ean = ?, current_parent_asin = ?,
                       variation_theme = ?, attributes_json = ?,
                       updated_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (marketplace, asin, sku, ean, current_parent_asin,
                    variation_theme, attributes_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
    """,
        marketplace, asin,
        sku, ean, parent_asin, variation_theme, attributes_json,
        marketplace, asin, sku, ean, parent_asin, variation_theme, attributes_json,
    )


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

def _extract_parent_asin(item: dict, marketplace_id: str) -> str | None:
    """Get parent ASIN from child's relationship data."""
    for rel_group in item.get("relationships", []):
        if rel_group.get("marketplaceId") == marketplace_id:
            for rel in rel_group.get("relationships", []):
                parent = rel.get("parentAsins", [])
                if isinstance(parent, list) and parent:
                    return parent[0]
                pa = rel.get("asin")
                if pa and rel.get("type") == "VARIATION_PARENT":
                    return pa
    return None


def _extract_variation_theme(item: dict, marketplace_id: str) -> str | None:
    for rel_group in item.get("relationships", []):
        if rel_group.get("marketplaceId") == marketplace_id:
            for rel in rel_group.get("relationships", []):
                theme = rel.get("variationTheme", {})
                if isinstance(theme, dict):
                    attrs = theme.get("attributes", [])
                    if attrs:
                        return "/".join(attrs)
                elif isinstance(theme, str):
                    return theme
    return None


def _extract_identifiers(item: dict, marketplace_id: str) -> dict:
    out: dict = {}
    for id_group in item.get("identifiers", []):
        mp = id_group.get("marketplaceId")
        if mp and mp != marketplace_id:
            continue
        for ident in id_group.get("identifiers", []):
            t = ident.get("identifierType", "")
            v = ident.get("identifier", "")
            if t == "EAN" and v:
                out["ean"] = v
            elif t == "SKU" and v:
                out["sku"] = v
    return out


def _extract_attributes(item: dict) -> dict:
    attrs: dict = {}
    raw = item.get("attributes", {})
    if not isinstance(raw, dict):
        return attrs
    for key, vals in raw.items():
        key_lower = key.lower()
        if not isinstance(vals, list) or not vals:
            continue
        val = vals[0].get("value", "") if isinstance(vals[0], dict) else str(vals[0])
        if "color" in key_lower or "colour" in key_lower:
            attrs["color"] = val
        elif "size" in key_lower:
            attrs["size"] = val
        elif "material" in key_lower:
            attrs["material"] = val
        elif "model" in key_lower:
            attrs["model"] = val
    return attrs


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def sync_marketplace_listings(
    marketplace_ids: list[str] | None = None,
    *,
    max_asins_per_mp: int = 500,
    family_ids: list[int] | None = None,
) -> dict:
    """
    Sync child listings for non-DE marketplaces.

    1) Reads DE child ASINs from global_family_child.
       If family_ids given, only those families' children.
    2) For each target marketplace, checks which of those ASINs exist
       via Catalog Items API and stores them in marketplace_listing_child.

    Returns summary dict.
    """
    targets = marketplace_ids or NON_DE_MARKETPLACES
    conn = _connect()
    cur = conn.cursor()

    # Gather DE child ASINs (optionally filtered by family)
    if family_ids:
        placeholders = ",".join(["?"] * len(family_ids))
        cur.execute(f"SELECT DISTINCT de_child_asin FROM dbo.global_family_child WHERE global_family_id IN ({placeholders})", *family_ids)
    else:
        cur.execute("SELECT DISTINCT de_child_asin FROM dbo.global_family_child")
    de_asins = [r[0] for r in cur.fetchall()]
    if not de_asins:
        conn.close()
        log.warning("marketplace_sync.no_de_children")
        return {"synced": 0, "marketplaces": 0}

    log.info("marketplace_sync.start", de_children=len(de_asins), targets=len(targets))

    stats: dict = {"synced": 0, "marketplaces": 0, "errors": 0}

    for mp_id in targets:
        mp_code = MARKETPLACE_REGISTRY.get(mp_id, {}).get("code", mp_id)
        catalog = CatalogClient(marketplace_id=mp_id)

        try:
            # Batch lookup: does this ASIN exist in this marketplace?
            items = await catalog.get_items_batch(
                de_asins[:max_asins_per_mp],
                included_data=_INCLUDE,
                batch_size=20,
            )

            for item in items:
                asin = item.get("asin", "")
                if not asin:
                    continue
                ids = _extract_identifiers(item, mp_id)
                attrs = _extract_attributes(item)
                parent = _extract_parent_asin(item, mp_id)
                theme = _extract_variation_theme(item, mp_id)

                _upsert_listing_child(
                    cur, mp_code, asin,
                    ids.get("sku"), ids.get("ean"),
                    parent, theme,
                    json.dumps({k: v for k, v in attrs.items() if v}, ensure_ascii=False) or None,
                )
                stats["synced"] += 1

            stats["marketplaces"] += 1
            log.info("marketplace_sync.mp_done", mp=mp_code, items=len(items))

        except Exception as exc:
            log.error("marketplace_sync.mp_error", mp=mp_code, error=str(exc))
            stats["errors"] += 1

        # Rate-limit between marketplaces
        await asyncio.sleep(1)

    conn.close()
    log.info("marketplace_sync.done", **stats)
    return stats
