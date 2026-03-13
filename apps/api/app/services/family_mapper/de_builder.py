"""
DE Canonical Builder — pull Amazon.de parent/child structures from SP-API
Catalog Items and populate global_family + global_family_child.

Uses Catalog Items API v2022-04-01 with includedData=relationships,summaries,
identifiers,attributes to extract variation themes, child ASINs, SKUs, EANs,
and variation attributes (color, size, material, model).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Optional

import asyncio
import structlog

from app.connectors.amazon_sp_api.catalog import CatalogClient
from app.core.config import settings
from app.core.db_connection import connect_acc
from app.services.family_mapper.master_key import build_master_key

log = structlog.get_logger(__name__)

DE_MARKETPLACE = settings.SP_API_PRIMARY_MARKETPLACE  # A1PA6795UKMFR9

# Catalog API includedData for relationship discovery
_INCLUDE_FULL = "summaries,relationships,identifiers,attributes"

# ---------------------------------------------------------------------------
# Rebuild status tracking (in-memory, single-worker safe)
# ---------------------------------------------------------------------------
_rebuild_status: dict = {
    "running": False,
    "phase": "idle",
    "detail": "",
    "progress": 0,
    "total": 0,
    "result": None,
}

def get_rebuild_status() -> dict:
    """Return current rebuild status snapshot."""
    return dict(_rebuild_status)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _extract_variation_theme(item: dict, marketplace_id: str) -> str | None:
    """Extract variationTheme from relationships or summaries."""
    # Try relationships first
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
    # Fallback to summaries
    for summary in item.get("summaries", []):
        if summary.get("marketplaceId") == marketplace_id:
            return summary.get("variationTheme")
    return None


def _extract_child_asins(item: dict, marketplace_id: str) -> list[str]:
    """Get child ASINs from parent's relationships."""
    children: list[str] = []
    for rel_group in item.get("relationships", []):
        if rel_group.get("marketplaceId") == marketplace_id:
            for rel in rel_group.get("relationships", []):
                child_asin = rel.get("childAsins", [])
                if isinstance(child_asin, list):
                    children.extend(child_asin)
                # Type can also be top-level asin
                asin = rel.get("asin")
                if asin:
                    children.append(asin)
    return list(dict.fromkeys(children))  # dedupe, preserve order


def _extract_brand(item: dict, marketplace_id: str) -> str | None:
    for summary in item.get("summaries", []):
        if summary.get("marketplaceId") == marketplace_id:
            return summary.get("brand") or summary.get("brandName")
    return None


def _extract_category(item: dict, marketplace_id: str) -> str | None:
    for summary in item.get("summaries", []):
        if summary.get("marketplaceId") == marketplace_id:
            # SP-API uses browseClassification (singular object)
            bc = summary.get("browseClassification")
            if bc:
                return bc.get("displayName")
            # Fallback: classifications array (older format)
            cls = summary.get("classifications", [])
            if cls:
                return cls[0].get("displayName")
    return None


def _extract_product_type(item: dict, marketplace_id: str) -> str | None:
    for summary in item.get("summaries", []):
        if summary.get("marketplaceId") == marketplace_id:
            # Try productType first, then websiteDisplayGroupName
            pt = summary.get("productType")
            if pt:
                return pt
            return summary.get("websiteDisplayGroupName") or summary.get("websiteDisplayGroup")
    return None


def _extract_identifiers(item: dict, marketplace_id: str) -> dict:
    """Extract EAN, SKU from identifiers block."""
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
            elif t == "UPC" and v and "ean" not in out:
                out["ean"] = v  # treat UPC as fallback
    return out


def _extract_attributes(item: dict, marketplace_id: str) -> dict:
    """Pull color, size, material, model from attributes block."""
    attrs: dict = {}
    raw_attrs = item.get("attributes", {})
    if isinstance(raw_attrs, dict):
        # SP-API attributes come as {attr_name: [{value, ...}]}
        for key, vals in raw_attrs.items():
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
            elif "model" in key_lower or "model_number" in key_lower:
                attrs["model"] = val
    return attrs


# ---------------------------------------------------------------------------
# DB persist (sync pyodbc — consistent with rest of codebase)
# ---------------------------------------------------------------------------

def _connect():
    return connect_acc(autocommit=True)


def _upsert_family(cur, parent_asin: str, brand: str | None,
                    category: str | None, product_type: str | None,
                    variation_theme: str | None) -> int:
    """MERGE global_family on de_parent_asin, return id."""
    cur.execute("""
        MERGE dbo.global_family AS tgt
        USING (SELECT ? AS de_parent_asin) AS src
            ON tgt.de_parent_asin = src.de_parent_asin
        WHEN MATCHED THEN
            UPDATE SET brand = ?, category = ?, product_type = ?,
                       variation_theme_de = ?
        WHEN NOT MATCHED THEN
            INSERT (de_parent_asin, brand, category, product_type,
                    variation_theme_de, created_at)
            VALUES (?, ?, ?, ?, ?, SYSUTCDATETIME())
        OUTPUT inserted.id;
    """,
        parent_asin,
        brand, category, product_type, variation_theme,
        parent_asin, brand, category, product_type, variation_theme,
    )
    row = cur.fetchone()
    return row[0] if row else 0


def _upsert_child(cur, family_id: int, master_key: str, key_type: str,
                   child_asin: str, sku: str | None, ean: str | None,
                   attributes_json: str | None) -> None:
    """MERGE global_family_child on (global_family_id, master_key)."""
    cur.execute("""
        MERGE dbo.global_family_child AS tgt
        USING (SELECT ? AS global_family_id, ? AS master_key) AS src
            ON tgt.global_family_id = src.global_family_id
           AND tgt.master_key = src.master_key
        WHEN MATCHED THEN
            UPDATE SET de_child_asin = ?, sku_de = ?, ean_de = ?,
                       attributes_json = ?, key_type = ?
        WHEN NOT MATCHED THEN
            INSERT (global_family_id, master_key, key_type, de_child_asin,
                    sku_de, ean_de, attributes_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
    """,
        family_id, master_key,
        child_asin, sku, ean, attributes_json, key_type,
        family_id, master_key, key_type, child_asin, sku, ean, attributes_json,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def rebuild_de_canonical(
    parent_asins: list[str] | None = None,
    *,
    max_parents: int = 200,
    brand_filter: str | None = None,
    only_missing: bool = False,
) -> dict:
    """
    Rebuild DE canonical families.

    If parent_asins is None, fetch from existing acc_product parents
    (distinct parent ASINs in DE marketplace).

    brand_filter: if set, only process products whose SKU, title or brand
    matches this string (e.g. 'KADAX').

    only_missing: if True, skip parent ASINs that already have a row
    in global_family (only discover new ones).

    Returns summary dict with counts.
    """
    if _rebuild_status["running"]:
        return {"error": "Rebuild already running", "phase": _rebuild_status["phase"]}

    _rebuild_status.update(running=True, phase="init", detail="", progress=0, total=0, result=None)
    try:
        return await _rebuild_de_inner(parent_asins, max_parents=max_parents,
                                        brand_filter=brand_filter, only_missing=only_missing)
    finally:
        _rebuild_status["running"] = False
        _rebuild_status["phase"] = "done"


async def _rebuild_de_inner(
    parent_asins: list[str] | None = None,
    *,
    max_parents: int = 200,
    brand_filter: str | None = None,
    only_missing: bool = False,
) -> dict:
    """Inner rebuild logic."""
    catalog = CatalogClient(marketplace_id=DE_MARKETPLACE)
    conn = _connect()
    cur = conn.cursor()

    # ----- gather parent ASINs -----
    if not parent_asins:
        # Build optional brand filter clause
        brand_clause = ""
        brand_params: list[str] = []
        if brand_filter:
            brand_clause = " AND (sku LIKE ? OR title LIKE ? OR brand LIKE ?)"
            bp = f"%{brand_filter}%"
            brand_params = [bp, bp, bp]

        # Step 1: pull already-known parent ASINs from acc_product
        cur.execute(f"""
            SELECT DISTINCT parent_asin
            FROM dbo.acc_product
            WHERE parent_asin IS NOT NULL
              AND LEN(parent_asin) > 0
              AND is_parent = 0
              {brand_clause}
            ORDER BY parent_asin
        """, *brand_params)
        known_parents: set[str] = {r[0] for r in cur.fetchall()}
        log.info("de_builder.known_parents", count=len(known_parents))

        # Step 2: ALWAYS discover parents for children that have no
        # parent_asin yet via SP-API relationships data.
        cur.execute(f"""
            SELECT DISTINCT asin
            FROM dbo.acc_product
            WHERE asin IS NOT NULL AND LEN(asin) > 0
              AND is_parent = 0
              AND (parent_asin IS NULL OR LEN(parent_asin) = 0)
              {brand_clause}
            ORDER BY asin
        """, *brand_params)
        undiscovered = [r[0] for r in cur.fetchall()]
        log.info("de_builder.undiscovered_children", count=len(undiscovered))

        discovered: set[str] = set()
        if undiscovered:
            _rebuild_status.update(phase="discovery",
                                   detail=f"Scanning {len(undiscovered)} children for parents",
                                   total=len(undiscovered))
            log.info("de_builder.discovering_parents",
                     msg="Scanning children without parent_asin via Catalog API")
            child_items = await catalog.get_items_batch(
                undiscovered,
                included_data="relationships,summaries",
                batch_size=20,
            )
            for item in child_items:
                for rel_group in item.get("relationships", []):
                    if rel_group.get("marketplaceId") != DE_MARKETPLACE:
                        continue
                    for rel in rel_group.get("relationships", []):
                        pa = rel.get("parentAsins", [])
                        if isinstance(pa, list):
                            discovered.update(pa)
                        single = rel.get("parentAsin")
                        if single:
                            discovered.add(single)
            log.info("de_builder.discovered_parents",
                     new=len(discovered - known_parents),
                     total=len(discovered))

            # Backfill parent_asin on acc_product for future runs
            backfilled = 0
            for item in child_items:
                child_asin = item.get("asin")
                for rel_group in item.get("relationships", []):
                    if rel_group.get("marketplaceId") != DE_MARKETPLACE:
                        continue
                    for rel in rel_group.get("relationships", []):
                        pa_list = rel.get("parentAsins", [])
                        pa_single = rel.get("parentAsin")
                        pa = (pa_list[0] if pa_list else pa_single) if (pa_list or pa_single) else None
                        if pa and child_asin:
                            cur.execute("""
                                UPDATE dbo.acc_product
                                SET parent_asin = ?
                                WHERE asin = ? AND (parent_asin IS NULL OR parent_asin = '')
                            """, pa, child_asin)
                            backfilled += cur.rowcount
            conn.commit()
            log.info("de_builder.backfilled_parent_asins", count=backfilled)

        # Merge known + discovered
        parent_asins = sorted(known_parents | discovered)

    # Filter out already-known families if only_missing=True
    if only_missing and parent_asins:
        placeholders = ",".join(["?"] * len(parent_asins))
        cur.execute(f"""
            SELECT de_parent_asin FROM dbo.global_family
            WHERE de_parent_asin IN ({placeholders})
        """, *parent_asins)
        existing = {r[0] for r in cur.fetchall()}
        before = len(parent_asins)
        parent_asins = [a for a in parent_asins if a not in existing]
        log.info("de_builder.skip_existing",
                 before=before, existing=len(existing),
                 remaining=len(parent_asins))

    parent_asins = parent_asins[:max_parents]
    log.info("de_builder.start", parents=len(parent_asins))

    stats = {"families": 0, "children": 0, "errors": 0}
    _rebuild_status.update(phase="processing", detail="Building families",
                           progress=0, total=len(parent_asins))

    for idx, parent_asin in enumerate(parent_asins):
        _rebuild_status["progress"] = idx + 1
        _rebuild_status["detail"] = f"Family {idx+1}/{len(parent_asins)}: {parent_asin}"
        try:
            # Fetch parent from Catalog API with relationships
            parent_data = await catalog.get_item(
                parent_asin, included_data=_INCLUDE_FULL,
            )

            brand = _extract_brand(parent_data, DE_MARKETPLACE)
            category = _extract_category(parent_data, DE_MARKETPLACE)
            product_type = _extract_product_type(parent_data, DE_MARKETPLACE)
            variation_theme = _extract_variation_theme(parent_data, DE_MARKETPLACE)

            family_id = _upsert_family(
                cur, parent_asin, brand, category, product_type, variation_theme,
            )
            stats["families"] += 1

            # Get child ASINs from relationships
            child_asins = _extract_child_asins(parent_data, DE_MARKETPLACE)
            if not child_asins:
                # Single-product family — the parent IS the single child
                child_asins = [parent_asin]

            # Fetch each child's details (batch of 20)
            child_items = await catalog.get_items_batch(
                child_asins,
                included_data="summaries,identifiers,attributes",
                batch_size=20,
            )

            # Build a lookup map
            child_map = {ci.get("asin"): ci for ci in child_items}

            # Fallback: if parent had no brand/category/product_type,
            # inherit from the first child that has summaries data.
            if not brand or not category or not product_type:
                for ci in child_items:
                    if not brand:
                        brand = _extract_brand(ci, DE_MARKETPLACE)
                    if not category:
                        category = _extract_category(ci, DE_MARKETPLACE)
                    if not product_type:
                        product_type = _extract_product_type(ci, DE_MARKETPLACE)
                    if brand and category and product_type:
                        break
                # Re-upsert the family with enriched metadata
                if brand or category or product_type:
                    _upsert_family(
                        cur, parent_asin, brand, category, product_type,
                        variation_theme,
                    )

            for child_asin in child_asins:
                child_data = child_map.get(child_asin, {})
                ids = _extract_identifiers(child_data, DE_MARKETPLACE)
                attrs = _extract_attributes(child_data, DE_MARKETPLACE)

                mk, kt, attrs_json = build_master_key(
                    sku=ids.get("sku"),
                    ean=ids.get("ean"),
                    brand=brand,
                    mpn=None,
                    model=attrs.get("model"),
                    size=attrs.get("size"),
                    color=attrs.get("color"),
                    material=attrs.get("material"),
                )

                _upsert_child(
                    cur, family_id, mk, kt,
                    child_asin, ids.get("sku"), ids.get("ean"), attrs_json,
                )
                stats["children"] += 1

        except Exception as exc:
            log.error("de_builder.parent_error", parent=parent_asin, error=str(exc))
            stats["errors"] += 1

    conn.close()
    log.info("de_builder.done", **stats)
    _rebuild_status.update(phase="done", detail="Complete", result=stats)
    return stats
