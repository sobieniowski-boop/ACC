"""Product Type Definitions (PTD) cache layer.

Fetches Amazon Product Type Definitions via SP-API and persists them in
``dbo.acc_ptd_cache`` for offline / high-speed access.

Key capabilities:
  • ``ensure_ptd_cache_schema()`` — idempotent DDL on startup
  • ``sync_ptd_for_marketplace()`` — fetch PTD for all known product types in a marketplace
  • ``get_cached_ptd()`` — retrieve cached definition JSON (never SP-API)
  • ``refresh_ptd()`` — force-refresh a single (product_type, marketplace) pair
  • ``list_cached_ptds()`` — list what's cached with freshness metadata
  • ``get_stale_ptds()`` — entries older than ``max_age_days``

Design decisions:
  1. Schema JSON is stored compressed (gzip) because PTD payloads average 200-400 KB.
  2. Freshness tracked via ``fetched_at`` + ``schema_version_hash`` (SHA-256 of body).
  3. ``product_type`` is UPPER-cased on storage for consistency.
  4. No in-memory cache — SQL Server is fast enough and this avoids stale-process issues.
"""
from __future__ import annotations

import gzip
import hashlib
import json
from datetime import date, datetime, timezone
from typing import Any

import structlog

from app.core.config import settings, MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

# Freshness threshold — PTDs rarely change, weekly refresh is sufficient.
DEFAULT_MAX_AGE_DAYS = 7


def _connect():
    return connect_acc(autocommit=False, timeout=30)


# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

def ensure_ptd_cache_schema() -> None:
    """No-op — schema managed by Alembic migration eb017."""


# ---------------------------------------------------------------------------
# SP-API fetch
# ---------------------------------------------------------------------------

async def _fetch_ptd_from_api(
    product_type: str,
    marketplace_id: str,
    *,
    requirements: str = "LISTING",
    locale: str = "DEFAULT",
) -> dict[str, Any]:
    """Call SP-API Product Type Definitions endpoint. Returns raw JSON dict."""
    from app.connectors.amazon_sp_api.listings import ListingsClient

    client = ListingsClient(marketplace_id=marketplace_id)
    return await client.get_product_type_definition(
        product_type,
        requirements=requirements,
        locale=locale,
    )


def _hash_schema(payload: bytes) -> str:
    """SHA-256 hash of the compressed schema for change detection."""
    return hashlib.sha256(payload).hexdigest()


def _extract_metadata(schema: dict) -> dict[str, Any]:
    """Pull structural counts from PTD schema JSON for quick queries."""
    property_groups = len(schema.get("propertyGroups", {}))

    # Count attributes and required ones
    json_schema = schema.get("schema", {})
    properties = json_schema.get("properties", {})
    total_attrs = len(properties)

    required = set(json_schema.get("required", []))
    required_count = len(required)

    # Variation detection
    has_variations = "child_parent_sku_relationship" in properties
    variation_theme = None
    if has_variations:
        vt_prop = properties.get("child_parent_sku_relationship", {})
        items = vt_prop.get("items", {})
        vt_inner = items.get("properties", {}).get("child_relationship_type", {})
        enum_vals = vt_inner.get("enum", [])
        if enum_vals:
            variation_theme = ",".join(enum_vals[:20])

    return {
        "property_groups": property_groups,
        "required_attributes": required_count,
        "total_attributes": total_attrs,
        "has_variations": has_variations,
        "variation_theme": variation_theme,
    }


# ---------------------------------------------------------------------------
# Cache CRUD
# ---------------------------------------------------------------------------

def _upsert_ptd(
    conn,
    product_type: str,
    marketplace_id: str,
    requirements: str,
    locale: str,
    schema_json: dict,
) -> str:
    """Insert or update a PTD entry. Returns 'created' or 'updated'."""
    pt = product_type.upper()
    raw_json = json.dumps(schema_json, separators=(",", ":"), ensure_ascii=False)
    compressed = gzip.compress(raw_json.encode("utf-8"), compresslevel=6)
    version_hash = _hash_schema(compressed)
    meta = _extract_metadata(schema_json)

    cur = conn.cursor()
    cur.execute("""
        SELECT id, schema_version_hash
        FROM dbo.acc_ptd_cache WITH (NOLOCK)
        WHERE product_type = ? AND marketplace_id = ?
          AND requirements = ? AND locale = ?
    """, (pt, marketplace_id, requirements, locale))
    row = cur.fetchone()

    if row:
        existing_hash = row[1]
        if existing_hash == version_hash:
            # Schema unchanged — just bump fetched_at
            cur.execute("""
                UPDATE dbo.acc_ptd_cache
                SET fetched_at = SYSUTCDATETIME(),
                    updated_at = SYSUTCDATETIME()
                WHERE id = ?
            """, (row[0],))
            conn.commit()
            return "unchanged"

        cur.execute("""
            UPDATE dbo.acc_ptd_cache
            SET schema_json_gz       = ?,
                schema_size_bytes    = ?,
                schema_version_hash  = ?,
                property_groups      = ?,
                required_attributes  = ?,
                total_attributes     = ?,
                has_variations       = ?,
                variation_theme      = ?,
                fetched_at           = SYSUTCDATETIME(),
                updated_at           = SYSUTCDATETIME()
            WHERE id = ?
        """, (
            compressed,
            len(compressed),
            version_hash,
            meta["property_groups"],
            meta["required_attributes"],
            meta["total_attributes"],
            1 if meta["has_variations"] else 0,
            meta["variation_theme"],
            row[0],
        ))
        conn.commit()
        return "updated"
    else:
        cur.execute("""
            SET LOCK_TIMEOUT 30000;
            INSERT INTO dbo.acc_ptd_cache (
                product_type, marketplace_id, requirements, locale,
                schema_json_gz, schema_size_bytes, schema_version_hash,
                property_groups, required_attributes, total_attributes,
                has_variations, variation_theme,
                fetched_at, created_at, updated_at
            ) VALUES (
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                SYSUTCDATETIME(), SYSUTCDATETIME(), SYSUTCDATETIME()
            )
        """, (
            pt, marketplace_id, requirements, locale,
            compressed, len(compressed), version_hash,
            meta["property_groups"], meta["required_attributes"], meta["total_attributes"],
            1 if meta["has_variations"] else 0, meta["variation_theme"],
        ))
        conn.commit()
        return "created"


def get_cached_ptd(
    product_type: str,
    marketplace_id: str,
    *,
    requirements: str = "LISTING",
    locale: str = "DEFAULT",
) -> dict[str, Any] | None:
    """Retrieve cached PTD schema JSON. Returns None if not cached."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT schema_json_gz, fetched_at, schema_version_hash,
                   property_groups, required_attributes, total_attributes,
                   has_variations, variation_theme
            FROM dbo.acc_ptd_cache WITH (NOLOCK)
            WHERE product_type = ? AND marketplace_id = ?
              AND requirements = ? AND locale = ?
        """, (product_type.upper(), marketplace_id, requirements, locale))
        row = cur.fetchone()
        if not row:
            return None

        gz_data = row[0]
        if not gz_data:
            return None

        schema = json.loads(gzip.decompress(bytes(gz_data)).decode("utf-8"))
        return {
            "product_type": product_type.upper(),
            "marketplace_id": marketplace_id,
            "schema": schema,
            "fetched_at": str(row[1]),
            "schema_version_hash": row[2],
            "metadata": {
                "property_groups": row[3],
                "required_attributes": row[4],
                "total_attributes": row[5],
                "has_variations": bool(row[6]),
                "variation_theme": row[7],
            },
        }
    finally:
        conn.close()


def list_cached_ptds(
    marketplace_id: str | None = None,
) -> list[dict[str, Any]]:
    """List all cached PTD entries with freshness metadata (no schema body)."""
    conn = _connect()
    try:
        cur = conn.cursor()
        where = "WHERE 1=1"
        params: list[Any] = []
        if marketplace_id:
            where += " AND marketplace_id = ?"
            params.append(marketplace_id)

        cur.execute(f"""
            SELECT product_type, marketplace_id, requirements, locale,
                   schema_size_bytes, schema_version_hash,
                   property_groups, required_attributes, total_attributes,
                   has_variations, variation_theme,
                   fetched_at, created_at, updated_at
            FROM dbo.acc_ptd_cache WITH (NOLOCK)
            {where}
            ORDER BY product_type, marketplace_id
        """, params)

        results = []
        for r in cur.fetchall():
            fetched = r[11]
            age_days = (datetime.now(timezone.utc) - fetched.replace(tzinfo=timezone.utc)).days if fetched else None
            results.append({
                "product_type": r[0],
                "marketplace_id": r[1],
                "requirements": r[2],
                "locale": r[3],
                "schema_size_bytes": r[4],
                "schema_version_hash": r[5],
                "property_groups": r[6],
                "required_attributes": r[7],
                "total_attributes": r[8],
                "has_variations": bool(r[9]),
                "variation_theme": r[10],
                "fetched_at": str(fetched),
                "created_at": str(r[12]),
                "updated_at": str(r[13]),
                "age_days": age_days,
                "is_stale": (age_days or 999) > DEFAULT_MAX_AGE_DAYS,
            })
        return results
    finally:
        conn.close()


def get_stale_ptds(max_age_days: int = DEFAULT_MAX_AGE_DAYS) -> list[dict[str, Any]]:
    """Return PTD entries older than max_age_days."""
    all_ptds = list_cached_ptds()
    return [p for p in all_ptds if (p.get("age_days") or 999) > max_age_days]


# ---------------------------------------------------------------------------
# Sync operations
# ---------------------------------------------------------------------------

async def refresh_ptd(
    product_type: str,
    marketplace_id: str,
    *,
    requirements: str = "LISTING",
    locale: str = "DEFAULT",
) -> dict[str, Any]:
    """Fetch a PTD from SP-API and upsert into cache. Returns status dict."""
    import asyncio

    schema_json = await _fetch_ptd_from_api(
        product_type, marketplace_id,
        requirements=requirements, locale=locale,
    )

    conn = _connect()
    try:
        status = _upsert_ptd(conn, product_type, marketplace_id,
                             requirements, locale, schema_json)
        meta = _extract_metadata(schema_json)
        log.info("ptd_cache.refreshed",
                 product_type=product_type, marketplace_id=marketplace_id,
                 status=status, attrs=meta["total_attributes"])
        return {
            "product_type": product_type.upper(),
            "marketplace_id": marketplace_id,
            "status": status,
            **meta,
        }
    finally:
        conn.close()


async def discover_product_types_for_marketplace(
    marketplace_id: str,
) -> list[str]:
    """Discover product types used by this seller in a marketplace.

    Strategy: query acc_listing_state + acc_product for known product_type values;
    if none, fall back to a default set of common KADAX types.
    """
    conn = _connect()
    try:
        cur = conn.cursor()
        # Try listing_state first (has product_type if populated from Listings Items API)
        ptypes: set[str] = set()

        # From listing report data — check acc_product.product_type
        cur.execute("""
            SELECT DISTINCT p.product_type
            FROM dbo.acc_product p WITH (NOLOCK)
            WHERE p.product_type IS NOT NULL
              AND p.product_type != ''
              AND p.product_type != 'PRODUCT'
        """)
        for r in cur.fetchall():
            if r[0]:
                ptypes.add(r[0].upper())

        if not ptypes:
            # Fallback: common KADAX product types (household/garden category)
            ptypes = {
                "HOME", "KITCHEN", "OUTDOOR_LIVING", "SEEDS",
                "PLANTERS", "PATIO_FURNITURE", "HOME_BED_AND_BATH",
            }
            log.info("ptd_cache.using_default_product_types",
                     marketplace_id=marketplace_id, count=len(ptypes))

        return sorted(ptypes)
    finally:
        conn.close()


async def sync_ptd_for_marketplace(
    marketplace_id: str,
    *,
    force: bool = False,
    max_age_days: int = DEFAULT_MAX_AGE_DAYS,
) -> dict[str, Any]:
    """Sync all known product types for a marketplace.

    Skips entries that are fresh enough (< max_age_days) unless ``force=True``.
    Rate-limits SP-API calls with 1s delay between requests.
    """
    import asyncio

    product_types = await discover_product_types_for_marketplace(marketplace_id)
    results = {"marketplace_id": marketplace_id, "synced": 0, "skipped": 0, "errors": 0, "details": []}

    for pt in product_types:
        # Check freshness
        if not force:
            existing = list_cached_ptds(marketplace_id)
            cached = next((p for p in existing if p["product_type"] == pt), None)
            if cached and not cached["is_stale"]:
                results["skipped"] += 1
                continue

        try:
            detail = await refresh_ptd(pt, marketplace_id)
            results["synced"] += 1
            results["details"].append(detail)
        except Exception as exc:
            log.warning("ptd_cache.sync_error",
                        product_type=pt, marketplace_id=marketplace_id,
                        error=str(exc))
            results["errors"] += 1
            results["details"].append({
                "product_type": pt,
                "marketplace_id": marketplace_id,
                "status": "error",
                "error": str(exc)[:200],
            })

        # Rate limit: SP-API definitions ~2 req/s
        await asyncio.sleep(0.6)

    # Update sync state
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SET LOCK_TIMEOUT 30000;
            MERGE dbo.acc_ptd_sync_state AS tgt
            USING (SELECT ? AS marketplace_id) AS src
            ON tgt.marketplace_id = src.marketplace_id
            WHEN MATCHED THEN
                UPDATE SET last_synced_at = SYSUTCDATETIME(),
                           product_types_count = ?,
                           last_error = ?,
                           updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (marketplace_id, last_synced_at, product_types_count, last_error, updated_at)
                VALUES (?, SYSUTCDATETIME(), ?, ?, SYSUTCDATETIME());
        """, (
            marketplace_id,
            results["synced"] + results["skipped"],
            str(results["errors"]) if results["errors"] else None,
            marketplace_id,
            results["synced"] + results["skipped"],
            str(results["errors"]) if results["errors"] else None,
        ))
        conn.commit()
    finally:
        conn.close()

    log.info("ptd_cache.marketplace_sync_done", **{k: v for k, v in results.items() if k != "details"})
    return results


async def sync_all_marketplaces(*, force: bool = False) -> dict[str, Any]:
    """Sync PTD cache for all 9 KADAX marketplaces."""
    totals = {"marketplaces": 0, "synced": 0, "skipped": 0, "errors": 0}
    for mkt_id in MARKETPLACE_REGISTRY:
        result = await sync_ptd_for_marketplace(mkt_id, force=force)
        totals["marketplaces"] += 1
        totals["synced"] += result["synced"]
        totals["skipped"] += result["skipped"]
        totals["errors"] += result["errors"]
    return totals
