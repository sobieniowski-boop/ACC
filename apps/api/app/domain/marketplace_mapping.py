"""Unified marketplace mapping — single entry-point for product identity resolution.

Replaces the scattered 4-source cascade (Ergonode → GSheet → Baselinker → ASIN)
with a single canonical lookup backed by ``acc_canonical_product`` and
``acc_marketplace_presence``.

Usage::

    from app.domain.marketplace_mapping import resolve_product, resolve_products_batch

    # Single lookup
    result = resolve_product(sku="FBA_0040010002", marketplace_id="A1PA6795UKMFR9")

    # Batch lookup (more efficient)
    results = resolve_products_batch(
        keys=[("FBA_004001", "A1PA6795UKMFR9"), ("MAG_004002", "A13V1IB3VIYZZH")],
    )
"""
from __future__ import annotations

import structlog
import pyodbc
from dataclasses import dataclass
from typing import Sequence

from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class CanonicalMatch:
    """Resolved canonical product identity."""
    internal_sku: str
    ean: str | None
    brand: str | None
    category: str | None
    k_number: str | None
    ergonode_id: str | None
    mapping_source: str | None
    confidence: float | None


@dataclass(frozen=True, slots=True)
class MappingResult:
    """Result of a single (seller_sku, marketplace_id) lookup."""
    seller_sku: str
    marketplace_id: str
    canonical: CanonicalMatch | None
    source: str  # 'canonical', 'presence', 'legacy', 'none'


# ---------------------------------------------------------------------------
# Core lookup — single product
# ---------------------------------------------------------------------------

def resolve_product(
    *,
    sku: str | None = None,
    asin: str | None = None,
    ean: str | None = None,
    marketplace_id: str | None = None,
) -> CanonicalMatch | None:
    """Resolve a product to its canonical identity.

    Lookup order:
      1. ``acc_marketplace_presence`` → ``acc_canonical_product`` (by seller_sku + marketplace)
      2. ``acc_canonical_product`` directly (by internal_sku or EAN)
      3. Returns None if no match found.

    This replaces the Ergonode → GSheet → Baselinker → ASIN cascade.
    """
    conn = connect_acc(autocommit=True, timeout=10)
    try:
        cur = conn.cursor()
        # Strategy 1: seller_sku + marketplace → presence → canonical
        if sku and marketplace_id:
            match = _lookup_via_presence(cur, sku, marketplace_id)
            if match:
                return match

        # Strategy 2: ASIN + marketplace → presence → canonical
        if asin and marketplace_id:
            match = _lookup_via_asin(cur, asin, marketplace_id)
            if match:
                return match

        # Strategy 3: EAN → canonical product directly
        if ean:
            match = _lookup_via_ean(cur, ean)
            if match:
                return match

        # Strategy 4: SKU as internal_sku directly on canonical table
        if sku:
            match = _lookup_via_internal_sku(cur, sku)
            if match:
                return match

        return None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Batch lookup — efficient for pipelines
# ---------------------------------------------------------------------------

def resolve_products_batch(
    keys: Sequence[tuple[str, str]],
) -> dict[tuple[str, str], CanonicalMatch | None]:
    """Batch resolve (seller_sku, marketplace_id) → CanonicalMatch.

    Returns a dict keyed by (seller_sku, marketplace_id) with CanonicalMatch or None.
    Uses a single DB connection and two bulk queries for efficiency.
    """
    if not keys:
        return {}

    conn = connect_acc(autocommit=True, timeout=15)
    try:
        cur = conn.cursor()
        results: dict[tuple[str, str], CanonicalMatch | None] = {}

        # Load all presences for the requested SKUs in one query
        sku_list = list({k[0] for k in keys})
        presence_map = _bulk_load_presences(cur, sku_list)

        # Load all canonical products referenced by presences
        internal_skus = list({p["internal_sku"] for p in presence_map.values()})
        canonical_map = _bulk_load_canonicals(cur, internal_skus) if internal_skus else {}

        for seller_sku, marketplace_id in keys:
            pkey = (seller_sku, marketplace_id)
            presence = presence_map.get(pkey)
            if presence:
                canon = canonical_map.get(presence["internal_sku"])
                if canon:
                    results[pkey] = canon
                    continue
            results[pkey] = None

        return results
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _lookup_via_presence(
    cur: pyodbc.Cursor, seller_sku: str, marketplace_id: str
) -> CanonicalMatch | None:
    cur.execute(
        """
        SELECT cp.internal_sku, cp.ean, cp.brand, cp.category,
               cp.k_number, cp.ergonode_id, cp.mapping_source, cp.mapping_confidence
        FROM dbo.acc_marketplace_presence mp WITH (NOLOCK)
        JOIN dbo.acc_canonical_product cp WITH (NOLOCK)
            ON cp.internal_sku = mp.internal_sku
        WHERE mp.seller_sku = ? AND mp.marketplace_id = ?
        """,
        (seller_sku, marketplace_id),
    )
    row = cur.fetchone()
    return _row_to_match(row) if row else None


def _lookup_via_asin(
    cur: pyodbc.Cursor, asin: str, marketplace_id: str
) -> CanonicalMatch | None:
    cur.execute(
        """
        SELECT TOP 1
               cp.internal_sku, cp.ean, cp.brand, cp.category,
               cp.k_number, cp.ergonode_id, cp.mapping_source, cp.mapping_confidence
        FROM dbo.acc_marketplace_presence mp WITH (NOLOCK)
        JOIN dbo.acc_canonical_product cp WITH (NOLOCK)
            ON cp.internal_sku = mp.internal_sku
        WHERE mp.asin = ? AND mp.marketplace_id = ?
        """,
        (asin, marketplace_id),
    )
    row = cur.fetchone()
    return _row_to_match(row) if row else None


def _lookup_via_ean(cur: pyodbc.Cursor, ean: str) -> CanonicalMatch | None:
    cur.execute(
        """
        SELECT TOP 1
               internal_sku, ean, brand, category,
               k_number, ergonode_id, mapping_source, mapping_confidence
        FROM dbo.acc_canonical_product WITH (NOLOCK)
        WHERE ean = ?
        """,
        (ean,),
    )
    row = cur.fetchone()
    return _row_to_match(row) if row else None


def _lookup_via_internal_sku(cur: pyodbc.Cursor, sku: str) -> CanonicalMatch | None:
    cur.execute(
        """
        SELECT internal_sku, ean, brand, category,
               k_number, ergonode_id, mapping_source, mapping_confidence
        FROM dbo.acc_canonical_product WITH (NOLOCK)
        WHERE internal_sku = ?
        """,
        (sku,),
    )
    row = cur.fetchone()
    return _row_to_match(row) if row else None


def _row_to_match(row) -> CanonicalMatch:
    return CanonicalMatch(
        internal_sku=row[0],
        ean=row[1],
        brand=row[2],
        category=row[3],
        k_number=row[4],
        ergonode_id=row[5],
        mapping_source=row[6],
        confidence=float(row[7]) if row[7] is not None else None,
    )


def _bulk_load_presences(
    cur: pyodbc.Cursor, sku_list: list[str],
) -> dict[tuple[str, str], dict]:
    """Load marketplace presences for a list of seller SKUs."""
    if not sku_list:
        return {}
    result: dict[tuple[str, str], dict] = {}
    # Process in chunks to avoid parameter limit
    chunk_size = 500
    for i in range(0, len(sku_list), chunk_size):
        chunk = sku_list[i : i + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        cur.execute(
            f"""
            SELECT seller_sku, marketplace_id, internal_sku
            FROM dbo.acc_marketplace_presence WITH (NOLOCK)
            WHERE seller_sku IN ({placeholders})
            """,
            chunk,
        )
        for row in cur.fetchall():
            result[(row[0], row[1])] = {"internal_sku": row[2]}
    return result


def _bulk_load_canonicals(
    cur: pyodbc.Cursor, internal_skus: list[str],
) -> dict[str, CanonicalMatch]:
    """Load canonical products for a list of internal SKUs."""
    if not internal_skus:
        return {}
    result: dict[str, CanonicalMatch] = {}
    chunk_size = 500
    for i in range(0, len(internal_skus), chunk_size):
        chunk = internal_skus[i : i + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        cur.execute(
            f"""
            SELECT internal_sku, ean, brand, category,
                   k_number, ergonode_id, mapping_source, mapping_confidence
            FROM dbo.acc_canonical_product WITH (NOLOCK)
            WHERE internal_sku IN ({placeholders})
            """,
            chunk,
        )
        for row in cur.fetchall():
            result[row[0]] = _row_to_match(row)
    return result
