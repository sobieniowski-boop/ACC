"""Catalog Definitions API — PTD cache + schema-driven validation.

Endpoints for browsing cached Product Type Definitions, triggering
refresh from SP-API, validating listing payloads, and comparing
requirements across marketplaces.
"""
from __future__ import annotations

from typing import Optional

import structlog
from fastapi import APIRouter, HTTPException, Query

from app.schemas.catalog_definitions import (
    MarketplaceDiffResponse,
    PTDListResponse,
    PTDRefreshResult,
    PTDSchemaResponse,
    ValidateRequest,
    ValidateResponse,
    VariationInfoResponse,
)
from app.services import ptd_cache, ptd_validator

log = structlog.get_logger(__name__)
router = APIRouter(prefix="/catalog/definitions", tags=["catalog-definitions"])


# ---------------------------------------------------------------------------
# Browse cache
# ---------------------------------------------------------------------------

@router.get("", response_model=PTDListResponse)
def list_definitions(
    marketplace_id: Optional[str] = Query(None),
):
    """List all cached PTD entries with freshness metadata."""
    entries = ptd_cache.list_cached_ptds(marketplace_id=marketplace_id)
    return PTDListResponse(count=len(entries), entries=entries)


@router.get("/{product_type}", response_model=PTDSchemaResponse)
def get_definition(
    product_type: str,
    marketplace_id: str = Query(..., description="Amazon marketplace ID"),
):
    """Get full cached PTD schema for a product type + marketplace."""
    cached = ptd_cache.get_cached_ptd(product_type, marketplace_id)
    if not cached:
        raise HTTPException(
            404,
            f"No cached PTD for {product_type} / {marketplace_id}. "
            f"Trigger a refresh first.",
        )
    return cached


@router.get("/{product_type}/required-attributes")
def get_required_attrs(
    product_type: str,
    marketplace_id: str = Query(...),
):
    """List required attributes for a product type in a marketplace."""
    attrs = ptd_validator.get_required_attributes(product_type, marketplace_id)
    if attrs is None:
        raise HTTPException(404, f"No cached PTD for {product_type} / {marketplace_id}")
    return {"product_type": product_type.upper(), "marketplace_id": marketplace_id,
            "required": attrs, "count": len(attrs)}


@router.get("/{product_type}/variations", response_model=VariationInfoResponse)
def get_variations(
    product_type: str,
    marketplace_id: str = Query(...),
):
    """Get variation theme info for a product type."""
    info = ptd_validator.get_variation_info(product_type, marketplace_id)
    if info is None:
        raise HTTPException(404, f"No cached PTD for {product_type} / {marketplace_id}")
    return info


# ---------------------------------------------------------------------------
# Refresh
# ---------------------------------------------------------------------------

@router.post("/refresh", response_model=PTDRefreshResult)
async def refresh_definitions(
    product_type: Optional[str] = Query(None),
    marketplace_id: Optional[str] = Query(None),
    force: bool = Query(False),
):
    """Trigger PTD cache refresh from SP-API.

    - No params → refresh all marketplaces (async, may take minutes)
    - marketplace_id only → refresh all product types for that marketplace
    - product_type + marketplace_id → refresh one specific definition
    """
    if product_type and marketplace_id:
        detail = await ptd_cache.refresh_ptd(product_type, marketplace_id)
        return PTDRefreshResult(
            marketplace_id=marketplace_id,
            synced=1,
            details=[detail],
        )
    elif marketplace_id:
        result = await ptd_cache.sync_ptd_for_marketplace(marketplace_id, force=force)
        return result
    else:
        # Full sync — all marketplaces
        totals = await ptd_cache.sync_all_marketplaces(force=force)
        return PTDRefreshResult(
            marketplace_id="ALL",
            synced=totals["synced"],
            skipped=totals["skipped"],
            errors=totals["errors"],
        )


# ---------------------------------------------------------------------------
# Validate
# ---------------------------------------------------------------------------

@router.post("/validate", response_model=ValidateResponse)
def validate_payload(body: ValidateRequest):
    """Validate a listing payload against the cached PTD schema.

    Returns issues list with severity (error/warning/info).
    """
    result = ptd_validator.validate_listing_payload(
        body.product_type, body.marketplace_id, body.attributes,
    )
    return result.to_dict()


# ---------------------------------------------------------------------------
# Cross-marketplace diff
# ---------------------------------------------------------------------------

@router.get("/{product_type}/diff", response_model=MarketplaceDiffResponse)
def diff_requirements(
    product_type: str,
    marketplace_ids: Optional[str] = Query(
        None,
        description="Comma-separated marketplace IDs. Omit for all 9 EU.",
    ),
):
    """Compare required attributes for a product type across marketplaces."""
    mkt_list = None
    if marketplace_ids:
        mkt_list = [m.strip() for m in marketplace_ids.split(",") if m.strip()]
    return ptd_validator.diff_marketplace_requirements(product_type, mkt_list)


# ---------------------------------------------------------------------------
# Stale entries
# ---------------------------------------------------------------------------

@router.get("/stale", tags=["catalog-definitions"])
def stale_definitions(max_age_days: int = Query(7, ge=1)):
    """List PTD cache entries older than max_age_days."""
    stale = ptd_cache.get_stale_ptds(max_age_days=max_age_days)
    return {"count": len(stale), "entries": stale}
