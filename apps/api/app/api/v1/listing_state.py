"""Listing State API — canonical listing health and operational monitoring.

Provides endpoints for querying, refreshing, and monitoring Amazon listing
state across all marketplaces.
"""
from __future__ import annotations

from typing import Optional

import structlog
from fastapi import APIRouter, HTTPException, Query

from app.services import listing_state

log = structlog.get_logger(__name__)
router = APIRouter(prefix="/listing-state", tags=["listing-state"])


@router.get("/health")
def listing_health(
    marketplace_id: Optional[str] = Query(None),
):
    """Aggregated listing health summary — counts by status, issues, suppression."""
    return listing_state.get_listing_health_summary(marketplace_id=marketplace_id)


@router.get("/listings")
def list_listings(
    marketplace_id: Optional[str] = Query(None),
    listing_status: Optional[str] = Query(None),
    has_issues: Optional[bool] = Query(None),
    is_suppressed: Optional[bool] = Query(None),
    asin: Optional[str] = Query(None),
    sku: Optional[str] = Query(None, description="Partial SKU search"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
):
    """Paginated listing state query with optional filters."""
    return listing_state.get_listing_states(
        marketplace_id=marketplace_id,
        listing_status=listing_status,
        has_issues=has_issues,
        is_suppressed=is_suppressed,
        asin=asin,
        sku_search=sku,
        page=page,
        page_size=page_size,
    )


@router.get("/listings/{seller_sku}")
def get_listing(
    seller_sku: str,
    marketplace_id: str = Query(..., description="Required marketplace ID"),
):
    """Get single listing state by SKU + marketplace."""
    result = listing_state.get_listing_state(seller_sku, marketplace_id)
    if not result:
        raise HTTPException(404, f"No listing state for SKU={seller_sku} in {marketplace_id}")
    return result


@router.get("/listings/{seller_sku}/history")
def listing_history(
    seller_sku: str,
    marketplace_id: str = Query(..., description="Required marketplace ID"),
    limit: int = Query(100, ge=1, le=500),
):
    """Status-change history for a listing, most recent first."""
    return listing_state.get_listing_history(
        seller_sku, marketplace_id, limit=limit,
    )


@router.post("/listings/{seller_sku}/refresh")
async def refresh_listing(
    seller_sku: str,
    marketplace_id: str = Query(..., description="Required marketplace ID"),
):
    """On-demand refresh from SP-API Listings Items API.

    Fetches live data (summaries, attributes, issues) and updates listing state.
    """
    try:
        result = await listing_state.refresh_from_sp_api(seller_sku, marketplace_id)
        return result
    except Exception as exc:
        log.error("listing_state.refresh_failed", sku=seller_sku, error=str(exc))
        raise HTTPException(502, f"SP-API refresh failed: {exc}")
