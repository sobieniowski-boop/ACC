"""Catalog Health API — unified health scorecard, diffs & suppression.

Sprint 9 – S9.5
"""
from __future__ import annotations

from typing import Optional

import structlog
from fastapi import APIRouter, HTTPException, Query

from app.intelligence import catalog_health

log = structlog.get_logger(__name__)
router = APIRouter(prefix="/catalog-health", tags=["catalog-health"])


@router.get("/scorecard")
def catalog_scorecard(
    marketplace_id: Optional[str] = Query(None),
):
    """Full catalog health scorecard — totals, rates, per-marketplace, content coverage."""
    return catalog_health.get_catalog_scorecard(marketplace_id=marketplace_id)


@router.get("/listing/{seller_sku}")
def listing_health_detail(
    seller_sku: str,
    marketplace_id: str = Query(..., description="Required marketplace ID"),
):
    """Per-listing health breakdown with scored components."""
    result = catalog_health.get_listing_health_detail(seller_sku, marketplace_id)
    if not result:
        raise HTTPException(404, f"No listing found for SKU={seller_sku} in {marketplace_id}")
    return result


@router.get("/suppressions")
def suppressions(
    marketplace_id: Optional[str] = Query(None),
    days: int = Query(30, ge=1, le=365),
):
    """Suppression timeline and trends — daily new/recovered counts + top reasons."""
    return catalog_health.get_suppression_timeline(
        days=days, marketplace_id=marketplace_id,
    )


@router.get("/suppressions/details")
def suppression_details(
    marketplace_id: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
):
    """Currently suppressed listings with reasons and metadata."""
    return catalog_health.get_suppression_details(
        marketplace_id=marketplace_id, limit=limit,
    )


@router.get("/diffs")
def recent_diffs(
    seller_sku: Optional[str] = Query(None),
    marketplace_id: Optional[str] = Query(None),
    field_name: Optional[str] = Query(None),
    days: int = Query(7, ge=1, le=90),
    limit: int = Query(100, ge=1, le=500),
):
    """Recent field-level changes across listings."""
    return catalog_health.get_recent_diffs(
        seller_sku=seller_sku,
        marketplace_id=marketplace_id,
        field_name=field_name,
        days=days,
        limit=limit,
    )


@router.get("/diffs/summary")
def diff_summary(
    marketplace_id: Optional[str] = Query(None),
    days: int = Query(7, ge=1, le=90),
):
    """Aggregate field-change statistics over a time window."""
    return catalog_health.get_diff_summary(
        days=days, marketplace_id=marketplace_id,
    )


@router.get("/worst")
def worst_performers(
    marketplace_id: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
):
    """Bottom-N listings by health score from latest snapshot."""
    return catalog_health.get_worst_performers(
        marketplace_id=marketplace_id, limit=limit,
    )


@router.get("/trends")
def health_trends(
    marketplace_id: Optional[str] = Query(None),
    days: int = Query(30, ge=1, le=365),
):
    """Daily health score averages from snapshot history."""
    return catalog_health.get_health_trends(
        days=days, marketplace_id=marketplace_id,
    )


@router.post("/snapshot")
def trigger_snapshot(
    marketplace_id: Optional[str] = Query(None),
):
    """Manually trigger health score snapshot computation."""
    return catalog_health.compute_and_persist_health_snapshots(
        marketplace_id=marketplace_id,
    )
