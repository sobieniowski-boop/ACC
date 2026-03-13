"""Content Optimization API — scoring, SEO analysis, optimisation opportunities.

Sprint 17 – S17.3
"""
from __future__ import annotations

from typing import Optional

import structlog
from fastapi import APIRouter, HTTPException, Query

from app.intelligence import content_optimization as co

log = structlog.get_logger(__name__)
router = APIRouter(prefix="/content-optimization", tags=["content-optimization"])


@router.get("/scores")
def list_scores(
    marketplace_id: Optional[str] = Query(None),
    min_score: Optional[int] = Query(None, ge=0, le=100),
    max_score: Optional[int] = Query(None, ge=0, le=100),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Paginated content quality scores with optional filters."""
    return co.get_content_scores(
        marketplace_id,
        min_score=min_score,
        max_score=max_score,
        limit=limit,
        offset=offset,
    )


@router.get("/scores/{seller_sku}")
def score_detail(
    seller_sku: str,
    marketplace_id: str = Query(..., description="Required marketplace ID"),
):
    """Content score detail for a single SKU."""
    result = co.get_content_score_for_sku(seller_sku, marketplace_id)
    if not result:
        raise HTTPException(404, f"No content score for SKU={seller_sku} in {marketplace_id}")
    return result


@router.get("/distribution")
def score_distribution(
    marketplace_id: Optional[str] = Query(None),
):
    """Score distribution buckets and averages for dashboard KPIs."""
    return co.get_score_distribution(marketplace_id)


@router.get("/opportunities")
def top_opportunities(
    marketplace_id: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
):
    """Listings with lowest content scores — biggest improvement opportunities."""
    return co.get_top_opportunities(marketplace_id, limit=limit)


@router.get("/history/{seller_sku}")
def score_history(
    seller_sku: str,
    marketplace_id: str = Query(..., description="Required marketplace ID"),
    days: int = Query(30, ge=1, le=365),
):
    """Content score trend over time for a listing."""
    return co.get_score_history(seller_sku, marketplace_id, days=days)


@router.get("/seo/{seller_sku}")
def seo_detail(
    seller_sku: str,
    marketplace_id: str = Query(..., description="Required marketplace ID"),
):
    """SEO analysis for a single SKU."""
    result = co.get_seo_analysis_for_sku(seller_sku, marketplace_id)
    if not result:
        raise HTTPException(404, f"No SEO analysis for SKU={seller_sku} in {marketplace_id}")
    return result


@router.post("/compute")
def trigger_scoring(
    marketplace_id: str = Query(..., description="Marketplace to score"),
    limit: int = Query(500, ge=1, le=5000),
):
    """Trigger content scoring computation for a marketplace."""
    try:
        result = co.score_listings_for_marketplace(marketplace_id, limit=limit)
        return result
    except Exception as exc:
        log.error("content_optimization.compute.error", error=str(exc))
        raise HTTPException(500, f"Scoring failed: {exc}")
