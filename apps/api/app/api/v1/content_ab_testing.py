"""Content A/B Testing & Multi-language API.

Sprint 18 – S18.4
"""
from __future__ import annotations

from typing import Optional

import structlog
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.intelligence import content_ab_testing as cab

log = structlog.get_logger(__name__)
router = APIRouter(prefix="/content-optimization", tags=["content-optimization"])


# ── Request bodies ───────────────────────────────────────────────────

class GenerateMultilangRequest(BaseModel):
    seller_sku: str
    source_marketplace_id: str
    asin: str | None = None
    target_markets: list[str] | None = None

class CreateExperimentRequest(BaseModel):
    name: str
    seller_sku: str
    marketplace_id: str
    hypothesis: str | None = None
    metric_primary: str = "conversion_rate"
    created_by: str | None = None

class AddVariantRequest(BaseModel):
    label: str
    version_id: str | None = None
    is_control: bool = False
    content_score: int | None = None

class RecordMetricsRequest(BaseModel):
    impressions: int | None = None
    clicks: int | None = None
    orders: int | None = None
    revenue: float | None = None


# ── Multi-language endpoints ─────────────────────────────────────────

@router.post("/multilang/generate")
def generate_multilang(body: GenerateMultilangRequest):
    """Generate localized content for all target markets."""
    try:
        return cab.generate_all_languages(
            seller_sku=body.seller_sku,
            source_marketplace_id=body.source_marketplace_id,
            asin=body.asin,
            target_markets=body.target_markets,
        )
    except Exception as exc:
        log.error("multilang.generate.error", error=str(exc))
        raise HTTPException(500, f"Generation failed: {exc}")


@router.post("/multilang/generate-single")
def generate_single_lang(
    seller_sku: str = Query(...),
    source_marketplace_id: str = Query(...),
    target_marketplace_id: str = Query(...),
    target_language: str = Query(...),
    asin: str | None = Query(None),
):
    """Generate content for a single target market."""
    try:
        return cab.generate_multilang_content(
            seller_sku=seller_sku,
            source_marketplace_id=source_marketplace_id,
            target_marketplace_id=target_marketplace_id,
            target_language=target_language,
            asin=asin,
        )
    except Exception as exc:
        log.error("multilang.single.error", error=str(exc))
        raise HTTPException(500, f"Generation failed: {exc}")


@router.get("/multilang/jobs")
def list_multilang_jobs(
    seller_sku: Optional[str] = Query(None),
    source_marketplace_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """List multi-language generation jobs."""
    return cab.get_multilang_jobs(
        seller_sku,
        source_marketplace_id=source_marketplace_id,
        status=status,
        limit=limit,
        offset=offset,
    )


@router.get("/multilang/coverage/{seller_sku}")
def multilang_coverage(
    seller_sku: str,
    source_marketplace_id: str = Query(...),
):
    """Get language coverage for a SKU across all markets."""
    return cab.get_multilang_coverage(seller_sku, source_marketplace_id)


# ── A/B Experiment endpoints ────────────────────────────────────────

@router.post("/experiments")
def create_experiment(body: CreateExperimentRequest):
    """Create a new A/B content experiment."""
    try:
        return cab.create_experiment(
            name=body.name,
            seller_sku=body.seller_sku,
            marketplace_id=body.marketplace_id,
            hypothesis=body.hypothesis,
            metric_primary=body.metric_primary,
            created_by=body.created_by,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@router.get("/experiments")
def list_experiments(
    marketplace_id: Optional[str] = Query(None),
    seller_sku: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """List experiments with filters."""
    return cab.list_experiments(
        marketplace_id,
        seller_sku=seller_sku,
        status=status,
        limit=limit,
        offset=offset,
    )


@router.get("/experiments/summary")
def experiment_summary(marketplace_id: Optional[str] = Query(None)):
    """Dashboard summary of experiment activity."""
    return cab.get_experiment_summary(marketplace_id)


@router.get("/experiments/{experiment_id}")
def get_experiment(experiment_id: int):
    """Get experiment detail with variants."""
    result = cab.get_experiment(experiment_id)
    if not result:
        raise HTTPException(404, f"Experiment {experiment_id} not found")
    return result


@router.post("/experiments/{experiment_id}/variants")
def add_variant(experiment_id: int, body: AddVariantRequest):
    """Add a variant to an experiment."""
    try:
        return cab.add_variant(
            experiment_id=experiment_id,
            label=body.label,
            version_id=body.version_id,
            is_control=body.is_control,
            content_score=body.content_score,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@router.post("/experiments/{experiment_id}/start")
def start_experiment(experiment_id: int):
    """Start an experiment (draft → running)."""
    try:
        return cab.start_experiment(experiment_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@router.post("/experiments/{experiment_id}/conclude")
def conclude_experiment(experiment_id: int):
    """Conclude an experiment and declare winner."""
    try:
        return cab.conclude_experiment(experiment_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@router.post("/experiments/variants/{variant_id}/metrics")
def record_metrics(variant_id: int, body: RecordMetricsRequest):
    """Record performance metrics for a variant."""
    return cab.record_variant_metrics(
        variant_id=variant_id,
        impressions=body.impressions,
        clicks=body.clicks,
        orders=body.orders,
        revenue=body.revenue,
    )
