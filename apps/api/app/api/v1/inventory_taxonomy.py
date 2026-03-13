from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.concurrency import run_in_threadpool

from app.connectors.mssql import enqueue_job
from app.core.security import require_analyst, require_director, require_ops
from app.schemas.jobs import JobRunOut
from app.schemas.taxonomy import (
    TaxonomyPredictionListResponse,
    TaxonomyRefreshResponse,
    TaxonomyReviewResponse,
)

router = APIRouter(prefix="/inventory/taxonomy", tags=["inventory-taxonomy"])


@router.post("/refresh", response_model=JobRunOut, dependencies=[Depends(require_ops)], status_code=202)
async def refresh_inventory_taxonomy(
    limit: int = Query(default=40000, ge=1, le=200000),
    min_auto_confidence: float = Query(default=0.90, ge=0.0, le=1.0),
    auto_apply: bool = Query(default=True),
):
    try:
        return await run_in_threadpool(
            enqueue_job,
            job_type="inventory_taxonomy_refresh",
            marketplace_id=None,
            trigger_source="manual",
            triggered_by="system",
            params={
                "limit": limit,
                "min_auto_confidence": min_auto_confidence,
                "auto_apply": auto_apply,
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Taxonomy refresh failed: {exc}") from exc


@router.get("/predictions", response_model=TaxonomyPredictionListResponse, dependencies=[Depends(require_analyst)])
async def list_inventory_taxonomy_predictions(
    status: str | None = Query(default=None),
    min_confidence: float = Query(default=0.0, ge=0.0, le=1.0),
    limit: int = Query(default=200, ge=1, le=2000),
):
    from app.services.taxonomy import list_taxonomy_predictions

    try:
        items = await run_in_threadpool(
            list_taxonomy_predictions,
            status=status,
            min_confidence=min_confidence,
            limit=limit,
        )
        return {"items": items, "total": len(items)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Taxonomy list failed: {exc}") from exc


@router.post("/predictions/{prediction_id}/review", response_model=TaxonomyReviewResponse, dependencies=[Depends(require_director)])
async def review_inventory_taxonomy_prediction(
    prediction_id: str,
    action: Literal["approve", "reject"] = Query(...),
):
    from app.services.taxonomy import review_taxonomy_prediction

    try:
        return await run_in_threadpool(
            review_taxonomy_prediction,
            prediction_id=prediction_id,
            action=action,
            actor="system",
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Taxonomy review failed: {exc}") from exc
