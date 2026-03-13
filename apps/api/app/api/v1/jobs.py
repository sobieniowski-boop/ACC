from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool

from app.connectors.mssql import enqueue_job, get_job, list_jobs
from app.core.config import settings
from app.schemas.jobs import JobListResponse, JobRunOut, JobRunRequest

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("/run", response_model=JobRunOut, status_code=202)
async def run_job(payload: JobRunRequest):
    try:
        job = await run_in_threadpool(
            enqueue_job,
            job_type=payload.job_type,
            marketplace_id=payload.marketplace_id,
            trigger_source="manual",
            triggered_by=settings.DEFAULT_ACTOR,
            params=payload.params,
        )
        return job
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Job run failed: {exc}") from exc


@router.get("", response_model=JobListResponse)
async def jobs_list(
    job_type: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=30, ge=1, le=200),
):
    try:
        return await run_in_threadpool(
            list_jobs,
            job_type=job_type,
            status=status,
            page=page,
            page_size=page_size,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Jobs query failed: {exc}") from exc


@router.post("/import-cogs", response_model=JobRunOut, status_code=202)
async def trigger_cogs_import():
    """Manually trigger COGS import from 'cogs from sell' folder."""
    try:
        return await run_in_threadpool(
            enqueue_job,
            job_type="cogs_import",
            marketplace_id=None,
            trigger_source="manual",
            triggered_by=settings.DEFAULT_ACTOR,
            params={},
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"COGS import failed: {exc}") from exc


@router.post("/sync-listings", response_model=JobRunOut, status_code=202)
async def trigger_sync_listings(
    marketplace_ids: str | None = Query(
        default=None,
        description="Comma-separated marketplace IDs (default: all)",
    ),
):
    """Fetch GET_MERCHANT_LISTINGS_ALL_DATA for all marketplaces and upsert new products."""
    try:
        mkt_list = [m.strip() for m in marketplace_ids.split(",") if m.strip()] if marketplace_ids else None
        return await run_in_threadpool(
            enqueue_job,
            job_type="sync_listings_to_products",
            marketplace_id=None,
            trigger_source="manual",
            triggered_by=settings.DEFAULT_ACTOR,
            params={"marketplace_ids": mkt_list or []},
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Listings sync failed: {exc}") from exc

@router.get("/{job_id}", response_model=JobRunOut)
async def job_details(job_id: str):
    try:
        job = await run_in_threadpool(get_job, job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return job
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Job fetch failed: {exc}") from exc
