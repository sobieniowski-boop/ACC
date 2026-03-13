from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.concurrency import run_in_threadpool

from app.connectors.mssql import enqueue_job
from app.core.security import require_analyst, require_director, require_ops
from app.schemas.jobs import JobRunOut
from app.schemas.manage_inventory import (
    InventoryAllResponse,
    InventoryDraftActionResponse,
    InventoryDraftCreate,
    InventoryDraftItem,
    InventoryDraftListResponse,
    InventoryFamilyDetailResponse,
    InventoryFamilyListResponse,
    InventoryJobListResponse,
    InventoryOverviewResponse,
    InventorySettingsResponse,
    InventorySettingsUpdate,
    InventorySkuDetailResponse,
)

router = APIRouter(prefix="/inventory", tags=["manage-inventory"])


@router.get("/overview", response_model=InventoryOverviewResponse, dependencies=[Depends(require_analyst)])
async def inventory_overview(marketplace: Optional[str] = Query(default=None)):
    from app.services.manage_inventory import get_inventory_overview

    marketplace_ids = [part.strip() for part in str(marketplace or "").split(",") if part.strip()] or None
    try:
        return await run_in_threadpool(get_inventory_overview, marketplace_ids=marketplace_ids)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Inventory overview failed: {exc}") from exc


@router.get("/all", response_model=InventoryAllResponse, dependencies=[Depends(require_analyst)])
async def inventory_all(
    marketplace: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    risk_type: Optional[str] = Query(default=None),
    listing_status: Optional[str] = Query(default=None),
):
    from app.services.manage_inventory import list_manage_inventory

    marketplace_ids = [part.strip() for part in str(marketplace or "").split(",") if part.strip()] or None
    try:
        return await run_in_threadpool(
            list_manage_inventory,
            marketplace_ids=marketplace_ids,
            search=search,
            risk_type=risk_type,
            listing_status=listing_status,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Inventory list failed: {exc}") from exc


@router.get("/sku/{sku}", response_model=InventorySkuDetailResponse, dependencies=[Depends(require_analyst)])
async def inventory_sku_detail(sku: str, marketplace_id: Optional[str] = Query(default=None)):
    from app.services.manage_inventory import get_inventory_sku_detail

    try:
        return await run_in_threadpool(get_inventory_sku_detail, sku=sku, marketplace_id=marketplace_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Inventory SKU detail failed: {exc}") from exc


@router.get("/families", response_model=InventoryFamilyListResponse, dependencies=[Depends(require_analyst)])
async def inventory_families(marketplace: Optional[str] = Query(default=None), limit: int = Query(default=200, ge=1, le=500)):
    from app.services.manage_inventory import list_inventory_families

    try:
        return await run_in_threadpool(list_inventory_families, marketplace=marketplace, limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Inventory families failed: {exc}") from exc


@router.get("/families/{parent_asin}", response_model=InventoryFamilyDetailResponse, dependencies=[Depends(require_analyst)])
async def inventory_family_detail(parent_asin: str, marketplace: Optional[str] = Query(default=None)):
    from app.services.manage_inventory import get_inventory_family_detail

    try:
        return await run_in_threadpool(get_inventory_family_detail, parent_asin=parent_asin, marketplace=marketplace)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Inventory family detail failed: {exc}") from exc


@router.get("/drafts", response_model=InventoryDraftListResponse, dependencies=[Depends(require_analyst)])
async def inventory_drafts():
    from app.services.manage_inventory import list_inventory_drafts

    try:
        return await run_in_threadpool(list_inventory_drafts)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Inventory drafts failed: {exc}") from exc


@router.post("/drafts", response_model=InventoryDraftItem, status_code=201, dependencies=[Depends(require_analyst)])
async def create_inventory_draft_route(payload: InventoryDraftCreate):
    from app.services.manage_inventory import create_inventory_draft

    try:
        return await run_in_threadpool(create_inventory_draft, payload.model_dump())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Create inventory draft failed: {exc}") from exc


@router.post("/drafts/{draft_id}/validate", response_model=InventoryDraftActionResponse, dependencies=[Depends(require_ops)])
async def validate_inventory_draft_route(draft_id: str):
    from app.services.manage_inventory import validate_inventory_draft

    try:
        return await run_in_threadpool(validate_inventory_draft, draft_id, None)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Validate inventory draft failed: {exc}") from exc


@router.post("/drafts/{draft_id}/approve", response_model=InventoryDraftActionResponse, dependencies=[Depends(require_director)])
async def approve_inventory_draft_route(draft_id: str):
    from app.services.manage_inventory import approve_inventory_draft

    try:
        return await run_in_threadpool(approve_inventory_draft, draft_id, None)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Approve inventory draft failed: {exc}") from exc


@router.post("/drafts/{draft_id}/apply", response_model=InventoryDraftActionResponse, dependencies=[Depends(require_ops)])
async def apply_inventory_draft_route(draft_id: str):
    from app.services.manage_inventory import apply_inventory_draft

    try:
        return await run_in_threadpool(apply_inventory_draft, draft_id, None)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Apply inventory draft failed: {exc}") from exc


@router.post("/drafts/{draft_id}/apply-job", response_model=JobRunOut, status_code=202, dependencies=[Depends(require_ops)])
async def apply_inventory_draft_job_route(draft_id: str):
    try:
        return await run_in_threadpool(
            enqueue_job,
            job_type="inventory_apply_draft",
            marketplace_id=None,
            trigger_source="manual",
            triggered_by="system",
            params={"draft_id": draft_id},
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Apply inventory draft job failed: {exc}") from exc


@router.post("/drafts/{draft_id}/rollback", response_model=InventoryDraftActionResponse, dependencies=[Depends(require_ops)])
async def rollback_inventory_draft_route(draft_id: str):
    from app.services.manage_inventory import rollback_inventory_draft

    try:
        return await run_in_threadpool(rollback_inventory_draft, draft_id, None)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Rollback inventory draft failed: {exc}") from exc


@router.post("/drafts/{draft_id}/rollback-job", response_model=JobRunOut, status_code=202, dependencies=[Depends(require_ops)])
async def rollback_inventory_draft_job_route(draft_id: str):
    try:
        return await run_in_threadpool(
            enqueue_job,
            job_type="inventory_rollback_draft",
            marketplace_id=None,
            trigger_source="manual",
            triggered_by="system",
            params={"draft_id": draft_id},
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Rollback inventory draft job failed: {exc}") from exc


@router.get("/jobs", response_model=InventoryJobListResponse, dependencies=[Depends(require_analyst)])
async def inventory_jobs():
    from app.services.manage_inventory import get_inventory_jobs

    try:
        return await run_in_threadpool(get_inventory_jobs)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Inventory jobs failed: {exc}") from exc


@router.post("/jobs/run", dependencies=[Depends(require_ops)])
async def run_inventory_job_route(
    job_type: str = Query(..., pattern="^(inventory_sync_listings|inventory_sync_snapshots|inventory_sync_sales_traffic|inventory_compute_rollups|inventory_run_alerts)$"),
):
    from app.services.manage_inventory import run_inventory_job

    try:
        return await run_in_threadpool(run_inventory_job, job_type, {})
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Inventory job failed: {exc}") from exc


@router.get("/settings", response_model=InventorySettingsResponse, dependencies=[Depends(require_analyst)])
async def get_inventory_settings_route():
    from app.services.manage_inventory import get_inventory_settings

    try:
        return await run_in_threadpool(get_inventory_settings)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Inventory settings failed: {exc}") from exc


@router.put("/settings", response_model=InventorySettingsResponse, dependencies=[Depends(require_director)])
async def update_inventory_settings_route(payload: InventorySettingsUpdate):
    from app.services.manage_inventory import update_inventory_settings

    try:
        return await run_in_threadpool(update_inventory_settings, payload.model_dump(exclude_none=True))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Update inventory settings failed: {exc}") from exc
