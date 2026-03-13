"""Returns Tracker API — dashboards, item list, manual overrides, sync trigger."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from app.connectors.mssql import enqueue_job
from app.core.config import settings
from app.schemas.jobs import JobRunOut
from app.services.return_tracker import (
    get_return_dashboard,
    get_return_items,
    update_return_status,
    seed_return_items_from_orders,
    reconcile_returns,
    rebuild_daily_summary,
    sync_fba_returns,
    backfill_fba_returns,
)

router = APIRouter(prefix="/returns", tags=["returns"])


# ──────────────────── Request / Response Models ────────────────────────

class ReturnStatusOverride(BaseModel):
    financial_status: str = Field(..., description="sellable_return | damaged_return | lost_in_transit | reimbursed | pending")
    note: str | None = Field(None, description="Optional note from warehouse team")
    updated_by: str = Field("admin")


class SyncRequest(BaseModel):
    days_back: int = Field(30, ge=1, le=365)
    marketplace_ids: list[str] | None = None


class SeedRequest(BaseModel):
    date_from: date | None = None
    date_to: date | None = None


# ──────────────────── Endpoints ────────────────────────

@router.get("/dashboard")
async def return_dashboard(
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
):
    """
    Return Tracker dashboard with KPIs:
    - Total refunds / returns / sellable rate
    - COGS recovered vs written off vs pending
    - Breakdown by marketplace
    - Top returned SKUs
    - Pending items needing attention
    """
    if date_from is None:
        date_from = date.today() - timedelta(days=30)
    if date_to is None:
        date_to = date.today()

    try:
        data = await run_in_threadpool(
            get_return_dashboard,
            date_from=date_from,
            date_to=date_to,
            marketplace_id=marketplace_id,
        )
        return data
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)[:500])


@router.get("/items")
async def return_items_list(
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    financial_status: Optional[str] = Query(default=None),
    sku_search: Optional[str] = Query(default=None),
    sort_by: str = Query(default="refund_date"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    """Paginated list of return items with filters."""
    if date_from is None:
        date_from = date.today() - timedelta(days=30)
    if date_to is None:
        date_to = date.today()

    try:
        data = await run_in_threadpool(
            get_return_items,
            date_from=date_from,
            date_to=date_to,
            marketplace_id=marketplace_id,
            financial_status=financial_status,
            sku_search=sku_search,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            page_size=page_size,
        )
        return data
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)[:500])


@router.put("/items/{item_id}/status")
async def override_return_status(
    item_id: int,
    body: ReturnStatusOverride,
):
    """
    Manual override of a return item's financial status.
    Used by warehouse team when they physically verify the returned item.
    """
    try:
        result = await run_in_threadpool(
            update_return_status,
            return_item_id=item_id,
            financial_status=body.financial_status,
            note=body.note,
            updated_by=body.updated_by,
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)[:500])


@router.post("/seed", response_model=JobRunOut, status_code=202)
async def seed_returns(body: SeedRequest | None = None):
    """
    Seed acc_return_item from refunded orders (acc_order.is_refund=1).
    Idempotent — only inserts items not yet tracked.
    """
    d_from = body.date_from if body and body.date_from else date.today() - timedelta(days=90)
    d_to = body.date_to if body and body.date_to else date.today()

    try:
        return await run_in_threadpool(
            enqueue_job,
            job_type="returns_seed_items",
            marketplace_id=None,
            trigger_source="manual",
            triggered_by=settings.DEFAULT_ACTOR,
            params={
                "date_from": d_from.isoformat(),
                "date_to": d_to.isoformat(),
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)[:500])


@router.post("/reconcile", response_model=JobRunOut, status_code=202)
async def run_reconcile():
    """
    Match FBA customer returns (physical) with return items (financial).
    Updates disposition + financial_status from FBA report data.
    Marks items pending 45+ days as lost_in_transit.
    """
    try:
        return await run_in_threadpool(
            enqueue_job,
            job_type="returns_reconcile",
            marketplace_id=None,
            trigger_source="manual",
            triggered_by=settings.DEFAULT_ACTOR,
            params={},
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)[:500])


@router.post("/rebuild-summary", response_model=JobRunOut, status_code=202)
async def rebuild_summary(
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
):
    """Rebuild daily summary from return items (acc_return_daily_summary)."""
    d_from = date_from or (date.today() - timedelta(days=90))
    d_to = date_to or date.today()

    try:
        return await run_in_threadpool(
            enqueue_job,
            job_type="returns_rebuild_summary",
            marketplace_id=None,
            trigger_source="manual",
            triggered_by=settings.DEFAULT_ACTOR,
            params={
                "date_from": d_from.isoformat(),
                "date_to": d_to.isoformat(),
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)[:500])


@router.post("/sync", response_model=JobRunOut, status_code=202)
async def trigger_sync(body: SyncRequest | None = None):
    """
    Trigger full FBA returns sync pipeline:
    1. Download FBA Customer Returns report per marketplace
    2. Parse + upsert raw data (uses watermark for incremental sync)
    3. Seed new return items from refunded orders
    4. Reconcile physical returns with financial items
    5. Rebuild daily summary
    """
    days_back = body.days_back if body else 30
    mkt_ids = body.marketplace_ids if body else None

    try:
        return await run_in_threadpool(
            enqueue_job,
            job_type="returns_sync_fba",
            marketplace_id=None,
            trigger_source="manual",
            triggered_by=settings.DEFAULT_ACTOR,
            params={
                "days_back": days_back,
                "marketplace_ids": mkt_ids or [],
                "use_watermark": True,
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)[:500])


class BackfillRequest(BaseModel):
    days_back: int = Field(90, ge=1, le=365)
    marketplace_ids: list[str] | None = None
    chunk_days: int = Field(30, ge=7, le=60)


@router.post("/backfill", response_model=JobRunOut, status_code=202)
async def trigger_backfill(body: BackfillRequest | None = None):
    """
    Backfill historical FBA Customer Returns data.
    Splits time range into chunks to avoid report timeout.
    Use this once to fill historical data — daily sync handles ongoing.
    """
    days_back = body.days_back if body else 90
    mkt_ids = body.marketplace_ids if body else None
    chunk_days = body.chunk_days if body else 30

    try:
        return await run_in_threadpool(
            enqueue_job,
            job_type="returns_backfill_fba",
            marketplace_id=None,
            trigger_source="manual",
            triggered_by=settings.DEFAULT_ACTOR,
            params={
                "days_back": days_back,
                "marketplace_ids": mkt_ids or [],
                "chunk_days": chunk_days,
            },
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)[:500])
