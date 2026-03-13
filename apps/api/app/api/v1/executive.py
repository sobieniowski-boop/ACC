"""Executive Command Center API — CEO strategic dashboard."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.concurrency import run_in_threadpool

from app.core.security import require_analyst
from app.schemas.executive import (
    ExecMarketplacesResponse,
    ExecOverviewResponse,
    ExecProductsResponse,
    ExecRecomputeResult,
)
from app.services.executive_service import (
    get_exec_marketplaces,
    get_exec_overview,
    get_exec_products,
    run_executive_pipeline,
)

router = APIRouter(prefix="/executive", tags=["executive"])


@router.get(
    "/overview",
    response_model=ExecOverviewResponse,
    dependencies=[Depends(require_analyst)],
)
async def overview(
    date_from: date = Query(default=None, alias="from"),
    date_to: date = Query(default=None, alias="to"),
    marketplace_id: Optional[str] = Query(default=None),
):
    if date_from is None:
        date_from = date.today() - timedelta(days=30)
    if date_to is None:
        date_to = date.today()
    try:
        return await run_in_threadpool(
            get_exec_overview,
            date_from=date_from,
            date_to=date_to,
            marketplace_id=marketplace_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/products",
    response_model=ExecProductsResponse,
    dependencies=[Depends(require_analyst)],
)
async def products(
    date_from: date = Query(..., alias="from"),
    date_to: date = Query(..., alias="to"),
    marketplace_id: Optional[str] = Query(default=None),
    sku: Optional[str] = Query(default=None),
    sort: str = Query(default="profit_pln"),
    dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    try:
        return await run_in_threadpool(
            get_exec_products,
            date_from=date_from, date_to=date_to,
            marketplace_id=marketplace_id, sku=sku,
            sort=sort, direction=dir,
            page=page, page_size=page_size,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/marketplaces",
    response_model=ExecMarketplacesResponse,
    dependencies=[Depends(require_analyst)],
)
async def marketplaces(
    date_from: date = Query(default=None, alias="from"),
    date_to: date = Query(default=None, alias="to"),
):
    if date_from is None:
        date_from = date.today() - timedelta(days=30)
    if date_to is None:
        date_to = date.today()
    try:
        items = await run_in_threadpool(get_exec_marketplaces, date_from, date_to)
        return {"items": items}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post(
    "/recompute",
    response_model=ExecRecomputeResult,
    dependencies=[Depends(require_analyst)],
)
async def recompute(days_back: int = Query(default=7, ge=1, le=365)):
    try:
        return await run_in_threadpool(run_executive_pipeline, days_back)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
