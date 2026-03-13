"""Profitability API — overview dashboard, orders, products, marketplace, simulator."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from starlette.responses import Response

from app.core.security import require_analyst
from app.schemas.profitability import (
    MarketplaceProfitabilityResponse,
    PriceSimulatorRequest,
    PriceSimulatorResult,
    ProfitabilityOrdersResponse,
    ProfitabilityOverviewResponse,
    ProfitabilityProductsResponse,
    RollupJobResult,
)
from app.services.profitability_service import (
    get_marketplace_profitability,
    get_profitability_orders,
    get_profitability_overview,
    get_profitability_products,
    recompute_rollups,
    simulate_price,
)

_DEPRECATION_HEADERS = {
    "Deprecation": "true",
    "Sunset": "2026-05-01",
    "Link": '</profit/v2/>; rel="successor-version"',
}

router = APIRouter(prefix="/profitability", tags=["profit"])


# ---------------------------------------------------------------------------
# Overview dashboard
# ---------------------------------------------------------------------------

@router.get(
    "/overview",
    response_model=ProfitabilityOverviewResponse,
    dependencies=[Depends(require_analyst)],
    deprecated=True,
)
async def overview(
    response: Response,
    date_from: date = Query(default=None, alias="from"),
    date_to: date = Query(default=None, alias="to"),
    marketplace_id: Optional[str] = Query(default=None),
):
    for k, v in _DEPRECATION_HEADERS.items():
        response.headers[k] = v
    if date_from is None:
        date_from = date.today() - timedelta(days=30)
    if date_to is None:
        date_to = date.today()
    try:
        result = await run_in_threadpool(
            get_profitability_overview,
            date_from=date_from,
            date_to=date_to,
            marketplace_id=marketplace_id,
        )
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Orders table (paginated, filterable)
# ---------------------------------------------------------------------------

@router.get(
    "/orders",
    response_model=ProfitabilityOrdersResponse,
    dependencies=[Depends(require_analyst)],
    deprecated=True,
)
async def orders(
    response: Response,
    date_from: date = Query(..., alias="from"),
    date_to: date = Query(..., alias="to"),
    marketplace_id: Optional[str] = Query(default=None),
    sku: Optional[str] = Query(default=None),
    loss_only: bool = Query(default=False),
    min_margin: Optional[float] = Query(default=None),
    max_margin: Optional[float] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    for k, v in _DEPRECATION_HEADERS.items():
        response.headers[k] = v
    try:
        return await run_in_threadpool(
            get_profitability_orders,
            date_from=date_from,
            date_to=date_to,
            marketplace_id=marketplace_id,
            sku=sku,
            loss_only=loss_only,
            min_margin=min_margin,
            max_margin=max_margin,
            page=page,
            page_size=page_size,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Products / SKU table (from rollup, paginated)
# ---------------------------------------------------------------------------

@router.get(
    "/products",
    response_model=ProfitabilityProductsResponse,
    dependencies=[Depends(require_analyst)],
    deprecated=True,
)
async def products(
    response: Response,
    date_from: date = Query(..., alias="from"),
    date_to: date = Query(..., alias="to"),
    marketplace_id: Optional[str] = Query(default=None),
    sku: Optional[str] = Query(default=None),
    sort_by: str = Query(default="profit_pln"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    for k, v in _DEPRECATION_HEADERS.items():
        response.headers[k] = v
    try:
        return await run_in_threadpool(
            get_profitability_products,
            date_from=date_from,
            date_to=date_to,
            marketplace_id=marketplace_id,
            sku=sku,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            page_size=page_size,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Marketplace breakdown
# ---------------------------------------------------------------------------

@router.get(
    "/marketplaces",
    response_model=MarketplaceProfitabilityResponse,
    dependencies=[Depends(require_analyst)],
    deprecated=True,
)
async def marketplaces(
    response: Response,
    date_from: date = Query(..., alias="from"),
    date_to: date = Query(..., alias="to"),
):
    for k, v in _DEPRECATION_HEADERS.items():
        response.headers[k] = v
    try:
        items = await run_in_threadpool(
            get_marketplace_profitability,
            date_from=date_from,
            date_to=date_to,
        )
        return {"items": items}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Price simulator
# ---------------------------------------------------------------------------

@router.post(
    "/simulate",
    response_model=PriceSimulatorResult,
    dependencies=[Depends(require_analyst)],
    deprecated=True,
)
async def price_simulator(response: Response, payload: PriceSimulatorRequest):
    for k, v in _DEPRECATION_HEADERS.items():
        response.headers[k] = v
    try:
        return simulate_price(
            sale_price=payload.sale_price,
            purchase_cost=payload.purchase_cost,
            shipping_cost=payload.shipping_cost,
            amazon_fee_pct=payload.amazon_fee_pct,
            fba_fee=payload.fba_fee,
            ad_cost=payload.ad_cost,
            currency=payload.currency,
            fx_rate=payload.fx_rate,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Manual rollup recompute trigger
# ---------------------------------------------------------------------------

@router.post(
    "/recompute",
    response_model=RollupJobResult,
    dependencies=[Depends(require_analyst)],
    deprecated=True,
)
async def trigger_recompute(
    response: Response,
    days_back: int = Query(default=7, ge=1, le=365),
):
    for k, v in _DEPRECATION_HEADERS.items():
        response.headers[k] = v
    try:
        return await run_in_threadpool(recompute_rollups, days_back=days_back)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
