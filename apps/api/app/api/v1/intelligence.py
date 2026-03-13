"""Intelligence Hub API — unified intelligence dashboard + opportunity funnel."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import ORJSONResponse

from app.core.security import require_analyst
from app.services.intelligence_service import (
    get_forecast_accuracy,
    get_opportunity_funnel,
    get_unified_dashboard,
)

router = APIRouter(prefix="/intelligence", tags=["intelligence"])


@router.get(
    "/dashboard",
    response_class=ORJSONResponse,
    dependencies=[Depends(require_analyst)],
)
async def unified_dashboard(
    date_from: date = Query(default=None, alias="from"),
    date_to: date = Query(default=None, alias="to"),
    marketplace_id: Optional[str] = Query(default=None),
):
    """Unified intelligence dashboard aggregating all 9 modules concurrently."""
    if date_from is None:
        date_from = date.today() - timedelta(days=30)
    if date_to is None:
        date_to = date.today()
    try:
        return await get_unified_dashboard(
            date_from=date_from,
            date_to=date_to,
            marketplace_id=marketplace_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/funnel",
    response_class=ORJSONResponse,
    dependencies=[Depends(require_analyst)],
)
async def opportunity_funnel(
    marketplace_id: Optional[str] = Query(default=None),
):
    """Opportunity pipeline funnel: detected → accepted → completed → measured."""
    try:
        return await get_opportunity_funnel(marketplace_id=marketplace_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/forecast-accuracy",
    response_class=ORJSONResponse,
    dependencies=[Depends(require_analyst)],
)
async def forecast_accuracy(
    opportunity_type: Optional[str] = Query(default=None),
):
    """Forecast accuracy: compare predicted vs actual outcomes by opportunity type."""
    try:
        return await get_forecast_accuracy(opportunity_type=opportunity_type)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
