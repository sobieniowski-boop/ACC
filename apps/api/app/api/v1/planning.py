from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool

from app.connectors.mssql import (
    enqueue_job,
    create_plan_month,
    delete_plan_month,
    get_plan_vs_actual,
    list_plan_months,
    refresh_plan_actuals,
    update_plan_status,
)
from app.core.config import settings
from app.schemas.jobs import JobRunOut
from app.schemas.planning import (
    PlanMonthCreate,
    PlanMonthOut,
    PlanStatusUpdate,
    PlanVsActualResponse,
)

router = APIRouter(prefix="/planning", tags=["planning"])


@router.get("/months", response_model=list[PlanMonthOut])
async def planning_months(
    year: int | None = Query(default=None),
):
    try:
        return await run_in_threadpool(list_plan_months, year)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Planning months failed: {exc}") from exc


@router.post("/months", response_model=PlanMonthOut, status_code=201)
async def planning_create(payload: PlanMonthCreate):
    try:
        return await run_in_threadpool(create_plan_month, payload.model_dump(), settings.DEFAULT_ACTOR)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Create plan failed: {exc}") from exc


@router.patch("/months/{plan_id}/status", response_model=PlanMonthOut)
async def planning_status(plan_id: int, payload: PlanStatusUpdate):
    try:
        return await run_in_threadpool(update_plan_status, plan_id, payload.status)
    except RuntimeError as exc:
        if "not found" in str(exc).lower():
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Status update failed: {exc}") from exc


@router.delete("/months/{plan_id}")
async def planning_delete(plan_id: int):
    try:
        await run_in_threadpool(delete_plan_month, plan_id)
        return {"status": "deleted", "plan_id": plan_id}
    except RuntimeError as exc:
        if "not found" in str(exc).lower():
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Delete plan failed: {exc}") from exc


@router.get("/vs-actual", response_model=PlanVsActualResponse)
async def planning_vs_actual(year: int = Query(..., ge=2024, le=2035)):
    try:
        return await run_in_threadpool(get_plan_vs_actual, year)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Plan vs actual failed: {exc}") from exc


@router.post("/refresh", response_model=JobRunOut, status_code=202)
async def planning_refresh(plan_id: int | None = Query(default=None), year: int | None = Query(default=None)):
    try:
        return await run_in_threadpool(
            enqueue_job,
            job_type="planning_refresh_actuals",
            marketplace_id=None,
            trigger_source="manual",
            triggered_by=settings.DEFAULT_ACTOR,
            params={
                "plan_id": plan_id,
                "year": year,
            },
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Refresh failed: {exc}") from exc
