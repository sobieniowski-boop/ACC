"""Decision Intelligence — Outcomes & Learning API endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from starlette.concurrency import run_in_threadpool

from app.core.security import require_analyst
from app.schemas.decision_intelligence import (
    ExecutionCreate,
    LearningDashboardResponse,
    OutcomesListResponse,
    WeeklyReportResponse,
)
from app.services.decision_intelligence_service import (
    get_execution_detail,
    get_learning_dashboard,
    get_opportunity_outcomes,
    get_outcomes_page,
    get_weekly_report,
    record_execution,
    run_learning_aggregation,
    run_model_recalibration,
    run_outcome_monitoring,
)

router = APIRouter(prefix="/strategy/decisions", tags=["decision-intelligence"])


# ── Execution Management ─────────────────────────────────────────

@router.post("/executions", dependencies=[Depends(require_analyst)])
async def create_execution(body: ExecutionCreate):
    """Record a new execution when an opportunity is acted upon."""
    try:
        result = await run_in_threadpool(
            record_execution,
            opportunity_id=body.opportunity_id,
            entity_type=body.entity_type,
            entity_id=body.entity_id,
            action_type=body.action_type,
            executed_by=body.executed_by,
            monitoring_days=body.monitoring_days,
        )
        return result
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


# ── Outcomes (paginated list + detail) ───────────────────────────

@router.get("/outcomes", dependencies=[Depends(require_analyst)])
async def list_outcomes(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    opportunity_type: str | None = None,
    marketplace_id: str | None = None,
    min_success: float | None = None,
    max_success: float | None = None,
    status: str | None = None,
):
    return await run_in_threadpool(
        get_outcomes_page,
        page=page,
        page_size=page_size,
        opportunity_type=opportunity_type,
        marketplace_id=marketplace_id,
        min_success=min_success,
        max_success=max_success,
        status=status,
    )


@router.get("/outcomes/{execution_id}", dependencies=[Depends(require_analyst)])
async def outcome_detail(execution_id: int):
    result = await run_in_threadpool(get_execution_detail, execution_id)
    if not result:
        raise HTTPException(status_code=404, detail="Execution not found")
    return result


@router.get("/outcomes/opportunity/{opportunity_id}", dependencies=[Depends(require_analyst)])
async def opportunity_outcomes(opportunity_id: int):
    """All executions & outcomes for a specific opportunity (for detail drawer)."""
    return await run_in_threadpool(get_opportunity_outcomes, opportunity_id)


# ── Learning Dashboard ──────────────────────────────────────────

@router.get("/learning", dependencies=[Depends(require_analyst)])
async def learning_dashboard():
    return await run_in_threadpool(get_learning_dashboard)


@router.get("/learning/report", dependencies=[Depends(require_analyst)])
async def weekly_report():
    return await run_in_threadpool(get_weekly_report)


# ── Manual Triggers ──────────────────────────────────────────────

@router.post("/evaluate", dependencies=[Depends(require_analyst)])
async def trigger_evaluation():
    """Manually trigger outcome evaluation (runs daily automatically)."""
    result = await run_in_threadpool(run_outcome_monitoring)
    return {"status": "ok", **result}


@router.post("/aggregate", dependencies=[Depends(require_analyst)])
async def trigger_aggregation():
    """Manually trigger learning aggregation (runs weekly automatically)."""
    result = await run_in_threadpool(run_learning_aggregation)
    return {"status": "ok", **result}


@router.post("/recalibrate", dependencies=[Depends(require_analyst)])
async def trigger_recalibration():
    """Manually trigger model recalibration (runs monthly automatically)."""
    result = await run_in_threadpool(run_model_recalibration)
    return {"status": "ok", **result}
