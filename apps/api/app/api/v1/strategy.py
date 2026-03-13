"""Strategy / Growth Engine — API endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from starlette.concurrency import run_in_threadpool

from app.core.security import require_analyst
from app.schemas.strategy import (
    BundleCandidateResponse,
    ExperimentCreate,
    ExperimentListResponse,
    JobRunRequest,
    JobRunResponse,
    MarketExpansionResponse,
    OpportunityDetailResponse,
    OpportunityListResponse,
    PlaybookListResponse,
    StatusChangeRequest,
    StatusChangeResponse,
    StrategyOverviewResponse,
)
from app.services.strategy_service import (
    change_opportunity_status,
    create_experiment,
    get_bundle_candidates,
    get_experiments,
    get_market_expansion_items,
    get_opportunities_page,
    get_opportunity_detail,
    get_playbooks,
    get_strategy_overview,
    run_strategy_detection,
)

router = APIRouter(prefix="/strategy", tags=["strategy"])


# ── Overview ────────────────────────────────────────────────────────
@router.get("/overview", response_model=StrategyOverviewResponse)
async def overview(_=Depends(require_analyst)):
    return await run_in_threadpool(get_strategy_overview)


# ── Opportunities list (paginated, filtered) ────────────────────────
@router.get("/opportunities", response_model=OpportunityListResponse)
async def opportunities(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    marketplace_id: str | None = None,
    opportunity_type: str | None = None,
    status: str | None = None,
    owner_role: str | None = None,
    min_priority: float | None = None,
    max_priority: float | None = None,
    min_confidence: float | None = None,
    sku: str | None = None,
    sort: str = "priority_score",
    dir: str = "desc",
    quick_filter: str | None = None,
    _=Depends(require_analyst),
):
    return await run_in_threadpool(
        get_opportunities_page,
        page=page,
        page_size=page_size,
        marketplace_id=marketplace_id,
        opportunity_type=opportunity_type,
        status=status,
        owner_role=owner_role,
        min_priority=min_priority,
        max_priority=max_priority,
        min_confidence=min_confidence,
        sku=sku,
        sort=sort,
        direction=dir,
        quick_filter=quick_filter,
    )


# ── Opportunity detail ──────────────────────────────────────────────
@router.get("/opportunities/{opp_id}", response_model=OpportunityDetailResponse)
async def opportunity_detail(opp_id: int, _=Depends(require_analyst)):
    result = await run_in_threadpool(get_opportunity_detail, opp_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Opportunity not found")
    return result


# ── Status changes ──────────────────────────────────────────────────
@router.post("/opportunities/{opp_id}/accept", response_model=StatusChangeResponse)
async def accept_opportunity(opp_id: int, body: StatusChangeRequest | None = None, _=Depends(require_analyst)):
    return await run_in_threadpool(change_opportunity_status, opp_id, "accepted", note=body.note if body else None)


@router.post("/opportunities/{opp_id}/reject", response_model=StatusChangeResponse)
async def reject_opportunity(opp_id: int, body: StatusChangeRequest | None = None, _=Depends(require_analyst)):
    return await run_in_threadpool(change_opportunity_status, opp_id, "rejected", note=body.note if body else None)


@router.post("/opportunities/{opp_id}/complete", response_model=StatusChangeResponse)
async def complete_opportunity(opp_id: int, body: StatusChangeRequest | None = None, _=Depends(require_analyst)):
    return await run_in_threadpool(change_opportunity_status, opp_id, "completed", note=body.note if body else None)


# ── Playbooks ───────────────────────────────────────────────────────
@router.get("/playbooks", response_model=PlaybookListResponse)
async def playbooks(_=Depends(require_analyst)):
    return {"playbooks": get_playbooks()}


# ── Market expansion ────────────────────────────────────────────────
@router.get("/market-expansion", response_model=MarketExpansionResponse)
async def market_expansion(_=Depends(require_analyst)):
    items = await run_in_threadpool(get_market_expansion_items)
    return {"items": items, "total": len(items)}


# ── Bundles ─────────────────────────────────────────────────────────
@router.get("/bundles", response_model=BundleCandidateResponse)
async def bundles(_=Depends(require_analyst)):
    return await run_in_threadpool(get_bundle_candidates)


# ── Experiments ─────────────────────────────────────────────────────
@router.get("/experiments", response_model=ExperimentListResponse)
async def experiments(status: str | None = None, _=Depends(require_analyst)):
    items = await run_in_threadpool(get_experiments, status=status)
    return {"items": items, "total": len(items)}


@router.post("/experiments", response_model=dict)
async def create_exp(body: ExperimentCreate, _=Depends(require_analyst)):
    return await run_in_threadpool(create_experiment, body.model_dump())


# ── Manual job trigger ──────────────────────────────────────────────
@router.post("/jobs/run", response_model=JobRunResponse)
async def run_job(body: JobRunRequest, _=Depends(require_analyst)):
    if body.job_type == "detect_all":
        return await run_in_threadpool(run_strategy_detection, days_back=body.days_back)
    raise HTTPException(status_code=400, detail=f"Unknown job type: {body.job_type}")
