"""Seasonality & Demand Intelligence — API endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from starlette.concurrency import run_in_threadpool

from app.core.security import require_analyst
from app.services import seasonality_service as svc
from app.services import seasonality_opportunity_engine as opp_engine

router = APIRouter(prefix="/seasonality", tags=["seasonality"])


# ── Overview ─────────────────────────────────────────────────────────

@router.get("/overview")
async def overview(
    marketplace: str | None = None,
    _=Depends(require_analyst),
):
    return await run_in_threadpool(svc.get_overview, marketplace=marketplace)


# ── Heatmap / Map ────────────────────────────────────────────────────

@router.get("/map")
async def seasonality_map(
    entity_type: str = Query("sku"),
    marketplace: str | None = None,
    seasonality_class: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    _=Depends(require_analyst),
):
    return await run_in_threadpool(
        svc.get_map,
        entity_type=entity_type,
        marketplace=marketplace,
        seasonality_class=seasonality_class,
        page=page,
        page_size=page_size,
    )


# ── Entities ─────────────────────────────────────────────────────────

@router.get("/entities")
async def entities(
    entity_type: str | None = None,
    marketplace: str | None = None,
    seasonality_class: str | None = None,
    sort: str = "demand_strength_score",
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    _=Depends(require_analyst),
):
    return await run_in_threadpool(
        svc.get_entities,
        entity_type=entity_type,
        marketplace=marketplace,
        seasonality_class=seasonality_class,
        sort=sort,
        page=page,
        page_size=page_size,
    )


# ── Entity Detail ────────────────────────────────────────────────────

@router.get("/entity/{entity_type}/{entity_id}")
async def entity_detail(
    entity_type: str,
    entity_id: str,
    marketplace: str | None = None,
    _=Depends(require_analyst),
):
    result = await run_in_threadpool(
        svc.get_entity_detail,
        entity_type,
        entity_id,
        marketplace=marketplace,
    )
    if result is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Entity not found")
    return result


# ── Opportunities ────────────────────────────────────────────────────

@router.get("/opportunities")
async def opportunities(
    marketplace: str | None = None,
    opportunity_type: str | None = None,
    status: str | None = None,
    entity_type: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    _=Depends(require_analyst),
):
    return await run_in_threadpool(
        svc.get_opportunities_page,
        marketplace=marketplace,
        opportunity_type=opportunity_type,
        status=status,
        entity_type=entity_type,
        page=page,
        page_size=page_size,
    )


@router.post("/opportunities/{opp_id}/accept")
async def accept_opportunity(opp_id: int, _=Depends(require_analyst)):
    return await run_in_threadpool(svc.change_opportunity_status, opp_id, "accepted")


@router.post("/opportunities/{opp_id}/reject")
async def reject_opportunity(opp_id: int, _=Depends(require_analyst)):
    return await run_in_threadpool(svc.change_opportunity_status, opp_id, "rejected")


# ── Clusters ─────────────────────────────────────────────────────────

@router.get("/clusters")
async def clusters(_=Depends(require_analyst)):
    return await run_in_threadpool(svc.get_clusters)


@router.get("/clusters/{cluster_id}")
async def cluster_detail(cluster_id: int, _=Depends(require_analyst)):
    result = await run_in_threadpool(svc.get_cluster_detail, cluster_id)
    if result is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Cluster not found")
    return result


@router.post("/clusters")
async def create_cluster(body: dict, _=Depends(require_analyst)):
    return await run_in_threadpool(
        svc.create_cluster,
        name=body["cluster_name"],
        description=body.get("description"),
        rules_json=body.get("rules_json"),
        members=body.get("members", []),
        created_by=body.get("created_by"),
    )


@router.put("/clusters/{cluster_id}")
async def update_cluster(cluster_id: int, body: dict, _=Depends(require_analyst)):
    return await run_in_threadpool(
        svc.update_cluster,
        cluster_id,
        name=body.get("cluster_name"),
        description=body.get("description"),
        rules_json=body.get("rules_json"),
    )


@router.post("/clusters/{cluster_id}/recompute")
async def recompute_cluster(cluster_id: int, _=Depends(require_analyst)):
    """Trigger recompute of seasonality for a cluster entity."""
    return {"status": "queued", "cluster_id": cluster_id}


# ── Settings ─────────────────────────────────────────────────────────

@router.get("/settings")
async def get_settings(_=Depends(require_analyst)):
    return await run_in_threadpool(svc.get_settings)


@router.put("/settings")
async def update_settings(body: dict, _=Depends(require_analyst)):
    return await run_in_threadpool(svc.update_settings, body)


# ── Jobs (manual trigger) ───────────────────────────────────────────

@router.post("/jobs/run")
async def run_job(
    job_type: str = Query(...),
    _=Depends(require_analyst),
):
    """Manually trigger a seasonality job."""
    if job_type == "build_monthly":
        result = await run_in_threadpool(svc.build_monthly_metrics)
    elif job_type == "recompute_indices":
        result = await run_in_threadpool(svc.recompute_indices)
    elif job_type == "recompute_profiles":
        result = await run_in_threadpool(svc.recompute_profiles)
    elif job_type == "detect_opportunities":
        result = await run_in_threadpool(opp_engine.detect_seasonality_opportunities)
    elif job_type == "sync_search_terms":
        from app.services.search_term_sync import sync_search_terms
        result = await sync_search_terms(months_back=3)
    else:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=f"Unknown job type: {job_type}")

    return {"job_type": job_type, "result": result}
