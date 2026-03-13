"""API routes — Repricing Decision Engine.

Sprint 15 — Strategy CRUD, proposal pipeline, approval workflow, dashboard.
Sprint 16 — Auto-execution, bulk ops, analytics.
"""
from __future__ import annotations

from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.intelligence import repricing_engine

router = APIRouter(prefix="/repricing", tags=["repricing"])


# ── Pydantic models ────────────────────────────────────────────────────

class StrategyIn(BaseModel):
    strategy_type: str = Field(..., description="buybox_match | competitive_undercut | margin_target | velocity_based")
    seller_sku: Optional[str] = None
    marketplace_id: Optional[str] = None
    parameters: Optional[dict] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    min_margin_pct: Optional[float] = None
    max_daily_change_pct: Optional[float] = None
    requires_approval: bool = True
    is_active: bool = True
    priority: int = 100


# ── Strategy CRUD ──────────────────────────────────────────────────────

@router.get("/strategies")
def list_strategies(
    marketplace_id: Optional[str] = Query(None),
    active_only: bool = Query(True),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """List repricing strategies."""
    return repricing_engine.list_strategies(
        marketplace_id,
        active_only=active_only,
        limit=limit,
        offset=offset,
    )


@router.get("/strategies/{strategy_id}")
def get_strategy(strategy_id: int):
    """Get a single repricing strategy."""
    result = repricing_engine.get_strategy(strategy_id)
    if not result:
        raise HTTPException(404, "Strategy not found")
    return result


@router.post("/strategies")
def create_strategy(body: StrategyIn):
    """Create or update a repricing strategy."""
    if body.strategy_type not in repricing_engine.VALID_STRATEGY_TYPES:
        raise HTTPException(
            400, f"Invalid strategy_type. Must be one of: {sorted(repricing_engine.VALID_STRATEGY_TYPES)}"
        )
    return repricing_engine.upsert_strategy(
        body.strategy_type,
        seller_sku=body.seller_sku,
        marketplace_id=body.marketplace_id,
        parameters=body.parameters,
        min_price=body.min_price,
        max_price=body.max_price,
        min_margin_pct=body.min_margin_pct,
        max_daily_change_pct=body.max_daily_change_pct,
        requires_approval=body.requires_approval,
        is_active=body.is_active,
        priority=body.priority,
    )


@router.delete("/strategies/{strategy_id}")
def delete_strategy(strategy_id: int):
    """Delete a repricing strategy."""
    ok = repricing_engine.delete_strategy(strategy_id)
    if not ok:
        raise HTTPException(404, "Strategy not found")
    return {"status": "deleted", "strategy_id": strategy_id}


# ── Proposal pipeline ─────────────────────────────────────────────────

@router.post("/compute")
def compute_proposals(
    marketplace_id: Optional[str] = Query(None),
):
    """Run the repricing engine and generate execution proposals."""
    count = repricing_engine.compute_repricing_proposals(marketplace_id)
    return {"proposals_created": count}


@router.get("/executions")
def list_executions(
    marketplace_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """List repricing execution proposals with status filter."""
    if status and status not in repricing_engine.VALID_STATUSES:
        raise HTTPException(400, f"Invalid status. Must be one of: {sorted(repricing_engine.VALID_STATUSES)}")
    return repricing_engine.get_execution_proposals(
        marketplace_id,
        status=status,
        limit=limit,
        offset=offset,
    )


@router.get("/executions/history/{seller_sku}")
def execution_history(
    seller_sku: str,
    marketplace_id: str = Query(...),
    days: int = Query(30, ge=1, le=365),
):
    """Get execution history for a specific SKU."""
    rows = repricing_engine.get_execution_history(seller_sku, marketplace_id, days=days)
    return {"seller_sku": seller_sku, "marketplace_id": marketplace_id, "history": rows}


# ── Approval workflow ──────────────────────────────────────────────────

@router.post("/executions/{execution_id}/approve")
def approve_execution(execution_id: int):
    """Approve a proposed repricing execution."""
    ok = repricing_engine.approve_execution(execution_id)
    if not ok:
        raise HTTPException(404, "Execution not found or not in proposed status")
    return {"status": "approved", "execution_id": execution_id}


@router.post("/executions/{execution_id}/reject")
def reject_execution(execution_id: int):
    """Reject a proposed repricing execution."""
    ok = repricing_engine.reject_execution(execution_id)
    if not ok:
        raise HTTPException(404, "Execution not found or not in proposed status")
    return {"status": "rejected", "execution_id": execution_id}


# ── Dashboard ──────────────────────────────────────────────────────────

@router.get("/dashboard")
def repricing_dashboard(
    marketplace_id: Optional[str] = Query(None),
):
    """Repricing engine dashboard — strategy counts, proposal stats."""
    return repricing_engine.get_repricing_dashboard(marketplace_id)


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 16 — Bulk operations
# ═══════════════════════════════════════════════════════════════════════════

class BulkActionIn(BaseModel):
    execution_ids: List[int] = Field(..., min_length=1, max_length=500)
    approved_by: Optional[str] = "operator"


@router.post("/executions/bulk-approve")
def bulk_approve(body: BulkActionIn):
    """Approve multiple proposed executions at once."""
    return repricing_engine.bulk_approve_executions(
        body.execution_ids,
        approved_by=body.approved_by or "operator",
    )


@router.post("/executions/bulk-reject")
def bulk_reject(body: BulkActionIn):
    """Reject multiple proposed executions at once."""
    return repricing_engine.bulk_reject_executions(body.execution_ids)


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 16 — Auto-execution
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/executions/auto-approve")
def auto_approve(
    marketplace_id: Optional[str] = Query(None),
):
    """Auto-approve eligible proposals (small changes, no-approval-required strategies)."""
    count = repricing_engine.auto_approve_proposals(marketplace_id)
    return {"auto_approved": count}


@router.post("/executions/execute")
def execute_prices(
    marketplace_id: str = Query(...),
):
    """Submit approved repricing proposals to Amazon via SP-API Feeds."""
    return repricing_engine.execute_approved_prices(marketplace_id)


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 16 — Analytics
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/analytics/compute")
def compute_analytics(
    marketplace_id: Optional[str] = Query(None),
):
    """Compute daily repricing analytics from execution data."""
    return repricing_engine.compute_daily_analytics(marketplace_id=marketplace_id)


@router.get("/analytics/trend")
def analytics_trend(
    days: int = Query(30, ge=1, le=365),
    marketplace_id: Optional[str] = Query(None),
):
    """Get daily analytics trend for charting."""
    return repricing_engine.get_analytics_trend(days=days, marketplace_id=marketplace_id)


@router.get("/analytics/by-strategy")
def analytics_by_strategy(
    days: int = Query(30, ge=1, le=365),
    marketplace_id: Optional[str] = Query(None),
):
    """Per-strategy execution summary for analytics."""
    return repricing_engine.get_execution_summary_by_strategy(days=days, marketplace_id=marketplace_id)
