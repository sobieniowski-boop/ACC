"""Inventory Risk Engine API — stockout probability, overstock cost, aging risk,
replenishment plan, velocity trends, risk alerts.

Sprint 13 – S13.3
Sprint 14 – Replenishment plan, alerts, trends
"""
from __future__ import annotations

from typing import Optional

import structlog
from fastapi import APIRouter, Query

from app.intelligence import inventory_risk

log = structlog.get_logger(__name__)
router = APIRouter(prefix="/inventory-risk", tags=["inventory-risk"])


@router.get("/dashboard")
def risk_dashboard(
    marketplace_id: Optional[str] = Query(None),
    days: int = Query(1, ge=1, le=30),
):
    """Aggregated inventory risk dashboard — tier counts, total cost exposure."""
    return inventory_risk.get_risk_dashboard(marketplace_id, days=days)


@router.get("/scores")
def risk_scores(
    marketplace_id: Optional[str] = Query(None),
    risk_tier: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    sort_by: str = Query("risk_score"),
    sort_dir: str = Query("desc"),
):
    """Paginated inventory risk scores for all SKUs."""
    return inventory_risk.get_risk_scores(
        marketplace_id,
        risk_tier=risk_tier,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )


@router.get("/history/{seller_sku}")
def risk_history(
    seller_sku: str,
    marketplace_id: str = Query(..., description="Required marketplace ID"),
    days: int = Query(30, ge=1, le=365),
):
    """Daily risk score history for a specific SKU."""
    rows = inventory_risk.get_risk_history(seller_sku, marketplace_id, days=days)
    return {"seller_sku": seller_sku, "marketplace_id": marketplace_id, "days": days, "history": rows}


@router.get("/stockout-watchlist")
def stockout_watchlist(
    marketplace_id: Optional[str] = Query(None),
    threshold: float = Query(0.3, ge=0.0, le=1.0),
    limit: int = Query(50, ge=1, le=200),
):
    """SKUs with highest stockout probability (above threshold)."""
    items = inventory_risk.get_stockout_watchlist(
        marketplace_id, threshold=threshold, limit=limit,
    )
    return {"count": len(items), "threshold": threshold, "items": items}


@router.get("/overstock-report")
def overstock_report(
    marketplace_id: Optional[str] = Query(None),
    min_cost_pln: float = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
):
    """SKUs with highest overstock holding cost."""
    items = inventory_risk.get_overstock_report(
        marketplace_id, min_cost_pln=min_cost_pln, limit=limit,
    )
    return {"count": len(items), "items": items}


@router.post("/compute")
def trigger_compute(
    marketplace_id: Optional[str] = Query(None),
    date_str: Optional[str] = Query(None, alias="date", description="YYYY-MM-DD, default today"),
):
    """Manually trigger daily risk score computation."""
    from datetime import date as d
    target = None
    if date_str:
        target = d.fromisoformat(date_str)
    upserted = inventory_risk.compute_daily_risk_scores(
        target_date=target, marketplace_id=marketplace_id,
    )
    return {"upserted": upserted, "date": str(target) if target else "today"}


# ── Sprint 14 endpoints ─────────────────────────────────────────────────


@router.get("/replenishment-plan")
def replenishment_plan(
    marketplace_id: Optional[str] = Query(None),
    urgency: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    sort_by: str = Query("reorder_urgency"),
    sort_dir: str = Query("desc"),
):
    """Risk-informed replenishment plan with reorder suggestions."""
    return inventory_risk.get_replenishment_plan(
        marketplace_id,
        urgency=urgency,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )


@router.post("/replenishment-plan/acknowledge")
def acknowledge_replenishment(
    seller_sku: str = Query(...),
    marketplace_id: str = Query(...),
    acknowledged_by: str = Query("operator"),
):
    """Mark a replenishment suggestion as acknowledged."""
    ok = inventory_risk.acknowledge_replenishment(
        seller_sku, marketplace_id, acknowledged_by=acknowledged_by,
    )
    return {"acknowledged": ok, "seller_sku": seller_sku, "marketplace_id": marketplace_id}


@router.get("/alerts")
def risk_alerts(
    marketplace_id: Optional[str] = Query(None),
    alert_type: Optional[str] = Query(None),
    include_resolved: bool = Query(False),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Inventory risk alerts — tier escalations, threshold breaches, velocity drops."""
    return inventory_risk.get_risk_alerts(
        marketplace_id,
        alert_type=alert_type,
        include_resolved=include_resolved,
        limit=limit,
        offset=offset,
    )


@router.post("/alerts/{alert_id}/resolve")
def resolve_alert(alert_id: int):
    """Mark a risk alert as resolved."""
    ok = inventory_risk.resolve_risk_alert(alert_id)
    return {"resolved": ok, "alert_id": alert_id}


@router.get("/trends/{seller_sku}")
def velocity_trends(
    seller_sku: str,
    marketplace_id: str = Query(..., description="Required marketplace ID"),
    days: int = Query(30, ge=1, le=365),
):
    """Velocity + risk score trends for a specific SKU."""
    rows = inventory_risk.get_velocity_trends(seller_sku, marketplace_id, days=days)
    return {
        "seller_sku": seller_sku,
        "marketplace_id": marketplace_id,
        "days": days,
        "trends": rows,
    }
