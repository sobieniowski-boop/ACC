"""Buy Box Radar API — competitor intelligence & win-rate analytics.

Sprint 11 – S11.5
"""
from __future__ import annotations

from typing import Optional

import structlog
from fastapi import APIRouter, HTTPException, Query

from app.intelligence import buybox_radar

log = structlog.get_logger(__name__)
router = APIRouter(prefix="/buybox-radar", tags=["buybox-radar"])


@router.get("/dashboard")
def buybox_dashboard(
    marketplace_id: Optional[str] = Query(None),
    days: int = Query(7, ge=1, le=90),
):
    """Aggregated BuyBox health dashboard — overall win-rate, winners/losers, trend direction."""
    return buybox_radar.get_buybox_dashboard(marketplace_id=marketplace_id, days=days)


@router.get("/trends/{seller_sku}")
def buybox_trends(
    seller_sku: str,
    marketplace_id: str = Query(..., description="Required marketplace ID"),
    days: int = Query(30, ge=1, le=365),
):
    """Daily BuyBox win-rate trend for a specific SKU."""
    rows = buybox_radar.get_buybox_trends(seller_sku, marketplace_id, days=days)
    return {"seller_sku": seller_sku, "marketplace_id": marketplace_id, "days": days, "trends": rows}


@router.get("/rolling/{seller_sku}")
def rolling_win_rates(
    seller_sku: str,
    marketplace_id: str = Query(..., description="Required marketplace ID"),
):
    """Rolling BuyBox win-rate (7d / 30d / 90d) for a specific SKU."""
    rates = buybox_radar.get_rolling_win_rates(seller_sku, marketplace_id)
    return {"seller_sku": seller_sku, "marketplace_id": marketplace_id, **rates}


@router.get("/competitors/{asin}")
def competitor_landscape(
    asin: str,
    marketplace_id: str = Query(..., description="Required marketplace ID"),
    hours: int = Query(24, ge=1, le=168),
):
    """Competitive landscape for an ASIN — all sellers, prices, BuyBox winner."""
    return buybox_radar.get_competitor_landscape(asin, marketplace_id, hours=hours)


@router.get("/losses")
def sustained_losses(
    marketplace_id: Optional[str] = Query(None),
    threshold_days: int = Query(3, ge=1, le=30),
):
    """SKUs with sustained BuyBox loss (consecutive days below 5% win-rate)."""
    losses = buybox_radar.detect_sustained_buybox_losses(
        marketplace_id, threshold_days=threshold_days,
    )
    return {"threshold_days": threshold_days, "count": len(losses), "losses": losses}


@router.get("/alerts")
def buybox_alerts(
    marketplace_id: Optional[str] = Query(None),
    days: int = Query(7, ge=1, le=90),
    limit: int = Query(50, ge=1, le=200),
):
    """Recent BuyBox loss alerts from the system alert table."""
    alerts = buybox_radar.get_buybox_alerts(marketplace_id, days=days, limit=limit)
    return {"count": len(alerts), "alerts": alerts}


@router.post("/compute-trends")
def compute_trends(
    marketplace_id: Optional[str] = Query(None),
    date_str: Optional[str] = Query(None, alias="date", description="YYYY-MM-DD, default yesterday"),
):
    """Manually trigger daily BuyBox trend computation."""
    from datetime import date as d
    target = None
    if date_str:
        try:
            target = d.fromisoformat(date_str)
        except ValueError:
            raise HTTPException(400, f"Invalid date: {date_str}")
    upserted = buybox_radar.compute_daily_buybox_trends(
        target_date=target, marketplace_id=marketplace_id,
    )
    return {"upserted": upserted, "date": str(target) if target else "yesterday"}


@router.post("/raise-alerts")
def trigger_alerts(
    marketplace_id: Optional[str] = Query(None),
    threshold_days: int = Query(3, ge=1, le=30),
):
    """Manually trigger sustained BuyBox loss alert detection."""
    count = buybox_radar.raise_sustained_loss_alerts(
        marketplace_id, threshold_days=threshold_days,
    )
    return {"alerts_raised": count}


# ─── Sprint 12: Landscape & price-history endpoints ──────────────────

@router.get("/landscape")
def landscape_overview(
    marketplace_id: Optional[str] = Query(None),
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(50, ge=1, le=500),
):
    """Cross-catalog competitive landscape — ASINs ranked by seller count."""
    rows = buybox_radar.get_landscape_overview(
        marketplace_id, hours=hours, limit=limit,
    )
    return {"count": len(rows), "landscape": rows}


@router.get("/competitors/{asin}/history")
def competitor_price_history(
    asin: str,
    marketplace_id: str = Query(..., description="Required marketplace ID"),
    days: int = Query(30, ge=1, le=365),
    seller_id: Optional[str] = Query(None),
):
    """Daily aggregated price history for competitor offers on an ASIN."""
    rows = buybox_radar.get_competitor_price_history(
        asin, marketplace_id, days=days, seller_id=seller_id,
    )
    return {"asin": asin, "marketplace_id": marketplace_id, "days": days, "history": rows}


@router.post("/capture-competitors")
async def capture_competitors(
    marketplace_id: str = Query(..., description="Marketplace to capture"),
    asin_limit: int = Query(50, ge=1, le=200),
):
    """Manually trigger competitor offer capture for a marketplace."""
    result = await buybox_radar.capture_competitor_offers(
        marketplace_id, asin_limit=asin_limit,
    )
    return result
