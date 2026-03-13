"""COGS audit & controlling API endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from fastapi.concurrency import run_in_threadpool

from app.core.security import require_analyst

router = APIRouter(prefix="/audit", tags=["audit"])


@router.get("/cogs")
async def cogs_audit(
    current_user: dict = Depends(require_analyst),
):
    """
    Run full COGS data quality audit.

    Returns a report with:
      - coverage (% of order lines with COGS stamped)
      - mapping integrity (internal_sku validation)
      - price sanity (outlier detection)
      - COGS consistency (stamped vs current price divergence)
      - margin ratio (COGS / revenue analysis)
      - stale prices (active products with outdated prices)
      - controlling alerts (blocked overwrites, flagged prices)
    """
    from app.services.cogs_audit import run_full_audit
    return await run_in_threadpool(
        run_full_audit, persist=True, trigger_source="manual"
    )


@router.get("/cogs/coverage")
async def cogs_coverage(
    current_user: dict = Depends(require_analyst),
):
    """Quick coverage check — lightweight."""
    from app.services.cogs_audit import check_coverage
    return await run_in_threadpool(check_coverage)


@router.get("/cogs/prices")
async def cogs_price_sanity(
    current_user: dict = Depends(require_analyst),
):
    """Price sanity check — outlier detection."""
    from app.services.cogs_audit import check_price_sanity
    return await run_in_threadpool(check_price_sanity)


@router.get("/cogs/margin")
async def cogs_margin(
    current_user: dict = Depends(require_analyst),
):
    """Margin ratio analysis with correct PLN conversion."""
    from app.services.cogs_audit import check_margin_ratio
    return await run_in_threadpool(check_margin_ratio)


# ---------------------------------------------------------------------------
# CONTROLLING ENDPOINTS
# ---------------------------------------------------------------------------

@router.get("/controlling/summary")
async def controlling_summary(
    current_user: dict = Depends(require_analyst),
):
    """Controlling dashboard — mapping + price change summary (last 7 days)."""
    from app.services.controlling import get_controlling_summary
    return await run_in_threadpool(get_controlling_summary)


@router.get("/controlling/mapping-history")
async def controlling_mapping_history(
    sku: str | None = Query(None, description="Filter by SKU"),
    product_id: str | None = Query(None, description="Filter by product ID"),
    limit: int = Query(50, ge=1, le=500),
    current_user: dict = Depends(require_analyst),
):
    """Mapping change audit trail — who changed internal_sku, when, from what to what."""
    from app.services.controlling import get_mapping_history
    return await run_in_threadpool(
        get_mapping_history, sku=sku, product_id=product_id, limit=limit
    )


@router.get("/controlling/price-history")
async def controlling_price_history(
    internal_sku: str | None = Query(None, description="Filter by internal SKU"),
    flagged_only: bool = Query(False, description="Show only flagged anomalies"),
    limit: int = Query(50, ge=1, le=500),
    current_user: dict = Depends(require_analyst),
):
    """Price change audit trail — every price change with anomaly flags."""
    from app.services.controlling import get_price_history
    return await run_in_threadpool(
        get_price_history, internal_sku=internal_sku,
        flagged_only=flagged_only, limit=limit
    )


@router.get("/controlling/stale-prices")
async def controlling_stale_prices(
    max_age_days: int = Query(90, ge=7, le=365),
    current_user: dict = Depends(require_analyst),
):
    """Detect active products with outdated purchase prices."""
    from app.services.controlling import check_stale_prices
    return await run_in_threadpool(check_stale_prices, max_age_days=max_age_days)


@router.get("/controlling/source-priority")
async def controlling_source_priority(
    current_user: dict = Depends(require_analyst),
):
    """Return the mapping source priority hierarchy."""
    from app.services.controlling import SOURCE_PRIORITY
    return {
        "priority_map": SOURCE_PRIORITY,
        "description": "Higher number = more trustworthy. A source can only overwrite another of equal or lower priority.",
    }
