"""API routes — Pricing module."""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import Integer, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user, require_role, Role
from app.models.user import User
from app.models.offer import Offer
from app.models.marketplace import Marketplace
from app.schemas.pricing import (
    PricingListResponse, OfferPriceOut,
    PriceUpdateRequest, PriceUpdateResponse,
    BuyBoxStatsOut,
)

router = APIRouter(prefix="/pricing", tags=["pricing"])


@router.get("/offers", response_model=PricingListResponse)
async def list_offers(
    marketplace_id: Optional[str] = Query(None),
    sku: Optional[str] = Query(None),
    has_buybox: Optional[bool] = Query(None),
    status: Optional[str] = Query(None),
    fulfillment_channel: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Return paginated offer price list with buybox status."""
    q = (
        select(Offer, Marketplace.code.label("marketplace_code"))
        .join(Marketplace, Marketplace.id == Offer.marketplace_id)
    )
    if marketplace_id:
        q = q.where(Offer.marketplace_id == marketplace_id)
    if sku:
        q = q.where(Offer.sku.ilike(f"%{sku}%"))
    if has_buybox is not None:
        q = q.where(Offer.has_buybox == has_buybox)
    if status:
        q = q.where(Offer.status == status)
    if fulfillment_channel:
        q = q.where(Offer.fulfillment_channel == fulfillment_channel)

    total_q = select(func.count()).select_from(q.subquery())
    total = (await db.execute(total_q)).scalar_one()

    q = q.order_by(Offer.sku).offset((page - 1) * page_size).limit(page_size)
    rows = (await db.execute(q)).all()

    items = []
    for offer, mp_code in rows:
        items.append(
            OfferPriceOut(
                id=offer.id,
                marketplace_id=offer.marketplace_id,
                marketplace_code=mp_code,
                sku=offer.sku,
                asin=offer.asin or "",
                current_price=float(offer.price or 0),
                currency=offer.currency or "EUR",
                buybox_price=float(offer.buybox_price) if offer.buybox_price else None,
                has_buybox=bool(offer.has_buybox),
                status=offer.status or "Active",
                fulfillment_channel=offer.fulfillment_channel or "FBA",
                fba_fee=float(offer.fba_fee) if offer.fba_fee else None,
                referral_fee_rate=float(offer.referral_fee_rate) if offer.referral_fee_rate else None,
                updated_at=offer.updated_at,
            )
        )

    return PricingListResponse(items=items, total=total, page=page, page_size=page_size)


@router.post("/offers/update", response_model=list[PriceUpdateResponse])
async def bulk_update_prices(
    updates: list[PriceUpdateRequest],
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_role(Role.CATEGORY_MGR)),
):
    """
    Queue bulk price updates.
    In production this queues a Celery task; here we record the intent
    and return a queued status.
    """
    results: list[PriceUpdateResponse] = []
    for upd in updates:
        q = select(Offer).where(
            Offer.sku == upd.sku,
            Offer.marketplace_id == upd.marketplace_id,
        )
        offer = (await db.execute(q)).scalar_one_or_none()
        if not offer:
            results.append(
                PriceUpdateResponse(
                    sku=upd.sku,
                    marketplace_id=upd.marketplace_id,
                    old_price=0,
                    new_price=upd.new_price,
                    status="error",
                    message="Offer not found",
                )
            )
            continue

        old_price = float(offer.price or 0)
        offer.price = upd.new_price
        await db.flush()
        results.append(
            PriceUpdateResponse(
                sku=upd.sku,
                marketplace_id=upd.marketplace_id,
                old_price=old_price,
                new_price=upd.new_price,
                status="queued",
                message="Price update queued for SP-API submission",
            )
        )
    await db.commit()
    return results


@router.get("/buybox-stats", response_model=list[BuyBoxStatsOut])
async def buybox_stats(
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Aggregated Buy Box win rate per marketplace."""
    q = (
        select(
            Offer.marketplace_id,
            Marketplace.code.label("marketplace_code"),
            func.count(Offer.id).label("total"),
            func.sum(func.cast(Offer.has_buybox, Integer)).label("wins"),
            func.avg(Offer.price - Offer.buybox_price).label("avg_gap"),
            func.sum(
                case((Offer.status == "Active", 1), else_=0)
            ).label("active_cnt"),
            func.sum(
                case((Offer.status != "Active", 1), else_=0)
            ).label("inactive_cnt"),
            func.sum(
                case((Offer.fulfillment_channel == "FBA", 1), else_=0)
            ).label("fba_cnt"),
            func.sum(
                case((Offer.fulfillment_channel != "FBA", 1), else_=0)
            ).label("fbm_cnt"),
            func.max(Offer.last_synced_at).label("last_sync"),
        )
        .join(Marketplace, Marketplace.id == Offer.marketplace_id)
        .group_by(Offer.marketplace_id, Marketplace.code)
    )
    rows = (await db.execute(q)).all()
    result = []
    for row in rows:
        total = int(row.total or 0)
        wins = int(row.wins or 0)
        result.append(
            BuyBoxStatsOut(
                marketplace_id=row.marketplace_id,
                marketplace_code=row.marketplace_code,
                total_active_offers=total,
                buybox_wins=wins,
                buybox_win_pct=round(wins / total * 100, 1) if total > 0 else 0.0,
                avg_price_gap=float(row.avg_gap) if row.avg_gap else None,
                active_offers=int(row.active_cnt or 0),
                inactive_offers=int(row.inactive_cnt or 0),
                fba_offers=int(row.fba_cnt or 0),
                fbm_offers=int(row.fbm_cnt or 0),
                last_sync=row.last_sync,
            )
        )
    return result
