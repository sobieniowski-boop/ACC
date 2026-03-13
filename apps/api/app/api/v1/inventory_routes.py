"""API routes — Inventory module."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.user import User
from app.models.inventory import InventorySnapshot
from app.models.marketplace import Marketplace
from app.schemas.inventory import (
    InventoryListResponse, InventorySnapshotOut,
    InventorySummary, OpenPOOut, ReorderSuggestionOut,
)

router = APIRouter(prefix="/inventory", tags=["inventory"])


def _doi_status(doi: Optional[int]) -> str:
    if doi is None:
        return "ok"
    if doi < 7:
        return "critical"
    if doi < 14:
        return "low"
    if doi > 90:
        return "overstock"
    return "ok"


@router.get("/", response_model=InventoryListResponse)
async def list_inventory(
    marketplace_id: Optional[str] = Query(None),
    sku: Optional[str] = Query(None),
    status: Optional[str] = Query(None, description="ok|low|critical|overstock"),
    snapshot_date: Optional[date] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
):
    # Default to latest snapshot date
    if not snapshot_date:
        latest = (
            await db.execute(select(func.max(InventorySnapshot.snapshot_date)))
        ).scalar_one_or_none()
        snapshot_date = latest or date.today()

    q = (
        select(InventorySnapshot, Marketplace.code.label("mp_code"))
        .join(Marketplace, Marketplace.id == InventorySnapshot.marketplace_id)
        .where(InventorySnapshot.snapshot_date == snapshot_date)
    )
    if marketplace_id:
        q = q.where(InventorySnapshot.marketplace_id == marketplace_id)
    if sku:
        q = q.where(InventorySnapshot.sku.ilike(f"%{sku}%"))

    all_rows = (await db.execute(q)).all()

    # Compute status + apply filter
    enriched = []
    for snap, mp_code in all_rows:
        s = _doi_status(snap.days_of_inventory)
        if status and s != status:
            continue
        enriched.append((snap, mp_code, s))

    total = len(enriched)
    page_rows = enriched[(page - 1) * page_size: page * page_size]

    items = [
        InventorySnapshotOut(
            id=snap.id,
            snapshot_date=snap.snapshot_date,
            marketplace_id=snap.marketplace_id,
            marketplace_code=mp_code,
            sku=snap.sku,
            asin=snap.asin,
            product_name=None,
            qty_fulfillable=int(snap.qty_fulfillable or 0),
            qty_reserved=int(snap.qty_reserved or 0),
            qty_inbound=int(snap.qty_inbound or 0),
            qty_unfulfillable=int(snap.qty_unfulfillable or 0),
            qty_total=int(
                (snap.qty_fulfillable or 0) +
                (snap.qty_reserved or 0) +
                (snap.qty_inbound or 0)
            ),
            days_of_inventory=snap.days_of_inventory,
            velocity_30d=float(snap.velocity_30d) if snap.velocity_30d else None,
            inventory_value_pln=float(snap.inventory_value_pln) if snap.inventory_value_pln else None,
            status=s,
        )
        for snap, mp_code, s in page_rows
    ]

    # Summary aggregation
    critical = sum(1 for _, _, s in enriched if s == "critical")
    low = sum(1 for _, _, s in enriched if s == "low")
    overstock = sum(1 for _, _, s in enriched if s == "overstock")
    total_val = sum(
        float(snap.inventory_value_pln or 0) for snap, _, _ in enriched
    )
    dois = [snap.days_of_inventory for snap, _, _ in enriched if snap.days_of_inventory]
    avg_doi = round(sum(dois) / len(dois), 1) if dois else 0.0

    summary = InventorySummary(
        total_skus=total,
        critical_count=critical,
        low_count=low,
        overstock_count=overstock,
        total_value_pln=round(total_val, 2),
        avg_doi=avg_doi,
    )

    return InventoryListResponse(
        items=items, total=total, page=page, page_size=page_size, summary=summary
    )


@router.get("/open-pos", response_model=list[OpenPOOut])
async def open_purchase_orders(
    sku: Optional[str] = Query(None),
    _: User = Depends(get_current_user),
):
    """Fetch open purchase orders from MSSQL NetfoxAnalityka."""
    from app.connectors.mssql.netfox import get_open_purchase_orders, test_connection

    if not test_connection():
        return []  # MSSQL not reachable — return empty instead of 500

    skus = [sku] if sku else None
    df = get_open_purchase_orders(skus)
    today = date.today()
    result = []
    for _, row in df.iterrows():
        delivery = row.get("expected_delivery")
        days_until = (delivery - today).days if delivery else None
        result.append(
            OpenPOOut(
                sku=row["sku"],
                product_name=row.get("product_name"),
                order_date=row.get("order_date"),
                expected_delivery=delivery,
                qty_ordered=int(row.get("qty_ordered", 0)),
                qty_received=int(row.get("qty_received", 0)),
                qty_open=int(row.get("qty_open", 0)),
                days_until_delivery=days_until,
            )
        )
    return result


@router.get("/reorder-suggestions", response_model=list[ReorderSuggestionOut])
async def reorder_suggestions(
    marketplace_id: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Auto-generate reorder suggestions for SKUs with DOI < 30 days."""
    latest = (
        await db.execute(select(func.max(InventorySnapshot.snapshot_date)))
    ).scalar_one_or_none()
    if not latest:
        return []

    q = (
        select(InventorySnapshot)
        .where(
            InventorySnapshot.snapshot_date == latest,
            InventorySnapshot.days_of_inventory < 30,
            InventorySnapshot.velocity_30d > 0,
        )
    )
    if marketplace_id:
        q = q.where(InventorySnapshot.marketplace_id == marketplace_id)

    snaps = (await db.execute(q)).scalars().all()
    suggestions = []
    today = date.today()

    for snap in snaps:
        doi = snap.days_of_inventory or 0
        velocity = float(snap.velocity_30d or 1)
        # Suggest enough for 60-day coverage
        target_days = 60
        needed = max(0, int((target_days - doi) * velocity))
        if needed == 0:
            continue

        urgency = "critical" if doi < 7 else ("high" if doi < 14 else "medium")
        order_by = today + timedelta(days=max(0, doi - 14))  # order 14 days before stockout

        suggestions.append(
            ReorderSuggestionOut(
                sku=snap.sku,
                current_doi=doi,
                velocity_30d=round(velocity, 2),
                suggested_qty=needed,
                suggested_order_date=order_by,
                urgency=urgency,
                reason=f"Current stock covers {doi} days. Target: {target_days} days. Velocity: {velocity:.1f} units/day.",
            )
        )

    suggestions.sort(key=lambda x: x.current_doi)
    return suggestions
