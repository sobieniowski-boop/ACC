"""Deprecated single-order profit helper.

Batch recalculation must use the canonical V2 path from
`app.connectors.mssql.mssql_store` so background jobs cannot reintroduce stale
revenue or logistics semantics. The single-order helper remains only for unit
tests and rollback diagnostics.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional, Union

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.exchange_rate import ExchangeRate
from app.models.order import AccOrder
from app.models.product import Product
from app.services.order_logistics_source import resolve_profit_logistics_pln

log = structlog.get_logger(__name__)


def _to_date(val: Union[str, datetime, date, None]) -> date:
    """Safely convert a purchase date value to a `date`."""
    if val is None:
        return date.today()
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    raw = str(val).strip()
    try:
        return datetime.fromisoformat(raw).date()
    except ValueError:
        pass
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return date.today()


async def get_exchange_rate(db: AsyncSession, currency: str, rate_date: date) -> float:
    """Get PLN exchange rate for the given currency and date."""
    result = await db.execute(
        select(ExchangeRate)
        .where(ExchangeRate.currency == currency, ExchangeRate.rate_date < rate_date)
        .order_by(ExchangeRate.rate_date.desc())
        .limit(1)
    )
    rate = result.scalar_one_or_none()
    if rate:
        return float(rate.rate_to_pln)
    from app.core.fx_service import get_rate_safe

    return get_rate_safe(currency, rate_date)


async def calculate_order_profit(
    db: AsyncSession,
    order: AccOrder,
    fx_rate: Optional[float] = None,
) -> None:
    """Legacy single-order helper used by tests.

    This helper aligns line revenue, COGS, fees, and canonical logistics with
    the historical CM1 semantics expected by the old tests. Production batch
    recalculation must go through `recalculate_profit_batch()`, which delegates
    to the V2 engine.
    """
    if fx_rate is None:
        fx_rate = await get_exchange_rate(db, order.currency, _to_date(order.purchase_date))

    total_revenue_pln = 0.0
    total_cogs = 0.0
    total_fees = 0.0

    for line in order.lines:
        qty = line.quantity_shipped or line.quantity_ordered or 1

        line_price = float(line.item_price or 0)
        line_tax = float(line.item_tax or 0)
        line_promo = float(line.promotion_discount or 0)
        total_revenue_pln += (line_price - line_tax - line_promo) * fx_rate

        if line.cogs_pln:
            total_cogs += float(line.cogs_pln) * qty
        elif line.product_id:
            prod_result = await db.execute(select(Product).where(Product.id == line.product_id))
            product = prod_result.scalar_one_or_none()
            if product and product.netto_purchase_price_pln:
                cogs_per_unit = float(product.netto_purchase_price_pln)
                line.cogs_pln = cogs_per_unit  # type: ignore[assignment]
                total_cogs += cogs_per_unit * qty

        fba = float(line.fba_fee_pln or 0)
        ref = float(line.referral_fee_pln or 0)
        total_fees += (fba + ref) * qty

    total_fees += float(order.shipping_surcharge_pln or 0)
    total_fees += float(order.promo_order_fee_pln or 0)
    total_fees += float(order.refund_commission_pln or 0)

    order.revenue_pln = round(total_revenue_pln, 2)  # type: ignore[assignment]
    order.cogs_pln = round(total_cogs, 2)  # type: ignore[assignment]
    order.amazon_fees_pln = round(total_fees, 2)  # type: ignore[assignment]

    ads = float(order.ads_cost_pln or 0)
    logistics = await resolve_profit_logistics_pln(
        db,
        amazon_order_id=order.amazon_order_id,
        legacy_logistics_pln=order.logistics_pln,
    )

    cm = total_revenue_pln - total_cogs - total_fees - ads - logistics
    order.contribution_margin_pln = round(cm, 2)  # type: ignore[assignment]
    order.cm_percent = round(cm / total_revenue_pln * 100, 4) if total_revenue_pln else 0  # type: ignore[assignment]


async def recalculate_profit_batch(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    marketplace_id: Optional[str] = None,
) -> int:
    """Run the canonical V2 batch recompute and refresh downstream snapshot."""
    import asyncio
    from app.connectors.mssql.mssql_store import (
        evaluate_alert_rules,
        recalc_profit_orders,
        sync_profit_snapshot,
    )

    del db

    if marketplace_id:
        log.warning(
            "profit.batch_marketplace_filter_ignored",
            marketplace_id=marketplace_id,
            reason="v2_recalc_is_date_scoped_only",
        )

    count = await asyncio.to_thread(
        recalc_profit_orders,
        date_from=date_from,
        date_to=date_to,
    )
    snapshot_rows = await asyncio.to_thread(
        sync_profit_snapshot,
        date_from=date_from,
        date_to=date_to,
    )

    alerts_generated = 0
    try:
        alerts_generated = await asyncio.to_thread(evaluate_alert_rules, 7)
    except Exception as exc:
        log.warning("profit.batch_alert_refresh_failed", error=str(exc))

    log.info(
        "profit.batch_complete",
        count=count,
        snapshot_rows=snapshot_rows,
        alerts_generated=alerts_generated,
    )
    return count
