"""KPI routes — Executive Dashboard data."""
from __future__ import annotations

import time
from datetime import date, timedelta
from typing import Any, Optional

import structlog
from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, column as sa_column, func, select, cast, Date, Numeric, String, table as sa_table, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from app.core.database import get_db
from app.core.security import require_analyst
from app.models.order import AccOrder
from app.models.marketplace import Marketplace
from app.models.alert import Alert
from app.models.order import OrderLine
from app.models.product import Product
from app.schemas.kpi import KPISummaryResponse, MarketplaceKPI, RevenueChartResponse, RevenueChartPoint, TrendChartResponse, TrendChartPoint
from app.models.shipment import AccOrderLogisticsFact
from app.services.order_logistics_source import profit_logistics_join_sqla, profit_logistics_value_sqla

log = structlog.get_logger(__name__)
router = APIRouter(prefix="/kpi", tags=["kpi"])


# ---------------------------------------------------------------------------
# In-memory TTL cache (replaces Redis when unavailable)
# ---------------------------------------------------------------------------
_MEM_CACHE: dict[str, tuple[float, Any]] = {}  # key → (expires_at, value)
_MEM_CACHE_MAX = 200  # max entries to prevent memory leak


def _cm1_expr_order(logistics_expr: Any | None = None) -> Any:
    """CM1 semantics (direct costs only): revenue - cogs - amazon_fees - logistics."""
    if logistics_expr is None:
        logistics_expr = profit_logistics_value_sqla(order_model=AccOrder)
    return (
        func.coalesce(AccOrder.revenue_pln, 0)
        - func.coalesce(AccOrder.cogs_pln, 0)
        - func.coalesce(AccOrder.amazon_fees_pln, 0)
        - logistics_expr
    )


def _cache_get(key: str) -> Any | None:
    """Return cached value if still valid, else None."""
    entry = _MEM_CACHE.get(key)
    if entry is None:
        return None
    expires_at, value = entry
    if time.monotonic() > expires_at:
        _MEM_CACHE.pop(key, None)
        return None
    return value


def _cache_set(key: str, value: Any, ttl: int = 300) -> None:
    """Cache a value with TTL in seconds."""
    # Evict expired entries if cache is getting large
    if len(_MEM_CACHE) > _MEM_CACHE_MAX:
        now = time.monotonic()
        expired = [k for k, (exp, _) in _MEM_CACHE.items() if now > exp]
        for k in expired:
            _MEM_CACHE.pop(k, None)
    _MEM_CACHE[key] = (time.monotonic() + ttl, value)


@router.get("/summary", response_model=KPISummaryResponse)
async def get_kpi_summary(
    date_from: date = Query(default=None),
    date_to: date = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    fulfillment_channel: Optional[str] = Query(default=None, description="FBA, MFN, or null for all"),
    current_user: dict = Depends(require_analyst),
    db: AsyncSession = Depends(get_db),
):
    if date_to is None:
        date_to = date.today()
    if date_from is None:
        date_from = date_to - timedelta(days=29)
    date_to_exclusive = date_to + timedelta(days=1)

    # In-memory cache
    cache_key = f"kpi:summary:{date_from}:{date_to}:{marketplace_id or 'all'}:{fulfillment_channel or 'all'}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    # ── Primary KPIs from pre-computed marketplace rollup (fast, NOLOCK) ──
    mkt_filter = "AND r.marketplace_id = :mkt" if marketplace_id else ""
    params: dict[str, Any] = {"d_from": str(date_from), "d_to": str(date_to)}
    if marketplace_id:
        params["mkt"] = marketplace_id

    rollup_q = text(f"""
        SELECT
            r.marketplace_id,
            ISNULL(SUM(r.revenue_pln), 0)         AS revenue_pln,
            ISNULL(SUM(r.total_orders), 0)         AS orders,
            ISNULL(SUM(r.total_units), 0)          AS units,
            ISNULL(SUM(r.cm1_pln), 0)              AS cm1_pln,
            ISNULL(SUM(r.cm2_pln), 0)              AS cm2_pln,
            ISNULL(SUM(r.overhead_pln), 0)         AS overhead_pln,
            ISNULL(SUM(r.profit_pln), 0)           AS net_profit_pln,
            ISNULL(SUM(r.ad_spend_pln), 0)         AS ads_spend_pln,
            ISNULL(SUM(r.logistics_pln), 0)        AS courier_cost_pln,
            ISNULL(SUM(r.refund_units), 0)         AS refund_units,
            ISNULL(SUM(r.refund_pln), 0)           AS refund_pln
        FROM dbo.acc_marketplace_profitability_rollup r WITH (NOLOCK)
        WHERE r.period_date >= CAST(:d_from AS DATE)
          AND r.period_date <= CAST(:d_to AS DATE)
          {mkt_filter}
        GROUP BY r.marketplace_id
    """)
    rollup_result = await db.execute(rollup_q, params)
    rows = rollup_result.fetchall()

    # Marketplace code lookup
    mkt_result = await db.execute(select(Marketplace))
    mkts = {m.id: m for m in mkt_result.scalars().all()}

    by_mkt: list[MarketplaceKPI] = []
    total_rev = total_cm = total_cm2 = total_np = total_ads = total_overhead = 0.0
    total_refund_pln_sum = 0.0
    total_refund_units_sum = 0
    total_orders = total_units = 0

    for row in rows:
        rev = float(row.revenue_pln or 0)
        cm = float(row.cm1_pln or 0)
        cm2 = float(row.cm2_pln or 0)
        overhead = float(row.overhead_pln or 0)
        np_ = float(row.net_profit_pln or 0)
        ads = float(row.ads_spend_pln or 0)
        orders = int(row.orders or 0)
        units = int(row.units or 0)
        courier = float(row.courier_cost_pln or 0)
        refund_u = int(row.refund_units or 0)
        refund_pln = float(row.refund_pln or 0)
        cm_pct = (cm / rev * 100) if rev else 0
        cm2_pct = (cm2 / rev * 100) if rev else 0
        np_pct = (np_ / rev * 100) if rev else 0
        acos = (ads / rev * 100) if rev else None
        ret_rate = round(refund_u / units * 100, 2) if units else None
        mkt = mkts.get(row.marketplace_id)

        by_mkt.append(MarketplaceKPI(
            marketplace_id=row.marketplace_id,
            marketplace_code=mkt.code if mkt else row.marketplace_id,
            revenue_pln=round(rev, 2),
            orders=orders,
            units=units,
            cm1_pln=round(cm, 2),
            cm1_percent=round(cm_pct, 2),
            cm2_pln=round(cm2, 2),
            cm2_percent=round(cm2_pct, 2),
            overhead_pln=round(overhead, 2),
            net_profit_pln=round(np_, 2),
            net_profit_percent=round(np_pct, 2),
            acos=round(acos, 2) if acos else None,
            ads_spend_pln=round(ads, 2),
            avg_order_value_pln=round(rev / orders, 2) if orders else 0,
            courier_cost_pln=round(courier, 2),
            return_rate_pct=ret_rate,
            refund_units=refund_u,
            refund_pln=round(refund_pln, 2),
        ))
        total_rev += rev
        total_orders += orders
        total_units += units
        total_cm += cm
        total_cm2 += cm2
        total_overhead += overhead
        total_np += np_
        total_ads += ads
        total_refund_pln_sum += refund_pln
        total_refund_units_sum += refund_u

    # Alert counts (lightweight queries on small table)
    alert_q = await db.execute(
        select(func.count(Alert.id)).where(Alert.is_resolved == False)  # noqa: E712
    )
    active_alerts = int(alert_q.scalar() or 0)
    critical_q = await db.execute(
        select(func.count(Alert.id)).where(
            Alert.is_resolved == False,  # noqa: E712
            Alert.severity == "critical",
        )
    )
    critical_alerts = int(critical_q.scalar() or 0)

    total_tacos = round(total_ads / total_rev * 100, 2) if total_rev else None

    # Courier cost from rollup (logistics_pln column)
    courier_mkt_filter = "AND r.marketplace_id = :mkt" if marketplace_id else ""
    courier_params: dict[str, Any] = {"d_from": str(date_from), "d_to": str(date_to)}
    if marketplace_id:
        courier_params["mkt"] = marketplace_id
    courier_sql = text(f"""
        SELECT
            ISNULL(SUM(r.logistics_pln), 0) AS courier_cost,
            ISNULL(SUM(r.refund_units), 0)  AS refund_units,
            ISNULL(SUM(r.total_units), 0)   AS total_units
        FROM dbo.acc_marketplace_profitability_rollup r WITH (NOLOCK)
        WHERE r.period_date >= CAST(:d_from AS DATE)
          AND r.period_date <= CAST(:d_to AS DATE)
          {courier_mkt_filter}
    """)
    courier_result = await db.execute(courier_sql, courier_params)
    courier_row = courier_result.fetchone()
    total_courier = float(courier_row.courier_cost or 0) if courier_row else 0
    total_refund_units = int(courier_row.refund_units or 0) if courier_row else 0
    total_sold_units = int(courier_row.total_units or 0) if courier_row else 0
    total_return_rate = round(total_refund_units / total_sold_units * 100, 2) if total_sold_units else None

    # FBM logistics coverage & avg cost per marketplace
    # Uses OUTER APPLY TOP 1 (ordered by calculated_at DESC) to deduplicate
    # multiple calc_version rows per order — same dedup logic as rollup engine.
    fbm_logistics_mkt_filter = "AND o.marketplace_id = :mkt" if marketplace_id else ""
    fbm_log_params: dict[str, Any] = {"d_from": str(date_from), "d_to": str(date_to)}
    if marketplace_id:
        fbm_log_params["mkt"] = marketplace_id
    fbm_logistics_sql = text(f"""
        SELECT
            m.code AS mkt_code,
            COUNT(DISTINCT o.id) AS fbm_orders,
            SUM(CASE WHEN olf.amazon_order_id IS NOT NULL THEN 1 ELSE 0 END) AS matched_orders,
            ISNULL(SUM(olf.total_logistics_pln), 0) AS total_cost,
            CASE WHEN SUM(CASE WHEN olf.amazon_order_id IS NOT NULL THEN 1 ELSE 0 END) > 0
                THEN SUM(olf.total_logistics_pln)
                     / SUM(CASE WHEN olf.amazon_order_id IS NOT NULL THEN 1 ELSE 0 END)
                ELSE 0 END AS avg_cost,
            SUM(CASE WHEN olf.calc_version IN ('dhl_v1','gls_v1') THEN 1 ELSE 0 END) AS billing_orders,
            CASE WHEN SUM(CASE WHEN olf.calc_version IN ('dhl_v1','gls_v1') THEN 1 ELSE 0 END) > 0
                THEN SUM(CASE WHEN olf.calc_version IN ('dhl_v1','gls_v1') THEN olf.total_logistics_pln ELSE 0 END)
                     / SUM(CASE WHEN olf.calc_version IN ('dhl_v1','gls_v1') THEN 1 ELSE 0 END)
                ELSE NULL END AS billing_avg_cost
        FROM dbo.acc_order o WITH (NOLOCK)
        OUTER APPLY (
            SELECT TOP 1
                olf_inner.amazon_order_id,
                olf_inner.total_logistics_pln,
                olf_inner.calc_version
            FROM dbo.acc_order_logistics_fact olf_inner WITH (NOLOCK)
            WHERE olf_inner.amazon_order_id = o.amazon_order_id
            ORDER BY olf_inner.calculated_at DESC
        ) olf
        LEFT JOIN dbo.acc_marketplace m WITH (NOLOCK)
            ON m.id = o.marketplace_id
        WHERE o.fulfillment_channel = 'MFN'
          AND o.status IN ('Shipped', 'Unshipped')
          AND o.purchase_date >= CAST(:d_from AS DATE)
          AND o.purchase_date < DATEADD(day, 1, CAST(:d_to AS DATE))
          {fbm_logistics_mkt_filter}
        GROUP BY o.marketplace_id, m.code
    """)
    fbm_log_result = await db.execute(fbm_logistics_sql, fbm_log_params)
    fbm_logistics_by_mkt: list[dict] = []
    total_fbm_matched = 0
    total_fbm_count = 0
    total_fbm_billing = 0
    for r in fbm_log_result.fetchall():
        fbm_cnt = int(r.fbm_orders or 0)
        matched = int(r.matched_orders or 0)
        billing = int(r.billing_orders or 0)
        avg_c = float(r.avg_cost or 0)
        billing_avg = float(r.billing_avg_cost) if r.billing_avg_cost is not None else None
        total_fbm_count += fbm_cnt
        total_fbm_matched += matched
        total_fbm_billing += billing
        if fbm_cnt > 0:
            entry: dict[str, Any] = {
                "mkt": r.mkt_code or "?",
                "avg_cost": round(avg_c, 2),
                "coverage_pct": round(matched / fbm_cnt * 100, 1),
                "billing_pct": round(billing / matched * 100, 1) if matched else 0,
            }
            if billing_avg is not None:
                entry["billing_avg_cost"] = round(billing_avg, 2)
            fbm_logistics_by_mkt.append(entry)
    fbm_coverage_pct = round(total_fbm_matched / total_fbm_count * 100, 1) if total_fbm_count else None
    fbm_billing_pct = round(total_fbm_billing / total_fbm_matched * 100, 1) if total_fbm_matched else None

    # True ACoS from PPC campaign data (spend / ad-attributed sales) — raw SQL
    acos_mkt_filter = "AND c.marketplace_id = :mkt" if marketplace_id else ""
    acos_params: dict[str, Any] = {"d_from": str(date_from), "d_to": str(date_to)}
    if marketplace_id:
        acos_params["mkt"] = marketplace_id
    acos_sql = text(f"""
        SELECT
            ISNULL(SUM(d.spend_pln), 0)  AS ppc_spend,
            ISNULL(SUM(d.sales_pln), 0)  AS ppc_sales
        FROM dbo.acc_ads_campaign_day d WITH (NOLOCK)
        INNER JOIN dbo.acc_ads_campaign c WITH (NOLOCK)
            ON d.campaign_id = c.campaign_id
        WHERE d.report_date >= CAST(:d_from AS DATE)
          AND d.report_date <  DATEADD(day, 1, CAST(:d_to AS DATE))
          {acos_mkt_filter}
    """)
    acos_result = await db.execute(acos_sql, acos_params)
    acos_row = acos_result.fetchone()
    ppc_spend = float(acos_row.ppc_spend or 0) if acos_row else 0
    ppc_sales = float(acos_row.ppc_sales or 0) if acos_row else 0
    total_acos = round(ppc_spend / ppc_sales * 100, 2) if ppc_sales else None

    # FBA / FBM channel breakdown — LEFT JOIN (faster than OUTER APPLY)
    chan_sql = text("""
        SELECT
            o.fulfillment_channel,
            COUNT(DISTINCT o.id)                    AS orders,
            ISNULL(SUM(ol.quantity_shipped), 0)      AS units
        FROM dbo.acc_order o WITH (NOLOCK)
        LEFT JOIN dbo.acc_order_line ol WITH (NOLOCK)
            ON ol.order_id = o.id
        WHERE o.status = 'Shipped'
          AND o.purchase_date >= CAST(:d_from AS DATE)
          AND o.purchase_date <  DATEADD(day, 1, CAST(:d_to AS DATE))
        GROUP BY o.fulfillment_channel
    """)
    chan_result = await db.execute(chan_sql, {"d_from": str(date_from), "d_to": str(date_to)})
    chan_orders: dict[str, int] = {}
    chan_units: dict[str, int] = {}
    for r in chan_result.fetchall():
        ch = str(r.fulfillment_channel or "")
        chan_orders[ch] = int(r.orders)
        chan_units[ch] = int(r.units)

    fba_orders = chan_orders.get("AFN", 0)
    fbm_orders = chan_orders.get("MFN", 0)
    fba_units = chan_units.get("AFN", 0)
    fbm_units = chan_units.get("MFN", 0)

    response = KPISummaryResponse(
        date_from=date_from,
        date_to=date_to,
        total_revenue_pln=round(total_rev, 2),
        total_orders=total_orders,
        total_units=total_units,
        total_cm1_pln=round(total_cm, 2),
        total_cm1_percent=round(total_cm / total_rev * 100, 2) if total_rev else 0,
        total_cm2_pln=round(total_cm2, 2),
        total_cm2_percent=round(total_cm2 / total_rev * 100, 2) if total_rev else 0,
        total_overhead_pln=round(total_overhead, 2),
        total_net_profit_pln=round(total_np, 2),
        total_net_profit_percent=round(total_np / total_rev * 100, 2) if total_rev else 0,
        total_ads_spend_pln=round(total_ads, 2),
        total_acos=total_acos,
        total_tacos=total_tacos,
        avg_order_value_pln=round(total_rev / total_orders, 2) if total_orders else 0,
        total_courier_cost_pln=round(total_courier, 2),
        total_return_rate_pct=total_return_rate,
        total_refund_pln=round(total_refund_pln_sum, 2),
        total_refund_units=total_refund_units_sum,
        fbm_logistics_by_mkt=fbm_logistics_by_mkt,
        fbm_coverage_pct=fbm_coverage_pct,
        fbm_billing_pct=fbm_billing_pct,
        fba_orders=fba_orders,
        fbm_orders=fbm_orders,
        fba_units=fba_units,
        fbm_units=fbm_units,
        fba_units_per_order=round(fba_units / fba_orders, 2) if fba_orders else None,
        fbm_units_per_order=round(fbm_units / fbm_orders, 2) if fbm_orders else None,
        by_marketplace=sorted(by_mkt, key=lambda x: x.revenue_pln, reverse=True),
        active_alerts_count=active_alerts,
        critical_alerts_count=critical_alerts,
    )

    # Latest successful order-sync timestamp for data freshness badge.
    last_sync = None
    try:
        last_sync_result = await db.execute(
            text(
                """
                SELECT MAX(last_finished_at)
                FROM dbo.acc_order_sync_state WITH (NOLOCK)
                WHERE LOWER(ISNULL(last_status, '')) = 'success'
                """
            )
        )
        last_sync = last_sync_result.scalar()
    except Exception:
        last_sync = None
    if last_sync is None:
        fallback_last_sync_result = await db.execute(select(func.max(AccOrder.synced_at)))
        last_sync = fallback_last_sync_result.scalar()
    if hasattr(last_sync, "isoformat"):
        response.last_sync = last_sync.isoformat()
    elif last_sync is not None:
        response.last_sync = str(last_sync)
    else:
        response.last_sync = None

    # ---- Delta vs prior period (from rollup — fast) ----
    span = (date_to - date_from).days
    prev_to = date_from - timedelta(days=1)
    prev_from = prev_to - timedelta(days=span)

    prev_q = text("""
        SELECT
            ISNULL(SUM(r.revenue_pln), 0) AS prev_rev,
            ISNULL(SUM(r.total_orders), 0) AS prev_orders,
            ISNULL(SUM(r.cm1_pln), 0) AS prev_cm
        FROM dbo.acc_marketplace_profitability_rollup r WITH (NOLOCK)
        WHERE r.period_date >= CAST(:pf AS DATE)
          AND r.period_date <= CAST(:pt AS DATE)
    """)
    prev_params: dict[str, str] = {"pf": str(prev_from), "pt": str(prev_to)}
    prev_result = await db.execute(prev_q, prev_params)
    prev = prev_result.fetchone()
    if prev:
        prev_rev = float(prev.prev_rev or 0)
        prev_orders = int(prev.prev_orders or 0)
        prev_cm = float(prev.prev_cm or 0)
        if prev_rev > 0:
            response.revenue_delta_pct = round((total_rev - prev_rev) / prev_rev * 100, 2)
        if prev_orders > 0:
            response.orders_delta_pct = round((total_orders - prev_orders) / prev_orders * 100, 2)
        if prev_cm > 0:
            response.cm1_delta_pct = round((total_cm - prev_cm) / prev_cm * 100, 2)

    _cache_set(cache_key, response, ttl=300)

    return response


@router.get("/chart/revenue", response_model=RevenueChartResponse)
async def get_revenue_chart(
    date_from: date = Query(default=None),
    date_to: date = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    fulfillment_channel: Optional[str] = Query(default=None),
    current_user: dict = Depends(require_analyst),
    db: AsyncSession = Depends(get_db),
):
    if date_to is None:
        date_to = date.today()
    if date_from is None:
        date_from = date_to - timedelta(days=29)

    # In-memory cache
    chart_cache_key = f"kpi:chart:{date_from}:{date_to}:{marketplace_id or 'all'}:{fulfillment_channel or 'all'}"
    cached = _cache_get(chart_cache_key)
    if cached is not None:
        return cached

    # Read from marketplace rollup (fast, NOLOCK) — grouped by day
    mkt_filter = "AND r.marketplace_id = :mkt" if marketplace_id else ""
    params: dict[str, Any] = {"d_from": str(date_from), "d_to": str(date_to)}
    if marketplace_id:
        params["mkt"] = marketplace_id

    chart_q = text(f"""
        SELECT
            r.period_date AS day,
            ISNULL(SUM(r.revenue_pln), 0) AS revenue_pln,
            ISNULL(SUM(r.cm1_pln), 0)     AS cm1_pln,
            ISNULL(SUM(r.total_orders), 0) AS orders
        FROM dbo.acc_marketplace_profitability_rollup r WITH (NOLOCK)
        WHERE r.period_date >= CAST(:d_from AS DATE)
          AND r.period_date <= CAST(:d_to AS DATE)
          {mkt_filter}
        GROUP BY r.period_date
        ORDER BY r.period_date
    """)
    result = await db.execute(chart_q, params)
    points = [
        RevenueChartPoint(
            date=row.day,
            revenue_pln=float(row.revenue_pln or 0),
            cm1_pln=float(row.cm1_pln or 0),
            orders=int(row.orders or 0),
        )
        for row in result.fetchall()
    ]

    chart_response = RevenueChartResponse(
        date_from=date_from,
        date_to=date_to,
        marketplace_id=marketplace_id,
        points=points,
    )
    _cache_set(chart_cache_key, chart_response, ttl=300)

    return chart_response


@router.get("/chart/trends", response_model=TrendChartResponse)
async def get_trend_chart(
    date_from: date = Query(default=None),
    date_to: date = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    current_user: dict = Depends(require_analyst),
    db: AsyncSession = Depends(get_db),
):
    """Multi-metric daily trend chart (revenue, CM1, CM2, profit, orders, units, ad_spend)."""
    if date_to is None:
        date_to = date.today()
    if date_from is None:
        date_from = date_to - timedelta(days=29)

    trend_cache_key = f"kpi:trends:{date_from}:{date_to}:{marketplace_id or 'all'}"
    cached = _cache_get(trend_cache_key)
    if cached is not None:
        return cached

    mkt_filter = "AND r.marketplace_id = :mkt" if marketplace_id else ""
    params: dict[str, Any] = {"d_from": str(date_from), "d_to": str(date_to)}
    if marketplace_id:
        params["mkt"] = marketplace_id

    trend_q = text(f"""
        SELECT
            r.period_date AS day,
            ISNULL(SUM(r.revenue_pln), 0)     AS revenue_pln,
            ISNULL(SUM(r.cm1_pln), 0)         AS cm1_pln,
            ISNULL(SUM(r.cm2_pln), 0)         AS cm2_pln,
            ISNULL(SUM(r.profit_pln), 0)      AS profit_pln,
            ISNULL(SUM(r.total_orders), 0)    AS orders,
            ISNULL(SUM(r.total_units), 0)     AS units,
            ISNULL(SUM(r.ad_spend_pln), 0)    AS ad_spend_pln
        FROM dbo.acc_marketplace_profitability_rollup r WITH (NOLOCK)
        WHERE r.period_date >= CAST(:d_from AS DATE)
          AND r.period_date <= CAST(:d_to AS DATE)
          {mkt_filter}
        GROUP BY r.period_date
        ORDER BY r.period_date
    """)
    result = await db.execute(trend_q, params)
    points = [
        TrendChartPoint(
            date=row.day,
            revenue_pln=float(row.revenue_pln or 0),
            cm1_pln=float(row.cm1_pln or 0),
            cm2_pln=float(row.cm2_pln or 0),
            profit_pln=float(row.profit_pln or 0),
            orders=int(row.orders or 0),
            units=int(row.units or 0),
            ad_spend_pln=float(row.ad_spend_pln or 0),
        )
        for row in result.fetchall()
    ]

    trend_response = TrendChartResponse(
        date_from=date_from,
        date_to=date_to,
        marketplace_id=marketplace_id,
        points=points,
    )
    _cache_set(trend_cache_key, trend_response, ttl=300)

    return trend_response


@router.get("/marketplaces")
async def list_marketplaces(
    current_user: dict = Depends(require_analyst),
    db: AsyncSession = Depends(get_db),
):
    """Return list of marketplaces that have orders."""
    # In-memory cache (long TTL — marketplace list doesn't change often)
    cached = _cache_get("kpi:marketplaces")
    if cached is not None:
        return cached

    result = await db.execute(
        select(
            AccOrder.marketplace_id,
            func.count(AccOrder.id).label("order_count"),
        )
        .where(AccOrder.status == "Shipped")
        .group_by(AccOrder.marketplace_id)
        .order_by(func.count(AccOrder.id).desc())
    )
    rows = result.fetchall()

    mkt_result = await db.execute(select(Marketplace))
    mkts = {m.id: m for m in mkt_result.scalars().all()}

    mkt_list = [
        {
            "marketplace_id": row.marketplace_id,
            "code": mkts[row.marketplace_id].code if row.marketplace_id in mkts else row.marketplace_id,
            "country": mkts[row.marketplace_id].code if row.marketplace_id in mkts else "?",
            "order_count": row.order_count,
        }
        for row in rows
    ]
    _cache_set("kpi:marketplaces", mkt_list, ttl=600)
    return mkt_list


@router.get("/top-drivers")
async def top_profit_drivers(
    date_from: date = Query(default=None),
    date_to: date = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    fulfillment_channel: Optional[str] = Query(default=None),
    brand: Optional[str] = Query(default=None),
    category: Optional[str] = Query(default=None, description="Polish category from PIM / acc_product.category"),
    limit: int = Query(default=10, ge=1, le=50),
    current_user: dict = Depends(require_analyst),
    db: AsyncSession = Depends(get_db),
):
    """Return top SKUs by absolute CM contribution (positive = drivers, negative = leaks)."""
    if date_to is None:
        date_to = date.today()
    if date_from is None:
        date_from = date_to - timedelta(days=29)
    date_to_exclusive = date_to + timedelta(days=1)

    # In-memory cache
    drivers_cache_key = (
        f"kpi:drivers:{date_from}:{date_to}:{marketplace_id or 'all'}:"
        f"{fulfillment_channel or 'all'}:{(brand or '').strip().lower() or 'all'}:"
        f"{(category or '').strip().lower() or 'all'}:{limit}"
    )
    cached = _cache_get(drivers_cache_key)
    if cached is not None:
        return cached

    try:
        # Check if acc_product table exists
        check = await db.execute(
            text("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'acc_product'")
        )
        has_product_table = check.scalar() is not None
    except Exception:
        has_product_table = False

    try:
        # ASIN-first contribution (revenue - cogs - line fees - ads allocated by ASIN/day feed)
        line_revenue = case(
            (
                (AccOrder.order_total.isnot(None)) & (AccOrder.order_total != 0),
                OrderLine.item_price * AccOrder.revenue_pln / AccOrder.order_total,
            ),
            else_=cast(0, Numeric(12, 2)),
        )
        line_share = case(
            (
                (AccOrder.order_total.isnot(None)) & (AccOrder.order_total != 0),
                func.coalesce(OrderLine.item_price, cast(0, Numeric(12, 2))) / AccOrder.order_total,
            ),
            else_=cast(0, Numeric(14, 6)),
        )
        order_logistics_expr = profit_logistics_value_sqla(order_model=AccOrder, fact_model=True)
        line_logistics = order_logistics_expr * line_share

        # acc_import_products carries the canonical Polish product name (nazwa_pelna)
        # sourced from the official price-list / cennik.  Join via internal_sku.
        _ip = sa_table("acc_import_products", sa_column("sku"), sa_column("nazwa_pelna"))

        if has_product_table:
            q = (
                select(
                    OrderLine.asin,
                    AccOrder.marketplace_id,
                    func.max(OrderLine.sku).label("sample_sku"),
                    func.coalesce(
                        func.max(_ip.c.nazwa_pelna),   # Polish name from cennik
                        func.max(Product.title),       # PIM title (may be PL or marketplace lang)
                        func.max(OrderLine.title),     # marketplace-language fallback
                    ).label("title"),
                    func.max(Product.internal_sku).label("internal_sku"),
                    func.sum(OrderLine.quantity_shipped).label("units"),
                    func.sum(line_revenue).label("revenue_pln"),
                    func.sum(func.coalesce(OrderLine.cogs_pln, cast(0, Numeric))).label("cogs_pln"),
                    func.sum(
                        func.coalesce(OrderLine.fba_fee_pln, cast(0, Numeric))
                        + func.coalesce(OrderLine.referral_fee_pln, cast(0, Numeric))
                    ).label("fees_pln"),
                    func.sum(line_logistics).label("logistics_pln"),
                )
                .join(AccOrder, AccOrder.id == OrderLine.order_id)
                .outerjoin(Product, Product.id == OrderLine.product_id)
                .outerjoin(_ip, _ip.c.sku == Product.internal_sku)
                .where(
                    AccOrder.status == "Shipped",
                    AccOrder.purchase_date >= date_from,
                    AccOrder.purchase_date < date_to_exclusive,
                    OrderLine.asin.isnot(None),
                )
                .group_by(OrderLine.asin, AccOrder.marketplace_id)
            )
        else:
            q = (
                select(
                    OrderLine.asin,
                    AccOrder.marketplace_id,
                    func.max(OrderLine.sku).label("sample_sku"),
                    func.max(OrderLine.title).label("title"),
                    cast(None, String(20)).label("internal_sku"),
                    func.sum(OrderLine.quantity_shipped).label("units"),
                    func.sum(line_revenue).label("revenue_pln"),
                    func.sum(func.coalesce(OrderLine.cogs_pln, cast(0, Numeric))).label("cogs_pln"),
                    func.sum(
                        func.coalesce(OrderLine.fba_fee_pln, cast(0, Numeric))
                        + func.coalesce(OrderLine.referral_fee_pln, cast(0, Numeric))
                    ).label("fees_pln"),
                    func.sum(line_logistics).label("logistics_pln"),
                )
                .join(AccOrder, AccOrder.id == OrderLine.order_id)
                .where(
                    AccOrder.status == "Shipped",
                    AccOrder.purchase_date >= date_from,
                    AccOrder.purchase_date < date_to_exclusive,
                    OrderLine.asin.isnot(None),
                )
                .group_by(OrderLine.asin, AccOrder.marketplace_id)
            )

        q = profit_logistics_join_sqla(q, order_model=AccOrder)

        if marketplace_id:
            q = q.where(AccOrder.marketplace_id == marketplace_id)
        if fulfillment_channel:
            q = q.where(AccOrder.fulfillment_channel == fulfillment_channel)
        if has_product_table and brand and brand.strip():
            brand_like = f"%{brand.strip().lower()}%"
            q = q.where(func.lower(func.coalesce(Product.brand, "")).like(brand_like))
        if has_product_table and category and category.strip():
            category_like = f"%{category.strip().lower()}%"
            q = q.where(func.lower(func.coalesce(Product.category, "")).like(category_like))

        result = await db.execute(q)
        rows = result.fetchall()

        items_by_asin: dict[str, dict[str, Any]] = {}
        for row in rows:
            asin_key = str(row.asin or "").strip()
            if not asin_key:
                continue
            mp = str(row.marketplace_id or "").strip()
            rev = float(row.revenue_pln or 0)
            cogs = float(row.cogs_pln or 0)
            fees = float(row.fees_pln or 0)
            logistics = float(row.logistics_pln or 0)
            if asin_key not in items_by_asin:
                items_by_asin[asin_key] = {
                    "sku": str(getattr(row, "sample_sku", "") or asin_key),
                    "asin": asin_key,
                    "title": ((row.title or "")[:80]),
                    "internal_sku": getattr(row, "internal_sku", None),
                    "units": 0,
                    "revenue_pln": 0.0,
                    "cogs_pln": 0.0,
                    "fees_pln": 0.0,
                    "logistics_pln": 0.0,
                    "cm1_pln": 0.0,
                    "cm1_percent": 0.0,
                }
            bucket = items_by_asin[asin_key]
            bucket["units"] += int(row.units or 0)
            bucket["revenue_pln"] += rev
            bucket["cogs_pln"] += cogs
            bucket["fees_pln"] += fees
            bucket["logistics_pln"] += logistics

        items = []
        for bucket in items_by_asin.values():
            rev = float(bucket["revenue_pln"] or 0.0)
            cogs = float(bucket["cogs_pln"] or 0.0)
            fees = float(bucket["fees_pln"] or 0.0)
            logistics = float(bucket["logistics_pln"] or 0.0)
            cm = rev - cogs - fees - logistics
            bucket["revenue_pln"] = round(rev, 2)
            bucket["cogs_pln"] = round(cogs, 2)
            bucket["cm1_pln"] = round(cm, 2)
            bucket["cm1_percent"] = round(cm / rev * 100, 2) if rev else 0
            items.append(bucket)

        items.sort(key=lambda x: x["cm1_pln"], reverse=True)

        # Leaks: only items with actual sales (units > 0) that have
        # low/negative margin.  Items with 0 sales are not "leaks".
        leaks_pool = [i for i in items if i["units"] > 0 and i["revenue_pln"] > 0]
        leaks = sorted(leaks_pool, key=lambda x: x["cm1_pln"])[:limit]

        result_data = {
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "drivers": items[:limit],
            "leaks": leaks,
        }
        _cache_set(drivers_cache_key, result_data, ttl=300)
        return result_data
    except Exception as exc:
        log.error("top_profit_drivers.failed", error=str(exc))
        return {
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "drivers": [],
            "leaks": [],
            "error": str(exc),
        }


@router.get("/recent-alerts")
async def recent_alerts(
    limit: int = Query(default=5, ge=1, le=20),
    current_user: dict = Depends(require_analyst),
    db: AsyncSession = Depends(get_db),
):
    """Return most recent unresolved alerts for the dashboard panel."""
    result = await db.execute(
        select(Alert)
        .where(Alert.is_resolved == False)  # noqa: E712
        .order_by(Alert.triggered_at.desc())
        .limit(limit)
    )
    alerts = result.scalars().all()

    return [
        {
            "id": str(a.id),
            "title": a.title,
            "detail": a.detail,
            "severity": a.severity,
            "marketplace_id": a.marketplace_id,
            "sku": a.sku,
            "is_read": a.is_read,
            "triggered_at": a.triggered_at.isoformat() if a.triggered_at else None,
        }
        for a in alerts
    ]
