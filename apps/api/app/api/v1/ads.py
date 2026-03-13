"""API routes — Ads / PPC module.

Uses raw pyodbc/pymssql SQL against acc_ads_* tables
(same pattern as profit_v2, manage_inventory, etc.).
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.concurrency import run_in_threadpool

from app.connectors.mssql import enqueue_job
from app.core.config import MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc
from app.core.security import get_current_user
from app.schemas.jobs import JobRunOut
from app.schemas.ads_schema import (
    AdsSummaryResponse,
    AdsChartResponse,
    AdsChartPoint,
    CampaignOut,
    TopCampaignRow,
)

log = logging.getLogger(__name__)

router = APIRouter(prefix="/ads", tags=["ads"])

# Build marketplace_id → code lookup
_MP_CODE: dict[str, str] = {
    mp_id: info["code"] for mp_id, info in MARKETPLACE_REGISTRY.items()
}


# ---------------------------------------------------------------------------
# GET /ads/campaigns
# ---------------------------------------------------------------------------
@router.get("/campaigns", response_model=list[CampaignOut])
async def list_campaigns(
    marketplace_id: Optional[str] = Query(None),
    ad_type: Optional[str] = Query(None, description="SP, SB, or SD"),
    state: Optional[str] = Query(None, description="ENABLED, PAUSED, ARCHIVED"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=500),
    user=Depends(get_current_user),
):
    """List campaigns from acc_ads_campaign."""
    conn = connect_acc()
    cur = conn.cursor()

    where_clauses: list[str] = []
    params: list[Any] = []

    if marketplace_id:
        where_clauses.append("c.marketplace_id = ?")
        params.append(marketplace_id)
    if ad_type:
        where_clauses.append("c.ad_type = ?")
        params.append(ad_type.upper())
    if state:
        where_clauses.append("c.state = ?")
        params.append(state.upper())

    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    offset = (page - 1) * page_size

    sql = f"""
        SELECT c.campaign_id, c.profile_id, c.marketplace_id,
               c.campaign_name, c.ad_type, c.state,
               c.targeting_type, c.daily_budget, c.currency,
               c.start_date, c.end_date, c.synced_at
        FROM acc_ads_campaign c WITH (NOLOCK)
        {where_sql}
        ORDER BY c.campaign_name
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
    """
    params.extend([offset, page_size])

    try:
        cur.execute(sql, params)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    result = []
    for r in rows:
        mp_code = _MP_CODE.get(r[2], "??")
        result.append(CampaignOut(
            campaign_id=r[0],
            marketplace_id=r[2],
            marketplace_code=mp_code,
            campaign_name=r[3],
            ad_type=r[4],
            state=r[5],
            targeting_type=r[6] or "",
            daily_budget=float(r[7]) if r[7] else None,
            currency=r[8] or "EUR",
            is_active=r[5] == "ENABLED",
        ))
    return result


# ---------------------------------------------------------------------------
# GET /ads/summary
# ---------------------------------------------------------------------------
@router.get("/summary", response_model=AdsSummaryResponse)
async def ads_summary(
    days: int = Query(30, ge=1, le=365),
    marketplace_id: Optional[str] = Query(None),
    ad_type: Optional[str] = Query(None),
    user=Depends(get_current_user),
):
    """Aggregate KPIs over the given period."""
    since = date.today() - timedelta(days=days)
    conn = connect_acc()
    cur = conn.cursor()

    where_clauses = ["d.report_date >= ?"]
    params: list[Any] = [since]

    if marketplace_id:
        where_clauses.append("""
            d.campaign_id IN (
                SELECT campaign_id FROM acc_ads_campaign WITH (NOLOCK)
                WHERE marketplace_id = ?
            )
        """)
        params.append(marketplace_id)
    if ad_type:
        where_clauses.append("d.ad_type = ?")
        params.append(ad_type.upper())

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = f"""
        SELECT ISNULL(SUM(d.spend_pln), 0)   AS spend,
               ISNULL(SUM(d.sales_pln), 0)   AS sales,
               ISNULL(SUM(d.orders_7d), 0)   AS orders,
               ISNULL(SUM(d.impressions), 0) AS impressions,
               ISNULL(SUM(d.clicks), 0)      AS clicks
        FROM acc_ads_campaign_day d WITH (NOLOCK)
        {where_sql}
    """

    try:
        cur.execute(sql, params)
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    spend = float(row[0])
    sales = float(row[1])
    orders = int(row[2])
    impressions = int(row[3])
    clicks = int(row[4])

    return AdsSummaryResponse(
        period_days=days,
        total_spend_pln=round(spend, 2),
        total_sales_pln=round(sales, 2),
        total_orders=orders,
        total_impressions=impressions,
        total_clicks=clicks,
        avg_acos=round(spend / sales * 100, 2) if sales else 0.0,
        avg_roas=round(sales / spend, 2) if spend else 0.0,
        avg_cpc=round(spend / clicks, 4) if clicks else 0.0,
        avg_ctr=round(clicks / impressions * 100, 4) if impressions else 0.0,
    )


# ---------------------------------------------------------------------------
# GET /ads/chart
# ---------------------------------------------------------------------------
@router.get("/chart", response_model=AdsChartResponse)
async def ads_chart(
    days: int = Query(30, ge=7, le=365),
    campaign_id: Optional[str] = Query(None),
    marketplace_id: Optional[str] = Query(None),
    ad_type: Optional[str] = Query(None),
    user=Depends(get_current_user),
):
    """Daily spend vs sales time series."""
    since = date.today() - timedelta(days=days)
    conn = connect_acc()
    cur = conn.cursor()

    where_clauses = ["d.report_date >= ?"]
    params: list[Any] = [since]

    if campaign_id:
        where_clauses.append("d.campaign_id = ?")
        params.append(campaign_id)
    if marketplace_id:
        where_clauses.append("""
            d.campaign_id IN (
                SELECT campaign_id FROM acc_ads_campaign WITH (NOLOCK)
                WHERE marketplace_id = ?
            )
        """)
        params.append(marketplace_id)
    if ad_type:
        where_clauses.append("d.ad_type = ?")
        params.append(ad_type.upper())

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = f"""
        SELECT d.report_date,
               ISNULL(SUM(d.spend_pln), 0) AS spend_pln,
               ISNULL(SUM(d.sales_pln), 0) AS sales_pln,
               ISNULL(SUM(d.orders_7d), 0) AS orders
        FROM acc_ads_campaign_day d WITH (NOLOCK)
        {where_sql}
        GROUP BY d.report_date
        ORDER BY d.report_date
    """

    try:
        cur.execute(sql, params)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    points = []
    for r in rows:
        spend = float(r[1])
        sales = float(r[2])
        points.append(AdsChartPoint(
            report_date=r[0].isoformat(),
            spend_pln=round(spend, 2),
            sales_pln=round(sales, 2),
            acos=round(spend / sales * 100, 2) if sales else 0.0,
            roas=round(sales / spend, 2) if spend else 0.0,
            orders=int(r[3]),
        ))

    return AdsChartResponse(
        points=points,
        campaign_id=campaign_id,
        marketplace_id=marketplace_id,
    )


# ---------------------------------------------------------------------------
# GET /ads/top-campaigns
# ---------------------------------------------------------------------------
@router.get("/top-campaigns", response_model=list[TopCampaignRow])
async def top_campaigns(
    days: int = Query(30, ge=1, le=365),
    marketplace_id: Optional[str] = Query(None),
    ad_type: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    user=Depends(get_current_user),
):
    """Top campaigns ranked by sales (with efficiency score)."""
    since = date.today() - timedelta(days=days)
    conn = connect_acc()
    cur = conn.cursor()

    where_clauses = ["d.report_date >= ?"]
    params: list[Any] = [since]

    if marketplace_id:
        where_clauses.append("c.marketplace_id = ?")
        params.append(marketplace_id)
    if ad_type:
        where_clauses.append("c.ad_type = ?")
        params.append(ad_type.upper())

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = f"""
        SELECT TOP ?
               c.campaign_id,
               c.campaign_name,
               c.marketplace_id,
               c.ad_type,
               ISNULL(SUM(d.spend_pln), 0) AS spend,
               ISNULL(SUM(d.sales_pln), 0) AS sales,
               ISNULL(SUM(d.orders_7d), 0) AS orders,
               ISNULL(SUM(d.impressions), 0) AS impressions,
               ISNULL(SUM(d.clicks), 0) AS clicks
        FROM acc_ads_campaign c WITH (NOLOCK)
        JOIN acc_ads_campaign_day d WITH (NOLOCK)
             ON d.campaign_id = c.campaign_id AND d.ad_type = c.ad_type
        {where_sql}
        GROUP BY c.campaign_id, c.campaign_name, c.marketplace_id, c.ad_type
        ORDER BY SUM(d.sales_pln) DESC
    """

    try:
        cur.execute(sql, [limit] + params)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    result = []
    for r in rows:
        spend = float(r[4])
        sales = float(r[5])
        acos = spend / sales * 100 if sales else 0.0
        roas = sales / spend if spend else 0.0
        score = min(100.0, max(0.0, roas * 20 - acos * 0.5))
        mp_code = _MP_CODE.get(r[2], "??")

        result.append(TopCampaignRow(
            campaign_id=r[0],
            campaign_name=r[1],
            marketplace_code=mp_code,
            total_spend_pln=round(spend, 2),
            total_sales_pln=round(sales, 2),
            avg_acos=round(acos, 2),
            avg_roas=round(roas, 2),
            orders=int(r[6]),
            efficiency_score=round(score, 1),
        ))
    return result


# ---------------------------------------------------------------------------
# GET /ads/profiles  — quick profile list
# ---------------------------------------------------------------------------
@router.get("/profiles")
async def list_profiles(user=Depends(get_current_user)):
    """Return all synced advertising profiles."""
    conn = connect_acc()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT profile_id, marketplace_id, country_code, currency,
                   account_type, account_name, synced_at
            FROM acc_ads_profile WITH (NOLOCK)
            ORDER BY country_code
        """)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return [
        {
            "profile_id": r[0],
            "marketplace_id": r[1],
            "country_code": r[2],
            "currency": r[3],
            "account_type": r[4],
            "account_name": r[5],
            "synced_at": r[6].isoformat() if r[6] else None,
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# POST /ads/sync  — trigger manual sync
# ---------------------------------------------------------------------------
@router.post("/sync", response_model=JobRunOut, status_code=202)
async def trigger_ads_sync(
    days_back: int = Query(3, ge=1, le=90),
    user=Depends(get_current_user),
):
    """Manually trigger full ads sync (profiles -> campaigns -> reports)."""
    try:
        return await run_in_threadpool(
            enqueue_job,
            job_type="sync_ads",
            marketplace_id=None,
            trigger_source="manual",
            triggered_by=str((user or {}).get("user_id") or "system"),
            params={"days_back": days_back},
        )
    except Exception as exc:
        log.error("ads.sync.manual_error", exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# GET /ads/campaign-stats  — per-ad-type breakdown
# ---------------------------------------------------------------------------
@router.get("/campaign-stats")
async def campaign_stats(
    days: int = Query(30, ge=1, le=365),
    marketplace_id: Optional[str] = Query(None),
    ad_type: Optional[str] = Query(None),
    user=Depends(get_current_user),
):
    """Per-ad-type aggregate stats (useful for pie charts / breakdowns)."""
    since = date.today() - timedelta(days=days)
    conn = connect_acc()
    cur = conn.cursor()

    where_clauses = ["d.report_date >= ?"]
    params: list[Any] = [since]
    if marketplace_id:
        where_clauses.append("""
            d.campaign_id IN (
                SELECT campaign_id FROM acc_ads_campaign WITH (NOLOCK)
                WHERE marketplace_id = ?
            )
        """)
        params.append(marketplace_id)
    if ad_type:
        where_clauses.append("d.ad_type = ?")
        params.append(ad_type.upper())

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = f"""
        SELECT d.ad_type,
               COUNT(DISTINCT d.campaign_id)     AS campaigns,
               ISNULL(SUM(d.spend_pln), 0)       AS spend,
               ISNULL(SUM(d.sales_pln), 0)       AS sales,
               ISNULL(SUM(d.orders_7d), 0)       AS orders,
               ISNULL(SUM(d.impressions), 0)      AS impressions,
               ISNULL(SUM(d.clicks), 0)           AS clicks,
               COUNT(DISTINCT d.report_date)      AS days_with_data
        FROM acc_ads_campaign_day d WITH (NOLOCK)
        {where_sql}
        GROUP BY d.ad_type
        ORDER BY SUM(d.sales_pln) DESC
    """

    try:
        cur.execute(sql, params)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return [
        {
            "ad_type": r[0],
            "campaigns_active": int(r[1]),
            "spend_pln": round(float(r[2]), 2),
            "sales_pln": round(float(r[3]), 2),
            "orders": int(r[4]),
            "impressions": int(r[5]),
            "clicks": int(r[6]),
            "days_with_data": int(r[7]),
            "acos": round(float(r[2]) / float(r[3]) * 100, 2) if float(r[3]) else 0.0,
            "roas": round(float(r[3]) / float(r[2]), 2) if float(r[2]) else 0.0,
        }
        for r in rows
    ]
