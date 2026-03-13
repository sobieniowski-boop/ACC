"""Amazon Ads API — Reporting (v3 async reports).

Uses the new v3 Reporting API:
1. POST /reporting/reports  → create report
2. Poll GET /reporting/reports/{reportId} until status=COMPLETED
3. Download gzip JSON from the URL in the response

Docs: https://advertising.amazon.com/API/docs/en-us/reporting/v3/overview
"""
from __future__ import annotations

import asyncio
import gzip
import json
from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

import structlog

from app.connectors.amazon_ads_api.client import AdsAPIClient

log = structlog.get_logger(__name__)


@dataclass
class CampaignDayMetrics:
    """One row from the campaign performance report."""
    campaign_id: str
    campaign_name: str
    ad_type: str  # SP, SB, SD
    report_date: date
    impressions: int
    clicks: int
    spend: float
    sales_7d: float  # attributedSales7d
    orders_7d: int  # attributedConversions7d
    units_7d: int  # attributedUnits7d
    currency: str


@dataclass
class ProductDayMetrics:
    """One row from the advertised product performance report (per ASIN)."""
    asin: str
    sku: str
    ad_type: str  # SP, SB, SD
    campaign_id: str
    report_date: date
    marketplace_id: str
    impressions: int
    clicks: int
    spend: float
    sales_7d: float
    orders_7d: int
    units_7d: int
    currency: str


async def request_sp_campaign_report(
    profile_id: int,
    start_date: date,
    end_date: date,
    poll_interval: float = 15.0,
    max_polls: int = 40,
) -> list[CampaignDayMetrics]:
    """Request and download a Sponsored Products campaign report.

    Groups by campaign + date, returns daily metrics.
    """
    client = AdsAPIClient(profile_id=profile_id)

    body = {
        "name": f"SP Campaign Report {start_date}..{end_date}",
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "groupBy": ["campaign"],
            "columns": [
                "campaignId", "campaignName", "date",
                "impressions", "clicks", "spend",
                "sales7d", "unitsSoldClicks7d", "purchases7d",
            ],
            "reportTypeId": "spCampaigns",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON",
        },
    }

    return await _create_poll_download(client, body, "SP", poll_interval, max_polls)


async def request_sb_campaign_report(
    profile_id: int,
    start_date: date,
    end_date: date,
    poll_interval: float = 15.0,
    max_polls: int = 40,
) -> list[CampaignDayMetrics]:
    """Sponsored Brands campaign report."""
    client = AdsAPIClient(profile_id=profile_id)

    body = {
        "name": f"SB Campaign Report {start_date}..{end_date}",
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "configuration": {
            "adProduct": "SPONSORED_BRANDS",
            "groupBy": ["campaign"],
            "columns": [
                "campaignId", "campaignName", "date",
                "impressions", "clicks", "cost",
                "sales", "unitsSold", "purchases",
            ],
            "reportTypeId": "sbCampaigns",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON",
        },
    }

    return await _create_poll_download(client, body, "SB", poll_interval, max_polls)


async def request_sd_campaign_report(
    profile_id: int,
    start_date: date,
    end_date: date,
    poll_interval: float = 15.0,
    max_polls: int = 40,
) -> list[CampaignDayMetrics]:
    """Sponsored Display campaign report."""
    client = AdsAPIClient(profile_id=profile_id)

    body = {
        "name": f"SD Campaign Report {start_date}..{end_date}",
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "configuration": {
            "adProduct": "SPONSORED_DISPLAY",
            "groupBy": ["campaign"],
            "columns": [
                "campaignId", "campaignName", "date",
                "impressions", "clicks", "cost",
                "sales", "unitsSold", "purchases",
            ],
            "reportTypeId": "sdCampaigns",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON",
        },
    }

    return await _create_poll_download(client, body, "SD", poll_interval, max_polls)


async def _create_poll_download(
    client: AdsAPIClient,
    body: dict,
    ad_type: str,
    poll_interval: float,
    max_polls: int,
) -> list[CampaignDayMetrics]:
    """Create report → poll → download → parse."""
    # Step 1: Create report
    create_resp = await client.post("/reporting/reports", body=body)
    report_id = create_resp.get("reportId")
    if not report_id:
        log.error("ads_api.report.no_id", response=create_resp)
        return []

    log.info("ads_api.report.created", report_id=report_id, ad_type=ad_type)

    # Step 2: Poll until COMPLETED
    download_url: Optional[str] = None
    for poll in range(max_polls):
        await asyncio.sleep(poll_interval)
        status_resp = await client.get(f"/reporting/reports/{report_id}")
        status = status_resp.get("status", "")

        if status == "COMPLETED":
            download_url = status_resp.get("url")
            log.info("ads_api.report.completed", report_id=report_id, poll=poll + 1)
            break
        elif status == "FAILURE":
            log.error("ads_api.report.failed", report_id=report_id, resp=status_resp)
            return []
        else:
            log.debug("ads_api.report.polling", report_id=report_id, status=status, poll=poll + 1)

    if not download_url:
        log.error("ads_api.report.timeout", report_id=report_id, polls=max_polls)
        return []

    # Step 3: Download and decompress
    raw_bytes = await client.download(download_url)
    try:
        json_bytes = gzip.decompress(raw_bytes)
        rows = json.loads(json_bytes)
    except gzip.BadGzipFile:
        # Maybe not compressed
        rows = json.loads(raw_bytes)

    log.info("ads_api.report.downloaded", report_id=report_id, rows=len(rows))

    # Step 4: Parse into CampaignDayMetrics
    return _parse_report_rows(rows, ad_type)


def _parse_report_rows(rows: list[dict[str, Any]], ad_type: str) -> list[CampaignDayMetrics]:
    """Parse raw report JSON rows into CampaignDayMetrics."""
    metrics: list[CampaignDayMetrics] = []

    for row in rows:
        # Column names vary slightly between SP / SB / SD
        campaign_id = str(row.get("campaignId", ""))
        campaign_name = row.get("campaignName", "")
        date_str = row.get("date", "")

        if not campaign_id or not date_str:
            continue

        try:
            report_date = date.fromisoformat(date_str)
        except ValueError:
            continue

        # Sales column: sales7d for SP; sales for SB/SD
        sales = _float(row.get("sales7d") or row.get("sales") or 0)
        orders = _int(row.get("purchases7d") or row.get("purchases") or 0)
        units = _int(row.get("unitsSoldClicks7d") or row.get("unitsSold") or 0)

        metrics.append(CampaignDayMetrics(
            campaign_id=campaign_id,
            campaign_name=campaign_name,
            ad_type=ad_type,
            report_date=report_date,
            impressions=_int(row.get("impressions", 0)),
            clicks=_int(row.get("clicks", 0)),
            spend=_float(row.get("spend") or row.get("cost") or 0),
            sales_7d=sales,
            orders_7d=orders,
            units_7d=units,
            currency=row.get("currency", ""),
        ))

    return metrics


def _float(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _int(v: Any) -> int:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


# ---------------------------------------------------------------------------
# Advertised Product reports — spend per ASIN
# ---------------------------------------------------------------------------

async def request_sp_product_report(
    profile_id: int,
    start_date: date,
    end_date: date,
    poll_interval: float = 15.0,
    max_polls: int = 40,
) -> list[ProductDayMetrics]:
    """SP Advertised Product report — returns daily spend per ASIN."""
    client = AdsAPIClient(profile_id=profile_id)

    body = {
        "name": f"SP Product Report {start_date}..{end_date}",
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "groupBy": ["advertiser"],
            "columns": [
                "campaignId", "adGroupId", "advertisedSku", "advertisedAsin",
                "date", "impressions", "clicks", "spend",
                "sales7d", "unitsSoldClicks7d", "purchases7d",
            ],
            "reportTypeId": "spAdvertisedProduct",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON",
        },
    }

    return await _create_poll_download_product(client, body, "SP", profile_id, poll_interval, max_polls)


async def request_sb_product_report(
    profile_id: int,
    start_date: date,
    end_date: date,
    poll_interval: float = 15.0,
    max_polls: int = 40,
) -> list[ProductDayMetrics]:
    """SB has no spend-per-ASIN report (sbPurchasedProduct only has sales attribution).

    Returns empty list — SB spend stays at campaign level only.
    """
    return []


async def request_sd_product_report(
    profile_id: int,
    start_date: date,
    end_date: date,
    poll_interval: float = 15.0,
    max_polls: int = 40,
) -> list[ProductDayMetrics]:
    """SD Advertised Product report — returns daily spend per ASIN."""
    client = AdsAPIClient(profile_id=profile_id)

    body = {
        "name": f"SD Product Report {start_date}..{end_date}",
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "configuration": {
            "adProduct": "SPONSORED_DISPLAY",
            "groupBy": ["advertiser"],
            "columns": [
                "campaignId", "promotedAsin", "promotedSku",
                "date", "impressions", "clicks", "cost",
                "sales", "unitsSold", "purchases",
            ],
            "reportTypeId": "sdAdvertisedProduct",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON",
        },
    }

    return await _create_poll_download_product(client, body, "SD", profile_id, poll_interval, max_polls)


async def _create_poll_download_product(
    client: AdsAPIClient,
    body: dict,
    ad_type: str,
    profile_id: int,
    poll_interval: float,
    max_polls: int,
) -> list[ProductDayMetrics]:
    """Create report → poll → download → parse into ProductDayMetrics."""
    # Step 1: Create report
    create_resp = await client.post("/reporting/reports", body=body)
    report_id = create_resp.get("reportId")
    if not report_id:
        log.error("ads_api.product_report.no_id", response=create_resp)
        return []

    log.info("ads_api.product_report.created", report_id=report_id, ad_type=ad_type)

    # Step 2: Poll until COMPLETED
    download_url: Optional[str] = None
    for poll in range(max_polls):
        await asyncio.sleep(poll_interval)
        status_resp = await client.get(f"/reporting/reports/{report_id}")
        status = status_resp.get("status", "")

        if status == "COMPLETED":
            download_url = status_resp.get("url")
            log.info("ads_api.product_report.completed", report_id=report_id, poll=poll + 1)
            break
        elif status == "FAILURE":
            log.error("ads_api.product_report.failed", report_id=report_id, resp=status_resp)
            return []
        else:
            log.debug("ads_api.product_report.polling", report_id=report_id, status=status, poll=poll + 1)

    if not download_url:
        log.error("ads_api.product_report.timeout", report_id=report_id, polls=max_polls)
        return []

    # Step 3: Download and decompress
    raw_bytes = await client.download(download_url)
    try:
        json_bytes = gzip.decompress(raw_bytes)
        rows = json.loads(json_bytes)
    except gzip.BadGzipFile:
        rows = json.loads(raw_bytes)

    log.info("ads_api.product_report.downloaded", report_id=report_id, rows=len(rows))

    # Step 4: Parse into ProductDayMetrics
    # marketplace_id is resolved by caller in ads_sync and set post-parse
    return _parse_product_rows(rows, ad_type, "")


def _parse_product_rows(
    rows: list[dict[str, Any]], ad_type: str, marketplace_id: str,
) -> list[ProductDayMetrics]:
    """Parse raw report JSON rows into ProductDayMetrics."""
    metrics: list[ProductDayMetrics] = []

    for row in rows:
        asin = str(row.get("advertisedAsin") or row.get("promotedAsin") or row.get("asin") or "").strip()
        date_str = row.get("date", "")

        if not asin or not date_str:
            continue

        try:
            report_date = date.fromisoformat(date_str)
        except ValueError:
            continue

        # Sales column: sales7d for SP; sales for SB/SD
        sales = _float(row.get("sales7d") or row.get("sales") or 0)
        orders = _int(row.get("purchases7d") or row.get("purchases") or 0)
        units = _int(row.get("unitsSoldClicks7d") or row.get("unitsSold") or 0)

        metrics.append(ProductDayMetrics(
            asin=asin,
            sku=str(row.get("advertisedSku") or row.get("advertiserSku") or ""),
            ad_type=ad_type,
            campaign_id=str(row.get("campaignId", "")),
            report_date=report_date,
            marketplace_id=marketplace_id,
            impressions=_int(row.get("impressions", 0)),
            clicks=_int(row.get("clicks", 0)),
            spend=_float(row.get("spend") or row.get("cost") or 0),
            sales_7d=sales,
            orders_7d=orders,
            units_7d=units,
            currency=row.get("currency", ""),
        ))

    return metrics
