"""SP-API Brand Analytics — Search Query Performance reports.

Uses the SP-API Reports 2021-06-30 endpoint to request Brand Analytics reports.
Requires Brand Owner (Brand Registry) enrollment.

Supported report types:
- GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT — weekly/monthly search term volume
  with click share and conversion share per ASIN.

Rate limits: same as Reports API (createReport 0.0167 req/s).

Reference:
  https://developer-docs.amazon.com/sp-api/docs/report-type-values-analytics
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

import structlog

from app.connectors.amazon_sp_api.reports import ReportsClient

log = structlog.get_logger(__name__)


# ── Report type constants ────────────────────────────────────────────

SEARCH_TERMS_REPORT = "GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT"

# Amazon delivers this report with reportOptions:
#   reportPeriod: WEEK | MONTH | QUARTER
# The report is JSON (compressed) containing search query performance data.


@dataclass
class SearchTermRecord:
    """One row from the Brand Analytics Search Terms report.

    Each record represents a (search_term, asin) pair for a given period:
    - search_term: customer search query
    - department: Amazon browse node / department
    - search_frequency_rank: rank by search volume (1 = highest)
    - asin: the clicked ASIN
    - click_share: % of clicks on this ASIN for this search term
    - conversion_share: % of conversions on this ASIN for this term
    """
    search_term: str
    search_frequency_rank: int
    department: str
    asin: str
    click_share: float
    conversion_share: float
    # Period metadata (set during parsing)
    report_date_start: date | None = None
    report_date_end: date | None = None
    marketplace_id: str = ""


class BrandAnalyticsClient(ReportsClient):
    """Thin layer on top of ReportsClient for Brand Analytics reports."""

    async def request_search_terms_report(
        self,
        *,
        marketplace_ids: list[str] | None = None,
        report_period: str = "WEEK",
        data_start_time: datetime | None = None,
        data_end_time: datetime | None = None,
    ) -> str:
        """Request a Brand Analytics Search Terms report.

        Args:
            marketplace_ids: Override marketplace (defaults to client's).
            report_period: WEEK | MONTH | QUARTER
            data_start_time: Report range start.
            data_end_time: Report range end.

        Returns:
            reportId for polling.
        """
        report_options = {"reportPeriod": report_period}

        report_id = await self.create_report(
            report_type=SEARCH_TERMS_REPORT,
            marketplace_ids=marketplace_ids,
            data_start_time=data_start_time,
            data_end_time=data_end_time,
            report_options=report_options,
        )
        log.info(
            "brand_analytics.search_terms.requested",
            report_id=report_id,
            period=report_period,
        )
        return report_id

    async def download_search_terms(
        self,
        *,
        marketplace_ids: list[str] | None = None,
        report_period: str = "WEEK",
        data_start_time: datetime | None = None,
        data_end_time: datetime | None = None,
        poll_interval: float = 30.0,
    ) -> list[SearchTermRecord]:
        """Request → wait → download → parse search terms report.

        Returns list of SearchTermRecord.
        """
        content = await self.request_and_download(
            report_type=SEARCH_TERMS_REPORT,
            marketplace_ids=marketplace_ids,
            data_start_time=data_start_time,
            data_end_time=data_end_time,
            report_options={"reportPeriod": report_period},
            poll_interval=poll_interval,
        )
        records = parse_search_terms_report(
            content,
            marketplace_id=(marketplace_ids or [self.marketplace_id])[0],
        )
        log.info(
            "brand_analytics.search_terms.downloaded",
            records=len(records),
            period=report_period,
        )
        return records


def parse_search_terms_report(
    content: str,
    marketplace_id: str = "",
) -> list[SearchTermRecord]:
    """Parse the JSON content of a Brand Analytics Search Terms report.

    Amazon delivers a JSON array. Each element contains:
      - searchTerm / keywordText
      - departmentName
      - searchFrequencyRank
      - clickedAsin, clickShareRank, clickShare
      - conversionShare

    The exact structure can vary by API version. We handle both old
    (flat) and new (nested clickedItemList) formats gracefully.
    """
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        log.warning("brand_analytics.parse_error", size=len(content))
        return []

    # The report may be wrapped in a top-level key.
    if isinstance(data, dict):
        data = data.get("dataByDepartmentAndSearchTerm", data.get("data", []))
    if not isinstance(data, list):
        log.warning("brand_analytics.unexpected_format", type=type(data).__name__)
        return []

    records: list[SearchTermRecord] = []
    for entry in data:
        search_term = entry.get("searchTerm", entry.get("keywordText", "")).strip()
        department = entry.get("departmentName", "")
        rank = _safe_int(entry.get("searchFrequencyRank", 0))

        # New format: nested clickedItemList with up to 3 ASINs per term
        items = entry.get("clickedItemList", [])
        if items:
            for item in items:
                records.append(SearchTermRecord(
                    search_term=search_term,
                    search_frequency_rank=rank,
                    department=department,
                    asin=item.get("clickedAsin", item.get("asin", "")),
                    click_share=_safe_float(item.get("clickShare", 0)),
                    conversion_share=_safe_float(item.get("conversionShare", 0)),
                    marketplace_id=marketplace_id,
                ))
        else:
            # Flat format: single ASIN per row
            records.append(SearchTermRecord(
                search_term=search_term,
                search_frequency_rank=rank,
                department=department,
                asin=entry.get("clickedAsin", entry.get("asin", "")),
                click_share=_safe_float(entry.get("clickShare", 0)),
                conversion_share=_safe_float(entry.get("conversionShare", 0)),
                marketplace_id=marketplace_id,
            ))

    return records


def _safe_int(v: Any) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _safe_float(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0
