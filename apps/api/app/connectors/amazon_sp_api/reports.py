"""SP-API Reports 2021-06-30 connector.

Generates and downloads bulk reports from Amazon:
- GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL — orders
- GET_MERCHANT_LISTINGS_ALL_DATA — active listings
- GET_FBA_MYI_ALL_INVENTORY_DATA — FBA inventory
- GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA — fee estimates
- GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2 — settlements

Rate limits: createReport 0.0167 req/s, getReport 2 req/s
Reports take 1-30 minutes to generate.

Reference: https://developer-docs.amazon.com/sp-api/docs/reports-api-v2021-06-30-reference
"""
from __future__ import annotations

import asyncio
import csv
import gzip
import io
from datetime import datetime, timezone
from typing import Optional

import httpx
import structlog

from app.connectors.amazon_sp_api.client import SPAPIClient

log = structlog.get_logger(__name__)

REPORTS_BASE = "/reports/2021-06-30"


class ReportType:
    """Common SP-API report types."""
    # Orders
    ALL_ORDERS_BY_UPDATE = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL"
    ALL_ORDERS_BY_DATE = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL"

    # Listings / Products
    ACTIVE_LISTINGS = "GET_MERCHANT_LISTINGS_ALL_DATA"
    LISTINGS_DATA = "GET_MERCHANT_LISTINGS_DATA"

    # Inventory
    FBA_INVENTORY = "GET_FBA_MYI_ALL_INVENTORY_DATA"
    FBA_INVENTORY_PLANNING = "GET_FBA_INVENTORY_PLANNING_DATA"
    STRANDED_INVENTORY = "GET_STRANDED_INVENTORY_UI_DATA"

    # Fees
    FBA_FEES = "GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA"

    # Finance / Settlements
    SETTLEMENT_V2 = "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2"

    # Sales & Traffic
    SALES_TRAFFIC_BUSINESS = "GET_SALES_AND_TRAFFIC_REPORT"

    # Returns
    FBA_CUSTOMER_RETURNS = "GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA"


class ReportsClient(SPAPIClient):
    """Generate, poll, and download Amazon bulk reports."""

    async def create_report(
        self,
        report_type: str,
        marketplace_ids: Optional[list[str]] = None,
        data_start_time: Optional[datetime] = None,
        data_end_time: Optional[datetime] = None,
        report_options: Optional[dict] = None,
    ) -> str:
        """
        Request a new report. Returns reportId.

        Args:
            report_type: One of ReportType constants
            marketplace_ids: List of marketplace IDs (defaults to current)
            data_start_time: Start of data range (optional)
            data_end_time: End of data range (optional)
            report_options: Extra report-specific options

        Returns:
            reportId (str) — use with get_report() to check status
        """
        body: dict = {
            "reportType": report_type,
            "marketplaceIds": marketplace_ids or [self.marketplace_id],
        }
        if data_start_time:
            body["dataStartTime"] = data_start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        if data_end_time:
            body["dataEndTime"] = data_end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        if report_options:
            body["reportOptions"] = report_options

        data = await self.post(f"{REPORTS_BASE}/reports", body)
        report_id = data.get("reportId", "")
        log.info("report.created", type=report_type, report_id=report_id)
        return report_id

    async def get_report(self, report_id: str) -> dict:
        """
        Get report status and metadata.

        Returns dict with:
            reportId, reportType, processingStatus,
            reportDocumentId (when DONE), ...

        processingStatus: IN_QUEUE, IN_PROGRESS, DONE, CANCELLED, FATAL
        """
        data = await self.get(f"{REPORTS_BASE}/reports/{report_id}")
        return data

    async def get_report_document(self, document_id: str) -> dict:
        """
        Get report document metadata including download URL.

        Returns dict with:
            reportDocumentId, url (pre-signed S3 URL),
            compressionAlgorithm (GZIP or null)
        """
        data = await self.get(f"{REPORTS_BASE}/documents/{document_id}")
        return data

    async def download_report_content(self, document_id: str) -> str:
        """
        Download and decompress report content as string.

        Handles GZIP decompression automatically.
        """
        doc_meta = await self.get_report_document(document_id)
        url = doc_meta.get("url", "")
        compression = doc_meta.get("compressionAlgorithm")

        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.get(url)
            resp.raise_for_status()

        raw_bytes = gzip.decompress(resp.content) if compression == "GZIP" else resp.content
        content, encoding = _decode_report_bytes(raw_bytes)

        log.info("report.downloaded", doc_id=document_id, size=len(content), encoding=encoding)
        return content

    async def wait_for_report(
        self,
        report_id: str,
        poll_interval: float = 30.0,
        max_wait: float = 1800.0,  # 30 minutes
    ) -> dict:
        """
        Poll report status until DONE or error.

        Returns full report dict on success.
        Raises RuntimeError on FATAL/CANCELLED or timeout.
        """
        elapsed = 0.0
        while elapsed < max_wait:
            report = await self.get_report(report_id)
            status = report.get("processingStatus", "")

            if status == "DONE":
                log.info("report.ready", report_id=report_id, elapsed=elapsed)
                return report

            if status in ("FATAL", "CANCELLED"):
                raise RuntimeError(
                    f"Report {report_id} failed with status {status}"
                )

            log.debug("report.polling", report_id=report_id, status=status, elapsed=elapsed)
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        raise RuntimeError(f"Report {report_id} timed out after {max_wait}s")

    async def request_and_download(
        self,
        report_type: str,
        marketplace_ids: Optional[list[str]] = None,
        data_start_time: Optional[datetime] = None,
        data_end_time: Optional[datetime] = None,
        report_options: Optional[dict] = None,
        poll_interval: float = 30.0,
    ) -> str:
        """
        High-level: request report → wait → download content.

        Returns report content as string (TSV format typically).
        """
        report_id = await self.create_report(
            report_type=report_type,
            marketplace_ids=marketplace_ids,
            data_start_time=data_start_time,
            data_end_time=data_end_time,
            report_options=report_options,
        )

        report = await self.wait_for_report(report_id, poll_interval)
        doc_id = report.get("reportDocumentId", "")

        if not doc_id:
            raise RuntimeError(f"Report {report_id} DONE but no reportDocumentId")

        content = await self.download_report_content(doc_id)
        return content

    async def get_existing_reports(
        self,
        report_types: list[str],
        processing_statuses: Optional[list[str]] = None,
        page_size: int = 10,
    ) -> list[dict]:
        """
        List existing reports (already generated).
        Useful for settlement reports or checking if a recent report exists.
        """
        params = {
            "reportTypes": ",".join(report_types),
            "marketplaceIds": self.marketplace_id,
            "pageSize": page_size,
        }
        if processing_statuses:
            params["processingStatuses"] = ",".join(processing_statuses)

        data = await self.get(f"{REPORTS_BASE}/reports", params)
        return data.get("reports", [])

    async def request_and_download_reuse_recent(
        self,
        report_type: str,
        marketplace_ids: Optional[list[str]] = None,
        max_age_minutes: int = 180,
        data_start_time: Optional[datetime] = None,
        data_end_time: Optional[datetime] = None,
        report_options: Optional[dict] = None,
        poll_interval: float = 30.0,
    ) -> str:
        reports = await self.get_existing_reports(
            report_types=[report_type],
            processing_statuses=["DONE"],
            page_size=10,
        )
        now = datetime.now(timezone.utc)
        for report in reports:
            created_raw = report.get("createdTime")
            document_id = report.get("reportDocumentId")
            if not created_raw or not document_id:
                continue
            try:
                created_at = datetime.fromisoformat(str(created_raw).replace("Z", "+00:00"))
            except Exception:
                continue
            age_min = (now - created_at).total_seconds() / 60.0
            if age_min <= max_age_minutes:
                log.info("report.reuse_existing", type=report_type, report_id=report.get("reportId"), age_min=round(age_min, 1))
                return await self.download_report_content(document_id)

        return await self.request_and_download(
            report_type=report_type,
            marketplace_ids=marketplace_ids,
            data_start_time=data_start_time,
            data_end_time=data_end_time,
            report_options=report_options,
            poll_interval=poll_interval,
        )


def parse_tsv_report(content: str) -> list[dict]:
    """
    Parse a flat-file Amazon report into a list of dicts.

    Most reports are TSV, but some FBA reports come back as quoted CSV.
    We auto-detect the delimiter from the first non-empty line.
    """
    if content.startswith("\ufeff"):
        content = content[1:]

    first_line = next((line for line in content.splitlines() if line.strip()), "")
    delimiter = "\t"
    if first_line.count(",") > first_line.count("\t"):
        delimiter = ","

    reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
    rows = []
    for row in reader:
        clean = {k.strip(): (v.strip() if v else "") for k, v in row.items() if k}
        rows.append(clean)
    return rows


def _decode_report_bytes(raw_bytes: bytes) -> tuple[str, str]:
    """
    Amazon flat-file reports are usually UTF-8, but merchant listing exports
    can contain cp1252 characters from localized titles and attributes.
    """
    encodings = ("utf-8-sig", "utf-8", "cp1252", "latin-1")
    last_error: UnicodeDecodeError | None = None
    for encoding in encodings:
        try:
            return raw_bytes.decode(encoding), encoding
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    return raw_bytes.decode("latin-1"), "latin-1"
