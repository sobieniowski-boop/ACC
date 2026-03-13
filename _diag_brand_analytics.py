"""Diagnose Brand Analytics FATAL reports — capture full error details."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))

import asyncio
import json
from datetime import datetime, date, timedelta, timezone
import structlog, logging

structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG))

async def main():
    from app.connectors.amazon_sp_api.brand_analytics import BrandAnalyticsClient, SEARCH_TERMS_REPORT
    from app.connectors.amazon_sp_api.reports import REPORTS_BASE
    from app.core.config import settings

    mkt = "A1PA6795UKMFR9"  # DE
    client = BrandAnalyticsClient(marketplace_id=mkt, sync_profile="diag")

    # --- Test 1: Check full FATAL response details ---
    print("=== Test 1: Request report and inspect FATAL details ===")
    # Use a completed past week (Sun-Sat pattern)
    week_start = date(2026, 2, 16)
    week_end = date(2026, 2, 22)

    report_id = await client.create_report(
        report_type=SEARCH_TERMS_REPORT,
        marketplace_ids=[mkt],
        data_start_time=datetime(week_start.year, week_start.month, week_start.day, tzinfo=timezone.utc),
        data_end_time=datetime(week_end.year, week_end.month, week_end.day, 23, 59, 59, tzinfo=timezone.utc),
        report_options={"reportPeriod": "WEEK"},
    )
    print(f"  report_id: {report_id}")

    # Poll and capture the full response
    for attempt in range(5):
        await asyncio.sleep(15)
        report = await client.get_report(report_id)
        status = report.get("processingStatus", "")
        print(f"  Poll {attempt+1}: status={status}")
        print(f"  Full response: {json.dumps(report, indent=2, default=str)}")
        if status in ("DONE", "FATAL", "CANCELLED"):
            break

    # --- Test 2: Try without reportPeriod (maybe it's auto-determined?) ---
    print("\n=== Test 2: Without reportPeriod option ===")
    try:
        report_id2 = await client.create_report(
            report_type=SEARCH_TERMS_REPORT,
            marketplace_ids=[mkt],
            data_start_time=datetime(2026, 2, 16, tzinfo=timezone.utc),
            data_end_time=datetime(2026, 2, 22, 23, 59, 59, tzinfo=timezone.utc),
        )
        print(f"  report_id: {report_id2}")
        for attempt in range(5):
            await asyncio.sleep(15)
            report2 = await client.get_report(report_id2)
            s2 = report2.get("processingStatus", "")
            print(f"  Poll {attempt+1}: status={s2}")
            print(f"  Full response: {json.dumps(report2, indent=2, default=str)}")
            if s2 in ("DONE", "FATAL", "CANCELLED"):
                break
    except Exception as e:
        print(f"  Error: {e}")

    # --- Test 3: Try with reportPeriod=MONTH and month boundaries ---
    print("\n=== Test 3: Monthly period (February 2026) ===")
    try:
        report_id3 = await client.create_report(
            report_type=SEARCH_TERMS_REPORT,
            marketplace_ids=[mkt],
            data_start_time=datetime(2026, 2, 1, tzinfo=timezone.utc),
            data_end_time=datetime(2026, 2, 28, 23, 59, 59, tzinfo=timezone.utc),
            report_options={"reportPeriod": "MONTH"},
        )
        print(f"  report_id: {report_id3}")
        for attempt in range(5):
            await asyncio.sleep(15)
            report3 = await client.get_report(report_id3)
            s3 = report3.get("processingStatus", "")
            print(f"  Poll {attempt+1}: status={s3}")
            print(f"  Full response: {json.dumps(report3, indent=2, default=str)}")
            if s3 in ("DONE", "FATAL", "CANCELLED"):
                break
    except Exception as e:
        print(f"  Error: {e}")

    # --- Test 4: List available report types (via getReports with type filter) ---
    print("\n=== Test 4: Check recent Brand Analytics reports ===")
    try:
        headers = await client._headers()
        import httpx
        async with httpx.AsyncClient(
            base_url=f"https://{settings.SP_API_ENDPOINT}",
            timeout=30,
        ) as http:
            resp = await http.get(
                f"{REPORTS_BASE}/reports",
                headers=headers,
                params={
                    "reportTypes": SEARCH_TERMS_REPORT,
                    "pageSize": "10",
                },
            )
            print(f"  Status: {resp.status_code}")
            print(f"  Body: {json.dumps(resp.json(), indent=2, default=str)}")
    except Exception as e:
        print(f"  Error: {e}")

    # --- Test 5: Check Selling Partner Analytics API direct endpoint ---
    print("\n=== Test 5: Try Analytics API endpoint (if available) ===")
    try:
        headers = await client._headers()
        import httpx
        async with httpx.AsyncClient(
            base_url=f"https://{settings.SP_API_ENDPOINT}",
            timeout=30,
        ) as http:
            # Search Query Performance v1
            resp = await http.get(
                "/analytics/brandAnalytics/v1",
                headers=headers,
            )
            print(f"  Analytics API: {resp.status_code}")
            print(f"  Body: {resp.text[:500]}")
    except Exception as e:
        print(f"  Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
