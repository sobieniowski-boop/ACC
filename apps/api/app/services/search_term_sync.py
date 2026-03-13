"""Search Term Sync Service — Brand Analytics search terms → Azure SQL.

Flow:
1. For each marketplace with Brand Owner access:
   request GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT (weekly)
2. Parse JSON → SearchTermRecord list
3. MERGE into acc_search_term_weekly (one row per term × ASIN × week)
4. Build monthly aggregates into acc_search_term_monthly (for seasonality)

Uses raw pymssql SQL with MERGE — same pattern as ads_sync.py.
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from typing import Any

import structlog

from app.connectors.amazon_sp_api.brand_analytics import (
    BrandAnalyticsClient,
    SearchTermRecord,
)
from app.core.config import settings, MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

MKT_CODE = {mid: info["code"] for mid, info in MARKETPLACE_REGISTRY.items()}

# ─── DDL — ensure tables exist ─────────────────────────────────────

_DDL_WEEKLY = """
IF OBJECT_ID('acc_search_term_weekly', 'U') IS NULL
CREATE TABLE acc_search_term_weekly (
    marketplace_id       NVARCHAR(30)  NOT NULL,
    week_start           DATE          NOT NULL,
    week_end             DATE          NOT NULL,
    search_term          NVARCHAR(500) NOT NULL,
    department           NVARCHAR(200) NULL,
    search_frequency_rank INT          NOT NULL DEFAULT 0,
    asin                 NVARCHAR(20)  NOT NULL,
    click_share          FLOAT         NOT NULL DEFAULT 0,
    conversion_share     FLOAT         NOT NULL DEFAULT 0,
    synced_at            DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_search_term_weekly
        PRIMARY KEY (marketplace_id, week_start, search_term, asin)
);
"""

_DDL_MONTHLY = """
IF OBJECT_ID('acc_search_term_monthly', 'U') IS NULL
CREATE TABLE acc_search_term_monthly (
    marketplace_id       NVARCHAR(30)  NOT NULL,
    year                 INT           NOT NULL,
    month                INT           NOT NULL,
    search_term          NVARCHAR(500) NOT NULL,
    department           NVARCHAR(200) NULL,
    avg_frequency_rank   FLOAT         NOT NULL DEFAULT 0,
    min_frequency_rank   INT           NOT NULL DEFAULT 0,
    weeks_seen           INT           NOT NULL DEFAULT 0,
    asin                 NVARCHAR(20)  NOT NULL,
    avg_click_share      FLOAT         NOT NULL DEFAULT 0,
    avg_conversion_share FLOAT         NOT NULL DEFAULT 0,
    synced_at            DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_search_term_monthly
        PRIMARY KEY (marketplace_id, year, month, search_term, asin)
);
"""

_DDL_INDEX_WEEKLY = """
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_search_term_weekly_asin' AND object_id = OBJECT_ID('acc_search_term_weekly')
)
CREATE INDEX IX_search_term_weekly_asin
    ON acc_search_term_weekly (asin, marketplace_id, week_start);
"""

_DDL_INDEX_MONTHLY_ASIN = """
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_search_term_monthly_asin' AND object_id = OBJECT_ID('acc_search_term_monthly')
)
CREATE INDEX IX_search_term_monthly_asin
    ON acc_search_term_monthly (asin, marketplace_id, year, month);
"""


def ensure_tables() -> None:
    """Create tables and indexes if they don't exist."""
    conn = connect_acc()
    cur = conn.cursor()
    for ddl in (_DDL_WEEKLY, _DDL_MONTHLY, _DDL_INDEX_WEEKLY, _DDL_INDEX_MONTHLY_ASIN):
        cur.execute(ddl)
    conn.commit()
    conn.close()
    log.info("search_term_sync.tables_ensured")


# ─── Sync pipeline ─────────────────────────────────────────────────

async def sync_search_terms(
    *,
    months_back: int = 3,
    weeks_back: int = 4,
    marketplace_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Download Brand Analytics search term MONTHLY reports and upsert into SQL.

    Amazon Brand Analytics supports MONTH-granularity reports (WEEK may be
    unavailable depending on account/marketplace).  We download completed
    months and MERGE directly into ``acc_search_term_monthly``.

    Args:
        months_back: How many completed past months to request (default 3).
        weeks_back: Ignored (kept for API compat). Use months_back instead.
        marketplace_ids: Subset of marketplaces (default: all with brand_owner flag).

    Returns:
        Summary with rows_upserted per marketplace.
    """
    ensure_tables()

    targets = marketplace_ids or [
        mid for mid, info in MARKETPLACE_REGISTRY.items()
        if info.get("brand_owner", False)
    ]
    if not targets:
        targets = [settings.SP_API_PRIMARY_MARKETPLACE]
        log.warning("search_term_sync.no_brand_owner_marketplaces_falling_back",
                     primary=targets[0])

    total_rows = 0
    per_marketplace: dict[str, int] = {}

    for mkt_id in targets:
        try:
            rows = await _sync_marketplace(mkt_id, months_back=months_back)
            per_marketplace[MKT_CODE.get(mkt_id, mkt_id)] = rows
            total_rows += rows
        except Exception as exc:
            log.error("search_term_sync.marketplace_error",
                      marketplace=mkt_id, error=str(exc))
            per_marketplace[MKT_CODE.get(mkt_id, mkt_id)] = -1

    result = {
        "total_monthly_rows": total_rows,
        "per_marketplace": per_marketplace,
    }
    log.info("search_term_sync.done", **result)
    return result


def _month_boundaries(months_back: int) -> list[tuple[date, date]]:
    """Return (first_day, last_day) for each of the last ``months_back`` completed months."""
    today = date.today()
    result = []
    for m in range(1, months_back + 1):
        # Go back m months from the 1st of current month
        first = (today.replace(day=1) - timedelta(days=1))
        for _ in range(m - 1):
            first = (first.replace(day=1) - timedelta(days=1))
        month_start = first.replace(day=1)
        # Last day = next month 1st - 1 day
        if month_start.month == 12:
            month_end = date(month_start.year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = date(month_start.year, month_start.month + 1, 1) - timedelta(days=1)
        result.append((month_start, month_end))
    return result


async def _sync_marketplace(marketplace_id: str, *, months_back: int) -> int:
    """Download and upsert monthly Brand Analytics reports for a single marketplace."""
    client = BrandAnalyticsClient(
        marketplace_id=marketplace_id,
        sync_profile="brand_analytics",
    )

    total_upserted = 0

    for month_start, month_end in _month_boundaries(months_back):
        log.info("search_term_sync.requesting_month",
                 marketplace=marketplace_id,
                 month_start=str(month_start), month_end=str(month_end))

        try:
            records = await client.download_search_terms(
                marketplace_ids=[marketplace_id],
                report_period="MONTH",
                data_start_time=datetime(month_start.year, month_start.month, month_start.day,
                                         tzinfo=timezone.utc),
                data_end_time=datetime(month_end.year, month_end.month, month_end.day, 23, 59, 59,
                                       tzinfo=timezone.utc),
            )
        except Exception as exc:
            log.warning("search_term_sync.month_failed",
                        marketplace=marketplace_id,
                        month_start=str(month_start), error=str(exc))
            continue

        if not records:
            continue

        rows = _upsert_monthly_direct(records, marketplace_id, month_start)
        total_upserted += rows
        log.info("search_term_sync.month_done",
                 marketplace=marketplace_id,
                 month=str(month_start), records=len(records), upserted=rows)

    return total_upserted


def _upsert_monthly_direct(
    records: list[SearchTermRecord],
    marketplace_id: str,
    month_start: date,
) -> int:
    """MERGE Brand Analytics monthly records directly into acc_search_term_monthly.

    Batches by 500 rows to avoid oversized SQL.
    """
    year = month_start.year
    month = month_start.month
    conn = connect_acc()
    cur = conn.cursor()
    total = 0

    batch_size = 500
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        values_parts = []
        for r in batch:
            term_safe = r.search_term.replace("'", "''")[:500]
            dept_safe = (r.department or "").replace("'", "''")[:200]
            asin_safe = r.asin.replace("'", "''")[:20]
            values_parts.append(
                f"('{marketplace_id}', {year}, {month}, "
                f"N'{term_safe}', N'{dept_safe}', "
                f"{r.search_frequency_rank}, {r.search_frequency_rank}, 1, "
                f"'{asin_safe}', {r.click_share}, {r.conversion_share})"
            )

        values_sql = ",\n".join(values_parts)
        cur.execute(f"""
            MERGE acc_search_term_monthly AS tgt
            USING (VALUES {values_sql})
                AS src (marketplace_id, year, month,
                        search_term, department, avg_frequency_rank,
                        min_frequency_rank, weeks_seen,
                        asin, avg_click_share, avg_conversion_share)
            ON tgt.marketplace_id = src.marketplace_id
               AND tgt.year = src.year
               AND tgt.month = src.month
               AND tgt.search_term = src.search_term
               AND tgt.asin = src.asin
            WHEN MATCHED THEN
                UPDATE SET
                    department = src.department,
                    avg_frequency_rank = src.avg_frequency_rank,
                    min_frequency_rank = src.min_frequency_rank,
                    weeks_seen = src.weeks_seen,
                    avg_click_share = src.avg_click_share,
                    avg_conversion_share = src.avg_conversion_share,
                    synced_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (marketplace_id, year, month, search_term,
                        department, avg_frequency_rank, min_frequency_rank,
                        weeks_seen, asin, avg_click_share, avg_conversion_share)
                VALUES (src.marketplace_id, src.year, src.month,
                        src.search_term, src.department,
                        src.avg_frequency_rank, src.min_frequency_rank,
                        src.weeks_seen, src.asin,
                        src.avg_click_share, src.avg_conversion_share);
        """)
        total += cur.rowcount

    conn.commit()
    conn.close()
    return total


def _rebuild_monthly_aggregates() -> int:
    """Rebuild acc_search_term_monthly from weekly data via SQL MERGE."""
    conn = connect_acc()
    cur = conn.cursor()

    cur.execute("""
        MERGE acc_search_term_monthly AS tgt
        USING (
            SELECT
                marketplace_id,
                YEAR(week_start) AS yr,
                MONTH(week_start) AS mo,
                search_term,
                MAX(department) AS department,
                AVG(CAST(search_frequency_rank AS FLOAT)) AS avg_rank,
                MIN(search_frequency_rank) AS min_rank,
                COUNT(DISTINCT week_start) AS weeks_seen,
                asin,
                AVG(click_share) AS avg_click,
                AVG(conversion_share) AS avg_conv
            FROM acc_search_term_weekly
            GROUP BY marketplace_id, YEAR(week_start), MONTH(week_start),
                     search_term, asin
        ) AS src
        ON tgt.marketplace_id = src.marketplace_id
           AND tgt.year = src.yr
           AND tgt.month = src.mo
           AND tgt.search_term = src.search_term
           AND tgt.asin = src.asin
        WHEN MATCHED THEN
            UPDATE SET
                department = src.department,
                avg_frequency_rank = src.avg_rank,
                min_frequency_rank = src.min_rank,
                weeks_seen = src.weeks_seen,
                avg_click_share = src.avg_click,
                avg_conversion_share = src.avg_conv,
                synced_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (marketplace_id, year, month, search_term, department,
                    avg_frequency_rank, min_frequency_rank, weeks_seen,
                    asin, avg_click_share, avg_conversion_share)
            VALUES (src.marketplace_id, src.yr, src.mo, src.search_term,
                    src.department, src.avg_rank, src.min_rank, src.weeks_seen,
                    src.asin, src.avg_click, src.avg_conv);
    """)
    n = cur.rowcount
    conn.commit()
    conn.close()
    log.info("search_term_sync.monthly_aggregates_rebuilt", rows=n)
    return n
