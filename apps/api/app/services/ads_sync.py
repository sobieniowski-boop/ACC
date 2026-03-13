"""Ads Sync Service — orchestrates Amazon Ads API → Azure SQL.

Flow:
1. Fetch profiles (GET /v2/profiles) → map marketplace_id ↔ profile_id
2. Upsert profiles into acc_ads_profile
3. For each profile: fetch campaigns (SP/SB/SD) → MERGE into acc_ads_campaign
4. For each profile: request daily reports → MERGE into acc_ads_campaign_day
5. Compute PLN equivalents using acc_exchange_rate

Uses raw pyodbc SQL with MERGE (same pattern as order pipeline).
All SQL uses `?` placeholders (pymssql_compat converts to `%s`).
"""
from __future__ import annotations

import asyncio
from datetime import date, timedelta
from typing import Any

import structlog

from app.connectors.amazon_ads_api.profiles import AdsProfile, list_profiles
from app.connectors.amazon_ads_api.campaigns import (
    AdsCampaignInfo,
    list_all_campaigns,
)
from app.connectors.amazon_ads_api.reporting import (
    CampaignDayMetrics,
    ProductDayMetrics,
    request_sp_campaign_report,
    request_sb_campaign_report,
    request_sd_campaign_report,
    request_sp_product_report,
    request_sb_product_report,
    request_sd_product_report,
)
from app.core.config import settings, MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# DDL — ensure tables exist
# ---------------------------------------------------------------------------

_DDL_MIGRATE = """
-- Drop old SQLAlchemy-created tables (UUID PK, no ad_type) if they exist
-- They have no real data — we recreate with new schema
IF OBJECT_ID('acc_ads_campaign_day', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_NAME = 'acc_ads_campaign_day' AND COLUMN_NAME = 'ad_type'
   )
    DROP TABLE acc_ads_campaign_day;

IF OBJECT_ID('acc_ads_campaign', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_NAME = 'acc_ads_campaign' AND COLUMN_NAME = 'ad_type'
   )
    DROP TABLE acc_ads_campaign;
"""

_DDL_PROFILE = """
IF OBJECT_ID('acc_ads_profile', 'U') IS NULL
CREATE TABLE acc_ads_profile (
    profile_id       BIGINT        NOT NULL PRIMARY KEY,
    marketplace_id   NVARCHAR(30)  NOT NULL,
    country_code     NVARCHAR(5)   NOT NULL,
    currency         NVARCHAR(5)   NOT NULL DEFAULT 'EUR',
    account_type     NVARCHAR(20)  NOT NULL DEFAULT 'seller',
    account_name     NVARCHAR(200) NULL,
    account_id       NVARCHAR(50)  NULL,
    synced_at        DATETIME2     NOT NULL DEFAULT GETUTCDATE()
);
"""

_DDL_CAMPAIGN = """
IF OBJECT_ID('acc_ads_campaign', 'U') IS NULL
CREATE TABLE acc_ads_campaign (
    campaign_id      NVARCHAR(50)  NOT NULL,
    profile_id       BIGINT        NOT NULL,
    marketplace_id   NVARCHAR(30)  NOT NULL,
    campaign_name    NVARCHAR(500) NOT NULL,
    ad_type          NVARCHAR(5)   NOT NULL DEFAULT 'SP',
    state            NVARCHAR(20)  NOT NULL DEFAULT 'ENABLED',
    targeting_type   NVARCHAR(50)  NULL,
    daily_budget     DECIMAL(10,2) NULL,
    currency         NVARCHAR(5)   NOT NULL DEFAULT 'EUR',
    start_date       DATE          NULL,
    end_date         DATE          NULL,
    synced_at        DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT PK_acc_ads_campaign PRIMARY KEY (campaign_id, ad_type)
);
"""

_DDL_CAMPAIGN_DAY = """
IF OBJECT_ID('acc_ads_campaign_day', 'U') IS NULL
CREATE TABLE acc_ads_campaign_day (
    campaign_id      NVARCHAR(50)  NOT NULL,
    ad_type          NVARCHAR(5)   NOT NULL DEFAULT 'SP',
    report_date      DATE          NOT NULL,
    impressions      INT           NOT NULL DEFAULT 0,
    clicks           INT           NOT NULL DEFAULT 0,
    spend            DECIMAL(10,4) NOT NULL DEFAULT 0,
    sales_7d         DECIMAL(12,4) NOT NULL DEFAULT 0,
    orders_7d        INT           NOT NULL DEFAULT 0,
    units_7d         INT           NOT NULL DEFAULT 0,
    currency         NVARCHAR(5)   NOT NULL DEFAULT 'EUR',
    acos             DECIMAL(8,4)  NULL,
    roas             DECIMAL(8,4)  NULL,
    spend_pln        DECIMAL(10,4) NULL,
    sales_pln        DECIMAL(12,4) NULL,
    synced_at        DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT PK_acc_ads_campaign_day PRIMARY KEY (campaign_id, ad_type, report_date)
);
"""

_DDL_PRODUCT_DAY = """
IF OBJECT_ID('acc_ads_product_day', 'U') IS NULL
CREATE TABLE acc_ads_product_day (
    asin             NVARCHAR(20)  NOT NULL,
    ad_type          NVARCHAR(5)   NOT NULL DEFAULT 'SP',
    report_date      DATE          NOT NULL,
    marketplace_id   NVARCHAR(30)  NOT NULL DEFAULT '',
    campaign_id      NVARCHAR(50)  NOT NULL DEFAULT '',
    sku              NVARCHAR(100) NULL,
    impressions      INT           NOT NULL DEFAULT 0,
    clicks           INT           NOT NULL DEFAULT 0,
    spend            DECIMAL(10,4) NOT NULL DEFAULT 0,
    sales_7d         DECIMAL(12,4) NOT NULL DEFAULT 0,
    orders_7d        INT           NOT NULL DEFAULT 0,
    units_7d         INT           NOT NULL DEFAULT 0,
    currency         NVARCHAR(5)   NOT NULL DEFAULT 'EUR',
    spend_pln        DECIMAL(10,4) NULL,
    sales_pln        DECIMAL(12,4) NULL,
    synced_at        DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT PK_acc_ads_product_day PRIMARY KEY (asin, ad_type, report_date, marketplace_id, campaign_id)
);
"""


def ensure_ads_tables() -> None:
    """Create acc_ads_* tables if they don't exist."""
    conn = connect_acc(autocommit=True)
    cur = conn.cursor()
    for ddl in [_DDL_MIGRATE, _DDL_PROFILE, _DDL_CAMPAIGN, _DDL_CAMPAIGN_DAY, _DDL_PRODUCT_DAY]:
        cur.execute(ddl)
    cur.close()
    conn.close()
    log.info("ads_sync.tables_ensured")


# ---------------------------------------------------------------------------
# Upsert: Profiles
# ---------------------------------------------------------------------------

_MERGE_PROFILE = """
MERGE acc_ads_profile AS tgt
USING (VALUES (?, ?, ?, ?, ?, ?, ?)) AS src (
    profile_id, marketplace_id, country_code, currency,
    account_type, account_name, account_id
)
ON tgt.profile_id = src.profile_id
WHEN MATCHED THEN UPDATE SET
    marketplace_id = src.marketplace_id,
    country_code   = src.country_code,
    currency       = src.currency,
    account_type   = src.account_type,
    account_name   = src.account_name,
    account_id     = src.account_id,
    synced_at      = GETUTCDATE()
WHEN NOT MATCHED THEN INSERT (
    profile_id, marketplace_id, country_code, currency,
    account_type, account_name, account_id
) VALUES (
    src.profile_id, src.marketplace_id, src.country_code, src.currency,
    src.account_type, src.account_name, src.account_id
);
"""


def _upsert_profiles(profiles: list[AdsProfile]) -> int:
    """MERGE profiles into acc_ads_profile."""
    if not profiles:
        return 0
    conn = connect_acc()
    cur = conn.cursor()
    for p in profiles:
        cur.execute(_MERGE_PROFILE, [
            p.profile_id, p.marketplace_id, p.country_code,
            p.currency, p.account_type, p.account_name, p.account_id,
        ])
    conn.commit()
    cur.close()
    conn.close()
    return len(profiles)


# ---------------------------------------------------------------------------
# Upsert: Campaigns
# ---------------------------------------------------------------------------

_MERGE_CAMPAIGN = """
MERGE acc_ads_campaign AS tgt
USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS src (
    campaign_id, profile_id, marketplace_id, campaign_name,
    ad_type, state, targeting_type, daily_budget, currency,
    start_date, end_date
)
ON tgt.campaign_id = src.campaign_id AND tgt.ad_type = src.ad_type
WHEN MATCHED THEN UPDATE SET
    profile_id     = src.profile_id,
    marketplace_id = src.marketplace_id,
    campaign_name  = src.campaign_name,
    state          = src.state,
    targeting_type = src.targeting_type,
    daily_budget   = src.daily_budget,
    currency       = src.currency,
    start_date     = src.start_date,
    end_date       = src.end_date,
    synced_at      = GETUTCDATE()
WHEN NOT MATCHED THEN INSERT (
    campaign_id, profile_id, marketplace_id, campaign_name,
    ad_type, state, targeting_type, daily_budget, currency,
    start_date, end_date
) VALUES (
    src.campaign_id, src.profile_id, src.marketplace_id, src.campaign_name,
    src.ad_type, src.state, src.targeting_type, src.daily_budget, src.currency,
    src.start_date, src.end_date
);
"""


def _upsert_campaigns(
    campaigns: list[AdsCampaignInfo],
    profile: AdsProfile,
) -> int:
    """MERGE campaigns into acc_ads_campaign."""
    if not campaigns:
        return 0
    conn = connect_acc()
    cur = conn.cursor()
    for c in campaigns:
        cur.execute(_MERGE_CAMPAIGN, [
            c.campaign_id, profile.profile_id, profile.marketplace_id,
            c.campaign_name, c.ad_type, c.state, c.targeting_type,
            c.daily_budget, profile.currency,
            c.start_date, c.end_date,
        ])
    conn.commit()
    cur.close()
    conn.close()
    return len(campaigns)


# ---------------------------------------------------------------------------
# Upsert: Campaign Day metrics
# ---------------------------------------------------------------------------

_MERGE_CAMPAIGN_DAY = """
MERGE acc_ads_campaign_day AS tgt
USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS src (
    campaign_id, ad_type, report_date,
    impressions, clicks, spend, sales_7d, orders_7d, units_7d,
    currency, acos, roas, spend_pln, sales_pln
)
ON tgt.campaign_id = src.campaign_id
   AND tgt.ad_type = src.ad_type
   AND tgt.report_date = src.report_date
WHEN MATCHED THEN UPDATE SET
    impressions = src.impressions,
    clicks      = src.clicks,
    spend       = src.spend,
    sales_7d    = src.sales_7d,
    orders_7d   = src.orders_7d,
    units_7d    = src.units_7d,
    currency    = src.currency,
    acos        = src.acos,
    roas        = src.roas,
    spend_pln   = src.spend_pln,
    sales_pln   = src.sales_pln,
    synced_at   = GETUTCDATE()
WHEN NOT MATCHED THEN INSERT (
    campaign_id, ad_type, report_date,
    impressions, clicks, spend, sales_7d, orders_7d, units_7d,
    currency, acos, roas, spend_pln, sales_pln
) VALUES (
    src.campaign_id, src.ad_type, src.report_date,
    src.impressions, src.clicks, src.spend, src.sales_7d, src.orders_7d, src.units_7d,
    src.currency, src.acos, src.roas, src.spend_pln, src.sales_pln
);
"""


def _get_exchange_rates(currencies: set[str], dates: set[date]) -> dict[tuple[str, date], float]:
    """Fetch exchange rates from acc_exchange_rate for PLN conversion.

    Returns {(currency, date): rate_to_pln}.
    PLN → 1.0 always.
    """
    rates: dict[tuple[str, date], float] = {}
    if not currencies or not dates:
        return rates

    # PLN is always 1.0
    for d in dates:
        rates[("PLN", d)] = 1.0

    non_pln = {c for c in currencies if c != "PLN"}
    if not non_pln:
        return rates

    conn = connect_acc()
    cur = conn.cursor()

    # Fetch rates for the date range
    min_date = min(dates)
    max_date = max(dates)

    cur.execute("""
        SELECT currency, rate_date, rate_to_pln
        FROM acc_exchange_rate WITH (NOLOCK)
        WHERE currency IN ({placeholders})
          AND rate_date BETWEEN ? AND ?
    """.replace("{placeholders}", ",".join(["?"] * len(non_pln))),
        list(non_pln) + [min_date, max_date]
    )

    for row in cur.fetchall():
        rates[(row[0], row[1])] = float(row[2])

    cur.close()
    conn.close()

    # Fill gaps: if a specific date is missing, find nearest earlier date
    for cur_code in non_pln:
        sorted_dates = sorted(d for (c, d) in rates if c == cur_code)
        for d in dates:
            if (cur_code, d) not in rates and sorted_dates:
                # Find nearest earlier date
                earlier = [sd for sd in sorted_dates if sd <= d]
                if earlier:
                    rates[(cur_code, d)] = rates[(cur_code, earlier[-1])]

    return rates


def _upsert_daily_metrics(metrics: list[CampaignDayMetrics]) -> int:
    """MERGE daily metrics into acc_ads_campaign_day with PLN conversion."""
    if not metrics:
        return 0

    # Collect unique currencies and dates for exchange rate lookup
    currencies = {m.currency for m in metrics if m.currency}
    dates = {m.report_date for m in metrics}
    rates = _get_exchange_rates(currencies, dates)

    conn = connect_acc()
    cur = conn.cursor()
    count = 0
    fx_missing_count = 0

    for m in metrics:
        acos = round(m.spend / m.sales_7d * 100, 4) if m.sales_7d else None
        roas = round(m.sales_7d / m.spend, 4) if m.spend else None

        # PLN conversion — SF-02/SF-10: block silent 1.0 fallback
        rate = rates.get((m.currency, m.report_date))
        if rate is None:
            fx_missing_count += 1
            log.warning("ads_sync.fx_rate_missing",
                        currency=m.currency, report_date=str(m.report_date),
                        msg="No FX rate found — spend_pln/sales_pln will be NULL")
        spend_pln = round(m.spend * rate, 4) if rate else None
        sales_pln = round(m.sales_7d * rate, 4) if rate else None

        cur.execute(_MERGE_CAMPAIGN_DAY, [
            m.campaign_id, m.ad_type, m.report_date,
            m.impressions, m.clicks, m.spend, m.sales_7d, m.orders_7d, m.units_7d,
            m.currency, acos, roas, spend_pln, sales_pln,
        ])
        count += 1

    conn.commit()
    cur.close()
    conn.close()

    # SF-10: surface the total count of FX gaps for alerting
    if fx_missing_count:
        log.error("ads_sync.fx_gap_summary",
                  missing=fx_missing_count, total=count,
                  msg="Campaign-day rows written with NULL PLN values due to missing FX rates")

    return count


# ---------------------------------------------------------------------------
# Upsert: Product Day metrics (per ASIN)
# ---------------------------------------------------------------------------

_MERGE_PRODUCT_DAY = """
MERGE acc_ads_product_day AS tgt
USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS src (
    asin, ad_type, report_date, marketplace_id, campaign_id, sku,
    impressions, clicks, spend, sales_7d, orders_7d, units_7d,
    currency, spend_pln, sales_pln
)
ON tgt.asin = src.asin
   AND tgt.ad_type = src.ad_type
   AND tgt.report_date = src.report_date
   AND tgt.marketplace_id = src.marketplace_id
   AND tgt.campaign_id = src.campaign_id
WHEN MATCHED THEN UPDATE SET
    sku         = src.sku,
    impressions = src.impressions,
    clicks      = src.clicks,
    spend       = src.spend,
    sales_7d    = src.sales_7d,
    orders_7d   = src.orders_7d,
    units_7d    = src.units_7d,
    currency    = src.currency,
    spend_pln   = src.spend_pln,
    sales_pln   = src.sales_pln,
    synced_at   = GETUTCDATE()
WHEN NOT MATCHED THEN INSERT (
    asin, ad_type, report_date, marketplace_id, campaign_id, sku,
    impressions, clicks, spend, sales_7d, orders_7d, units_7d,
    currency, spend_pln, sales_pln
) VALUES (
    src.asin, src.ad_type, src.report_date, src.marketplace_id, src.campaign_id, src.sku,
    src.impressions, src.clicks, src.spend, src.sales_7d, src.orders_7d, src.units_7d,
    src.currency, src.spend_pln, src.sales_pln
);
"""


def _upsert_product_day_metrics(metrics: list[ProductDayMetrics]) -> int:
    """MERGE product-level daily metrics into acc_ads_product_day with PLN conversion.

    Uses batch temp-table + MERGE for performance (~100x faster than individual MERGEs).
    Processes in chunks of BATCH_SIZE rows, each with multi-row INSERTs of ROWS_PER_INSERT.
    SQL Server limit: 2100 params/stmt → 15 columns → max 140 rows/INSERT → use 100.
    Deduplicates within each batch to avoid MERGE source-duplicate errors.
    Retries on lock timeout (error 1222) with exponential backoff.
    """
    if not metrics:
        return 0

    import time as _time

    currencies = {m.currency for m in metrics if m.currency}
    dates = {m.report_date for m in metrics}
    rates = _get_exchange_rates(currencies, dates)

    BATCH_SIZE = 5_000        # rows per temp-table cycle (reduced for Azure SQL DTU limits)
    ROWS_PER_INSERT = 100     # rows per INSERT statement (15 cols × 100 = 1500 params < 2100)
    MAX_RETRIES = 3

    conn = connect_acc()
    cur = conn.cursor()

    cur.execute("SET LOCK_TIMEOUT 60000")  # 60 seconds

    total = 0
    fx_missing_total = 0
    for batch_start in range(0, len(metrics), BATCH_SIZE):
        batch = metrics[batch_start : batch_start + BATCH_SIZE]

        for attempt in range(MAX_RETRIES):
            try:
                # 1. Create temp table
                cur.execute("""
                    CREATE TABLE #tmp_pd (
                        asin NVARCHAR(20), ad_type NVARCHAR(5), report_date DATE,
                        marketplace_id NVARCHAR(20), campaign_id NVARCHAR(30), sku NVARCHAR(50),
                        impressions INT, clicks INT, spend DECIMAL(18,4), sales_7d DECIMAL(18,4),
                        orders_7d INT, units_7d INT, currency NVARCHAR(10),
                        spend_pln DECIMAL(18,4), sales_pln DECIMAL(18,4)
                    )
                """)

                # 2. Multi-row INSERT into temp table
                row_placeholder = "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                for ins_start in range(0, len(batch), ROWS_PER_INSERT):
                    sub = batch[ins_start : ins_start + ROWS_PER_INSERT]
                    placeholders = ",".join([row_placeholder] * len(sub))
                    params: list = []
                    for m in sub:
                        rate = rates.get((m.currency, m.report_date))
                        if rate is None:
                            fx_missing_total += 1
                            log.warning("ads_sync.fx_rate_missing_batch",
                                        currency=m.currency, report_date=str(m.report_date))
                        spend_pln = round(m.spend * rate, 4) if rate else None
                        sales_pln = round(m.sales_7d * rate, 4) if rate else None
                        params.extend([
                            m.asin, m.ad_type, m.report_date, m.marketplace_id, m.campaign_id,
                            m.sku or None,
                            m.impressions, m.clicks, m.spend, m.sales_7d, m.orders_7d, m.units_7d,
                            m.currency, spend_pln, sales_pln,
                        ])
                    cur.execute(f"INSERT INTO #tmp_pd VALUES {placeholders}", params)

                # 3. Deduplicate: keep last row per key (handles duplicate rows from API)
                #    MERGE requires unique source rows on the join keys
                cur.execute("""
                    ;WITH cte AS (
                        SELECT *, ROW_NUMBER() OVER (
                            PARTITION BY asin, ad_type, report_date, marketplace_id, campaign_id
                            ORDER BY spend DESC
                        ) AS rn
                        FROM #tmp_pd
                    )
                    DELETE FROM cte WHERE rn > 1
                """)

                # 4. Single MERGE from temp to target
                cur.execute("""
                    MERGE acc_ads_product_day AS tgt
                    USING #tmp_pd AS src
                    ON tgt.asin = src.asin
                       AND tgt.ad_type = src.ad_type
                       AND tgt.report_date = src.report_date
                       AND tgt.marketplace_id = src.marketplace_id
                       AND tgt.campaign_id = src.campaign_id
                    WHEN MATCHED THEN UPDATE SET
                        sku         = src.sku,
                        impressions = src.impressions,
                        clicks      = src.clicks,
                        spend       = src.spend,
                        sales_7d    = src.sales_7d,
                        orders_7d   = src.orders_7d,
                        units_7d    = src.units_7d,
                        currency    = src.currency,
                        spend_pln   = src.spend_pln,
                        sales_pln   = src.sales_pln,
                        synced_at   = GETUTCDATE()
                    WHEN NOT MATCHED THEN INSERT (
                        asin, ad_type, report_date, marketplace_id, campaign_id, sku,
                        impressions, clicks, spend, sales_7d, orders_7d, units_7d,
                        currency, spend_pln, sales_pln
                    ) VALUES (
                        src.asin, src.ad_type, src.report_date, src.marketplace_id, src.campaign_id, src.sku,
                        src.impressions, src.clicks, src.spend, src.sales_7d, src.orders_7d, src.units_7d,
                        src.currency, src.spend_pln, src.sales_pln
                    );
                """)

                cur.execute("DROP TABLE #tmp_pd")
                conn.commit()
                total += len(batch)
                log.info("ads_sync.product_day.batch_upserted", batch=total, total=len(metrics))
                break  # success — exit retry loop

            except Exception as exc:
                # Clean up temp table if it exists
                try:
                    cur.execute("IF OBJECT_ID('tempdb..#tmp_pd') IS NOT NULL DROP TABLE #tmp_pd")
                    conn.commit()
                except Exception:
                    # Connection might be broken — reconnect
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = connect_acc()
                    cur = conn.cursor()
                    cur.execute("SET LOCK_TIMEOUT 60000")

                if attempt < MAX_RETRIES - 1:
                    wait = 10 * (2 ** attempt)
                    log.warning("ads_sync.product_day.retry", attempt=attempt + 1, wait=wait, error=str(exc))
                    _time.sleep(wait)
                else:
                    raise

    cur.close()
    conn.close()

    # SF-10: surface total FX gaps for product-day batch
    if fx_missing_total:
        log.error("ads_sync.fx_gap_summary_product_day",
                  missing=fx_missing_total, total=total,
                  msg="Product-day rows written with NULL PLN values due to missing FX rates")

    return total


# ---------------------------------------------------------------------------
# Public API — main sync entry points
# ---------------------------------------------------------------------------

async def sync_ads_profiles() -> dict[str, Any]:
    """Step 1: Fetch and upsert advertising profiles."""
    profiles = await list_profiles()
    upserted = _upsert_profiles(profiles)
    return {
        "profiles_found": len(profiles),
        "profiles_upserted": upserted,
        "marketplaces": [p.country_code for p in profiles],
    }


async def sync_ads_campaigns() -> dict[str, Any]:
    """Step 2: Fetch and upsert campaigns for all profiles."""
    profiles = await list_profiles()
    total = 0

    for i, profile in enumerate(profiles):
        campaigns = await list_all_campaigns(profile.profile_id)
        upserted = _upsert_campaigns(campaigns, profile)
        total += upserted
        log.info(
            "ads_sync.campaigns",
            country=profile.country_code,
            campaigns=upserted,
        )
        # Rate-limit: 2s pause between profiles to avoid 429
        if i < len(profiles) - 1:
            await asyncio.sleep(2)

    return {"campaigns_upserted": total, "profiles_processed": len(profiles)}


async def sync_ads_daily_reports(
    days_back: int = 3,
) -> dict[str, Any]:
    """Step 3: Request and process daily performance reports.

    Default: last 3 days (to catch attribution window updates).
    For historical backfill: use days_back=60 (auto-splits into 31-day chunks).
    Amazon Ads Reporting API max date range = 31 days.
    """
    MAX_RANGE_DAYS = 31

    profiles = await list_profiles()
    overall_end = date.today() - timedelta(days=1)  # yesterday
    overall_start = overall_end - timedelta(days=days_back - 1)

    # Split into 31-day chunks
    chunks: list[tuple[date, date]] = []
    chunk_start = overall_start
    while chunk_start <= overall_end:
        chunk_end = min(chunk_start + timedelta(days=MAX_RANGE_DAYS - 1), overall_end)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end + timedelta(days=1)

    log.info("ads_sync.reports.plan", profiles=len(profiles), chunks=len(chunks),
             range=f"{overall_start}..{overall_end}", days=days_back)

    total_rows = 0

    for i, profile in enumerate(profiles):
        profile_rows = 0

        for chunk_idx, (start, end) in enumerate(chunks):
            log.info("ads_sync.reports.start", country=profile.country_code,
                     start=str(start), end=str(end), chunk=f"{chunk_idx+1}/{len(chunks)}")

            # Request SP, SB, SD reports sequentially to respect rate limits
            all_metrics: list[CampaignDayMetrics] = []
            for report_fn, label in [
                (request_sp_campaign_report, "SP"),
                (request_sb_campaign_report, "SB"),
                (request_sd_campaign_report, "SD"),
            ]:
                try:
                    rows = await report_fn(profile.profile_id, start, end)
                    # Amazon Ads v3 reports don't return currency per row —
                    # set from profile so PLN conversion works correctly.
                    for r in rows:
                        r.currency = profile.currency
                    all_metrics.extend(rows)
                    log.info("ads_sync.report_rows", ad_type=label, country=profile.country_code, rows=len(rows))
                except Exception as exc:
                    log.error("ads_sync.report_error", ad_type=label, country=profile.country_code, error=str(exc))
                # 5s pause between report types to avoid throttling
                await asyncio.sleep(5)

            upserted = _upsert_daily_metrics(all_metrics)
            profile_rows += upserted

            # 5s pause between chunks
            if chunk_idx < len(chunks) - 1:
                await asyncio.sleep(5)

        total_rows += profile_rows
        log.info("ads_sync.reports.done", country=profile.country_code, rows=profile_rows)

        # 8s pause between profiles to stay well under rate limits
        if i < len(profiles) - 1:
            await asyncio.sleep(8)

    return {
        "profiles_processed": len(profiles),
        "daily_rows_upserted": total_rows,
        "date_range": f"{start}..{end}",
    }


async def sync_ads_product_reports(
    days_back: int = 3,
) -> dict[str, Any]:
    """Step 4: Request advertised-product reports — spend per ASIN.

    Uses spAdvertisedProduct / sbAdvertisedProduct / sdAdvertisedProduct
    report types for precise ASIN-level cost allocation.
    """
    MAX_RANGE_DAYS = 31

    profiles = await list_profiles()
    overall_end = date.today() - timedelta(days=1)
    overall_start = overall_end - timedelta(days=days_back - 1)

    chunks: list[tuple[date, date]] = []
    chunk_start = overall_start
    while chunk_start <= overall_end:
        chunk_end = min(chunk_start + timedelta(days=MAX_RANGE_DAYS - 1), overall_end)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end + timedelta(days=1)

    log.info("ads_sync.product_reports.plan", profiles=len(profiles),
             chunks=len(chunks), range=f"{overall_start}..{overall_end}")

    total_rows = 0

    for i, profile in enumerate(profiles):
        profile_rows = 0

        for chunk_idx, (start, end) in enumerate(chunks):
            log.info("ads_sync.product_reports.start", country=profile.country_code,
                     start=str(start), end=str(end), chunk=f"{chunk_idx+1}/{len(chunks)}")

            all_metrics: list[ProductDayMetrics] = []
            for report_fn, label in [
                (request_sp_product_report, "SP"),
                (request_sb_product_report, "SB"),
                (request_sd_product_report, "SD"),
            ]:
                try:
                    rows = await report_fn(profile.profile_id, start, end)
                    # Set marketplace_id + currency from profile
                    # (Amazon Ads v3 reports don't include currency per row)
                    for r in rows:
                        r.marketplace_id = profile.marketplace_id
                        r.currency = profile.currency
                    all_metrics.extend(rows)
                    log.info("ads_sync.product_report_rows", ad_type=label,
                             country=profile.country_code, rows=len(rows))
                except Exception as exc:
                    log.error("ads_sync.product_report_error", ad_type=label,
                              country=profile.country_code, error=str(exc))
                await asyncio.sleep(5)

            upserted = _upsert_product_day_metrics(all_metrics)
            profile_rows += upserted

            if chunk_idx < len(chunks) - 1:
                await asyncio.sleep(5)

        total_rows += profile_rows
        log.info("ads_sync.product_reports.done", country=profile.country_code, rows=profile_rows)

        if i < len(profiles) - 1:
            await asyncio.sleep(8)

    return {
        "profiles_processed": len(profiles),
        "product_rows_upserted": total_rows,
        "date_range": f"{overall_start}..{overall_end}",
    }


async def run_full_ads_sync(days_back: int = 3) -> dict[str, Any]:
    """Run the complete ads sync pipeline:
    1. Profiles → 2. Campaigns → 3. Daily reports → 4. Product reports
    """
    if not settings.amazon_ads_enabled:
        return {"status": "skipped", "reason": "Amazon Ads API not configured"}

    log.info("ads_sync.full.start", days_back=days_back)

    # Step 0: Ensure tables
    ensure_ads_tables()

    # Step 1: Profiles
    profile_result = await sync_ads_profiles()
    log.info("ads_sync.profiles.done", **profile_result)

    # Step 2: Campaigns
    campaign_result = await sync_ads_campaigns()
    log.info("ads_sync.campaigns.done", **campaign_result)

    # Step 3: Daily campaign reports
    report_result = await sync_ads_daily_reports(days_back=days_back)
    log.info("ads_sync.reports.done", **report_result)

    # Step 4: Daily product (ASIN) reports
    product_result = await sync_ads_product_reports(days_back=days_back)
    log.info("ads_sync.product_reports.done", **product_result)

    result = {
        "status": "ok",
        **profile_result,
        **campaign_result,
        **report_result,
        **product_result,
    }
    log.info("ads_sync.full.done", **result)
    return result
