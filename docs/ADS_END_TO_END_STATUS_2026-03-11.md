# Ads End-to-End Status 2026-03-11

## Current status

- Amazon Ads integration is active in ACC.
- Profiles and campaigns are fresh on `2026-03-11`.
- Daily ads facts are **not fully complete** from the start of 2026:
  - `acc_ads_campaign_day`: data from `2026-01-03` through `2026-03-10`
  - `acc_ads_product_day`: data from `2026-01-03` through `2026-03-09`
- Missing dates:
  - both tables: `2026-01-01`, `2026-01-02`
  - `acc_ads_product_day`: also missing `2026-03-10`

## Current running jobs

`dbo.acc_al_jobs` contains many `sync_ads` rows with `status='running'`.

Important nuance:
- `sync_ads` does **not** update heartbeat during execution.
- Old `last_heartbeat_at` alone does not prove a zombie.

What is definitely true on `2026-03-11`:
- there are multiple overlapping manual `sync_ads` runs,
- `sync_ads` is **not** in `_SINGLE_FLIGHT_JOB_TYPES`,
- partial data for `report_date='2026-03-10'` is already visible in `acc_ads_campaign_day`,
- `acc_ads_product_day` still has `0` rows for `2026-03-10`.

This means:
- at least one current run is/was writing,
- some older `running` rows are almost certainly stale/orphaned,
- launching another fresh sync immediately would risk duplicate load on Amazon Ads API.

## Canonical table usage

### `dbo.acc_ads_product_day`

This is the **profit-critical** ads fact table.

Read by:
- `apps/api/app/services/profit_engine.py`
  - live product profit path
  - main ads spend lookup by `(asin, marketplace_id)`
- `apps/api/app/services/profitability_service.py`
  - rollup recompute path
  - direct SKU/day ad spend enrichment

Business meaning:
- this is the main source for product-level ad cost in profit/rollup logic.

### `dbo.acc_ads_campaign_day`

This is the **campaign/dashboard/fallback** ads fact table.

Read by:
- `apps/api/app/api/v1/ads.py`
  - ads summary/chart/top-campaigns/campaign-stats style endpoints
- `apps/api/app/services/guardrails.py`
  - ads freshness / ads presence checks
- `apps/api/app/services/profit_engine.py`
  - fallback when `acc_ads_product_day` has no ASIN-level data for a marketplace

Business meaning:
- good for ads dashboards and marketplace-level fallback,
- not the preferred product-level profit source.

### `dbo.acc_ads_campaign`

Metadata/dimension table used by:
- `apps/api/app/api/v1/ads.py`
- `apps/api/app/services/profit_engine.py` fallback join with `acc_ads_campaign_day`

## Why profit can lag behind ads dashboards

`acc_ads_campaign_day` may already have a newer date while `acc_ads_product_day` is still behind.

That means:
- Ads dashboard endpoints can look fresher,
- while product profit / rollups are still one day behind on ad spend.

This is exactly the current state on `2026-03-11`:
- `campaign_day` has partial `2026-03-10`
- `product_day` still stops at `2026-03-09`

## End-to-end freshness checklist

To confirm Ads is fully fresh in ACC, check all of the below:

1. Job health
- `dbo.acc_al_jobs`:
  - no old `sync_ads` rows stuck in `running`
  - one latest run reaches `completed`
- if multiple manual runs overlap, do not trigger another until they settle

2. Profiles/campaign metadata freshness
- `dbo.acc_ads_profile`: `MAX(synced_at)` recent
- `dbo.acc_ads_campaign`: `MAX(synced_at)` recent

3. Campaign-day freshness
- `dbo.acc_ads_campaign_day`:
  - `MAX(report_date)` should be yesterday
  - no unexpected gaps in date series

4. Product-day freshness
- `dbo.acc_ads_product_day`:
  - `MAX(report_date)` should be yesterday
  - no unexpected gaps in date series
- this is the most important check for profit accuracy

5. Profit source validation
- confirm `profit_engine.py` is reading `dbo.acc_ads_product_day`
- confirm rollup recompute in `profitability_service.py` is also reading `dbo.acc_ads_product_day`
- only treat `campaign_day` as fallback or dashboard source

6. Same-day partial writes
- if `campaign_day` has yesterday and `product_day` does not, profit is still behind
- do not sign off end-to-end freshness until `product_day` catches up

## Safe operational guidance

- Do not spam `POST /api/v1/ads/sync` manually.
- `sync_ads` currently allows overlap because it is not single-flight.
- If active current runs are still within normal runtime and writing data, wait.
- If they stop progressing for well beyond the normal runtime window, mark stale runs terminal and then launch exactly one fresh sync.

## SQL checks used

### Range and counts

```sql
SELECT MIN(report_date), MAX(report_date), COUNT(*)
FROM dbo.acc_ads_campaign_day WITH (NOLOCK);

SELECT MIN(report_date), MAX(report_date), COUNT(*)
FROM dbo.acc_ads_product_day WITH (NOLOCK);
```

### Missing dates from 2026-01-01

```sql
WITH d AS (
  SELECT CAST('2026-01-01' AS date) dt
  UNION ALL
  SELECT DATEADD(day, 1, dt) FROM d WHERE dt < CAST('2026-03-10' AS date)
),
p AS (
  SELECT DISTINCT CAST(report_date AS date) dt
  FROM dbo.acc_ads_product_day WITH (NOLOCK)
  WHERE report_date >= '2026-01-01' AND report_date <= '2026-03-10'
)
SELECT d.dt
FROM d
LEFT JOIN p ON p.dt = d.dt
WHERE p.dt IS NULL
OPTION (MAXRECURSION 400);
```

### Running sync jobs

```sql
SELECT
    id,
    status,
    created_at,
    started_at,
    finished_at,
    last_heartbeat_at,
    progress_pct,
    progress_message,
    params_json
FROM dbo.acc_al_jobs WITH (NOLOCK)
WHERE job_type = 'sync_ads'
ORDER BY created_at DESC;
```

### Current partial writes for one report date

```sql
SELECT COUNT(*), MIN(synced_at), MAX(synced_at)
FROM dbo.acc_ads_campaign_day WITH (NOLOCK)
WHERE report_date = '2026-03-10';

SELECT COUNT(*), MIN(synced_at), MAX(synced_at)
FROM dbo.acc_ads_product_day WITH (NOLOCK)
WHERE report_date = '2026-03-10';
```
