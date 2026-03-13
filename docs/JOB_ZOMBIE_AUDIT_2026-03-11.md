# Job Zombie Audit 2026-03-11

## Scope

Audit of `dbo.acc_al_jobs` active rows (`pending`, `running`, `retrying`) on `2026-03-11`.

Goal:
- identify rows that are safe to treat as **certain zombies**
- separate them from rows that are only **likely stale**
- define a **safe cleanup plan per job type**

All checks below were read-only.

## Audit method

For each active row:
- compare current age vs historical `completed` runtimes for the same `job_type` over the last 30 days
- inspect `progress_pct` and `progress_message`
- inspect `last_heartbeat_at`
- inspect `trigger_source`
- inspect whether there are many overlapping duplicates of the same manual job type

Important nuance:
- not every job type writes progress after start
- not every job type has completed history
- `pending` does not mean zombie; it usually means queue/backlog residue

## Certain zombies

These rows are safe to classify as orphaned/stale because they are many times above their own historical max completed runtime.

### `sync_pricing`

- historical max completed runtime: `36.3 min`
- active running row age: `2037 min`
- rows:
  - `c1f40417-a1a0-42a7-bf04-40065a6231fc`

### `dhl_aggregate_logistics`

- historical max completed runtime: `17.8 min`
- active running row age: `1957 min`
- rows:
  - `6ee1ac22-847b-4122-968d-d8d627be6a16`

### `dhl_seed_shipments_from_staging`

- historical max completed runtime: `61.6 min`
- active running row age: `1957 min`
- rows:
  - `89111856-fa0d-401f-8e7a-da9a7f88deee`

### `dhl_shadow_logistics`

- historical max completed runtime: `14.0 min`
- active running row age: `1957 min`
- rows:
  - `88d9a931-ca43-44fa-9c2b-25ab687c7876`

### `dhl_sync_costs`

- historical max completed runtime: `1.7 min`
- active running row age: `1957 min`
- rows:
  - `deb1eacc-a042-4776-916b-1ba3d8923a7f`

### `gls_aggregate_logistics`

- historical max completed runtime: `24.8 min`
- active running row age: `1942 min`
- rows:
  - `368ab11e-2afb-48d0-ba0f-45fc9d441619`

### `gls_seed_shipments_from_staging`

- historical max completed runtime: `51.7 min`
- active running row ages: `1942 min`, `502 min`
- rows:
  - `248e04cb-e34e-4e22-8960-1c491825d274`
  - `7be99399-2b2a-4e86-850c-e0115c462385`

### `gls_shadow_logistics`

- historical max completed runtime: `23.3 min`
- active running row age: `1942 min`
- rows:
  - `90138188-eb77-428e-9e31-a671988b30a4`

### `family_sync_marketplace_listings`

- historical max completed runtime: `11.5 min`
- active running rows: `28`
- active age range: `702 .. 1923 min`
- all `28` active rows are safe to classify as stale

Row ids:
- `ed526b6e-be88-4bd2-a8c2-0e82e390d973`
- `fd4018ea-41ee-404d-895b-782c29fdf508`
- `45cb6a7a-8465-42a7-881b-45073597c95c`
- `7edf8953-907d-468a-8cab-2a05f48c8476`
- `4d25bbd2-7ff2-48da-9695-045ff207c3cf`
- `58a86a29-c1c2-4391-b418-b1384c7bdac0`
- `2b29007f-7f42-4182-b955-727c652edea0`
- `efdec3e9-9631-4be6-8b83-07ade76ec9e2`
- `52868c26-aa16-4573-af7c-2405daf8c6a2`
- `f725170f-bf55-452e-981e-d6f5552b18fe`
- `bb8d5c69-bb76-4e76-adfd-832ce3f1ceb3`
- `61629967-e813-4e8f-abe7-d66080cb1a3d`
- `3fba2cf3-8498-4d74-aefe-18f737cdc24c`
- `ea38a566-aa48-4781-abfa-2537517d115a`
- `e018c8f6-a01d-4ec3-8051-b5b8f6bdca61`
- `f1030aa3-1197-48af-9b8b-02fb594966d2`
- `e94a8a80-2a03-4dab-9254-19b273c3c392`
- `60e9b2fe-ad9f-4018-92bf-3ff108d20f42`
- `775429a8-61d0-410e-8746-7832c4426db6`
- `95fec33b-7eab-4533-849b-b2043c827b86`
- `33c29450-68c4-40cf-ac19-cd0ff75a16f5`
- `29be07e7-b1ed-4d1e-9ac7-6e16ca20554d`
- `a7b0eef2-d115-4ae5-8a25-a5b24144f451`
- `913a4ea1-6b65-4982-a8c8-ed6e7c2598c3`
- `eb5ad67a-0c9f-4277-b011-3a62d0f777d3`
- `a48b76ab-4018-41a1-9099-2e32d8c8262f`
- `ba90e267-6a42-4945-8f07-0d7bbc605f88`
- `3a00b2aa-d5f0-4f4c-ab75-26d226cf5a10`

### `sync_listings_to_products`

- historical max completed runtime: `3.4 min`
- active running rows: `6`
- active age range: `577 .. 1918 min`
- all `6` active rows are safe to classify as stale

Row ids:
- `ce4fffbb-8519-4805-9e70-d2d7a58f4c9d`
- `656f101b-8173-427f-93ea-3c7bc10fe35f`
- `4ebbc985-855c-4e40-9786-6e478976901f`
- `858cd1bd-b2a8-4938-bf9e-1510f758e87b`
- `f72ed561-2769-4dab-bb52-4edab6cd770c`
- `4f2dfc90-5d9f-4f72-88af-6ad98a5ab46a`

## Likely stale, but do not auto-kill blindly

These buckets are heavily polluted, but not every row should be failed blindly without a narrower rule.

### `family_matching_pipeline`

- no completed runtime baseline in `acc_al_jobs`
- active rows:
  - `64` running older than `360 min`
  - `8` running `<= 120 min`
  - `1` pending
- strong signal of staleness:
  - many overlapping manual duplicates
  - rows stuck at `10% Starting...` or `85% Family matching done, recomputing coverage`

Safe rule:
- fail only rows older than `360 min`
- keep rows `<= 120 min`
- review rows `121..360 min` manually

### `profit_ai_match_run`

- no completed runtime baseline in `acc_al_jobs`
- active rows:
  - `38` running older than `360 min`
  - `8` running `<= 120 min`
- all rows are manual duplicates, all stuck at `10% Starting...`

Safe rule:
- fail rows older than `360 min`
- keep rows `<= 120 min`

### `returns_backfill_fba`

- no completed runtime baseline in `acc_al_jobs`
- active rows:
  - `23` running older than `360 min`
  - `4` running `<= 120 min`
  - `1` pending

Safe rule:
- fail rows older than `360 min`
- keep rows `<= 120 min`
- drop old pending row if a fresh backfill will be launched manually

### `inventory_taxonomy_refresh`

- historical max completed runtime: `114.9 min`
- active rows:
  - `18` running older than `360 min`
  - `4` running `<= 120 min`
  - `2` retrying

Safe rule:
- fail running rows older than `360 min`
- keep fresh rows `<= 120 min`
- keep current retrying rows unless they age beyond `360 min`

### `courier_refresh_monthly_kpis`

- `1` running row older than `360 min`
- no completed baseline in `acc_al_jobs`
- known operationally to be much shorter than many hours for scoped runs

Safe rule:
- inspect latest carrier/month params first
- if no worker/log ownership is confirmed, fail the row and rerun one scoped KPI refresh

### `returns_sync_fba`

- `1` running row older than `360 min`
- no completed baseline in current audit window

Safe rule:
- confirm whether any FBA returns sync is really live
- if not, fail and rerun one controlled job

### `inventory_apply_draft`

- `1` running row older than `360 min`
- high-risk job type because it changes inventory state

Safe rule:
- do not auto-fail blindly
- first confirm with operator whether a draft apply was intentionally running

## Stale pending backlog

These are not zombies in the strict sense. They are queued rows that have not started and now look obsolete.

### Scheduler backlog

- `run_fba_alerts`: `8 pending`
- `sync_orders`: `6 pending`
- `poll_sqs_notifications`: `3 pending`
- `process_notification_events`: `2 pending`
- `sync_fba_inbound`: `1 pending`
- `sync_tkl_cache`: `1 pending`

Safe rule:
- for scheduler-driven job types, keep only the newest pending row per `job_type`
- older pending scheduler rows can be marked terminal as obsolete backlog

### Manual backlog

- `family_matching_pipeline`: `1 pending`
- `returns_backfill_fba`: `1 pending`

Safe rule:
- ask whether these manual runs are still wanted
- otherwise mark terminal before launching a fresh controlled run

## Suggested cleanup order

1. Fail all rows in the **Certain zombies** section.
2. Prune stale scheduler `pending` backlog, keeping only the newest pending row per type.
3. For `family_matching_pipeline`, `profit_ai_match_run`, `returns_backfill_fba`, `inventory_taxonomy_refresh`:
   - fail only rows older than `360 min`
   - leave rows `<= 120 min` untouched
4. Do manual confirmation before touching:
   - `inventory_apply_draft`
   - `returns_sync_fba`
   - `courier_refresh_monthly_kpis`

## Execution result

Cleanup was executed on `2026-03-11` after this audit.

Applied:
- certain zombies: cleared
- stale manual running duplicates `>360 min`: `77`
- stale manual pending duplicates `>360 min`: `2`
- stale scheduler pending backlog rows: `15`

Rows intentionally left untouched:
- `sync_ads`: `1 running`
- `courier_refresh_monthly_kpis`: `1 running`
- `inventory_apply_draft`: `1 running`
- `returns_sync_fba`: `1 running`
- newest scheduler `pending` rows:
  - `run_fba_alerts`: `1`
  - `sync_orders`: `1`
  - `poll_sqs_notifications`: `1`
  - `process_notification_events`: `1`
  - `sync_fba_inbound`: `1`
  - `sync_tkl_cache`: `1`

Post-cleanup active state for previously polluted manual buckets:
- `family_matching_pipeline`: `8 running`, age `17..123 min`
- `profit_ai_match_run`: `8 running`, age `18..124 min`
- `returns_backfill_fba`: `4 running`, age `41..117 min`
- `inventory_taxonomy_refresh`: `3 running`, `3 retrying`, fresh/young only

## SQL patterns used

### Active rows

```sql
SELECT id, job_type, status, trigger_source, created_at, started_at, finished_at,
       last_heartbeat_at, progress_pct, progress_message,
       DATEDIFF(minute, COALESCE(started_at, created_at), SYSUTCDATETIME()) AS age_min
FROM dbo.acc_al_jobs WITH (NOLOCK)
WHERE status IN ('pending','running','retrying');
```

### Historical completed runtime by job type

```sql
SELECT job_type,
       COUNT(*) AS completed_cnt,
       AVG(DATEDIFF(second, started_at, finished_at)) / 60.0 AS avg_min,
       MAX(DATEDIFF(second, started_at, finished_at)) / 60.0 AS max_min
FROM dbo.acc_al_jobs WITH (NOLOCK)
WHERE status = 'completed'
  AND started_at IS NOT NULL
  AND finished_at IS NOT NULL
  AND finished_at >= DATEADD(day, -30, SYSUTCDATETIME())
GROUP BY job_type;
```

### Example safe stale rule

```sql
SELECT id, job_type
FROM dbo.acc_al_jobs WITH (NOLOCK)
WHERE status = 'running'
  AND job_type = 'sync_listings_to_products'
  AND DATEDIFF(minute, COALESCE(started_at, created_at), SYSUTCDATETIME()) > 60;
```
