# ACC Rollout Plan (2026-03-07)

## Scope
- API fixes:
  - `last_sync` in KPI response
  - Strategy Overview error visibility (`Run Detection` / `Retry`)
  - Strategy family detection fix (`family_coverage_cache` join + `sku=None`)
- Performance:
  - in-memory cache: `executive.overview` (120s)
  - in-memory cache: `profitability.overview` (120s)
  - in-memory cache: `finance.dashboard` (120s)
  - in-memory cache: `inventory.overview` (180s)
  - in-memory cache: `courier.readiness` (180s)
- Ops tooling:
  - read-only smoke: `run_api_readonly_endpoint_smoke.py`
  - prod audit: `run_acc_prod_audit.py`
  - courier idle watcher: `watch_courier_idle.py`
  - SQL index recommendations: `sql_acc_perf_recommendations.sql`

## Pre-Deploy Checklist
1. Ensure no critical manual actions in UI.
2. Confirm courier background jobs are idle:
   - `python scripts/watch_courier_idle.py --poll-sec 15 --max-minutes 30`
3. Verify queue state:
   - only expected `pending`; no stale `running` > 60 min.
4. Take current baseline:
   - `python scripts/run_acc_prod_audit.py`
   - save generated JSON path.

## Deploy Window (short)
1. Stop API process (`uvicorn`).
2. Start API process with current command/profile.
3. Wait for `/health` and `/api/v1/health/order-sync` = healthy.

## Post-Deploy Validation
1. Read-only endpoint smoke:
   - `python scripts/run_api_readonly_endpoint_smoke.py`
2. Verify key pages:
   - Executive, Strategy, Profitability, Inventory, Finance, FBA, Courier.
3. Confirm KPI freshness badge:
   - no false `Brak synchronizacji` when sync is healthy.
4. Confirm Strategy button:
   - failures now visible (not silent `No data` only).

## Optional SQL Step (separate maintenance window)
1. Review and apply indexes one-by-one from:
   - `scripts/sql_acc_perf_recommendations.sql`
2. After each index:
   - run `python scripts/run_acc_prod_audit.py`
   - compare endpoint timings and SQL `avg_data_io_pct`.
3. Roll back individual index if no improvement.

## Rollback
1. Revert API code to previous revision.
2. Restart API.
3. If SQL indexes were applied, drop only the new index that regressed performance.

