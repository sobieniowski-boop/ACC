# Courier Production Cutover (DHL/GLS)

## 1) Scope and DoD
- Target months: `2025-11`, `2025-12`, `2026-01`
- Carriers: `DHL`, `GLS`
- Hard DoD per scope (`month+carrier`):
  - `orders_with_fact == orders_universe`
  - `go_no_go == GO`

## 2) Runtime Architecture (Production)
- Job type: `courier_order_universe_linking`
- Execution mode: sequential `month+carrier` (no giant batch)
- Supervisor runner:
  - `scripts/run_courier_order_universe_supervisor.py`
  - stale watchdog (`--stale-timeout-sec`)
  - hard timeout (`--hard-timeout-sec`)
  - transient retries (`--transient-retries`)
  - checkpoint artifact JSON
- Single-flight enforced for this job type (no concurrent duplicate run).

## 3) Scheduling
- Production launcher: `scripts/run_courier_production_supervisor.cmd`
- Recommended windows:
  - Main window: nightly (02:00 local)
  - Retry window: 05:00 local (same command)
- Windows Task Scheduler:
  - Action: run `run_courier_production_supervisor.cmd`
  - Run whether user logged in or not
  - Stop task if running longer than 8h

## 4) Guardrails and Alerts
- Existing courier alerts + added guardrails:
  - `courier_pipeline_stale_run` (critical)
  - `courier_readiness_closed_months` (critical)
- Alert triggers:
  - stale run (no progress heartbeat >= 20 min)
  - readiness `overall_go_no_go != GO`
  - coverage and shadow drift alerts from existing rules

## 5) Readiness and Decision
- Endpoint: `GET /api/v1/courier/readiness`
- Inputs:
  - `months=2025-11&months=2025-12&months=2026-01`
  - `carriers=DHL&carriers=GLS`
- Output:
  - `overall_go_no_go`
  - `summary.scopes_total/scopes_go/scopes_no_go/running_jobs`
  - matrix per month+carrier
- Decision rule:
  - `GO`: all 6 scopes are `GO`
  - `NO_GO`: any scope is `NO_GO`

## 6) DB Stability Hardening
- Applied:
  - pymssql query timeout respects caller timeout (with safe floor)
  - longer timeouts for heavy linking/snapshot stages
  - transient retry in supervisor for connection failures
- Operational note:
  - avoid starting parallel heavy courier jobs manually

## 7) API Performance Verification (p95)
- Script: `scripts/measure_api_p95.py`
- Example:
  - `python scripts/measure_api_p95.py --base-url http://127.0.0.1:8000 --samples 40`
- Baseline acceptance:
  - `/api/v1/health` p95 <= 1000 ms
  - `/api/v1/jobs` p95 <= 3000 ms during active courier run
  - no >2% 5xx/timeouts in probe window

## 8) E2E and Idempotency
- Script: `scripts/run_courier_e2e_idempotency.py`
- Flow:
  - readiness before
  - full run
  - readiness after first
  - full run again
  - readiness after second
  - artifact with `idempotency_pass`
- Artifact path:
  - `scripts/courier_e2e_artifact_YYYYMMDD_HHMMSS.json`

## 9) SQL Performance Review
- Validate plans for heavy phases (`link`, `aggregate`, `shadow`) on production-sized data.
- Review index coverage on:
  - `acc_shipment_order_link`
  - `acc_order_logistics_fact`
  - `acc_order_logistics_shadow`
  - BL cache tables (`acc_cache_bl_orders`, `acc_cache_packages`, distribution caches)
- Apply index changes only in low-traffic window.

## 10) Cutover Procedure
- Step 1: confirm API healthy (`/api/v1/health=200`)
- Step 2: run supervisor for target scopes
- Step 3: monitor `jobs` + `courier/readiness`
- Step 4: if `NO_GO`, inspect failed scope and rerun only failed scope
- Step 5: when all scopes `GO`, mark production readiness

## 11) Rollback / Re-run
- Rollback trigger:
  - repeated transient failures beyond retry window
  - p95 degradation beyond thresholds
  - readiness regression to `NO_GO`
- Rollback action:
  - stop current supervisor run
  - postpone to next retry window
  - rerun failed scopes only

## 12) Ownership / Response SLA
- Primary owner: Courier pipeline operator (on-call)
- Backup: API/platform owner
- Response targets:
  - stale run alert: react <= 15 min
  - failed run alert: react <= 30 min
  - readiness `NO_GO` at cutover time: immediate block on release

## 13) Release Freeze Checklist
- Freeze courier scope after final validation (no new logic changes)
- Capture artifacts:
  - readiness JSON
  - supervisor checkpoint JSON
  - p95 probe report
  - e2e idempotency artifact
- Tag release commit and open deployment window
- Confirm rollback operator and communication channel
