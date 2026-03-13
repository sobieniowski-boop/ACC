# Courier Job Architecture

## Scope

This document defines the job model for the courier logistics module across:

- DHL billing files + DHL operational API
- GLS billing files + optional GLS operational API
- canonical shipment model (`acc_shipment*`)
- order logistics fact/shadow (`acc_order_logistics_fact`, `acc_order_logistics_shadow`)
- downstream profit/KPI consumers

Rule:

- No heavy or potentially long-running operation may run in request-response.
- API/UI calls may only enqueue a job and return `job_id`.
- Only lightweight read-only diagnostics stay synchronous.

## Runtime Contract

Every courier job must use the existing `acc_al_jobs` contract and should be extended conceptually with:

- `id`
- `job_type`
- `trigger_source`
- `status`: `pending | running | succeeded | failed | partial | cancelled`
- `progress_pct`
- `progress_message`
- `records_processed`
- `error_message`
- `started_at`
- `last_heartbeat_at`
- `finished_at`
- `duration_seconds`
- `params_json`

Recommended next extensions to the job contract:

- `result_json`
- `parent_job_id`
- `attempt_no`
- `idempotency_key`
- `carrier`
- `window_from`
- `window_to`

## Global Rules

### Idempotency

- File imports: idempotent by `(source_kind, file_path, file_size, file_mtime_utc)`.
- Shipment seed: idempotent by `(carrier, shipment_number)` and link uniqueness on `(shipment_id, amazon_order_id, link_method)`.
- Shipment cost: idempotent by `(shipment_id, cost_source)`.
- Order logistics fact: idempotent by `(amazon_order_id, calc_version)`.
- Shadow: idempotent by `(amazon_order_id, calc_version)`; full rebuild jobs should replace scoped rows first.
- Alerts: deduplicate by `(rule, entity_key, unresolved window)`.
- Apply jobs: idempotent by immutable override id / approval id.

### Retry / Backoff

- DB lock / transient SQL: exponential backoff `1s, 2s, 4s, 8s, 16s`, max 5 attempts.
- External API/network: `30s, 2m, 10m`, max 3 attempts.
- File parse / validation error: no blind retry; mark file `failed`.
- Parent/orchestrator jobs do not restart completed child stages.

### Logging

Every job logs structured events with:

- `job_id`
- `job_type`
- `carrier`
- `window_from`
- `window_to`
- `stage`
- `attempt`
- `chunk_no`
- `records_processed`
- `duration_ms`
- `error_class`
- `error_message`

### Progress / Status

Recommended stage split:

- `0-5`: enqueue / bootstrap
- `5-20`: discovery / candidate selection
- `20-80`: main processing
- `80-95`: finalization / upserts / commit
- `95-100`: verification + finish

`partial` is valid when:

- some files succeeded and some failed
- some child jobs succeeded and parent orchestrator must stop

## Pipeline Jobs

These are top-level orchestrators. Child jobs remain independently runnable.

### `courier_dhl_nightly_pipeline`

- Trigger: daily, night, scheduler
- Input: `window_from`, `window_to`, `purchase_from`, `purchase_to`, limits
- Output: child job ids + summary
- Idempotency: parent uses `idempotency_key = dhl_nightly:{date}`
- Retry/backoff: child-aware; does not rerun succeeded children
- Logging: one line per child stage start/finish
- Progress/status: aggregates child progress

Stages:

1. `dhl_import_billing_files`
2. `dhl_seed_shipments_from_staging`
3. `dhl_sync_tracking_events`
4. `dhl_sync_pod`
5. `dhl_sync_costs`
6. `dhl_aggregate_logistics`
7. `dhl_shadow_logistics`
8. `courier_evaluate_alerts`

### `courier_gls_nightly_pipeline`

- Trigger: daily, night, scheduler
- Input: same shape as DHL
- Output: child job ids + summary
- Idempotency: parent uses `idempotency_key = gls_nightly:{date}`
- Retry/backoff: child-aware; does not rerun succeeded children
- Logging: one line per child stage start/finish
- Progress/status: aggregates child progress

Stages:

1. `gls_import_billing_files`
2. `gls_seed_shipments_from_staging`
3. `gls_sync_tracking_events` if API enabled
4. `gls_sync_costs`
5. `gls_aggregate_logistics`
6. `gls_shadow_logistics`
7. `courier_evaluate_alerts`

### `courier_recompute_window`

- Trigger: on-demand
- Input: `carrier`, `window_from`, `window_to`, `purchase_from`, `purchase_to`, `seed_all_existing`, `refresh_existing`, `replace_shadow`
- Output: full recompute summary for the window
- Idempotency: key on `(carrier, window, params_hash)`
- Retry/backoff: stage-aware
- Logging: logs each child stage with same `parent_job_id`
- Progress/status: weighted by stage

## Sync Jobs

### `dhl_import_billing_files` (existing)

- Trigger: daily incremental; on-demand backfill
- Input:
  - `invoice_root`
  - `jj_root`
  - `manifest_path`
  - `force_reimport`
  - `limit_invoice_files`
  - `limit_jj_files`
- Output:
  - imported invoice rows
  - imported JJ rows
  - imported manifest rows
  - failed file list
- Idempotency:
  - file snapshot based
  - replaces billing lines per document
  - merges JJ rows by source row
- Retry/backoff:
  - file-level retry only on transient IO
  - SQL lock retry for JJ chunk writes
- Logging:
  - per file start/finish
  - per JJ chunk retry
  - final stats
- Progress/status:
  - discovery
  - invoice import
  - JJ import
  - final summary

### `gls_import_billing_files` (existing)

- Trigger: daily incremental; on-demand backfill
- Input:
  - `invoice_root`
  - `bl_map_path`
  - `force_reimport`
  - `limit_invoice_files`
- Output:
  - billing lines imported
  - BL map rows imported
  - failed file list
- Idempotency:
  - file snapshot based
  - replaces billing lines per source file
  - replaces BL map by source file
- Retry/backoff:
  - file-level retry on transient IO
  - no retry on CSV parse errors
- Logging:
  - per file start/finish
  - parse failures with file path
  - final stats
- Progress/status:
  - BL map phase
  - invoice file phase
  - final summary

### `dhl_seed_shipments_from_staging` (recommended explicit job)

- Trigger: daily after import; on-demand after backfill
- Input:
  - `seed_all_existing`
  - optional `parcel_base_scope`
  - optional `window_from`, `window_to`
- Output:
  - shipments seeded
  - links written
  - linked/unlinked shipment counts
- Idempotency:
  - upsert shipment on `(carrier, shipment_number)`
  - upsert links on `(shipment_id, amazon_order_id, link_method)`
- Retry/backoff:
  - SQL transient retry
  - no retry for deterministic mapping ambiguity
- Logging:
  - seed scope size
  - linked/unlinked counts
  - top ambiguity reasons
- Progress/status:
  - load parcel scope
  - load seeds
  - seed shipments
  - seed links

### `gls_seed_shipments_from_staging` (recommended explicit job)

- Trigger: daily after import; on-demand after backfill
- Input:
  - `seed_all_existing`
  - optional `parcel_number_scope`
  - optional `window_from`, `window_to`
- Output:
  - shipments seeded
  - links written
  - linked/unlinked shipment counts
- Idempotency: same pattern as DHL
- Retry/backoff: SQL transient retry
- Logging:
  - tracking matches
  - `note1 -> bl_order_id` fallback usage
  - unresolved parcels
- Progress/status:
  - load parcel scope
  - seed shipments
  - seed links

### `dhl_sync_tracking_events` (existing)

- Trigger: hourly for recent 7-14 days; on-demand
- Input:
  - `created_from`, `created_to`
  - `limit_shipments`
  - `refresh_recent_hours`
- Output:
  - events inserted
  - shipments updated
  - delivered truth updates
- Idempotency:
  - event dedup by `(shipment_id, event_code, event_at, event_label)`
  - shipment upsert by id
- Retry/backoff: carrier API retry
- Logging:
  - carrier API batch size
  - event counts
  - failed shipment ids
- Progress/status:
  - target selection
  - API reads
  - event upserts
  - shipment state updates

### `dhl_sync_pod` (missing, should be added)

- Trigger: hourly for newly delivered shipments; on-demand
- Input:
  - `delivered_from`
  - `delivered_to`
  - `limit_shipments`
  - `refresh_existing`
- Output:
  - POD rows inserted/updated
  - missing POD count
- Idempotency:
  - upsert by `(shipment_id, pod_type)`
- Retry/backoff: API retry with carrier-specific throttling
- Logging:
  - pod fetch attempts
  - payload presence / absence
- Progress/status:
  - select delivered targets
  - fetch POD
  - upsert POD

### `dhl_backfill_shipments` (existing)

- Trigger: on-demand only
- Input:
  - `created_from`, `created_to`
  - `limit_shipments`
- Output:
  - shipment registry rows
  - link rows
- Idempotency: shipment upsert + link upsert
- Retry/backoff: API retry
- Logging: API page counts, shipment ids, link results
- Progress/status: page-based

### `gls_sync_tracking_events` (optional, only if GLS API used operationally)

- Trigger: hourly, only if GLS operational API is enabled
- Input:
  - `created_from`, `created_to`
  - `limit_shipments`
- Output:
  - tracking events inserted
  - delivered state updates
- Idempotency: same event-dedup pattern as DHL
- Retry/backoff: API retry
- Logging: parcel batch stats, failures
- Progress/status: target selection -> API -> upsert

## Rollup / Cache Jobs

### `dhl_sync_costs` (existing)

- Trigger: daily after seed; on-demand during recompute
- Input:
  - `created_from`, `created_to`
  - `limit_shipments`
  - `refresh_existing`
  - `allow_estimated` (should remain `False` in production financial runs)
- Output:
  - actual costs written
  - estimated costs written
  - no-match count
- Idempotency:
  - upsert `(shipment_id, cost_source)`
  - delete competing actual rows for same shipment
- Retry/backoff:
  - SQL transient retry
  - carrier estimate retry only if estimation mode allowed
- Logging:
  - selected targets
  - actual-vs-estimated split
  - no-match reasons
- Progress/status:
  - target selection
  - imported cost lookup
  - cost upserts

### `gls_sync_costs` (existing)

- Trigger: daily after seed; on-demand during recompute
- Input:
  - `created_from`, `created_to`
  - `limit_shipments`
  - `refresh_existing`
- Output:
  - actual costs written
  - no-match count
- Idempotency: upsert `(shipment_id, cost_source)`
- Retry/backoff: SQL transient retry
- Logging:
  - parcel lookup results
  - missing billing cost coverage
- Progress/status:
  - target selection
  - billing lookup
  - cost upserts

### `dhl_aggregate_logistics` (existing)

- Trigger: daily after cost sync; on-demand
- Input:
  - `created_from`, `created_to`
  - `limit_orders`
- Output:
  - orders aggregated
  - shipments aggregated
  - actual / estimated shipment counts
- Idempotency:
  - upsert fact by `(amazon_order_id, calc_version='dhl_v1')`
- Retry/backoff: SQL transient retry
- Logging:
  - order count
  - null-cost shipments skipped
- Progress/status:
  - load order scope
  - aggregate
  - upsert fact

### `gls_aggregate_logistics` (existing)

- Trigger: daily after cost sync; on-demand
- Input: same shape as DHL
- Output: same shape as DHL
- Idempotency: upsert fact by `(amazon_order_id, calc_version='gls_v1')`
- Retry/backoff: SQL transient retry
- Logging: same pattern as DHL
- Progress/status: same pattern as DHL

### `courier_rollup_daily_kpis` (recommended)

- Trigger: hourly small window + nightly full day
- Input:
  - `date_from`, `date_to`
  - `carrier`
- Output:
  - per-day/per-carrier operational KPIs snapshot:
    - shipments
    - linked %
    - cost coverage %
    - delivered %
    - shadow drift %
- Idempotency:
  - replace snapshot rows by `(carrier, kpi_date)`
- Retry/backoff: SQL transient retry
- Logging: row counts + coverage summary
- Progress/status: date-window based

### `courier_rollup_gap_snapshot` (recommended)

- Trigger: hourly
- Input:
  - `carrier`
  - `window_from`, `window_to`
- Output:
  - unmatched shipments snapshot
  - missing cost snapshot
  - stale tracking snapshot
- Idempotency:
  - replace snapshot rows by `(carrier, snapshot_ts, gap_type)` or rolling current snapshot
- Retry/backoff: SQL transient retry
- Logging: counts per gap type
- Progress/status: gap-type stages

## Alert Engine Jobs

### `courier_evaluate_alerts` (recommended shared engine)

- Trigger: hourly
- Input:
  - `carrier` or `all`
  - `window_from`, `window_to`
  - thresholds JSON
- Output:
  - alerts created / updated / auto-resolved
- Idempotency:
  - dedupe unresolved alerts by `(rule_type, entity_key, severity)`
- Retry/backoff: SQL transient retry
- Logging:
  - each rule family
  - created vs deduped
  - top offenders
- Progress/status:
  - missing cost rules
  - missing link rules
  - shadow drift rules
  - stale tracking rules
  - billing completeness rules

Recommended rule families:

- `courier_missing_cost`
- `courier_missing_order_link`
- `courier_shadow_delta_spike`
- `courier_tracking_stale`
- `courier_delivered_without_pod`
- `courier_billing_manifest_gap`
- `courier_profit_fallback_usage`

### `courier_prepare_alert_digest` (recommended)

- Trigger: daily morning
- Input:
  - `date_from`, `date_to`
  - recipient group
- Output:
  - digest artifact / email payload / Slack payload
- Idempotency:
  - one digest per `(date, recipient_group, carrier)`
- Retry/backoff: notification retry only
- Logging:
  - digest totals and recipients
- Progress/status:
  - gather
  - render
  - dispatch

## Apply Jobs

These jobs intentionally write approved business decisions into canonical data.

### `courier_apply_link_overrides` (recommended)

- Trigger: on-demand after analyst approval
- Input:
  - list of approved overrides:
    - `carrier`
    - `shipment_number`
    - `amazon_order_id`
    - `link_method='manual_override'`
    - actor / reason
- Output:
  - links inserted/updated
  - old primary links demoted if required
- Idempotency:
  - immutable `override_id`
  - final state idempotent by `(shipment_id, amazon_order_id, link_method='manual_override')`
- Retry/backoff: SQL transient retry
- Logging:
  - each override id
  - previous vs new primary link
- Progress/status:
  - validate
  - apply
  - summarize

### `courier_apply_cost_adjustments` (recommended)

- Trigger: on-demand after finance approval
- Input:
  - shipment-level manual corrections / credits / debits
- Output:
  - adjustment rows in cost table or separate adjustment table
  - downstream recompute request
- Idempotency:
  - immutable `adjustment_id`
- Retry/backoff: SQL transient retry
- Logging:
  - invoice ref
  - actor
  - delta amount
- Progress/status:
  - validate
  - apply
  - mark recompute needed

### `courier_apply_delivered_truth` (recommended)

- Trigger: hourly or on-demand
- Input:
  - carrier
  - shipment ids or date window
- Output:
  - downstream order/ops cache updated from canonical delivered truth
- Idempotency:
  - final downstream state derived from canonical shipment state
- Retry/backoff: SQL transient retry
- Logging:
  - orders updated
  - skipped rows
- Progress/status:
  - target selection
  - downstream writes

## Recompute Jobs

### `courier_rebuild_fact_window` (recommended)

- Trigger: on-demand, after backfill or apply jobs
- Input:
  - `carrier`
  - `created_from`, `created_to`
  - `limit_orders`
- Output:
  - rebuilt fact rows
- Idempotency:
  - upsert by `(amazon_order_id, calc_version)`
- Retry/backoff: SQL transient retry
- Logging:
  - orders rebuilt
  - costless orders
- Progress/status:
  - select
  - aggregate
  - upsert

### `courier_rebuild_shadow_window` (recommended)

- Trigger: on-demand; nightly after aggregate
- Input:
  - `carrier`
  - `purchase_from`, `purchase_to`
  - `replace_existing`
- Output:
  - rebuilt shadow rows
  - status distribution
- Idempotency:
  - replace scoped shadow rows, then upsert
- Retry/backoff: SQL transient retry
- Logging:
  - status counts
  - top deltas
- Progress/status:
  - compare
  - write
  - summarize

### `calc_profit` (existing downstream recompute)

- Trigger: on-demand after courier recompute; scheduled for controlled windows
- Input:
  - `from_date`
  - `to_date`
  - optional marketplace filters
- Output:
  - recalculated profit rows
- Idempotency:
  - deterministic recalculation per order/date window
- Retry/backoff: SQL transient retry
- Logging:
  - orders recalculated
  - fallback usage
- Progress/status:
  - select orders
  - recompute
  - finalize

## Verification Jobs

### `dhl_shadow_logistics` (existing)

- Trigger: nightly after aggregate; on-demand
- Input:
  - `purchase_from`, `purchase_to`
  - `limit_orders`
  - `replace_all_existing`
- Output:
  - shadow rows
  - status distribution
- Idempotency:
  - replace or upsert by `(amazon_order_id, calc_version='dhl_v1')`
- Retry/backoff: SQL transient retry
- Logging:
  - match / delta / shadow_only / legacy_only
- Progress/status:
  - compare scope
  - write
  - summarize

### `gls_shadow_logistics` (existing)

- Trigger: nightly after aggregate; on-demand
- Input: same shape as DHL
- Output: same shape as DHL
- Idempotency: same pattern as DHL
- Retry/backoff: SQL transient retry
- Logging: same pattern as DHL
- Progress/status: same pattern as DHL

### `courier_verify_billing_completeness` (recommended)

- Trigger: daily after imports
- Input:
  - `carrier`
  - `billing_period`
- Output:
  - expected docs vs imported docs
  - doc totals vs line totals
  - missing file list
- Idempotency:
  - immutable audit row by `(carrier, billing_period, trigger_source)`
- Retry/backoff: SQL transient retry
- Logging:
  - counts
  - missing docs
  - total discrepancies
- Progress/status:
  - enumerate expected docs
  - compare staging
  - write audit

### `courier_verify_match_quality` (recommended)

- Trigger: daily after seed/cost
- Input:
  - `carrier`
  - `window_from`, `window_to`
- Output:
  - match coverage %
  - unlinked shipment sample
  - ambiguous link sample
- Idempotency:
  - immutable audit row by `(carrier, date_window, trigger_source)`
- Retry/backoff: SQL transient retry
- Logging:
  - linked/unlinked counts
  - top link methods
- Progress/status:
  - coverage calc
  - sample extraction
  - write audit

### `courier_verify_profit_fallback_usage` (recommended)

- Trigger: daily after `calc_profit`
- Input:
  - `from_date`, `to_date`
- Output:
  - orders still using legacy fallback
  - fallback percentage by carrier
- Idempotency:
  - immutable audit row by `(date_window, trigger_source)`
- Retry/backoff: SQL transient retry
- Logging:
  - fallback counts
  - top causes
- Progress/status:
  - scan
  - summarize
  - write audit

### `courier_verify_job_health` (recommended)

- Trigger: every 15 minutes
- Input:
  - none or optional carrier filter
- Output:
  - stuck jobs
  - heartbeat stale jobs
  - scheduler gap anomalies
- Idempotency:
  - alert dedupe by `(job_type, stale_window)`
- Retry/backoff: none beyond standard SQL retry
- Logging:
  - stale job ids
  - age in minutes
- Progress/status:
  - scan
  - alert write

## Recommended Scheduler

### Hourly

- `dhl_sync_tracking_events`
- `dhl_sync_pod`
- `courier_evaluate_alerts`
- `courier_verify_job_health`
- `courier_rollup_gap_snapshot`

### Daily Night

- `courier_dhl_nightly_pipeline`
- `courier_gls_nightly_pipeline`
- `courier_rollup_daily_kpis`
- `courier_verify_billing_completeness`
- `courier_verify_match_quality`

### On-demand

- `courier_recompute_window`
- `courier_apply_link_overrides`
- `courier_apply_cost_adjustments`
- `calc_profit`
- `courier_verify_profit_fallback_usage`

## Request/Response Boundary

Allowed synchronous endpoints:

- health checks
- lightweight diagnostics on a single shipment/order
- job enqueue
- job status read

Not allowed synchronous:

- file imports
- shipment seed
- tracking backfill
- POD fetch
- cost sync
- aggregate/shadow rebuild
- profit recompute
- alert scans

## Immediate Implementation Order

1. Decouple seed from import in production flows:
   - `dhl_seed_shipments_from_staging`
   - `gls_seed_shipments_from_staging`
2. Add missing operational jobs:
   - `dhl_sync_pod`
   - `courier_evaluate_alerts`
   - `courier_verify_billing_completeness`
3. Add parent orchestrators:
   - `courier_dhl_nightly_pipeline`
   - `courier_gls_nightly_pipeline`
   - `courier_recompute_window`
4. Add apply jobs for controlled manual corrections.
5. Add verification and KPI snapshot jobs before full production cutover.
