# Courier Recovery Report (2026-03-09)

## Scope

- Only courier module (`DHL` / `GLS`) and shipment-cost-to-order matching.
- Success criterion: reach coverage near `95%+` for accounting-relevant closed months, especially `2026-01` backward.
- Safety rule: no broad or risky work on Netfox; prefer ACC-only writes and read-only verification.

## Documentation Context Reviewed

This report was prepared against the current project documents:

- `docs/COURIER_JOB_ARCHITECTURE_2026-03-07.md`
- `docs/COURIER_MODULE_GROUND_TRUTH_2026-03-07.md`
- `docs/COURIER_PRODUCTION_CUTOVER_2026-03-07.md`
- `docs/COURIER_HANDOVER_RECOVERY_2026-03-08.md`

## Work Completed

### 1. Diagnostics added for link-gap analysis

Read-only diagnostics were added so we can separate:

- missing shipment -> order link,
- linked shipment without actual cost,
- estimated-only coverage,
- carrier/month-specific identifier buckets.

Implemented in:

- `apps/api/app/services/courier_link_diagnostics.py`
- `apps/api/app/api/v1/courier.py`

### 2. GLS link heuristic improved

GLS order-universe linking was extended to use shipment token buckets from:

- `tracking_number`
- `shipment_number`
- `piece_id`

instead of relying too heavily on a narrower shipment number path.

Implemented in:

- `apps/api/app/services/courier_order_universe_linking.py`

### 3. Safe actual-cost catch-up enabled for GLS

GLS cost sync got safe filters for targeted recovery:

- `billing_periods`
- `seeded_only`
- `only_primary_linked`

This allowed controlled actual-cost backfill without broad rewrite scope.

Implemented in:

- `apps/api/app/services/gls_cost_sync.py`
- `apps/api/app/api/v1/gls.py`
- `apps/api/app/connectors/mssql/mssql_store.py`

### 4. Actual-cost catch-up executed in monitored batches

Operationally completed:

- GLS actual-cost catch-up for `2025-12`, `2026-01`, `2026-02`
- DHL actual-only catch-up for `2025-12` to `2026-02`
- aggregate + shadow reruns where needed
- monthly KPI snapshot refresh per `month + carrier`

Important operational note:

- bulk monthly snapshot refresh timed out,
- per-scope refresh (`month + carrier`) succeeded and was kept as the safe path.

## Final Coverage Outcome

### Closed / accounting-relevant scope

- `2025-12 DHL`: `5847 / 5909 = 98.95%`
- `2025-12 GLS`: `10977 / 11080 = 99.07%`
- `2026-01 DHL`: `6183 / 6263 = 98.72%`
- `2026-01 GLS`: `9836 / 9938 = 98.97%`

Combined result for `2025-12 + 2026-01`:

- `32843 / 33190 = 98.95%` actual-cost coverage on order level

This meets the business target for the closed scope.

### February status

- `2026-02 DHL`: `3077 / 3810 = 80.76%`
- `2026-02 GLS`: `2787 / 7644 = 36.46%`

After cost catch-up:

- `linked_but_no_cost = 0`
- remaining gap is dominated by `missing_link`

So February is no longer a courier-invoice-cost problem. It is a shipment -> order linking problem.

Open-month interpretation was additionally hardened in code:

- `GET /api/v1/courier/monthly-kpis` now derives an `operational` block from the snapshot row,
- open months are no longer only `PENDING`; they are classified as:
  - `OPEN_AWAITING_INVOICES`
  - `OPEN_LINKED_NO_COST`
  - `OPEN_COST_PENDING`
  - `OPEN_LINK_GAP`
  - `OPEN_MIXED`

Latest safe read-only snapshot interpretation on `2026-03-09`:

- `2026-02 DHL`: `OPEN_LINK_GAP`
  - `missing_primary_link = 733`
  - `estimated_only = 0`
  - `linked_no_cost = 0`
  - `billing_link_coverage_pct = 79.93`
- `2026-02 GLS`: `OPEN_LINK_GAP`
  - `missing_primary_link = 4857`
  - `estimated_only = 0`
  - `linked_no_cost = 0`
  - `billing_link_coverage_pct = 99.82`

This means February should currently be treated as a link backlog, not as an "invoice still missing" backlog.

## Snapshot State

`dbo.acc_courier_monthly_kpi_snapshot` currently contains rows for:

- `2025-12 DHL`
- `2025-12 GLS`
- `2026-01 DHL`
- `2026-01 GLS`
- `2026-02 DHL`
- `2026-02 GLS`

Latest known purchase actual-cost coverage in the snapshot:

- `2025-12 DHL`: `98.95`
- `2025-12 GLS`: `99.07`
- `2026-01 DHL`: `98.72`
- `2026-01 GLS`: `98.97`
- `2026-02 DHL`: `80.76`
- `2026-02 GLS`: `36.46`

Readiness labels still show `NO_GO` / `PENDING` because current readiness rules depend on:

- `buffer_days = 45`
- `orders_missing_actual_cost = 0`

This is stricter than the business rule used for the courier recovery target.

## February Diagnostic Conclusion

Observed blockers after relinking attempts:

- `2026-02 DHL`: gap remains mostly `missing_link`
- `2026-02 GLS`: large group of unlinked shipments carries numeric `note1`, but these values are not currently mapped into the order universe

Current interpretation:

- further February work should focus on link buckets and source identifiers,
- not on another wide courier-cost backfill.

## Latest February Link-Gap Breakdown

Safe read-only `GET /api/v1/courier/link-gap-summary` was extended with
`unlinked_identifier_patterns` so February can be reviewed without running the heavier sample-based diagnostics.

### 2026-02 DHL

- `orders_universe = 3810`
- `shipments_in_scope = 11178`
- `shipments_without_primary_link = 2258` (`20.2%`)
- `shipments_unlinked_with_actual_cost = 2258`
- `source_system = dhl_billing_files` for the full unlinked set

Identifier-pattern split:

- `numeric_core_token = 1544`
- `jjd_like_core_token = 709`
- `non_numeric_core_token = 5`

Operational conclusion:

- DHL February is not blocked by missing invoice-cost rows.
- The unresolved backlog is now mostly order-identity coverage on the ACC side.
- Even a perfect JJD fallback would only address part of the backlog (`709 / 2258` shipments), so this is not a full DHL fix by itself.

Small Netfox micro-probe executed afterward:

- sample: `TOP 20` unresolved DHL `JJD` tokens from ACC February scope
- Netfox hits: `1 / 20`
- ACC order matches from returned Netfox `external_order_id`: `0 / 20`

Interpretation:

- the current Netfox JJD route does not yet justify a broader rollout into the order-universe linker,
- the bottleneck is not only "missing JJD lookup", but also the downstream identity mapping that would need to land back in ACC.

### 2026-02 GLS

- `orders_universe = 7644`
- `shipments_in_scope = 9519`
- `shipments_without_primary_link = 3143` (`33.02%`)
- `shipments_unlinked_with_actual_cost = 18`
- `source_system = gls_billing_files` for the full unlinked set

Identifier-pattern split:

- `numeric_core_token = 1606`
- `gls_note1_numeric_unmapped = 1537`

Operational conclusion:

- GLS February is also not mainly an invoice-cost gap anymore.
- The unresolved backlog is split almost exactly into:
  - numeric shipment identifiers that still do not resolve into the current order universe,
  - numeric `note1` BL-order style values that are absent from current ACC cache resolution.
- This points to source-order identity completeness, not another courier-cost sync pass.

## February Next Safe Step

- DHL: investigate recovery paths separately for `jjd_like_core_token` and `numeric_core_token`; do not treat them as one problem.
- GLS: prioritize BL/distribution cache completeness and order-universe token coverage; another linker tweak alone is unlikely to unlock the `gls_note1_numeric_unmapped` block.
- For both carriers: keep using the light summary path first; avoid broad reruns of sample-heavy diagnostics on ACC unless a very narrow scope is needed.

## Source Completeness Follow-Up

Code was extended with a new summary-only diagnostic path:

- `GET /api/v1/courier/identifier-source-gaps`

Implemented in:

- `apps/api/app/services/courier_link_diagnostics.py`
- `apps/api/app/api/v1/courier.py`

The purpose of this path is to separate:

- GLS `note1` values missing from ACC BL/distribution caches,
- GLS tracking values present in `acc_gls_bl_map` but not resolvable back to `acc_order`,
- DHL `JJD` values present in `acc_dhl_parcel_map`,
- DHL numeric parcel-base values missing from order-side package caches.

Operational note:

- full live execution of this new summary on ACC for `2026-02` was still too heavy and timed out,
- no repeated retries were made against the shared production-like DB,
- for now it should be treated as a code-level tool ready for narrower scopes and further optimization.

Repo-level source-completeness finding:

- there is no narrow in-repo writer for `dbo.acc_cache_packages`,
- the only visible refresh path is the broad Netfox script `apps/api/cache_courier_tables.py`, which does a full `TRUNCATE TABLE` + reload for `acc_cache_packages`.

This means the next engineering move should be:

- build a targeted cache-backfill path for missing courier identifiers,
- not run the full cache refresh blindly on shared ACC.

Follow-up implemented on `2026-03-09`:

- targeted identifier-source backfill now exists in `apps/api/app/services/courier_identifier_backfill.py`,
- it runs in small Netfox `IN (...)` batches and writes only idempotent upserts into ACC cache tables,
- it supports four narrow modes:
  - `gls_note1`
  - `gls_tracking_map`
  - `dhl_numeric`
  - `dhl_jjd`
- it is exposed operationally as:
  - `POST /api/v1/courier/jobs/backfill-identifier-sources`
  - job type `courier_backfill_identifier_sources`

Additional linker hardening implemented in the same follow-up:

- courier order-universe linking and diagnostics now resolve `acc_cache_packages` through `acc_cache_dis_map`, so DIS-side package rows are visible to the matcher,
- DHL order-universe linking now adds `JJD -> acc_dhl_parcel_map -> parcel_number_base` fallback tokens before candidate matching,
- courier readiness / coverage SQL now uses the same DIS-aware package resolution path, so KPI denominator and linker behavior stay aligned.

## Safety / Monitoring Notes

- Netfox was not touched in this run.
- ACC writes were done in targeted batches only.
- Post-write verification was read-only.
- No destructive SQL was used.
- On `2026-03-09`, one direct refresh attempt for `2026-02` from the current code timed out in `_shipment_month_coverage`.
- No repeated write retries were made after that timeout; verification fell back to safe read-only snapshot reads.

## Validation

Courier workstream tests executed:

- `apps/api/tests/test_gls_phase3.py`
- `apps/api/tests/test_api_gls.py`
- `apps/api/tests/test_courier_link_diagnostics.py`
- `apps/api/tests/test_courier_identifier_backfill.py`
- `apps/api/tests/test_courier_order_universe_linking.py`
- `apps/api/tests/test_courier_order_universe_pipeline.py`
- `apps/api/tests/test_courier_readiness.py`
- `apps/api/tests/test_courier_monthly_kpi.py`
- `apps/api/tests/test_api_courier.py`

Result:

- `30 passed`

## ACC-Wide Propagation Hardening

After the courier-domain recovery, ACC still had downstream paths that either bypassed the
canonical courier fact or zeroed it out. Those paths were hardened in code on `2026-03-09`.

Implemented in:

- `apps/api/app/services/profitability_service.py`
  - profitability overview/orders now read canonical order logistics via the shared resolver,
  - `recompute_rollups()` now rebuilds `logistics_pln` from canonical order logistics instead of preserving stale target values.
- `apps/api/app/connectors/mssql/mssql_store.py`
  - `sync_profit_snapshot()` now writes canonical `transport`,
  - snapshot revenue now includes customer-paid shipping revenue allocation instead of product-only revenue.
- `apps/api/app/services/guardrails.py`
  - shipping-cost guardrail now reads `dbo.acc_courier_monthly_kpi_snapshot` for closed-month coverage instead of obsolete shipment-schema fields.
- `apps/api/app/services/profit_engine.py`
  - shipping revenue is now resolved per order from finance `ShippingCharge + ShippingTax`,
  - `orders_with_shipping_charge` is no longer hardcoded to `0`,
  - legacy marketplace shipping pool overwrite was disabled.
- `_audit_gate.py`
  - revenue audit now validates line revenue plus customer shipping revenue.

Validation executed after these changes:

- `apps/api/tests/test_order_logistics_source.py`
- `apps/api/tests/test_profitability_logistics_enrichment.py`
- `apps/api/tests/test_p1_financial_fixes.py`
- `apps/api/tests/test_guardrails.py`
- `apps/api/tests/test_courier_cost_propagation.py`

Result:

- `100 passed`

## Production Rollout Completed

### Legacy batch path neutralized

The last dangerous batch entrypoint was still `app.services.profit_service.recalculate_profit_batch()`,
used by the legacy Celery `calc_profit` task. That path no longer computes profit from its old V1 logic.

It now delegates to the canonical V2 pipeline:

- `recalc_profit_orders()`
- `sync_profit_snapshot()`
- alert refresh from the same canonical source

This prevents background jobs from reintroducing:

- gross `order_total * fx` revenue semantics,
- stale `logistics_pln` sourcing,
- historical VAT-inflated COGS fallback behavior.

### ACC canary write completed safely

Canary scope:

- `2026-01-01` to `2026-01-07`

Execution:

- `recalc_profit_orders`: `10879` orders in `9.1s`
- `sync_profit_snapshot`: `11612` rows in `6.7s`
- `recompute_rollups`: `7149` SKU rows / `63` marketplace rows in `30.8s`

Observed outcome:

- snapshot was empty before the canary and became populated,
- rollup and snapshot logistics aligned to canonical order-level resolved logistics,
- no deadlock, timeout, or row explosion occurred.

Parity after canary:

- allocated order logistics: `282545.14 PLN`
- rollup logistics: `282545.48 PLN`
- snapshot transport: `282542.81 PLN`

### Closed-month rollout completed

Executed sequentially, month by month, on ACC:

- `2025-12`
- `2026-01`

For each month:

- `recalc_profit_orders()`
- `sync_profit_snapshot()`
- `recompute_rollups()`

Closed-month parity after rollout:

- `2025-12`
  - allocated order logistics: `585352.74 PLN`
  - rollup logistics: `585352.54 PLN`
  - snapshot transport: `585349.61 PLN`
- `2026-01`
  - allocated order logistics: `963930.18 PLN`
  - rollup logistics: `963929.90 PLN`
  - snapshot transport: `963925.37 PLN`

The remaining deltas are only rounding/allocation noise.

### Downstream cache/materialization refresh completed

Operational refreshes executed after the closed-month rollout:

- executive
  - `recompute_executive_metrics(days_back=100)` -> `723` rows
  - `compute_health_score()` -> overall `77.2`
  - `detect_risks(days_back=30)` -> `318` active risk entries
- seasonality
  - `build_monthly_metrics(months_back=36)` -> `36195` SKU rows, `1344` category rows
  - `recompute_indices()` -> `38200` rows
  - `recompute_profiles()` -> `9948` entities
  - `detect_seasonality_opportunities()` -> `201` opportunities
- strategy
  - `run_strategy_detection(days_back=30)` -> `760` opportunities

### Running API verified against updated code

The live backend listening on `127.0.0.1:8000` was checked after rollout.

Verification:

- local function call: `get_profitability_overview(2026-01-01..2026-01-07)`
- live HTTP call: `GET /api/v1/profitability/overview?from=2026-01-01&to=2026-01-07`

The KPI values matched exactly for:

- `total_revenue_pln`
- `total_profit_pln`
- `total_orders`
- `total_units`
- `total_ad_spend_pln`
- `total_margin_pct`
- `return_rate_pct`

This confirms the running API process is serving the corrected courier-cost path.

### Final ACC status

For the closed-month scope that matters to accounting:

- courier actual-cost coverage remains near `99%`,
- order-level logistics, profitability rollups, and profit snapshot are aligned,
- executive/seasonality/strategy materializations were refreshed from corrected rollups,
- the running API serves the updated logic.

Remaining future work is not a closed-month integrity blocker:

- February shipment -> order linking gaps,
- explicit estimation policy for orders not yet invoiced by the carrier.

## Recommended Next Step

If work continues on February:

1. Start from `GET /api/v1/courier/monthly-kpis?months=2026-02&carriers=DHL&carriers=GLS` and use the derived `operational` block as the first triage layer.
2. Break missing shipments into identifier buckets per carrier.
3. Validate only the missing identifier classes against source systems.
4. Keep Netfox strictly read-only and only if ACC-side evidence is insufficient.
