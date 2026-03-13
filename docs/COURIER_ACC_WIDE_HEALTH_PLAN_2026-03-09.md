# ACC Courier Cost Health Plan (2026-03-09)

## Objective

Bring all ACC business modules to one courier-cost truth without unsafe production behavior.

Success means:

- closed months keep courier actual-cost coverage at business-acceptable levels,
- every downstream module reads courier cost from the correct canonical source,
- no module mixes courier cost with customer shipping revenue or simulator inputs,
- historical rollups and derived analytics are rebuilt from the corrected source.

Out of scope for this phase:

- estimating courier cost for shipments not yet invoiced by the carrier.

## Current State

### What is already healthy

- Courier-domain recovery is complete for the closed accounting-relevant scope:
  - `2025-12 DHL`: `98.95%`
  - `2025-12 GLS`: `99.07%`
  - `2026-01 DHL`: `98.72%`
  - `2026-01 GLS`: `98.97%`
- `dbo.acc_courier_monthly_kpi_snapshot` exists and has validated rows for `2025-12`, `2026-01`, `2026-02`.
- February is now mostly a `missing_link` problem, not a `linked_but_no_cost` problem.

### What is still unhealthy ACC-wide

- Canonical order-level courier cost exists in `dbo.acc_order_logistics_fact`, but not all downstream consumers use it consistently.
- `apps/api/app/services/profitability_service.py` still uses legacy `acc_order.logistics_pln` in overview and order queries.
- `apps/api/app/services/profitability_service.py` recomputes rollups from a source query that seeds `0 as logistics_pln` and preserves target logistics on update, which prevents a true historical rebuild from courier facts.
- `apps/api/app/services/guardrails.py` still checks shipping gaps against obsolete `acc_shipment.order_reference`, `acc_shipment.cost_total`, and `acc_order.order_status` semantics.
- The common logistics resolver is feature-flagged via `PROFIT_USE_LOGISTICS_FACT`; code default is `False`, while audit artifacts show it has been `True` in at least one runtime. The flag state must be treated as deployment-controlled, not assumed.

## Canonical Contract

### 1. Shipment-level courier cost

Primary tables:

- `dbo.acc_shipment_cost`
- `dbo.acc_courier_cost_estimate`

Meaning:

- carrier-side shipment cost,
- actual when invoiced,
- estimated only as a pre-invoice fallback.

### 2. Order-level resolved logistics cost

Primary table:

- `dbo.acc_order_logistics_fact`

Meaning:

- canonical courier logistics cost aggregated from shipment level to Amazon order level,
- the only business-valid source for downstream `logistics_pln`.

### 3. Monthly courier readiness / KPI snapshot

Primary table:

- `dbo.acc_courier_monthly_kpi_snapshot`

Meaning:

- stable month+carrier KPI materialization for coverage, readiness, and business monitoring,
- the correct source for closed-month courier coverage reporting.

### 4. Legacy compatibility field

Legacy field:

- `dbo.acc_order.logistics_pln`

Meaning:

- historical compatibility fallback only,
- not acceptable as the canonical downstream source after cutover.

Allowed post-cutover usage:

- temporary fallback during dual-read,
- diagnostic comparison in shadow/parity tooling.

### 5. Non-courier fields that must stay separate

- `shipping_charge_pln`: shipping revenue collected from the buyer; not courier cost.
- `shipping_cost` in simulator: manual what-if input; not courier data.
- `shipping_costs` in tax/finance clearing: Amazon finance shipping adjustments; not DHL/GLS invoice cost.

## Consumer Map

### Green: already on the right path

- Courier operational monitoring:
  - `apps/api/app/services/courier_readiness.py`
  - `apps/api/app/services/courier_link_diagnostics.py`
  - `apps/api/app/services/courier_monthly_kpi.py`
  - `apps/api/app/services/courier_alerts.py`
- Resolver-based business consumers:
  - `apps/api/app/api/v1/kpi.py`
  - `apps/api/app/api/v1/profit.py`
  - `apps/api/app/services/profit_engine.py`
  - `apps/api/app/services/profit_service.py`
  - `apps/api/app/connectors/mssql/mssql_store.py`

### Amber: structurally correct but deployment-sensitive

- `apps/api/app/services/order_logistics_source.py`

Risk:

- behavior depends on `PROFIT_USE_LOGISTICS_FACT`,
- default code config is not enough evidence of runtime state.

### Red: must be migrated or rebuilt

- `apps/api/app/services/profitability_service.py`
  - overview and orders still read legacy `o.logistics_pln`,
  - rollup recompute does not rebuild logistics from canonical fact.
- `apps/api/app/services/guardrails.py`
  - shipping cost guardrail is based on obsolete schema semantics.

### Downstreams blocked on rollup correctness

These modules depend on `dbo.acc_sku_profitability_rollup`, so they are only trustworthy after the rollup foundation is fixed and recomputed:

- `apps/api/app/services/executive_service.py`
- `apps/api/app/services/strategy_service.py`
- `apps/api/app/services/seasonality_service.py`
- `apps/api/app/services/decision_intelligence_service.py`

## Safety Rules

1. No Netfox writes. Netfox is read-only and only for targeted identifier validation if ACC evidence is insufficient.
2. No big-bang recompute first. Start with read-only parity, then canary windows, then closed months, then broader history.
3. No parallel heavy jobs across courier linking, profitability recompute, and downstream recomputes.
4. Every write phase needs before/after snapshots captured from ACC tables before the next phase starts.
5. Use bounded scopes only:
   - first `7-14` day canary,
   - then `2025-12` and `2026-01`,
   - only then wider historical ranges.
6. Keep lock and timeout discipline:
   - explicit query timeouts,
   - `SET LOCK_TIMEOUT 30000` for heavier writes,
   - abort on repeated lock timeouts or degraded p95.
7. Prefer additive/read-only hardening first:
   - views,
   - parity reports,
   - diagnostics,
   - feature-flagged reads.
8. No destructive SQL in shared production tables as part of cutover.

## Main Risks and Mitigations

### R1. Semantic mix-up risk

Problem:

- courier cost, shipping revenue, and finance shipping adjustments are easy to confuse.

Mitigation:

- freeze field semantics in docs and API contracts before further migration,
- audit labels and query names where `shipping_*` appears,
- block any new consumer from reading ambiguous fields without explicit meaning.

### R2. False consistency risk

Problem:

- some modules already use the resolver path, but if the feature flag is off they silently fall back to legacy values.

Mitigation:

- treat `PROFIT_USE_LOGISTICS_FACT` as an explicit rollout switch,
- run dual-read parity on canary windows before and after flag changes,
- log source mode in parity and cutover artifacts.

### R3. Historical rollup contamination risk

Problem:

- `acc_sku_profitability_rollup` can keep stale logistics because recompute preserves target logistics instead of rebuilding from the canonical fact.

Mitigation:

- fix recompute logic before any broad historical rebuild,
- canary recompute on small windows,
- validate rollup logistics totals against order-level canonical totals.

### R4. Production SQL pressure risk

Problem:

- profitability and downstream recomputes can be heavy and may affect API latency or lock contention.

Mitigation:

- run one heavy operation at a time,
- watch API p95 and DB timeout signals,
- execute closed-month rebuilds in low-traffic windows,
- stop on runtime regression instead of forcing completion.

### R5. Monitoring drift risk

Problem:

- current guardrail logic can report the wrong problem because it is built on obsolete courier schema assumptions.

Mitigation:

- replace it with guardrails based on `acc_courier_monthly_kpi_snapshot` or current link/cost tables,
- require operational monitoring to agree with courier readiness before sign-off.

## Recommended Migration Waves

### Wave 0. Freeze the contract and inventory consumers

Goal:

- finish the source-of-truth map before changing behavior.

Actions:

- confirm the canonical contract in this document and `docs/SHIPPING_COST_USAGE_REVIEW_2026-03-09.md`,
- inventory every business consumer of `logistics_pln` and classify it as canonical, fallback, or obsolete,
- capture current runtime state of `PROFIT_USE_LOGISTICS_FACT`.

Optional hardening:

- add a read-only deduplicated SQL view such as `dbo.acc_order_logistics_current_v` that exposes the latest `total_logistics_pln` per `amazon_order_id`, so raw SQL consumers stop re-implementing `TOP 1 ... ORDER BY calculated_at DESC`.

Gate:

- no writes,
- complete consumer map,
- complete parity inventory for `2025-12` and `2026-01`.

### Wave 1. Migrate direct business consumers

Goal:

- remove direct business dependency on `acc_order.logistics_pln`.

Actions:

- migrate `apps/api/app/services/profitability_service.py` overview and orders queries to the same canonical resolver semantics already used by KPI / Profit V1 / Profit V2,
- keep dual-read logging or parity checks during rollout.

Gate:

- on the same filters, profitability overview/orders logistics totals match canonical order logistics within agreed tolerance,
- no unexplained delta between profitability pages and Profit V2 / KPI for the same date range.

### Wave 2. Fix rollup recompute foundation

Goal:

- make `recompute_rollups()` rebuild logistics from canonical order logistics instead of preserving stale target values.

Actions:

- change the source query so `logistics_pln` comes from canonical order logistics allocation,
- remove the "preserve target logistics" behavior in matched updates,
- keep enrichment idempotent and verify no duplicate allocation occurs.

Gate:

- canary recompute for a small window succeeds,
- rollup `SUM(logistics_pln)` matches canonical order-logistics allocation for the same scope,
- revenue and non-courier metrics stay stable except for expected profit/logistics deltas.

### Wave 3. Recompute closed months and dependent downstreams

Goal:

- propagate corrected courier cost into all rollup-based analytics.

Actions:

- rebuild `acc_sku_profitability_rollup` for `2025-12` and `2026-01`,
- rebuild `acc_marketplace_profitability_rollup`,
- rerun dependent pipelines:
  - executive,
  - strategy,
  - seasonality,
  - decision intelligence.

Gate:

- downstream modules no longer diverge from base profitability rollups,
- observed deltas are limited to logistics/profit-derived measures and are documented.

### Wave 4. Replace obsolete guardrails with current-model monitoring

Goal:

- operational monitoring must measure the same truth the business modules use.

Actions:

- replace `check_shipping_cost_gaps()` with a guardrail based on:
  - `dbo.acc_courier_monthly_kpi_snapshot` for closed-month monitoring, and/or
  - current `acc_shipment_order_link` + `acc_shipment_cost` semantics for recent operational gaps,
- keep severity thresholds aligned with business meaning, not old schema assumptions.

Gate:

- guardrail results agree with courier readiness and monthly KPI snapshot,
- no stale-schema shipping alert remains in the active health checks.

### Wave 5. Cut over and deprecate legacy reads

Goal:

- complete the ACC-wide cutover to canonical courier cost.

Actions:

- make the canonical resolver path the enforced default for business modules,
- leave `acc_order.logistics_pln` only as a compatibility/shadow field until explicit retirement,
- document the remaining allowed legacy reads, if any.

Gate:

- no business-critical module reads legacy `acc_order.logistics_pln` directly,
- all remaining direct legacy usage is diagnostic only.

### Wave 6. Frontend and reporting semantics cleanup

Goal:

- prevent user-facing confusion after backend truth is fixed.

Actions:

- ensure UI labels distinguish:
  - courier logistics cost,
  - shipping revenue from customer,
  - manual scenario shipping input,
- align dashboards and exports with the canonical meaning.

Gate:

- no UI field or exported column presents shipping revenue as courier cost.

## Validation Matrix

### A. Closed-month courier health

Validate from `dbo.acc_courier_monthly_kpi_snapshot`:

- `purchase_actual_cost_coverage_pct >= 95` for closed months,
- any shortfall is explainable as missing carrier invoice, not missing ACC linkage or stale rollup logic.

### B. Order-level parity

For the same scoped months:

- compare canonical order logistics totals against module outputs for:
  - KPI,
  - Profit V1,
  - Profit V2,
  - Profitability overview/orders.

Acceptance:

- no unexplained delta beyond rounding/allocation tolerance.

### C. Rollup parity

Validate:

- `acc_sku_profitability_rollup.logistics_pln`,
- `acc_marketplace_profitability_rollup.logistics_pln`,
- canonical order-level allocated logistics.

Acceptance:

- totals align by period and marketplace after recompute.

### D. Downstream parity

Validate:

- executive totals,
- strategy opportunity baselines,
- seasonality monthly metrics,
- decision intelligence baselines.

Acceptance:

- after recompute, downstream values reconcile to corrected rollups.

### E. Semantic hygiene

Validate by code review and endpoint output:

- `shipping_charge_pln` is only revenue,
- simulator `shipping_cost` is only what-if input,
- finance `shipping_costs` is never treated as carrier invoice cost.

### F. Runtime safety

Validate during every write wave:

- API health stays stable,
- p95 does not regress materially,
- no repeated lock-timeout pattern,
- no unexpected rowcount explosion.

## Rollback Strategy

Rollback triggers:

- major API latency regression,
- repeated lock timeouts,
- canary parity failure,
- recompute output that diverges from canonical order logistics,
- guardrail/readiness contradiction after change.

Rollback actions:

- stop the current heavy job,
- keep the last validated snapshot and parity artifact,
- revert the feature flag to the previous read mode if a flag was changed,
- postpone broader recompute until the failing canary scope is explained.

## Definition of Done

The ACC courier-cost workstream is complete only when all of the following are true:

1. Closed months from `2026-01` backward are business-healthy for courier actual-cost coverage, or any remaining gap is explicitly proven to be missing carrier invoice rather than ACC-side linkage or sourcing error.
2. `profitability_service.py` no longer uses legacy order logistics for business calculations.
3. `recompute_rollups()` rebuilds logistics from canonical courier facts.
4. `acc_sku_profitability_rollup` and all downstream analytics are recomputed from the corrected source.
5. Guardrails monitor the current courier model, not obsolete schema shapes.
6. No business module confuses courier cost with customer shipping revenue or simulator input.
7. `acc_order.logistics_pln` is no longer a direct business source, only a temporary compatibility/shadow field if still retained.

## Immediate Next Implementation Step

The first implementation wave should be narrow and safe:

1. migrate `profitability_service.py` overview/orders to canonical logistics resolution,
2. fix `recompute_rollups()` so logistics is sourced from canonical order logistics,
3. run a canary recompute on a small date window,
4. validate parity before touching broader history.

This is the smallest change set that unlocks the rest of ACC without forcing risky wide-scope database operations.

## Status Update

Completed on `2026-03-09`:

- `profitability_service.py` overview/orders switched to canonical logistics resolution,
- `recompute_rollups()` now rebuilds `logistics_pln` from canonical order logistics,
- `sync_profit_snapshot()` now writes canonical `transport` and includes customer shipping revenue allocation,
- `guardrails.py` shipping-cost check now reads `acc_courier_monthly_kpi_snapshot`,
- `profit_engine.py` shipping revenue paths now use net `ShippingCharge + ShippingTax` semantics and real order coverage counts,
- `_audit_gate.py` revenue audit now validates customer shipping revenue as part of revenue,
- legacy `profit_service.recalculate_profit_batch()` was neutralized and now delegates to V2 so the old Celery `calc_profit` path cannot reintroduce stale courier semantics.

Operational rollout completed on ACC:

- bounded canary recompute on `2026-01-01..2026-01-07`,
- closed-month rollout for `2025-12` and `2026-01`,
- executive cache refresh,
- seasonality chain rebuild,
- strategy detection refresh.

Post-rollout validation:

- `2025-12` allocated order logistics vs rollup vs snapshot:
  - `585352.74 / 585352.54 / 585349.61 PLN`
- `2026-01` allocated order logistics vs rollup vs snapshot:
  - `963930.18 / 963929.90 / 963925.37 PLN`
- live HTTP `GET /api/v1/profitability/overview` matched local post-fix logic on `2026-01-01..2026-01-07`, confirming the running API serves the corrected path.

Current conclusion:

- the closed-month ACC courier-cost path is production-complete,
- remaining work is limited to February link-gap improvement and any future estimation policy for not-yet-invoiced shipments,
- those items are operational follow-up, not blockers for closed-month accounting integrity.
