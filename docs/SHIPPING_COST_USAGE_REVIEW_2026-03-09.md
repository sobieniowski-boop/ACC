# Shipping Cost Usage Review (2026-03-09)

## Purpose

This review maps where the project uses "shipping cost" or related fields, because the codebase currently mixes several different meanings:

- courier invoice cost,
- order logistics cost,
- shipping charge collected from the customer,
- manual simulation shipping cost,
- Amazon finance shipping adjustments.

These are not interchangeable.

## Canonical Meanings

### 1. Courier actual / estimated shipment cost

Source tables:

- `dbo.acc_shipment_cost`
- `dbo.acc_courier_cost_estimate`

Meaning:

- carrier-side shipment cost,
- actual when `is_estimated = 0`,
- estimated pre-invoice fallback when `is_estimated = 1`.

Primary writers:

- `apps/api/app/services/dhl_cost_sync.py`
- `apps/api/app/services/gls_cost_sync.py`
- `apps/api/app/services/courier_cost_estimation.py`

### 2. Order logistics cost

Source tables:

- `dbo.acc_order_logistics_fact`
- `dbo.acc_order_logistics_shadow`

Meaning:

- courier cost aggregated from shipment level to Amazon order level,
- canonical downstream representation of courier logistics cost.

Primary writers:

- `apps/api/app/services/dhl_logistics_aggregation.py`
- `apps/api/app/services/gls_logistics_aggregation.py`

### 3. Legacy order-level logistics field

Source field:

- `dbo.acc_order.logistics_pln`

Meaning:

- old per-order logistics field from the legacy mapping path,
- now only a fallback / legacy compatibility field,
- not the canonical source of truth for the courier module anymore.

Legacy path still present in:

- `apps/api/app/services/order_pipeline.py`

but the step is explicitly removed and returns early.

### 4. Shipping charge collected from the buyer

Relevant field names:

- `shipping_charge_pln`
- `estimated_shipping_charge_pln`

Meaning:

- revenue-side shipping charge from Amazon finance events (`ShippingCharge`),
- not courier cost.

Primary usage:

- `apps/api/app/services/profit_engine.py`
- `apps/api/app/schemas/profit_v2.py`
- `apps/web/src/pages/ProductProfitTable.tsx`

### 5. Manual simulator shipping cost

Relevant field name:

- `shipping_cost`

Meaning:

- user-entered scenario cost in the Price Simulator,
- not read from courier module tables.

Primary usage:

- `apps/api/app/services/profitability_service.py`
- `apps/api/app/schemas/profitability.py`
- `apps/web/src/pages/PriceSimulator.tsx`

### 6. Amazon finance shipping adjustments

Relevant field name:

- `shipping_costs`

Meaning:

- Amazon-side finance charges such as `ShippingHB`, `ShippingChargeback`, return-postage charges,
- not carrier invoice cost from DHL/GLS.

Primary usage:

- `apps/api/app/services/tax_compliance/amazon_clearing.py`

## Current Consumer Map

### Canonical courier/logistics consumption

These paths already consume the canonical order logistics fact, with fallback to legacy `acc_order.logistics_pln`:

- `apps/api/app/services/order_logistics_source.py`
- `apps/api/app/api/v1/kpi.py`
- `apps/api/app/api/v1/profit.py`
- `apps/api/app/services/profit_engine.py`
- `apps/api/app/services/profit_service.py`

This is the correct integration direction for downstream analytics.

### Courier readiness / operational monitoring

These paths read directly from shipment/link/cost tables and correctly stay close to the courier domain:

- `apps/api/app/services/courier_readiness.py`
- `apps/api/app/services/courier_link_diagnostics.py`
- `apps/api/app/services/courier_alerts.py`
- `apps/api/app/services/courier_monthly_kpi.py`

### Legacy or semantically mixed consumption

These areas still rely on legacy `acc_order.logistics_pln` or on a stale meaning of shipping cost:

- `apps/api/app/services/profitability_service.py`
- `apps/api/app/services/guardrails.py`

## Review Findings

### F1. High: Profitability module still bypasses canonical courier cost

`apps/api/app/services/profitability_service.py` still computes overview and orders from `o.logistics_pln` instead of the canonical order-logistics fact.

Impact:

- profitability overview/orders can diverge from KPI, Profit V1, and Profit V2,
- courier recovery to ~99% for closed months does not automatically propagate into those screens,
- users can see different logistics cost depending on which page they open.

Evidence:

- overview loss formula uses legacy logistics: `apps/api/app/services/profitability_service.py`
- orders filter/formula uses legacy logistics: `apps/api/app/services/profitability_service.py`
- orders select returns `ISNULL(o.logistics_pln, 0)`: `apps/api/app/services/profitability_service.py`

### F2. High: Rollup recompute preserves stale logistics instead of rebuilding from canonical fact

`recompute_rollups()` seeds `0 as logistics_pln` in the source query and then preserves `tgt.logistics_pln` on update.

Impact:

- new rows can enter with zero logistics,
- existing rows keep whatever logistics was already there,
- the profitability rollups do not truly rehydrate from the recovered courier data.

This is the main downstream integrity gap after the courier recovery.

Evidence:

- source query seeds `0 as logistics_pln`: `apps/api/app/services/profitability_service.py`
- matched update keeps `logistics_pln = ISNULL(tgt.logistics_pln, 0)`: `apps/api/app/services/profitability_service.py`

### F3. High: Shipping-cost guardrail still points at obsolete schema semantics

`check_shipping_cost_gaps()` uses `_SHIPPING_COST_GAPS_SQL` that joins:

- `acc_shipment.order_reference`
- `acc_shipment.cost_total`
- `acc_order.order_status`

These fields belong to an older data shape and are not aligned with the current courier model based on:

- `acc_shipment_order_link`
- `acc_shipment_cost`
- `acc_order.status`

Impact:

- guardrail can fail silently, misreport, or measure the wrong thing,
- operational monitoring can contradict the courier readiness snapshot.

Evidence:

- `apps/api/app/services/guardrails.py`

## Non-Bug but Important Semantic Notes

### Shipping charge in Profit V2 is revenue, not cost

`shipping_charge_pln` in `profit_engine.py` comes from finance `ShippingCharge` and is added to revenue for FBM analysis.

This is correct, but easy to misread in UI or ad-hoc analysis if treated as courier cost.

### Price Simulator shipping cost is manual

`shipping_cost` in the profitability simulator is not connected to courier data.

This is acceptable for a scenario tool, but it should not be used as evidence of real logistics coverage.

### Amazon clearing shipping bucket is not DHL/GLS invoice cost

`shipping_costs` in tax/finance reconciliation groups Amazon finance events, not carrier invoices.

This bucket should not be compared one-to-one with `acc_shipment_cost`.

## Recommended Next Actions

1. Migrate `profitability_service.py` to the same canonical logistics resolver used by KPI, Profit V1, and Profit V2.
2. Change `recompute_rollups()` so `logistics_pln` is rebuilt from canonical order logistics, not preserved from target rows.
3. Replace `check_shipping_cost_gaps()` with a guardrail built on `acc_shipment_order_link`, `acc_shipment_cost`, or directly on the monthly courier snapshot.
4. Keep UI and docs explicit about the difference between:
   - `logistics_pln` = courier/order logistics cost,
   - `shipping_charge_pln` = revenue from customer shipping charge,
   - `shipping_cost` in simulator = manual what-if input.
