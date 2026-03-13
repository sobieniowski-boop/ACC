# Courier GLS January Recovery - 2026-03-10

## Scope

- Module: courier / GLS only
- Focus: already invoiced courier costs only
- Safety: no Netfox write path, no wide rebuilds, no truncate

## Root Cause

The main January 2026 GLS gap was not unresolved order identity.

- Primary-linked January GLS shipments already existed for affected Amazon orders.
- Those shipments were seeded from `gls_billing_files`.
- Many of them had no row in `dbo.acc_shipment_cost`.
- Example checked during recovery:
  - `parcel_number = 30646666685`
  - seeded shipment existed in `dbo.acc_shipment`
  - billing line existed in `dbo.acc_gls_billing_line`
  - no actual row existed in `dbo.acc_shipment_cost`

This matched an operational failure:

- stale scheduler job `gls_sync_costs`
- job id `6410AAAF-9FB0-417F-A642-A671D2C305E3`
- status was `running`
- progress stopped at `6600/15851`
- no active lease owner / no recent heartbeat

## Actions Performed

1. Marked stale courier/backfill and GLS cost-sync zombie jobs as terminal failures.
2. Verified January 2026 GLS sample orders were linked to `gls_billing_files` shipments with missing cost rows.
3. Verified billing lines existed for those parcel numbers.
4. Ran scoped GLS cost sync:
   - `created_from=2026-01-01`
   - `created_to=2026-01-31`
   - `seeded_only=true`
   - `only_primary_linked=true`
   - `refresh_existing=false`
5. Refreshed January 2026 courier monthly KPI snapshot for GLS.
6. Rebuilt January 2026 GLS order logistics fact.
7. Recomputed profitability rollups for `2026-01-01..2026-01-31`.

## Execution Results

### Scoped GLS cost sync

- job id: `3E778588-53AB-498A-A85A-7F11EBCB9784`
- processed: `3144`
- actual written: `3144`
- no-cost matches: `0`

### January 2026 GLS KPI before

- `orders_with_actual_cost = 15216`
- `orders_linked_but_no_cost = 3111`
- `orders_missing_actual_cost = 3268`
- `actual_cost_coverage_pct = 82.32`

### January 2026 GLS KPI after

- `orders_with_actual_cost = 18327`
- `orders_linked_but_no_cost = 0`
- `orders_missing_actual_cost = 157`
- `actual_cost_coverage_pct = 99.15`

## Remaining Gap

Remaining January GLS gap is no longer cost materialization.

- only `157` orders remain without primary link
- operational driver changed from `linked_no_cost` to `missing_primary_link`

This is now a matching problem, not a courier invoice-cost problem.

## Business Meaning

- Closed-month-style quality is now effectively reached for January GLS on actual cost attachment to already linked orders.
- Profitability downstream can consume the corrected logistics via `acc_order_logistics_fact`.
- Shipment-month and billing-period link coverage are still lower than purchase-month coverage because they include broader shipment populations, including non-Amazon / non-order-universe noise.

## Recommended Next Move

Work only on the remaining `missing_primary_link` slice for January GLS.

- target the `157` unlinked Amazon orders
- keep scope to month + carrier
- do not return to broad Netfox sweeps unless a specific identifier bucket proves missing
