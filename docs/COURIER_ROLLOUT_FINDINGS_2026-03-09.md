# Courier Rollout Findings — 2026-03-09

## Scope

- Controlled rollout work on the courier module only.
- Goal: improve shipment-to-order matching for courier actual cost allocation without broad SQL operations.
- Safety constraints respected:
  - no `TRUNCATE`,
  - no broad `cache_courier_tables.py`,
  - only scoped month + carrier operations,
  - Netfox touched only through small read-only lookups inside bounded backfill batches.

## Code change made

File:

- `apps/api/app/services/courier_identifier_backfill.py`

Change:

- Replaced the heavy batch candidate selection path based on full diagnostics classification with a lighter `unlinked` shipment CTE.
- This preserves backfill behavior but removes an ACC timeout hotspot for:
  - `gls_note1`
  - `gls_tracking_map`
  - `dhl_numeric`
  - `dhl_jjd`
- Added targeted missing-`external_order_id` repair modes in `courier_identifier_backfill.py`:
  - `gls_note1_external_order`
  - `dhl_numeric_external_order`
  - `dhl_jjd_external_order`

Verification:

- `pytest -q tests/test_courier_identifier_backfill.py tests/test_courier_order_universe_linking.py tests/test_api_courier.py tests/test_courier_link_diagnostics.py`
- Result: `20 passed`

## Operational rollout performed

### 1. `2026-02 GLS gls_note1` batch `limit=25`

Result:

- `candidate_values=25`
- `candidate_shipments=48`
- `candidate_shipments_with_actual_cost=11`
- `resolved_order_ids=46`
- `acc_package_rows_written=31`
- `acc_bl_order_rows_written=21`
- `acc_dis_map_rows_written=21`

Observed effect after scoped `order-universe-linking` + monthly KPI refresh:

- no material coverage improvement
- `shipment` side remained:
  - `shipments_in_scope=9519`
  - `shipments_with_primary_link=6376`
  - `shipments_unlinked=3143`
- `purchase_month` snapshot for current code path:
  - `orders_universe=18790`
  - `orders_with_actual_cost=6276`
  - `actual_cost_coverage_pct=33.4`

Important diagnostic finding:

- top `note1` candidate values successfully resolved into ACC cache rows,
- but sampled resolved BL orders did **not** map to any `acc_order`,
- example pattern: `acc_cache_bl_orders.external_order_id` present, `acc_order.amazon_order_id` absent.

Conclusion:

- this GLS bucket is not blocked by courier invoice import,
- it is blocked by missing order identity continuity into `acc_order`.

### 2. `2026-02 GLS gls_tracking_map` batch `limit=25`

Result:

- `candidate_values=0`

Conclusion:

- no immediate GLS tracking-map wave is available in current ACC state for February.

### 3. `2026-02 DHL dhl_numeric` batch `limit=25`

Result:

- `candidate_values=25`
- `candidate_shipments=25`
- `candidate_shipments_with_actual_cost=25`
- `resolved_candidate_values=6`
- `resolved_order_ids=12`
- `acc_package_rows_written=8`
- `acc_bl_order_rows_written=6`
- `acc_dis_map_rows_written=6`

Observed effect after scoped `order-universe-linking` + monthly KPI refresh:

- no material link improvement
- refreshed current-code snapshot:
  - `orders_universe=9940`
  - `orders_with_actual_cost=8017`
  - `actual_cost_coverage_pct=80.65`

Important diagnostic finding:

- sampled resolved DHL package identifiers also ended in cache rows whose `external_order_id` did not exist in `acc_order`.

### 4. `2026-02 DHL dhl_jjd` batch `limit=25`

Result:

- `candidate_values=25`
- `candidate_shipments=25`
- `candidate_shipments_with_actual_cost=25`
- `resolved_candidate_values=3`
- `resolved_order_ids=6`
- `acc_package_rows_written=6`
- `acc_bl_order_rows_written=2`
- `acc_dis_map_rows_written=3`
- `acc_dhl_parcel_map_rows_written=4`

Observed effect after scoped `order-universe-linking` + monthly KPI refresh:

- no material link improvement
- KPI remained:
  - `orders_universe=9940`
  - `orders_with_actual_cost=8017`
  - `actual_cost_coverage_pct=80.65`

## Business conclusion

The immediate rollout blocker is no longer the courier identifier extraction itself.

The current blocker is:

- identifiers can now be backfilled into ACC cache tables in a controlled way,
- but many of the resolved BL/package identities terminate at `external_order_id` values that do not exist in `acc_order`,
- so `order-universe-linking` still cannot promote them into primary shipment-to-order links.

This means the next production-critical workstream is:

1. audit and repair ACC order identity continuity:
   - `acc_cache_bl_orders.external_order_id`
   - `acc_bl_distribution_order_cache.external_order_id`
   - `acc_order.amazon_order_id`
2. classify missing order matches into explicit buckets:
   - valid external IDs absent in `acc_order`
   - non-Amazon / foreign channel identifiers leaking into courier cache
   - transformed / prefixed / UUID-like identifiers not normalized to ACC order IDs
3. only then continue more backfill waves for February and closed-month historical catch-up.

## Order identity audit

New read-only diagnostic added:

- `GET /api/v1/courier/order-identity-gaps`

Purpose:

- inspect where the ACC identity chain breaks for unresolved courier matches:
  - courier token / `note1`
  - cache package / BL order resolution
  - `external_order_id`
  - `acc_order`

### Live findings: `2026-02 DHL`

- `dhl_numeric_order_identity`
  - `shipments=1544`
  - `distinct_values=1544`
  - `values_with_package_match=910`
  - `values_with_external_order_id=6`
  - `values_resolved_to_acc_order=0`
  - `values_missing_external_order_id=904`
  - `values_missing_acc_order=6`
- `dhl_jjd_order_identity`
  - `shipments=709`
  - `distinct_values=709`
  - `values_with_parcel_map=709`
  - `values_with_package_match=511`
  - `values_with_external_order_id=2`
  - `values_resolved_to_acc_order=0`
  - `values_missing_external_order_id=509`
  - `values_missing_acc_order=2`

Conclusion:

- DHL February is blocked primarily by missing `external_order_id` continuity after package match.
- The minority tail that does reach `external_order_id` still does not resolve to `acc_order`.

### Live findings: `2026-02 GLS`

- `gls_note1_order_identity`
  - `shipments=1537`
  - `distinct_values=1512`
  - `values_with_external_order_id=21`
  - `values_resolved_to_acc_order=0`
  - `values_missing_external_order_id=1491`
  - `values_missing_acc_order=21`

Sample missing-`acc_order` external IDs were UUID-like, for example:

- `e678aaf0-c980-11f0-8035-b3308aed1baa`
- `16ab6d81-efc3-11f0-b9a8-5daa9b9bfbe3`

Conclusion:

- GLS February is dominated by missing `external_order_id` resolution.
- Even when `external_order_id` exists, the sample values look like internal UUID-style identities rather than Amazon order IDs.

### Live findings: `2025-11 GLS`

- `gls_note1_order_identity`
  - `shipments=9994`
  - `distinct_values=9837`
  - `values_with_external_order_id=125`
  - `values_resolved_to_acc_order=124`
  - `values_missing_external_order_id=9712`
  - `values_missing_acc_order=1`

Conclusion:

- November GLS is overwhelmingly blocked before `acc_order`.
- This historical gap is mostly not an `acc_order` import failure.
- It is mainly absent `external_order_id` resolution for the `note1` path.

## External-order repair canary

Prepared remediation:

- the new `*_external_order` backfill modes select already-resolved BL order IDs
  that are missing `external_order_id`,
- they only backfill `dbo.acc_cache_bl_orders`,
- they avoid the heavier package/distribution writes.

Canary attempted:

- `2025-11 GLS`
- mode: `gls_note1_external_order`
- `limit_values=25`

Result:

- blocked before any ACC write by Netfox connectivity failure:
  - `pyodbc.OperationalError 08001`
  - `SQL Server does not exist or access denied`

Conclusion:

- the next write-wave is technically ready in ACC code,
- but the operational blocker for that wave is current Netfox connectivity, not the courier logic itself.

## 2025-11 baseline under current code semantics

Read-only baseline from current service logic:

- `2025-11 GLS`
  - `orders_universe=28348`
  - `orders_with_actual_cost=23664`
  - `actual_cost_coverage_pct=83.48`
- `2025-11 DHL`
  - `orders_universe=16272`
  - `orders_with_actual_cost=16193`
  - `actual_cost_coverage_pct=99.51`

Interpretation:

- `2025-11 DHL` is already close to the closed-month target.
- `2025-11 GLS` is not.
- Therefore November cannot honestly be declared "similar to December/January" until the GLS order identity gap is fixed.

## Recommendation for next step

Do not spend the next production window on another courier cost sync.

Do this instead:

1. build a scoped audit for unresolved courier cache identities that have:
   - package match in ACC cache,
   - BL/external order identity,
   - but no matching `acc_order`.
2. split results separately for:
   - GLS numeric `note1` / BL order IDs,
   - DHL numeric parcel IDs,
   - DHL `JJD` parcel lineage.
3. add a safe report/API on top of that bucketization before any broad historical rollout.
