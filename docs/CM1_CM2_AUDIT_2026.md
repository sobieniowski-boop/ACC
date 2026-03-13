# CM1 / CM2 / NP — Audit Report

> Generated from code analysis of `profit_engine.py` + `fee_taxonomy.py` + `order_pipeline.py`

---

## 1. Current CM1 Formula

```
revenue_pln = SUM( (item_price - item_tax - promotion_discount) × FX_rate )
shipping_charge_pln = allocated from finance transactions per order
                      (ShippingCharge + ShippingTax + ShippingDiscount)
rev = revenue_pln + shipping_charge_pln

cogs = SUM(ol.cogs_pln)                  — from purchase price stamp
fba  = SUM(ol.fba_fee_pln)               — from step_bridge_fees()
ref  = SUM(ol.referral_fee_pln)          — from step_bridge_fees()
logistics = SUM(TKL cost × allocation_weight)  — via profit_logistics_join_sql

CM1 = rev - cogs - (fba + ref) - logistics
```

### bridge_fees coverage (order_pipeline.py line 2256)

Bridged charge types:
- `FBAPerUnitFulfillmentFee`, `FBAPerOrderFulfillmentFee`, `FBAWeightBasedFee`, `FBAPickAndPackFee` → `fba_fee_pln`
- `Commission` → `referral_fee_pln`

**Missing from bridge** (FEE_REGISTRY has them but bridge ignores):
- FBA: `FBAWeightHandlingFee`, `FBAOrderHandlingFee`, `FBAPerUnitFulfillment` (alt name), `FBADeliveryServicesFee`
- Referral: `ReferralFee` (alt name for Commission), `VariableClosingFee`, `FixedClosingFee`

---

## 2. Current CM2 Formula

```
CM2 = CM1
      - ads_cost
      - returns_net          (refund_gross - recovered_cogs + handling)
      - storage              (pool: fba_storage)
      - aged                 (pool: fba_aged)
      - removal              (pool: fba_removal)
      - liquidation          (pool: fba_liquidation)
      - refund_finance       (pool: refund_cost)
      - ship_surcharge       (pool: shipping_surcharge)
      - fba_inbound          (pool: fba_inbound)
      - promo                (pool: promo)
      - warehouse_loss       (pool: warehouse_loss)
      - amazon_other_fee     (pool: amazon_other_fee)
```

CM2 pools are loaded from `acc_finance_transaction` via `_load_fba_component_pools()` which:
1. Queries all finance transactions in date range
2. Classifies each via `_classify_finance_charge() → get_profit_classification()`
3. **Filters: `layer == "cm2"` only**
4. Groups by marketplace + bucket
5. Distributes to products by revenue-weight (or AFN-units fallback)

---

## 3. Current NP Formula

```
NP = CM2 - overhead_allocated
```

Overhead pools loaded from `acc_profit_cost_model` (subscription fees, regulatory, etc.).

---

## 4. BUGS FOUND — Items Falling Into a Black Hole

### BUG #1: Shipping Surcharges → INVISIBLE (profit overstated)

| charge_type | FEE_REGISTRY layer | FEE_REGISTRY bucket | In CM1? | In CM2 pool? |
|---|---|---|---|---|
| `ShippingHB` | **cm1** | shipping_surcharge | ❌ (only fba+ref deducted) | ❌ (pool loads cm2 only) |
| `ShippingChargeback` | **cm1** | shipping_surcharge | ❌ | ❌ |
| `FBAOverSizeSurcharge` | **cm1** | shipping_surcharge | ❌ | ❌ |

**Impact**: Heavy/bulky surcharges, oversize surcharges, and shipping chargebacks are
 never deducted from profit at any layer. CM1 and CM2 are both overstated.

**Fix**: Change `profit_layer` from `"cm1"` to `"cm2"` in FEE_REGISTRY (these are
account-level charges, not per-order, so pool allocation to CM2 is correct).

### BUG #2: Order-level Promo Fees → INVISIBLE (profit overstated)

| charge_type | FEE_REGISTRY layer | FEE_REGISTRY bucket | In CM1? | In CM2 pool? |
|---|---|---|---|---|
| `CouponRedemptionFee` | **cm1** | promo_order | ❌ | ❌ |
| `PrimeExclusiveDiscountFee` | **cm1** | promo_order | ❌ | ❌ |
| `SubscribeAndSavePerformanceFee` | **cm1** | promo_order | ❌ | ❌ |

**Impact**: Per-order promo costs (coupon redemption, Prime exclusive, S&S) are
never deducted. These ARE order-specific but CM1 only reads fba+ref from order_line.

**Fix options**:
- **Option A (better)**: Add to `step_bridge_fees()` as a new column `promo_order_fee_pln`
  on `acc_order_line`, then include in CM1 calculation.
- **Option B (simpler)**: Change to `cm2`/`promo` so they enter CM2 pool allocation.

### BUG #3: RefundCommission → INVISIBLE (profit understated)

| charge_type | FEE_REGISTRY layer | FEE_REGISTRY bucket | sign | In CM1? | In CM2 pool? |
|---|---|---|---|---|---|
| `RefundCommission` | **cm1** | referral_refund | **-1** (recovery) | ❌ | ❌ |

**Impact**: When an order is refunded, Amazon refunds part of the referral fee.
This credit is classified as `cm1` but never captured → profit slightly understated
(recovery not recognized).

**Fix**: Add to `step_bridge_fees()` — subtract from `referral_fee_pln` on refund orders.
Or reclassify to `cm2`/`refund_cost` with sign=-1.

### BUG #4: bridge_fees misses 7 FBA/referral charge type variants

`step_bridge_fees()` hardcodes 5 charge types. Missing:
- `FBAWeightHandlingFee` — common for heavy items
- `FBAOrderHandlingFee` — per-order handling
- `FBAPerUnitFulfillment` — alternate name for FBAPerUnitFulfillmentFee
- `FBADeliveryServicesFee` — delivery services
- `ReferralFee` — alternate name for Commission
- `VariableClosingFee` — media category closing fee
- `FixedClosingFee` — media category fixed fee

**Impact**: Any orders charged via alternate fee names will have $0 FBA/referral fees.

**Fix**: Add all FBA_FEE category (profit_layer=None) and REFERRAL_FEE category
charge types to bridge_fees.

---

## 5. What-if Module CM2 Gap

`get_product_what_if_table()` (line 3565) only deducts 6 CM2 components:

```python
cm2 = cm1 - ads - returns_net - storage - aged - removal - liquidation
```

**Missing** from what-if CM2 (but present in PPT CM2):
- `refund_finance` (refund_cost pool)
- `shipping_surcharge` (even after fixing BUG #1)
- `fba_inbound`
- `promo`
- `warehouse_loss`
- `amazon_other_fee`

**Impact**: What-if CM2 is ~6 buckets more optimistic than actual CM2.
The `ratio_by_marketplace` calculation doesn't compute ratios for these buckets.

**Fix**: Add the missing 6 buckets to `ratio_by_marketplace` computation and
include them in the what-if CM2 formula.

---

## 6. Summary Priority Matrix

| # | Bug | Severity | Profit Impact | Fix Effort |
|---|-----|----------|---------------|------------|
| 1 | Shipping surcharges invisible | **HIGH** | Overstated | 1 line change in fee_taxonomy.py |
| 4 | bridge_fees missing charge types | **HIGH** | Mixed | Add 7 types to SQL IN-clause |
| 2 | Order-level promo fees invisible | **MEDIUM** | Overstated | Either 1 line or new column |
| 5 | What-if CM2 missing 6 buckets | **MEDIUM** | Simulation gap | ~30 lines in profit_engine |
| 3 | RefundCommission invisible | **LOW** | Understated | 1 line or add to bridge |

---

## 7. Recommended Action Plan

### Phase 1 — Quick Fixes (no schema changes)

1. **fee_taxonomy.py**: Change ShippingHB/ShippingChargeback/FBAOverSizeSurcharge
   from `"cm1"` to `"cm2"` (layer) — enables pool allocation.

2. **fee_taxonomy.py**: Change CouponRedemptionFee/PrimeExclusiveDiscountFee/
   SubscribeAndSavePerformanceFee from `"cm1"/"promo_order"` to `"cm2"/"promo"`.

3. **fee_taxonomy.py**: Change RefundCommission from `"cm1"/"referral_refund"`
   to `"cm2"/"refund_cost"`.

4. **order_pipeline.py step_bridge_fees()**: Expand IN-clause to include all
   FBA_FEE + REFERRAL_FEE charge types from FEE_REGISTRY.

### Phase 2 — What-if Completeness

5. Add missing 6 CM2 buckets to what-if `ratio_by_marketplace` computation
   and include in CM2 formula.

### Phase 3 — Order-level Attribution (optional, requires schema change)

6. Add `promo_order_fee_pln` column to `acc_order_line` + bridge from finance.
   This moves coupon/S&S fees to CM1 (per-order granularity) instead of CM2 pool.

---

*Audit date: 2026-03 | Engine: profit_engine.py ~6,400 lines | Taxonomy: fee_taxonomy.py 70+ charge types*
