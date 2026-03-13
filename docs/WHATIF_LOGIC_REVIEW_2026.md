# What-if Module — Logic Review

> Function: `get_product_what_if_table()` in `profit_engine.py` (line 2655)
> Purpose: Simulate margin for currently open Amazon offers at given quantity

---

## 1. Architecture Summary

The module simulates profitability for each active offer in `acc_offer` by:

1. Loading offers with filters (marketplace, fulfillment, SKU search, status)
2. Loading historical per-unit costs from actual shipped orders in date range
3. Loading CM2/NP pools and marketplace-level ratios (when extended costs needed)
4. Computing per-SKU logistics buckets via PERCENTILE_CONT on historical orders
5. Applying a 4-tier logistics decision engine (TKL matrix vs. observed data)
6. Computing CM1/CM2/NP per offer
7. Optionally grouping by ASIN or parent with re-aggregation

---

## 2. Cost Source Priority (per-unit)

| Cost | Priority | Source 1 | Source 2 | Source 3 | Fallback |
|------|----------|----------|----------|----------|----------|
| **COGS** | 1→2→miss | `acc_product.netto_purchase_price_pln` | historical line avg | — | 0 |
| **FBA Fee** | 1→2→3→miss | `offer.fba_fee × FX` | historical line avg | `expected_fba_fee` API | 0 |
| **Referral** | 1→2→3→4→miss | `offer.referral_rate × price × FX` | historical line avg | `expected_referral_rate × price` | `expected_referral_fee × FX` |
| **Logistics** | 4-tier | TKL country matrix | TKL SKU fallback | observed median/P75 | 0 |
| **Shipping** | 1→miss | historical finance `ShippingCharge` median per qty bucket | — | — | 0 |
| **Ads** | 1→miss | historical ads-per-unit from `ads_cost_pln` allocation | — | — | 0 |

✅ Priority system is well-designed with clear fallback chains.

---

## 3. Logistics Decision Engine (FBM only)

4-tier blending algorithm:

```
1. tkl_low_sample    — TKL plan if < 3 observed samples
2. blend_safe_max    — 40/60 TKL/observed blend, floored at safe_max
                       (where safe_max = max(plan, P75))
3. safe_max          — max(plan, P75) when neither low nor stable
4. observed_only     — P75 or median if no TKL data
```

✅ Conservative approach — always prefers higher cost estimate (P75 > median,
   includes TKL plan as floor). This avoids overstating simulated profit.

### Pack Quantity Logic

- Uses TKL matrix `pack_qty` if available
- Falls back to historical single-SKU order patterns
- `packages_count = ceil(scenario_qty / pack_qty)`
- Logistics and shipping are multiplied by `packages_count`

✅ Correct for multi-package scenarios.

---

## 4. Known Issues

### ISSUE #1: CM2 missing 6 cost buckets (MEDIUM)

What-if CM2 formula (line 3565):
```python
cm2 = cm1 - ads - returns_net - storage - aged - removal - liquidation
```

Product Profit Table CM2 formula (line 2447):
```python
cm2 = cm1 - ads - returns_net - storage - aged - removal - liquidation
      - refund_fin - ship_surcharge - fba_inbound - promo - wh_loss - amz_other
```

**Missing from what-if**: `refund_finance`, `shipping_surcharge`, `fba_inbound`,
`promo`, `warehouse_loss`, `amazon_other_fee`

**Impact**: What-if CM2 will be optimistic by the sum of 6 missing buckets.
For a typical month these could represent 2-5% of revenue combined.

### ISSUE #2: Grouped logistics_gap_pct averaging is imprecise (LOW)

```python
acc["logistics_gap_pct"] = (float(current_gap) + float(gap_val)) / 2.0
```

When grouping 3+ offers, this running average gives wrong results:
- 3 offers with gaps [10%, 20%, 30%] → result: 20% (correct by luck)
- But with [10%, 20%, 30%] processed as: (10+20)/2=15 → (15+30)/2=22.5 (wrong, should be 20%)

Should use weighted average or sum/count.

### ISSUE #3: Grouped CM2 omits same buckets as per-offer (LOW)

The grouped recalculation (line ~3850) also uses the 6-bucket CM2 formula,
consistent with per-offer but still missing the same buckets.

### ISSUE #4: `_load_finance_lookup()` also dead in what-if context (INFO)

The what-if module builds its own `shipping_per_order` CTE inline
(line ~3210) rather than using a cached lookup.

---

## 5. Positive Findings

| Aspect | Assessment |
|--------|------------|
| Cost source cascading | ✅ Excellent — 3-4 tier priority per cost component |
| Logistics decision engine | ✅ Conservative and robust |
| Pack quantity estimation | ✅ Proper TKL → historical fallback |
| FBA vs FBM differentiation | ✅ FBA skips logistics, FBM has full courier model |
| Confidence scoring | ✅ Data-source-aware (35 base + component bonuses) |
| Execution drift detection | ✅ Flags when observed >> planned logistics |
| Flag system | ✅ Clear warnings for missing data |
| Cache key | ✅ Covers all parameters — no stale data risk |
| Amazon Renewed exclusion | ✅ `amzn.gr.%` SKUs filtered out |

---

## 6. Recommendations

1. **Add 6 missing CM2 buckets** — compute `refund_cost_per_rev_ratio`,
   `shipping_surcharge_per_afn`, `fba_inbound_per_afn`, `promo_per_rev`,
   `wh_loss_per_afn`, `amz_other_per_rev` in `ratio_by_marketplace`.

2. **Fix grouped averaging** — use total_sum/total_count instead of running `/2`.

3. **Consider using `_load_finance_lookup()`** for shipping data instead of
   inline CTE — would benefit from 30-min cache across repeated queries.

---

*Review date: 2026-03 | Module: profit_engine.py lines 2655–3920 (~1,270 lines)*
