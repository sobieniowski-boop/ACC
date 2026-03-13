# Silent Financial Error Audit — ACC System

**Date:** 2025-06-18  
**Scope:** All modules that touch revenue, cost, profit, FX, VAT, fees, rollups, dashboards  
**Method:** Static code analysis with line-level verification against repository  
**Auditor:** GitHub Copilot (Claude Opus 4.6)  

---

## 1. Executive Summary

The ACC system contains **20 silent financial errors** — code paths that produce incorrect monetary values without raising exceptions or logging errors.

| Severity | Count | Estimated Annual Impact |
|----------|-------|------------------------|
| **P1 — Critical** | 5 | Material — can distort profit/loss by 20-100% per order |
| **P2 — High** | 9 | Meaningful — dashboards and rollups show wrong numbers |
| **P3 — Edge-case** | 6 | Low-frequency — affects fallback paths or rare scenarios |

**The five most damaging findings:**

1. **SF-05** — Rollup MERGE hardcodes logistics, ads, refund, storage, and other fees to **zero**. The profitability rollup table — the single source for executive dashboards — permanently understates costs.
2. **SF-01** — COGS is inflated by 23% on the ORM path (`profit_service.py`) because net purchase price is multiplied by `DEFAULT_VAT = 1.23`.
3. **SF-03** — Revenue is calculated as GROSS in `profit_service.py` but NET in `profit_engine.py`, making contribution margin incomparable.
4. **SF-02 / SF-10** — FX rate fallback to `1.0` silently undervalues EUR amounts by ~77% whenever the rate cache is cold or has a gap.
5. **SF-04** — "Profit" means four different things across four modules; the executive dashboard shows the broadest definition but labels it generically.

---

## 2. Financial Flow Map

```
Amazon SP-API Orders
        │
        ▼
┌──────────────────┐     ┌─────────────────────┐
│  order_pipeline   │────▶│  acc_order            │
│  (ingest)         │     │  acc_order_line       │
└──────────────────┘     └─────────┬─────────────┘
                                   │
          ┌────────────────────────┼──────────────────────┐
          ▼                        ▼                      ▼
┌─────────────────┐   ┌──────────────────────┐   ┌───────────────┐
│ profit_service   │   │  profit_engine        │   │  ads_sync      │
│ (ORM per-order)  │   │  (batch SQL engine)   │   │  (Amazon Ads)  │
│                  │   │                       │   │                │
│ Revenue: GROSS   │   │ Revenue: NET          │   │ FX: dict.get   │
│ COGS: ×1.23 bug  │   │ CM1 = Rev-COGS-Fees  │   │ fallback=1.0   │
│ CM includes ADS  │   │ CM1 excludes ADS      │   │                │
└────────┬─────────┘   └──────────┬────────────┘   └───────┬───────┘
         │                        │                         │
         ▼                        ▼                         │
   acc_order.                acc_order_line.                 │
   contribution_             profit fields                  │
   margin_pln                                               │
         │                        │                         │
         └────────────┬───────────┘                         │
                      ▼                                     │
          ┌──────────────────────┐                          │
          │ profitability_service │◀─────────────────────────┘
          │ (rollup MERGE)       │     ads/logistics/refund
          │                      │     ALL HARDCODED TO 0 ◀── SF-05
          └──────────┬───────────┘
                     ▼
     ┌───────────────────────────────┐
     │ acc_sku_profitability_rollup   │
     │ (executive dashboards source)  │
     └──────────────┬────────────────┘
                    ▼
     ┌───────────────────────────────┐
     │ executive_service              │
     │ executive_daily_metrics        │
     │ (health score, margin, KPIs)   │
     └───────────────────────────────┘

  Supporting modules:
  ┌───────────────┐  ┌────────────────┐  ┌──────────────────┐
  │ fx_service     │  │ fee_taxonomy    │  │ finance_center   │
  │ fallback=1.0   │  │ UNKNOWN→GL 599  │  │ FX fallback=1.0  │
  └───────────────┘  └────────────────┘  └──────────────────┘
  ┌───────────────┐  ┌────────────────┐  ┌──────────────────┐
  │ dhl_cost_sync  │  │ gls_cost_sync   │  │ cogs_importer    │
  │ gross=net bug  │  │ net-only lines  │  │ Excel fallback   │
  └───────────────┘  └────────────────┘  └──────────────────┘
```

---

## 3. Silent Financial Error Findings

### P1 — Critical (5 findings)

---

#### SF-01: COGS Inflated by 23% — Net Purchase Price × DEFAULT_VAT

| Field | Value |
|-------|-------|
| **ID** | SF-01 |
| **Severity** | P1 — Critical |
| **Module** | `app/services/profit_service.py` |
| **Line** | 92 |
| **Impact** | Every order processed through the ORM path has COGS overstated by 23% |

**Code:**
```python
# profit_service.py:92
cogs_per_unit = float(product.netto_purchase_price_pln) * DEFAULT_VAT  # DEFAULT_VAT = 1.23
```

**Problem:** COGS (Cost of Goods Sold) is a NET concept — the purchase price before VAT. Multiplying by 1.23 converts it to a gross value. This inflates COGS by 23%, understating profit by the same amount for every order line that uses this fallback.

**Fix:**
```python
cogs_per_unit = float(product.netto_purchase_price_pln)  # COGS is always NET
```

---

#### SF-02: FX get_rate_safe() Returns 1.0 When Rate Missing

| Field | Value |
|-------|-------|
| **ID** | SF-02 |
| **Severity** | P1 — Critical |
| **Module** | `app/core/fx_service.py` |
| **Line** | 182 |
| **Impact** | EUR amounts understated by ~77% when rate cache is empty or stale |

**Code:**
```python
# fx_service.py:182
return 1.0  # ultimate fallback
```

And in SQL generation:
```python
# fx_service.py:201
build_fx_case_sql() → "... ELSE 1.0 END"
```

**Problem:** When no exchange rate is found for a currency pair, the system assumes 1 foreign unit = 1 PLN. For EUR→PLN (≈4.30), this means EUR amounts are valued at 23% of their true PLN equivalent. This affects every financial module that uses `get_rate_safe()` or `build_fx_case_sql()`: profit calculations, finance transactions, ads spend conversion.

**Fix:**
```python
def get_rate_safe(self, currency: str, date: date) -> float:
    rate = self._lookup(currency, date)
    if rate is None:
        raise FXRateMissingError(f"No FX rate for {currency} on {date}")
    return rate
```
Callers must handle the exception and surface it as a data-quality alert rather than silently accepting 1.0.

---

#### SF-03: Revenue Formula Inconsistency — GROSS vs NET

| Field | Value |
|-------|-------|
| **ID** | SF-03 |
| **Severity** | P1 — Critical |
| **Module** | `app/services/profit_service.py` vs `app/services/profit_engine.py` |
| **Lines** | `profit_service.py:79` / `profit_engine.py:1786` |
| **Impact** | Contribution margin differs by the full VAT amount between ORM and batch paths |

**Code — profit_service.py (ORM path):**
```python
# profit_service.py:79
total_revenue_pln = float(order.order_total or 0) * fx_rate  # GROSS (includes VAT)
```

**Code — profit_engine.py (batch path):**
```python
# profit_engine.py:1786-1792
revenue = (item_price - item_tax - promotion_discount) * FX  # NET (VAT subtracted)
```

**Problem:** `profit_service.py` uses `order_total` (a gross amount including VAT) as revenue, while `profit_engine.py` correctly subtracts `item_tax`. The ORM path overstates revenue by the full VAT amount (~19-23% depending on marketplace). Both write to `acc_order` fields, creating inconsistent data depending on which code path ran last.

**Fix:**
```python
# profit_service.py — align with profit_engine's NET formula
total_revenue_pln = (
    float(order.order_total or 0)
    - float(order.order_tax or 0)
) * fx_rate
```

---

#### SF-04: Cross-Module Profit Definition Mismatch

| Field | Value |
|-------|-------|
| **ID** | SF-04 |
| **Severity** | P1 — Critical |
| **Module** | Multiple |
| **Lines** | `profit_service.py:100`, `profit_engine.py:2097`, `profitability_service.py:910`, `executive_service.py` |
| **Impact** | "Profit" means 4 different things — users cannot compare numbers across views |

**Definitions in use:**

| Module | Field | Formula | Includes ADS? | Includes Returns? |
|--------|-------|---------|---------------|-------------------|
| `profit_service.py` | `contribution_margin_pln` | Revenue - COGS - Fees - ADS - Logistics | Yes | No |
| `profit_engine.py` | `cm1_pln` (CM1) | Revenue - COGS - FBA - Referral - Logistics | No | No |
| `profit_engine.py` | `cm2_pln` (CM2) | CM1 - Ads - Returns - Storage - Aged - Removal - Liquidation | Yes | Yes |
| `profitability_service.py` | `profit_pln` (rollup) | Revenue - COGS - 9 cost categories | Yes | Yes |
| Executive dashboard | "profit" label | From rollup `profit_pln` | Yes | Yes |

**Problem:** The executive dashboard shows "profit" from the rollup (broadest definition), but the order-detail view shows `contribution_margin_pln` from `profit_service.py` (narrower). Comparing them side-by-side gives different totals for the same orders.

**Fix:** Standardize on three explicit tiers (CM1, CM2, Net Profit) across all modules. Add a `profit_tier` column or explicit naming in all API responses and UI labels.

---

#### SF-05: Rollup MERGE Hardcodes Logistics/Ads/Refund/Storage/Other to Zero

| Field | Value |
|-------|-------|
| **ID** | SF-05 |
| **Severity** | P1 — Critical |
| **Module** | `app/services/profitability_service.py` |
| **Lines** | 895–905 |
| **Impact** | Rollup table — the sole source for executive dashboards — permanently omits 5 of 9 cost categories |

**Code:**
```sql
-- profitability_service.py:895-905 — inside MERGE USING (SELECT ...)
0 as logistics_pln,
0 as ad_spend_pln,
0 as refund_pln,
0 as storage_fee_pln,
0 as other_fees_pln,
0 as refund_units
```

**Problem:** The MERGE's source SELECT aggregates revenue, COGS, amazon_fees, and fba_fees from `acc_order_line`, but hardcodes the remaining 5 cost columns to zero. The WHEN MATCHED clause then overwrites the target with these zero values, erasing any previously enriched data. This means:
- `profit_pln = revenue - cogs - amazon_fees - fba_fees - 0 - 0 - 0 - 0 - 0`
- Logistics, ads, refunds, storage, and other costs **never appear** in the rollup

**Fix:** Either:
1. Join to the relevant source tables (ads, logistics, returns) in the MERGE SELECT, or
2. Change the MERGE to NOT overwrite these columns (use `ISNULL(src.ad_spend_pln, tgt.ad_spend_pln)` pattern), or
3. Run separate UPDATE passes after the MERGE to populate ads/logistics/refund from their source tables.

---

### P2 — High (9 findings)

---

#### SF-06: Scheduler Timing Gap — Ads Sync Runs After Rollup

| Field | Value |
|-------|-------|
| **ID** | SF-06 |
| **Severity** | P2 — High |
| **Module** | `app/scheduler.py` |
| **Impact** | Today's rollup is always computed without today's ads data (1-day lag) |

**Timing:**
- 05:45 — Profitability rollup recompute (`days_back=7`)
- 07:00 — Ads sync (fetches last 3 days)

**Fix:** Either move ads sync before the rollup, or trigger a targeted rollup refresh after ads sync completes.

---

#### SF-07: _f() Helper Silently Converts NULL to 0.0

| Field | Value |
|-------|-------|
| **ID** | SF-07 |
| **Severity** | P2 — High |
| **Module** | `app/services/profit_engine.py` |
| **Lines** | 62–78 |
| **Impact** | Any NULL COGS, fee, shipping, or overhead silently becomes 0.0 — used 100+ times |

**Code:**
```python
def _f(val) -> float:
    """Safe float — None / empty → 0.0"""
    if val is None:
        return 0.0
    ...
```

**Problem:** There is no logging or tracking when `_f()` converts a NULL to zero. A missing COGS value (which should block computation or trigger an alert) silently becomes free goods.

**Fix:** Add an optional `field_name` parameter and emit a structured warning when converting NULL for financially significant fields (COGS, revenue, FX rate).

---

#### SF-08: FBA Fee Allocation by Quantity, Not Revenue-Weighted

| Field | Value |
|-------|-------|
| **ID** | SF-08 |
| **Severity** | P2 — High |
| **Module** | `app/services/profit_engine.py` |
| **Line** | ~1410 |
| **Impact** | Multi-SKU orders mis-allocate FBA fees to low-value items |

**Code:**
```python
line_fba_fee = order_total_fba * (line_qty / order_total_qty)
```

**Problem:** FBA fees correlate with item size/weight, not quantity. A 10 kg item and a 50 g item in the same order get equal per-unit FBA allocation, overstating costs for the small item and understating for the large one.

**Fix:** Use revenue-weighted or weight-weighted allocation where dimensional data is available.

---

#### SF-09: Shipping Cost Net/Gross Ambiguity (DHL/GLS)

| Field | Value |
|-------|-------|
| **ID** | SF-09 |
| **Severity** | P2 — High |
| **Module** | `app/services/dhl_cost_sync.py`, `app/services/gls_cost_sync.py` |
| **Line** | `dhl_cost_sync.py:290` |
| **Impact** | DHL costs may be understated if VAT is excluded but all other costs include it |

**Code (DHL):**
```python
# dhl_cost_sync.py:290
gross_amount = net_amount  # No VAT calculation
```

**Problem:** DHL sync sets `gross_amount = net_amount` (no VAT added). GLS sums component costs. Revenue is NET (after tax). If logistics costs are compared against NET revenue but are also NET, the comparison is consistent — but if any downstream formula expects GROSS logistics, margins will be wrong. The ambiguity is not documented.

**Fix:** Add explicit `is_gross` flag to shipment cost records, or normalize all costs to NET with a documented convention.

---

#### SF-10: Ads Currency → PLN Fallback to 1.0

| Field | Value |
|-------|-------|
| **ID** | SF-10 |
| **Severity** | P2 — High (same root cause as SF-02) |
| **Module** | `app/services/ads_sync.py` |
| **Line** | ~367 |
| **Impact** | Ads spend in EUR/GBP/SEK/CZK silently converted at 1:1 to PLN |

**Code:**
```python
rate = rates.get((m.currency, m.report_date), 1.0)
```

**Problem:** If the FX rate dictionary does not contain a rate for the specific (currency, date) tuple, the ads spend is recorded at 1.0 — making EUR spend appear ~4x cheaper than reality. Gap-filling fetches the nearest earlier date, but dates before the earliest available rate have no fallback except 1.0.

**Fix:**
```python
rate = rates.get((m.currency, m.report_date))
if rate is None:
    raise FXRateGapError(f"No rate for {m.currency} on {m.report_date}")
```

---

#### SF-11: Overhead Pool NULL Amount Silently Skipped

| Field | Value |
|-------|-------|
| **ID** | SF-11 |
| **Severity** | P2 — High |
| **Module** | `app/services/profit_engine.py` |
| **Lines** | 1528–1530 |
| **Impact** | Entire overhead pool silently dropped if amount_pln is NULL |

**Code:**
```python
amount = _f(pool.get("amount_pln"))  # NULL → 0.0
if amount == 0:
    continue  # pool skipped entirely
```

**Problem:** A misconfigured overhead pool with NULL amount is silently excluded from profit calculations. Rent, salaries, or other fixed costs can vanish without a trace.

**Fix:** Log a WARNING when a configured pool has NULL/0 amount. Optionally block the calculation if critical pools are missing.

---

#### SF-12: Refund Allocation Fails on Zero Order Total

| Field | Value |
|-------|-------|
| **ID** | SF-12 |
| **Severity** | P2 — High |
| **Module** | `app/services/profit_engine.py` |
| **Line** | ~1750 |
| **Impact** | Refund cost = 0 when the original order total was zero |

**Code:**
```sql
refund_amount * (order_line_total / NULLIF(olt.order_line_total, 0))
```

**Problem:** When the order total is zero (e.g., fully promotional / free order), `NULLIF` returns NULL, and the refund amount becomes NULL → `_f()` → 0.0. The refund cost is silently lost.

**Fix:** Use a fallback allocation (equal split across lines, or flag for manual review) when order total is zero.

---

#### SF-14: TKL Cache Miss → Logistics Cost = 0

| Field | Value |
|-------|-------|
| **ID** | SF-14 |
| **Severity** | P2 — High |
| **Module** | `app/services/profit_engine.py` |
| **Lines** | 550–563 |
| **Impact** | If logistics cost files are missing, all orders get logistics_pln = 0 |

**Code:**
```python
# When logistics files are missing:
signature = "missing"
# → cache miss → logistics_pln = 0 for all SKUs
```

**Problem:** The TKL (logistics) cache uses a file-based signature. If the files are missing or unreadable, the signature is "missing", which causes a cache miss, and all logistics costs default to zero. No error is raised.

**Fix:** Raise an exception or emit a data-quality alert when logistics cost files are unavailable. Use the last-known-good cache with a staleness warning.

---

#### SF-16: Unknown Fee Types Default to GL 599, Sign +1

| Field | Value |
|-------|-------|
| **ID** | SF-16 |
| **Severity** | P2 — High |
| **Module** | `app/core/fee_taxonomy.py` |
| **Lines** | 407–413 |
| **Impact** | Unknown Amazon charge types are always treated as costs, even if they're credits |

**Code:**
```python
# fee_taxonomy.py:407-413
return FeeMeta(
    category="UNKNOWN",
    gl_account="599",
    sign=1,  # always positive = cost
    ...
)
# WARNING logged only once per unknown type
```

**Problem:** Amazon occasionally introduces new fee types. If a new fee type is actually a credit/recovery (negative), treating it as a cost (sign +1) double-penalizes the seller: the credit is not recognized, and an equal cost is added.

**Fix:** Default to `sign=0` (suspended) for unknown fees and surface them in a reconciliation queue for manual classification.

---

### P3 — Edge-case (6 findings)

---

#### SF-13: Official Price Workbook — Silent Empty on Excel Load Failure

| Field | Value |
|-------|-------|
| **ID** | SF-13 |
| **Severity** | P3 — Edge-case |
| **Module** | `app/services/profit_engine.py` |
| **Lines** | 199–239 |
| **Impact** | If Excel file fails to load, all COGS lookups return 0.0 |

**Problem:** Any exception loading the official price workbook sets `prices = {}`. All subsequent COGS lookups find no match and fall back to `_f(None)` → 0.0. No error is surfaced.

**Fix:** Raise on workbook load failure. If the file is optional, log a WARNING and use database prices exclusively.

---

#### SF-15: Sellerboard Currency Fallback to EUR

| Field | Value |
|-------|-------|
| **ID** | SF-15 |
| **Severity** | P3 — Edge-case |
| **Module** | `app/services/sellerboard_history.py` |
| **Line** | 425 |
| **Impact** | Non-EUR marketplaces (SE/SEK, PL/PLN, CZ/CZK) silently treated as EUR |

**Code:**
```sql
COALESCE(NULLIF(s.currency, ''), tgt.currency, 'EUR')
```

**Problem:** If both source and target currencies are empty/NULL, defaults to EUR. For SEK or CZK transactions, this causes incorrect FX conversion downstream.

**Fix:** Make currency a NOT NULL column or reject rows with unknown currency.

---

#### SF-17: Finance Center FX Lookup Returns 1.0 Silently

| Field | Value |
|-------|-------|
| **ID** | SF-17 |
| **Severity** | P3 — Edge-case |
| **Module** | `app/services/finance_center/service.py` |
| **Lines** | 456, 501 |
| **Impact** | Finance center transactions with missing FX rates are valued at 1:1 |

**Problem:** Same root cause as SF-02, but in the finance center module. No logging when the fallback is used.

**Fix:** Align with the centralized FX service fix (SF-02). Add logging.

---

#### SF-18: Executive Health Score Defaults to 95 on Missing Shipment Data

| Field | Value |
|-------|-------|
| **ID** | SF-18 |
| **Severity** | P3 — Edge-case |
| **Module** | `app/services/executive_service.py` |
| **Impact** | Missing data is presented as near-perfect health |

**Problem:** When no shipment cost data is available, the inventory health component defaults to 95/100 instead of showing "no data" or a conservative estimate.

**Fix:** Return NULL or a low default (e.g., 50) when data is missing. Display "insufficient data" in the UI.

---

#### SF-19: Executive Margin Returns 0% When Revenue = 0

| Field | Value |
|-------|-------|
| **ID** | SF-19 |
| **Severity** | P3 — Edge-case |
| **Module** | `app/services/executive_service.py` |
| **Line** | 127 |
| **Impact** | Masks losses when revenue is exactly zero |

**Problem:** If revenue is 0 but costs exist, margin is returned as 0% instead of NULL or negative infinity. The executive dashboard shows "break-even" when the reality is pure loss.

**Fix:** Return NULL when revenue is 0. Let the frontend display "N/A".

---

#### SF-20: Strategy Confidence Starts at 30 Even with No Data

| Field | Value |
|-------|-------|
| **ID** | SF-20 |
| **Severity** | P3 — Edge-case |
| **Module** | `app/services/strategy_service.py` |
| **Impact** | Strategy recommendations appear moderately confident even with zero input data |

**Problem:** The confidence algorithm starts at a floor of 30/100. With no sales data, no ads data, and no cost data, the system still reports 30% confidence — misleading users into trusting recommendations that have no empirical basis.

**Fix:** Set floor to 0 when no input data is available. Display a "no data" badge in the UI.

---

## 4. Cross-Module Inconsistencies

### 4.1 Revenue: GROSS vs NET

| Module | Formula | Result |
|--------|---------|--------|
| `profit_service.py` | `order_total × FX` | GROSS (includes VAT) |
| `profit_engine.py` | `(item_price - item_tax - promotion_discount) × FX` | NET |
| `profitability_service.py` (rollup) | `(item_price - item_tax - promotion_discount) × FX` | NET |

**Consequence:** `acc_order.contribution_margin_pln` (from profit_service) and `acc_order_line` profit fields (from profit_engine) are not comparable. Summing them produces meaningless totals.

### 4.2 Profit Definition

| Module | Label | Formula |
|--------|-------|---------|
| `profit_service.py` | `contribution_margin_pln` | Revenue - COGS - Fees - ADS - Logistics |
| `profit_engine.py` | `cm1_pln` | Revenue - COGS - FBA - Referral - Logistics |
| `profit_engine.py` | `cm2_pln` | CM1 - Ads - Returns - Storage - Aged - Removal - Liquidation |
| `profitability_service.py` | `profit_pln` | Revenue - COGS - 9 cost categories |

**Consequence:** The same order can show different "profit" depending on which API endpoint is queried.

### 4.3 FX Fallback

| Module | Fallback Behavior |
|--------|-------------------|
| `fx_service.py` | Returns 1.0 |
| `ads_sync.py` | `dict.get(key, 1.0)` |
| `finance_center/service.py` | Returns 1.0, no logging |
| `profit_engine.py` | SQL `ELSE 1.0 END` |

**Consequence:** Four independent 1.0 fallbacks. Fixing one does not fix the others.

### 4.4 COGS Source

| Module | COGS Source |
|--------|-------------|
| `profit_service.py` | `netto_purchase_price_pln × 1.23` (BUG: includes VAT) |
| `profit_engine.py` | Official price workbook → `acc_purchase_price` → `_f()` fallback |
| `profitability_service.py` | From `acc_order_line.cogs_pln` (whatever profit_engine wrote) |

**Consequence:** The ORM path writes a 23%-inflated COGS; the batch path writes the correct NET COGS. If both run for the same order, the last writer wins.

---

## 5. Trustworthiness Matrix

| Module | Trust Level | Key Risk | Data-Quality Gate |
|--------|-------------|----------|-------------------|
| `profit_engine.py` | **Medium** | `_f()` zeros NULLs; FX `ELSE 1.0`; TKL cache miss → 0 | None — runs silently |
| `profit_service.py` | **Low** | GROSS revenue; COGS ×1.23; includes ADS in CM | None |
| `profitability_service.py` | **Low** | 5 cost columns hardcoded to 0 | None |
| `executive_service.py` | **Low** | Inherits rollup zeros; masks 0-revenue as 0% margin | None |
| `fx_service.py` | **Medium** | 1.0 fallback; 7-day circuit breaker | Staleness warning at 1 day |
| `ads_sync.py` | **Medium** | 1.0 FX fallback; gap-filling incomplete | None |
| `fee_taxonomy.py` | **Medium-High** | UNKNOWN → GL 599 sign +1 | WARN once per type |
| `finance_center/service.py` | **Medium** | 1.0 FX fallback; no logging | None |
| `dhl_cost_sync.py` | **Medium** | gross = net ambiguity | None |
| `gls_cost_sync.py` | **Medium-High** | Component-based; no major silent errors | None |
| `cogs_importer.py` | **Medium** | Excel load failure → empty dict | None |
| `sellerboard_history.py` | **Medium** | Currency fallback to EUR; historical only | None |
| `strategy_service.py` | **Medium** | Confidence floor = 30; no data = fake confidence | None |

---

## 6. Immediate Fix Plan

### Sprint 1 — Week 1 (P1 fixes)

| # | Finding | Fix | Effort | Risk |
|---|---------|-----|--------|------|
| 1 | **SF-05** Rollup zeros | Replace hardcoded zeros with JOINs to ads/logistics/returns tables in MERGE source | 4h | Medium — requires testing rollup rebuild |
| 2 | **SF-01** COGS ×1.23 | Remove `* DEFAULT_VAT` from `profit_service.py:92` | 15min | Low |
| 3 | **SF-03** Revenue GROSS→NET | Change `profit_service.py:79` to subtract `order_tax` | 30min | Low |
| 4 | **SF-02** FX 1.0 fallback | Replace `return 1.0` with exception; add guardrail check for stale rates | 2h | Medium — need to handle callers |
| 5 | **SF-04** Profit definitions | Add explicit `cm1_pln`, `cm2_pln`, `net_profit_pln` columns; update API responses | 8h | High — schema change, frontend update |

### Sprint 2 — Week 2 (P2 fixes)

| # | Finding | Fix | Effort |
|---|---------|-----|--------|
| 6 | **SF-10** Ads FX 1.0 | Use centralized FX service from SF-02 fix | 1h |
| 7 | **SF-06** Scheduler gap | Move ads sync to 05:00 or add post-ads rollup refresh | 30min |
| 8 | **SF-07** `_f()` logging | Add field-name param + structured warning for financially critical fields | 2h |
| 9 | **SF-16** UNKNOWN sign | Change default sign to 0 (suspended); add reconciliation queue | 2h |
| 10 | **SF-11** Overhead NULL | Add WARNING log + optional block for critical pools | 1h |
| 11 | **SF-14** TKL cache miss | Raise on missing files; use last-known-good cache | 2h |
| 12 | **SF-12** Refund ÷ 0 | Equal-split fallback + flag for review | 1h |
| 13 | **SF-09** Shipping ambiguity | Add `is_gross` flag; normalize to NET convention | 2h |
| 14 | **SF-17** Finance FX 1.0 | Align with centralized FX fix | 30min |

### Sprint 3 — Week 3 (P3 fixes)

| # | Finding | Fix | Effort |
|---|---------|-----|--------|
| 15 | **SF-13** Workbook failure | Raise on load failure | 30min |
| 16 | **SF-15** Sellerboard EUR | NOT NULL currency column + reject unknown | 1h |
| 17 | **SF-18** Health score 95 | Return NULL when data missing | 30min |
| 18 | **SF-19** Margin 0% | Return NULL when revenue=0 | 15min |
| 19 | **SF-20** Confidence floor | Set floor to 0 when no data | 15min |

---

## 7. Strategic Hardening Plan

### 7.1 Centralized FX Gateway
- Create a single `get_rate_or_fail(currency, date)` method
- Remove all per-module fallback-to-1.0 patterns (SF-02, SF-10, SF-17)
- Add a guardrail that blocks financial computations when FX data is older than 24h
- Emit `fx.rate_missing` metric for monitoring

### 7.2 Financial Calculation Contract
- Define three official profit tiers: **CM1**, **CM2**, **Net Profit**
- Create a `FinancialContract` dataclass that every calculator must return
- Add schema-level CHECK constraints ensuring `profit_pln = revenue - sum(all costs)`
- Version the contract so changes are tracked

### 7.3 Data-Quality Gates
- Before every rollup: assert FX rates exist for all active currencies for the date range
- Before every rollup: assert COGS coverage ≥ 95% of SKUs with orders
- Before every rollup: assert logistics cost data is ≤ 24h old
- Guardrail: `ads_spend_pln = 0 AND ads_impressions > 0` → block rollup
- Guardrail: `revenue_pln = 0 AND order_count > 0` → alert

### 7.4 Reconciliation Layer
- Daily automated comparison: `SUM(acc_order_line.revenue)` vs `SUM(rollup.revenue)` — must match within 0.01%
- Daily automated comparison: `profit_service CM` vs `profit_engine CM1+CM2` for same order set
- Weekly: compare ACC revenue against Amazon Settlement Reports
- Monthly: compare ACC COGS against purchase ledger

### 7.5 Observability
- Add structured logging to `_f()` for NULL→0 conversions on financial fields
- Add `fee_taxonomy.unknown_type` counter to Prometheus/monitoring
- Dashboard alert: rollup `ad_spend_pln = 0` for any date with known ads activity
- Dashboard alert: `logistics_pln = 0` for any date with known shipments

### 7.6 Testing
- Property-based tests: for any order, `profit_service.CM ≈ profit_engine.CM2` (within rounding)
- Regression test: rollup MERGE must produce non-zero values for all 9 cost columns when source data exists
- Golden-file tests: known order → known profit breakdown, checked after every code change
- Integration test: FX service must raise when rate is missing, not return 1.0

---

## Appendix: Finding Index

| ID | Title | Severity | Module | Line |
|----|-------|----------|--------|------|
| SF-01 | COGS × 1.23 VAT bug | P1 | profit_service.py | 92 |
| SF-02 | FX fallback 1.0 | P1 | fx_service.py | 182 |
| SF-03 | Revenue GROSS vs NET | P1 | profit_service.py / profit_engine.py | 79 / 1786 |
| SF-04 | Profit definition mismatch | P1 | Multiple | — |
| SF-05 | Rollup zeros 5 columns | P1 | profitability_service.py | 895–905 |
| SF-06 | Scheduler timing gap | P2 | scheduler.py | — |
| SF-07 | _f() zeros NULLs | P2 | profit_engine.py | 62–78 |
| SF-08 | FBA allocation by qty | P2 | profit_engine.py | ~1410 |
| SF-09 | Shipping net/gross | P2 | dhl_cost_sync.py | 290 |
| SF-10 | Ads FX fallback 1.0 | P2 | ads_sync.py | ~367 |
| SF-11 | Overhead NULL skip | P2 | profit_engine.py | 1528–1530 |
| SF-12 | Refund ÷ zero | P2 | profit_engine.py | ~1750 |
| SF-13 | Workbook silent empty | P3 | profit_engine.py | 199–239 |
| SF-14 | TKL cache miss → 0 | P2 | profit_engine.py | 550–563 |
| SF-15 | Sellerboard EUR fallback | P3 | sellerboard_history.py | 425 |
| SF-16 | UNKNOWN fee sign +1 | P2 | fee_taxonomy.py | 407–413 |
| SF-17 | Finance FX 1.0 | P3 | finance_center/service.py | 456 |
| SF-18 | Health score default 95 | P3 | executive_service.py | — |
| SF-19 | Margin 0% on zero rev | P3 | executive_service.py | 127 |
| SF-20 | Strategy confidence 30 | P3 | strategy_service.py | — |

---

*End of Silent Financial Error Audit*
