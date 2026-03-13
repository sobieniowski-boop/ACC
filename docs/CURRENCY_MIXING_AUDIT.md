# Currency Mixing Audit — ACC Codebase

**Date**: 2026-03-04 (audit) / 2026-03-05 (all fixes applied)  
**Scope**: Full backend (`apps/api/`) + frontend (`apps/web/`) audit + fix  
**Base currency**: PLN  
**Currencies in play**: EUR (DE/FR/IT/ES/NL/BE/IE), SEK (SE), PLN (PL), GBP (UK), AED (AE), SAR (SA), TRY (TR)

---

## CRITICAL Bugs

### BUG-1: `get_profit_by_sku()` — SUM(item_price) labeled as `revenue_pln` without FX

| | |
|---|---|
| **File** | `apps/api/app/connectors/mssql/mssql_store.py` |
| **Line** | 946 |
| **Severity** | **CRITICAL** |
| **Status** | **FIXED** (2026-03-05) — added `OUTER APPLY acc_exchange_rate` + CASE fallback |
| **Called by** | `run_job_type("generate_ai_report", ...)` in same file (~line 2115), planning module |

**Code:**
```sql
SELECT
    ISNULL(ol.sku, '')                       AS sku,
    ...
    SUM(ISNULL(ol.item_price, 0))            AS revenue_pln,   -- ← BUG
    SUM(ISNULL(ol.cogs_pln, 0))             AS cogs_pln,
    SUM(ISNULL(ol.fba_fee_pln, 0) + ISNULL(ol.referral_fee_pln, 0))  AS amazon_fees_pln,
    ...
FROM dbo.acc_order_line ol WITH (NOLOCK)
JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
WHERE {where_sql}
GROUP BY ol.sku
ORDER BY SUM(ISNULL(ol.item_price, 0)) DESC
```

**Why it's a bug:**  
`ol.item_price` is stored in the order's **native currency** (EUR for DE, SEK for SE, GBP for UK, PLN for PL). When `marketplace_id` is NULL (cross-marketplace query), this SUM adds `15.99 EUR + 199 SEK + 12.50 GBP` as if they were all the same unit, then labels the result `revenue_pln`. Downstream, `contribution_margin_pln` is computed as `revenue_pln - cogs_pln - amazon_fees_pln` — mixing unconverted revenue with PLN-denominated costs.

**Impact:**  
- CM1/CM% is wrong for any SKU sold on multiple marketplaces
- SKU ranking by revenue is distorted (SEK amounts inflate apparent revenue)
- AI report source data is affected (called by `generate_ai_report` job)

**Suggested fix:**  
Add `OUTER APPLY` on `acc_exchange_rate` (same pattern as `profit_engine.py` line ~1060):
```sql
SUM(ISNULL(ol.item_price, 0) * ISNULL(fx.rate_to_pln,
    CASE o.currency
        WHEN 'EUR' THEN 4.25 WHEN 'GBP' THEN 5.10
        WHEN 'SEK' THEN 0.39 WHEN 'PLN' THEN 1.0 ELSE 4.25
    END)) AS revenue_pln,
```

---

### BUG-2: `sync_profit_snapshot()` — native-currency item_price stored as `revenue_net`

| | |
|---|---|
| **File** | `apps/api/app/connectors/mssql/mssql_store.py` |
| **Line** | 1195 |
| **Severity** | **CRITICAL** |
| **Status** | **FIXED** (2026-03-05) — added `OUTER APPLY acc_exchange_rate` + CASE fallback for revenue_net & revenue_gross |
| **Called by** | `run_job_type("calc_profit", ...)`, `run_job_type("sync_finances", ...)`, `run_job_type("sync_inventory", ...)` |

**Code:**
```sql
INSERT INTO dbo.acc_al_profit_snapshot (
    sales_date, order_number, sku, title, quantity,
    revenue_net, revenue_gross, cogs, transport, channel, source_table
)
SELECT
    CAST(o.purchase_date AS date),
    o.amazon_order_id,
    ol.sku,
    ol.title,
    ISNULL(ol.quantity_ordered, 0),
    ISNULL(ol.item_price, 0)                                     AS revenue_net,   -- ← BUG
    ISNULL(ol.item_price, 0) + ISNULL(ol.item_tax, 0)           AS revenue_gross,  -- ← BUG
    ISNULL(ol.cogs_pln, ...) AS cogs,  -- this IS in PLN
    0 AS transport,
    o.marketplace_id AS channel,
    'acc_order'
FROM dbo.acc_order o
JOIN dbo.acc_order_line ol ON ol.order_id = o.id
WHERE ...
```

**Why it's a bug:**  
`ol.item_price` is in native currency (EUR/SEK/GBP/PLN) but the snapshot table has no `currency` column. The `cogs` column uses `cogs_pln` (PLN), so `revenue_net` (native currency) and `cogs` (PLN) are in different currencies within the same row. Any snapshot-based margin calculation (`revenue_net - cogs`) is meaningless for non-PLN orders.

**Impact:**  
- `acc_al_profit_snapshot` contains mixed currencies in `revenue_net` / `revenue_gross`
- Planning module and alerts reading the snapshot will see incorrect margins
- Particularly wrong for DE/FR/IT/ES/NL/BE (EUR) and SE (SEK) orders

**Suggested fix:**  
Either: (a) convert `item_price * fx_rate` to PLN inline, or (b) add a `currency` column and convert in downstream queries. Option (a) is consistent with the rest of the codebase:
```sql
ISNULL(ol.item_price, 0) * ISNULL(fx.rate_to_pln,
    CASE o.currency WHEN 'EUR' THEN 4.25 WHEN 'GBP' THEN 5.10
        WHEN 'SEK' THEN 0.39 WHEN 'PLN' THEN 1.0 ELSE 4.25 END)
    AS revenue_net,
```
Add the same `OUTER APPLY (SELECT TOP 1 rate_to_pln FROM acc_exchange_rate ...)` join.

---

## MEDIUM Bugs

### BUG-3: `build_settlement_summaries()` — fallback to unconverted `amount` when exchange_rate is NULL

| | |
|---|---|
| **File** | `apps/api/app/services/finance_center/service.py` |
| **Line** | 1763 |
| **Severity** | **MEDIUM** |
| **Status** | **FIXED** (2026-03-05) — removed `COALESCE(exchange_rate, 1)` fallback; now uses CASE-based hardcoded rates |

**Code:**
```sql
SUM(CAST(
    COALESCE(amount_pln, amount * COALESCE(exchange_rate, 1), amount)
AS DECIMAL(18,4))) AS total_amount_base
```

**Why it's a bug:**  
The COALESCE chain is: `amount_pln` → `amount * exchange_rate` → `amount * 1` → `amount`. When both `amount_pln` IS NULL and `exchange_rate` IS NULL, the fallback is `amount * 1 = amount` in native currency, treated as if it were PLN. For EUR transactions without exchange rate data, 49.99 EUR would become 49.99 "PLN" (actual value ~212 PLN).

**Impact:**  
- Settlement summary `total_amount_base` is understated for EUR/GBP rows missing exchange rate data
- The query groups by `marketplace_id, currency` so at least the mixing is within one currency per group, but the "base" (PLN) amount is wrong

**Mitigating factor:**  
The query groups by `currency`, so raw `total_amount` (original) is always correct per-group. Only `total_amount_base` is affected. If exchange rates are populated for most transactions, the impact is limited to edge cases.

**Suggested fix:**  
Replace the innermost fallback with the same hardcoded CASE rates:
```sql
COALESCE(
    amount_pln,
    amount * COALESCE(exchange_rate,
        CASE currency
            WHEN 'EUR' THEN 4.25 WHEN 'GBP' THEN 5.10
            WHEN 'SEK' THEN 0.39 WHEN 'PLN' THEN 1.0 ELSE 4.25
        END)
)
```

---

## LOW Bugs

### BUG-4: `get_data_quality()` missing_cogs ranking — cross-currency SUM for sort

| | |
|---|---|
| **File** | `apps/api/app/services/profit_engine.py` |
| **Line** | ~3908 |
| **Severity** | **LOW** |

**Code:**
```sql
SELECT TOP 50
    ol.sku,
    ...
    SUM(ISNULL(ol.item_price, 0)) AS revenue_orig,
    ...
FROM dbo.acc_order_line ol WITH (NOLOCK)
...
GROUP BY ol.sku
ORDER BY SUM(ISNULL(ol.item_price, 0)) DESC
```

**Why it's a bug:**  
`item_price` is in native currency; a SEK 199 order looks bigger than a EUR 19.99 order in the ranking. SKUs sold primarily on SE may appear higher-priority than DE SKUs with more actual revenue.

**Mitigating factors:**  
- Column is labeled `revenue_orig` (not `_pln`) — honest labeling
- Used solely for diagnostics ranking (missing COGS top-50), not for financial reporting
- Most KADAX revenue is EUR, so cross-currency distortion is moderate

**Suggested fix:**  
Apply FX conversion to get `revenue_pln_est` for accurate ranking, or accept the approximation with a note.

---

### BUG-5: `manage_inventory.py` traffic rollup `revenue` — native currency without indicator

| | |
|---|---|
| **File** | `apps/api/app/services/manage_inventory.py` |
| **Lines** | 2141, 2212 |
| **Severity** | **LOW** |

**Code:**
```sql
SUM(ISNULL(curr.revenue, 0)) AS revenue,
```

**Why it's a (potential) bug:**  
`revenue` in `acc_inv_traffic_rollup` comes from Amazon's Sales & Traffic Business Reports and is in the marketplace's native currency. The rollup groups by `marketplace_id + sku`, keeping currencies consistent within each row. However:
- The column has no `_eur`/`_sek`/`_pln` suffix or companion `currency` column
- If any downstream code ever aggregates across marketplaces, it would silently mix currencies

**Current state:** Safe today — the frontend displays `revenue` per-marketplace item, never cross-marketplace. But the schema invites future bugs.

**Suggested fix:**  
Add `currency NVARCHAR(5)` column to `acc_inv_traffic_rollup`, or rename to `revenue_local_currency`.

---

## Correctly Handled Places ✓

| File | Function/Area | Line(s) | Technique |
|------|--------------|---------|-----------|
| `profit_engine.py` | `get_product_profit_table()` | ~1060 | `OUTER APPLY acc_exchange_rate` + fallback CASE |
| `profit_engine.py` | `get_product_drilldown()` | ~3140 | `OUTER APPLY acc_exchange_rate` + fallback CASE |
| `profit_engine.py` | `get_loss_orders()` | ~3270 | `OUTER APPLY acc_exchange_rate` + fallback CASE |
| `profit_engine.py` | `get_product_what_if_table()` | ~1600 | `_fx_rate_for_currency()` in-memory FX cache |
| `profit_engine.py` | `_load_fx_cache()` / `_FX_FALLBACK` | ~60-120 | Centralized FX rate loading with fallback dict |
| `order_pipeline.py` | `step_bridge_fees()` | ~2120 | `ABS(df.amount) * ISNULL(fx.rate_to_pln, CASE ...)` |
| `order_pipeline.py` | `_upsert_items()` | ~980 | Raw storage of `item_price` in native currency (no false _pln label) |
| `mssql_store.py` | `recalc_profit_orders()` | ~1060 | Proper FX rate join in Step 3, writes `revenue_pln` to `acc_order` |
| `mssql_store.py` | `get_profit_orders()` | ~860 | Reads pre-calculated `o.revenue_pln` from acc_order |
| `mssql_store.py` | `refresh_plan_actuals()` | ~2450 | Reads `o.revenue_pln` and `o.contribution_margin_pln` — already converted |
| `mssql_store.py` | `_metric_for_rule()` | ~1570 | Uses `o.revenue_pln`, `o.contribution_margin_pln` — all _pln columns |
| `ads_sync.py` | `_upsert_daily_metrics()` | ~310 | `_get_exchange_rates()` lookup → `spend * rate`, `sales * rate` |
| `ads_sync.py` | `_upsert_product_day_metrics()` | ~440 | Same proper FX conversion pattern |
| `finance_center/service.py` | `generate_ledger_from_amazon()` | ~1830 | `_lookup_fx_rate()` → stores `amount` (orig) + `amount_base` (PLN) |
| `return_tracker.py` | All functions | throughout | Exclusively uses `_pln` suffixed columns (`refund_amount_pln`, `cogs_pln`, etc.) |
| Frontend (all pages) | `formatPLN()` | throughout | Displays only `_pln` suffixed API fields, never raw amounts |

---

## Summary

| Severity | Count | Status | Impact |
|----------|-------|--------|--------|
| **CRITICAL** | 2 | **FIXED** | Wrong revenue/CM1 in profit-by-SKU and profit snapshot |
| **MEDIUM** | 1 | **FIXED** | Settlement base amounts may fallback to unconverted native currency |
| **LOW** | 2 | Open (acceptable risk) | Diagnostic sort inaccuracy; schema invites future bugs |

### Fix Status

1. **BUG-1** (`get_profit_by_sku`): ✅ FIXED — added `OUTER APPLY acc_exchange_rate` + CASE fallback
2. **BUG-2** (`sync_profit_snapshot`): ✅ FIXED — added `OUTER APPLY acc_exchange_rate` + CASE fallback for revenue_net & revenue_gross
3. **BUG-3** (`build_settlement_summaries`): ✅ FIXED — removed `COALESCE(exchange_rate, 1)` fallback, now uses CASE-based hardcoded rates
4. **BUG-4** (`get_data_quality`): Open — LOW severity, diagnostic-only ranking, acceptable approximation
5. **BUG-5** (`manage_inventory.py` traffic rollup): Open — LOW severity, per-marketplace grouping keeps it safe today

### Systemic Pattern

The codebase has a **correct pattern** established in `profit_engine.py`:
```sql
OUTER APPLY (
    SELECT TOP 1 rate_to_pln
    FROM dbo.acc_exchange_rate er WITH (NOLOCK)
    WHERE er.currency = o.currency
      AND er.rate_date <= o.purchase_date
    ORDER BY er.rate_date DESC
) fx
```
with fallback:
```sql
ISNULL(fx.rate_to_pln,
    CASE o.currency
        WHEN 'EUR' THEN 4.25 WHEN 'GBP' THEN 5.10
        WHEN 'SEK' THEN 0.39 WHEN 'PLN' THEN 1.0 ELSE 4.25
    END)
```

The bugs occur in **older or auxiliary functions** (`mssql_store.py`, `finance_center`) that weren't updated when the FX conversion pattern was established. Applying the same pattern to BUG-1 and BUG-2 is straightforward.
