# Product Profit Table — Performance Analysis

> Endpoint: `GET /api/v1/profit/v2/products` → `get_product_profit_table()`
> Reported latency: ~14.5s for default 30-day window

---

## 1. Query Architecture

The main query (`agg_sql`, lines 1870–2050) does:

```
WITH order_scope (distinct orders in date range)
   , shipping_per_order (finance transactions → shipping_charge_pln per order)
SELECT ... 60+ columns including 10 aggregate expressions ...
FROM acc_order_line ol
JOIN acc_order o
LEFT JOIN logistics fact table
LEFT JOIN shipping_per_order
LEFT JOIN acc_product p
LEFT JOIN acc_amazon_listing_registry (subquery)
OUTER APPLY acc_exchange_rate fx          -- correlated per-row
OUTER APPLY acc_order_line ol2 (totals)   -- correlated per-row
WHERE date range + filters
GROUP BY group_expr [+ marketplace_id]
```

After the main SQL, additional sequential queries:
1. Ads cost lookup (acc_ads_product_day) — only when CM2/NP mode
2. Inventory available map
3. CM2 component pools (when need_extended_costs)
4. Overhead pools (when need_extended_costs)

---

## 2. Identified Bottlenecks

### 🔴 #1: No SQL-level pagination (CRITICAL)

All matching rows (~4,300 product groups × 60 columns) are fetched into Python.
Sorting + pagination happens in Python after full fetch:

```python
rows = _fetchall_dict(cur)          # ALL rows
products.sort(...)                   # Python sort
start = (page - 1) * page_size      # Python slice
```

**Impact**: Even if user wants page 1 (50 items), SQL computes all ~4,300 groups.

### 🔴 #2: shipping_per_order CTE joins all finance transactions (CRITICAL)

```sql
shipping_per_order AS (
    SELECT os.amazon_order_id, ...,
        SUM(CASE WHEN charge_type IN (...) THEN amount_pln ...)
    FROM order_scope os
    LEFT JOIN acc_finance_transaction ft
        ON ft.amazon_order_id = os.amazon_order_id
    GROUP BY ...
)
```

For a 30-day window with ~10K orders × ~20 finance rows each = ~200K row join.
There's an unused `_load_finance_lookup()` function (line 1655) with 30-min cache
that does exactly this aggregation — but **it's never called** (dead code).

### 🟡 #3: OUTER APPLY acc_exchange_rate (MEDIUM)

```sql
OUTER APPLY (
    SELECT TOP 1 rate_to_pln
    FROM acc_exchange_rate
    WHERE currency = o.currency AND rate_date <= o.purchase_date
    ORDER BY rate_date DESC
) fx
```

Correlated TOP 1 + ORDER BY per order_line row. With ~100K order lines and
~3K exchange rate rows, this is moderately expensive. The `_FX_CACHE` in Python
is NOT used by this SQL — it's for Python-level FX conversions only.

### 🟡 #4: OUTER APPLY order_line_totals (MEDIUM)

```sql
OUTER APPLY (
    SELECT SUM(...), SUM(...), COUNT(*)
    FROM acc_order_line ol2
    WHERE ol2.order_id = o.id
) olt
```

Self-join aggregation per order — ~100K order_lines × avg 1.5 lines/order.
Indexed via IX_acc_order_line_order so it's seek-based, but still adds up.

### 🟡 #5: loss_lines recalculates full formula inline (MEDIUM)

The `loss_lines` column (lines 2000-2035) duplicates the ENTIRE revenue-cost
expression as an inline CASE, effectively doubling the computation work.

### 🟡 #6: acc_amazon_listing_registry — no index (MEDIUM)

```sql
LEFT JOIN (
    SELECT merchant_sku, MAX(parent_asin) AS parent_asin
    FROM acc_amazon_listing_registry
    WHERE parent_asin IS NOT NULL AND parent_asin != ''
    GROUP BY merchant_sku
) reg ON reg.merchant_sku = ol.sku
```

No index on `merchant_sku` — full table scan for the subquery.

### 🟢 #7: STRING_AGG on asin_list (LOW)

```sql
STRING_AGG(CAST(NULLIF(ol.asin, '') AS NVARCHAR(MAX)), ',') AS asin_list
```

Aggregating all ASINs per group for informational purposes. Minor overhead.

---

## 3. Current Mitigations

| Mitigation | TTL | Effectiveness |
|---|---|---|
| Result cache (`_RESULT_CACHE`) | 3 min | ✅ Eliminates repeat queries |
| FX fallback CASE expression | N/A | ✅ Fallback when no exchange rate |
| `need_extended_costs` guard | N/A | ✅ Skips CM2/NP pools for CM1-only |
| `_FX_CACHE` (Python) | 1 hour | ⚠️ Not used by main SQL |
| `_load_finance_lookup` | 30 min | ❌ Dead code — never called |

---

## 4. Recommended Optimizations

### Priority 1: SQL-level pagination (biggest win)

Replace Python sort+slice with SQL `ORDER BY ... OFFSET/FETCH`:

```sql
ORDER BY {sort_expression} {sort_dir}
OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
```

Requires moving the sort expression into SQL. For the default `cm1_profit`
sort, this would be based on the SUM expressions already in the query.
Reduces rows from ~4,300 to 50 per request.

**Complication**: Python filters (`only_loss`, `only_low_confidence`) and
CM2 allocation modify the sort order post-SQL. Would need a 2-phase approach:
1. SQL query returns all rows for CM2 allocation
2. CM2 enrichment in Python
3. Sort + paginate result

### Priority 2: Replace shipping_per_order CTE with lookup cache

Activate and use the existing `_load_finance_lookup()` function:
1. Call it before the main query
2. Remove the `order_scope` and `shipping_per_order` CTEs from SQL
3. Inject shipping_charge_pln in Python post-fetch

This avoids the 200K-row finance join in SQL and uses 30-min cached data.

### Priority 3: Create missing index on acc_amazon_listing_registry

```sql
CREATE NONCLUSTERED INDEX IX_registry_merchant_sku
    ON dbo.acc_amazon_listing_registry (merchant_sku)
    INCLUDE (parent_asin);
```

### Priority 4: Replace FX OUTER APPLY with pre-loaded Python lookup

Since `_FX_CACHE` already loads all ~3K exchange rates into memory:
1. Remove the `OUTER APPLY acc_exchange_rate` from SQL
2. Use `_fx_case()` CASE fallback (already in place, hardcoded rates)
3. Do precise FX conversion in Python using the cached rates

OR: Pre-join FX in a temp table before the main query.

### Priority 5: Cache the aggregation (separate CTE → temp table)

If SQL-level pagination is hard:
1. Compute the base aggregation once into a `#temp` table
2. Query `#temp` for paginated results
3. Keep `#temp` alive for 3 minutes (same as result cache TTL)

This way users paginating through results hit the temp table, not the
100K+ row base tables.

---

## 5. Estimated Impact

| Optimization | Est. Time Saved | Effort |
|---|---|---|
| SQL pagination | ~60% of 14.5s → ~5-6s | Medium |
| Finance lookup cache | ~20% → saves ~3s | Low |
| Registry index | ~5% → saves ~0.5-1s | Trivial |
| FX OUTER APPLY removal | ~10% → saves ~1-2s | Low |
| Combined | **~80-85%** → target **2-3s** | Medium total |

---

*Note: Exact timing depends on Azure SQL tier (ACC uses Azure SQL Free Tier with
limited DTUs). The CTE + 2 OUTER APPLY pattern is particularly DTU-expensive.*
