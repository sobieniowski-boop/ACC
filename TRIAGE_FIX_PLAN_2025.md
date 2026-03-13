# ACC Frontend/Backend Triage & Fix Plan

**Date:** 2025-01-20  
**Status:** IN PROGRESS

---

## Dependency Map

```
 ┌─────────────────────────────────────────────────────────┐
 │            DATA FLOW (source of truth chain)            │
 │                                                         │
 │  acc_order + acc_order_line                             │
 │       │                                                 │
 │       ▼                                                 │
 │  MERGE → acc_sku_profitability_rollup (zeros 6 cols)    │
 │       │                                                 │
 │       ▼                                                 │
 │  _enrich_rollup_from_finance()  ←── acc_finance_transaction
 │       │                              acc_ads_product_day│
 │       │                              acc_order.is_refund│
 │       ▼                                                 │
 │  MERGE → executive_daily_metrics                        │
 │       │                                                 │
 │       ▼                                                 │
 │  get_exec_overview() ──► Return Rate KPI (BUG: uses     │
 │                           refund_pln/revenue_pln,       │
 │                           should use refund_units)      │
 └─────────────────────────────────────────────────────────┘

 ┌──────────────────────────────────────────────┐
 │          FRONTEND BLOCKERS (independent)      │
 │                                               │
 │  SeasonalitySettings.tsx                      │
 │    └── for...of on dict → TypeError crash     │
 │                                               │
 │  api.ts timeout: 30s                          │
 │    └── Strategy detection needs 180s+         │
 │    └── Profitability recompute needs 120s+    │
 └──────────────────────────────────────────────┘

 ┌──────────────────────────────────────────────────────────┐
 │    STRATEGY EXPANSION LOGIC (independent chain)          │
 │                                                          │
 │  _detect_marketplace_expansion()                         │
 │    └── HAVING > 500 PLN = "active" threshold             │
 │    └── SKU selling 300 PLN on DE ≠ active → DE→DE sugg. │
 │                                                          │
 │  get_market_expansion_items()                            │
 │    └── Hardcodes source="A1PA6795UKMFR9" (DE) always    │
 │    └── signals.get("de_revenue") but key="source_revenue"│
 │    └── No amzn.gr filter in display query                │
 └──────────────────────────────────────────────────────────┘
```

---

## Priority Triage

### A — Source-of-Truth Financial / Data Bugs

| # | Issue | Root Cause | Fix | File | Impact |
|---|-------|-----------|-----|------|--------|
| A1 | Return Rate = 0.0% in Overview | `get_exec_overview()` computes `refund_pln / revenue_pln * 100` (monetary rate). Correct unit-based `return_rate_pct` is stored in `executive_daily_metrics` but never used by overview. | Change overview query to fetch weighted return_rate_pct from stored metrics | `executive_service.py` | CEO dashboard shows wrong metric |
| A2 | MERGE→Enrich architecture risk | MERGE zeros 6 cost columns, enrichment fills them. If enrichment fails/is skipped, costs vanish. NOT a current code bug — enrichment IS called at line 1053. | No code change needed now. Architecture is sound IF recompute runs end-to-end. | `profitability_service.py` | Understood, no fix needed |

### B — UI Blockers / Crashes

| # | Issue | Root Cause | Fix | File | Impact |
|---|-------|-----------|-----|------|--------|
| B1 | SeasonalitySettings crash | `for (const s of settings.settings)` iterates JS object with `for...of` → TypeError | Replace with `{ ...settings.settings }` spread | `SeasonalitySettings.tsx` | Page crashes on load |
| B2 | Run Detection timeout | Axios global timeout 30s; detection runs 10 SQL engines with 180s timeout | Add per-request timeout override for long operations | `api.ts` | Button appears to fail |

### C — Business Logic Errors

| # | Issue | Root Cause | Fix | File | Impact |
|---|-------|-----------|-----|------|--------|
| C1 | DE→DE expansion suggestion | `HAVING SUM(revenue_pln) > 500` conflates "strong source" with "present on market". SKU selling 300 PLN on DE → not "active" → suggested to "expand to DE" | Separate presence check (any sales > 0) from source threshold (>500 PLN) | `strategy_service.py` | Nonsensical suggestions |
| C2 | Expansion always shows DE as source | `get_market_expansion_items()` hardcodes `"source_marketplace": "A1PA6795UKMFR9"` | Read actual source from `source_signals_json` | `strategy_service.py` | Wrong source market display |
| C3 | Expansion source_revenue = 0 | `signals.get("de_revenue", 0)` but detection stores key as `"source_revenue"` | Change to `signals.get("source_revenue", 0)` | `strategy_service.py` | Revenue shows as 0 |
| C4 | amzn.gr SKUs in expansion list | Detection filters them, but display query doesn't | Add `AND sku NOT LIKE 'amzn.gr.%'` to display query | `strategy_service.py` | Junk items in list |

### D — Can Wait (UX / Polish)

| # | Issue | Notes |
|---|-------|-------|
| D1 | Global Filters not shared across all pages | ExecOverview, Strategy*, Seasonality* use local filters. Requires major refactor. |
| D2 | TR marketplace orphan data | TR not in MARKETPLACE_REGISTRY. Data cleanup task, not code fix. |
| D3 | Run Evaluation button clarity | Works correctly, label could be clearer. |

---

## Fix Order (by dependency + impact)

1. **B1** SeasonalitySettings crash — standalone, instant fix
2. **B2** Axios timeout — standalone, instant fix
3. **A1** Return Rate KPI — fixes CEO dashboard accuracy
4. **C2 + C3 + C4** Expansion display bugs — all in same function, fix together
5. **C1** Expansion detection threshold — separate query addition

---

## Verification Plan

After all fixes:
1. Load SeasonalitySettings page → should render without crash
2. Click "Run Detection" → should not timeout (spinner stays, completes when backend done)
3. Trigger profitability recompute → verify rollup has non-zero refund/ads/storage columns
4. Trigger executive recompute → verify return_rate_pct > 0 in executive_daily_metrics
5. Load Executive Overview → return rate should be non-zero
6. Load Strategy Expansion tab → no DE→DE, no amzn.gr, correct source markets + revenue
