# Current State Recovery Audit — 2026-03-09

## 1. Implementation Status by Workstream

### WS-1: Data Truth Repair (P1 Financial Fixes)

| Fix ID | Description | Status | Evidence |
|--------|------------|--------|----------|
| SF-01 | COGS uses NET purchase price (no ×1.23 VAT) | ✅ DONE | `profit_engine.py` has no `DEFAULT_VAT` / `1.23` references; `test_p1_financial_fixes.py` covers it |
| SF-02 | FX fallback raises instead of returning 1.0 | ✅ DONE | `fx_service.py` (new module) loaded from `acc_exchange_rate`, raises `StaleFxRateError` when >7 days stale; imported in `profit_engine.py` lines 275, 356, 366 |
| SF-03 | Revenue = NET (item_price − item_tax − promo) | ✅ DONE | `profit_engine.py:1806` confirms correct formula |
| SF-04 | Profit tier labels in API responses | ✅ DONE | `profitability_service.py:123` returns `"profit_tier": "net_profit"`; `ProfitTierBadge.tsx` component exists and is used by 6 pages (ExecOverview, ExecProducts, ExecMarketplaces, ProfitOverview, ProfitabilityProducts, ProfitabilityOrders) |
| SF-05 | MERGE preserves enriched cost columns | ✅ DONE | Test in `test_p1_financial_fixes.py` covers it |
| SF-09 | DHL gross_amount = net + VAT (not just net) | ✅ DONE | `dhl_cost_sync.py:235` computes `gross_amount = net_amount + vat_amount` |

### WS-2: Recomputation Backend

| Fix ID | Description | Status | Evidence |
|--------|------------|--------|----------|
| SF-06 | Scheduler dependency chain (ads+finance before rollup) | ✅ DONE | `scheduler.py:1066-1120` — `_recompute_profitability()` calls `run_full_ads_sync()` then `step_sync_finances()` BEFORE `recompute_rollups()`; aborts if deps fail; covered by `test_p2_financial_fixes.py` |
| SF-07 | `_f()` null/None warning | ⚠️ UNVERIFIED | Not explicitly checked — likely added given P2 test exists |
| SF-08 | FBA revenue-weighted allocation | ⚠️ UNVERIFIED | P2 test file exists but content not fully read |
| Rollup | `recompute_rollups()` function | ✅ DONE | `profitability_service.py:864` |
| Alerts | `evaluate_profitability_alerts()` | ✅ DONE | `profitability_service.py:1107` |
| Exec metrics | `recompute_executive_metrics()` | ✅ DONE | `executive_service.py:432` |
| Health score | `compute_health_score()` | ✅ DONE | `executive_service.py:501` |
| Risk detection | `detect_risks()` | ✅ DONE | `executive_service.py:642` |

### WS-3: Frontend — Global Filter Removal

| Item | Status | Evidence |
|------|--------|----------|
| `globalFilters.ts` deprecated | ✅ DONE | File header: `@deprecated – Use usePageFilters() and useUserPreferences() instead` |
| `usePageFilters.ts` created | ✅ DONE | Full implementation: URL search params-based, 156 lines, includes `pageFiltersToApiParams()` |
| `userPreferences.ts` created | ✅ DONE | Zustand persist store with `profitMode`, `currencyView`, `rowDensity` |
| `PageFilterBar.tsx` created | ✅ DONE | Component exists, exported from `shared/index.ts` |
| GlobalFilterBar removed from Layout | ✅ DONE | `Layout.tsx` imports only `Sidebar` and `TopBar` — no filter bar |
| Pages migrated to `usePageFilters` | ⚠️ PARTIAL | Only 3 pages confirmed migrated: **ProductProfitTable**, **LossOrders**, **DataQuality**. The other ~12 modified pages (Dashboard, ProfitOverview, ProfitabilityProducts, ProfitabilityOrders, ExecOverview, ExecProducts, ExecMarketplaces, PriceSimulator, ManageAllInventory, SeasonalityOverview, Ads, FinanceDashboard) do NOT import `usePageFilters` |
| `PageFilterBar` actually consumed | ❌ NOT DONE | Zero pages import `PageFilterBar` — the component is orphaned |

### WS-4: TypeScript Interface Alignment

| Interface | Status | Details |
|-----------|--------|---------|
| `KPISummary` | ⚠️ PARTIAL MISMATCH | Backend schema has `total_units`, `avg_order_value_pln`, `total_acos` — TS interface is MISSING all three |
| `MarketplaceKPI` | ⚠️ PARTIAL MISMATCH | Backend has `units`, `avg_order_value_pln` — TS interface missing both |
| `ProductProfitItem` | ✅ MOSTLY ALIGNED | TS has all major CM2/NP fields. REMAINING ISSUES: (1) TS has `refund_rate` but backend sends `return_rate` (no `refund_rate`); (2) TS has `roas` but backend does NOT send it; (3) TS has `shipping_charge_pln` but backend sends it inconsistently |
| `ProductProfitSummary` | ✅ ALIGNED | All CM2/NP/refund summary fields present in both sides |
| `DrilldownItem` | ✅ ALIGNED | Fields match backend response |

### WS-5: Financial Follow-ups (P2)

| Fix ID | Description | Status | Evidence |
|--------|------------|--------|----------|
| Fee taxonomy unified | ✅ DONE | `fee_taxonomy.py` — 70+ charge_types mapped; imported by both `profit_engine.py:1070` and `amazon_to_ledger.py:7` |
| FX service centralized | ✅ DONE | `fx_service.py` — rates from `acc_exchange_rate`, 1h cache, circuit breaker on staleness |
| Circuit breaker (Content Publish) | ✅ DONE | `circuit_breaker.py` — Redis-backed sliding window, 10 failures/1hr trips open, 30min cooldown |
| SP-API exponential backoff | ✅ DONE | `client.py` — backoff with jitter, Retry-After header respect, retryable status codes |
| Guardrails system | ✅ DONE | `guardrails.py` — 15+ checks (order freshness, finance, FX, ads, fees, profit anomalies, scheduler health, circuit breaker, duplicate detection, order-finance drift) |
| DHL billing import | ✅ DONE | `dhl_billing_import.py` exists |
| DHL cost sync | ✅ DONE | `dhl_cost_sync.py` — net+VAT=gross, identifier extraction, invoice matching |
| Content ops | ✅ DONE | `content_ops.py` + `content_ops.py` (router) registered in `router.py` |
| Netfox safe runner | ✅ DONE | `scripts/netfox_safe_runner.py` exists |

### WS-6: Ads Pipeline

| Item | Status | Evidence |
|------|--------|----------|
| `ads_sync.py` | ✅ EXISTS | Modified March 8 — full sync pipeline operational |
| Campaign-day fallback in profit engine | ⚠️ BUG | `profit_engine.py:2037-2038` — `FROM acc_ads_campaign_day` and `JOIN acc_ads_campaign` missing `dbo.` prefix (all other tables use `dbo.`) |

---

## 2. DB Recomputation Status / Commands Already Wired

| Recompute Function | Wired In | Trigger |
|-------------------|----------|---------|
| `profitability_service.recompute_rollups(days_back=7)` | ✅ scheduler.py:1107 | Daily at 05:45 via `_recompute_profitability()` |
| `profitability_service.evaluate_profitability_alerts()` | ✅ scheduler.py:1115 | Immediately after rollup |
| `executive_service.recompute_executive_metrics(days_back=7)` | ✅ exists | Needs verification if wired to scheduler |
| `executive_service.compute_health_score()` | ✅ exists | Needs verification if wired to scheduler |
| `executive_service.detect_risks()` | ✅ exists | Needs verification if wired to scheduler |
| SF-06 dependency chain (ads→finance→rollup) | ✅ wired | scheduler.py:1076-1100 — ads_sync + finance_sync run before rollup; rollup aborted if deps fail |

**Manual recompute (safe to run):**
```python
# 1. Ads sync (fresh data)
from app.services.ads_sync import run_full_ads_sync
await run_full_ads_sync(days_back=7)

# 2. Finance sync
from app.services.order_pipeline import step_sync_finances
await step_sync_finances(days_back=7)

# 3. Profitability rollup
from app.services.profitability_service import recompute_rollups, evaluate_profitability_alerts
recompute_rollups(days_back=14)

# 4. Alerts
from datetime import date, timedelta
evaluate_profitability_alerts(date.today() - timedelta(days=14), date.today())

# 5. Executive metrics + health + risks
from app.services.executive_service import recompute_executive_metrics, compute_health_score, detect_risks
recompute_executive_metrics(days_back=14)
compute_health_score()
detect_risks(days_back=7)
```

---

## 3. Partial or Risky Unfinished Changes

### 🔴 CRITICAL

1. **`profit_engine.py:2037-2038` — Missing `dbo.` prefix on ads tables**
   - `FROM acc_ads_campaign_day cd` → should be `FROM dbo.acc_ads_campaign_day cd`
   - `JOIN acc_ads_campaign c` → should be `FROM dbo.acc_ads_campaign c`
   - Risk: Query may fail or hit wrong schema on Azure SQL

2. **KPISummary TS ↔ Pydantic mismatch (3 missing fields)**
   - Backend sends: `total_units`, `avg_order_value_pln`, `total_acos`
   - Frontend `KPISummary` interface: missing all three
   - Risk: Data silently discarded, dashboard shows incomplete KPIs

3. **KPI `total_units` = `total_orders` (still wrong)**
   - `kpi.py:139` `units=orders` and `kpi.py:165` `total_units=total_orders`
   - Backend labels it "units" but returns order count, not item quantity
   - Schema says `int` which is correct for orders but semantically wrong for units

### 🟡 MEDIUM

4. **`PageFilterBar` component orphaned**
   - Created and exported but imported by zero pages
   - 3 pages use `usePageFilters()` directly without the visual component
   - 12+ pages don't use local filters at all yet

5. **MarketplaceKPI TS missing `units` and `avg_order_value_pln`**
   - Backend schema has both; frontend interface doesn't
   - Pages using marketplace breakdown won't show these values

6. **`refund_rate` vs `return_rate` naming conflict**
   - Backend sends `return_rate` in ProductProfitItem
   - Frontend TS has both `return_rate?: number` AND `refund_rate?: number`
   - Frontend may read `refund_rate` when backend sets `return_rate`

7. **`roas` field — TS declares it, backend never sends it**
   - `ProductProfitItem.roas?: number` exists in TS
   - Backend response has no `roas` key — always `undefined`

### 🟢 LOW

8. **`globalFilters.ts` store still exists (deprecated but present)**
   - Marked `@deprecated` — no pages import it — safe to delete later
   - `globalFilters.ts` (separate lib file) also exists — should be checked

9. **12 pages modified but NOT migrated to `usePageFilters`**
   - Dashboard, ProfitOverview, ProfitabilityProducts/Orders, ExecOverview/Products/Marketplaces, PriceSimulator, ManageAllInventory, SeasonalityOverview, Ads, FinanceDashboard
   - These likely still have hardcoded date ranges or no filtering

---

## 4. Recommended Next Step

**Fix the 3 Critical items in one prompt:**

1. Add `dbo.` prefix to `profit_engine.py:2037-2038`
2. Add `total_units`, `avg_order_value_pln`, `total_acos` to `KPISummary` TS interface
3. Add `units`, `avg_order_value_pln` to `MarketplaceKPI` TS interface
4. Remove orphan `refund_rate` and `roas` from `ProductProfitItem` TS (or implement `roas` in backend)

This is a 15-minute surgical fix that eliminates all data-loss bugs between backend and frontend.

---

## 5. Recommended Next 3 Prompts

### Prompt 1: "Critical TS + SQL alignment fix"
> Fix the 4 critical/medium mismatches: (1) add `dbo.` to `profit_engine.py:2037-2038`, (2) add `total_units`, `avg_order_value_pln`, `total_acos` to `KPISummary` in `api.ts`, (3) add `units`, `avg_order_value_pln` to `MarketplaceKPI` in `api.ts`, (4) remove `refund_rate` and `roas` from `ProductProfitItem` or alias properly. Also fix `kpi.py` to compute true `total_units` as `SUM(quantity)` instead of order count.

### Prompt 2: "Migrate remaining 12 pages to usePageFilters + PageFilterBar"
> Migrate Dashboard, ProfitOverview, ProfitabilityProducts, ProfitabilityOrders, ExecOverview, ExecProducts, ExecMarketplaces, PriceSimulator, ManageAllInventory, SeasonalityOverview, Ads, and FinanceDashboard to use `usePageFilters()` + `useUserPreferences()` + `PageFilterBar`. Each page should get URL-based filter state and the shared filter bar component.

### Prompt 3: "Wire executive recompute to scheduler + run full recompute"
> Wire `recompute_executive_metrics()`, `compute_health_score()`, and `detect_risks()` into the scheduler's `_recompute_profitability()` chain (after rollup + alerts). Then trigger a full 14-day recompute of all pipelines to refresh data after the P1/P2 financial fixes.
