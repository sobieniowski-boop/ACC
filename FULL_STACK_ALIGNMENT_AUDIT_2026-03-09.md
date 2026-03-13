# ACC Full-Stack Alignment Audit Report

**Date:** 2026-03-09  
**Auditor:** Senior Full-Stack Systems Auditor  
**Project:** Amazon Command Center (ACC)  
**Stack:** Python/FastAPI + Azure SQL + React/TypeScript + TanStack Tables

---

## Executive Summary

The ACC backend has been significantly hardened (FX centralization, enrichment pipeline, P&L unification). **Field naming between backend and frontend is 100% aligned** — no renamed/missing field mismatches exist. However, **6 critical issues** and **12 high-severity issues** were identified across the stack, primarily:

1. Backend endpoints returning **hardcoded zeros** for enriched cost fields
2. Frontend pages with **local filter state** instead of global filters
3. **Stale rollup data** beyond the 7-day scheduler lookback
4. Frontend **missing error handlers** on 7+ pages
5. **Hardcoded date** in FinanceDashboard causing data staleness
6. **Stub fields** (cvr_pct, inventory_risk) in executive dashboard

---

## PART 1 — API Contract Validation

### 1.1 Endpoint Map (31 Routers, 300+ Endpoints)

| Router | Prefix | Endpoints | Key Service | Primary Tables |
|--------|--------|-----------|-------------|----------------|
| auth | `/auth` | 4 | JWT/bcrypt | users |
| kpi | `/kpi` | 5 | ORM AccOrder | acc_order, acc_order_line |
| profit | `/profit` | 3 | mssql_store | acc_order, acc_order_line |
| profit_v2 | `/profit/v2` | 8 | profit_engine | acc_order_line, acc_exchange_rate, acc_ads_product_day |
| profitability | `/profitability` | 4 | profitability_service | acc_sku_profitability_rollup |
| executive | `/executive` | 4 | executive_service | executive_daily_metrics, acc_sku_profitability_rollup |
| ads | `/ads` | 4 | direct SQL | acc_ads_campaign, acc_ads_campaign_day |
| finance_center | `/finance` | 22 | finance_center/service | acc_finance_transaction, acc_fin_* |
| fba_ops | `/fba` | 35 | fba_ops/service | SP-API inventory, acc_fba_* |
| manage_inventory | `/inventory` | 31 | manage_inventory | acc_inv_*, acc_product |
| families | `/families` | 15 | family_mapper/* | acc_family_*, acc_product |
| returns | `/returns` | 9 | return_tracker | acc_return_item, acc_fba_customer_return |
| strategy | `/strategy` | 10 | strategy_service | growth_opportunity |
| seasonality | `/seasonality` | 8 | seasonality_service | seasonality_monthly_metrics, seasonality_profile |
| content_ops | `/content` | 55+ | content_ops | content_task, content_version |
| pricing | `/pricing` | 3 | mssql_store | acc_offer |
| tax_compliance | `/tax` | 14 | tax_compliance/* | tax_* tables |
| courier | `/courier` | 6 | courier_* | acc_order, ITJK_DHL_Costs |
| dhl | `/dhl` | 7 | dhl_* | ITJK_DHL_Costs, acc_dhl_* |
| gls | `/gls` | 7 | gls_* | acc_gls_* |
| alerts | `/alerts` | 6 | mssql_store | acc_alert, acc_alert_rule |
| jobs | `/jobs` | 4 | mssql_store | acc_al_jobs |
| planning | `/planning` | 4 | mssql_store | acc_plan_month |
| inventory_routes | `/inventory` | 3 | mssql_store | acc_inventory_snapshot |
| import_products | `/import-products` | 3 | import_products | acc_product |
| inventory_taxonomy | `/inventory/taxonomy` | 3 | taxonomy | acc_taxonomy_* |
| audit | `/audit` | 2 | cogs_audit | acc_order_line |
| ai_rec | `/ai` | 4 | ai_service | acc_ai_recommendation |
| outcomes | `/strategy/decisions` | 6 | decision_intelligence | decision_execution_record |
| guardrails | `/guardrails` | 2 | guardrails | guardrail_check |
| health | `/health` | 3 | deep check | Azure SQL + Redis + SP-API |

### 1.2 Field Alignment Status

**✅ PASS: 100% field name alignment between backend dict keys and frontend TypeScript interfaces.**

All monetary fields use `_pln` suffix. Percentage fields consistently use `_percent` for profit margins (CM1/CM2/NP) and `_pct` for operational percentages (ACoS, return rate). No renamed or missing field mismatches detected.

### 1.3 Contract Mismatches Found

| # | Severity | Endpoint | Issue | Location |
|---|----------|----------|-------|----------|
| C1 | 🔴 CRITICAL | `GET /profitability/orders` | Returns `fba_fees_pln: 0`, `ad_cost_pln: 0`, `refund_pln: 0` as **hardcoded zeros** | [profitability_service.py](apps/api/app/services/profitability_service.py) L314-318 |
| C2 | 🔴 HIGH | `GET /executive/products` | `cvr_pct` and `inventory_risk` are **always NULL** (hardcoded stubs) | [executive_service.py](apps/api/app/services/executive_service.py) L372-373 |
| C3 | 🟡 MEDIUM | `GET /kpi/summary` | Returns `total_units`, `avg_order_value_pln`, `date_from/date_to` — frontend TypeScript types don't define these | [kpi.py](apps/api/app/api/v1/kpi.py) |
| C4 | 🟡 MEDIUM | `GET /kpi/summary` | CM formula = Revenue − COGS − AmazonFees − Logistics (**excludes ads & refunds**); ads returned separately but not subtracted | [kpi.py](apps/api/app/api/v1/kpi.py) L21-30 |
| C5 | ℹ️ LOW | Profitability overview | Returns both `tacos_pct` and `ad_spend_share_pct` (same calculation) | [profitability_service.py](apps/api/app/services/profitability_service.py) |

### 1.4 Unused Backend Capabilities

| Backend Feature | Status |
|-----------------|--------|
| `GET /profit/v2/data-quality` coverage metrics | ✅ Used by DataQuality.tsx |
| `GET /profit/v2/what-if` logistics drift detection | ✅ Used by ProductProfitTable what-if mode |
| Saved filter views (global store methods) | ❌ Implemented in Zustand but no UI exposed |
| `GET /returns/*` (9 endpoints) | ⚠️ Backend ready, frontend pages not built yet |
| content_ops publishing workflow | ✅ Fully wired |

---

## PART 2 — Frontend Screen Audit

### 2.1 Screen Map (80 Pages)

| Page | API Endpoint(s) | Filter Type | Loading | Error | Null Safety |
|------|----------------|-------------|---------|-------|-------------|
| Dashboard.tsx | kpi/summary, chart/revenue, top-drivers, recent-alerts, data-quality | LOCAL | ✅ | ⚠️ Partial | ✅ |
| ProfitOverview.tsx | profitability/overview | LOCAL | ✅ | ❌ | ✅ |
| ProfitExplorer.tsx | profit/orders, profit/export | LOCAL | ✅ | ✅ | ✅ |
| ProductProfitTable.tsx | profit/v2/products, what-if, marketplaces | GLOBAL | ✅ | ✅ | ✅ |
| LossOrders.tsx | profit/v2/loss-orders | GLOBAL | ✅ | ✅ | ✅ |
| ProfitabilityOrders.tsx | profitability/orders | LOCAL | ✅ | ❌ | ✅ |
| ProfitabilityProducts.tsx | profitability/products | LOCAL | ✅ | ❌ | ✅ |
| ExecOverview.tsx | executive/overview | LOCAL | ✅ | ❌ | ✅ |
| ExecMarketplaces.tsx | executive/marketplaces | LOCAL | ✅ | ❌ | ✅ |
| ExecProducts.tsx | executive/products | LOCAL | ✅ | ❌ | ✅ |
| Ads.tsx | ads/summary, chart, top-campaigns | LOCAL | ✅ | ❌ | ✅ |
| FinanceDashboard.tsx | finance/dashboard + 5 mutations | LOCAL | ✅ | ❌ | ⚠️ |
| FbaOverview.tsx | fba/overview, fba/report-diagnostics | NONE | ✅ | ❌ | ✅ |
| ManageAllInventory.tsx | inventory/all, inventory/sku/{sku} | LOCAL | ✅ | ❌ | ⚠️ |
| InventoryOverview.tsx | inventory/overview | NONE | ✅ | ❌ | ✅ |
| Pricing.tsx | pricing/offers, buybox-stats | LOCAL | ✅ | ❌ | ✅ |
| StrategyOverview.tsx | strategy/overview | NONE | ✅ | ✅ | ✅ |
| SeasonalityOverview.tsx | seasonality/overview | NONE | ✅ | ❌ | ⚠️ |
| ContentDashboard.tsx | content/tasks, data-quality, impact | LOCAL | ✅ | ❌ | ✅ |
| DataQuality.tsx | data-quality, ai-suggestions | GLOBAL | ✅ | ✅ | ✅ |

### 2.2 Critical Frontend Bugs

| # | Severity | Page | Bug | Fix |
|---|----------|------|-----|-----|
| F1 | 🔴 CRITICAL | FinanceDashboard.tsx | **Hardcoded `from: "2025-09-01"`** — stale start date, should be dynamic | Change to `dayjs().subtract(6, 'month').format('YYYY-MM-DD')` |
| F2 | 🔴 HIGH | FinanceDashboard.tsx | 5 mutation triggers (import, settlements, ledger, reconcile, auto-match) have **no `onError` callbacks** — silent failures | Add toast notification on mutation.onError |
| F3 | 🟡 MEDIUM | SeasonalityOverview.tsx | `MONTH_NAMES[m - 1]` — if backend sends month=0 or >12, renders "undefined" | Add bounds check: `MONTH_NAMES[m-1] ?? '?'` |
| F4 | 🟡 MEDIUM | ManageAllInventory.tsx | 14 accumulation operations on potentially undefined values during grouping — NaN cascade risk | Add null coalescing on accumulation: `(acc.x ?? 0) + (item.x ?? 0)` |
| F5 | 🟡 LOW | PriceSimulator.tsx | `sale_price: parseFloat(salePrice) \|\| 0` sends 0 to backend — potential division by zero in margin calculation | Backend should validate `sale_price > 0` |

### 2.3 Global Filter Inconsistency

**Only 3 pages use the global filter store:**
- ProductProfitTable.tsx ✅
- LossOrders.tsx ✅  
- DataQuality.tsx ✅

**These pages should use global filters but have LOCAL state:**
- Dashboard.tsx (7 `useState` hooks)
- ProfitOverview.tsx (2 `useState` hooks)
- ProfitabilityOrders.tsx / ProfitabilityProducts.tsx
- ExecOverview.tsx / ExecMarketplaces.tsx / ExecProducts.tsx
- Ads.tsx

**Impact:** Changing marketplace/date in ProductProfitTable does NOT sync to Dashboard or Executive pages. Users must re-select filters on each page.

### 2.4 Client-Side Calculations

| Page | Calculation | Risk |
|------|-------------|------|
| Dashboard.tsx | `cm_percent: (cm_pln / revenue_pln) * 100` | LOW — chart display only, backend sends cm_percent separately |
| ProductProfitTable.tsx | Profit mode fallback: `np_profit ?? cm2_profit ?? cm1_profit` | OK — safe fallback chain |
| All others | **NONE** — metrics rendered directly from backend | ✅ |

**Verdict:** No contradictory client-side calculations found. Frontend trusts backend-computed values.

### 2.5 Hardcoded Values in Frontend

| Page | Hardcoded Value | Should Be |
|------|-----------------|-----------|
| FinanceDashboard.tsx | `from: "2025-09-01"` | Dynamic (6mo rolling window) |
| Ads.tsx | ACoS thresholds: 10% (green) / 20% (red) | Backend-configurable |
| ExecProducts.tsx | ACoS > 30% and return_rate > 5% red flags | Backend-configurable |
| ManageAllInventory.tsx | Scoring: 120/80/65/35/20/-10 action weights | Backend-configurable |
| ContentDashboard.tsx | Market codes: DE/FR/IT/ES/NL/PL/SE/BE | Fetch from `/kpi/marketplaces` |
| FbaOverview.tsx | `.slice(0, 20)` on stockout risks | Backend pagination |

---

## PART 3 — Data Freshness & DB Health

### 3.1 Tables Requiring Recomputation

| Table | Scheduler | Default Lookback | Stale Since Fixes | Action |
|-------|-----------|------------------|-------------------|--------|
| `acc_sku_profitability_rollup` | Daily 05:45 | 7 days | **>7 days stale** | 🔴 **Full recompute** |
| `acc_marketplace_profitability_rollup` | Auto (cascades) | 7 days | **>7 days stale** | Auto via SKU rollup |
| `executive_daily_metrics` | Daily 06:00 | 7 days | **>7 days stale** | Auto after rollup fix |
| `executive_health_score` | ❓ No scheduler found | Unknown | **Possibly never refreshed** | 🔴 **Manual trigger** |
| `seasonality_monthly_metrics` | Daily 04:30 | 36 months | **All historical months stale** | 🔴 **Full rebuild** |
| `seasonality_index_cache` | Weekly Sun 05:00 | Full delete+rebuild | Stale until monthly fixed | Auto after monthly fix |
| `seasonality_profile` | Weekly Sun 05:00 | Full delete+rebuild | Stale until indices fixed | Auto after indices fix |
| `growth_opportunity` | Daily 06:30 | 30 days | **Scores based on stale rollup** | 🟡 Redetect after rollup |

### 3.2 Tables Safe to Keep

| Table | Reason |
|-------|--------|
| `acc_order` / `acc_order_line` | Raw transactional data — not derived |
| `acc_exchange_rate` / `ecb_exchange_rate` | FX rates are factual external data |
| `acc_finance_transaction` | Raw Amazon finance imports |
| `acc_ads_campaign` / `acc_ads_campaign_day` | Raw Amazon Ads data |
| `acc_product` / `acc_purchase_price` | Master data — not derived |
| `acc_return_item` / `acc_fba_customer_return` | Raw return data |
| `acc_inv_traffic_*` | Raw traffic telemetry |
| `acc_order_sync_state` | Watermark state |
| `acc_amazon_listing_registry` | Identity metadata |
| `acc_taxonomy_*` | Prediction/review data |

### 3.3 Safe Recomputation Plan

Execute in this exact order (dependency chain):

```
STEP 1 — Profitability Rollup (CRITICAL, ~5 min)
─────────────────────────────────────────────────
POST /api/v1/profitability/recompute
Body: { "days_back": 365 }

Or via Python:
  from app.services.profitability_service import recompute_rollups
  recompute_rollups(days_back=365)

This will:
  ✓ MERGE base data from acc_order + acc_order_line
  ✓ Enrich storage fees from acc_finance_transaction
  ✓ Enrich refunds (SKU-specific + revenue-proportional allocation)
  ✓ Enrich ad_spend from acc_ads_product_day
  ✓ Enrich refund_units from acc_order (is_refund=1)
  ✓ Recalc profit_pln = revenue - all_9_cost_categories
  ✓ Auto-rebuild acc_marketplace_profitability_rollup

STEP 2 — Executive Pipeline (auto, ~2 min)
──────────────────────────────────────────
POST /api/v1/executive/recompute
Body: { "days": 365 }

Or wait for 06:00 scheduler. Reads fresh rollup data.

STEP 3 — Seasonality Full Rebuild (~10 min)
───────────────────────────────────────────
Via Python (admin endpoint or direct):
  from app.services.seasonality_service import SeasonalityService
  svc = SeasonalityService()
  svc.build_monthly_metrics(months_back=36)
  svc.recompute_indices()
  svc.recompute_profiles()

STEP 4 — Strategy Re-detection (~3 min)
────────────────────────────────────────
POST /api/v1/strategy/detect
Body: { "days": 90 }

Or wait for 06:30 scheduler with fresh rollup data.

STEP 5 — Verify Health Score
─────────────────────────────
GET /api/v1/executive/overview
Check if health_score is non-null. If null, trigger:
  POST /api/v1/executive/recompute
```

### 3.4 Enrichment Atomicity Warning

`_enrich_rollup_from_finance()` in [profitability_service.py](apps/api/app/services/profitability_service.py) performs 6 separate UPDATE + COMMIT cycles within a single recompute run. If the process crashes between Step 4 (ad_spend) and Step 5 (profit recalc), the rollup will have **partially enriched data** with an **incorrect profit_pln**. Consider wrapping all enrichment steps in a single transaction.

---

## PART 4 — Query Layer Validation

### 4.1 FX Rate Centralization: ✅ CLEAN

All SQL modules use `build_fx_case_sql()` from `core/fx_service.py`. Zero hardcoded FX CASE blocks or fallback dicts remain. Verified in:
- mssql_store.py (9 usages)
- profit_engine.py (10 usages)
- order_pipeline.py (5 usages)
- sellerboard_history.py (2 usages)
- profitability_service.py (1 usage)

### 4.2 Deprecated Tables: ✅ CLEAN

`sellerboard_*` tables referenced only in one-off migration scripts, not in any production query path.

### 4.3 Query Issues Found

| # | Severity | Location | Issue |
|---|----------|----------|-------|
| Q1 | 🔴 CRITICAL | [profitability_service.py](apps/api/app/services/profitability_service.py) L314-318 | `get_profitability_orders()` uses `0 as fba_fees_pln, 0 as ad_cost_pln, 0 as refund_pln` — should JOIN to enriched data from finance/ads tables |
| Q2 | 🟡 MEDIUM | [executive_service.py](apps/api/app/services/executive_service.py) L603-609 | `recompute_executive_metrics()` reads from rollup but does NOT include `sessions`, `cvr_pct`, `inventory_risk` — these remain NULL in `executive_daily_metrics` |
| Q3 | 🟡 MEDIUM | [profitability_service.py](apps/api/app/services/profitability_service.py) enrichment | 6 separate COMMIT operations in enrichment pipeline — no atomic transaction for the full enrichment + profit recalc |
| Q4 | ℹ️ LOW | [kpi.py](apps/api/app/api/v1/kpi.py) L21-30 | KPI CM formula `Revenue - COGS - Fees - Logistics` excludes ads/refunds; this is intentionally CM1 but the response key `cm_pln` could mislead (should be labeled `cm1_pln`) |

### 4.4 Index Considerations

All production queries use `WITH (NOLOCK)` hints and `SET LOCK_TIMEOUT 30000` — correct for read-heavy analytics workload on Azure SQL.

The `OUTER APPLY` pattern for FX rates (date-specific fallback to latest) is efficient with an index on `(currency, rate_date DESC)`.

---

## PART 5 — UI Consistency

### 5.1 Metric Definition Alignment

| Metric | Backend Definition | Frontend Display | Aligned? |
|--------|--------------------|-----------------|----------|
| **CM1** | Revenue − COGS − AmazonFees − Logistics | `cm1_profit` from backend | ✅ |
| **CM2** | CM1 − Ads − Returns − Storage − Aged − Removal − Liquidation − Refund\_Finance − Shipping\_Surcharge − FBA\_Inbound | `cm2_profit` from backend | ✅ |
| **NP** | CM2 − Overhead (revenue-share allocated) | `np_profit` from backend | ✅ |
| **ACoS** | Ad Spend / Ad Sales × 100 | Backend `acos_pct` | ✅ |
| **TACoS** | Ad Spend / Total Revenue × 100 | Backend `tacos` | ✅ |
| **Return Rate** | Refund units / Units sold × 100 | Backend `return_rate_pct` | ✅ |
| **Margin %** | Profit / Revenue × 100 | Backend `margin_pct` or `cm1_percent` | ✅ |

### 5.2 Profit Definition Mixing Risk

| Page | Profit Tier Used | Source |
|------|-----------------|--------|
| Dashboard (KPI) | **CM1** (Revenue − COGS − Fees − Logistics) | kpi.py ORM |
| ProfitOverview | **NP** (full 9-cost rollup) | profitability_service (rollup) |
| ProductProfitTable | **CM1/CM2/NP** (user selectable via profitMode) | profit_engine (V2) |
| ExecOverview | **NP** (profit_pln from rollup) | executive_service |
| ProfitabilityOrders | **CM1** (contribution_margin_pln from acc_order) | profitability_service (order-level) |
| ProfitabilityProducts | **NP** (from rollup) | profitability_service |

**⚠️ INCONSISTENCY:** Dashboard shows **CM1** (no ads/refunds subtracted) while ProfitOverview and Executive pages show **NP** (all costs subtracted). A user comparing Dashboard profit to Executive profit will see different numbers for the same period/marketplace.

**Recommendation:** Either:
- Add a "Profit Tier" badge on Dashboard clarifying it shows CM1
- Or migrate Dashboard KPI to use the rollup-based NP calculation

### 5.3 Financial Metric Safety

| Function | Null/NaN Handling | Status |
|----------|-------------------|--------|
| `formatPLN()` | Handles null → "—" | ✅ |
| `formatPct()` | Handles null/undefined/NaN → "—" | ✅ |
| `formatDelta()` | Handles null → "—" | ✅ |

Frontend utility functions correctly handle edge cases. No risk of NaN display.

---

## PART 6 — Structured Output

### 6.1 Full Issue Registry (Priority Ordered)

| Priority | ID | Type | Component | Issue | Fix Effort |
|----------|----|------|-----------|-------|------------|
| **P0** | C1 | Backend | profitability_service.py | `get_profitability_orders()` returns hardcoded 0 for fba_fees, ad_cost, refund | 2h — JOIN to acc_finance_transaction + acc_ads_product_day |
| **P0** | F1 | Frontend | FinanceDashboard.tsx | Hardcoded `from: "2025-09-01"` — stale query range | 15min — dynamic date |
| **P0** | DB1 | Database | acc_sku_profitability_rollup | All data >7 days old uses pre-fix logic | 5min — trigger `recompute_rollups(days_back=365)` |
| **P1** | C2 | Backend | executive_service.py | `cvr_pct` and `inventory_risk` always NULL | 4h — compute from traffic/inventory data |
| **P1** | F2 | Frontend | FinanceDashboard.tsx | 5 mutations lack onError handlers | 1h — add toast notifications |
| **P1** | DB2 | Database | seasonality_monthly_metrics | All 36 months of historical data stale | 10min — trigger full rebuild |
| **P1** | Q3 | Backend | profitability_service.py | Non-atomic enrichment (6 commits) | 2h — wrap in single transaction |
| **P1** | UI1 | Frontend | 20+ pages | Local filter state instead of global store | 8h — migrate to globalFiltersStore |
| **P2** | C4 | Backend | kpi.py | CM formula name confusion (CM1 labeled as generic "cm") | 1h — rename response keys |
| **P2** | F3 | Frontend | SeasonalityOverview.tsx | MONTH_NAMES array access without bounds check | 15min |
| **P2** | F4 | Frontend | ManageAllInventory.tsx | Undefined value accumulation in grouping | 30min — null coalescing |
| **P2** | F5 | Frontend | PriceSimulator.tsx | Can send sale_price=0 to backend | 15min — validate >0 |
| **P2** | DB3 | Database | executive_health_score | No scheduler job found — may never auto-refresh | 30min — add to scheduler |
| **P2** | UI2 | UI | Dashboard vs Executive | Different profit tiers (CM1 vs NP) without labeling | 1h — add tier badges |
| **P3** | C3 | Backend | kpi.py | Extra fields (total_units, avg_order_value) not in TS types | 30min — update interfaces |
| **P3** | C5 | Backend | profitability overview | Duplicate tacos_pct / ad_spend_share_pct fields | 15min — remove duplicate |
| **P3** | FE1 | Frontend | 7 pages | Missing isError/error display | 4h — add error UI |
| **P3** | FE2 | Frontend | Multiple | Hardcoded thresholds (ACoS, return rate, scoring) | 4h — move to config/backend |

### 6.2 Recommended Fix Sequence

```
PHASE 1 — Immediate (Day 1) ─────────────────────────────────
  
  [1] Trigger profitability rollup recompute:
      POST /api/v1/profitability/recompute { "days_back": 365 }
  
  [2] Fix FinanceDashboard.tsx hardcoded date:
      - from: "2025-09-01"  →  dayjs().subtract(6, 'month').format('YYYY-MM-DD')
  
  [3] Wait for executive pipeline auto-run (06:00) after rollup

PHASE 2 — Critical Backend (Day 2-3) ───────────────────────

  [4] Fix get_profitability_orders() hardcoded zeros:
      - JOIN acc_finance_transaction for fba_fees, refund
      - JOIN acc_ads_product_day for ad_cost
      - Or: pull from acc_sku_profitability_rollup (pre-enriched)

  [5] Add onError handlers to FinanceDashboard mutations

  [6] Wrap enrichment pipeline in atomic transaction

PHASE 3 — Data Quality (Day 4-5) ───────────────────────────

  [7] Rebuild seasonality:
      build_monthly_metrics(months_back=36)
      recompute_indices()
      recompute_profiles()
  
  [8] Re-detect strategy opportunities:
      POST /api/v1/strategy/detect { "days": 90 }
  
  [9] Verify executive_health_score scheduler exists

PHASE 4 — Frontend Hardening (Week 2) ──────────────────────

  [10] Migrate Dashboard.tsx to globalFiltersStore
  [11] Migrate ProfitOverview.tsx to globalFiltersStore
  [12] Migrate Executive pages to globalFiltersStore
  [13] Add error states to 7 pages (use StrategyOverview.tsx as template)
  [14] Add profit tier badges to Dashboard ("CM1") and Executive ("NP")

PHASE 5 — Backend Enhancement (Week 3) ─────────────────────

  [15] Compute cvr_pct in executive: 
       CVR = orders / sessions × 100 (from acc_inv_traffic_rollup)
  [16] Compute inventory_risk in executive:
       Based on days_of_cover from acc_inventory_snapshot
  [17] Add executive_health_score to scheduler
  [18] Rename kpi cm_pln → cm1_pln for clarity
```

### 6.3 Code Locations to Update

| File | Line(s) | Change |
|------|---------|--------|
| [apps/api/app/services/profitability_service.py](apps/api/app/services/profitability_service.py) | 314-318 | Replace `0 as fba_fees_pln, 0 as ad_cost_pln, 0 as refund_pln` with JOINs |
| [apps/api/app/services/profitability_service.py](apps/api/app/services/profitability_service.py) | 783-1028 | Wrap 6 enrichment UPDATE+COMMIT in single transaction |
| [apps/api/app/services/executive_service.py](apps/api/app/services/executive_service.py) | 372-373 | Compute cvr_pct and inventory_risk from real data |
| [apps/api/app/services/executive_service.py](apps/api/app/services/executive_service.py) | 603-609 | Add sessions/cvr/inventory to executive metric computation |
| [apps/api/app/api/v1/kpi.py](apps/api/app/api/v1/kpi.py) | 21-30 | Rename cm_pln to cm1_pln or document tier |
| [apps/web/src/pages/FinanceDashboard.tsx](apps/web/src/pages/FinanceDashboard.tsx) | 143 | Dynamic date instead of "2025-09-01" |
| [apps/web/src/pages/FinanceDashboard.tsx](apps/web/src/pages/FinanceDashboard.tsx) | mutations | Add onError callbacks with toast |
| [apps/web/src/pages/SeasonalityOverview.tsx](apps/web/src/pages/SeasonalityOverview.tsx) | 159 | Add bounds check for MONTH_NAMES access |
| [apps/web/src/pages/ManageAllInventory.tsx](apps/web/src/pages/ManageAllInventory.tsx) | grouping | Add null coalescing to accumulations |
| [apps/web/src/pages/Dashboard.tsx](apps/web/src/pages/Dashboard.tsx) | filters | Migrate 7 useState to globalFiltersStore |
| [apps/web/src/pages/ProfitOverview.tsx](apps/web/src/pages/ProfitOverview.tsx) | filters | Migrate 2 useState to globalFiltersStore |
| [apps/web/src/pages/ExecOverview.tsx](apps/web/src/pages/ExecOverview.tsx) | filters | Migrate 3 useState to globalFiltersStore |
| [apps/api/app/scheduler.py](apps/api/app/scheduler.py) | health_score | Add scheduled job for executive_health_score refresh |

### 6.4 Safe Recomputation Commands Summary

```bash
# STEP 1: Profitability Rollup (REQUIRED FIRST)
curl -X POST http://localhost:8010/api/v1/profitability/recompute \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"days_back": 365}'

# STEP 2: Executive Pipeline 
curl -X POST http://localhost:8010/api/v1/executive/recompute \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"days": 365}'

# STEP 3: Strategy Re-detection
curl -X POST http://localhost:8010/api/v1/strategy/detect \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"days": 90}'

# STEP 4: Seasonality (Python shell or admin endpoint)
# python -c "
# from app.services.seasonality_service import SeasonalityService
# svc = SeasonalityService()
# svc.build_monthly_metrics(months_back=36)
# svc.recompute_indices()
# svc.recompute_profiles()
# "
```

---

## Appendix A — Scheduler Job Chain (37 Jobs)

```
00:00 ──────────────────────────────────────────
01:00  sync_listings_to_products
01:30  sync_amazon_listing_registry  
01:40  refresh_tkl_sql_cache
02:00  sync_purchase_prices
02:30  sync_ecb_exchange_rates
03:00  sync_finances (SF-06 safety gate)
03:20  fee_gap_recheck
04:00  sync_inventory_snapshots
04:30  sync_inventory_sales_traffic + seasonality_build_monthly
05:00  calc_profit (CM1 on orders)
05:30  cogs_data_quality_audit
05:45  profitability_rollup (re-syncs ads+finance, then MERGE)
06:00  executive_pipeline (reads fresh rollup)
06:30  strategy_detection
07:00  sync_amazon_ads
Every 15m: order_pipeline (10-step)
Weekly Sun 05:00: seasonality_recompute_profiles
Monthly 1st 09:00: opportunity_model_recalibration
```

## Appendix B — DB Table Dependency Graph

```
acc_order + acc_order_line (raw)
    │
    ├──► acc_sku_profitability_rollup (MERGE + enrichment)
    │        │
    │        ├──► acc_marketplace_profitability_rollup (aggregate)
    │        ├──► executive_daily_metrics (aggregate)
    │        │       └──► executive_health_score (compute)
    │        ├──► seasonality_monthly_metrics (36mo aggregate)
    │        │       ├──► seasonality_index_cache (compute)
    │        │       └──► seasonality_profile (classify)
    │        └──► growth_opportunity (detect + score)
    │                └──► decision_execution_record (track)
    │
    ├──► acc_finance_transaction (raw) ──► enrichment source
    └──► acc_ads_product_day (raw) ──► enrichment source
```

---

*End of Full-Stack Alignment Audit Report*
