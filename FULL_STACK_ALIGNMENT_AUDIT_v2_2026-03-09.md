# Full Stack Alignment Audit — ACC (Amazon Command Center)

**Date:** 2026-03-09  
**Auditor:** Senior Full-Stack Systems Auditor  
**Scope:** DB → Backend services → API endpoints → Frontend data fetch → UI components  

---

## Table of Contents

1. [Frontend Screen Map](#1-frontend-screen-map)
2. [API Endpoint Map](#2-api-endpoint-map)
3. [Mismatches Between Frontend and Backend](#3-mismatches-between-frontend-and-backend)
4. [DB Tables Requiring Recomputation](#4-db-tables-requiring-recomputation)
5. [Frontend Bugs or Crashes Detected](#5-frontend-bugs-or-crashes-detected)
6. [Recommended Fixes in Priority Order](#6-recommended-fixes-in-priority-order)
7. [Safe Recomputation Commands](#7-safe-recomputation-commands)
8. [Code Locations to Update](#8-code-locations-to-update)

---

## 1. Frontend Screen Map

**77 pages** identified across 12 functional modules:

### Core Dashboard & Profit (7 pages)
| Page | Route | Primary API Endpoints |
|------|-------|----------------------|
| Dashboard | `/dashboard` | `GET /kpi/summary`, `GET /kpi/revenue-chart`, `GET /kpi/marketplace-breakdown` (top drivers), alerts |
| ProfitOverview | `/profit/overview` | `GET /profitability/overview` |
| ProductProfitTable | `/profit/products` | `GET /profit/v2/products`, `GET /profit/v2/products/export.xlsx` |
| ProductDrilldown | `/profit/drilldown` | `GET /profit/v2/drilldown` |
| LossOrders | `/profit/loss-orders` | `GET /profit/v2/loss-orders` |
| DataQuality | `/profit/data-quality` | `GET /profit/v2/data-quality`, AI match endpoints |
| ProfitExplorer | `/profit` | `GET /profit/orders` (V1 legacy) |

### Profitability Module (3 pages)
| Page | Route | Primary API Endpoints |
|------|-------|----------------------|
| ProfitabilityProducts | `/profitability/products` | `GET /profitability/products` |
| ProfitabilityOrders | `/profitability/orders` | `GET /profitability/orders` |
| PriceSimulator | `/profitability/simulator` | `POST /profitability/simulate` |

### Executive (3 pages)
| Page | Route | Primary API Endpoints |
|------|-------|----------------------|
| ExecOverview | `/exec/overview` | `GET /executive/overview`, `POST /executive/recompute` |
| ExecProducts | `/exec/products` | `GET /executive/products` |
| ExecMarketplaces | `/exec/marketplaces` | `GET /executive/marketplaces` |

### Strategy & Decision Intelligence (8 pages)
| Page | Route | Primary API Endpoints |
|------|-------|----------------------|
| StrategyOverview | `/strategy/overview` | `GET /strategy/overview` |
| StrategyOpportunities | `/strategy/opportunities` | `GET /strategy/opportunities` |
| StrategyMarketExpansion | `/strategy/market-expansion` | `GET /strategy/market-expansion` |
| StrategyBundles | `/strategy/bundles` | `GET /strategy/bundles` |
| StrategyPlaybooks | `/strategy/playbooks` | `GET /strategy/playbooks` |
| StrategyExperiments | `/strategy/experiments` | `GET /strategy/experiments` |
| StrategyOutcomes | `/strategy/outcomes` | `GET /strategy/decisions/outcomes` |
| StrategyLearning | `/strategy/learning` | `GET /strategy/decisions/learning` |

### Seasonality (6 pages)
| Page | Route | Primary API Endpoints |
|------|-------|----------------------|
| SeasonalityOverview | `/seasonality/overview` | `GET /seasonality/overview` |
| SeasonalityMap | `/seasonality/map` | `GET /seasonality/map` |
| SeasonalityEntities | `/seasonality/entities` | `GET /seasonality/entities` |
| SeasonalityEntityDetail | `/seasonality/entity/:type/:id` | `GET /seasonality/entity/{type}/{id}` |
| SeasonalityClusters | `/seasonality/clusters` | `GET /seasonality/overview` (cluster view) |
| SeasonalityOpportunities | `/seasonality/opportunities` | `GET /seasonality/opportunities` |

### Inventory & FBA (12 pages)
| Page | Route | Primary API Endpoints |
|------|-------|----------------------|
| InventoryOverview | `/inventory/overview` | `GET /inventory/overview` |
| ManageAllInventory | `/inventory/all` | `GET /inventory/all` |
| InventoryFamilies | `/inventory/families` | `GET /inventory/families` |
| InventoryDrafts | `/inventory/drafts` | `GET /inventory/drafts`, draft CRUD |
| FbaOverview | `/fba/overview` | `GET /fba/overview`, `GET /fba/diagnostics/report-status` |
| FbaInventory | `/fba/inventory` | `GET /fba/inventory` |
| FbaReplenishment | `/fba/replenishment` | `GET /fba/replenishment/suggestions` |
| FbaInbound | `/fba/inbound` | `GET /fba/inbound/shipments` |
| FbaAgedStranded | `/fba/aged-stranded` | `GET /fba/aged`, `GET /fba/stranded` |
| FbaBundles | `/fba/bundles` | `GET /fba/bundles` |
| FbaScorecard | `/fba/scorecard` | `GET /fba/kpi/scorecard` |
| Inventory (legacy) | `/inventory` | `GET /inventory/` |

### Family Mapper (4 pages)
| Page | Route | Primary API Endpoints |
|------|-------|----------------------|
| FamilyMapper | `/families` | `GET /families`, matching/rebuild triggers |
| FamilyDetail | `/families/:id` | `GET /families/{id}`, children, links, coverage, issues, restructure |
| FixPackages | `/families/fix-packages` | `GET /families/{id}/fix-packages` |
| ReviewQueue | `/families/review` | `GET /families/review` |

### Finance (3 pages)
| Page | Route | Primary API Endpoints |
|------|-------|----------------------|
| FinanceDashboard | `/finance/dashboard` | `GET /finance/dashboard`, completeness, gap diagnostics |
| FinanceLedger | `/finance/ledger` | `GET /finance/ledger` |
| FinanceReconciliation | `/finance/reconciliation` | `GET /finance/reconcile/payouts` |

### Content Ops (7 pages)
| Page | Route | Primary API Endpoints |
|------|-------|----------------------|
| ContentDashboard | `/content/dashboard` | Content overview |
| ContentOps | `/content/ops` | `GET /content/tasks` |
| ContentEditor | `/content/editor` | Version CRUD |
| ContentCompliance | `/content/compliance` | Policy checks |
| ContentAssets | `/content/assets` | `GET /content/assets` |
| ContentPublish | `/content/publish` | `GET /content/publish/jobs` |
| ContentStudio | `/content/studio` | Content diff |

### Tax Compliance (7 pages)
| Page | Route | Primary API Endpoints |
|------|-------|----------------------|
| TaxOverview | `/tax/overview` | `GET /tax/overview` |
| TaxVatClassification | `/tax/classification` | `GET /tax/vat-events` |
| TaxOss | `/tax/oss` | `GET /tax/oss/summary` |
| TaxLocalVat | `/tax/local-vat` | `GET /tax/local-vat` |
| TaxEvidence | `/tax/evidence` | Evidence endpoints |
| TaxFbaMovements | `/tax/movements` | Movement endpoints |
| TaxReconciliation | `/tax/reconciliation` | Tax reconciliation |

### Other (Ads, Pricing, Planning, AI, Monitoring)
| Page | Route | Primary API Endpoints |
|------|-------|----------------------|
| Ads | `/ads` | `GET /ads/summary`, `GET /ads/chart`, `GET /ads/top-campaigns` |
| Pricing | `/pricing` | `GET /pricing/offers`, `GET /pricing/buybox-stats` |
| Planning | `/planning` | `GET /planning/months`, `GET /planning/vs-actual` |
| AIRecommendations | `/ai` | `GET /ai/recommendations`, `GET /ai/summary` |
| Alerts | `/alerts` | `GET /alerts`, alert rules CRUD |
| Jobs | `/jobs` | `GET /jobs`, `POST /jobs/run` |
| ProductTasks | `/profit/tasks` | `GET /profit/v2/tasks` |
| ImportProductsPage | `/import-products` | `GET /import-products` |
| Login | `/login` | `POST /auth/token` |

### MISSING Frontend Pages (Backend Ready, No UI)
| Backend Module | Endpoints Available | Status |
|----------------|---------------------|--------|
| **Return Tracker** | 8 endpoints under `/returns/*` | NO frontend pages |
| **FBA Fee Audit** | 3 endpoints under `/fba/fee-audit/*` | No dedicated page |
| **Courier Cost Mapping** | 6 endpoints under `/courier/*` | No frontend pages |
| **DHL Integration** | 5 endpoints under `/dhl/*` | No frontend pages |
| **GLS Integration** | 5 endpoints under `/gls/*` | No frontend pages |
| **Guardrails** | 4 endpoints under `/guardrails/*` | No frontend pages |

---

## 2. API Endpoint Map

**31 router files** registered. **~185 endpoints** total.

### Endpoint Count by Module
| Module | Prefix | Endpoint Count | Frontend Pages Using |
|--------|--------|----------------|---------------------|
| Health | `/health` | 5 | Jobs page (partial) |
| Auth | `/auth` | 4 | Login |
| Profit V1 | `/profit` | 3 | ProfitExplorer |
| Profit V2 | `/profit/v2` | 25+ | ProductProfitTable, Drilldown, LossOrders, DataQuality, Tasks |
| KPI | `/kpi` | 3 | Dashboard |
| Ads | `/ads` | 6 | Ads |
| Pricing | `/pricing` | 3 | Pricing |
| Inventory | `/inventory` | 15+ | InventoryOverview, ManageAll, Families, Drafts, Settings, Jobs |
| Families | `/families` | 15+ | FamilyMapper, FamilyDetail, FixPackages, ReviewQueue |
| Jobs | `/jobs` | 6 | Jobs |
| Alerts | `/alerts` | 6 | Alerts |
| Returns | `/returns` | 8 | **NONE** |
| FBA Ops | `/fba` | 20+ | FbaOverview, Inventory, Replenishment, Inbound, AgedStranded, Scorecard |
| Finance | `/finance` | 18+ | FinanceDashboard, Ledger, Reconciliation |
| Audit/Controlling | `/audit` | 9 | (Possibly internal) |
| Planning | `/planning` | 5 | Planning |
| AI | `/ai` | 4 | AIRecommendations |
| Content | `/content` | 18+ | ContentOps, Editor, Compliance, Assets, Publish, Studio |
| Import Products | `/import-products` | 2 | ImportProductsPage |
| Profitability | `/profitability` | 6 | ProfitabilityProducts, ProfitabilityOrders, PriceSimulator |
| Executive | `/executive` | 4 | ExecOverview, ExecProducts, ExecMarketplaces |
| Strategy | `/strategy` | 12+ | StrategyOverview, Opportunities, MarketExpansion, etc. |
| Decisions | `/strategy/decisions` | 8+ | StrategyOutcomes, StrategyLearning |
| Seasonality | `/seasonality` | 5+ | SeasonalityOverview, Map, Entities, Opportunities |
| GLS | `/gls` | 5 | **NONE** |
| DHL | `/dhl` | 5 | **NONE** |
| Courier | `/courier` | 6 | **NONE** |
| Tax | `/tax` | 6+ | TaxOverview, VatClassification, Oss, etc. |
| Guardrails | `/guardrails` | 4 | **NONE** |

---

## 3. Mismatches Between Frontend and Backend

### 3.1 CRITICAL — Schema Drift (TypeScript vs Pydantic)

#### 3.1.1 ProductProfitItem: 13 Missing Fields in Frontend

| Backend Field (Pydantic) | In TypeScript? | Impact |
|:-------------------------|:---------------|:-------|
| `refund_orders: int` | MISSING | Refund order count invisible in UI |
| `refund_units: int` | MISSING | Refund unit count invisible |
| `refund_cost_pln: float` | MISSING | Refund cost invisible |
| `return_cogs_recovered_pln: float` | MISSING | CM2 component hidden |
| `return_cogs_write_off_pln: float` | MISSING | CM2 component hidden |
| `return_cogs_pending_pln: float` | MISSING | CM2 component hidden |
| `cm1_adjusted: float` | MISSING | Adjusted CM1 hidden |
| `refund_finance_pln: float` | MISSING | Refund finance fees hidden |
| `shipping_surcharge_pln: float` | MISSING | Shipping surcharge hidden |
| `fba_inbound_fee_pln: float` | MISSING | FBA inbound fee hidden |
| `promo_cost_pln: float` | MISSING | Promo discount costs hidden |
| `warehouse_loss_pln: float` | MISSING | Warehouse loss hidden |
| `amazon_other_fee_pln: float` | MISSING | Other Amazon fees hidden |

**Files:** `apps/web/src/lib/api.ts:1367` vs `apps/api/app/schemas/profit_v2.py:14`

#### 3.1.2 ProductProfitItem: Frontend Fields NOT in Backend

| TypeScript Field | In Backend? | Impact |
|:-----------------|:------------|:-------|
| `refund_rate?: number` | NOT IN SCHEMA (backend has `return_rate`) | Column shows blank — field name mismatch |
| `roas?: number` | NOT IN SCHEMA | Column always empty — ROAS in Ads only |

#### 3.1.3 ProductProfitSummary: 6 Missing Fields in Frontend

| Backend Field | In TypeScript? |
|:--------------|:---------------|
| `refund_shipped_orders: int` | MISSING |
| `refund_shipped_units: int` | MISSING |
| `refund_shipped_cost_pln: float` | MISSING |
| `total_return_cogs_recovered_pln: float` | MISSING |
| `total_return_cogs_write_off_pln: float` | MISSING |
| `total_return_cogs_pending_pln: float` | MISSING |

#### 3.1.4 KPISummary: 4 Missing Backend Fields in Frontend

| Backend Field | In TypeScript? | Impact |
|:--------------|:---------------|:-------|
| `total_acos: Optional[float]` | MISSING | ACOS not shown in KPI summary |
| `avg_order_value_pln: float` | MISSING | AOV not shown |
| `date_from: date` | MISSING | Period context missing |
| `date_to: date` | MISSING | Period context missing |

**Files:** `apps/web/src/lib/api.ts:74` vs `apps/api/app/schemas/kpi.py:29`

#### 3.1.5 MarketplaceKPI: 1 Missing Field

| Backend Field | In TypeScript? |
|:--------------|:---------------|
| `avg_order_value_pln: float` | MISSING |

#### 3.1.6 DrilldownItem: 3 Missing Refund Fields

| Backend Field | In TypeScript? |
|:--------------|:---------------|
| `is_refund: bool` | MISSING |
| `refund_type: Optional[str]` | MISSING |
| `refund_amount_pln: Optional[float]` | MISSING |

### 3.2 HIGH — Backend Modules Without Frontend Consumption

| Backend Module | Endpoints | Business Impact |
|:---------------|:----------|:----------------|
| **Return Tracker** | 8 endpoints | Return lifecycle data invisible |
| **Courier Cost Mapping** | 6 endpoints | Logistics cost analysis inaccessible |
| **GLS/DHL Integration** | 10 endpoints | Shipment tracking not in UI |
| **Guardrails** | 4 endpoints | System health checks invisible |
| **FBA Fee Audit** | 3 endpoints | Fee anomaly detection hidden |

### 3.3 MEDIUM — KPI total_units Bug

The KPI endpoint at `apps/api/app/api/v1/kpi.py:139` sets `units=orders` (order count, not unit count). At line 169: `total_units=total_orders`.

This means **units and orders are identical**, which is incorrect. Units should be `SUM(quantity_shipped)` from `acc_order_line`. The Top Drivers path (line 425) correctly uses `func.sum(OrderLine.quantity_shipped)`, so the bug is only in the KPI summary path.

---

## 4. DB Tables Requiring Recomputation

### 4.1 CRITICAL — Full Recompute Required

| Table | Service | Lookback | Endpoint |
|:------|:--------|:---------|:---------|
| `acc_sku_profitability_rollup` | `profitability_service.recompute_rollups()` | 365 days | `POST /profitability/recompute?days_back=365` |
| `acc_marketplace_profitability_rollup` | Cascades from SKU rollup | 365 days | Automatic with above |

**Reason:** Profit calc fixes (fee allocation, FX logic, ad spend enrichment, return enrichment) invalidate all historical rollup rows. Daily scheduler only recomputes last 7 days.

### 4.2 HIGH — Rebuild After Rollup Fix (Cascade Chain)

| Table | Service | Depends On | Command |
|:------|:--------|:-----------|:--------|
| `seasonality_monthly_metrics` | `build_monthly_metrics(36)` | SKU rollup | Python call |
| `seasonality_index_cache` | `recompute_indices()` | Monthly metrics | Python call |
| `seasonality_profile` | `recompute_profiles()` | Index cache | Python call |
| `seasonality_opportunity` | `detect_seasonality_opportunities()` | Profiles | Python call |
| `executive_daily_metrics` | `recompute_executive_metrics()` | SKU rollup | `POST /executive/recompute` |
| `executive_health_score` | Within executive pipeline | Daily metrics | Cascaded |
| `executive_opportunities` | Within executive pipeline | Daily+SKU | Cascaded |
| `growth_opportunity` | `run_strategy_detection(30)` | SKU rollup | `POST /strategy/jobs/run` |

### 4.3 MEDIUM — Partial Recompute

| Table | Action |
|:------|:-------|
| `acc_return_daily_summary` | `POST /returns/rebuild-daily-summary` |
| `acc_inv_traffic_rollup` | Auto-rebuilds on next sync (04:30) |

### 4.4 SAFE — No Recompute Needed

All raw data tables (`acc_order`, `acc_order_line`, `acc_finance_event*`, `acc_exchange_rate`, `acc_purchase_price`, `acc_product`, `acc_ads_*`, `acc_inventory`, `acc_return_item`, `acc_fba_customer_return`) contain source data that is not affected by calculation fixes.

Decision intelligence tables (`opportunity_execution`, `opportunity_outcome`, `decision_learning`) are LOW risk tracking data.

---

## 5. Frontend Bugs or Crashes Detected

### 5.1 Schema Mismatches (Silent Data Loss)

| Bug ID | Severity | Description | Location |
|:-------|:---------|:------------|:---------|
| BUG-001 | HIGH | 13 backend fields missing from `ProductProfitItem` TS interface — CM2 components & return COGS silently dropped | `api.ts:1367` |
| BUG-002 | MEDIUM | `refund_rate` in TS doesn't match backend's `return_rate` — column always empty | `api.ts:1417` |
| BUG-003 | MEDIUM | `roas` in TS `ProductProfitItem` but backend doesn't compute it — always empty | `api.ts:1419` |
| BUG-004 | LOW | KPI summary missing `total_acos`, `avg_order_value_pln` in TS | `api.ts:74` |
| BUG-005 | LOW | DrilldownItem missing `is_refund`, `refund_type`, `refund_amount_pln` | `api.ts:1592` |
| BUG-006 | LOW | KPI `total_units` equals `total_orders` (not actual item units) | `kpi.py:169` |

### 5.2 Missing UI Capabilities

| Issue | Description | Business Impact |
|:------|:------------|:----------------|
| No Return Tracker UI | Backend has 8 endpoints | Warehouse team can't track returns |
| No FBA Fee Audit page | Backend detects fee anomalies | Potential EUR overcharges undetected |
| No Courier Cost page | Backend maps DHL/GLS costs | Logistics cost analysis inaccessible |

### 5.3 Positive Findings (No Issues)

- All financial metrics delegate to backend (zero client-side profit formulas)
- `formatPct()`/`formatCurrency()` handle null/NaN safely → "—"
- React Query `keepPreviousData` prevents flash on filter changes
- Error states extract `detail` from Axios error responses
- No hardcoded fee percentages or margins in frontend code
- CM1/CM2/NP definitions are consistent across all screens
- ACOS/TACOS displayed from backend, not recomputed client-side

---

## 6. Recommended Fixes in Priority Order

### P0 — Critical (Fix Immediately)

| # | Fix | Effort | Files |
|:--|:----|:-------|:------|
| 1 | **Recompute profitability rollups** (365 days) | 1-3h runtime | API call |
| 2 | **Add 13 missing fields to `ProductProfitItem` TS interface** | 15 min | `api.ts:1367` |
| 3 | **Fix `refund_rate` → `return_rate` naming mismatch** in TS | 5 min | `api.ts:1417` |
| 4 | **Add 6 missing fields to `ProductProfitSummary` TS interface** | 10 min | `api.ts:1427` |

### P1 — High (This Sprint)

| # | Fix | Effort | Files |
|:--|:----|:-------|:------|
| 5 | Add `KPISummary` missing fields (`total_acos`, `avg_order_value_pln`) to TS | 10 min | `api.ts:74` |
| 6 | Add `MarketplaceKPI.avg_order_value_pln` to TS | 5 min | `api.ts:89` |
| 7 | Fix KPI `total_units` — should be `SUM(quantity_shipped)` not order count | 30 min | `kpi.py:169` |
| 8 | Add DrilldownItem refund fields to TS | 10 min | `api.ts:1592` |
| 9 | Rebuild seasonality chain after rollup fix | 1h runtime | See §7 |
| 10 | Rebuild executive pipeline after rollup fix | 30 min runtime | API call |
| 11 | Re-run strategy detection after rollup fix | 15 min runtime | API call |
| 12 | Remove `roas` from `ProductProfitItem` TS (or implement in backend) | 5 min | `api.ts:1419` |

### P2 — Medium (Next Sprint)

| # | Fix | Effort |
|:--|:----|:-------|
| 13 | Build Return Tracker frontend page (Dashboard + Items list) | 2-3 days |
| 14 | Build FBA Fee Audit frontend page | 1-2 days |
| 15 | Add ProductProfitTable columns for new CM2 components | 1-2h |
| 16 | Add refund badge to ProductDrilldown | 1h |

### P3 — Low (Backlog)

| # | Fix | Effort |
|:--|:----|:-------|
| 17 | Build Courier Cost analysis page | 2-3 days |
| 18 | Build GLS/DHL tracking page | 1-2 days |
| 19 | Build Guardrails dashboard page | 1 day |
| 20 | Unify Dashboard to use GlobalFilterBar | 2h |

---

## 7. Safe Recomputation Commands

### Phase 1: Core Profitability (Run FIRST)

```powershell
# Option A: Via API (safe, with timeouts)
# POST /api/v1/profitability/recompute with days_back=365
Invoke-RestMethod -Uri "http://localhost:8010/api/v1/profitability/recompute" `
  -Method POST -Headers @{Authorization="Bearer $TOKEN"; "Content-Type"="application/json"} `
  -Body '{"days_back": 365}'

# Option B: Direct Python
cd C:\ACC\apps\api
python -c "
from datetime import date, timedelta
from app.core.db_connection import connect_acc
from app.services.profitability_service import recompute_rollups
conn = connect_acc()
try:
    recompute_rollups(conn, date_from=date(2025, 3, 9), date_to=date.today())
    conn.commit()
finally:
    conn.close()
print('Phase 1 done: profitability rollups recomputed')
"
```

### Phase 2: Seasonality Chain (Run AFTER Phase 1)

```powershell
cd C:\ACC\apps\api
python -c "
from app.core.db_connection import connect_acc
from app.services.seasonality_service import SeasonalityService

conn = connect_acc()
svc = SeasonalityService(conn)
try:
    print('Step 1/4: Building monthly metrics (36 months)...')
    svc.build_monthly_metrics(months_back=36)
    conn.commit()

    print('Step 2/4: Recomputing indices...')
    svc.recompute_indices()
    conn.commit()

    print('Step 3/4: Recomputing profiles...')
    svc.recompute_profiles()
    conn.commit()

    print('Step 4/4: Detecting opportunities...')
    from app.services.seasonality_opportunity_engine import SeasonalityOpportunityEngine
    eng = SeasonalityOpportunityEngine(conn)
    eng.detect_opportunities()
    conn.commit()
finally:
    conn.close()
print('Phase 2 done: seasonality chain rebuilt')
"
```

### Phase 3: Executive & Strategy (Run AFTER Phase 1, parallel OK)

```powershell
# Executive pipeline
Invoke-RestMethod -Uri "http://localhost:8010/api/v1/executive/recompute" `
  -Method POST -Headers @{Authorization="Bearer $TOKEN"}

# Strategy detection
Invoke-RestMethod -Uri "http://localhost:8010/api/v1/strategy/jobs/run" `
  -Method POST -Headers @{Authorization="Bearer $TOKEN"; "Content-Type"="application/json"} `
  -Body '{"job_type": "strategy_detection", "days_back": 30}'
```

### Phase 4: Returns Summary (Independent, Run Anytime)

```powershell
Invoke-RestMethod -Uri "http://localhost:8010/api/v1/returns/rebuild-daily-summary" `
  -Method POST -Headers @{Authorization="Bearer $TOKEN"}
```

### Safety Notes
- All recomputations use `SET LOCK_TIMEOUT 30000` (30s lock timeout)
- Rollup recompute uses MERGE upsert (idempotent, safe to re-run)
- Seasonality chain is sequential — do NOT parallelize steps
- Executive + Strategy can run in parallel after Phase 1
- No data deletion — all operations are upserts or inserts

---

## 8. Code Locations to Update

### Frontend (TypeScript) — `apps/web/src/lib/api.ts`

| Line | Change |
|:-----|:-------|
| ~1367 | Add to `ProductProfitItem`: `refund_orders`, `refund_units`, `refund_cost_pln`, `return_cogs_recovered_pln`, `return_cogs_write_off_pln`, `return_cogs_pending_pln`, `cm1_adjusted`, `refund_finance_pln`, `shipping_surcharge_pln`, `fba_inbound_fee_pln`, `promo_cost_pln`, `warehouse_loss_pln`, `amazon_other_fee_pln` |
| ~1417 | Rename `refund_rate` → `return_rate` |
| ~1419 | Remove `roas` (or implement in backend) |
| ~1427 | Add to `ProductProfitSummary`: `refund_shipped_orders`, `refund_shipped_units`, `refund_shipped_cost_pln`, `total_return_cogs_recovered_pln`, `total_return_cogs_write_off_pln`, `total_return_cogs_pending_pln` |
| ~74 | Add to `KPISummary`: `total_acos`, `avg_order_value_pln`, `date_from`, `date_to` |
| ~89 | Add to `MarketplaceKPI`: `avg_order_value_pln` |
| ~1592 | Add to `DrilldownItem`: `is_refund`, `refund_type`, `refund_amount_pln` |

### Backend (Python)

| File | Line | Change |
|:-----|:-----|:-------|
| `apps/api/app/api/v1/kpi.py` | ~169 | Fix `total_units`: query should use `SUM(acc_order_line.quantity_shipped)` not `COUNT(acc_order.id)` |
| `apps/api/app/services/profit_engine.py` | ~2037 | Add `dbo.` prefix: `FROM dbo.acc_ads_campaign_day cd WITH (NOLOCK)` |

### UI Components (New Columns/Badges)

| File | Change |
|:-----|:-------|
| `ProductProfitTable.tsx` | Add columns for new CM2 components after TS interface update |
| `ProductDrilldown.tsx` | Add refund badge using `is_refund` + `refund_type` fields |
| `Dashboard.tsx` | Consider showing `avg_order_value_pln` and `total_acos` in KPI cards |

---

## Summary Statistics

| Category | Count |
|:---------|:------|
| Frontend pages | 77 |
| Backend endpoints | ~185 |
| Schema mismatches (TS vs Pydantic) | 22 fields |
| Backend modules without frontend | 5 modules |
| Tables needing full recompute | 2 (CRITICAL) |
| Tables needing cascade rebuild | 8 (HIGH) |
| Tables safe to keep | 15+ |
| Frontend bugs detected | 6 |
| P0 fixes | 4 |
| P1 fixes | 8 |
| P2 fixes | 4 |
| P3 fixes | 4 |

### Overall Assessment

**Architecture quality: GOOD.** The backend profit engine is well-designed with proper CM1/CM2/NP layer separation. Frontend correctly delegates all financial calculations to the backend (zero client-side profit formulas). SQL security is excellent (WITH NOLOCK, SET LOCK_TIMEOUT, parameterized queries).

The main issues are:
1. **Schema drift** — 22 fields added to backend schemas after frontend was built
2. **Stale aggregate data** — rollup tables need full historical recompute
3. **Missing frontend pages** — 5 backend modules have no UI (Return Tracker most important)
4. **Minor naming mismatches** — `refund_rate` vs `return_rate`, missing `dbo.` prefix

The recomputation plan is safe (MERGE upserts, lock timeouts, idempotent operations) and can be executed without downtime.
