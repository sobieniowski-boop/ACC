# ACC Implementation Roadmap & Sprint Plan
### Date: 2026-03-11
### Based on: ACC Backend Architecture Audit 2026-03-11
### Methodology: Multi-Agent Analysis (11 perspectives)

---

## Agent Perspectives Applied

| Agent | Role in This Plan |
|---|---|
| **Agents Orchestrator** | Overall pipeline sequencing, dependency tracking, quality gates |
| **Backend Architect** | Technical decomposition, module boundaries, risk assessment |
| **Frontend Developer** | UI impact analysis, component planning, frontend sprints |
| **AI Engineer** | ML/AI module roadmap, data pipeline requirements |
| **DevOps** | CI/CD, containerization, deployment strategy, infrastructure |
| **Sprint Prioritizer** | RICE scoring, sprint composition, capacity planning |
| **Trend Researcher** | Industry alignment, technology choices, competitive positioning |
| **Feedback Synthesizer** | User pain points → priority mapping |
| **Analytics Reporter** | Metrics, KPIs, success criteria per sprint |
| **Finance Tracker** | Financial impact, ROI per initiative, cost of delay |
| **Data Analytics** | Data model strategy, quality metrics, reporting pipeline |

---

# PART I: STRATEGIC DIRECTION

## 1. Vision Statement

**From:** Collection of vertical slices with duplicated infrastructure
**To:** Event-driven Amazon Command Center with canonical data model, unified action gateway, and intelligence layer

### North Star Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                        OPERATOR CONSOLE                         │
│              (React SPA — 81 pages, Recharts, Radix)            │
├─────────────────────────────────────────────────────────────────┤
│                         API GATEWAY                             │
│                  (FastAPI — 425+ endpoints)                      │
├──────────┬──────────┬──────────┬──────────┬─────────────────────┤
│INTELLIGENCE│ EXECUTION │INGESTION │COMPLIANCE│    LOGISTICS       │
│  profit   │content_ops│ orders   │   tax    │    dhl/gls         │
│  strategy │fba_ops   │ listings │          │    cost_est         │
│  pricing  │repricing │ pricing  │          │    billing          │
│  executive│family_map│ finance  │          │                     │
│  seasonal │actions   │ inventory│          │                     │
├──────────┴──────────┴──────────┴──────────┴─────────────────────┤
│                     PLATFORM LAYER                               │
│  event_bus │ rate_governor │ action_center │ job_scheduler        │
│  account_hub │ shared/db │ shared/cache │ shared/fx              │
├─────────────────────────────────────────────────────────────────┤
│                     DOMAIN LAYER                                 │
│  canonical_product │ marketplace_mapping │ fee_taxonomy           │
├─────────────────────────────────────────────────────────────────┤
│                     CONNECTORS                                   │
│  SP-API (12) │ Ads API (4) │ DHL │ GLS │ MSSQL │ Redis │ SQS   │
├─────────────────────────────────────────────────────────────────┤
│                     INFRASTRUCTURE                               │
│  Azure SQL │ Redis 6380 │ SQS │ Azure Container │ Blob Storage   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. RICE Prioritization Matrix

*Sprint Prioritizer perspective — scoring all major initiatives*

| Initiative | Reach | Impact | Confidence | Effort (weeks) | RICE Score | Priority |
|---|---|---|---|---|---|---|
| Fix finance transaction dedup | 9 mkts | 3.0 | 95% | 0.5 | **51.3** | **P0** |
| Fix inventory upsert | 9 mkts | 2.0 | 95% | 0.5 | **34.2** | **P0** |
| Fix asyncio deprecation | All jobs | 2.0 | 100% | 0.3 | **60.0** | **P0** |
| Kill legacy profit_service.py | Dev team | 1.0 | 90% | 0.5 | **16.2** | P1 |
| Extract shared utilities | Dev team | 2.0 | 85% | 1.5 | **10.2** | P1 |
| Split scheduler.py | Dev team | 2.0 | 80% | 1.5 | **9.6** | P1 |
| Canonical product model | 9 mkts | 3.0 | 70% | 5.0 | **3.8** | P0-strategic |
| Split profit_engine.py | Dev team | 2.0 | 80% | 2.5 | **5.8** | P1 |
| Split content_ops.py | Dev team | 2.0 | 80% | 2.5 | **5.8** | P1 |
| Internal event bus | 9 mkts | 3.0 | 65% | 3.0 | **5.9** | P1 |
| Action center | 9 mkts | 2.0 | 60% | 2.5 | **4.3** | P1 |
| CI/CD pipeline | Dev team | 2.0 | 90% | 1.0 | **16.2** | P1 |
| Frontend test setup | Dev team | 1.0 | 90% | 1.0 | **8.1** | P2 |
| Backend test coverage 60% | Dev team | 2.0 | 70% | 6.0 | **2.1** | P2-ongoing |
| Repricing engine | Revenue | 3.0 | 40% | 6.0 | **1.8** | P3 |
| Competitor intelligence | Revenue | 2.0 | 50% | 4.0 | **2.3** | P3 |

### Financial Impact Assessment (Finance Tracker perspective)

| Risk | Estimated Annual Cost if Unaddressed | Fix Cost |
|---|---|---|
| Duplicate finance transactions | Up to **5-15% margin error** per marketplace | 0.5 week |
| Duplicate inventory snapshots | Wrong DOI → bad replenishment → **€10-50k** stockout/overstock | 0.5 week |
| Python 3.12+ breakage | **Full job pipeline down** when Azure upgrades runtime | 0.3 week |
| No canonical product model | **€20-50k/year** in manual cross-market reconciliation | 5 weeks |
| 3 profit paths diverging | **Unknowable** — financial reporting cannot be trusted | 2 weeks |

**ROI verdict**: Fixing P0 items costs ~1.3 weeks of effort and prevents €50-200k/year in data quality issues. This is the highest-ROI work possible.

---

## 3. Technology Direction (Trend Researcher perspective)

### Aligned with Industry Trends
| Decision | Trend Alignment | Rationale |
|---|---|---|
| Keep FastAPI + async | ✅ Industry standard | Python async ecosystem mature; FastAPI is #1 for API development |
| Event-driven via SQS | ✅ Cloud-native | AWS native; matches Amazon's own SP-API notification model |
| React + Vite + TanStack Query | ✅ Modern frontend | Dominant stack for data-heavy SPAs in 2026 |
| Azure SQL as primary DB | ⚠️ Pragmatic | Not trendy but solid; team knows it. Migration would be high-cost, low-value |
| Zustand for state | ✅ Lightweight | Correct choice for server-state-heavy app (TanStack Query does the heavy lifting) |
| No GraphQL | ✅ Correct | REST is simpler for internal tool; GraphQL overhead not justified |

### Technology Gaps to Close
| Gap | Recommendation | Sprint |
|---|---|---|
| No CI/CD | GitHub Actions (Docker build + test + deploy to Azure) | Sprint 1 |
| No frontend tests | Vitest + Testing Library + Playwright for E2E | Sprint 2 |
| No backend test coverage | pytest coverage target 60% on critical paths | Ongoing |
| No OpenTelemetry | Add OTEL tracing to SP-API + Ads API connectors | Sprint 4 |
| No feature flags system | Use existing `core/config.py` flags — formalize pattern | Sprint 3 |

### AI/ML Direction (AI Engineer perspective)

| Current AI Capability | Status | Next Step |
|---|---|---|
| `ai_service.py` — GPT-based content recommendations | ✅ Working | Expand to multi-language content generation |
| `ai_product_matcher.py` — Product matching | ✅ Working | Integrate with canonical product model |
| Courier cost estimation (ML) | ✅ Working | Keep as-is |
| Strategy opportunity detection (rule-based) | ✅ Working | Add ML scoring layer on top |
| Demand forecasting | ❌ Missing | Phase 3 — requires inventory history + seasonality data |
| Anomaly detection (fees, returns) | ❌ Missing | Phase 3 — requires clean financial data first |
| Dynamic repricing | ❌ Missing | Phase 4 — requires competitor data + margin engine |

**AI sequencing rule**: No ML model should be trained until its underlying data pipeline produces clean, deduplicated, properly timestamped data. This means Phase 1 (data fixes) **must** precede any AI expansion.

---

# PART II: SPRINT PLAN

## Sprint Duration: 2 weeks
## Team Assumption: 1-2 senior backend developers + 1 frontend developer
## Velocity Target: 40-50 story points per sprint (backend), 20-30 (frontend)

---

## EPIC 0: Emergency Fixes (Pre-Sprint — Days 1-3)

*These are not story-pointed. They are critical data integrity fixes that must ship before any sprint begins.*

| # | Task | Owner | Acceptance Criteria | Risk |
|---|---|---|---|---|
| E0.1 | **Fix `asyncio.get_event_loop()` in 4 Celery jobs** | Backend | All 4 files use `asyncio.run()`. All Celery tasks execute without deprecation warnings. | Low — mechanical replacement |
| E0.2 | **Add UNIQUE constraint on `acc_finance_transaction`** | Backend | (1) Run dedup query to find/resolve duplicates. (2) Alembic migration adds `UNIQUE(posted_date, amazon_order_id, sku, charge_type, amount)`. (3) Existing data passes constraint. | Medium — must dedup first |
| E0.3 | **Fix `sync_inventory.py` upsert** | Backend | Replace `db.add()` with SQL MERGE. Dedup existing duplicate snapshots. Add `UNIQUE(product_id, marketplace_id, snapshot_date)`. | Medium — data cleanup |

**Quality Gate**: All 3 fixes deployed + verified in production before Sprint 1 begins.

### Files to Touch:
- `app/jobs/calc_profit.py` — L17: `asyncio.get_event_loop()` → `asyncio.run()`
- `app/jobs/sync_finances.py` — L19: same fix
- `app/jobs/sync_inventory.py` — L17: same fix + rewrite to MERGE/upsert
- `app/jobs/sync_purchase_prices.py` — L21: same fix
- `app/models/finance.py` — add UniqueConstraint
- `migrations/versions/` — new migration for UNIQUE constraints

---

## Sprint 1: Foundation & DevOps (Weeks 1-2)

**Sprint Goal**: Establish CI/CD, clean project hygiene, eliminate legacy code, begin shared utility extraction.

### Backend Tasks

| # | Story | Points | RICE | Acceptance Criteria |
|---|---|---|---|---|
| S1.1 | **Delete `profit_service.py`** — verify no prod callers, migrate test references to V2 engine | 3 | 16.2 | File deleted. All tests pass. No import references remain. |
| S1.2 | **Clean project root** — move 95 `tmp_*`, 30 `_*`, 18 `*.log`, 11 `backfill_*` to `_archive/` | 2 | N/A | Root contains only production files, Dockerfiles, configs. `.gitignore` updated. |
| S1.3 | **Create `app/platform/shared/db.py`** — extract `_connect()`, `_fetchall_dict()`, `_f()`, `_i()`, `_mkt_code()` | 8 | 10.2 | Single source module. 30+ files updated to import from shared. All tests pass. |
| S1.4 | **Create `app/platform/shared/cache.py`** — generic TTL in-memory cache | 3 | — | One implementation replaces 5+ ad-hoc caches. At least 3 services migrated. |
| S1.5 | **Add UNIQUE constraint on `acc_offer(sku, marketplace_id)`** | 2 | — | Alembic migration. Existing data deduplicated. |
| S1.6 | **Fix `Alert.marketplace_id` FK constraint** | 2 | — | Alembic migration. FK to `acc_marketplace`. Orphan data cleaned. |
| S1.7 | **Decrease SQS poll interval** to 30 seconds | 1 | — | `scheduler.py` config change. Verified in logs. |

**Sprint 1 Backend Total: 21 points**

### DevOps Tasks

| # | Story | Points | Acceptance Criteria |
|---|---|---|---|
| S1.D1 | **Create GitHub Actions CI pipeline** — lint + test + Docker build on push/PR | 8 | Pipeline runs on every push. Tests must pass. Docker image builds successfully. |
| S1.D2 | **Create `docker-compose.yml`** for local development (API + Web + Redis) | 5 | `docker-compose up` starts full stack locally. Hot reload works. |
| S1.D3 | **Add `.gitignore` rules** for tmp_*, *.log, _archive/, *.pyc, __pycache__ | 1 | Verified no temp files tracked in git. |

**Sprint 1 DevOps Total: 14 points**

### Frontend Tasks

| # | Story | Points | Acceptance Criteria |
|---|---|---|---|
| S1.F1 | **Add Vitest + React Testing Library** — install, configure, write 1 smoke test per page group | 5 | `npm run test` works. At least 10 basic render tests for key pages (Dashboard, Profit, Finance, FBA, Content). |
| S1.F2 | **Add Playwright E2E setup** — install, configure, write login + dashboard smoke test | 5 | `npx playwright test` runs login flow + dashboard load against dev API. |
| S1.F3 | **Create shared `ApiError` component** — standardize error display across all pages | 3 | Error boundary wraps all routes. Consistent error UI for 4xx/5xx. |

**Sprint 1 Frontend Total: 13 points**

### Sprint 1 KPIs (Analytics Reporter perspective)
- ✅ CI pipeline green on every merge
- ✅ Project root < 20 non-production files
- ✅ Shared utility module created with 30+ consumers
- ✅ Frontend tests executable
- ✅ Zero remaining asyncio deprecation warnings

---

## Sprint 2: Scheduler Decomposition & Schema Consolidation (Weeks 3-4)

**Sprint Goal**: Tame the scheduler monolith and begin centralizing schema management.

### Backend Tasks

| # | Story | Points | Acceptance Criteria |
|---|---|---|---|
| S2.1 | **Split `scheduler.py` into domain modules** — create `app/platform/scheduler/` with: `orders.py`, `finance.py`, `inventory.py`, `ads.py`, `profit.py`, `content.py`, `logistics.py`, `strategy.py`, `system.py` | 13 | 42 jobs redistributed. `scheduler.py` becomes thin orchestrator (< 200 lines). All jobs fire on same schedule. Integration test verifies. |
| S2.2 | **Create unified schema registration** — `app/platform/schema_registry.py` that calls all 25 `ensure_*_schema()` functions in correct order | 5 | Single entry point. `main.py` calls one function. Order verified by dependency. |
| S2.3 | **Convert 3 most critical `ensure_*_schema` to Alembic** — `ensure_event_backbone_schema`, `ensure_finance_center_schema`, `ensure_profit_tkl_cache_schema` | 5 | 3 new Alembic migrations. `ensure_*` functions become no-ops if table exists. |
| S2.4 | **Wire Celery Beat to APScheduler** — eliminate dual scheduling by routing Celery Beat jobs through APScheduler | 5 | `worker.py` beat schedule removed. All jobs trigger from APScheduler only. Celery workers still execute tasks. |
| S2.5 | **Add distributed tracing** — correlation_id middleware for FastAPI + propagate to SP-API connector telemetry | 3 | Every request gets `X-Correlation-ID`. SP-API telemetry includes it. Logs searchable by correlation_id. |

**Sprint 2 Backend Total: 31 points**

### DevOps Tasks

| # | Story | Points | Acceptance Criteria |
|---|---|---|---|
| S2.D1 | **Add pytest coverage reporting** to CI — target 30% initial, track trend | 3 | Coverage report in CI output. Badge in README. |
| S2.D2 | **Create staging deploy pipeline** — Docker push to Azure Container Registry on merge to main | 5 | Automated. Staging updated within 10 min of merge. |

**Sprint 2 DevOps Total: 8 points**

### Frontend Tasks

| # | Story | Points | Acceptance Criteria |
|---|---|---|---|
| S2.F1 | **Create `DataFreshness` indicator v2** — show last sync time per domain (orders, finance, inventory, pricing, ads) on Dashboard | 5 | Dashboard shows 5 freshness indicators. API endpoint exists. Color-coded (green/yellow/red). |
| S2.F2 | **Create `SystemHealth` widget** — expose guardrails results on Dashboard | 5 | Widget shows 5 latest guardrail check results. Links to `/system/guardrails`. |
| S2.F3 | **Standardize page loading states** — create shared `PageSkeleton` component using skeleton UI primitives | 3 | 10+ pages use consistent loading state. |

**Sprint 2 Frontend Total: 13 points**

### Sprint 2 KPIs
- ✅ `scheduler.py` < 200 lines
- ✅ 3 schemas migrated to Alembic
- ✅ Single scheduling path (APScheduler canonical)
- ✅ Correlation IDs in all API logs
- ✅ pytest coverage ≥ 30%

---

## Sprint 3: Profit Engine Decomposition (Weeks 5-6)

**Sprint Goal**: Break the largest God module into maintainable pieces. Single profit calculation path.

### Backend Tasks

| # | Story | Points | Acceptance Criteria |
|---|---|---|---|
| S3.1 | **Split `profit_engine.py` (6632 loc)** into 4 modules: `intelligence/profit/calculator.py` (CM1/CM2/NP core), `intelligence/profit/query.py` (API data access), `intelligence/profit/export.py` (XLSX), `intelligence/profit/cost_model.py` (config) | 13 | 4 files created. Each < 2000 lines. All existing tests pass. All API endpoints work identically. |
| S3.2 | **Merge `profitability_service.py` queries** into `profit/query.py` | 5 | `profitability_service.py` deleted or reduced to thin facade. Zero duplicate query logic. |
| S3.3 | **Consolidate profit calculation path** — `sync_service.calc_profit` and `order_pipeline.step_calc_profit` both delegate to single `profit/calculator.recalc()` | 5 | One entry point for profit calculation. `recalc_profit_orders` in mssql_store remains as SQL engine but with single caller. |
| S3.4 | **Add profit calculation integration test** — end-to-end: create order → bridge fees → calc profit → verify CM1/CM2/NP values | 8 | Test covers full profit waterfall. Runs in CI. Uses test fixtures, not production data. |
| S3.5 | **Convert 3 more `ensure_*_schema` to Alembic** — `ensure_listing_state_schema`, `ensure_pricing_state_schema`, `ensure_manage_inventory_schema` | 3 | 3 new migrations. Running total: 6 schemas migrated to Alembic. |

**Sprint 3 Backend Total: 34 points**

### Frontend Tasks

| # | Story | Points | Acceptance Criteria |
|---|---|---|---|
| S3.F1 | **Profit page refactor** — ProfitOverview, ProfitExplorer updated to use new unified profit query endpoints | 5 | No change in functionality. Faster load (single endpoint vs. multiple). |
| S3.F2 | **Add cost model management UI** in ProfitOverview — CRUD for cost models from new `cost_model.py` API | 5 | Users can view/edit/activate cost models. Changes reflected in next profit recalc. |
| S3.F3 | **Add profit data quality dashboard** — surface data quality checks from profit engine | 3 | Page shows missing costs, unmapped SKUs, stale exchange rates. Drive operator action. |

**Sprint 3 Frontend Total: 13 points**

### Sprint 3 KPIs
- ✅ `profit_engine.py` no longer exists as single file
- ✅ `profitability_service.py` merged or deleted
- ✅ Single profit calculation entry point
- ✅ Profit integration test in CI
- ✅ 6 schemas in Alembic (up from 7 original)

---

## Sprint 4: Content Ops & FBA Decomposition (Weeks 7-8)

**Sprint Goal**: Decompose the second and fourth largest God modules.

### Backend Tasks

| # | Story | Points | Acceptance Criteria |
|---|---|---|---|
| S4.1 | **Split `content_ops.py` (4906 loc)** into `execution/content_ops/tasks.py`, `versions.py`, `publish.py`, `policy.py`, `compliance.py` | 13 | 5 files. Each < 1200 lines. All content API endpoints work. Content publish circuit breaker preserved. |
| S4.2 | **Split `fba_ops/service.py` (3921 loc)** into `overview.py`, `replenishment.py`, `inbound.py`, `cases.py`, `fee_audit.py` | 8 | 5 files. Each < 1000 lines. All FBA endpoints work. Fee audit preserved. |
| S4.3 | **Convert 5 more `ensure_*_schema` to Alembic** — courier schemas (DHL, GLS, courier_verification, courier_monthly_kpi, bl_distribution_cache) | 5 | 5 new migrations. Running total: 11 schemas in Alembic. |
| S4.4 | **Add OpenTelemetry tracing** to SP-API and Ads API connectors | 5 | OTEL spans for every API call. Latency, status code, marketplace as attributes. Exportable to Azure Monitor. |

**Sprint 4 Backend Total: 31 points**

### Frontend Tasks

| # | Story | Points | Acceptance Criteria |
|---|---|---|---|
| S4.F1 | **Content Studio refactor** — update to use split content_ops endpoints (tasks, versions, publish separately) | 5 | No functionality change. Cleaner API calls. |
| S4.F2 | **FBA Dashboard refactor** — update FBA pages to use split FBA endpoints | 5 | All 9 FBA pages work with new endpoints. |
| S4.F3 | **Add component tests** — 20 component tests for shared/ and layout/ components | 3 | 20 tests pass in CI. Coverage on components/ ≥ 60%. |

**Sprint 4 Frontend Total: 13 points**

### Sprint 4 KPIs
- ✅ `content_ops.py` split into 5 modules
- ✅ `fba_ops/service.py` split into 5 modules
- ✅ 11 schemas in Alembic
- ✅ OpenTelemetry traces visible in monitoring
- ✅ 20 new frontend component tests

---

## Sprint 5: Canonical Product Model — Design & Foundation (Weeks 9-10)

**Sprint Goal**: Design and implement the canonical product model. This is the strategic P0 item.

### Backend Tasks

| # | Story | Points | Acceptance Criteria |
|---|---|---|---|
| S5.1 | **Design canonical product schema** — `acc_canonical_product` (internal_sku PK, EAN, brand, category, lifecycle_status) + `acc_marketplace_presence` (canonical_product FK, marketplace FK, SKU, ASIN, status, last_seen) | 8 | Alembic migration created. Tables exist. No data yet. Schema reviewed by team. |
| S5.2 | **Build initial data population** — scan `acc_product` + `acc_offer` + `acc_amazon_listing_registry` → generate canonical products via SKU matching heuristic | 13 | ≥ 80% of existing products mapped to canonical entries. Unmapped products flagged for manual review. |
| S5.3 | **Create `domain/marketplace_mapping.py`** — unified lookup: `(sku, marketplace) → canonical_product` | 5 | Single function replaces 4 lookup strategies (Ergonode → GSheet → Baselinker → ASIN cascade). Used by order_pipeline.step_map_products. |
| S5.4 | **Migrate `order_pipeline.step_map_products`** to use canonical mapping | 5 | Order pipeline uses new mapping. Fallback to old logic if canonical miss. Both paths logged for comparison. |
| S5.5 | **Convert remaining `ensure_*_schema` to Alembic** — all remaining (taxonomy, ptd_cache, sp_api_usage, sellerboard_history, import_products, amazon_listing_registry, fba_fee_audit, profit_data_quality, profit_cost_model) | 5 | 9 more migrations. **All 25 schemas now in Alembic.** `ensure_*_schema` functions become migration-only wrappers. |

**Sprint 5 Backend Total: 36 points**

### Frontend Tasks

| # | Story | Points | Acceptance Criteria |
|---|---|---|---|
| S5.F1 | **Product Canonical View page** — new page showing canonical product → marketplace presences → listing/offer/family for each | 8 | Page accessible at `/products/canonical`. Shows product with all marketplace links. |
| S5.F2 | **Unmapped Products queue** — show products that couldn't be auto-mapped, with manual mapping UI | 5 | Operator can manually link SKUs to canonical products. Queue shrinks over time. |

**Sprint 5 Frontend Total: 13 points**

### Sprint 5 KPIs
- ✅ `acc_canonical_product` table populated with ≥ 80% of products
- ✅ Unified mapping function used by order_pipeline
- ✅ All 25 schemas migrated to Alembic
- ✅ Canonical product UI accessible
- ✅ Unmapped queue < 20% of products

---

## Sprint 6: Event Bus & Action Center (Weeks 11-12)

**Sprint Goal**: Wire batch syncs through event bus. Build unified Amazon write gateway.

### Backend Tasks

| # | Story | Points | Acceptance Criteria |
|---|---|---|---|
| S6.1 | **Extend event_backbone with internal domain events** — add `emit_domain_event(domain, action, payload)` alongside existing SQS ingestion. Events stored in `acc_event_log`. | 8 | Internal events emittable from any service. Stored with same dedup/audit as SQS events. Handler registry supports internal event subscriptions. |
| S6.2 | **Wire ingestion modules to emit events** — order_pipeline emits `orders.synced`, ads_sync emits `ads.synced`, pricing_state emits `pricing.captured` | 5 | 3+ ingestion modules emit domain events. Events visible in acc_event_log. |
| S6.3 | **Replace time-coupled job dependencies** — profitability chain (ads→finance→rollup→executive→strategy) triggers via events instead of fixed times | 8 | No fixed 02:00→03:00→04:00→05:00 chain. Each step triggers on predecessor's completion event. Faster end-to-end. |
| S6.4 | **Build `platform/action_center.py`** — unified gateway for Amazon write operations with audit trail, circuit breaker, rate limiting | 8 | All write operations (content publish, family restructure, price changes) go through action_center. Audit log per action. Circuit breaker from existing pattern. Rate limit per marketplace. |
| S6.5 | **Migrate content_ops publish to action_center** | 3 | Content publish uses action_center. Audit trail visible. Circuit breaker behavior preserved. |

**Sprint 6 Backend Total: 32 points**

### DevOps Tasks

| # | Story | Points | Acceptance Criteria |
|---|---|---|---|
| S6.D1 | **Production deploy pipeline** — automated Docker deploy to Azure Container on tagged release | 5 | `git tag v1.x` triggers build + deploy. Rollback via previous tag. |
| S6.D2 | **Health check endpoint** — `/health` returns guardrails summary + dependency status (DB, Redis, SQS) | 3 | Azure can use for container health. Returns JSON with component status. |

**Sprint 6 DevOps Total: 8 points**

### Frontend Tasks

| # | Story | Points | Acceptance Criteria |
|---|---|---|---|
| S6.F1 | **Event Timeline page** — real-time view of domain events from acc_event_log | 5 | Filterable by domain, marketplace, time range. Auto-refresh. |
| S6.F2 | **Action Center UI** — show pending/completed/failed actions with audit trail | 5 | Operators see all Amazon write operations. Can retry failed actions. |
| S6.F3 | **Dashboard notification banner** — show active circuit breakers and rate limit status | 3 | Banner appears when any circuit breaker is open. Links to detail page. |

**Sprint 6 Frontend Total: 13 points**

### Sprint 6 KPIs
- ✅ 3+ ingestion modules emit domain events
- ✅ Profitability chain triggered by events, not fixed times
- ✅ All Amazon writes go through action_center
- ✅ Production deploy automated
- ✅ Event timeline visible in UI

---

## Sprint 7: MSSQL Store Decomposition & Inventory Consolidation (Weeks 13-14)

**Sprint Goal**: Break the last God module and fix the inventory data problem.

### Backend Tasks

| # | Story | Points | Acceptance Criteria |
|---|---|---|---|
| S7.1 | **Extract job dispatch from `mssql_store.py`** — move `run_job_type()` dispatch to `platform/job_dispatch.py` | 8 | `mssql_store.py` reduced by ~500 lines. Job dispatch is own module. 80+ job types still work. |
| S7.2 | **Consolidate inventory ingestion** — single path: SP-API Inventory API + FBA Reports → `ingestion/inventory.py` → upsert `acc_inventory_snapshot` | 8 | One ingestion module. `sync_inventory.py` job delegates to it. `fba_ops.sync_inventory_cache` delegates to it. No duplicate snapshots. |
| S7.3 | **Consolidate listings ingestion** — `ingestion/listings.py` unifying report-based + event-based + registry paths | 8 | One listing state per (sku, marketplace) regardless of source. Listing state history preserved. |
| S7.4 | **Add `READ_COMMITTED` isolation for finance writes** | 3 | Finance transaction inserts/updates use READ_COMMITTED. All other queries remain READ_UNCOMMITTED. |

**Sprint 7 Backend Total: 27 points**

### Frontend Tasks

| # | Story | Points | Acceptance Criteria |
|---|---|---|---|
| S7.F1 | **Inventory Dashboard refactor** — unified inventory view from consolidated backend | 5 | Single data source. DOI, velocity, FBA detail all from same pipeline. |
| S7.F2 | **Add Playwright E2E tests** for critical flows — profit explorer, content publish, FBA inventory, finance reconciliation | 8 | 4 E2E test suites pass in CI. Cover login → navigate → interact → verify. |

**Sprint 7 Frontend Total: 13 points**

### Sprint 7 KPIs
- ✅ `mssql_store.py` < 3500 lines
- ✅ Zero duplicate inventory snapshots for 7 consecutive days
- ✅ Unified listing state regardless of source
- ✅ Finance writes use READ_COMMITTED
- ✅ 4 E2E test suites in CI

---

## Sprint 8: Intelligence Layer Consolidation (Weeks 15-16)

**Sprint Goal**: Unify strategy/executive/decision_intelligence. Wire canonical product into intelligence.

### Backend Tasks

| # | Story | Points | Acceptance Criteria |
|---|---|---|---|
| S8.1 | **Merge strategy_service + executive_service + decision_intelligence_service** overlapping logic — unified `growth_opportunity` access layer | 8 | One service coordinates `growth_opportunity` writes. No conflicting updates. Each service has distinct bounded responsibility. |
| S8.2 | **Wire canonical product into profit engine** — profit queries join through `acc_canonical_product` | 5 | Profit drilldown by canonical product. Cross-marketplace profit aggregation. |
| S8.3 | **Wire canonical product into pricing intelligence** — pricing snapshots linked to canonical product | 5 | Cross-market price comparison by canonical product. |
| S8.4 | **Wire canonical product into content_ops** — content tasks linked to canonical product instead of flat ASIN | 3 | Content tasks reference canonical product. Cross-market content gap visible. |
| S8.5 | **Backend test coverage push** — add tests for profit/calculator, event_bus, action_center | 5 | Coverage on critical modules ≥ 60%. |

**Sprint 8 Backend Total: 26 points**

### Frontend Tasks

| # | Story | Points | Acceptance Criteria |
|---|---|---|---|
| S8.F1 | **Cross-market product view** — show same product across all 9 marketplaces with price, stock, profit, content status | 8 | Page at `/products/canonical/:id`. Side-by-side marketplace comparison. |
| S8.F2 | **Strategy opportunities linked to canonical products** — opportunity cards show product context | 5 | Strategy page shows canonical product info alongside opportunity. |

**Sprint 8 Frontend Total: 13 points**

### Sprint 8 KPIs
- ✅ No conflicting `growth_opportunity` writes
- ✅ Cross-market profit aggregation by canonical product
- ✅ Cross-market product view in UI
- ✅ Backend critical path coverage ≥ 60%

---

# PART III: PHASE 2-3 QUARTERLY ROADMAP

## Q2 2026 (Sprints 9-14) — "Intelligence & Automation"

| Sprint | Focus | Key Deliverables |
|---|---|---|
| Sprint 9-10 | **Catalog Health Monitor** | Catalog health scorecard. Listing diff detector. Suppression tracking. Content completeness scoring. |
| Sprint 11-12 | **Buy Box Radar & Competitor Intelligence** | Competitor offer snapshots. BuyBox win-rate trends. Sustained loss alerts. Competitive landscape page. |
| Sprint 13-14 | **Inventory Risk Engine** | Stockout probability model. Overstock cost estimation. Aging write-off risk. Replenishment automation improvements. |

## Q3 2026 (Sprints 15-20) — "Automation & Scale"

| Sprint | Focus | Key Deliverables |
|---|---|---|
| Sprint 15-16 | **Repricing Decision Engine** | Dynamic pricing strategies. Competitor-aware algorithms. Margin guardrails. Human approval flow. |
| Sprint 17-18 | **Content Optimization Engine** | Content scoring model. SEO analysis. Multi-language generation. A/B content testing. |
| Sprint 19-20 | **Full SQS Topology** | 4 queues deployed. DLQ strategy active. All modules event-wired. Replay operational. |

## Q4 2026 (Sprints 21-26) — "Scale & Polish"

| Sprint | Focus | Key Deliverables |
|---|---|---|
| Sprint 21-22 | **Refund / Fee Anomaly Engine** | Refund spike detection. Serial returner identification. Automated reimbursement claims. |
| Sprint 23-24 | **Operator Console v2** | Unified alert feed. Case management. Action queue with approvals. Operator dashboard. |
| Sprint 25-26 | **Multi-seller support** | Account Hub with credential vault. Multi-seller scheduler. Permission model. |

---

# PART IV: METRICS & SUCCESS CRITERIA

## Technical Health Metrics (Data Analytics perspective)

| Metric | Current | Sprint 4 Target | Sprint 8 Target | End of Year Target |
|---|---|---|---|---|
| Python LOC | 99,797 | 95,000 (-5%) | 92,000 (-8%) | 88,000 (-12%) |
| Largest file LOC | 6,632 (profit_engine) | 2,000 | 2,000 | 1,500 |
| God modules (>2500) | 6 | 2 | 0 | 0 |
| Test files | 44 | 60 | 80 | 120 |
| pytest coverage | ~15% (est) | 30% | 60% (critical) | 70% |
| Frontend tests | 0 | 30 | 50 | 100 |
| Alembic migrations | 7 | 17 | 32+ | 40+ |
| Inline DDL (`ensure_*`) | 25 | 14 | 0 | 0 |
| `tmp_*` files in root | 95 | 0 | 0 | 0 |
| Duplicated helpers | 30+ `_connect()` | 0 | 0 | 0 |
| CI pipeline | None | ✅ | ✅ + staging | ✅ + prod |
| Scheduling paths | 3 | 1 | 1 | 1 |
| Profit calc paths | 3 | 1 | 1 | 1 |

## Business Impact Metrics (Finance Tracker perspective)

| Metric | Current | After Phase 1 (Sprint 8) | After Phase 2 (Sprint 14) |
|---|---|---|---|
| Finance transaction duplicates | Unknown (no constraint) | 0 guaranteed | 0 guaranteed |
| Inventory snapshot accuracy | Unknown (duplicates exist) | Verified (upsert + unique) | Verified + risk-scored |
| Profit calculation trust | Low (3 paths) | High (1 path) | High + auditable |
| Time from order → profit | ~3 hours (cron chain) | ~30 min (event-driven) | ~15 min (optimized) |
| Cross-market product reconciliation | Manual | 80% automated (canonical model) | 95% automated |
| SQS notification latency | 2 min | 30 sec | 30 sec |
| Deploy lead time | Manual (hours) | Automated (minutes) | Automated (minutes) |

## Feedback Synthesis (Feedback Synthesizer perspective)

*Inferred from audit findings — operator pain points that drive priority:*

| Pain Point | Affected Users | Sprint Addressed |
|---|---|---|
| "Which profit number is right?" | Finance, Executive | Sprint 3 |
| "Can't see product across all markets" | Strategy, Content | Sprint 5, 8 |
| "Content publish sometimes fails silently" | Content operators | Sprint 6 (action center) |
| "Inventory data doesn't match Amazon" | FBA, Purchasing | Sprint 7 |
| "Jobs fail at night, nobody knows until morning" | Ops | Sprint 2 (alerts), Sprint 6 (events) |
| "Can't deploy without fear" | Dev team | Sprint 1 (CI/CD) |
| "Which SKU is which in different markets?" | Everyone | Sprint 5 (canonical product) |
| "Reports take too long to generate" | Finance, Executive | Sprint 3 (profit engine split) |

---

# PART V: RISK REGISTER

| Risk | Probability | Impact | Mitigation | Owner |
|---|---|---|---|---|
| Canonical product migration breaks order pipeline | Medium | High | Dual-write + shadow comparison for 2 weeks before cutover | Backend |
| Scheduler split changes job timing | Low | High | Integration test comparing job execution times before/after | Backend |
| DDL-to-Alembic migration corrupts production schema | Low | Critical | Run migration on staging clone first. Backup before each batch. | DevOps |
| Profit engine split breaks financial reporting | Medium | High | Full regression test suite before/after. Parallel run old+new for 1 sprint. | Backend |
| Team velocity lower than planned | High | Medium | Buffer 15% in each sprint. Quick wins fill slack. | Sprint Lead |
| Event-driven chain slower than cron chain | Low | Medium | Keep cron as fallback. Feature flag to switch. | Backend |
| Frontend refactors break existing pages | Medium | Medium | Playwright E2E tests catch regressions. Feature flags for new UIs. | Frontend |

---

# PART VI: SPRINT CALENDAR

```
2026
March    April        May          June         July         August
├─E0──┤
│ Fix  │
│ Crit │
├──────┼─S1──────┬──S2──────┬──S3──────┬──S4──────┬──S5──────┤
│      │CI/CD    │Scheduler │Profit    │Content   │Canonical │
│      │Cleanup  │Schema    │Engine    │FBA ops   │Product   │
│      │Shared   │Decompose │Decompose │OTEL      │Model     │
├──────┼─────────┼──────────┼──────────┼──────────┼──────────┤
       Sept      Oct        Nov        Dec
       ├──S6──────┬──S7──────┬──S8──────┤
       │Event Bus │MSSQL     │Intelli-  │
       │Action    │Inventory │gence     │
       │Center    │Listings  │Canonical │
       │Deploy    │Consolid. │Product   │
       ├──────────┼──────────┼──────────┤
                                        │
                             Q2-Q4: Phase 2-3
                             Intelligence & Automation
```

### Sprint Velocity Tracking Template

| Sprint | Planned | Completed | Velocity | Carryover | Notes |
|---|---|---|---|---|---|
| E0 | — | — | — | — | Pre-sprint critical fixes |
| S1 | 48 | | | | Foundation + DevOps + Frontend |
| S2 | 52 | | | | Scheduler + Schema |
| S3 | 47 | | | | Profit Engine |
| S4 | 44 | | | | Content + FBA |
| S5 | 49 | | | | Canonical Product |
| S6 | 53 | | | | Event Bus + Action Center |
| S7 | 40 | | | | MSSQL + Inventory + Listings |
| S8 | 39 | | | | Intelligence + Canonical Wire |

---

# PART VII: DEFINITION OF DONE

## Per Story
- [ ] Code reviewed (PR approved)
- [ ] Unit tests written for new/changed logic
- [ ] Existing tests pass
- [ ] No new lint warnings
- [ ] Alembic migration (if schema change)
- [ ] API documentation updated (if endpoint change)
- [ ] Feature flag (if risky change)

## Per Sprint
- [ ] All stories meet individual DoD
- [ ] CI pipeline green
- [ ] Coverage does not decrease
- [ ] Staging deploy successful
- [ ] Sprint retrospective held

## Per Phase
- [ ] All sprints complete
- [ ] KPIs met (see Part IV)
- [ ] Architecture audit re-run shows improvement
- [ ] No P0 issues remaining from audit

---

# APPENDIX: Quick Reference — File → Sprint Mapping

| File | Problem | Sprint |
|---|---|---|
| `jobs/calc_profit.py` | deprecated asyncio | E0 |
| `jobs/sync_finances.py` | deprecated asyncio | E0 |
| `jobs/sync_inventory.py` | deprecated asyncio + no upsert | E0 |
| `jobs/sync_purchase_prices.py` | deprecated asyncio | E0 |
| `models/finance.py` | missing UNIQUE | E0 |
| `services/profit_service.py` | legacy/dead | S1 |
| 30+ files | duplicated `_connect()` | S1 |
| `scheduler.py` (1675 loc) | God module | S2 |
| `worker.py` | dual scheduling | S2 |
| 25x `ensure_*_schema` | inline DDL | S2-S5 |
| `services/profit_engine.py` (6632 loc) | God module | S3 |
| `services/profitability_service.py` | overlap | S3 |
| `services/content_ops.py` (4906 loc) | God module | S4 |
| `services/fba_ops/service.py` (3921 loc) | God module | S4 |
| `models/product.py` + `offer.py` + family + registry | no canonical model | S5 |
| `services/event_backbone.py` | SQS-only, no internal events | S6 |
| `connectors/mssql/mssql_store.py` (4297 loc) | God module | S7 |
| `services/manage_inventory.py` | overlap | S7 |
| `services/strategy_service.py` | overlap | S8 |
| `services/executive_service.py` | overlap | S8 |
| `services/decision_intelligence_service.py` | overlap | S8 |
