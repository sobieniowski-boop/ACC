# ACC — Sprint Plan

> Version: 2026-03-12 | Cadence: 2-week sprints | Team: 2-3 developers
> Completed: 26 sprints (E0 through S25-26) | Status: All green (1765/1765 tests)

---

## 1. Completed Sprint History

| Sprint | Period | Key Deliverables | Tests Added |
|---|---|---|---|
| **E0** | Week 1 | Project scaffolding, FastAPI + React, Docker Compose | ~50 |
| **S1-2** | Weeks 2-3 | Amazon SP-API, order sync, profitability engine, KPI dashboard | ~200 |
| **S3-4** | Weeks 4-5 | Family mapper, product catalog, Ergonode sync, Alembic migrations | ~150 |
| **S5-6** | Weeks 6-7 | Pricing engine, Buy Box tracking, ads integration, AI recommendations | ~150 |
| **S7-8** | Weeks 8-9 | FBA operations, inventory management, content ops pipeline | ~200 |
| **S9-10** | Weeks 10-11 | Finance center, tax compliance, exchange rates, ledger | ~150 |
| **S11-12** | Weeks 12-13 | Strategy engine, seasonality, decision intelligence, outcomes | ~120 |
| **S13-14** | Weeks 14-15 | DHL + GLS courier integration, logistics cost allocation | ~100 |
| **S15-16** | Weeks 16-17 | Catalog health, BuyBox radar, inventory risk intelligence | ~100 |
| **S17-18** | Weeks 18-19 | Repricing engine, content optimization, refund anomaly detector | ~100 |
| **S19-20** | Weeks 20-21 | Event backbone (SQS topology, event wiring), notifications | ~100 |
| **S21-22** | Weeks 22-23 | Operator console, returns module, guardrails framework | ~100 |
| **S23-24** | Weeks 24-25 | Executive dashboard, listing state, catalog definitions | ~100 |
| **S25-26** | Weeks 26-27 | Account hub, multi-seller foundation, 100% test green | ~45 |

**Cumulative**: 26 sprints → 1765 tests, 128,991 backend LOC, 91 frontend pages

---

## 2. Current State vs. EOY Targets

| Metric | Current | EOY Target | Gap | Status |
|---|---|---|---|---|
| Tests passing | 1,765 | 3,000+ | -1,235 | 🟡 59% |
| Test files | 61 | 120 | -59 | 🟡 51% |
| Backend LOC | 128,991 | < 88,000 | +40,991 | 🔴 147% |
| God modules (>2500 LOC) | 7 | 0 | -7 | 🔴 |
| Inline DDL functions | 39 | 0 | -39 | 🔴 |
| Intelligence engines | 11 | 11 | 0 | ✅ 100% |
| API routers | 49 | 49 | 0 | ✅ 100% |
| Frontend pages | 91 | 91 | 0 | ✅ 100% |
| Alembic migrations | 40 | 80 | -40 | 🟡 50% |
| CI/CD pipeline | CI only | Full CI/CD | — | 🟡 |
| Auth coverage | ~50% | 100% | ~50% | 🔴 |

---

## 3. Upcoming Sprint Plan

### Sprint 27-28: Security Hardening
**Duration**: 2 weeks | **Priority**: P0

| # | Task | Owner | Criteria |
|---|---|---|---|
| 1 | Add auth to 25 unprotected routers | Backend | All endpoints require min ANALYST role |
| 2 | Update tests with auth headers | Backend | 1765 tests still pass |
| 3 | Add CSRF protection middleware | Backend | State-changing requests validated |
| 4 | WebSocket auth (JWT in connection) | Backend | WS connections authenticated |
| 5 | Rate limit fine-tuning | Backend | Auth: 10/min, Jobs: 5/min, Default: 100/min |

**Exit Criteria**: 100% endpoint auth coverage, all tests green, security scan clean

---

### Sprint 29-30: God Module Splits (Phase 1)
**Duration**: 2 weeks | **Priority**: P0

| # | Task | Target | Split Into |
|---|---|---|---|
| 1 | Split `profit/query.py` (4689 LOC) | < 800 LOC each | query_builder, query_filters, query_aggregation, query_export |
| 2 | Split `mssql_store.py` (3546 LOC) | < 800 LOC each | mssql_core, mssql_orders, mssql_finance, mssql_inventory |
| 3 | Update all imports across codebase | — | Backward-compatible re-exports |
| 4 | Run full test suite | — | 1765+ tests green |

**Exit Criteria**: 2 modules split, all tests pass, no God module > 3500 LOC

---

### Sprint 31-32: God Module Splits (Phase 2)
**Duration**: 2 weeks | **Priority**: P0

| # | Task | Target |
|---|---|---|
| 1 | Split `order_pipeline.py` (3394 LOC) | pipeline_ingest, pipeline_enrich, pipeline_finance |
| 2 | Split `sync_service.py` (2945 LOC) | sync_orders, sync_listings, sync_reports |
| 3 | Split `family_mapper/restructure.py` (2924 LOC) | restructure_analyzer, restructure_executor, restructure_validator |
| 4 | Update imports, run tests | 1765+ tests green |

**Exit Criteria**: 5 total modules split, 2 remaining (manage_inventory, finance_center)

---

### Sprint 33-34: God Module Splits (Phase 3) + Inline DDL Start
**Duration**: 2 weeks | **Priority**: P0

| # | Task | Target |
|---|---|---|
| 1 | Split `manage_inventory.py` (2710 LOC) | inventory_drafts, inventory_overview, inventory_families |
| 2 | Split `finance_center/service.py` (2674 LOC) | finance_import, finance_ledger, finance_reconciliation |
| 3 | Audit all 39 ensure_*_schema functions | Document tables/columns per function |
| 4 | Create first 10 Alembic migrations (FBA group) | Migrate FBA inline DDL |

**Exit Criteria**: 0 God modules, 10 inline DDL functions migrated

---

### Sprint 35-36: Inline DDL Migration
**Duration**: 2 weeks | **Priority**: P0

| # | Task | Target |
|---|---|---|
| 1 | Migrate 15 ensure_*_schema (Finance Center group) | 15 new Alembic migrations |
| 2 | Migrate 14 ensure_*_schema (remaining groups) | 14 new Alembic migrations |
| 3 | Remove all ensure_*_schema functions | 0 inline DDL remaining |
| 4 | Verify `alembic upgrade head` matches current schema | Full schema parity |

**Exit Criteria**: 0 inline DDL functions, ~80 Alembic migrations, all tests green

---

### Sprint 37-38: Test Coverage Push (Phase 1)
**Duration**: 2 weeks | **Priority**: P1

| # | Task | Target |
|---|---|---|
| 1 | Write tests for 11 intelligence engines | 11 new test files, ~200 tests |
| 2 | Write tests for 16 scheduler modules | 16 new test files, ~160 tests |
| 3 | Write tests for connector modules | 5 new test files, ~75 tests |

**Exit Criteria**: 93 test files, ~2200 tests, 100% pass

---

### Sprint 39-40: Test Coverage Push (Phase 2)
**Duration**: 2 weeks | **Priority**: P1

| # | Task | Target |
|---|---|---|
| 1 | Write tests for remaining API routers | 27 new test files, ~300 tests |
| 2 | Write integration tests (e2e workflows) | 5 new test files, ~60 tests |
| 3 | Add pytest-cov with minimum 70% threshold | CI enforced |

**Exit Criteria**: 120+ test files, 2500+ tests, 70%+ coverage

---

### Sprint 41-42: CI/CD Pipeline
**Duration**: 2 weeks | **Priority**: P1

| # | Task | Target |
|---|---|---|
| 1 | Add lint (ruff) + type check (mypy) to CI | CI gates |
| 2 | Add security scan (bandit) to CI | No high-severity findings |
| 3 | Set up Azure Container Registry (ACR) | Docker push on main |
| 4 | Azure Container Apps deployment | Staging auto-deploy |
| 5 | Production promotion workflow | Manual approval gate |

**Exit Criteria**: Full CI/CD pipeline, staging auto-deploy, production manual

---

### Sprint 43-44: LOC Reduction
**Duration**: 2 weeks | **Priority**: P2

| # | Task | Target |
|---|---|---|
| 1 | Remove deprecated /profit/ and /profitability/ endpoints | -9 endpoints |
| 2 | Extract shared CRUD base class | Reduce ~5K LOC |
| 3 | Deduplicate job trigger patterns | Reduce ~3K LOC |
| 4 | Consolidate duplicate query patterns | Reduce ~5K LOC |

**Exit Criteria**: Backend < 115K LOC

---

### Sprint 45-46: Monitoring & Performance
**Duration**: 2 weeks | **Priority**: P1-P2

| # | Task | Target |
|---|---|---|
| 1 | Azure Monitor integration | Dashboard for API metrics |
| 2 | Query performance audit (top 20 slow queries) | All dashboards < 2s |
| 3 | Redis caching for dashboards | 5-10min TTL on heavy endpoints |
| 4 | Alert rules (error rate, job failure, DB pool) | PagerDuty/Teams integration |

**Exit Criteria**: SLO dashboards live, all critical alerts configured

---

### Sprint 47-48: Multi-Seller Data Isolation
**Duration**: 2 weeks | **Priority**: P1

| # | Task | Target |
|---|---|---|
| 1 | Add seller_account_id to core tables | Migration for 5 tables |
| 2 | Seller context middleware | Auto-filter by current seller |
| 3 | Update all queries for seller-aware filtering | Full data isolation |
| 4 | Test isolation: user A can't see user B's data | Integration tests |

**Exit Criteria**: Full multi-seller data isolation, permission tests pass

---

## 4. Sprint Velocity Summary

| Phase | Sprints | Focus | Target Metrics |
|---|---|---|---|
| **Security** | 27-28 | Auth hardening | 100% auth coverage |
| **Refactoring** | 29-36 | God modules + DDL migration | 0 God modules, 0 inline DDL |
| **Quality** | 37-42 | Tests + CI/CD | 120 test files, full pipeline |
| **Optimization** | 43-48 | LOC, perf, monitoring, multi-seller | <115K LOC, SLOs, isolation |

**Total Remaining**: 22 sprints (44 weeks) to reach all EOY targets

---

## 5. Risk Register

| Risk | Impact | Mitigation |
|---|---|---|
| God module splits break imports | HIGH | Backward-compat re-exports + run all 1765 tests |
| Inline DDL migration misses columns | HIGH | Schema diff tool (compare before/after) |
| Auth addition breaks frontend | MEDIUM | Update API client to include tokens on all requests |
| Test coverage push creates flaky tests | MEDIUM | Strict mock isolation, no shared state |
| CI/CD pipeline slows development | LOW | Parallel CI jobs, Docker layer caching |
