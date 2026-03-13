# ACC — Task List

> Version: 2026-03-12 | All 26 sprints complete | 1765 tests passing (100%)
> Priority: P0 (Critical), P1 (High), P2 (Medium), P3 (Low)

---

## Summary

| Priority | Open | Description |
|---|---|---|
| **P0** | 3 | Security auth gaps, God module splits, inline DDL migration |
| **P1** | 5 | Test coverage, CI/CD pipeline, monitoring, API auth |
| **P2** | 6 | Performance, code quality, documentation, feature refinements |
| **P3** | 4 | Nice-to-haves, cleanup, optimization |

---

## P0 — Critical

### TASK-001: Add Auth to Unprotected Routers
**Status**: Not Started | **Estimate**: 2 sprints | **Owner**: Backend Lead

**Description**: ~25 of 49 API routers have no authentication middleware. This means any HTTP client can access operational data (courier shipments, tax records, inventory risk, repricing strategies, refund anomalies, account hub, etc.) without credentials.

**Specific Criteria**:
- [ ] Add `require_analyst` dependency to all GET endpoints on: courier, tax, guardrails, notifications, listing-state, catalog-definitions, pricing-state, backbone, catalog-health, buybox-radar, inventory-risk, repricing, content-optimization, sqs-topology, event-wiring, refund-anomaly, operator-console, account-hub, returns, import-products, planning, alerts, jobs
- [ ] Add `require_ops` to all state-changing (POST/PUT/PATCH/DELETE) endpoints on those routers
- [ ] Add `require_admin` to account-hub credential management and seller creation
- [ ] Add `require_director` to repricing execution and operator action approval
- [ ] Update all test files to include auth headers in requests
- [ ] Verify no regression — all 1765 tests still pass
- [ ] Update API_SPEC.md with new auth requirements

---

### TASK-002: Split God Modules (>2500 LOC)
**Status**: Not Started | **Estimate**: 3 sprints | **Owner**: Backend Lead

**Description**: 7 modules exceed 2500 LOC, violating maintainability targets. These modules are hard to test, review, and debug. Target: 0 files > 2500 LOC.

**God Modules**:
| Module | LOC | Target Split |
|---|---|---|
| `profit/query.py` | 4,689 | query_builder.py, query_filters.py, query_aggregation.py, query_export.py |
| `connectors/mssql_store.py` | 3,546 | mssql_core.py, mssql_orders.py, mssql_finance.py, mssql_inventory.py |
| `connectors/order_pipeline.py` | 3,394 | pipeline_ingest.py, pipeline_enrich.py, pipeline_finance.py |
| `connectors/sync_service.py` | 2,945 | sync_orders.py, sync_listings.py, sync_reports.py |
| `intelligence/family_mapper/restructure.py` | 2,924 | restructure_analyzer.py, restructure_executor.py, restructure_validator.py |
| `intelligence/manage_inventory.py` | 2,710 | inventory_drafts.py, inventory_overview.py, inventory_families.py |
| `intelligence/finance_center/service.py` | 2,674 | finance_import.py, finance_ledger.py, finance_reconciliation.py |

**Specific Criteria**:
- [ ] Each split file < 800 LOC
- [ ] No circular imports between split files
- [ ] All existing imports updated across entire codebase
- [ ] All tests pass without modification (or with minimal import path changes)
- [ ] Each split module has a clear single responsibility
- [ ] Preserve all `__init__.py` re-exports for backward compatibility
- [ ] Run full test suite — 1765 tests green after each module split

---

### TASK-003: Migrate Inline DDL to Alembic
**Status**: Not Started | **Estimate**: 3 sprints | **Owner**: Backend Lead

**Description**: 39 `ensure_*_schema` functions create/alter tables inline at runtime. This means the schema can drift between environments, makes rollbacks impossible, and is untestable. All DDL should be in Alembic migrations.

**Specific Criteria**:
- [ ] Audit all 39 `ensure_*_schema` functions — list tables, columns, indexes created
- [ ] For each function, create an Alembic migration with equivalent DDL
- [ ] Ensure migrations are idempotent (IF NOT EXISTS patterns for tables/columns)
- [ ] Replace each `ensure_*_schema` call with a no-op or assertion that migration already ran
- [ ] Remove the `ensure_*_schema` functions from codebase
- [ ] Verify `alembic upgrade head` creates same schema as current inline DDL
- [ ] Verify `alembic downgrade` works for each new migration
- [ ] Run full test suite — 1765 tests green
- [ ] Update SCHEMA.md migration chain

---

## P1 — High

### TASK-004: Increase Test Coverage to 120 Test Files
**Status**: Not Started | **Estimate**: 4 sprints | **Owner**: Full Team

**Description**: Current: 61 test files, 1765 tests. EOY target: 120 test files, 3000+ tests. Many modules have zero or minimal test coverage.

**Specific Criteria**:
- [ ] Identify untested modules: all intelligence engines (11), platform modules (22), connector modules
- [ ] Write tests for each intelligence engine: buybox_radar, catalog_health, content_ab_testing, content_optimization, event_wiring, inventory_risk, operator_console, refund_anomaly, repricing_engine, sqs_topology, account_hub
- [ ] Write tests for platform modules: action_center, job_dispatch, schema_registry, otel, all 16 scheduler domain modules
- [ ] Write tests for connectors: DHL, GLS, Ergonode, Amazon Ads, SP-API mocking
- [ ] Write tests for all API routers (49 routers — many have no test coverage)
- [ ] Each new test file: minimum 15 tests (unit + integration mix)
- [ ] Total: 59 new test files → 120 test files, ~2200+ new tests
- [ ] Maintain 100% pass rate — no flaky tests

---

### TASK-005: Production CI/CD Pipeline
**Status**: Partial | **Estimate**: 2 sprints | **Owner**: DevOps

**Description**: CI exists (GitHub Actions `ci.yml`) but there's no CD pipeline. Deployments are manual Docker builds. Need automated deploy to Azure Container Apps or equivalent.

**Specific Criteria**:
- [ ] CI: Add lint step (ruff + mypy) to `ci.yml`
- [ ] CI: Add test coverage reporting (pytest-cov with minimum 70% threshold)
- [ ] CI: Add security scanning (bandit or safety)
- [ ] CD: Azure Container Registry (ACR) push on `main` merge
- [ ] CD: Azure Container Apps deploy (staging → production promotion)
- [ ] CD: Database migration step (alembic upgrade head) before deploy
- [ ] CD: Health check validation after deploy
- [ ] CD: Rollback mechanism (previous image tag revert)
- [ ] Branch protection: require CI pass + 1 review for `main`

---

### TASK-006: Comprehensive Monitoring & Alerting
**Status**: Partial | **Estimate**: 2 sprints | **Owner**: Backend Lead

**Description**: Sentry + OpenTelemetry exist but no structured alerting for business metrics. Need runbook-ready alerting.

**Specific Criteria**:
- [ ] Define SLOs: API p95 < 500ms, error rate < 1%, job success rate > 99%
- [ ] Azure Monitor / Application Insights integration
- [ ] Dashboard: API latency, error rate, throughput per router
- [ ] Dashboard: Job success/failure rates per domain
- [ ] Dashboard: External API health (SP-API, DHL, GLS)
- [ ] Alert: API error rate > 5% for 5 minutes → PagerDuty/Teams
- [ ] Alert: Job failure 3x consecutive → notification
- [ ] Alert: Database connection pool exhaustion → warning
- [ ] Alert: Redis connection failure → critical
- [ ] Structured log aggregation (Azure Log Analytics or Elasticsearch)

---

### TASK-007: Multi-Seller Data Isolation
**Status**: Partial | **Estimate**: 2 sprints | **Owner**: Backend Lead

**Description**: Account Hub and seller permissions exist (Sprint 25-26) but most queries don't filter by seller account. Need tenant-aware data access.

**Specific Criteria**:
- [ ] Add `seller_account_id` column to: acc_order, acc_product, acc_offer, acc_ads_campaign, acc_fba_inventory_snapshot
- [ ] Create migration for new columns
- [ ] Update all repository queries to filter by seller_account_id when user has seller permissions
- [ ] Add middleware to inject active seller context from JWT claims
- [ ] Update profitability, KPI, executive views to respect seller boundaries
- [ ] Test: user with seller A permission cannot see seller B data
- [ ] Test: admin can see all sellers
- [ ] Backward compatible — existing single-seller data works without migration

---

### TASK-008: API Pagination Standardization
**Status**: Partial | **Estimate**: 1 sprint | **Owner**: Backend

**Description**: Most endpoints use `skip/limit` but response shapes vary. Need standardized paginated response wrapper.

**Specific Criteria**:
- [ ] Define standard `PaginatedResponse[T]` generic: `{ items: T[], total: int, skip: int, limit: int, has_more: bool }`
- [ ] Apply to all list endpoints (at least 50 endpoints)
- [ ] Add cursor-based pagination option for large datasets (orders, events)
- [ ] Update frontend TanStack Query hooks to use standardized shape
- [ ] Add `total` count to all list responses
- [ ] Default limit: 50, max limit: 500

---

## P2 — Medium

### TASK-009: Frontend Type Safety
**Status**: Not Started | **Estimate**: 2 sprints | **Owner**: Frontend Lead

**Description**: Frontend uses TypeScript but many API responses are typed as `any`. Generate types from backend Pydantic models.

**Specific Criteria**:
- [ ] Set up openapi-typescript or similar code generator
- [ ] Generate TypeScript interfaces from FastAPI OpenAPI schema
- [ ] Replace all `any` types in API hooks with generated types
- [ ] Add API response validation at runtime (zod or similar)
- [ ] Configure CI to regenerate types on schema change
- [ ] Zero `any` in API layer code

---

### TASK-010: Database Query Performance Audit
**Status**: Not Started | **Estimate**: 1 sprint | **Owner**: Backend

**Description**: No systematic performance review of database queries. Profit/query.py (4689 LOC) likely has slow queries. Need query plan analysis.

**Specific Criteria**:
- [ ] Enable Azure SQL Query Performance Insight
- [ ] Identify top 20 slowest queries (by cumulative time)
- [ ] Add missing indexes for frequent filter patterns
- [ ] Review profit rollup queries for N+1 problems
- [ ] Add database connection pooling tuning (min/max connections)
- [ ] Benchmark: all dashboard endpoints < 2s response time
- [ ] Add query duration logging middleware

---

### TASK-011: Error Handling Standardization
**Status**: Partial | **Estimate**: 1 sprint | **Owner**: Backend

**Description**: Error responses are inconsistent. Some use FastAPI HTTPException, others raise raw exceptions. Need standard error envelope.

**Specific Criteria**:
- [ ] Define `ErrorResponse { code: str, message: str, details: dict | None, request_id: str }`
- [ ] Create global exception handler middleware
- [ ] Map all business exceptions to typed error codes
- [ ] Ensure all 4xx/5xx responses follow the standard shape
- [ ] Remove raw exception propagation to clients
- [ ] Log all 5xx errors to Sentry with request context

---

### TASK-012: Reduce Backend LOC
**Status**: Not Started | **Estimate**: 3 sprints | **Owner**: Full Team

**Description**: Backend is 128,991 LOC (target was 88K). Much of this is duplicated patterns and verbose implementations. Need systematic deduplication.

**Specific Criteria**:
- [ ] Identify top 20 duplicated code patterns (service methods, query builders, CRUD operations)
- [ ] Extract shared CRUD base class for simple entities
- [ ] Deduplicate job trigger patterns (currently copy-pasted per domain)
- [ ] Consolidate ensure_*_schema patterns (after TASK-003)
- [ ] Remove deprecated profit/profitability legacy endpoints
- [ ] Target: < 100K LOC after cleanup (31% reduction from 128K)

---

### TASK-013: caching Strategy
**Status**: Partial | **Estimate**: 1 sprint | **Owner**: Backend

**Description**: Redis is used for rate limiting and some caching but no systematic caching strategy. Dashboard endpoints recalculate on every request.

**Specific Criteria**:
- [ ] Cache KPI summary (5 min TTL)
- [ ] Cache executive overview (5 min TTL)
- [ ] Cache catalog health scorecard (10 min TTL)
- [ ] Cache buybox dashboard (5 min TTL)
- [ ] Use cache invalidation on data changes (job completion events)
- [ ] Add cache hit/miss metrics to monitoring
- [ ] Document caching strategy and TTLs

---

### TASK-014: WebSocket Enhancement
**Status**: Basic | **Estimate**: 1 sprint | **Owner**: Full Stack

**Description**: WebSocket exists for job progress and alerts but is minimal. Need heartbeat, reconnection, and broader real-time updates.

**Specific Criteria**:
- [ ] Add WebSocket heartbeat (30s ping/pong)
- [ ] Client-side automatic reconnection with exponential backoff
- [ ] Add WebSocket channel for real-time alert updates
- [ ] Add WebSocket channel for inventory risk warnings
- [ ] Add auth to WebSocket connections (JWT in connection params)
- [ ] Connection limit per user (max 5 concurrent)
- [ ] Broadcast job completion to all connected clients

---

## P3 — Low

### TASK-015: OpenAPI Documentation Enhancement
**Status**: Auto-generated | **Estimate**: 1 sprint | **Owner**: Backend

**Description**: FastAPI auto-generates OpenAPI docs but descriptions, examples, and tags are minimal.

**Specific Criteria**:
- [ ] Add detailed descriptions to all Pydantic models
- [ ] Add request/response examples for top 30 endpoints
- [ ] Group endpoints by functional domain in Swagger UI
- [ ] Add authentication instructions to OpenAPI description
- [ ] Export and version OpenAPI JSON in repository

---

### TASK-016: Frontend Accessibility (a11y)
**Status**: Not Started | **Estimate**: 2 sprints | **Owner**: Frontend

**Description**: No systematic accessibility testing. Shadcn/ui provides good baseline but custom components need audit.

**Specific Criteria**:
- [ ] Run Lighthouse accessibility audit on all 91 pages
- [ ] Fix all Critical/Serious axe-core violations
- [ ] Add ARIA labels to all interactive components
- [ ] Ensure keyboard navigation works on all data tables
- [ ] Color contrast: WCAG AA on all text elements
- [ ] Screen reader testing on top 10 workflows

---

### TASK-017: Data Export & Reporting
**Status**: Partial | **Estimate**: 1 sprint | **Owner**: Full Stack

**Description**: Only profit products has xlsx export. Need exports for all major domains.

**Specific Criteria**:
- [ ] Add CSV/XLSX export to: FBA inventory, orders, finance ledger, ads campaigns, refund anomalies
- [ ] Add scheduled report generation (daily/weekly email)
- [ ] Add export queue for large datasets (>10K rows)
- [ ] Frontend: unified export button component

---

### TASK-018: Development Environment Automation
**Status**: Manual | **Estimate**: 1 sprint | **Owner**: DevOps

**Description**: Developer setup requires manual .env configuration, database seeding, and Docker knowledge.

**Specific Criteria**:
- [ ] Create `make setup` one-command dev environment
- [ ] Docker Compose dev profile with hot-reload
- [ ] Auto-seed database with test data
- [ ] Pre-commit hooks: ruff format, ruff lint, mypy
- [ ] README.md with 5-minute quickstart guide
- [ ] VS Code devcontainer configuration

---

## Completed (26 Sprints)

| Sprint | Key Deliverables |
|---|---|
| E0 | Project scaffolding, FastAPI + React setup, Docker Compose |
| S1-2 | Amazon SP-API integration, order sync, profitability engine |
| S3-4 | Family mapper, product catalog, Ergonode sync |
| S5-6 | Pricing engine, Buy Box tracking, ads integration |
| S7-8 | FBA operations, inventory management, content ops |
| S9-10 | Finance center, tax compliance, exchange rates |
| S11-12 | Strategy engine, seasonality, decision intelligence |
| S13-14 | Courier integration (DHL + GLS), logistics cost allocation |
| S15-16 | Intelligence engines (catalog health, buybox radar, inventory risk) |
| S17-18 | Repricing engine, content optimization, refund anomaly |
| S19-20 | Event backbone (SQS topology, event wiring), notifications |
| S21-22 | Operator console, returns module, guardrails |
| S23-24 | Executive dashboard, listing state, catalog definitions |
| S25-26 | Account hub, multi-seller foundation, final test green |
