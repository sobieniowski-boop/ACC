# ACC Comprehensive Task List
## Amazon Command Center — FY2026/27 Development Plan

| Field | Value |
|-------|-------|
| **Date** | 2026-03-13 |
| **Author** | SeniorProjectManager Agent |
| **Classification** | Strategic — Internal |
| **Prepared for** | Miłosz Sobieniowski, Founder |
| **Source Documents** | Phase 0 Executive Summary, Data Audit Report, Feedback Synthesis Report, Tech Stack Assessment, UX Research Report, Market Intelligence Report, Strategic Portfolio Plan, System Architecture Spec, Financial Plan, Brand Identity System, UX Architecture, ML System Design |
| **Planning Horizon** | Apr 2026 – Mar 2027+ (Phases 1–4) |
| **Review Cadence** | Weekly (Phase 1), Bi-weekly (Phase 2+) |

---

## Document Navigation

1. [Work Breakdown Structure (WBS)](#1-work-breakdown-structure-wbs)
2. [Detailed Task List](#2-detailed-task-list)
3. [Dependency Map](#3-dependency-map)
4. [Critical Path Analysis](#4-critical-path-analysis)
5. [Risk Register for Implementation](#5-risk-register-for-implementation)
6. [Phase Gate Checklists](#6-phase-gate-checklists)
7. [Appendix A: Effort Summary by Phase](#appendix-a-effort-summary-by-phase)
8. [Appendix B: Source Traceability Matrix](#appendix-b-source-traceability-matrix)

---

## Conventions

- **SP** = Story Points (1=hours, 2=1-2d, 3=3-5d, 5=1-2wk, 8=2-4wk, 13=1-2mo)
- **P0** = Must-have for phase gate; **P1** = High value; **P2** = Should-have; **P3** = Nice-to-have
- All tasks assume **solo developer** (Miłosz), 20h/week deep engineering, 50h/week total
- All SQL reads follow `WITH (NOLOCK)` convention per ADR-002
- All SQL writes follow `SET LOCK_TIMEOUT 30000` convention

---

# 1. Work Breakdown Structure (WBS)

## 1.1 Phase 1 — HARDEN (Apr 1 – May 15, 2026)

| WBS | Task ID | Title | SP | Week |
|-----|---------|-------|----|------|
| 1.1.1 | T-101 | Set up UptimeRobot external monitoring | 1 | W1 |
| 1.1.2 | T-102 | Replace FX rate silent `return 1.0` with alert system | 2 | W1 |
| 1.1.3 | T-103 | Add ads sync heartbeat updates during execution | 2 | W2 |
| 1.1.4 | T-104 | Implement single-flight guard for ads sync | 2 | W2 |
| 1.1.5 | T-105 | Add recommended indexes for PPT query | 2 | W3 |
| 1.1.6 | T-106 | Implement SQL-level pagination for PPT backend | 5 | W3-4 |
| 1.1.7 | T-107 | Implement server-side pagination for PPT frontend | 3 | W4 |
| 1.1.8 | T-108 | Hide/collapse 33 underused sidebar pages | 2 | W5 |
| 1.1.9 | T-109 | Replace python-jose with pyjwt | 1 | W5 |
| 1.1.10 | T-110 | Create Data Observability Layer (baseline alarms) | 3 | W2-3 |
| 1.1.11 | T-111 | Bridge FBA fees to order lines | 2 | W3 |
| 1.1.12 | T-112 | Archive/drop 72 empty tables | 1 | W5 |
| 1.1.13 | T-113 | Add materialized `acc_profit_daily_snapshot` table | 3 | W4 |
| 1.1.14 | T-114 | Implement Data Freshness API endpoint | 2 | W3 |
| 1.1.15 | T-115 | Fix test suite to reach ≥85% pass rate | 3 | W6 |
| 1.1.16 | T-116 | Write core system runbooks (top 5 incidents) | 2 | W6 |
| 1.1.17 | T-117 | Phase 1 gate review and stabilization | 1 | W7 |

**Phase 1 Total: ~34 SP (~155 hours, ~7.7 weeks at 20h/week deep eng)**

## 1.2 Phase 2 — BETA (May 16 – Sep 30, 2026)

| WBS | Task ID | Title | SP | Month |
|-----|---------|-------|----|-------|
| 1.2.1 | T-201 | Multi-tenant database schema (tenant isolation) | 5 | May-Jun |
| 1.2.2 | T-202 | User registration and onboarding flow (backend) | 5 | Jun |
| 1.2.3 | T-203 | User onboarding wizard (frontend) | 3 | Jun |
| 1.2.4 | T-204 | Stripe billing integration (3-tier pricing) | 5 | Jul |
| 1.2.5 | T-205 | Weight-based logistics model v3 | 8 | Jun-Jul |
| 1.2.6 | T-206 | Morning Brief auto-digest (email) | 3 | Aug |
| 1.2.7 | T-207 | Add rate limiting (slowapi) | 2 | Jun |
| 1.2.8 | T-208 | Implement RBAC enforcement for multi-user | 3 | Jun |
| 1.2.9 | T-209 | API versioning (URL-based /api/v1/) | 2 | May |
| 1.2.10 | T-210 | UX sidebar consolidation (12 → 7 groups) | 3 | Jun |
| 1.2.11 | T-211 | Breadcrumbs + Recently Visited navigation | 2 | Jul |
| 1.2.12 | T-212 | Global Search (⌘K) | 3 | Jul |
| 1.2.13 | T-213 | Module Visibility Toggle (Settings page) | 2 | Jul |
| 1.2.14 | T-214 | Email delivery integration (Resend/Postmark) | 2 | Jun |
| 1.2.15 | T-215 | Private beta recruitment and launch | 2 | Jun |
| 1.2.16 | T-216 | Product analytics integration (PostHog) | 2 | Jul |
| 1.2.17 | T-217 | NPS micro-survey component (in-app) | 1 | Aug |
| 1.2.18 | T-218 | Connection pooling via SQLAlchemy QueuePool | 3 | May |
| 1.2.19 | T-219 | Introduce Alembic for schema migrations | 3 | Jun |
| 1.2.20 | T-220 | Marketing landing page (static) | 2 | Jun |
| 1.2.21 | T-221 | Error response standardization (RFC 7807) | 2 | May |
| 1.2.22 | T-222 | Unified alert triage view | 5 | Sep |

**Phase 2 Total: ~63 SP (~310 hours, ~15.5 weeks at 20h/week deep eng)**

## 1.3 Phase 3 — LAUNCH (Oct 2026 – Jan 2027)

| WBS | Task ID | Title | SP | Month |
|-----|---------|-------|----|-------|
| 1.3.1 | T-301 | Weekly P&L PDF report (WeasyPrint) | 5 | Oct |
| 1.3.2 | T-302 | Profit→Refund drill path | 5 | Nov |
| 1.3.3 | T-303 | Bank feed automation (basic) | 8 | Nov-Dec |
| 1.3.4 | T-304 | DACH marketplace deep testing | 3 | Dec |
| 1.3.5 | T-305 | German UI string localization (critical paths) | 3 | Dec |
| 1.3.6 | T-306 | Public marketing site + content | 3 | Oct |
| 1.3.7 | T-307 | Referral program implementation | 2 | Nov |
| 1.3.8 | T-308 | Help center / documentation site (20+ articles) | 5 | Nov-Jan |
| 1.3.9 | T-309 | Onboarding funnel optimization (from analytics) | 3 | Oct-Nov |
| 1.3.10 | T-310 | Activate Celery workers for heavy sync jobs | 5 | Oct |
| 1.3.11 | T-311 | Security hardening: CORS, security headers, WAF eval | 3 | Oct |
| 1.3.12 | T-312 | PII/GDPR compliance audit | 2 | Jan |
| 1.3.13 | T-313 | E2E testing with Playwright (critical flows) | 5 | Oct-Nov |
| 1.3.14 | T-314 | Phase 3 gate review | 1 | Jan |

**Phase 3 Total: ~53 SP (~260 hours, ~16 weeks at 16h/week deep eng)**

## 1.4 Phase 4 — SCALE (Feb 2027+)

| WBS | Task ID | Title | SP | Quarter |
|-----|---------|-------|----|---------|
| 1.4.1 | T-401 | Azure SQL tier upgrade evaluation (S3 → S4/P1) | 2 | Q1'27 |
| 1.4.2 | T-402 | Horizontal API scaling (2-4 replicas) | 5 | Q1'27 |
| 1.4.3 | T-403 | DACH market soft launch (DE beta) | 3 | Q1'27 |
| 1.4.4 | T-404 | Contractor onboarding (first frontend hire) | 3 | Q1-Q2'27 |
| 1.4.5 | T-405 | JWT RS256 migration (from HS256) | 3 | Q1'27 |
| 1.4.6 | T-406 | Mobile responsive design improvements | 5 | Q2'27 |
| 1.4.7 | T-407 | Time-series analytics layer | 5 | Q2'27 |
| 1.4.8 | T-408 | AI-powered margin alerts | 5 | Q3'27 |
| 1.4.9 | T-409 | Export & reporting infrastructure | 5 | Q2'27 |

**Phase 4 Total: ~36 SP (ongoing, staff-augmented)**

---

# 2. Detailed Task List

---

## Phase 1 Tasks (T-1xx) — HARDEN

---

### T-101: Set Up UptimeRobot External Monitoring

| Field | Value |
|-------|-------|
| Phase | 1 |
| Priority | P0 |
| Effort | 1 SP (~2 hours) |
| Owner | Miłosz |
| Depends On | — (no dependency) |
| Blocks | T-117 (Phase gate needs H-5 uptime metric) |

**Requirement Source**: "C-5: External uptime monitor (UptimeRobot) — 30 min" — [Source: PHASE_0_EXECUTIVE_SUMMARY §Conditions]

"No external uptime check; backend offline = silent failure" — [Source: TECH_STACK_ASSESSMENT §Layer 6 Observability]

**Scope**:
- Create UptimeRobot Pro account ($7/mo)
- Configure HTTP(S) monitor on `/health` endpoint, 1-min interval
- Configure alert channels: email + SMS (or Telegram)
- Add monitors for Azure SQL availability (TCP check on port 1433)
- Add monitor for Redis availability

**Acceptance Criteria**:
- [ ] UptimeRobot dashboard shows 3+ monitors active
- [ ] Alert fires within 2 minutes when backend is manually stopped
- [ ] 7 consecutive days of 99%+ uptime recorded (H-5 gate criterion)

**Out of Scope**: Custom status page, multi-region monitoring, SLA reporting

---

### T-102: Replace FX Rate Silent `return 1.0` with Alert System

| Field | Value |
|-------|-------|
| Phase | 1 |
| Priority | P0 |
| Effort | 2 SP (~8 hours) |
| Owner | Miłosz |
| Depends On | — |
| Blocks | T-117 (H-3 gate) |

**Requirement Source**: "`return 1.0 # TODO: raise once all callers handle missing rates`" at `finance_center/service.py:506` — [Source: FEEDBACK_SYNTHESIS §PP-05, RICE 7.2]

"FX rates fail silently (RICE 7.2) — 1–2 days" — [Source: PHASE_0_EXECUTIVE_SUMMARY §C-3]

**Scope**:
- Modify `_lookup_fx_rate()` in `finance_center/service.py` to raise a typed exception (`FXRateStaleError`) instead of returning 1.0
- Add guardrail check: if `acc_exchange_rate` latest `rate_date` > 24h stale, trigger alert
- Add callers' error handling: catch `FXRateStaleError`, log to `acc_al_alerts`, fall back to last-known rate with `confidence_flag='stale_fx'`
- Create Sentry alert rule for repeated FX failures
- Verify all 3 FX consumers handle the exception: `finance_center/service.py`, `profit/cost_model.py` (`_fx_rate_for_currency`), `ads_sync.py` (PLN conversion)

**Acceptance Criteria**:
- [ ] No `return 1.0` fallback exists in codebase for FX rate lookups
- [ ] Alert fires within 5 minutes when FX rate data is >24h stale
- [ ] Unit test: `_lookup_fx_rate()` with missing rate raises `FXRateStaleError`
- [ ] Integration test: profit calculation flags `stale_fx` when rate is stale
- [ ] Guardrail dashboard shows FX freshness check passing

**Out of Scope**: FX rate hedging, multi-provider failover (ECB→NBP auto-switch)

---

### T-103: Add Ads Sync Heartbeat Updates During Execution

| Field | Value |
|-------|-------|
| Phase | 1 |
| Priority | P0 |
| Effort | 2 SP (~6 hours) |
| Owner | Miłosz |
| Depends On | — |
| Blocks | T-104, T-117 (H-2 gate) |

**Requirement Source**: "sync_ads does not update heartbeat during execution" — [Source: FEEDBACK_SYNTHESIS §PP-06]

"C-2: Ads sync heartbeat + single-flight guard — 2–3 days" — [Source: PHASE_0_EXECUTIVE_SUMMARY §Conditions]

**Scope**:
- Modify `sync_ads_daily_reports()` and `sync_ads_product_reports()` in `ads_sync.py` to update `acc_al_jobs.last_heartbeat_at` every 30 seconds during execution
- Add heartbeat update at each major step: per-profile, per-day, per-report-type
- Implement zombie detection: if `last_heartbeat_at` > 10 minutes stale while job status is 'running', mark as zombie
- Add guardrail check for ads data freshness: `ads_product_day` latest `report_date` < 6h

**Acceptance Criteria**:
- [ ] `last_heartbeat_at` updates every 30s during ads sync execution (verified via SQL query)
- [ ] Zombie job detected and logged within 15 minutes of actual hang
- [ ] Ads data freshness guardrail passes when `ads_product_day` latest date is < 6h stale (H-2)
- [ ] Guardrails dashboard shows ads freshness check status

**Out of Scope**: Auto-restart of zombie jobs, Celery-based retry queue

---

### T-104: Implement Single-Flight Guard for Ads Sync

| Field | Value |
|-------|-------|
| Phase | 1 |
| Priority | P0 |
| Effort | 2 SP (~6 hours) |
| Owner | Miłosz |
| Depends On | T-103 (heartbeat needed for proper lock detection) |
| Blocks | T-117 (H-2 gate) |

**Requirement Source**: "C-2: Ads sync heartbeat + single-flight guard — 2–3 days" — [Source: PHASE_0_EXECUTIVE_SUMMARY §Conditions]

"zombie sync jobs in acc_al_jobs with no heartbeat, multiple overlapping manual runs" — [Source: FEEDBACK_SYNTHESIS §PP-02]

**Scope**:
- Add Redis `SET NX EX` lock before `run_full_ads_sync()` starts (lock key: `ads_sync:lock`, TTL: 30 minutes, renewed every 60s via heartbeat)
- If lock already held and heartbeat is fresh (< 10 min), skip with log: "Ads sync already running"
- If lock held but heartbeat is stale (> 10 min), force-acquire lock (zombie override)
- Ensure lock is released in `finally` block on normal completion or error
- Update scheduler to check lock before dispatching ads sync job

**Acceptance Criteria**:
- [ ] Two concurrent ads sync calls: second skips with logged message
- [ ] Zombie lock (held > 10 min, no heartbeat) is automatically broken
- [ ] Lock released cleanly on both success and exception
- [ ] Unit test: concurrent lock acquisition returns `False` for second caller

**Out of Scope**: Distributed lock across multiple API instances (deferred to Celery activation)

---

### T-105: Add Recommended Database Indexes for PPT Query

| Field | Value |
|-------|-------|
| Phase | 1 |
| Priority | P0 |
| Effort | 2 SP (~4 hours) |
| Owner | Miłosz |
| Depends On | — |
| Blocks | T-106 (indexes needed before pagination shows full benefit) |

**Requirement Source**: "Recommended Index Additions" — [Source: SYSTEM_ARCHITECTURE_SPEC §2.2]

"PPT query joins 6+ tables with complex CTEs. Without proper indexing, the main aggregation takes 14.5s" — [Source: SYSTEM_ARCHITECTURE_SPEC §2.2]

**Scope**:
- Create `IX_acc_order_profit_core` on `acc_order (status, purchase_date, marketplace_id)` with INCLUDE 
- Create `IX_acc_order_line_profit` on `acc_order_line (order_id)` with INCLUDE
- Create `IX_acc_finance_tx_shipping` on `acc_finance_transaction` filtered index for shipping charges
- Create `IX_acc_finance_tx_cm2` on `acc_finance_transaction (posted_date, marketplace_id)`
- Create `IX_acc_ads_pd_profit` on `acc_ads_product_day (asin, report_date, marketplace_id)`
- Create `IX_acc_fx_lookup` on `acc_exchange_rate (currency, rate_date DESC)`
- Create `IX_logistics_fact_lookup` on `acc_order_logistics_fact (amazon_order_id)`
- Run each in a maintenance window; measure query time before/after

**Acceptance Criteria**:
- [ ] All 7 indexes created on Azure SQL production
- [ ] PPT query execution time reduced by ≥ 50% (measure via SET STATISTICS TIME ON)
- [ ] No index creation errors or blocking during creation
- [ ] Index usage confirmed via `sys.dm_db_index_usage_stats` after 24h

**Out of Scope**: Index maintenance schedule, unused index cleanup, columnstore indexes

---

### T-106: Implement SQL-Level Pagination for PPT (Backend)

| Field | Value |
|-------|-------|
| Phase | 1 |
| Priority | P0 |
| Effort | 5 SP (~35 hours) |
| Owner | Miłosz |
| Depends On | T-105 (indexes), T-113 (materialized view fallback) |
| Blocks | T-107 (frontend), T-117 (H-1 gate) |

**Requirement Source**: "C-1: SQL pagination for PPT (target < 2s) — 5–8 days" — [Source: PHASE_0_EXECUTIVE_SUMMARY §Conditions]

"No SQL-level pagination (CRITICAL)" — "all 4,300 groups fetched to Python, in-memory sort/page" — [Source: FEEDBACK_SYNTHESIS §PP-01]

"§3.4 PPT pagination (OFFSET/FETCH)" — [Source: SYSTEM_ARCHITECTURE_SPEC §3.4]

**Scope**:
- Refactor `get_product_profit_table()` in the profit intelligence module (split from `app.intelligence.profit`) to accept `page`, `page_size`, `sort_by`, `sort_dir` parameters
- Move pagination from Python to SQL: `ORDER BY {sort_col} {sort_dir} OFFSET @offset ROWS FETCH NEXT @page_size ROWS ONLY`
- Add `COUNT(*) OVER()` window function for total count without separate query
- Implement server-side sorting for all major columns: revenue, CM1, CM1%, units, COGS, ACoS
- Add parameterized `search` filter pushed to SQL WHERE clause
- Parameters use SQL parameterization (`?` placeholders) — never string concatenation (SQL injection prevention)
- Keep in-memory cache keyed by (page, page_size, sort_by, sort_dir, date_range, marketplace, search)
- Endpoint: `GET /api/v1/profit-v2/products?page=1&page_size=50&sort_by=cm1_pln&sort_dir=desc`
- Response format: `{ "items": [...], "total": 4300, "page": 1, "page_size": 50 }`

**Acceptance Criteria**:
- [ ] PPT endpoint returns paginated results with `total` count
- [ ] Server-side sort works for ≥6 columns
- [ ] `page_size=50` query responds in < 2.0s (p95) — H-1 gate criterion
- [ ] No full-table scans in execution plan (all use indexes from T-105)
- [ ] SQL injection test: sort_by/search parameters properly parameterized
- [ ] Existing downstream consumers (dashboard KPI aggregations) still work

**Out of Scope**: Cursor-based pagination (OFFSET/FETCH is sufficient at 4.3K rows), ElasticSearch integration

---

### T-107: Implement Server-Side Pagination for PPT (Frontend)

| Field | Value |
|-------|-------|
| Phase | 1 |
| Priority | P0 |
| Effort | 3 SP (~12 hours) |
| Owner | Miłosz |
| Depends On | T-106 (backend pagination API) |
| Blocks | T-117 (H-1 gate) |

**Requirement Source**: "PPT load time < 2s (from 14.5s) by May 2026" — [Source: STRATEGIC_PORTFOLIO_PLAN §2.4 KR1]

**Scope**:
- Refactor `ProductProfitTable.tsx` to use server-side pagination via TanStack Query
- Replace client-side sort/filter with API query parameters (`page`, `page_size`, `sort_by`, `sort_dir`, `search`)
- Add pagination controls UI (page numbers, next/prev, page size selector)
- Add column header sort indicators (click to sort)
- Add debounced search input (300ms delay before API call)
- Preserve URL query parameters in browser URL for shareable links
- Show loading skeleton during page transitions

**Acceptance Criteria**:
- [ ] PPT renders first page in < 2s from navigation (measured in DevTools)
- [ ] Clicking column headers triggers server-side sort (no client-side re-sort)
- [ ] Search input filters results via API (debounced, no client-side filter)
- [ ] Pagination navigation works: first, prev, next, last, page size change
- [ ] URL updates with pagination state (back button restores previous page/sort)

**Out of Scope**: Infinite scroll, virtual table rendering, column resizing

---

### T-108: Hide/Collapse 33 Underused Sidebar Pages

| Field | Value |
|-------|-------|
| Phase | 1 |
| Priority | P0 |
| Effort | 2 SP (~6 hours) |
| Owner | Miłosz |
| Depends On | — |
| Blocks | T-117 (H-4 gate) |

**Requirement Source**: "C-4: Hide/collapse 33 underused pages — 1–2 days" — [Source: PHASE_0_EXECUTIVE_SUMMARY §Conditions]

"~37% pages unused" — [Source: UX_RESEARCH_REPORT §BI-01]

"Sidebar pruned to ≤ 20 items" — [Source: STRATEGIC_PORTFOLIO_PLAN §Phase 1 W5]

**Scope**:
- In `Sidebar.tsx`, add a `hidden` flag to `NavGroup` / `NavItem` interface
- Hide the following groups by default (verified from sidebar audit: 13 groups, 70+ items currently):
  - **Strategy** (8 pages): Growth Engine, Opportunities, Playbooks, Market Expansion, Bundles, Experiments, Outcomes, Learning
  - **Seasonality** (6 pages): Dashboard, Heatmap, Entities, Clusters, Opportunities, Settings
  - **Tax Compliance** (10 pages): Overview through Settings
  - **Content** (6 pages): Studio, Compliance, Assets, Publish, Score, A/B Testing
  - **Cennik & Plan** (3 pages): Cennik & Buy Box, Repricing Engine, Planowanie
- Hidden pages remain accessible via direct URL (routes not removed)
- Count visible sidebar items ≤ 20

**Acceptance Criteria**:
- [ ] Sidebar shows ≤ 20 navigable items (H-4 gate)
- [ ] Hidden pages load correctly when accessed via direct URL
- [ ] No React router errors or broken links
- [ ] Strategy, Seasonality, Tax, Content, Pricing groups are not visible in sidebar

**Out of Scope**: Module Visibility Toggle UI (T-213, Phase 2), permanent route removal

---

### T-109: Replace python-jose with pyjwt

| Field | Value |
|-------|-------|
| Phase | 1 |
| Priority | P1 |
| Effort | 1 SP (~2 hours) |
| Owner | Miłosz |
| Depends On | — |
| Blocks | — |

**Requirement Source**: "TD-12: REPLACE python-jose with pyjwt — 1 hour, 0 risk" — [Source: TECH_STACK_ASSESSMENT §2.2]

"§4.3: python-jose → pyjwt migration (1 hour)" — [Source: SYSTEM_ARCHITECTURE_SPEC §4.3]

**Scope**:
- In `core/security.py`: replace `from jose import JWTError, jwt` with `import jwt; from jwt import PyJWTError`
- Update `jwt.encode()` / `jwt.decode()` calls (API is nearly identical)
- Update `except JWTError` to `except (jwt.InvalidTokenError, jwt.ExpiredSignatureError)`
- Replace `python-jose[cryptography]` with `PyJWT[crypto]` in `requirements.txt`
- Run all auth-related tests

**Acceptance Criteria**:
- [ ] `python-jose` removed from `requirements.txt`
- [ ] `PyJWT` in `requirements.txt`
- [ ] Login and token refresh work end-to-end
- [ ] All existing auth tests pass

**Out of Scope**: RS256 migration (T-405, Phase 4), token rotation changes

---

### T-110: Create Data Observability Layer (Baseline Alarms)

| Field | Value |
|-------|-------|
| Phase | 1 |
| Priority | P0 |
| Effort | 3 SP (~15 hours) |
| Owner | Miłosz |
| Depends On | T-101 (uptime monitoring), T-102 (FX alerts) |
| Blocks | T-117 (H-8 gate: zero silent failures) |

**Requirement Source**: "R-01: Implement Data Observability Layer (🔴 Critical) — 2 days" — [Source: DATA_AUDIT_REPORT §Recommendations]

"H-8: Zero silent failures — All 8+ guardrails passing with alert on fail" — [Source: STRATEGIC_PORTFOLIO_PLAN §Phase 1 Exit Criteria]

**Scope**:
- Create a unified data observability service (`app/services/data_observability.py`) that aggregates all guardrail checks:
  1. Order sync freshness (< 45 min)
  2. Finance sync freshness (< 24h)
  3. Ads sync freshness (< 6h)
  4. FX rate freshness (< 24h)
  5. Inventory freshness (< 24h)
  6. Profit calculation freshness (< 12h)
  7. Courier data freshness (< 24h)
  8. Guardrail pass/fail aggregation
- Each check writes result to `acc_guardrail_results` with severity level
- On failure: write to `acc_al_alerts` with actionable message + Sentry alert
- Create `/api/v1/system/observability` endpoint returning all checks in one response
- Wire to scheduler: run every 15 minutes

**Acceptance Criteria**:
- [ ] All 8 guardrail checks execute every 15 minutes
- [ ] Failed check → alert in `acc_al_alerts` within 5 minutes
- [ ] Failed check → Sentry notification
- [ ] `/api/v1/system/observability` returns status of all 8 checks
- [ ] H-8 verified: no silent failures for 7 consecutive days

**Out of Scope**: Grafana dashboards, PagerDuty integration, SLA reporting

---

### T-111: Bridge FBA Fees to Order Lines

| Field | Value |
|-------|-------|
| Phase | 1 |
| Priority | P0 |
| Effort | 2 SP (~8 hours) |
| Owner | Miłosz |
| Depends On | — |
| Blocks | T-117 (H-6 DQ Score gate) |

**Requirement Source**: "R-03: Bridge FBA Fees to Order Lines (🔴 Critical) — 1 day" — [Source: DATA_AUDIT_REPORT §Recommendations]

"Only 30.1% of order lines have FBA fees, despite 34% of orders being AFN (FBA)" — [Source: DATA_AUDIT_REPORT §Executive Summary]

**Scope**:
- Enhance `step_bridge_fees()` in the order pipeline to match FBA fees from `acc_finance_transaction` to `acc_order_line`:
  - Match by `amazon_order_id` + `sku` for line-level FBA fees
  - For order-level FBA fees (no SKU in finance record), allocate proportionally by `item_price`
- Handle edge cases: partial shipments, multi-quantity orders, split fees
- Backfill: run once for historical orders (last 12 months)
- Update `acc_order_line.fba_fee_pln` column

**Acceptance Criteria**:
- [ ] FBA fee coverage ≥ 80% for AFN orders (from 30.1%)
- [ ] Backfill completed for last 12 months without errors
- [ ] Incremental bridging runs as part of each order sync cycle
- [ ] CM1 calculation reflects bridged FBA fees correctly

**Out of Scope**: FBA storage fees allocation, long-term storage fee breakdown

---

### T-112: Archive/Drop 72 Empty Tables

| Field | Value |
|-------|-------|
| Phase | 1 |
| Priority | P1 |
| Effort | 1 SP (~3 hours) |
| Owner | Miłosz |
| Depends On | — |
| Blocks | — |

**Requirement Source**: "R-05: Schema Cleanup — Archive Empty Tables (🟠 High) — 2 hours" — [Source: DATA_AUDIT_REPORT §Recommendations]

"72 empty tables (38.5%) — significant schema bloat" — [Source: DATA_AUDIT_REPORT §1.3]

**Scope**:
- Query all tables with 0 rows: `SELECT name FROM sys.tables WHERE OBJECTPROPERTY(object_id, 'TableHasRows') = 0`
- Cross-reference with codebase (ensure no `ensure_*_schema()` function actively uses the table)
- For tables with no code references: prefix with `__archived_` and add a comment
- For tables referenced by frozen modules (Content, Strategy, Seasonality, Tax stubs): keep but document as "frozen schema"
- Create a manifest: `docs/archived_tables_2026-04.md`

**Acceptance Criteria**:
- [ ] ≥ 50 empty tables archived (prefixed or documented)
- [ ] No application errors after archival
- [ ] Manifest document lists all archived tables with reason

**Out of Scope**: Dropping tables permanently (reversible archival only), schema migration for active tables

---

### T-113: Add Materialized `acc_profit_daily_snapshot` Table

| Field | Value |
|-------|-------|
| Phase | 1 |
| Priority | P1 |
| Effort | 3 SP (~15 hours) |
| Owner | Miłosz |
| Depends On | T-105 (indexes) |
| Blocks | T-106 (PPT can use snapshot as fast-path) |

**Requirement Source**: "R-06: Add Materialized Analytics Views (🟡 Medium) — 1 day" — [Source: DATA_AUDIT_REPORT §Recommendations]

"§2.3 acc_profit_daily_snapshot materialized view design" — [Source: SYSTEM_ARCHITECTURE_SPEC §2.3]

**Scope**:
- Create `acc_profit_daily_snapshot` table per spec in SYSTEM_ARCHITECTURE_SPEC §2.3 (columns: snapshot_date, group_key, marketplace_id, entity_type, revenue/cogs/fees/ads/cm1/cm2/np/margin/confidence)
- Create refresh logic in `scheduler/profit.py`: full rebuild for T-1 (previous day) after nightly profit calc
- Create index `IX_profit_snap_date` on (snapshot_date DESC, marketplace_id)
- Update PPT query to use snapshot for date ranges fully within snapshot coverage; fall through to live CTE for current day
- Initial populate: build snapshots for last 90 days

**Acceptance Criteria**:
- [ ] Snapshot table created and populated for last 90 days
- [ ] Nightly refresh runs without error
- [ ] PPT query against snapshot-covered range responds in < 1.5s
- [ ] Snapshot data matches live CTE query within 0.01% (floating point tolerance)

**Out of Scope**: Real-time streaming refresh, hourly granularity, custom aggregation levels

---

### T-114: Implement Data Freshness API Endpoint

| Field | Value |
|-------|-------|
| Phase | 1 |
| Priority | P1 |
| Effort | 2 SP (~6 hours) |
| Owner | Miłosz |
| Depends On | T-110 (observability layer) |
| Blocks | — |

**Requirement Source**: "R-07: Implement Data Freshness API (🟡 Medium) — 4 hours" — [Source: DATA_AUDIT_REPORT §Recommendations]

**Scope**:
- Create `GET /api/v1/system/data-freshness` endpoint returning:
  - Per-source freshness: orders, finances, ads, inventory, FX rates, courier, COGS, profit calculation
  - Each entry: `{ source, last_updated, staleness_minutes, threshold_minutes, status: "ok" | "stale" | "critical" }`
- Query actual table timestamps (MAX(updated_at), MAX(report_date), etc.)
- Cache response for 5 minutes (Redis)
- Wire to frontend Data Quality page for real-time freshness display

**Acceptance Criteria**:
- [ ] Endpoint returns freshness for ≥ 8 data sources
- [ ] Stale data correctly flagged (orders > 45min, ads > 6h, FX > 24h)
- [ ] Response time < 500ms
- [ ] Frontend Data Quality page shows live freshness data

**Out of Scope**: Historical freshness trends, SLA compliance reports

---

### T-115: Fix Test Suite to Reach ≥85% Pass Rate

| Field | Value |
|-------|-------|
| Phase | 1 |
| Priority | P0 |
| Effort | 3 SP (~15 hours) |
| Owner | Miłosz |
| Depends On | T-102, T-103, T-109 (code changes may affect tests) |
| Blocks | T-117 (H-7 gate) |

**Requirement Source**: "Test pass rate ≥ 85% (from 73%)" — [Source: STRATEGIC_PORTFOLIO_PLAN §H-7]

"Test Coverage (backend): 422/577 passing (73%)" — [Source: TECH_STACK_ASSESSMENT §Layer 8]

**Scope**:
- Run full test suite, categorize failing tests:
  - Tests broken by recent code changes → fix
  - Tests with stale fixtures/mocks → update
  - Tests for frozen modules (Strategy, Seasonality, Tax) → mark as `@pytest.mark.skip(reason="module frozen")`
  - Flaky tests → add retry or fix root cause
- Add missing tests for T-102 (FX alert), T-103 (heartbeat), T-109 (pyjwt)
- Frontend: run `vitest` and fix failures
- Target: ≥ 85% combined pass rate (backend + frontend)

**Acceptance Criteria**:
- [ ] `pytest` pass rate ≥ 85% (verified in CI)
- [ ] `vitest` pass rate ≥ 85%
- [ ] No test depends on live Azure SQL / external API (all mocked)
- [ ] CI pipeline runs in < 10 minutes

**Out of Scope**: 100% coverage, E2E tests (T-313, Phase 3), performance tests

---

### T-116: Write Core System Runbooks (Top 5 Incidents)

| Field | Value |
|-------|-------|
| Phase | 1 |
| Priority | P1 |
| Effort | 2 SP (~8 hours) |
| Owner | Miłosz |
| Depends On | T-110 (observability alerts defined) |
| Blocks | — |

**Requirement Source**: "Core runbook documentation (deployment, incident response, data pipeline) by May 2026" — [Source: STRATEGIC_PORTFOLIO_PLAN §Objective 5 KR2]

"Bus factor = 1" — [Source: PHASE_0_EXECUTIVE_SUMMARY §Complication 4]

**Scope**:
- Write 5 runbooks in `docs/runbooks/`:
  1. **Backend crash recovery**: restart uvicorn, verify scheduler, check data freshness
  2. **Order sync failure**: diagnose SP-API errors, manual backfill procedure, verify guardrails
  3. **Ads sync zombie**: detect via heartbeat, force-kill lock, restart, verify data
  4. **FX rate staleness**: check NBP/ECB endpoints, manual rate insertion, alert resolution
  5. **Azure SQL connection failure**: connection string verification, failover, read-only mode
- Each runbook: trigger, diagnosis steps, resolution steps, escalation, verification checklist

**Acceptance Criteria**:
- [ ] 5 runbook files created in `docs/runbooks/`
- [ ] Each runbook has: trigger, diagnosis, resolution, verification sections
- [ ] Runbooks tested: follow each procedure manually to confirm accuracy

**Out of Scope**: Video walkthroughs, automated remediation, PagerDuty integration

---

### T-117: Phase 1 Gate Review and Stabilization

| Field | Value |
|-------|-------|
| Phase | 1 |
| Priority | P0 |
| Effort | 1 SP (~4 hours) |
| Owner | Miłosz |
| Depends On | T-101 through T-116 (all Phase 1 tasks) |
| Blocks | Phase 2 start |

**Requirement Source**: "W7 (May 13-15): Phase gate review" — [Source: STRATEGIC_PORTFOLIO_PLAN §Phase 1 Milestones]

**Scope**:
- Self-assessment against all 8 exit criteria (H-1 through H-8):
  - H-1: PPT load < 2.0s (p95)
  - H-2: Ads freshness < 6h
  - H-3: FX rate safety alert active
  - H-4: ≤ 20 sidebar pages
  - H-5: UptimeRobot 99%+ for 7 days
  - H-6: DQ Score ≥ 82/100
  - H-7: Test pass rate ≥ 85%
  - H-8: Zero silent failures
- Document: pass/fail for each, evidence, remaining risks
- GO/NO-GO decision for Phase 2

**Acceptance Criteria**:
- [ ] All 8 exit criteria assessed with evidence
- [ ] Gate review document written: `docs/PHASE_1_GATE_REVIEW.md`
- [ ] GO/NO-GO decision recorded

**Out of Scope**: External review, stakeholder sign-off (solo founder)

---

## Phase 2 Tasks (T-2xx) — BETA

---

### T-201: Multi-Tenant Database Schema (Tenant Isolation)

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P0 |
| Effort | 5 SP (~25 hours) |
| Owner | Miłosz |
| Depends On | T-117 (Phase 1 gate passed) |
| Blocks | T-202, T-204, T-208 |

**Requirement Source**: "User onboarding + multi-tenant basic (13-21d, required for revenue)" — [Source: STRATEGIC_PORTFOLIO_PLAN §Feature Priority Queue #2]

"§4.5: Multi-tenant roadmap (Phases 1-4)" — [Source: SYSTEM_ARCHITECTURE_SPEC §4.5]

**Scope**:
- Create `acc_tenant` table (id, name, amazon_seller_id, plan_tier, created_at, status)
- Create `acc_tenant_user` table (id, tenant_id, email, hashed_password, role, created_at)
- Add `tenant_id` column to core tables: `acc_order`, `acc_order_line`, `acc_product`, `acc_purchase_price`, `acc_ads_campaign`, `acc_finance_transaction`
- Create Row-Level Security policies or middleware-level tenant filtering per ADR in spec
- Migrate existing data to tenant_id = 1 (Miłosz's own tenant)
- Add `tenant_id` to JWT claims
- Update `connect_acc()` usage to include tenant context

**Acceptance Criteria**:
- [ ] `acc_tenant` and `acc_tenant_user` tables created
- [ ] Core tables have `tenant_id` column with default=1
- [ ] API queries filter by `tenant_id` from JWT claims
- [ ] Existing functionality works for tenant_id=1 (backward compatible)
- [ ] A second test tenant can be created and queried in isolation

**Out of Scope**: Self-serve tenant provisioning, data isolation guarantees for enterprise (Phase 3-4)

---

### T-202: User Registration and Onboarding Flow (Backend)

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P0 |
| Effort | 5 SP (~25 hours) |
| Owner | Miłosz |
| Depends On | T-201 (tenant schema), T-214 (email for verification) |
| Blocks | T-203, T-204, T-215 |

**Requirement Source**: "Multi-tenant basics + onboarding: User registration flow; tenant isolation; basic onboarding wizard" — [Source: STRATEGIC_PORTFOLIO_PLAN §Phase 2 May Milestone]

**Scope**:
- `POST /api/v1/auth/register` endpoint: email, password, seller_name, marketplace_ids
- Email verification via signed token link
- Create tenant + user in single transaction
- Amazon SP-API OAuth flow: user authorizes ACC to access their seller account
- Onboarding steps tracked in `acc_tenant_onboarding` table: registration → email verified → SP-API connected → first data sync → onboarding complete
- Rate limit: 5 registrations per IP per hour

**Acceptance Criteria**:
- [ ] Registration creates tenant + user
- [ ] Email verification link works (expires after 24h)
- [ ] SP-API OAuth connects to user's Amazon account
- [ ] Onboarding status tracked per step
- [ ] Rate limiting blocks abuse (> 5 registrations/IP/hour)

**Out of Scope**: Social login (Google, Amazon SSO), phone verification, two-factor auth

---

### T-203: User Onboarding Wizard (Frontend)

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P0 |
| Effort | 3 SP (~15 hours) |
| Owner | Miłosz |
| Depends On | T-202 (backend registration) |
| Blocks | T-215 (beta launch) |

**Requirement Source**: "Time-to-value: signup → first 'aha moment' < 10 minutes" — [Source: STRATEGIC_PORTFOLIO_PLAN §B-6]

**Scope**:
- Multi-step onboarding wizard component:
  1. Welcome + account creation (email, password)
  2. Amazon marketplace connection (SP-API OAuth)
  3. Data sync progress indicator (first orders, first profit calc)
  4. First insight: "Your estimated CM1 margin is X%" 
  5. Quick tour of key pages (Dashboard, PPT, Orders)
- Progress bar showing completion
- Skip functionality (can return later)
- Aha moment: show first profit insight within setup flow

**Acceptance Criteria**:
- [ ] 5-step wizard renders on first login for new users
- [ ] SP-API OAuth flow completes from wizard
- [ ] First profit insight shown within 10 minutes of signup (target B-6)
- [ ] Wizard state persisted (can resume after page refresh)

**Out of Scope**: Video tutorials, interactive product tour, gamification

---

### T-204: Stripe Billing Integration (3-Tier Pricing)

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P0 |
| Effort | 5 SP (~30 hours) |
| Owner | Miłosz |
| Depends On | T-201 (tenant schema), T-202 (user registration) |
| Blocks | T-215 (beta launch needs free tier), T-307 (referral) |

**Requirement Source**: "Stripe billing integration (5-8d, required for revenue)" — [Source: STRATEGIC_PORTFOLIO_PLAN §Feature Priority Queue #3]

"First paying customer by Jul 2026" — [Source: STRATEGIC_PORTFOLIO_PLAN §Objective 2 KR2]

**Scope**:
- Integrate Stripe Checkout for subscription management
- Create 3 pricing tiers per Financial Plan:
  - Free/Explorer: $0 (30-day profit view, 1 marketplace)
  - Pro/Seller Pro: €39/mo (full CM1/CM2/NP, 9 marketplaces, 12-month history)
  - Business/Business Pro: €79/mo (Pro + Ads attribution, logistics model, Morning Brief, API access)
- Stripe webhook handler for subscription lifecycle (created, updated, cancelled, payment_failed)
- `acc_tenant.plan_tier` updated on subscription change
- Feature gating middleware: check plan_tier before accessing gated features
- Trial: 14-day free trial of Pro (no credit card required)
- Annual billing option (15% discount = 2 months free)

**Acceptance Criteria**:
- [ ] Stripe Checkout creates a subscription for each tier
- [ ] Webhook updates `plan_tier` correctly on all lifecycle events
- [ ] Feature gating prevents free users from accessing Pro features
- [ ] Stripe test mode: full subscription lifecycle tested (create, upgrade, cancel)
- [ ] 14-day trial functions without credit card

**Out of Scope**: Enterprise custom pricing, usage-based billing, manual invoicing

---

### T-205: Weight-Based Logistics Model v3

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P1 |
| Effort | 8 SP (~50 hours) |
| Owner | Miłosz |
| Depends On | T-117 (Phase 1 gate) |
| Blocks | — |

**Requirement Source**: "Weight-based logistics model v3 (8-13d, 170% ROI)" — [Source: STRATEGIC_PORTFOLIO_PLAN §Feature Priority Queue #1]

"~75% of MFN orders lack actual billing-matched logistics cost — estimation model sku_country_v2 has 5–8% overestimate" — [Source: FEEDBACK_SYNTHESIS §PP-03]

"Does NOT consider weight, quantity, or product type" — [Source: logistics-pricing.md from repo memory]

**Scope**:
- Extend logistics cost model to factor in: package weight, dimensions, quantity, courier zone
- Source weight data from: `acc_product.weight_kg` (if available), GLS billing dimensions, manual entry
- Build weight→cost regression model from actual GLS/DHL billing data (265K+ records)
- Create fallback hierarchy: actual billing → weight model → sku_country_v2 → marketplace average
- Update `acc_order_logistics_fact` with model version indicator
- Retrain monthly on new billing data

**Acceptance Criteria**:
- [ ] Model trained on ≥ 100K actual billing records
- [ ] Mean Absolute Error (MAE) < 3% (down from 5-8%)
- [ ] Coverage: ≥ 85% of MFN orders get modeled cost (up from 24.8%)
- [ ] Model version tracked in `acc_order_logistics_fact.calc_version`
- [ ] CM1 calculation uses new model for orders without actual billing

**Out of Scope**: InPost integration, Poczta Polska, real-time carrier quote API

---

### T-206: Morning Brief Auto-Digest (Email)

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P1 |
| Effort | 3 SP (~15 hours) |
| Owner | Miłosz |
| Depends On | T-214 (email delivery), T-113 (snapshot data) |
| Blocks | — |

**Requirement Source**: "Morning Brief auto-digest (3-5d, 130% ROI)" — [Source: STRATEGIC_PORTFOLIO_PLAN §Feature Priority Queue #4]

"FR-05: Morning Brief digest — 3-5d, ROI 130%" — [Source: FEEDBACK_SYNTHESIS §Feature Requests]

**Scope**:
- Daily email at 07:00 CET with key KPIs:
  - Yesterday's revenue, CM1, CM1%, units, orders (with trend vs. prior day)
  - Top 3 gainers/losers by CM1 change
  - Active alerts count + top alert
  - Data freshness summary (green/yellow/red per source)
  - ACoS summary (if ads data available)
- HTML email template (responsive, dark + light mode)
- Per-user preference: enable/disable, delivery time
- Scheduler: APScheduler job triggers at configured time per tenant

**Acceptance Criteria**:
- [ ] Email sent daily at 07:00 CET with correct KPIs
- [ ] Email renders correctly in Gmail, Outlook, Apple Mail
- [ ] KPIs match dashboard values (within snapshot staleness)
- [ ] Enable/disable toggle in user settings

**Out of Scope**: Slack/Telegram delivery, real-time push notifications, weekly summary

---

### T-207: Add Rate Limiting (slowapi)

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P1 |
| Effort | 2 SP (~6 hours) |
| Owner | Miłosz |
| Depends On | — |
| Blocks | T-202 (rate limit on registration) |

**Requirement Source**: "TD-15: ADD rate limiting (slowapi per-IP limits)" — [Source: TECH_STACK_ASSESSMENT §Layer 7]

"§3.5: Rate limiting with slowapi" — [Source: SYSTEM_ARCHITECTURE_SPEC §3.5]

**Scope**:
- Install `slowapi` (already in requirements.txt as dependency)
- Configure per-endpoint rate limits:
  - Auth endpoints (`/auth/*`): 10 req/min per IP
  - PPT and heavy queries: 30 req/min per IP
  - General API: 100 req/min per IP
  - System/health: exempt
- Redis-backed rate limit store
- Return `429 Too Many Requests` with `Retry-After` header
- Exempt internal scheduler and health check calls

**Acceptance Criteria**:
- [ ] Auth endpoints rate-limited at 10 req/min
- [ ] PPT endpoint rate-limited at 30 req/min
- [ ] 429 response includes `Retry-After` header
- [ ] Redis stores rate limit counters
- [ ] Health endpoint not rate-limited

**Out of Scope**: Per-user rate limiting (use per-IP), rate limit dashboard, token bucket algorithm

---

### T-208: Implement RBAC Enforcement for Multi-User

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P0 |
| Effort | 3 SP (~15 hours) |
| Owner | Miłosz |
| Depends On | T-201 (tenant schema) |
| Blocks | T-202 (registration needs role assignment) |

**Requirement Source**: "§4.4: RBAC enforcement patterns" — [Source: SYSTEM_ARCHITECTURE_SPEC §4.4]

"RBAC Enforcement" — [Source: SYSTEM_ARCHITECTURE_SPEC §4.4]

**Scope**:
- Define roles in `core/security.py` (already has 5 roles in skeleton): `admin`, `manager`, `analyst`, `viewer`, `api_key`
- Create `require_role()` FastAPI dependency decorator
- Enforce on write endpoints: only `admin` and `manager` can modify settings, manual price entries
- Enforce on read endpoints: `viewer` can access dashboards but not raw data exports
- Add `role` to JWT claims (already present in `create_access_token`)
- Audit log: write `(user_id, action, resource, timestamp)` for all mutating operations

**Acceptance Criteria**:
- [ ] `require_role("admin")` blocks non-admin users on protected endpoints
- [ ] 4 roles have distinct permission levels
- [ ] Audit log captures all write operations with user context
- [ ] Existing endpoints continue to work for admin users

**Out of Scope**: Fine-grained permissions (field-level), custom role creation, OAuth2 scopes

---

### T-209: API Versioning (URL-based /api/v1/)

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P1 |
| Effort | 2 SP (~6 hours) |
| Owner | Miłosz |
| Depends On | — |
| Blocks | — |

**Requirement Source**: "ADR-006: URL-based API versioning" — [Source: SYSTEM_ARCHITECTURE_SPEC §3.3]

**Scope**:
- Verify all current endpoints are under `/api/v1/` prefix (most already are)
- Fix any endpoints not following versioned pattern
- Add API version header in responses: `X-API-Version: 1`
- Document versioning strategy in API docs (Swagger/ReDoc)
- Ensure OpenAPI spec generates correctly with version prefix

**Acceptance Criteria**:
- [ ] All public endpoints under `/api/v1/` prefix
- [ ] `X-API-Version` header present in all responses
- [ ] Swagger docs accessible at `/api/v1/docs`

**Out of Scope**: v2 API design, header-based versioning, GraphQL

---

### T-210: UX Sidebar Consolidation (12 → 7 Groups)

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P1 |
| Effort | 3 SP (~15 hours) |
| Owner | Miłosz |
| Depends On | T-108 (hidden pages from Phase 1) |
| Blocks | T-211, T-212, T-213 |

**Requirement Source**: "Sidebar: 12 → 7 groups" — [Source: UX_ARCHITECTURE §1.1]

"12-group sidebar exceeds Miller's Law (7±2)" — [Source: UX_RESEARCH_REPORT §BI-03]

**Scope**:
- Restructure `Sidebar.tsx` navigation from 13 groups to 7 per UX Architecture spec:
  1. **Dashboard** → Overview
  2. **Profitability** → PPT, CM1/CM2/NP views, Margin Analysis, Data Quality
  3. **Orders & Inventory** → Orders, FBA Inventory, Manage All, Purchase Prices
  4. **Advertising** → Campaigns, Ad Spend→Profit
  5. **Analytics** → Brand Analytics, Market Intelligence, Courier Analysis
  6. **Finance** → Finance Center, Ledger, Reconciliation, FX Rates
  7. **Settings** → Account, Integrations, Jobs, Scheduler, Module Visibility
- Update URL structure per UX Architecture §1.4 (`/profitability`, `/orders`, `/ads`, etc.)
- Add React Router redirects from old URLs to new URLs (backward compat)
- Make sidebar collapsible to 64px icon-only mode

**Acceptance Criteria**:
- [ ] Sidebar shows exactly 7 navigation groups
- [ ] All active features accessible within 2 clicks from sidebar
- [ ] Old URLs redirect to new structure (no 404s)
- [ ] Sidebar collapse/expand functions correctly

**Out of Scope**: Mobile responsive sidebar (T-406, Phase 4), keyboard navigation

---

### T-211: Breadcrumbs + Recently Visited Navigation

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P2 |
| Effort | 2 SP (~8 hours) |
| Owner | Miłosz |
| Depends On | T-210 (new URL structure) |
| Blocks | — |

**Requirement Source**: "Breadcrumbs: High priority, Sprint 2" / "Recently Visited (last 5 pages): Medium, Sprint 2" — [Source: UX_ARCHITECTURE §1.3]

**Scope**:
- Breadcrumb component in TopBar: auto-generated from URL path segments
- Recently Visited: store last 5 pages in localStorage, show in TopBar dropdown
- Breadcrumb format: `Dashboard > Profitability > Product Profit Table`

**Acceptance Criteria**:
- [ ] Breadcrumbs show on all pages except Dashboard
- [ ] Recently Visited shows last 5 unique pages
- [ ] Clicking breadcrumb navigates correctly

**Out of Scope**: Rich breadcrumbs with data (e.g., product name in breadcrumb)

---

### T-212: Global Search (⌘K)

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P2 |
| Effort | 3 SP (~15 hours) |
| Owner | Miłosz |
| Depends On | T-210 (new navigation structure for search targets) |
| Blocks | — |

**Requirement Source**: "Global Search ⌘K: High priority, Sprint 3" — [Source: UX_ARCHITECTURE §1.3]

**Scope**:
- Command palette (⌘K / Ctrl+K) with search across:
  - Pages (navigation items)
  - Products (SKU, ASIN, title search)
  - Orders (amazon_order_id search)
  - Actions (e.g., "refresh profit data", "run ads sync")
- Fuzzy matching via client-side scoring
- Keyboard navigation (↑↓ + Enter)
- API endpoint for product/order search: `GET /api/v1/search?q=...`

**Acceptance Criteria**:
- [ ] ⌘K opens command palette overlay
- [ ] Search finds pages, products, and orders
- [ ] Keyboard navigation works (arrows + enter)
- [ ] Response time < 300ms for product/order search

**Out of Scope**: Full-text search engine (Elasticsearch), AI-powered search

---

### T-213: Module Visibility Toggle (Settings Page)

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P2 |
| Effort | 2 SP (~8 hours) |
| Owner | Miłosz |
| Depends On | T-210 (sidebar groups defined) |
| Blocks | — |

**Requirement Source**: "Module Visibility Toggle: Medium, Sprint 3" — [Source: UX_ARCHITECTURE §1.3]

"FR-10: Module visibility management — 1-2d, ROI 130%" — [Source: FEEDBACK_SYNTHESIS §Feature Requests]

**Scope**:
- Settings page with toggle switches per navigation group and sub-page
- Store per-user visibility preferences in `acc_tenant_user.sidebar_config` (JSON)
- Sidebar reads config and hides disabled modules
- Default config: all Phase 1-hidden modules (Strategy, Seasonality, Tax, Content) remain hidden
- "Reset to defaults" button

**Acceptance Criteria**:
- [ ] Toggle switches show/hide sidebar groups
- [ ] Preferences persisted across sessions (stored in DB)
- [ ] Hidden modules remain accessible via direct URL
- [ ] Reset to defaults restores standard visibility

**Out of Scope**: Per-page visibility (only group-level), admin override for all users

---

### T-214: Email Delivery Integration (Resend/Postmark)

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P0 |
| Effort | 2 SP (~6 hours) |
| Owner | Miłosz |
| Depends On | — |
| Blocks | T-202 (verification email), T-206 (Morning Brief) |

**Requirement Source**: "BUY: email delivery (Resend or Postmark, $0-20/mo)" — [Source: TECH_STACK_ASSESSMENT §Build vs Buy]

**Scope**:
- Integrate Resend (or Postmark) for transactional email
- Create email service abstraction: `app/services/email.py` with `send_email(to, subject, html_body)`
- Email templates: verification, password reset, Morning Brief, billing receipt
- Domain verification (SPF, DKIM) for ascend-commerce.com
- Rate limiting: max 100 emails/hour overall

**Acceptance Criteria**:
- [ ] Verification email sends and delivers to Gmail/Outlook
- [ ] SPF + DKIM pass (check via mail-tester.com)
- [ ] Email service abstraction allows swapping providers

**Out of Scope**: Marketing emails, newsletters, email design builder

---

### T-215: Private Beta Recruitment and Launch

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P0 |
| Effort | 2 SP (~10 hours, mostly GTM) |
| Owner | Miłosz |
| Depends On | T-202, T-203 (onboarding), T-220 (landing page) |
| Blocks | T-217 (NPS survey needs users) |

**Requirement Source**: "Private beta launch (20-50 users): Invite-only signups" — [Source: STRATEGIC_PORTFOLIO_PLAN §Phase 2 Jun Milestone]

"200+ private beta signups by Jun 2026" — [Source: STRATEGIC_PORTFOLIO_PLAN §Objective 1 KR1]

**Scope**:
- Invite code system: generate unique codes, track usage
- Post in Polish Amazon seller communities (see acquisition strategy in Strategic Plan)
- Personal outreach to 20 known PL Amazon sellers
- Collect feedback via in-app form + email
- Target: 20-50 users in first 2 weeks

**Acceptance Criteria**:
- [ ] Invite code system functional
- [ ] ≥ 20 beta signups within 2 weeks of launch
- [ ] Feedback collection mechanism active
- [ ] No P0 bugs during beta onboarding

**Out of Scope**: Paid acquisition, PR, paid influencer marketing

---

### T-216: Product Analytics Integration (PostHog)

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P2 |
| Effort | 2 SP (~6 hours) |
| Owner | Miłosz |
| Depends On | T-203 (onboarding — track funnel) |
| Blocks | T-309 (funnel optimization needs data) |

**Requirement Source**: "Simple analytics: PostHog (free/OSS) or Plausible" — [Source: STRATEGIC_PORTFOLIO_PLAN §Phase 2 BUY Decisions]

**Scope**:
- Integrate PostHog JS SDK in React app
- Track key events: page_view, ppt_load, onboarding_step, search, export, alert_click
- Feature flags: PostHog feature flag for Beta features
- User identification: link PostHog user to tenant_id
- Track onboarding funnel: signup → verify → connect → first_data → aha_moment

**Acceptance Criteria**:
- [ ] PostHog receives events from frontend
- [ ] Onboarding funnel visible in PostHog dashboard
- [ ] Feature flags toggleable from PostHog UI
- [ ] No PII sent to PostHog (anonymized IDs only)

**Out of Scope**: Session recordings, heatmaps, A/B testing

---

### T-217: NPS Micro-Survey Component (In-App)

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P2 |
| Effort | 1 SP (~4 hours) |
| Owner | Miłosz |
| Depends On | T-215 (needs beta users to survey) |
| Blocks | — |

**Requirement Source**: "NPS ≥ 30 (beta cohort survey)" — [Source: STRATEGIC_PORTFOLIO_PLAN §B-5]

**Scope**:
- React component: single-question NPS (0-10 scale) + optional text feedback
- Trigger: after user's 10th session in 30 days
- Store responses in `acc_nps_response` table
- Show at most once per quarter per user
- Aggregate NPS score displayed in admin dashboard

**Acceptance Criteria**:
- [ ] NPS survey appears after 10th session
- [ ] Score + feedback stored in database
- [ ] Survey does not appear more than once per 90 days
- [ ] Admin can view aggregate NPS score

**Out of Scope**: CSAT, CES, multi-question surveys

---

### T-218: Connection Pooling via SQLAlchemy QueuePool

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P1 |
| Effort | 3 SP (~12 hours) |
| Owner | Miłosz |
| Depends On | T-117 (Phase 1 stable before refactoring connections) |
| Blocks | T-310 (Celery needs proper connection management) |

**Requirement Source**: "ADR-005: Sync connection pooling via QueuePool" — [Source: SYSTEM_ARCHITECTURE_SPEC §2.6]

"connect_acc() creates a new pymssql connection per call — No connection pooling" — [Source: SYSTEM_ARCHITECTURE_SPEC §2.6]

**Scope**:
- Replace per-request connection creation in `connect_acc()` with SQLAlchemy `QueuePool`
- Configuration: `pool_size=10`, `max_overflow=20`, `pool_recycle=3600`, `pool_timeout=30`
- Keep `connect_netfox()` as-is (ERP connections are infrequent)
- Add connection pool health metrics to observability endpoint
- Ensure proper connection return on error (context manager)

**Acceptance Criteria**:
- [ ] Connection pool active with 10 base + 20 overflow connections
- [ ] No connection leaks under load (pool_size stable after 100 requests)
- [ ] Pool metrics visible in observability endpoint
- [ ] All existing tests pass with pooled connections

**Out of Scope**: Async connection pool (requires aioodbc migration), read replica routing

---

### T-219: Introduce Alembic for Schema Migrations

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P1 |
| Effort | 3 SP (~12 hours) |
| Owner | Miłosz |
| Depends On | T-218 (SQLAlchemy engine needed for Alembic) |
| Blocks | — |

**Requirement Source**: "ADR-004: Introduce Alembic incrementally" — [Source: SYSTEM_ARCHITECTURE_SPEC §2.5]

**Scope**:
- Initialize Alembic in `apps/api/alembic/`
- Create `001_baseline.py` migration from current schema snapshot
- Convert first 3 `ensure_*_schema()` functions to Alembic migrations
- Add `alembic upgrade head` to Docker entrypoint
- Document migration workflow in `docs/runbooks/schema_migration.md`
- Keep existing `ensure_*` functions as fallbacks during transition

**Acceptance Criteria**:
- [ ] `alembic upgrade head` runs without errors on production DB
- [ ] Baseline migration matches current schema
- [ ] New table creation uses Alembic migration (not `ensure_*`)
- [ ] Rollback tested: `alembic downgrade` works for new migrations

**Out of Scope**: Full conversion of all 13 ensure functions (incremental per ADR-004)

---

### T-220: Marketing Landing Page (Static)

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P1 |
| Effort | 2 SP (~10 hours) |
| Owner | Miłosz |
| Depends On | — |
| Blocks | T-215 (beta needs landing page for signup link) |

**Requirement Source**: "Landing page (Vercel free): Static marketing site" — [Source: STRATEGIC_PORTFOLIO_PLAN §Phase 2 BUY Decisions]

"Brand: Ascend Commerce Cloud, tagline: 'Profit Truth, Not Guesswork'" — [Source: BRAND_IDENTITY_SYSTEM]

**Scope**:
- Single-page marketing site on ascend-commerce.com
- Sections: Hero (value prop), Features (CM1/CM2/NP, Ads, Logistics), Pricing, Beta signup CTA
- Deploy on Vercel (free tier)
- SEO basics: meta tags, OG images, sitemap
- Polish as primary language, English version secondary

**Acceptance Criteria**:
- [ ] Landing page live on ascend-commerce.com
- [ ] Beta signup CTA collects email
- [ ] Page loads in < 2s (Lighthouse performance ≥ 90)
- [ ] Polish and English versions available

**Out of Scope**: Blog, documentation, video demos, interactive demos

---

### T-221: Error Response Standardization (RFC 7807)

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P2 |
| Effort | 2 SP (~6 hours) |
| Owner | Miłosz |
| Depends On | — |
| Blocks | — |

**Requirement Source**: "§3.6: Error Response Standardization" — [Source: SYSTEM_ARCHITECTURE_SPEC §3.6]

**Scope**:
- Create standard error response format (RFC 7807 Problem Details):
  ```json
  { "type": "errors/validation", "title": "Validation Error", "status": 422, "detail": "...", "instance": "/api/v1/..." }
  ```
- Global exception handler in FastAPI
- Map all existing error codes to RFC 7807 format
- Update frontend API client (`api.ts`) to parse standardized errors

**Acceptance Criteria**:
- [ ] All API errors return RFC 7807 format
- [ ] Frontend displays meaningful error messages
- [ ] Swagger docs show error response schemas

**Out of Scope**: Error code registry, error translation (i18n)

---

### T-222: Unified Alert Triage View

| Field | Value |
|-------|-------|
| Phase | 2 |
| Priority | P2 |
| Effort | 5 SP (~25 hours) |
| Owner | Miłosz |
| Depends On | T-110 (observability), T-210 (sidebar group) |
| Blocks | — |

**Requirement Source**: "Unified alert triage view (5-8d, 65% ROI)" — [Source: STRATEGIC_PORTFOLIO_PLAN §Feature Priority Queue #5]

"Alert fatigue risk — multiple alert sources with no unified priority/triage" — [Source: FEEDBACK_SYNTHESIS §PP-08]

**Scope**:
- Single page combining: guardrail alerts, FBA alerts, courier alerts, data quality alerts, system alerts
- Sortable/filterable by: severity, source, timestamp, status (open/acknowledged/resolved)
- One-click acknowledge and resolve actions
- Alert dedup: same alert within 1h window = single entry with count
- Severity-based badge in TopBar (red for P0, yellow for P1)

**Acceptance Criteria**:
- [ ] All alert sources visible in single triage view
- [ ] Filter by severity, source, status works
- [ ] Acknowledge/resolve actions update alert status
- [ ] Alert dedup prevents duplicate entries within 1h
- [ ] TopBar badge shows unresolved alert count with severity color

**Out of Scope**: Alert escalation policies, PagerDuty integration, alert routing rules

---

## Phase 3 Tasks (T-3xx) — LAUNCH

---

### T-301: Weekly P&L PDF Report (WeasyPrint)

| Field | Value |
|-------|-------|
| Phase | 3 |
| Priority | P1 |
| Effort | 5 SP (~25 hours) |
| Owner | Miłosz |
| Depends On | T-113 (snapshot data), T-214 (email delivery) |
| Blocks | — |

**Requirement Source**: "FR-09: Weekly P&L PDF report — 5-8d, ROI 90%" — [Source: FEEDBACK_SYNTHESIS §Feature Requests]

"BUY: PDF report generation (WeasyPrint OSS lib)" — [Source: TECH_STACK_ASSESSMENT §Build vs Buy]

**Scope**:
- Weekly scheduler job (Monday 08:00 CET): generate PDF P&L for prior week
- Content: revenue, COGS, CM1, CM2, NP by marketplace; top/bottom 10 products; trend vs. prior week
- Use WeasyPrint to render HTML→PDF (no headless browser needed)
- Email PDF to tenant admin
- Stored in Azure Blob for download via `/api/v1/reports/weekly/{date}`

**Acceptance Criteria**:
- [ ] PDF generated weekly with correct financial data
- [ ] PDF renders properly (tables, charts, branding)
- [ ] Email delivery with PDF attachment
- [ ] Historical PDFs downloadable via API

**Out of Scope**: Monthly/annual reports, custom date ranges, Excel export

---

### T-302: Profit→Refund Drill Path

| Field | Value |
|-------|-------|
| Phase | 3 |
| Priority | P2 |
| Effort | 5 SP (~25 hours) |
| Owner | Miłosz |
| Depends On | T-113 (snapshot), T-106 (PPT pagination) |
| Blocks | — |

**Requirement Source**: "Profit→refund drill path (5-8d, 50% ROI)" — [Source: STRATEGIC_PORTFOLIO_PLAN §Feature Priority Queue #8]

"Refund/return impact not visible in CM1" — [Source: FEEDBACK_SYNTHESIS §PP-12]

**Scope**:
- From PPT product row: click to see refund/return history
- Show: refund amount, refund reason, return quantity, impact on CM1
- Return rate per SKU/product with trend chart
- Link from refund anomaly alerts to profit impact view

**Acceptance Criteria**:
- [ ] PPT row click opens drill-down with refund data
- [ ] Return rate and refund amount per SKU visible
- [ ] CM1 impact from refunds calculated and displayed
- [ ] Refund anomaly alert links to relevant product

**Out of Scope**: Refund prevention recommendations, Amazon case filing automation

---

### T-303: Bank Feed Automation (Basic)

| Field | Value |
|-------|-------|
| Phase | 3 |
| Priority | P2 |
| Effort | 8 SP (~40 hours) |
| Owner | Miłosz |
| Depends On | T-204 (billing — reconciliation context) |
| Blocks | — |

**Requirement Source**: "Bank feed automation (13-21d, 60% ROI)" — [Source: STRATEGIC_PORTFOLIO_PLAN §Feature Priority Queue #7]

"Finance reconciliation blocked by missing bank import" — [Source: FEEDBACK_SYNTHESIS §PP-04]

**Scope**:
- CSV/MT940 bank statement import (manual upload initially)
- Auto-categorization: match Amazon payout amounts to finance transactions
- Reconciliation: mark matched transactions as reconciled
- Dashboard showing: unmatched bank entries, unmatched Amazon payouts
- Future hook: Open Banking API (PSD2) for automated feed (Phase 4)

**Acceptance Criteria**:
- [ ] CSV bank statement upload parses correctly
- [ ] Auto-matching correctly links ≥ 80% of Amazon payouts
- [ ] Unmatched entries visible in reconciliation UI
- [ ] Finance dashboard removes `blocked_by_missing_bank_import` status after import

**Out of Scope**: Real-time bank feed (PSD2), multi-currency bank reconciliation, accounting software export

---

### T-304: DACH Marketplace Deep Testing

| Field | Value |
|-------|-------|
| Phase | 3 |
| Priority | P1 |
| Effort | 3 SP (~15 hours) |
| Owner | Miłosz |
| Depends On | T-201 (multi-tenant for DE seller testing) |
| Blocks | T-305 (localization), T-403 (DACH launch) |

**Requirement Source**: "DACH preparation: Amazon.de marketplace fully tested with 5+ DE users" — [Source: STRATEGIC_PORTFOLIO_PLAN §L-9]

**Scope**:
- Create test accounts for Amazon.de marketplace
- Verify end-to-end: order sync, profit calculation, FX conversion (EUR), fee breakdown
- Test with 5+ real DE sellers from beta program
- Document DE-specific issues (VAT rates, fulfillment patterns, FBA fee structure)
- Fix any DE-specific bugs found

**Acceptance Criteria**:
- [ ] Amazon.de orders sync correctly with EUR currency
- [ ] CM1/CM2/NP calculation correct for DE marketplace (verified against Seller Central)
- [ ] 5+ DE beta users onboarded and providing feedback
- [ ] DE-specific bug list triaged (P0 fixed, P1 tracked)

**Out of Scope**: French, Italian, Spanish marketplace testing

---

### T-305: German UI String Localization (Critical Paths)

| Field | Value |
|-------|-------|
| Phase | 3 |
| Priority | P2 |
| Effort | 3 SP (~15 hours) |
| Owner | Miłosz |
| Depends On | T-304 (DACH testing identifies what to translate) |
| Blocks | T-403 (DACH launch needs German UI) |

**Requirement Source**: "German UI strings (critical paths)" — [Source: STRATEGIC_PORTFOLIO_PLAN §Phase 3 Dec Milestone]

**Scope**:
- Implement i18n framework (react-i18next)
- Translate critical user paths to German: onboarding, Dashboard, PPT, pricing page
- 3 locales: Polish (default), English, German
- Locale selection in user settings
- All remaining pages fall back to English

**Acceptance Criteria**:
- [ ] i18n framework installed and configured
- [ ] Dashboard, PPT, onboarding available in DE
- [ ] Language selector in settings works
- [ ] Fallback to English for untranslated strings

**Out of Scope**: Full platform translation, RTL support, machine translation for all strings

---

### T-306: Public Marketing Site + Content

| Field | Value |
|-------|-------|
| Phase | 3 |
| Priority | P1 |
| Effort | 3 SP (~15 hours) |
| Owner | Miłosz |
| Depends On | T-220 (landing page base) |
| Blocks | — |

**Requirement Source**: "Public launch (PL market): Marketing site; content marketing (3 blog posts)" — [Source: STRATEGIC_PORTFOLIO_PLAN §Phase 3 Oct Milestone]

**Scope**:
- Expand landing page to full marketing site: features, pricing, FAQ, blog, about
- Write 3 launch blog posts (PL): "Why your Amazon margins are wrong", "CM1 vs. CM2 explained", "ACC profit analytics vs. spreadsheets"
- SEO optimization for Polish Amazon seller keywords
- Add social proof section (beta user quotes, data quality score)
- Pricing page with tier comparison

**Acceptance Criteria**:
- [ ] Full marketing site live with ≥ 5 pages
- [ ] 3 blog posts published
- [ ] Pricing page shows tier comparison
- [ ] Basic SEO: meta descriptions, structured data, sitemap

**Out of Scope**: Content marketing calendar, video content, case studies

---

### T-307: Referral Program Implementation

| Field | Value |
|-------|-------|
| Phase | 3 |
| Priority | P2 |
| Effort | 2 SP (~10 hours) |
| Owner | Miłosz |
| Depends On | T-204 (Stripe billing), T-202 (user accounts) |
| Blocks | — |

**Requirement Source**: "Referral program" — [Source: STRATEGIC_PORTFOLIO_PLAN §Phase 3 Nov Milestone]

**Scope**:
- Unique referral code per user (stored in `acc_tenant_user.referral_code`)
- Referee gets 1 month free on Pro tier
- Referrer gets 1 month free (applied as Stripe credit)
- Track referrals in `acc_referral` table (referrer_id, referee_id, status, rewarded_at)
- In-app "Invite Friends" section with shareable link

**Acceptance Criteria**:
- [ ] Referral codes generated for all users
- [ ] Referee gets 1 month free on signup with code
- [ ] Referrer gets Stripe credit on referee's first payment
- [ ] Referral tracking visible in admin dashboard

**Out of Scope**: Multi-level referrals, cash payouts, affiliate program

---

### T-308: Help Center / Documentation Site (20+ Articles)

| Field | Value |
|-------|-------|
| Phase | 3 |
| Priority | P1 |
| Effort | 5 SP (~30 hours) |
| Owner | Miłosz |
| Depends On | — |
| Blocks | T-314 (L-8 gate: docs site live) |

**Requirement Source**: "Public docs site / help center: Live with ≥ 20 articles" — [Source: STRATEGIC_PORTFOLIO_PLAN §L-8]

**Scope**:
- Deploy documentation site (e.g., Mintlify, Docusaurus, or custom Next.js)
- Write ≥ 20 help articles covering:
  - Getting Started (5): signup, SP-API connection, first profit view, understanding CM1, first morning routine
  - Features (8): PPT usage, ads attribution, logistics costs, data quality, alerts, exports, finance center, Morning Brief
  - FAQ (4): pricing, data security, supported marketplaces, data freshness
  - Troubleshooting (3): common errors, data gaps, sync issues
- Searchable, categorized, with screenshots

**Acceptance Criteria**:
- [ ] Documentation site live at docs.ascend-commerce.com (or /docs path)
- [ ] ≥ 20 articles published
- [ ] Search functionality works
- [ ] Articles include screenshots and step-by-step instructions

**Out of Scope**: Video tutorials, API reference docs (auto-generated from OpenAPI), community forum

---

### T-309: Onboarding Funnel Optimization (from Analytics)

| Field | Value |
|-------|-------|
| Phase | 3 |
| Priority | P1 |
| Effort | 3 SP (~15 hours) |
| Owner | Miłosz |
| Depends On | T-216 (analytics data from PostHog) |
| Blocks | — |

**Requirement Source**: "Onboarding optimization from funnel data" — [Source: STRATEGIC_PORTFOLIO_PLAN §Phase 3 Nov Milestone]

**Scope**:
- Analyze PostHog onboarding funnel: identify drop-off points
- Implement top 3 optimizations based on data (e.g., simplify SP-API step, add progress indicators, reduce form fields)
- A/B test key changes via PostHog feature flags
- Target: time-to-value < 10 minutes, funnel completion > 60%

**Acceptance Criteria**:
- [ ] Funnel analysis report written
- [ ] ≥ 3 optimizations implemented
- [ ] Time-to-value improved (measured pre/post)
- [ ] Funnel completion rate improved (measured pre/post)

**Out of Scope**: Full UX redesign, hiring UX designer, user research interviews

---

### T-310: Activate Celery Workers for Heavy Sync Jobs

| Field | Value |
|-------|-------|
| Phase | 3 |
| Priority | P1 |
| Effort | 5 SP (~25 hours) |
| Owner | Miłosz |
| Depends On | T-218 (connection pooling), T-117 (stable base) |
| Blocks | T-402 (horizontal scaling) |

**Requirement Source**: "ADR-008: Activate Celery as first scaling step" — [Source: SYSTEM_ARCHITECTURE_SPEC §6.1]

"TD-02: KEEP APScheduler + activate Celery (WORKER_EXECUTION_ENABLED=True)" — [Source: TECH_STACK_ASSESSMENT §Layer 4]

**Scope**:
- Set `WORKER_EXECUTION_ENABLED=True` in configuration
- Move heavy sync jobs to Celery tasks: `sync_orders`, `sync_finances`, `sync_ads`, `calc_profit`
- Keep APScheduler for scheduling triggers (cron) → dispatch to Celery
- Configure 3 queues: `default`, `sync`, `compute`
- Celery worker runs as separate Docker container
- Redis as broker (already configured)
- Flower dashboard for monitoring (optional)

**Acceptance Criteria**:
- [ ] Celery worker active and processing tasks
- [ ] Order sync runs via Celery (verified in job logs)
- [ ] Ads sync runs via Celery
- [ ] No duplicate execution (APScheduler + Celery coordination)
- [ ] Job failures visible in Sentry + `acc_al_jobs`

**Out of Scope**: Multi-worker pool (single worker initially), auto-scaling, Kubernetes

---

### T-311: Security Hardening (CORS, Headers, WAF Eval)

| Field | Value |
|-------|-------|
| Phase | 3 |
| Priority | P0 |
| Effort | 3 SP (~12 hours) |
| Owner | Miłosz |
| Depends On | T-201 (multi-tenant makes security critical) |
| Blocks | — |

**Requirement Source**: "§5: Security Architecture — 7-layer defense" — [Source: SYSTEM_ARCHITECTURE_SPEC §5]

**Scope**:
- CORS: restrict origins to production domain only (remove wildcard if present)
- Security headers middleware: `X-Content-Type-Options`, `X-Frame-Options`, `X-XSS-Protection`, `Strict-Transport-Security`, `Content-Security-Policy`
- Evaluate WAF options (Azure Front Door WAF, Cloudflare) — document decision
- Secrets rotation: rotate JWT secret, DB passwords
- pip-audit: scan dependencies for known vulnerabilities, update as needed

**Acceptance Criteria**:
- [ ] CORS allows only production domain(s)
- [ ] All security headers present (verified by securityheaders.com)
- [ ] WAF evaluation documented with recommendation
- [ ] Secrets rotated and documented
- [ ] pip-audit reports zero critical vulnerabilities

**Out of Scope**: Penetration testing, SOC 2 preparation, bug bounty program

---

### T-312: PII/GDPR Compliance Audit

| Field | Value |
|-------|-------|
| Phase | 3 |
| Priority | P1 |
| Effort | 2 SP (~10 hours) |
| Owner | Miłosz |
| Depends On | T-201 (multi-tenant with user PII) |
| Blocks | — |

**Requirement Source**: "§5.9: PII & GDPR" — [Source: SYSTEM_ARCHITECTURE_SPEC §5.9]

**Scope**:
- Audit all tables for PII columns (email, name, address, phone)
- Document data processing register (GDPR Article 30)
- Implement user data export endpoint (`GET /api/v1/me/data-export`)
- Implement user data deletion endpoint (`DELETE /api/v1/me/account`)
- Privacy policy: create (or update) for public launch
- Verify Sentry PII settings (`send_default_pii=False`)

**Acceptance Criteria**:
- [ ] PII inventory document created
- [ ] Data export endpoint returns all user data as JSON
- [ ] Account deletion removes/anonymizes all PII
- [ ] Privacy policy published on marketing site
- [ ] Sentry confirmed PII-safe

**Out of Scope**: DPO appointment, GDPR certification, legal review by attorney

---

### T-313: E2E Testing with Playwright (Critical Flows)

| Field | Value |
|-------|-------|
| Phase | 3 |
| Priority | P1 |
| Effort | 5 SP (~25 hours) |
| Owner | Miłosz |
| Depends On | T-107 (PPT frontend), T-203 (onboarding) |
| Blocks | — |

**Requirement Source**: "No Playwright/Cypress for critical user flows — an E2E test would have caught [14.5s PPT regression] early" — [Source: TECH_STACK_ASSESSMENT §Layer 8]

**Scope**:
- Install Playwright and configure for ACC frontend
- Write E2E tests for 5 critical flows:
  1. Login → Dashboard loads in < 5s
  2. Dashboard → PPT → first page loads in < 2s
  3. PPT → sort by CM1 → results update
  4. PPT → search product → results filter
  5. Onboarding wizard → complete all steps
- Run in CI (headless Chromium)
- Screenshot comparison for regression detection

**Acceptance Criteria**:
- [ ] 5 E2E tests pass in CI
- [ ] PPT load time regression detected by test (fails if > 3s)
- [ ] Tests run in < 3 minutes total
- [ ] Screenshot baselines established

**Out of Scope**: Visual regression testing (beyond screenshots), cross-browser testing, mobile testing

---

### T-314: Phase 3 Gate Review

| Field | Value |
|-------|-------|
| Phase | 3 |
| Priority | P0 |
| Effort | 1 SP (~4 hours) |
| Owner | Miłosz |
| Depends On | T-301 through T-313 |
| Blocks | Phase 4 entry |

**Requirement Source**: "Phase 3 gate criteria: L-1 through L-10" — [Source: STRATEGIC_PORTFOLIO_PLAN §5.3]

**Scope**:
- Self-assessment against all 10 Phase 3 exit criteria:
  - L-1: MRR ≥ $5,000
  - L-2: ≥ 100 paying users
  - L-3: Monthly churn < 5%
  - L-4: NPS ≥ 40
  - L-5: DQ Score ≥ 90/100
  - L-6: PPT p95 < 1.5s
  - L-7: Support SLA < 2h
  - L-8: Docs site with ≥ 20 articles
  - L-9: DACH tested with 5+ DE users
  - L-10: LTV:CAC > 4:1
- Document: `docs/PHASE_3_GATE_REVIEW.md`

**Acceptance Criteria**:
- [ ] All 10 criteria assessed with evidence
- [ ] GO/HOLD/PIVOT decision documented
- [ ] Phase 4 entry trigger conditions confirmed or deferred

**Out of Scope**: Board review (solo founder)

---

## Phase 4 Tasks (T-4xx) — SCALE

---

### T-401: Azure SQL Tier Upgrade Evaluation

| Field | Value |
|-------|-------|
| Phase | 4 |
| Priority | P1 |
| Effort | 2 SP (~8 hours) |
| Owner | Miłosz |
| Depends On | T-314 (Phase 3 passed), user count > 200 |
| Blocks | — |

**Requirement Source**: "500+ paying users: Azure SQL tier upgrade (S4/P1)" — [Source: STRATEGIC_PORTFOLIO_PLAN §Phase 4 Triggers]

"Azure SQL S3 supports up to 250 GB" — [Source: SYSTEM_ARCHITECTURE_SPEC §2.4]

**Scope**:
- Monitor DTU usage, I/O, and connection count on current S3 tier
- Create decision matrix: S4 vs P1 vs P2 based on workload profile
- Implement upgrade if 500+ user trigger met
- Test connection pool behavior during tier change

**Acceptance Criteria**:
- [ ] DTU analysis document with recommendation
- [ ] Upgrade path tested in staging
- [ ] Zero downtime during tier switch

**Out of Scope**: Azure SQL Managed Instance migration, multi-region replication

---

### T-402: Horizontal API Scaling (2-4 Replicas)

| Field | Value |
|-------|-------|
| Phase | 4 |
| Priority | P1 |
| Effort | 5 SP (~25 hours) |
| Owner | Miłosz |
| Depends On | T-310 (Celery active), T-218 (connection pool) |
| Blocks | — |

**Requirement Source**: "API horizontally scaled (2–4 replicas behind nginx)" — [Source: SYSTEM_ARCHITECTURE_SPEC §1.1 Phase 2 Evolution]

**Scope**:
- Configure nginx for round-robin load balancing across 2-4 API replicas
- Ensure API is stateless: all state in Redis or SQL (no in-process state)
- Remove APScheduler from API instances (scheduler runs only in Celery beat)
- Redis session store for any session-like data
- Health check endpoint for nginx upstream health

**Acceptance Criteria**:
- [ ] 2 API replicas running behind nginx
- [ ] Requests load-balanced across replicas
- [ ] No scheduler duplication across replicas
- [ ] Failover: one replica down, traffic continues

**Out of Scope**: Kubernetes, auto-scaling, blue-green deployment

---

### T-403: DACH Market Soft Launch (DE Beta)

| Field | Value |
|-------|-------|
| Phase | 4 |
| Priority | P1 |
| Effort | 3 SP (~15 hours, mostly GTM) |
| Owner | Miłosz |
| Depends On | T-304, T-305 (DACH testing + German UI) |
| Blocks | — |

**Requirement Source**: "DACH > 50 paying users → Dedicated DE content + support" — [Source: STRATEGIC_PORTFOLIO_PLAN §Phase 4 Triggers]

**Scope**:
- German marketing content: 2 blog posts, pricing page in DE
- Outreach to DE Amazon seller communities (AMZ Seller Forum DE, AMZ FBA Groups)
- Target: 5-10 DE beta users, then scale to 50
- Track DE-specific metrics separately

**Acceptance Criteria**:
- [ ] German marketing content live
- [ ] 5+ DE users onboarded
- [ ] DE-specific revenue tracked in analytics

**Out of Scope**: AT/CH specific content, German legal entity, German customer support

---

### T-404: Contractor Onboarding (First Frontend Hire)

| Field | Value |
|-------|-------|
| Phase | 4 |
| Priority | P1 |
| Effort | 3 SP (~15 hours) |
| Owner | Miłosz |
| Depends On | $5K MRR sustained 2 months (trigger) |
| Blocks | — |

**Requirement Source**: "MRR > $5,000 sustained 2 months → Part-time contractor (frontend)" — [Source: STRATEGIC_PORTFOLIO_PLAN §3.4]

"Senior Frontend Developer (React/TypeScript) — Part-time contractor (20h/week)" — [Source: STRATEGIC_PORTFOLIO_PLAN §3.4]

**Scope**:
- Write job description (React/TypeScript, shadcn/ui, Tailwind)
- Post on JustJoinIT, useme.com, LinkedIn Poland
- Screen candidates (portfolio review, code challenge)
- Onboard: codebase walkthrough, architecture review, first task assignment
- Document onboarding process for future hires

**Acceptance Criteria**:
- [ ] Job posted on ≥ 2 platforms
- [ ] Contractor hired and producing code within 2 weeks of start
- [ ] First PR merged and deployed
- [ ] Onboarding documentation created

**Out of Scope**: Full-time employment, benefits, DevOps hire (second hire at $10K MRR)

---

### T-405: JWT RS256 Migration (from HS256)

| Field | Value |
|-------|-------|
| Phase | 4 |
| Priority | P2 |
| Effort | 3 SP (~12 hours) |
| Owner | Miłosz |
| Depends On | T-109 (pyjwt migration), multi-service potential |
| Blocks | — |

**Requirement Source**: "ADR-007: Defer RS256 migration to Phase 2" — [Source: SYSTEM_ARCHITECTURE_SPEC §4.2]

"HS256 is symmetric — RS256 preferred for multi-service" — [Source: TECH_STACK_ASSESSMENT §Layer 7]

**Scope**:
- Generate RSA key pair, store private key securely
- Update `create_access_token()` to use RS256
- Update `verify_token()` to use public key only
- Transition period: accept both HS256 and RS256 tokens for 7 days
- Update all token consumers

**Acceptance Criteria**:
- [ ] New tokens issued with RS256
- [ ] Old HS256 tokens accepted during 7-day transition
- [ ] Public key available for external verification
- [ ] All auth tests pass

**Out of Scope**: JWKS endpoint, external IdP integration, OIDC

---

### T-406: Mobile Responsive Design Improvements

| Field | Value |
|-------|-------|
| Phase | 4 |
| Priority | P3 |
| Effort | 5 SP (~25 hours) |
| Owner | Miłosz (or contractor) |
| Depends On | T-404 (frontend contractor may lead this) |
| Blocks | — |

**Requirement Source**: "No mobile/responsive optimization — field operator usage blocked" — [Source: FEEDBACK_SYNTHESIS §PP-09]

"Critical: PPT table on mobile degrades to card-list view" — [Source: UX_ARCHITECTURE §2.4]

**Scope**:
- Implement responsive breakpoints per UX Architecture §2.4
- Sidebar: hidden + hamburger on <768px
- Dashboard grid: single column on mobile
- PPT: card-list view instead of table on mobile
- Test on iOS Safari and Android Chrome

**Acceptance Criteria**:
- [ ] All critical pages usable on 375px width
- [ ] Sidebar overlay works on mobile
- [ ] PPT card view readable on phone
- [ ] No horizontal scroll on any page at <768px

**Out of Scope**: Native mobile app, offline support, push notifications

---

### T-407: Time-Series Analytics Layer

| Field | Value |
|-------|-------|
| Phase | 4 |
| Priority | P3 |
| Effort | 5 SP (~25 hours) |
| Owner | Miłosz |
| Depends On | T-113 (snapshot table as data source) |
| Blocks | — |

**Requirement Source**: "R-08: Add Time-Series Analytics Layer (🟢 Future) — 1 week" — [Source: DATA_AUDIT_REPORT §Recommendations]

**Scope**:
- Build time-series API: `/api/v1/analytics/time-series?metric=cm1&granularity=daily&from=...&to=...`
- Metrics: revenue, CM1, CM2, units, ACoS, return rate, logistics cost
- Granularity: daily, weekly, monthly
- Data source: `acc_profit_daily_snapshot` + `acc_ads_campaign_day`
- Frontend: trend charts with comparison periods

**Acceptance Criteria**:
- [ ] API returns time-series data for ≥ 7 metrics
- [ ] Daily, weekly, monthly granularity works
- [ ] Frontend chart renders with comparison overlay
- [ ] Response time < 1s for 90-day window

**Out of Scope**: Forecasting, anomaly detection, custom metric definitions

---

### T-408: AI-Powered Margin Alerts

| Field | Value |
|-------|-------|
| Phase | 4 |
| Priority | P3 |
| Effort | 5 SP (~25 hours) |
| Owner | Miłosz |
| Depends On | T-407 (time-series data), T-222 (alert triage) |
| Blocks | — |

**Requirement Source**: "AI-powered margin alerts — Q3 2027 (competitive moat deepening)" — [Source: STRATEGIC_PORTFOLIO_PLAN §Phase 4 Targets]

**Scope**:
- Simple anomaly detection: z-score on daily CM1% per ASIN (if today's margin is >2σ from 30-day mean, alert)
- Alert types: sudden margin drop, COGS increase, fee spike, ads spend anomaly
- LLM-generated alert summaries (OpenAI GPT): "Product X CM1 dropped 15% yesterday due to FBA fee increase"
- User configurable thresholds (default: 2σ)

**Acceptance Criteria**:
- [ ] Anomaly detection runs daily on CM1 per ASIN
- [ ] Alert generated when margin deviates > 2σ
- [ ] LLM summary attached to alert (human-readable)
- [ ] User can adjust sensitivity thresholds

**Out of Scope**: Predictive alerts, custom ML model training, real-time alerts

---

### T-409: Export & Reporting Infrastructure

| Field | Value |
|-------|-------|
| Phase | 4 |
| Priority | P2 |
| Effort | 5 SP (~25 hours) |
| Owner | Miłosz |
| Depends On | T-301 (PDF reports as first export type) |
| Blocks | — |

**Requirement Source**: "R-09: Export & Reporting Infrastructure (🟢 Future) — 1 week" — [Source: DATA_AUDIT_REPORT §Recommendations]

**Scope**:
- Unified export service: CSV, Excel (openpyxl), PDF (WeasyPrint)
- Export from any data table: PPT, orders, campaigns, finance
- Scheduled exports: weekly/monthly auto-generate and email
- API: `POST /api/v1/exports` with format, filters, destination
- Export history: list past exports, re-download

**Acceptance Criteria**:
- [ ] CSV export works for PPT, orders, campaigns
- [ ] Excel export includes formatting and headers
- [ ] Scheduled export sends email with attachment
- [ ] Export API documented in Swagger

**Out of Scope**: Custom report builder, drag-and-drop report designer, BI tool integration

---

# 3. Dependency Map

## 3.1 Phase 1 Dependencies

```
T-101 (UptimeRobot) ────────────────────────────────────┐
T-102 (FX Alert) ───────────────────┐                   │
T-103 (Ads Heartbeat) ──► T-104 (Single-Flight Guard)   │
T-105 (Indexes) ──► T-113 (Snapshot) ──► T-106 (PPT SQL)│
T-106 (PPT Backend) ──► T-107 (PPT Frontend)            │
T-108 (Hide Pages) ─────────────────────────────────────│
T-109 (pyjwt) ─────────────────────────────────────────│
T-110 (Observability) ──► T-114 (Freshness API)         │
T-111 (FBA Bridge) ─────────────────────────────────────│
T-112 (Archive Tables) ────────────────────────────────│
T-115 (Tests) ──────────────────────────────────────────│
T-116 (Runbooks) ──────────────────────────────────────│
                                                        │
ALL ──────────────────────────────────► T-117 (Gate) ───┘
```

| Task | Depends On | Blocks |
|------|-----------|--------|
| T-101 | — | T-110, T-117 |
| T-102 | — | T-110, T-115, T-117 |
| T-103 | — | T-104, T-117 |
| T-104 | T-103 | T-117 |
| T-105 | — | T-106, T-113 |
| T-106 | T-105, T-113 | T-107, T-117 |
| T-107 | T-106 | T-117 |
| T-108 | — | T-117 |
| T-109 | — | T-115 |
| T-110 | T-101, T-102 | T-114, T-117 |
| T-111 | — | T-117 |
| T-112 | — | — |
| T-113 | T-105 | T-106 |
| T-114 | T-110 | — |
| T-115 | T-102, T-103, T-109 | T-117 |
| T-116 | T-110 | — |
| T-117 | ALL Phase 1 | Phase 2 |

## 3.2 Cross-Phase Dependencies

| Phase 2 Task | Depends On (Phase 1) | Blocks (Phase 3+) |
|-------------|---------------------|-------------------|
| T-201 (Multi-tenant) | T-117 | T-202, T-204, T-208, T-304 |
| T-202 (Registration) | T-201, T-214 | T-203, T-204, T-215 |
| T-204 (Stripe) | T-201, T-202 | T-307, T-303 |
| T-205 (Logistics v3) | T-117 | — |
| T-206 (Morning Brief) | T-214, T-113 | — |
| T-210 (Sidebar 7 groups) | T-108 | T-211, T-212, T-213 |
| T-218 (Conn Pool) | T-117 | T-219, T-310 |

| Phase 3 Task | Depends On (Phase 2) | Blocks (Phase 4+) |
|-------------|---------------------|-------------------|
| T-301 (PDF Report) | T-113, T-214 | T-409 |
| T-304 (DACH Test) | T-201 | T-305, T-403 |
| T-310 (Celery) | T-218 | T-402 |
| T-313 (E2E Tests) | T-107, T-203 | — |

---

# 4. Critical Path Analysis

## 4.1 Phase 1 Critical Path

The longest dependency chain determining Phase 1 minimum duration:

```
T-105 (Indexes, 2SP/W3) → T-113 (Snapshot, 3SP/W3-4) → T-106 (PPT SQL, 5SP/W3-5) → T-107 (PPT Frontend, 3SP/W5) → T-117 (Gate, 1SP/W7)
```

**Critical path length**: 14 SP (~70 hours, ~3.5 weeks at 20h/week)

**Parallel paths** (can run simultaneously):
- Path A: T-101 (1SP) → T-110 (3SP) → T-114 (2SP) = 6 SP
- Path B: T-102 (2SP) = 2 SP
- Path C: T-103 (2SP) → T-104 (2SP) = 4 SP
- Path D: T-108 (2SP) + T-109 (1SP) + T-111 (2SP) + T-112 (1SP) = 6 SP (all independent)
- Path E: T-115 (3SP) + T-116 (2SP) = 5 SP

**Schedule compression opportunity**: Paths A-E can all run in W1-W3 while indexes/snapshot build. The critical path bottleneck is T-106 (PPT SQL pagination) — this is the largest, highest-risk task.

## 4.2 Phase 2 Critical Path

```
T-201 (Multi-tenant, 5SP) → T-202 (Registration, 5SP) → T-203 (Onboarding, 3SP) → T-215 (Beta Launch, 2SP) → [Beta running] → T-204 (Stripe, 5SP)
```

**Critical path length**: 20 SP (~100 hours, ~5 weeks at 20h/week)

**Parallel paths**:
- T-205 (Logistics v3, 8SP) — independent, can run in Jun
- T-207, T-209, T-221 — small independent tasks
- T-210-213 (UX improvements) — parallel to onboarding

## 4.3 Full Project Critical Path (Phase 1 → Phase 4 Entry)

```
T-105 → T-113 → T-106 → T-107 → T-117 (Phase 1 gate)
  → T-201 → T-202 → T-203 → T-215 → T-204 → [Beta revenue]
    → T-304 → T-305 → T-314 (Phase 3 gate)
      → $5K MRR → T-404 (First hire)
```

**Minimum time to Phase 4 entry**: ~10 months (Apr 2026 → Jan 2027)

**Key milestones on critical path**:
1. PPT < 2s: W5 of Phase 1 (early May 2026)
2. Phase 1 gate: May 15, 2026
3. Beta launch: Jun 2026
4. First paying customer: Jul 2026
5. Phase 3 gate: Jan 2027
6. Phase 4 entry: Feb 2027

---

# 5. Risk Register for Implementation

## 5.1 Technical Risks

| ID | Risk | P(%) | Impact | Score | Mitigation |
|----|------|------|--------|-------|------------|
| TR-01 | PPT pagination refactor breaks existing profit calculations | 40% | HIGH (8) | 3.2 | Extensive testing; keep old endpoint as fallback; snapshot table as safety net |
| TR-02 | FX alert changes cascade errors in finance pipeline | 30% | HIGH (7) | 2.1 | Staged rollout; test all 3 FX consumers; keep log.warning→alert transition gradual |
| TR-03 | Multi-tenant schema migration corrupts existing data | 20% | CRITICAL (10) | 2.0 | Full backup before migration; `tenant_id` defaults to 1; reversible migration |
| TR-04 | Ads API rate limiting during intensive sync | 50% | MED (5) | 2.5 | Exponential backoff already in place; reduce sync frequency if needed |
| TR-05 | Connection pool exhaustion under beta user load | 35% | MED (6) | 2.1 | Monitor pool stats; alarm at 80% utilization; increase max_overflow |

## 5.2 Schedule Risks

| ID | Risk | P(%) | Impact | Score | Mitigation |
|----|------|------|--------|-------|------------|
| SR-01 | PPT pagination takes longer than 5 SP (complexity in CTE refactor) | 40% | HIGH (8) | 3.2 | Materialized view (T-113) as fast fallback; timebox to W5 |
| SR-02 | Multi-tenant takes longer than 5 SP (cross-cutting change) | 45% | HIGH (7) | 3.15 | Start with minimal tenant isolation (middleware filter); defer RLS |
| SR-03 | Zero beta signups in first 2 weeks | 25% | HIGH (8) | 2.0 | Pre-warm community before launch; have 5 committed beta testers lined up |
| SR-04 | Phase 1 gate missed (>7 weeks) | 30% | MED (6) | 1.8 | Weekly progress check; cut T-112, T-116 if behind |

## 5.3 Resource Risks

| ID | Risk | P(%) | Impact | Score | Mitigation |
|----|------|------|--------|-------|------------|
| RR-01 | Founder burnout during Phase 1 intense hardening | 50% | HIGH (8) | 4.0 | 50h/week cap; no weekend coding in Phase 1; T-112, T-116 are cuttable |
| RR-02 | No contractor available at $5K MRR trigger | 25% | MED (6) | 1.5 | Build network now; pre-screen 3 candidates before trigger |
| RR-03 | Azure cost spike from index creation + snapshot table | 20% | LOW (4) | 0.8 | Monitor DTU during index creation; off-hours execution |

## 5.4 Risk Mitigation Actions

| Action | Mitigates | Owner | Due |
|--------|-----------|-------|-----|
| Weekly Phase 1 progress check (Fri 17:00) | SR-01, SR-04 | Miłosz | Every Friday, W1-W7 |
| Full DB backup before any schema change | TR-01, TR-03 | Miłosz | Before T-105, T-113, T-201 |
| Pre-screen 3 frontend contractor candidates | RR-02 | Miłosz | Jul 2026 |
| Enforce 50h/week hard cap | RR-01 | Miłosz | Continuous |
| Create "cut list" for Phase 1 descoping | SR-04 | Miłosz | Apr 7, 2026 |

---

# 6. Phase Gate Checklists

## 6.1 Phase 1 → Phase 2 Gate (Target: May 15, 2026)

| # | Criterion | Metric | Target | Verification Method | Related Tasks |
|---|-----------|--------|--------|---------------------|---------------|
| H-1 | PPT load time | p95 server response + render | < 2.0s | 10 consecutive loads in DevTools | T-105, T-106, T-107, T-113 |
| H-2 | Ads data freshness | `ads_product_day` latest date vs now() | < 6 hours | SQL query + guardrail check | T-103, T-104 |
| H-3 | FX rate safety | No `return 1.0` in codebase | Alert fires on stale FX | Unit test + grep codebase | T-102 |
| H-4 | UI surface reduction | Visible sidebar items | ≤ 20 pages | Manual count | T-108 |
| H-5 | Uptime monitoring | UptimeRobot health checks | 99%+ for 7 days | UptimeRobot dashboard | T-101 |
| H-6 | Data Quality Score | Composite DQ metric | ≥ 82/100 | `/profit/data-quality` endpoint | T-111, T-112 |
| H-7 | Test pass rate | pytest + vitest combined | ≥ 85% | CI pipeline output | T-115 |
| H-8 | Zero silent failures | Guardrail coverage | All 8+ passing with alerts | Guardrails dashboard 7-day history | T-110 |

**GO decision**: All 8 criteria met → proceed to Phase 2  
**CONDITIONAL GO**: 6 of 8 met, remaining 2 have clear 1-week fix plan  
**NO-GO**: < 6 met → extend Phase 1 by 1-2 weeks

## 6.2 Phase 2 → Phase 3 Gate (Target: Sep 30, 2026)

| # | Criterion | Target | Related Tasks |
|---|-----------|--------|---------------|
| B-1 | Beta signups | ≥ 200 | T-215, T-220 |
| B-2 | Weekly active users | ≥ 50 | T-203, T-210 |
| B-3 | Paid customers | ≥ 20 | T-204 |
| B-4 | MRR | ≥ $2,000 | T-204 |
| B-5 | NPS (beta cohort) | ≥ 30 | T-217 |
| B-6 | Time-to-value | < 10 min | T-203 |
| B-7 | Data Trust Score | ≥ 88/100 | T-111, T-205 |
| B-8 | Support response time | < 4h (biz hours) | — |
| B-9 | Churn rate | < 8% | T-204 |
| B-10 | Zero P0 bugs | 0 for 14 days | T-115, T-313 |

## 6.3 Phase 3 → Phase 4 Gate (Target: Jan 31, 2027)

| # | Criterion | Target | Related Tasks |
|---|-----------|--------|---------------|
| L-1 | MRR | ≥ $5,000 | T-204, T-307 |
| L-2 | Paying users | ≥ 100 | T-306, T-309 |
| L-3 | Monthly churn | < 5% | T-309 |
| L-4 | NPS | ≥ 40 | T-217 |
| L-5 | Data Trust Score | ≥ 90/100 | T-205 |
| L-6 | PPT p95 load | < 1.5s | T-106, T-113 |
| L-7 | Support SLA | < 2h | — |
| L-8 | Docs site | ≥ 20 articles | T-308 |
| L-9 | DACH tested | 5+ DE users | T-304 |
| L-10 | LTV:CAC | > 4:1 | T-204, T-306 |

---

# Appendix A: Effort Summary by Phase

| Phase | Tasks | Total SP | Est. Hours | Calendar Weeks | Deep Eng h/wk |
|-------|-------|---------|------------|---------------|---------------|
| **Phase 1 — HARDEN** | 17 | 34 | ~155h | 7 weeks | 22 h/wk |
| **Phase 2 — BETA** | 22 | 63 | ~310h | 20 weeks | 16 h/wk |
| **Phase 3 — LAUNCH** | 14 | 53 | ~260h | 16 weeks | 16 h/wk |
| **Phase 4 — SCALE** | 9 | 36 | ~175h | Ongoing | Variable |
| **TOTAL** | **62** | **186** | **~900h** | **~43 weeks + ongoing** | — |

### Phase 1 Weekly Schedule

| Week | Tasks | SP | Focus |
|------|-------|----|-------|
| W1 (Apr 1-7) | T-101, T-102 | 3 | Uptime monitoring + FX safety |
| W2 (Apr 8-14) | T-103, T-104, T-110 (start) | 6 | Ads heartbeat + observability |
| W3 (Apr 15-21) | T-105, T-110 (finish), T-111, T-114 | 9 | Indexes + DQ improvements |
| W4 (Apr 22-28) | T-106 (main), T-113 | 8 | SQL pagination + snapshot |
| W5 (Apr 29-May 5) | T-106 (finish), T-107, T-108, T-109, T-112 | 9 | PPT frontend + page hiding |
| W6 (May 6-12) | T-115, T-116 | 5 | Testing + documentation |
| W7 (May 13-15) | T-117 | 1 | Gate review |

### Priority Matrix

| Priority | Phase 1 | Phase 2 | Phase 3 | Phase 4 | Total |
|----------|---------|---------|---------|---------|-------|
| P0 (Must-have) | 10 tasks (26 SP) | 6 tasks (22 SP) | 2 tasks (4 SP) | — | 18 (52 SP) |
| P1 (High value) | 5 tasks (7 SP) | 10 tasks (29 SP) | 7 tasks (30 SP) | 5 tasks (16 SP) | 27 (82 SP) |
| P2 (Should-have) | 2 tasks (1 SP) | 6 tasks (12 SP) | 5 tasks (19 SP) | 3 tasks (15 SP) | 16 (47 SP) |
| P3 (Nice-to-have) | — | — | — | 1 task (5 SP) | 1 (5 SP) |

---

# Appendix B: Source Traceability Matrix

Every task traces to at least one requirement from the source documents.

| Task | Primary Source | Section |
|------|---------------|---------|
| T-101 | PHASE_0_EXECUTIVE_SUMMARY | C-5 |
| T-102 | PHASE_0_EXECUTIVE_SUMMARY, FEEDBACK_SYNTHESIS | C-3, PP-05 |
| T-103 | PHASE_0_EXECUTIVE_SUMMARY, FEEDBACK_SYNTHESIS | C-2, PP-06 |
| T-104 | PHASE_0_EXECUTIVE_SUMMARY, FEEDBACK_SYNTHESIS | C-2, PP-02 |
| T-105 | SYSTEM_ARCHITECTURE_SPEC | §2.2 |
| T-106 | PHASE_0_EXECUTIVE_SUMMARY, FEEDBACK_SYNTHESIS | C-1, PP-01 |
| T-107 | STRATEGIC_PORTFOLIO_PLAN | Obj 4 KR1 |
| T-108 | PHASE_0_EXECUTIVE_SUMMARY, UX_RESEARCH_REPORT | C-4, BI-01 |
| T-109 | TECH_STACK_ASSESSMENT | TD-12 |
| T-110 | DATA_AUDIT_REPORT | R-01 |
| T-111 | DATA_AUDIT_REPORT | R-03 |
| T-112 | DATA_AUDIT_REPORT | R-05 |
| T-113 | DATA_AUDIT_REPORT, SYSTEM_ARCHITECTURE_SPEC | R-06, §2.3 |
| T-114 | DATA_AUDIT_REPORT | R-07 |
| T-115 | STRATEGIC_PORTFOLIO_PLAN, TECH_STACK_ASSESSMENT | H-7, Layer 8 |
| T-116 | STRATEGIC_PORTFOLIO_PLAN | Obj 5 KR2 |
| T-117 | STRATEGIC_PORTFOLIO_PLAN | Phase 1 Exit Criteria |
| T-201 | STRATEGIC_PORTFOLIO_PLAN, SYSTEM_ARCHITECTURE_SPEC | Feature Queue #2, §4.5 |
| T-202 | STRATEGIC_PORTFOLIO_PLAN | Phase 2 May Milestone |
| T-203 | STRATEGIC_PORTFOLIO_PLAN | B-6 (time-to-value) |
| T-204 | STRATEGIC_PORTFOLIO_PLAN, FINANCIAL_PLAN | Feature Queue #3, Obj 2 |
| T-205 | STRATEGIC_PORTFOLIO_PLAN, FEEDBACK_SYNTHESIS | Feature Queue #1, PP-03 |
| T-206 | STRATEGIC_PORTFOLIO_PLAN, FEEDBACK_SYNTHESIS | Feature Queue #4, FR-05 |
| T-207 | TECH_STACK_ASSESSMENT, SYSTEM_ARCHITECTURE_SPEC | TD-15, §3.5 |
| T-208 | SYSTEM_ARCHITECTURE_SPEC | §4.4 |
| T-209 | SYSTEM_ARCHITECTURE_SPEC | §3.3, ADR-006 |
| T-210 | UX_ARCHITECTURE, UX_RESEARCH_REPORT | §1.1, BI-03 |
| T-211 | UX_ARCHITECTURE | §1.3 |
| T-212 | UX_ARCHITECTURE | §1.3 |
| T-213 | UX_ARCHITECTURE, FEEDBACK_SYNTHESIS | §1.3, FR-10 |
| T-214 | TECH_STACK_ASSESSMENT | Build vs Buy |
| T-215 | STRATEGIC_PORTFOLIO_PLAN | Phase 2 Jun Milestone, KR1 |
| T-216 | STRATEGIC_PORTFOLIO_PLAN | Phase 2 BUY Decisions |
| T-217 | STRATEGIC_PORTFOLIO_PLAN | B-5 (NPS) |
| T-218 | SYSTEM_ARCHITECTURE_SPEC | §2.6, ADR-005 |
| T-219 | SYSTEM_ARCHITECTURE_SPEC | §2.5, ADR-004 |
| T-220 | STRATEGIC_PORTFOLIO_PLAN, BRAND_IDENTITY_SYSTEM | Phase 2, Brand |
| T-221 | SYSTEM_ARCHITECTURE_SPEC | §3.6 |
| T-222 | STRATEGIC_PORTFOLIO_PLAN, FEEDBACK_SYNTHESIS | Feature Queue #5, PP-08 |
| T-301 | FEEDBACK_SYNTHESIS | FR-09 |
| T-302 | STRATEGIC_PORTFOLIO_PLAN, FEEDBACK_SYNTHESIS | Feature Queue #8, PP-12 |
| T-303 | STRATEGIC_PORTFOLIO_PLAN, FEEDBACK_SYNTHESIS | Feature Queue #7, PP-04 |
| T-304 | STRATEGIC_PORTFOLIO_PLAN | L-9 |
| T-305 | STRATEGIC_PORTFOLIO_PLAN | Phase 3 Dec Milestone |
| T-306 | STRATEGIC_PORTFOLIO_PLAN | Phase 3 Oct Milestone |
| T-307 | STRATEGIC_PORTFOLIO_PLAN | Phase 3 Nov Milestone |
| T-308 | STRATEGIC_PORTFOLIO_PLAN | L-8 |
| T-309 | STRATEGIC_PORTFOLIO_PLAN | Phase 3 Nov Milestone |
| T-310 | SYSTEM_ARCHITECTURE_SPEC, TECH_STACK_ASSESSMENT | ADR-008, TD-02 |
| T-311 | SYSTEM_ARCHITECTURE_SPEC | §5 |
| T-312 | SYSTEM_ARCHITECTURE_SPEC | §5.9 |
| T-313 | TECH_STACK_ASSESSMENT | Layer 8 (E2E gap) |
| T-314 | STRATEGIC_PORTFOLIO_PLAN | Phase 3 Exit Criteria |
| T-401 | STRATEGIC_PORTFOLIO_PLAN, SYSTEM_ARCHITECTURE_SPEC | Phase 4 Triggers, §2.4 |
| T-402 | SYSTEM_ARCHITECTURE_SPEC | §1.1 Phase 2 Evolution |
| T-403 | STRATEGIC_PORTFOLIO_PLAN | Phase 4 DACH Trigger |
| T-404 | STRATEGIC_PORTFOLIO_PLAN | §3.4 Hiring Triggers |
| T-405 | SYSTEM_ARCHITECTURE_SPEC | §4.2, ADR-007 |
| T-406 | FEEDBACK_SYNTHESIS, UX_ARCHITECTURE | PP-09, §2.4 |
| T-407 | DATA_AUDIT_REPORT | R-08 |
| T-408 | STRATEGIC_PORTFOLIO_PLAN | Phase 4 Targets |
| T-409 | DATA_AUDIT_REPORT | R-09 |

---

**Document prepared by**: SeniorProjectManager Agent  
**Date**: 2026-03-13  
**Total tasks**: 62 | **Total effort**: 186 SP (~900 hours)  
**Next action**: Phase 1 kickoff — April 1, 2026. Start with T-101 (UptimeRobot) + T-102 (FX Alert).

---

*This document is the single source of truth for ACC development planning. All tasks trace to validated Phase 0 reports and Phase 1 architecture specifications. No luxury features have been added. Review weekly during Phase 1, bi-weekly during Phase 2+.*
