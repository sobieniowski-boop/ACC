# 🔧 ACC Technology Stack Assessment
## Technology Landscape Evaluation — Amazon Command Center

**Agent**: Tool Evaluator | **Date**: 2026-03-12 | **Classification**: Strategic — Internal  
**Prepared for**: Miłosz Sobieniowski, Founder  
**Sources**: Codebase audit (requirements.txt, package.json, config.py, Dockerfiles, CI/CD), 4 Phase 0 reports, 67 service modules, 12 intelligence modules, 8 connector packages  
**Methodology**: Stack census → build/buy scoring → integration feasibility → OSS/commercial matrix → risk quantification

---

## EXECUTIVE SUMMARY

ACC runs on a **mature, well-chosen, predominantly open-source tech stack** centered on FastAPI + React 18 + Azure SQL. The architecture is technically sound for a **single-operator analytics platform** currently processing ~850K orders across 9 EU marketplaces. However, four strategic technology tensions demand resolution:

| # | Tension | Verdict | Urgency |
|---|---------|---------|---------|
| **T-1** | In-process scheduler (APScheduler) vs. distributed orchestrator | **Keep APScheduler** until >3 workers | 🟡 Medium |
| **T-2** | Raw SQL everywhere vs. query builder / ORM for new code | **Keep raw SQL** + add materialized views | 🟢 Low |
| **T-3** | Custom-built everything vs. buying SaaS components | **Buy**: reporting/PDF, email delivery, uptime monitoring. **Build**: profit engine, logistics model, ads attribution | 🔴 High |
| **T-4** | Monolith API vs. microservice decomposition | **Keep monolith** for 12+ months; extract Celery workers only | 🟢 Low |

**Overall Stack Health Score: 78/100** — Strong foundation, over-breadth in module coverage, performance bottlenecks addressable without re-architecture.

**Key Recommendation**: Redirect 30% of "building new modules" effort to **hardening existing infrastructure** — pagination, observability, reporting. The stack needs depth, not breadth.

---

## 1. TECHNOLOGY STACK ASSESSMENT FOR THE PROBLEM DOMAIN

### 1.1 Problem Domain Definition

ACC operates in the **Amazon Seller Profit Analytics** space with these core requirements:

| Requirement | Technical Implication | ACC's Current Approach |
|-------------|----------------------|----------------------|
| Real-time order ingestion | API polling + event processing | SP-API polling (30min) + SQS events |
| Multi-source COGS calculation | ERP integration + file parsing + DB lookup | 6 price sources + Netfox ERP + XLSX |
| Profit calculation (CM1/CM2/NP) | Complex SQL aggregation + cross-table joins | Raw SQL engine (profit_engine.py, ~5K LoC) |
| Amazon Ads attribution | Ads API → product-level spend → profit join | Amazon Ads API v3 + daily reports |
| Multi-marketplace FX | Currency conversion at transaction level | NBP + ECB dual-source FX |
| Logistics cost modeling | Carrier API + billing reconciliation | GLS SOAP + DHL SOAP + billing CSV + estimation model |
| Dashboard visualization | Real-time charts + drill-down tables | React + Recharts + TanStack Query |
| Regulatory compliance | VAT/OSS tracking across EU | Tax module (schema-ready, partially populated) |

### 1.2 Stack Layer Assessment

#### Layer 1: Runtime & Language — Score: 9/10

| Component | Choice | Version | Assessment |
|-----------|--------|---------|-----------|
| **Backend Language** | Python | 3.12 | 🟢 Excellent for data-heavy analytics; rich ecosystem for Amazon APIs, data processing, ML |
| **Backend Framework** | FastAPI | 0.115.6 | 🟢 Best-in-class async Python framework; auto-docs, type validation, high performance |
| **Frontend Language** | TypeScript | 5.7.2 | 🟢 Industry standard; type safety prevents runtime errors in complex dashboards |
| **Frontend Framework** | React | 18.3.1 | 🟢 Dominant ecosystem; massive component library availability |
| **ASGI Server** | Uvicorn | 0.32.1 | 🟢 Production-grade, supports WebSocket, HTTP/2 ready |

**Verdict**: Language/framework choices are ideal for this domain. No changes recommended.

#### Layer 2: Data Storage — Score: 7/10

| Component | Choice | Version/Tier | Assessment |
|-----------|--------|-------------|-----------|
| **Primary DB** | Azure SQL | Standard S3 (~19 GB) | 🟢 Reliable, managed, good for structured seller data |
| **ERP DB** | Netfox MSSQL | On-prem | 🟢 Direct ERP access is unique competitive advantage |
| **Cache** | Redis | 7-alpine | 🟢 Standard choice for caching + pub/sub |
| **DB Driver (Azure)** | pymssql | 2.3.2 | 🟡 Works but limited: no async, no connection pooling, last release 2024 |
| **DB Driver (ERP)** | pyodbc | 5.2.0 | 🟢 Mature, well-supported |
| **ORM** | SQLAlchemy | 2.0.36 | 🟡 Installed but mostly unused (raw SQL preferred). Schema management via Alembic is the real value. |

**Gap**: pymssql is a low-activity OSS project — last significant release was 2024. No async support limits concurrency under load. However, switching would be disruptive and unnecessary at current scale.

**Recommendation**: Keep current stack. Plan migration to `aioodbc` + ODBC Driver 18 for Azure SQL when request concurrency exceeds 50 concurrent DB connections.

#### Layer 3: API & Integration — Score: 8/10

| Component | Choice | Assessment |
|-----------|--------|-----------|
| **HTTP Client** | httpx 0.28.1 | 🟢 Async-first, excellent for multi-API orchestration |
| **SOAP Client** | zeep 4.3.2 | 🟢 Best Python SOAP library; handles GLS/DHL WSDLs well |
| **AWS SDK** | boto3 1.37.1 | 🟢 Standard; used for SQS |
| **Excel Parser** | openpyxl 3.1.5 | 🟢 Reliable for .xlsx purchase price files |
| **XML Parser** | lxml 5.3.0 | 🟢 Fast C-based XML for ECB/SOAP |
| **JSON Serializer** | orjson 3.10.12 | 🟢 10-15× faster than stdlib json; used in all API responses |
| **Data Processing** | pandas 2.2.3 | 🟢 TSV/CSV parsing, batch data manipulation |

**Verdict**: Integration layer is comprehensive and well-chosen. All 8 connector packages use appropriate protocols. No changes needed.

#### Layer 4: Background Processing — Score: 6/10

| Component | Choice | Assessment |
|-----------|--------|-----------|
| **Scheduler** | APScheduler (AsyncIO) | 🟡 Works for single-instance; no distributed coordination beyond DB lock |
| **Task Queue** | Celery 5.4.0 + Redis | 🟡 Installed but `WORKER_EXECUTION_ENABLED=False` — not actively used in production |
| **Event System** | Custom SQS + SQL tables | 🟢 Well-designed event backbone with dedup + dispatch |

**Gap**: Dual scheduler architecture (APScheduler + Celery) creates confusion. APScheduler runs in-process with a SQL-based leader lock, which works but fails silently when the backend is offline (as evidenced by 105-min order sync gap, 447-min guardrail gap). Celery is present but disabled.

**Recommendation**: 
- **Short-term**: Add heartbeat monitoring to APScheduler jobs (detect zombie jobs)
- **Medium-term**: When scaling to >1 API instance, activate Celery for heavy sync jobs (orders, finance, ads) while keeping APScheduler for lightweight cron triggers
- **Do NOT migrate to Airflow/Prefect** — overkill for current scale

#### Layer 5: Frontend Architecture — Score: 8/10

| Component | Choice | Assessment |
|-----------|--------|-----------|
| **Build Tool** | Vite 6.0.3 | 🟢 Fastest dev experience; HMR, optimized production builds |
| **UI Components** | shadcn/ui (Radix + Tailwind + CVA) | 🟢 Best-in-class composable UI; full ownership of code |
| **State (Server)** | TanStack React Query 5.62.7 | 🟢 Industry standard for API-fetched data with caching |
| **State (Client)** | Zustand 5.0.2 | 🟢 Minimal, fast, ergonomic |
| **Charts** | Recharts 2.14.1 | 🟡 Functional but limited for complex financial charts; no drilldown clicks, limited animation |
| **CSS** | Tailwind CSS 3.4.16 | 🟢 Rapid development, consistent design system |
| **Routing** | React Router 6.28.0 | 🟢 Standard, stable |

**Gap**: Recharts is adequate for basic chart types but will become a bottleneck for:
- Drill-down interactive charts (click bar segment → filter table)
- Waterfall charts (CM1→CM2→NP visualization)
- Complex multi-axis financial charts

**Recommendation**: Keep Recharts for standard charts; evaluate **Nivo** (for pre-built financial chart types) or **ECharts** (for maximum flexibility) for the profit analytics dashboards only.

#### Layer 6: Observability — Score: 7/10

| Component | Choice | Assessment |
|-----------|--------|-----------|
| **Logging** | structlog 24.4.0 | 🟢 Production-grade structured logging; JSON output |
| **Tracing** | OpenTelemetry 1.29.0 | 🟢 Vendor-neutral, future-proof |
| **Error Tracking** | Sentry (backend + frontend) | 🟢 Industry standard |
| **Azure Monitor** | azure-monitor-otel-exporter 1.0.0b33 | 🟡 **Beta** — production use of beta exporter is a risk |
| **Uptime Monitoring** | None | 🔴 No external uptime check; backend offline = silent failure |
| **Dashboard** | None (guardrails only) | 🟡 No Grafana/equivalent for infrastructure metrics |

**Gap**: Observability is code-instrumented but lacks **operational monitoring**. When the FastAPI process dies, nobody knows until data goes stale. This was confirmed by 105-min+ order sync gaps.

**Recommendation**: Add **UptimeRobot** or **Better Uptime** (free tier: 50 monitors) for external HTTP health checks. This is the highest-ROI ops improvement (30 min setup, prevents hours of silent downtime).

#### Layer 7: Security — Score: 7/10

| Component | Choice | Assessment |
|-----------|--------|-----------|
| **Auth** | JWT (HS256, python-jose) | 🟡 HS256 is symmetric — RS256 preferred for multi-service. Acceptable for single-API. |
| **Password Hashing** | bcrypt (passlib) | 🟢 Industry standard |
| **CORS** | FastAPI CORSMiddleware | 🟢 Explicit origins configured |
| **Secrets Management** | .env files + pydantic-settings | 🟡 No vault; passwords in env vars. Acceptable for single-server. |
| **PII** | Sentry send_default_pii=False | 🟢 PII-safe error reporting |
| **TLS** | Enforced on Azure SQL (pymssql+TLS1.2) | 🟢 Encrypted in transit |

**Gap**: No secrets rotation. No WAF. No rate limiting on auth endpoints. These are acceptable for a single-operator internal tool but will need addressing before multi-user launch.

#### Layer 8: Testing — Score: 6/10

| Component | Choice | Assessment |
|-----------|--------|-----------|
| **Backend Tests** | pytest 8.3.4 + pytest-asyncio | 🟢 Standard, well-configured |
| **Frontend Tests** | vitest 4.0.18 + testing-library + msw | 🟢 Modern, fast, comprehensive mocking |
| **Linting** | ruff (backend) + eslint 9 (frontend) | 🟢 Fast, modern linters |
| **E2E Tests** | None | 🔴 No Playwright/Cypress for critical user flows |
| **Test Coverage (backend)** | 422/577 passing (73%) | 🟡 Adequate but declining |

**Gap**: No E2E tests for critical paths (PPT load, order sync verification, profit drill-down). Given the 14.5s PPT load regression, an E2E test would have caught this early.

---

### 1.3 Domain Fitness Scorecard

| Domain Requirement | Stack Fitness | Score | Notes |
|-------------------|--------------|-------|-------|
| Amazon API integration | Excellent | 9/10 | 13 SP-API modules, comprehensive coverage |
| Multi-source profit calculation | Excellent | 9/10 | 6 COGS sources, CM1/CM2/NP layers |
| Real-time dashboards | Good | 7/10 | React+TanStack strong, but 14.5s PPT is a regression |
| Multi-marketplace FX | Excellent | 9/10 | Dual-source (NBP+ECB), per-transaction conversion |
| Logistics cost modeling | Very Good | 8/10 | GLS+DHL SOAP integration, estimation model |
| ERP integration | Excellent | 10/10 | Direct pyodbc to Netfox — unique competitive advantage |
| Regulatory compliance | Adequate | 5/10 | Schema exists, data partially populated |
| Scalability (>5K orders/day) | Adequate | 6/10 | Single-process scheduler, no async DB, no pagination |
| **Weighted Average** | — | **7.8/10** | — |

---

## 2. BUILD VS. BUY ANALYSIS FOR KEY COMPONENTS

### 2.1 Decision Framework

Each component is scored across 5 dimensions (1–10 scale):

| Dimension | Weight | Description |
|-----------|--------|-------------|
| **Competitive Differentiation** | 0.30 | Does building this create moat? |
| **Implementation Complexity** | 0.20 | How hard is it to build well? |
| **Maintenance Burden** | 0.20 | Ongoing cost of ownership |
| **Alternative Availability** | 0.15 | Quality of buy/SaaS options |
| **Time-to-Value** | 0.15 | How fast does buying deliver value? |

**Decision rule**: Score ≥ 7.0 → Build | Score 4.0–6.9 → Evaluate | Score < 4.0 → Buy

### 2.2 Component-Level Build vs. Buy Matrix

#### CORE ENGINE COMPONENTS (Build ✅)

| Component | Diff. | Complex. | Maint. | Alt. Avail. | Time | **Score** | **Verdict** |
|-----------|-------|----------|--------|-------------|------|-----------|-------------|
| **Profit Engine** (CM1/CM2/NP) | 10 | 8 | 7 | 2 | — | **8.1** | 🟢 **BUILD** |
| **Logistics Cost Model** | 9 | 7 | 6 | 2 | — | **7.3** | 🟢 **BUILD** |
| **Order Pipeline** (5-step sync) | 8 | 6 | 5 | 3 | — | **6.7** | 🟢 **BUILD** |
| **Multi-source COGS** | 9 | 7 | 6 | 1 | — | **7.5** | 🟢 **BUILD** |
| **Ads→Profit Attribution** | 8 | 5 | 4 | 3 | — | **6.5** | 🟢 **BUILD** |
| **Guardrails System** | 7 | 4 | 3 | 4 | — | **5.7** | 🟢 **BUILD** (domain-specific) |

**Rationale**: These components ARE ACC's product. No SaaS tool provides CM1/CM2/NP calculation with ERP-sourced COGS, multi-carrier logistics, and Amazon Ads attribution in a single pipeline. This is the highest-defensibility moat identified by the Market Intelligence Report.

#### INFRASTRUCTURE COMPONENTS (Evaluate / Buy 💰)

| Component | Diff. | Complex. | Maint. | Alt. Avail. | Time | **Score** | **Verdict** |
|-----------|-------|----------|--------|-------------|------|-----------|-------------|
| **PDF/Report Generation** | 1 | 6 | 5 | 9 | 9 | **4.2** | 💰 **BUY** |
| **Email Delivery** | 1 | 3 | 2 | 10 | 10 | **3.4** | 💰 **BUY** |
| **Uptime Monitoring** | 1 | 2 | 1 | 10 | 10 | **2.8** | 💰 **BUY** |
| **Error Tracking** | 1 | 8 | 7 | 10 | 10 | **4.2** | 💰 **BUY** (Sentry ✅ already) |
| **Auth/RBAC** | 2 | 7 | 6 | 8 | 8 | **4.6** | 🔄 **BUY when multi-user** |
| **CSV/XLSX Export** | 2 | 3 | 2 | 8 | 9 | **3.5** | 💰 **BUY** (lib) |
| **Mobile App** | 3 | 8 | 7 | 5 | 7 | **5.4** | 🔄 **DEFER** |
| **Bank Feed Import** | 3 | 6 | 5 | 7 | 7 | **4.7** | 🔄 **EVALUATE** |

#### INTELLIGENCE COMPONENTS (Build with caution ⚠️)

| Component | Diff. | Complex. | Maint. | Alt. Avail. | Time | **Score** | **Verdict** |
|-----------|-------|----------|--------|-------------|------|-----------|-------------|
| **Seasonality Engine** | 6 | 5 | 5 | 5 | 6 | **5.6** | ⚠️ **BUILD-LIGHT** |
| **Decision Intelligence** | 7 | 8 | 7 | 3 | — | **6.7** | ⚠️ **BUILD-LIGHT** |
| **Repricing Engine** | 5 | 7 | 7 | 7 | 7 | **5.8** | 🔄 **EVALUATE** |
| **Content Optimization** | 3 | 6 | 5 | 8 | 8 | **4.5** | 💰 **DEFER/BUY** |
| **BuyBox Radar** | 5 | 4 | 3 | 6 | 7 | **5.0** | ⚠️ **BUILD-LIGHT** |
| **Inventory Risk** | 5 | 5 | 4 | 6 | 7 | **5.1** | ⚠️ **BUILD-LIGHT** |

### 2.3 Build vs. Buy Summary

```
┌──────────────────────────────────────────────────────────────────┐
│                  BUILD VS. BUY VERDICT MAP                       │
│                                                                  │
│  🟢 BUILD (Score ≥ 7.0) — Core product, high moat               │
│  ├─ Profit Engine (CM1/CM2/NP)        Score: 8.1                │
│  ├─ Multi-source COGS                  Score: 7.5                │
│  ├─ Logistics Cost Model               Score: 7.3                │
│  ├─ Order Pipeline                     Score: 6.7                │
│  └─ Ads→Profit Attribution             Score: 6.5                │
│                                                                  │
│  ⚠️ BUILD-LIGHT (Score 5.0-6.9) — Useful but scope carefully    │
│  ├─ Decision Intelligence              Score: 6.7                │
│  ├─ Repricing Engine                   Score: 5.8                │
│  ├─ Seasonality Engine                 Score: 5.6                │
│  ├─ Guardrails System                  Score: 5.7                │
│  ├─ Inventory Risk                     Score: 5.1                │
│  └─ BuyBox Radar                       Score: 5.0                │
│                                                                  │
│  💰 BUY / USE SaaS (Score < 5.0) — Commodity, buy speed          │
│  ├─ Uptime Monitoring                  → UptimeRobot (free)      │
│  ├─ Email Delivery                     → SendGrid ($0→$20/mo)    │
│  ├─ PDF/Report Generation              → WeasyPrint (OSS)        │
│  ├─ CSV/XLSX Export                    → openpyxl (already have) │
│  ├─ Error Tracking                     → Sentry (already have)   │
│  └─ Auth/RBAC (when multi-user)        → Clerk/Auth0 ($25/mo)    │
│                                                                  │
│  🔄 DEFER — Not needed yet                                       │
│  ├─ Mobile App                         12+ months away           │
│  ├─ Content Optimization               Content tables empty      │
│  └─ Bank Feed Import                   MT940 / open banking      │
└──────────────────────────────────────────────────────────────────┘
```

### 2.4 Cost Impact of Build vs. Buy Decisions

| Component | Build Cost (time) | Buy Cost ($/mo) | Recommendation | Annual Savings |
|-----------|------------------|-----------------|----------------|---------------|
| Uptime monitoring | 3 days | $0 (UptimeRobot free) | BUY | 3 dev-days saved |
| Email delivery | 1 week | $0–$20 (SendGrid) | BUY | 5 dev-days saved |
| PDF reports | 2 weeks | $0 (WeasyPrint OSS) | BUY (lib) | 0 (use OSS lib) |
| Auth/RBAC | 13 weeks | $25 (Clerk/Auth0) | BUY when needed | 60+ dev-days saved |
| Content Ops | 4+ weeks | Defer | DEFER | 20 dev-days saved |
| Repricing | 3+ weeks | Evaluate | EVALUATE | — |

**Total opportunity cost of building commodity components**: ~90 dev-days that could be spent on core profit engine improvements.

---

## 3. INTEGRATION FEASIBILITY WITH EXISTING SYSTEMS

### 3.1 Current Integration Architecture

```
┌──────────────────────────────────────────────────────────────┐
│              ACC INTEGRATION LANDSCAPE                        │
│                                                              │
│  ┌─────────────────── OPERATIONAL ──────────────────┐        │
│  │ ✅ Amazon SP-API (13 modules, 30min cycle)       │        │
│  │ ✅ Amazon Ads API v3 (4h cycle, 3 ad types)      │        │
│  │ ✅ Netfox ERP (direct SQL, read-only)             │        │
│  │ ✅ GLS Poland (SOAP ADE + billing CSV)            │        │
│  │ ✅ DHL24 (SOAP WebAPI2 + billing CSV)             │        │
│  │ ✅ NBP FX rates (REST, daily)                     │        │
│  │ ✅ ECB FX rates (XML, daily backup)               │        │
│  │ ✅ BaseLinker (REST API, distribution cache)       │        │
│  │ ✅ Google Sheets (CSV URLs, listings/EAN)          │        │
│  │ ✅ AWS SQS (notification polling)                  │        │
│  │ ✅ Network share (XLSX pricing, billing CSVs)      │        │
│  └───────────────────────────────────────────────────┘        │
│                                                              │
│  ┌─────────────────── INTELLIGENCE ─────────────────┐        │
│  │ ✅ OpenAI GPT-5.2 (pricing + reorder recs)       │        │
│  │ ⚪ Ergonode PIM (connector exists, minimal use)    │        │
│  │ ⚪ ProductOnboard (configured, Content Ops empty)  │        │
│  └───────────────────────────────────────────────────┘        │
│                                                              │
│  ┌─────────────────── OBSERVABILITY ────────────────┐        │
│  │ ✅ Sentry (error tracking, backend + frontend)    │        │
│  │ ✅ Azure Monitor (OTEL exporter, beta)             │        │
│  │ ⚪ Redis (cache/pubsub, Celery broker ready)       │        │
│  └───────────────────────────────────────────────────┘        │
│                                                              │
│  ┌─────────────────── PLANNED / NEEDED ─────────────┐        │
│  │ 🔲 Email delivery (SendGrid/SES)                  │        │
│  │ 🔲 Uptime monitoring (UptimeRobot/BetterUptime)   │        │
│  │ 🔲 Bank feed (MT940/CAMT or open banking)          │        │
│  │ 🔲 Accounting (WFIRMA/Fakturownia for PL)          │        │
│  │ 🔲 Additional ERPs (beyond Netfox)                  │        │
│  └───────────────────────────────────────────────────┘        │
└──────────────────────────────────────────────────────────────┘
```

### 3.2 Integration Feasibility Matrix

Each planned integration is scored on 5 feasibility dimensions (1=hard, 10=easy):

| Integration | API Quality | Auth Complexity | Data Model Fit | Dev Effort | Maintenance | **Feasibility** |
|-------------|-----------|----------------|---------------|-----------|-------------|:---:|
| **SendGrid email** | 10 | 9 (API key) | 10 (fire-and-forget) | 9 (2 days) | 10 | **9.6** |
| **UptimeRobot** | 10 | 9 (API key) | 10 (external) | 10 (30 min) | 10 | **9.8** |
| **WeasyPrint PDF** | 8 (OSS lib) | N/A | 8 (HTML→PDF) | 7 (1 week) | 7 | **7.5** |
| **Clerk/Auth0** | 9 | 7 (OAuth/OIDC) | 6 (JWT refactor) | 5 (2 weeks) | 8 | **7.0** |
| **WFIRMA API** | 6 (PL-only) | 7 (API key) | 5 (mapping needed) | 5 (2 weeks) | 6 | **5.8** |
| **Fakturownia** | 7 (REST) | 8 (token) | 6 (invoice mapping) | 6 (1 week) | 7 | **6.8** |
| **MT940 bank import** | 5 (file-based) | 6 (file access) | 5 (custom parser) | 4 (2 weeks) | 5 | **5.0** |
| **Open Banking PL** | 4 (PSD2 immature in PL) | 3 (bank approval) | 5 (transaction mapping) | 3 (4 weeks) | 4 | **3.8** |
| **InPost courier** | 7 (REST) | 7 (token) | 8 (same model as GLS/DHL) | 7 (1 week) | 7 | **7.2** |
| **Allegro marketplace** | 8 (REST) | 6 (OAuth2) | 4 (different order model) | 4 (3 weeks) | 5 | **5.4** |
| **Other ERPs** (SAP B1, Comarch) | 3 (varied) | 4 (custom per ERP) | 3 (schema mapping) | 2 (months) | 3 | **3.0** |
| **Grafana dashboarding** | 9 | 8 (self-hosted) | 9 (SQL direct) | 7 (3 days) | 6 | **7.8** |
| **Power BI embedded** | 8 | 5 (Azure AD) | 8 (SQL direct) | 6 (1 week) | 5 | **6.4** |

### 3.3 Integration Priority Recommendations

| Priority | Integration | Feasibility | Business Value | Effort | Status |
|----------|------------|-------------|---------------|--------|--------|
| **P0** | UptimeRobot | 9.8 | 🔴 Critical (prevents silent downtime) | 30 min | 🔲 Do now |
| **P1** | SendGrid email | 9.6 | 🟠 High (Morning Brief, reports) | 2 days | 🔲 Sprint 2 |
| **P1** | WeasyPrint PDF | 7.5 | 🟠 High (P&L reports, competitive gap) | 1 week | 🔲 Sprint 3 |
| **P2** | InPost courier | 7.2 | 🟡 Medium (PL market, growing carrier) | 1 week | 🔲 Backlog |
| **P2** | Grafana ops dashboard | 7.8 | 🟡 Medium (infrastructure visibility) | 3 days | 🔲 Backlog |
| **P3** | Clerk/Auth0 RBAC | 7.0 | 🟡 Medium (multi-user enablement) | 2 weeks | 🔲 When needed |
| **P3** | Fakturownia (PL invoicing) | 6.8 | 🟡 Medium (PL compliance gap) | 1 week | 🔲 Backlog |
| **P4** | WFIRMA accounting | 5.8 | 🟢 Low (bridgeable manually) | 2 weeks | 🔲 Icebox |
| **P4** | Allegro marketplace | 5.4 | 🟢 Low (expands TAM, high effort) | 3 weeks | 🔲 Icebox |
| **P5** | Open Banking PL | 3.8 | 🟢 Low (PSD2 immature for PL banks) | 4 weeks | 🔲 Monitor |
| **P5** | Other ERPs | 3.0 | 🟢 Low (per-customer customization) | Months | 🔲 Only on demand |

---

## 4. OPEN SOURCE VS. COMMERCIAL EVALUATION

### 4.1 Current Stack OSS/Commercial Split

ACC's stack is **92% open source** by component count. Commercial dependencies are minimal:

| Category | OSS | Commercial | Analysis |
|----------|-----|-----------|----------|
| **Backend (36 packages)** | 35 | 1 (OpenAI API) | 97% OSS |
| **Frontend (28 packages)** | 28 | 0 | 100% OSS |
| **Database** | 0 | 1 (Azure SQL) | Commercial (managed) |
| **Infrastructure** | 3 (Docker, nginx, Redis) | 1 (Sentry SaaS) | 75% OSS |
| **Observability** | 2 (OTEL, structlog) | 2 (Sentry, Azure Monitor) | 50/50 |
| **CI/CD** | 0 | 1 (GitHub Actions) | Commercial (freemium) |
| **External APIs** | 2 (NBP, ECB) | 5 (Amazon SP-API, Ads API, AWS SQS, GLS, DHL) | 71% commercial (vendor APIs) |

### 4.2 Component-Level OSS vs. Commercial Evaluation

For each key technology decision, we evaluate the OSS and commercial alternatives:

#### 4.2.1 Database: Azure SQL vs. PostgreSQL

| Criterion | Azure SQL (Current) | PostgreSQL (Alternative) |
|-----------|-------------------|------------------------|
| **Cost** | ~$150–300/mo (S3 tier, 19 GB) | $0 (self-hosted) or $50–100/mo (Azure Postgres Flex) |
| **MSSQL compatibility** | ✅ Native (ERP is MSSQL) | ❌ Different SQL dialect; cross-DB queries impossible |
| **T-SQL features used** | MERGE, OUTER APPLY, CROSS APPLY, CTEs, window functions | Most available in PG, but MERGE syntax differs |
| **Driver ecosystem** | pymssql (limited), pyodbc (good) | asyncpg (excellent async), psycopg3 (excellent) |
| **JSON support** | Limited (JSON_VALUE, OPENJSON) | Superior (JSONB, indexing, operators) |
| **Managed offering** | Azure SQL (excellent SLA, backups) | Azure Postgres Flex (excellent, cheaper) |
| **Migration effort** | — | 🔴 **MASSIVE** (187 tables with MSSQL-specific SQL, ~20K LoC of raw SQL) |

**Verdict**: 🟢 **KEEP Azure SQL**. Migration to PostgreSQL would require rewriting ~20K lines of T-SQL (MERGE, OUTER APPLY, CROSS APPLY patterns used extensively). The ERP is also MSSQL, making cross-database queries straightforward. Cost difference (~$100/mo) does not justify the migration risk.

#### 4.2.2 Scheduler: APScheduler vs. Alternatives

| Criterion | APScheduler (Current) | Celery Beat (Ready) | Airflow | Prefect/Dagster |
|-----------|---------------------|-------------------|---------|-----------------|
| **Cost** | $0 (OSS) | $0 (OSS) + Redis | $0 (OSS) + server | $0–$400/mo |
| **Complexity** | Low | Medium | High | Medium-High |
| **Distributed** | No (single-process + DB lock) | Yes (Redis-brokered) | Yes (Kubernetes/Docker) | Yes (cloud/self) |
| **Job monitoring** | Custom (DB tables) | Flower dashboard | Built-in web UI | Built-in web UI |
| **Current scale fit** | ✅ 42 jobs, working | ✅ Already installed | ❌ Overkill | ❌ Overkill |
| **Scale ceiling** | ~1 API instance | ~10 workers | Unlimited | Unlimited |

**Verdict**: 🟢 **KEEP APScheduler** for now. Activate Celery workers only when scaling beyond 1 API instance. Do NOT introduce Airflow/Prefect — the overhead (infra + learning curve) far exceeds the benefit for a single-operator tool.

#### 4.2.3 AI/LLM: OpenAI vs. Alternatives

| Criterion | OpenAI GPT-5.2 (Current) | Claude (Anthropic) | Open-Source (Llama/Mistral) |
|-----------|-------------------------|-------------------|---------------------------|
| **Cost** | ~$0.01–0.03/request | ~$0.01–0.05/request | $0 (self-hosted) + GPU cost |
| **Quality** | Excellent for structured JSON output | Excellent for reasoning | Good but lower accuracy |
| **API stability** | Excellent | Excellent | N/A (self-hosted) |
| **Latency** | ~1–3s per request | ~1–3s per request | Depends on hardware |
| **Privacy** | Data sent to OpenAI (API terms) | Data sent to Anthropic | Full data control |
| **Current usage** | Low (pricing + reorder recs only) | — | — |
| **Setup effort** | ✅ Done | 1 day (SDK swap) | 1–2 weeks + GPU |

**Verdict**: 🟢 **KEEP OpenAI** — usage is minimal (2 recommendation types), cost is negligible. Add an LLM abstraction layer (simple adapter pattern) so models can be swapped if pricing changes or privacy requirements emerge. Do NOT self-host LLMs — hardware cost and complexity are unjustified at current usage levels.

#### 4.2.4 Charts: Recharts vs. Alternatives

| Criterion | Recharts (Current) | Nivo | ECharts (Apache) | Highcharts |
|-----------|-------------------|------|------------------|------------|
| **License** | MIT (OSS) | MIT (OSS) | Apache 2.0 (OSS) | Commercial ($590+) |
| **React integration** | Native | Native | Wrapper (echarts-for-react) | Wrapper |
| **Financial chart types** | Basic (bar, line, area, pie) | Rich (heatmap, waffle, funnel) | Comprehensive (waterfall, treemap, sankey) | Most comprehensive |
| **Interactive drill-down** | Limited | Limited | Excellent | Excellent |
| **Bundle size** | ~45 KB | ~60 KB | ~200 KB (tree-shakable) | ~100 KB |
| **Performance (10K+ points)** | Slow (SVG-based) | Moderate (SVG) | Fast (Canvas) | Fast (SVG + Canvas) |
| **Learning curve** | Low | Low-Medium | Medium | Low-Medium |

**Verdict**: 🟡 **KEEP Recharts** for standard views + **ADD ECharts** for profit analytics only. ECharts provides waterfall charts (CM1→CM2→NP), canvas rendering for large datasets, and superior drill-down — all needed for the profit dashboard evolution. Apache 2.0 license, zero cost.

#### 4.2.5 Reporting: OSS Libraries vs. SaaS

| Criterion | WeasyPrint (OSS) | wkhtmltopdf (OSS) | Puppeteer/Playwright | Anvil (SaaS) |
|-----------|-----------------|-------------------|---------------------|--------------|
| **License/Cost** | BSD (free) | LGPL (free) | Apache 2.0 (free) | $150/mo |
| **Quality** | Good (CSS-based) | Moderate | Excellent (Chrome-fidelity) | Excellent |
| **Python integration** | Native | subprocess | subprocess / node | REST API |
| **Charts in PDF** | Requires static images | Requires static images | Renders live charts | Renders templates |
| **Server dependency** | Pure Python | External binary | Headless Chromium | None (API) |
| **Setup effort** | pip install (easy) | System install (medium) | npm install (medium) | API key (easy) |

**Verdict**: 🟢 **USE WeasyPrint** for basic P&L reports (HTML→PDF, Python-native). For chart-heavy reports (profit waterfall + charts), consider **Playwright** PDF renderer which can capture the React-rendered dashboard directly.

### 4.3 OSS Risk Assessment

| Package | Risk Level | Concern | Mitigation |
|---------|-----------|---------|-----------|
| **pymssql** 2.3.2 | 🟡 Medium | Low-activity project; 6 maintainers; last release 2024 | Pin version; plan migration to aioodbc when async needed |
| **zeep** 4.3.2 | 🟡 Medium | SOAP dying protocol; limited development | GLS/DHL are stable SOAP APIs, unlikely to change |
| **python-jose** 3.3.0 | 🟡 Medium | Not actively maintained since 2022 | Consider `pyjwt` + `cryptography` as drop-in replacement |
| **passlib** 1.7.4 | 🟡 Medium | Last release 2021 | `bcrypt` library directly is the modern alternative |
| **celery** 5.4.0 | 🟢 Low | Large active community | Currently unused; low risk |
| **azure-monitor-otel-exporter** 1.0.0b33 | 🟠 High | **BETA** version in production | May have breaking changes; consider stable OTLP exporter |

---

## 5. TECHNOLOGY RISK ASSESSMENT

### 5.1 Risk Register

Each risk is scored on Probability (1–5) × Impact (1–5) = Risk Score (1–25).

#### 5.1.1 Infrastructure Risks

| ID | Risk | Prob | Impact | Score | Category | Mitigation |
|----|------|------|--------|-------|----------|-----------|
| **TR-01** | **Silent backend crash** — APScheduler dies, no external monitoring, data goes stale for hours | 4 | 5 | **20** | 🔴 Critical | Add UptimeRobot (P0, 30 min); Azure health probe on `/api/v1/health` |
| **TR-02** | **Azure SQL outage** — single database, no read replica, 19 GB growing | 2 | 5 | **10** | 🟠 High | Azure SQL automatic backups (35-day retention); enable geo-replication when >50 GB |
| **TR-03** | **Network share failure** (N:\ drive) — blocks XLSX pricing + billing CSV imports | 3 | 4 | **12** | 🟠 High | Implement file cache in Azure Blob Storage as fallback |
| **TR-04** | **Redis unavailability** — Celery broker/backend and cache layer down | 2 | 3 | **6** | 🟡 Medium | Redis is currently non-critical (APScheduler doesn't need it); add Redis Sentinel when activating Celery |

#### 5.1.2 Dependency Risks

| ID | Risk | Prob | Impact | Score | Category | Mitigation |
|----|------|------|--------|-------|----------|-----------|
| **TR-05** | **Amazon SP-API rate limit changes** — Amazon tightens quota, breaking 30-min sync | 3 | 4 | **12** | 🟠 High | Already tracking `acc_sp_api_usage_daily`; implement adaptive throttling + larger batch windows |
| **TR-06** | **Amazon Ads API deprecation** — v3→v4 migration (happened v2→v3 in 2024) | 2 | 4 | **8** | 🟡 Medium | Ads connector is well-abstracted; ~2 week migration per major version |
| **TR-07** | **pymssql abandonment** — project goes unmaintained, no async support | 3 | 3 | **9** | 🟡 Medium | Migration path: aioodbc + ODBC Driver 18 (already used for Netfox). ~1 week migration. |
| **TR-08** | **python-jose / passlib unmaintained** — no security patches | 4 | 2 | **8** | 🟡 Medium | Replace with `pyjwt` + `bcrypt` directly (1 day each) |
| **TR-09** | **Azure Monitor OTEL exporter beta breaking change** | 3 | 2 | **6** | 🟡 Medium | Switch to stable generic OTLP exporter (1 hour change) |
| **TR-10** | **GLS/DHL SOAP API sunset** — carriers migrate to REST | 2 | 3 | **6** | 🟡 Medium | GLS already has REST Track & Trace alternative; DHL is slower to change |

#### 5.1.3 Operational Risks

| ID | Risk | Prob | Impact | Score | Category | Mitigation |
|----|------|------|--------|-------|----------|-----------|
| **TR-11** | **Build-fatigue burnout** — founder building 33+ pages, 67 services, 12 intelligence modules alone | 5 | 5 | **25** | 🔴 Critical | **Freeze new module development**. Focus on hardening 20 core pages. Buy commodity components. |
| **TR-12** | **Technical debt accumulation** — 72 empty tables, 37% overinvested UI, declining test coverage | 4 | 3 | **12** | 🟠 High | Sprint dedicated to cleanup: drop empty tables, hide unused pages, increase test coverage to 85%+ |
| **TR-13** | **Single point of knowledge** — all architecture decisions in one person's head | 5 | 4 | **20** | 🔴 Critical | This Phase 0 documentation effort is the #1 mitigation. Continue COPILOT_CONTEXT.md updates. |
| **TR-14** | **Performance regression undetected** — 14.5s PPT load not caught by any automated test | 4 | 3 | **12** | 🟠 High | Add E2E performance test: `PPT load < 3s` assertion in CI pipeline |

#### 5.1.4 Security Risks

| ID | Risk | Prob | Impact | Score | Category | Mitigation |
|----|------|------|--------|-------|----------|-----------|
| **TR-15** | **JWT HS256 key compromise** — symmetric key leaked from env → full auth bypass | 2 | 5 | **10** | 🟠 High | Acceptable for single-user; migrate to RS256 before multi-user launch |
| **TR-16** | **No WAF** — direct API exposure without rate limiting on auth endpoints | 3 | 3 | **9** | 🟡 Medium | Add `slowapi` rate limiter on `/auth/*` endpoints (2 hours) |
| **TR-17** | **Secrets in .env files** — no vault, no rotation | 3 | 3 | **9** | 🟡 Medium | Acceptable for single-server; Azure Key Vault when multi-instance |
| **TR-18** | **ODBC connection string in memory** — Netfox ERP credentials accessible via process dump | 2 | 3 | **6** | 🟡 Medium | Standard risk for DB clients; network isolation is primary control |

#### 5.1.5 Scalability Risks

| ID | Risk | Prob | Impact | Score | Category | Mitigation |
|----|------|------|--------|-------|----------|-----------|
| **TR-19** | **Single-process ceiling** — all 42 scheduler jobs + API requests in one Python process | 4 | 3 | **12** | 🟠 High | Celery already installed. Activate with Redis broker when load increases. |
| **TR-20** | **No SQL pagination** — PPT loads all data; will degrade as product count grows | 5 | 3 | **15** | 🔴 Critical | Implement OFFSET/FETCH NEXT in profit_engine.py (Data Audit R-04, 1 day) |
| **TR-21** | **Azure SQL S3 limit** — 250 GB max per S3 tier; 19 GB current, ~5 GB/yr growth | 1 | 2 | **2** | 🟢 Low | ~46 years at current rate; S4 tier if ever needed ($2×) |

### 5.2 Risk Heat Map

```
                     IMPACT
              1      2      3      4      5
         ┌──────┬──────┬──────┬──────┬──────┐
    5    │      │TR-08 │TR-12 │TR-13 │TR-11 │
         │      │      │TR-14 │      │      │
    P  4 │      │TR-09 │TR-07 │TR-05 │TR-01 │
    R    │      │      │TR-19 │      │      │
    O  3 │      │TR-16 │TR-17 │TR-03 │      │
    B    │      │      │TR-10 │      │      │
       2 │      │TR-18 │TR-04 │TR-06 │TR-02 │
         │      │      │      │      │TR-15 │
       1 │      │TR-21 │      │      │      │
         └──────┴──────┴──────┴──────┴──────┘

  🟢 Low (1-4)   🟡 Medium (5-9)   🟠 High (10-15)   🔴 Critical (16-25)
```

### 5.3 Top 5 Risks by Score

| Rank | ID | Risk | Score | Action |
|------|-----|------|-------|--------|
| **1** | TR-11 | Build-fatigue burnout (33+ pages solo) | **25** | IMMEDIATE: Buy commodity, freeze new modules |
| **2** | TR-01 | Silent backend crash (no uptime monitoring) | **20** | IMMEDIATE: UptimeRobot (30 min setup) |
| **3** | TR-13 | Single point of knowledge | **20** | IN PROGRESS: Phase 0 documentation wave |
| **4** | TR-20 | No SQL pagination → PPT regression | **15** | SPRINT 1: OFFSET/FETCH NEXT |
| **5** | TR-05 | Amazon SP-API rate limit changes | **12** | PLANNING: Adaptive throttling |

---

## 6. RECOMMENDATION MATRIX — CONSOLIDATED

### 6.1 Technology Decisions Summary

| # | Decision | Verdict | Confidence | Effort | Timeline |
|---|----------|---------|-----------|--------|----------|
| **TD-01** | Keep Azure SQL vs. migrate to PostgreSQL | **KEEP** Azure SQL | 95% | — | — |
| **TD-02** | Keep APScheduler vs. migrate to Celery/Airflow | **KEEP** APScheduler + activate Celery workers when scaling | 90% | 2 days (Celery activation) | When >1 instance |
| **TD-03** | Keep raw SQL vs. adopt ORM | **KEEP** raw SQL + add materialized views | 95% | 1 day (views) | Sprint 2 |
| **TD-04** | Keep Recharts vs. migrate to ECharts | **ADD** ECharts for profit dashboards; keep Recharts for standard | 80% | 3 days | Sprint 3 |
| **TD-05** | Keep OpenAI vs. self-host LLM | **KEEP** OpenAI + add abstraction layer | 90% | 2 hours (adapter) | Backlog |
| **TD-06** | Buy uptime monitoring (UptimeRobot) | **BUY** (free tier) | 99% | 30 min | **NOW** |
| **TD-07** | Buy email delivery (SendGrid) | **BUY** (free tier → $20/mo) | 95% | 2 days | Sprint 2 |
| **TD-08** | Build PDF reports with WeasyPrint | **USE OSS LIB** | 85% | 1 week | Sprint 3 |
| **TD-09** | Buy auth/RBAC (Clerk/Auth0) when multi-user | **BUY** when needed | 85% | 2 weeks | 6+ months |
| **TD-10** | Freeze Content Ops module development | **FREEZE** (17/18 tables empty, 4 unused pages) | 90% | 0 (savings) | **NOW** |
| **TD-11** | Freeze Repricing engine development | **EVALUATE** (competitors exist) | 70% | — | Research Q2 |
| **TD-12** | Replace python-jose with pyjwt | **REPLACE** (unmaintained) | 85% | 1 day | Sprint 2 |
| **TD-13** | Replace azure-monitor-otel-exporter beta | **REPLACE** with stable OTLP exporter | 90% | 1 hour | Sprint 1 |
| **TD-14** | Add E2E test framework (Playwright) | **ADD** | 80% | 3 days | Sprint 3 |
| **TD-15** | Add rate limiting (slowapi) | **ADD** | 85% | 2 hours | Sprint 1 |

### 6.2 Immediate Actions (This Week)

| # | Action | Effort | Impact |
|---|--------|--------|--------|
| **IA-1** | Set up UptimeRobot free account → monitor `/api/v1/health` | 30 min | Prevents hours of silent downtime |
| **IA-2** | Replace `azure-monitor-opentelemetry-exporter` beta with stable OTLP exporter | 1 hour | Removes beta dependency risk |
| **IA-3** | Add `slowapi` rate limiter on `/auth/login` and `/auth/refresh` | 2 hours | Basic brute-force protection |
| **IA-4** | Freeze development on Content Ops, Repricing, Planning modules | 0 effort | Redirects dev time to core |

### 6.3 Sprint Plan (Weeks 1–4)

| Sprint | Focus | Technology Actions |
|--------|-------|-------------------|
| **Sprint 1** (Week 1) | Critical fixes | IA-1→IA-4 + SQL pagination (TD-03) + ads heartbeat fix |
| **Sprint 2** (Week 2) | Infrastructure | SendGrid integration (TD-07) + python-jose→pyjwt (TD-12) + materialized views |
| **Sprint 3** (Weeks 3–4) | Reporting + quality | WeasyPrint PDF (TD-08) + ECharts for profit (TD-04) + Playwright E2E (TD-14) |
| **Sprint 4** (Month 2) | Intelligence | Celery worker activation (TD-02) + LLM abstraction (TD-05) |

---

## 7. TECHNOLOGY LANDSCAPE MAP

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ACC TECHNOLOGY LANDSCAPE 2026-03-12                       │
│                                                                             │
│  ┌─── PRESENTATION ─────────────────────────────────────────────────────┐   │
│  │  React 18.3    │ TypeScript 5.7  │ Vite 6.0      │ TailwindCSS 3.4 │   │
│  │  TanStack Q 5  │ Zustand 5.0     │ shadcn/ui     │ Recharts 2.14   │   │
│  │  Lucide icons  │ React Router 6  │ Sentry React  │ date-fns 4.1    │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                              ▼ HTTP/WS                                      │
│  ┌─── APPLICATION ──────────────────────────────────────────────────────┐   │
│  │  FastAPI 0.115 │ Uvicorn 0.32    │ Pydantic 2.10 │ orjson 3.10     │   │
│  │  httpx 0.28    │ python-jose 3.3 │ passlib 1.7   │ structlog 24.4  │   │
│  │  websockets 14 │ pandas 2.2      │ openpyxl 3.1  │ lxml 5.3        │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                              ▼                                              │
│  ┌─── BUSINESS LOGIC ──────────────────────────────────────────────────┐   │
│  │  CORE (Build ✅)                    │ INTELLIGENCE (Build-Light ⚠️)  │   │
│  │  ├─ profit_engine.py (~5K LoC)     │ ├─ seasonality_service.py     │   │
│  │  ├─ order_pipeline.py (5-step)     │ ├─ decision_intelligence.py   │   │
│  │  ├─ finance_center/ (sub-pkg)      │ ├─ buybox_radar.py            │   │
│  │  ├─ cogs_importer.py (6 sources)   │ ├─ inventory_risk.py          │   │
│  │  ├─ ads_sync.py                    │ ├─ repricing_engine.py        │   │
│  │  ├─ logistics models (GLS/DHL)     │ ├─ content_optimization.py    │   │
│  │  └─ guardrails.py (27 checks)      │ └─ refund_anomaly.py          │   │
│  │                                     │                               │   │
│  │  SERVICES: 67 modules  │  API: 51 endpoints  │  JOBS: 42 scheduled │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                              ▼                                              │
│  ┌─── INTEGRATION ──────────────────────────────────────────────────────┐   │
│  │  Amazon SP-API (13 mod) │ Amazon Ads v3    │ Netfox ERP (pyodbc)   │   │
│  │  GLS ADE (zeep SOAP)    │ DHL24 (SOAP)     │ NBP/ECB (FX REST/XML) │   │
│  │  BaseLinker REST        │ OpenAI GPT-5.2   │ AWS SQS (boto3)       │   │
│  │  Google Sheets CSV      │ Ergonode PIM     │ ProductOnboard PIM    │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                              ▼                                              │
│  ┌─── DATA ────────────────────────────────────────────────────────────┐   │
│  │  Azure SQL 19 GB        │ Redis 7-alpine   │ N:\ network share     │   │
│  │  187 tables, 26.6M rows │ Cache + Pub/Sub  │ XLSX + CSV files      │   │
│  │  pymssql 2.3 (Azure)    │ Celery broker    │ openpyxl parser       │   │
│  │  pyodbc 5.2 (ERP)       │                  │                       │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                              ▼                                              │
│  ┌─── OBSERVABILITY ───────────────────────────────────────────────────┐   │
│  │  Sentry (be+fe)        │ OTEL 1.29        │ structlog JSON        │   │
│  │  Azure Monitor (beta!) │ pytest 8.3       │ vitest 4.0            │   │
│  │  ⚠️ No uptime monitor  │ ruff + ESLint 9  │ 422/577 tests (73%)  │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                              ▼                                              │
│  ┌─── INFRASTRUCTURE ──────────────────────────────────────────────────┐   │
│  │  Docker (Python 3.12 + Node 20 + nginx + Redis)                    │   │
│  │  GitHub Actions CI/CD (lint + test + build + Docker push)          │   │
│  │  APScheduler (42 jobs, in-process) + Celery (ready, disabled)      │   │
│  │  ⚠️ No IaC │ ⚠️ No WAF │ ⚠️ No secrets vault                     │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## APPENDIX A: Full Dependency Inventory

### Python Backend (36 packages)

| # | Package | Version | License | OSS? | Risk |
|---|---------|---------|---------|------|------|
| 1 | fastapi | 0.115.6 | MIT | ✅ | 🟢 Low |
| 2 | uvicorn[standard] | 0.32.1 | BSD | ✅ | 🟢 Low |
| 3 | sqlalchemy[asyncio] | 2.0.36 | MIT | ✅ | 🟢 Low |
| 4 | alembic | 1.14.0 | MIT | ✅ | 🟢 Low |
| 5 | aioodbc | 0.5.0 | Apache 2.0 | ✅ | 🟡 Medium |
| 6 | pydantic | 2.10.3 | MIT | ✅ | 🟢 Low |
| 7 | pydantic-settings | 2.6.1 | MIT | ✅ | 🟢 Low |
| 8 | python-jose[cryptography] | 3.3.0 | MIT | ✅ | 🟡 Stale (2022) |
| 9 | passlib[bcrypt] | 1.7.4 | BSD | ✅ | 🟡 Stale (2021) |
| 10 | python-multipart | 0.0.20 | Apache 2.0 | ✅ | 🟢 Low |
| 11 | httpx | 0.28.1 | BSD | ✅ | 🟢 Low |
| 12 | boto3 | 1.37.1 | Apache 2.0 | ✅ | 🟢 Low |
| 13 | redis | 5.2.1 | MIT | ✅ | 🟢 Low |
| 14 | celery | 5.4.0 | BSD | ✅ | 🟢 Low |
| 15 | kombu | 5.4.2 | BSD | ✅ | 🟢 Low |
| 16 | pyodbc | 5.2.0 | MIT | ✅ | 🟢 Low |
| 17 | pymssql | 2.3.2 | LGPL | ✅ | 🟡 Medium |
| 18 | sqlalchemy-pytds | 0.3.0 | Apache 2.0 | ✅ | 🟡 Niche |
| 19 | pytz | 2024.2 | MIT | ✅ | 🟢 Low |
| 20 | openai | 1.58.1 | MIT | ✅ | 🟢 Low |
| 21 | pandas | 2.2.3 | BSD | ✅ | 🟢 Low |
| 22 | openpyxl | 3.1.5 | MIT | ✅ | 🟢 Low |
| 23 | lxml | 5.3.0 | BSD | ✅ | 🟢 Low |
| 24 | orjson | 3.10.12 | Apache/MIT | ✅ | 🟢 Low |
| 25 | structlog | 24.4.0 | Apache 2.0 | ✅ | 🟢 Low |
| 26 | opentelemetry-api | 1.29.0 | Apache 2.0 | ✅ | 🟢 Low |
| 27 | opentelemetry-sdk | 1.29.0 | Apache 2.0 | ✅ | 🟢 Low |
| 28 | opentelemetry-exporter-otlp-proto-grpc | 1.29.0 | Apache 2.0 | ✅ | 🟢 Low |
| 29 | azure-monitor-opentelemetry-exporter | 1.0.0b33 | MIT | ✅ | 🟠 **BETA** |
| 30 | sentry-sdk[fastapi] | 2.19.2 | MIT | ✅ | 🟢 Low |
| 31 | websockets | 14.1 | BSD | ✅ | 🟢 Low |
| 32 | zeep | 4.3.2 | MIT | ✅ | 🟡 Medium |
| 33 | python-dotenv | 1.0.1 | BSD | ✅ | 🟢 Low |
| 34 | pytest | 8.3.4 | MIT | ✅ | 🟢 Low |
| 35 | pytest-asyncio | 0.25.0 | Apache 2.0 | ✅ | 🟢 Low |
| 36 | pytest-cov | 6.0.0 | MIT | ✅ | 🟢 Low |

### Frontend (28 production + 17 dev packages)

| # | Package | Version | License | Risk |
|---|---------|---------|---------|------|
| 1 | react | 18.3.1 | MIT | 🟢 |
| 2 | react-dom | 18.3.1 | MIT | 🟢 |
| 3 | react-router-dom | 6.28.0 | MIT | 🟢 |
| 4 | @tanstack/react-query | 5.62.7 | MIT | 🟢 |
| 5 | zustand | 5.0.2 | MIT | 🟢 |
| 6 | axios | 1.7.9 | MIT | 🟢 |
| 7 | recharts | 2.14.1 | MIT | 🟢 |
| 8 | @radix-ui/* (10 packages) | Various | MIT | 🟢 |
| 9 | @sentry/react | 10.43.0 | MIT | 🟢 |
| 10 | class-variance-authority | 0.7.1 | Apache 2.0 | 🟢 |
| 11 | clsx | 2.1.1 | MIT | 🟢 |
| 12 | date-fns | 4.1.0 | MIT | 🟢 |
| 13 | lucide-react | 0.460.0 | ISC | 🟢 |
| 14 | tailwind-merge | 2.5.5 | MIT | 🟢 |
| 15 | tailwindcss-animate | 1.0.7 | MIT | 🟢 |

**All frontend dependencies are MIT/ISC/Apache 2.0 licensed with active communities. No risk flags.**

## APPENDIX B: Cross-Reference to Phase 0 Reports

| Finding | Source Report | This Report Section |
|---------|-------------|-------------------|
| PPT 14.5s load → SQL pagination needed | Data Audit R-04, Feedback PP-01, UX Research | §1.2 Layer 2 (DB), §5 TR-20 |
| Ads data stale 69–93h | Data Audit DQ-01 | §1.2 Layer 4 (Background), §5 TR-01 |
| 72 empty tables / module overinvestment | Data Audit DQ-04, Feedback anti-churn | §2.2 (Content Ops freeze), §5 TR-12 |
| No automated reports (competitive gap vs Sellerboard) | Market Intelligence, Feedback FR-09, UX Research | §2.2 (PDF reports), §3 (WeasyPrint) |
| Build fatigue risk | Feedback (75% confidence) | §5 TR-11 (#1 risk) |
| Single point of knowledge | UX Research | §5 TR-13 |
| No uptime monitoring | Data Audit R-01 | §1.2 Layer 6, §5 TR-01, §6.2 IA-1 |
| ERP integration = unique moat | Market Intelligence | §2.2 (Build verdict for COGS) |
| Recharts limitations for profit charts | Feedback FR-01 | §4.2.4 (ECharts recommendation) |
| Auth/RBAC needed for multi-user | Feedback PP-13, UX Research | §2.2 (Buy Clerk/Auth0), §3 |

---

*Tech Stack Assessment v1.0 — Generated by Tool Evaluator Agent*  
*Methodology: Stack Census → Build/Buy Scoring → Integration Feasibility → OSS/Commercial Matrix → Risk Quantification*  
*Overall Stack Health: 78/100 — Strong foundation, operational gaps addressable without re-architecture*  
*#1 Risk: Build-fatigue burnout (TR-11, score 25/25) — mitigate by buying commodity, freezing new modules*  
*#1 Recommendation: Set up UptimeRobot NOW (30 minutes, prevents hours of silent downtime)*  
*Cross-references: [Data Audit Report](DATA_AUDIT_REPORT_2026-03-12.md), [Market Intelligence](MARKET_INTELLIGENCE_REPORT_2026-03-12.md), [Feedback Synthesis](FEEDBACK_SYNTHESIS_REPORT_2026-03-12.md), [UX Research](UX_RESEARCH_REPORT_2026-03-12.md)*
