# ACC — System Architecture

> Amazon Command Center — Event-driven analytics and decision platform
> Version: 2026-03-12 | Status: Production

---

## 1. System Overview

ACC is an event-driven **Digital Twin** of an Amazon seller business. It ingests data from Amazon SP-API, Ads API, ERP systems, courier APIs, and financial feeds, normalizes them into a canonical schema, computes a unified financial model (CM1 → CM2 → NP), and exposes analytics, intelligence, and operational tooling through a React SPA.

```
┌──────────────────────────────────────────────────────────────────────┐
│                        OPERATOR CONSOLE                              │
│           React 18 SPA — 91 pages, Recharts, Radix, TanStack Query  │
├──────────────────────────────────────────────────────────────────────┤
│                          API GATEWAY                                 │
│               FastAPI — 500+ endpoints, JWT RBAC, CORS               │
├──────────┬──────────┬──────────┬──────────┬──────────────────────────┤
│INTELLIGENCE│ EXECUTION │INGESTION │COMPLIANCE│      LOGISTICS         │
│ profit/   │content_ops│ orders   │  tax     │   dhl/gls              │
│ buybox    │fba_ops   │ listings │  vat     │   cost_estimation       │
│ catalog   │repricing │ pricing  │          │   billing_import        │
│ inventory │family_map│ finance  │          │   courier_verification  │
│ refund    │actions   │ inventory│          │                          │
│ content   │          │ ads      │          │                          │
│ operator  │          │          │          │                          │
│ account   │          │          │          │                          │
├──────────┴──────────┴──────────┴──────────┴──────────────────────────┤
│                       PLATFORM LAYER                                  │
│  event_backbone │ action_center │ job_dispatch │ schema_registry      │
│  shared/db      │ shared/cache  │ otel         │ scheduler (12 mods)  │
├──────────────────────────────────────────────────────────────────────┤
│                        DOMAIN LAYER                                   │
│  canonical_product │ marketplace_mapping │ fee_taxonomy │ pl_model    │
├──────────────────────────────────────────────────────────────────────┤
│                        CONNECTORS                                     │
│  SP-API (11) │ Ads API (3) │ DHL24 │ GLS │ MSSQL │ Redis │ SQS      │
│  Ergonode    │ BaseLinker   │ ECB   │ NBP │ OpenAI                    │
├──────────────────────────────────────────────────────────────────────┤
│                       INFRASTRUCTURE                                  │
│  Azure SQL (MSSQL) │ Redis 7 │ AWS SQS │ Azure Container Apps        │
│  Sentry │ OpenTelemetry │ Azure Monitor │ GitHub Actions CI           │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 2. Deployment Topology

```
                    ┌─────────────┐
                    │  GitHub CI   │  lint + test + Docker build
                    └──────┬──────┘
                           │ push to main
                           ▼
             ┌─────────────────────────────┐
             │   Azure Container Apps       │
             │  ┌───────┐  ┌────────────┐  │
             │  │  API  │  │   Worker   │  │
             │  │ :8000 │  │  Celery×2  │  │
             │  └───┬───┘  └─────┬──────┘  │
             │      │            │          │
             │  ┌───┴────────────┴──────┐  │
             │  │  APScheduler (leader) │  │
             │  │  42 jobs, Redis lock  │  │
             │  └───────────────────────┘  │
             └──────┬──────────────────────┘
                    │
        ┌───────────┼───────────┐
        ▼           ▼           ▼
  ┌──────────┐ ┌─────────┐ ┌─────────┐
  │Azure SQL │ │ Redis 7  │ │ AWS SQS │
  │ (MSSQL)  │ │ 3 DBs    │ │ 4 queues│
  │ 130+ tbl │ │ 6380     │ │ + DLQ   │
  └──────────┘ └─────────┘ └─────────┘

  ┌──────────────────────┐
  │    Web (nginx)       │
  │    React SPA :3010   │
  │    91 pages          │
  └──────────────────────┘
```

### Docker Services (docker-compose.yml)

| Service | Image | Port | Purpose |
|---|---|---|---|
| `redis` | redis:7-alpine | 6380→6379 | Cache, rate-limit, circuit breaker, Celery broker/results, pub/sub |
| `api` | Python 3.12-slim + ODBC 18 | 8000 | FastAPI + APScheduler (uvicorn --reload) |
| `worker` | Same as API | — | Celery worker, concurrency=2 |
| `web` | Node 20 → nginx | 3010 | React SPA static serve |

---

## 3. Data Flow Architecture

### 3.1 Ingestion Pipeline

```
Amazon SP-API ──► SQS ──► event_backbone ──► acc_event_log
      │                        │
      ▼                        ▼
 APScheduler ──► order_pipeline ──► acc_order + acc_order_item
      │              │                    + acc_order_item_finance
      │              ▼
      │         step_sync_finances ──► acc_finance_transaction
      │              │
      │              ▼
      │         step_bridge_fees ──► fee_taxonomy ──► profit buckets
      │              │
      │              ▼
      │         step_calc_profit ──► acc_profitability
      │
      ├──► ads_sync ──► acc_ads_campaign + acc_ads_daily_stat
      ├──► pricing_state ──► acc_pricing_snapshot
      ├──► listing_state ──► acc_listing_state
      └──► sync_inventory ──► acc_fba_inventory_snapshot
```

### 3.2 Profitability Chain (Event-Driven)

```
ads.synced ─────┐
                ├──► dependency gate ──► profitability rollup
finance.synced ─┘         │                    │
                          │              ┌─────┴──────┐
                     05:45 cron          │  CM1/CM2/NP│
                     safety-net          │  per order  │
                                         └─────┬──────┘
                                               │
                                    ┌──────────┼──────────┐
                                    ▼          ▼          ▼
                              executive   strategy    alerts
                              dashboard   opportun.   anomaly
```

### 3.3 Financial Model

```
Revenue (order total)
  − COGS (purchase price × quantity)
  − Amazon Fees (commission, FBA, referral — via fee_taxonomy)
  − Fulfillment/Logistics (DHL/GLS actual or estimated)
  − Shipping Surcharges
  − Order-level Promotions
= CM1 (Contribution Margin 1)

CM1
  − Advertising (ACoS allocation)
  − Returns/Refund friction
  − Storage costs (FBA long-term + monthly)
  − Inventory handling
  − Operational costs
= CM2 (Contribution Margin 2)

CM2
  − SaaS (tools, subscriptions pro-rata)
  − Regulatory fees (EPR, LUCID, etc.)
  − Overhead allocation
= NP (Net Profit)
```

---

## 4. Technology Stack

### Backend
| Component | Technology | Version |
|---|---|---|
| Runtime | Python | 3.12 |
| Framework | FastAPI | latest |
| ORM | SQLAlchemy | 2.x (async) |
| Migrations | Alembic | latest |
| Task Queue | Celery + Redis | 5.x |
| Scheduler | APScheduler | 3.x (AsyncIO) |
| HTTP Client | httpx | async |
| Logging | structlog | JSON structured |
| Observability | OpenTelemetry + Sentry | OTLP + Azure Monitor |
| Auth | python-jose (JWT) + passlib (bcrypt) | HS256 |

### Frontend
| Component | Technology | Version |
|---|---|---|
| Framework | React | 18.3.1 |
| Language | TypeScript | 5.7.2 |
| Bundler | Vite | 6.0.3 |
| Styling | TailwindCSS + shadcn/ui | 3.4.16 |
| State | Zustand | 5.0.2 |
| Server State | TanStack React Query | 5.62.7 |
| Charts | Recharts | 2.14.1 |
| Icons | Lucide React | latest |
| HTTP | Axios | latest |
| UI Primitives | Radix UI | latest |

### Infrastructure
| Component | Technology |
|---|---|
| Database | Azure SQL (MSSQL) |
| Cache/Broker | Redis 7 (alpine) |
| Message Queue | AWS SQS |
| Container | Azure Container Apps |
| CI/CD | GitHub Actions |
| Monitoring | Sentry + Azure Monitor |
| Tracing | OpenTelemetry (OTLP gRPC) |

---

## 5. Module Inventory

### 5.1 Intelligence Layer (11 engines + profit subpackage)

| Module | LOC | Purpose |
|---|---|---|
| `profit/calculator.py` | 659 | CM1/CM2/NP computation core |
| `profit/query.py` | 4,689 | API data access, drilldowns, what-if |
| `profit/rollup.py` | 1,762 | SKU/marketplace aggregation |
| `profit/cost_model.py` | 1,605 | Cost model configuration, COGS |
| `profit/export.py` | 81 | XLSX export |
| `profit/helpers.py` | 178 | _f(), _i(), null coercion |
| `buybox_radar.py` | 1,082 | Competitor tracking, win-rate, alerts |
| `catalog_health.py` | 1,007 | Listing quality, suppression, diffs |
| `inventory_risk.py` | 1,536 | Stockout/overstock probability, DOI |
| `repricing_engine.py` | 1,498 | Dynamic pricing, margin guardrails |
| `content_optimization.py` | 1,087 | Content scoring, SEO analysis |
| `content_ab_testing.py` | 894 | Multi-language A/B experiments |
| `refund_anomaly.py` | 1,534 | Refund spikes, serial returners |
| `operator_console.py` | 778 | Unified feed, case management |
| `account_hub.py` | 658 | Multi-seller, credential vault |
| `event_wiring.py` | 874 | Event dependency gates |
| `sqs_topology.py` | 747 | Queue management, DLQ |

### 5.2 Platform Layer (22 modules)

| Module | Purpose |
|---|---|
| `action_center.py` | Unified Amazon write gateway, circuit breaker, audit trail |
| `job_dispatch.py` | Job type routing (80+ types) |
| `schema_registry.py` | Startup schema initialization |
| `otel.py` | OpenTelemetry configuration |
| `shared/db.py` | Shared DB helpers (_connect, _fetchall_dict) |
| `shared/cache.py` | Generic TTL in-memory cache |
| `scheduler/*.py` | 16 domain scheduler modules (orders, finance, inventory, ads, profit, content, logistics, strategy, seasonality, system, buybox_radar, catalog_health, inventory_risk, repricing, registry, base) |

### 5.3 Connectors (12 integrations)

| Connector | Protocol | Purpose |
|---|---|---|
| Amazon SP-API | REST (httpx async) | Orders, listings, pricing, reports, notifications, inventory |
| Amazon Ads API | REST (LWA OAuth2) | Campaigns, profiles, reporting |
| DHL24 | SOAP/XML (zeep) | Shipment tracking, labels, POD |
| GLS | REST + ADE | T&T, billing, cost center |
| MSSQL ERP (Netfox) | pyodbc/pymssql | Product master data, prices, orders |
| Redis | redis-py | Cache, broker, pub/sub, locks |
| AWS SQS | boto3 | Notification ingestion, DLQ |
| Ergonode PIM | REST (JWT) | Product attributes sync |
| BaseLinker | REST | Courier distribution |
| ECB | XML feed | EUR exchange rates |
| NBP | REST | PLN exchange rates |
| OpenAI | REST | GPT-5.2 content generation, recommendations |

---

## 6. Job Scheduling Architecture

### APScheduler (Primary — 42 jobs)

| Domain | Jobs | Schedule |
|---|---|---|
| Orders | sync_orders (per marketplace), process_sqs | Every 15min, continuous |
| Finance | sync_finances, bridge_fees | Daily 03:00 |
| Inventory | sync_inventory, aging_analysis | Daily 04:00 |
| Ads | sync_campaigns, sync_stats | Daily 02:00 |
| Profit | profitability_chain (event-driven + 05:45 safety net) | Event + cron |
| Content | content_tasks, publish_queue | Every 30min |
| Logistics | dhl_sync, gls_sync, courier_kpis | Daily 06:00 |
| Strategy | detect_opportunities, experiments | Daily 07:00 |
| Seasonality | cluster_recompute | Weekly Monday |
| System | guardrails, cleanup, health_check | Every 5min / daily |
| Intelligence | catalog_health, buybox_capture, inventory_risk, repricing | Various |

### Celery Worker (Secondary — 7 task modules)

- concurrency=2, Redis broker (DB 1), Redis results (DB 2)
- Tasks: order pipeline (15min), finance sync (daily 03:00), heavy computation offload

### Leader Election

- Redis-based distributed lock for APScheduler
- Only one API instance runs scheduler jobs
- Lock key: `apscheduler:leader`

---

## 7. Observability Stack

```
Application ──► structlog (JSON) ──► stdout ──► Azure Log Analytics
     │
     ├──► OpenTelemetry SDK
     │       ├──► OTLP gRPC Exporter ──► Collector
     │       └──► Azure Monitor Exporter ──► Application Insights
     │
     ├──► Sentry SDK ──► Sentry Cloud (errors + performance)
     │
     └──► /health endpoint ──► Azure Container Apps health probe
              ├── database: ping
              ├── redis: ping
              ├── sqs: queue attributes
              └── event_backbone: handler health
```

---

## 8. Codebase Metrics (as of 2026-03-12)

| Metric | Value |
|---|---|
| Python files (app/) | 308 |
| Python LOC (app/) | 128,991 |
| Test files | 61 |
| Test LOC | 23,190 |
| Tests passing | 1,765 / 1,765 (100%) |
| Alembic migrations | 40 |
| API routers | 48 |
| API endpoints | ~500+ |
| Frontend pages | 91 |
| Intelligence engines | 11 + profit subpackage |
| Platform modules | 22 |
| Database tables | ~130+ |
| Inline DDL functions | 39 (migration target) |
| Docker services | 4 |
| External integrations | 12 |
