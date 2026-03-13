# System Architecture Specification — Amazon Command Center (ACC)

| Field            | Value                                      |
|------------------|--------------------------------------------|
| **Date**         | 2026-03-13                                 |
| **Agent**        | Backend Architect                          |
| **Classification** | Internal — Confidential                  |
| **Version**      | 1.0                                        |
| **Status**       | Phase 1 — Baseline Architecture            |

---

## Table of Contents

1. [System Architecture Specification](#1-system-architecture-specification)
   1.1 [Architecture Pattern Analysis](#11-architecture-pattern-analysis)
   1.2 [C4 Diagrams](#12-c4-diagrams)
   1.3 [Module Dependency Map](#13-module-dependency-map)
   1.4 [Critical Data Flow Diagrams](#14-critical-data-flow-diagrams)
   1.5 [Deployment Architecture](#15-deployment-architecture)
   1.6 [Communication Patterns](#16-communication-patterns)
2. [Database Schema Design](#2-database-schema-design)
   2.1 [Current Schema Analysis](#21-current-schema-analysis)
   2.2 [Indexing Strategy](#22-indexing-strategy)
   2.3 [Materialized View Design](#23-materialized-view-design)
   2.4 [Data Partitioning Strategy](#24-data-partitioning-strategy)
   2.5 [Migration Patterns](#25-migration-patterns)
   2.6 [Connection Pooling](#26-connection-pooling)
3. [API Design Specification](#3-api-design-specification)
   3.1 [Endpoint Inventory](#31-endpoint-inventory)
   3.2 [RESTful Design Standards](#32-restful-design-standards)
   3.3 [Versioning Strategy](#33-versioning-strategy)
   3.4 [Pagination Specification](#34-pagination-specification)
   3.5 [Rate Limiting](#35-rate-limiting)
   3.6 [Error Response Standardization](#36-error-response-standardization)
   3.7 [WebSocket API](#37-websocket-api)
4. [Authentication & Authorization Architecture](#4-authentication--authorization-architecture)
   4.1 [Current Auth System](#41-current-auth-system)
   4.2 [JWT HS256 → RS256 Migration](#42-jwt-hs256--rs256-migration)
   4.3 [python-jose → pyjwt Migration](#43-python-jose--pyjwt-migration)
   4.4 [RBAC Enforcement](#44-rbac-enforcement)
   4.5 [Multi-Tenant Preparation](#45-multi-tenant-preparation)
   4.6 [API Key Authentication](#46-api-key-authentication)
   4.7 [Session & Token Strategy](#47-session--token-strategy)
   4.8 [Audit Logging](#48-audit-logging)
5. [Security Architecture](#5-security-architecture)
   5.1 [Defense-in-Depth Layers](#51-defense-in-depth-layers)
   5.2 [Secrets Management](#52-secrets-management)
   5.3 [Input Validation](#53-input-validation)
   5.4 [SQL Injection Prevention](#54-sql-injection-prevention)
   5.5 [CORS Hardening](#55-cors-hardening)
   5.6 [TLS & Encryption](#56-tls--encryption)
   5.7 [Dependency Scanning](#57-dependency-scanning)
   5.8 [Security Headers](#58-security-headers)
   5.9 [PII & GDPR](#59-pii--gdpr)
6. [Scalability Plan](#6-scalability-plan)
   6.1 [Horizontal Scaling Roadmap](#61-horizontal-scaling-roadmap)
   6.2 [Database Scaling](#62-database-scaling)
   6.3 [Caching Architecture](#63-caching-architecture)
   6.4 [Queue Architecture](#64-queue-architecture)
   6.5 [CDN & Static Assets](#65-cdn--static-assets)
   6.6 [Performance Monitoring & SLOs](#66-performance-monitoring--slos)
   6.7 [Load Testing Strategy](#67-load-testing-strategy)
   6.8 [Cost Projection](#68-cost-projection)

---

## 1. System Architecture Specification

### 1.1 Architecture Pattern Analysis

**Current State: Modular Monolith (single-process)**

ACC runs as a single FastAPI process (uvicorn) containing the API server, 42+ APScheduler jobs, WebSocket endpoints, and schema migration logic — all in one OS process. Redis provides scheduler leader-election and pub/sub for alerts. Azure SQL is the single source of truth.

**Architecture Decision Record — ADR-001: Keep Monolith (ref TD-04)**

| Attribute     | Value |
|---------------|-------|
| Status        | ACCEPTED |
| Context       | Solo founder, 67 services, 51 routers, single-tenant today |
| Decision      | Keep monolith for ≥12 months; extract Celery workers first |
| Consequences  | Simpler deployment, faster iteration; TR-19 risk managed by worker extraction |

**Evolution Roadmap:**

```
Phase 1 (Now → Month 6):  MODULAR MONOLITH
  - Single FastAPI process + separate Celery worker(s)
  - APScheduler for cron orchestration → Celery for heavy compute
  - Redis as broker + cache + pub/sub

Phase 2 (Month 6 → 12):  MONOLITH + WORKERS
  - Dedicated worker pools: db-heavy, courier-heavy, ads-sync
  - API horizontally scaled (2–4 replicas behind nginx)
  - Shared-nothing API instances (Redis session, SQL state)

Phase 3 (Month 12 → 24): BOUNDED CONTEXTS
  - Extract Profit Engine as independent service (if >50 clients)
  - Extract Ads Sync as independent pipeline
  - Event backbone (SQS) as inter-service bus
  - API gateway / BFF pattern for multi-tenant SaaS
```

### 1.2 C4 Diagrams

#### C4 Level 1 — System Context

```
                           ┌─────────────────┐
                           │  Seller / Ops    │
                           │  (Browser)       │
                           └────────┬─────────┘
                                    │ HTTPS
                           ┌────────▼─────────┐
                           │    ACC Platform   │
                           │  (FastAPI+React)  │
                           └──┬───┬───┬───┬───┘
                              │   │   │   │
            ┌─────────────────┘   │   │   └──────────────────┐
            ▼                     ▼   ▼                      ▼
   ┌─────────────────┐   ┌──────────────────┐   ┌──────────────────┐
   │ Amazon SP-API   │   │ Amazon Ads API   │   │ Netfox ERP       │
   │ (13 modules)    │   │ (v3, 10 profiles)│   │ (MSSQL, on-prem) │
   └─────────────────┘   └──────────────────┘   └──────────────────┘
            │                     │                      │
   ┌────────┘      ┌─────────────┘                      │
   ▼               ▼                                    ▼
  Orders        Campaigns                      COGS, Inventory,
  Finances      Daily Reports                  Courier Costs
  Inventory     Product Reports                (ITJK_* tables)
  Listings
  Reports                 ┌──────────────────┐
  Catalog                 │ External Services │
                          │ GLS / DHL SOAP   │
                          │ NBP / ECB (FX)   │
                          │ BaseLinker API    │
                          │ OpenAI GPT-5.2   │
                          │ AWS SQS          │
                          │ Google Sheets    │
                          └──────────────────┘
```

#### C4 Level 2 — Container Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│                        ACC Platform (Docker Compose)                 │
│                                                                      │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐   │
│  │   nginx (:3010)  │  │  FastAPI (:8000) │  │  Celery Worker   │   │
│  │                  │  │                  │  │  (dormant)       │   │
│  │  - SPA serve     │──│  - 51 routers    │  │                  │   │
│  │  - /api/ proxy   │  │  - 42 APSched    │  │  - Queues:       │   │
│  │  - /ws/ upgrade  │  │  - WebSocket     │  │    default,sync  │   │
│  └──────────────────┘  │  - Sentry/OTel   │  │    ai            │   │
│                        └────────┬─────────┘  └──────────────────┘   │
│                                 │                      │            │
│                    ┌────────────┼──────────────────────┘            │
│                    ▼            ▼                                    │
│            ┌──────────────┐  ┌──────────────┐                       │
│            │ Redis 7      │  │ Azure SQL S3 │                       │
│            │ (:6380)      │  │ 19.3 GB      │                       │
│            │              │  │ 187 tables   │                       │
│            │ - Sched lock │  │ 26.6M rows   │                       │
│            │ - Alert pub  │  └──────┬───────┘                       │
│            │ - Cache/sess │         │                               │
│            │ - Celery brk │         │ pyodbc (read-only)            │
│            └──────────────┘         ▼                               │
│                              ┌──────────────┐                       │
│                              │ Netfox ERP   │                       │
│                              │ (on-prem)    │                       │
│                              └──────────────┘                       │
└──────────────────────────────────────────────────────────────────────┘
```

#### C4 Level 3 — Component Diagram (API)

```
┌───────────────────────────────────────────────────────────────┐
│                    FastAPI Application                         │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  API Layer (51 routers, /api/v1/*)                     │   │
│  │                                                        │   │
│  │  CORE:     auth, profit_v2, kpi, health, jobs          │   │
│  │  CATALOG:  families, inventory, manage_inventory,      │   │
│  │            listing_state, catalog_health, backbone      │   │
│  │  FINANCE:  finance_center, profitability, executive    │   │
│  │  ADS:      ads, buybox_radar                           │   │
│  │  OPS:      fba_ops, returns, gls, dhl, courier         │   │
│  │  STRATEGY: strategy, outcomes, seasonality, repricing  │   │
│  │  CONTENT:  content_ops, content_optimization, ab_test  │   │
│  │  SYSTEM:   alerts, audit, guardrails, notifications,   │   │
│  │            operator_console, system, intelligence       │   │
│  └────────────┬───────────────────────────────────────────┘   │
│               │                                               │
│  ┌────────────▼───────────────────────────────────────────┐   │
│  │  Service Layer (67 services)                           │   │
│  │                                                        │   │
│  │  order_pipeline.py ─── 10-step pipeline, 30min cadence │   │
│  │  ads_sync.py ───────── profiles→campaigns→reports      │   │
│  │  sync_service.py ───── FX, inventory, finances         │   │
│  │  guardrails.py ─────── pipeline health checks          │   │
│  │  event_backbone.py ─── domain events, SQS integration  │   │
│  │  gls/dhl_integration ─ SOAP carrier APIs               │   │
│  │  courier_*.py (12) ─── logistics cost engine           │   │
│  │  pricing_*.py ──────── repricing + pricing state       │   │
│  │  taxonomy.py ───────── ML-based product categorization │   │
│  └────────────┬───────────────────────────────────────────┘   │
│               │                                               │
│  ┌────────────▼───────────────────────────────────────────┐   │
│  │  Intelligence Layer (12 modules)                       │   │
│  │                                                        │   │
│  │  profit/          ── CM1/CM2/NP engine (4 sub-modules) │   │
│  │  catalog_health   ── coverage + listing quality        │   │
│  │  buybox_radar     ── BuyBox win rate tracking          │   │
│  │  repricing_engine ── algorithmic repricing             │   │
│  │  inventory_risk   ── stockout / overstock prediction   │   │
│  │  refund_anomaly   ── refund pattern detection          │   │
│  │  content_*        ── A/B testing, optimization         │   │
│  │  sqs_topology     ── event routing topology            │   │
│  │  operator_console ── operational dashboard backend     │   │
│  └────────────┬───────────────────────────────────────────┘   │
│               │                                               │
│  ┌────────────▼───────────────────────────────────────────┐   │
│  │  Connectors (8 packages)                               │   │
│  │                                                        │   │
│  │  amazon_sp_api/ (13): orders, finances, inventory,     │   │
│  │    listings, reports, catalog, feeds, brand_analytics,  │   │
│  │    pricing_api, notifications, inbound, client          │   │
│  │  amazon_ads_api/ (4): client, profiles, campaigns,     │   │
│  │    reporting                                            │   │
│  │  mssql/: schema DDL, raw SQL operations                │   │
│  │  gls_api/: SOAP client (zeep)                          │   │
│  │  dhl24_api/: SOAP client (zeep)                        │   │
│  │  ecb.py, nbp.py: FX rate providers                    │   │
│  │  ergonode.py: PIM integration                          │   │
│  └────────────────────────────────────────────────────────┘   │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  Core (infrastructure)                                 │   │
│  │                                                        │   │
│  │  config.py ──── pydantic-settings, dual DB config      │   │
│  │  db_connection ─ connect_acc() / connect_netfox()      │   │
│  │  security.py ── JWT HS256 + RBAC (5 roles)             │   │
│  │  redis_client ─ async Redis pool                       │   │
│  │  scheduler_lock ─ Redis SET NX EX leader election      │   │
│  │  fx_service ── FX rate SQL fragment builder             │   │
│  │  logging_config ─ structlog setup                      │   │
│  └────────────────────────────────────────────────────────┘   │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐   │
│  │  Platform (scheduler orchestration)                    │   │
│  │                                                        │   │
│  │  scheduler/base.py ──── job record creation            │   │
│  │  scheduler/orders.py ── 3 jobs (pipeline, listings)    │   │
│  │  scheduler/finance.py ─ 5 jobs (COGS, FX, fees)       │   │
│  │  scheduler/inventory ── 7 jobs (FBA, returns, sales)   │   │
│  │  scheduler/ads.py ──── 1 job  (4h cycle)               │   │
│  │  scheduler/profit.py ─ 4 jobs (TKL, calc, audit)      │   │
│  │  scheduler/logistics ── 5 jobs (GLS/DHL/estimation)    │   │
│  │  scheduler/content.py ─ 3 jobs (PTD, pricing state)   │   │
│  │  scheduler/strategy ── 4 jobs (DI, search terms)       │   │
│  │  scheduler/seasonality ─ 3 jobs (build, recompute)    │   │
│  │  scheduler/system.py ── 7 jobs (retries, guardails)   │   │
│  └────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────┘
```

### 1.3 Module Dependency Map

The 67 services form a directed acyclic dependency graph. High-fan-in nodes are critical:

```
High Fan-In (depended on by many):
  db_connection.py ─────────── ALL services (67 consumers)
  config.py ────────────────── ALL modules
  profit_engine.py (facade) ── 12 consumers (endpoints, scheduler, controlling)
  order_pipeline.py ────────── scheduler/orders, guardrails, event_backbone
  sync_service.py ──────────── scheduler/finance, scheduler/inventory
  event_backbone.py ────────── 8+ emitters (ads, orders, listings, pricing)

High Fan-Out (depends on many):
  order_pipeline.py ─── SP-API orders, db_connection, listing_registry,
                         sync_service, courier_cost, event_backbone
  profit/query.py ───── helpers, cost_model, calculator, order_logistics_source
  ads_sync.py ───────── ads_api (profiles, campaigns, reporting),
                         db_connection, fx rates
  guardrails.py ─────── order_pipeline, finance_center, ads_sync, inventory

Critical Path Modules (outage = revenue impact):
  1. order_pipeline.py    → all downstream analytics depend on fresh orders
  2. profit/query.py      → PPT dashboard (primary user-facing view)
  3. ads_sync.py          → CM2 accuracy (ads cost attribution)
  4. sync_service.py      → COGS, FX rates, inventory freshness
```

### 1.4 Critical Data Flow Diagrams

#### Flow 1: Order Pipeline (every 30 min)

```
SP-API (9 marketplaces)
    │
    │  GET /orders (LastUpdatedAfter=30min)
    ▼
┌─────────────────────────────────────────────────────┐
│  Step 1: step_sync_orders()                         │
│  - Fetch orders + items per marketplace             │
│  - MERGE into acc_order + acc_order_line             │
│  - Commit every 25 orders (safety)                  │
│  - sync_payload_hash dedup                          │
│                                                     │
│  Step 2: step_backfill_products()                   │
│  - INSERT acc_product for new SKU/ASIN combos       │
│                                                     │
│  Step 3: step_link_order_lines()                    │
│  - UPDATE acc_order_line SET product_id              │
│                                                     │
│  Step 4: step_map_products() — 5-step cascade:     │
│    Ergonode → GSheet → BaseLinker → ASIN → AI       │
│                                                     │
│  Step 5: step_stamp_purchase_prices()               │
│  - 2-pass: acc_product cache → CROSS APPLY 8-level │
│  - Holding/erp_holding ×1.04 multiplier             │
│                                                     │
│  Step 5.8: sync_exchange_rates() (NBP/ECB)          │
│  Step 5.8b: step_sync_finances() (SP-API)          │
│  Step 5.9: step_bridge_fees() → fba/referral cols  │
│  Step 5.95: step_sync_courier_costs() (GLS/DHL)    │
│                                                     │
│  Step 6: step_calc_profit() → CM1 snapshot          │
└─────────────────────────────────────────────────────┘
    │
    │  emit_domain_event("orders", "synced")
    ▼
  Event Backbone → downstream triggers
```

#### Flow 2: Profit Calculation (PPT query)

```
HTTP GET /api/v1/profit-v2/products
    │   ?date_from, date_to, marketplace_id, group_by, page, page_size
    ▼
┌────────────────────────────────────────────────────────────────┐
│  get_product_profit_table()                                    │
│                                                                │
│  1. Check in-memory cache (3min TTL, 50 keys max)             │
│                                                                │
│  2. Build main CTE aggregation query:                         │
│     ┌──────────────────────────────────────────────────────┐  │
│     │  WITH order_scope AS (                               │  │
│     │    SELECT DISTINCT amazon_order_id, marketplace_id   │  │
│     │    FROM acc_order WITH (NOLOCK) WHERE status='Shipped'│  │
│     │  ),                                                  │  │
│     │  shipping_per_order AS (                             │  │
│     │    ...acc_finance_transaction shipping charges...     │  │
│     │  )                                                   │  │
│     │  SELECT group_key, revenue_pln, cogs_pln,            │  │
│     │    fba_fee_pln, referral_fee_pln, logistics_pln,     │  │
│     │    amazon_fees_pln, shipping_charge_pln, ...         │  │
│     │  FROM acc_order o WITH (NOLOCK)                      │  │
│     │  JOIN acc_order_line ol WITH (NOLOCK)                 │  │
│     │  LEFT JOIN acc_product p WITH (NOLOCK)                │  │
│     │  LEFT JOIN acc_amazon_listing_registry reg            │  │
│     │  OUTER APPLY (FX rates) fx                           │  │
│     │  LEFT JOIN acc_order_logistics_fact olf               │  │
│     │  LEFT JOIN shipping_per_order spo                    │  │
│     │  GROUP BY group_expr                                 │  │
│     └──────────────────────────────────────────────────────┘  │
│                                                                │
│  3. Post-processing (Python):                                 │
│     - CM1 = revenue + shipping - cogs - amazon_fees - logistics│
│     - CM2 = CM1 - ads_cost - storage - returns (pool alloc)  │
│     - NP  = CM2 - overhead (pool allocation)                  │
│     - Page/sort, confidence scores, data quality flags        │
│                                                                │
│  4. Cache result (TTL=180s) → return JSON response            │
└────────────────────────────────────────────────────────────────┘
```

#### Flow 3: Ads Attribution Pipeline

```
Amazon Ads API v3
    │
    │  Every 4 hours (IntervalTrigger)
    ▼
┌────────────────────────────────────────────────────────────────┐
│  run_full_ads_sync(days_back=3)                               │
│                                                                │
│  1. list_profiles() → 10 profiles (9 EU + 1 UK)              │
│     MERGE → acc_ads_profile                                   │
│                                                                │
│  2. For each profile:                                         │
│     list_all_campaigns(SP + SB + SD)                          │
│     → 5,083 campaigns MERGE → acc_ads_campaign                │
│                                                                │
│  3. For each profile × each day (3d window):                  │
│     request_sp/sb/sd_campaign_report()                        │
│     → CampaignDayMetrics × FX rate lookup                     │
│     → MERGE acc_ads_campaign_day (with spend_pln, sales_pln)  │
│                                                                │
│  4. Product-level reports:                                    │
│     request_sp/sb/sd_product_report()                         │
│     → ProductDayMetrics → batch MERGE acc_ads_product_day     │
│     (temp table + multi-row INSERT + MERGE, 5K/batch)         │
│                                                                │
│  5. emit_domain_event("ads", "synced")                        │
└────────────────────────────────────────────────────────────────┘
    │
    │  Downstream: profit CM2 calculation uses ads spend per ASIN
    ▼
  acc_ads_product_day.spend_pln → profit query LEFT JOIN
```

### 1.5 Deployment Architecture

**Current (Docker Compose — single host):**

```
┌─────────────────────────────────────────┐
│  Docker Host (dev / staging)            │
│                                         │
│  ┌─────────────┐  ┌─────────────┐      │
│  │ nginx:3010  │──│ api:8000    │      │
│  │ (SPA+proxy) │  │ (uvicorn    │      │
│  └─────────────┘  │  --reload)  │      │
│                    └──────┬──────┘      │
│  ┌─────────────┐         │              │
│  │ redis:6380  │◄────────┘              │
│  └─────────────┘                        │
│  ┌─────────────┐                        │
│  │ worker      │  (dormant — ENABLED=F) │
│  └─────────────┘                        │
└───────────────────────┬─────────────────┘
                        │
        ┌───────────────┼───────────────┐
        ▼               ▼               ▼
   Azure SQL S3    Netfox ERP     External APIs
   (cloud)         (on-prem)      (Amazon, GLS...)
```

**Target (Phase 2 — multi-instance):**

```
┌──────────────────────────────────────────────────────┐
│  Docker Swarm / ACI                                  │
│                                                      │
│  ┌──────────┐   ┌─────────┐  ┌─────────┐           │
│  │ nginx    │──▶│ api ×2  │  │ api ×2  │           │
│  │ (LB)    │   │ (no     │  │ (no     │           │
│  └──────────┘   │  sched) │  │  sched) │           │
│                 └────┬────┘  └────┬────┘           │
│                      │            │                 │
│  ┌───────────────────▼────────────▼──────────────┐  │
│  │  Redis 7 (leader lock + Celery broker)        │  │
│  └──────────────────┬────────────────────────────┘  │
│                     │                               │
│  ┌──────────────────▼──────────────────────────┐    │
│  │  Celery Workers                             │    │
│  │  ┌───────────┐ ┌───────────┐ ┌───────────┐ │    │
│  │  │ db-heavy  │ │ courier-  │ │ light-    │ │    │
│  │  │ (×3 max)  │ │ heavy(×1) │ │ default   │ │    │
│  │  └───────────┘ └───────────┘ └───────────┘ │    │
│  └─────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────┘
```

### 1.6 Communication Patterns

| Pattern | Implementation | Usage |
|---------|---------------|-------|
| **Sync HTTP** | FastAPI → httpx/zeep | SP-API, Ads API, GLS/DHL SOAP, BaseLinker |
| **Async Queue** | APScheduler + (future) Celery | 42 scheduled jobs, background processing |
| **WebSocket** | FastAPI WS `/ws/jobs/{id}`, `/ws/alerts` | Job progress push (2s poll), alert broadcast |
| **Redis Pub/Sub** | `acc:alerts` channel | Real-time alert notifications to connected clients |
| **Redis Lock** | `SET NX EX` + Lua scripts | Scheduler leader election (60s TTL, 20s renew) |
| **Domain Events** | `event_backbone.py` → SQL + SQS | Cross-domain triggers (ads→profit, orders→listings) |
| **MSSQL Polling** | WS job endpoint polls `acc_al_jobs` | Job progress updates (every 2 seconds) |

---

## 2. Database Schema Design

### 2.1 Current Schema Analysis

**Azure SQL Server Standard S3** — 19.3 GB, 187 tables, 26.6M rows.

**Core Business Tables (ordered by importance):**

| Table | Est. Rows | Growth | Purpose |
|-------|-----------|--------|---------|
| `acc_order` | ~850K | ~15K/mo | Order headers (all 9 EU marketplaces) |
| `acc_order_line` | ~1.1M | ~20K/mo | Order line items (SKU, price, fees) |
| `acc_finance_transaction` | ~8M | ~300K/mo | SP-API financial events |
| `acc_product` | ~4.3K groups | ~50/mo | Product catalog (internal_sku mapped) |
| `acc_purchase_price` | ~12K | ~200/mo | COGS history (8-level priority) |
| `acc_exchange_rate` | ~3K | ~30/mo | FX rates (NBP/ECB) |
| `acc_ads_campaign_day` | ~800K | ~50K/mo | Ads daily performance |
| `acc_ads_product_day` | ~2M | ~200K/mo | Ads per-ASIN daily performance |
| `acc_ads_campaign` | ~5K | ~100/mo | Campaign metadata (SP/SB/SD) |
| `acc_ads_profile` | ~10 | static | Marketplace ↔ Ads profile mapping |
| `acc_order_logistics_fact` | ~158K | ~5K/mo | Per-order logistics costs |
| `acc_shipment_cost` | ~265K | ~10K/mo | Actual carrier billing costs |
| `acc_amazon_listing_registry` | ~8K | ~100/mo | SKU→internal_sku mapping |
| `acc_al_jobs` | ~50K | ~2K/mo | Job run history (scheduler audit) |
| `acc_al_alerts` | ~2K | ~200/mo | Alert instances |

**Netfox ERP (read-only):**

| Table | Purpose |
|-------|---------|
| `ITJK_ZamowieniaBaselinkerAPI` | Amazon order → BaseLinker tracking link |
| `ITJK_CouriersInvoicesDetails` | GLS billing line items |
| `ITJK_DHL_Costs` | DHL shipping costs |
| `tw_*` | Product master data (Holding, Subiekt) |
| `dok_*` | Document/invoice data |

### 2.2 Indexing Strategy

**Current Pain Points (ref TR-20):**

The PPT query joins 6+ tables with complex CTEs. Without proper indexing, the main aggregation takes 14.5s on cold cache. Target: <2s.

**Recommended Index Additions:**

```sql
-- acc_order: the most-joined table
-- Existing PK: id (IDENTITY)
-- Critical composite for profit queries:
CREATE NONCLUSTERED INDEX IX_acc_order_profit_core
ON dbo.acc_order (status, purchase_date, marketplace_id)
INCLUDE (amazon_order_id, currency, fulfillment_channel, is_refund,
         refund_amount_pln, shipping_surcharge_pln, promo_order_fee_pln,
         refund_commission_pln)
WHERE status = 'Shipped';

-- acc_order_line: the heaviest table in profit queries
CREATE NONCLUSTERED INDEX IX_acc_order_line_profit
ON dbo.acc_order_line (order_id)
INCLUDE (sku, asin, quantity_ordered, item_price, item_tax,
         promotion_discount, cogs_pln, fba_fee_pln, referral_fee_pln,
         product_id, title);

-- acc_finance_transaction: shipping CTE + CM2 pools
CREATE NONCLUSTERED INDEX IX_acc_finance_tx_shipping
ON dbo.acc_finance_transaction (amazon_order_id, charge_type)
INCLUDE (amount_pln, amount, currency, marketplace_id, posted_date)
WHERE charge_type IN ('ShippingCharge', 'ShippingTax', 'ShippingDiscount');

CREATE NONCLUSTERED INDEX IX_acc_finance_tx_cm2
ON dbo.acc_finance_transaction (posted_date, marketplace_id)
INCLUDE (charge_type, transaction_type, amount_pln);

-- acc_ads_product_day: ads cost per ASIN
CREATE NONCLUSTERED INDEX IX_acc_ads_pd_profit
ON dbo.acc_ads_product_day (asin, report_date, marketplace_id)
INCLUDE (spend_pln);

-- acc_exchange_rate: FX lookup
CREATE NONCLUSTERED INDEX IX_acc_fx_lookup
ON dbo.acc_exchange_rate (currency, rate_date DESC)
INCLUDE (rate_to_pln);

-- acc_order_logistics_fact: logistics cost per order
CREATE NONCLUSTERED INDEX IX_logistics_fact_lookup
ON dbo.acc_order_logistics_fact (amazon_order_id)
INCLUDE (logistics_cost_pln, calculated_at, calc_version);
```

**NOLOCK Strategy:**

All read queries use `WITH (NOLOCK)` hints. This is safe for analytics (dirty reads are acceptable for approximate real-time dashboards). Write paths use `SET LOCK_TIMEOUT 30000` to prevent blocking.

> **ADR-002**: Keep NOLOCK for all analytics reads. The 0.01% risk of phantom/dirty reads is acceptable given the 15-30 minute data freshness window. Switch to `READ COMMITTED SNAPSHOT` isolation only if Azure SQL cost model allows (no extra charge on S3+).

### 2.3 Materialized View Design

Azure SQL does not support native materialized views. Alternatives:

**Strategy: Pre-computed Summary Tables + Incremental Refresh**

```sql
-- Table 1: Daily ASIN-marketplace profit snapshot
-- Refreshed by scheduler/profit.py after each profit calc
CREATE TABLE dbo.acc_profit_daily_snapshot (
    snapshot_date     DATE           NOT NULL,
    group_key         NVARCHAR(200)  NOT NULL,  -- ASIN or parent
    marketplace_id    NVARCHAR(30)   NOT NULL,
    entity_type       NVARCHAR(10)   NOT NULL,  -- asin|parent|sku
    sample_sku        NVARCHAR(100),
    asin              NVARCHAR(20),
    parent_asin       NVARCHAR(20),
    title             NVARCHAR(500),
    brand             NVARCHAR(200),
    fulfillment       NVARCHAR(10),
    units             INT            NOT NULL DEFAULT 0,
    order_count       INT            NOT NULL DEFAULT 0,
    revenue_pln       DECIMAL(18,4)  NOT NULL DEFAULT 0,
    cogs_pln          DECIMAL(18,4)  NOT NULL DEFAULT 0,
    amazon_fees_pln   DECIMAL(18,4)  NOT NULL DEFAULT 0,
    logistics_pln     DECIMAL(18,4)  NOT NULL DEFAULT 0,
    ads_spend_pln     DECIMAL(18,4)  NOT NULL DEFAULT 0,
    cm1_pln           DECIMAL(18,4)  NOT NULL DEFAULT 0,
    cm2_pln           DECIMAL(18,4)  NOT NULL DEFAULT 0,
    np_pln            DECIMAL(18,4)  NOT NULL DEFAULT 0,
    cm1_margin_pct    DECIMAL(8,4),
    confidence_score  DECIMAL(5,4),
    computed_at       DATETIME2      NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT PK_profit_daily PRIMARY KEY (snapshot_date, group_key, marketplace_id)
);

-- Index for PPT dashboard queries
CREATE NONCLUSTERED INDEX IX_profit_snap_date
ON dbo.acc_profit_daily_snapshot (snapshot_date DESC, marketplace_id)
INCLUDE (cm1_pln, cm1_margin_pct, revenue_pln, units);
```

**Refresh Strategy:**

1. Nightly: Full rebuild for T-1 (previous day) via `step_calc_profit()`
2. On-demand: When PPT query detects no snapshot for requested range, fall through to live CTE query
3. Cache layer: In-memory Python cache (3min TTL) sits in front of both snapshot and live query

**Expected Impact:** PPT load time: 14.5s → <1.5s for snapshot hits, <4s for live query with proper indexes.

### 2.4 Data Partitioning Strategy

**Growth Projection:** ~5 GB/year at current volume. Azure SQL S3 supports up to 250 GB.

At current growth rate, partitioning is not required for 3+ years. When needed:

```
Year 1–2: No partitioning needed (19.3 GB → ~30 GB)
Year 3:   Consider partitioning acc_finance_transaction by posted_date
          (largest table, ~8M rows → ~20M rows by Year 3)
Year 4+:  Partition acc_order + acc_order_line by purchase_date (quarterly)
```

**Archive Strategy:**

```sql
-- Archive orders older than 24 months to cold storage
-- Phase 1: Create archive tables (same schema)
-- Phase 2: Nightly job moves old data
-- Phase 3: Archive to Azure Blob (Parquet) for compliance
```

**ADR-003**: Defer partitioning. Azure SQL S3 at 250 GB max with 5 GB/year growth gives 40+ years of runway. Focus on indexing and materialized views first.

### 2.5 Migration Patterns

**Current State:** Schema changes are applied via `ensure_*_schema()` functions at startup (13 ensure functions in `main.py` lifespan). This is ad-hoc and not version-tracked.

**Target: Alembic Integration**

```
apps/api/
  alembic/
    env.py          ── SQLAlchemy engine from config.py
    versions/
      001_baseline.py
      002_add_profit_snapshot.py
      ...
  alembic.ini
```

**Migration Rules:**

1. All DDL changes go through Alembic (no more inline `IF OBJECT_ID` patterns)
2. Existing `ensure_*_schema()` functions become the **baseline** migration
3. New migrations are additive-only for 12 months (no destructive changes)
4. `alembic upgrade head` runs before app startup in Docker entrypoint
5. Rollback scripts required for every migration

**ADR-004**: Introduce Alembic incrementally. Convert one module per sprint. Keep ensure functions as fallback during transition (idempotent by design).

### 2.6 Connection Pooling

**Current State:**

- `connect_acc()` creates a new pymssql/pyodbc connection per call
- `connect_netfox()` creates a new pyodbc connection per call
- No connection pooling — each request/job opens and closes its own connection
- pymssql does not support async I/O

**Recommendation (Phase 1 — synchronous pool):**

```python
# Use SQLAlchemy's connection pool with raw pyodbc connections
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

engine = create_engine(
    settings.DATABASE_URL_SYNC,
    pool_size=10,          # base connections
    max_overflow=20,       # burst connections
    pool_timeout=30,       # wait for connection
    pool_recycle=1800,     # recycle every 30min
    pool_pre_ping=True,    # verify before checkout
)
```

**Phase 2 — async pool (when Celery workers offload blocking SQL):**

```python
# aioodbc for async queries in API process
# SQLAlchemy async engine with aioodbc dialect
async_engine = create_async_engine(
    settings.DATABASE_URL,   # mssql+aioodbc://...
    pool_size=5,
    max_overflow=10,
)
```

**ADR-005**: Introduce sync connection pooling via SQLAlchemy QueuePool in Phase 1. Layer it behind `connect_acc()` so all 67 services benefit without code changes. Defer async pool to Phase 2 when Celery workers handle the blocking SQL workload.

---

## 3. API Design Specification

### 3.1 Endpoint Inventory

The API currently serves **51 routers** across these functional domains:

| Domain | Routers | Priority | Notes |
|--------|---------|----------|-------|
| **Core** | auth, health, profit, profit_v2, kpi, jobs | P0 | Revenue-critical |
| **Catalog** | families, manage_inventory, listing_state, inventory_taxonomy, catalog_definitions, backbone, catalog_health | P1 | Product management |
| **Finance** | finance_center, profitability, executive | P1 | Financial analytics |
| **Ads** | ads, buybox_radar | P1 | Advertising management |
| **Ops** | fba_ops, returns, gls, dhl, courier, inventory_routes | P1 | Operations |
| **Strategy** | strategy, outcomes, seasonality, repricing, pricing, pricing_state | P2 | Decision support |
| **Content** | content_ops, content_optimization, content_ab_testing | P2 | Listing content |
| **System** | alerts, audit, guardrails, notifications, operator_console, system, intelligence, sqs_topology, event_wiring, account_hub | P2 | Platform |
| **Import** | import_products, refund_anomaly, inventory_risk | P2 | Data ingestion |
| **AI** | ai_rec | P3 | AI recommendations |

### 3.2 RESTful Design Standards

**v2 Endpoint Conventions:**

```
GET    /api/v2/{resource}              → List (paginated)
GET    /api/v2/{resource}/{id}         → Get single
POST   /api/v2/{resource}              → Create
PUT    /api/v2/{resource}/{id}         → Full update
PATCH  /api/v2/{resource}/{id}         → Partial update
DELETE /api/v2/{resource}/{id}         → Soft-delete

GET    /api/v2/{resource}/{id}/{sub}   → Sub-resource list
POST   /api/v2/{resource}/{id}/actions/{action}  → Custom action
```

**Pydantic v2 Schemas (standard envelope):**

```python
from pydantic import BaseModel, Field
from datetime import datetime

class PaginatedResponse[T](BaseModel):
    data: list[T]
    meta: PaginationMeta
    warnings: list[str] = []

class PaginationMeta(BaseModel):
    page: int
    page_size: int
    total_count: int
    total_pages: int
    has_next: bool
    has_previous: bool

class ErrorResponse(BaseModel):
    error: str
    code: str          # machine-readable: USER_NOT_FOUND, RATE_LIMITED
    detail: str | None = None
    request_id: str
    timestamp: datetime
```

### 3.3 Versioning Strategy

**Current:** All endpoints under `/api/v1/`. No v2 exists yet except `profit_v2`.

**Transition Plan:**

```
Phase 1 (Now):
  /api/v1/*          ── existing endpoints (maintained)
  /api/v1/profit-v2/ ── already migrated profit endpoints

Phase 2 (Month 3-6):
  /api/v2/products/profit  ── new PPT with pagination + snapshot
  /api/v2/ads/performance  ── unified ads analytics
  /api/v1/*                ── deprecated header: Sunset: <date>

Phase 3 (Month 9-12):
  /api/v2/*                ── full v2 surface
  /api/v1/*                ── read-only, no new features
  /api/v1/ removal         ── after 6-month deprecation notice
```

**ADR-006**: URL-based versioning (`/v1/`, `/v2/`). Header-based versioning rejected — too complex for a solo developer. Use `Deprecation` and `Sunset` headers on v1 endpoints.

### 3.4 Pagination Specification

**Current Problem (ref TR-20):** PPT returns all rows in a single response — 14.5s load for 4,300 product groups.

**Standard Pagination (OFFSET/FETCH):**

```sql
-- Server-side SQL pagination for all table views
SELECT ...
FROM (
    <aggregation CTE>
) AS result
ORDER BY {sort_column} {sort_dir}
OFFSET @offset ROWS
FETCH NEXT @page_size ROWS ONLY;
```

**API Contract:**

```
GET /api/v2/profit/products?page=1&page_size=50&sort_by=cm1_profit&sort_dir=desc

Response:
{
  "data": [...],
  "meta": {
    "page": 1,
    "page_size": 50,
    "total_count": 4287,
    "total_pages": 86,
    "has_next": true,
    "has_previous": false
  }
}
```

**Note:** `get_product_profit_table()` already supports `page` and `page_size` parameters. The fix is to ensure frontend uses them (default `page_size=50` instead of loading all).

### 3.5 Rate Limiting

**Implementation: slowapi (ref TD-15)**

```python
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)

# Auth endpoints: aggressive limits
@router.post("/auth/token")
@limiter.limit("5/minute")
async def login(request: Request, ...): ...

@router.post("/auth/register")
@limiter.limit("3/minute")
async def register(request: Request, ...): ...

# API endpoints: standard limits
@router.get("/profit/products")
@limiter.limit("30/minute")
async def get_products(request: Request, ...): ...

# Heavy computation: conservative limits
@router.post("/profit/export")
@limiter.limit("5/minute")
async def export_xlsx(request: Request, ...): ...
```

**Rate Limit Tiers (per IP, per user in SaaS):**

| Tier | Endpoints | Limit |
|------|-----------|-------|
| Auth | `/auth/*` | 5/min, 20/hour |
| Heavy | `/profit/export`, `/ai/*` | 5/min |
| Standard | All other GET | 60/min |
| Write | POST/PUT/DELETE | 30/min |

### 3.6 Error Response Standardization

```python
# Consistent error envelope
{
    "error": "Validation Error",
    "code": "VALIDATION_FAILED",
    "detail": "Field 'date_from' must be before 'date_to'",
    "request_id": "req_abc123",
    "timestamp": "2026-03-13T10:30:00Z"
}

# HTTP status code mapping:
# 400 → VALIDATION_FAILED, INVALID_PARAMETER
# 401 → AUTHENTICATION_REQUIRED, TOKEN_EXPIRED
# 403 → INSUFFICIENT_ROLE, TENANT_MISMATCH
# 404 → RESOURCE_NOT_FOUND
# 409 → CONFLICT, DUPLICATE_ENTRY
# 422 → UNPROCESSABLE_ENTITY (Pydantic validation)
# 429 → RATE_LIMITED
# 500 → INTERNAL_ERROR (+ Sentry event ID)
# 503 → SERVICE_UNAVAILABLE (DB down, external API down)
```

### 3.7 WebSocket API

**Current Implementation:**

| Endpoint | Purpose | Pattern |
|----------|---------|---------|
| `WS /ws/jobs/{job_id}` | Job progress streaming | DB poll (2s interval), closes on terminal state |
| `WS /ws/alerts` | Alert push notifications | Redis pub/sub (`acc:alerts` channel) |

**Future Additions:**

| Endpoint | Purpose | Phase |
|----------|---------|-------|
| `WS /ws/profit/live` | Live profit updates during recalc | Phase 2 |
| `WS /ws/orders/feed` | Real-time order feed | Phase 2 |
| `WS /ws/system/health` | System health dashboard stream | Phase 2 |

---

## 4. Authentication & Authorization Architecture

### 4.1 Current Auth System

- **JWT HS256** via python-jose (STALE — last release 2021)
- **bcrypt** via passlib (STALE) — now using `bcrypt` directly
- 5-role hierarchy: `ANALYST → OPS → CATEGORY_MGR → DIRECTOR → ADMIN`
- `require_role()` FastAPI dependency — checks hierarchy index
- Access tokens: 8 hours (`ACCESS_TOKEN_EXPIRE_MINUTES=480`)
- Refresh tokens: 30 days (`REFRESH_TOKEN_EXPIRE_DAYS=30`)
- Token claims: `sub` (user_id), `role`, `type`, `exp`, `allowed_marketplaces`, `allowed_brands`

### 4.2 JWT HS256 → RS256 Migration

**Rationale:** HS256 requires sharing the secret key across all services. RS256 uses asymmetric keys — only the auth service needs the private key; all others verify with the public key.

**Migration Plan:**

```
Phase 1 (Now): Keep HS256 — single service, no key distribution issue
Phase 2 (Multi-instance): Migrate to RS256
  - Generate RSA-2048 key pair
  - Private key: auth service only (Azure Key Vault)
  - Public key: distributed to all API instances
  - Transition: Accept both HS256 and RS256 for 30 days
  - Then: Reject HS256 tokens (force re-login)
Phase 3 (SaaS): JWKS endpoint for key rotation
  - GET /.well-known/jwks.json
  - Automatic key rotation every 90 days
```

**ADR-007**: Defer RS256 migration to Phase 2 (multi-instance). HS256 is secure when the secret is not shared. Priority: replace python-jose first (ref TD-12).

### 4.3 python-jose → pyjwt Migration (ref TD-12)

**Current Risk:** python-jose last released June 2021. No security patches.

**Migration Steps:**

```python
# BEFORE (python-jose):
from jose import JWTError, jwt
jwt.encode(payload, key, algorithm="HS256")
jwt.decode(token, key, algorithms=["HS256"])

# AFTER (pyjwt):
import jwt
from jwt.exceptions import InvalidTokenError
jwt.encode(payload, key, algorithm="HS256")
jwt.decode(token, key, algorithms=["HS256"])
```

**Impact Assessment:**
- `security.py`: 6 call sites (encode, decode)
- Error class: `JWTError` → `InvalidTokenError`
- API: Nearly identical — drop-in replacement
- **Estimated effort: 1 hour, 0 risk**

### 4.4 RBAC Enforcement

**Current Hierarchy (index-based):**

```
ANALYST (0) → OPS (1) → CATEGORY_MGR (2) → DIRECTOR (3) → ADMIN (4)
                                   │
                                   ├── allowed_marketplaces: [...]
                                   └── allowed_brands: [...]
```

**Enforcement Patterns:**

```python
# Endpoint-level (FastAPI dependency):
@router.get("/profit/products", dependencies=[Depends(require_analyst)])
async def get_products(...): ...

# Fine-grained (in-handler):
@router.patch("/products/{id}/price")
async def update_price(
    product_id: str,
    user: dict = Depends(require_ops)
):
    # CATEGORY_MGR can only modify own brands
    if user["role"] == "category_mgr":
        if product.brand not in user["allowed_brands"]:
            raise HTTPException(403, "Brand not in your scope")
```

**Multi-Tenant RBAC Extension (Phase 2):**

```
tenant_id (UUID) ── added to JWT claims
  └── user_id
       └── role
            └── allowed_marketplaces
                 └── allowed_brands
```

### 4.5 Multi-Tenant Preparation

**Current:** Single-tenant (one seller account, one Azure SQL database).

**SaaS Readiness Roadmap:**

```
Phase 1 (Now):     Single tenant, single DB
Phase 2 (Month 6): Multi-user, single tenant
  - User table (acc_user) with email/password/role
  - Invite flow (ADMIN creates users)
  - All data still shared (same seller account)
Phase 3 (Month 12): Multi-tenant, shared DB
  - tenant_id column on all acc_* tables
  - Row-level security (SQL Server RLS)
  - Tenant context middleware (extract from JWT)
  - Connection-level SET CONTEXT_INFO for RLS policies
Phase 4 (Month 18+): Multi-tenant, isolated DB (premium)
  - Dedicated Azure SQL per large tenant
  - Shared DB for small tenants (RLS)
  - Tenant router middleware selects connection
```

### 4.6 API Key Authentication

For programmatic access (data exports, CI/CD, external tools):

```python
# API Key model:
class APIKey(BaseModel):
    key_id: str          # acc_ak_{random}
    key_hash: str        # bcrypt hash of the key
    user_id: UUID
    tenant_id: UUID
    scopes: list[str]    # ["profit:read", "ads:read"]
    expires_at: datetime
    created_at: datetime

# Header: X-API-Key: acc_ak_live_xxxxxxxxxxxxx
# Verify: hash(key) == stored key_hash
# Map to same RBAC system (scopes reduce role permissions)
```

**Implementation Phase:** Month 6 (multi-user launch).

### 4.7 Session & Token Strategy

```
Login Flow:
  POST /auth/token {email, password}
  → verify bcrypt hash
  → issue access_token (8h) + refresh_token (30d)

Token Refresh:
  POST /auth/refresh {refresh_token}
  → verify type="refresh", not expired
  → issue new access_token (8h)
  → optionally rotate refresh_token

Logout:
  POST /auth/logout {refresh_token}
  → add refresh_token to Redis blocklist (TTL = remaining lifetime)
  → client deletes both tokens

Token Blocklist (Redis):
  SET blocklist:{jti} EX {remaining_seconds}
  Check on every request: if jti in blocklist → 401
```

### 4.8 Audit Logging

```python
# Security events to log:
AUDIT_EVENTS = [
    "auth.login_success",
    "auth.login_failed",
    "auth.token_refresh",
    "auth.logout",
    "auth.password_changed",
    "rbac.access_denied",
    "rbac.role_changed",
    "data.export_requested",
    "data.bulk_update",
    "admin.user_created",
    "admin.user_deactivated",
]

# Storage: acc_audit_log table + structlog to stdout (Sentry for errors)
# Retention: 90 days in SQL, 1 year in log archive
```

---

## 5. Security Architecture

### 5.1 Defense-in-Depth Layers

```
Layer 1: NETWORK
  ├── Azure SQL Firewall (IP whitelist)
  ├── Docker network isolation
  └── [Future] Azure Front Door / WAF

Layer 2: TRANSPORT
  ├── TLS 1.2+ for Azure SQL (pymssql enforced)
  ├── HTTPS for all external APIs
  └── [Future] TLS termination at nginx/LB

Layer 3: RATE LIMITING
  ├── slowapi per-IP limits (ref §3.5)
  └── [Future] per-tenant limits

Layer 4: AUTHENTICATION
  ├── JWT Bearer tokens (HS256 → RS256)
  ├── Password: bcrypt (cost factor 12)
  └── [Future] MFA, API keys

Layer 5: AUTHORIZATION
  ├── Role hierarchy (5 levels)
  ├── Marketplace/brand scoping
  └── [Future] Row-level security (SQL Server RLS)

Layer 6: INPUT VALIDATION
  ├── Pydantic v2 models at API boundary
  ├── SQL parameterized queries (? placeholders)
  └── orjson serialization (safe by default)

Layer 7: DATA
  ├── Azure SQL TDE (transparent data encryption at rest)
  ├── No PII in logs (send_default_pii=False in Sentry)
  └── Secrets in .env (→ Key Vault roadmap)
```

### 5.2 Secrets Management

**Current:** All secrets in `.env` file. `.gitignore` includes `*.tokens.json`, `.env`.

**Roadmap:**

```
Phase 1 (Now):     .env file (12-Factor compliant)
  ✅ Not committed to git
  ✅ Read by pydantic-settings
  ⚠️ Plaintext on disk
  ⚠️ No rotation mechanism

Phase 2 (Month 6): Azure Key Vault
  - SP-API credentials → Key Vault secrets
  - DB passwords → Key Vault secrets
  - SECRET_KEY → Key Vault
  - pydantic-settings KeyVault provider
  - Cost: ~$0.03/10K operations

Phase 3 (Month 12): Managed Identity
  - Azure SQL: passwordless via Managed Identity
  - Key Vault: Managed Identity access
  - No secrets in config at all
```

### 5.3 Input Validation

**Boundary Validation (Pydantic v2):**

All API request bodies must use Pydantic models. Query parameters validated via FastAPI's `Query()` with constraints.

```python
from pydantic import BaseModel, Field, field_validator
from datetime import date

class ProfitQueryParams(BaseModel):
    date_from: date
    date_to: date
    marketplace_id: str | None = Field(None, max_length=30)
    page: int = Field(1, ge=1, le=10000)
    page_size: int = Field(50, ge=1, le=500)
    sort_by: str = Field("cm1_profit", pattern=r"^[a-z_]+$")
    sort_dir: str = Field("desc", pattern=r"^(asc|desc)$")

    @field_validator("date_to")
    @classmethod
    def date_range_valid(cls, v, info):
        if info.data.get("date_from") and v < info.data["date_from"]:
            raise ValueError("date_to must be >= date_from")
        return v
```

### 5.4 SQL Injection Prevention

**Current Status: GOOD** — all SQL uses parameterized queries with `?` placeholders.

**Audit Checklist:**

| Check | Status | Evidence |
|-------|--------|----------|
| Parameterized queries (?) | ✅ | All MERGE/SELECT/UPDATE use `?` params |
| No string concatenation in WHERE | ✅ | `cur.execute(sql, params)` pattern |
| No f-string SQL with user input | ⚠️ | `sort_by` column names constructed dynamically — validated against allowlist |
| MERGE statements | ✅ | All use `?` placeholders |

**Remaining Risk:** Dynamic `ORDER BY` clause in profit queries constructs column name from user input. Currently validated against known safe columns. Ensure strict allowlist:

```python
SAFE_SORT_COLUMNS = {
    "cm1_profit", "cm1_margin_pct", "revenue_pln", "units",
    "cogs_pln", "amazon_fees_pln", "ads_spend_pln", "title",
}
if sort_by not in SAFE_SORT_COLUMNS:
    sort_by = "cm1_profit"  # default fallback
```

### 5.5 CORS Hardening

**Current:**

```python
CORS_ORIGINS = [
    "http://localhost:3010",
    "http://localhost:5173",
    "http://192.168.49.97:3010",
]
```

**Recommendations:**

1. Remove `192.168.49.97` if no longer used
2. Add production domain when deployed
3. Restrict credentials mode: `allow_credentials=True` only with explicit origins (never `*`)
4. Add `Vary: Origin` header
5. Move origins to `.env` for per-environment control

### 5.6 TLS & Encryption

| Layer | Status | Detail |
|-------|--------|--------|
| Azure SQL connection | ✅ TLS 1.2 | pymssql forces TLS for `*.database.windows.net` |
| External API calls | ✅ HTTPS | httpx default, zeep uses HTTPS endpoints |
| Data at rest (SQL) | ✅ TDE | Azure SQL Standard includes TDE by default |
| Data at rest (Redis) | ⚠️ No TLS | Redis 7-alpine on Docker — encrypt if exposed |
| Client ↔ Server | ⚠️ HTTP | Dev only. **Must** add TLS for production |

### 5.7 Dependency Scanning

**Recommended Pipeline:**

```yaml
# GitHub Actions workflow
- name: Python dependency audit
  run: |
    pip install pip-audit
    pip-audit --strict --desc

- name: npm audit
  run: |
    cd apps/web
    npm audit --audit-level=high

- name: Dependabot (auto)
  # .github/dependabot.yml — weekly checks
```

**Known Stale Dependencies:**

| Package | Risk | Action |
|---------|------|--------|
| python-jose 3.3.0 | HIGH — unmaintained since 2021 | Replace with pyjwt (TD-12) |
| passlib 1.7.4 | MEDIUM — unmaintained | Already using bcrypt directly |
| azure-monitor-opentelemetry-exporter 1.0.0b33 | LOW — beta | Pin version, monitor for stable release |

### 5.8 Security Headers

**nginx Configuration Additions:**

```nginx
# Add to nginx.conf server block:
add_header X-Frame-Options "DENY" always;
add_header X-Content-Type-Options "nosniff" always;
add_header X-XSS-Protection "1; mode=block" always;
add_header Referrer-Policy "strict-origin-when-cross-origin" always;
add_header Content-Security-Policy "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data: https:; connect-src 'self' ws: wss:;" always;

# When HTTPS is enabled:
add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
```

### 5.9 PII & GDPR

**PII Inventory:**

| Data | Location | Classification |
|------|----------|---------------|
| Buyer email | acc_order.buyer_email | PII — encrypted |
| Buyer name | acc_order.buyer_name | PII — encrypted |
| Shipping address | acc_order.ship_* | PII — encrypted |
| User passwords | acc_user.password_hash | Sensitive — bcrypt |
| API tokens | .env | Sensitive — not in DB |

**GDPR Compliance Plan:**

1. **Data minimization:** Don't store buyer PII unless needed for operations
2. **Right to erasure:** Implement anonymize function for old orders (>2 years)
3. **Sentry PII:** `send_default_pii=False` already configured
4. **Log scrubbing:** structlog filter to redact email/name patterns
5. **Data retention:** Auto-archive orders >24 months (anonymize buyer fields)

---

## 6. Scalability Plan

### 6.1 Horizontal Scaling Roadmap

```
Phase 1 (Now — 1-5 users):
  SINGLE PROCESS
  ├── FastAPI (uvicorn) + APScheduler
  ├── 42 in-process jobs
  ├── Redis (cache + lock)
  └── Bottleneck: CPU-bound profit queries block API (TR-19)

Phase 2 (Month 3-6 — 5-50 users):
  API + WORKERS
  ├── FastAPI ×1 (no scheduler)
  ├── Celery workers (3 pools):
  │   ├── db-heavy (×3): order pipeline, profit calc, finance sync
  │   ├── courier-heavy (×1): GLS/DHL SOAP calls
  │   └── light-default (×4): alerts, taxonomy, content
  ├── APScheduler → Celery beat (cron → task dispatch)
  └── Redis (broker + cache + lock)

Phase 3 (Month 6-12 — 50-500 users):
  MULTI-INSTANCE API
  ├── FastAPI ×2-4 (behind nginx LB)
  ├── Celery workers (scaled per queue)
  ├── Redis Cluster (or Azure Cache)
  ├── Azure SQL S4 (or Elastic Pool)
  └── WebSocket: sticky sessions or Redis-backed broadcast

Phase 4 (Month 12-24 — 500-10K users):
  SERVICE EXTRACTION
  ├── Profit Service (dedicated, cached)
  ├── Ads Pipeline (independent, event-driven)
  ├── API Gateway / BFF
  ├── Azure SQL Elastic Pool (per-tenant isolation)
  └── CDN for static assets
```

**ADR-008**: Activate Celery workers as the first scaling step (ref TD-02). Infrastructure already exists (`docker-compose.yml` has `worker` service, `CELERY_BROKER_URL` configured). Flip `WORKER_EXECUTION_ENABLED=True` and route heavy jobs to worker queues.

**Celery Queue Routing (ready in config.py):**

```python
# Already defined in config.py:
WORKER_CONCURRENCY_COURIER_HEAVY: int = 1
WORKER_CONCURRENCY_INVENTORY_HEAVY: int = 1
WORKER_CONCURRENCY_FINANCE_HEAVY: int = 1
WORKER_CONCURRENCY_FBA_MEDIUM: int = 2
WORKER_CONCURRENCY_CORE_MEDIUM: int = 2
WORKER_CONCURRENCY_LIGHT_DEFAULT: int = 4
WORKER_DB_HEAVY_MAX: int = 3
```

### 6.2 Database Scaling

**Current: Azure SQL Standard S3 (100 DTU, 250 GB max)**

| Milestone | Action | Cost | Reason |
|-----------|--------|------|--------|
| Now | Indexing improvements (§2.2) | $0 | Immediate PPT speedup |
| Now | Profit snapshot table (§2.3) | $0 | 14.5s → <2s |
| Month 3 | Connection pooling (§2.6) | $0 | Reduce connection churn |
| Month 6 | S3 → S4 (200 DTU) | +$150/mo | Multi-user load |
| Month 12 | Read replica (Geo-Replication) | +$300/mo | Separate analytics reads |
| Month 18 | Elastic Pool | Variable | Multi-tenant isolation |

### 6.3 Caching Architecture

**Redis 7 — Multi-layer Cache:**

```
Layer 1: In-Process (Python dict)
  ├── Profit result cache (TTL=180s, max=50 keys)
  ├── FX rate cache (TTL=3600s)
  ├── SKU→ISK mapping cache (TTL=600s)
  └── Use case: hot path, zero latency

Layer 2: Redis Application Cache
  ├── Session data (user context)
  ├── Rate limit counters (slowapi)
  ├── Computed KPIs (TTL=5min)
  ├── API response cache (TTL=60s, key=URL+params hash)
  └── Use case: shared across API instances

Layer 3: Redis Pub/Sub
  ├── acc:alerts channel (real-time alerts)
  ├── acc:cache:invalidate (cache bust signals)
  └── Use case: event propagation

Layer 4: SQL-level (Materialized Views)
  ├── acc_profit_daily_snapshot (nightly rebuild)
  ├── acc_ptd_cache (period-to-date)
  └── Use case: heavy aggregation pre-compute
```

**Cache Invalidation Strategy:**

```
Event: New orders synced
  → Invalidate profit result cache (prefix "ppt:")
  → Publish acc:cache:invalidate {"scope": "profit"}

Event: FX rates updated
  → Clear _FX_CACHE in-memory
  → Publish acc:cache:invalidate {"scope": "fx"}

Event: Profit recalculated
  → Update acc_profit_daily_snapshot
  → Invalidate profit API cache
```

### 6.4 Queue Architecture

**Current APScheduler Jobs (42 total, 16 domain modules):**

| Domain | Jobs | Schedule | Phase 2 Queue |
|--------|------|----------|---------------|
| Orders | 3 | 30min, 01:00, 01:30 | db-heavy |
| Finance | 5 | 02:00, 02:30, 03:00, 03:15, daily | finance-heavy |
| Inventory | 7 | 04:00-04:30, various | core-medium |
| Ads | 1 | every 4h | core-medium |
| Profit | 4 | 05:00, daily, triggered | db-heavy |
| Content | 3 | 06:00, hourly | light-default |
| Logistics | 5 | 23:00-23:30, hourly | courier-heavy |
| Strategy | 4 | 06:30, 07:00, daily | light-default |
| Seasonality | 3 | monthly, weekly | light-default |
| System | 7 | 1min-hourly | light-default |

**Celery Activation Plan:**

1. Set `WORKER_EXECUTION_ENABLED=True`
2. Existing scheduler code already checks this flag and routes to `run_scheduled_job_type()` → `enqueue_job()`
3. Start worker container: `celery -A app.worker.celery_app worker --queues=default,sync,ai`
4. APScheduler becomes a dispatcher (enqueues tasks) rather than an executor

### 6.5 CDN & Static Assets

**Current:** nginx serves Vite-built static files (SPA bundle).

**Phase 2:**
- Move static assets to Azure Blob Storage + Azure CDN
- Cache-busted filenames (Vite generates hashed names)
- nginx only handles `/api/` proxy and `/ws/` upgrade
- Expected savings: 60% reduction in nginx load

### 6.6 Performance Monitoring & SLOs

**Service Level Objectives:**

| SLI | Target (SLO) | Current | Measurement |
|-----|-------------|---------|-------------|
| API P95 latency | <500ms | ~2s (unoptimized PPT) | OpenTelemetry spans |
| PPT P95 latency | <2s | 14.5s | Application metrics |
| Order pipeline freshness | <45 min | ~30 min | `acc_order_sync_state` |
| API availability | 99.5% | Unknown (no monitoring) | Uptime check (TR-01) |
| Error rate | <1% | ~0.3% (Sentry) | Sentry error count |
| Scheduler job success rate | >95% | ~92% | `acc_al_jobs` table |

**Monitoring Stack (recommended):**

```
Tier 1 (Free — implement now):
  ├── Sentry: errors + performance traces (already active)
  ├── structlog: structured logging to stdout
  ├── acc_al_jobs: job success/failure tracking
  └── /api/v1/health: basic health endpoint

Tier 2 (Low cost — Month 3):
  ├── Azure Monitor: uptime checks (fixes TR-01)
  ├── Prometheus metrics endpoint (/metrics)
  ├── Grafana Cloud free tier (10K metrics)
  └── PagerDuty/Opsgenie: on-call alerting

Tier 3 (Scale — Month 12):
  ├── Full OpenTelemetry pipeline
  ├── Azure Application Insights
  ├── Custom dashboards per tenant
  └── SLO burn-rate alerting
```

### 6.7 Load Testing Strategy

**Tools:** k6 (JavaScript-based, free, CI-friendly)

**Test Scenarios:**

```javascript
// Scenario 1: PPT Dashboard (primary bottleneck)
export default function () {
  http.get(`${BASE_URL}/api/v1/profit-v2/products?date_from=2026-01-01&date_to=2026-03-01&page=1&page_size=50`);
}
// Target: 50 VU, P95 < 2s

// Scenario 2: Order Sync Concurrent with API
// Simulate scheduler running profit calc while users browse
// Target: API P95 < 500ms during background job execution

// Scenario 3: Multi-User SaaS Simulation
// 100 concurrent users, mixed read/write, different marketplaces
// Target: P95 < 1s, 0% 5xx errors
```

**Capacity Planning Milestones:**

| Users | API RPS | DB DTU | Redis Memory | Workers |
|-------|---------|--------|-------------|---------|
| 1-5 | <10 | S3 (100) | <100 MB | 0 (in-process) |
| 50 | ~50 | S4 (200) | <500 MB | 3 Celery |
| 500 | ~200 | S6 (400) | <2 GB | 8 Celery |
| 1K | ~500 | S9 (1600) | <4 GB | 16 Celery |
| 10K | ~2K | Elastic Pool | Redis Cluster | K8s autoscale |

### 6.8 Cost Projection

**Current Monthly Cost: ~$308**

| Component | Now | Month 6 | Month 12 | Month 24 |
|-----------|-----|---------|----------|----------|
| Azure SQL | $150 (S3) | $300 (S4) | $600 (S6) | $1,200 (Elastic) |
| Azure VM / ACI | $50 | $100 | $300 | $800 |
| Redis | $0 (Docker) | $0 (Docker) | $50 (Azure Cache) | $200 (Premium) |
| CDN | $0 | $10 | $30 | $100 |
| Sentry | $0 (free) | $26 (Team) | $26 | $80 (Business) |
| Monitoring | $0 | $0 (free tier) | $50 (Grafana) | $200 |
| Key Vault | $0 | $5 | $5 | $10 |
| DNS / SSL | $8 | $8 | $8 | $10 |
| CI/CD | $0 (GitHub) | $0 | $0 | $50 |
| Misc | $100 | $50 | $100 | $250 |
| **Total** | **$308** | **$499** | **$1,169** | **$2,900** |
| Target Users | 1 | 10 | 100 | 1,000 |
| Revenue (MRR) | $0 | $2K | $5K | $20K |
| Margin | - | 75% | 77% | 85% |

---

## Appendix A: Decision Records Summary

| ID | Decision | Status | Cross-ref |
|----|----------|--------|-----------|
| ADR-001 | Keep monolith for ≥12 months | ACCEPTED | TD-04 |
| ADR-002 | Keep NOLOCK for analytics reads | ACCEPTED | — |
| ADR-003 | Defer table partitioning | ACCEPTED | — |
| ADR-004 | Introduce Alembic incrementally | PROPOSED | — |
| ADR-005 | Sync connection pooling via QueuePool | PROPOSED | — |
| ADR-006 | URL-based API versioning | ACCEPTED | — |
| ADR-007 | Defer RS256 migration to Phase 2 | ACCEPTED | TD-12 |
| ADR-008 | Activate Celery as first scaling step | PROPOSED | TD-02 |

## Appendix B: Technology Risk Cross-References

| Risk ID | Title | Mitigation in This Spec |
|---------|-------|------------------------|
| TR-01 | Silent backend crash | §6.6: Uptime monitoring via Azure Monitor |
| TR-11 | Build-fatigue burnout | ADR-001: Keep monolith, defer complexity |
| TR-13 | Single point of knowledge | This document + architecture docs |
| TR-19 | Single-process ceiling | §6.1: Celery workers (ADR-008) |
| TR-20 | No SQL pagination | §3.4: OFFSET/FETCH pagination spec |

## Appendix C: Technology Decision Cross-References

| Decision ID | Title | Section |
|-------------|-------|---------|
| TD-01 | KEEP Azure SQL | §2.1 (20K LoC T-SQL investment) |
| TD-02 | KEEP APScheduler + Celery | §6.4 (activation plan) |
| TD-03 | KEEP raw SQL + materialized views | §2.2, §2.3 |
| TD-04 | KEEP monolith 12+ months | §1.1 (ADR-001) |
| TD-12 | REPLACE python-jose with pyjwt | §4.3 (migration plan) |
| TD-15 | ADD rate limiting (slowapi) | §3.5 (implementation spec) |

---

*End of System Architecture Specification. Generated by Backend Architect agent.*
