# ACC — Architecture Specification

> Version: 2026-03-12 | Architecture: Modular Monolith | Runtime: Python 3.12 + React 18
> Backend LOC: 128,991 | Frontend Pages: 91 | Database Tables: 130+

---

## 1. Architecture Style

**Modular Monolith** — single deployable unit with clear internal module boundaries, designed for eventual service extraction.

```
┌──────────────────────────────────────────────────────────────┐
│                       PRESENTATION LAYER                      │
│  React 18 SPA │ TanStack Query │ Zustand │ Shadcn/UI         │
├──────────────────────────────────────────────────────────────┤
│                       API GATEWAY LAYER                       │
│  FastAPI Routers (49) │ Pydantic Validation │ JWT Auth        │
├──────────────────────────────────────────────────────────────┤
│                       SERVICE / DOMAIN LAYER                  │
│  Intelligence Engines (11) │ Platform (22) │ Connectors (12)  │
├──────────────────────────────────────────────────────────────┤
│                       DATA ACCESS LAYER                       │
│  MSSQL Store │ Redis Cache │ Raw SQL Queries                  │
├──────────────────────────────────────────────────────────────┤
│                       INFRASTRUCTURE                          │
│  Azure SQL │ Redis 7 │ Docker │ APScheduler │ Celery          │
└──────────────────────────────────────────────────────────────┘
```

---

## 2. Layer Architecture

### 2.1 Presentation Layer (Frontend)

```
web/src/
├── pages/              # 91 page components (route-level)
├── components/         # Shared UI components
│   ├── ui/            # Shadcn/UI primitives (40+ components)
│   └── domain/        # Domain-specific components
├── hooks/             # React hooks (TanStack Query wrappers)
├── stores/            # Zustand state stores
├── lib/               # Utilities, API client (Axios)
└── App.tsx            # Router + providers
```

**Patterns**:
- Page components are route-level containers
- Data fetching via custom hooks wrapping TanStack Query
- Global state via Zustand (auth, theme, sidebar)
- API client with interceptors (auth token, error handling)
- Lazy loading via `React.lazy()` for route-based code splitting

### 2.2 API Gateway Layer (Routers)

```
app/api/v1/
├── auth.py                # Authentication endpoints
├── profit_v2.py           # Profitability V2 (primary)
├── finance_center.py      # Finance center
├── fba_ops.py             # FBA operations
├── content_ops.py         # Content operations
├── manage_inventory.py    # Inventory management
├── strategy.py            # Strategy engine
├── ...                    # 42 more router files
└── __init__.py            # Router aggregation
```

**Patterns**:
- Each router maps to one functional domain
- Router dependencies inject auth context
- Request validation via Pydantic models
- Response models enforce output shape
- Async job triggers return 202 with `JobRunOut`

### 2.3 Service / Domain Layer

#### Intelligence Engines (11 modules)

Brain of the system — analytics, scoring, anomaly detection.

```
app/intelligence/
├── account_hub/           # Multi-seller management (658 LOC)
├── buybox_radar/          # Buy Box monitoring (1082 LOC)
├── catalog_health/        # Listing quality scoring (1007 LOC)
├── content_ab_testing/    # A/B experiment engine (894 LOC)
├── content_optimization/  # Content scoring (1087 LOC)
├── event_wiring/          # Event handler registry (874 LOC)
├── family_mapper/         # Product family mapping (2924 LOC)
├── finance_center/        # Financial processing (2674 LOC)
├── inventory_risk/        # Stockout/overstock risk (1536 LOC)
├── manage_inventory/      # Inventory lifecycle (2710 LOC)
├── operator_console/      # Ops case management (778 LOC)
├── refund_anomaly/        # Return fraud detection (1534 LOC)
├── repricing_engine/      # Dynamic pricing (1498 LOC)
└── sqs_topology/          # Queue management (747 LOC)
```

#### Platform Modules (22 modules)

Cross-cutting infrastructure services.

```
app/platform/
├── action_center/     # Action tracking
├── job_dispatch/      # Job orchestration
├── schema_registry/   # Schema management
├── otel/              # OpenTelemetry setup
├── shared/
│   ├── db.py         # Database connection pool
│   └── cache.py      # Redis cache helpers
└── scheduler/         # APScheduler domain schedulers (16 modules)
    ├── profit.py
    ├── orders.py
    ├── families.py
    ├── pricing.py
    ├── ads.py
    ├── fba.py
    ├── finance.py
    ├── returns.py
    ├── content.py
    ├── logistics.py
    ├── strategy.py
    ├── seasonality.py
    ├── listing_state.py
    ├── tax.py
    ├── backbone.py
    └── intelligence.py
```

#### Connectors (12 external integrations)

```
app/connectors/
├── mssql.py             # Azure SQL direct access
├── mssql_store.py       # MSSQL repository layer (3546 LOC)
├── order_pipeline.py    # Order ingestion pipeline (3394 LOC)
├── sync_service.py      # Amazon sync orchestration (2945 LOC)
├── sp_api.py            # Amazon SP-API client
├── ads_connector.py     # Amazon Ads API client
├── dhl_connector.py     # DHL Parcel API
├── gls_connector.py     # GLS ADE API
├── gls_ade.py           # GLS ADE SOAP proxy
├── ergonode.py          # Ergonode PIM API
├── ecb_rates.py         # ECB exchange rates
└── sellerboard.py       # Sellerboard data import
```

### 2.4 Data Access Layer

```
Database: Azure SQL (MSSQL)
├── Alembic Migrations (40 revisions)
├── Inline DDL (39 ensure_*_schema functions) — to be migrated
├── Raw SQL via pyodbc (parameterized)
└── Connection pool: 10 min / 50 max connections

Cache: Redis 7
├── Rate limit counters (per-IP)
├── Job status tracking
├── Session/token blacklist
└── Domain cache (KPIs, dashboards)
```

---

## 3. Module Dependency Rules

```
┌─────────────────────┐
│    API Routers       │  May depend on: Intelligence, Platform, Connectors
├─────────────────────┤
│  Intelligence        │  May depend on: Platform, Connectors
├─────────────────────┤
│    Platform          │  May depend on: Connectors (shared only)
├─────────────────────┤
│    Connectors        │  May depend on: nothing (leaf nodes)
└─────────────────────┘

FORBIDDEN:
- Connectors → Intelligence (no reverse dependency)
- Platform → Intelligence (no reverse dependency)
- Routers → Routers (no cross-router coupling)
- Intelligence → Intelligence across domains (no lateral coupling)
```

---

## 4. Data Flow Patterns

### 4.1 Order Ingestion Pipeline

```
Amazon SP-API → sync_service.py → order_pipeline.py → MSSQL
                                       │
                                       ├── acc_order
                                       ├── acc_order_item
                                       ├── acc_order_item_finance
                                       └── acc_shipment
                                       │
                                  Event: ORDER_SYNCED
                                       │
                              ┌────────┴────────┐
                              │                 │
                     Profitability          Courier Linking
                     Computation           (DHL/GLS match)
```

### 4.2 Profitability Chain (CM1 → CM2 → NP)

```
Revenue (order_item_finance)
  - COGS (purchase_price × quantity)
  - Amazon Fees (referral + FBA + closing)
  ────────────────────────────────────────
  = CM1 (Contribution Margin 1)
  - Ads (ads_daily_stat allocation)
  ────────────────────────────────────────
  = CM2 (Contribution Margin 2)
  - Logistics (DHL/GLS cost allocation)
  - Overhead (fixed cost allocation)
  ────────────────────────────────────────
  = NP (Net Profit)
```

### 4.3 Event-Driven Flow

```
Domain Event (e.g., PRICE_CHANGED)
  → acc_event_log (persist with idempotency_key)
  → Event Wiring (lookup registered handlers)
  → SQS Queue (if external async needed)
  → Handler execution
  → acc_event_processing_log (track success/failure)
  → acc_event_handler_health (aggregate metrics)
```

### 4.4 Job Dispatch Pattern

```
API Request: POST /domain/jobs/run
  → JobDispatch.schedule(job_name, params)
  → APScheduler adds one-shot job
  → Worker executes job function
  → WebSocket: progress updates to /ws/jobs/{job_id}
  → On complete: set_job_success() / set_job_failure()
  → Alert generation if failure
```

---

## 5. Key Design Patterns

### 5.1 Repository Pattern

```python
# Connector layer — raw data access
class MSSQLStore:
    def get_orders(self, marketplace_id, date_from, date_to) -> list[dict]:
        sql = "SELECT ... FROM acc_order WHERE ..."
        return execute_query(sql, params)

# Intelligence layer — business logic
class ProfitService:
    def compute_profitability(self, order_id):
        order = mssql_store.get_order(order_id)
        cogs = mssql_store.get_purchase_price(order.sku)
        fees = mssql_store.get_order_finance(order_id)
        return self._calculate_margins(order, cogs, fees)
```

### 5.2 Job Pattern

```python
# Scheduler registration
scheduler.add_job(
    sync_orders_job,
    trigger="interval",
    hours=1,
    id="sync_orders",
    replace_existing=True,
)

# Job function
async def sync_orders_job():
    set_job_running("sync_orders")
    try:
        result = await sync_service.sync_all_marketplaces()
        set_job_success("sync_orders", result.summary)
    except Exception as e:
        set_job_failure("sync_orders", str(e))
        raise
```

### 5.3 Circuit Breaker Pattern

```python
class CircuitBreaker:
    CLOSED, OPEN, HALF_OPEN = "closed", "open", "half_open"

    def call(self, func, *args):
        if self.state == self.OPEN:
            if time.time() - self.last_failure > self.recovery_timeout:
                self.state = self.HALF_OPEN
            else:
                raise CircuitBreakerOpen()

        try:
            result = func(*args)
            self._on_success()
            return result
        except Exception:
            self._on_failure()
            raise
```

### 5.4 Ensure Schema Pattern (Legacy — to be migrated)

```python
def ensure_fba_schema(conn):
    """Create tables if not exists — DEPRECATED, migrate to Alembic"""
    conn.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'acc_fba_inventory_snapshot')
        CREATE TABLE acc_fba_inventory_snapshot (...)
    """)
```

---

## 6. Concurrency Model

| Component | Model | Details |
|---|---|---|
| **FastAPI** | Async I/O (uvicorn) | 4 workers, async/await for I/O-bound ops |
| **APScheduler** | Thread pool | BackgroundScheduler with max 20 threads |
| **Celery** | Process pool | Redis broker, 4 worker processes |
| **Database** | Connection pool | pyodbc pool: 10 min, 50 max |
| **Redis** | Single-threaded | Connection pool: 20 max connections |

---

## 7. Deployment Architecture

```
Docker Compose (4 services)
├── redis:7      (port 6379)
├── api          (port 8000, 4 uvicorn workers)
│   └── APScheduler (embedded, thread pool)
├── worker       (Celery, Redis broker, 4 processes)
└── web          (port 5173, Vite dev / nginx prod)
```

### Production Target

```
Azure Container Apps
├── api (2-4 replicas, auto-scale on CPU)
├── worker (1-2 replicas)
├── web (static files on Azure CDN)
├── Azure SQL (S2 tier)
└── Azure Redis Cache (C1 tier)
```

---

## 8. Cross-Cutting Concerns

| Concern | Implementation |
|---|---|
| **Authentication** | JWT HS256, 30min access + 7d refresh |
| **Authorization** | 5-role RBAC hierarchy, marketplace/brand filtering |
| **Logging** | structlog with JSON output, Sentry integration |
| **Tracing** | OpenTelemetry SDK → OTLP exporter |
| **Caching** | Redis with TTL-based invalidation |
| **Rate Limiting** | Redis-backed sliding window per IP |
| **Error Handling** | FastAPI exception handlers + Sentry capture |
| **Validation** | Pydantic v2 strict mode |
| **Migration** | Alembic (40 revisions) + legacy inline DDL (39 functions) |
| **Testing** | pytest (1765 tests), httpx AsyncClient, mock-heavy |
| **CI** | GitHub Actions — test, lint, build |

---

## 9. Scalability Considerations

| Bottleneck | Current | Mitigation |
|---|---|---|
| **Profit query** | Single large SQL per request | Add Redis cache (5min TTL), pre-compute rollups |
| **Order sync** | Sequential per marketplace | Parallelize with asyncio.gather |
| **Database connections** | 50 max pool | Azure SQL connection pooling, read replicas |
| **File exports** | Synchronous XLSX generation | Queue export jobs, serve from blob storage |
| **WebSocket** | In-memory connections | Redis pub/sub for multi-instance |
| **Large tables** | 130+ tables, some >1M rows | Partition by date, archive old data |

---

## 10. Architecture Decision Records (ADRs)

| # | Decision | Rationale |
|---|---|---|
| ADR-001 | Modular monolith over microservices | Small team (2-3 devs), fast iteration, single deploy |
| ADR-002 | Raw SQL over ORM (SQLAlchemy Core) | Complex reporting queries, performance control |
| ADR-003 | APScheduler over Celery Beat | Simpler for in-process scheduling, Celery for async |
| ADR-004 | Shadcn/UI over Material UI | Better customization, smaller bundle, Radix primitives |
| ADR-005 | Zustand over Redux | Simpler API, less boilerplate, sufficient for SPA |
| ADR-006 | TanStack Query over SWR | Better DevTools, query invalidation, infinite scroll |
| ADR-007 | Azure SQL over PostgreSQL | Client requirement (existing Azure infrastructure) |
| ADR-008 | JWT over session cookies | SPA architecture, API-first design |
| ADR-009 | Docker Compose over Kubernetes | Deployment simplicity at current scale |
| ADR-010 | Alembic + inline DDL (hybrid) | Historical debt — inline DDL to be fully migrated |
