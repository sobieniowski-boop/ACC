# ACC — Integration Specification

> Version: 2026-03-12 | Total Integrations: 12   
> Protocols: REST, SOAP, SDK, File-based, TCP

---

## 1. Integration Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                         ACC PLATFORM                              │
│                                                                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │ SP-API   │  │ Ads API  │  │   DHL    │  │   GLS    │        │
│  │ Client   │  │ Client   │  │ Client   │  │ Client   │        │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘        │
│       │              │              │              │              │
├───────┼──────────────┼──────────────┼──────────────┼──────────────┤
│       │              │              │              │              │
│  ┌────┴─────┐  ┌────┴─────┐  ┌────┴─────┐  ┌────┴─────┐        │
│  │ Ergonode │  │   ECB    │  │  Sentry  │  │Sellerboard│       │
│  │ Client   │  │ Client   │  │  SDK     │  │ Importer  │       │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
│                                                                    │
│  ┌──────────┐  ┌──────────┐                                      │
│  │Azure SQL │  │  Redis   │                                      │
│  │(MSSQL)   │  │  7       │                                      │
│  └──────────┘  └──────────┘                                      │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. Amazon SP-API

### Connection Details

| Property | Value |
|---|---|
| **Protocol** | REST (HTTPS) |
| **Base URL** | `https://sellingpartnerapi-eu.amazon.com` |
| **Auth** | LWA OAuth2 (Login with Amazon) |
| **Client** | `app/connectors/sp_api.py` |
| **Circuit Breaker** | Yes (5 failures → 60s open) |

### Authentication Flow

```
1. Use refresh_token + client_id + client_secret
2. POST https://api.amazon.com/auth/o2/token
3. Receive access_token (1 hour validity)
4. Include in Authorization: Bearer header
5. Auto-refresh before expiry
```

### Environment Variables

| Variable | Description |
|---|---|
| `SP_API_REFRESH_TOKEN` | Long-lived refresh token |
| `SP_API_CLIENT_ID` | LWA application client ID |
| `SP_API_CLIENT_SECRET` | LWA application client secret |
| `SP_API_MARKETPLACE_IDS` | Comma-separated marketplace IDs |
| `SP_API_SELLER_ID` | Amazon seller ID |

### API Endpoints Used

| API | Endpoint | Purpose | Frequency |
|---|---|---|---|
| **Orders** | `GET /orders/v0/orders` | Fetch orders by date range | Hourly |
| **Orders** | `GET /orders/v0/orders/{id}/orderItems` | Order item details | Per order |
| **Finances** | `GET /finances/v0/financialEventGroups` | Financial events | Hourly |
| **Finances** | `GET /finances/v0/financialEvents` | Detailed charge breakdown | Per order |
| **Catalog** | `GET /catalog/2022-04-01/items` | Product catalog data | On demand |
| **Listings** | `GET /listings/2021-08-01/items` | Listing details | Daily |
| **FBA Inventory** | `GET /fba/inventory/v1/summaries` | FBA inventory levels | Every 4h |
| **FBA Inbound** | `GET /fba/inbound/v0/shipments` | Inbound shipment status | Every 4h |
| **Reports** | `POST /reports/2021-06-30/reports` | Request report generation | On demand |
| **Reports** | `GET /reports/2021-06-30/reports/{id}` | Check report status | Polling |
| **Product Type Definitions** | `GET /definitions/2020-09-01/productTypes` | Product type schemas | On demand |
| **Notifications** | `GET /notifications/v1/subscriptions` | SQS notification subscriptions | On setup |

### Rate Limiting

| API | Rate | Burst |
|---|---|---|
| Orders | 1 request/second | 20 |
| Finances | 0.5 request/second | 30 |
| Catalog | 2 requests/second | 10 |
| Reports | 0.0222/second (1 per 45s) | 10 |

### Error Handling

| Error | Handling |
|---|---|
| 429 Too Many Requests | Exponential backoff (1s, 2s, 4s, 8s, max 30s) |
| 503 Service Unavailable | Retry 3 times with backoff |
| 401 Unauthorized | Refresh access token, retry once |
| 400 Bad Request | Log and skip (data issue) |
| Network timeout | Retry 2 times, then circuit breaker |

---

## 3. Amazon Ads API

### Connection Details

| Property | Value |
|---|---|
| **Protocol** | REST (HTTPS) |
| **Base URL** | `https://advertising-api-eu.amazon.com` |
| **Auth** | LWA OAuth2 + Profile ID |
| **Client** | `app/connectors/ads_connector.py` |
| **Circuit Breaker** | Yes |

### Environment Variables

| Variable | Description |
|---|---|
| `ADS_API_CLIENT_ID` | Ads API client ID |
| `ADS_API_CLIENT_SECRET` | Ads API client secret |
| `ADS_API_REFRESH_TOKEN` | Ads API refresh token |
| `ADS_API_PROFILE_IDS` | Comma-separated profile IDs per market |

### API Endpoints Used

| Endpoint | Purpose | Frequency |
|---|---|---|
| `GET /v2/profiles` | List advertising profiles | On setup |
| `POST /sp/campaigns/list` | List Sponsored Products campaigns | Daily |
| `POST /sp/targets/list` | Campaign targeting data | On demand |
| `POST /reporting/reports` | Request campaign report | Daily |
| `GET /reporting/reports/{id}` | Download report | Polling |

### Data Flow

```
Daily Sync:
  1. List all active profiles
  2. For each profile/marketplace:
     a. Request campaign performance report
     b. Poll until report ready
     c. Download and parse report
     d. Upsert acc_ads_campaign + acc_ads_daily_stat
  3. Allocate ads cost to SKUs for profitability
```

---

## 4. DHL Parcel API

### Connection Details

| Property | Value |
|---|---|
| **Protocol** | REST (HTTPS) |
| **Base URL** | `https://api-eu.dhl.com` |
| **Auth** | API Key (header: `DHL-API-Key`) |
| **Client** | `app/connectors/dhl_connector.py` |
| **Circuit Breaker** | Yes |

### Environment Variables

| Variable | Description |
|---|---|
| `DHL_API_KEY` | DHL API key for tracking |
| `DHL_API_SECRET` | DHL API secret (if needed) |

### API Endpoints Used

| Endpoint | Purpose | Frequency |
|---|---|---|
| `GET /track/shipments` | Track by tracking number | On demand |
| `GET /track/shipments/{id}/events` | Shipment event history | On demand |
| `GET /track/shipments/{id}/pod` | Proof of delivery document | On demand |

### Data Tables

| Table | Description |
|---|---|
| `acc_dhl_shipment` | Shipment header (tracking number, status, dates) |
| `acc_dhl_piece` | Individual package pieces |
| `acc_dhl_event` | Tracking events (status, location, timestamp) |

### Billing Import

DHL billing files are imported via file upload (`POST /dhl/jobs/import-billing-files`):
- CSV format with shipment costs
- Matched to orders via tracking number or reference
- Used for logistics cost allocation in profitability

---

## 5. GLS API (ADE + REST)

### Connection Details

| Property | Value |
|---|---|
| **Protocol** | SOAP (ADE) + REST |
| **ADE WSDL** | GLS ADE Webservice endpoint |
| **Auth** | Username + Password |
| **Client** | `app/connectors/gls_connector.py`, `app/connectors/gls_ade.py` |
| **Circuit Breaker** | Yes |

### Environment Variables

| Variable | Description |
|---|---|
| `GLS_USERNAME` | GLS API username |
| `GLS_PASSWORD` | GLS API password |
| `GLS_ADE_URL` | GLS ADE webservice URL |

### API Endpoints Used

#### REST (Tracking)

| Endpoint | Purpose |
|---|---|
| `GET /track/{parcel_number}` | Track single parcel |
| `POST /track/batch` | Track multiple parcels |
| `GET /track-by-ref` | Track by reference number |

#### SOAP (ADE)

| Operation | Purpose |
|---|---|
| `adeTrack` | Advanced tracking with full event history |
| `adePOD` | Proof of delivery document |
| `adeServices` | Available services list |
| `adeGetConsignment` | Consignment details |
| `adeGetLabels` | Shipping labels |
| `adeGetPickups` | Pickup scheduling |

### Data Tables

| Table | Description |
|---|---|
| `acc_gls_shipment` | Shipment header |
| `acc_gls_event` | Tracking events |

---

## 6. Ergonode PIM

### Connection Details

| Property | Value |
|---|---|
| **Protocol** | REST (HTTPS) |
| **Auth** | API Key (header: `X-API-KEY`) |
| **Client** | `app/connectors/ergonode.py` |
| **Circuit Breaker** | Yes |

### Environment Variables

| Variable | Description |
|---|---|
| `ERGONODE_API_URL` | Ergonode instance base URL |
| `ERGONODE_API_KEY` | Ergonode API key |

### Usage

| Operation | Purpose | Frequency |
|---|---|---|
| Get products | Sync product master data (name, EAN, brand, category) | Daily |
| Get attributes | Fetch product attribute definitions | On demand |
| Get categories | Category hierarchy sync | On demand |

### Data Flow

```
Ergonode PIM → acc_product (ergonode_sku, ergonode_synced_at)
  → Update product names, brands, categories
  → Feed into family mapper for cross-market linking
```

---

## 7. ECB Exchange Rates

### Connection Details

| Property | Value |
|---|---|
| **Protocol** | REST (HTTPS, public) |
| **Base URL** | `https://data-api.ecb.europa.eu/service/data` |
| **Auth** | None (public API) |
| **Client** | `app/connectors/ecb_rates.py` |

### Usage

| Endpoint | Purpose | Frequency |
|---|---|---|
| `GET /EXR/D.PLN+SEK.EUR.SP00.A` | Daily EUR→PLN, EUR→SEK rates | Daily |

### Data Flow

```
ECB API → acc_exchange_rate
  → Used by profitability engine for multi-currency conversion
  → Used by tax compliance for VAT amount conversion
  → PLN is base reporting currency
```

---

## 8. Sellerboard

### Connection Details

| Property | Value |
|---|---|
| **Protocol** | File-based import |
| **Auth** | N/A (manual file export) |
| **Client** | `app/connectors/sellerboard.py` |

### Usage

Historical data import for orders/financials before ACC was deployed:

| Operation | Purpose |
|---|---|
| Order import | Seed historical orders from sellerboard CSV export |
| Revenue data | Import revenue figures for profitability backfill |
| Fee data | Import Amazon fee breakdown data |

### Related Scripts

- `oneoff_seed_orders_from_sellerboard.py`
- `oneoff_seed_2025_orders_from_sellerboard.py`
- `oneoff_stage_2025_sellerboard_lines.py`
- `oneoff_rebuild_2025_order_lines_from_sellerboard.py`

---

## 9. Sentry

### Connection Details

| Property | Value |
|---|---|
| **Protocol** | SDK (HTTPS) |
| **Auth** | DSN (Data Source Name) |
| **SDKs** | `sentry-sdk[fastapi]` (backend), `@sentry/react` (frontend) |

### Environment Variables

| Variable | Description |
|---|---|
| `SENTRY_DSN` | Sentry project DSN |
| `SENTRY_ENVIRONMENT` | Environment name (production/staging/dev) |
| `SENTRY_TRACES_SAMPLE_RATE` | Trace sampling rate (0.0 - 1.0) |

### Features Used

| Feature | Description |
|---|---|
| Error tracking | Automatic exception capture with stack traces |
| Performance | Transaction tracing for API requests |
| User context | Attach user email and role to events |
| Release tracking | Version tagging for deployment correlation |
| Breadcrumbs | HTTP request, database query, log breadcrumbs |

---

## 10. Redis

### Connection Details

| Property | Value |
|---|---|
| **Protocol** | TCP (Redis wire protocol) |
| **Port** | 6379 (local), 6380 (Azure TLS) |
| **Auth** | Password (`REDIS_PASSWORD`) |
| **Client** | `redis-py` via `app/platform/shared/cache.py` |

### Environment Variables

| Variable | Description |
|---|---|
| `REDIS_HOST` | Redis server hostname |
| `REDIS_PORT` | Redis port (default 6379) |
| `REDIS_PASSWORD` | Redis authentication password |
| `REDIS_DB` | Database number (default 0) |

### Usage

| Purpose | Key Pattern | TTL |
|---|---|---|
| Rate limiting | `ratelimit:{ip}:{endpoint}` | 60s |
| Job status | `job:{job_id}` | 1h |
| Session cache | `session:{user_id}` | 30min |
| Domain cache | `cache:{domain}:{key}` | 5-10min |
| Celery broker | `celery-*` | — |
| Circuit breaker | `cb:{service}` | 60s |

---

## 11. Azure SQL (MSSQL)

### Connection Details

| Property | Value |
|---|---|
| **Protocol** | TDS over TLS |
| **Driver** | pyodbc + ODBC Driver 18 for SQL Server |
| **Auth** | SQL authentication (username + password) |
| **Client** | `app/connectors/mssql.py`, `app/platform/shared/db.py` |

### Environment Variables

| Variable | Description |
|---|---|
| `DATABASE_URL` | Full connection string (mssql+pyodbc://...) |
| `DB_HOST` | Azure SQL server hostname |
| `DB_NAME` | Database name |
| `DB_USER` | SQL username |
| `DB_PASSWORD` | SQL password |

### Connection Pool

| Setting | Value |
|---|---|
| Min connections | 10 |
| Max connections | 50 |
| Connection timeout | 30 seconds |
| Command timeout | 120 seconds |
| Encrypt | Yes (TLS) |
| Trust Server Certificate | No (production) |

### Migration

| Tool | Details |
|---|---|
| Alembic | 40 revisions (fm001 → eb038) |
| Inline DDL | 39 ensure_*_schema functions (legacy, to be migrated) |

---

## 12. OpenTelemetry

### Connection Details

| Property | Value |
|---|---|
| **Protocol** | OTLP (gRPC or HTTP) |
| **Client** | `app/platform/otel/` module |

### Environment Variables

| Variable | Description |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP collector endpoint |
| `OTEL_SERVICE_NAME` | Service name (acc-api) |
| `OTEL_TRACES_SAMPLER` | Sampling strategy |

### Instrumentation

| Layer | Instrumentation |
|---|---|
| HTTP requests | FastAPI auto-instrumentation |
| Database queries | pyodbc manual spans |
| Redis operations | redis auto-instrumentation |
| External API calls | httpx/requests auto-instrumentation |
| Background jobs | Manual span creation |

---

## 13. Integration Health Monitoring

### Health Endpoints

| Integration | Health Check |
|---|---|
| Azure SQL | `GET /health` (connection test) |
| Redis | `GET /health` (PING) |
| SP-API | `GET /health/sp-api-usage` |
| DHL | `GET /dhl/health` |
| GLS | `GET /gls/health` |
| GLS ADE | `GET /gls/ade/health` |
| Backbone | `GET /backbone/health` |
| Notifications | `GET /notifications/health` |
| Content Ops | `GET /content-ops/health` |
| Event Wiring | `GET /event-wiring/health` |
| SQS Topology | `GET /sqs-topology/health` |

### Circuit Breaker Status

All external API clients have circuit breaker protection:

| Service | Failure Threshold | Recovery Timeout | Reset Endpoint |
|---|---|---|---|
| SP-API | 5 failures | 60 seconds | — |
| Ads API | 5 failures | 60 seconds | — |
| DHL | 5 failures | 60 seconds | — |
| GLS | 5 failures | 60 seconds | — |
| Ergonode | 5 failures | 60 seconds | — |
| Content Publish | 5 failures | 60 seconds | `POST /content-ops/publish/circuit-breaker/reset` |

---

## 14. Data Flow Summary

```
INBOUND:
  Amazon SP-API ──→ Orders, Listings, FBA, Catalog, Finances
  Amazon Ads API ──→ Campaigns, Spend
  DHL API ──────────→ Shipment tracking, billing
  GLS API ──────────→ Shipment tracking, billing
  Ergonode PIM ────→ Product master data
  ECB ──────────────→ Exchange rates
  Sellerboard ─────→ Historical order data (one-time)

OUTBOUND:
  ACC ──→ Amazon SP-API (listing updates via Content Publish)
  ACC ──→ Sentry (errors, traces)
  ACC ──→ OTLP Collector (traces, metrics)
  ACC ──→ Notification channels (email, Slack, webhook)

INTERNAL:
  API ←→ Azure SQL (read/write)
  API ←→ Redis (cache, rate limit, jobs)
  API ←→ Celery/Worker (async jobs via Redis)
  API ←→ WebSocket clients (real-time updates)
```
