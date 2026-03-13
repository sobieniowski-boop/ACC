# SP-API Utilization Review & Digital Twin Roadmap
## Amazon Command Center (ACC) — KADAX

**Author:** SP-API Architecture Audit  
**Date:** 2026-03-09  
**Scope:** Full codebase review, 11 SP-API families, 40+ endpoints, 30+ service modules

---

## 1. EXECUTIVE SUMMARY

ACC is a **mature, production-grade** Amazon seller analytics platform covering 9 EU marketplaces for the KADAX brand. It successfully integrates **8 SP-API families** (Orders, Finances, Inventory, Catalog, Pricing, Reports, Listings Items, Feeds) plus **Amazon Advertising API** (profiles, campaigns SP/SB/SD, async reports).

**Strengths:**
- Fully async HTTP layer (`httpx.AsyncClient`) with exponential backoff + jitter across all connectors
- Idempotent SQL MERGE upserts throughout — safe for restarts and replays
- Strong profit engine (6,400-line CM1/CM2/NP SQL engine) and 7-table family/variation model
- SP-API telemetry (usage tracking table) and watermark-based incremental sync for orders
- Listings Items API + PTD integration for family restructuring (GET/PUT/PATCH/DELETE + PTD validation)

**Critical gaps toward Digital Twin:**
1. **Zero event-driven architecture** — all data arrives via polling (15 min to 24h lag). Notifications API not used.
2. **No repricing engine** — pricing is import-driven only; no automated BuyBox reaction loop.
3. **No continuous Listings/Catalog sync** — listing attributes and catalog data fetched on-demand only; stale by design.
4. **No A+ Content API integration** — content ops are editor-only, cannot read published A+ from Amazon.
5. **Coarse telemetry** — daily aggregates in `sp_api_usage`, no per-request trace IDs, no `x-amzn-RequestId` capture.
6. **No normalization DTO layer** — Amazon API responses go directly to SQL; fragile to API version changes.

**Digital Twin readiness: ~55%** — ACC is a strong *data warehouse* of Amazon state, but not yet a *live mirror*. The polling-only architecture, lack of event subscriptions, and absence of a repricing reaction loop are the biggest structural gaps.

---

## 2. CURRENT SP-API FOOTPRINT

### 2.1 SP-API Families in Use

| # | API Family | Version | Connector File | Status |
|---|-----------|---------|---------------|--------|
| 1 | **Orders API** | v0 | `connectors/amazon_sp_api/orders.py` | ✅ Active |
| 2 | **Finances API** | v2024-06-19 | `connectors/amazon_sp_api/finances.py` | ✅ Active |
| 3 | **FBA Inventory API** | v1 | `connectors/amazon_sp_api/inventory.py` | ✅ Active |
| 4 | **Catalog Items API** | v2022-04-01 | `connectors/amazon_sp_api/catalog.py` | ✅ Active |
| 5 | **Product Pricing API** | v0 | `connectors/amazon_sp_api/pricing_api.py` | ✅ Active |
| 6 | **Reports API** | v2021-06-30 | `connectors/amazon_sp_api/reports.py` | ✅ Active |
| 7 | **Listings Items API** | v2021-08-01 | `connectors/amazon_sp_api/listings.py` | ✅ Active |
| 8 | **Product Type Definitions API** | v2020-09-01 | `connectors/amazon_sp_api/listings.py` | ✅ Active |
| 9 | **Feeds API** | v2021-06-30 | `connectors/amazon_sp_api/feeds.py` | ✅ Active |
| 10 | **FBA Inbound API** | v0 | `connectors/amazon_sp_api/inbound.py` | ✅ Active |
| 11 | **Amazon Ads API** | v2-v4 | `connectors/amazon_ads_api/*` | ✅ Active |

### 2.2 Detailed Endpoint Usage

#### Orders API v0
| Endpoint | Method | Module | Purpose | Pattern |
|----------|--------|--------|---------|---------|
| `/orders/v0/orders` | GET | `sync_service.py`, `order_pipeline.py` | Fetch orders by date window | Read, polled every 15 min |
| `/orders/v0/orders/{id}/orderItems` | GET | `sync_service.py`, `order_pipeline.py` | Fetch line items per order | Read, per-order enrichment |

#### Finances API v2024-06-19
| Endpoint | Method | Module | Purpose | Pattern |
|----------|--------|--------|---------|---------|
| `/finances/2024-06-19/transactions` | GET | `sync_service.py`, `finance_center/service.py` | Financial transactions (shipment, refund, fees) | Read, daily 03:00 batch |

#### FBA Inventory API v1
| Endpoint | Method | Module | Purpose | Pattern |
|----------|--------|--------|---------|---------|
| `/fba/inventory/v1/summaries` | GET | `sync_service.py`, `manage_inventory.py` | FBA stock levels | Read, daily 04:00 batch |

#### Catalog Items API v2022-04-01
| Endpoint | Method | Module | Purpose | Pattern |
|----------|--------|--------|---------|---------|
| `/catalog/2022-04-01/items/{asin}` | GET | `sync_service.py`, `family_mapper/*` | Single ASIN lookup | Read, on-demand |
| `/catalog/2022-04-01/items` (search) | GET | `sync_service.py` | Batch ASIN/EAN search | Read, on-demand |

#### Product Pricing API v0
| Endpoint | Method | Module | Purpose | Pattern |
|----------|--------|--------|---------|---------|
| `/products/pricing/v0/competitivePrice` | GET | `sync_service.py` | BuyBox price, offer counts | Read, on-demand |
| `/products/pricing/v0/price` | GET | `sync_service.py` | Listing price, featured merchant | Read, on-demand |
| `/products/pricing/v0/items/{asin}/offers` | GET | `sync_service.py` | All competitive offers for ASIN | Read, on-demand |
| `/products/fees/v0/items/{asin}/feesEstimate` | POST | `sync_service.py` | Fee estimation at price point | Read (POST for estimation) |

#### Listings Items API v2021-08-01
| Endpoint | Method | Module | Purpose | Pattern |
|----------|--------|--------|---------|---------|
| `/listings/2021-08-01/items/{sellerId}/{sku}` | GET | `restructure.py` | Fetch listing attributes/issues | Read, on-demand (restructure) |
| `/listings/2021-08-01/items/{sellerId}/{sku}` | PUT | `restructure.py` | Create/replace parent listing | Write, on-demand (restructure) |
| `/listings/2021-08-01/items/{sellerId}/{sku}` | PATCH | `restructure.py`, `manage_inventory.py` | Update child parent relationship | Write, on-demand |
| `/listings/2021-08-01/items/{sellerId}/{sku}` | DELETE | `restructure.py` | Remove foreign parent | Write, on-demand |

#### Product Type Definitions API v2020-09-01
| Endpoint | Method | Module | Purpose | Pattern |
|----------|--------|--------|---------|---------|
| `/definitions/2020-09-01/productTypes/{type}` | GET | `restructure.py`, `content_ops.py` | Validate variation themes / attribute schema | Read, on-demand |

#### Reports API v2021-06-30
| Endpoint | Method | Module | Purpose | Pattern |
|----------|--------|--------|---------|---------|
| `/reports/2021-06-30/reports` | POST | `reports.py` (all callers) | Create async report | Write (trigger) |
| `/reports/2021-06-30/reports/{id}` | GET | `reports.py` | Poll report status | Read (polling loop) |
| `/reports/2021-06-30/documents/{id}` | GET | `reports.py` | Get report download URL | Read |

**Report Types Used:**
| Report Type | Caller | Schedule | Purpose |
|------------|--------|----------|---------|
| `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL` | `backfill_via_reports.py` | Manual backfill | Bulk order/line backfill |
| `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL` | `backfill_via_reports.py` | Manual backfill | Incremental catch-up |
| `GET_MERCHANT_LISTINGS_ALL_DATA` | `sync_service.py` | Daily 01:00 | Product master refresh |
| `GET_FBA_MYI_ALL_INVENTORY_DATA` | `sync_service.py` | Daily 04:00 | FBA inventory snapshot |
| `GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA` | `fba_fee_audit.py` | On-demand | Fee audit reference data |
| `GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2` | `finance_center/service.py` | On-demand | Settlement reconciliation |
| `GET_SALES_AND_TRAFFIC_REPORT` | `sync_service.py` | Daily 04:30 | ASIN/SKU traffic metrics |
| `GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA` | `return_tracker.py` | On-demand | FBA return reconciliation |
| `GET_FBA_INVENTORY_PLANNING_DATA` | `fba_ops/service.py` | On-demand | Inventory planning |

#### Feeds API v2021-06-30
| Endpoint | Method | Module | Purpose | Pattern |
|----------|--------|--------|---------|---------|
| `/feeds/2021-06-30/documents` | POST | `feeds.py` → `manage_inventory.py` | Create feed upload URL | Write |
| S3 pre-signed URL | PUT | `feeds.py` | Upload feed content | Write (bulk) |
| `/feeds/2021-06-30/feeds` | POST | `feeds.py` | Submit feed for processing | Write |
| `/feeds/2021-06-30/feeds/{id}` | GET | `feeds.py` | Poll feed status | Read |

**Feed Types Used:** `JSON_LISTINGS_FEED` (reparent, update_theme operations)

#### FBA Inbound API v0
| Endpoint | Method | Module | Purpose | Pattern |
|----------|--------|--------|---------|---------|
| `/fba/inbound/v0/shipments` | GET | `fba_ops/service.py` | List inbound shipments | Read, on-demand |
| `/fba/inbound/v0/shipments/{id}/items` | GET | `fba_ops/service.py` | Shipment line items | Read, on-demand |

#### Amazon Ads API (separate auth)
| Endpoint | Method | Module | Purpose | Pattern |
|----------|--------|--------|---------|---------|
| `/v2/profiles` | GET | `ads_sync.py` → `profiles.py` | Marketplace profile mapping | Read, daily 07:00 |
| `/sp/campaigns/list` | POST | `ads_sync.py` → `campaigns.py` | SP campaigns (v3 header) | Read, daily 07:00 |
| `/sb/v4/campaigns/list` | POST | `ads_sync.py` → `campaigns.py` | SB campaigns (v4 + legacy fallback) | Read, daily 07:00 |
| `/sd/campaigns` | GET | `ads_sync.py` → `campaigns.py` | SD campaigns | Read, daily 07:00 |
| `/reporting/reports` | POST | `ads_sync.py` → `reporting.py` | Create async ad reports (SP/SB/SD) | Write (trigger) |
| `/reporting/reports/{id}` | GET | `ads_sync.py` → `reporting.py` | Poll report status | Read (polling) |

---

## 3. GAP ANALYSIS BY DOMAIN

### 3.1 Catalog Control

| Aspect | Status | Detail |
|--------|--------|--------|
| **Well implemented** | Listings Items API GET/PUT/PATCH/DELETE | Full CRUD in `listings.py`, used for family restructure |
| **Well implemented** | PTD validation | Variation theme check before PATCH, locale-aware |
| **Well implemented** | Family/variation model | 7-table `global_family*` architecture, DE-first canonical |
| **Partially implemented** | Catalog Items API | Connector exists with batch + search, but no scheduled sync job |
| **Partially implemented** | Merchant Listings report | Daily 01:00 via `GET_MERCHANT_LISTINGS_ALL_DATA`, but only populates `acc_product` (basic fields) |
| **Missing** | Continuous listing attribute sync | No scheduled `sync_listings_attributes()` job — attributes go stale |
| **Missing** | PTD cache table | Fetched on-demand every time; no `acc_product_type_definition` cache |
| **Missing** | Listing issue monitoring | `getListingsItem` returns `issues[]` but they're not stored or alerted on |
| **Missing** | Listing health dashboard | No automated compliance/suppression detection |
| **Wrong pattern** | Product master is report-driven only | `GET_MERCHANT_LISTINGS_ALL_DATA` is good for bulk, but listing detail/issues need Listings Items API |

**Risk: MEDIUM-HIGH** — Family restructure works, but ongoing catalog health is unmonitored.

### 3.2 Pricing / Repricing

| Aspect | Status | Detail |
|--------|--------|--------|
| **Well implemented** | Competitive pricing fetch | `get_competitive_pricing()` + `get_pricing()` with batch support |
| **Well implemented** | Fee estimation | `get_fees_estimate()` for margin simulation |
| **Well implemented** | Offer state storage | `acc_offer` table per (sku, marketplace) |
| **Partially implemented** | BuyBox monitoring | Data fetched but no continuous tracking / alerting |
| **Missing** | Automated repricing engine | ❌ Zero repricing logic — prices are import-only |
| **Missing** | Pricing rules / strategy | No rule engine (min/max, target margin, competitor response) |
| **Missing** | Pricing change detection | Price history exists (`acc_price_change_log`) but only for COGS changes, not selling price |
| **Missing** | Scheduled pricing sync | Not a cron job; manual via CLI `--pricing` or API trigger |
| **Wrong pattern** | Pricing is pull-only | Should be Notification-driven (ANY_OFFER_CHANGED event) |

**Risk: HIGH** — BuyBox loss cannot be detected or responded to automatically.

### 3.3 Notifications / Event-Driven Flow

| Aspect | Status | Detail |
|--------|--------|--------|
| **Missing** | Notifications API connector | ❌ No `notifications.py` in connectors |
| **Missing** | SQS/EventBridge destination | ❌ No AWS infrastructure for event delivery |
| **Missing** | ANY_OFFER_CHANGED subscription | Cannot detect BuyBox changes in real-time |
| **Missing** | ORDER_STATUS_CHANGE subscription | Would reduce order polling from 15 min to near-real-time |
| **Missing** | LISTINGS_ITEM_STATUS_CHANGE subscription | Cannot detect listing suppressions immediately |
| **Missing** | FBA_INVENTORY_AVAILABILITY_CHANGES | Cannot detect stockouts immediately |
| **Missing** | REPORT_PROCESSING_FINISHED | Must poll report status instead of reacting |

**Risk: CRITICAL** — This is the single biggest architectural gap. Every data flow is polling-based with 15-min to 24-hour lag. Event-driven architecture is table stakes for a Digital Twin.

### 3.4 Reports / Analytics

| Aspect | Status | Detail |
|--------|--------|--------|
| **Well implemented** | Reports API connector | Full create → poll → download → decompress pipeline |
| **Well implemented** | 8+ report types | Orders, listings, inventory, fees, settlements, traffic, returns, planning |
| **Well implemented** | Backfill via reports | `backfill_via_reports.py` — 30-day windows, resumable, dual MERGE |
| **Well implemented** | Sales & Traffic sync | `GET_SALES_AND_TRAFFIC_REPORT` daily with ASIN+SKU daily tables + rollup |
| **Partially implemented** | Settlement reports | Download exists, `build_settlement_summaries()` exists, but not scheduled |
| **Missing** | Business reports (sessions, conversion) | No `GET_FLAT_FILE_BROWSER_SESSIONS_BY_ASIN` or `GET_CONVERSION_RATE_REPORT` |
| **Missing** | Return reports scheduled sync | `GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA` is on-demand only |
| **Missing** | Inventory age reports | `GET_FBA_INVENTORY_AGED_DATA` not regularly synced |

**Risk: LOW-MEDIUM** — Reports backbone is solid. Scheduling gaps are easy to fill.

### 3.5 Finance / Profit

| Aspect | Status | Detail |
|--------|--------|--------|
| **Excellent** | Profit engine | 6,400-line CM1/CM2/NP SQL engine with 49 charge_type classifier |
| **Excellent** | Finance transaction sync | v2024-06-19 API, daily 03:00, hierarchical breakdown parsing |
| **Excellent** | COGS pipeline | 8-level price source cascade, 99% coverage |
| **Well implemented** | Settlement reconciliation | `build_settlement_summaries()` + `acc_fin_reconciliation_payout` |
| **Well implemented** | Ad spend enrichment | Rollups enriched from `acc_ads_product_day` (ACOS/TACoS) |
| **Well implemented** | FX conversion | NBP primary + ECB backup, daily sync, per-order FX stamping |
| **Partially implemented** | Return P&L | `return_tracker.py` tracks COGS recovered/written-off, but no scheduled auto-sync |
| **Missing** | Real-time margin alerts | No alert when margin drops below threshold on an order |

**Risk: LOW** — Finance/profit is ACC's strongest domain. Minor scheduling gaps.

### 3.6 Orders

| Aspect | Status | Detail |
|--------|--------|--------|
| **Excellent** | Order pipeline | 5-step pipeline every 15 min with watermark state |
| **Excellent** | Order line enrichment | COGS stamping, internal_sku mapping, FX conversion |
| **Well implemented** | Bulk backfill | Reports API TSV with dual MERGE |
| **Well implemented** | Multi-marketplace | 9 EU markets with per-market sync state |
| **Partially implemented** | Order status tracking | Basic status field, but no lifecycle state machine |
| **Missing** | Order change notifications | Polling-only; ORDER_STATUS_CHANGE subscription would reduce lag |

**Risk: LOW** — Orders work well. Notification subscription is the only meaningful upgrade.

### 3.7 Inventory

| Aspect | Status | Detail |
|--------|--------|--------|
| **Well implemented** | FBA inventory summaries | Daily sync with snapshot history |
| **Well implemented** | Inbound shipment tracking | FBA Inbound API v0 integrated |
| **Well implemented** | Inventory planning fallback | 3-tier: planning report → API summaries → last-known |
| **Partially implemented** | Stranded inventory | Report-based, but `GET_STRANDED_INVENTORY_UI_DATA` often `CANCELLED` |
| **Missing** | Real-time stockout detection | Daily snapshots miss intra-day stockouts |
| **Missing** | Reorder point automation | No automatic PO generation or alert |
| **Missing** | Multi-channel inventory | MFN stock levels not tracked |

**Risk: MEDIUM** — Daily snapshots are sufficient for planning but not for stockout prevention.

### 3.8 Content / A+

| Aspect | Status | Detail |
|--------|--------|--------|
| **Partially implemented** | Content editor | `content_ops.py` with `aplus_json` storage, AI generation |
| **Partially implemented** | PTD-driven attribute validation | Used in restructure, could power content compliance |
| **Missing** | A+ Content API read | ❌ No `aplusContent` connector — cannot fetch published A+ |
| **Missing** | A+ Content API write | ❌ Cannot push A+ to Amazon programmatically |
| **Missing** | Content sync / diff | Cannot detect when Amazon-side content diverges from ACC |
| **Missing** | Brand content audit | No automated image/text compliance checking |

**Risk: HIGH** — Content ops are editor-only. No API integration means no scalable Brand Content management.

---

## 4. PATTERN QUALITY REVIEW

### 4.1 Global Rate Limiting

| Aspect | Status | Location | Risk |
|--------|--------|----------|------|
| Per-endpoint throttle delays | ✅ Implemented | `client.py` (0.3s orders, 2s finances/pricing, 0.6s catalog) | LOW |
| `x-amzn-RateLimit-Limit` parsing | ✅ Implemented | `client.py` line ~357 — used in backoff calculation | LOW |
| `Retry-After` header honoring | ✅ Implemented | `client.py` — max(calculated_backoff, retry_after) | LOW |
| Global cross-endpoint token bucket | ❌ Missing | — | **MEDIUM** |
| Per-marketplace rate isolation | ❌ Missing | All marketplaces share one token budget | **MEDIUM** |

**Recommendation:** Add a `TokenBucketRateLimiter` in `client.py` with per-endpoint sustained rates matching SP-API documentation. Current per-page sleeps are a good proxy but don't protect burst scenarios across concurrent jobs.

### 4.2 Retry with Exponential Backoff + Jitter

| Aspect | Status | Location | Risk |
|--------|--------|----------|------|
| Exponential backoff | ✅ Implemented | `client.py`: `1s * 2^attempt`, cap 60s | LOW |
| Jitter (±25%) | ✅ Implemented | `client.py`: random ±25% of delay | LOW |
| Ads API backoff | ✅ Implemented | `ads_api/client.py`: `3s * 2^attempt`, +0–30% jitter | LOW |
| Retryable status codes | ✅ Implemented | 429, 500, 502, 503, 504 | LOW |
| Network error retry | ✅ Implemented | ConnectError, ReadTimeout, WriteTimeout, PoolTimeout | LOW |
| Max retries configurable | ✅ Implemented | Default 6 (SP-API), 5 (Ads GET), 6 (Ads POST) | LOW |

**Current status: EXCELLENT** — Both SP-API and Ads API have production-grade retry logic.

### 4.3 Priority Queues

| Aspect | Status | Location | Risk |
|--------|--------|----------|------|
| Job scheduling | ✅ APScheduler | `scheduler.py` — cron + interval triggers | LOW |
| Job priority levels | ❌ Missing | All jobs equal priority | **MEDIUM** |
| Queue back-pressure | ❌ Missing | No overflow handling | **MEDIUM** |
| Retryable job dispatch | ✅ Implemented | `dispatch_retryable_jobs` every 1 min | LOW |
| Task dependencies | ❌ Missing | Jobs run independently; profit calc doesn't wait for finance sync | **LOW** |

**Recommendation:** Not critical for current scale (single seller, 9 marketplaces). If ACC scales to multi-seller or adds repricing, a proper task queue (Celery/Redis or Dramatiq) will be needed.

### 4.4 Idempotent Processing

| Aspect | Status | Location | Risk |
|--------|--------|----------|------|
| SQL MERGE upserts | ✅ Pervasive | All sync services, backfill, ads_sync | LOW |
| Composite key dedup | ✅ Implemented | Orders: `amazon_order_id`, Lines: `(order_id, item_id)` | LOW |
| Report reuse | ✅ Implemented | `request_and_download_reuse_recent()` — skip if <3h old | LOW |
| Replay safety | ✅ Implemented | Watermark-based windows, `acc_backfill_report_progress` | LOW |
| ROW_NUMBER dedup | ✅ Implemented | `ads_sync.py` — CTE dedup before MERGE | LOW |

**Current status: EXCELLENT** — ACC can be restarted or replayed without data corruption.

### 4.5 Reports Instead of Chatty Polling

| Aspect | Status | Location | Risk |
|--------|--------|----------|------|
| Orders bulk backfill | ✅ Reports | `backfill_via_reports.py` | LOW |
| Product master | ✅ Reports | `GET_MERCHANT_LISTINGS_ALL_DATA` daily | LOW |
| Inventory snapshot | ✅ Reports | `GET_FBA_MYI_ALL_INVENTORY_DATA` daily | LOW |
| Traffic/sessions | ✅ Reports | `GET_SALES_AND_TRAFFIC_REPORT` daily | LOW |
| FBA returns | ✅ Reports | `GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA` (on-demand) | LOW |
| Fee estimation | ✅ Reports | `GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA` (on-demand) | LOW |
| Live order sync | ⚠️ Polling | Orders API every 15 min (justified for freshness) | LOW |
| Pricing | ⚠️ Polling | On-demand only, no scheduled sync | **MEDIUM** |

**Current status: GOOD** — Bulk operations correctly use Reports. Live sync uses API where freshness matters.

### 4.6 Notifications Instead of Polling

| Aspect | Status | Risk |
|--------|--------|------|
| ANY_OFFER_CHANGED | ❌ Not subscribed | **HIGH** — BuyBox loss undetected |
| ORDER_STATUS_CHANGE | ❌ Not subscribed | **MEDIUM** — 15-min lag acceptable but improvable |
| LISTINGS_ITEM_STATUS_CHANGE | ❌ Not subscribed | **HIGH** — suppressions undetected |
| FBA_INVENTORY_AVAILABILITY_CHANGES | ❌ Not subscribed | **MEDIUM** — daily snapshot acceptable for planning |
| REPORT_PROCESSING_FINISHED | ❌ Not subscribed | **LOW** — polling works fine |

**Recommendation: P0** — At minimum, subscribe to `ANY_OFFER_CHANGED` and `LISTINGS_ITEM_STATUS_CHANGE`. These are the highest-leverage notifications for a seller.

### 4.7 Normalization Layer

| Aspect | Status | Location | Risk |
|--------|--------|----------|------|
| Catalog response parser | ✅ Basic | `catalog.py` → `parse_catalog_item()` | LOW |
| Pricing response parser | ✅ Basic | `pricing_api.py` → `parse_competitive_pricing()` | LOW |
| Finance transaction parser | ✅ Good | `finances.py` → `parse_transaction_fees()` | LOW |
| Canonical DTOs | ❌ Missing | API response → direct SQL; no intermediate objects | **MEDIUM** |
| Version-abstracted interface | ❌ Missing | Connector tied to specific API versions | **MEDIUM** |
| Schema evolution strategy | ❌ Missing | API version bump = code changes | **MEDIUM** |

**Recommendation:** Introduce dataclass-based DTOs between connector and service layer. When Amazon migrates an API (e.g., Finances v0 → v2024-06-19, which ACC already handled), only the connector changes — the service layer stays stable.

### 4.8 Canonical Marketplace/Data Model

| Aspect | Status | Location | Risk |
|--------|--------|----------|------|
| Marketplace table | ✅ Implemented | `acc_marketplace` — 13 EU configs | LOW |
| Per-marketplace sync state | ✅ Implemented | `acc_order_sync_state`, `acc_return_sync_state` | LOW |
| Multi-marketplace product | ✅ Implemented | `acc_product` → `acc_offer` per marketplace | LOW |
| Family normalization (DE-first) | ✅ Excellent | `global_family*` 7-table model | LOW |
| Unified listing identity | ❌ Missing | Must join Product → Offer → Family to reconstruct | **MEDIUM** |
| Cross-marketplace price/inventory view | ❌ Missing | No single "catalog card" view per ASIN | **LOW** |

**Recommendation:** Consider a materialized `acc_listing_state` view/table that combines product + offer + inventory + family for a single ASIN across all marketplaces. This is the core Digital Twin entity.

### 4.9 Audit Logging with Request IDs

| Aspect | Status | Location | Risk |
|--------|--------|----------|------|
| SP-API usage tracking | ✅ Implemented | `sp_api_usage` table (daily aggregates) | LOW |
| `x-amzn-RequestId` capture | ❌ Missing | Not stored from response headers | **MEDIUM** |
| Per-request trace IDs | ❌ Missing | No request_id correlation | **MEDIUM** |
| FastAPI request middleware | ❌ Missing | No `X-Request-ID` injection | **LOW** |
| structlog centralized config | ❌ Missing | Each module initializes independently | **LOW** |
| Job execution audit | ✅ Implemented | `acc_al_jobs` table with create/running/success/failure | LOW |

**Recommendation:** 
1. Capture `x-amzn-RequestId` from SP-API response headers and store in a detail-level telemetry table (not just daily aggregates). This is critical for Amazon support escalation.
2. Add FastAPI middleware to inject `X-Request-ID` into every request context.

---

## 5. AMAZON DIGITAL TWIN ASSESSMENT

### 5.1 Definition

An **Amazon Digital Twin** is a local, always-current representation of your entire Amazon seller state — every listing, price, offer, inventory position, order, financial event, and content element — with the ability to:
1. **Read** any Amazon state locally (without API call)
2. **Detect** state changes within seconds (event-driven)
3. **React** to state changes automatically (rules engine)
4. **Write** corrections back to Amazon (bi-directional sync)

### 5.2 Scorecard

| Digital Twin Dimension | Current State | Freshness | Bi-Directional | Score |
|----------------------|--------------|-----------|----------------|-------|
| **SKU/ASIN Identity** | `acc_product` + `acc_amazon_listing_registry` | Daily (report) | Read-only | 7/10 |
| **Marketplace Normalization** | `acc_marketplace` + per-market sync | Good | — | 8/10 |
| **Family/Variation** | `global_family*` 7-table DE-first model | On-demand (restructure) | ✅ Write (restructure) | 9/10 |
| **Pricing State** | `acc_offer` per (sku, marketplace) | On-demand only | Read-only | 4/10 |
| **Listing State** | No dedicated table; attributes not synced | Stale | ✅ Write (restructure) | 3/10 |
| **Inventory State** | `acc_inventory_snapshot` daily | 24h lag | Read-only | 6/10 |
| **Finance State** | `acc_finance_transaction` + CM1/CM2/NP engine | Daily (03:00) | Read-only | 8/10 |
| **Order State** | `acc_order` + `acc_order_line` + watermark | 15 min lag | Read-only | 8/10 |
| **Ad Spend State** | `acc_ads_campaign_day` + `acc_ads_product_day` | Daily (07:00) | Read-only | 7/10 |
| **Content State** | `content_ops.py` (editor only) | Never synced from Amazon | Write-only (editor) | 2/10 |
| **Event Reactions** | ❌ None | — | — | 0/10 |

**Overall Digital Twin Score: 5.6/10**

### 5.3 Strengths

1. **Profit Engine is world-class** — 6,400-line CM1/CM2/NP engine with 49 charge_type classifier, 7 FBA cost buckets, revenue-weighted allocation, per-order FX stamping. This is the best module in ACC.
2. **Family/Variation model is enterprise-grade** — 7-table architecture with DE-first canonical model, coverage caching, issue tracking, and automated restructure pipeline (107 steps dry-run).
3. **Idempotent everywhere** — SQL MERGE upserts, watermark-based sync, report reuse, replay safety. ACC can crash and restart without data issues.
4. **Multi-marketplace from day one** — 13 marketplace configs, per-market sync state, per-market FX conversion. Not a single-market hack.
5. **Comprehensive SP-API coverage** — 10 API families integrated with async HTTP, exponential backoff, and structured error handling.

### 5.4 Structural Gaps (Must Fix for Digital Twin)

1. **No event layer** — Zero Notifications API subscriptions. Every data is polled. A Digital Twin must react, not just record.
2. **No listing state table** — Listing attributes, issues, compliance status, suppression state are not stored. The Digital Twin cannot answer "is this listing healthy right now?" without an API call.
3. **No pricing reaction loop** — Competitive pricing is fetched on-demand but never acted on. BuyBox loss = revenue loss, and ACC cannot detect or respond.
4. **No content sync** — A+ Content, bullet points, images, titles as published on Amazon are not read back. Content Twin doesn't exist.
5. **Coarse telemetry** — Daily API usage aggregates. For Amazon support escalation, per-request `x-amzn-RequestId` is essential.

### 5.5 What Must Be Added to Become a Proper Digital Twin

| Layer | Required Component | Complexity | Priority |
|-------|-------------------|-----------|----------|
| **Event** | SQS destination + Notifications API subscriptions | Medium | P0 |
| **State** | `acc_listing_state` materialized table (attributes, issues, status, last_synced) | Medium | P0 |
| **Reaction** | Repricing rules engine (min/max, margin target, BuyBox defense) | High | P0 |
| **State** | `acc_pricing_history` table with hourly tracking | Low | P1 |
| **Content** | A+ Content API read connector | Medium | P1 |
| **Normalization** | Canonical DTO layer between connectors and services | Medium | P1 |
| **Observability** | Per-request telemetry with `x-amzn-RequestId` | Low | P1 |
| **Automation** | Reorder point engine (inventory threshold → PO suggestion) | Medium | P2 |

---

## 6. P0 / P1 / P2 ROADMAP

### P0 — Must Build Now

#### P0.1 — Notifications API: Event Foundation
**Why:** Eliminates 15-min to 24h polling lag. Enables real-time BuyBox/listing/order reactions. This is the single highest-leverage investment.

**What:**
- Create `connectors/amazon_sp_api/notifications.py` — subscribe/manage/delete subscriptions
- Set up SQS destination (or EventBridge) for event delivery
- Create `services/notification_handler.py` — event router
- Subscribe to: `ANY_OFFER_CHANGED`, `LISTINGS_ITEM_STATUS_CHANGE`, `ORDER_STATUS_CHANGE`, `REPORT_PROCESSING_FINISHED`

**APIs:** Notifications API v1 (`/notifications/v1/subscriptions`, `/notifications/v1/destinations`)

**Business leverage:** Real-time BuyBox loss detection → immediate repricing response. Real-time listing suppression detection → immediate compliance fix. Order sync lag reduced from 15 min to <30s.

#### P0.2 — Continuous Listing State Sync
**Why:** Listings are the catalog core. Without continuous health tracking, suppressions and compliance issues go unnoticed for days.

**What:**
- Create `acc_listing_state` table: `(sku, marketplace_id, status, issues_json, attributes_json, last_synced_at, product_type, variation_theme)`
- Create `services/listing_sync.py` — scheduled job (01:30 daily) to sweep all active SKUs via Listings Items API
- Parse and store `issues[]` from each listing response
- Create alert rules for: `SUPPRESSED`, `MISSING_ATTRIBUTES`, `PRICING_ERROR`
- Wire into `LISTINGS_ITEM_STATUS_CHANGE` notification handler (P0.1) for real-time updates

**APIs:** Listings Items API v2021-08-01 (`GET /listings/2021-08-01/items/{sellerId}/{sku}`)

**Business leverage:** Detect listing suppressions within minutes instead of discovering them during manual checks. Automated compliance audit across 9 marketplaces.

**Touches:** New table + new service + alert system + scheduler + notification handler

#### P0.3 — Pricing Reaction Core (BuyBox Defense)
**Why:** BuyBox loss = direct revenue loss. Without automated response, the seller manually checks and adjusts — hours or days of lost sales.

**What:**
- Create `services/repricing_engine.py` — rule-based repricing
- Rules: min price (COGS + margin floor), max price (ceiling), BuyBox match, competitor tracking
- Create `acc_repricing_rule` table: `(sku, marketplace_id, strategy, min_price, max_price, target_margin_pct)`
- Create `acc_pricing_event` table: track every price change with reason
- Wire `ANY_OFFER_CHANGED` notification → evaluate rules → submit price update via Listings Items API PATCH or Feeds API
- Create `acc_pricing_history` table for hourly competitive price tracking

**APIs:** 
- Product Pricing API v0 (read current competitive landscape)
- Notifications API v1 (`ANY_OFFER_CHANGED`)
- Listings Items API v2021-08-01 (PATCH price) OR Feeds API (bulk price updates)

**Business leverage:** Automated BuyBox defense across 9 marketplaces. Expected 5-15% revenue recovery on competitive ASINs.

**Touches:** `pricing_api.py`, new `repricing_engine.py`, `notifications.py`, `listings.py`/`feeds.py`, new tables, scheduler, alert system

#### P0.4 — PTD Cache + Listing Issue Monitoring
**Why:** PTD calls are slow (~2-5s each). Caching enables fast validation in restructure pipeline and content ops. Issue monitoring catches compliance drift.

**What:**
- Create `acc_product_type_definition` cache table with TTL
- Modify `get_product_type_definition()` to check cache first (7-day TTL)
- Store listing `issues[]` from Listings Items API responses in `acc_listing_state`
- Create alert for new issues detected

**APIs:** Product Type Definitions API v2020-09-01

**Business leverage:** 10-100x faster restructure/content operations. Automated compliance drift detection.

---

### P1 — Next

#### P1.1 — Scheduled Pricing Sync
**Why:** Even before full repricing engine, regular pricing snapshots enable trend analysis and competitive intelligence.

**What:**
- Schedule `sync_pricing_buybox()` as daily cron job (06:00) across all active marketplaces
- Create `acc_pricing_snapshot` table for historical competitive pricing
- Add to sync_runner as `--pricing-daily` flag
- Dashboard: price trend charts, BuyBox ownership rate

**APIs:** Product Pricing API v0

**Business leverage:** Historical BuyBox ownership tracking, competitive price trends, pricing decision support.

#### P1.2 — A+ Content API Integration
**Why:** Enables scalable brand content management across 9 marketplaces. Currently content is editor-only with no Amazon sync.

**What:**
- Create `connectors/amazon_sp_api/aplus_content.py`
- Operations: `searchContentDocuments`, `getContentDocument`, `createContentDocument`, `updateContentDocument`, `submitContentDocumentAsinRelations`
- Create `acc_aplus_content` table: `(content_reference_key, marketplace_id, status, modules_json, asin_set, last_synced_at)`
- Wire into `content_ops.py` for push/pull of A+ content

**APIs:** A+ Content API v2020-11-01

**Business leverage:** Programmatic A+ Content management. Push DE content translations to 8 other marketplaces automatically. Content diff detection.

#### P1.3 — Canonical DTO Normalization Layer
**Why:** Current pattern of API response → direct SQL insert is fragile. Any Amazon API version change requires code edits in multiple places.

**What:**
- Create `app/dto/` directory with canonical dataclasses: `ListingDTO`, `OfferDTO`, `CatalogItemDTO`, `InventoryDTO`, `FinanceTransactionDTO`
- Refactor each connector's parser to return DTOs
- Refactor each service to consume DTOs (not raw dicts)
- DTOs become the stable interface; connector changes are isolated

**Business leverage:** Reduced maintenance on API version migrations. Enables testing with mock DTOs. Cleaner dependency graph.

#### P1.4 — Enhanced Telemetry + Request IDs
**Why:** `x-amzn-RequestId` is required for Amazon support escalation. Per-request telemetry enables debugging specific sync failures.

**What:**
- Capture `x-amzn-RequestId` from all SP-API response headers
- Create `sp_api_request_log` table: `(request_id, timestamp, endpoint, method, status, duration_ms, amzn_request_id, marketplace_id, error_text)`
- Retain 30 days, aggregate into existing `sp_api_usage` for long-term
- Add FastAPI middleware for internal `X-Request-ID` correlation

**Business leverage:** Faster debugging. Amazon support cases resolved faster with request IDs. API usage compliance proof.

#### P1.5 — Scheduled Return Sync + Inventory Age Reports
**Why:** Returns and inventory aging are cost drivers that need continuous monitoring, not on-demand snapshots.

**What:**
- Schedule `sync_fba_returns()` daily (06:30)
- Schedule `GET_FBA_INVENTORY_AGED_DATA` report weekly
- Create `acc_inventory_age_bucket` table for aging breakdown
- Wire aging costs into CM2 engine (already has `aged_inventory` bucket)

**APIs:** Reports API (`GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA`, `GET_FBA_INVENTORY_AGED_DATA`)

**Business leverage:** Automated return cost tracking, inventory aging awareness, proactive liquidation decisions.

---

### P2 — Later

#### P2.1 — Reorder Point Engine
**Why:** Automated inventory replenishment suggestions based on velocity, lead time, and safety stock.

**What:**
- Create `services/reorder_engine.py` with configurable lead times and safety stock
- Use `acc_inventory_snapshot` + `acc_inv_traffic_rollup` for velocity calculation
- Generate reorder alerts when stock hits threshold
- Optional: Inbound Shipment Plan API integration

**Business leverage:** Prevent stockouts, optimize FBA storage costs, automate purchase planning.

#### P2.2 — Multi-Channel Inventory (MFN)
**Why:** ACC tracks FBA-only inventory. MFN (merchant-fulfilled) stock is not monitored.

**What:**
- Integrate with ERP/WMS for MFN stock levels
- Create `acc_mfn_inventory` table
- Unified inventory view: FBA + MFN per SKU per marketplace

**Business leverage:** Complete inventory picture for capacity planning and fulfillment routing.

#### P2.3 — Settlement Auto-Reconciliation
**Why:** Settlement reports exist but aren't automatically reconciled with finance transactions.

**What:**
- Schedule `GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2` weekly
- Auto-match settlement lines with `acc_finance_transaction`
- Flag discrepancies in `acc_fin_reconciliation_payout`
- Dashboard: reconciliation status, unmatched amounts

**Business leverage:** Automated financial reconciliation, catch Amazon overbilling/underpayment.

#### P2.4 — Catalog Enrichment Pipeline
**Why:** Catalog data (images, BSR, dimensions, identifiers) is fetched on-demand. Systematic enrichment improves listing quality and competitive analysis.

**What:**
- Schedule weekly catalog refresh for all active ASINs
- Store `includedData=summaries,images,salesRanks,dimensions,identifiers` fields
- Create `acc_catalog_snapshot` table with history
- Use BSR trends for growth opportunity detection

**APIs:** Catalog Items API v2022-04-01

**Business leverage:** BSR trend analysis, image quality audit, EAN/UPC cross-reference, competitive ranking.

#### P2.5 — Brand Analytics API
**Why:** Search terms, market basket analysis, repeat purchase insights — available to Brand Registered sellers.

**What:**
- Create `connectors/amazon_sp_api/brand_analytics.py`
- Integrate: Search Terms Report, Market Basket, Repeat Purchase
- Create tables for analytics storage
- Dashboard: keyword performance, cross-sell opportunities

**APIs:** Brand Analytics API (via Reports API)

**Business leverage:** Keyword optimization, cross-sell insights, advertising strategy data.

---

## 7. RECOMMENDED IMPLEMENTATION ORDER

```
Phase 1 (P0) — Foundation for Digital Twin
════════════════════════════════════════════
Week 1-2:  P0.1 Notifications API: Event Foundation
           └── SQS destination + 4 subscription types
Week 2-3:  P0.2 Continuous Listing State Sync
           └── acc_listing_state table + daily sweep + issue alerting
Week 3-5:  P0.3 Pricing Reaction Core
           └── Repricing rules engine + ANY_OFFER_CHANGED handler + price write-back
Week 5-6:  P0.4 PTD Cache + Issue Monitoring
           └── Cache table + TTL + compliance alerting

Phase 2 (P1) — Analytics & Content Core
════════════════════════════════════════════
Week 7-8:  P1.1 Scheduled Pricing Sync
           └── Daily competitive pricing + BuyBox ownership history
Week 8-10: P1.2 A+ Content API Integration
           └── Read/write A+ + content diff + push translations
Week 10-11: P1.3 Canonical DTO Normalization Layer
           └── Refactor 8 connectors → DTOs → services
Week 11-12: P1.4 Enhanced Telemetry
           └── x-amzn-RequestId capture + per-request logging
Week 12-13: P1.5 Scheduled Return + Inventory Age
           └── Daily return sync + weekly age reports + CM2 wiring

Phase 3 (P2) — Scale & Intelligence
════════════════════════════════════════════
Week 14+:  P2.1-P2.5 in parallel as business needs dictate
           └── Reorder engine, MFN inventory, settlement recon, catalog enrichment, brand analytics
```

### Architecture Evolution Summary

```
CURRENT STATE (Mar 2026):
  Amazon APIs → Polling (15min–24h) → Raw SQL Upsert → Profit Engine → Dashboard
  
TARGET STATE (Digital Twin):
  Amazon APIs ─┬→ Notifications (real-time) ──→ Event Router ──→ Reaction Engine
               ├→ Reports (bulk, daily)      ──→ Canonical DTOs → State Tables
               └→ Listings/Pricing (targeted) ─→ Cache Layer   → Analytics
                                                                → Decision Layer
                                                                → Write-Back Loop
```

### Key Metric Targets

| Metric | Current | After P0 | After P1 | After P2 |
|--------|---------|----------|----------|----------|
| BuyBox loss detection | Never (manual) | <30 seconds | <30 seconds | <30 seconds |
| Listing suppression detection | Days (manual) | <5 minutes | <1 minute | <1 minute |
| Order sync lag | 15 minutes | <30 seconds | <30 seconds | <30 seconds |
| Pricing data freshness | On-demand | Real-time (events) | Real-time + history | Real-time + history |
| Content sync coverage | 0% | 0% | 80%+ | 95%+ |
| SP-API families used | 10 | 11 (+Notifications) | 12 (+A+ Content) | 13+ |
| Digital Twin score | 5.6/10 | 7.5/10 | 8.5/10 | 9.5/10 |

---

*Generated: 2026-03-09 | Codebase: ACC (Amazon Command Center) | Seller: KADAX (A1O0H08K2DYVHX) | Region: EU (9 marketplaces)*
