# ACC Backend Architecture Audit
### Date: 2026-03-11
### Scope: Full backend audit against Amazon Command Center target architecture
### Auditor: Backend Architect Agent

---

# 1. Executive Summary

1. **ACC is a real system, not a prototype.** ~100k lines of Python, 425+ API endpoints, 50+ database tables, 9 EU marketplaces, dual Amazon API coverage (SP-API + Ads API). This is production software serving an ecommerce operation.

2. **The SP-API connector layer is production-grade.** All 29 SP-API endpoints and 12 report types are covered with proper auth, exponential backoff, telemetry, and marketplace parameterization. This is the strongest layer of the codebase.

3. **The Event Backbone (SQS) exists and works.** Deterministic dedup, circuit breaker, handler registry, replay, adaptive polling. It is, however, underutilized — 90%+ of data flow still goes through cron-based batch sync rather than event-driven pipes.

4. **The financial engine is comprehensive but fragile.** Fee taxonomy (70+ charge types), 3-layer profit model (CM1/CM2/NP), FX service with staleness circuit breaker, and double-entry ledger generation all exist. However, the profit calculation has 3 competing code paths (sync_service.calc_profit, profit_service.recalculate_profit_batch, order_pipeline.step_calc_profit), creating divergence risk.

5. **There are at least 6 God modules** exceeding 2,500 lines: `profit_engine.py` (6,632), `content_ops.py` (4,906), `mssql_store.py` (4,297), `fba_ops/service.py` (3,921), `order_pipeline.py` (3,023), `family_mapper/restructure.py` (2,662). These need decomposition.

6. **Three parallel execution paths** exist for the same jobs: APScheduler (in-process), Celery Beat, and CLI sync_runner. They share logic but use different wiring, creating maintenance burden and divergence risk.

7. **Schema management is chaotic.** 7 Alembic migrations exist, but 14+ service modules create tables inline via `ensure_*_schema()` DDL embedded in business logic. Two worlds coexist with no migration strategy.

8. **There is no canonical product model.** `acc_product` is a flat master table (ASIN-keyed). `global_family` is a separate system for DE-canonical family mapping. `acc_amazon_listing_registry` is a third product store synced from Google Sheets. `acc_offer` is a fourth view of the same product. None of these form a cohesive canonical model with source→canonical→target transformations.

9. **Marketplace logic is 95% parameterized** via `MARKETPLACE_REGISTRY` — a strong design. The 5% of hardcoded marketplace logic (Polish leak detection, MAG_/FBA_ prefix swaps, DE-canonical family assumption) is documented and intentional.

10. **The project root is polluted.** 95 `tmp_*` files, 30 `_*` diagnostic files, 18 log files, and 11 `backfill_*` scripts sit alongside production code. No `.gitignore` discipline.

11. **Test coverage is minimal.** 45 test files for ~100k lines of code. No CI pipeline visible. Tests exist but are not the safety net they should be.

12. **The target architecture (30 modules) is ~60% covered** by existing code, but almost nothing is in the correct shape. Most modules exist as tangled slices inside God-class files rather than cleanly separated components.

13. **Decision/Intelligence layer is surprisingly advanced.** Strategy detection (20+ opportunity types), decision intelligence feedback loop, seasonality engine, and executive analytics all exist. However, three modules (strategy_service, executive_service, decision_intelligence_service) operate on the same `growth_opportunity` table without coordination.

14. **The courier/logistics subsystem is a separate product** embedded inside ACC — 10 shipment tables, DHL/GLS integrations, courier cost estimation with ML, billing import, order-universe linking. It's well-built but represents ~15% of codebase complexity.

15. **READ UNCOMMITTED globally** — every query runs without locks. Acceptable for a read-heavy analytics workload on Azure SQL, but dangerous for financial consistency guarantees. Finance writes should use at least READ COMMITTED.

---

# 2. Current ACC Module Inventory

| Current Module / Area | Purpose in Practice | Key Files / Folders | Amazon Endpoints Used | Quality | Notes |
|---|---|---|---|---|---|
| **SP-API Client** | Base HTTP client with auth, backoff, telemetry | `connectors/amazon_sp_api/client.py` | LWA token endpoint | ★★★★★ | Best-in-class |
| **Orders Connector** | Fetch orders + order items | `connectors/amazon_sp_api/orders.py` | getOrders, getOrderItems | ★★★★★ | Auto-pagination |
| **Listings Connector** | CRUD listings + product type definitions | `connectors/amazon_sp_api/listings.py` | getListingsItem, putListingsItem, patchListingsItem, deleteListingsItem, getDefinitionsProductType | ★★★★★ | Full CRUD |
| **Catalog Connector** | Fetch catalog items + search | `connectors/amazon_sp_api/catalog.py` | getCatalogItem, searchCatalogItems | ★★★★★ | Batch + parser |
| **Pricing Connector** | Competitive pricing, offers, fee estimates | `connectors/amazon_sp_api/pricing_api.py` | getCompetitivePricing, getItemOffers, getFeesEstimate | ★★★★★ | Complete |
| **Inventory Connector** | FBA inventory summaries | `connectors/amazon_sp_api/inventory.py` | getInventorySummaries | ★★★★☆ | Minimal but correct |
| **Finances Connector** | Transaction + legacy event fetching | `connectors/amazon_sp_api/finances.py` | v2024-06-19 transactions + v0 events | ★★★★★ | Dual-version |
| **Reports Connector** | Report lifecycle (create→poll→download) | `connectors/amazon_sp_api/reports.py` | createReport, getReport, getReportDocument + 12 report types | ★★★★★ | Smart reuse |
| **Feeds Connector** | Feed submission pipeline | `connectors/amazon_sp_api/feeds.py` | createFeedDocument, createFeed, getFeed | ★★★★★ | 3-step pipeline |
| **Notifications Connector** | SQS/EventBridge destinations + subscriptions | `connectors/amazon_sp_api/notifications.py` | 9 notification types, full CRUD | ★★★★★ | Auth switching |
| **Brand Analytics** | Search term reports | `connectors/amazon_sp_api/brand_analytics.py` | GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT | ★★★★☆ | Dual-format parser |
| **Inbound Connector** | FBA shipment tracking | `connectors/amazon_sp_api/inbound.py` | getShipments, getShipmentItems | ★★★★☆ | Minimal |
| **Ads API Client** | Base HTTP for Amazon Ads | `connectors/amazon_ads_api/client.py` | LWA token (Ads) | ★★★★☆ | No telemetry |
| **Ads Profiles** | Profile ↔ marketplace mapping | `connectors/amazon_ads_api/profiles.py` | GET /v2/profiles | ★★★★★ | Clean mapping |
| **Ads Campaigns** | SP/SB/SD campaign listing | `connectors/amazon_ads_api/campaigns.py` | SP/SB/SD campaign list endpoints | ★★★★★ | Fallback design |
| **Ads Reporting** | Campaign + product daily reports | `connectors/amazon_ads_api/reporting.py` | POST /reporting/reports + 5 report types | ★★★★★ | DRY impl |
| **Order Pipeline** | 6-step order sync + enrichment + profit | `services/order_pipeline.py` (3023 loc) | Orders API, Finances (indirect) | ★★★★☆ | Canonical order path |
| **Sync Service** | Legacy SP-API sync orchestrator | `services/sync_service.py` (2567 loc) | Orders, Inventory, Pricing, Catalog, FX | ★★★☆☆ | Partially superseded |
| **Profit Engine** | 3-layer profit model (CM1/CM2/NP) + queries + export | `services/profit_engine.py` (6632 loc) | None (reads pre-synced data) | ★★★★☆ | God module |
| **Profitability Service** | SKU/marketplace profit rollups + alerts + dashboards | `services/profitability_service.py` (1637 loc) | None | ★★★★☆ | Overlap with profit_engine |
| **Finance Center** | Ledger, settlements, reconciliation | `services/finance_center/` (2524 loc) | None | ★★★★☆ | Well-structured |
| **Event Backbone** | SQS-based event ingestion + dispatch | `services/event_backbone.py` (1255 loc) | SQS (AWS) | ★★★★★ | Best module |
| **Pricing State** | Price snapshot history + buybox tracking | `services/pricing_state.py` | Competitive Pricing API | ★★★★☆ | Clean event integration |
| **Pricing Rules** | Guardrail-based pricing recommendations | `services/pricing_rules.py` | None | ★★★★☆ | Human-in-loop |
| **Listing State** | Canonical listing status tracking | `services/listing_state.py` | Listings Items API | ★★★★★ | Event-sourced |
| **Content Ops** | Content lifecycle + publish + compliance | `services/content_ops.py` (4906 loc) | Catalog API, Listings API | ★★★☆☆ | God module |
| **FBA Ops** | FBA inventory + replenishment + inbound + fee audit | `services/fba_ops/` (3921 loc) | Reports API, Inventory API | ★★★☆☆ | Monolithic |
| **Family Mapper** | Cross-marketplace product family mapping | `services/family_mapper/` (8 files) | Catalog API | ★★★★☆ | Well-structured package |
| **Ads Sync** | Ads data synchronization | `services/ads_sync.py` | All Ads API endpoints | ★★★★☆ | Clean MERGEs |
| **Strategy Service** | 20+ opportunity type detection | `services/strategy_service.py` (1288 loc) | None | ★★★★☆ | Overlaps with executive |
| **Decision Intelligence** | Execution feedback loop | `services/decision_intelligence_service.py` (833 loc) | None | ★★★★☆ | Clean design |
| **Executive Service** | CEO-level KPIs + health scoring | `services/executive_service.py` (843 loc) | None | ★★★★☆ | Overlaps with strategy |
| **Seasonality** | Demand seasonality profiling | `services/seasonality_service.py` + `seasonality_opportunity_engine.py` | None | ★★★★☆ | Clean |
| **Tax Compliance** | EU VAT, OSS, evidence, filing | `services/tax_compliance/` (11 files) | None | ★★★★★ | Best architecture |
| **Return Tracker** | Return lifecycle + COGS adjustment | `services/return_tracker.py` | FBA Returns Report | ★★★★☆ | Good financial model |
| **Guardrails** | 23+ runtime health checks | `services/guardrails.py` + `guardrails_backbone.py` | None | ★★★★★ | Excellent observability |
| **Courier/Logistics** | DHL + GLS integration + cost estimation | 12+ courier service files | DHL24 SOAP, GLS REST | ★★★★☆ | Separate subsystem |
| **Scheduler** | ~35 scheduled jobs with leader election | `scheduler.py` (1675 loc) | SQS (indirect) | ★★★★☆ | Megafile |
| **MSSQL Store** | Central DB helper with 80+ job dispatches | `connectors/mssql/mssql_store.py` (4297 loc) | None | ★★★☆☆ | God module |
| **Config** | Pydantic settings with feature flags | `core/config.py` | N/A | ★★★★★ | Well-structured |
| **Security** | JWT, RBAC, marketplace-scoped tokens | `core/security.py` | N/A | ★★★★☆ | Solid |
| **Fee Taxonomy** | 70+ Amazon charge type classification | `core/fee_taxonomy.py` | N/A | ★★★★★ | Accounting brain |

---

# 3. Target Module Mapping (1–30)

| # | Target Module | Status | Current Code Location | Main Problems | Recommendation | Priority |
|---|---|---|---|---|---|---|
| 1 | **Account Hub** | PARTIALLY_EXISTS | `core/config.py` (credentials), `models/marketplace.py`, `seller_registry.py` | No unified account abstraction. Single seller account assumed. Marketplace registry is a static table, not a dynamic hub. No multi-seller support. | EXTRACT_TO_MODULE — Create `app/platform/account_hub.py` with Seller model, credential vault, marketplace registry, and SP-API token lifecycle. | P1 |
| 2 | **SP-API Gateway** | EXISTS_GOOD | `connectors/amazon_sp_api/` (12 files) | No connection pooling (new httpx client per request). Manual `asyncio.sleep()` in some modules instead of centralized rate governor. | REFACTOR_IN_PLACE — Add connection pooling and extract per-endpoint sleep into a rate governor config. | P2 |
| 3 | **Ads API Gateway** | EXISTS_GOOD | `connectors/amazon_ads_api/` (4 files) | Missing telemetry (unlike SP-API). No 5xx retry. | REFACTOR_IN_PLACE — Add SP-API-style telemetry and 5xx retry to Ads base client. | P2 |
| 4 | **Rate Limit Manager** | EXISTS_BUT_WEAK | `core/rate_limit.py` (login only), SP-API client backoff, manual sleeps | Rate limiting exists per-connector but there's no centralized rate manager across all Amazon API calls. SP-API rate headers (`x-amzn-RateLimit-Limit`) are read but not used for adaptive throttling. | EXTRACT_TO_MODULE — Build unified `platform/rate_governor.py` that tracks per-endpoint quotas across all SP-API and Ads calls. | P1 |
| 5 | **Event Bus Orchestrator** | EXISTS_BUT_WRONG_SHAPE | `services/event_backbone.py` | Excellent ingestion + dispatch, but: (a) only handles SP-API notifications, not internal domain events; (b) batch syncs don't emit events; (c) no pub/sub for inter-module communication; (d) uses sync pyodbc, not async. Should be the central nervous system, currently it's a notification receiver. | REBUILD_FROM_SCRATCH — Keep SQS poller + dedup + circuit breaker, but wrap in a proper domain event bus that handles both external (SQS) and internal (in-process) events. All batch syncs should emit events on completion. | P1 |
| 6 | **Job Scheduler & Async Runner** | EXISTS_BUT_WRONG_SHAPE | `scheduler.py`, `worker.py`, `sync_runner.py`, `mssql_store.py` (job functions), `jobs/*.py` | Three parallel execution paths. `scheduler.py` is 1675 lines mixing all domains. `mssql_store.run_job_type()` dispatches 80+ job types in one switch-case. Celery tasks use deprecated `asyncio.get_event_loop()`. | REFACTOR_IN_PLACE — (1) Split scheduler into domain-specific modules. (2) Delete Celery Beat schedule (keep APScheduler as canonical). (3) Fix deprecated async patterns. (4) Add retry-at-scheduler-level with backoff. | P1 |
| 7 | **Canonical Product Model** | MISSING | `models/product.py` is a flat master table; `global_family` is DE-canonical families; `acc_amazon_listing_registry` is a GSheet import; `acc_offer` is marketplace state | No canonical model that represents: one product → many marketplace representations → with source/canonical/target transformation. Product, Offer, Listing, and Family are four disconnected tables. No product lifecycle (draft → active → discontinued). | REBUILD_FROM_SCRATCH — Design a proper canonical product model: `CanonicalProduct` (brand owner's product), `MarketplacePresence` (per-market listing), `OfferSnapshot` (price/buybox state), `FamilyAssignment` (variation hierarchy). This is the single most important architectural gap. | P0 |
| 8 | **Marketplace Mapping Engine** | PARTIALLY_EXISTS | `family_mapper/matching.py`, `sync_service._find_or_create_product`, `order_pipeline.step_map_products` | Matching exists in family_mapper for variation families. SKU mapping exists in order_pipeline with 4-source cascade (Ergonode → GSheet → Baselinker → ASIN). But there's no unified mapping engine — each module has its own lookup strategy. | EXTRACT_TO_MODULE — Unify all mapping logic into `platform/marketplace_mapping.py`. Single entry point: given (sku, marketplace) → (canonical_product, internal_sku, ean, family_id). | P1 |
| 9 | **Listing Snapshot Store** | EXISTS_GOOD | `services/listing_state.py` | Well-designed event-sourced model with history. Status tracking from reports, SP-API events, and on-demand refresh. Only gap: no content snapshot (just status/issues). | KEEP_AS_IS — Expand to capture content snapshots (title, bullets, images) alongside status. | P2 |
| 10 | **Offer & Price Snapshot Store** | EXISTS_GOOD | `services/pricing_state.py`, `models/offer.py` | `pricing_state` has proper snapshot history with archive. `acc_offer` captures current state. BuyBox tracking exists. Missing: competitive landscape storage (all sellers, not just own offer). | REFACTOR_IN_PLACE — Add competitor offer snapshots alongside own-offer tracking. | P2 |
| 11 | **Inventory & Supply Snapshot Store** | EXISTS_BUT_WEAK | `models/inventory.py`, `jobs/sync_inventory.py`, `services/fba_ops/service.py`, `services/manage_inventory.py` | Three overlapping inventory implementations: (a) `sync_inventory.py` job with business logic, (b) `fba_ops.sync_inventory_cache` with report-based data, (c) `manage_inventory` with traffic/DOI. No single inventory truth. No upsert protection in Celery job. | MERGE_WITH_ANOTHER_MODULE — Consolidate into `data/inventory_store.py`. Single ingestion path: SP-API Inventory API + FBA Reports → normalize → upsert into `acc_inventory_snapshot` with proper dedup. | P1 |
| 12 | **Finance & Margin Ledger** | EXISTS_BUT_WRONG_SHAPE | `services/finance_center/`, `models/finance.py`, `core/fee_taxonomy.py`, `services/profit_engine.py`, `services/profitability_service.py`, `order_pipeline.step_bridge_fees` | Fee taxonomy is excellent. Finance Center generates ledger entries. BUT: profit calculation has 3 code paths. Fee bridging (finance transaction → order line) is buried in order_pipeline (400+ lines of raw SQL). Profitability rollups duplicate profit_engine queries. `acc_finance_transaction` has no dedup constraint. | REFACTOR_IN_PLACE — (1) Eliminate profit_service.py legacy path. (2) Extract fee bridging from order_pipeline into finance_center. (3) Add unique constraint on finance transactions. (4) Merge profitability_service queries into profit_engine as the single query interface. | P0 |
| 13 | **Orders Ingestion Module** | EXISTS_GOOD | `services/order_pipeline.py` steps 1-4, `connectors/amazon_sp_api/orders.py` | Canonical path. Per-marketplace sync state. Gap detection. Hash-based change detection. The 6-step pipeline is well-designed. | KEEP_AS_IS | P3 |
| 14 | **Listings Ingestion Module** | EXISTS_BUT_WEAK | `services/sync_service.py` (report-based), `services/listing_state.py` (event-based), `services/amazon_listing_registry.py` (GSheet) | Three ingestion paths for listing data, none of which produce a unified listing snapshot. `sync_service` fetches listing reports, `listing_state` handles SP-API events, and listing_registry imports from Google Sheets. | EXTRACT_TO_MODULE — Create `ingestion/listings_ingestion.py` that unifies: report-based bulk sync + real-time events + registry mapping into a single listing state per (sku, marketplace). | P1 |
| 15 | **Catalog Intelligence Module** | PARTIALLY_EXISTS | `connectors/amazon_sp_api/catalog.py`, `services/family_mapper/de_builder.py`, `services/ptd_cache.py`, `services/ptd_validator.py` | Catalog data is fetched but not systematically stored. Product type definitions are cached. Family mapping uses catalog data for cross-market matching. No persistent catalog snapshot store. | EXTRACT_TO_MODULE — Build `ingestion/catalog_intelligence.py` with: catalog item snapshots, BSR history, product type cache, category tree. Feed into canonical product model. | P1 |
| 16 | **Pricing Intelligence Module** | EXISTS_GOOD | `services/pricing_state.py`, `services/pricing_rules.py`, `connectors/amazon_sp_api/pricing_api.py` | Clean separation: capture (pricing_state) → evaluate (pricing_rules) → recommend. Event backbone integration for real-time notifications. Missing: competitive summary batch via `getCompetitiveSummary` new API. | KEEP_AS_IS — Add `getCompetitiveSummary` batch API support. | P2 |
| 17 | **Inventory Ingestion Module** | EXISTS_BUT_WEAK | `jobs/sync_inventory.py`, `services/fba_ops/service.py` | See #11 — duplicate ingestion with no dedup. | MERGE_WITH_ANOTHER_MODULE (see #11) | P1 |
| 18 | **Reports Ingestion Module** | EXISTS_GOOD | `connectors/amazon_sp_api/reports.py` | 12 report types supported. Smart reuse. Proper lifecycle. Used by 5+ services. | KEEP_AS_IS | P3 |
| 19 | **Notifications Ingestion Module** | EXISTS_GOOD | `connectors/amazon_sp_api/notifications.py`, `services/event_backbone.py` | Full lifecycle: destinations → subscriptions → SQS polling → ingest → dispatch. 9 notification types. Dedup + circuit breaker. | KEEP_AS_IS — Increase SQS polling frequency from 2min to 30s for latency-critical events. | P2 |
| 20 | **Ads Reporting Ingestion Module** | EXISTS_GOOD | `services/ads_sync.py`, `connectors/amazon_ads_api/reporting.py` | Profiles → campaigns → daily metrics → product metrics. SP/SB/SD coverage. PLN conversion. MERGE-based upserts. | KEEP_AS_IS | P3 |
| 21 | **Catalog Health Monitor** | PARTIALLY_EXISTS | `services/guardrails.py` (check_unknown_fee_types, check_order_vs_finance_totals), `services/listing_state.py` (health_summary) | Listing health exists via `get_listing_health_summary()`. Guardrails check pipeline freshness. No dedicated catalog health monitor with suppression tracking, content completeness scoring, image quality analysis. | EXTRACT_TO_MODULE — Build `ops_intelligence/catalog_health.py` pulling from listing_state + content_ops + family_mapper to produce a unified catalog health scorecard. | P2 |
| 22 | **Listing Diff & Anomaly Detector** | PARTIALLY_EXISTS | `services/listing_state.py` (history), `services/content_ops.py` (get_content_diff) | Content diffing exists in content_ops. Listing state history captures transitions. No automated anomaly detection (e.g., unexpected title changes, hijacked listings, image swaps). | DEFER_TO_PHASE_2 | P2 |
| 23 | **Buy Box & Offer Radar** | PARTIALLY_EXISTS | `services/pricing_state.py` (buybox_overview), `models/offer.py` (has_buybox) | BuyBox ownership tracked per snapshot. No competitor tracking, no win-rate trends, no alert on sustained BuyBox loss. | DEFER_TO_PHASE_2 | P2 |
| 24 | **Inventory Risk Engine** | PARTIALLY_EXISTS | `services/fba_ops/service.py` (replenishment), `services/manage_inventory.py` (DOI), `services/strategy_service.py` (inventory opportunities) | DOI calculation exists. Replenishment suggestions exist. Strategy detects low-stock opportunities. No unified risk scoring (stockout probability, overstock cost, aging write-off risk). | EXTRACT_TO_MODULE | P2 |
| 25 | **Profit Engine** | EXISTS_BUT_WRONG_SHAPE | `services/profit_engine.py`, `services/profit_service.py`, `services/profitability_service.py`, `order_pipeline.step_calc_profit` | Three code paths for profit calculation. God module at 6632 lines mixes calculation, queries, export, and cost model management. Profitability_service duplicates query patterns. | REFACTOR_IN_PLACE — (1) Kill profit_service.py legacy. (2) Split profit_engine into: `profit_calculator.py` (CM1/CM2/NP logic), `profit_query.py` (API queries), `profit_export.py` (XLSX), `cost_model.py` (config). (3) Merge profitability_service query functions into profit_query. | P0 |
| 26 | **Refund / Fee Anomaly Engine** | PARTIALLY_EXISTS | `services/return_tracker.py`, `services/fba_ops/fba_fee_audit.py`, `services/guardrails.py` | Returns tracking is good. Fee audit exists in FBA ops. Guardrails check for unknown fees. No unified refund anomaly detection (sudden refund rate spikes, serial returners, reimbursement claim automation). | DEFER_TO_PHASE_2 | P2 |
| 27 | **Repricing Decision Engine** | PARTIALLY_EXISTS | `services/pricing_rules.py` (guardrails + recommendations), `services/strategy_service.py` (pricing opportunities) | Pricing rules evaluate guardrails and generate recommendations. Strategy detects pricing opportunities. No automated repricing execution — always human-in-loop. No dynamic repricing algorithm (min/max strategies, competitor tracking, margin-aware). | DEFER_TO_PHASE_3 | P3 |
| 28 | **Content Optimization Engine** | PARTIALLY_EXISTS | `services/content_ops.py` (full lifecycle), `services/ai_service.py` (recommendations) | Content task management, version control, publish pipeline, policy validation all exist. AI can generate recommendations. No automated content scoring, no SEO analysis, no competitor content benchmarking. | REFACTOR_IN_PLACE — Split content_ops.py (4906 loc) into: task mgmt, version mgmt, publish pipeline, policy engine, compliance queue. | P1 |
| 29 | **Feed / Listings Action Center** | EXISTS_BUT_WEAK | `connectors/amazon_sp_api/feeds.py`, `services/content_ops.py` (publish push), `services/family_mapper/restructure.py` | Feed submission works. Content publish uses circuit breaker. Family restructure executes feeds. No unified action center: scattered across content_ops (publish), family_mapper (restructure), and direct SP-API calls. | EXTRACT_TO_MODULE — Create `execution/action_center.py` as single entry point for all write operations to Amazon (price changes, content updates, family restructures, inventory adjustments). Central audit trail, circuit breaker, and rate limiting. | P1 |
| 30 | **Alerting / Cases / Operator Console** | PARTIALLY_EXISTS | `services/guardrails.py` (health checks), `models/alert.py`, `mssql_store.py` (alert CRUD), `services/fba_ops/service.py` (cases), `services/courier_alerts.py` | Alerts exist but as simple threshold checks. Cases exist in FBA ops. Guardrails provide system health. No unified operator console backend — alerts, cases, actions, and approvals are scattered across modules. | EXTRACT_TO_MODULE — Create `ops_console/` package: unified alert feed, case management, action queue, approval workflows, operator dashboard API. | P2 |

---

# 4. Architecture Smells and Structural Risks

## Coupling

| Issue | Severity | Location |
|---|---|---|
| `scheduler.py` imports 20+ service modules, creating a dependency fan-out | HIGH | `app/scheduler.py` |
| `mssql_store.run_job_type()` dispatches 80+ job types — universal coupling point | HIGH | `connectors/mssql/mssql_store.py` |
| 3 services share `growth_opportunity` table without coordination layer | MEDIUM | `strategy_service`, `executive_service`, `decision_intelligence_service` |
| `content_ops.py` directly calls SP-API from within content management CRUD | MEDIUM | `services/content_ops.py` |
| `order_pipeline.step_bridge_fees` embeds 400+ lines of finance logic | MEDIUM | `services/order_pipeline.py` |
| `main.py` lifespan imports ~18 service modules for schema creation | LOW | `app/main.py` |

## Async / Queues

| Issue | Severity | Location |
|---|---|---|
| Event backbone only handles SP-API notifications, not internal domain events — 90% of data flow bypasses it | HIGH | `services/event_backbone.py` |
| 4 Celery jobs use deprecated `asyncio.get_event_loop().run_until_complete()` — will break on Python 3.12+ | HIGH | `jobs/calc_profit.py`, `sync_finances.py`, `sync_inventory.py`, `sync_purchase_prices.py` |
| Three parallel execution paths (APScheduler, Celery Beat, CLI) for same operations | MEDIUM | `scheduler.py`, `worker.py`, `sync_runner.py` |
| SQS polling at 2-min interval — latency-sensitive events (offer changes, order status) may be stale | MEDIUM | `scheduler.py._poll_sqs_notifications` |
| Time-coupled job dependencies (prices at 02:00 → finance at 03:00 → profit at 05:00) without explicit dependency checks | MEDIUM | `scheduler.py` |
| No dead-letter handling for failed scheduled jobs — must wait until next nightly run | MEDIUM | `scheduler.py` |

## Persistence

| Issue | Severity | Location |
|---|---|---|
| 14+ services create tables inline via `ensure_*_schema()` DDL — dual world with Alembic | HIGH | Nearly all services |
| `acc_finance_transaction` has no unique constraint — duplicate inserts possible | HIGH | `models/finance.py` |
| `InventorySnapshot` has no upsert — `sync_inventory.py` job always `db.add()` creating duplicates | HIGH | `jobs/sync_inventory.py`, `models/inventory.py` |
| `Alert.marketplace_id` and `JobRun.marketplace_id` are plain strings, not FK — no referential integrity | MEDIUM | `models/alert.py`, `models/job.py` |
| Family tables lack `acc_` prefix convention | LOW | `models/family.py` |
| `READ UNCOMMITTED` globally — dirty reads on financial data | MEDIUM | `core/database.py` |
| JSON stored as `Text` columns in 5+ models — loses query capability | LOW | Multiple models |

## API Integration

| Issue | Severity | Location |
|---|---|---|
| No connection pooling in SP-API/Ads clients (new httpx client per request) | MEDIUM | `connectors/amazon_sp_api/client.py`, `connectors/amazon_ads_api/client.py` |
| Manual `asyncio.sleep()` rate limiting in 4 connector modules — not centralized | LOW | `orders.py`, `catalog.py`, `pricing_api.py`, `finances.py` |
| Amazon Ads client lacks 5xx retry and telemetry (SP-API has both) | LOW | `connectors/amazon_ads_api/client.py` |
| `getCompetitiveSummary` (batch pricing) endpoint not used — chatty per-ASIN calls instead | LOW | `connectors/amazon_sp_api/pricing_api.py` |

## Domain Modeling

| Issue | Severity | Location |
|---|---|---|
| **No canonical product model** — Product, Offer, Listing, Family, Registry are 5 disconnected representations | CRITICAL | Multiple models |
| SKU mapping has 4 competing lookup strategies with no unified interface | HIGH | `order_pipeline`, `sync_service`, `amazon_listing_registry`, `family_mapper` |
| Profit has 3 calculation paths (`calc_profit`, `recalculate_profit_batch`, `step_calc_profit`) | HIGH | 3 files |
| Decision layer uses non-prefixed table names (`growth_opportunity`, `executive_daily_metrics`) | LOW | `strategy_service`, `executive_service`, `decision_intelligence_service` |

## Observability

| Issue | Severity | Location |
|---|---|---|
| Guardrails system is excellent (23+ checks) but results not exposed via alerts/PagerDuty | LOW | `services/guardrails.py` |
| SP-API telemetry records every call — good. Ads API has no telemetry | LOW | Connectors |
| No distributed tracing (correlation IDs exist in event backbone only) | MEDIUM | System-wide |

## Config / Secrets

| Issue | Severity | Location |
|---|---|---|
| Google Sheets URL with specific GID hardcoded in config — fragile external dependency | MEDIUM | `core/config.py`, `services/amazon_listing_registry.py` |
| Purchase price XLSX path is a network share (`N:\Analityka\...`) — environment-specific | LOW | `core/config.py` |
| All credentials in env vars — correct pattern | OK | `core/config.py` |

## Performance

| Issue | Severity | Location |
|---|---|---|
| New httpx client per SP-API/Ads request — connection setup overhead | MEDIUM | Connectors |
| `profit_engine.py` loads all data into Python for processing — no DB-level aggregation for large date ranges | MEDIUM | `services/profit_engine.py` |
| `seller_registry` global cache with no TTL — stale until restart | LOW | `services/seller_registry.py` |
| In-memory cache pattern reimplemented 5+ times — no shared utility | LOW | Multiple services |

## Testability

| Issue | Severity | Location |
|---|---|---|
| 45 test files for ~100k lines of code — minimal coverage | HIGH | `tests/` |
| Services use raw pyodbc connections — hard to mock for unit tests | MEDIUM | All services using `connect_acc()` |
| No dependency injection — services import singletons directly | MEDIUM | System-wide |
| `profit_service.py` kept only for tests — dead code in production path | LOW | `services/profit_service.py` |

---

# 5. Missing Core Building Blocks

| Priority | Missing Block | Why It Blocks Command Center | Impact |
|---|---|---|---|
| **P0** | **Canonical Product Model** | Every module has its own product representation. No source → canonical → target transformation exists. Cannot build reliable cross-market intelligence without unified product identity. | Blocks modules 7, 8, 15, 21, 24, 25, 27, 28 |
| **P0** | **Unified Profit Calculator** (single path) | Three competing profit calculation paths create divergence risk and make auditing impossible. Nobody can answer "which profit number is right?" | Blocks trust in financial data |
| **P1** | **Internal Domain Event Bus** | Event backbone only handles SP-API notifications. Batch syncs don't emit events. No pub/sub between modules. The system is still 90% cron-driven, not event-driven. | Blocks reactive architecture |
| **P1** | **Centralized Schema Migration** | 14+ services embed DDL. No way to know current schema state. No rollback capability. | Blocks deployment confidence |
| **P1** | **Action Center** (unified write gateway) | Writes to Amazon (prices, content, feeds, family restructures) are scattered across 4+ modules with no central audit trail or rate governance. | Blocks operational safety |
| **P1** | **Shared Utility Library** | `_f()`, `_fetchall_dict()`, `_mkt_code()`, `_connect()` duplicated in 8+ files. In-memory cache reimplemented 5 times. | Blocks maintainability |
| **P2** | **Centralized Rate Governor** | Per-endpoint rate limits handled ad-hoc (manual sleeps). No adaptive throttling based on response headers. | Blocks API efficiency |
| **P2** | **Competitor Intelligence Store** | Only own-offer tracked. No persistent competitor pricing, seller count trends, or BuyBox win-rate history. | Blocks modules 23, 27 |
| **P2** | **Integration Test Suite** | 45 test files for 100k LOC. No CI pipeline. Cannot refactor safely. | Blocks all refactoring |

---

# 6. Rebuild vs Refactor Matrix

| Component | Current State | Refactor? | Rebuild? | Why? | Risk of Leaving As-Is |
|---|---|---|---|---|---|
| SP-API Connectors | Production-grade | Minor | No | Already good. Add connection pooling. | Low |
| Ads API Connectors | Production-grade | Minor | No | Add telemetry + 5xx retry. | Low |
| Event Backbone | Excellent but narrow | Yes | Partially | Keep dedup/CB/dispatch. Add internal domain events. | Medium — system stays cron-coupled |
| Canonical Product Model | Missing | N/A | **Yes** | Nothing to refactor — fundamental gap | **Critical** — perpetual data inconsistency |
| Profit Engine | God module | **Yes** | No | Split into calculator + query + export + cost model | High — unmaintainable at 6632 lines |
| Content Ops | God module | **Yes** | No | Split into 5 sub-modules | High — unmaintainable at 4906 lines |
| FBA Ops | Monolith | **Yes** | No | Extract fee audit, cases, inbound, replenishment | Medium |
| Order Pipeline | Good but mixed | Minor | No | Extract fee bridging to finance_center | Medium |
| Finance Center | Well-structured | No | No | Keep. Add dedup constraint. | Low |
| Tax Compliance | Best-structured | No | No | Reference architecture for other modules | Low |
| Scheduler | Megafile | **Yes** | No | Split into domain modules | Medium — maintenance burden |
| MSSQL Store | God module | **Yes** | No | Extract job queue into own module | Medium |
| Profit Service | Legacy/dead | **Delete** | No | Only used by tests — migrate tests to V2 | Low but confusing |
| Sync Service | Partially superseded | Phase out | No | Order path superseded. Keep inventory/pricing paths until ingestion module built. | Medium — dual paths |
| Inventory Sync (Job) | Broken (no upsert) | N/A | **Yes** | Business logic in job layer, no dedup, deprecated async | **High** — creates duplicate snapshots |
| Celery Jobs (4 files) | Deprecated async | **Fix** | No | Replace `get_event_loop()` with `asyncio.run()` | **High** — breaks on Python 3.12+ |

---

# 7. Recommended Target Backend Structure

```
app/
├── platform/                          # Cross-cutting infrastructure
│   ├── account_hub.py                 # Seller accounts, credentials, marketplace registry
│   ├── rate_governor.py               # Centralized per-endpoint rate tracking
│   ├── event_bus.py                   # Domain event bus (SQS external + internal pub/sub)
│   ├── job_scheduler.py               # Unified scheduler (from scheduler.py + worker.py)
│   ├── action_center.py               # Unified write gateway to Amazon (feeds, listings, prices)
│   └── shared/                        # Shared utilities
│       ├── db.py                      # _connect, _fetchall_dict, _f, _i, _mkt_code
│       ├── cache.py                   # Generic in-memory TTL cache
│       └── fx.py                      # FX service (from core/fx_service.py)
│
├── connectors/                        # External API clients (KEEP AS-IS)
│   ├── amazon_sp_api/                 # 12 files — production-grade
│   ├── amazon_ads_api/                # 4 files — add telemetry
│   ├── dhl24_api/
│   ├── gls_api/
│   ├── mssql/
│   └── ergonode.py, ecb.py, nbp.py
│
├── domain/                            # Core domain models (NEW)
│   ├── canonical_product.py           # Canonical product + marketplace presence + family
│   ├── marketplace_mapping.py         # Unified SKU/ASIN/EAN/InternalSKU mapping
│   └── fee_taxonomy.py               # Move from core/ — it's domain logic
│
├── ingestion/                         # Data ingestion modules (EXTRACT FROM services/)
│   ├── orders.py                      # From order_pipeline steps 1-4
│   ├── listings.py                    # From sync_service + listing_state + registry
│   ├── catalog.py                     # From catalog connector + ptd_cache
│   ├── pricing.py                     # From pricing_state.capture_*
│   ├── inventory.py                   # From sync_inventory + fba_ops.sync_cache
│   ├── finance.py                     # From order_pipeline.step_sync_finances
│   ├── ads.py                         # From ads_sync.py
│   ├── returns.py                     # From return_tracker.py sync functions
│   └── notifications.py              # SQS poller (from event_backbone.poll_sqs)
│
├── warehouse/                         # Normalized data stores (EXTRACT FROM models/)
│   ├── orders_store.py                # AccOrder, OrderLine — query interface
│   ├── listing_store.py               # ListingState + history — query interface
│   ├── pricing_store.py               # PricingSnapshot + archive — query interface
│   ├── inventory_store.py             # InventorySnapshot — query interface
│   ├── finance_store.py               # FinanceTransaction + Ledger — query interface
│   ├── ads_store.py                   # AdsCampaign + daily metrics — query interface
│   └── catalog_store.py              # CatalogSnapshot + ProductType — query interface
│
├── intelligence/                      # Business logic / analytics (REFACTOR FROM services/)
│   ├── profit/
│   │   ├── calculator.py              # CM1/CM2/NP computation (from profit_engine core)
│   │   ├── query.py                   # Profit tables, drilldowns, KPIs
│   │   ├── export.py                  # XLSX export
│   │   └── cost_model.py             # Cost model config
│   ├── catalog_health.py             # From guardrails + listing_state + content health
│   ├── inventory_risk.py             # From fba_ops + manage_inventory + strategy
│   ├── pricing_intelligence.py       # From pricing_rules + strategy pricing detection
│   ├── seasonality.py                # From seasonality_service + opportunity_engine
│   ├── strategy.py                   # From strategy_service (opportunity detection)
│   ├── decision_feedback.py          # From decision_intelligence_service
│   └── executive.py                  # From executive_service (health scoring)
│
├── execution/                         # Write operations (EXTRACT)
│   ├── content_ops/
│   │   ├── tasks.py                   # Task CRUD
│   │   ├── versions.py               # Version CRUD
│   │   ├── publish.py                # Publish pipeline
│   │   ├── policy.py                 # Policy validation
│   │   └── compliance.py             # Compliance queue
│   ├── family_mapper/                # KEEP AS-IS — well-structured
│   ├── fba_ops/
│   │   ├── overview.py
│   │   ├── replenishment.py
│   │   ├── inbound.py
│   │   ├── cases.py
│   │   └── fee_audit.py
│   ├── pricing_actions.py            # Execute price changes via action_center
│   └── returns.py                    # Return actions/COGS adjustments
│
├── compliance/                        # KEEP AS-IS — best-structured module
│   └── tax_compliance/               # 11 files — reference architecture
│
├── logistics/                         # Courier subsystem (KEEP AS-IS)
│   ├── dhl/
│   ├── gls/
│   ├── cost_estimation.py
│   ├── billing_import.py
│   └── order_linking.py
│
├── api/                              # API routes (KEEP, minor cleanup)
│   ├── v1/
│   └── ws.py
│
├── core/                             # Framework infrastructure (KEEP)
│   ├── config.py
│   ├── database.py
│   ├── db_connection.py
│   ├── security.py
│   ├── redis_client.py
│   ├── circuit_breaker.py
│   └── scheduler_lock.py
│
├── models/                           # SQLAlchemy ORM models (KEEP, add canonical)
│
└── migrations/                       # ALL schema changes go here (CONSOLIDATE)
    └── versions/
```

### Ownership Boundaries
- **Platform team** owns: platform/, core/, connectors/, models/, migrations/
- **Data team** owns: ingestion/, warehouse/
- **Analytics team** owns: intelligence/
- **Operations team** owns: execution/, compliance/, logistics/
- **Product team** owns: api/

### Service Boundaries (if ever splitting)
1. **Data Ingestion Service** — ingestion/ + connectors/ + warehouse/ (write path)
2. **Intelligence Service** — intelligence/ + warehouse/ (read path)
3. **Operator Console Service** — api/ + execution/ (UI + actions)
4. **Compliance Service** — compliance/ (tax + regulatory)

### Storage Boundaries
- **Primary DB** (Azure SQL) — all `acc_*` tables
- **Redis** — scheduler lock, circuit breaker, rate limiter, Celery broker, response cache
- **SQS** — SP-API notifications, internal domain events (future)
- **Blob Storage** — reports, exports, audit packs (future)

---

# 8. Suggested SQS Topology

### Queues

| Queue Name | Purpose | Producers | Consumers | Concurrency |
|---|---|---|---|---|
| `acc-spapi-notifications` | SP-API events (existing) | Amazon SQS | event_bus.poll_sqs | 1 (adaptive) |
| `acc-ingestion-complete` | "Sync X finished" domain events | All ingestion modules | intelligence/, execution/ | 3 |
| `acc-actions` | Write operations to Amazon | execution/ modules | action_center worker | 1 (rate-limited) |
| `acc-alerts` | Alert triggers | intelligence/, guardrails | ops_console worker | 2 |

### DLQ Strategy

| Queue | DLQ | Max Receives | Retention |
|---|---|---|---|
| `acc-spapi-notifications` | `acc-spapi-notifications-dlq` | 3 | 14 days |
| `acc-ingestion-complete` | `acc-ingestion-complete-dlq` | 5 | 7 days |
| `acc-actions` | `acc-actions-dlq` | 3 | 14 days |
| `acc-alerts` | `acc-alerts-dlq` | 5 | 7 days |

### Idempotency Strategy
- **Event backbone** (existing): SHA-256 deterministic fingerprint — KEEP
- **ingestion-complete events**: Include `{source}:{marketplace}:{sync_window_end}` as dedup key
- **Action messages**: Include `{action_type}:{sku}:{marketplace}:{timestamp}` as idempotency key with 24h TTL in Redis

### Retry Strategy
- **Exponential backoff** with jitter in SQS visibility timeout: 30s → 60s → 120s
- **DLQ after 3-5 failures** depending on queue
- **Manual replay** via `event_bus.replay_events()` (existing capability)

### Replay Strategy
- Keep existing `replay_events()` from event_backbone
- Extend to all queues: `replay_dlq(queue_name, filter)` function
- All events persisted in `acc_event_log` before processing — replay from DB if SQS state lost

---

# 9. Suggested Data Model / Storage Domains

### Accounts
| Table | Purpose |
|---|---|
| `acc_marketplace` | Marketplace registry (EXISTS — ★★★★★) |
| `acc_seller_account` | NEW — Seller account + credentials + status |
| `acc_user` | Users + RBAC (EXISTS) |

### Catalog / Canonical Product
| Table | Purpose |
|---|---|
| `acc_canonical_product` | NEW — Brand owner's product identity (internal_sku PK, EAN, brand, category) |
| `acc_marketplace_presence` | NEW — Per-market listing existence (canonical_product FK, marketplace FK, SKU, ASIN, status) |
| `acc_product` | EXISTS — Keep as migration bridge, gradually replace |
| `acc_catalog_snapshot` | NEW — Point-in-time catalog data from SP-API |
| `acc_product_type_cache` | EXISTS (implicit) — Product type definitions cache |

### Listings
| Table | Purpose |
|---|---|
| `acc_listing_state` | EXISTS — Canonical listing status (★★★★★) |
| `acc_listing_state_history` | EXISTS — Status transitions |
| `acc_amazon_listing_registry` | EXISTS — GSheet-sourced master mapping |

### Offers / Pricing
| Table | Purpose |
|---|---|
| `acc_offer` | EXISTS — Current offer state |
| `acc_pricing_snapshot` | EXISTS — Price observation history |
| `acc_pricing_snapshot_archive` | EXISTS — Aged snapshots |
| `acc_pricing_rule` | EXISTS — Pricing guardrails |
| `acc_pricing_recommendation` | EXISTS — Human-reviewed suggestions |

### Inventory
| Table | Purpose |
|---|---|
| `acc_inventory_snapshot` | EXISTS — Daily inventory state (needs UPSERT fix) |
| `acc_fba_inventory_snapshot` | EXISTS — FBA-specific detail (from reports) |
| `acc_inv_traffic_sku_daily` | EXISTS — Sales velocity |

### Finance
| Table | Purpose |
|---|---|
| `acc_finance_transaction` | EXISTS — Raw Amazon financial events (needs UNIQUE constraint) |
| `acc_finance_ledger` | EXISTS — Double-entry ledger |
| `acc_finance_account` | EXISTS — Chart of accounts |
| `acc_finance_settlement` | EXISTS — Settlement summaries |
| `acc_finance_payout_reconciliation` | EXISTS — Bank reconciliation |

### Orders
| Table | Purpose |
|---|---|
| `acc_order` | EXISTS — Order header with profit waterfall (★★★★★) |
| `acc_order_line` | EXISTS — Order items with cost breakdown |
| `acc_order_sync_state` | EXISTS — Per-marketplace sync watermark |

### Ads
| Table | Purpose |
|---|---|
| `acc_ads_profile` | EXISTS |
| `acc_ads_campaign` | EXISTS |
| `acc_ads_campaign_day` | EXISTS — Daily campaign metrics |
| `acc_ads_product_day` | EXISTS — Daily ASIN-level metrics |

### Reports
| Table | Purpose |
|---|---|
| `acc_report_request` | NEW — Track all report requests centrally |

### Feeds
| Table | Purpose |
|---|---|
| `acc_feed_submission` | NEW — Track all feed submissions centrally |

### Notifications / Events
| Table | Purpose |
|---|---|
| `acc_event_log` | EXISTS — All events (★★★★★) |
| `acc_event_processing_log` | EXISTS — Handler execution audit |
| `acc_event_handler_health` | EXISTS — Circuit breaker state |
| `acc_event_destination` | EXISTS — SQS destinations |
| `acc_event_subscription` | EXISTS — Notification subscriptions |

### Alerts / Cases
| Table | Purpose |
|---|---|
| `acc_al_rule` | EXISTS — Alert rule definitions |
| `acc_al_alert` | EXISTS — Triggered alerts |
| `acc_system_alert` | EXISTS — System-level alerts |
| `acc_fba_case` | EXISTS — FBA support cases |
| `acc_guardrail_results` | EXISTS — Health check results |

### Jobs
| Table | Purpose |
|---|---|
| `acc_job` | EXISTS — Job runs + status + retry |
| `acc_job_run` | EXISTS (ORM) — Duplicate job tracking (ORM-based) |

---

# 10. Implementation Roadmap

## Phase 1: Foundation (Must-Have) — 4-6 weeks

### Goals
- Eliminate data corruption risks
- Establish single source of truth for profit
- Create shared infrastructure
- Fix Python 3.12+ compatibility

### Tasks

| # | Task | Effort | Dependencies | Risk |
|---|---|---|---|---|
| 1.1 | **Add UNIQUE constraint on `acc_finance_transaction`** (`posted_date, amazon_order_id, sku, charge_type, amount`) | 1 day | Dedup existing data first | Schema migration on live DB |
| 1.2 | **Fix inventory sync upsert** — replace `db.add()` with MERGE in `sync_inventory.py` | 1 day | None | Low |
| 1.3 | **Fix deprecated async patterns** — replace `asyncio.get_event_loop().run_until_complete()` with `asyncio.run()` in 4 Celery jobs | 0.5 day | None | Test Celery task execution |
| 1.4 | **Extract shared utilities** — `_f()`, `_i()`, `_mkt_code()`, `_fetchall_dict()`, `_connect()`, in-memory cache → `platform/shared/` | 3 days | Touch 8+ files | Regression risk across services |
| 1.5 | **Eliminate legacy profit path** — delete `profit_service.py`, migrate tests to V2 engine | 2 days | Verify no production callers | Low |
| 1.6 | **Split `scheduler.py`** into domain modules (orders, finance, inventory, ads, profit, content, logistics, strategy) | 3 days | None | Must not change scheduling behavior |
| 1.7 | **Consolidate schema management** — extract all `ensure_*_schema()` DDL into migration scripts, run via a single `ensure_all_schemas()` | 5 days | Audit all 14+ schema files | Must be idempotent |
| 1.8 | **Fix `Alert.marketplace_id` and `JobRun.marketplace_id`** — add FK constraints | 1 day | Data cleanup if orphans exist | Low |
| 1.9 | **Clean project root** — move `tmp_*`, `_*`, `*.log` files to `_archive/` or delete | 0.5 day | Confirm nothing is referenced | Very low |
| 1.10 | **Add missing UNIQUE constraints** — `acc_inventory_snapshot(product_id, marketplace_id, snapshot_date)`, `acc_offer(sku, marketplace_id)` | 1 day | Dedup existing data | Schema migration |

### Phase 1 Total: ~18 days

---

## Phase 2: Architecture (High Leverage) — 6-10 weeks

### Goals
- Establish canonical product model
- Build internal domain event bus
- Decompose God modules
- Create unified action center

### Tasks

| # | Task | Effort | Dependencies | Risk |
|---|---|---|---|---|
| 2.1 | **Design & implement Canonical Product Model** — `acc_canonical_product` + `acc_marketplace_presence` tables, migration from `acc_product` + `acc_offer` | 10 days | Phase 1 complete | High — touches everything |
| 2.2 | **Build unified Marketplace Mapping Engine** — single lookup: `(sku, marketplace) → canonical_product` | 5 days | 2.1 | Medium |
| 2.3 | **Extend Event Backbone to internal domain events** — ingestion modules emit `{domain}.{action}` events on batch completion; intelligence modules subscribe | 5 days | Phase 1 shared utilities | Medium |
| 2.4 | **Split `profit_engine.py`** into calculator + query + export + cost_model (4 files) | 5 days | 1.5 (legacy profit removed) | Medium — refactoring 6632 lines |
| 2.5 | **Split `content_ops.py`** into tasks + versions + publish + policy + compliance (5 files) | 5 days | None | Medium |
| 2.6 | **Split `fba_ops/service.py`** into overview + replenishment + inbound + cases + launches (5 files) | 3 days | None | Low |
| 2.7 | **Build Action Center** — unified write gateway with audit trail, circuit breaker, rate limiting | 5 days | 2.3 (event bus) | Medium |
| 2.8 | **Build `ingestion/listings.py`** — unify report-based + event-based + registry listing ingestion | 5 days | 2.1 (canonical product) | Medium |
| 2.9 | **Build `ingestion/inventory.py`** — single path from SP-API + Reports → normalized inventory with upsert | 3 days | 1.2, 2.1 | Low |
| 2.10 | **Merge `profitability_service` query functions into profit query module** | 2 days | 2.4 | Low |

### Phase 2 Total: ~48 days

---

## Phase 3: Advanced / Optimization — 8-12 weeks

### Goals
- Full event-driven architecture
- Automated decision execution
- Competitor intelligence
- Production hardening

### Tasks

| # | Task | Effort | Dependencies | Risk |
|---|---|---|---|---|
| 3.1 | **Repricing Decision Engine** — dynamic repricing strategies, competitor tracking, margin-aware algorithms | 15 days | Phase 2 canonical product + pricing | High — direct P&L impact |
| 3.2 | **Competitor Intelligence Store** — track all sellers' offers, BuyBox win-rate trends, competitive landscape | 10 days | Phase 2 pricing intelligence | Medium |
| 3.3 | **Inventory Risk Engine** — stockout probability, overstock cost, aging write-off modeling | 8 days | Phase 2 canonical product + inventory | Medium |
| 3.4 | **Content Optimization Engine** — content scoring, SEO analysis, competitor content benchmarking | 10 days | Phase 2 content ops split | Medium |
| 3.5 | **Implement SQS topology** — ingestion-complete, actions, alerts queues with DLQ | 5 days | Phase 2 event bus | Medium |
| 3.6 | **Build comprehensive test suite** — target 60%+ coverage on critical paths (profit, finance, orders) | 15 days | Phase 2 complete | Low |
| 3.7 | **Connection pooling** for SP-API/Ads clients — shared httpx.AsyncClient per connector | 3 days | None | Low |
| 3.8 | **Refund / Fee Anomaly Engine** — spike detection, serial returner identification, reimbursement claims | 8 days | Phase 2 complete | Medium |
| 3.9 | **Operator Console backend** — unified alert feed, case management, approval workflows | 10 days | Phase 2 action center | Medium |
| 3.10 | **Buy Box Radar** — win-rate trends, competitive alerts, position tracking | 5 days | 3.2 (competitor store) | Low |

### Phase 3 Total: ~89 days

---

# 11. Quick Wins (Next 7 Days)

| # | Change | Impact | Effort | Where |
|---|---|---|---|---|
| 1 | **Fix 4 deprecated `asyncio.get_event_loop()` calls** → `asyncio.run()` | Prevents Python 3.12+ breakage | 2 hours | `jobs/calc_profit.py`, `sync_finances.py`, `sync_inventory.py`, `sync_purchase_prices.py` |
| 2 | **Add `UNIQUE` constraint on `acc_finance_transaction`** | Prevents duplicate financial data | 4 hours (dedup + migrate) | `models/finance.py` + migration script |
| 3 | **Fix `sync_inventory.py` to use MERGE/upsert** instead of `db.add()` | Prevents duplicate inventory snapshots | 3 hours | `jobs/sync_inventory.py` |
| 4 | **Delete `profit_service.py`** (verify no prod callers first) | Eliminates confusion about which profit path is canonical | 2 hours | `services/profit_service.py` |
| 5 | **Clean project root** — move 95 `tmp_*` + 30 `_*` + 18 `*.log` to `_archive/` | Reduces cognitive load, clarifies codebase | 1 hour | Project root |
| 6 | **Extract `_f`, `_i`, `_mkt_code`, `_fetchall_dict`, `_connect`** into `app/platform/shared/db.py` | Eliminates 8-way duplication | 1 day | 8+ service files |
| 7 | **Decrease SQS poll interval** from 2 min to 30 sec | Faster reaction to offer/order changes | 30 min | `scheduler.py` |

---

# 12. Brutal Truth Section

**ACC is an impressive operational tool that has outgrown its architecture.**

The codebase has ~100k lines of Python, 425 API endpoints, 50+ tables, and genuine domain depth across orders, finance, pricing, content, logistics, families, ads, tax compliance, and strategy. This is not a toy. It handles 9 EU Amazon marketplaces with real PLN-denominated financial accounting.

**But it is not yet a Command Center.**

It is a **collection of vertical slices** — each slice (orders, finance, content, FBA, pricing, logistics, strategy) was built end-to-end with its own data model, its own sync logic, its own query patterns, and its own in-memory cache. The `services/` directory has 80+ files, many exceeding 1,000 lines, with duplicated helper functions and embedded DDL. There is no horizontal architecture connecting these slices.

**The single highest debt is the absence of a canonical product model.** Five different tables (`acc_product`, `acc_offer`, `acc_listing_state`, `global_family`, `acc_amazon_listing_registry`) represent "product" from different angles, and no service unifies them. Until this is fixed, every module will continue building its own product lookup, and cross-market intelligence will remain fragile.

**The second highest debt is the profit calculation.** Three code paths compute profit, and nobody can definitively answer which one is authoritative. The 6,632-line `profit_engine.py` must be split, and the legacy paths must be killed.

**The third highest debt is that the event backbone — the best-engineered module in the codebase — is almost unused.** 90% of data flow goes through time-coupled cron jobs (`02:00 → 03:00 → 04:00 → 05:00`). The system has SQS, has handlers, has dedup, has circuit breakers — and then routes almost everything through `scheduler.py` on fixed time intervals. The event-driven future is architecturally present but operationally dormant.

**What must change first:**
1. Canonical product model — without it, nothing else assembles correctly.
2. Single profit path — financial trust is non-negotiable.
3. Internal domain events — the backbone exists; wire everything through it.
4. God module decomposition — `profit_engine`, `content_ops`, `fba_ops`, and `mssql_store` must be split before any team can work in parallel without merge conflicts.

**The tax_compliance module is what the entire codebase should aspire to be:** 11 files, each with a clear bounded responsibility, clean interfaces, proper domain separation. Use it as the reference architecture for refactoring everything else.
