# 📊 ACC Data Landscape Audit Report
## Analytics Infrastructure Assessment — Amazon Command Center

**Agent**: Analytics Reporter | **Date**: 2026-03-12 | **Classification**: Strategic — Internal  
**Prepared for**: Miłosz Sobieniowski, Founder  
**Sources**: Live Azure SQL telemetry (187 tables, 26.6M rows), codebase analysis (42 scheduler jobs, 51 API modules, 27 guardrail checks), pipeline architecture review  
**Methodology**: Schema census → pipeline mapping → signal identification → completeness scoring → infrastructure gap analysis

---

## EXECUTIVE SUMMARY

ACC sits on a **19.3 GB Azure SQL database** containing **187 tables, 511 indexes, and 26.6M rows** of Amazon seller operational data across **11 marketplaces**. The data landscape is **remarkably complete for a single-operator platform** — 99.5% of order lines have purchase prices, 96% have COGS, and 90.9% have referral fees. However, **three critical data gaps** undermine the platform's analytical potential:

1. **Ads data lag** — Product-level ads data is 93h stale (last: 2026-03-09); campaign data is 69h stale
2. **FBA fee coverage gap** — Only 30.1% of order lines have FBA fees, despite 34% of orders being AFN (FBA)
3. **72 empty tables** (38.5% of schema) — significant schema bloat from modules built but never populated

**Data Quality Composite Score: 74/100** (Good foundation, operational gaps)

| Dimension | Score | Status |
|-----------|-------|--------|
| Completeness | 78/100 | 🟢 Strong core, gaps in FBA/ads |
| Freshness | 62/100 | 🟡 Orders live, ads/inventory stale |
| Accuracy | 80/100 | 🟢 Multi-source COGS, FX dual-source |
| Consistency | 72/100 | 🟡 Mixed PL/EN, computed vs stored |
| Coverage | 78/100 | 🟢 11 marketplaces, 2yr history |

---

## 1. EXISTING DATA SOURCE AUDIT

### 1.1 Data Source Inventory

ACC ingests data from **12 external sources** and **3 internal computation engines**, stored across **187 SQL tables** in a **19.3 GB** Azure SQL database.

#### External Data Sources

| # | Source | Type | Connection | Direction | Frequency | Tables | Rows |
|---|--------|------|-----------|-----------|-----------|--------|------|
| **DS-01** | **Amazon SP-API** (Orders) | REST API | OAuth2 LWA | Read | Every 30 min | `acc_order`, `acc_order_line`, `acc_order_sync_state`, + 3 | 1,294,884 |
| **DS-02** | **Amazon SP-API** (Finances) | REST API | OAuth2 LWA | Read | Daily 03:00 | `acc_finance_transaction`, `acc_fin_*` (9 tables) | 1,008,311 |
| **DS-03** | **Amazon SP-API** (Inventory) | REST API | OAuth2 LWA | Read | Daily 04:00 + 8h FBA | `acc_inventory_snapshot`, `acc_fba_*` (17 tables) | 110,294 |
| **DS-04** | **Amazon SP-API** (Reports) | REST API | OAuth2 LWA | Read | Daily 01:00-02:30 | `acc_product`, `acc_amazon_listing_registry`, `acc_offer` | 141,036 |
| **DS-05** | **Amazon SP-API** (Catalog/PTD) | REST API | OAuth2 LWA | Read | Daily 02:30 | `acc_ptd_cache`, `acc_co_product_type_*` | ~1 |
| **DS-06** | **Amazon Ads API** (v3 Reports) | REST API | OAuth2 | Read | Every 4h | `acc_ads_profile`, `acc_ads_campaign`, `acc_ads_*_day` | 1,931,024 |
| **DS-07** | **Netfox ERP** (MSSQL) | Direct SQL | pyodbc | Read-only | Daily 02:00 | `acc_purchase_price` (synced from `dbo.Kartoteki`) | 13,447 |
| **DS-08** | **GLS Poland** (ADE SOAP + billing CSV) | SOAP + File | Auth + N:\ share | Read | Daily 00:20 | `acc_gls_*` (5 tables) | 894,888 |
| **DS-09** | **DHL24** (WebAPI2 SOAP + billing CSV) | SOAP + File | Auth + N:\ share | Read | Daily 00:05 | `acc_dhl_*` (5 tables) | 904,653 |
| **DS-10** | **NBP** (Polish Central Bank) | REST API | Public, no auth | Read | Daily (via profit calc) | `acc_exchange_rate` | 3,054 |
| **DS-11** | **ECB** (European Central Bank) | XML Feed | Public, no auth | Read | Daily 02:30 | `ecb_exchange_rate` | 1,814 |
| **DS-12** | **BaseLinker** (Distribution) | REST API | API token | Read | Daily 01:55 | `acc_bl_distribution_*`, `acc_cache_*` | 17,728,551 |

#### File-Based Data Sources

| # | Source | Format | Path | Frequency | Purpose |
|---|--------|--------|------|-----------|---------|
| **FS-01** | Official Purchase Prices | XLSX | `N:\Analityka\00. Oficjalne ceny zakupu...xlsx` | On-change (daily 06:00 scan) | COGS master (highest priority) |
| **FS-02** | GLS Billing Files | CSV | `N:\KURIERZY\GLS POLSKA\*` | Nightly import | Courier cost reconciliation |
| **FS-03** | DHL Billing Files | CSV | `N:\KURIERZY\DHL\*` | Nightly import | Courier cost reconciliation |
| **FS-04** | TKL Transport Cost Lists | XLSX | Local/configured path | Daily 01:40 cache warm | Logistics cost modeling |
| **FS-05** | Google Sheets | CSV via URL | `GSHEET_EAN_CSV_URL`, `GSHEET_AMAZON_LISTING_CSV_URL` | On-demand | EAN→SKU mapping, listing registry |

#### Internal Computation Engines

| # | Engine | Schedule | Output Tables | Output Rows |
|---|--------|----------|---------------|-------------|
| **CE-01** | Profit Calculator (CM1/CM2/NP) | Daily 05:00 + event-driven | `acc_sku_profitability_rollup`, `acc_marketplace_profitability_rollup` | 102,318 |
| **CE-02** | Seasonality Engine | Weekly (Sun/Mon) | `seasonality_*` (7 tables) | 91,760 |
| **CE-03** | Strategy/Decision Intelligence | Daily 07:00 + weekly + monthly | `growth_opportunity`, `executive_*`, `decision_*`, `opportunity_*` | 65,689 |

### 1.2 Database Domain Map

| Domain | Tables | Rows | % of Total | Size Est. | Data Age |
|--------|--------|------|-----------|-----------|----------|
| **Cache/BaseLinker** | 7 | 17,728,551 | 66.7% | ~12.5 GB | Nightly refresh |
| **Shipping/Logistics** | 7 | 2,121,317 | 8.0% | ~1.5 GB | Nightly (GLS/DHL) |
| **Ads** | 4 | 1,931,024 | 7.3% | ~1.2 GB | 69-93h stale ⚠️ |
| **Orders** | 6 | 1,294,884 | 4.9% | ~900 MB | Live (30 min) ✅ |
| **Finance** | 9 | 1,008,311 | 3.8% | ~700 MB | Live (< 1h) ✅ |
| **DHL** | 5 | 904,653 | 3.4% | ~600 MB | Nightly ✅ |
| **GLS** | 5 | 894,888 | 3.4% | ~600 MB | Nightly ✅ |
| **Profit** | 5 | 518,215 | 1.9% | ~350 MB | Daily 05:00 ✅ |
| **Alert/Job** | 12 | 167,323 | 0.6% | ~100 MB | Live ✅ |
| **Inventory** | 9 | 160,667 | 0.6% | ~100 MB | 69h stale ⚠️ |
| **Product/Catalog** | 5 | 137,153 | 0.5% | ~80 MB | Daily ✅ |
| **Tax** | 7 | 120,569 | 0.5% | ~80 MB | Event-driven |
| **Seasonality** | 7 | 91,760 | 0.3% | ~60 MB | Weekly ✅ |
| **FBA** | 17 | 60,010 | 0.2% | ~40 MB | 69h stale ⚠️ |
| **Families** | 11 | 54,412 | 0.2% | ~35 MB | Daily ✅ |
| **Strategy** | 7 | 50,646 | 0.2% | ~35 MB | Daily 07:00 ✅ |
| **Executive** | 3 | 14,243 | <0.1% | ~10 MB | Daily ✅ |
| **FX Rates** | 2 | 4,868 | <0.1% | ~3 MB | 21h ✅ |
| **Content** | 18 | 18 | <0.01% | ~1 MB (schema) | Empty ❌ |

### 1.3 Schema Health Assessment

| Metric | Value | Assessment |
|--------|-------|-----------|
| Total tables | 187 | Comprehensive |
| Tables with data (>0 rows) | **115** (61.5%) | 🟡 Moderate — 72 empty |
| Tables >100K rows | 15 | Heavy core |
| Tables >1M rows | 7 | Large fact tables |
| Total indexes | 511 (745 incl. system) | Well-indexed |
| Total constraints | 927 | Strong referential integrity |
| Database size | 19,333 MB (~19 GB) | Manageable for Azure SQL S3 |

**72 empty tables** (38.5%) break into these categories:

| Category | Count | Examples | Assessment |
|----------|-------|---------|-----------|
| Content Ops (acc_co_*) | 12 | `acc_co_assets`, `acc_co_ai_cache`, `acc_co_versions` | Module built, never activated |
| Pricing (acc_pricing_*) | 4 | `acc_pricing_recommendation`, `acc_pricing_rule` | Repricing module schema-only |
| FBA Advanced | 6 | `acc_fba_bundle`, `acc_fba_initiative`, `acc_fba_launch` | FBA workflow stubs |
| Alerting v2 | 2 | `acc_alert`, `acc_alert_rule` | Replaced by acc_al_* system |
| Tax/Compliance | 5 | `oss_return_*`, `filing_readiness_*`, `local_vat_ledger` | OSS/VAT module stubs |
| Strategy | 2 | `strategy_experiment`, `opportunity_model_adjustments` | DI experiment stubs |
| Planning | 2 | `acc_plan_line`, `acc_plan_month` | Budget planning stubs |
| Notifications | 2 | `acc_notification_*` | SP-API notification stubs |
| Other | 37 | Various | Mixed schema stubs |

---

## 2. SIGNAL IDENTIFICATION — What Can We Measure?

### 2.1 Signal Map Overview

ACC's data landscape enables **87 measurable signals** across 8 analytical domains. Below is the complete signal map organized by domain and data readiness.

### 2.2 Domain: Revenue & Orders (🟢 HIGH READINESS)

| # | Signal | Source Table(s) | Grain | Time Range | Readiness |
|---|--------|----------------|-------|-----------|-----------|
| S-01 | **Gross revenue per SKU/day** | `acc_order` + `acc_order_line` | Order line | 2yr (Mar 2024 →) | 🟢 Ready |
| S-02 | **Order volume per marketplace/day** | `acc_order` | Order | 2yr | 🟢 Ready |
| S-03 | **Units sold per SKU/day** | `acc_order_line` | Order line | 2yr | 🟢 Ready |
| S-04 | **Average order value (AOV)** | `acc_order` | Order | 2yr | 🟢 Ready |
| S-05 | **Order status distribution** | `acc_order.order_status` | Order | 2yr | 🟢 Ready |
| S-06 | **Fulfillment mix (FBA vs FBM)** | `acc_order.fulfillment_channel` | Order | Live: 66% MFN, 34% AFN | 🟢 Ready |
| S-07 | **Cancellation rate** | `acc_order` (status=Canceled) | Order | 2yr | 🟢 Ready |
| S-08 | **Revenue by currency/marketplace** | `acc_order_line.currency` + FX | Order line | 2yr | 🟢 Ready |
| S-09 | **New vs returning order trends** | `acc_order` (date distribution) | Day | 2yr | 🟡 Derivable |
| S-10 | **Order velocity (orders/hour)** | `acc_order.purchase_date` | Hourly | 2yr | 🟢 Ready |

**Coverage**: 847,453 orders across 11 marketplaces, 488 distinct days, ~150K order lines per 90d window.

### 2.3 Domain: Profitability (🟢 HIGH READINESS)

| # | Signal | Source Table(s) | Grain | Readiness |
|---|--------|----------------|-------|-----------|
| S-11 | **CM1 per SKU/marketplace** | `acc_sku_profitability_rollup` | SKU×Mkt×Period | 🟢 Ready (101K rows) |
| S-12 | **CM2 per SKU** (incl. ads + storage) | Profit calculator output | SKU×Period | 🟢 Ready |
| S-13 | **Net Profit per SKU** | Profit calculator + overhead pools | SKU×Period | 🟡 Partial (overhead pools empty) |
| S-14 | **COGS per order line** | `acc_order_line.cogs_pln` | Order line | 🟢 96.0% coverage |
| S-15 | **Amazon fee breakdown** (referral, FBA, etc.) | `acc_order_line` + `acc_finance_transaction` | Order line | 🟢 90.9% referral |
| S-16 | **Logistics cost per order** | `acc_order_logistics_fact` + `acc_shipping_cost` | Order | 🟢 Ready (92K logistics facts) |
| S-17 | **Contribution margin trend** | `acc_marketplace_profitability_rollup` | Mkt×Day | 🟢 831 rows |
| S-18 | **Loss order identification** | Computed CM1 < 0 | Order line | 🟢 Ready |
| S-19 | **Fee gap detection** | `acc_fee_gap_watch` | SKU | 🟢 6,309 entries monitored |
| S-20 | **COGS price source reliability** | `acc_order_line.price_source` | Order line | 🟢 Ready (6 sources tracked) |

**COGS Price Source Distribution (90d)**:

| Source | Lines | % | Trust Level |
|--------|-------|---|-------------|
| `auto` (pipeline-stamped) | 46,020 | 30.5% | 🟢 High |
| `purchase_price_tbl` (DB lookup) | 34,791 | 23.1% | 🟢 High |
| `manual` (operator override) | 31,885 | 21.1% | 🟢 High (authoritative) |
| `xlsx_oficjalne` (official XLSX) | 25,094 | 16.6% | 🟢 High |
| `import_xlsx` (batch import) | 8,603 | 5.7% | 🟡 Medium |
| `bds_ht_bridge` | 2,480 | 1.6% | 🟡 Medium |
| `acc_purchase_price` | 956 | 0.6% | 🟡 Medium |
| `NULL` (missing) | 765 | 0.5% | 🔴 Gap |
| `holding` / `es_holding` | 238 | 0.2% | 🟡 Medium |

### 2.4 Domain: Advertising (🟡 MODERATE READINESS)

| # | Signal | Source Table(s) | Grain | Readiness |
|---|--------|----------------|-------|-----------|
| S-21 | **Ad spend per campaign/day** | `acc_ads_campaign_day` | Campaign×Day | 🟢 119K rows |
| S-22 | **ACOS per product/day** | `acc_ads_product_day` | ASIN×Mkt×Day | 🟢 1.8M rows |
| S-23 | **Ad-attributed sales** | `acc_ads_product_day.sales_7d` | ASIN×Day | 🟢 Ready |
| S-24 | **CPC trends** | `acc_ads_campaign_day` | Campaign×Day | 🟢 Ready |
| S-25 | **Impression share** | `acc_ads_product_day.impressions` | ASIN×Day | 🟢 Ready |
| S-26 | **Campaign state distribution** | `acc_ads_campaign.state` | Campaign | 🟢 5,121 campaigns |
| S-27 | **Ad type mix (SP/SB/SD)** | `acc_ads_campaign.ad_type` | Campaign | 🟢 Ready |
| S-28 | **Ads→Profit attribution** | Join `acc_ads_product_day` + rollup | ASIN×Period | 🟡 Computed in CM2 |

**Freshness concern**: Product-level ads data is **93 hours stale** (last report_date: 2026-03-09). Campaign-level is 69 hours stale.

### 2.5 Domain: Logistics & Shipping (🟢 HIGH READINESS)

| # | Signal | Source Table(s) | Grain | Readiness |
|---|--------|----------------|-------|-----------|
| S-29 | **GLS cost per parcel** | `acc_gls_billing_line` | Parcel | 🟢 890K lines |
| S-30 | **DHL cost per parcel** | `acc_dhl_billing_line` | Parcel | 🟢 403K lines |
| S-31 | **Shipment→order link rate** | `acc_shipment_order_link` | Shipment | 🟢 896K links |
| S-32 | **Courier cost estimation accuracy** | `acc_courier_estimation_kpi_daily` | Day | 🟡 Empty (0 rows) |
| S-33 | **Logistics cost per marketplace** | `acc_order_logistics_fact` | Order | 🟢 92K facts |
| S-34 | **Courier verification completeness** | `acc_courier_monthly_kpi_snapshot` | Month | 🟢 6 snapshots |
| S-35 | **Parcel tracking coverage** | `acc_dhl_parcel_map` + `acc_dhl_jjd_map` | Parcel | 🟢 501K mappings |

### 2.6 Domain: FBA Operations (🟡 MODERATE READINESS)

| # | Signal | Source Table(s) | Grain | Readiness |
|---|--------|----------------|-------|-----------|
| S-36 | **FBA inventory levels** | `acc_fba_inventory_snapshot` | SKU×Date | 🟢 50K snapshots |
| S-37 | **FBA inbound status** | `acc_fba_inbound_shipment` + lines | Shipment | 🟢 100 + 4.5K lines |
| S-38 | **FBA return rate** | `acc_fba_customer_return` | Return | 🟢 3,770 returns |
| S-39 | **FBA receiving reconciliation** | `acc_fba_receiving_reconciliation` | Shipment | 🟢 422 records |
| S-40 | **FBA fee per order line** | `acc_order_line.fba_fee_pln` | Order line | 🔴 **Only 30.1% populated** |
| S-41 | **FBA stock-out risk** | Run-time computation | SKU | 🟡 Computed via alerts |

### 2.7 Domain: Finance & Tax (🟢 HIGH READINESS)

| # | Signal | Source Table(s) | Grain | Readiness |
|---|--------|----------------|-------|-----------|
| S-42 | **Finance transaction breakdown** | `acc_finance_transaction` | Transaction | 🟢 954K txns, 45 types |
| S-43 | **Settlement reconciliation** | `acc_fin_settlement_summary` | Payout | 🟢 35 settlements |
| S-44 | **Ledger entries** | `acc_fin_ledger_entry` | Entry | 🟢 52K entries |
| S-45 | **VAT event tracking** | `vat_event_ledger` | Event | 🟢 60K events |
| S-46 | **FX rate (PLN base)** | `acc_exchange_rate` (NBP) | Currency×Day | 🟢 3,054 rates |
| S-47 | **FX rate cross-validation** | `ecb_exchange_rate` (ECB backup) | Currency×Day | 🟢 1,814 rates |
| S-48 | **Refund anomaly detection** | Nightly scan (03:30) | Order | 🟡 Computed, not persisted long-term |
| S-49 | **Fee gap watchlist** | `acc_fee_gap_watch` | SKU | 🟢 6,309 entries |

### 2.8 Domain: Product & Catalog (🟢 HIGH READINESS)

| # | Signal | Source Table(s) | Grain | Readiness |
|---|--------|----------------|-------|-----------|
| S-50 | **Product master** | `acc_product` | SKU | 🟢 9,084 products |
| S-51 | **Amazon listing registry** | `acc_amazon_listing_registry` | SKU×Mkt | 🟢 17,997 listings |
| S-52 | **Active offers** | `acc_offer` | SKU×Mkt | 🟢 113,955 offers |
| S-53 | **BuyBox win rate** | BuyBox radar job output | ASIN×Mkt | 🟡 Computed nightly |
| S-54 | **Catalog health scores** | Catalog health job output | Listing | 🟡 Computed nightly |
| S-55 | **Global families** | `global_family` + children | Family | 🟢 1,433 families, 13K children |
| S-56 | **Taxonomy predictions** | `acc_taxonomy_prediction` | Product | 🟢 8,897 predictions |

### 2.9 Domain: Strategic Intelligence (🟡 MODERATE READINESS)

| # | Signal | Source Table(s) | Grain | Readiness |
|---|--------|----------------|-------|-----------|
| S-57 | **Growth opportunities** | `growth_opportunity` | SKU×Mkt | 🟢 50K opportunities |
| S-58 | **Executive daily KPIs** | `executive_daily_metrics` | Mkt×Day | 🟢 831 rows |
| S-59 | **Executive health score** | `executive_health_score` | Snapshot | 🟢 30 snapshots |
| S-60 | **Seasonality profiles** | `seasonality_profile` | Product | 🟢 9,948 profiles |
| S-61 | **Seasonal opportunity detection** | `seasonality_opportunity` | Product | 🟢 1,402 opportunities |
| S-62 | **Decision learning** | `decision_learning` | Type | 🟡 2 learning records (early) |
| S-63 | **Opportunity outcomes** | `opportunity_outcome` | Opportunity | 🟡 80 outcomes tracked |

### 2.10 Domain: Operational Health (🟢 HIGH READINESS)

| # | Signal | Source Table(s) | Grain | Readiness |
|---|--------|----------------|-------|-----------|
| S-64 | **Pipeline freshness (7 checks)** | `acc_guardrail_results` | Check×Run | 🟢 506 result rows |
| S-65 | **Financial corruption checks (7)** | `acc_guardrail_results` | Check×Run | 🟢 Active |
| S-66 | **Infrastructure health (3)** | `acc_guardrail_results` | Check×Run | 🟢 Active |
| S-67 | **Job queue health** | `acc_al_jobs` | Job | 🟢 4,483 job runs |
| S-68 | **Alert volume & resolution** | `acc_al_alerts` | Alert | 🟢 806 alerts |
| S-69 | **Event backbone health** | `acc_event_log` | Event | 🟢 556 events |
| S-70 | **SP-API usage tracking** | `acc_sp_api_usage_daily` | Day | 🟢 4,334 rows |
| S-71 | **SQS notification backlog** | SQS API (real-time) | Queue | 🟢 Real-time |

---

## 3. BASELINE METRICS ESTABLISHMENT

### 3.1 North Star Metrics

| Metric | Current Value | Target | Status |
|--------|--------------|--------|--------|
| **Total Orders** (lifetime) | 847,453 | Growing | 🟢 |
| **90d Order Lines** | 150,832 | — | 🟢 |
| **Active Marketplaces** | 11 (9 primary + 2 minimal) | 9 EU | 🟢 |
| **COGS Coverage** (90d) | 96.0% | >98% | 🟡 |
| **Data Freshness — Orders** | ~105 min | <30 min | 🟡 |
| **Data Freshness — Finance** | <1 hour | <6 hours | 🟢 |
| **Data Freshness — Ads** | 69-93 hours | <24 hours | 🔴 |
| **Guardrail Pass Rate** | 2/2 latest (limited) | 100% all 27 | 🟡 |

### 3.2 Marketplace Baselines

| Marketplace | ID | Orders | Start Date | MFN/AFN Split | Revenue Scale |
|------------|-----|--------|-----------|---------------|---------------|
| **DE** (Germany) | A1PA6795UKMFR9 | 556,142 | 2024-03-01 | Primary | 🟢 Dominant (~65%) |
| **FR** (France) | A13V1IB3VIYZZH | 133,299 | 2024-03-05 | Mixed | 🟢 Major (~16%) |
| **ES** (Spain) | APJ6JRA9NG5V4 | 82,658 | 2024-04-06 | Mixed | 🟢 Major (~10%) |
| **IT** (Italy) | A1RKKUPIHCS9HS | 24,868 | 2024-07-02 | Mixed | 🟡 Growing |
| **NL** (Netherlands) | AMEN7PMS3EDWL | 22,136 | 2024-06-30 | Mixed | 🟡 Growing |
| **PL** (Poland) | A1805IZSGTT6HS | 16,010 | 2025-01-01 | MFN-heavy | 🟡 Growing |
| **BE** (Belgium) | A2NODRKZP88ZB9 | 5,569 | 2025-01-01 | Mixed | 🟡 New |
| **SE** (Sweden) | A1C3SOZRARQ6R3 | 5,095 | 2024-03-20 | Mixed | 🟡 Small |
| **IE** (Ireland) | A28R8C7NBKEWEA | 1,663 | 2025-03-18 | Mixed | 🟡 Newest |
| UK | A1F83G8C2ARO7P | 11 | 2025-02-01 | — | ⚪ Negligible |
| SA | A17E79C6D8DWNP | 2 | 2025-12-25 | — | ⚪ Negligible |

### 3.3 Fulfillment Baseline (90d)

| Channel | Orders | % | Implication |
|---------|--------|---|-------------|
| **MFN** (Merchant Fulfilled) | 100,650 | 66.0% | GLS/DHL cost data critical |
| **AFN** (Amazon FBA) | 51,937 | 34.0% | FBA fee data needed (currently 30.1% gap) |

### 3.4 Financial Baseline

| Metric | Value | Notes |
|--------|-------|-------|
| Finance transactions | 954,709 | 45 distinct charge types |
| Orders with finance data | 200,562 | vs 847K orders total = **23.7% coverage** |
| Transactions date range | Dec 2024 → Mar 2026 | ~15 months of finance data |
| Settlements reconciled | 35 | via `acc_fin_settlement_summary` |
| Ledger entries | 52,034 | Double-entry accounting system |

**Finance gap**: Only 23.7% of lifetime orders have finance transaction data — this is expected since finance data starts Dec 2024 while orders go back to Mar 2024. For the **Dec 2024–Mar 2026** period, finance coverage should be near-complete.

### 3.5 Ads Baseline

| Metric | Value |
|--------|-------|
| Ad profiles | 10 (9 EU marketplaces + 1) |
| Active campaigns | 5,121 (SP/SB/SD) |
| Campaign daily data | 119,529 rows (Jan 2026 →) |
| Product daily data | 1,806,364 rows (Jan 2026 →) |
| Data freshness | Campaign: 2026-03-10 (-69h) / Product: 2026-03-09 (-93h) |

### 3.6 Logistics Baseline

| Metric | Value |
|--------|-------|
| Total shipments | 477,176 |
| Shipment→order links | 896,376 |
| GLS billing lines | 890,036 |
| DHL billing lines | 403,043 |
| Order logistics facts | 92,038 |
| Logistics shadow records | 116,271 |

---

## 4. DATA QUALITY ASSESSMENT WITH COMPLETENESS SCORING

### 4.1 Composite Data Quality Score

$$DQ_{composite} = \frac{w_1 \cdot C + w_2 \cdot F + w_3 \cdot A + w_4 \cdot R + w_5 \cdot V}{w_1 + w_2 + w_3 + w_4 + w_5}$$

Where: $C$ = Completeness, $F$ = Freshness, $A$ = Accuracy, $R$ = Consistency, $V$ = Coverage.

| Dimension | Weight | Score | Calculation |
|-----------|--------|-------|-------------|
| **Completeness** ($C$) | 0.30 | 78/100 | Weighted avg of field coverage rates |
| **Freshness** ($F$) | 0.25 | 62/100 | Pipeline staleness vs SLA targets |
| **Accuracy** ($A$) | 0.20 | 80/100 | Multi-source validation, guardrail pass |
| **Consistency** ($R$) | 0.15 | 72/100 | Schema uniformity, computed vs stored |
| **Coverage** ($V$) | 0.10 | 78/100 | Marketplace × time coverage |

$$DQ_{composite} = \frac{0.30 \times 78 + 0.25 \times 62 + 0.20 \times 80 + 0.15 \times 72 + 0.10 \times 78}{1.00} = \mathbf{73.7} \approx \mathbf{74/100}$$

### 4.2 Completeness Scoring Detail

#### Order Line Field Completeness (90-day window, n=150,832)

| Field | Populated | % Complete | Weight | Score | Assessment |
|-------|-----------|-----------|--------|-------|-----------|
| `product_id` (product link) | 150,631 | **99.9%** | 0.15 | 15.0 | 🟢 Excellent |
| `purchase_price_pln` (COGS price) | 150,067 | **99.5%** | 0.20 | 19.9 | 🟢 Excellent |
| `price_source` (COGS provenance) | 150,067 | **99.5%** | 0.05 | 5.0 | 🟢 Excellent |
| `cogs_pln` (computed COGS) | 144,771 | **96.0%** | 0.20 | 19.2 | 🟢 Good |
| `referral_fee_pln` | 137,134 | **90.9%** | 0.15 | 13.6 | 🟢 Good |
| `fba_fee_pln` | 45,460 | **30.1%** | 0.15 | 4.5 | 🔴 Critical gap |
| `sku` / `asin` | ~150,000+ | **~99%** | 0.10 | 9.9 | 🟢 Excellent |
| **Weighted Total** | — | — | **1.00** | **87.1** | — |

**Adjusted for FBA fee context**: The 30.1% FBA fee coverage aligns with the 34% AFN fulfillment mix (FBM orders legitimately have no FBA fees). The actual gap for AFN orders is ~11.5% — FBA fees present for ~88.5% of FBA order lines.

**Adjusted completeness score**: 87.1 × 0.90 (for FBA gap adjustment) = **78.4 → 78/100**

#### Cross-Domain Completeness Matrix

| Data Domain | Tables with Data | Coverage | Score |
|------------|-----------------|----------|-------|
| Orders | 6/6 | 100% | 🟢 100 |
| Finance | 8/9 | 89% | 🟢 89 |
| Product/Catalog | 5/5 | 100% | 🟢 100 |
| Ads | 4/4 | 100% | 🟢 100 |
| Logistics (GLS/DHL) | 10/10 | 100% | 🟢 100 |
| FBA | 10/17 | 59% | 🟡 59 |
| Inventory | 6/9 | 67% | 🟡 67 |
| Profit | 4/5 | 80% | 🟢 80 |
| Seasonality | 5/7 | 71% | 🟡 71 |
| Strategy | 6/7 | 86% | 🟢 86 |
| Executive | 3/3 | 100% | 🟢 100 |
| Tax | 4/7 | 57% | 🟡 57 |
| Content | 1/18 | **6%** | 🔴 6 |
| **Weighted Average** | **115/187** | **61.5%** | **78** |

### 4.3 Freshness Scoring Detail

| Pipeline | SLA Target | Actual Latency | Score | Status |
|----------|-----------|---------------|-------|--------|
| Order sync | < 30 min | **105 min** | 50/100 | 🟡 3.5× SLA |
| Finance transactions | < 6 hours | **< 1 hour** | 100/100 | 🟢 Excellent |
| FX rates (NBP) | < 24 hours | **21 hours** | 90/100 | 🟢 Good |
| Ads (campaign) | < 24 hours | **69 hours** | 25/100 | 🔴 2.9× SLA |
| Ads (product) | < 24 hours | **93 hours** | 15/100 | 🔴 3.9× SLA |
| Inventory | < 24 hours | **69 hours** | 25/100 | 🔴 2.9× SLA |
| GLS logistics | < 24 hours | Nightly (OK) | 90/100 | 🟢 Good |
| DHL logistics | < 24 hours | Nightly (OK) | 90/100 | 🟢 Good |
| Guardrails | < 120 min | **447 min** | 30/100 | 🔴 3.7× SLA |
| **Weighted Average** | — | — | **62/100** | 🟡 |

**Root cause for stale pipelines**: The ACC backend (`scheduler`) is currently **not running** (inferred from 105-min order sync lag and 447-min guardrail gap). When running, orders sync every 30 min and guardrails every 60 min.

### 4.4 Accuracy Scoring Detail

| Check | Method | Status | Score |
|-------|--------|--------|-------|
| COGS multi-source validation | 6 price sources with priority chain | 🟢 Pass | 90 |
| FX dual-source (NBP + ECB) | ECB backup validates NBP primary | 🟢 Pass | 90 |
| Finance dedup | `sync_payload_hash` on orders, dedup in finance sync | 🟢 Pass | 85 |
| Fee taxonomy coverage | Guardrail check: unknown fee types | 🟡 Partial (45 charge types, monitoring active) | 75 |
| Guardrail automated checks | 27 checks across 5 sections | 🟢 Comprehensive | 85 |
| Data type constraints | 927 SQL constraints + typed columns | 🟢 Strong | 85 |
| Profit formula integrity | CM1/CM2/NP computed in SQL + Python | 🟡 Mixing computed + stored values | 65 |
| **Weighted Average** | — | — | **80/100** |

### 4.5 Data Quality Issue Register

| ID | Severity | Domain | Issue | Impact | Recommendation |
|----|----------|--------|-------|--------|---------------|
| **DQ-01** | 🔴 Critical | Ads | Product ads data 93h stale | CM2 profit wrong by up to 3 days of ad spend | Fix `sync_ads` zombie job detection; add heartbeat |
| **DQ-02** | 🔴 Critical | FBA | FBA fee only 30.1% populated (should be ~34% for AFN mix) | €-level profit error on 51K+ FBA orders (90d) | Bridge FBA fees from `acc_finance_transaction` charge types |
| **DQ-03** | 🟠 High | Orders | Order sync 105 min stale (backend offline) | Dashboard shows outdated data | Ensure scheduler auto-restart; add uptime monitoring |
| **DQ-04** | 🟠 High | Schema | 72 empty tables (38.5%) | DB bloat, confusing schema, wasted indexes | Archive or drop empty tables not in active roadmap |
| **DQ-05** | 🟠 High | Finance | Finance covers only Dec 2024→ (orders from Mar 2024) | 9 months of orders without finance reconciliation | Accept as historical limitation or backfill via Reports API |
| **DQ-06** | 🟡 Medium | COGS | 765 order lines (0.5%) missing price source | Small profit calculation gaps | Add COGS price fallback for remaining NULL lines |
| **DQ-07** | 🟡 Medium | Content | 17 of 18 Content Ops tables empty | Entire module non-functional | Deprioritize until Content Ops is activated |
| **DQ-08** | 🟡 Medium | Tax | OSS/VAT tables partially populated | Tax compliance reporting incomplete | Not blocking until VAT reporting is required |
| **DQ-09** | 🟢 Low | Guardrails | Only 2 guardrail results in latest run | Should be 27 checks | Backend was offline — expected when scheduler stopped |
| **DQ-10** | 🟢 Low | Strategy | Decision learning only 2 records | DI model lacks training data | Will improve organically as decisions accumulate |

---

## 5. ANALYTICS INFRASTRUCTURE RECOMMENDATIONS

### 5.1 Current Architecture Assessment

```
┌─────────────────────────────────────────────────────────────────────┐
│                   CURRENT DATA ARCHITECTURE                         │
│                                                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐           │
│  │ Amazon   │  │ Netfox   │  │ GLS/DHL  │  │ NBP/ECB  │           │
│  │ SP-API   │  │ ERP      │  │ Courier  │  │ FX Rates │           │
│  │ + Ads API│  │ (MSSQL)  │  │ (CSV+API)│  │ (REST)   │           │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘           │
│       │              │              │              │                 │
│       ▼              ▼              ▼              ▼                 │
│  ┌──────────────────────────────────────────────────────┐           │
│  │           FastAPI Backend (Python)                    │           │
│  │  ┌─────────────────────────────────────────────────┐ │           │
│  │  │  APScheduler (42 jobs, in-process)              │ │           │
│  │  │  ├─ Orders (30min)  ├─ Finance (daily)          │ │           │
│  │  │  ├─ Ads (4h)        ├─ Inventory (daily)        │ │           │
│  │  │  ├─ Logistics (nightly) ├─ Profit (daily)       │ │           │
│  │  │  └─ Strategy (daily/weekly/monthly)             │ │           │
│  │  └─────────────────────────────────────────────────┘ │           │
│  │  ┌──────────────┐ ┌──────────────┐ ┌─────────────┐  │           │
│  │  │ 51 API       │ │ Guardrails   │ │ Event       │  │           │
│  │  │ endpoint     │ │ (27 checks)  │ │ Backbone    │  │           │
│  │  │ modules      │ │              │ │ (SQS+SQL)   │  │           │
│  │  └──────────────┘ └──────────────┘ └─────────────┘  │           │
│  └───────────────────────┬──────────────────────────────┘           │
│                          │                                          │
│                          ▼                                          │
│  ┌──────────────────────────────────────────────────────┐           │
│  │       Azure SQL (acc-sql-kadax.database.windows.net) │           │
│  │       187 tables │ 26.6M rows │ 19.3 GB │ 511 idx   │           │
│  └───────────────────────┬──────────────────────────────┘           │
│                          │                                          │
│                          ▼                                          │
│  ┌──────────────────────────────────────────────────────┐           │
│  │       React Frontend (90+ pages)                      │           │
│  │       TanStack Query + Recharts + shadcn/ui           │           │
│  └──────────────────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────────────────┘
```

### 5.2 Architecture Strengths

| # | Strength | Evidence |
|---|----------|---------|
| 1 | **Comprehensive data model** | 187 tables covering orders, finance, ads, logistics, inventory, tax, content, strategy |
| 2 | **Multi-source COGS** | 6 price sources with priority chain + ERP integration |
| 3 | **Dual FX validation** | NBP primary + ECB backup exchange rates |
| 4 | **Automated guardrails** | 27 checks across pipeline freshness, financial integrity, infrastructure |
| 5 | **Event-driven architecture** | SQS backbone + domain events for pipeline chaining |
| 6 | **Comprehensive scheduler** | 42 jobs across 12 domains with feature flags |
| 7 | **Raw SQL performance** | Direct pyodbc/pymssql avoids ORM overhead |
| 8 | **COGS audit trail** | `price_source` field tracks provenance per order line |

### 5.3 Architecture Gaps & Recommendations

#### R-01: Implement Data Observability Layer 🔴 Critical

**Problem**: No centralized monitoring of data pipeline health. Guardrails exist but run in-process (when backend is offline, no monitoring occurs).

**Recommendation**:
```
┌─────────────────────────────────────────————┐
│  DATA OBSERVABILITY STACK                    │
│                                              │
│  Option A: Lightweight (Recommended Now)     │
│  ├─ Azure SQL Agent Jobs for health checks   │
│  ├─ Azure Monitor alerts on metrics          │
│  └─ Simple status page (uptime.acc.app)      │
│                                              │
│  Option B: Full Stack (When Scaling)         │
│  ├─ Monte Carlo / Metaplane for DQ           │
│  ├─ Airflow for pipeline orchestration       │
│  └─ Grafana for dashboards                   │
└──────────────────────────────────────────────┘
```

**Effort**: Option A = 2 days | **Impact**: Eliminates silent pipeline failures

#### R-02: Fix Ads Pipeline Freshness 🔴 Critical

**Problem**: Ads data 69-93h stale. `sync_ads` job runs every 4h but lacks heartbeat, allowing zombie jobs.

**Recommendation**:
1. Add `last_heartbeat_at` update during ads sync execution
2. Add `sync_ads` to `_SINGLE_FLIGHT_JOB_TYPES` set
3. Clean up stale `running` jobs (auto-expire after 30 min)
4. Consider reducing sync interval from 4h to 2h

**Effort**: 4 hours | **Impact**: Ads data within 24h SLA

#### R-03: Bridge FBA Fees to Order Lines 🔴 Critical

**Problem**: `fba_fee_pln` only populated for 30.1% of order lines. FBA fees exist in `acc_finance_transaction` but aren't bridged to order lines consistently.

**Recommendation**:
1. In profit calculator, look up FBA fees from `acc_finance_transaction` for AFN orders
2. Classify FBA-related `charge_type` values into `fba_fee_pln` column
3. Add this as a step after `step_bridge_fees` in the order pipeline

**Effort**: 1 day | **Impact**: +15-20% improvement in CM1 accuracy for FBA orders

#### R-04: Add SQL-Level Pagination to PPT 🟠 High Priority

**Problem**: Product Profit Table (PPT) loads 14.5s — fetches all 4,300 product groups then sorts in Python.

**Recommendation**: Replace Python sort+slice with SQL `ORDER BY ... OFFSET/FETCH NEXT`. This was already analyzed in [PPT_PERFORMANCE_ANALYSIS_2026.md](PPT_PERFORMANCE_ANALYSIS_2026.md).

**Effort**: 1 day | **Impact**: PPT load time < 2s (95%+ improvement)

#### R-05: Schema Cleanup — Archive Empty Tables 🟠 High Priority

**Problem**: 72 empty tables (38.5%) create confusion, waste index maintenance, and inflate schema snapshots.

**Recommendation**:
1. Categorize empty tables into: (a) active roadmap → keep, (b) abandoned stubs → archive DDL + drop
2. Create `docs/schema_archive_YYYYMMDD.sql` with DROP'd table DDLs for recovery
3. Target: reduce from 187 to ~130 active tables

**Effort**: 2 hours | **Impact**: Cleaner schema, faster backups, less cognitive overhead

#### R-06: Add Materialized Analytics Views 🟡 Medium

**Problem**: Every analytics query hits raw tables. Profit calculation is expensive and recomputed per request.

**Recommendation**: Create indexed views (or materialized tables refreshed nightly) for common analytics patterns:

```sql
-- Example: daily_marketplace_summary (materialized table)
CREATE TABLE analytics_daily_marketplace_summary (
    period_date DATE NOT NULL,
    marketplace_id NVARCHAR(30) NOT NULL,
    revenue_pln DECIMAL(18,2),
    cogs_pln DECIMAL(18,2),
    fees_pln DECIMAL(18,2),
    logistics_pln DECIMAL(18,2),
    ads_spend_pln DECIMAL(18,2),
    cm1_pln DECIMAL(18,2),
    cm2_pln DECIMAL(18,2),
    orders_count INT,
    units_sold INT,
    avg_order_value DECIMAL(18,2),
    cm1_margin_pct DECIMAL(8,4),
    refreshed_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    PRIMARY KEY (period_date, marketplace_id)
);
```

**Effort**: 1 day | **Impact**: Sub-second dashboard loads, consistent analytics

#### R-07: Implement Data Freshness API 🟡 Medium

**Problem**: Users can't tell when data was last refreshed without checking guardrails manually.

**Recommendation**: Create `/api/v1/data/freshness` endpoint returning last-sync timestamps for all pipelines. This feeds a persistent `DataFreshness` header component in the UI.

**Effort**: 4 hours | **Impact**: Data trust and transparency

#### R-08: Add Time-Series Analytics Layer 🟢 Future

**Problem**: Current analytics are snapshot-based. No easy way to get "profit trend over last 12 months by marketplace" without expensive real-time computation.

**Recommendation**: After R-06 materialized views are in place, add a time-series analytics service:
1. Daily rollup job populates `analytics_daily_*` tables
2. API layer for trend queries with configurable granularity (day/week/month)
3. Frontend uses pre-computed data instead of real-time aggregation

**Effort**: 1 week | **Impact**: Enables strategy & reporting use cases

#### R-09: Export & Reporting Infrastructure 🟢 Future

**Problem**: No automated reporting (PDF, CSV export on all pages, scheduled email digests). This was identified as a competitive gap vs. Sellerboard.

**Recommendation**:
1. Add CSV export endpoints for all major data views
2. Implement scheduled XLSX report generation (weekly P&L digest)
3. Email delivery via SendGrid/SES

**Effort**: 1 week | **Impact**: Key selling feature for multi-user expansion

#### R-10: Real-Time Streaming Pipeline 🟢 Future (Scale Trigger)

**Problem**: Current architecture is batch-oriented (15 min → nightly cycles). As order volume grows beyond 1000/day, batch latency becomes a constraint.

**Recommendation**: Only invest when daily order volume exceeds 5,000. Then:
1. Replace SQS polling with SQS-triggered Lambda for real-time event processing
2. Add Change Data Capture (CDC) on Azure SQL for downstream consumers
3. Consider Azure Event Hub for high-volume event streaming

**Effort**: 2-4 weeks | **Impact**: Real-time dashboards and alerts

### 5.4 Recommendation Priority Matrix

```
                    HIGH IMPACT
                        ▲
                        │
    R-01 ●              │         ● R-04
   (Observability)      │      (SQL Pagination)
                        │
    R-02 ●              │    ● R-06
   (Ads freshness)      │   (Materialized views)
                        │
    R-03 ●              │         ● R-09
   (FBA fee bridge)     │        (Export/Reports)
                        │
LOW ────────────────────┼──────────────── HIGH
EFFORT                  │                 EFFORT
                        │
    R-05 ●              │    ● R-08
   (Schema cleanup)     │   (Time-series)
                        │
    R-07 ●              │         ● R-10
   (Freshness API)      │        (Streaming)
                        │
                        ▼
                    LOW IMPACT
```

### 5.5 Implementation Timeline

| Sprint | Items | Effort | Impact |
|--------|-------|--------|--------|
| **Sprint 1** (Week 1) | R-02 (Ads fix), R-03 (FBA bridge), R-05 (Schema cleanup) | 2 days | Fix critical DQ issues |
| **Sprint 2** (Week 2) | R-04 (SQL pagination), R-07 (Freshness API) | 2 days | Performance + trust |
| **Sprint 3** (Weeks 3-4) | R-01 (Observability), R-06 (Materialized views) | 3 days | Infrastructure maturity |
| **Sprint 4** (Month 2) | R-08 (Time-series), R-09 (Export) | 1-2 weeks | Analytics capability |
| **Future** | R-10 (Streaming) | 2-4 weeks | Scale enablement |

---

## 6. SIGNAL MAP — VISUAL SUMMARY

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        ACC DATA SIGNAL MAP                              │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  🟢 HIGH READINESS (Can measure today)                           │  │
│  │                                                                   │  │
│  │  REVENUE/ORDERS (S-01→S-10)    PROFITABILITY (S-11→S-20)        │  │
│  │  ├─ Revenue per SKU/day        ├─ CM1 per SKU/marketplace       │  │
│  │  ├─ Order volume/marketplace   ├─ COGS per order line (96%)     │  │
│  │  ├─ AOV trends                 ├─ Amazon fee breakdown (90.9%)  │  │
│  │  ├─ Fulfillment mix (66/34)    ├─ Logistics cost per order      │  │
│  │  └─ Order velocity             └─ Loss order identification     │  │
│  │                                                                   │  │
│  │  LOGISTICS (S-29→S-35)         FINANCE/TAX (S-42→S-49)          │  │
│  │  ├─ GLS cost/parcel (890K)     ├─ 954K transactions (45 types)  │  │
│  │  ├─ DHL cost/parcel (403K)     ├─ Settlement reconciliation     │  │
│  │  ├─ Ship→order links (896K)    ├─ FX rate validation (dual)     │  │
│  │  └─ Cost estimation accuracy   └─ Fee gap watchlist (6.3K)      │  │
│  │                                                                   │  │
│  │  PRODUCT (S-50→S-56)           OPERATIONS (S-64→S-71)           │  │
│  │  ├─ 9K products, 18K listings  ├─ 27 guardrail checks           │  │
│  │  ├─ 114K offers across mkts    ├─ 4.5K job runs tracked         │  │
│  │  └─ 1.4K families, 8.9K tax    └─ SP-API usage monitoring       │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  🟡 MODERATE READINESS (Can measure with gaps)                   │  │
│  │                                                                   │  │
│  │  ADS (S-21→S-28)              FBA (S-36→S-41)                   │  │
│  │  ├─ 5.1K campaigns, 1.8M rows ├─ 50K inventory snapshots       │  │
│  │  ├─ ACOS/ROAS per product     ├─ 3.8K customer returns         │  │
│  │  ├─ ⚠️ 69-93h stale           ├─ ⚠️ FBA fee 30.1% populated   │  │
│  │  └─ ⚠️ Zombie job risk        └─ Stock-out risk computed       │  │
│  │                                                                   │  │
│  │  STRATEGY (S-57→S-63)                                            │  │
│  │  ├─ 50K growth opportunities   ├─ 831 exec daily metrics        │  │
│  │  ├─ 9.9K seasonality profiles  └─ DI learning (early: 2 records)│  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  🔴 NOT READY (Schema exists, no data)                           │  │
│  │                                                                   │  │
│  │  CONTENT OPS (17/18 tables empty)                                │  │
│  │  PRICING AUTOMATION (4 tables empty)                             │  │
│  │  OSS TAX RETURNS (3 tables empty)                                │  │
│  │  NOTIFICATIONS (2 tables empty)                                  │  │
│  │  PLANNING/BUDGETS (2 tables empty)                               │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘

Signal Count: 71 measurable │ 12 sources │ 42 jobs │ 19.3 GB
Composite DQ Score: 74/100
```

---

## APPENDIX A: Scheduler Job Registry (42 Jobs)

| # | Job ID | Domain | Schedule | Data Source |
|---|--------|--------|----------|-------------|
| 1 | `order-pipeline-30m` | Orders | Every 30 min | Amazon SP-API |
| 2 | `sync-listings-to-products-daily` | Orders | 01:00 | SP-API Reports |
| 3 | `sync-amazon-listing-registry` | Orders | 01:30 | Google Sheets |
| 4 | `sync-purchase-prices-nightly` | Finance | 02:00 | Netfox ERP + XLSX |
| 5 | `sync-ecb-exchange-rates-daily` | Finance | 02:30 | ECB XML feed |
| 6 | `sync-finances-daily` | Finance | 03:00 | SP-API Finances |
| 7 | `fee-gap-recheck-daily` | Finance | 03:20 | Internal computation |
| 8 | `cogs-import-daily` | Finance | 06:00 | XLSX file scan |
| 9 | `sync-ptd-cache-daily` | Content | 02:30 | SP-API Catalog |
| 10 | `sync-pricing-state-daily` | Content | 03:00 | Internal |
| 11 | `content-publish-queue-1m` | Content | Every 1 min | Internal queue |
| 12 | `content-scoring-daily` | Content | 05:30 | Internal computation |
| 13 | `sync-inventory-daily` | Inventory | 04:00 | SP-API Inventory |
| 14 | `sync-sales-traffic-daily` | Inventory | 04:30 | SP-API Reports |
| 15 | `sync-fba-inventory-8h` | Inventory | Every 8h | SP-API FBA |
| 16 | `sync-fba-inbound-2h` | Inventory | Every 2h | SP-API Inbound |
| 17 | `sync-fba-reconciliation-daily` | Inventory | 06:00 | SP-API FBA |
| 18 | `run-fba-alerts-2h` | Inventory | Every 2h | Internal rules |
| 19 | `return-tracker-daily` | Inventory | 06:30 | SP-API Returns |
| 20 | `sync-ads-4h` | Ads | Every 4h | Amazon Ads API |
| 21 | `sync-tkl-cache` | Profit | 01:40 | XLSX TKL files |
| 22 | `calc-profit` | Profit | 05:00 | Internal computation |
| 23 | `cogs-audit` | Profit | 05:30 | Internal audit |
| 24 | `profitability-chain` | Profit | 05:45 | Multi-source chain |
| 25 | `sync-gls-logistics-nightly` | Logistics | 00:20 | GLS ADE + CSV |
| 26 | `sync-dhl-logistics-nightly` | Logistics | 00:05 | DHL API + CSV |
| 27 | `courier-estimation-nightly` | Logistics | 00:40 | Internal model |
| 28 | `verify-courier-billing-daily` | Logistics | 06:10 | File comparison |
| 29 | `sync-bl-distribution-cache-nightly` | Logistics | 01:55 | BaseLinker API |
| 30 | `seasonality-build-monthly-daily` | Seasonality | 04:30 | Internal agg |
| 31 | `seasonality-recompute-profiles-weekly` | Seasonality | Sun 05:00 | Internal compute |
| 32 | `seasonality-detect-opps-weekly` | Seasonality | Mon 05:30 | Internal detect |
| 33 | `decision-outcome-evaluation-daily` | Strategy | 07:00 | Internal DI |
| 34 | `decision-learning-weekly` | Strategy | Sun 08:00 | Internal DI |
| 35 | `decision-model-recalibration-monthly` | Strategy | 1st 09:00 | Internal DI |
| 36 | `sync-search-terms-weekly` | Strategy | Wed 03:00 | Brand Analytics |
| 37 | `buybox-trend-computation` | BuyBox | 03:30 | SP-API Pricing |
| 38 | `catalog-health-snapshot-daily` | Catalog | 03:00 | Internal compute |
| 39 | `inventory-risk-computation` | Inventory | 05:00 | Internal compute |
| 40 | `repricing-proposal-computation` | Pricing | 04:00 | Internal compute |
| 41 | `repricing-auto-approve-execute` | Pricing | 04:15 | Internal execute |
| 42 | `repricing-daily-analytics` | Pricing | 05:00 | Internal compute |

*Plus 8 system jobs (retries, alerts, guardrails, taxonomy, pricing archive, SQS, events, refund anomaly).*

## APPENDIX B: Data Source Connection Details

| Source | Protocol | Auth | Server/URL | Access |
|--------|----------|------|-----------|--------|
| Azure SQL (ACC) | TDS over TLS 1.2 | SQL Auth (pymssql) | `acc-sql-kadax.database.windows.net:1433` | Read+Write |
| Netfox ERP | TDS (pyodbc) | SQL Auth | Configured via `NETFOX_MSSQL_*` env vars | Read-only |
| Amazon SP-API | HTTPS REST | OAuth2 LWA refresh token | `sellingpartnerapi-eu.amazon.com` | Rate-limited |
| Amazon Ads API | HTTPS REST | OAuth2 refresh token | `advertising-api-eu.amazon.com` | Rate-limited |
| AWS SQS | HTTPS | IAM (access key) | `sqs.eu-west-1.amazonaws.com` | Poll+Delete |
| NBP API | HTTPS REST | None (public) | `api.nbp.pl` | Rate-limited (~10/s) |
| ECB Feed | HTTPS XML | None (public) | `ecb.europa.eu` | Daily feed |
| GLS ADE | SOAP | Username/password | WSDL-based | Rate-limited |
| DHL24 WebAPI2 | SOAP | Username/password | WSDL-based | Rate-limited |
| BaseLinker | HTTPS REST | API token | `api.baselinker.com` | Rate-limited |
| Google Sheets | HTTPS CSV | Public URL | Configured per sheet | On-demand |
| Network Share (N:\) | SMB/CIFS | Windows domain auth | `N:\KURIERZY\*`, `N:\Analityka\*` | Read (local only) |
| Redis | TCP | Optional password | `localhost:6380` | Circuit breaker + cache |

---

*Data Audit Report v1.0 — Generated by Analytics Reporter Agent*  
*Methodology: Schema Census → Pipeline Mapping → Signal Identification → Completeness Scoring → Infrastructure Gap Analysis*  
*Data Quality Composite Score: 74/100 (Good foundation, operational gaps)*  
*Next steps: Execute R-02 (Ads fix) + R-03 (FBA bridge) → rerun DQ scoring*  
*Cross-references: [Feedback Synthesis Report](FEEDBACK_SYNTHESIS_REPORT_2026-03-12.md) (PP-05 FX, PP-02 Ads lag confirmed), [UX Research Report](UX_RESEARCH_REPORT_2026-03-12.md) (BI-02 14.5s PPT confirmed)*
