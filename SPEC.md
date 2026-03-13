# ACC — Product Specification

> Version: 2026-03-12 | Product: Amazon Commerce Cockpit (ACC)
> Platform: Web Application (SPA) | Markets: 8 European Amazon marketplaces

---

## 1. Product Vision

**ACC (Amazon Commerce Cockpit)** is an all-in-one intelligence platform for managing and optimizing Amazon marketplace operations across 8 European markets (DE, PL, FR, IT, ES, NL, SE, BE).

### Target Users

| Persona | Role | Primary Use |
|---|---|---|
| **E-commerce Analyst** | ANALYST | View dashboards, analyze profitability, monitor KPIs |
| **Operations Manager** | OPS | Run sync jobs, manage FBA shipments, content tasks |
| **Category Manager** | CATEGORY_MGR | Update pricing, manage product strategies |
| **Director** | DIRECTOR | Approve decisions, review AI recommendations, inventory settings |
| **Admin** | ADMIN | Import financial data, manage users, system configuration |

### Value Proposition

1. **Unified View**: Single pane of glass for orders, inventory, pricing, advertising, content, logistics across 8 markets
2. **Real-time Profitability**: Order-level P&L with CM1 → CM2 → NP cascade, automated COGS and fee allocation
3. **Intelligence-Driven**: 11 AI engines for anomaly detection, optimization, and strategy recommendations
4. **Operational Efficiency**: Automated workflows for content publishing, repricing, FBA management
5. **Financial Control**: Finance center with ledger, reconciliation, tax compliance, multi-currency support

---

## 2. Feature Domains

### 2.1 Profitability Engine

**Purpose**: Order-level profit & loss computation with full cost waterfall.

| Feature | Description |
|---|---|
| **P&L Waterfall** | Revenue → COGS → Amazon Fees → CM1 → Ads → CM2 → Logistics → NP |
| **SKU Rollup** | Aggregate profitability by product, marketplace, time period |
| **What-If Simulator** | Model price changes and see margin impact before applying |
| **Fee Gap Analysis** | Detect discrepancies between expected and actual Amazon fees |
| **Data Quality Score** | Measure completeness of COGS mapping, fee coverage, FX rates |
| **Excel Export** | Download product profitability table as XLSX |
| **Product Tasks** | Track action items per SKU (missing COGS, pricing review) |
| **AI SKU Matcher** | ML-based suggestion engine for unmapped SKUs |

### 2.2 Order Management

**Purpose**: Real-time order synchronization from Amazon SP-API.

| Feature | Description |
|---|---|
| **Order Sync** | Hourly sync of orders across all 8 marketplaces |
| **Order Finance** | Financial event breakdown per order item |
| **Exchange Rates** | ECB/NBP daily rates for multi-currency conversion |
| **Fulfillment Mix** | FBA vs. MFN order tracking |
| **Backfill** | Historical order import with sellerboard data support |

### 2.3 Product Catalog & Families

**Purpose**: Product hierarchy management, cross-marketplace mapping.

| Feature | Description |
|---|---|
| **Family Groups** | Parent-child ASIN grouping, variation mapping |
| **Cross-Market Links** | Same product tracking across DE, PL, FR, etc. |
| **Coverage Analysis** | Identify missing marketplace presence |
| **Fix Packages** | Auto-generated fix suggestions for catalog issues |
| **Restructure Engine** | Analyze and execute family structure changes |
| **Ergonode Sync** | PIM integration for product master data |
| **Review Queue** | Operator queue for family mapping decisions |

### 2.4 Pricing & Buy Box

**Purpose**: Competitive pricing intelligence and optimization.

| Feature | Description |
|---|---|
| **Price Snapshots** | Track our price vs. Buy Box vs. competitors over time |
| **Buy Box Ownership** | Real-time monitoring of Buy Box win rate |
| **Pricing Rules** | Define min/max price boundaries, margin floors |
| **Repricing Strategies** | Rule-based, competitor-aware, ML-driven repricing |
| **Execution Pipeline** | Compute → Review → Approve/Reject → Execute → Push to Amazon |
| **Competitor Landscape** | Track competitor prices and seller counts per ASIN |
| **Analytics** | Repricing impact on revenue and margin over time |

### 2.5 Advertising

**Purpose**: Amazon Ads campaign monitoring and cost allocation.

| Feature | Description |
|---|---|
| **Campaign Sync** | Daily sync of Sponsored Products/Brands/Display campaigns |
| **Spend Summary** | Total spend, ACoS, RoAS across campaigns |
| **Per-SKU Allocation** | Ads cost allocated to individual SKUs for CM2 calculation |
| **Top Campaigns** | Rank campaigns by spend, sales, efficiency |
| **Profile Management** | Multi-profile support for different markets |

### 2.6 Inventory & FBA

**Purpose**: End-to-end FBA inventory lifecycle management.

| Feature | Description |
|---|---|
| **FBA Dashboard** | Overview of inventory levels, inbound shipments, aged/stranded stock |
| **Replenishment** | Automated reorder suggestions based on velocity and lead time |
| **Shipment Plans** | Create and track inbound shipment plans to FBA fulfillment centers |
| **Case Management** | Track Amazon cases (missing inventory, damage claims) with timeline |
| **Fee Audit** | Detect FBA fee overcharges by comparing against reference rates |
| **Launch Tracking** | New product launch pipeline with FBA readiness milestones |
| **Initiative Board** | Strategic inventory initiatives (expansion, rebalancing) |
| **Inventory Drafts** | Draft → Validate → Approve → Apply workflow for inventory changes |
| **Taxonomy Prediction** | ML-based product type classification for Browse Node assignment |

### 2.7 Content Operations

**Purpose**: Content creation, review, and publishing pipeline.

| Feature | Description |
|---|---|
| **Task Management** | Create, assign, track content tasks per SKU/marketplace |
| **Version Control** | Content versioning with diff view and approval workflow |
| **Policy Check** | Automated content policy compliance validation |
| **Asset Management** | Upload and link images, A+ content, videos to listings |
| **Publishing Pipeline** | Package → Push to Amazon with circuit breaker protection |
| **Product Type Mappings** | Map internal product types to Amazon category definitions |
| **Attribute Mappings** | Map internal attributes to Amazon required/recommended attributes |
| **AI Content Generation** | Generate listing content using AI models |
| **Multi-language** | Generate and manage translations for all marketplaces |
| **A/B Testing** | Create experiments on content variants and measure impact |
| **Content Scoring** | Automated quality scoring for listing optimization |
| **SEO Analysis** | Keyword density, title/bullet optimization scoring |

### 2.8 Finance Center

**Purpose**: Financial data management, reconciliation, and reporting.

| Feature | Description |
|---|---|
| **Finance Dashboard** | Revenue, costs, margins, cash flow overview |
| **Amazon Import** | Import transaction and settlement reports from Amazon |
| **Bank Import** | CSV bank statement import for payout reconciliation |
| **Ledger** | Double-entry accounting ledger with manual entries and reversals |
| **Account Management** | Chart of accounts, tax codes configuration |
| **Payout Reconciliation** | Match Amazon payouts with bank transactions |
| **Sync Diagnostics** | Data completeness, gap analysis, revenue integrity checks |

### 2.9 Tax Compliance

**Purpose**: EU VAT compliance and reporting.

| Feature | Description |
|---|---|
| **VAT Classification** | Automatic VAT event classification (domestic, reverse charge, OSS) |
| **OSS Reporting** | One-Stop-Shop quarterly period building and reporting |
| **Transport Evidence** | Evidence collection for cross-border shipment proof |
| **FBA Movement Tracking** | Stock movement VAT implications tracking |
| **Filing Readiness** | Per-country filing readiness assessment with blockers |
| **Compliance Issues** | Issue detection, assignment, and resolution workflow |
| **VAT Rate Management** | Country-specific VAT rates with override capability |
| **Audit Pack** | Generate tax audit archive for authorities |
| **Amazon Reconciliation** | Compare Amazon-calculated vs. self-calculated VAT |

### 2.10 Logistics

**Purpose**: Courier integration and shipping cost management.

| Feature | Description |
|---|---|
| **DHL Integration** | Tracking, POD, scan documents, billing import |
| **GLS Integration** | ADE API tracking, shipment management, billing |
| **Cost Allocation** | Per-order shipping cost allocation for NP calculation |
| **Coverage Matrix** | Courier coverage across countries and service types |
| **Order Linking** | Match courier shipments to Amazon orders |
| **KPI Monitoring** | Delivery performance, cost per shipment, on-time rate |
| **Pre-invoice Estimation** | Estimate logistics costs before billing |

### 2.11 Strategy & Intelligence

**Purpose**: Data-driven strategy recommendations and execution.

| Feature | Description |
|---|---|
| **Opportunity Detection** | Automated detection of revenue/margin improvement opportunities |
| **Strategy Playbooks** | Predefined playbook templates for common strategies |
| **Market Expansion** | Identify expansion opportunities in new marketplaces |
| **Bundle Candidates** | Suggest product bundling opportunities based on co-purchase data |
| **Experiment Framework** | Design, run, and measure strategic experiments |
| **Decision Intelligence** | Track execution → outcome → learning feedback loop |
| **Seasonality Engine** | Seasonal pattern detection, cluster-based forecasting |

### 2.12 Returns & Refunds

**Purpose**: Return management and refund anomaly detection.

| Feature | Description |
|---|---|
| **Return Dashboard** | Return rates, refund amounts, category breakdown |
| **Refund Anomaly Scan** | Statistical detection of abnormal refund patterns |
| **Serial Returner Detection** | Identify repeat returners with risk scoring |
| **Reimbursement Cases** | Track Amazon reimbursement claims for lost/damaged items |
| **Trend Analysis** | Refund rate trends over time per SKU and marketplace |

### 2.13 Platform & Operations

| Feature | Description |
|---|---|
| **Event Backbone** | Domain event bus with SQS topology and event wiring |
| **Notifications** | Multi-channel notification system (email, Slack, webhook) |
| **Operator Console** | Unified operations case management with action tracking |
| **Guardrails** | Runtime health checks and business rule validation |
| **Alert System** | Configurable alert rules with read/resolve workflow |
| **Job Orchestration** | Scheduled and on-demand job management with WebSocket progress |
| **Account Hub** | Multi-seller account management with encrypted credentials |

---

## 3. Non-Functional Requirements

### Performance

| Metric | Target |
|---|---|
| API p95 latency | < 500ms |
| Dashboard page load | < 2 seconds |
| Order sync throughput | 10,000 orders/hour |
| Concurrent users | 50 |
| WebSocket connections | 100 simultaneous |

### Reliability

| Metric | Target |
|---|---|
| Uptime | 99.5% |
| Job success rate | > 99% |
| Data sync freshness | < 2 hours |
| Recovery Time Objective | < 1 hour |

### Security

| Requirement | Implementation |
|---|---|
| Authentication | JWT HS256 with access + refresh tokens |
| Authorization | 5-level RBAC with marketplace/brand filtering |
| Encryption at rest | Azure SQL TDE + Fernet for credentials |
| Encryption in transit | TLS 1.2+ on all connections |
| Audit trail | Event log with idempotency |

### Scalability

| Dimension | Current | Target |
|---|---|---|
| Marketplaces | 8 | 12 (add UK, CZ, TR, SA) |
| Seller accounts | 1 | 10+ (multi-seller) |
| Products | ~2,000 | 10,000 |
| Orders/month | ~50,000 | 200,000 |
| Database tables | 130+ | 150 |

---

## 4. User Workflows

### Workflow 1: Daily Operations Review

```
Login → KPI Dashboard → Check alerts → Review profit overview
  → Drill into underperforming SKUs → Create action tasks
  → Check FBA inventory levels → Review replenishment suggestions
```

### Workflow 2: Content Publishing

```
Create content task → Write listing (AI assist) → Policy check
  → Submit for review → Approve → Package for publish
  → Push to Amazon → Monitor publish status → Verify on Amazon
```

### Workflow 3: Pricing Optimization

```
BuyBox Radar → Identify Buy Box losses → Review competitor prices
  → Create repricing strategy → Compute new prices
  → Review executions → Approve (or reject) → Execute push
  → Monitor impact on BuyBox win rate and margins
```

### Workflow 4: Financial Close

```
Import Amazon settlements → Import bank statements
  → Auto-match payouts → Review unmatched items
  → Create manual ledger entries → Run reconciliation
  → Check tax compliance → Generate filing reports
```

### Workflow 5: Inventory Planning

```
Review inventory dashboard → Check aged/stranded inventory
  → Review replenishment suggestions → Create draft
  → Validate draft → Director approval → Apply changes
  → Monitor inbound shipment status → Update forecasts
```

---

## 5. Integration Points

| System | Purpose | Protocol |
|---|---|---|
| Amazon SP-API | Orders, listings, reports, FBA, catalog | REST + OAuth2 |
| Amazon Ads API | Campaign data, spend metrics | REST + OAuth2 |
| DHL Parcel API | Shipment tracking, billing, POD | REST + API Key |
| GLS ADE API | Shipment tracking, label generation | SOAP + REST |
| Ergonode PIM | Product master data synchronization | REST + API Key |
| ECB Exchange Rates | Daily EUR exchange rates | REST (public) |
| Sellerboard | Historical order data import | File-based |
| Sentry | Error tracking and performance monitoring | SDK |
| Redis | Caching, rate limiting, job queue | TCP |
| Azure SQL | Primary database | TDS/TLS |

---

## 6. Data Model Summary

| Domain | Core Tables | Relationships |
|---|---|---|
| **Orders** | order, order_item, order_item_finance, shipment | order → items → finance |
| **Products** | product, offer, canonical_product, marketplace_presence | product → offers → marketplaces |
| **Families** | family_group, family_child, family_child_market_link | group → children → links |
| **Pricing** | pricing_snapshot, buybox_snapshot, pricing_rule, repricing_strategy | snapshots per SKU/market |
| **FBA** | fba_inventory_snapshot, fba_inbound_shipment, fba_case, fba_fee_reference | inventory → shipments → cases |
| **Finance** | fin_ledger_entry, fin_account, fin_payout_reconciliation | ledger → accounts |
| **Tax** | vat_event, oss_period, transport_evidence, compliance_issue | events → periods → issues |
| **Content** | content_task, content_version, content_asset, content_experiment | task → versions → assets |
| **Logistics** | dhl_shipment, gls_shipment, courier_shipment | shipments → events |
| **Intelligence** | strategy_opportunity, refund_anomaly, inventory_risk_score | detected → tracked → resolved |
| **Platform** | event_log, alert, notification, operator_case | events → alerts → cases |
