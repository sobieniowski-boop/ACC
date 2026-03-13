# ACC — Database Schema Specification

> Version: 2026-03-12 | Database: Azure SQL (MSSQL) | Migration Tool: Alembic
> Total Tables: ~130+ | Alembic Revisions: 40 (fm001 → eb038)

---

## 1. Schema Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     CORE DOMAIN                              │
│  acc_user, acc_marketplace, acc_canonical_product,           │
│  acc_marketplace_presence                                    │
├─────────────────────────────────────────────────────────────┤
│                     ORDERS & FINANCE                         │
│  acc_order, acc_order_item, acc_order_item_finance,          │
│  acc_shipment, acc_finance_transaction, acc_exchange_rate,   │
│  acc_profitability, acc_profitability_product                │
├─────────────────────────────────────────────────────────────┤
│                     PRODUCTS & CATALOG                       │
│  acc_product, acc_offer, acc_listing_state,                  │
│  acc_family_group, acc_family_child,                         │
│  acc_family_child_market_link, acc_sku_mapping,              │
│  acc_purchase_price, acc_import_job, acc_import_record       │
├─────────────────────────────────────────────────────────────┤
│                     PRICING & BUYBOX                         │
│  acc_pricing_snapshot, acc_pricing_recommendation,           │
│  acc_pricing_rule, acc_buybox_snapshot,                      │
│  acc_buybox_competitor, acc_buybox_trend, acc_buybox_alert   │
├─────────────────────────────────────────────────────────────┤
│                     ADVERTISING                              │
│  acc_ads_campaign, acc_ads_daily_stat                        │
├─────────────────────────────────────────────────────────────┤
│                     CONTENT                                  │
│  acc_content_task, acc_content_version, acc_content_asset,   │
│  acc_content_asset_link, acc_content_experiment,             │
│  acc_content_experiment_variant, acc_content_multilang_job   │
├─────────────────────────────────────────────────────────────┤
│                     FBA & INVENTORY                          │
│  acc_fba_inventory_snapshot, acc_fba_inbound_shipment,       │
│  acc_fba_inbound_shipment_line, acc_fba_bundle,             │
│  acc_fba_bundle_event, acc_fba_kpi_snapshot,                │
│  acc_fba_sku_status, acc_fba_shipment_plan, acc_fba_case,   │
│  acc_fba_case_event, acc_fba_fee_reference,                 │
│  acc_fba_register, acc_fba_receiving_reconciliation,         │
│  acc_manage_inventory_*, acc_taxonomy_prediction             │
├─────────────────────────────────────────────────────────────┤
│                     INVENTORY RISK                            │
│  acc_inventory_risk_score, acc_inventory_risk_snapshot,      │
│  acc_stockout_watchlist, acc_overstock_report,               │
│  acc_replenishment_plan, acc_inventory_risk_alert,           │
│  acc_inventory_risk_trend                                    │
├─────────────────────────────────────────────────────────────┤
│                     LOGISTICS                                │
│  acc_dhl_shipment, acc_dhl_piece, acc_dhl_event,            │
│  acc_gls_shipment, acc_gls_event, acc_courier_*             │
├─────────────────────────────────────────────────────────────┤
│                     FINANCE CENTER                           │
│  acc_fin_*, vat_*, oss_*, transport_evidence_record,        │
│  fba_stock_movement_ledger, compliance_issue                │
├─────────────────────────────────────────────────────────────┤
│                     STRATEGY & INTELLIGENCE                  │
│  acc_strategy_opportunity, acc_strategy_playbook,            │
│  acc_strategy_execution, acc_strategy_experiment,            │
│  acc_strategy_learning, acc_strategy_outcome,                │
│  acc_seasonality_*, acc_repricing_*,                         │
│  acc_catalog_health_*, acc_refund_anomaly,                   │
│  acc_serial_returner, acc_reimbursement_case                │
├─────────────────────────────────────────────────────────────┤
│                     PLATFORM                                 │
│  acc_event_log, acc_event_processing_log,                    │
│  acc_event_handler_health, acc_notification_*,               │
│  acc_sqs_queue_config, acc_sqs_dlq_entry,                   │
│  acc_event_wire, acc_event_replay_job,                       │
│  acc_alert, acc_alert_rule, acc_plan_month,                  │
│  acc_kpi_snapshot, acc_ai_recommendation, acc_ai_log         │
├─────────────────────────────────────────────────────────────┤
│                     OPERATOR & MULTI-SELLER                  │
│  acc_operator_case, acc_operator_case_event,                 │
│  acc_operator_action, acc_seller_account,                    │
│  acc_seller_credential, acc_user_seller_permission           │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. Core Domain Tables

### acc_user
| Column | Type | Constraints |
|---|---|---|
| id | UNIQUEIDENTIFIER | PK, DEFAULT NEWID() |
| email | NVARCHAR(255) | UNIQUE, NOT NULL |
| hashed_password | NVARCHAR(255) | NOT NULL |
| role | NVARCHAR(50) | NOT NULL (analyst/ops/category_mgr/director/admin) |
| is_active | BIT | DEFAULT 1 |
| is_superuser | BIT | DEFAULT 0 |
| allowed_marketplaces | NVARCHAR(500) | NULL (JSON array) |
| allowed_brands | NVARCHAR(500) | NULL (JSON array) |
| last_login_at | DATETIME2 | NULL |
| created_at | DATETIME2 | DEFAULT GETUTCDATE() |
| updated_at | DATETIME2 | DEFAULT GETUTCDATE() |

### acc_marketplace
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| code | NVARCHAR(10) | UNIQUE (DE, PL, FR, IT, ES, NL, SE, BE) |
| name | NVARCHAR(100) | NOT NULL |
| amazon_marketplace_id | NVARCHAR(50) | UNIQUE |
| currency | NVARCHAR(5) | NOT NULL |
| is_active | BIT | DEFAULT 1 |

---

## 3. Orders & Finance Tables

### acc_order
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| amazon_order_id | NVARCHAR(50) | UNIQUE, NOT NULL |
| marketplace_id | INT | FK → acc_marketplace |
| purchase_date | DATETIME2 | NOT NULL |
| order_status | NVARCHAR(30) | |
| order_total | DECIMAL(18,2) | |
| currency_code | NVARCHAR(5) | |
| fulfillment_channel | NVARCHAR(10) | (AFN/MFN) |
| sales_channel | NVARCHAR(100) | |
| ship_city | NVARCHAR(100) | |
| ship_state | NVARCHAR(50) | |
| ship_postal_code | NVARCHAR(20) | |
| ship_country | NVARCHAR(5) | |
| is_business_order | BIT | |
| is_prime | BIT | |
| created_at | DATETIME2 | DEFAULT GETUTCDATE() |
| updated_at | DATETIME2 | |

**Indexes**: `ix_acc_order_marketplace_date` (marketplace_id, purchase_date), `ix_acc_order_status`

### acc_order_item
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| order_id | INT | FK → acc_order |
| amazon_order_item_id | NVARCHAR(50) | NOT NULL |
| seller_sku | NVARCHAR(50) | NOT NULL |
| asin | NVARCHAR(20) | |
| quantity | INT | NOT NULL |
| item_price | DECIMAL(18,2) | |
| item_tax | DECIMAL(18,2) | |
| promotion_discount | DECIMAL(18,2) | |
| product_id | INT | FK → acc_product (nullable) |

### acc_order_item_finance
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| order_item_id | INT | FK → acc_order_item |
| charge_type | NVARCHAR(100) | NOT NULL |
| charge_amount | DECIMAL(18,4) | NOT NULL |
| charge_currency | NVARCHAR(5) | |
| posted_date | DATE | |
| source_marketplace_id | NVARCHAR(50) | |
| source_sku | NVARCHAR(50) | |

**Indexes**: `ix_oif_order_item` (order_item_id), `ix_oif_charge_type`

### acc_profitability
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| order_id | INT | FK → acc_order |
| marketplace_id | INT | |
| seller_sku | NVARCHAR(50) | |
| revenue_pln | DECIMAL(18,2) | |
| cogs_pln | DECIMAL(18,2) | |
| amazon_fees_pln | DECIMAL(18,2) | |
| logistics_pln | DECIMAL(18,2) | |
| ads_pln | DECIMAL(18,2) | |
| cm1_pln | DECIMAL(18,2) | |
| cm2_pln | DECIMAL(18,2) | |
| np_pln | DECIMAL(18,2) | |
| fx_rate | DECIMAL(18,6) | |
| computed_at | DATETIME2 | |

### acc_exchange_rate
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| date | DATE | NOT NULL |
| source_currency | NVARCHAR(5) | NOT NULL |
| target_currency | NVARCHAR(5) | NOT NULL |
| rate | DECIMAL(18,6) | NOT NULL |
| source | NVARCHAR(20) | (ECB/NBP) |

**Unique**: (date, source_currency, target_currency)

---

## 4. Products & Catalog Tables

### acc_product
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| sku | NVARCHAR(50) | NOT NULL |
| name | NVARCHAR(500) | |
| brand | NVARCHAR(100) | |
| ean | NVARCHAR(20) | |
| ergonode_sku | NVARCHAR(100) | |
| product_type_id | NVARCHAR(50) | |
| category_path | NVARCHAR(500) | |
| lifecycle_status | NVARCHAR(20) | |
| ergonode_synced_at | DATETIME2 | |
| created_at | DATETIME2 | |
| updated_at | DATETIME2 | |

### acc_offer
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| marketplace_id | INT | FK → acc_marketplace |
| seller_sku | NVARCHAR(50) | NOT NULL |
| asin | NVARCHAR(20) | |
| product_id | INT | FK → acc_product |
| status | NVARCHAR(20) | |
| price | DECIMAL(18,2) | |
| currency | NVARCHAR(5) | |
| fulfillment_channel | NVARCHAR(10) | |
| created_at | DATETIME2 | |
| updated_at | DATETIME2 | |

**Unique**: (seller_sku, marketplace_id)

### acc_listing_state
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| marketplace_id | INT | NOT NULL |
| seller_sku | NVARCHAR(50) | NOT NULL |
| asin | NVARCHAR(20) | |
| status | NVARCHAR(30) | (Active/Inactive/Suppressed) |
| buy_box_status | NVARCHAR(30) | |
| buy_box_price | DECIMAL(18,2) | |
| buy_box_landed_price | DECIMAL(18,2) | |
| is_suppressed | BIT | |
| suppression_reason | NVARCHAR(200) | |
| captured_at | DATETIME2 | |

### acc_canonical_product
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| internal_sku | NVARCHAR(50) | UNIQUE |
| ean | NVARCHAR(20) | |
| brand | NVARCHAR(100) | |
| category | NVARCHAR(200) | |
| lifecycle_status | NVARCHAR(20) | |

### acc_marketplace_presence
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| canonical_product_id | INT | FK → acc_canonical_product |
| marketplace_id | INT | FK → acc_marketplace |
| seller_sku | NVARCHAR(50) | |
| asin | NVARCHAR(20) | |
| status | NVARCHAR(20) | |
| last_seen | DATETIME2 | |

---

## 5. Pricing & Buy Box Tables

### acc_pricing_snapshot
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| marketplace_id | INT | |
| seller_sku | NVARCHAR(50) | |
| asin | NVARCHAR(20) | |
| our_price | DECIMAL(18,2) | |
| buybox_price | DECIMAL(18,2) | |
| buybox_seller | NVARCHAR(100) | |
| buybox_is_ours | BIT | |
| lowest_price | DECIMAL(18,2) | |
| captured_at | DATETIME2 | |

### acc_buybox_snapshot
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| marketplace_id | INT | |
| asin | NVARCHAR(20) | |
| seller_sku | NVARCHAR(50) | |
| our_price | DECIMAL(18,2) | |
| buybox_price | DECIMAL(18,2) | |
| we_own_buybox | BIT | |
| competitors_count | INT | |
| captured_at | DATETIME2 | |

### acc_repricing_strategy
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| name | NVARCHAR(100) | |
| strategy_type | NVARCHAR(30) | (rule_based/competitor_aware/ml_driven) |
| config | NVARCHAR(MAX) | JSON |
| min_margin_pct | DECIMAL(5,2) | |
| max_discount_pct | DECIMAL(5,2) | |
| is_active | BIT | |
| marketplace_ids | NVARCHAR(200) | JSON array |
| created_at | DATETIME2 | |

---

## 6. Logistics Tables

### acc_dhl_shipment
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| shipment_number | NVARCHAR(50) | UNIQUE |
| order_id | NVARCHAR(50) | |
| status | NVARCHAR(30) | |
| service_type | NVARCHAR(30) | |
| weight_kg | DECIMAL(8,3) | |
| created_at | DATETIME2 | |
| delivered_at | DATETIME2 | |

### acc_courier_shipment (unified)
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| carrier | NVARCHAR(10) | (DHL/GLS) |
| tracking_number | NVARCHAR(50) | |
| order_id | NVARCHAR(50) | |
| status | NVARCHAR(30) | |
| cost_estimated | DECIMAL(18,2) | |
| cost_actual | DECIMAL(18,2) | |
| created_at | DATETIME2 | |

---

## 7. Strategy & Intelligence Tables

### acc_strategy_opportunity
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| type | NVARCHAR(50) | |
| marketplace_id | INT | |
| seller_sku | NVARCHAR(50) | |
| title | NVARCHAR(200) | |
| description | NVARCHAR(MAX) | |
| estimated_impact_pln | DECIMAL(18,2) | |
| confidence | DECIMAL(5,2) | |
| status | NVARCHAR(20) | (detected/accepted/rejected/completed) |
| detected_at | DATETIME2 | |

### acc_refund_anomaly
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| anomaly_type | NVARCHAR(50) | |
| marketplace_id | INT | |
| seller_sku | NVARCHAR(50) | |
| severity | NVARCHAR(10) | (low/medium/high/critical) |
| refund_count | INT | |
| refund_amount | DECIMAL(18,2) | |
| baseline_rate | DECIMAL(8,4) | |
| current_rate | DECIMAL(8,4) | |
| detected_at | DATETIME2 | |
| status | NVARCHAR(20) | |

### acc_serial_returner
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| buyer_id | NVARCHAR(100) | |
| return_count | INT | |
| return_rate | DECIMAL(5,2) | |
| total_refund_amount | DECIMAL(18,2) | |
| risk_score | DECIMAL(5,2) | |
| first_seen | DATETIME2 | |
| last_seen | DATETIME2 | |

---

## 8. Operator & Multi-Seller Tables

### acc_operator_case
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| source_type | NVARCHAR(30) | (alert/anomaly/manual) |
| source_id | NVARCHAR(100) | |
| category | NVARCHAR(50) | |
| priority | NVARCHAR(10) | (low/medium/high/critical) |
| status | NVARCHAR(20) | (open/in_progress/resolved/closed/escalated) |
| title | NVARCHAR(200) | |
| description | NVARCHAR(MAX) | |
| assigned_to | NVARCHAR(255) | |
| created_at | DATETIME2 | |
| updated_at | DATETIME2 | |

### acc_seller_account
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| seller_id | NVARCHAR(50) | UNIQUE |
| name | NVARCHAR(200) | |
| status | NVARCHAR(20) | (active/suspended/onboarding/archived) |
| marketplace_ids | NVARCHAR(MAX) | JSON array |
| primary_marketplace | NVARCHAR(50) | |
| created_at | DATETIME2 | |
| updated_at | DATETIME2 | |

### acc_seller_credential
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| seller_account_id | INT | FK → acc_seller_account |
| credential_type | NVARCHAR(30) | (sp_api/ads_api/custom) |
| encrypted_value | NVARCHAR(MAX) | Fernet encrypted |
| status | NVARCHAR(20) | (active/revoked/expired) |
| created_at | DATETIME2 | |
| revoked_at | DATETIME2 | |

**Unique filtered index**: (seller_account_id, credential_type) WHERE status = 'active'

### acc_user_seller_permission
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| user_email | NVARCHAR(255) | NOT NULL |
| seller_account_id | INT | FK → acc_seller_account |
| permission_level | NVARCHAR(20) | (viewer/operator/manager/admin) |
| granted_at | DATETIME2 | |
| revoked_at | DATETIME2 | |

**Unique filtered index**: (user_email, seller_account_id) WHERE revoked_at IS NULL

---

## 9. Platform Tables

### acc_event_log
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| domain | NVARCHAR(50) | NOT NULL |
| action | NVARCHAR(100) | NOT NULL |
| payload | NVARCHAR(MAX) | JSON |
| idempotency_key | NVARCHAR(200) | UNIQUE |
| source | NVARCHAR(30) | (sqs/internal) |
| created_at | DATETIME2 | DEFAULT GETUTCDATE() |

### acc_alert
| Column | Type | Constraints |
|---|---|---|
| id | INT IDENTITY | PK |
| type | NVARCHAR(50) | |
| severity | NVARCHAR(10) | (info/warning/error/critical) |
| marketplace_id | INT | |
| title | NVARCHAR(200) | |
| message | NVARCHAR(MAX) | |
| is_read | BIT | DEFAULT 0 |
| is_resolved | BIT | DEFAULT 0 |
| auto_resolve | BIT | DEFAULT 0 |
| created_at | DATETIME2 | |

---

## 10. Migration Chain

```
fm001 → eb002 → eb003 → eb004 → eb004a → eb005 → eb006 → eb007 →
eb008 → eb009 → eb010 → eb011 → eb012 → eb013 → eb014 → eb015 →
eb016 → eb017 → eb018 → eb019 (SQS) → eb019 (Sellerboard) →
eb020 → eb021 → eb022 → eb023 → eb024 → eb025 → eb026 → eb027 →
eb028 → eb029 → eb030 → eb031 → eb032 → eb033 → eb034 → eb035 →
eb036 → eb037 → eb038
```

Total: **40 revisions** | Linear chain (no branches)
