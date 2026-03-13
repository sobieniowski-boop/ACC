# ACC Database Schema Snapshot — 20260310

**Generated:** 2026-03-09T23:15:24.024064Z
**Checksum (SHA-256 prefix):** `aa024d8cef4d2ebd`
**Database:** Azure SQL (acc-sql-kadax.database.windows.net)

## Tables (187)

| # | Table | Rows |
|---|-------|------|
| 1 | `acc_ads_campaign` | 5,105 |
| 2 | `acc_ads_campaign_day` | 113,073 |
| 3 | `acc_ads_product_day` | 1,710,917 |
| 4 | `acc_ads_profile` | 10 |
| 5 | `acc_ai_recommendation` | 0 |
| 6 | `acc_al_alert_rules` | 21 |
| 7 | `acc_al_alerts` | 615 |
| 8 | `acc_al_job_semaphore` | 3 |
| 9 | `acc_al_jobs` | 1,161 |
| 10 | `acc_al_plan_lines` | 0 |
| 11 | `acc_al_plans` | 0 |
| 12 | `acc_al_product_task_comments` | 0 |
| 13 | `acc_al_product_tasks` | 78 |
| 14 | `acc_al_profit_snapshot` | 155,683 |
| 15 | `acc_al_task_owner_rules` | 0 |
| 16 | `acc_alert` | 0 |
| 17 | `acc_alert_rule` | 0 |
| 18 | `acc_amazon_listing_registry` | 17,997 |
| 19 | `acc_amazon_listing_registry_sync_state` | 1 |
| 20 | `acc_audit_log` | 6 |
| 21 | `acc_backfill_progress` | 7 |
| 22 | `acc_backfill_report_progress` | 39 |
| 23 | `acc_bl_distribution_order_cache` | 22,185 |
| 24 | `acc_bl_distribution_package_cache` | 23,217 |
| 25 | `acc_cache_bl_orders` | 1,169,386 |
| 26 | `acc_cache_dis_map` | 1,931,849 |
| 27 | `acc_cache_extras` | 9,836,371 |
| 28 | `acc_cache_invoices` | 1,721,201 |
| 29 | `acc_cache_packages` | 3,020,299 |
| 30 | `acc_co_ai_cache` | 0 |
| 31 | `acc_co_asset_links` | 0 |
| 32 | `acc_co_assets` | 0 |
| 33 | `acc_co_attribute_map` | 0 |
| 34 | `acc_co_impact_snapshots` | 0 |
| 35 | `acc_co_policy_checks` | 0 |
| 36 | `acc_co_policy_rules` | 0 |
| 37 | `acc_co_product_type_defs` | 1 |
| 38 | `acc_co_product_type_map` | 0 |
| 39 | `acc_co_publish_jobs` | 0 |
| 40 | `acc_co_retry_policy` | 0 |
| 41 | `acc_co_tasks` | 0 |
| 42 | `acc_co_versions` | 0 |
| 43 | `acc_cogs_import_log` | 7 |
| 44 | `acc_courier_audit_log` | 4 |
| 45 | `acc_courier_cost_estimate` | 0 |
| 46 | `acc_courier_estimation_kpi_daily` | 0 |
| 47 | `acc_courier_monthly_kpi_snapshot` | 6 |
| 48 | `acc_dhl_billing_document` | 150 |
| 49 | `acc_dhl_billing_line` | 403,043 |
| 50 | `acc_dhl_import_file` | 166 |
| 51 | `acc_dhl_jjd_map` | 197,605 |
| 52 | `acc_dhl_parcel_map` | 303,689 |
| 53 | `acc_event_log` | 556 |
| 54 | `acc_event_processing_log` | 6 |
| 55 | `acc_exchange_rate` | 3,036 |
| 56 | `acc_fba_bundle` | 0 |
| 57 | `acc_fba_bundle_event` | 0 |
| 58 | `acc_fba_case` | 1 |
| 59 | `acc_fba_case_event` | 5 |
| 60 | `acc_fba_config` | 2 |
| 61 | `acc_fba_customer_return` | 3,385 |
| 62 | `acc_fba_fee_reference` | 0 |
| 63 | `acc_fba_inbound_shipment` | 100 |
| 64 | `acc_fba_inbound_shipment_line` | 4,552 |
| 65 | `acc_fba_initiative` | 0 |
| 66 | `acc_fba_inventory_snapshot` | 44,598 |
| 67 | `acc_fba_kpi_snapshot` | 0 |
| 68 | `acc_fba_launch` | 0 |
| 69 | `acc_fba_receiving_reconciliation` | 422 |
| 70 | `acc_fba_report_diagnostic` | 828 |
| 71 | `acc_fba_shipment_plan` | 0 |
| 72 | `acc_fba_sku_status` | 0 |
| 73 | `acc_fee_gap_recheck_run` | 11 |
| 74 | `acc_fee_gap_watch` | 6,309 |
| 75 | `acc_fin_bank_line` | 0 |
| 76 | `acc_fin_chart_of_accounts` | 9 |
| 77 | `acc_fin_event_group_sync` | 1,280 |
| 78 | `acc_fin_ledger_entry` | 52,034 |
| 79 | `acc_fin_mapping_rule` | 11 |
| 80 | `acc_fin_reconciliation_payout` | 35 |
| 81 | `acc_fin_settlement_summary` | 35 |
| 82 | `acc_fin_tax_code` | 3 |
| 83 | `acc_finance_transaction` | 217,757 |
| 84 | `acc_gls_billing_document` | 9 |
| 85 | `acc_gls_billing_line` | 890,036 |
| 86 | `acc_gls_bl_map` | 4,101 |
| 87 | `acc_gls_import_file` | 377 |
| 88 | `acc_guardrail_results` | 506 |
| 89 | `acc_import_products` | 721 |
| 90 | `acc_inv_category_cvr_baseline` | 0 |
| 91 | `acc_inv_change_draft` | 0 |
| 92 | `acc_inv_change_event` | 0 |
| 93 | `acc_inv_item_cache` | 5,561 |
| 94 | `acc_inv_settings` | 1 |
| 95 | `acc_inv_traffic_asin_daily` | 35,092 |
| 96 | `acc_inv_traffic_rollup` | 80,326 |
| 97 | `acc_inv_traffic_sku_daily` | 22 |
| 98 | `acc_inventory_snapshot` | 2,285 |
| 99 | `acc_job_run` | 0 |
| 100 | `acc_listing_state` | 3 |
| 101 | `acc_mapping_change_log` | 43 |
| 102 | `acc_marketplace` | 13 |
| 103 | `acc_marketplace_profitability_rollup` | 751 |
| 104 | `acc_notification_destination` | 0 |
| 105 | `acc_notification_subscription` | 0 |
| 106 | `acc_offer` | 113,955 |
| 107 | `acc_offer_fee_expected` | 439 |
| 108 | `acc_order` | 840,852 |
| 109 | `acc_order_line` | 159,407 |
| 110 | `acc_order_logistics_fact` | 92,038 |
| 111 | `acc_order_logistics_shadow` | 116,271 |
| 112 | `acc_order_sync_state` | 13 |
| 113 | `acc_plan_line` | 0 |
| 114 | `acc_plan_month` | 0 |
| 115 | `acc_price_change_log` | 115 |
| 116 | `acc_pricing_recommendation` | 0 |
| 117 | `acc_pricing_rule` | 0 |
| 118 | `acc_pricing_snapshot` | 0 |
| 119 | `acc_pricing_sync_state` | 0 |
| 120 | `acc_product` | 8,989 |
| 121 | `acc_product_match_suggestion` | 50 |
| 122 | `acc_profit_cost_config` | 1 |
| 123 | `acc_profit_overhead_pool` | 0 |
| 124 | `acc_ptd_cache` | 0 |
| 125 | `acc_ptd_sync_state` | 0 |
| 126 | `acc_purchase_price` | 13,242 |
| 127 | `acc_return_daily_summary` | 347 |
| 128 | `acc_return_item` | 5,441 |
| 129 | `acc_return_sync_state` | 6 |
| 130 | `acc_sb_order_line_rebuild_state` | 2 |
| 131 | `acc_sb_order_line_staging` | 758,008 |
| 132 | `acc_sb_order_line_sync_state` | 1 |
| 133 | `acc_shipment` | 451,676 |
| 134 | `acc_shipment_cost` | 199,935 |
| 135 | `acc_shipment_event` | 0 |
| 136 | `acc_shipment_order_link` | 572,857 |
| 137 | `acc_shipment_pod` | 0 |
| 138 | `acc_shipping_cost` | 469,203 |
| 139 | `acc_sku_profitability_rollup` | 95,327 |
| 140 | `acc_sp_api_usage_daily` | 4,334 |
| 141 | `acc_spapi_usage` | 0 |
| 142 | `acc_taxonomy_alias` | 0 |
| 143 | `acc_taxonomy_node` | 1 |
| 144 | `acc_taxonomy_prediction` | 8,897 |
| 145 | `acc_tkl_cache_meta` | 1 |
| 146 | `acc_tkl_cache_rows` | 412,529 |
| 147 | `acc_user` | 1 |
| 148 | `amazon_clearing_reconciliation` | 0 |
| 149 | `compliance_issue` | 0 |
| 150 | `decision_learning` | 2 |
| 151 | `ecb_exchange_rate` | 1,814 |
| 152 | `executive_daily_metrics` | 751 |
| 153 | `executive_health_score` | 30 |
| 154 | `executive_opportunities` | 3,635 |
| 155 | `family_coverage_cache` | 690 |
| 156 | `family_fix_job` | 0 |
| 157 | `family_fix_package` | 16,932 |
| 158 | `family_issues_cache` | 46 |
| 159 | `family_restructure_log` | 10 |
| 160 | `family_restructure_run` | 28 |
| 161 | `fba_stock_movement_ledger` | 0 |
| 162 | `filing_readiness_snapshot` | 0 |
| 163 | `global_family` | 1,433 |
| 164 | `global_family_child` | 13,113 |
| 165 | `global_family_child_market_link` | 531 |
| 166 | `global_family_market_link` | 690 |
| 167 | `growth_opportunity` | 10,747 |
| 168 | `growth_opportunity_log` | 4 |
| 169 | `local_vat_ledger` | 0 |
| 170 | `marketplace_listing_child` | 16,561 |
| 171 | `opportunity_execution` | 40 |
| 172 | `opportunity_model_adjustments` | 0 |
| 173 | `opportunity_outcome` | 80 |
| 174 | `oss_return_line` | 0 |
| 175 | `oss_return_period` | 0 |
| 176 | `seasonality_cluster` | 0 |
| 177 | `seasonality_cluster_member` | 0 |
| 178 | `seasonality_index_cache` | 38,200 |
| 179 | `seasonality_monthly_metrics` | 38,202 |
| 180 | `seasonality_opportunity` | 1,402 |
| 181 | `seasonality_profile` | 9,948 |
| 182 | `seasonality_settings` | 7 |
| 183 | `strategy_experiment` | 0 |
| 184 | `transport_evidence_record` | 0 |
| 185 | `vat_event_ledger` | 60,271 |
| 186 | `vat_rate_mapping` | 27 |
| 187 | `vat_transaction_classification` | 60,271 |

**Total rows across all tables:** 26,588,120

## Indexes (511)

| Table | Index | Type | Unique | PK |
|-------|-------|------|--------|-----|
| `acc_ads_campaign` | `PK_acc_ads_campaign` | CLUSTERED | Yes | Yes |
| `acc_ads_campaign_day` | `PK_acc_ads_campaign_day` | CLUSTERED | Yes | Yes |
| `acc_ads_product_day` | `PK_acc_ads_product_day` | CLUSTERED | Yes | Yes |
| `acc_ads_profile` | `PK__acc_ads___AEBB701F4F6CBC46` | CLUSTERED | Yes | Yes |
| `acc_ai_recommendation` | `IX_acc_ai_generated` | NONCLUSTERED |  |  |
| `acc_ai_recommendation` | `IX_acc_ai_marketplace` | NONCLUSTERED |  |  |
| `acc_ai_recommendation` | `IX_acc_ai_product` | NONCLUSTERED |  |  |
| `acc_ai_recommendation` | `IX_acc_ai_type` | NONCLUSTERED |  |  |
| `acc_ai_recommendation` | `PK__acc_ai_r__3213E83F7960A050` | CLUSTERED | Yes | Yes |
| `acc_al_alert_rules` | `IX_acc_al_alert_rules_type` | NONCLUSTERED |  |  |
| `acc_al_alert_rules` | `PK__acc_al_a__3213E83F3F249EF8` | CLUSTERED | Yes | Yes |
| `acc_al_alerts` | `IX_acc_al_alerts_rule` | NONCLUSTERED |  |  |
| `acc_al_alerts` | `IX_acc_al_alerts_state` | NONCLUSTERED |  |  |
| `acc_al_alerts` | `PK__acc_al_a__3213E83F11453F1F` | CLUSTERED | Yes | Yes |
| `acc_al_job_semaphore` | `IX_acc_al_job_semaphore_holder` | NONCLUSTERED |  |  |
| `acc_al_job_semaphore` | `PK_acc_al_job_semaphore` | CLUSTERED | Yes | Yes |
| `acc_al_jobs` | `IX_acc_al_jobs_lease` | NONCLUSTERED |  |  |
| `acc_al_jobs` | `IX_acc_al_jobs_main` | NONCLUSTERED |  |  |
| `acc_al_jobs` | `IX_acc_al_jobs_retry` | NONCLUSTERED |  |  |
| `acc_al_jobs` | `PK__acc_al_j__3213E83F68E97B9C` | CLUSTERED | Yes | Yes |
| `acc_al_plan_lines` | `IX_acc_al_plan_lines_plan` | NONCLUSTERED |  |  |
| `acc_al_plan_lines` | `PK__acc_al_p__3213E83FF157107B` | CLUSTERED | Yes | Yes |
| `acc_al_plans` | `PK__acc_al_p__3213E83F9A62C926` | CLUSTERED | Yes | Yes |
| `acc_al_plans` | `UQ_acc_al_plan_month` | NONCLUSTERED | Yes |  |
| `acc_al_product_task_comments` | `IX_acc_al_product_task_comments_task` | NONCLUSTERED |  |  |
| `acc_al_product_task_comments` | `PK__acc_al_p__3213E83FFE9159FE` | CLUSTERED | Yes | Yes |
| `acc_al_product_tasks` | `IX_acc_al_product_tasks_main` | NONCLUSTERED |  |  |
| `acc_al_product_tasks` | `IX_acc_al_product_tasks_sku` | NONCLUSTERED |  |  |
| `acc_al_product_tasks` | `PK__acc_al_p__3213E83F7DDDD521` | CLUSTERED | Yes | Yes |
| `acc_al_profit_snapshot` | `IX_acc_al_profit_snapshot_date` | NONCLUSTERED |  |  |
| `acc_al_profit_snapshot` | `PK__acc_al_p__3213E83F6D9C29CD` | CLUSTERED | Yes | Yes |
| `acc_al_task_owner_rules` | `IX_acc_al_task_owner_rules_match` | NONCLUSTERED |  |  |
| `acc_al_task_owner_rules` | `PK__acc_al_t__3213E83FA32B0121` | CLUSTERED | Yes | Yes |
| `acc_alert` | `IX_acc_alert_rule` | NONCLUSTERED |  |  |
| `acc_alert` | `IX_acc_alert_state` | NONCLUSTERED |  |  |
| `acc_alert` | `IX_acc_alert_triggered` | NONCLUSTERED |  |  |
| `acc_alert` | `PK__acc_aler__3213E83F7CCDD3E4` | CLUSTERED | Yes | Yes |
| `acc_alert_rule` | `PK__acc_aler__3213E83FE3966B95` | CLUSTERED | Yes | Yes |
| `acc_amazon_listing_registry` | `IX_acc_amazon_listing_registry_asin` | NONCLUSTERED |  |  |
| `acc_amazon_listing_registry` | `IX_acc_amazon_listing_registry_ean` | NONCLUSTERED |  |  |
| `acc_amazon_listing_registry` | `IX_acc_amazon_listing_registry_internal_sku` | NONCLUSTERED |  |  |
| `acc_amazon_listing_registry` | `IX_acc_amazon_listing_registry_merchant_sku` | NONCLUSTERED |  |  |
| `acc_amazon_listing_registry` | `IX_acc_amazon_listing_registry_merchant_sku_alt` | NONCLUSTERED |  |  |
| `acc_amazon_listing_registry` | `PK__acc_amaz__3213E83F1C78D679` | CLUSTERED | Yes | Yes |
| `acc_amazon_listing_registry_sync_state` | `PK__acc_amaz__8656F73C8925F6D5` | CLUSTERED | Yes | Yes |
| `acc_audit_log` | `IX_acc_audit_log_date` | NONCLUSTERED | Yes |  |
| `acc_audit_log` | `PK__acc_audi__3213E83FE4AE5660` | CLUSTERED | Yes | Yes |
| `acc_backfill_progress` | `PK__acc_back__3213E83F8BDA4070` | CLUSTERED | Yes | Yes |
| `acc_backfill_progress` | `UQ__acc_back__F7535F464D70E488` | NONCLUSTERED | Yes |  |
| `acc_backfill_report_progress` | `PK__acc_back__3213E83F90E58B26` | CLUSTERED | Yes | Yes |
| `acc_backfill_report_progress` | `UQ__acc_back__F7535F46D28DF5EC` | NONCLUSTERED | Yes |  |
| `acc_bl_distribution_order_cache` | `IX_acc_bl_distribution_order_confirmed` | NONCLUSTERED |  |  |
| `acc_bl_distribution_order_cache` | `IX_acc_bl_distribution_order_external` | NONCLUSTERED |  |  |
| `acc_bl_distribution_order_cache` | `IX_acc_bl_distribution_order_package_nr` | NONCLUSTERED |  |  |
| `acc_bl_distribution_order_cache` | `PK__acc_bl_d__465962297D1F412F` | CLUSTERED | Yes | Yes |
| `acc_bl_distribution_package_cache` | `IX_acc_bl_distribution_package_inner` | NONCLUSTERED |  |  |
| `acc_bl_distribution_package_cache` | `IX_acc_bl_distribution_package_order` | NONCLUSTERED |  |  |
| `acc_bl_distribution_package_cache` | `IX_acc_bl_distribution_package_tracking` | NONCLUSTERED |  |  |
| `acc_bl_distribution_package_cache` | `PK__acc_bl_d__63846AE8AFE77F07` | CLUSTERED | Yes | Yes |
| `acc_cache_bl_orders` | `ix_cache_bl_extorder` | NONCLUSTERED |  |  |
| `acc_cache_bl_orders` | `ix_cache_bl_orderid` | NONCLUSTERED |  |  |
| `acc_cache_dis_map` | `ix_cache_dis_dis` | NONCLUSTERED |  |  |
| `acc_cache_dis_map` | `ix_cache_dis_holding` | NONCLUSTERED |  |  |
| `acc_cache_extras` | `ix_cache_ext_other` | NONCLUSTERED |  |  |
| `acc_cache_extras` | `ix_cache_ext_parcel` | NONCLUSTERED |  |  |
| `acc_cache_invoices` | `ix_cache_inv_courier` | NONCLUSTERED |  |  |
| `acc_cache_invoices` | `ix_cache_inv_parcel` | NONCLUSTERED |  |  |
| `acc_cache_packages` | `ix_cache_pkg_order` | NONCLUSTERED |  |  |
| `acc_cache_packages` | `ix_cache_pkg_tracking` | NONCLUSTERED |  |  |
| `acc_co_ai_cache` | `IX_acc_co_ai_cache_hash` | NONCLUSTERED | Yes |  |
| `acc_co_ai_cache` | `PK__acc_co_a__3213E83FF7385D3F` | CLUSTERED | Yes | Yes |
| `acc_co_asset_links` | `IX_acc_co_asset_links_lookup` | NONCLUSTERED |  |  |
| `acc_co_asset_links` | `PK__acc_co_a__3213E83FA3B88AE9` | CLUSTERED | Yes | Yes |
| `acc_co_assets` | `IX_acc_co_assets_hash` | NONCLUSTERED | Yes |  |
| `acc_co_assets` | `PK__acc_co_a__3213E83FC0F45A02` | CLUSTERED | Yes | Yes |
| `acc_co_attribute_map` | `IX_acc_co_attribute_map_lookup` | NONCLUSTERED |  |  |
| `acc_co_attribute_map` | `PK__acc_co_a__3213E83F8384F0E8` | CLUSTERED | Yes | Yes |
| `acc_co_impact_snapshots` | `IX_acc_co_impact_snapshots_lookup` | NONCLUSTERED |  |  |
| `acc_co_impact_snapshots` | `PK__acc_co_i__3213E83F8D46D456` | CLUSTERED | Yes | Yes |
| `acc_co_policy_checks` | `IX_acc_co_policy_checks_version` | NONCLUSTERED |  |  |
| `acc_co_policy_checks` | `PK__acc_co_p__3213E83FC009B195` | CLUSTERED | Yes | Yes |
| `acc_co_policy_rules` | `IX_acc_co_policy_rules_active` | NONCLUSTERED |  |  |
| `acc_co_policy_rules` | `PK__acc_co_p__3213E83F65C8A9F9` | CLUSTERED | Yes | Yes |
| `acc_co_product_type_defs` | `IX_acc_co_product_type_defs_lookup` | NONCLUSTERED |  |  |
| `acc_co_product_type_defs` | `IX_acc_co_product_type_defs_unique` | NONCLUSTERED | Yes |  |
| `acc_co_product_type_defs` | `PK__acc_co_p__3213E83F796C5D39` | CLUSTERED | Yes | Yes |
| `acc_co_product_type_map` | `IX_acc_co_product_type_map_match` | NONCLUSTERED |  |  |
| `acc_co_product_type_map` | `PK__acc_co_p__3213E83F5AD2BECC` | CLUSTERED | Yes | Yes |
| `acc_co_publish_jobs` | `IX_acc_co_publish_jobs_idempotency` | NONCLUSTERED |  |  |
| `acc_co_publish_jobs` | `IX_acc_co_publish_jobs_main` | NONCLUSTERED |  |  |
| `acc_co_publish_jobs` | `IX_acc_co_publish_jobs_manual_retry` | NONCLUSTERED |  |  |
| `acc_co_publish_jobs` | `IX_acc_co_publish_jobs_retry` | NONCLUSTERED |  |  |
| `acc_co_publish_jobs` | `PK__acc_co_p__3213E83F2A93A899` | CLUSTERED | Yes | Yes |
| `acc_co_retry_policy` | `IX_acc_co_retry_policy_lookup` | NONCLUSTERED |  |  |
| `acc_co_retry_policy` | `PK__acc_co_r__3213E83F2EB7372A` | CLUSTERED | Yes | Yes |
| `acc_co_tasks` | `IX_acc_co_tasks_main` | NONCLUSTERED |  |  |
| `acc_co_tasks` | `IX_acc_co_tasks_owner_due` | NONCLUSTERED |  |  |
| `acc_co_tasks` | `IX_acc_co_tasks_sku` | NONCLUSTERED |  |  |
| `acc_co_tasks` | `PK__acc_co_t__3213E83FDA299179` | CLUSTERED | Yes | Yes |
| `acc_co_versions` | `IX_acc_co_versions_lookup` | NONCLUSTERED |  |  |
| `acc_co_versions` | `IX_acc_co_versions_unique` | NONCLUSTERED | Yes |  |
| `acc_co_versions` | `PK__acc_co_v__3213E83FF8649E11` | CLUSTERED | Yes | Yes |
| `acc_cogs_import_log` | `PK__acc_cogs__3213E83F46D23FEE` | CLUSTERED | Yes | Yes |
| `acc_cogs_import_log` | `UQ__acc_cogs__AB51902A9B85E840` | NONCLUSTERED | Yes |  |
| `acc_courier_audit_log` | `PK__acc_cour__3213E83FD6EDEFF2` | CLUSTERED | Yes | Yes |
| `acc_courier_audit_log` | `UX_acc_courier_audit_scope` | NONCLUSTERED | Yes |  |
| `acc_courier_cost_estimate` | `IX_acc_courier_cost_estimate_status` | NONCLUSTERED |  |  |
| `acc_courier_cost_estimate` | `PK__acc_cour__3213E83F7C6A58C0` | CLUSTERED | Yes | Yes |
| `acc_courier_cost_estimate` | `UX_acc_courier_cost_estimate_shipment_estimator` | NONCLUSTERED | Yes |  |
| `acc_courier_estimation_kpi_daily` | `PK_acc_courier_estimation_kpi_daily` | CLUSTERED | Yes | Yes |
| `acc_courier_monthly_kpi_snapshot` | `IX_acc_courier_monthly_kpi_snapshot_readiness` | NONCLUSTERED |  |  |
| `acc_courier_monthly_kpi_snapshot` | `IX_acc_courier_monthly_kpi_snapshot_updated` | NONCLUSTERED |  |  |
| `acc_courier_monthly_kpi_snapshot` | `PK_acc_courier_monthly_kpi_snapshot` | CLUSTERED | Yes | Yes |
| `acc_dhl_billing_document` | `IX_acc_dhl_billing_document_issue` | NONCLUSTERED |  |  |
| `acc_dhl_billing_document` | `PK__acc_dhl___C8FE0D8DF5E36FF8` | CLUSTERED | Yes | Yes |
| `acc_dhl_billing_line` | `IX_acc_dhl_billing_line_parcel` | NONCLUSTERED |  |  |
| `acc_dhl_billing_line` | `PK__acc_dhl___3213E83FD4AF24A9` | CLUSTERED | Yes | Yes |
| `acc_dhl_billing_line` | `UX_acc_dhl_billing_line_doc_row` | NONCLUSTERED | Yes |  |
| `acc_dhl_import_file` | `PK__acc_dhl___3213E83FEA1D89DB` | CLUSTERED | Yes | Yes |
| `acc_dhl_import_file` | `UX_acc_dhl_import_file_kind_path` | NONCLUSTERED | Yes |  |
| `acc_dhl_jjd_map` | `IX_acc_dhl_jjd_map_parcel` | NONCLUSTERED |  |  |
| `acc_dhl_jjd_map` | `PK_acc_dhl_jjd_map` | CLUSTERED | Yes | Yes |
| `acc_dhl_parcel_map` | `IX_acc_dhl_parcel_map_lookup` | NONCLUSTERED |  |  |
| `acc_dhl_parcel_map` | `PK__acc_dhl___3213E83F980A4CE6` | CLUSTERED | Yes | Yes |
| `acc_dhl_parcel_map` | `UX_acc_dhl_parcel_map_source_row` | NONCLUSTERED | Yes |  |
| `acc_event_log` | `ix_event_log_asin` | NONCLUSTERED |  |  |
| `acc_event_log` | `ix_event_log_correlation` | NONCLUSTERED |  |  |
| `acc_event_log` | `ix_event_log_domain_status` | NONCLUSTERED |  |  |
| `acc_event_log` | `ix_event_log_type_received` | NONCLUSTERED |  |  |
| `acc_event_log` | `PK__acc_even__3213E83F274D6CB3` | CLUSTERED | Yes | Yes |
| `acc_event_log` | `UQ__acc_even__2370F72608E9B924` | NONCLUSTERED | Yes |  |
| `acc_event_processing_log` | `ix_proc_log_event` | NONCLUSTERED |  |  |
| `acc_event_processing_log` | `PK__acc_even__3213E83FF53AE1A8` | CLUSTERED | Yes | Yes |
| `acc_exchange_rate` | `IX_acc_exchange_rate_currency` | NONCLUSTERED |  |  |
| `acc_exchange_rate` | `IX_acc_exchange_rate_date` | NONCLUSTERED |  |  |
| `acc_exchange_rate` | `IX_acc_exchange_rate_fx_lookup` | NONCLUSTERED |  |  |
| `acc_exchange_rate` | `IX_acc_exchange_rate_lookup` | NONCLUSTERED |  |  |
| `acc_exchange_rate` | `PK__acc_exch__3213E83F99E04564` | CLUSTERED | Yes | Yes |
| `acc_exchange_rate` | `UQ_acc_rate_date_currency` | NONCLUSTERED | Yes |  |
| `acc_fba_bundle` | `PK__acc_fba___3213E83F0F56E889` | CLUSTERED | Yes | Yes |
| `acc_fba_bundle_event` | `IX_acc_fba_bundle_event_bundle` | NONCLUSTERED |  |  |
| `acc_fba_bundle_event` | `PK__acc_fba___3213E83FCFB81AED` | CLUSTERED | Yes | Yes |
| `acc_fba_case` | `IX_acc_fba_case_status` | NONCLUSTERED |  |  |
| `acc_fba_case` | `PK__acc_fba___3213E83F68895C44` | CLUSTERED | Yes | Yes |
| `acc_fba_case_event` | `IX_acc_fba_case_event_case` | NONCLUSTERED |  |  |
| `acc_fba_case_event` | `PK__acc_fba___3213E83FD1E2BC0D` | CLUSTERED | Yes | Yes |
| `acc_fba_config` | `PK__acc_fba___DFD83CAEF4A7BF46` | CLUSTERED | Yes | Yes |
| `acc_fba_customer_return` | `IX_acc_fba_customer_return_date` | NONCLUSTERED |  |  |
| `acc_fba_customer_return` | `IX_acc_fba_customer_return_order` | NONCLUSTERED |  |  |
| `acc_fba_customer_return` | `PK__acc_fba___3213E83FCEDC940D` | CLUSTERED | Yes | Yes |
| `acc_fba_customer_return` | `UQ_acc_fba_return_row` | NONCLUSTERED | Yes |  |
| `acc_fba_fee_reference` | `IX_fba_fee_ref_sku` | NONCLUSTERED |  |  |
| `acc_fba_fee_reference` | `PK__acc_fba___3213E83F241D9695` | CLUSTERED | Yes | Yes |
| `acc_fba_inbound_shipment` | `IX_acc_fba_inbound_shipment_market_status` | NONCLUSTERED |  |  |
| `acc_fba_inbound_shipment` | `PK__acc_fba___3213E83F2DB25A39` | CLUSTERED | Yes | Yes |
| `acc_fba_inbound_shipment` | `UQ_acc_fba_inbound_shipment` | NONCLUSTERED | Yes |  |
| `acc_fba_inbound_shipment_line` | `IX_acc_fba_inbound_shipment_line_shipment` | NONCLUSTERED |  |  |
| `acc_fba_inbound_shipment_line` | `IX_acc_fba_inbound_shipment_line_sku` | NONCLUSTERED |  |  |
| `acc_fba_inbound_shipment_line` | `PK__acc_fba___3213E83F774E0A39` | CLUSTERED | Yes | Yes |
| `acc_fba_initiative` | `IX_acc_fba_initiative_quarter` | NONCLUSTERED |  |  |
| `acc_fba_initiative` | `PK__acc_fba___3213E83FD420B404` | CLUSTERED | Yes | Yes |
| `acc_fba_inventory_snapshot` | `IX_acc_fba_inv_snapshot_date` | NONCLUSTERED |  |  |
| `acc_fba_inventory_snapshot` | `IX_acc_fba_inventory_snapshot_market_date` | NONCLUSTERED |  |  |
| `acc_fba_inventory_snapshot` | `IX_acc_fba_inventory_snapshot_sku_date` | NONCLUSTERED |  |  |
| `acc_fba_inventory_snapshot` | `PK__acc_fba___3213E83FC50E6047` | CLUSTERED | Yes | Yes |
| `acc_fba_inventory_snapshot` | `UQ_acc_fba_inventory_snapshot` | NONCLUSTERED | Yes |  |
| `acc_fba_kpi_snapshot` | `IX_acc_fba_kpi_snapshot_quarter` | NONCLUSTERED |  |  |
| `acc_fba_kpi_snapshot` | `PK__acc_fba___3213E83F15DD0FF5` | CLUSTERED | Yes | Yes |
| `acc_fba_launch` | `IX_acc_fba_launch_quarter` | NONCLUSTERED |  |  |
| `acc_fba_launch` | `PK__acc_fba___3213E83F470A9E6E` | CLUSTERED | Yes | Yes |
| `acc_fba_receiving_reconciliation` | `IX_acc_fba_receiving_reconciliation_date` | NONCLUSTERED |  |  |
| `acc_fba_receiving_reconciliation` | `PK__acc_fba___3213E83FDAEDB181` | CLUSTERED | Yes | Yes |
| `acc_fba_report_diagnostic` | `IX_acc_fba_report_diagnostic_market_report` | NONCLUSTERED |  |  |
| `acc_fba_report_diagnostic` | `IX_acc_fba_report_diagnostic_scope` | NONCLUSTERED |  |  |
| `acc_fba_report_diagnostic` | `PK__acc_fba___3213E83F40953E00` | CLUSTERED | Yes | Yes |
| `acc_fba_shipment_plan` | `IX_acc_fba_shipment_plan_quarter` | NONCLUSTERED |  |  |
| `acc_fba_shipment_plan` | `PK__acc_fba___3213E83FD637BB4D` | CLUSTERED | Yes | Yes |
| `acc_fba_sku_status` | `IX_acc_fba_sku_status_sku_market` | NONCLUSTERED |  |  |
| `acc_fba_sku_status` | `PK__acc_fba___3213E83F6D0D0D37` | CLUSTERED | Yes | Yes |
| `acc_fee_gap_recheck_run` | `IX_acc_fee_gap_recheck_run_started` | NONCLUSTERED |  |  |
| `acc_fee_gap_recheck_run` | `PK__acc_fee___3213E83FA93982D9` | CLUSTERED | Yes | Yes |
| `acc_fee_gap_watch` | `IX_acc_fee_gap_watch_state` | NONCLUSTERED |  |  |
| `acc_fee_gap_watch` | `IX_acc_fee_gap_watch_unique` | NONCLUSTERED | Yes |  |
| `acc_fee_gap_watch` | `PK__acc_fee___3213E83F56B2EC2C` | CLUSTERED | Yes | Yes |
| `acc_fin_bank_line` | `PK__acc_fin___3213E83FDBE896A4` | CLUSTERED | Yes | Yes |
| `acc_fin_bank_line` | `UX_acc_fin_bank_line_hash` | NONCLUSTERED | Yes |  |
| `acc_fin_chart_of_accounts` | `PK__acc_fin___5C3BE50EACA5CA40` | CLUSTERED | Yes | Yes |
| `acc_fin_event_group_sync` | `IX_acc_fin_event_group_sync_open_refresh` | NONCLUSTERED |  |  |
| `acc_fin_event_group_sync` | `PK__acc_fin___2E40757BD9F71CBB` | CLUSTERED | Yes | Yes |
| `acc_fin_ledger_entry` | `IX_acc_fin_ledger_entry_filters` | NONCLUSTERED |  |  |
| `acc_fin_ledger_entry` | `PK__acc_fin___3213E83FD7B6851A` | CLUSTERED | Yes | Yes |
| `acc_fin_ledger_entry` | `UX_acc_fin_ledger_entry_hash` | NONCLUSTERED | Yes |  |
| `acc_fin_mapping_rule` | `PK__acc_fin___3213E83F18345E53` | CLUSTERED | Yes | Yes |
| `acc_fin_mapping_rule` | `UX_acc_fin_mapping_rule_source` | NONCLUSTERED | Yes |  |
| `acc_fin_reconciliation_payout` | `PK__acc_fin___3213E83F38C6E002` | CLUSTERED | Yes | Yes |
| `acc_fin_reconciliation_payout` | `UX_acc_fin_reconciliation_payout_settlement` | NONCLUSTERED | Yes |  |
| `acc_fin_settlement_summary` | `PK__acc_fin___3213E83F02393513` | CLUSTERED | Yes | Yes |
| `acc_fin_settlement_summary` | `UX_acc_fin_settlement_summary_settlement` | NONCLUSTERED | Yes |  |
| `acc_fin_tax_code` | `PK__acc_fin___357D4CF8ABF50A51` | CLUSTERED | Yes | Yes |
| `acc_finance_transaction` | `IX_acc_finance_transaction_group` | NONCLUSTERED |  |  |
| `acc_finance_transaction` | `IX_acc_finance_tx_order` | NONCLUSTERED |  |  |
| `acc_finance_transaction` | `IX_acc_finance_tx_posted` | NONCLUSTERED |  |  |
| `acc_finance_transaction` | `IX_acc_finance_tx_settlement` | NONCLUSTERED |  |  |
| `acc_finance_transaction` | `IX_acc_finance_tx_sku` | NONCLUSTERED |  |  |
| `acc_finance_transaction` | `IX_acc_finance_tx_type` | NONCLUSTERED |  |  |
| `acc_finance_transaction` | `IX_acc_ft_bridge_fees` | NONCLUSTERED |  |  |
| `acc_finance_transaction` | `PK__acc_fina__3213E83F8594CF45` | CLUSTERED | Yes | Yes |
| `acc_gls_billing_document` | `PK__acc_gls___C8FE0D8DF31911B8` | CLUSTERED | Yes | Yes |
| `acc_gls_billing_line` | `IX_acc_gls_billing_line_note1` | NONCLUSTERED |  |  |
| `acc_gls_billing_line` | `IX_acc_gls_billing_line_parcel_number` | NONCLUSTERED |  |  |
| `acc_gls_billing_line` | `PK__acc_gls___3213E83FF32A268A` | CLUSTERED | Yes | Yes |
| `acc_gls_billing_line` | `UX_acc_gls_billing_line_source_row` | NONCLUSTERED | Yes |  |
| `acc_gls_bl_map` | `IX_acc_gls_bl_map_bl_order_id` | NONCLUSTERED |  |  |
| `acc_gls_bl_map` | `IX_acc_gls_bl_map_tracking` | NONCLUSTERED |  |  |
| `acc_gls_bl_map` | `PK__acc_gls___3213E83FE40E3EB9` | CLUSTERED | Yes | Yes |
| `acc_gls_bl_map` | `UX_acc_gls_bl_map_source_row` | NONCLUSTERED | Yes |  |
| `acc_gls_import_file` | `IX_acc_gls_import_file_status` | NONCLUSTERED |  |  |
| `acc_gls_import_file` | `PK__acc_gls___3213E83F47B822E0` | CLUSTERED | Yes | Yes |
| `acc_gls_import_file` | `UX_acc_gls_import_file_kind_path` | NONCLUSTERED | Yes |  |
| `acc_guardrail_results` | `ix_guardrail_name_date` | NONCLUSTERED |  |  |
| `acc_guardrail_results` | `PK__acc_guar__3213E83FEDCDFE02` | CLUSTERED | Yes | Yes |
| `acc_import_products` | `IX_acc_import_products_import` | NONCLUSTERED |  |  |
| `acc_import_products` | `IX_acc_import_products_kod` | NONCLUSTERED |  |  |
| `acc_import_products` | `PK__acc_impo__3213E83F8406C770` | CLUSTERED | Yes | Yes |
| `acc_import_products` | `UX_acc_import_products_sku` | NONCLUSTERED | Yes |  |
| `acc_inv_category_cvr_baseline` | `PK_acc_inv_category_cvr_baseline` | CLUSTERED | Yes | Yes |
| `acc_inv_change_draft` | `IX_acc_inv_change_draft_status` | NONCLUSTERED |  |  |
| `acc_inv_change_draft` | `PK__acc_inv___3213E83F37D4566A` | CLUSTERED | Yes | Yes |
| `acc_inv_change_event` | `IX_acc_inv_change_event_main` | NONCLUSTERED |  |  |
| `acc_inv_change_event` | `PK__acc_inv___3213E83F9D0DD8A2` | CLUSTERED | Yes | Yes |
| `acc_inv_item_cache` | `IX_acc_inv_item_cache_filters` | NONCLUSTERED |  |  |
| `acc_inv_item_cache` | `PK_acc_inv_item_cache` | CLUSTERED | Yes | Yes |
| `acc_inv_settings` | `PK__acc_inv___DFD83CAE1D8084F2` | CLUSTERED | Yes | Yes |
| `acc_inv_traffic_asin_daily` | `IX_acc_inv_traffic_asin_daily_date` | NONCLUSTERED |  |  |
| `acc_inv_traffic_asin_daily` | `PK_acc_inv_traffic_asin_daily` | CLUSTERED | Yes | Yes |
| `acc_inv_traffic_rollup` | `UX_acc_inv_traffic_rollup` | NONCLUSTERED | Yes |  |
| `acc_inv_traffic_sku_daily` | `IX_acc_inv_traffic_sku_daily_date` | NONCLUSTERED |  |  |
| `acc_inv_traffic_sku_daily` | `PK_acc_inv_traffic_sku_daily` | CLUSTERED | Yes | Yes |
| `acc_inventory_snapshot` | `IX_acc_inventory_date` | NONCLUSTERED |  |  |
| `acc_inventory_snapshot` | `IX_acc_inventory_marketplace` | NONCLUSTERED |  |  |
| `acc_inventory_snapshot` | `IX_acc_inventory_product` | NONCLUSTERED |  |  |
| `acc_inventory_snapshot` | `IX_acc_inventory_sku` | NONCLUSTERED |  |  |
| `acc_inventory_snapshot` | `PK__acc_inve__3213E83F62EC3F39` | CLUSTERED | Yes | Yes |
| `acc_job_run` | `IX_acc_job_celery` | NONCLUSTERED | Yes |  |
| `acc_job_run` | `IX_acc_job_created` | NONCLUSTERED |  |  |
| `acc_job_run` | `IX_acc_job_status` | NONCLUSTERED |  |  |
| `acc_job_run` | `IX_acc_job_type` | NONCLUSTERED |  |  |
| `acc_job_run` | `PK__acc_job___3213E83F992D9FFD` | CLUSTERED | Yes | Yes |
| `acc_listing_state` | `ix_ls_asin` | NONCLUSTERED |  |  |
| `acc_listing_state` | `ix_ls_issues` | NONCLUSTERED |  |  |
| `acc_listing_state` | `ix_ls_mkt_status` | NONCLUSTERED |  |  |
| `acc_listing_state` | `ix_ls_product` | NONCLUSTERED |  |  |
| `acc_listing_state` | `ix_ls_suppressed` | NONCLUSTERED |  |  |
| `acc_listing_state` | `ix_ls_synced` | NONCLUSTERED |  |  |
| `acc_listing_state` | `PK__acc_list__3213E83F4C920D13` | CLUSTERED | Yes | Yes |
| `acc_listing_state` | `uq_listing_state_sku_mkt` | NONCLUSTERED | Yes |  |
| `acc_mapping_change_log` | `IX_mapping_change_date` | NONCLUSTERED |  |  |
| `acc_mapping_change_log` | `IX_mapping_change_sku` | NONCLUSTERED |  |  |
| `acc_mapping_change_log` | `PK__acc_mapp__3213E83FA3D6FC0B` | CLUSTERED | Yes | Yes |
| `acc_marketplace` | `IX_acc_marketplace_code` | NONCLUSTERED |  |  |
| `acc_marketplace` | `PK__acc_mark__3213E83F344E5B58` | CLUSTERED | Yes | Yes |
| `acc_marketplace` | `UQ_acc_marketplace_code` | NONCLUSTERED | Yes |  |
| `acc_marketplace_profitability_rollup` | `IX_mkt_rollup_date` | NONCLUSTERED |  |  |
| `acc_marketplace_profitability_rollup` | `PK__acc_mark__3213E83F7E5D67A2` | CLUSTERED | Yes | Yes |
| `acc_marketplace_profitability_rollup` | `UQ_mkt_rollup_day` | NONCLUSTERED | Yes |  |
| `acc_notification_destination` | `PK__acc_noti__3213E83FCF772A2B` | CLUSTERED | Yes | Yes |
| `acc_notification_destination` | `UQ__acc_noti__550153907B371016` | NONCLUSTERED | Yes |  |
| `acc_notification_subscription` | `PK__acc_noti__3213E83F34C921C4` | CLUSTERED | Yes | Yes |
| `acc_notification_subscription` | `UQ__acc_noti__863A7EC06B17694F` | NONCLUSTERED | Yes |  |
| `acc_notification_subscription` | `uq_sub_type` | NONCLUSTERED | Yes |  |
| `acc_offer` | `IX_acc_offer_asin` | NONCLUSTERED |  |  |
| `acc_offer` | `IX_acc_offer_marketplace` | NONCLUSTERED |  |  |
| `acc_offer` | `IX_acc_offer_product` | NONCLUSTERED |  |  |
| `acc_offer` | `IX_acc_offer_sku` | NONCLUSTERED |  |  |
| `acc_offer` | `PK__acc_offe__3213E83FD94CD642` | CLUSTERED | Yes | Yes |
| `acc_offer_fee_expected` | `IX_acc_offer_fee_expected_lookup` | NONCLUSTERED |  |  |
| `acc_offer_fee_expected` | `PK__acc_offe__3213E83F6AA76D30` | CLUSTERED | Yes | Yes |
| `acc_offer_fee_expected` | `UX_acc_offer_fee_expected_key` | NONCLUSTERED | Yes |  |
| `acc_order` | `IX_acc_order_amazon_id` | NONCLUSTERED |  |  |
| `acc_order` | `IX_acc_order_is_refund` | NONCLUSTERED |  |  |
| `acc_order` | `IX_acc_order_marketplace` | NONCLUSTERED |  |  |
| `acc_order` | `IX_acc_order_profit_query` | NONCLUSTERED |  |  |
| `acc_order` | `IX_acc_order_purchase_date` | NONCLUSTERED |  |  |
| `acc_order` | `IX_acc_order_shipped_date` | NONCLUSTERED |  |  |
| `acc_order` | `IX_acc_order_status` | NONCLUSTERED |  |  |
| `acc_order` | `PK__acc_orde__3213E83F7838E346` | CLUSTERED | Yes | Yes |
| `acc_order` | `UQ_acc_order_amazon_id` | NONCLUSTERED | Yes |  |
| `acc_order_line` | `IX_acc_order_line_asin` | NONCLUSTERED |  |  |
| `acc_order_line` | `IX_acc_order_line_order` | NONCLUSTERED |  |  |
| `acc_order_line` | `IX_acc_order_line_order_cover` | NONCLUSTERED |  |  |
| `acc_order_line` | `IX_acc_order_line_product` | NONCLUSTERED |  |  |
| `acc_order_line` | `IX_acc_order_line_profit` | NONCLUSTERED |  |  |
| `acc_order_line` | `IX_acc_order_line_sku` | NONCLUSTERED |  |  |
| `acc_order_line` | `PK__acc_orde__3213E83F6FD06FCE` | CLUSTERED | Yes | Yes |
| `acc_order_logistics_fact` | `IX_acc_order_logistics_fact_order` | NONCLUSTERED |  |  |
| `acc_order_logistics_fact` | `PK_acc_order_logistics_fact` | CLUSTERED | Yes | Yes |
| `acc_order_logistics_shadow` | `IX_acc_order_logistics_shadow_order` | NONCLUSTERED |  |  |
| `acc_order_logistics_shadow` | `IX_acc_order_logistics_shadow_status` | NONCLUSTERED |  |  |
| `acc_order_logistics_shadow` | `PK_acc_order_logistics_shadow` | CLUSTERED | Yes | Yes |
| `acc_order_sync_state` | `PK__acc_orde__8BC9CBAE13AFFBD8` | CLUSTERED | Yes | Yes |
| `acc_plan_line` | `IX_acc_plan_line_month` | NONCLUSTERED |  |  |
| `acc_plan_line` | `IX_acc_plan_line_product` | NONCLUSTERED |  |  |
| `acc_plan_line` | `IX_acc_plan_line_sku` | NONCLUSTERED |  |  |
| `acc_plan_line` | `PK__acc_plan__3213E83FA8A30EA9` | CLUSTERED | Yes | Yes |
| `acc_plan_month` | `PK__acc_plan__3213E83F2F2B906C` | CLUSTERED | Yes | Yes |
| `acc_plan_month` | `UQ_acc_plan_month_mkt` | NONCLUSTERED | Yes |  |
| `acc_price_change_log` | `IX_price_change_flagged` | NONCLUSTERED |  |  |
| `acc_price_change_log` | `IX_price_change_sku` | NONCLUSTERED |  |  |
| `acc_price_change_log` | `PK__acc_pric__3213E83F6AAA7E07` | CLUSTERED | Yes | Yes |
| `acc_pricing_recommendation` | `IX_pricing_rec_sku_mkt` | NONCLUSTERED |  |  |
| `acc_pricing_recommendation` | `IX_pricing_rec_status` | NONCLUSTERED |  |  |
| `acc_pricing_recommendation` | `PK__acc_pric__3213E83FDF264CC3` | CLUSTERED | Yes | Yes |
| `acc_pricing_rule` | `IX_pricing_rule_active` | NONCLUSTERED |  |  |
| `acc_pricing_rule` | `PK__acc_pric__3213E83FF194AEF9` | CLUSTERED | Yes | Yes |
| `acc_pricing_rule` | `uq_pricing_rule_sku_mkt_type` | NONCLUSTERED | Yes |  |
| `acc_pricing_snapshot` | `IX_pricing_snap_asin` | NONCLUSTERED |  |  |
| `acc_pricing_snapshot` | `IX_pricing_snap_buybox` | NONCLUSTERED |  |  |
| `acc_pricing_snapshot` | `IX_pricing_snap_observed` | NONCLUSTERED |  |  |
| `acc_pricing_snapshot` | `IX_pricing_snap_sku_mkt` | NONCLUSTERED |  |  |
| `acc_pricing_snapshot` | `PK__acc_pric__3213E83F70B04CC1` | CLUSTERED | Yes | Yes |
| `acc_pricing_sync_state` | `PK__acc_pric__8BC9CBAEC9009C0E` | CLUSTERED | Yes | Yes |
| `acc_product` | `IX_acc_product_asin` | NONCLUSTERED |  |  |
| `acc_product` | `IX_acc_product_ean` | NONCLUSTERED |  |  |
| `acc_product` | `IX_acc_product_internal_sku` | NONCLUSTERED |  |  |
| `acc_product` | `IX_acc_product_k_number` | NONCLUSTERED |  |  |
| `acc_product` | `IX_acc_product_parent_asin` | NONCLUSTERED |  |  |
| `acc_product` | `IX_acc_product_sku` | NONCLUSTERED |  |  |
| `acc_product` | `PK__acc_prod__3213E83F7D43DD6A` | CLUSTERED | Yes | Yes |
| `acc_product` | `UQ_acc_product_asin` | NONCLUSTERED | Yes |  |
| `acc_product_match_suggestion` | `IX_match_suggestion_sku` | NONCLUSTERED |  |  |
| `acc_product_match_suggestion` | `IX_match_suggestion_status` | NONCLUSTERED |  |  |
| `acc_product_match_suggestion` | `PK__acc_prod__3213E83F37D4EFA2` | CLUSTERED | Yes | Yes |
| `acc_profit_cost_config` | `PK__acc_prof__BDF6033CD39A2123` | CLUSTERED | Yes | Yes |
| `acc_profit_overhead_pool` | `IX_acc_profit_overhead_pool_scope` | NONCLUSTERED |  |  |
| `acc_profit_overhead_pool` | `PK__acc_prof__3213E83F1823B8DD` | CLUSTERED | Yes | Yes |
| `acc_ptd_cache` | `ix_ptd_cache_fetched` | NONCLUSTERED |  |  |
| `acc_ptd_cache` | `ix_ptd_cache_mkt` | NONCLUSTERED |  |  |
| `acc_ptd_cache` | `ix_ptd_cache_type` | NONCLUSTERED |  |  |
| `acc_ptd_cache` | `PK__acc_ptd___3213E83F28023A8B` | CLUSTERED | Yes | Yes |
| `acc_ptd_cache` | `uq_ptd_cache_type_mkt_req_locale` | NONCLUSTERED | Yes |  |
| `acc_ptd_sync_state` | `PK__acc_ptd___8BC9CBAE11C2D9EF` | CLUSTERED | Yes | Yes |
| `acc_purchase_price` | `IX_acc_purchase_price_sku` | NONCLUSTERED |  |  |
| `acc_purchase_price` | `PK__acc_purc__3213E83FCAC8E213` | CLUSTERED | Yes | Yes |
| `acc_return_daily_summary` | `IX_acc_return_daily_date` | NONCLUSTERED |  |  |
| `acc_return_daily_summary` | `PK__acc_retu__3213E83FB2DE2A2D` | CLUSTERED | Yes | Yes |
| `acc_return_daily_summary` | `UQ_acc_return_daily` | NONCLUSTERED | Yes |  |
| `acc_return_item` | `IX_acc_return_item_marketplace` | NONCLUSTERED |  |  |
| `acc_return_item` | `IX_acc_return_item_order` | NONCLUSTERED |  |  |
| `acc_return_item` | `IX_acc_return_item_sku` | NONCLUSTERED |  |  |
| `acc_return_item` | `IX_acc_return_item_status` | NONCLUSTERED |  |  |
| `acc_return_item` | `PK__acc_retu__3213E83F293EB1FD` | CLUSTERED | Yes | Yes |
| `acc_return_item` | `UQ_acc_return_item_order_line` | NONCLUSTERED | Yes |  |
| `acc_return_sync_state` | `PK__acc_retu__3213E83FB039978E` | CLUSTERED | Yes | Yes |
| `acc_return_sync_state` | `UQ__acc_retu__8BC9CBAFD1B9C3BE` | NONCLUSTERED | Yes |  |
| `acc_sb_order_line_rebuild_state` | `PK__acc_sb_o__A913765D4D4BBC8D` | CLUSTERED | Yes | Yes |
| `acc_sb_order_line_staging` | `IX_acc_sb_order_line_staging_asin` | NONCLUSTERED |  |  |
| `acc_sb_order_line_staging` | `IX_acc_sb_order_line_staging_marketplace` | NONCLUSTERED |  |  |
| `acc_sb_order_line_staging` | `IX_acc_sb_order_line_staging_order` | NONCLUSTERED |  |  |
| `acc_sb_order_line_staging` | `PK__acc_sb_o__3213E83F82B28503` | CLUSTERED | Yes | Yes |
| `acc_sb_order_line_staging` | `UX_acc_sb_order_line_staging_source_row` | NONCLUSTERED | Yes |  |
| `acc_sb_order_line_sync_state` | `PK__acc_sb_o__52379DC092E5B793` | CLUSTERED | Yes | Yes |
| `acc_shipment` | `IX_acc_shipment_delivery` | NONCLUSTERED |  |  |
| `acc_shipment` | `IX_acc_shipment_piece_id` | NONCLUSTERED |  |  |
| `acc_shipment` | `IX_acc_shipment_tracking_number` | NONCLUSTERED |  |  |
| `acc_shipment` | `PK__acc_ship__3213E83F69506A9F` | CLUSTERED | Yes | Yes |
| `acc_shipment` | `UX_acc_shipment_carrier_shipment_number` | NONCLUSTERED | Yes |  |
| `acc_shipment_cost` | `IX_acc_shipment_cost_main` | NONCLUSTERED |  |  |
| `acc_shipment_cost` | `PK__acc_ship__3213E83F29A456C1` | CLUSTERED | Yes | Yes |
| `acc_shipment_event` | `PK__acc_ship__3213E83F17D6E0AC` | CLUSTERED | Yes | Yes |
| `acc_shipment_event` | `UX_acc_shipment_event_dedup` | NONCLUSTERED | Yes |  |
| `acc_shipment_order_link` | `IX_acc_shipment_order_link_main` | NONCLUSTERED |  |  |
| `acc_shipment_order_link` | `PK__acc_ship__3213E83F43E37371` | CLUSTERED | Yes | Yes |
| `acc_shipment_order_link` | `UX_acc_shipment_order_link_primary` | NONCLUSTERED | Yes |  |
| `acc_shipment_pod` | `PK__acc_ship__3213E83FC5339A1B` | CLUSTERED | Yes | Yes |
| `acc_shipment_pod` | `UX_acc_shipment_pod_one` | NONCLUSTERED | Yes |  |
| `acc_shipping_cost` | `ix_acc_shipping_cost_courier` | NONCLUSTERED |  |  |
| `acc_shipping_cost` | `ix_acc_shipping_cost_mapped_at` | NONCLUSTERED |  |  |
| `acc_shipping_cost` | `PK__acc_ship__3213E83F0B29095B` | CLUSTERED | Yes | Yes |
| `acc_shipping_cost` | `uq_acc_shipping_cost_order` | NONCLUSTERED | Yes |  |
| `acc_sku_profitability_rollup` | `IX_sku_rollup_date` | NONCLUSTERED |  |  |
| `acc_sku_profitability_rollup` | `IX_sku_rollup_margin` | NONCLUSTERED |  |  |
| `acc_sku_profitability_rollup` | `IX_sku_rollup_mkt_sku` | NONCLUSTERED |  |  |
| `acc_sku_profitability_rollup` | `PK__acc_sku___3213E83FDA710894` | CLUSTERED | Yes | Yes |
| `acc_sku_profitability_rollup` | `UQ_sku_rollup_day` | NONCLUSTERED | Yes |  |
| `acc_sp_api_usage_daily` | `IX_acc_sp_api_usage_daily_lookup` | NONCLUSTERED |  |  |
| `acc_sp_api_usage_daily` | `PK_acc_sp_api_usage_daily` | CLUSTERED | Yes | Yes |
| `acc_spapi_usage` | `ix_spapi_usage_called_at` | NONCLUSTERED |  |  |
| `acc_spapi_usage` | `PK__acc_spap__3213E83FF609EE58` | CLUSTERED | Yes | Yes |
| `acc_taxonomy_alias` | `IX_acc_taxonomy_alias_node` | NONCLUSTERED |  |  |
| `acc_taxonomy_alias` | `PK__acc_taxo__3213E83FB1469744` | CLUSTERED | Yes | Yes |
| `acc_taxonomy_alias` | `UX_acc_taxonomy_alias_key_source` | NONCLUSTERED | Yes |  |
| `acc_taxonomy_node` | `IX_acc_taxonomy_node_parent` | NONCLUSTERED |  |  |
| `acc_taxonomy_node` | `PK__acc_taxo__3213E83F6F1BF7BD` | CLUSTERED | Yes | Yes |
| `acc_taxonomy_node` | `UQ__acc_taxo__9418F59A956D0F4F` | NONCLUSTERED | Yes |  |
| `acc_taxonomy_prediction` | `IX_acc_taxonomy_prediction_asin` | NONCLUSTERED |  |  |
| `acc_taxonomy_prediction` | `IX_acc_taxonomy_prediction_ean` | NONCLUSTERED |  |  |
| `acc_taxonomy_prediction` | `IX_acc_taxonomy_prediction_sku` | NONCLUSTERED |  |  |
| `acc_taxonomy_prediction` | `IX_acc_taxonomy_prediction_state` | NONCLUSTERED |  |  |
| `acc_taxonomy_prediction` | `PK__acc_taxo__3213E83FB2E24EBE` | CLUSTERED | Yes | Yes |
| `acc_tkl_cache_meta` | `PK__acc_tkl___8B49A43B58A77608` | CLUSTERED | Yes | Yes |
| `acc_tkl_cache_rows` | `IX_acc_tkl_cache_rows_lookup` | NONCLUSTERED |  |  |
| `acc_tkl_cache_rows` | `PK__acc_tkl___3213E83FA8D57F80` | CLUSTERED | Yes | Yes |
| `acc_user` | `IX_acc_user_email` | NONCLUSTERED |  |  |
| `acc_user` | `PK__acc_user__3213E83F02E6B825` | CLUSTERED | Yes | Yes |
| `acc_user` | `UQ_acc_user_email` | NONCLUSTERED | Yes |  |
| `amazon_clearing_reconciliation` | `PK__amazon_c__3213E83F0896F379` | CLUSTERED | Yes | Yes |
| `amazon_clearing_reconciliation` | `UX_amazon_clearing_settlement` | NONCLUSTERED | Yes |  |
| `compliance_issue` | `IX_compliance_issue_status` | NONCLUSTERED |  |  |
| `compliance_issue` | `IX_compliance_issue_type` | NONCLUSTERED |  |  |
| `compliance_issue` | `PK__complian__3213E83FEE70FE3D` | CLUSTERED | Yes | Yes |
| `decision_learning` | `IX_decision_learning_type` | NONCLUSTERED |  |  |
| `decision_learning` | `PK__decision__3213E83F6FCC18AA` | CLUSTERED | Yes | Yes |
| `ecb_exchange_rate` | `PK__ecb_exch__3213E83F78D94C4F` | CLUSTERED | Yes | Yes |
| `ecb_exchange_rate` | `UX_ecb_rate` | NONCLUSTERED | Yes |  |
| `executive_daily_metrics` | `IX_exec_daily_date` | NONCLUSTERED |  |  |
| `executive_daily_metrics` | `PK__executiv__3213E83F8C1F873A` | CLUSTERED | Yes | Yes |
| `executive_daily_metrics` | `UQ_exec_daily_mkt` | NONCLUSTERED | Yes |  |
| `executive_health_score` | `PK__executiv__3213E83FF3D980C7` | CLUSTERED | Yes | Yes |
| `executive_health_score` | `UQ__executiv__CD23BEA10F61E1E6` | NONCLUSTERED | Yes |  |
| `executive_opportunities` | `IX_exec_opp_active` | NONCLUSTERED |  |  |
| `executive_opportunities` | `PK__executiv__3213E83FDA980D43` | CLUSTERED | Yes | Yes |
| `family_coverage_cache` | `PK_family_coverage` | CLUSTERED | Yes | Yes |
| `family_fix_job` | `IX_ffj_status` | NONCLUSTERED |  |  |
| `family_fix_job` | `PK__family_f__3213E83FBC709F87` | CLUSTERED | Yes | Yes |
| `family_fix_package` | `IX_ffp_family` | NONCLUSTERED |  |  |
| `family_fix_package` | `IX_ffp_mp_status` | NONCLUSTERED |  |  |
| `family_fix_package` | `PK__family_f__3213E83FF996F486` | CLUSTERED | Yes | Yes |
| `family_issues_cache` | `IX_fic_family` | NONCLUSTERED |  |  |
| `family_issues_cache` | `IX_fic_severity` | NONCLUSTERED |  |  |
| `family_issues_cache` | `PK__family_i__3213E83FE8DB4EED` | CLUSTERED | Yes | Yes |
| `family_restructure_log` | `PK__family_r__3213E83FF71B8B27` | CLUSTERED | Yes | Yes |
| `family_restructure_run` | `IX_family_restructure_run_lookup` | NONCLUSTERED |  |  |
| `family_restructure_run` | `PK__family_r__7D3D901B4596F44E` | CLUSTERED | Yes | Yes |
| `fba_stock_movement_ledger` | `IX_fba_movement_countries` | NONCLUSTERED |  |  |
| `fba_stock_movement_ledger` | `IX_fba_movement_date` | NONCLUSTERED |  |  |
| `fba_stock_movement_ledger` | `IX_fba_movement_sku` | NONCLUSTERED |  |  |
| `fba_stock_movement_ledger` | `IX_fba_movement_treatment` | NONCLUSTERED |  |  |
| `fba_stock_movement_ledger` | `PK__fba_stoc__3213E83F384798C9` | CLUSTERED | Yes | Yes |
| `filing_readiness_snapshot` | `IX_filing_readiness_period` | NONCLUSTERED |  |  |
| `filing_readiness_snapshot` | `PK__filing_r__3213E83F023E9948` | CLUSTERED | Yes | Yes |
| `global_family` | `IX_global_family_de_parent` | NONCLUSTERED |  |  |
| `global_family` | `PK__global_f__3213E83FC4312649` | CLUSTERED | Yes | Yes |
| `global_family` | `UQ_global_family_de_parent` | NONCLUSTERED | Yes |  |
| `global_family_child` | `IX_gfc_de_child_asin` | NONCLUSTERED |  |  |
| `global_family_child` | `IX_gfc_master_key` | NONCLUSTERED |  |  |
| `global_family_child` | `PK__global_f__3213E83F1F8FD23A` | CLUSTERED | Yes | Yes |
| `global_family_child` | `UX_gfc_family_master` | NONCLUSTERED | Yes |  |
| `global_family_child_market_link` | `IX_gfcl_mp_current_parent` | NONCLUSTERED |  |  |
| `global_family_child_market_link` | `IX_gfcl_mp_target_child` | NONCLUSTERED |  |  |
| `global_family_child_market_link` | `PK_gfcml` | CLUSTERED | Yes | Yes |
| `global_family_market_link` | `PK_gfml` | CLUSTERED | Yes | Yes |
| `growth_opportunity` | `IX_growth_opp_sku` | NONCLUSTERED |  |  |
| `growth_opportunity` | `IX_growth_opp_status_priority` | NONCLUSTERED |  |  |
| `growth_opportunity` | `IX_growth_opp_type` | NONCLUSTERED |  |  |
| `growth_opportunity` | `PK__growth_o__3213E83F635D8791` | CLUSTERED | Yes | Yes |
| `growth_opportunity_log` | `IX_growth_log_opp` | NONCLUSTERED |  |  |
| `growth_opportunity_log` | `PK__growth_o__3213E83F99510B5D` | CLUSTERED | Yes | Yes |
| `local_vat_ledger` | `IX_local_vat_country` | NONCLUSTERED |  |  |
| `local_vat_ledger` | `IX_local_vat_status` | NONCLUSTERED |  |  |
| `local_vat_ledger` | `PK__local_va__3213E83FE9A40BAE` | CLUSTERED | Yes | Yes |
| `marketplace_listing_child` | `IX_mlc_mp_ean` | NONCLUSTERED |  |  |
| `marketplace_listing_child` | `IX_mlc_mp_parent` | NONCLUSTERED |  |  |
| `marketplace_listing_child` | `IX_mlc_mp_sku` | NONCLUSTERED |  |  |
| `marketplace_listing_child` | `PK_marketplace_listing_child` | CLUSTERED | Yes | Yes |
| `opportunity_execution` | `IX_opp_exec_opp_id` | NONCLUSTERED |  |  |
| `opportunity_execution` | `IX_opp_exec_status` | NONCLUSTERED |  |  |
| `opportunity_execution` | `PK__opportun__3213E83F3B4A15FB` | CLUSTERED | Yes | Yes |
| `opportunity_model_adjustments` | `IX_model_adj_type` | NONCLUSTERED |  |  |
| `opportunity_model_adjustments` | `PK__opportun__3213E83F5F738C4E` | CLUSTERED | Yes | Yes |
| `opportunity_outcome` | `IX_opp_outcome_exec` | NONCLUSTERED |  |  |
| `opportunity_outcome` | `PK__opportun__3213E83FA85374EE` | CLUSTERED | Yes | Yes |
| `oss_return_line` | `IX_oss_line_country` | NONCLUSTERED |  |  |
| `oss_return_line` | `IX_oss_line_period` | NONCLUSTERED |  |  |
| `oss_return_line` | `PK__oss_retu__3213E83FB136FD73` | CLUSTERED | Yes | Yes |
| `oss_return_period` | `PK__oss_retu__3213E83F6D613470` | CLUSTERED | Yes | Yes |
| `oss_return_period` | `UX_oss_return_period` | NONCLUSTERED | Yes |  |
| `seasonality_cluster` | `PK__seasonal__3213E83F57EDF64D` | CLUSTERED | Yes | Yes |
| `seasonality_cluster` | `UX_cluster_name` | NONCLUSTERED | Yes |  |
| `seasonality_cluster_member` | `IX_cluster_member_cid` | NONCLUSTERED |  |  |
| `seasonality_cluster_member` | `IX_cluster_member_sku` | NONCLUSTERED |  |  |
| `seasonality_cluster_member` | `PK__seasonal__3213E83F955953E7` | CLUSTERED | Yes | Yes |
| `seasonality_index_cache` | `PK_season_index_cache` | CLUSTERED | Yes | Yes |
| `seasonality_monthly_metrics` | `IX_season_monthly_entity` | NONCLUSTERED |  |  |
| `seasonality_monthly_metrics` | `PK__seasonal__3213E83FF67B7CC4` | CLUSTERED | Yes | Yes |
| `seasonality_monthly_metrics` | `UX_season_monthly` | NONCLUSTERED | Yes |  |
| `seasonality_opportunity` | `IX_season_opp_entity` | NONCLUSTERED |  |  |
| `seasonality_opportunity` | `IX_season_opp_status` | NONCLUSTERED |  |  |
| `seasonality_opportunity` | `IX_season_opp_type` | NONCLUSTERED |  |  |
| `seasonality_opportunity` | `PK__seasonal__3213E83F154D5533` | CLUSTERED | Yes | Yes |
| `seasonality_profile` | `IX_season_profile_class` | NONCLUSTERED |  |  |
| `seasonality_profile` | `PK__seasonal__3213E83F662630D6` | CLUSTERED | Yes | Yes |
| `seasonality_profile` | `UX_season_profile` | NONCLUSTERED | Yes |  |
| `seasonality_settings` | `PK__seasonal__3213E83F09756CC5` | CLUSTERED | Yes | Yes |
| `seasonality_settings` | `UX_season_setting_key` | NONCLUSTERED | Yes |  |
| `strategy_experiment` | `IX_strategy_exp_status` | NONCLUSTERED |  |  |
| `strategy_experiment` | `PK__strategy__3213E83F2B6FF4E8` | CLUSTERED | Yes | Yes |
| `transport_evidence_record` | `IX_transport_evidence_order` | NONCLUSTERED |  |  |
| `transport_evidence_record` | `IX_transport_evidence_status` | NONCLUSTERED |  |  |
| `transport_evidence_record` | `PK__transpor__3213E83F474A42F7` | CLUSTERED | Yes | Yes |
| `vat_event_ledger` | `IX_vat_event_classification` | NONCLUSTERED |  |  |
| `vat_event_ledger` | `IX_vat_event_jurisdiction` | NONCLUSTERED |  |  |
| `vat_event_ledger` | `IX_vat_event_marketplace_date` | NONCLUSTERED |  |  |
| `vat_event_ledger` | `IX_vat_event_order_id` | NONCLUSTERED |  |  |
| `vat_event_ledger` | `IX_vat_event_ship_from_to` | NONCLUSTERED |  |  |
| `vat_event_ledger` | `IX_vat_event_sku` | NONCLUSTERED |  |  |
| `vat_event_ledger` | `PK__vat_even__3213E83FBEBB414B` | CLUSTERED | Yes | Yes |
| `vat_rate_mapping` | `IX_vat_rate_country` | NONCLUSTERED |  |  |
| `vat_rate_mapping` | `PK__vat_rate__3213E83F347E5B1A` | CLUSTERED | Yes | Yes |
| `vat_transaction_classification` | `IX_vat_class_source` | NONCLUSTERED |  |  |
| `vat_transaction_classification` | `IX_vat_class_status` | NONCLUSTERED |  |  |
| `vat_transaction_classification` | `PK__vat_tran__3213E83FB01E2EE4` | CLUSTERED | Yes | Yes |

## Constraints (927)

### CHECK_CONSTRAINT (23)

| Table | Constraint |
|-------|-----------|
| `acc_al_jobs` | `CK_al_jobs_max_retries_gte0` |
| `acc_al_jobs` | `CK_al_jobs_progress_pct_range` |
| `acc_al_jobs` | `CK_al_jobs_retry_count_gte0` |
| `acc_al_jobs` | `CK_al_jobs_retry_policy_valid` |
| `acc_al_jobs` | `CK_al_jobs_status_valid` |
| `acc_al_jobs` | `CK_al_jobs_trigger_source_valid` |
| `acc_exchange_rate` | `CK_exchange_rate_rate_gt0` |
| `acc_exchange_rate` | `CK_exchange_rate_source_known` |
| `acc_inventory_snapshot` | `CK_inv_snap_qty_fulfillable_gte0` |
| `acc_inventory_snapshot` | `CK_inv_snap_qty_inbound_gte0` |
| `acc_inventory_snapshot` | `CK_inv_snap_qty_reserved_gte0` |
| `acc_inventory_snapshot` | `CK_inv_snap_qty_unfulfillable_gte0` |
| `acc_order` | `CK_order_fulfillment_channel` |
| `acc_order` | `CK_order_status_valid` |
| `acc_order` | `CK_order_total_gte0` |
| `acc_order_line` | `CK_order_line_item_price_gte0` |
| `acc_order_line` | `CK_order_line_item_tax_gte0` |
| `acc_order_line` | `CK_order_line_promotion_discount_gte0` |
| `acc_order_line` | `CK_order_line_qty_ordered_gte0` |
| `acc_order_line` | `CK_order_line_qty_shipped_gte0` |
| `acc_sku_profitability_rollup` | `CK_sku_profit_orders_count_gte0` |
| `acc_sku_profitability_rollup` | `CK_sku_profit_units_sold_gte0` |
| `ecb_exchange_rate` | `CK_ecb_rate_gt0` |

### DEFAULT_CONSTRAINT (666)

| Table | Constraint |
|-------|-----------|
| `acc_ads_campaign` | `DF__acc_ads_c__ad_ty__047AA831` |
| `acc_ads_campaign` | `DF__acc_ads_c__curre__0662F0A3` |
| `acc_ads_campaign` | `DF__acc_ads_c__state__056ECC6A` |
| `acc_ads_campaign` | `DF__acc_ads_c__synce__075714DC` |
| `acc_ads_campaign_day` | `DF__acc_ads_c__ad_ty__0A338187` |
| `acc_ads_campaign_day` | `DF__acc_ads_c__click__0C1BC9F9` |
| `acc_ads_campaign_day` | `DF__acc_ads_c__curre__10E07F16` |
| `acc_ads_campaign_day` | `DF__acc_ads_c__impre__0B27A5C0` |
| `acc_ads_campaign_day` | `DF__acc_ads_c__order__0EF836A4` |
| `acc_ads_campaign_day` | `DF__acc_ads_c__sales__0E04126B` |
| `acc_ads_campaign_day` | `DF__acc_ads_c__spend__0D0FEE32` |
| `acc_ads_campaign_day` | `DF__acc_ads_c__synce__11D4A34F` |
| `acc_ads_campaign_day` | `DF__acc_ads_c__units__0FEC5ADD` |
| `acc_ads_product_day` | `DF__acc_ads_p__ad_ty__4301EA8F` |
| `acc_ads_product_day` | `DF__acc_ads_p__campa__44EA3301` |
| `acc_ads_product_day` | `DF__acc_ads_p__click__46D27B73` |
| `acc_ads_product_day` | `DF__acc_ads_p__curre__4B973090` |
| `acc_ads_product_day` | `DF__acc_ads_p__impre__45DE573A` |
| `acc_ads_product_day` | `DF__acc_ads_p__marke__43F60EC8` |
| `acc_ads_product_day` | `DF__acc_ads_p__order__49AEE81E` |
| `acc_ads_product_day` | `DF__acc_ads_p__sales__48BAC3E5` |
| `acc_ads_product_day` | `DF__acc_ads_p__spend__47C69FAC` |
| `acc_ads_product_day` | `DF__acc_ads_p__synce__4C8B54C9` |
| `acc_ads_product_day` | `DF__acc_ads_p__units__4AA30C57` |
| `acc_ads_profile` | `DF__acc_ads_p__accou__00AA174D` |
| `acc_ads_profile` | `DF__acc_ads_p__curre__7FB5F314` |
| `acc_ads_profile` | `DF__acc_ads_p__synce__019E3B86` |
| `acc_ai_recommendation` | `DF__acc_ai_re__gener__6EC0713C` |
| `acc_ai_recommendation` | `DF__acc_ai_re__model__6CD828CA` |
| `acc_ai_recommendation` | `DF__acc_ai_re__statu__6DCC4D03` |
| `acc_ai_recommendation` | `DF__acc_ai_recom__id__6BE40491` |
| `acc_al_alert_rules` | `DF__acc_al_al__creat__76619304` |
| `acc_al_alert_rules` | `DF__acc_al_al__is_ac__756D6ECB` |
| `acc_al_alert_rules` | `DF__acc_al_al__sever__74794A92` |
| `acc_al_alerts` | `DF__acc_al_al__is_re__793DFFAF` |
| `acc_al_alerts` | `DF__acc_al_al__is_re__7A3223E8` |
| `acc_al_alerts` | `DF__acc_al_al__trigg__7B264821` |
| `acc_al_job_semaphore` | `DF__acc_al_jo__updat__2E5BD364` |
| `acc_al_jobs` | `DF__acc_al_jo__creat__00DF2177` |
| `acc_al_jobs` | `DF__acc_al_jo__max_r__1D66518C` |
| `acc_al_jobs` | `DF__acc_al_jo__progr__7FEAFD3E` |
| `acc_al_jobs` | `DF__acc_al_jo__retry__1C722D53` |
| `acc_al_jobs` | `DF__acc_al_jo__retry__1E5A75C5` |
| `acc_al_jobs` | `DF__acc_al_jo__statu__7EF6D905` |
| `acc_al_jobs` | `DF__acc_al_jo__trigg__7E02B4CC` |
| `acc_al_plan_lines` | `DF__acc_al_pl__budge__0C50D423` |
| `acc_al_plan_lines` | `DF__acc_al_pl__targe__0880433F` |
| `acc_al_plan_lines` | `DF__acc_al_pl__targe__09746778` |
| `acc_al_plan_lines` | `DF__acc_al_pl__targe__0A688BB1` |
| `acc_al_plan_lines` | `DF__acc_al_pl__targe__0B5CAFEA` |
| `acc_al_plans` | `DF__acc_al_pl__creat__05A3D694` |
| `acc_al_plans` | `DF__acc_al_pl__statu__04AFB25B` |
| `acc_al_product_task_comments` | `DF__acc_al_pr__creat__1975C517` |
| `acc_al_product_tasks` | `DF__acc_al_pr__creat__15A53433` |
| `acc_al_product_tasks` | `DF__acc_al_pr__statu__14B10FFA` |
| `acc_al_product_tasks` | `DF__acc_al_pr__updat__1699586C` |
| `acc_al_profit_snapshot` | `DF__acc_al_pr__quant__10216507` |
| `acc_al_profit_snapshot` | `DF__acc_al_pr__reven__11158940` |
| `acc_al_profit_snapshot` | `DF__acc_al_pr__reven__1209AD79` |
| `acc_al_profit_snapshot` | `DF__acc_al_pr__synce__14E61A24` |
| `acc_al_profit_snapshot` | `DF__acc_al_pr__trans__13F1F5EB` |
| `acc_al_profit_snapshot` | `DF__acc_al_pro__cogs__12FDD1B2` |
| `acc_al_task_owner_rules` | `DF__acc_al_ta__creat__1E3A7A34` |
| `acc_al_task_owner_rules` | `DF__acc_al_ta__is_ac__1C5231C2` |
| `acc_al_task_owner_rules` | `DF__acc_al_ta__prior__1D4655FB` |
| `acc_alert` | `DF__acc_alert__id__4E53A1AA` |
| `acc_alert` | `DF__acc_alert__is_re__503BEA1C` |
| `acc_alert` | `DF__acc_alert__is_re__51300E55` |
| `acc_alert` | `DF__acc_alert__sever__4F47C5E3` |
| `acc_alert` | `DF__acc_alert__trigg__5224328E` |
| `acc_alert_rule` | `DF__acc_alert__creat__489AC854` |
| `acc_alert_rule` | `DF__acc_alert__is_ac__47A6A41B` |
| `acc_alert_rule` | `DF__acc_alert__sever__46B27FE2` |
| `acc_alert_rule` | `DF__acc_alert__updat__498EEC8D` |
| `acc_alert_rule` | `DF__acc_alert_ru__id__45BE5BA9` |
| `acc_amazon_listing_registry` | `DF__acc_amazo__synce__0CA5D9DE` |
| `acc_amazon_listing_registry` | `DF__acc_amazo__updat__0D99FE17` |
| `acc_amazon_listing_registry` | `DF__acc_amazon_l__id__0BB1B5A5` |
| `acc_amazon_listing_registry_sync_state` | `DF__acc_amazo__row_c__10766AC2` |
| `acc_amazon_listing_registry_sync_state` | `DF__acc_amazo__updat__116A8EFB` |
| `acc_audit_log` | `DF__acc_audit__creat__19AACF41` |
| `acc_audit_log` | `DF__acc_audit__total__17C286CF` |
| `acc_audit_log` | `DF__acc_audit__trigg__18B6AB08` |
| `acc_backfill_progress` | `DF__acc_backf__items__5006DFF2` |
| `acc_backfill_progress` | `DF__acc_backf__order__4F12BBB9` |
| `acc_backfill_progress` | `DF__acc_backf__statu__4E1E9780` |
| `acc_backfill_report_progress` | `DF__acc_backf__order__7CD98669` |
| `acc_backfill_report_progress` | `DF__acc_backf__statu__7BE56230` |
| `acc_bl_distribution_order_cache` | `DF__acc_bl_di__last___2136E270` |
| `acc_bl_distribution_package_cache` | `DF__acc_bl_di__last___24134F1B` |
| `acc_co_ai_cache` | `DF__acc_co_ai__creat__45544755` |
| `acc_co_asset_links` | `DF__acc_co_as__creat__361203C5` |
| `acc_co_asset_links` | `DF__acc_co_as__statu__351DDF8C` |
| `acc_co_assets` | `DF__acc_co_as__statu__314D4EA8` |
| `acc_co_assets` | `DF__acc_co_as__uploa__324172E1` |
| `acc_co_attribute_map` | `DF__acc_co_at__creat__5772F790` |
| `acc_co_attribute_map` | `DF__acc_co_at__is_ac__567ED357` |
| `acc_co_attribute_map` | `DF__acc_co_at__prior__558AAF1E` |
| `acc_co_attribute_map` | `DF__acc_co_at__trans__54968AE5` |
| `acc_co_attribute_map` | `DF__acc_co_at__updat__58671BC9` |
| `acc_co_impact_snapshots` | `DF__acc_co_im__creat__51BA1E3A` |
| `acc_co_policy_checks` | `DF__acc_co_po__check__2E70E1FD` |
| `acc_co_policy_rules` | `DF__acc_co_po__creat__2B947552` |
| `acc_co_policy_rules` | `DF__acc_co_po__is_ac__2AA05119` |
| `acc_co_product_type_defs` | `DF__acc_co_pr__refre__4EDDB18F` |
| `acc_co_product_type_defs` | `DF__acc_co_pr__sourc__4DE98D56` |
| `acc_co_product_type_map` | `DF__acc_co_pr__creat__4A18FC72` |
| `acc_co_product_type_map` | `DF__acc_co_pr__is_ac__4924D839` |
| `acc_co_product_type_map` | `DF__acc_co_pr__prior__4830B400` |
| `acc_co_product_type_map` | `DF__acc_co_pr__updat__4B0D20AB` |
| `acc_co_publish_jobs` | `DF__acc_co_pu__creat__3AD6B8E2` |
| `acc_co_publish_jobs` | `DF__acc_co_pu__max_r__3CBF0154` |
| `acc_co_publish_jobs` | `DF__acc_co_pu__progr__39E294A9` |
| `acc_co_publish_jobs` | `DF__acc_co_pu__retry__3BCADD1B` |
| `acc_co_publish_jobs` | `DF__acc_co_pu__statu__38EE7070` |
| `acc_co_retry_policy` | `DF__acc_co_re__creat__4183B671` |
| `acc_co_retry_policy` | `DF__acc_co_re__is_ac__408F9238` |
| `acc_co_retry_policy` | `DF__acc_co_re__max_m__3F9B6DFF` |
| `acc_co_retry_policy` | `DF__acc_co_re__updat__4277DAAA` |
| `acc_co_tasks` | `DF__acc_co_ta__creat__22FF2F51` |
| `acc_co_tasks` | `DF__acc_co_ta__prior__2116E6DF` |
| `acc_co_tasks` | `DF__acc_co_ta__statu__220B0B18` |
| `acc_co_tasks` | `DF__acc_co_ta__updat__23F3538A` |
| `acc_co_versions` | `DF__acc_co_ve__creat__27C3E46E` |
| `acc_co_versions` | `DF__acc_co_ve__statu__26CFC035` |
| `acc_cogs_import_log` | `DF__acc_cogs___impor__4A4E069C` |
| `acc_cogs_import_log` | `DF__acc_cogs___rows___4865BE2A` |
| `acc_cogs_import_log` | `DF__acc_cogs___rows___4959E263` |
| `acc_courier_audit_log` | `DF__acc_couri__creat__37E53D9E` |
| `acc_courier_audit_log` | `DF__acc_couri__expec__3138400F` |
| `acc_courier_audit_log` | `DF__acc_couri__extra__3508D0F3` |
| `acc_courier_audit_log` | `DF__acc_couri__faile__33208881` |
| `acc_courier_audit_log` | `DF__acc_couri__impor__322C6448` |
| `acc_courier_audit_log` | `DF__acc_couri__match__35FCF52C` |
| `acc_courier_audit_log` | `DF__acc_couri__missi__3414ACBA` |
| `acc_courier_audit_log` | `DF__acc_couri__trigg__36F11965` |
| `acc_courier_audit_log` | `DF__acc_couri__updat__38D961D7` |
| `acc_courier_cost_estimate` | `DF__acc_couri__estim__29CC2871` |
| `acc_courier_cost_estimate` | `DF__acc_couri__estim__2E90DD8E` |
| `acc_courier_cost_estimate` | `DF__acc_couri__estim__2F8501C7` |
| `acc_courier_cost_estimate` | `DF__acc_couri__horiz__2BB470E3` |
| `acc_courier_cost_estimate` | `DF__acc_couri__min_s__2CA8951C` |
| `acc_courier_cost_estimate` | `DF__acc_couri__model__2AC04CAA` |
| `acc_courier_cost_estimate` | `DF__acc_couri__sampl__2D9CB955` |
| `acc_courier_cost_estimate` | `DF__acc_couri__statu__30792600` |
| `acc_courier_cost_estimate` | `DF__acc_courier___id__28D80438` |
| `acc_courier_estimation_kpi_daily` | `DF__acc_couri__calcu__3449B6E4` |
| `acc_courier_estimation_kpi_daily` | `DF__acc_couri__sampl__335592AB` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__billi__192BAC54` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__billi__1A1FD08D` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__billi__1B13F4C6` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__creat__1CFC3D38` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__is_cl__08F5448B` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__purch__09E968C4` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__purch__0ADD8CFD` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__purch__0BD1B136` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__purch__0CC5D56F` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__purch__0DB9F9A8` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__purch__0EAE1DE1` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__purch__0FA2421A` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__purch__10966653` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__purch__118A8A8C` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__purch__127EAEC5` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__purch__1372D2FE` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__readi__1C0818FF` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__shipm__1466F737` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__shipm__155B1B70` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__shipm__164F3FA9` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__shipm__174363E2` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__shipm__1837881B` |
| `acc_courier_monthly_kpi_snapshot` | `DF__acc_couri__updat__1DF06171` |
| `acc_dhl_billing_document` | `DF__acc_dhl_b__curre__7DEDA633` |
| `acc_dhl_billing_document` | `DF__acc_dhl_b__detai__7EE1CA6C` |
| `acc_dhl_billing_document` | `DF__acc_dhl_b__last___7FD5EEA5` |
| `acc_dhl_billing_document` | `DF__acc_dhl_b__updat__00CA12DE` |
| `acc_dhl_billing_line` | `DF__acc_dhl_b__impor__049AA3C2` |
| `acc_dhl_billing_line` | `DF__acc_dhl_bill__id__03A67F89` |
| `acc_dhl_import_file` | `DF__acc_dhl_i__last___7A1D154F` |
| `acc_dhl_import_file` | `DF__acc_dhl_i__rows___7928F116` |
| `acc_dhl_import_file` | `DF__acc_dhl_i__statu__7834CCDD` |
| `acc_dhl_import_file` | `DF__acc_dhl_i__updat__7B113988` |
| `acc_dhl_import_file` | `DF__acc_dhl_impo__id__7740A8A4` |
| `acc_dhl_parcel_map` | `DF__acc_dhl_p__impor__086B34A6` |
| `acc_dhl_parcel_map` | `DF__acc_dhl_parc__id__0777106D` |
| `acc_event_log` | `DF__acc_event__creat__36BC0F3B` |
| `acc_event_log` | `DF__acc_event__recei__34D3C6C9` |
| `acc_event_log` | `DF__acc_event__retry__35C7EB02` |
| `acc_event_log` | `DF__acc_event__sever__31F75A1E` |
| `acc_event_log` | `DF__acc_event__sourc__33DFA290` |
| `acc_event_log` | `DF__acc_event__statu__32EB7E57` |
| `acc_event_processing_log` | `DF__acc_event__creat__3B80C458` |
| `acc_event_processing_log` | `DF__acc_event__retry__3A8CA01F` |
| `acc_event_processing_log` | `DF__acc_event__statu__39987BE6` |
| `acc_exchange_rate` | `DF__acc_excha__creat__160F4887` |
| `acc_exchange_rate` | `DF__acc_excha__sourc__151B244E` |
| `acc_exchange_rate` | `DF__acc_exchange__id__14270015` |
| `acc_fba_bundle` | `DF__acc_fba_b__creat__7132C993` |
| `acc_fba_bundle` | `DF__acc_fba_b__statu__703EA55A` |
| `acc_fba_bundle_event` | `DF__acc_fba_bund__at__740F363E` |
| `acc_fba_case` | `DF__acc_fba_c__creat__062DE679` |
| `acc_fba_case` | `DF__acc_fba_c__statu__0539C240` |
| `acc_fba_case` | `DF__acc_fba_c__updat__07220AB2` |
| `acc_fba_case_event` | `DF__acc_fba_c__event__21D600EE` |
| `acc_fba_config` | `DF__acc_fba_c__updat__1EF99443` |
| `acc_fba_customer_return` | `DF__acc_fba_c__quant__758D6A5C` |
| `acc_fba_customer_return` | `DF__acc_fba_c__synce__76818E95` |
| `acc_fba_fee_reference` | `DF__acc_fba_f__creat__5DB5E0CB` |
| `acc_fba_fee_reference` | `DF__acc_fba_f__sourc__5CC1BC92` |
| `acc_fba_fee_reference` | `DF__acc_fba_f__updat__5EAA0504` |
| `acc_fba_inbound_shipment` | `DF__acc_fba_i__units__689D8392` |
| `acc_fba_inbound_shipment` | `DF__acc_fba_i__units__6991A7CB` |
| `acc_fba_inbound_shipment_line` | `DF__acc_fba_i__qty_p__6C6E1476` |
| `acc_fba_inbound_shipment_line` | `DF__acc_fba_i__qty_r__6D6238AF` |
| `acc_fba_initiative` | `DF__acc_fba_i__appro__1A34DF26` |
| `acc_fba_initiative` | `DF__acc_fba_i__creat__1B29035F` |
| `acc_fba_initiative` | `DF__acc_fba_i__plann__1940BAED` |
| `acc_fba_initiative` | `DF__acc_fba_i__statu__184C96B4` |
| `acc_fba_initiative` | `DF__acc_fba_i__updat__1C1D2798` |
| `acc_fba_inventory_snapshot` | `DF__acc_fba_i__aged___60083D91` |
| `acc_fba_inventory_snapshot` | `DF__acc_fba_i__aged___60FC61CA` |
| `acc_fba_inventory_snapshot` | `DF__acc_fba_i__aged___61F08603` |
| `acc_fba_inventory_snapshot` | `DF__acc_fba_i__aged___62E4AA3C` |
| `acc_fba_inventory_snapshot` | `DF__acc_fba_i__creat__64CCF2AE` |
| `acc_fba_inventory_snapshot` | `DF__acc_fba_i__exces__63D8CE75` |
| `acc_fba_inventory_snapshot` | `DF__acc_fba_i__inbou__5D2BD0E6` |
| `acc_fba_inventory_snapshot` | `DF__acc_fba_i__on_ha__5C37ACAD` |
| `acc_fba_inventory_snapshot` | `DF__acc_fba_i__reser__5E1FF51F` |
| `acc_fba_inventory_snapshot` | `DF__acc_fba_i__stran__5F141958` |
| `acc_fba_kpi_snapshot` | `DF__acc_fba_k__bundl__76EBA2E9` |
| `acc_fba_kpi_snapshot` | `DF__acc_fba_k__creat__77DFC722` |
| `acc_fba_launch` | `DF__acc_fba_l__creat__147C05D0` |
| `acc_fba_launch` | `DF__acc_fba_l__incid__119F9925` |
| `acc_fba_launch` | `DF__acc_fba_l__launc__10AB74EC` |
| `acc_fba_launch` | `DF__acc_fba_l__statu__1387E197` |
| `acc_fba_launch` | `DF__acc_fba_l__updat__15702A09` |
| `acc_fba_launch` | `DF__acc_fba_l__vine___1293BD5E` |
| `acc_fba_receiving_reconciliation` | `DF__acc_fba_r__creat__0DCF0841` |
| `acc_fba_receiving_reconciliation` | `DF__acc_fba_r__damag__0BE6BFCF` |
| `acc_fba_receiving_reconciliation` | `DF__acc_fba_r__reimb__0CDAE408` |
| `acc_fba_receiving_reconciliation` | `DF__acc_fba_r__shipp__09FE775D` |
| `acc_fba_receiving_reconciliation` | `DF__acc_fba_r__short__0AF29B96` |
| `acc_fba_report_diagnostic` | `DF__acc_fba_r__creat__24B26D99` |
| `acc_fba_shipment_plan` | `DF__acc_fba_s__plann__7F80E8EA` |
| `acc_fba_shipment_plan` | `DF__acc_fba_s__statu__0169315C` |
| `acc_fba_shipment_plan` | `DF__acc_fba_s__toler__00750D23` |
| `acc_fba_shipment_plan` | `DF__acc_fba_s__updat__025D5595` |
| `acc_fba_sku_status` | `DF__acc_fba_s__is_ex__7ABC33CD` |
| `acc_fba_sku_status` | `DF__acc_fba_s__is_re__7BB05806` |
| `acc_fba_sku_status` | `DF__acc_fba_s__updat__7CA47C3F` |
| `acc_fee_gap_recheck_run` | `DF__acc_fee_g__amazo__58F12BAE` |
| `acc_fee_gap_recheck_run` | `DF__acc_fee_g__check__5708E33C` |
| `acc_fee_gap_recheck_run` | `DF__acc_fee_g__resol__57FD0775` |
| `acc_fee_gap_recheck_run` | `DF__acc_fee_g__start__5614BF03` |
| `acc_fee_gap_recheck_run` | `DF__acc_fee_g__still__59E54FE7` |
| `acc_fee_gap_watch` | `DF__acc_fee_g__first__515009E6` |
| `acc_fee_gap_watch` | `DF__acc_fee_g__last___52442E1F` |
| `acc_fee_gap_watch` | `DF__acc_fee_g__last___53385258` |
| `acc_fee_gap_watch` | `DF__acc_fee_g__statu__505BE5AD` |
| `acc_fin_bank_line` | `DF__acc_fin_b__impor__546180BB` |
| `acc_fin_bank_line` | `DF__acc_fin_bank__id__536D5C82` |
| `acc_fin_chart_of_accounts` | `DF__acc_fin_c__creat__3AA1AEB8` |
| `acc_fin_chart_of_accounts` | `DF__acc_fin_c__is_ac__39AD8A7F` |
| `acc_fin_chart_of_accounts` | `DF__acc_fin_c__updat__3B95D2F1` |
| `acc_fin_event_group_sync` | `DF__acc_fin_e__last___72E607DB` |
| `acc_fin_event_group_sync` | `DF__acc_fin_e__last___73DA2C14` |
| `acc_fin_ledger_entry` | `DF__acc_fin_l__base___592635D8` |
| `acc_fin_ledger_entry` | `DF__acc_fin_l__creat__5A1A5A11` |
| `acc_fin_ledger_entry` | `DF__acc_fin_l__fx_ra__5832119F` |
| `acc_fin_ledger_entry` | `DF__acc_fin_ledg__id__573DED66` |
| `acc_fin_mapping_rule` | `DF__acc_fin_m__creat__47FBA9D6` |
| `acc_fin_mapping_rule` | `DF__acc_fin_m__is_ac__4707859D` |
| `acc_fin_mapping_rule` | `DF__acc_fin_m__sign___46136164` |
| `acc_fin_mapping_rule` | `DF__acc_fin_m__updat__48EFCE0F` |
| `acc_fin_mapping_rule` | `DF__acc_fin_mapp__id__451F3D2B` |
| `acc_fin_reconciliation_payout` | `DF__acc_fin_r__creat__60C757A0` |
| `acc_fin_reconciliation_payout` | `DF__acc_fin_r__diff___5FD33367` |
| `acc_fin_reconciliation_payout` | `DF__acc_fin_r__match__5EDF0F2E` |
| `acc_fin_reconciliation_payout` | `DF__acc_fin_r__statu__5DEAEAF5` |
| `acc_fin_reconciliation_payout` | `DF__acc_fin_r__updat__61BB7BD9` |
| `acc_fin_reconciliation_payout` | `DF__acc_fin_reco__id__5CF6C6BC` |
| `acc_fin_settlement_summary` | `DF__acc_fin_s__creat__4F9CCB9E` |
| `acc_fin_settlement_summary` | `DF__acc_fin_s__total__4CC05EF3` |
| `acc_fin_settlement_summary` | `DF__acc_fin_s__total__4DB4832C` |
| `acc_fin_settlement_summary` | `DF__acc_fin_s__trans__4EA8A765` |
| `acc_fin_settlement_summary` | `DF__acc_fin_s__updat__5090EFD7` |
| `acc_fin_settlement_summary` | `DF__acc_fin_sett__id__4BCC3ABA` |
| `acc_fin_tax_code` | `DF__acc_fin_t__creat__414EAC47` |
| `acc_fin_tax_code` | `DF__acc_fin_t__is_ac__405A880E` |
| `acc_fin_tax_code` | `DF__acc_fin_t__oss_f__3F6663D5` |
| `acc_fin_tax_code` | `DF__acc_fin_t__updat__4242D080` |
| `acc_fin_tax_code` | `DF__acc_fin_t__vat_r__3E723F9C` |
| `acc_finance_transaction` | `DF__acc_finan__synce__1DB06A4F` |
| `acc_finance_transaction` | `DF__acc_finance___id__1CBC4616` |
| `acc_gls_billing_document` | `DF__acc_gls_b__detai__11F49EE0` |
| `acc_gls_billing_document` | `DF__acc_gls_b__last___12E8C319` |
| `acc_gls_billing_document` | `DF__acc_gls_b__updat__13DCE752` |
| `acc_gls_billing_line` | `DF__acc_gls_b__impor__17AD7836` |
| `acc_gls_billing_line` | `DF__acc_gls_bill__id__16B953FD` |
| `acc_gls_bl_map` | `DF__acc_gls_b__impor__1B7E091A` |
| `acc_gls_bl_map` | `DF__acc_gls_bl_m__id__1A89E4E1` |
| `acc_gls_import_file` | `DF__acc_gls_i__last___0E240DFC` |
| `acc_gls_import_file` | `DF__acc_gls_i__rows___0D2FE9C3` |
| `acc_gls_import_file` | `DF__acc_gls_i__statu__0C3BC58A` |
| `acc_gls_import_file` | `DF__acc_gls_i__updat__0F183235` |
| `acc_gls_import_file` | `DF__acc_gls_impo__id__0B47A151` |
| `acc_guardrail_results` | `DF__acc_guard__check__20CCCE1C` |
| `acc_import_products` | `DF__acc_impor__is_im__42ACE4D4` |
| `acc_import_products` | `DF__acc_impor__updat__44952D46` |
| `acc_import_products` | `DF__acc_impor__uploa__43A1090D` |
| `acc_inv_category_cvr_baseline` | `DF__acc_inv_c__updat__2759D01A` |
| `acc_inv_change_draft` | `DF__acc_inv_c__apply__1DD065E0` |
| `acc_inv_change_draft` | `DF__acc_inv_c__appro__1CDC41A7` |
| `acc_inv_change_draft` | `DF__acc_inv_c__creat__1EC48A19` |
| `acc_inv_change_draft` | `DF__acc_inv_c__valid__1BE81D6E` |
| `acc_inv_change_event` | `DF__acc_inv_c__creat__21A0F6C4` |
| `acc_inv_item_cache` | `DF__acc_inv_i__traff__2A363CC5` |
| `acc_inv_item_cache` | `DF__acc_inv_i__updat__2B2A60FE` |
| `acc_inv_settings` | `DF__acc_inv_s__updat__247D636F` |
| `acc_inv_traffic_asin_daily` | `DF__acc_inv_t__updat__17236851` |
| `acc_inv_traffic_rollup` | `DF__acc_inv_t__updat__190BB0C3` |
| `acc_inv_traffic_sku_daily` | `DF__acc_inv_t__updat__1446FBA6` |
| `acc_inventory_snapshot` | `DF__acc_inven__qty_f__22751F6C` |
| `acc_inventory_snapshot` | `DF__acc_inven__qty_i__245D67DE` |
| `acc_inventory_snapshot` | `DF__acc_inven__qty_r__236943A5` |
| `acc_inventory_snapshot` | `DF__acc_inven__qty_u__25518C17` |
| `acc_inventory_snapshot` | `DF__acc_inven__synce__2645B050` |
| `acc_inventory_snapshot` | `DF__acc_inventor__id__2180FB33` |
| `acc_job_run` | `DF__acc_job_r__creat__681373AD` |
| `acc_job_run` | `DF__acc_job_r__progr__671F4F74` |
| `acc_job_run` | `DF__acc_job_r__statu__662B2B3B` |
| `acc_job_run` | `DF__acc_job_r__trigg__65370702` |
| `acc_job_run` | `DF__acc_job_run__id__6442E2C9` |
| `acc_listing_state` | `DF__acc_listi__creat__45FE52CB` |
| `acc_listing_state` | `DF__acc_listi__has_i__40457975` |
| `acc_listing_state` | `DF__acc_listi__is_su__4321E620` |
| `acc_listing_state` | `DF__acc_listi__issue__41399DAE` |
| `acc_listing_state` | `DF__acc_listi__issue__422DC1E7` |
| `acc_listing_state` | `DF__acc_listi__last___450A2E92` |
| `acc_listing_state` | `DF__acc_listi__listi__3F51553C` |
| `acc_listing_state` | `DF__acc_listi__sync___44160A59` |
| `acc_listing_state` | `DF__acc_listi__updat__46F27704` |
| `acc_mapping_change_log` | `DF__acc_mappi__creat__31A25463` |
| `acc_marketplace` | `DF__acc_marke__is_ac__797309D9` |
| `acc_marketplace_profitability_rollup` | `DF__acc_marke__ad_sp__4FF1D159` |
| `acc_marketplace_profitability_rollup` | `DF__acc_marke__amazo__4D1564AE` |
| `acc_marketplace_profitability_rollup` | `DF__acc_marke__cogs___4C214075` |
| `acc_marketplace_profitability_rollup` | `DF__acc_marke__compu__55AAAAAF` |
| `acc_marketplace_profitability_rollup` | `DF__acc_marke__fba_f__4E0988E7` |
| `acc_marketplace_profitability_rollup` | `DF__acc_marke__logis__4EFDAD20` |
| `acc_marketplace_profitability_rollup` | `DF__acc_marke__other__52CE3E04` |
| `acc_marketplace_profitability_rollup` | `DF__acc_marke__profi__53C2623D` |
| `acc_marketplace_profitability_rollup` | `DF__acc_marke__refun__50E5F592` |
| `acc_marketplace_profitability_rollup` | `DF__acc_marke__refun__54B68676` |
| `acc_marketplace_profitability_rollup` | `DF__acc_marke__reven__4B2D1C3C` |
| `acc_marketplace_profitability_rollup` | `DF__acc_marke__stora__51DA19CB` |
| `acc_marketplace_profitability_rollup` | `DF__acc_marke__total__4850AF91` |
| `acc_marketplace_profitability_rollup` | `DF__acc_marke__total__4944D3CA` |
| `acc_marketplace_profitability_rollup` | `DF__acc_marke__uniqu__4A38F803` |
| `acc_notification_destination` | `DF__acc_notif__creat__25918339` |
| `acc_notification_destination` | `DF__acc_notif__statu__249D5F00` |
| `acc_notification_destination` | `DF__acc_notif__updat__2685A772` |
| `acc_notification_subscription` | `DF__acc_notif__creat__2D32A501` |
| `acc_notification_subscription` | `DF__acc_notif__paylo__2B4A5C8F` |
| `acc_notification_subscription` | `DF__acc_notif__statu__2C3E80C8` |
| `acc_notification_subscription` | `DF__acc_notif__updat__2E26C93A` |
| `acc_offer` | `DF__acc_offer__creat__30C33EC3` |
| `acc_offer` | `DF__acc_offer__curre__2BFE89A6` |
| `acc_offer` | `DF__acc_offer__fulfi__2EDAF651` |
| `acc_offer` | `DF__acc_offer__has_b__2CF2ADDF` |
| `acc_offer` | `DF__acc_offer__id__2B0A656D` |
| `acc_offer` | `DF__acc_offer__is_fe__2DE6D218` |
| `acc_offer` | `DF__acc_offer__statu__2FCF1A8A` |
| `acc_offer` | `DF__acc_offer__updat__31B762FC` |
| `acc_offer_fee_expected` | `DF__acc_offer__sourc__68336F3E` |
| `acc_offer_fee_expected` | `DF__acc_offer__statu__69279377` |
| `acc_offer_fee_expected` | `DF__acc_offer__synce__6A1BB7B0` |
| `acc_offer_fee_expected` | `DF__acc_offer_fe__id__673F4B05` |
| `acc_order` | `DF__acc_order__curre__06CD04F7` |
| `acc_order` | `DF__acc_order__fulfi__05D8E0BE` |
| `acc_order` | `DF__acc_order__id__04E4BC85` |
| `acc_order` | `DF__acc_order__is_re__4D7F7902` |
| `acc_order` | `DF__acc_order__synce__07C12930` |
| `acc_order_line` | `DF__acc_order__curre__0E6E26BF` |
| `acc_order_line` | `DF__acc_order__quant__0C85DE4D` |
| `acc_order_line` | `DF__acc_order__quant__0D7A0286` |
| `acc_order_line` | `DF__acc_order_li__id__0B91BA14` |
| `acc_order_logistics_fact` | `DF__acc_order__calc___5D80D6A1` |
| `acc_order_logistics_fact` | `DF__acc_order__calcu__5F691F13` |
| `acc_order_logistics_fact` | `DF__acc_order__deliv__5B988E2F` |
| `acc_order_logistics_fact` | `DF__acc_order__shipm__5AA469F6` |
| `acc_order_logistics_fact` | `DF__acc_order__sourc__5E74FADA` |
| `acc_order_logistics_fact` | `DF__acc_order__total__5C8CB268` |
| `acc_order_logistics_fact` | `DF_acc_order_logistics_fact_actual` |
| `acc_order_logistics_fact` | `DF_acc_order_logistics_fact_estimated` |
| `acc_order_logistics_shadow` | `DF__acc_order__actua__670A40DB` |
| `acc_order_logistics_shadow` | `DF__acc_order__calc___69E6AD86` |
| `acc_order_logistics_shadow` | `DF__acc_order__calcu__6ADAD1BF` |
| `acc_order_logistics_shadow` | `DF__acc_order__compa__68F2894D` |
| `acc_order_logistics_shadow` | `DF__acc_order__delta__642DD430` |
| `acc_order_logistics_shadow` | `DF__acc_order__delta__6521F869` |
| `acc_order_logistics_shadow` | `DF__acc_order__estim__67FE6514` |
| `acc_order_logistics_shadow` | `DF__acc_order__legac__62458BBE` |
| `acc_order_logistics_shadow` | `DF__acc_order__shado__6339AFF7` |
| `acc_order_logistics_shadow` | `DF__acc_order__shipm__66161CA2` |
| `acc_order_sync_state` | `DF__acc_order__last___0504B816` |
| `acc_order_sync_state` | `DF__acc_order__updat__05F8DC4F` |
| `acc_plan_line` | `DF__acc_plan_lin__id__5F7E2DAC` |
| `acc_plan_month` | `DF__acc_plan___creat__59C55456` |
| `acc_plan_month` | `DF__acc_plan___statu__58D1301D` |
| `acc_plan_month` | `DF__acc_plan___updat__5AB9788F` |
| `acc_plan_month` | `DF__acc_plan_mon__id__57DD0BE4` |
| `acc_price_change_log` | `DF__acc_price__creat__3572E547` |
| `acc_price_change_log` | `DF__acc_price__flagg__347EC10E` |
| `acc_pricing_recommendation` | `DF__acc_prici__confi__74B941B4` |
| `acc_pricing_recommendation` | `DF__acc_prici__creat__76A18A26` |
| `acc_pricing_recommendation` | `DF__acc_prici__statu__75AD65ED` |
| `acc_pricing_rule` | `DF__acc_prici__creat__70E8B0D0` |
| `acc_pricing_rule` | `DF__acc_prici__is_ac__6F00685E` |
| `acc_pricing_rule` | `DF__acc_prici__prior__6FF48C97` |
| `acc_pricing_rule` | `DF__acc_prici__strat__6E0C4425` |
| `acc_pricing_rule` | `DF__acc_prici__updat__71DCD509` |
| `acc_pricing_snapshot` | `DF__acc_prici__creat__6A3BB341` |
| `acc_pricing_snapshot` | `DF__acc_prici__has_b__666B225D` |
| `acc_pricing_snapshot` | `DF__acc_prici__is_fe__675F4696` |
| `acc_pricing_snapshot` | `DF__acc_prici__obser__69478F08` |
| `acc_pricing_snapshot` | `DF__acc_prici__our_c__6576FE24` |
| `acc_pricing_snapshot` | `DF__acc_prici__sourc__68536ACF` |
| `acc_pricing_sync_state` | `DF__acc_prici__recom__7C5A637C` |
| `acc_pricing_sync_state` | `DF__acc_prici__snaps__7B663F43` |
| `acc_pricing_sync_state` | `DF__acc_prici__updat__7D4E87B5` |
| `acc_product` | `DF__acc_produ__creat__00200768` |
| `acc_product` | `DF__acc_produ__is_pa__7E37BEF6` |
| `acc_product` | `DF__acc_produ__updat__01142BA1` |
| `acc_product` | `DF__acc_produ__vat_r__7F2BE32F` |
| `acc_product` | `DF__acc_product__id__7D439ABD` |
| `acc_product_match_suggestion` | `DF__acc_produ__confi__76B698BF` |
| `acc_product_match_suggestion` | `DF__acc_produ__creat__7993056A` |
| `acc_product_match_suggestion` | `DF__acc_produ__quant__77AABCF8` |
| `acc_product_match_suggestion` | `DF__acc_produ__statu__789EE131` |
| `acc_product_match_suggestion` | `DF__acc_produ__updat__7A8729A3` |
| `acc_profit_cost_config` | `DF__acc_profi__updat__0E591826` |
| `acc_profit_overhead_pool` | `DF__acc_profi__alloc__1229A90A` |
| `acc_profit_overhead_pool` | `DF__acc_profi__confi__131DCD43` |
| `acc_profit_overhead_pool` | `DF__acc_profi__creat__150615B5` |
| `acc_profit_overhead_pool` | `DF__acc_profi__is_ac__1411F17C` |
| `acc_profit_overhead_pool` | `DF__acc_profi__updat__15FA39EE` |
| `acc_profit_overhead_pool` | `DF__acc_profit_o__id__113584D1` |
| `acc_ptd_cache` | `DF__acc_ptd_c__creat__5DD5DC5C` |
| `acc_ptd_cache` | `DF__acc_ptd_c__fetch__5CE1B823` |
| `acc_ptd_cache` | `DF__acc_ptd_c__has_v__5BED93EA` |
| `acc_ptd_cache` | `DF__acc_ptd_c__local__5728DECD` |
| `acc_ptd_cache` | `DF__acc_ptd_c__prope__5911273F` |
| `acc_ptd_cache` | `DF__acc_ptd_c__requi__5634BA94` |
| `acc_ptd_cache` | `DF__acc_ptd_c__requi__5A054B78` |
| `acc_ptd_cache` | `DF__acc_ptd_c__schem__581D0306` |
| `acc_ptd_cache` | `DF__acc_ptd_c__total__5AF96FB1` |
| `acc_ptd_cache` | `DF__acc_ptd_c__updat__5ECA0095` |
| `acc_ptd_sync_state` | `DF__acc_ptd_s__produ__61A66D40` |
| `acc_ptd_sync_state` | `DF__acc_ptd_s__updat__629A9179` |
| `acc_purchase_price` | `DF__acc_purch__creat__18EBB532` |
| `acc_purchase_price` | `DF__acc_purch__updat__19DFD96B` |
| `acc_return_daily_summary` | `DF__acc_retur__cogs___07AC1A97` |
| `acc_return_daily_summary` | `DF__acc_retur__cogs___08A03ED0` |
| `acc_return_daily_summary` | `DF__acc_retur__cogs___09946309` |
| `acc_return_daily_summary` | `DF__acc_retur__cogs___0A888742` |
| `acc_return_daily_summary` | `DF__acc_retur__damag__03DB89B3` |
| `acc_return_daily_summary` | `DF__acc_retur__pendi__04CFADEC` |
| `acc_return_daily_summary` | `DF__acc_retur__refun__000AF8CF` |
| `acc_return_daily_summary` | `DF__acc_retur__refun__00FF1D08` |
| `acc_return_daily_summary` | `DF__acc_retur__refun__06B7F65E` |
| `acc_return_daily_summary` | `DF__acc_retur__reimb__05C3D225` |
| `acc_return_daily_summary` | `DF__acc_retur__retur__01F34141` |
| `acc_return_daily_summary` | `DF__acc_retur__sella__02E7657A` |
| `acc_return_daily_summary` | `DF__acc_retur__updat__0B7CAB7B` |
| `acc_return_item` | `DF__acc_retur__creat__6FD49106` |
| `acc_return_item` | `DF__acc_retur__finan__6EE06CCD` |
| `acc_return_item` | `DF__acc_retur__quant__6DEC4894` |
| `acc_return_item` | `DF__acc_retur__sourc__71BCD978` |
| `acc_return_item` | `DF__acc_retur__updat__70C8B53F` |
| `acc_return_sync_state` | `DF__acc_retur__last___7A521F79` |
| `acc_return_sync_state` | `DF__acc_retur__rows___7B4643B2` |
| `acc_return_sync_state` | `DF__acc_retur__statu__7C3A67EB` |
| `acc_sb_order_line_rebuild_state` | `DF__acc_sb_or__candi__3C54ED00` |
| `acc_sb_order_line_rebuild_state` | `DF__acc_sb_or__candi__3D491139` |
| `acc_sb_order_line_rebuild_state` | `DF__acc_sb_or__candi__3E3D3572` |
| `acc_sb_order_line_rebuild_state` | `DF__acc_sb_or__inser__3F3159AB` |
| `acc_sb_order_line_rebuild_state` | `DF__acc_sb_or__statu__3A6CA48E` |
| `acc_sb_order_line_rebuild_state` | `DF__acc_sb_or__targe__3B60C8C7` |
| `acc_sb_order_line_rebuild_state` | `DF__acc_sb_or__updat__40257DE4` |
| `acc_sb_order_line_staging` | `DF__acc_sb_or__synce__2EFAF1E2` |
| `acc_sb_order_line_staging` | `DF__acc_sb_or__updat__2FEF161B` |
| `acc_sb_order_line_staging` | `DF__acc_sb_order__id__2E06CDA9` |
| `acc_sb_order_line_sync_state` | `DF__acc_sb_or__row_c__32CB82C6` |
| `acc_sb_order_line_sync_state` | `DF__acc_sb_or__row_c__33BFA6FF` |
| `acc_sb_order_line_sync_state` | `DF__acc_sb_or__statu__34B3CB38` |
| `acc_sb_order_line_sync_state` | `DF__acc_sb_or__updat__35A7EF71` |
| `acc_shipment` | `DF__acc_shipm__carri__3C1FE2D6` |
| `acc_shipment` | `DF__acc_shipm__first__3EFC4F81` |
| `acc_shipment` | `DF__acc_shipm__is_de__3D14070F` |
| `acc_shipment` | `DF__acc_shipm__last___3FF073BA` |
| `acc_shipment` | `DF__acc_shipm__last___40E497F3` |
| `acc_shipment` | `DF__acc_shipm__sourc__3E082B48` |
| `acc_shipment` | `DF__acc_shipment__id__3B2BBE9D` |
| `acc_shipment_cost` | `DF__acc_shipm__creat__56D3D912` |
| `acc_shipment_cost` | `DF__acc_shipm__curre__54EB90A0` |
| `acc_shipment_cost` | `DF__acc_shipm__is_es__55DFB4D9` |
| `acc_shipment_cost` | `DF__acc_shipm__updat__57C7FD4B` |
| `acc_shipment_cost` | `DF__acc_shipment__id__53F76C67` |
| `acc_shipment_event` | `DF__acc_shipm__creat__4B622666` |
| `acc_shipment_order_link` | `DF__acc_shipm__creat__47919582` |
| `acc_shipment_order_link` | `DF__acc_shipm__is_pr__469D7149` |
| `acc_shipment_order_link` | `DF__acc_shipm__link___44B528D7` |
| `acc_shipment_order_link` | `DF__acc_shipm__link___45A94D10` |
| `acc_shipment_order_link` | `DF__acc_shipm__updat__4885B9BB` |
| `acc_shipment_order_link` | `DF__acc_shipment__id__43C1049E` |
| `acc_shipment_pod` | `DF__acc_shipm__avail__5026DB83` |
| `acc_shipment_pod` | `DF__acc_shipm__last___511AFFBC` |
| `acc_shipment_pod` | `DF__acc_shipm__pod_t__4F32B74A` |
| `acc_shipment_pod` | `DF__acc_shipment__id__4E3E9311` |
| `acc_shipping_cost` | `DF__acc_shipp__mappe__53D770D6` |
| `acc_sku_profitability_rollup` | `DF__acc_sku_p__ad_sp__3EC74557` |
| `acc_sku_profitability_rollup` | `DF__acc_sku_p__amazo__3BEAD8AC` |
| `acc_sku_profitability_rollup` | `DF__acc_sku_p__cogs___3AF6B473` |
| `acc_sku_profitability_rollup` | `DF__acc_sku_p__compu__44801EAD` |
| `acc_sku_profitability_rollup` | `DF__acc_sku_p__fba_f__3CDEFCE5` |
| `acc_sku_profitability_rollup` | `DF__acc_sku_p__logis__3DD3211E` |
| `acc_sku_profitability_rollup` | `DF__acc_sku_p__order__390E6C01` |
| `acc_sku_profitability_rollup` | `DF__acc_sku_p__other__41A3B202` |
| `acc_sku_profitability_rollup` | `DF__acc_sku_p__profi__4297D63B` |
| `acc_sku_profitability_rollup` | `DF__acc_sku_p__refun__3FBB6990` |
| `acc_sku_profitability_rollup` | `DF__acc_sku_p__refun__438BFA74` |
| `acc_sku_profitability_rollup` | `DF__acc_sku_p__reven__3A02903A` |
| `acc_sku_profitability_rollup` | `DF__acc_sku_p__stora__40AF8DC9` |
| `acc_sku_profitability_rollup` | `DF__acc_sku_p__units__381A47C8` |
| `acc_sp_api_usage_daily` | `DF__acc_sp_ap__calls__1ABEEF0B` |
| `acc_sp_api_usage_daily` | `DF__acc_sp_ap__error__1CA7377D` |
| `acc_sp_api_usage_daily` | `DF__acc_sp_ap__marke__18D6A699` |
| `acc_sp_api_usage_daily` | `DF__acc_sp_ap__rows___1F83A428` |
| `acc_sp_api_usage_daily` | `DF__acc_sp_ap__succe__1BB31344` |
| `acc_sp_api_usage_daily` | `DF__acc_sp_ap__sync___19CACAD2` |
| `acc_sp_api_usage_daily` | `DF__acc_sp_ap__throt__1D9B5BB6` |
| `acc_sp_api_usage_daily` | `DF__acc_sp_ap__total__1E8F7FEF` |
| `acc_sp_api_usage_daily` | `DF__acc_sp_ap__updat__2077C861` |
| `acc_spapi_usage` | `DF__acc_spapi__calle__49CEE3AF` |
| `acc_spapi_usage` | `DF__acc_spapi__metho__4AC307E8` |
| `acc_taxonomy_alias` | `DF__acc_taxon__creat__290D0E62` |
| `acc_taxonomy_alias` | `DF__acc_taxon__updat__2A01329B` |
| `acc_taxonomy_node` | `DF__acc_taxon__creat__253C7D7E` |
| `acc_taxonomy_node` | `DF__acc_taxon__is_ac__24485945` |
| `acc_taxonomy_node` | `DF__acc_taxon__updat__2630A1B7` |
| `acc_taxonomy_prediction` | `DF__acc_taxon__creat__2DD1C37F` |
| `acc_taxonomy_prediction` | `DF__acc_taxon__statu__2CDD9F46` |
| `acc_taxonomy_prediction` | `DF__acc_taxon__updat__2EC5E7B8` |
| `acc_tkl_cache_meta` | `DF__acc_tkl_c__loade__618671AF` |
| `acc_tkl_cache_rows` | `DF__acc_tkl_c__creat__6462DE5A` |
| `acc_user` | `DF__acc_user__create__74AE54BC` |
| `acc_user` | `DF__acc_user__id__70DDC3D8` |
| `acc_user` | `DF__acc_user__is_act__72C60C4A` |
| `acc_user` | `DF__acc_user__is_sup__73BA3083` |
| `acc_user` | `DF__acc_user__role__71D1E811` |
| `acc_user` | `DF__acc_user__update__75A278F5` |
| `amazon_clearing_reconciliation` | `DF__amazon_cl__statu__5C229E14` |
| `compliance_issue` | `DF__complianc__creat__66A02C87` |
| `compliance_issue` | `DF__complianc__statu__65AC084E` |
| `decision_learning` | `DF__decision___last___0B129727` |
| `decision_learning` | `DF__decision___sampl__0A1E72EE` |
| `ecb_exchange_rate` | `DF__ecb_excha__creat__6A70BD6B` |
| `ecb_exchange_rate` | `DF__ecb_excha__targe__697C9932` |
| `executive_daily_metrics` | `DF__executive__ad_sp__5E3FF0B0` |
| `executive_daily_metrics` | `DF__executive__cogs___60283922` |
| `executive_daily_metrics` | `DF__executive__compu__6304A5CD` |
| `executive_daily_metrics` | `DF__executive__margi__5B638405` |
| `executive_daily_metrics` | `DF__executive__order__5D4BCC77` |
| `executive_daily_metrics` | `DF__executive__profi__5A6F5FCC` |
| `executive_daily_metrics` | `DF__executive__refun__5F3414E9` |
| `executive_daily_metrics` | `DF__executive__reven__597B3B93` |
| `executive_daily_metrics` | `DF__executive__stock__611C5D5B` |
| `executive_daily_metrics` | `DF__executive__suppr__62108194` |
| `executive_daily_metrics` | `DF__executive__units__5C57A83E` |
| `executive_health_score` | `DF__executive__compu__6C8E1007` |
| `executive_health_score` | `DF__executive__deman__68BD7F23` |
| `executive_health_score` | `DF__executive__inven__69B1A35C` |
| `executive_health_score` | `DF__executive__opera__6AA5C795` |
| `executive_health_score` | `DF__executive__overa__6B99EBCE` |
| `executive_health_score` | `DF__executive__profi__67C95AEA` |
| `executive_health_score` | `DF__executive__reven__66D536B1` |
| `executive_opportunities` | `DF__executive__creat__7152C524` |
| `executive_opportunities` | `DF__executive__is_ac__705EA0EB` |
| `executive_opportunities` | `DF__executive__prior__6F6A7CB2` |
| `family_coverage_cache` | `DF__family_co__confi__336AA144` |
| `family_coverage_cache` | `DF__family_co__theme__32767D0B` |
| `family_coverage_cache` | `DF__family_co__updat__345EC57D` |
| `family_fix_job` | `DF__family_fi__job_t__3DE82FB7` |
| `family_fix_job` | `DF__family_fi__progr__3FD07829` |
| `family_fix_job` | `DF__family_fi__statu__3EDC53F0` |
| `family_fix_package` | `DF__family_fi__gener__3B0BC30C` |
| `family_fix_package` | `DF__family_fi__statu__3A179ED3` |
| `family_issues_cache` | `DF__family_is__creat__373B3228` |
| `family_restructure_log` | `DF__family_re__execu__384F51F2` |
| `family_restructure_run` | `DF__family_re__child__7187CF4E` |
| `family_restructure_run` | `DF__family_re__child__727BF387` |
| `family_restructure_run` | `DF__family_re__creat__737017C0` |
| `family_restructure_run` | `DF__family_re__dry_r__6F9F86DC` |
| `family_restructure_run` | `DF__family_re__progr__7093AB15` |
| `family_restructure_run` | `DF__family_re__updat__74643BF9` |
| `fba_stock_movement_ledger` | `DF__fba_stock__creat__4CE05A84` |
| `fba_stock_movement_ledger` | `DF__fba_stock__match__4AF81212` |
| `fba_stock_movement_ledger` | `DF__fba_stock__trans__4BEC364B` |
| `fba_stock_movement_ledger` | `DF__fba_stock__vat_t__4A03EDD9` |
| `filing_readiness_snapshot` | `DF__filing_re__creat__62CF9BA3` |
| `filing_readiness_snapshot` | `DF__filing_re__criti__61DB776A` |
| `global_family` | `DF__global_fa__creat__1D7B6025` |
| `global_family_child` | `DF__global_fa__creat__214BF109` |
| `global_family_child_market_link` | `DF__global_fa__confi__27F8EE98` |
| `global_family_child_market_link` | `DF__global_fa__statu__28ED12D1` |
| `global_family_child_market_link` | `DF__global_fa__updat__29E1370A` |
| `global_family_market_link` | `DF__global_fa__confi__2DB1C7EE` |
| `global_family_market_link` | `DF__global_fa__statu__2CBDA3B5` |
| `global_family_market_link` | `DF__global_fa__updat__2EA5EC27` |
| `growth_opportunity` | `DF__growth_op__confi__75235608` |
| `growth_opportunity` | `DF__growth_op__creat__770B9E7A` |
| `growth_opportunity` | `DF__growth_op__prior__742F31CF` |
| `growth_opportunity` | `DF__growth_op__statu__76177A41` |
| `growth_opportunity` | `DF__growth_op__updat__77FFC2B3` |
| `growth_opportunity_log` | `DF__growth_op__creat__7FA0E47B` |
| `local_vat_ledger` | `DF__local_vat__statu__59463169` |
| `marketplace_listing_child` | `DF__marketpla__updat__251C81ED` |
| `opportunity_execution` | `DF__opportuni__execu__027D5126` |
| `opportunity_execution` | `DF__opportuni__monit__0371755F` |
| `opportunity_execution` | `DF__opportuni__statu__04659998` |
| `opportunity_model_adjustments` | `DF__opportuni__confi__0EE3280B` |
| `opportunity_model_adjustments` | `DF__opportuni__impac__0DEF03D2` |
| `opportunity_model_adjustments` | `DF__opportuni__prior__0FD74C44` |
| `opportunity_model_adjustments` | `DF__opportuni__updat__10CB707D` |
| `opportunity_outcome` | `DF__opportuni__evalu__07420643` |
| `oss_return_line` | `DF__oss_retur__corre__54817C4C` |
| `oss_return_line` | `DF__oss_retur__sourc__5575A085` |
| `oss_return_period` | `DF__oss_retur__corre__50B0EB68` |
| `oss_return_period` | `DF__oss_retur__creat__51A50FA1` |
| `oss_return_period` | `DF__oss_retur__statu__4FBCC72F` |
| `seasonality_cluster` | `DF__seasonali__creat__25C68D63` |
| `seasonality_index_cache` | `DF__seasonali__updat__1F198FD4` |
| `seasonality_monthly_metrics` | `DF__seasonali__creat__13A7DD28` |
| `seasonality_opportunity` | `DF__seasonali__creat__22EA20B8` |
| `seasonality_opportunity` | `DF__seasonali__statu__21F5FC7F` |
| `seasonality_profile` | `DF__seasonali__deman__168449D3` |
| `seasonality_profile` | `DF__seasonali__everg__1960B67E` |
| `seasonality_profile` | `DF__seasonali__profi__186C9245` |
| `seasonality_profile` | `DF__seasonali__sales__17786E0C` |
| `seasonality_profile` | `DF__seasonali__seaso__1B48FEF0` |
| `seasonality_profile` | `DF__seasonali__updat__1C3D2329` |
| `seasonality_profile` | `DF__seasonali__volat__1A54DAB7` |
| `seasonality_settings` | `DF__seasonali__updat__2B7F66B9` |
| `strategy_experiment` | `DF__strategy___creat__7BD05397` |
| `strategy_experiment` | `DF__strategy___statu__7ADC2F5E` |
| `strategy_experiment` | `DF__strategy___updat__7CC477D0` |
| `transport_evidence_record` | `DF__transport__creat__4727812E` |
| `transport_evidence_record` | `DF__transport__evide__46335CF5` |
| `transport_evidence_record` | `DF__transport__proof__4262CC11` |
| `transport_evidence_record` | `DF__transport__proof__4356F04A` |
| `transport_evidence_record` | `DF__transport__proof__444B1483` |
| `transport_evidence_record` | `DF__transport__proof__453F38BC` |
| `vat_event_ledger` | `DF__vat_event__creat__3BB5CE82` |
| `vat_rate_mapping` | `DF__vat_rate___is_de__5EFF0ABF` |
| `vat_transaction_classification` | `DF__vat_trans__creat__3F865F66` |
| `vat_transaction_classification` | `DF__vat_trans__statu__3E923B2D` |

### FOREIGN_KEY_CONSTRAINT (26)

| Table | Constraint |
|-------|-----------|
| `acc_ai_recommendation` | `FK_acc_ai_marketplace` |
| `acc_ai_recommendation` | `FK_acc_ai_product` |
| `acc_ai_recommendation` | `FK_acc_ai_user` |
| `acc_al_plan_lines` | `FK_acc_al_plan_lines_plan` |
| `acc_alert` | `FK_acc_alert_resolved_by` |
| `acc_alert` | `FK_acc_alert_rule` |
| `acc_alert_rule` | `FK_acc_alert_rule_marketplace` |
| `acc_alert_rule` | `FK_acc_alert_rule_user` |
| `acc_inventory_snapshot` | `FK_acc_inventory_marketplace` |
| `acc_inventory_snapshot` | `FK_acc_inventory_product` |
| `acc_job_run` | `FK_acc_job_run_user` |
| `acc_offer` | `FK_acc_offer_marketplace` |
| `acc_offer` | `FK_acc_offer_product` |
| `acc_order` | `FK_acc_order_marketplace` |
| `acc_order_line` | `FK_acc_order_line_order` |
| `acc_order_line` | `FK_acc_order_line_product` |
| `acc_plan_line` | `FK_acc_plan_line_month` |
| `acc_plan_line` | `FK_acc_plan_line_product` |
| `acc_plan_month` | `FK_acc_plan_month_marketplace` |
| `acc_plan_month` | `FK_acc_plan_month_user` |
| `acc_pricing_recommendation` | `FK_rec_rule` |
| `acc_pricing_recommendation` | `FK_rec_snapshot` |
| `global_family_child` | `FK_gfc_family` |
| `global_family_market_link` | `FK_gfml_family` |
| `oss_return_line` | `FK_oss_line_period` |
| `seasonality_cluster_member` | `FK_cluster_member_cluster` |

### PRIMARY_KEY_CONSTRAINT (181)

| Table | Constraint |
|-------|-----------|
| `acc_ads_campaign` | `PK_acc_ads_campaign` |
| `acc_ads_campaign_day` | `PK_acc_ads_campaign_day` |
| `acc_ads_product_day` | `PK_acc_ads_product_day` |
| `acc_ads_profile` | `PK__acc_ads___AEBB701F4F6CBC46` |
| `acc_ai_recommendation` | `PK__acc_ai_r__3213E83F7960A050` |
| `acc_al_alert_rules` | `PK__acc_al_a__3213E83F3F249EF8` |
| `acc_al_alerts` | `PK__acc_al_a__3213E83F11453F1F` |
| `acc_al_job_semaphore` | `PK_acc_al_job_semaphore` |
| `acc_al_jobs` | `PK__acc_al_j__3213E83F68E97B9C` |
| `acc_al_plan_lines` | `PK__acc_al_p__3213E83FF157107B` |
| `acc_al_plans` | `PK__acc_al_p__3213E83F9A62C926` |
| `acc_al_product_task_comments` | `PK__acc_al_p__3213E83FFE9159FE` |
| `acc_al_product_tasks` | `PK__acc_al_p__3213E83F7DDDD521` |
| `acc_al_profit_snapshot` | `PK__acc_al_p__3213E83F6D9C29CD` |
| `acc_al_task_owner_rules` | `PK__acc_al_t__3213E83FA32B0121` |
| `acc_alert` | `PK__acc_aler__3213E83F7CCDD3E4` |
| `acc_alert_rule` | `PK__acc_aler__3213E83FE3966B95` |
| `acc_amazon_listing_registry` | `PK__acc_amaz__3213E83F1C78D679` |
| `acc_amazon_listing_registry_sync_state` | `PK__acc_amaz__8656F73C8925F6D5` |
| `acc_audit_log` | `PK__acc_audi__3213E83FE4AE5660` |
| `acc_backfill_progress` | `PK__acc_back__3213E83F8BDA4070` |
| `acc_backfill_report_progress` | `PK__acc_back__3213E83F90E58B26` |
| `acc_bl_distribution_order_cache` | `PK__acc_bl_d__465962297D1F412F` |
| `acc_bl_distribution_package_cache` | `PK__acc_bl_d__63846AE8AFE77F07` |
| `acc_co_ai_cache` | `PK__acc_co_a__3213E83FF7385D3F` |
| `acc_co_asset_links` | `PK__acc_co_a__3213E83FA3B88AE9` |
| `acc_co_assets` | `PK__acc_co_a__3213E83FC0F45A02` |
| `acc_co_attribute_map` | `PK__acc_co_a__3213E83F8384F0E8` |
| `acc_co_impact_snapshots` | `PK__acc_co_i__3213E83F8D46D456` |
| `acc_co_policy_checks` | `PK__acc_co_p__3213E83FC009B195` |
| `acc_co_policy_rules` | `PK__acc_co_p__3213E83F65C8A9F9` |
| `acc_co_product_type_defs` | `PK__acc_co_p__3213E83F796C5D39` |
| `acc_co_product_type_map` | `PK__acc_co_p__3213E83F5AD2BECC` |
| `acc_co_publish_jobs` | `PK__acc_co_p__3213E83F2A93A899` |
| `acc_co_retry_policy` | `PK__acc_co_r__3213E83F2EB7372A` |
| `acc_co_tasks` | `PK__acc_co_t__3213E83FDA299179` |
| `acc_co_versions` | `PK__acc_co_v__3213E83FF8649E11` |
| `acc_cogs_import_log` | `PK__acc_cogs__3213E83F46D23FEE` |
| `acc_courier_audit_log` | `PK__acc_cour__3213E83FD6EDEFF2` |
| `acc_courier_cost_estimate` | `PK__acc_cour__3213E83F7C6A58C0` |
| `acc_courier_estimation_kpi_daily` | `PK_acc_courier_estimation_kpi_daily` |
| `acc_courier_monthly_kpi_snapshot` | `PK_acc_courier_monthly_kpi_snapshot` |
| `acc_dhl_billing_document` | `PK__acc_dhl___C8FE0D8DF5E36FF8` |
| `acc_dhl_billing_line` | `PK__acc_dhl___3213E83FD4AF24A9` |
| `acc_dhl_import_file` | `PK__acc_dhl___3213E83FEA1D89DB` |
| `acc_dhl_jjd_map` | `PK_acc_dhl_jjd_map` |
| `acc_dhl_parcel_map` | `PK__acc_dhl___3213E83F980A4CE6` |
| `acc_event_log` | `PK__acc_even__3213E83F274D6CB3` |
| `acc_event_processing_log` | `PK__acc_even__3213E83FF53AE1A8` |
| `acc_exchange_rate` | `PK__acc_exch__3213E83F99E04564` |
| `acc_fba_bundle` | `PK__acc_fba___3213E83F0F56E889` |
| `acc_fba_bundle_event` | `PK__acc_fba___3213E83FCFB81AED` |
| `acc_fba_case` | `PK__acc_fba___3213E83F68895C44` |
| `acc_fba_case_event` | `PK__acc_fba___3213E83FD1E2BC0D` |
| `acc_fba_config` | `PK__acc_fba___DFD83CAEF4A7BF46` |
| `acc_fba_customer_return` | `PK__acc_fba___3213E83FCEDC940D` |
| `acc_fba_fee_reference` | `PK__acc_fba___3213E83F241D9695` |
| `acc_fba_inbound_shipment` | `PK__acc_fba___3213E83F2DB25A39` |
| `acc_fba_inbound_shipment_line` | `PK__acc_fba___3213E83F774E0A39` |
| `acc_fba_initiative` | `PK__acc_fba___3213E83FD420B404` |
| `acc_fba_inventory_snapshot` | `PK__acc_fba___3213E83FC50E6047` |
| `acc_fba_kpi_snapshot` | `PK__acc_fba___3213E83F15DD0FF5` |
| `acc_fba_launch` | `PK__acc_fba___3213E83F470A9E6E` |
| `acc_fba_receiving_reconciliation` | `PK__acc_fba___3213E83FDAEDB181` |
| `acc_fba_report_diagnostic` | `PK__acc_fba___3213E83F40953E00` |
| `acc_fba_shipment_plan` | `PK__acc_fba___3213E83FD637BB4D` |
| `acc_fba_sku_status` | `PK__acc_fba___3213E83F6D0D0D37` |
| `acc_fee_gap_recheck_run` | `PK__acc_fee___3213E83FA93982D9` |
| `acc_fee_gap_watch` | `PK__acc_fee___3213E83F56B2EC2C` |
| `acc_fin_bank_line` | `PK__acc_fin___3213E83FDBE896A4` |
| `acc_fin_chart_of_accounts` | `PK__acc_fin___5C3BE50EACA5CA40` |
| `acc_fin_event_group_sync` | `PK__acc_fin___2E40757BD9F71CBB` |
| `acc_fin_ledger_entry` | `PK__acc_fin___3213E83FD7B6851A` |
| `acc_fin_mapping_rule` | `PK__acc_fin___3213E83F18345E53` |
| `acc_fin_reconciliation_payout` | `PK__acc_fin___3213E83F38C6E002` |
| `acc_fin_settlement_summary` | `PK__acc_fin___3213E83F02393513` |
| `acc_fin_tax_code` | `PK__acc_fin___357D4CF8ABF50A51` |
| `acc_finance_transaction` | `PK__acc_fina__3213E83F8594CF45` |
| `acc_gls_billing_document` | `PK__acc_gls___C8FE0D8DF31911B8` |
| `acc_gls_billing_line` | `PK__acc_gls___3213E83FF32A268A` |
| `acc_gls_bl_map` | `PK__acc_gls___3213E83FE40E3EB9` |
| `acc_gls_import_file` | `PK__acc_gls___3213E83F47B822E0` |
| `acc_guardrail_results` | `PK__acc_guar__3213E83FEDCDFE02` |
| `acc_import_products` | `PK__acc_impo__3213E83F8406C770` |
| `acc_inv_category_cvr_baseline` | `PK_acc_inv_category_cvr_baseline` |
| `acc_inv_change_draft` | `PK__acc_inv___3213E83F37D4566A` |
| `acc_inv_change_event` | `PK__acc_inv___3213E83F9D0DD8A2` |
| `acc_inv_item_cache` | `PK_acc_inv_item_cache` |
| `acc_inv_settings` | `PK__acc_inv___DFD83CAE1D8084F2` |
| `acc_inv_traffic_asin_daily` | `PK_acc_inv_traffic_asin_daily` |
| `acc_inv_traffic_sku_daily` | `PK_acc_inv_traffic_sku_daily` |
| `acc_inventory_snapshot` | `PK__acc_inve__3213E83F62EC3F39` |
| `acc_job_run` | `PK__acc_job___3213E83F992D9FFD` |
| `acc_listing_state` | `PK__acc_list__3213E83F4C920D13` |
| `acc_mapping_change_log` | `PK__acc_mapp__3213E83FA3D6FC0B` |
| `acc_marketplace` | `PK__acc_mark__3213E83F344E5B58` |
| `acc_marketplace_profitability_rollup` | `PK__acc_mark__3213E83F7E5D67A2` |
| `acc_notification_destination` | `PK__acc_noti__3213E83FCF772A2B` |
| `acc_notification_subscription` | `PK__acc_noti__3213E83F34C921C4` |
| `acc_offer` | `PK__acc_offe__3213E83FD94CD642` |
| `acc_offer_fee_expected` | `PK__acc_offe__3213E83F6AA76D30` |
| `acc_order` | `PK__acc_orde__3213E83F7838E346` |
| `acc_order_line` | `PK__acc_orde__3213E83F6FD06FCE` |
| `acc_order_logistics_fact` | `PK_acc_order_logistics_fact` |
| `acc_order_logistics_shadow` | `PK_acc_order_logistics_shadow` |
| `acc_order_sync_state` | `PK__acc_orde__8BC9CBAE13AFFBD8` |
| `acc_plan_line` | `PK__acc_plan__3213E83FA8A30EA9` |
| `acc_plan_month` | `PK__acc_plan__3213E83F2F2B906C` |
| `acc_price_change_log` | `PK__acc_pric__3213E83F6AAA7E07` |
| `acc_pricing_recommendation` | `PK__acc_pric__3213E83FDF264CC3` |
| `acc_pricing_rule` | `PK__acc_pric__3213E83FF194AEF9` |
| `acc_pricing_snapshot` | `PK__acc_pric__3213E83F70B04CC1` |
| `acc_pricing_sync_state` | `PK__acc_pric__8BC9CBAEC9009C0E` |
| `acc_product` | `PK__acc_prod__3213E83F7D43DD6A` |
| `acc_product_match_suggestion` | `PK__acc_prod__3213E83F37D4EFA2` |
| `acc_profit_cost_config` | `PK__acc_prof__BDF6033CD39A2123` |
| `acc_profit_overhead_pool` | `PK__acc_prof__3213E83F1823B8DD` |
| `acc_ptd_cache` | `PK__acc_ptd___3213E83F28023A8B` |
| `acc_ptd_sync_state` | `PK__acc_ptd___8BC9CBAE11C2D9EF` |
| `acc_purchase_price` | `PK__acc_purc__3213E83FCAC8E213` |
| `acc_return_daily_summary` | `PK__acc_retu__3213E83FB2DE2A2D` |
| `acc_return_item` | `PK__acc_retu__3213E83F293EB1FD` |
| `acc_return_sync_state` | `PK__acc_retu__3213E83FB039978E` |
| `acc_sb_order_line_rebuild_state` | `PK__acc_sb_o__A913765D4D4BBC8D` |
| `acc_sb_order_line_staging` | `PK__acc_sb_o__3213E83F82B28503` |
| `acc_sb_order_line_sync_state` | `PK__acc_sb_o__52379DC092E5B793` |
| `acc_shipment` | `PK__acc_ship__3213E83F69506A9F` |
| `acc_shipment_cost` | `PK__acc_ship__3213E83F29A456C1` |
| `acc_shipment_event` | `PK__acc_ship__3213E83F17D6E0AC` |
| `acc_shipment_order_link` | `PK__acc_ship__3213E83F43E37371` |
| `acc_shipment_pod` | `PK__acc_ship__3213E83FC5339A1B` |
| `acc_shipping_cost` | `PK__acc_ship__3213E83F0B29095B` |
| `acc_sku_profitability_rollup` | `PK__acc_sku___3213E83FDA710894` |
| `acc_sp_api_usage_daily` | `PK_acc_sp_api_usage_daily` |
| `acc_spapi_usage` | `PK__acc_spap__3213E83FF609EE58` |
| `acc_taxonomy_alias` | `PK__acc_taxo__3213E83FB1469744` |
| `acc_taxonomy_node` | `PK__acc_taxo__3213E83F6F1BF7BD` |
| `acc_taxonomy_prediction` | `PK__acc_taxo__3213E83FB2E24EBE` |
| `acc_tkl_cache_meta` | `PK__acc_tkl___8B49A43B58A77608` |
| `acc_tkl_cache_rows` | `PK__acc_tkl___3213E83FA8D57F80` |
| `acc_user` | `PK__acc_user__3213E83F02E6B825` |
| `amazon_clearing_reconciliation` | `PK__amazon_c__3213E83F0896F379` |
| `compliance_issue` | `PK__complian__3213E83FEE70FE3D` |
| `decision_learning` | `PK__decision__3213E83F6FCC18AA` |
| `ecb_exchange_rate` | `PK__ecb_exch__3213E83F78D94C4F` |
| `executive_daily_metrics` | `PK__executiv__3213E83F8C1F873A` |
| `executive_health_score` | `PK__executiv__3213E83FF3D980C7` |
| `executive_opportunities` | `PK__executiv__3213E83FDA980D43` |
| `family_coverage_cache` | `PK_family_coverage` |
| `family_fix_job` | `PK__family_f__3213E83FBC709F87` |
| `family_fix_package` | `PK__family_f__3213E83FF996F486` |
| `family_issues_cache` | `PK__family_i__3213E83FE8DB4EED` |
| `family_restructure_log` | `PK__family_r__3213E83FF71B8B27` |
| `family_restructure_run` | `PK__family_r__7D3D901B4596F44E` |
| `fba_stock_movement_ledger` | `PK__fba_stoc__3213E83F384798C9` |
| `filing_readiness_snapshot` | `PK__filing_r__3213E83F023E9948` |
| `global_family` | `PK__global_f__3213E83FC4312649` |
| `global_family_child` | `PK__global_f__3213E83F1F8FD23A` |
| `global_family_child_market_link` | `PK_gfcml` |
| `global_family_market_link` | `PK_gfml` |
| `growth_opportunity` | `PK__growth_o__3213E83F635D8791` |
| `growth_opportunity_log` | `PK__growth_o__3213E83F99510B5D` |
| `local_vat_ledger` | `PK__local_va__3213E83FE9A40BAE` |
| `marketplace_listing_child` | `PK_marketplace_listing_child` |
| `opportunity_execution` | `PK__opportun__3213E83F3B4A15FB` |
| `opportunity_model_adjustments` | `PK__opportun__3213E83F5F738C4E` |
| `opportunity_outcome` | `PK__opportun__3213E83FA85374EE` |
| `oss_return_line` | `PK__oss_retu__3213E83FB136FD73` |
| `oss_return_period` | `PK__oss_retu__3213E83F6D613470` |
| `seasonality_cluster` | `PK__seasonal__3213E83F57EDF64D` |
| `seasonality_cluster_member` | `PK__seasonal__3213E83F955953E7` |
| `seasonality_index_cache` | `PK_season_index_cache` |
| `seasonality_monthly_metrics` | `PK__seasonal__3213E83FF67B7CC4` |
| `seasonality_opportunity` | `PK__seasonal__3213E83F154D5533` |
| `seasonality_profile` | `PK__seasonal__3213E83F662630D6` |
| `seasonality_settings` | `PK__seasonal__3213E83F09756CC5` |
| `strategy_experiment` | `PK__strategy__3213E83F2B6FF4E8` |
| `transport_evidence_record` | `PK__transpor__3213E83F474A42F7` |
| `vat_event_ledger` | `PK__vat_even__3213E83FBEBB414B` |
| `vat_rate_mapping` | `PK__vat_rate__3213E83F347E5B1A` |
| `vat_transaction_classification` | `PK__vat_tran__3213E83FB01E2EE4` |

### UNIQUE_CONSTRAINT (31)

| Table | Constraint |
|-------|-----------|
| `acc_al_plans` | `UQ_acc_al_plan_month` |
| `acc_backfill_progress` | `UQ__acc_back__F7535F464D70E488` |
| `acc_backfill_report_progress` | `UQ__acc_back__F7535F46D28DF5EC` |
| `acc_cogs_import_log` | `UQ__acc_cogs__AB51902A9B85E840` |
| `acc_event_log` | `UQ__acc_even__2370F72608E9B924` |
| `acc_exchange_rate` | `UQ_acc_rate_date_currency` |
| `acc_fba_customer_return` | `UQ_acc_fba_return_row` |
| `acc_fba_inbound_shipment` | `UQ_acc_fba_inbound_shipment` |
| `acc_fba_inventory_snapshot` | `UQ_acc_fba_inventory_snapshot` |
| `acc_listing_state` | `uq_listing_state_sku_mkt` |
| `acc_marketplace` | `UQ_acc_marketplace_code` |
| `acc_marketplace_profitability_rollup` | `UQ_mkt_rollup_day` |
| `acc_notification_destination` | `UQ__acc_noti__550153907B371016` |
| `acc_notification_subscription` | `UQ__acc_noti__863A7EC06B17694F` |
| `acc_notification_subscription` | `uq_sub_type` |
| `acc_order` | `UQ_acc_order_amazon_id` |
| `acc_plan_month` | `UQ_acc_plan_month_mkt` |
| `acc_pricing_rule` | `uq_pricing_rule_sku_mkt_type` |
| `acc_product` | `UQ_acc_product_asin` |
| `acc_ptd_cache` | `uq_ptd_cache_type_mkt_req_locale` |
| `acc_return_daily_summary` | `UQ_acc_return_daily` |
| `acc_return_item` | `UQ_acc_return_item_order_line` |
| `acc_return_sync_state` | `UQ__acc_retu__8BC9CBAFD1B9C3BE` |
| `acc_shipping_cost` | `uq_acc_shipping_cost_order` |
| `acc_sku_profitability_rollup` | `UQ_sku_rollup_day` |
| `acc_taxonomy_node` | `UQ__acc_taxo__9418F59A956D0F4F` |
| `acc_user` | `UQ_acc_user_email` |
| `executive_daily_metrics` | `UQ_exec_daily_mkt` |
| `executive_health_score` | `UQ__executiv__CD23BEA10F61E1E6` |
| `global_family` | `UQ_global_family_de_parent` |
| `global_family_child` | `UX_gfc_family_master` |
