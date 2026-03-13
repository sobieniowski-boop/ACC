# ACC — API Specification

> Version: 2026-03-12 | Framework: FastAPI 0.115 | Base URL: `/api/v1`
> Total Routers: 49 | HTTP Endpoints: ~380+ | WebSocket Endpoints: 2

---

## 1. Global Conventions

| Aspect | Convention |
|---|---|
| **Base URL** | `/api/v1` (all v1 routes), `/ws` (WebSocket) |
| **Auth** | Bearer JWT HS256 in `Authorization` header |
| **Content-Type** | `application/json` (default), `multipart/form-data` (uploads), `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet` (xlsx exports) |
| **Pagination** | `?skip=0&limit=50` (offset-based) |
| **Filtering** | `?marketplace_id=`, `?date_from=&date_to=`, `?seller_sku=` |
| **Error Shape** | `{ "detail": "..." }` (422 for validation, 401/403 for auth, 404 for not found) |
| **Async Jobs** | Return `202 Accepted` with `JobRunOut { job_id, status, message }` |
| **Deprecation** | Legacy `/profit/` and `/profitability/` — use `/profit/v2/` |

---

## 2. Authentication & Authorization

### Auth Dependencies

| Dependency | RBAC Level | Role Hierarchy |
|---|---|---|
| None | Public | No auth required |
| `get_current_user` | Any logged-in | JWT valid |
| `require_analyst` | ANALYST+ | ANALYST < OPS < CATEGORY_MGR < DIRECTOR < ADMIN |
| `require_ops` | OPS+ | OPS < CATEGORY_MGR < DIRECTOR < ADMIN |
| `require_role(CATEGORY_MGR)` | CATEGORY_MGR+ | CATEGORY_MGR < DIRECTOR < ADMIN |
| `require_director` | DIRECTOR+ | DIRECTOR < ADMIN |
| `require_admin` | ADMIN only | ADMIN |

### Auth Endpoints — `/auth`

| Method | Path | Auth | Request | Response | Notes |
|---|---|---|---|---|---|
| POST | `/auth/token` | — | `OAuth2PasswordRequestForm` (form data) | `TokenResponse { access_token, refresh_token, token_type }` | Login |
| POST | `/auth/refresh` | — | `{ refresh_token }` | `TokenResponse` | Refresh JWT |
| GET | `/auth/me` | `get_current_user` | — | `UserOut { id, email, role, is_active, allowed_marketplaces, allowed_brands }` | Current user |
| POST | `/auth/register` | `get_current_user` | `{ email, password, role }` | `UserOut` (201) | Create user |
| POST | `/auth/change-password` | `get_current_user` | `{ old_password, new_password }` | `{ message }` | Change password |

---

## 3. Health & System

### Health — `/health`

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/health` | — | Quick liveness check (DB + Redis) |
| GET | `/health/deep` | ANALYST+ | Deep check: DB pool, Redis, scheduler, SP-API tokens |
| GET | `/health/netfox-sessions` | ANALYST+ | Active Netfox debug sessions |
| GET | `/health/order-sync` | ANALYST+ | Order sync pipeline status |
| GET | `/health/sp-api-usage` | ANALYST+ | SP-API quota/rate usage |

### System — `/system`

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/system/health` | — | System-level health status |

### Guardrails — `/guardrails`

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/guardrails` | — | All guardrail check results |
| GET | `/guardrails/summary` | — | Summary pass/fail counts |
| GET | `/guardrails/check/{check_name}` | — | Specific check result |
| GET | `/guardrails/history` | — | Historical guardrail runs |

---

## 4. Profitability — `/profit/v2`

| Method | Path | Auth | Response | Description |
|---|---|---|---|---|
| GET | `/profit/v2/overview` | ANALYST+ | `ProfitabilityOverviewResponse` | Revenue, costs, margins KPIs |
| GET | `/profit/v2/orders` | ANALYST+ | `ProfitabilityOrdersResponse` | Order-level P&L breakdown |
| GET | `/profit/v2/sku-rollup` | ANALYST+ | `ProfitabilityProductsResponse` | SKU-level rollup |
| GET | `/profit/v2/marketplace-rollup` | ANALYST+ | `MarketplaceProfitabilityResponse` | Marketplace-level P&L |
| GET | `/profit/v2/products` | — | `ProductProfitTableResponse` | Product profit table |
| GET | `/profit/v2/products/export.xlsx` | — | XLSX binary | Excel export |
| GET | `/profit/v2/drilldown` | — | `ProductDrilldownResponse` | Per-SKU detailed breakdown |
| GET | `/profit/v2/loss-orders` | — | `LossOrdersResponse` | Loss-making orders |
| GET | `/profit/v2/what-if` | — | `ProductWhatIfResponse` | What-if scenario |
| GET | `/profit/v2/fee-breakdown` | — | `FeeBreakdownResponse` | Amazon fee detail |
| GET | `/profit/v2/data-quality` | — | `DataQualityResponse` | Data completeness score |
| GET | `/profit/v2/fee-gap-diagnostics` | — | `FeeGapDiagnosticsResponse` | Fee gap analysis |
| GET | `/profit/v2/kpis` | — | `ProfitKPIResponse` | Profit KPI cards |
| POST | `/profit/v2/simulate` | ANALYST+ | `PriceSimulatorResult` | Price change simulation |
| POST | `/profit/v2/recompute` | ANALYST+ | `RollupJobResult` | Trigger recomputation |
| POST | `/profit/v2/fee-gap-watch/seed` | — | `FeeGapWatchSeedResponse` | Seed fee gap watch |
| POST | `/profit/v2/fee-gap-watch/recheck` | — | `FeeGapRecheckResponse` | Recheck fee gaps |
| POST | `/profit/v2/purchase-price` | — | `PurchasePriceUpsertResponse` | Upsert purchase price |
| POST | `/profit/v2/map-and-price` | — | `MapAndPriceResponse` | Map SKU and set price |

### Product Tasks — `/profit/v2/tasks`

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/profit/v2/tasks` | — | `ProductTaskListResponse` |
| POST | `/profit/v2/tasks` | — | `ProductTaskItem` (201) |
| PATCH | `/profit/v2/tasks/{task_id}` | — | `ProductTaskItem` |
| GET | `/profit/v2/tasks/{task_id}/comments` | — | `list[ProductTaskCommentItem]` |
| POST | `/profit/v2/tasks/{task_id}/comments` | — | `ProductTaskCommentItem` |
| GET | `/profit/v2/tasks/owner-rules` | — | `list[TaskOwnerRuleItem]` |
| POST | `/profit/v2/tasks/owner-rules` | — | `TaskOwnerRuleItem` |
| DELETE | `/profit/v2/tasks/owner-rules/{rule_id}` | — | — (204) |

### AI Match — `/profit/v2/ai-match`

| Method | Path | Auth | Response |
|---|---|---|---|
| POST | `/profit/v2/ai-match/run` | — | `JobRunOut` (202) |
| GET | `/profit/v2/ai-match/suggestions` | — | `AIMatchSuggestionsResponse` |
| POST | `/profit/v2/ai-match/{suggestion_id}/approve` | — | `AIMatchActionResponse` |
| POST | `/profit/v2/ai-match/{suggestion_id}/reject` | — | `AIMatchActionResponse` |

---

## 5. KPI Dashboard — `/kpi`

| Method | Path | Auth | Response | Description |
|---|---|---|---|---|
| GET | `/kpi/summary` | ANALYST+ | `KPISummaryResponse` | Revenue, margin, orders, AOV |
| GET | `/kpi/chart/revenue` | ANALYST+ | `RevenueChartResponse` | Revenue over time |
| GET | `/kpi/chart/trends` | ANALYST+ | `TrendChartResponse` | Multi-metric trend data |
| GET | `/kpi/marketplaces` | ANALYST+ | — | Per-marketplace KPIs |
| GET | `/kpi/top-drivers` | ANALYST+ | — | Top revenue/margin drivers |
| GET | `/kpi/recent-alerts` | ANALYST+ | — | Recent alert feed |

---

## 6. Executive — `/executive`

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/executive/overview` | ANALYST+ | `ExecOverviewResponse` |
| GET | `/executive/products` | ANALYST+ | `ExecProductsResponse` |
| GET | `/executive/marketplaces` | ANALYST+ | `ExecMarketplacesResponse` |
| POST | `/executive/recompute` | ANALYST+ | `ExecRecomputeResult` |

---

## 7. Alerts — `/alerts`

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/alerts` | — | `AlertListResponse` |
| POST | `/alerts/{alert_id}/read` | — | — |
| POST | `/alerts/{alert_id}/resolve` | — | — |
| GET | `/alerts/rules` | — | `list[AlertRuleOut]` |
| POST | `/alerts/rules` | — | `AlertRuleOut` (201) |
| DELETE | `/alerts/rules/{rule_id}` | — | — (204) |

---

## 8. Jobs — `/jobs`

| Method | Path | Auth | Response | Description |
|---|---|---|---|---|
| POST | `/jobs/run` | — | `JobRunOut` (202) | Trigger named job |
| GET | `/jobs` | — | `JobListResponse` | List all jobs |
| GET | `/jobs/{job_id}` | — | `JobRunOut` | Job status |
| POST | `/jobs/import-cogs` | — | `JobRunOut` (202) | Import COGS file |
| POST | `/jobs/sync-listings` | — | `JobRunOut` (202) | Sync Amazon listings |

---

## 9. Pricing — `/pricing` & `/pricing-state`

### Pricing — `/pricing`

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/pricing/offers` | Any user | `PricingListResponse` |
| POST | `/pricing/offers/update` | CATEGORY_MGR+ | `list[PriceUpdateResponse]` |
| GET | `/pricing/buybox-stats` | Any user | `list[BuyBoxStatsOut]` |

### Pricing State — `/pricing-state`

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/pricing-state/snapshots/{seller_sku}` | — | `SnapshotHistoryResponse` |
| GET | `/pricing-state/snapshots/{seller_sku}/latest` | — | — |
| GET | `/pricing-state/buybox-overview` | — | `BuyBoxOverviewResponse` |
| GET | `/pricing-state/rules` | — | `PricingRuleListResponse` |
| POST | `/pricing-state/rules` | — | `PricingRuleOut` |
| DELETE | `/pricing-state/rules/{rule_id}` | — | — |
| GET | `/pricing-state/recommendations` | — | `RecommendationListResponse` |
| POST | `/pricing-state/recommendations/{rec_id}/decide` | — | — |
| POST | `/pricing-state/capture` | — | `CaptureResult` |
| POST | `/pricing-state/capture/all` | — | `CaptureAllResult` |
| POST | `/pricing-state/evaluate` | — | `EvalResult` |
| POST | `/pricing-state/evaluate/all` | — | `EvalAllResult` |
| POST | `/pricing-state/self-test` | — | — |

---

## 10. Advertising — `/ads`

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/ads/campaigns` | Any user | `list[CampaignOut]` |
| GET | `/ads/summary` | Any user | `AdsSummaryResponse` |
| GET | `/ads/chart` | Any user | `AdsChartResponse` |
| GET | `/ads/top-campaigns` | Any user | `list[TopCampaignRow]` |
| GET | `/ads/profiles` | Any user | — |
| POST | `/ads/sync` | Any user | `JobRunOut` (202) |
| GET | `/ads/campaign-stats` | Any user | — |

---

## 11. Inventory — `/inventory`

### Base Inventory

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/inventory/` | Any user | `InventoryListResponse` |
| GET | `/inventory/open-pos` | Any user | `list[OpenPOOut]` |
| GET | `/inventory/reorder-suggestions` | Any user | `list[ReorderSuggestionOut]` |

### Manage Inventory — `/inventory`

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/inventory/overview` | ANALYST+ | `InventoryOverviewResponse` |
| GET | `/inventory/all` | ANALYST+ | `InventoryAllResponse` |
| GET | `/inventory/sku/{sku}` | ANALYST+ | `InventorySkuDetailResponse` |
| GET | `/inventory/families` | ANALYST+ | `InventoryFamilyListResponse` |
| GET | `/inventory/families/{parent_asin}` | ANALYST+ | `InventoryFamilyDetailResponse` |
| GET | `/inventory/drafts` | ANALYST+ | `InventoryDraftListResponse` |
| POST | `/inventory/drafts` | ANALYST+ | `InventoryDraftItem` (201) |
| POST | `/inventory/drafts/{draft_id}/validate` | OPS+ | `InventoryDraftActionResponse` |
| POST | `/inventory/drafts/{draft_id}/approve` | DIRECTOR+ | `InventoryDraftActionResponse` |
| POST | `/inventory/drafts/{draft_id}/apply` | OPS+ | `InventoryDraftActionResponse` |
| POST | `/inventory/drafts/{draft_id}/apply-job` | OPS+ | `JobRunOut` (202) |
| POST | `/inventory/drafts/{draft_id}/rollback` | OPS+ | `InventoryDraftActionResponse` |
| POST | `/inventory/drafts/{draft_id}/rollback-job` | OPS+ | `JobRunOut` (202) |
| GET | `/inventory/jobs` | ANALYST+ | `InventoryJobListResponse` |
| POST | `/inventory/jobs/run` | OPS+ | — |
| GET | `/inventory/settings` | ANALYST+ | `InventorySettingsResponse` |
| PUT | `/inventory/settings` | DIRECTOR+ | `InventorySettingsResponse` |

### Inventory Taxonomy — `/inventory/taxonomy`

| Method | Path | Auth | Response |
|---|---|---|---|
| POST | `/inventory/taxonomy/refresh` | OPS+ | `JobRunOut` (202) |
| GET | `/inventory/taxonomy/predictions` | ANALYST+ | `TaxonomyPredictionListResponse` |
| POST | `/inventory/taxonomy/predictions/{id}/review` | DIRECTOR+ | `TaxonomyReviewResponse` |

### Inventory Risk — `/inventory-risk`

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/inventory-risk/dashboard` | — | — |
| GET | `/inventory-risk/scores` | — | — |
| GET | `/inventory-risk/history/{seller_sku}` | — | — |
| GET | `/inventory-risk/stockout-watchlist` | — | — |
| GET | `/inventory-risk/overstock-report` | — | — |
| POST | `/inventory-risk/compute` | — | — |
| GET | `/inventory-risk/replenishment-plan` | — | — |
| POST | `/inventory-risk/replenishment-plan/acknowledge` | — | — |
| GET | `/inventory-risk/alerts` | — | — |
| POST | `/inventory-risk/alerts/{alert_id}/resolve` | — | — |
| GET | `/inventory-risk/trends/{seller_sku}` | — | — |

---

## 12. Families — `/families`

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/families` | ANALYST+ | `FamilyListResponse` |
| GET | `/families/marketplaces` | ANALYST+ | — |
| GET | `/families/review` | ANALYST+ | `ReviewQueueResponse` |
| GET | `/families/fix-packages` | ANALYST+ | `FixPackageListResponse` |
| GET | `/families/{family_id}` | ANALYST+ | — |
| GET | `/families/{family_id}/children` | ANALYST+ | `list[ChildOut]` |
| GET | `/families/{family_id}/links` | ANALYST+ | `list[ChildMarketLinkOut]` |
| PUT | `/families/{family_id}/links/status` | OPS+ | — |
| GET | `/families/{family_id}/coverage` | ANALYST+ | `list[CoverageOut]` |
| GET | `/families/{family_id}/issues` | ANALYST+ | `list[IssueOut]` |
| POST | `/families/trigger/rebuild-de` | — | `TriggerResponse` |
| POST | `/families/trigger/sync-mp` | — | `JobRunOut` (202) |
| POST | `/families/trigger/matching` | — | `JobRunOut` (202) |
| POST | `/families/jobs/recompute-coverage` | — | `JobRunOut` (202) |
| POST | `/families/fix-packages/generate` | — | `TriggerResponse` |
| POST | `/families/fix-packages/{pkg_id}/approve` | — | — |
| POST | `/families/{family_id}/analyze-restructure` | — | — |
| POST | `/families/{family_id}/execute-restructure` | — | — |
| POST | `/families/{family_id}/execute-restructure/start` | — | — |
| GET | `/families/{family_id}/execute-restructure/status` | — | — |

---

## 13. Import Products — `/import-products`

| Method | Path | Auth | Response |
|---|---|---|---|
| POST | `/import-products/upload` | — | `JobRunOut` (202) |
| GET | `/import-products` | — | — |
| GET | `/import-products/summary` | — | — |
| GET | `/import-products/filter-options` | — | — |
| GET | `/import-products/skus` | — | — |

---

## 14. Content Operations — `/content-ops`

### Tasks & Versions

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/content-ops/tasks` | ANALYST+ | `ContentTaskListResponse` |
| POST | `/content-ops/tasks` | OPS+ | `ContentTaskItem` (201) |
| PATCH | `/content-ops/tasks/{task_id}` | OPS+ | `ContentTaskItem` |
| POST | `/content-ops/tasks/bulk-update` | OPS+ | `ContentTaskBulkUpdateResponse` |
| GET | `/content-ops/{sku}/{mp_id}/versions` | ANALYST+ | `ContentVersionListResponse` |
| POST | `/content-ops/{sku}/{mp_id}/versions` | OPS+ | `ContentVersionItem` (201) |
| PUT | `/content-ops/versions/{version_id}` | OPS+ | `ContentVersionItem` |
| POST | `/content-ops/versions/{version_id}/submit-review` | OPS+ | `ContentVersionItem` |
| POST | `/content-ops/versions/{version_id}/approve` | — | `ContentVersionItem` |

### Policy & Assets

| Method | Path | Auth | Response |
|---|---|---|---|
| POST | `/content-ops/policy/check` | — | `PolicyCheckResponse` |
| GET | `/content-ops/policy/rules` | — | `list[PolicyRuleItem]` |
| PUT | `/content-ops/policy/rules` | — | `list[PolicyRuleItem]` |
| POST | `/content-ops/assets/upload` | — | `AssetItem` (201) |
| GET | `/content-ops/assets` | — | `AssetListResponse` |
| POST | `/content-ops/assets/{asset_id}/link` | — | `AssetLinkItem` (201) |

### Publishing Pipeline

| Method | Path | Auth | Response |
|---|---|---|---|
| POST | `/content-ops/publish/package` | — | `PublishJobItem` |
| GET | `/content-ops/publish/jobs` | — | `PublishJobsResponse` |
| POST | `/content-ops/publish/push` | — | `PublishJobItem` |
| POST | `/content-ops/publish/jobs/{job_id}/retry` | — | `PublishPushAcceptedResponse` |
| GET | `/content-ops/publish/coverage` | — | `ContentPublishCoverageResponse` |
| GET | `/content-ops/publish/queue-health` | — | `ContentPublishQueueHealthResponse` |
| GET | `/content-ops/publish/circuit-breaker` | — | — |
| POST | `/content-ops/publish/circuit-breaker/reset` | — | — |

### Product Type Mappings

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/content-ops/publish/product-type-mappings` | — | `list[ContentProductTypeMapRule]` |
| PUT | `/content-ops/publish/product-type-mappings` | — | `list[ContentProductTypeMapRule]` |
| GET | `/content-ops/publish/product-type-definitions` | — | `list[ContentProductTypeDefinitionItem]` |
| POST | `/content-ops/publish/product-type-definitions/refresh` | — | `ContentProductTypeDefinitionItem` |
| POST | `/content-ops/publish/product-type-definitions/refresh-job` | — | `JobRunOut` (202) |
| GET | `/content-ops/publish/attribute-mappings` | — | `list[ContentAttributeMapRule]` |
| PUT | `/content-ops/publish/attribute-mappings` | — | `list[ContentAttributeMapRule]` |
| GET | `/content-ops/publish/mapping-suggestions` | — | `ContentPublishMappingSuggestionsResponse` |
| POST | `/content-ops/publish/mapping-suggestions/apply` | — | `ContentPublishMappingApplyResponse` |
| POST | `/content-ops/publish/mapping-suggestions/apply-job` | — | `JobRunOut` (202) |

### AI & Quality

| Method | Path | Auth | Response |
|---|---|---|---|
| POST | `/content-ops/ai/generate` | — | `AIContentGenerateResponse` |
| POST | `/content-ops/onboard/preflight` | — | `ContentOnboardPreflightResponse` |
| POST | `/content-ops/qa/verify` | — | `ContentQAVerifyResponse` |
| GET | `/content-ops/onboard/restrictions/check` | — | `ContentOnboardRestrictionResponse` |
| GET | `/content-ops/onboard/catalog/search-by-ean` | — | `ContentOnboardCatalogResponse` |
| GET | `/content-ops/{sku}/diff` | — | `ContentDiffResponse` |
| POST | `/content-ops/{sku}/sync` | — | `ContentSyncResponse` |
| GET | `/content-ops/health` | — | `ContentOpsHealthResponse` |
| GET | `/content-ops/compliance/queue` | — | `ContentComplianceQueueResponse` |
| GET | `/content-ops/impact` | — | `ContentImpactResponse` |
| GET | `/content-ops/data-quality` | — | `ContentDataQualityResponse` |

---

## 15. Content Optimization — `/content-optimization`

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/content-optimization/scores` | — | All listing optimization scores |
| GET | `/content-optimization/scores/{seller_sku}` | — | Single SKU score |
| GET | `/content-optimization/distribution` | — | Score distribution chart |
| GET | `/content-optimization/opportunities` | — | Optimization opportunities |
| GET | `/content-optimization/history/{seller_sku}` | — | Score history |
| GET | `/content-optimization/seo/{seller_sku}` | — | SEO analysis |
| POST | `/content-optimization/compute` | — | Recompute scores |

### Multi-language

| Method | Path | Auth |
|---|---|---|
| POST | `/content-optimization/multilang/generate` | — |
| POST | `/content-optimization/multilang/generate-single` | — |
| GET | `/content-optimization/multilang/jobs` | — |
| GET | `/content-optimization/multilang/coverage/{seller_sku}` | — |

### A/B Experiments

| Method | Path | Auth |
|---|---|---|
| POST | `/content-optimization/experiments` | — |
| GET | `/content-optimization/experiments` | — |
| GET | `/content-optimization/experiments/summary` | — |
| GET | `/content-optimization/experiments/{experiment_id}` | — |
| POST | `/content-optimization/experiments/{experiment_id}/variants` | — |
| POST | `/content-optimization/experiments/{experiment_id}/start` | — |
| POST | `/content-optimization/experiments/{experiment_id}/conclude` | — |
| POST | `/content-optimization/experiments/variants/{variant_id}/metrics` | — |

---

## 16. FBA Operations — `/fba`

### Dashboard & Inventory

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/fba/overview` | ANALYST+ | `FbaOverviewResponse` |
| GET | `/fba/diagnostics/report-status` | ANALYST+ | `FbaReportDiagnosticsResponse` |
| GET | `/fba/inventory` | ANALYST+ | `FbaInventoryListResponse` |
| GET | `/fba/inventory/{sku}` | ANALYST+ | `FbaInventoryDetailResponse` |
| GET | `/fba/replenishment/suggestions` | ANALYST+ | `FbaReplenishmentResponse` |
| GET | `/fba/aged` | ANALYST+ | `list[FbaAgedItem]` |
| GET | `/fba/stranded` | ANALYST+ | `list[FbaStrandedItem]` |
| GET | `/fba/kpi/scorecard` | ANALYST+ | `FbaKpiScorecardResponse` |

### Inbound Shipments

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/fba/inbound/shipments` | ANALYST+ | `FbaInboundShipmentListResponse` |
| GET | `/fba/inbound/shipments/{shipment_id}` | ANALYST+ | `FbaInboundShipmentDetailResponse` |
| GET | `/fba/shipment-plans` | ANALYST+ | `FbaShipmentPlanListResponse` |
| POST | `/fba/shipment-plans` | OPS+ | `FbaShipmentPlanItem` (201) |
| PATCH | `/fba/shipment-plans/{record_id}` | OPS+ | `FbaShipmentPlanItem` |
| DELETE | `/fba/shipment-plans/{record_id}` | OPS+ | — (204) |

### Cases

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/fba/cases` | ANALYST+ | `FbaCaseListResponse` |
| POST | `/fba/cases` | OPS+ | `FbaCaseItem` (201) |
| PATCH | `/fba/cases/{record_id}` | OPS+ | `FbaCaseItem` |
| DELETE | `/fba/cases/{record_id}` | OPS+ | — (204) |
| GET | `/fba/cases/{record_id}/timeline` | ANALYST+ | `FbaCaseTimelineResponse` |
| POST | `/fba/cases/{record_id}/comments` | OPS+ | `FbaCaseTimelineResponse` |

### Fee Audit

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/fba/fee-audit/anomalies` | ANALYST+ | `FbaFeeAnomalyResponse` |
| GET | `/fba/fee-audit/timeline/{sku}` | ANALYST+ | `FbaFeeTimelineResponse` |
| GET | `/fba/fee-audit/overcharges` | ANALYST+ | `FbaOverchargeSummaryResponse` |
| GET | `/fba/fee-audit/reference` | ANALYST+ | `FbaFeeReferenceResponse` |

### Launches & Initiatives

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/fba/launches` | ANALYST+ | `FbaLaunchListResponse` |
| POST | `/fba/launches` | OPS+ | `FbaLaunchItem` (201) |
| PATCH | `/fba/launches/{record_id}` | OPS+ | `FbaLaunchItem` |
| DELETE | `/fba/launches/{record_id}` | OPS+ | — (204) |
| GET | `/fba/initiatives` | ANALYST+ | `FbaInitiativeListResponse` |
| POST | `/fba/initiatives` | OPS+ | `FbaInitiativeItem` (201) |
| PATCH | `/fba/initiatives/{record_id}` | OPS+ | `FbaInitiativeItem` |
| DELETE | `/fba/initiatives/{record_id}` | OPS+ | — (204) |

### Register & Reconciliation

| Method | Path | Auth |
|---|---|---|
| POST | `/fba/registers/import` | OPS+ |
| POST | `/fba/reconciliation/sync` | OPS+ |
| POST | `/fba/jobs/run` | OPS+ |

---

## 17. Finance Center — `/finance`

### Dashboard & Sync

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/finance/dashboard` | ANALYST+ | `FinanceDashboardResponse` |
| GET | `/finance/sync/diagnostics` | ANALYST+ | `FinanceSyncDiagnosticsResponse` |
| GET | `/finance/sync/completeness` | ANALYST+ | `FinanceCompletenessResponse` |
| GET | `/finance/sync/gap-diagnostics` | ANALYST+ | `FinanceGapDiagnosticsResponse` |
| GET | `/finance/sync/order-revenue-integrity` | ANALYST+ | `FinanceRevenueIntegrityResponse` |

### Imports

| Method | Path | Auth | Response |
|---|---|---|---|
| POST | `/finance/import/amazon/transactions` | ADMIN | `JobRunOut` (202) |
| POST | `/finance/import/amazon/settlements` | ADMIN | `JobRunOut` (202) |
| POST | `/finance/import/bank/csv` | ADMIN | `FinanceBankImportOut` |

### Ledger & Accounts

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/finance/ledger` | ANALYST+ | `FinanceLedgerListResponse` |
| POST | `/finance/ledger/manual` | ADMIN | `FinanceCreateOut` |
| POST | `/finance/ledger/reverse/{entry_id}` | ADMIN | `FinanceCreateOut` |
| GET | `/finance/accounts` | ANALYST+ | `list[FinanceAccountItem]` |
| PUT | `/finance/accounts` | ADMIN | `FinanceAccountItem` |
| GET | `/finance/tax-codes` | ANALYST+ | `list[FinanceTaxCodeItem]` |
| PUT | `/finance/tax-codes` | ADMIN | `FinanceTaxCodeItem` |

### Reconciliation

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/finance/reconcile/payouts` | ANALYST+ | `FinancePayoutReconciliationListResponse` |
| POST | `/finance/reconcile/payouts/auto-match` | ADMIN | `FinanceAutoMatchOut` |
| POST | `/finance/jobs/run-ledger` | ADMIN | `JobRunOut` (202) |
| POST | `/finance/jobs/run-reconciliation` | ADMIN | `JobRunOut` (202) |
| GET | `/finance/jobs` | ANALYST+ | `JobListResponse` |
| GET | `/finance/jobs/{job_id}` | ANALYST+ | `JobRunOut` |

---

## 18. Tax Compliance — `/tax`

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/tax/overview` | — | Tax compliance dashboard |
| GET | `/tax/vat-events` | — | VAT transaction events |
| POST | `/tax/classification/recompute` | — | Recompute VAT classifications |
| POST | `/tax/vat-events/{event_id}/override-classification` | — | Override VAT class |
| GET | `/tax/oss/overview` | — | OSS scheme overview |
| GET | `/tax/oss/period/{year}/{quarter}` | — | OSS quarterly period |
| POST | `/tax/oss/build-period` | — | Build OSS period |
| GET | `/tax/oss/corrections` | — | OSS corrections |
| GET | `/tax/evidence` | — | Transport evidence records |
| GET | `/tax/local-vat` | — | Local VAT registrations |
| GET | `/tax/fba-movements` | — | FBA stock movements |
| GET | `/tax/reconciliation/amazon` | — | Amazon vs computed tax |
| POST | `/tax/reconciliation/run` | — | Run reconciliation |
| GET | `/tax/filing-readiness` | — | Filing readiness per country |
| GET | `/tax/filing-readiness/blockers` | — | Blockers for filing |
| GET | `/tax/compliance-issues` | — | Open compliance issues |
| POST | `/tax/compliance-issues/{issue_id}/assign` | — | Assign issue |
| POST | `/tax/compliance-issues/{issue_id}/resolve` | — | Resolve issue |
| POST | `/tax/detect-issues` | — | Run issue detection |
| GET | `/tax/vat-rates` | — | VAT rate table |
| POST | `/tax/vat-rates/upsert` | — | Upsert VAT rates |
| POST | `/tax/ecb-rates/sync` | — | Sync ECB exchange rates |
| POST | `/tax/pipeline/run` | — | Run full tax pipeline |
| POST | `/tax/audit-pack/generate` | — | Generate audit archive |

---

## 19. Logistics

### DHL — `/dhl`

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/dhl/health` | — | `DHLHealthResponse` |
| GET | `/dhl/shipments` | — | `DHLShipmentListResponse` |
| GET | `/dhl/shipments/count` | — | `DHLShipmentCountResponse` |
| GET | `/dhl/shipments/{shipment_id}/track` | — | `DHLTrackResponse` |
| GET | `/dhl/shipments/{shipment_id}/scan` | — | binary |
| GET | `/dhl/shipments/{shipment_id}/pod` | — | binary |
| GET | `/dhl/shipments/{shipment_id}/labels-data` | — | `DHLLabelsDataResponse` |
| GET | `/dhl/piece-id` | — | `DHLPieceResponse` |
| GET | `/dhl/cost-trace` | — | `DHLCostTraceResponse` |
| GET | `/dhl/unmatched-shipments` | — | `DHLUnmatchedShipmentsResponse` |
| GET | `/dhl/shadow-diff` | — | `DHLShadowDiffResponse` |
| POST | `/dhl/jobs/*` | — | `JobRunOut` (6 job triggers) |

### GLS — `/gls`

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/gls/health` | — | `GLSHealthResponse` |
| GET | `/gls/track/{parcel_number}` | — | `GLSTrackResponse` |
| POST | `/gls/track/batch` | — | `GLSTrackBatchResponse` |
| GET | `/gls/track-by-ref` | — | `GLSTrackByRefResponse` |
| GET | `/gls/event-codes` | — | — |
| POST | `/gls/cost-center/post` | — | `CostCenterPostResponse` |
| POST | `/gls/jobs/*` | — | `JobRunOut` (5 job triggers) |
| GET | `/gls/ade/*` | — | ADE API proxy endpoints |

### Courier (Unified) — `/courier`

| Method | Path | Auth |
|---|---|---|
| GET | `/courier/readiness` | — |
| GET | `/courier/coverage-matrix` | — |
| GET | `/courier/monthly-kpis` | — |
| GET | `/courier/order-relations` | — |
| GET | `/courier/shipment-outcomes` | — |
| GET | `/courier/link-gap-diagnostics` | — |
| GET | `/courier/closed-month-readiness` | — |
| POST | `/courier/jobs/*` | — (11 job triggers) |

---

## 20. Returns — `/returns`

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/returns/dashboard` | — | — |
| GET | `/returns/items` | — | — |
| PUT | `/returns/items/{item_id}/status` | — | — |
| POST | `/returns/seed` | — | `JobRunOut` (202) |
| POST | `/returns/reconcile` | — | `JobRunOut` (202) |
| POST | `/returns/rebuild-summary` | — | `JobRunOut` (202) |
| POST | `/returns/sync` | — | `JobRunOut` (202) |
| POST | `/returns/backfill` | — | `JobRunOut` (202) |

---

## 21. Strategy — `/strategy`

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/strategy/overview` | ANALYST+ | `StrategyOverviewResponse` |
| GET | `/strategy/opportunities` | ANALYST+ | `OpportunityListResponse` |
| GET | `/strategy/opportunities/{opp_id}` | ANALYST+ | `OpportunityDetailResponse` |
| POST | `/strategy/opportunities/{opp_id}/accept` | ANALYST+ | `StatusChangeResponse` |
| POST | `/strategy/opportunities/{opp_id}/reject` | ANALYST+ | `StatusChangeResponse` |
| POST | `/strategy/opportunities/{opp_id}/complete` | ANALYST+ | `StatusChangeResponse` |
| GET | `/strategy/playbooks` | ANALYST+ | `PlaybookListResponse` |
| GET | `/strategy/market-expansion` | ANALYST+ | `MarketExpansionResponse` |
| GET | `/strategy/bundles` | ANALYST+ | `BundleCandidateResponse` |
| GET | `/strategy/experiments` | ANALYST+ | `ExperimentListResponse` |
| POST | `/strategy/experiments` | ANALYST+ | — |
| POST | `/strategy/jobs/run` | ANALYST+ | `JobRunResponse` |

### Decision Intelligence — `/strategy/decisions`

| Method | Path | Auth |
|---|---|---|
| POST | `/strategy/decisions/executions` | ANALYST+ |
| GET | `/strategy/decisions/outcomes` | ANALYST+ |
| GET | `/strategy/decisions/outcomes/{execution_id}` | ANALYST+ |
| GET | `/strategy/decisions/outcomes/opportunity/{opp_id}` | ANALYST+ |
| GET | `/strategy/decisions/learning` | ANALYST+ |
| GET | `/strategy/decisions/learning/report` | ANALYST+ |
| POST | `/strategy/decisions/evaluate` | ANALYST+ |
| POST | `/strategy/decisions/aggregate` | ANALYST+ |
| POST | `/strategy/decisions/recalibrate` | ANALYST+ |

### Seasonality — `/seasonality`

| Method | Path | Auth |
|---|---|---|
| GET | `/seasonality/overview` | ANALYST+ |
| GET | `/seasonality/map` | ANALYST+ |
| GET | `/seasonality/entities` | ANALYST+ |
| GET | `/seasonality/entity/{type}/{id}` | ANALYST+ |
| GET | `/seasonality/opportunities` | ANALYST+ |
| POST | `/seasonality/opportunities/{opp_id}/accept` | ANALYST+ |
| POST | `/seasonality/opportunities/{opp_id}/reject` | ANALYST+ |
| GET | `/seasonality/clusters` | ANALYST+ |
| POST | `/seasonality/clusters` | ANALYST+ |
| PUT | `/seasonality/clusters/{cluster_id}` | ANALYST+ |
| POST | `/seasonality/clusters/{cluster_id}/recompute` | ANALYST+ |
| GET | `/seasonality/settings` | ANALYST+ |
| PUT | `/seasonality/settings` | ANALYST+ |
| POST | `/seasonality/jobs/run` | ANALYST+ |

---

## 22. Intelligence Engines

### BuyBox Radar — `/buybox-radar`

| Method | Path | Auth |
|---|---|---|
| GET | `/buybox-radar/dashboard` | — |
| GET | `/buybox-radar/trends/{seller_sku}` | — |
| GET | `/buybox-radar/rolling/{seller_sku}` | — |
| GET | `/buybox-radar/competitors/{asin}` | — |
| GET | `/buybox-radar/competitors/{asin}/history` | — |
| GET | `/buybox-radar/losses` | — |
| GET | `/buybox-radar/alerts` | — |
| GET | `/buybox-radar/landscape` | — |
| POST | `/buybox-radar/compute-trends` | — |
| POST | `/buybox-radar/raise-alerts` | — |
| POST | `/buybox-radar/capture-competitors` | — |

### Catalog Health — `/catalog-health`

| Method | Path | Auth |
|---|---|---|
| GET | `/catalog-health/scorecard` | — |
| GET | `/catalog-health/listing/{seller_sku}` | — |
| GET | `/catalog-health/suppressions` | — |
| GET | `/catalog-health/diffs` | — |
| GET | `/catalog-health/worst` | — |
| GET | `/catalog-health/trends` | — |
| POST | `/catalog-health/snapshot` | — |

### Repricing — `/repricing`

| Method | Path | Auth |
|---|---|---|
| GET | `/repricing/strategies` | — |
| POST | `/repricing/strategies` | — |
| DELETE | `/repricing/strategies/{strategy_id}` | — |
| POST | `/repricing/compute` | — |
| GET | `/repricing/executions` | — |
| POST | `/repricing/executions/{id}/approve` | — |
| POST | `/repricing/executions/{id}/reject` | — |
| POST | `/repricing/executions/bulk-approve` | — |
| POST | `/repricing/executions/execute` | — |
| GET | `/repricing/dashboard` | — |
| GET | `/repricing/analytics/trend` | — |

### Refund Anomaly — `/refund-anomaly`

| Method | Path | Auth |
|---|---|---|
| GET | `/refund-anomaly/dashboard` | — |
| POST | `/refund-anomaly/scan` | — |
| GET | `/refund-anomaly/anomalies` | — |
| PUT | `/refund-anomaly/anomalies/{id}/status` | — |
| GET | `/refund-anomaly/serial-returners` | — |
| GET | `/refund-anomaly/reimbursement-cases` | — |
| GET | `/refund-anomaly/trends` | — |

---

## 23. Platform

### Notifications — `/notifications`

| Method | Path | Auth |
|---|---|---|
| GET | `/notifications/health` | — |
| GET | `/notifications/destinations` | — |
| POST | `/notifications/destinations` | — (201) |
| DELETE | `/notifications/destinations/{id}` | — |
| GET | `/notifications/subscriptions` | — |
| POST | `/notifications/subscriptions` | — (201) |
| DELETE | `/notifications/subscriptions/{type}` | — |
| GET | `/notifications/supported-types` | — |
| POST | `/notifications/intake` | — |
| POST | `/notifications/intake/batch` | — |
| POST | `/notifications/poll-sqs` | — |
| GET | `/notifications/events` | — |
| POST | `/notifications/events/replay` | — |
| POST | `/notifications/events/process` | — |
| GET | `/notifications/sqs-metrics` | — |

### SQS Topology — `/sqs-topology`

| Method | Path | Auth |
|---|---|---|
| GET | `/sqs-topology/queues` | — |
| POST | `/sqs-topology/queues` | — |
| PATCH | `/sqs-topology/queues/{domain}/status` | — |
| GET | `/sqs-topology/health` | — |
| GET | `/sqs-topology/routing` | — |
| POST | `/sqs-topology/poll/{domain}` | — |
| POST | `/sqs-topology/poll-all` | — |
| POST | `/sqs-topology/seed` | — |
| GET | `/sqs-topology/dlq` | — |
| POST | `/sqs-topology/dlq/{entry_id}/resolve` | — |

### Event Wiring — `/event-wiring`

| Method | Path | Auth |
|---|---|---|
| GET | `/event-wiring/wires` | — |
| POST | `/event-wiring/wires` | — |
| PATCH | `/event-wiring/wires/{handler_name}/toggle` | — |
| DELETE | `/event-wiring/wires/{handler_name}` | — |
| POST | `/event-wiring/wires/seed` | — |
| GET | `/event-wiring/health` | — |
| POST | `/event-wiring/register-handlers` | — |
| POST | `/event-wiring/replay` | — |
| POST | `/event-wiring/replay/dlq` | — |
| GET | `/event-wiring/replay/jobs` | — |

### Backbone — `/backbone`

| Method | Path | Auth |
|---|---|---|
| GET | `/backbone/health` | — |
| POST | `/backbone/evaluate` | — |
| GET | `/backbone/alerts` | — |

---

## 24. Operator & Multi-Seller

### Operator Console — `/operator-console`

| Method | Path | Auth |
|---|---|---|
| GET | `/operator-console/dashboard` | — |
| GET | `/operator-console/feed` | — |
| GET | `/operator-console/cases` | — |
| GET | `/operator-console/cases/{case_id}` | — |
| POST | `/operator-console/cases` | — (201) |
| PATCH | `/operator-console/cases/{case_id}` | — |
| GET | `/operator-console/actions` | — |
| POST | `/operator-console/actions` | — (201) |
| POST | `/operator-console/actions/{id}/approve` | — |
| POST | `/operator-console/actions/{id}/reject` | — |
| POST | `/operator-console/actions/{id}/executed` | — |

### Account Hub — `/account-hub`

| Method | Path | Auth |
|---|---|---|
| GET | `/account-hub/dashboard` | — |
| GET | `/account-hub/sellers` | — |
| GET | `/account-hub/sellers/{id}` | — |
| POST | `/account-hub/sellers` | — (201) |
| PATCH | `/account-hub/sellers/{id}` | — |
| GET | `/account-hub/sellers/{id}/credentials` | — |
| POST | `/account-hub/sellers/{id}/credentials` | — |
| DELETE | `/account-hub/credentials/{id}` | — (204) |
| GET | `/account-hub/sellers/{id}/credentials/validate` | — |
| GET | `/account-hub/sellers/{id}/permissions` | — |
| POST | `/account-hub/sellers/{id}/permissions` | — |
| DELETE | `/account-hub/sellers/{id}/permissions` | — |
| GET | `/account-hub/users/{email}/permissions` | — |
| GET | `/account-hub/scheduler-status` | — |

---

## 25. AI & Intelligence — `/ai` & `/intelligence`

### AI Recommendations — `/ai`

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/ai/recommendations` | Any user | `AIRecommendationListResponse` |
| GET | `/ai/summary` | Any user | `AIInsightSummary` |
| POST | `/ai/generate` | DIRECTOR+ | `AIRecommendationOut` (201) |
| PATCH | `/ai/recommendations/{rec_id}` | Any user | `AIRecommendationOut` |

### Intelligence Dashboard — `/intelligence`

| Method | Path | Auth |
|---|---|---|
| GET | `/intelligence/dashboard` | ANALYST+ |
| GET | `/intelligence/funnel` | ANALYST+ |
| GET | `/intelligence/forecast-accuracy` | ANALYST+ |

---

## 26. Catalog & Listings

### Listing State — `/listing-state`

| Method | Path | Auth |
|---|---|---|
| GET | `/listing-state/health` | — |
| GET | `/listing-state/listings` | — |
| GET | `/listing-state/listings/{seller_sku}` | — |
| GET | `/listing-state/listings/{seller_sku}/history` | — |
| POST | `/listing-state/listings/{seller_sku}/refresh` | — |

### Catalog Definitions — `/catalog/definitions`

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/catalog/definitions` | — | `PTDListResponse` |
| GET | `/catalog/definitions/{product_type}` | — | `PTDSchemaResponse` |
| GET | `/catalog/definitions/{product_type}/required-attributes` | — | — |
| GET | `/catalog/definitions/{product_type}/variations` | — | `VariationInfoResponse` |
| POST | `/catalog/definitions/refresh` | — | `PTDRefreshResult` |
| POST | `/catalog/definitions/validate` | — | `ValidateResponse` |
| GET | `/catalog/definitions/{product_type}/diff` | — | `MarketplaceDiffResponse` |
| GET | `/catalog/definitions/stale` | — | — |

---

## 27. WebSocket — `/ws`

| Path | Description |
|---|---|
| `ws://host/ws/jobs/{job_id}` | Real-time job progress updates |
| `ws://host/ws/alerts` | Live alert feed |

---

## 28. Planning — `/planning`

| Method | Path | Auth | Response |
|---|---|---|---|
| GET | `/planning/months` | — | `list[PlanMonthOut]` |
| POST | `/planning/months` | — | `PlanMonthOut` (201) |
| PATCH | `/planning/months/{plan_id}/status` | — | `PlanMonthOut` |
| GET | `/planning/vs-actual` | — | `PlanVsActualResponse` |
| POST | `/planning/refresh` | — | `JobRunOut` (202) |

---

## 29. Audit — `/audit`

| Method | Path | Auth |
|---|---|---|
| GET | `/audit/cogs` | ANALYST+ |
| GET | `/audit/cogs/coverage` | ANALYST+ |
| GET | `/audit/cogs/prices` | ANALYST+ |
| GET | `/audit/cogs/margin` | ANALYST+ |
| GET | `/audit/controlling/summary` | ANALYST+ |
| GET | `/audit/controlling/mapping-history` | ANALYST+ |
| GET | `/audit/controlling/price-history` | ANALYST+ |
| GET | `/audit/controlling/stale-prices` | ANALYST+ |
| GET | `/audit/controlling/source-priority` | ANALYST+ |

---

## Appendix: Endpoint Count by Domain

| Domain | Endpoints |
|---|---|
| Content Ops | ~45 |
| FBA Ops | ~38 |
| Profit V2 | ~28 |
| Finance Center | ~20 |
| Tax Compliance | ~28 |
| Courier (Unified) | ~21 |
| GLS | ~22 |
| DHL | ~17 |
| Repricing | ~17 |
| Families | ~21 |
| Manage Inventory | ~17 |
| BuyBox Radar | ~11 |
| Refund Anomaly | ~15 |
| Seasonality | ~15 |
| Account Hub | ~14 |
| Operator Console | ~13 |
| Notifications | ~16 |
| Content Optimization | ~19 |
| Strategy | ~12 |
| Event Wiring | ~12 |
| SQS Topology | ~12 |
| Other (health, auth, kpi, etc.) | ~40 |
| **Total** | **~380+** |
