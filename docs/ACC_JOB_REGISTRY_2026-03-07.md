# ACC Job Registry

Date: 2026-03-07
Status: Active architectural contract for ACC background processing

## Rules

- Heavy work must not run in request-response.
- API endpoints may validate input, stage files, and enqueue a job, then return `202 Accepted` with `job_id`.
- Long-running work must execute as a job through `acc_al_jobs` or a dedicated queue table when the workflow needs its own queue semantics.
- Standard job status lifecycle:
  - `pending`
  - `running`
  - `retry_scheduled`
  - `completed`
  - `failure`
- Standard progress fields:
  - `progress_pct`
  - `progress_message`
  - `records_processed`
  - `last_heartbeat_at`
- Standard logging:
  - structured `*.start`
  - structured phase logs
  - structured `*.done`
  - structured `*.error`
- Standard idempotency rule:
  - rerun by the same natural key or same date window must upsert/replace, not duplicate
- Standard retry/backoff rule:
  - request path: never retry inline
  - scheduler/manual rerun: safe by idempotency
  - transient API/DB/file errors: `acc_al_jobs` now uses framework-level retry scheduling for retryable jobs
  - default backoff: `1m -> 5m -> 15m -> 60m`
  - apply jobs and other side-effect-heavy jobs default to `retry_policy=none`

## Shared execution model

| Area | Contract |
| --- | --- |
| Queue table | `dbo.acc_al_jobs` |
| Enqueue API | `enqueue_job(...)` |
| Specialized queue | `dbo.acc_co_publish_jobs` for Content Ops publish pushes |
| Status source of truth | Job row, not HTTP response |
| Output contract | Persisted domain data plus job row status/progress |
| Request-response behavior | `202 + JobRunOut`; no blocking on full execution |

## Sync Jobs

| Job | Trigger | Input | Output | Idempotency | Retry/backoff | Logging | Progress/status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `sync_orders` | on-demand, orchestrator | optional date/window params | refreshed order cache and downstream order sync state | window-based upsert | manual rerun; transient connector retries where implemented | `order_sync.*` | queued, running, completed/failure with processed order count |
| `sync_finances` | daily 03:00, on-demand | optional lookback | refreshed finance cache and settlement source data | import/upsert by transaction natural key | manual rerun; finance connector retries per source | `finance.sync.*` | phase progress by source, rows processed |
| `sync_inventory` | daily 04:00, on-demand | optional lookback | refreshed inventory snapshots/cache | snapshot/date-scoped upsert | manual rerun | `inventory.sync.*` | progress by stage: listings, stock, traffic |
| `sync_ads` | daily 07:00, on-demand | `days_back`, actor | ads profiles/campaigns/daily rows | upsert by profile/campaign/date | transient Amazon Ads retry inside sync; rerunnable by window | `ads.sync.*` | progress by profile/campaign/report rows |
| `sync_fba_inventory` | every 4h, on-demand | optional marketplace/window | FBA inventory snapshots | snapshot key upsert | manual rerun | `fba.inventory.*` | running with rows processed |
| `sync_fba_inbound` | every 2h, on-demand | optional shipment filters | inbound shipments/lines state | shipment/line upsert | manual rerun | `fba.inbound.*` | phase progress by shipment batch |
| `sync_pricing` | on-demand | marketplace/product filters | repricing snapshot/cache | latest-state overwrite | manual rerun | `pricing.sync.*` | rows processed |
| `sync_offer_fee_estimates` | on-demand | SKU/marketplace/date filters | refreshed fee estimates | upsert by SKU-marketplace-date | manual rerun | `fees.estimates.*` | rows processed |
| `sync_tkl_cache` | daily 01:40, on-demand | optional lookback | warmed TKL SQL cache | replace cache window | manual rerun | `tkl.cache.*` | stage progress |
| `sync_purchase_prices` | daily 02:00, on-demand | optional lookback | refreshed purchase price cache | upsert by SKU-date/source | manual rerun | `purchase_prices.sync.*` | rows processed |
| `sync_product_mapping` | on-demand | product/listing filters | refreshed product mapping state | upsert by listing/product key | manual rerun | `product_mapping.sync.*` | rows processed |
| `sync_listings_to_products` | daily 01:00, on-demand | `marketplace_ids`, actor | synced listing-to-product linkage | upsert by listing identity | manual rerun | `marketplace_sync.*` | progress by marketplace/listing count |
| `sync_amazon_listing_registry` | daily 01:30, on-demand | marketplace/window | registry tables refreshed | upsert by listing key | manual rerun | `listing_registry.sync.*` | rows processed |
| `sync_taxonomy` | nightly if enabled, on-demand | optional taxonomy scope | synced taxonomy source data | source-key upsert | manual rerun | `taxonomy.sync.*` | stage progress |
| `inventory_sync_listings` | on-demand, orchestrator | marketplace/scope filters | inventory listing staging refreshed | upsert by listing key | manual rerun | `inventory.jobs.sync_listings.*` | rows processed |
| `inventory_sync_snapshots` | on-demand, orchestrator | snapshot date/window | inventory snapshots refreshed | snapshot overwrite/upsert | manual rerun | `inventory.jobs.sync_snapshots.*` | rows processed |
| `inventory_sync_sales_traffic` | on-demand, orchestrator | date window | sales/traffic cache refreshed | window upsert | manual rerun | `inventory.jobs.sync_sales_traffic.*` | rows processed |
| `order_pipeline` | every 15 min, on-demand | optional date/window/force flags | order sync, enrichment, downstream consistency updates | rerun-safe by order natural key and step upserts | manual rerun; service-level retries for remote pulls | `order_pipeline.*` | detailed phase messages and records processed |
| `finance_sync_transactions` | on-demand/manual import | `days_back`, `marketplace_id` | Amazon transaction imports | transaction-key upsert | manual rerun | `finance.transactions.*` | phase progress by imported rows |
| `finance_prepare_settlements` | on-demand/manual | optional marketplace/window | settlement staging prepared | settlement-key upsert | manual rerun | `finance.settlements.*` | rows processed |
| `returns_seed_items` | on-demand/bootstrap | optional days/lookback | return tracker seeded with return items | return item upsert by order/return key | manual rerun | `returns.seed.*` | rows processed |
| `returns_sync_fba` | daily 06:30 pipeline, on-demand | `days_back`, marketplace scope | FBA return events synced | return event upsert | manual rerun; connector retries where implemented | `returns.sync_fba.*` | progress by marketplace/date chunk |
| `returns_backfill_fba` | on-demand | `days_back`, `chunk_days`, marketplace scope | historical FBA return sync | chunk-scoped upsert | chunk rerun safe | `returns.backfill.*` | progress by chunk |
| `dhl_backfill_shipments` | on-demand | date window, pagination | shipment registry and tracking seed | upsert by DHL shipment identity | manual rerun | `dhl.registry.*` | progress by shipment batch |
| `dhl_sync_tracking_events` | on-demand, future periodic | shipment/date scope | DHL tracking events and delivered truth | event upsert by shipment/timestamp/status | manual rerun; connector retries where implemented | `dhl.tracking.*` | progress by shipment/event count |
| `dhl_import_billing_files` | on-demand/backfill | file scope, force flags | DHL billing staging imported | source file + row natural key dedupe | internal file chunk retry; rerun safe | `dhl.billing_import.*` | progress by file/chunk/rows |
| `gls_import_billing_files` | nightly 00:20 if enabled, on-demand/backfill | file scope, force flags | GLS billing staging imported | source file + row natural key dedupe | internal chunk retry where implemented; rerun safe | `gls.billing_import.*` | progress by file/chunk/rows |
| `sync_bl_distribution_order_cache` | on-demand; scheduled sync optional and disabled by default | `date_confirmed_from`, `date_confirmed_to`, optional `source_ids`, `include_packages`, `limit_orders` | read-only cache of BaseLinker Distribution orders and packages in ACC | upsert by `order_id` and `package_id`; rerun-safe by window | framework retry for transient API/network errors; throttled single-threaded sync | `bl_distribution_cache.*` | progress by source, cursor, orders/packages synced |

## Rollup / Cache Jobs

| Job | Trigger | Input | Output | Idempotency | Retry/backoff | Logging | Progress/status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `inventory_compute_rollups` | on-demand, orchestrator | date window, marketplace/product scope | inventory rollup/cache tables | replace rollup for target grain | manual rerun | `inventory.rollups.*` | stage progress with rows |
| `dhl_seed_shipments_from_staging` | on-demand after import | `created_from`, `created_to`, `seed_all_existing` | `acc_shipment`, `acc_shipment_order_link` from DHL staging | upsert by shipment key and link key | manual rerun | `dhl.seed_shipments.*` | progress by selected/matched/linked shipments |
| `dhl_sync_costs` | on-demand after seed/import | shipment/date scope, `refresh_existing` | `acc_shipment_cost` for DHL | upsert by shipment + source | manual rerun | `dhl.cost_sync.*` | progress by shipment/cost count |
| `dhl_aggregate_logistics` | on-demand after costs | purchase/create date window, limits | `acc_order_logistics_fact` (`dhl_v1`) | replace aggregate for target order/source scope | manual rerun | `dhl.aggregate.*` | progress by orders aggregated |
| `gls_seed_shipments_from_staging` | nightly GLS pipeline, on-demand | `created_from`, `created_to`, `seed_all_existing` | `acc_shipment`, `acc_shipment_order_link` from GLS staging | upsert by shipment key and link key | manual rerun | `gls.seed_shipments.*` | progress by selected/matched/linked shipments |
| `gls_sync_costs` | nightly GLS pipeline, on-demand | shipment/date scope, `refresh_existing` | `acc_shipment_cost` for GLS | upsert by shipment + source | manual rerun | `gls.cost_sync.*` | progress by shipment/cost count |
| `gls_aggregate_logistics` | nightly GLS pipeline, on-demand | purchase/create date window, limits | `acc_order_logistics_fact` (`gls_v1`) | replace aggregate for target order/source scope | manual rerun | `gls.aggregate.*` | progress by orders aggregated |
| `planning_refresh_actuals` | on-demand | `year` or date scope | planning actuals cache/tables refreshed | replace actuals for target period | manual rerun | `planning.refresh_actuals.*` | progress by month and row count |
| `fee_gap_watch_seed` | on-demand/bootstrap | date scope | fee-gap watch baseline cache | upsert by order/listing/date grain | manual rerun | `fee_gap.seed.*` | rows processed |
| `fee_gap_watch_recheck` | daily 03:20, on-demand | recent lookback | refreshed fee-gap watch results | replace latest recheck window | manual rerun | `fee_gap.recheck.*` | rows processed |
| `calc_profit` | daily 05:00, on-demand | date window, marketplace scope, batch size | recalculated profit facts and rollups | replace/recompute deterministically by order/date | manual rerun in batches | `profit.calc.*` | progress by date batch/orders recomputed |
| `generate_ai_report` | on-demand | report params and scope | generated AI report artifacts | idempotent only with explicit idempotency key; otherwise new artifact | manual rerun; no inline retry | `ai_report.*` | phase progress |

## Alert Engine Jobs

| Job | Trigger | Input | Output | Idempotency | Retry/backoff | Logging | Progress/status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `evaluate_alerts` | every 60 min, on-demand | recent window; may include module flags | refreshed generic alert rows, finance health, order sync health | evaluate-and-upsert/resolve by alert fingerprint | manual rerun; no GET side effects | `alerts.evaluate.*` | progress by sub-engine and alert counts |
| `inventory_run_alerts` | on-demand, orchestrator | scope of inventory SKUs/marketplaces | inventory alert rows | alert fingerprint upsert/resolve | manual rerun | `inventory.alerts.*` | rows created/updated/resolved |
| `run_fba_alerts` | every 2h, on-demand | FBA scope/window | FBA alert rows | alert fingerprint upsert/resolve | manual rerun | `fba.alerts.*` | counts by alert type |
| `courier_evaluate_alerts` | hourly via alert loop, on-demand | `window_days`, courier scope | courier health alerts for billing/matching/pipeline drift | alert fingerprint upsert/resolve | manual rerun | `courier.alerts.*` | created/updated/resolved counts |

## Apply Jobs

| Job | Trigger | Input | Output | Idempotency | Retry/backoff | Logging | Progress/status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `inventory_apply_draft` | on-demand from UI | `draft_id`, actor | applied inventory draft mutations | guarded by draft status; repeat after success must no-op | manual rerun only if first run failed before final commit | `inventory.apply_draft.*` | progress by line/apply phase |
| `inventory_rollback_draft` | on-demand from UI | `draft_id`, actor | rolled back draft effects | guarded by draft status and rollback markers | manual rerun only if rollback incomplete | `inventory.rollback_draft.*` | progress by line/rollback phase |
| `content_apply_publish_mapping_suggestions` | on-demand from UI | marketplaces, selection, confidence, limit, `dry_run` | accepted/rejected publish mapping suggestions | suggestion state transition makes rerun safe; dry run is read-only | manual rerun | `content.publish_mapping.apply.*` | progress by scanned/applied suggestion count |
| `content_refresh_product_type_definition` | on-demand from UI | marketplace, product type, actor | refreshed content definition cache/state | replace latest definition snapshot | manual rerun | `content.product_type.refresh.*` | phase progress |
| `cogs_import` | daily 06:00, on-demand | folder/file filters | imported COGS rows | file/row natural key dedupe | manual rerun | `cogs.import.*` | progress by file/rows |
| `import_products_upload` | on-demand from upload endpoint | staged `file_path`, original filename, actor | parsed import rows and upserts into import-products tables | staged file processed once; domain upserts by product natural key | staged file cleaned on success/failure; rerun by re-enqueueing file | `import_products.upload.*` | progress by read/parse/upsert phases |

## Recompute Jobs

| Job | Trigger | Input | Output | Idempotency | Retry/backoff | Logging | Progress/status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `recompute_fba_replenishment` | on-demand, downstream of FBA refresh | marketplace/SKU scope | refreshed replenishment recommendations | deterministic replace by target scope | manual rerun | `fba.replenishment.*` | progress by SKU count |
| `family_sync_marketplace_listings` | on-demand; nightly candidate later | `marketplace_ids`, `family_ids`, actor | refreshed marketplace listing staging for family mapper | upsert by family/listing identity | manual rerun | `families.sync_marketplace.*` | progress by marketplace/family |
| `family_matching_pipeline` | on-demand; nightly candidate later | `marketplace_ids`, `family_ids`, actor | family matches plus coverage recompute | rerun-safe by family/listing keys | manual rerun | `families.matching.*` | progress by match phase, then coverage phase |
| `family_recompute_coverage` | on-demand and downstream of matching | `marketplace_ids`, `family_ids`, actor | refreshed family coverage metrics | replace coverage snapshot for target scope | manual rerun | `families.coverage.*` | progress by family count |
| `returns_reconcile` | daily 06:30 pipeline, on-demand | recent return scope | matched return items with financial/operational truth | upsert by return/order/item key | manual rerun | `returns.reconcile.*` | progress by marketplace/chunk |
| `returns_rebuild_summary` | daily 06:30 pipeline, on-demand | date window | rebuilt return summary tables/views | replace summary for target window | manual rerun | `returns.summary.*` | progress by date chunk |
| `profit_ai_match_run` | on-demand | optional date window, limit, actor | AI-assisted profit matching suggestions/results | replace or append by explicit run scope; should use idempotency key for repeated exact runs | manual rerun; no inline retry | `profit.ai_match.*` | progress by candidate/match count |

## Verification Jobs

| Job | Trigger | Input | Output | Idempotency | Retry/backoff | Logging | Progress/status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `dhl_shadow_logistics` | on-demand after DHL aggregate | purchase window, order limits, rebuild flag | `acc_order_logistics_shadow` for `dhl_v1` | replace shadow rows for target scope | manual rerun | `dhl.shadow.*` | progress by orders compared and status counts |
| `gls_shadow_logistics` | nightly GLS pipeline, on-demand | purchase window, order limits, rebuild flag | `acc_order_logistics_shadow` for `gls_v1` | replace shadow rows for target scope | manual rerun | `gls.shadow.*` | progress by orders compared and status counts |
| `courier_verify_billing_completeness` | daily configurable, on-demand | expected billing file scope/months | audit rows for imported vs expected courier billing files | replace verification snapshot per day/month/source | manual rerun | `courier.verification.*` | progress by source, month, missing file count |

## Specialized queues outside `acc_al_jobs`

| Queue / Job family | Trigger | Input | Output | Idempotency | Retry/backoff | Logging | Progress/status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Content publish queue (`acc_co_publish_jobs`) | every 1 min worker, on-demand enqueue from Content Ops | marketplace, sku/asin, payload, idempotency key, actor | outbound publish pushes and confirmation status | explicit `idempotency_key` support already built in | queue has retry metadata (`retry_count`, `max_retries`, `next_retry_at`) and stale-job detection | `content.publish_queue.*` | queue row status, heartbeat, last error, retry metadata |

## Endpoint policy after this refactor

- Job endpoints must be enqueue-only.
- `GET` endpoints must stay read-only.
- Upload endpoints may stage files locally or in temp storage, but parsing/import execution must happen in the job.
- Bulk apply/recompute operations must expose a job endpoint even if a direct endpoint still exists for narrow/manual use.

## Remaining policy debt to close later

- Standardize framework-level retry/backoff in `acc_al_jobs`, not only service-specific retries.
- Add explicit idempotency keys for more manual jobs where exact repeated requests are common.
- Migrate any remaining heavy direct mutations to job endpoints if they grow beyond single-record or short-lived operations.
