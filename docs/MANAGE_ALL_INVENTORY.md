# Manage All Inventory

## Scope

`Manage All Inventory` is the ACC-native decision surface for Amazon EU inventory and listing operations. It joins:
- FBA inventory truth from `acc_fba_inventory_snapshot`
- listing / family context from `marketplace_listing_child`, `global_family_*`, and `acc_amazon_listing_registry`
- order-derived velocity from `acc_order` + `acc_order_line`
- Sales & Traffic coverage, when available, from `acc_inv_traffic_*`

It is intentionally built as an ACC module:
- FastAPI router under `apps/api/app/api/v1/manage_inventory.py`
- service layer under `apps/api/app/services/manage_inventory.py`
- MSSQL schema bootstrap via `ensure_manage_inventory_schema()`
- frontend pages under `apps/web/src/pages/Inventory*.tsx`
- ACC job runner integration via `inventory_*` job types

## Routes

Frontend:
- `/inventory/overview`
- `/inventory/all`
- `/inventory/families`
- `/inventory/drafts`
- `/inventory/jobs`
- `/inventory/settings`

Backend:
- `GET /api/v1/inventory/overview`
- `GET /api/v1/inventory/all`
- `GET /api/v1/inventory/sku/{sku}`
- `GET /api/v1/inventory/families`
- `GET /api/v1/inventory/families/{parent_asin}`
- `GET /api/v1/inventory/drafts`
- `POST /api/v1/inventory/drafts`
- `POST /api/v1/inventory/drafts/{id}/validate`
- `POST /api/v1/inventory/drafts/{id}/approve`
- `POST /api/v1/inventory/drafts/{id}/apply`
- `POST /api/v1/inventory/drafts/{id}/rollback`
- `GET /api/v1/inventory/jobs`
- `POST /api/v1/inventory/jobs/run`
- `GET /api/v1/inventory/settings`
- `PUT /api/v1/inventory/settings`

## Tables

Schema bootstrap currently creates:
- `dbo.acc_inv_traffic_sku_daily`
- `dbo.acc_inv_traffic_asin_daily`
- `dbo.acc_inv_traffic_rollup`
- `dbo.acc_inv_item_cache`
- `dbo.acc_inv_change_draft`
- `dbo.acc_inv_change_event`
- `dbo.acc_inv_settings`
- `dbo.acc_inv_category_cvr_baseline`

## Current Truth Model

Inventory and listing context are live enough for shell usage:
- inventory base comes from the latest `acc_fba_inventory_snapshot`
- listing state comes from `marketplace_listing_child`
- global family context comes from `global_family_child` and `global_family_market_link`
- registry enrichment comes from `acc_amazon_listing_registry`

Traffic is intentionally honest:
- if `acc_inv_traffic_rollup` is empty, UI and API expose traffic coverage as `partial`
- decision badges remain conservative rather than fabricating Sessions / CVR confidence

Cache layer is now part of the runtime truth path:
- `acc_inv_item_cache` stores prejoined inventory/listing/family/traffic rows for fast `/inventory/overview` and `/inventory/all`
- runtime uses cache only when the cache fully covers the requested snapshot scope
- if cache coverage is incomplete, service falls back to live build instead of serving partial cached answers

## Job Types

Manual job controls are wired through ACC:
- `inventory_sync_listings`
- `inventory_sync_snapshots`
- `inventory_sync_sales_traffic`
- `inventory_compute_rollups`
- `inventory_run_alerts`

Current runtime behavior:
- listings -> `family_mapper.marketplace_sync.sync_marketplace_listings()`
- snapshots -> `fba_ops.sync_inventory_cache(return_meta=True)`
- sales traffic -> real SP-API Reports sync via `GET_SALES_AND_TRAFFIC_REPORT`
- rollups -> rebuild from `acc_inv_traffic_sku_daily` + `acc_inv_traffic_asin_daily`, then refresh `acc_inv_item_cache`
- alerts -> candidate evaluation from overview decision badges

Current Sales & Traffic note:
- live DE smoke confirmed Amazon report payload is JSON, not TSV
- current reliable source path is `salesAndTrafficByAsin`
- SKU-level rows are still sparse or absent in live DE reports, so join strategy remains:
  - prefer SKU
  - fallback by ASIN

## Production Caveats

This module is production-safe only in the sense that it does not fake certainty.

Important limitations today:
- Sales & Traffic connector is live at raw import level, but traffic rollup/cache still needs post-sync completion on live runtime before all screens show full coverage
- draft/apply/rollback now uses `JSON_LISTINGS_FEED`, but safe auto-build currently covers only:
  - `reparent`
  - `update_theme`
  Other mutation types (`create_parent`, `detach`) require explicit feed payload in draft
- alert persistence is not yet integrated into the shared alert engine; current `inventory_run_alerts` returns candidate counts
- live backend restart on the new code has been completed
- DE smoke passed on live API:
  - `/api/v1/inventory/overview`
  - `/api/v1/inventory/all`
- all-market runtime still depends on cache coverage; partial cache scopes fall back to live build

Because of that, the module should currently be treated as:
- production-grade inventory/listing decision shell
- fast cached inventory/listing/family surface for covered scopes
- honest partial traffic surface until rollups are fully refreshed
- safe change workflow with constrained live apply

It should not yet be treated as the final source of truth for:
- all mutation types in live parent/child apply automation
- canonical Sales & Traffic coverage on every marketplace until rollups are populated
- inventory alert history

## Next Practical Steps

1. Finish live traffic rollup/cache rebuild so `acc_inv_traffic_rollup` stops being partial on production runtime.
2. Persist inventory alerts into shared ACC alert tables.
3. Extend safe live apply beyond `reparent` / `update_theme` with guarded payload builders for more draft types.
4. Add CVR baselines into `acc_inv_category_cvr_baseline`.
