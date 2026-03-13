# ADR-004: Rollup Divergence — Root Cause & Resolution

**Status**: Accepted  
**Date**: 2025-01-14  
**Author**: Backend Architect Agent (Phase 4)

## Context

Dashboard (`/profit/v2/overview`) and Products (`/profit/v2/products`) showed **different numbers** for the same date range. Users reported inconsistency.

## Root Cause

Two independent data sources with divergent refresh cycles:

| Endpoint | Data Source | Refresh | Latency |
|---|---|---|---|
| `/profit/v2/overview` | `acc_sku_profitability_rollup` | Daily 05:45, last 7 days | Up to 24h stale |
| `/profit/v2/products` | `acc_order` + `acc_order_line` (LIVE) | Real-time | ~0s |

Additionally, within `/overview` itself, `loss_orders` comes from LIVE `acc_order` while KPIs come from rollup — internal inconsistency within the same response.

The `staleMinutes=30` on the frontend `DataFreshness` component was incorrect (scheduler runs once daily, so 30min threshold meant perpetual "stale" state).

## Decision

**Keep rollup tables for Dashboard KPIs** (performance) but make the divergence explicit and optional:

1. **acc_system_metadata** table — persists `rollup_recomputed_at`, `rollup_date_from`, `rollup_date_to` after every recompute job. Single source of truth for freshness.

2. **data_freshness** on every response — includes `data_source: "rollup"|"live"`, `rollup_recomputed_at`, `rollup_covers: {date_from, date_to}`, `cache_age_seconds`.

3. **`use_rollup` parameter** on `/profit/v2/products` — `?use_rollup=true` reads from rollup (fast), default (`false`) reads live order data (accurate).

4. **Frontend staleMinutes** fixed from 30 → 1440 (24h) to match actual scheduler frequency.

## Rationale

- The live V2 engine (`profit_engine.get_product_profit_table`) is comprehensive (34 columns, parent rollup, what-if mode) but slower (~2-5s).
- The rollup tables are fast (<200ms) but stale by design.
- Eliminating the rollup entirely is viable **only if** the V2 engine can serve Dashboard aggregates in <500ms. Until then, rollup remains the performance path for Dashboard KPIs.

## Consequences

- Frontend can toggle between rollup and live for Products view.
- Users see explicit "data source" and "last recomputed" indicators — no hidden staleness.
- The internal Overview inconsistency (loss_orders from LIVE, KPIs from rollup) remains documented but unfixed — `data_source` is set to `"mixed"` to communicate this. A future phase should unify all Overview data to one source.
- The rollup path on `/products?use_rollup=true` returns `ProfitabilityProductsResponse` shape (16 fields/item) via `JSONResponse`, bypassing the `ProductProfitTableResponse` Pydantic model (85+ fields). Frontend should handle both shapes or use the dedicated `/sku-rollup` endpoint for rollup reads.

## Reality Check (2026-03-10)

Audit found and fixed:
- **P1 CRITICAL**: `use_rollup=True` crashed with 500 — response shape mismatch with `response_model=ProductProfitTableResponse`. Fixed with `JSONResponse` bypass + sort column mapping.
- **P2 HIGH**: DDL on every `/overview` read — added module-level `_METADATA_TABLE_VERIFIED` flag to run DDL check only once per process.
- **P3 HIGH**: Frontend `DataFreshnessInfo` TS type missing `rollup_covers` and `data_source` fields — added.
- **P4 MEDIUM**: `data_source: "rollup"` on Overview was misleading — changed to `"mixed"` (KPIs from rollup, loss_orders from live).
- **P5 MEDIUM**: Sort column `cm1_profit` silently fell back to `profit_pln` on rollup path — added `_SORT_LIVE_TO_ROLLUP` mapping.
- **P6 LOW**: `from datetime import datetime` moved to module-level import.
