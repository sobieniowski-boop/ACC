# FBA Ops

Status: active module, updated 2026-03-02.

## Scope

`FBA Ops` is an ACC module for Amazon FBA operations:
- overview / operational radar
- inventory health
- replenishment suggestions
- inbound shipments control tower
- aged / stranded actions
- bundles / launches / initiatives registers
- KPI scorecard

Routes:
- `/fba/overview`
- `/fba/inventory`
- `/fba/replenishment`
- `/fba/inbound`
- `/fba/aged-stranded`
- `/fba/bundles`
- `/fba/kpi-scorecard`

Backend API:
- `GET /api/v1/fba/overview`
- `GET /api/v1/fba/diagnostics/report-status`
- `GET /api/v1/fba/inventory`
- `GET /api/v1/fba/inventory/{sku}`
- `GET /api/v1/fba/replenishment/suggestions`
- `GET /api/v1/fba/inbound/shipments`
- `GET /api/v1/fba/inbound/shipments/{shipment_id}`
- `GET /api/v1/fba/aged`
- `GET /api/v1/fba/stranded`
- `GET /api/v1/fba/kpi/scorecard`
- `POST /api/v1/fba/jobs/run`
- CRUD for shipment plans, cases, launches, initiatives
- case timeline + comment edit/delete endpoints

Primary files:
- `apps/api/app/api/v1/fba_ops.py`
- `apps/api/app/schemas/fba_ops.py`
- `apps/api/app/services/fba_ops/service.py`
- `apps/web/src/pages/FbaOverview.tsx`
- `apps/web/src/pages/FbaInventory.tsx`
- `apps/web/src/pages/FbaInbound.tsx`
- `apps/web/src/pages/FbaAgedStranded.tsx`
- `apps/web/src/pages/FbaBundles.tsx`
- `apps/web/src/pages/FbaScorecard.tsx`

## Runtime Model

This module follows ACC runtime conventions:
- schema bootstrap via `ensure_fba_schema()`
- jobs scheduled by `APScheduler`
- manual job run remains available as operational fallback
- Redis/WebSocket patterns reused from ACC

Do not treat FBA Ops as a separate subsystem with Alembic-first or Celery-first assumptions.

## Data Sources

Working:
- SP-API Inbound API for shipments and lines
- SP-API FBA Inventory API (`/fba/inventory/v1/summaries`)
- SP-API Reports API for planning / stranded when Amazon returns usable reports
- ACC product tables + Netfox/PIM fallbacks for product naming in alerts

Important nuance:
- FBA connectivity is not globally broken.
- Problems are report-specific and marketplace-specific.

## Production Fallback Strategy

`sync_fba_inventory` now uses a production fallback chain:

1. try `GET_FBA_INVENTORY_PLANNING_DATA`
2. if planning report is in cooldown because of recent persistent `FATAL`, skip requesting it
3. fallback to FBA Inventory API summaries
4. for stranded:
   - try canonical stranded report
   - if `CANCELLED`, try last `DONE`
   - if unavailable, fallback to `unfulfillable` proxy from planning or inventory API

This is implemented because some marketplaces return persistent planning `FATAL` despite Inventory API working.

Current known production behavior:
- `IE` (`A28R8C7NBKEWEA`) and `SE` (`A2NODRKZP88ZB9`) use planning cooldown + Inventory API fallback
- stranded canonical report is still unstable and often returns `CANCELLED`

## Diagnostics

Diagnostics are stored in `dbo.acc_fba_report_diagnostic`.

Use:
- `GET /api/v1/fba/diagnostics/report-status`

What the diagnostics show:
- marketplace
- planning status / fetch mode
- inventory API fallback status
- stranded status / fallback source
- latest diagnostic payload

Frontend:
- `FBA Ops Overview` includes a simple diagnostics panel per marketplace.

## Alerts

FBA alerts now include:
- richer product context
- product name fallback from ACC import/product/order data and Netfox
- structured `detail_json`
- structured `context_json`
- drill-through into FBA inventory / inbound pages

Implemented alert families:
- stockout top SKU
- inbound stuck
- receiving variance
- stranded spike
- aging spike

## KPI Notes

Scorecard is live, but KPI quality depends on source quality.

Best-covered inputs:
- stockout / inventory coverage
- shipment and inbound operational metrics

Still source-sensitive:
- canonical stranded
- aged / excess where Amazon planning reports are unstable per marketplace

For IE/SE, diagnostics should be checked before using aged/excess comparisons as hard management truth.
