# SP-API Orders P0 (Profiles + Hash-Skip + Telemetry)

Data: 2026-03-05  
Zakres: etap P0 z planu optymalizacji call-volume i kosztów API.

## Co wdrożono

### 1) Profile syncu w `step_sync_orders`

W `app/services/order_pipeline.py` dodano profile:

- `core_sync`
  - `fetch_items=true`
  - pełna ścieżka pod finanse/marżę/order_line
- `ops_tracking`
  - `fetch_items=false`
  - lekki header-sync bez wywołań order items
- `pii_support`
  - `fetch_items=false`
  - profil rezerwowy (support/PII workflow)

Parametr:

- `step_sync_orders(..., sync_profile="core_sync")`

Domyślnie scheduler i pipeline używają `core_sync`.

### 2) Hash-skip order payload

Dodano fingerprint payloadu ordera (`sync_payload_hash`) w tabeli `acc_order`.

- Kolumna: `acc_order.sync_payload_hash`
- Hash nie zawiera `LastUpdateDate`, więc timestamp-only refresh nie wymusza reprocessingu.
- Dla istniejącego ordera:
  - hash bez zmian -> skip update line-items
  - hash zmieniony -> standardowy update + ewentualne `get_order_items`

Nowy licznik w wyniku `step_sync_orders`:

- `orders_hash_skipped`

### 3) Telemetria SP-API (call-volume, latency, status)

Dodano agregację dzienną:

- tabela: `dbo.acc_sp_api_usage_daily`
- klucz: `(usage_date, endpoint_name, http_method, marketplace_id, sync_profile, status_code)`
- metryki:
  - `calls_count`
  - `success_count`
  - `error_count`
  - `throttled_count`
  - `total_duration_ms`
  - `rows_returned`

Instrumentacja jest w bazowym kliencie:

- `app/connectors/amazon_sp_api/client.py`
- działa dla `GET` i `POST` (best-effort, bez wpływu na flow biznesowy)

Tagi endpointów Orders:

- `orders_v0.list_orders`
- `orders_v0.list_order_items`

### 4) Endpoint diagnostyczny

Dodano:

- `GET /api/v1/health/sp-api-usage?days=7&endpoint_name=&marketplace_id=&sync_profile=`

Zwraca:

- `totals` (sumaryczne calls/errors/throttles/avg latency)
- `rows` (szczegóły dzienne per endpoint/marketplace/profile)

## Smoke test (live)

Wykonano na DE:

1. `core_sync` (`max_results=5`, `use_watermark=false`)  
   wynik: `orders=5`, `updated_orders=5`, `orders_hash_skipped=0`, `items_updated=5`

2. drugi run `core_sync` na podobnym oknie  
   wynik: `orders=5`, `updated_orders=2`, `orders_hash_skipped=3`, `items_updated=2`

3. `ops_tracking`  
   wynik: `orders=5`, `updated_orders=0`, `orders_hash_skipped=5`, `items_updated=0`  
   (`fetch_items=false` zgodnie z profilem)

Telemetria po smoke:

- `orders_v0.list_orders`: profile `core_sync` i `ops_tracking` raportują osobno
- `orders_v0.list_order_items`: tylko `core_sync`

## Jak uruchomić profile manualnie

Przez `POST /api/v1/jobs/run`:

- `job_type=sync_orders`
- `params.sync_profile`:
  - `core_sync`
  - `ops_tracking`
  - `pii_support`

Przykład payload:

```json
{
  "job_type": "sync_orders",
  "marketplace_id": "A1PA6795UKMFR9",
  "params": {
    "days_back": 1,
    "sync_profile": "ops_tracking"
  }
}
```

## Ograniczenia P0 (świadomie)

- Orders nadal lecą przez `orders/v0` (bez `includedData` z v2026) — to jest kolejny etap migracji.
- Watermark state jest nadal jeden per marketplace (używany praktycznie przez `core_sync`).
- UI dashboard dla usage telemetry nie został jeszcze dodany (diagnostyka przez endpoint health + SQL/API).

## Następny krok (P1)

- Twarda migracja read path na Orders `v2026-01-01` dla profili z `includedData`.
- Oddzielny watermark per `marketplace + sync_profile`.
- UI diagnostyczne SP-API usage w Jobs/System.
