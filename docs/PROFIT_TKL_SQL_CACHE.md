# Profit v2: TKL SQL Cache

## Cel
Zmniejszenie cold-startu `GET /api/v1/profit/v2/what-if` po restarcie backendu.

Wcześniej pierwszy request czytał duże pliki XLSX z `N:\Analityka\TKL`.
Teraz dane TKL są trwale cache’owane w Azure SQL i odczytywane z bazy, jeśli podpis źródeł się nie zmienił.

## Co wdrożono
- Nowe tabele:
  - `dbo.acc_tkl_cache_meta`
  - `dbo.acc_tkl_cache_rows`
- Podpis źródeł (path + mtime + size) dla:
  - `00. Wyliczanie Kurier*2.0.xlsx`
  - `00. Tabela Koszt*Logistycznych.xlsx`
- Tryby ładowania:
  1. `sql_cache_signature_match` — szybki odczyt z SQL.
  2. `file_parse_and_sql_refresh` — parse XLSX + odświeżenie SQL cache.
  3. `sql_cache_stale_fallback` — fallback do ostatniego SQL cache, gdy pliki źródłowe są niedostępne.
- In-memory TTL nadal działa (1h), ale źródłem po cold-starcie jest SQL cache.

## Job manualny
Dodany job:
- `sync_tkl_cache`

Uruchomienie:
- `POST /api/v1/jobs/run` z `{"job_type":"sync_tkl_cache","params":{"force":true}}`
- albo z UI `System -> Jobs -> Sync TKL SQL Cache`

## Startup
Na starcie aplikacji wywoływane jest:
- `ensure_profit_tkl_cache_schema()`

To tworzy tylko schemat tabel cache (bez ciężkiego parsowania plików).

## Scheduler
Automatyczne odświeżenie:
- codziennie `01:40` (`scheduler._sync_tkl_cache`)

## Logi techniczne
Kluczowe eventy:
- `profit_engine.tkl_loaded`
- `profit_engine.tkl_cache_sql_read_error`
- `profit_engine.tkl_cache_sql_write_error`
- `profit_engine.tkl_cache_schema_error`
