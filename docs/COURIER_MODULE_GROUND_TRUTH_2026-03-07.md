# Courier Module Ground Truth (Code + Runtime)

Cel: opis aktualnej koncepcji modulu kurierow na podstawie kodu i runtime, bez zalozenia ze starsza dokumentacja jest aktualna.

## 1) Zrodla prawdy (priorytet)

1. Kod wykonywany przez joby:
   - `apps/api/app/services/courier_order_universe_pipeline.py`
   - `apps/api/app/services/courier_order_universe_linking.py`
   - `apps/api/app/connectors/mssql/mssql_store.py` (dispatcher job_type)
2. Runtime DB:
   - `dbo.acc_al_jobs`
   - `apps/api/scripts/courier_order_universe_supervisor_checkpoint.json`
3. Testy kontraktowe API/uslug:
   - `apps/api/tests/test_api_courier.py`
   - `apps/api/tests/test_courier_order_universe_pipeline.py`
   - `apps/api/tests/test_courier_order_universe_linking.py`
4. Dokumenty opisowe (pomocniczo, nie jako jedyne zrodlo).

## 2) Faktyczny flow modulu (DHL/GLS)

### A. Uruchomienie

- API enqueue:
  - `POST /api/v1/courier/jobs/order-universe-linking`
  - przekazuje params do `enqueue_job(job_type='courier_order_universe_linking')`
- Wykonanie:
  - dispatcher w `mssql_store.py` uruchamia `run_courier_order_universe_pipeline(...)`
  - progress:
    - start `10%`
    - etapy per scope (`linking`, `aggregate+shadow`, `coverage`)
    - finish `100%`

### B. Linkowanie order-universe (kluczowy krok)

`backfill_order_links_order_universe(...)`:

- buduje universe zamowien MFN z:
  - `acc_order` + `acc_cache_bl_orders`
  - plus zamowienia distribution (`acc_bl_distribution_order_cache`, `acc_cache_dis_map`)
- buduje tokeny paczek z:
  - `acc_cache_packages` (`courier_package_nr`, `courier_inner_number`)
  - distribution package/order cache
  - GLS-specific:
    - fallback `note1` z `acc_shipment.source_payload_json`
    - mapa `acc_gls_bl_map`
- buduje tokeny shipmentu z:
  - `tracking_number`, `shipment_number`, `piece_id`, `cedex_number`
- laczy tokeny -> kandydaci -> `MERGE` do `acc_shipment_order_link`
- ustawia `is_primary=1` dla najlepszego linku per shipment

### C. Agregacja + shadow

Pipeline po linkowaniu uruchamia:

- DHL:
  - `aggregate_dhl_order_logistics(...)`
  - `build_dhl_logistics_shadow(...)`
- GLS:
  - `aggregate_gls_order_logistics(...)`
  - `build_gls_logistics_shadow(...)`

Wynik trafia do:
- `acc_order_logistics_fact`
- `acc_order_logistics_shadow`

### D. Coverage/Readiness

`_coverage_snapshot(...)` liczy per miesiac+carrier:
- `orders_universe`
- `orders_linked_primary`
- `orders_with_fact`
- `link_coverage_pct`
- `fact_coverage_pct`

`get_courier_readiness_snapshot(...)`:
- `GO` tylko gdy `orders_with_fact == orders_universe` dla scope
- `overall_go_no_go=GO` tylko gdy wszystkie scope sa `GO`

## 3) Orkiestracja produkcyjna

- Supervisor:
  - `apps/api/scripts/run_courier_order_universe_supervisor.py`
- Tryb:
  - sekwencyjnie po scope (`month + carrier`)
  - stale timeout + hard timeout + transient retries
- Checkpoint:
  - `apps/api/scripts/courier_order_universe_supervisor_checkpoint.json`
- Single-flight:
  - aktywny tylko jeden `courier_order_universe_linking` naraz

## 4) Co runtime potwierdza teraz (2026-03-07)

- Scope `2025-11 DHL`, `2025-11 GLS`, `2025-12 DHL` sa zakonczone w checkpoint.
- Aktywny jest `2025-12 GLS` (`aggregate+shadow`) w `acc_al_jobs`.
- Readiness globalnie nadal `NO_GO` (pokrycia faktow < 100% w scope'ach).

## 5) Znane niespojnosci techniczne

- W kodzie jest endpoint `GET /api/v1/courier/readiness`.
- Na aktualnie uruchomionym lokalnym runtime ten endpoint zwraca `404`.
- To wskazuje na rozjazd "kod na dysku" vs "kod uruchomionego procesu" (lub inny routing build/runtime).

## 6) Wniosek operacyjny

Koncepcja modulu jest odtwarzalna i juz odtworzona z kodu/runtime:
- order-universe-first linking,
- potem aggregate+shadow,
- potem coverage/readiness GO/NO_GO,
- wszystko sterowane przez `acc_al_jobs` i supervisor z checkpointem.

To jest obecnie pewniejsze niz opisowe notatki z poprzedniej sesji.
