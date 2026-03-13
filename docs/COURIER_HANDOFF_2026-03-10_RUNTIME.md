# Courier Runtime Handoff — 2026-03-10

## Scope

Tylko moduł kurierski ACC.

## Co zostało już zrobione

- Dodane nowe semantyki i relacje kurierskie:
  - `acc_order_courier_relation`
  - `acc_shipment_outcome_fact`
- Dodane modele / serwisy / endpointy:
  - `apps/api/app/services/courier_order_relations.py`
  - `apps/api/app/services/courier_shipment_semantics.py`
  - `apps/api/app/services/courier_order_universe_linking.py`
  - `apps/api/app/services/courier_order_universe_pipeline.py`
  - `apps/api/app/services/courier_monthly_kpi.py`
  - `apps/api/app/services/courier_readiness.py`
  - `apps/api/app/api/v1/courier.py`
- Dodana migracja:
  - `apps/api/migrations/versions/20260310_courier_relation_semantics.py`
- Testy lokalne wcześniej przeszły:
  - `apps/api/tests/test_courier_order_relations.py`
  - `apps/api/tests/test_courier_shipment_semantics.py`
  - `apps/api/tests/test_courier_order_universe_linking.py`
  - `apps/api/tests/test_courier_order_universe_pipeline.py`
  - `apps/api/tests/test_courier_monthly_kpi.py`
  - `apps/api/tests/test_api_courier.py`

## Runtime / operacyjnie

- Backend lokalny był postawiony na `127.0.0.1:8000`.
- Bezpieczny tryb do pracy nad kurierem:
  - `SCHEDULER_ENABLED=false`
  - `JOB_CANARY_MODE=off`
- Powód:
  - domyślnie joby kurierskie lecą do Celery (`JOB_CANARY_MODE=courier_fba`),
  - na tej maszynie nie było aktywnego workera,
  - więc zwykłe `POST /api/v1/courier/jobs/...` tylko odkładałoby zadania.
- Logi lokalnego backendu:
  - `apps/api/logs_backend_courier_runtime_local.out.log`
  - `apps/api/logs_backend_courier_runtime_local.err.log`

## Ważne obserwacje

- System jobów w DB jest zaśmiecony:
  - `GET /api/v1/jobs?status=running` pokazywał `99` jobów `running`.
- Był stary schedulerowy job blokujący single-flight dla monthly KPI:
  - `job_id = EDF90E1E-6D0D-4F32-93A6-69BAAF47FEC5`
  - `job_type = courier_refresh_monthly_kpis`
  - `status = running`
  - `started_at = 2026-03-10T05:10:25`
  - `last_heartbeat_at = 2026-03-10T05:44:16`
  - `lease_owner = NULL`
  - `lease_expires_at = NULL`
- To wygląda jak orphan / zombie job i blokuje `courier_refresh_monthly_kpis` przez `single-flight`.

## Ostatni blocker

Pilot `2026-01 / DHL` nie poszedł, bo `courier_refresh_order_relations` failuje na SQL formatowaniu:

- `job_id = 2E4F518D-210E-4860-B9C6-EC028A717A4A`
- `job_type = courier_refresh_order_relations`
- `status = failure`
- `error_message = more placeholders in sql than params available`

Reprodukcja bez kolejki też failuje:

- `refresh_courier_order_relations(months=['2026-01'], carriers=['DHL'])`

Traceback wskazuje:

- `apps/api/app/services/courier_order_relations.py`
- funkcja `_load_candidate_orders()`
- `cur.execute(...)`

## Co już poprawiłem w kodzie

Zmieniłem wzorce z podwójnego `%` na pojedynczy `%` w:

- `apps/api/app/services/courier_order_relations.py`
- `apps/api/app/services/courier_order_universe_pipeline.py`

To nie wystarczyło. Błąd nadal występuje.

## Najbardziej prawdopodobna przyczyna

Nowe predykaty kurierskie nadal używają `LIKE '%dhl%' / '%gls%'`.

Przy naszym adapterze `pymssql_compat`:

- `%` jest dodatkowo escapowane,
- potem `?` zamieniane na `%s`,
- a `pymssql` nadal źle interpretuje finalny SQL z literalnymi `%...%`.

Sprawdzony wydruk `CompatCursor._convert_sql(...)` dla `_load_candidate_orders()` pokazał:

- tylko `2` placeholdery `%s`,
- ale nadal `LIKE '%%dhl%%'` w finalnym SQL.

W praktyce to nadal kończy się:

- `ValueError: more placeholders in sql than params available`

## Najbliższy sensowny fix

Nie walczyć dalej z `LIKE '%...%'` w tych nowych query.

Zamienić predykaty kurierskie na `CHARINDEX`, np.:

```sql
CHARINDEX('dhl', LOWER(ISNULL(dco.delivery_method, ''))) > 0
```

i analogicznie dla:

- `delivery_package_module`
- `courier_code`
- `courier_other_name`
- `gls`

To trzeba zrobić co najmniej w:

- `apps/api/app/services/courier_order_relations.py`
- `apps/api/app/services/courier_order_universe_pipeline.py`

Prawdopodobnie warto też przejrzeć wszystkie nowe helpery `_carrier_predicate(...)` / `_distribution_order_carrier_predicate(...)` w świeżo dodanych plikach i wszędzie wyrzucić `%...%`.

## Bezpieczny następny krok po restarcie

1. Postawić backend lokalnie na `:8000` w trybie:
   - `SCHEDULER_ENABLED=false`
   - `JOB_CANARY_MODE=off`
2. Zamienić nowe predykaty `LIKE '%dhl%' / '%gls%'` na `CHARINDEX(...) > 0`.
3. Odpalić reprodukcję bez kolejki:
   - `refresh_courier_order_relations(months=['2026-01'], carriers=['DHL'])`
4. Jeśli przejdzie, puścić pilot API:
   - `POST /api/v1/courier/jobs/refresh-order-relations?months=2026-01&carriers=DHL`
   - `POST /api/v1/courier/jobs/order-universe-linking?months=2026-01&carriers=DHL&run_aggregate_shadow=false`
   - `POST /api/v1/courier/jobs/refresh-shipment-outcomes?months=2026-01&carriers=DHL`
5. Monthly KPI:
   - albo najpierw ręcznie rozwiązać zombie `EDF90E1E-...`,
   - albo na czas recovery uruchomić refresh snapshotu bez kolejki bezpośrednio z Pythona.

## Czego nie robić

- Nie używać szerokich resetów / truncate.
- Nie ruszać ciężko Netfoxa.
- Nie odpalać szerokich courier jobów dla wielu miesięcy naraz.
- Nie estymować jeszcze kosztów niewyfakturowanych.

## Istotne bezpieczeństwo

- Do tej pory nie było szerokich operacji na Netfox SQL.
- Problem dotyczy nowego ACC-side query / job runtime.
- Fail `courier_refresh_order_relations` zdarza się przed etapem insertów relacji, więc nie było sensownego postępu danych z tego joba.
