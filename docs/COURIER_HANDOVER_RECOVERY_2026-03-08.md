# Courier Handover Recovery (2026-03-08)

Cel: odtworzenie aktualnego stanu modulu przypisywania kosztow DHL/GLS do zamowien Amazon (ACC), po utracie historii czatu.

## 1) Szybka odpowiedz na pytanie o 100% w grudniu

Nie potwierdzam 100% na ten moment.

Najswiezszy artefakt produkcyjny:
- `apps/api/scripts/courier_prod_supervisor_checkpoint.json` (mtime: 2026-03-08 05:05)

Wynik dla `2025-12`:
- `DHL`: `orders_with_fact=4938 / orders_universe=5909` -> `fact_coverage_pct=83.57`
- `GLS`: `orders_with_fact=9889 / orders_universe=11080` -> `fact_coverage_pct=89.25`

Wniosek:
- Grudzien jest wysoko wzgledem poprzednich prob, ale nadal ponizej warunku DoD (`orders_with_fact == orders_universe`).

## 2) Co jest zrobione i potwierdzone

- Flow dziala zgodnie z order-universe-first:
  - linkowanie shipment -> order,
  - aggregate + shadow,
  - coverage snapshot.
- Dla GLS sa fallbacki:
  - `note1` z payloadu shipmentu,
  - `acc_gls_bl_map`.
- Supervisor i checkpointowanie dzialaja:
  - `apps/api/scripts/run_courier_order_universe_supervisor.py`
  - checkpointy JSON zapisujace coverage per `month+carrier`.

## 3) Najwazniejszy problem runtime (blokuje stabilny run)

W najnowszym runie scope'y koncza sie jako `failure` przez blad schematu:
- `Invalid column name 'holder_job_id'`

Artefakty:
- `apps/api/scripts/courier_prod_supervisor.log`
- `apps/api/scripts/courier_prod_supervisor_checkpoint.json`

To wskazuje na rozjazd kod <-> schema DB dla `acc_al_job_semaphore`:
- kod zaklada kolumne `holder_job_id` (mssql_store),
- DB na runtime jej nie ma.

## 4) Rozjazd miedzy checkpointami (dlaczego liczby sie roznia)

W repo sa rownolegle checkpointy z innych uruchomien i parametrow, np.:
- `apps/api/scripts/courier_order_universe_supervisor_checkpoint.json` (starszy handover, inny stan)
- `apps/api/scripts/courier_supervisor_full_20260307_193438.checkpoint.json`
- `apps/api/scripts/courier_supervisor_pass2_20260307_193438.checkpoint.json`
- `apps/api/scripts/courier_prod_supervisor_checkpoint.json` (najswiezszy)

Do biezacej decyzji operacyjnej przyjmowac ostatni plik produkcyjny z 2026-03-08 05:05.

## 5) Aktualny status celu

Cel biznesowy:
- 100% mapowania kosztu kuriera do zamowien Amazon dla scoped miesiecy.

Status:
- `2025-12 DHL`: 83.57% (NO_GO)
- `2025-12 GLS`: 89.25% (NO_GO)

Czy 100% jest realne?
- Potencjalnie tak po domknieciu brakujacego linkowania/faktow.
- Operacyjnie nieosiagniete na obecnych artefaktach.

## 6) Rekomendowane nastepne kroki (operacyjne)

1. Naprawic schema drift:
   - dodac `holder_job_id` do `dbo.acc_al_job_semaphore` zgodnie z oczekiwaniem kodu,
   - zweryfikowac indeks `IX_acc_al_job_semaphore_holder`.
2. Powtorzyc supervisor tylko dla scope grudniowych:
   - `2025-12 + DHL`
   - `2025-12 + GLS`
3. Po runie policzyc readiness/coverage i zapisac jeden canonical artifact.
4. Dopiero po tym podejmowac decyzje GO/NO_GO dla grudnia.

## 7) Zrodla prawdy (na teraz)

- Kod:
  - `apps/api/app/services/courier_order_universe_linking.py`
  - `apps/api/app/services/courier_order_universe_pipeline.py`
  - `apps/api/app/connectors/mssql/mssql_store.py`
- Artefakty runtime:
  - `apps/api/scripts/courier_prod_supervisor.log`
  - `apps/api/scripts/courier_prod_supervisor_checkpoint.json`
- Dokumenty pomocnicze:
  - `docs/COURIER_MODULE_GROUND_TRUTH_2026-03-07.md`
  - `docs/COURIER_PRODUCTION_CUTOVER_2026-03-07.md`
  - `docs/COURIER_HANDOVER_RECOVERY_2026-03-07.md`

## 8) Update 2026-03-09

Biezacy raport wykonanych prac jest w:
- `docs/COURIER_RECOVERY_REPORT_2026-03-09.md`
- ACC-wide plan dalszej migracji i cutoveru jest w:
  - `docs/COURIER_ACC_WIDE_HEALTH_PLAN_2026-03-09.md`

Stan po recovery:
- scope zamkniety dla miesiecy ksiegowo istotnych `2025-12` i `2026-01`,
- weighted actual-cost coverage dla `2025-12 + 2026-01` i obu carrierow: `32843 / 33190 = 98.95%`,
- `2026-02` nadal nie spelnia targetu, ale po catch-upie kosztow problem jest juz po stronie `missing_link`, a nie `linked_but_no_cost`.

Najwazniejsze dopiski do ground truth:
- doszedl sync read-only endpoint `GET /api/v1/courier/link-gap-diagnostics`,
- GLS linking dostal dodatkowe tokeny shipmentowe (`tracking_number`, `shipment_number`, `piece_id`),
- snapshot `dbo.acc_courier_monthly_kpi_snapshot` ma juz zmaterializowane wiersze dla `2025-12`, `2026-01`, `2026-02`,
- Netfox nie byl dotykany w tej turze; wszystkie write operacje byly ograniczone do ACC.
