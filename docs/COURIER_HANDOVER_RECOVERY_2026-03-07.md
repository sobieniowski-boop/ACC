# Courier Handover Recovery (2026-03-07)

Cel: szybkie odtworzenie stanu prac po utracie historii agenta dla modulu liczenia kosztow kuriera (ACC).

## 1) Aktywny job (stan na 2026-03-07 15:33 CET)

- Process:
  - `python.exe` PID `11424`
  - command:
    - `python scripts/run_courier_order_universe_supervisor.py --months 2025-11 2025-12 2026-01 --carriers DHL GLS --limit-orders 3000000 --stale-timeout-sec 900 --hard-timeout-sec 7200 --transient-retries 2`
- Active DB job:
  - `job_type`: `courier_order_universe_linking`
  - `job_id`: `8a34b2e5-e8b9-4691-a790-669c42211fa3`
  - `status`: `running`
  - `progress_pct`: `36`
  - `progress_message`: `2025-12 DHL: aggregate+shadow`
  - `params`: `months=[2025-12], carriers=[DHL], run_aggregate_shadow=true, limit_orders=3000000`

## 2) Potwierdzone zakonczone scope'y (checkpoint)

Plik: `apps/api/scripts/courier_order_universe_supervisor_checkpoint.json`

- `2025-11 + DHL`:
  - completed, `orders_with_fact=1608`
  - `orders_universe=9788`
  - `link_coverage_pct=16.77`
  - `fact_coverage_pct=16.43`
- `2025-11 + GLS`:
  - completed, `orders_with_fact=3583`
  - `orders_universe=16999`
  - `link_coverage_pct=21.31`
  - `fact_coverage_pct=21.08`

## 3) Ostatnia os czasu jobow (acc_al_jobs)

Najnowsze wpisy `courier_order_universe_linking`:

- `8a34b2e5-e8b9-4691-a790-669c42211fa3`: `running` (2025-12 DHL, aggregate+shadow)
- `e33fd4ed-18c3-434b-b84a-709d7bee5838`: `failure` (Adaptive Server connection timed out / DBPROCESS is dead)
- `e47c6a82-320e-49cc-9792-00241bdbef51`: `completed` (2025-11 GLS)
- `d82a1d13-15ac-46d9-be49-5769da9335fd`: `completed` (2025-11 DHL)
- Starsze failury przed stabilizacja:
  - timeout/connection reset,
  - `more placeholders in sql than params available`,
  - `Invalid column name 'courier_code'`,
  - manualne stop/restarty podczas hotfixow.

Wniosek: biezacy supervisor to kontynuacja po seriach retry i timeout-hardening.

## 4) Gdzie jest aktualna dokumentacja merytoryczna

- `docs/COURIER_PRODUCTION_CUTOVER_2026-03-07.md` (operacyjny cutover i DoD)
- `docs/COURIER_JOB_ARCHITECTURE_2026-03-07.md` (architektura jobow)
- `docs/ACC_JOB_REGISTRY_2026-03-07.md` (rejestr jobow)
- `COPILOT_CONTEXT.md` (sekcja shipping/courier context)
- `DEVELOPER_GUIDE.md` (pipeline steps, courier step 5.95)

## 5) Monitoring teraz

- Runtime process check:
  - `Get-CimInstance Win32_Process | ? { $_.CommandLine -like '*run_courier_order_universe_supervisor.py*' }`
- Checkpoint:
  - `apps/api/scripts/courier_order_universe_supervisor_checkpoint.json`
- API:
  - `GET /api/v1/jobs`
  - `GET /api/v1/courier/readiness?months=2025-11&months=2025-12&months=2026-01&carriers=DHL&carriers=GLS`

## 6) Najwazniejsze ryzyko operacyjne

- Timeouts na SQL przy ciezkich etapach (`aggregate+shadow`) nadal moga wystepowac.
- Supervisor ma retry transient (`--transient-retries 2`), ale po kolejnej failurze trzeba wznowic tylko brakujacy scope.

## 7) Live update (2026-03-07 15:42 CET)

- `2025-12 DHL` zostal zakonczony:
  - job `8A34B2E5-E8B9-4691-A790-669C42211FA3` -> `completed`
  - `orders_with_fact=4936`
- Aktualnie aktywny scope:
  - job `244D72F4-EA09-4641-91F0-94F4B3E4D8F3`
  - `status=running`
  - `progress_message=2025-12 GLS: linking`

Snapshot readiness (`months=2025-11,2025-12,2026-01`, `carriers=DHL,GLS`):

- `overall_go_no_go=NO_GO`
- `scopes_go=0 / scopes_total=6`
- Matrix:
  - `2025-11 DHL`: `NO_GO` (`fact_coverage_pct=16.77`)
  - `2025-11 GLS`: `NO_GO` (`fact_coverage_pct=21.08`)
  - `2025-12 DHL`: `NO_GO` (`fact_coverage_pct=83.53`)
  - `2025-12 GLS`: `NO_GO` (`fact_coverage_pct=13.68`)
  - `2026-01 DHL`: `NO_GO` (`fact_coverage_pct=12.23`)
  - `2026-01 GLS`: `NO_GO` (`fact_coverage_pct=0.01`)

Uwaga runtime:
- Endpoint `/api/v1/courier/readiness` jest obecny w kodzie, ale aktualnie uruchomiony backend lokalny zwraca `404` dla tej trasy.
- Snapshot readiness zostal policzony bezposrednio przez `app.services.courier_readiness.get_courier_readiness_snapshot(...)`.

Diagnoza 404 (potwierdzona):
- Proces backendu (`uvicorn`, PID 21232) start: `14:58`.
- Plik `apps/api/app/api/v1/courier.py` (z endpointem readiness) mtime: `15:04`.
- Wniosek: runtime chodzi na starszym kodzie zaladowanym przed dodaniem trasy.

Bezpieczna naprawa (po zakonczeniu runow kuriera):
1. Poczekac az aktywne `courier_order_universe_linking` przejda w `completed/failure` (brak `running`).
2. Zrestartowac backend API (`uvicorn`) i potwierdzic:
   - `GET /openapi.json` zawiera `/api/v1/courier/readiness`
   - `GET /api/v1/courier/readiness?...` zwraca `200`
