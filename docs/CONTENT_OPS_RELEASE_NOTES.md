# Content Ops Release Notes

Date: 2026-03-02

## 1) Gotowe

- Content Ops API i workflow P0/P1 backend:
  - tasks, versions, policy checker, diff/sync, assets, publish package/jobs, publish push, AI generate, onboard preflight, QA verify.
- Auth/RBAC hardening:
  - router `/api/v1/content/*` wymaga autoryzacji (`require_analyst`),
  - operacje write/ops wymagaja `require_ops`.
- Publish queue hardening:
  - usuniety in-process `threading.Thread`,
  - `mode=confirm` zapisuje job jako `queued`,
  - scheduler przetwarza kolejke cyklicznie (`queued -> running -> completed|partial|failed`).
- Deterministyczny preflight publish blocker:
  - blokada dla brakujacych required attrs,
  - blokada gdy PTD definition nie istnieje,
  - blokada gdy PTD required attrs sa puste,
  - brak bridge fallback dla `preflight_blocker_*`.
- Testy:
  - API contract + RBAC scenariusze (401/403),
  - testy service dla publish queue i PTD state.

## 2) Ryzyka i luki

- Brak pelnego E2E na realnym DB + SP-API + scheduler worker (obecne testy sa mocne, ale nadal glownie API/service-level).
- Natywny push nadal wymaga dalszego domkniecia mapowania atrybutow per product type/category dla pelnego EU coverage.
- Front Content Ops jest MVP i wymaga dalszego UX hardeningu dla pracy masowej (bulk workflow i wygodniejsza kolejka compliance).

## 3) Next

1. Uruchomic testy integracyjne E2E dla push flow (preview/confirm, retry, status transitions, error logs per SKU).
2. Rozszerzyc registry mapowania atrybutow per `marketplace + product_type` i domknac coverage raportami.
3. Domknac frontend produkcyjny: kalendarz release, pelny diff MAIN<->TARGET, bulk actions i queue triage.
4. Dodac runbook go-live (checklista operacyjna + rollback + SLA alerty dla taskow Content Ops).

## 4) Deployment checklist

- Konfiguracja:
  - zweryfikowac `SP_API_*` i uprawnienia konta na wszystkich aktywnych marketplace.
  - zweryfikowac `OPENAI_API_KEY` i model dla `/content/ai/generate`.
  - (opcjonalnie fallback) ustawic `PRODUCTONBOARD_*`, jesli bridge ma pozostac awaryjnie aktywny.

- Baza danych:
  - potwierdzic, ze `ensure_v2_schema()` utworzyl/uzupelnil tabele `acc_co_*`.
  - sprawdzic indeksy dla `acc_co_publish_jobs`, `acc_co_versions`, `acc_co_tasks`.

- Security:
  - potwierdzic, ze endpointy `/api/v1/content/*` sa za auth (brak anonimowego dostepu).
  - potwierdzic RBAC: `analyst` read-only, `ops+` write/publish.

- Scheduler i kolejka:
  - potwierdzic zarejestrowane zadanie `content-publish-queue-1m`.
  - wykonac test techniczny: push `mode=confirm` i obserwacja statusow `queued -> running -> completed|failed`.
  - potwierdzic, ze restart API nie gubi jobow w statusie `queued`.

- Publish safety gates:
  - sprawdzic, ze brak PTD definition i puste PTD required attrs blokuja confirm push.
  - sprawdzic, ze `preflight_blocker_*` nie jest obchodzony przez bridge fallback.

- Testy i observability:
  - uruchomic: `python -m pytest tests/test_api_content_ops.py tests/test_content_ops_service_publish.py`.
  - sprawdzic logi API/scheduler dla bledow push per marketplace/SKU.
  - ustawic alert operacyjny dla stale rosnacej kolejki `queued` oraz dla jobow `failed`.
