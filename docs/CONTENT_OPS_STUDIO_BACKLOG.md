# Content Ops Studio - Technical Backlog (P0/P1)

Scope: addon to existing ACC stack (`React+TS`, `FastAPI`, `MSSQL/Postgres-compatible model`, `Redis/Celery jobs`, `GPT-5.2`).

Design principle: reuse existing `productonboard` generation/QA where possible, and build missing operational layer in `apps/api/app/api/v1` + `apps/web/src`.

## 1) Target architecture in current ACC

- API namespace: `/api/v1/content/*` (new router: `apps/api/app/api/v1/content_ops.py`)
- Service layer: `app/services/content_ops.py` (+ helper modules `content_policy.py`, `content_ai.py`, `content_publish.py`)
- Schemas: `app/schemas/content_ops.py`
- DB bootstrap: extend `ensure_v2_schema()` with `acc_co_*` tables
- Jobs: register Celery + scheduler tasks in existing job framework (`app/worker.py`, `app/scheduler.py`)
- Frontend routes:
  - `/content/dashboard`
  - `/content/editor/:sku`
  - `/content/ai`
  - `/content/compliance`
  - `/content/assets`
  - `/content/publish`
  - `/content/impact`

## 2) Data model (MVP-first)

Tables (prefix `acc_co_`):

1. `acc_co_tasks`
- `id`, `type`, `sku`, `asin`, `marketplace_id`, `priority`, `owner`, `due_date`, `status`, `tags_json`, `source_page`, `created_by`, `created_at`, `updated_at`

2. `acc_co_versions`
- `id`, `sku`, `asin`, `marketplace_id`, `version_no`, `status`, `fields_json`, `compliance_notes`, `created_by`, `created_at`, `approved_by`, `approved_at`, `published_at`, `parent_version_id`

3. `acc_co_policy_rules`
- `id`, `name`, `pattern`, `severity`, `applies_to_json`, `is_active`, `created_by`, `created_at`

4. `acc_co_policy_checks`
- `id`, `version_id`, `results_json`, `passed`, `checked_at`, `checker_version`

5. `acc_co_assets`
- `id`, `filename`, `mime`, `content_hash`, `storage_path`, `metadata_json`, `uploaded_by`, `uploaded_at`, `status`

6. `acc_co_asset_links`
- `id`, `asset_id`, `sku`, `asin`, `marketplace_id`, `role`, `status`, `created_at`

7. `acc_co_publish_jobs`
- `id`, `job_type`, `marketplaces_json`, `selection_mode`, `status`, `progress_pct`, `log_json`, `created_by`, `created_at`, `finished_at`

8. `acc_co_impact_snapshots`
- `id`, `sku`, `asin`, `marketplace_id`, `version_id`, `range_days`, `metrics_json`, `created_at`

9. `acc_co_prompt_templates`
- `id`, `name`, `mode`, `template_text`, `version_no`, `is_active`, `created_by`, `created_at`

10. `acc_co_ai_cache`
- `id`, `context_hash`, `mode`, `marketplace_id`, `input_json`, `output_json`, `model`, `created_at`, `expires_at`

Indexes (minimum):
- `tasks(status, owner, due_date)`
- `versions(sku, marketplace_id, status, created_at)`
- `policy_checks(version_id, checked_at desc)`
- `assets(content_hash unique)`
- `asset_links(sku, marketplace_id, role)`
- `impact_snapshots(sku, marketplace_id, range_days, created_at desc)`

## 3) API contract (P0 then P1)

P0 endpoints:

1. `GET /api/v1/content/tasks`
- filters: `status`, `owner`, `marketplace_id`, `type`, `priority`, `sku_search`, `page`, `page_size`

2. `POST /api/v1/content/tasks`
- create task with owner auto-assignment fallback (reuse rule concept from product tasks)

3. `PATCH /api/v1/content/tasks/{task_id}`
- update `status`, `owner`, `due_date`, `priority`, `note`

4. `GET /api/v1/content/{sku}/{marketplace_id}/versions`

5. `POST /api/v1/content/{sku}/{marketplace_id}/versions`
- create draft version from latest approved/published

6. `PUT /api/v1/content/versions/{version_id}`
- update editable fields (`title`, `bullets`, `description`, `keywords`, `attributes_json`, `aplus_json`)

7. `POST /api/v1/content/versions/{version_id}/submit-review`

8. `POST /api/v1/content/versions/{version_id}/approve`

9. `POST /api/v1/content/policy/check`
- body: `version_id`
- returns lint findings + pass/fail

10. `GET /api/v1/content/policy/rules`

11. `PUT /api/v1/content/policy/rules`
- batch upsert rule set

12. `POST /api/v1/content/assets/upload`

13. `GET /api/v1/content/assets`
- filters: `sku`, `tag`, `role`, `status`

14. `POST /api/v1/content/assets/{asset_id}/link`

15. `POST /api/v1/content/publish/package`
- `marketplaces[]`, `selection=approved|draft`, `format=xlsx|csv`
- writes `acc_co_publish_jobs` and returns blob/job metadata

16. `GET /api/v1/content/publish/jobs`

17. `GET /api/v1/content/{sku}/diff`
- query: `main=DE&target=FR&version_main=&version_target=`

18. `POST /api/v1/content/{sku}/sync`
- body: `fields[]`, `from_market`, `to_markets[]`, `overwrite_mode`

19. `POST /api/v1/content/ai/generate`
- modes: `new_listing`, `improve`, `localize`
- guardrails enforced before/after model call

P1 endpoints:

1. `POST /api/v1/content/versions/{version_id}/rollback`
2. `POST /api/v1/content/publish/push`
3. `GET /api/v1/content/impact`
4. `POST /api/v1/content/impact/recompute`
5. `GET /api/v1/content/prompts`
6. `POST /api/v1/content/prompts`
7. `POST /api/v1/content/ai/improve`
8. `POST /api/v1/content/ai/localize`

## 4) Backlog P0/P1

| Priorytet | Zadanie | Wartość biznesowa | Wysiłek |
|---|---|---|---|
| P0 | Schema bootstrap `acc_co_*` + indeksy + migracja idempotentna | Fundament pod workflow i audyt zmian | M |
| P0 | Router `content_ops.py` + CRUD tasks + filtry/paginacja | Operacyjny backlog contentu zamiast Excela | M |
| P0 | Versions API (draft/review/approve) + status machine | Kontrola jakości i odpowiedzialności per rynek | M |
| P0 | Policy rules + checker (regex/lint + severity) | Redukcja ryzyka blokad/suppression na Amazon | M |
| P0 | Diff + sync MAIN->TARGET | Skalowanie DE template na pozostałe EU bez ręcznej roboty | M |
| P0 | Asset library MVP (upload/link/status) + image validation | Spójność mediów i mniej błędów przy publikacji | M |
| P0 | Publish package (CSV/XLSX) + job history | Powtarzalny release contentu i audyt co wyszło | S |
| P0 | AI generate/localize/improve z guardrails + cache hash | Krótszy lead time tworzenia listingu | M |
| P0 | Frontend: 5 ekranów MVP (`dashboard/editor/ai/compliance/publish`) | Kompletny flow daily operations | L |
| P1 | Push to Amazon via SP-API feeds + job status polling | Zamknięcie pętli publikacji bez ręcznego uploadu | L |
| P1 | Rollback endpoint + UI one-click restore | Ograniczenie strat po złej zmianie contentu | M |
| P1 | Impact module 7/14/30 + negative impact alert | Decyzje based on outcome, nie tylko output | M |
| P1 | Prompt registry (versioned templates, A/B prompt test) | Stabilność jakości AI i mniejsze drift errors | M |
| P1 | Compliance queue SLA + auto-owner rules | Lepsza egzekucja i krótszy czas zamknięcia flag | S |

## 5) Acceptance criteria (ready for sprint)

### P0-1: DB schema
- Given clean DB, when app starts, then all `acc_co_*` tables exist.
- Given existing DB, when app starts, then bootstrap is idempotent and does not drop data.
- Given 100k versions, list queries remain under 500ms p95 for indexed filters.

### P0-2: Tasks API
- `GET /content/tasks` supports combined filters and returns `total/page/pages/items`.
- `POST /content/tasks` validates enum values and writes audit fields.
- `PATCH /content/tasks/{id}` rejects invalid transitions with `400`.

### P0-3: Versions workflow
- New draft increments `version_no` per `sku+marketplace`.
- Status transitions allowed only: `draft -> review -> approved -> published`; rollback in P1.
- Every edit stores `updated_at`, `updated_by`, and immutable previous snapshot.

### P0-4: Policy checker
- Rule engine evaluates title/bullets/description/keywords.
- Response contains `passed`, `severity counts`, `findings[]` with exact field and snippet.
- Critical findings block approve endpoint with clear error payload.

### P0-5: Diff + sync
- Diff returns structured JSON (`added/removed/changed`) per field.
- Sync supports selected fields and target markets.
- Sync writes new draft versions, never overwrites approved directly.

### P0-6: Assets
- Upload deduplicates by `content_hash`.
- Image validator flags Amazon constraints (minimum size, aspect/background hints when available).
- Asset can be linked to multiple marketplaces and roles.

### P0-7: Publish package
- Export respects filters (`marketplaces`, `selection`) and format (`csv/xlsx`).
- Job record is created with `status/progress/log`.
- Download filename includes date/time + marketplaces.

### P0-8: AI engine + guardrails
- Request with same `context_hash` returns cached result within TTL.
- Guardrails remove/block disallowed claims before response is saved.
- Endpoint returns `model`, `cache_hit`, `policy_flags`.

### P0-9: Frontend MVP
- Content dashboard shows backlog with owner/status/priority filters.
- Editor supports per-market tabs, version history, and policy check action.
- Compliance center displays failed checks queue and allows recheck.
- Publish center shows package jobs and downloadable artifacts.

## 6) Telemetry and alerting

Mandatory events (`event_name`, `payload`):

1. `content.task.created`
- `task_id`, `type`, `sku`, `marketplace_id`, `priority`, `owner`, `source`

2. `content.version.created`
- `version_id`, `sku`, `marketplace_id`, `status`, `base_version_id`

3. `content.version.status_changed`
- `version_id`, `from_status`, `to_status`, `actor`

4. `content.policy.checked`
- `version_id`, `passed`, `critical_count`, `major_count`, `rule_set_version`, `duration_ms`

5. `content.ai.generated`
- `mode`, `sku`, `from_market`, `to_market`, `model`, `cache_hit`, `tokens_in`, `tokens_out`, `duration_ms`, `blocked_claims_count`

6. `content.sync.executed`
- `sku`, `from_market`, `to_markets_count`, `fields`, `drafts_created`

7. `content.publish.package_created`
- `job_id`, `selection`, `marketplaces`, `format`, `items_count`, `duration_ms`

8. `content.publish.push_completed` (P1)
- `job_id`, `success_count`, `failed_count`, `duration_ms`

9. `content.impact.snapshot_created` (P1)
- `sku`, `marketplace_id`, `version_id`, `range_days`, `sessions`, `cvr`, `sales`

Alerts (wire to existing alerts engine):

- `P1`: policy critical findings on approved candidate.
- `P1`: publish job failed for any marketplace.
- `P2`: task SLA breach (`open > 48h`, `investigating > 72h`) for `content` type.
- `P2`: negative impact after publish (`CVR delta <= -15%` with minimum sessions threshold).

## 7) Delivery plan (2 sprints)

Sprint 1 (P0 core):
- DB schema + API skeleton + tasks + versions + policy checker + diff/sync.
- Frontend dashboard/editor/compliance MVP.

Sprint 2 (P0 completion + P1 start):
- Asset library + publish package + AI cache/guardrails hardening.
- Publish center + job history + initial impact snapshot pipeline.

## 8) Reuse map from existing ProductOnboard

Reuse directly:
- AI generation primitives and market handling.
- Existing QA/compliance heuristics as base rule set.
- SP-API submission utilities (for P1 push).

Do not reuse as-is:
- Session-based state model for operational content lifecycle.
- Wizard-first UI flow (replace with persistent task/version workflow).

## 9) Implementation Notes (Codex, 2026-03-02)

What is implemented now:
- Backend scaffold registered in API router:
  - `apps/api/app/api/v1/content_ops.py`
  - `apps/api/app/schemas/content_ops.py`
  - `apps/api/app/services/content_ops.py`
- Story `COS-P0-01` delivered:
  - `ensure_v2_schema()` extended with `acc_co_*` tables and indexes for P0 base.
- Story `COS-P0-02` delivered:
  - Working task endpoints:
    - `GET /api/v1/content/tasks`
    - `POST /api/v1/content/tasks`
    - `PATCH /api/v1/content/tasks/{task_id}`
  - Validation included:
    - enum checks (`type`, `priority`, `status`)
    - guarded status transitions (`open -> investigating -> resolved`)
    - pagination and filter handling
  - Owner auto-assignment fallback:
    - uses existing `acc_al_task_owner_rules` matching flow (best-effort by marketplace/brand/task_type).
- Story `COS-P0-03` delivered:
  - Working version workflow endpoints:
    - `GET /api/v1/content/{sku}/{marketplace_id}/versions`
    - `POST /api/v1/content/{sku}/{marketplace_id}/versions`
    - `PUT /api/v1/content/versions/{version_id}`
    - `POST /api/v1/content/versions/{version_id}/submit-review`
    - `POST /api/v1/content/versions/{version_id}/approve`
  - Status transitions implemented:
    - edit only for `draft`
    - `submit-review` only from `draft`
    - `approve` only from `review`
  - Approve guard:
    - approval is blocked when latest policy check for version has critical findings.
- Story `COS-P0-04` delivered:
  - Working policy endpoints:
    - `GET /api/v1/content/policy/rules`
    - `PUT /api/v1/content/policy/rules`
    - `POST /api/v1/content/policy/check`
  - Checker behavior:
    - evaluates active regex rules over `title`, `bullets`, `description`, `keywords` (or rule-defined fields)
    - stores check output in `acc_co_policy_checks`
    - returns counts (`critical/major/minor`) and findings with snippets
  - Version flow integration:
    - `approve` enforces guard on latest policy check and blocks when critical findings exist.
- API tests added for `COS-P0-02/03`:
  - file: `apps/api/tests/test_api_content_ops.py`
  - covers: `GET/POST/PATCH tasks` + versions endpoints (`list/create/update/submit-review/approve`).
- Story `COS-P0-05` delivered:
  - Working endpoints:
    - `GET /api/v1/content/{sku}/diff`
    - `POST /api/v1/content/{sku}/sync`
  - Diff behavior:
    - compares selected version pair (or latest MAIN/TARGET), returns per-field `added/removed/changed/same`.
  - Sync behavior:
    - supports `overwrite_mode = missing_only | force`
    - creates new `draft` versions on target markets only when changes exist
    - keeps source untouched and reports `drafts_created/skipped/warnings`.
- API tests updated for `COS-P0-05`:
  - same test file `apps/api/tests/test_api_content_ops.py` now includes diff/sync endpoint coverage.
- Story `COS-P0-06` delivered:
  - Working asset endpoints:
    - `POST /api/v1/content/assets/upload`
    - `GET /api/v1/content/assets`
    - `POST /api/v1/content/assets/{asset_id}/link`
  - Upload behavior:
    - validates base64 payload and mime whitelist
    - computes `sha256` content hash
    - deduplicates by `content_hash` against `acc_co_assets`
    - persists metadata and logical `storage_path`
  - List behavior:
    - supports filters `sku`, `tag`, `role`, `status` + pagination
  - Link behavior:
    - validates asset existence and persists mapping to SKU/ASIN/marketplace/role.
- API tests updated for `COS-P0-06`:
  - same test file `apps/api/tests/test_api_content_ops.py` now includes assets endpoint coverage.
- Story `COS-P0-07` delivered:
  - Working publish endpoints:
    - `POST /api/v1/content/publish/package`
    - `GET /api/v1/content/publish/jobs`
  - Package behavior (P0):
    - creates job record (`running -> completed`)
    - computes package scope summary from `acc_co_versions` by `selection`, `marketplaces`, optional `sku_filter`
    - stores summary in `log_json` and returns placeholder `artifact_url`
  - Jobs history behavior:
    - paginated list by `created_at DESC`.
- API tests updated for `COS-P0-07`:
  - same test file `apps/api/tests/test_api_content_ops.py` now includes publish endpoints coverage.
- Story `COS-P0-08` delivered:
  - Working endpoint:
    - `POST /api/v1/content/ai/generate`
  - AI behavior:
    - computes deterministic `context_hash` from request context
    - reads/writes cache in `acc_co_ai_cache` (`cache_hit` response supported)
    - uses OpenAI generation path when configured; falls back to deterministic local template when unavailable
    - applies guardrails before response/cache write:
      - PII redaction (email/phone patterns)
      - removal of banned claims/superlatives
      - title/keywords length caps.
- API tests updated for `COS-P0-08`:
  - same test file `apps/api/tests/test_api_content_ops.py` now includes AI generate endpoint coverage.
- Story `COS-P0-09` delivered:
  - New onboarding-preflight endpoint:
    - `POST /api/v1/content/onboard/preflight`
  - Behavior:
    - computes per-SKU PIM readiness score from `acc_product`
    - checks family coverage on target markets via `global_family*` / `family_coverage_cache`
    - returns blockers/warnings/recommended actions
    - optional `auto_create_tasks=true` creates deduplicated `acc_co_tasks` (create listing / expand marketplaces)
  - This closes first gap vs ProductOnboard for "time-to-listing readiness" inside ACC.
- Story `COS-P0-10` delivered:
  - New deep QA endpoint:
    - `POST /api/v1/content/qa/verify`
  - Behavior:
    - evaluates content quality with scoring + findings (`language/accuracy/seo/conversion/compliance`)
    - detects banned claims, language leaks and basic PIM consistency drift
    - returns decision status: `passed | needs_revision | rejected`
  - This narrows the QA gap vs ProductOnboard `verify_product_content` before full AI-QA parity.
- Story `COS-P0-11` delivered:
  - `onboard/preflight` now has hard restrictions/catalog gate via native ACC SP-API checks.
  - Bridge is configurable via env:
    - `PRODUCTONBOARD_BASE_URL`
    - `PRODUCTONBOARD_API_KEY`
    - `PRODUCTONBOARD_RESTRICTIONS_PATH`
    - `PRODUCTONBOARD_CATALOG_BY_EAN_PATH`
  - Blocking conditions:
    - SP-API unavailable
    - catalog check failed / no identifier
    - restrictions failed / listing blocked / approval required
  - New native endpoints for frontend:
    - `GET /api/v1/content/onboard/catalog/search-by-ean`
    - `GET /api/v1/content/onboard/restrictions/check`
- Story `COS-P0-12` delivered:
  - New endpoint:
    - `POST /api/v1/content/publish/push`
  - Supports:
    - `mode=preview` (candidate preview by marketplace)
    - `mode=confirm` (native SP-API push attempt per marketplace, bridge fallback optional)
  - Persists job in `acc_co_publish_jobs` with `job_type=publish_push` and detailed `per_marketplace` status in `log_json`.
- Story `COS-P0-13` delivered:
  - Frontend route and screen wired:
    - `/content-ops` in `apps/web/src/App.tsx`
    - Sidebar entry "Content Ops"
    - page: `apps/web/src/pages/ContentOps.tsx`
  - UI supports:
    - onboard preflight execution
    - ad-hoc catalog by EAN check
    - ad-hoc restrictions check
    - publish push preview/confirm
    - publish jobs list with per-job status badges
- Story `COS-P0-14` delivered:
  - Native push `productType` is now resolved per SKU/category instead of fixed `"PRODUCT"`.
  - Resolution order:
    1. explicit `attributes_json.product_type` / `amazon_product_type`
    2. `acc_product.category` / `subcategory` hints
    3. title/category keyword heuristic map
    4. fallback `"HOME"`
  - Push job log now includes resolved product type set per marketplace (`log_json.per_marketplace.*.product_types`).
- Story `COS-P0-15` delivered:
  - Frontend rollout expanded beyond single `/content-ops`:
    - `/content/dashboard`
    - `/content/editor`
    - `/content/compliance`
    - `/content/assets`
    - `/content/publish`
  - Sidebar now exposes direct navigation to each Content Ops screen for daily workflow.
- Story `COS-P0-16` delivered:
  - Native push flow fixed to run without bridge dependency:
    - removed hard pre-check requiring ProductOnboard bridge before native push
    - bridge remains fallback only when native push fails
  - Push preview now returns resolver diagnostics per SKU (`product_type`, `required_attrs`, `resolver_source`).
  - `_load_push_candidates` now correctly fetches `brand` for mapping rules.
  - Native push now serializes additional `attributes_json` fields (not only required attrs), with required-attr enforcement preserved.
- Story `COS-P0-17` delivered:
  - API tests extended for product type mapping endpoints:
    - `GET /api/v1/content/publish/product-type-mappings`
    - `PUT /api/v1/content/publish/product-type-mappings`
- Story `COS-P1-01` delivered:
  - Deterministyczny foundation pod native push:
    - nowa tabela cache `acc_co_product_type_defs` (per marketplace + product_type)
    - nowe endpointy:
      - `GET /api/v1/content/publish/product-type-definitions`
      - `POST /api/v1/content/publish/product-type-definitions/refresh`
    - publish resolver używa teraz required attrs z definicji, gdy mapping rule ich nie podaje.
- Story `COS-P1-02` delivered:
  - Publish push `mode=confirm` działa asynchronicznie (queued -> running -> completed/partial/failed).
  - Job status i szczegóły per marketplace/SKU pozostają w `acc_co_publish_jobs.log_json`.
- Story `COS-P1-03` delivered:
  - UX/workflow backend:
    - `POST /api/v1/content/tasks/bulk-update`
    - `GET /api/v1/content/compliance/queue`
    - `GET /api/v1/content/impact` (7/14/30 before/after)
    - `GET /api/v1/content/data-quality`
- Story `COS-P1-04` delivered:
  - Production hardening:
    - SLA alert evaluation rozszerzone o `acc_co_tasks` (content tasks), nie tylko `acc_al_product_tasks`.
    - write-endpointy content policy/mappings/bulk update mają gate `require_ops`.
- Story `COS-P1-05` delivered:
  - Attribute Mapping Registry (per product_type / marketplace):
    - nowa tabela `acc_co_attribute_map`
    - endpointy:
      - `GET /api/v1/content/publish/attribute-mappings`
      - `PUT /api/v1/content/publish/attribute-mappings`
  - Push resolver używa registry do automatycznego mapowania `source_field -> target_attribute` przed walidacją required attrs.
- Story `COS-P1-06` delivered:
  - Twardy publish preflight blocker:
    - missing required attrs z PTD blokują push SKU (`preflight_blocker_missing_required`)
    - brak fallbacku bridge dla preflight-blocked SKU
    - preview zwraca `missing_required_attrs` i `blocked_count`.
- Story `COS-P1-07` delivered:
  - Coverage endpoint pod rollout kategoriami:
    - `GET /api/v1/content/publish/coverage?marketplaces=...`
    - metryki: `total_candidates`, `fully_covered`, `coverage_pct`, `missing_required_top`.

Open technical notes:
- Remaining gap to ProductOnboard:
  - final direct SP-API native push from ACC (without bridge dependency) is still P1.
- `content` endpoints currently use `settings.DEFAULT_ACTOR` as `created_by` fallback.
- Next incremental step should be frontend wiring for Content Ops screens on top of completed P0 backend.

## 10) Hardening Update (Codex, 2026-03-02)

This section documents production-risk fixes implemented after initial P0/P1 scaffold.

- Auth/RBAC lock-down:
  - `apps/api/app/api/v1/content_ops.py` now has router-level dependency `require_analyst`.
  - Mutating and operational endpoints (tasks write, versions write, policy check, assets write, publish package/push, sync, AI generate, onboard preflight, QA verify) require `require_ops`.
  - This removes previous risk where most Content Ops endpoints were callable without auth.

- Durable publish queue (no in-process thread runner):
  - `POST /api/v1/content/publish/push` in `mode=confirm` persists job as `queued` and returns immediately.
  - New service worker method `process_queued_publish_jobs(limit=...)` claims jobs atomically (`queued -> running`) and executes processing.
  - APScheduler now runs queue processing every minute (`content-publish-queue-1m`) in `apps/api/app/scheduler.py`.
  - This replaces non-durable `threading.Thread` behavior and reduces risk of lost jobs after API process restart.

- Hard PTD blocker is now deterministic:
  - Resolver now distinguishes PTD states from cache:
    - `missing_definition`
    - `empty_required_attrs`
    - `ok`
  - If PTD is missing/empty for resolved `marketplace + product_type`, confirm push is blocked:
    - `preflight_blocker_ptd_missing_definition`
    - `preflight_blocker_ptd_empty_required_attrs`
  - Existing blocker `preflight_blocker_missing_required` remains for missing required attributes.
  - Any `preflight_blocker_*` blocks bridge fallback as well.

- Tests extended beyond route-contract mocks:
  - Added RBAC assertions in API tests:
    - unauthenticated access returns `401` on protected endpoints
    - low-role (`analyst`) on ops endpoints returns `403`
  - Added service tests for publish hardening:
    - PTD missing/empty state detection
    - confirm mode stays queued (no immediate inline processing)
    - queued job processing success and failure paths
  - Files:
    - `apps/api/tests/test_api_content_ops.py`
    - `apps/api/tests/test_content_ops_service_publish.py`
