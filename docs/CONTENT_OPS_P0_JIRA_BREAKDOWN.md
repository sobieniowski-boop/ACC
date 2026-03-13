# Content Ops Studio - P0 Jira Breakdown (Stories + Subtasks + Hours)

Assumptions:
- 1 FE + 1 BE + 1 QA (part-time).
- Existing auth/RBAC framework is reused.
- Scope is P0 only (no SP-API push, no impact analytics automation).

## Story COS-P0-01: DB schema and bootstrap for Content Ops

Goal: create `acc_co_*` tables + indexes + idempotent startup migration.

Estimate total: 14h

Subtasks:
1. Add schema DDL in bootstrap (`ensure_v2_schema`) for `acc_co_tasks`, `acc_co_versions`, `acc_co_policy_rules`, `acc_co_policy_checks`, `acc_co_assets`, `acc_co_asset_links`, `acc_co_publish_jobs`, `acc_co_ai_cache`.
- Owner: BE
- Estimate: 7h

2. Add indexes + unique constraints (`content_hash`, versions per `sku+marketplace+version_no`).
- Owner: BE
- Estimate: 3h

3. Add smoke checks in startup logs and error handling for partial schema.
- Owner: BE
- Estimate: 2h

4. Validate migration on dev DB and create rollback script.
- Owner: BE
- Estimate: 2h

Acceptance:
- Restarting API does not fail on repeated migration.
- All required tables/columns/indexes exist after startup.

## Story COS-P0-02: Content tasks API

Goal: CRUD for content tasks with filters and pagination.

Estimate total: 16h

Subtasks:
1. Implement service methods `list/create/update` in `content_ops.py`.
- Owner: BE
- Estimate: 8h

2. Add input validation and status transition guard (`open -> investigating -> resolved`).
- Owner: BE
- Estimate: 3h

3. Add API tests for filters/pagination/invalid payloads.
- Owner: BE
- Estimate: 3h

4. Add audit fields and actor fallback from auth context.
- Owner: BE
- Estimate: 2h

Acceptance:
- `GET /content/tasks` returns deterministic paged response.
- Invalid enum/status transitions return `400`.

## Story COS-P0-03: Versioning workflow API

Goal: draft/review/approve lifecycle per `sku+marketplace`.

Estimate total: 22h

Subtasks:
1. Implement list/create/update version service methods.
- Owner: BE
- Estimate: 10h

2. Implement transition endpoints `submit-review`, `approve`.
- Owner: BE
- Estimate: 4h

3. Implement immutable history behavior (new snapshot on edit).
- Owner: BE
- Estimate: 4h

4. Add tests for version increments and status machine.
- Owner: BE
- Estimate: 4h

Acceptance:
- New draft increments `version_no`.
- Approved version cannot be overwritten by raw update endpoint.

## Story COS-P0-04: Policy rules and policy checker

Goal: rules management + linting endpoint with severity counts.

Estimate total: 20h

Subtasks:
1. Implement rule repository (`get/upsert`) with active filter.
- Owner: BE
- Estimate: 5h

2. Implement checker engine for title/bullets/description/keywords.
- Owner: BE
- Estimate: 8h

3. Persist check results in `acc_co_policy_checks`.
- Owner: BE
- Estimate: 3h

4. Add unit tests for regex matching and severity aggregation.
- Owner: BE
- Estimate: 4h

Acceptance:
- Checker returns `passed`, counts, findings with field/snippet.
- `approve` blocks when critical findings exist.

## Story COS-P0-05: Diff and sync MAIN->TARGET

Goal: compare content between markets and create synced drafts.

Estimate total: 18h

Subtasks:
1. Implement diff service with field-by-field comparison model.
- Owner: BE
- Estimate: 7h

2. Implement sync service (`missing_only` and `force`) creating new drafts.
- Owner: BE
- Estimate: 7h

3. Add tests for overwrite modes and partial field sync.
- Owner: BE
- Estimate: 4h

Acceptance:
- Diff endpoint returns structured changes.
- Sync never overwrites approved version directly.

## Story COS-P0-06: Asset library MVP API

Goal: upload/list/link assets with hash dedup.

Estimate total: 20h

Subtasks:
1. Implement upload handler (`base64` scaffold mode) with hash dedup.
- Owner: BE
- Estimate: 7h

2. Implement list endpoint with filters and pagination.
- Owner: BE
- Estimate: 4h

3. Implement link endpoint (`sku/asin/marketplace/role`).
- Owner: BE
- Estimate: 4h

4. Add validations (mime whitelist, size limit, required metadata).
- Owner: BE
- Estimate: 3h

5. API tests.
- Owner: BE
- Estimate: 2h

Acceptance:
- Duplicate upload returns same logical asset or deterministic conflict handling.
- Linked asset is queryable by SKU and role.

## Story COS-P0-07: Publish package and jobs history API

Goal: generate package job records and expose job list.

Estimate total: 16h

Subtasks:
1. Implement package generation orchestrator (stub file writer or placeholder artifact).
- Owner: BE
- Estimate: 7h

2. Implement job persistence in `acc_co_publish_jobs`.
- Owner: BE
- Estimate: 4h

3. Implement list jobs endpoint.
- Owner: BE
- Estimate: 2h

4. Add tests for status/progress transitions.
- Owner: BE
- Estimate: 3h

Acceptance:
- Creating package returns job object with status/progress.
- Jobs endpoint returns historical jobs paged.

## Story COS-P0-08: AI generate endpoint with guardrails and cache

Goal: deterministic AI wrapper for `new_listing/improve/localize`.

Estimate total: 24h

Subtasks:
1. Implement context hash and cache lookup/store (`acc_co_ai_cache`).
- Owner: BE
- Estimate: 6h

2. Implement guardrails pre/post filters (claims, forbidden tokens, PII strip).
- Owner: BE
- Estimate: 8h

3. Integrate with existing AI client and normalized response model.
- Owner: BE
- Estimate: 6h

4. Add tests for cache hit/miss and blocked claims.
- Owner: BE
- Estimate: 4h

Acceptance:
- Same context hash can return `cache_hit=true`.
- Output includes `policy_flags` and sanitized content.

## Story COS-P0-09: Frontend scaffold for Content Ops pages

Goal: route skeleton and API wiring for dashboard/editor/compliance/assets/publish.

Estimate total: 26h

Subtasks:
1. Add routes and page shells.
- Owner: FE
- Estimate: 6h

2. Add API client methods and request models.
- Owner: FE
- Estimate: 6h

3. Implement tasks list + filters + status update UI.
- Owner: FE
- Estimate: 6h

4. Implement versions list/editor skeleton + policy check action.
- Owner: FE
- Estimate: 5h

5. Implement publish jobs table and basic asset list/link flow.
- Owner: FE
- Estimate: 3h

Acceptance:
- User can navigate all Content Ops pages and execute basic API calls.

## Story COS-P0-10: QA + observability + release hardening

Goal: regression coverage, telemetry events, and release checklist.

Estimate total: 18h

Subtasks:
1. Add integration tests for all P0 endpoints.
- Owner: BE/QA
- Estimate: 8h

2. Add structured logs and telemetry events (`content.*`).
- Owner: BE
- Estimate: 4h

3. Add alert rules for API errors and slow endpoints.
- Owner: BE/Ops
- Estimate: 3h

4. Prepare release checklist + post-deploy smoke test script.
- Owner: QA
- Estimate: 3h

Acceptance:
- P0 endpoint smoke tests pass on staging.
- Dashboard/logs show key content workflow events.

## Capacity summary

- Backend total: 150h
- Frontend total: 26h
- QA/Ops explicit: 18h
- Full P0 package: 194h

Recommended sprint split (2 weeks each):
- Sprint A: COS-P0-01..05 (90h)
- Sprint B: COS-P0-06..10 (104h)
