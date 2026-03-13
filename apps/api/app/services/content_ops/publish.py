"""Content Ops - publishing workflows, feed submission, queue processing."""
from __future__ import annotations

import json
import math
import re
import uuid
import hashlib
import base64
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from pathlib import Path

import pyodbc
import httpx

from app.connectors.mssql.mssql_store import ensure_v2_schema
from app.core.circuit_breaker import (
    ContentPublishCircuitOpen,
)
from app.platform.action_center import (
    is_action_circuit_open as _ac_is_circuit_open,
    _cb_record_failure as _ac_cb_record_failure,
    _cb_record_success as _ac_cb_record_success,
    ActionCircuitOpen,
)

_ACTION_TYPE = "content_publish"


def _is_circuit_open_sync() -> bool:
    return _run_async(_ac_is_circuit_open(_ACTION_TYPE))


def _cb_record_failure_sync() -> None:
    _run_async(_ac_cb_record_failure(_ACTION_TYPE))


def _cb_record_success_sync() -> None:
    _run_async(_ac_cb_record_success(_ACTION_TYPE))
from app.core.config import settings, MARKETPLACE_REGISTRY
from ._helpers import (
    _connect, _fetchall_dict, _json_load, _json_list, _is_missing_value,
    _marketplace_to_id, _marketplace_to_code, _language_tag_for_market,
    _map_publish_job_row, _normalize_publish_selection, _normalize_publish_format,
    _run_async, _spapi_ready,
    _bridge_enabled, _bridge_base_url, _bridge_headers, _bridge_timeout,
    _bridge_get_json, _bridge_post_json,
    _native_push_listing_content,
    _PUBLISH_RETRY_BASE_MINUTES, _PUBLISH_RETRY_MAX,
    _ALLOWED_ASSET_MIME, _PUBLISH_SELECTIONS,
)
from .catalog import (
    _resolve_native_product_type_and_requirements,
    _resolve_native_product_type,
    _apply_attribute_registry,
    _attrs_missing_required,
    _normalize_attribute_values,
    _suggest_source_fields_for_attr,
    _load_required_attrs_state_from_definition,
)


def create_publish_package(*, payload: dict):
    ensure_v2_schema()
    marketplaces = [str(m).strip().upper() for m in (payload.get("marketplaces") or []) if str(m).strip()]
    if not marketplaces:
        raise ValueError("marketplaces is required")
    selection = _normalize_publish_selection(str(payload.get("selection") or "approved"))
    fmt = _normalize_publish_format(str(payload.get("format") or "xlsx"))
    sku_filter = [str(s).strip() for s in (payload.get("sku_filter") or []) if str(s).strip()]

    conn = _connect()
    try:
        cur = conn.cursor()
        job_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO dbo.acc_co_publish_jobs
                (id, job_type, marketplaces_json, selection_mode, status, progress_pct, log_json, created_by)
            VALUES
                (?, 'publish_package', ?, ?, 'running', 5, ?, ?)
            """,
            (
                job_id,
                json.dumps(marketplaces, ensure_ascii=True),
                selection,
                json.dumps({"step": "started"}, ensure_ascii=True),
                settings.DEFAULT_ACTOR,
            ),
        )

        # Build package scope summary from versions table.
        where = ["status = ?"]
        params: list[Any] = [selection]

        market_placeholders = ",".join("?" for _ in marketplaces)
        where.append(f"marketplace_id IN ({market_placeholders})")
        params.extend(marketplaces)

        if sku_filter:
            sku_placeholders = ",".join("?" for _ in sku_filter)
            where.append(f"sku IN ({sku_placeholders})")
            params.extend(sku_filter)

        where_sql = " AND ".join(where)
        cur.execute(
            f"""
            SELECT
                COUNT(*) AS rows_count,
                COUNT(DISTINCT sku) AS sku_count
            FROM dbo.acc_co_versions WITH (NOLOCK)
            WHERE {where_sql}
            """,
            params,
        )
        agg = cur.fetchone()
        rows_count = int((agg[0] if agg else 0) or 0)
        sku_count = int((agg[1] if agg else 0) or 0)

        artifact_url = f"/api/v1/content/publish/jobs/{job_id}/artifact.{fmt}"
        log_payload = {
            "selection": selection,
            "format": fmt,
            "rows_count": rows_count,
            "sku_count": sku_count,
            "marketplaces": marketplaces,
            "sku_filter_count": len(sku_filter),
            "note": "P0 package summary generated; artifact endpoint placeholder",
        }

        cur.execute(
            """
            UPDATE dbo.acc_co_publish_jobs
            SET status = 'completed',
                progress_pct = 100,
                artifact_url = ?,
                log_json = ?,
                finished_at = SYSUTCDATETIME()
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (
                artifact_url,
                json.dumps(log_payload, ensure_ascii=True),
                job_id,
            ),
        )
        conn.commit()

        cur.execute(
            """
            SELECT TOP 1
                id, job_type, marketplaces_json, selection_mode, status, progress_pct, log_json,
                artifact_url, created_by, created_at, finished_at
            FROM dbo.acc_co_publish_jobs WITH (NOLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (job_id,),
        )
        return _map_publish_job_row(_fetchall_dict(cur)[0])
    finally:
        conn.close()


def _load_push_candidates(
    cur: pyodbc.Cursor,
    *,
    marketplaces: list[str],
    selection: str,
    sku_filter: list[str],
    version_ids: list[str],
) -> list[dict[str, Any]]:
    if version_ids:
        placeholders = ",".join("CAST(? AS UNIQUEIDENTIFIER)" for _ in version_ids)
        where_parts = [f"v.id IN ({placeholders})"]
        params: list[Any] = [*version_ids]
    else:
        where_parts = ["v.status = ?"]
        params = [selection]

    mp_placeholders = ",".join("?" for _ in marketplaces)
    where_parts.append(f"v.marketplace_id IN ({mp_placeholders})")
    params.extend(marketplaces)

    if sku_filter:
        sku_placeholders = ",".join("?" for _ in sku_filter)
        where_parts.append(f"v.sku IN ({sku_placeholders})")
        params.extend(sku_filter)

    where_sql = " AND ".join(where_parts)
    query = f"""
        WITH base AS (
            SELECT
                v.id,
                v.sku,
                v.asin,
                v.marketplace_id,
                v.version_no,
                v.status,
                v.fields_json,
                v.created_at,
                p.category,
                p.subcategory,
                p.brand,
                ROW_NUMBER() OVER (
                    PARTITION BY v.sku, v.marketplace_id
                    ORDER BY v.version_no DESC, v.created_at DESC
                ) AS rn
            FROM dbo.acc_co_versions v WITH (NOLOCK)
            OUTER APPLY (
                SELECT TOP 1 ap.category, ap.subcategory, ap.brand
                FROM dbo.acc_product ap WITH (NOLOCK)
                WHERE ap.sku = v.sku OR ap.internal_sku = v.sku
                ORDER BY ap.updated_at DESC
            ) p
            WHERE {where_sql}
        )
        SELECT
            id, sku, asin, marketplace_id, version_no, status, fields_json, created_at, category, subcategory, brand
        FROM base
        WHERE rn = 1
        ORDER BY marketplace_id, sku
    """
    cur.execute(query, params)
    return _fetchall_dict(cur)


def _get_publish_job(job_id: str) -> dict[str, Any]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 1
                id, job_type, marketplaces_json, selection_mode, status, progress_pct, log_json,
                artifact_url, created_by, created_at, finished_at
            FROM dbo.acc_co_publish_jobs WITH (NOLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (job_id,),
        )
        rows = _fetchall_dict(cur)
        if not rows:
            raise ValueError("publish job not found")
        return _map_publish_job_row(rows[0])
    finally:
        conn.close()


def _find_publish_job_by_idempotency(*, idempotency_key: str) -> dict[str, Any] | None:
    if not idempotency_key:
        return None
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 1
                id, job_type, marketplaces_json, selection_mode, status, progress_pct, log_json,
                artifact_url, created_by, created_at, finished_at
            FROM dbo.acc_co_publish_jobs WITH (NOLOCK)
            WHERE job_type = 'publish_push'
              AND idempotency_key = ?
              AND status IN ('queued', 'running', 'completed', 'partial')
            ORDER BY created_at DESC
            """,
            (idempotency_key,),
        )
        rows = _fetchall_dict(cur)
        if not rows:
            return None
        return _map_publish_job_row(rows[0])
    finally:
        conn.close()


def _retry_backoff_minutes(retry_count: int) -> int:
    safe_retry = max(1, retry_count)
    return _PUBLISH_RETRY_BASE_MINUTES * (2 ** (safe_retry - 1))


def _process_publish_push_job(
    *,
    job_id: str,
    marketplaces: list[str],
    selection: str,
    mode: str,
    sku_filter: list[str],
    version_ids: list[str],
) -> dict[str, Any]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE dbo.acc_co_publish_jobs
            SET status = 'running',
                progress_pct = CASE WHEN progress_pct < 15 THEN 15 ELSE progress_pct END,
                heartbeat_at = SYSUTCDATETIME(),
                finished_at = NULL
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (job_id,),
        )
        conn.commit()
        candidates = _load_push_candidates(
            cur,
            marketplaces=marketplaces,
            selection=selection,
            sku_filter=sku_filter,
            version_ids=version_ids,
        )
        mapping_rules = _load_product_type_map_rules(cur)
        attribute_rules = _load_attribute_mapping_rules(cur)
        by_market: dict[str, list[dict[str, Any]]] = {}
        for row in candidates:
            mp = str(row.get("marketplace_id") or "").upper()
            by_market.setdefault(mp, []).append(row)

        per_market: dict[str, Any] = {}
        success_count = 0
        failed_count = 0

        # ── Circuit breaker gate ────────────────────────────────────────
        if mode != "preview" and _is_circuit_open_sync():
            for market in marketplaces:
                per_market[market] = {
                    "status": "blocked",
                    "reason": "circuit_breaker_open",
                    "items": len(by_market.get(market, [])),
                }
                failed_count += len(by_market.get(market, []))
            raise ContentPublishCircuitOpen(
                "Content publish circuit breaker is OPEN — "
                "too many SP-API failures in the last hour. "
                "Publishing paused for 30 minutes."
            )

        for market in marketplaces:
            rows = by_market.get(market, [])
            if not rows:
                per_market[market] = {"status": "skipped", "reason": "no_candidates", "items": 0}
                continue

            if mode == "preview":
                preview_rows: list[dict[str, Any]] = []
                for r in rows:
                    sku_value = str(r.get("sku") or "").strip()
                    fields = _json_load(r.get("fields_json"))
                    category_hint = str(r.get("category") or "").strip() or None
                    subcategory_hint = str(r.get("subcategory") or "").strip() or None
                    brand_hint = str(r.get("brand") or "").strip() or None
                    resolved_product_type, required_attrs, resolver_source = _resolve_native_product_type_and_requirements(
                        sku=sku_value,
                        fields=fields,
                        category_hint=category_hint,
                        subcategory_hint=subcategory_hint,
                        brand_hint=brand_hint,
                        marketplace=market,
                        mapping_rules=mapping_rules,
                    )
                    ptd_state = "not_needed"
                    ptd_blocker = None
                    if resolver_source in {"heuristic", "default"}:
                        ptd_blocker = "preflight_blocker_product_type_unmapped"
                    if not required_attrs:
                        ptd_state, required_attrs = _load_required_attrs_state_from_definition(
                            cur,
                            marketplace=market,
                            product_type=resolved_product_type,
                        )
                        if ptd_state != "ok":
                            ptd_blocker = f"preflight_blocker_ptd_{ptd_state}"
                    attrs_preview = _apply_attribute_registry(
                        fields=fields,
                        marketplace=market,
                        product_type=resolved_product_type,
                        rules=attribute_rules,
                    )
                    missing_required = _attrs_missing_required(attrs_preview, required_attrs)
                    preview_rows.append(
                        {
                            "version_id": str(r["id"]),
                            "sku": sku_value,
                            "product_type": resolved_product_type,
                            "required_attrs": required_attrs,
                            "missing_required_attrs": missing_required,
                            "resolver_source": resolver_source,
                            "ptd_state": ptd_state,
                            "blocker": ptd_blocker,
                        }
                    )
                per_market[market] = {
                    "status": "preview_ready",
                    "items": len(rows),
                    "version_ids": [str(r["id"]) for r in rows],
                    "preview_rows": preview_rows,
                    "product_types": sorted(
                        list({str(item.get("product_type") or "") for item in preview_rows if item.get("product_type")})
                    ),
                    "blocked_count": len(
                        [x for x in preview_rows if x.get("missing_required_attrs") or x.get("blocker")]
                    ),
                }
                continue

            bridge_payload = {
                "source": "acc_content_ops",
                "mode": mode,
                "marketplace": market,
                "selection": selection,
                "items": [
                    {
                        "version_id": str(r["id"]),
                        "sku": r.get("sku"),
                        "asin": r.get("asin"),
                        "version_no": int(r.get("version_no") or 0),
                        "status": str(r.get("status") or ""),
                        "fields": _json_load(r.get("fields_json")),
                    }
                    for r in rows
                ],
            }

            try:
                native_results: list[dict[str, Any]] = []
                native_errors: list[dict[str, Any]] = []
                for r in rows:
                    sku_value = str(r.get("sku") or "").strip()
                    fields = _json_load(r.get("fields_json"))
                    category_hint = str(r.get("category") or "").strip() or None
                    subcategory_hint = str(r.get("subcategory") or "").strip() or None
                    brand_hint = str(r.get("brand") or "").strip() or None
                    resolved_product_type, required_attrs, resolver_source = _resolve_native_product_type_and_requirements(
                        sku=sku_value,
                        fields=fields,
                        category_hint=category_hint,
                        subcategory_hint=subcategory_hint,
                        brand_hint=brand_hint,
                        marketplace=market,
                        mapping_rules=mapping_rules,
                    )
                    ptd_state = "not_needed"
                    if resolver_source in {"heuristic", "default"}:
                        native_errors.append(
                            {
                                "sku": sku_value,
                                "product_type": resolved_product_type,
                                "resolver_source": resolver_source,
                                "code": "preflight_blocker_product_type_unmapped",
                                "message": "Product type resolved heuristically. Add explicit mapping rule or attribute.",
                            }
                        )
                        continue
                    if not required_attrs:
                        ptd_state, required_attrs = _load_required_attrs_state_from_definition(
                            cur,
                            marketplace=market,
                            product_type=resolved_product_type,
                        )
                        if ptd_state != "ok":
                            native_errors.append(
                                {
                                    "sku": sku_value,
                                    "product_type": resolved_product_type,
                                    "resolver_source": resolver_source,
                                    "code": f"preflight_blocker_ptd_{ptd_state}",
                                    "message": f"PTD state is {ptd_state}",
                                }
                            )
                            continue
                    attrs_merged = _apply_attribute_registry(
                        fields=fields,
                        marketplace=market,
                        product_type=resolved_product_type,
                        rules=attribute_rules,
                    )
                    missing_required = _attrs_missing_required(attrs_merged, required_attrs)
                    if missing_required:
                        native_errors.append(
                            {
                                "sku": sku_value,
                                "product_type": resolved_product_type,
                                "resolver_source": resolver_source,
                                "code": "preflight_blocker_missing_required",
                                "message": f"missing required attrs: {','.join(missing_required)}",
                                "missing_required_attrs": missing_required,
                            }
                        )
                        continue
                    fields = dict(fields)
                    fields["attributes_json"] = attrs_merged
                    try:
                        rsp = _native_push_listing_content(
                            marketplace=market,
                            sku=sku_value,
                            fields=fields,
                            category_hint=category_hint,
                            subcategory_hint=subcategory_hint,
                            brand_hint=brand_hint,
                            product_type_override=resolved_product_type,
                            required_attrs=required_attrs,
                        )
                        _cb_record_success_sync()
                        native_results.append(
                            {
                                "version_id": str(r["id"]),
                                "sku": sku_value,
                                "product_type": resolved_product_type,
                                "required_attrs": required_attrs,
                                "resolver_source": resolver_source,
                                "ptd_state": ptd_state,
                                "status": "submitted",
                                "response": rsp,
                            }
                        )
                    except Exception as native_exc:
                        _cb_record_failure_sync()
                        native_errors.append(
                            {
                                "sku": sku_value,
                                "product_type": resolved_product_type,
                                "resolver_source": resolver_source,
                                "code": "native_push_error",
                                "message": str(native_exc),
                                "required_attrs": required_attrs,
                            }
                        )

                if not native_errors:
                    per_market[market] = {
                        "status": "submitted",
                        "items": len(rows),
                        "transport": "native_sp_api",
                        "native_results_count": len(native_results),
                        "product_types": sorted(
                            list({str(item.get("product_type") or "") for item in native_results if item.get("product_type")})
                        ),
                    }
                    success_count += 1
                else:
                    if _bridge_enabled():
                        preflight_blockers = [
                            x for x in native_errors if str(x.get("code") or "").startswith("preflight_blocker_")
                        ]
                        if preflight_blockers:
                            per_market[market] = {
                                "status": "failed",
                                "reason": "preflight_blocker",
                                "items": len(rows),
                                "transport": "blocked_preflight",
                                "native_errors": native_errors,
                            }
                            failed_count += 1
                            continue
                        status_code, bridge_resp = _bridge_post_json(
                            str(getattr(settings, "PRODUCTONBOARD_PUSH_PATH", "") or "/api/productonboard/acc-content/push"),
                            bridge_payload,
                        )
                        if status_code >= 300:
                            per_market[market] = {
                                "status": "failed",
                                "reason": f"native_and_bridge_failed_http_{status_code}",
                                "items": len(rows),
                                "transport": "bridge_fallback",
                                "native_errors": native_errors,
                                "bridge_response": bridge_resp,
                            }
                            failed_count += 1
                        else:
                            per_market[market] = {
                                "status": "submitted",
                                "items": len(rows),
                                "transport": "bridge_fallback",
                                "native_errors": native_errors,
                                "bridge_response": bridge_resp,
                            }
                            success_count += 1
                    else:
                        per_market[market] = {
                            "status": "failed",
                            "reason": "native_push_failed_no_bridge_fallback",
                            "items": len(rows),
                            "native_errors": native_errors,
                        }
                        failed_count += 1
            except Exception as exc:
                per_market[market] = {
                    "status": "failed",
                    "reason": f"push_error:{exc}",
                    "items": len(rows),
                }
                failed_count += 1

        total_candidates = len(candidates)
        if mode == "preview":
            final_status = "completed"
        else:
            if failed_count == 0 and success_count > 0:
                final_status = "completed"
            elif success_count > 0:
                final_status = "partial"
            else:
                final_status = "failed"

        log_payload = {
            "mode": mode,
            "selection": selection,
            "marketplaces": marketplaces,
            "total_candidates": total_candidates,
            "sku_filter_count": len(sku_filter),
            "version_ids_count": len(version_ids),
            "success_count": success_count,
            "failed_count": failed_count,
            "per_marketplace": per_market,
        }

        cur.execute(
            """
            UPDATE dbo.acc_co_publish_jobs
            SET status = ?,
                progress_pct = 100,
                log_json = ?,
                last_error = NULL,
                heartbeat_at = SYSUTCDATETIME(),
                finished_at = SYSUTCDATETIME()
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (
                final_status,
                json.dumps(log_payload, ensure_ascii=True),
                job_id,
            ),
        )
        conn.commit()
        return {"status": final_status, "log_json": log_payload}
    except Exception as exc:
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE dbo.acc_co_publish_jobs
                SET status = 'failed',
                    progress_pct = 100,
                    log_json = ?,
                    last_error = ?,
                    heartbeat_at = SYSUTCDATETIME(),
                    finished_at = SYSUTCDATETIME()
                WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                """,
                (
                    json.dumps({"error": str(exc), "step": "_process_publish_push_job"}, ensure_ascii=True),
                    str(exc)[:2000],
                    job_id,
                ),
            )
            conn.commit()
        except Exception:
            pass
        return {"status": "failed", "error": str(exc)}
    finally:
        conn.close()


def create_publish_push(*, payload: dict):
    ensure_v2_schema()
    marketplaces = [str(m).strip().upper() for m in (payload.get("marketplaces") or []) if str(m).strip()]
    if not marketplaces:
        raise ValueError("marketplaces is required")
    selection = _normalize_publish_selection(str(payload.get("selection") or "approved"))
    mode = str(payload.get("mode") or "preview").strip().lower()
    if mode not in {"preview", "confirm"}:
        raise ValueError("mode must be one of: preview, confirm")
    sku_filter = [str(s).strip() for s in (payload.get("sku_filter") or []) if str(s).strip()]
    version_ids = [str(v).strip() for v in (payload.get("version_ids") or []) if str(v).strip()]
    idempotency_key = str(payload.get("idempotency_key") or "").strip()

    if mode == "confirm" and idempotency_key:
        existing = _find_publish_job_by_idempotency(idempotency_key=idempotency_key)
        if existing:
            return {
                "job": existing,
                "queued": existing.get("status") in {"queued", "running"},
                "detail": "idempotent replay: existing publish job returned",
            }

    conn = _connect()
    try:
        cur = conn.cursor()
        job_id = str(uuid.uuid4())
        initial_status = "queued" if mode == "confirm" else "running"
        cur.execute(
            """
            INSERT INTO dbo.acc_co_publish_jobs
                (id, job_type, marketplaces_json, selection_mode, status, progress_pct, log_json, created_by, idempotency_key, retry_count, max_retries, next_retry_at)
            VALUES
                (?, 'publish_push', ?, ?, ?, 5, ?, ?, ?, 0, ?, NULL)
            """,
            (
                job_id,
                json.dumps(marketplaces, ensure_ascii=True),
                selection,
                initial_status,
                json.dumps(
                    {
                        "step": "started",
                        "mode": mode,
                        "sku_filter": sku_filter,
                        "version_ids": version_ids,
                        "idempotency_key": idempotency_key or None,
                    },
                    ensure_ascii=True,
                ),
                settings.DEFAULT_ACTOR,
                idempotency_key or None,
                _PUBLISH_RETRY_MAX,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    if mode == "confirm":
        return {
            "job": _get_publish_job(job_id),
            "queued": True,
            "detail": "publish push queued for async processing",
        }

    _process_publish_push_job(
        job_id=job_id,
        marketplaces=marketplaces,
        selection=selection,
        mode=mode,
        sku_filter=sku_filter,
        version_ids=version_ids,
    )
    return _get_publish_job(job_id)


def process_queued_publish_jobs(*, limit: int = 3) -> dict[str, int]:
    ensure_v2_schema()

    # Short-circuit when the circuit breaker is open — don't claim any jobs.
    if _is_circuit_open_sync():
        return {"claimed": 0, "processed": 0, "failed": 0, "circuit_breaker": "open"}

    safe_limit = max(1, min(int(limit or 3), 50))
    claimed = 0
    processed = 0
    failed = 0

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP (?)
                id, marketplaces_json, selection_mode, log_json, retry_count, max_retries
            FROM dbo.acc_co_publish_jobs WITH (UPDLOCK, READPAST)
            WHERE job_type = 'publish_push'
              AND status = 'queued'
              AND (next_retry_at IS NULL OR next_retry_at <= SYSUTCDATETIME())
            ORDER BY created_at ASC
            """,
            (safe_limit,),
        )
        rows = _fetchall_dict(cur)
    finally:
        conn.close()

    for row in rows:
        job_id = str(row.get("id") or "")
        if not job_id:
            continue
        marketplaces = _json_list(row.get("marketplaces_json"))
        selection = str(row.get("selection_mode") or "approved").strip().lower()
        if selection not in _PUBLISH_SELECTIONS:
            selection = "approved"
        log_json = _json_load(row.get("log_json"))
        sku_filter = [str(x).strip() for x in (log_json.get("sku_filter") or []) if str(x).strip()]
        version_ids = [str(x).strip() for x in (log_json.get("version_ids") or []) if str(x).strip()]
        retry_count = int(row.get("retry_count") or 0)
        max_retries = int(row.get("max_retries") or _PUBLISH_RETRY_MAX)

        conn_claim = _connect()
        try:
            cur_claim = conn_claim.cursor()
            cur_claim.execute(
                """
                UPDATE dbo.acc_co_publish_jobs
                SET status = 'running',
                    progress_pct = 15,
                    log_json = ?,
                    heartbeat_at = SYSUTCDATETIME(),
                    finished_at = NULL
                WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                  AND status = 'queued'
                """,
                (
                    json.dumps(
                        {
                            **log_json,
                            "step": "running",
                            "claimed_at": datetime.now(timezone.utc).isoformat(),
                        },
                        ensure_ascii=True,
                    ),
                    job_id,
                ),
            )
            if cur_claim.rowcount <= 0:
                conn_claim.commit()
                continue
            conn_claim.commit()
            claimed += 1
        finally:
            conn_claim.close()

        try:
            result = _process_publish_push_job(
                job_id=job_id,
                marketplaces=marketplaces,
                selection=selection,
                mode="confirm",
                sku_filter=sku_filter,
                version_ids=version_ids,
            )
            status = str(result.get("status") or "failed").lower()
            if status in {"completed", "partial"}:
                processed += 1
                continue

            should_retry = retry_count < max_retries
            conn_retry = _connect()
            try:
                cur_retry = conn_retry.cursor()
                if should_retry:
                    next_retry = _retry_backoff_minutes(retry_count + 1)
                    cur_retry.execute(
                        """
                        UPDATE dbo.acc_co_publish_jobs
                        SET status = 'queued',
                            retry_count = retry_count + 1,
                            next_retry_at = DATEADD(MINUTE, ?, SYSUTCDATETIME()),
                            last_error = ?,
                            heartbeat_at = SYSUTCDATETIME(),
                            log_json = JSON_MODIFY(ISNULL(log_json, '{}'), '$.retry_scheduled', JSON_QUERY(?))
                        WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                        """,
                        (
                            next_retry,
                            str(result.get("error") or status)[:2000],
                            json.dumps(
                                {
                                    "retry_count": retry_count + 1,
                                    "max_retries": max_retries,
                                    "next_retry_in_minutes": next_retry,
                                },
                                ensure_ascii=True,
                            ),
                            job_id,
                        ),
                    )
                else:
                    cur_retry.execute(
                        """
                        UPDATE dbo.acc_co_publish_jobs
                        SET status = 'failed',
                            retry_count = ?,
                            next_retry_at = NULL,
                            last_error = ?,
                            heartbeat_at = SYSUTCDATETIME(),
                            finished_at = SYSUTCDATETIME()
                        WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                        """,
                        (
                            retry_count,
                            str(result.get("error") or "publish_failed_after_retries")[:2000],
                            job_id,
                        ),
                    )
                conn_retry.commit()
            finally:
                conn_retry.close()
            failed += 1
        except Exception as exc:
            failed += 1
            should_retry = retry_count < max_retries
            conn_fail = _connect()
            try:
                cur_fail = conn_fail.cursor()
                if should_retry:
                    next_retry = _retry_backoff_minutes(retry_count + 1)
                    cur_fail.execute(
                        """
                        UPDATE dbo.acc_co_publish_jobs
                        SET status = 'queued',
                            retry_count = retry_count + 1,
                            next_retry_at = DATEADD(MINUTE, ?, SYSUTCDATETIME()),
                            last_error = ?,
                            heartbeat_at = SYSUTCDATETIME()
                        WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                        """,
                        (
                            next_retry,
                            str(exc)[:2000],
                            job_id,
                        ),
                    )
                else:
                    cur_fail.execute(
                        """
                        UPDATE dbo.acc_co_publish_jobs
                        SET status = 'failed',
                            next_retry_at = NULL,
                            last_error = ?,
                            heartbeat_at = SYSUTCDATETIME(),
                            finished_at = SYSUTCDATETIME()
                        WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                        """,
                        (str(exc)[:2000], job_id),
                    )
                conn_fail.commit()
            finally:
                conn_fail.close()

    return {"claimed": claimed, "processed": processed, "failed": failed}


def evaluate_publish_queue_alerts(*, stale_minutes: int = 30, threshold_count: int = 5) -> dict[str, int]:
    ensure_v2_schema()
    safe_minutes = max(5, int(stale_minutes or 30))
    safe_threshold = max(1, int(threshold_count or 5))

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*)
            FROM dbo.acc_co_publish_jobs WITH (NOLOCK)
            WHERE job_type = 'publish_push'
              AND status = 'queued'
              AND created_at <= DATEADD(MINUTE, -?, SYSUTCDATETIME())
            """,
            (safe_minutes,),
        )
        stale_count = int((cur.fetchone() or [0])[0] or 0)

        cur.execute(
            """
            SELECT TOP 1 id
            FROM dbo.acc_al_alert_rules WITH (NOLOCK)
            WHERE rule_type = 'content_publish_queue_stale'
            ORDER BY created_at DESC
            """
        )
        row = cur.fetchone()
        if row and row[0]:
            rule_id = str(row[0])
        else:
            rule_id = str(uuid.uuid4())
            cur.execute(
                """
                INSERT INTO dbo.acc_al_alert_rules
                    (id, name, description, rule_type, severity, is_active, created_by)
                VALUES
                    (?, 'Content Publish Queue Stale', 'Queued content publish jobs exceeded stale threshold.', 'content_publish_queue_stale', 'critical', 1, ?)
                """,
                (rule_id, settings.DEFAULT_ACTOR),
            )

        if stale_count >= safe_threshold:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM dbo.acc_al_alerts WITH (NOLOCK)
                WHERE rule_id = CAST(? AS UNIQUEIDENTIFIER)
                  AND is_resolved = 0
                """,
                (rule_id,),
            )
            open_count = int((cur.fetchone() or [0])[0] or 0)
            if open_count == 0:
                cur.execute(
                    """
                    INSERT INTO dbo.acc_al_alerts
                        (id, rule_id, title, detail, severity, current_value)
                    VALUES
                        (?, CAST(? AS UNIQUEIDENTIFIER), ?, ?, 'critical', ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        rule_id,
                        "Content publish queue backlog",
                        f"Queued publish jobs older than {safe_minutes} minutes: {stale_count}",
                        float(stale_count),
                    ),
                )
        else:
            cur.execute(
                """
                UPDATE dbo.acc_al_alerts
                SET is_resolved = 1,
                    resolved_at = SYSUTCDATETIME(),
                    resolved_by = ?
                WHERE rule_id = CAST(? AS UNIQUEIDENTIFIER)
                  AND is_resolved = 0
                """,
                (settings.DEFAULT_ACTOR, rule_id),
            )
        conn.commit()
        return {"stale_queued": stale_count, "threshold": safe_threshold}
    finally:
        conn.close()


def list_publish_jobs(*, page: int = 1, page_size: int = 50):
    ensure_v2_schema()
    safe_page_size = max(1, min(page_size, 200))
    safe_page = max(1, page)
    offset = (safe_page - 1) * safe_page_size

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM dbo.acc_co_publish_jobs WITH (NOLOCK)")
        total = int((cur.fetchone() or [0])[0] or 0)
        pages = math.ceil(total / safe_page_size) if total else 0

        cur.execute(
            """
            SELECT
                id, job_type, marketplaces_json, selection_mode, status, progress_pct, log_json,
                artifact_url, created_by, created_at, finished_at
            FROM dbo.acc_co_publish_jobs WITH (NOLOCK)
            ORDER BY created_at DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            (offset, safe_page_size),
        )
        rows = _fetchall_dict(cur)
        return {
            "total": total,
            "page": safe_page,
            "page_size": safe_page_size,
            "pages": pages,
            "items": [_map_publish_job_row(r) for r in rows],
        }
    finally:
        conn.close()


def get_publish_queue_health(*, stale_minutes: int = 30):
    ensure_v2_schema()
    safe_stale = max(5, min(int(stale_minutes or 30), 240))

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*)
            FROM dbo.acc_co_publish_jobs WITH (NOLOCK)
            WHERE job_type = 'publish_push'
              AND status = 'queued'
            """
        )
        queued_total = int((cur.fetchone() or [0])[0] or 0)

        cur.execute(
            """
            SELECT COUNT(*)
            FROM dbo.acc_co_publish_jobs WITH (NOLOCK)
            WHERE job_type = 'publish_push'
              AND status = 'queued'
              AND created_at <= DATEADD(MINUTE, -?, SYSUTCDATETIME())
            """,
            (safe_stale,),
        )
        queued_stale = int((cur.fetchone() or [0])[0] or 0)

        cur.execute(
            """
            SELECT COUNT(*)
            FROM dbo.acc_co_publish_jobs WITH (NOLOCK)
            WHERE job_type = 'publish_push'
              AND status = 'running'
            """
        )
        running_total = int((cur.fetchone() or [0])[0] or 0)

        cur.execute(
            """
            SELECT COUNT(*)
            FROM dbo.acc_co_publish_jobs WITH (NOLOCK)
            WHERE job_type = 'publish_push'
              AND status = 'queued'
              AND ISNULL(retry_count, 0) > 0
            """
        )
        retry_in_progress = int((cur.fetchone() or [0])[0] or 0)

        cur.execute(
            """
            SELECT COUNT(*)
            FROM dbo.acc_co_publish_jobs WITH (NOLOCK)
            WHERE job_type = 'publish_push'
              AND status = 'failed'
              AND finished_at >= DATEADD(HOUR, -24, SYSUTCDATETIME())
            """
        )
        failed_last_24h = int((cur.fetchone() or [0])[0] or 0)

        cur.execute(
            """
            SELECT COUNT(*)
            FROM dbo.acc_co_publish_jobs WITH (NOLOCK)
            WHERE job_type = 'publish_push'
              AND status = 'failed'
              AND finished_at >= DATEADD(HOUR, -24, SYSUTCDATETIME())
              AND ISNULL(retry_count, 0) >= ISNULL(max_retries, ?)
            """,
            (_PUBLISH_RETRY_MAX,),
        )
        max_retry_reached_last_24h = int((cur.fetchone() or [0])[0] or 0)

        return {
            "generated_at": datetime.now(timezone.utc),
            "queued_total": queued_total,
            "queued_stale_30m": queued_stale,
            "running_total": running_total,
            "retry_in_progress": retry_in_progress,
            "failed_last_24h": failed_last_24h,
            "max_retry_reached_last_24h": max_retry_reached_last_24h,
            "thresholds": {
                "stale_minutes": safe_stale,
                "stale_warning_count": 5,
            },
        }
    finally:
        conn.close()


def _extract_failed_skus_from_job_log(log_json: dict[str, Any]) -> list[str]:
    out: list[str] = []
    per_market = log_json.get("per_marketplace")
    if not isinstance(per_market, dict):
        return out
    for value in per_market.values():
        if not isinstance(value, dict):
            continue
        errs = value.get("native_errors")
        if not isinstance(errs, list):
            continue
        for err in errs:
            if isinstance(err, dict):
                sku = str(err.get("sku") or "").strip()
                if sku:
                    out.append(sku)
    return list(dict.fromkeys(out))


def retry_publish_job(*, job_id: str, payload: dict):
    ensure_v2_schema()
    retry_sku_filter = [str(x).strip() for x in (payload.get("sku_filter") or []) if str(x).strip()]
    failed_only = bool(payload.get("failed_only", True))
    idempotency_key = str(payload.get("idempotency_key") or "").strip()

    base_job = _get_publish_job(job_id)
    if str(base_job.get("job_type") or "") != "publish_push":
        raise ValueError("only publish_push jobs can be retried")

    base_status = str(base_job.get("status") or "").lower()
    if base_status not in {"failed", "partial", "completed"}:
        raise ValueError("retry allowed only for failed/partial/completed jobs")

    base_log = base_job.get("log_json") if isinstance(base_job.get("log_json"), dict) else {}
    marketplaces = [str(x).strip().upper() for x in (base_job.get("marketplaces") or []) if str(x).strip()]
    selection = str(base_job.get("selection_mode") or "approved").strip().lower()
    if selection not in _PUBLISH_SELECTIONS:
        selection = "approved"

    sku_filter: list[str] = []
    if retry_sku_filter:
        sku_filter = retry_sku_filter
    elif failed_only:
        sku_filter = _extract_failed_skus_from_job_log(base_log)
    else:
        sku_filter = [str(x).strip() for x in (_json_list(base_log.get("sku_filter")) if isinstance(base_log, dict) else []) if str(x).strip()]

    inherited_key = idempotency_key or f"retry-{job_id}-{int(datetime.now(timezone.utc).timestamp())}"
    created = create_publish_push(
        payload={
            "marketplaces": marketplaces,
            "selection": selection,
            "mode": "confirm",
            "sku_filter": sku_filter,
            "version_ids": [],
            "idempotency_key": inherited_key,
        }
    )
    if isinstance(created, dict) and created.get("job"):
        return {
            **created,
            "detail": f"{created.get('detail', 'retry scheduled')} (source_job={job_id})",
        }
    return {
        "job": created,
        "queued": True,
        "detail": f"retry scheduled (source_job={job_id})",
    }

