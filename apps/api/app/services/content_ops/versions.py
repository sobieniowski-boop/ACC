"""Content Ops - version history, drafts, review, content versioning."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

import pyodbc

from app.connectors.mssql.mssql_store import ensure_v2_schema
from app.core.config import settings
from ._helpers import (
    _connect, _fetchall_dict, _json_load, _json_list,
    _normalize_version_status, _map_version_row, _is_missing_value,
    _marketplace_to_id, _marketplace_to_code,
    _ensure_co_internal_sku_columns, resolve_internal_sku,
)


def _parse_critical_count_from_policy(results_json: Any) -> int:
    data = _json_load(results_json)
    if not data:
        return 0
    if isinstance(data.get("critical_count"), int):
        return int(data["critical_count"])
    findings = data.get("findings")
    if isinstance(findings, list):
        critical = 0
        for f in findings:
            if isinstance(f, dict) and str(f.get("severity", "")).lower() == "critical":
                critical += 1
        return critical
    return 0


def _extract_rule_fields(rule_applies: dict[str, Any]) -> list[str]:
    default_fields = ["title", "bullets", "description", "keywords"]
    fields = rule_applies.get("fields")
    if isinstance(fields, list):
        normalized = [str(f).strip().lower() for f in fields if str(f).strip()]
        return normalized or default_fields
    return default_fields


def _collect_field_texts(fields_payload: dict[str, Any], field_name: str) -> list[str]:
    value = fields_payload.get(field_name)
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value if str(v).strip()]
    if isinstance(value, str):
        return [value]
    return [str(value)]


def _get_version_by_id(cur: pyodbc.Cursor, version_id: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT TOP 1
            id, sku, asin, marketplace_id, version_no, status, fields_json,
            created_by, created_at, approved_by, approved_at, published_at, parent_version_id
        FROM dbo.acc_co_versions WITH (NOLOCK)
        WHERE id = CAST(? AS UNIQUEIDENTIFIER)
        """,
        (version_id,),
    )
    rows = _fetchall_dict(cur)
    return rows[0] if rows else None


def _get_latest_version_for_market(cur: pyodbc.Cursor, sku: str, marketplace_id: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT TOP 1
            id, sku, asin, marketplace_id, version_no, status, fields_json,
            created_by, created_at, approved_by, approved_at, published_at, parent_version_id
        FROM dbo.acc_co_versions WITH (NOLOCK)
        WHERE sku = ?
          AND marketplace_id = ?
        ORDER BY version_no DESC, created_at DESC
        """,
        (sku, marketplace_id),
    )
    rows = _fetchall_dict(cur)
    return rows[0] if rows else None


def list_versions(*, sku: str, marketplace_id: str):
    ensure_v2_schema()
    sku_value = (sku or "").strip()
    market_value = (marketplace_id or "").strip()
    if not sku_value:
        raise ValueError("sku is required")
    if not market_value:
        raise ValueError("marketplace_id is required")

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id, sku, asin, marketplace_id, version_no, status, fields_json,
                created_by, created_at, approved_by, approved_at, published_at, parent_version_id
            FROM dbo.acc_co_versions WITH (NOLOCK)
            WHERE sku = ?
              AND marketplace_id = ?
            ORDER BY version_no DESC, created_at DESC
            """,
            (sku_value, market_value),
        )
        rows = _fetchall_dict(cur)
        return {
            "sku": sku_value,
            "marketplace_id": market_value,
            "items": [_map_version_row(r) for r in rows],
        }
    finally:
        conn.close()


def create_version(*, sku: str, marketplace_id: str, payload: dict):
    ensure_v2_schema()
    sku_value = (sku or "").strip()
    market_value = (marketplace_id or "").strip()
    asin = (payload.get("asin") or "").strip() or None
    base_version_id = (payload.get("base_version_id") or "").strip() or None
    fields = payload.get("fields")

    if not sku_value:
        raise ValueError("sku is required")
    if not market_value:
        raise ValueError("marketplace_id is required")

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ISNULL(MAX(version_no), 0)
            FROM dbo.acc_co_versions WITH (NOLOCK)
            WHERE sku = ?
              AND marketplace_id = ?
            """,
            (sku_value, market_value),
        )
        next_version_no = int((cur.fetchone() or [0])[0] or 0) + 1

        inherited_fields: dict[str, Any] = {}
        inherited_asin: str | None = None
        parent_version: str | None = None

        if base_version_id:
            cur.execute(
                """
                SELECT TOP 1 id, asin, fields_json
                FROM dbo.acc_co_versions WITH (NOLOCK)
                WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                """,
                (base_version_id,),
            )
            base_row = _fetchall_dict(cur)
            if not base_row:
                raise ValueError("base version not found")
            inherited_fields = _json_load(base_row[0].get("fields_json"))
            inherited_asin = base_row[0].get("asin")
            parent_version = str(base_row[0]["id"])
        else:
            cur.execute(
                """
                SELECT TOP 1 id, asin, fields_json
                FROM dbo.acc_co_versions WITH (NOLOCK)
                WHERE sku = ?
                  AND marketplace_id = ?
                ORDER BY version_no DESC, created_at DESC
                """,
                (sku_value, market_value),
            )
            prev = _fetchall_dict(cur)
            if prev:
                inherited_fields = _json_load(prev[0].get("fields_json"))
                inherited_asin = prev[0].get("asin")
                parent_version = str(prev[0]["id"])

        merged_fields = inherited_fields
        if isinstance(fields, dict):
            merged_fields = fields

        version_id = str(uuid.uuid4())
        _ensure_co_internal_sku_columns()
        internal_sku = resolve_internal_sku(cur, sku_value, market_value)
        cur.execute(
            """
            INSERT INTO dbo.acc_co_versions
                (id, sku, asin, marketplace_id, internal_sku, version_no, status, fields_json, compliance_notes, created_by, parent_version_id)
            VALUES
                (?, ?, ?, ?, ?, ?, 'draft', ?, ?, ?, ?)
            """,
            (
                version_id,
                sku_value,
                asin or inherited_asin,
                market_value,
                internal_sku,
                next_version_no,
                json.dumps(merged_fields, ensure_ascii=True),
                merged_fields.get("compliance_notes") if isinstance(merged_fields, dict) else None,
                settings.DEFAULT_ACTOR,
                parent_version,
            ),
        )
        conn.commit()

        cur.execute(
            """
            SELECT TOP 1
                id, sku, asin, marketplace_id, version_no, status, fields_json,
                created_by, created_at, approved_by, approved_at, published_at, parent_version_id,
                internal_sku
            FROM dbo.acc_co_versions WITH (NOLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (version_id,),
        )
        row = _fetchall_dict(cur)[0]
        return _map_version_row(row)
    finally:
        conn.close()


def update_version(*, version_id: str, payload: dict):
    ensure_v2_schema()
    fields = payload.get("fields")
    if not isinstance(fields, dict):
        raise ValueError("fields payload is required")

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 1 status
            FROM dbo.acc_co_versions WITH (NOLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (version_id,),
        )
        found = cur.fetchone()
        if not found:
            raise ValueError("version not found")
        status = _normalize_version_status(str(found[0] or "draft"))
        if status != "draft":
            raise ValueError("only draft versions can be edited")

        cur.execute(
            """
            UPDATE dbo.acc_co_versions
            SET fields_json = ?,
                compliance_notes = ?
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (
                json.dumps(fields, ensure_ascii=True),
                fields.get("compliance_notes"),
                version_id,
            ),
        )
        conn.commit()

        cur.execute(
            """
            SELECT TOP 1
                id, sku, asin, marketplace_id, version_no, status, fields_json,
                created_by, created_at, approved_by, approved_at, published_at, parent_version_id
            FROM dbo.acc_co_versions WITH (NOLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (version_id,),
        )
        row = _fetchall_dict(cur)[0]
        return _map_version_row(row)
    finally:
        conn.close()


def submit_version_review(*, version_id: str):
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 1 status
            FROM dbo.acc_co_versions WITH (NOLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (version_id,),
        )
        found = cur.fetchone()
        if not found:
            raise ValueError("version not found")
        status = _normalize_version_status(str(found[0] or "draft"))
        if status != "draft":
            raise ValueError("only draft versions can be moved to review")

        cur.execute(
            """
            UPDATE dbo.acc_co_versions
            SET status = 'review'
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (version_id,),
        )
        conn.commit()

        cur.execute(
            """
            SELECT TOP 1
                id, sku, asin, marketplace_id, version_no, status, fields_json,
                created_by, created_at, approved_by, approved_at, published_at, parent_version_id
            FROM dbo.acc_co_versions WITH (NOLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (version_id,),
        )
        return _map_version_row(_fetchall_dict(cur)[0])
    finally:
        conn.close()


def approve_version(*, version_id: str):
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 1 status
            FROM dbo.acc_co_versions WITH (NOLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (version_id,),
        )
        found = cur.fetchone()
        if not found:
            raise ValueError("version not found")
        status = _normalize_version_status(str(found[0] or "draft"))
        if status != "review":
            raise ValueError("only review versions can be approved")

        cur.execute(
            """
            SELECT TOP 1 passed, results_json
            FROM dbo.acc_co_policy_checks WITH (NOLOCK)
            WHERE version_id = CAST(? AS UNIQUEIDENTIFIER)
            ORDER BY checked_at DESC
            """,
            (version_id,),
        )
        policy = cur.fetchone()
        if policy:
            passed = bool(policy[0])
            critical_count = _parse_critical_count_from_policy(policy[1])
            if (not passed) and critical_count > 0:
                raise ValueError("approval blocked: critical policy findings detected")

        cur.execute(
            """
            UPDATE dbo.acc_co_versions
            SET status = 'approved',
                approved_by = ?,
                approved_at = SYSUTCDATETIME()
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (settings.DEFAULT_ACTOR, version_id),
        )
        conn.commit()

        cur.execute(
            """
            SELECT TOP 1
                id, sku, asin, marketplace_id, version_no, status, fields_json,
                created_by, created_at, approved_by, approved_at, published_at, parent_version_id
            FROM dbo.acc_co_versions WITH (NOLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (version_id,),
        )
        return _map_version_row(_fetchall_dict(cur)[0])
    finally:
        conn.close()


def get_content_diff(
    *,
    sku: str,
    main: str,
    target: str,
    version_main: Optional[str] = None,
    version_target: Optional[str] = None,
):
    ensure_v2_schema()
    sku_value = (sku or "").strip()
    main_market = (main or "").strip().upper()
    target_market = (target or "").strip().upper()
    if not sku_value:
        raise ValueError("sku is required")
    if not main_market:
        raise ValueError("main market is required")
    if not target_market:
        raise ValueError("target market is required")

    conn = _connect()
    try:
        cur = conn.cursor()
        main_row = _get_version_by_id(cur, version_main) if version_main else _get_latest_version_for_market(cur, sku_value, main_market)
        target_row = _get_version_by_id(cur, version_target) if version_target else _get_latest_version_for_market(cur, sku_value, target_market)

        if not main_row:
            raise ValueError("main market version not found")

        main_fields = _json_load(main_row.get("fields_json"))
        target_fields = _json_load(target_row.get("fields_json")) if target_row else {}
        all_field_keys = sorted(set(main_fields.keys()) | set(target_fields.keys()))

        diff_fields: list[dict[str, Any]] = []
        for key in all_field_keys:
            main_value = main_fields.get(key)
            target_value = target_fields.get(key)
            if key not in main_fields and key in target_fields:
                change_type = "removed"
            elif key in main_fields and key not in target_fields:
                change_type = "added"
            elif main_value == target_value:
                change_type = "same"
            else:
                change_type = "changed"
            diff_fields.append(
                {
                    "field": key,
                    "main_value": main_value,
                    "target_value": target_value,
                    "change_type": change_type,
                }
            )

        return {
            "sku": sku_value,
            "main_market": main_market,
            "target_market": target_market,
            "version_main": str(main_row["id"]),
            "version_target": str(target_row["id"]) if target_row else None,
            "fields": diff_fields,
            "created_at": datetime.now(timezone.utc),
        }
    finally:
        conn.close()


def sync_content(*, sku: str, payload: dict):
    ensure_v2_schema()
    sku_value = (sku or "").strip()
    if not sku_value:
        raise ValueError("sku is required")

    from_market = str(payload.get("from_market") or "").strip().upper()
    to_markets = [str(m).strip().upper() for m in (payload.get("to_markets") or []) if str(m).strip()]
    fields = [str(f).strip() for f in (payload.get("fields") or []) if str(f).strip()]
    overwrite_mode = str(payload.get("overwrite_mode") or "missing_only").strip().lower()

    if not from_market:
        raise ValueError("from_market is required")
    if not to_markets:
        raise ValueError("to_markets is required")
    if not fields:
        raise ValueError("fields is required")
    if overwrite_mode not in {"missing_only", "force"}:
        raise ValueError("overwrite_mode must be one of: missing_only, force")

    conn = _connect()
    try:
        cur = conn.cursor()
        source_row = _get_latest_version_for_market(cur, sku_value, from_market)
        if not source_row:
            raise ValueError("source market version not found")
        source_fields = _json_load(source_row.get("fields_json"))
        source_asin = source_row.get("asin")

        drafts_created = 0
        skipped = 0
        warnings: list[str] = []

        for target_market in to_markets:
            if target_market == from_market:
                skipped += 1
                warnings.append(f"skipped {target_market}: same as source market")
                continue

            target_row = _get_latest_version_for_market(cur, sku_value, target_market)
            target_fields = _json_load(target_row.get("fields_json")) if target_row else {}
            new_fields = dict(target_fields)

            changed = False
            for field_name in fields:
                source_val = source_fields.get(field_name)
                target_val = target_fields.get(field_name)

                if overwrite_mode == "missing_only":
                    if _is_missing_value(target_val) and not _is_missing_value(source_val):
                        new_fields[field_name] = source_val
                        changed = True
                else:  # force
                    if target_val != source_val:
                        new_fields[field_name] = source_val
                        changed = True

            if not changed:
                skipped += 1
                warnings.append(f"skipped {target_market}: no field changes")
                continue

            cur.execute(
                """
                SELECT ISNULL(MAX(version_no), 0)
                FROM dbo.acc_co_versions WITH (NOLOCK)
                WHERE sku = ?
                  AND marketplace_id = ?
                """,
                (sku_value, target_market),
            )
            next_version = int((cur.fetchone() or [0])[0] or 0) + 1
            new_id = str(uuid.uuid4())
            parent_version_id = str(target_row["id"]) if target_row else None
            asin = (target_row.get("asin") if target_row else None) or source_asin

            cur.execute(
                """
                INSERT INTO dbo.acc_co_versions
                    (id, sku, asin, marketplace_id, version_no, status, fields_json, compliance_notes, created_by, parent_version_id)
                VALUES
                    (?, ?, ?, ?, ?, 'draft', ?, ?, ?, ?)
                """,
                (
                    new_id,
                    sku_value,
                    asin,
                    target_market,
                    next_version,
                    json.dumps(new_fields, ensure_ascii=True),
                    new_fields.get("compliance_notes") if isinstance(new_fields, dict) else None,
                    settings.DEFAULT_ACTOR,
                    parent_version_id,
                ),
            )
            drafts_created += 1

        conn.commit()
        return {
            "sku": sku_value,
            "from_market": from_market,
            "to_markets": to_markets,
            "drafts_created": drafts_created,
            "skipped": skipped,
            "warnings": warnings,
        }
    finally:
        conn.close()

