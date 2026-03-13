"""Content Ops - task queue / async job management."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

import pyodbc

from app.connectors.mssql.mssql_store import ensure_v2_schema
from app.core.config import settings
from ._helpers import (
    _connect, _fetchall_dict, _json_load,
    _normalize_status, _normalize_priority, _normalize_task_type,
    _status_transition_allowed, _map_task_row,
    _ensure_co_internal_sku_columns, resolve_internal_sku,
)


def _auto_assign_owner(
    cur: pyodbc.Cursor,
    *,
    task_type: str,
    marketplace_id: str | None,
    sku: str,
) -> str | None:
    brand = _detect_brand_for_sku(cur, sku)

    # Prefer rules matching content-specific task_type or wildcard.
    cur.execute(
        """
        SELECT TOP 1 owner
        FROM dbo.acc_al_task_owner_rules WITH (NOLOCK)
        WHERE is_active = 1
          AND (task_type IS NULL OR task_type = ? OR task_type = 'content')
          AND (marketplace_id IS NULL OR marketplace_id = ?)
          AND (brand IS NULL OR brand = ?)
        ORDER BY
          priority ASC,
          CASE WHEN brand IS NOT NULL THEN 0 ELSE 1 END,
          CASE WHEN marketplace_id IS NOT NULL THEN 0 ELSE 1 END,
          CASE WHEN task_type IS NOT NULL THEN 0 ELSE 1 END
        """,
        (task_type, marketplace_id, brand),
    )
    row = cur.fetchone()
    if row and row[0]:
        return str(row[0]).strip()
    return None


def list_content_tasks(
    *,
    status: Optional[str] = None,
    owner: Optional[str] = None,
    marketplace_id: Optional[str] = None,
    task_type: Optional[str] = None,
    priority: Optional[str] = None,
    sku_search: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
):
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        where = ["1=1"]
        params: list[Any] = []

        if status:
            where.append("t.status = ?")
            params.append(_normalize_status(status))
        if owner:
            where.append("t.owner = ?")
            params.append(owner)
        if marketplace_id:
            where.append("t.marketplace_id = ?")
            params.append(marketplace_id)
        if task_type:
            where.append("t.task_type = ?")
            params.append(_normalize_task_type(task_type))
        if priority:
            where.append("t.priority = ?")
            params.append(_normalize_priority(priority))
        if sku_search:
            where.append("(t.sku LIKE ? OR ISNULL(t.asin, '') LIKE ?)")
            params.extend([f"%{sku_search}%", f"%{sku_search}%"])

        where_sql = " AND ".join(where)
        safe_page_size = max(1, min(page_size, 200))
        safe_page = max(1, page)
        offset = (safe_page - 1) * safe_page_size

        cur.execute(f"SELECT COUNT(*) FROM dbo.acc_co_tasks t WITH (NOLOCK) WHERE {where_sql}", params)
        total = int(cur.fetchone()[0] or 0)
        pages = math.ceil(total / safe_page_size) if total else 0

        cur.execute(
            f"""
            SELECT
                t.id, t.task_type, t.sku, t.asin, t.marketplace_id,
                t.priority, t.owner, t.due_date, t.status, t.tags_json,
                t.title, t.note, t.source_page, t.created_by,
                t.created_at, t.updated_at
            FROM dbo.acc_co_tasks t WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY
                CASE t.priority
                    WHEN 'p0' THEN 0
                    WHEN 'p1' THEN 1
                    WHEN 'p2' THEN 2
                    ELSE 3
                END ASC,
                t.created_at DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """,
            (*params, offset, safe_page_size),
        )
        rows = _fetchall_dict(cur)
        return {
            "total": total,
            "page": safe_page,
            "page_size": safe_page_size,
            "pages": pages,
            "items": [_map_task_row(r) for r in rows],
        }
    finally:
        conn.close()


def create_content_task(*, payload: dict):
    ensure_v2_schema()
    task_type = _normalize_task_type(str(payload.get("type") or ""))
    sku = str(payload.get("sku") or "").strip()
    if not sku:
        raise ValueError("sku is required")

    priority = _normalize_priority(str(payload.get("priority") or "p1"))
    task_id = str(uuid.uuid4())
    owner = (payload.get("owner") or "").strip() or None
    marketplace_id = (payload.get("marketplace_id") or "").strip() or None
    asin = (payload.get("asin") or "").strip() or None
    due_date = payload.get("due_date")
    tags_json = payload.get("tags_json") or {}

    conn = _connect()
    try:
        cur = conn.cursor()
        _ensure_co_internal_sku_columns()
        internal_sku = resolve_internal_sku(cur, sku, marketplace_id)
        resolved_owner = owner or _auto_assign_owner(
            cur,
            task_type=task_type,
            marketplace_id=marketplace_id,
            sku=sku,
        )
        cur.execute(
            """
            INSERT INTO dbo.acc_co_tasks
                (id, task_type, sku, asin, marketplace_id, internal_sku, priority, owner, due_date, status, tags_json,
                 title, note, source_page, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                task_type,
                sku,
                asin,
                marketplace_id,
                internal_sku,
                priority,
                resolved_owner,
                due_date,
                json.dumps(tags_json, ensure_ascii=True),
                payload.get("title"),
                payload.get("note"),
                payload.get("source_page") or "content_dashboard",
                settings.DEFAULT_ACTOR,
            ),
        )
        conn.commit()

        cur.execute(
            """
            SELECT TOP 1
                id, task_type, sku, asin, marketplace_id, priority, owner, due_date, status, tags_json,
                title, note, source_page, created_by, created_at, updated_at, internal_sku
            FROM dbo.acc_co_tasks WITH (NOLOCK)
            WHERE id = ?
            """,
            (task_id,),
        )
        row = _fetchall_dict(cur)[0]
        return _map_task_row(row)
    finally:
        conn.close()


def update_content_task(*, task_id: str, payload: dict):
    ensure_v2_schema()
    if not payload:
        raise ValueError("no update fields provided")

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 1 status
            FROM dbo.acc_co_tasks WITH (NOLOCK)
            WHERE id = ?
            """,
            (task_id,),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError("task not found")
        current_status = _normalize_status(str(row[0] or "open"))

        updates: list[str] = []
        params: list[Any] = []

        if "status" in payload:
            next_status = _normalize_status(str(payload.get("status")))
            if not _status_transition_allowed(current_status, next_status):
                raise ValueError(
                    f"invalid status transition: {current_status} -> {next_status}. "
                    "Allowed flow: open -> investigating -> resolved"
                )
            updates.append("status = ?")
            params.append(next_status)

        if "owner" in payload:
            updates.append("owner = ?")
            params.append((payload.get("owner") or "").strip() or None)
        if "priority" in payload:
            updates.append("priority = ?")
            params.append(_normalize_priority(str(payload.get("priority"))))
        if "due_date" in payload:
            updates.append("due_date = ?")
            params.append(payload.get("due_date"))
        if "title" in payload:
            updates.append("title = ?")
            params.append(payload.get("title"))
        if "note" in payload:
            updates.append("note = ?")
            params.append(payload.get("note"))

        if not updates:
            raise ValueError("no update fields provided")

        updates.append("updated_at = SYSUTCDATETIME()")
        cur.execute(
            f"""
            UPDATE dbo.acc_co_tasks
            SET {", ".join(updates)}
            WHERE id = ?
            """,
            (*params, task_id),
        )
        conn.commit()
        if cur.rowcount == 0:
            raise ValueError("task not found")

        cur.execute(
            """
            SELECT TOP 1
                id, task_type, sku, asin, marketplace_id, priority, owner, due_date, status, tags_json,
                title, note, source_page, created_by, created_at, updated_at
            FROM dbo.acc_co_tasks WITH (NOLOCK)
            WHERE id = ?
            """,
            (task_id,),
        )
        result = _fetchall_dict(cur)[0]
        return _map_task_row(result)
    finally:
        conn.close()


def bulk_update_content_tasks(*, payload: dict):
    ensure_v2_schema()
    task_ids = [str(x).strip() for x in (payload.get("task_ids") or []) if str(x).strip()]
    if not task_ids:
        raise ValueError("task_ids is required")
    status = payload.get("status")
    owner = payload.get("owner")
    priority = payload.get("priority")

    sets: list[str] = ["updated_at = SYSUTCDATETIME()"]
    params: list[Any] = []
    if status is not None:
        sets.append("status = ?")
        params.append(_normalize_status(str(status)))
    if owner is not None:
        sets.append("owner = ?")
        params.append(str(owner).strip() or None)
    if priority is not None:
        sets.append("priority = ?")
        params.append(_normalize_priority(str(priority)))
    if len(sets) == 1:
        raise ValueError("at least one field to update is required")

    placeholders = ",".join("CAST(? AS UNIQUEIDENTIFIER)" for _ in task_ids)
    params.extend(task_ids)

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            UPDATE dbo.acc_co_tasks
            SET {", ".join(sets)}
            WHERE id IN ({placeholders})
            """,
            params,
        )
        updated = int(cur.rowcount or 0)
        conn.commit()
        return {"updated_count": updated, "task_ids": task_ids}
    finally:
        conn.close()

