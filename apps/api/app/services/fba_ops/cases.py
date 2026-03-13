from __future__ import annotations

import json
import uuid
from typing import Any

from ._helpers import (
    _connect,
    _fetchall_dict,
    _fetchone_dict,
    _iso_now,
    _marketplace_code,
    _parse_json,
    _to_float,
    _to_int,
    ensure_fba_schema,
)


# ──────────── Shipment Plans ────────────

def list_shipment_plans(*, quarter: str | None = None, marketplace_id: str | None = None, status: str | None = None) -> dict[str, Any]:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        where = ["1=1"]
        params: list[Any] = []
        if quarter:
            where.append("quarter = ?")
            params.append(quarter)
        if marketplace_id:
            where.append("marketplace_id = ?")
            params.append(marketplace_id)
        if status:
            where.append("status = ?")
            params.append(status)
        cur.execute(
            f"""
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                quarter, marketplace_id, shipment_id, plan_week_start, planned_ship_date,
                planned_units, actual_ship_date, actual_units, tolerance_pct, status, owner, notes_json, updated_at
            FROM dbo.acc_fba_shipment_plan WITH (NOLOCK)
            WHERE {" AND ".join(where)}
            ORDER BY quarter DESC, plan_week_start ASC, shipment_id ASC
            """,
            params,
        )
        rows = _fetchall_dict(cur)
        items = [
            {
                **row,
                "marketplace_code": _marketplace_code(row.get("marketplace_id")),
                "tolerance_pct": _to_float(row.get("tolerance_pct"), 0.10),
                "planned_units": _to_int(row.get("planned_units")),
                "actual_units": _to_int(row.get("actual_units")) if row.get("actual_units") is not None else None,
                "notes_json": _parse_json(row.get("notes_json"), {}),
            }
            for row in rows
        ]
        return {"total": len(items), "items": items}
    finally:
        conn.close()


def create_shipment_plan(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_fba_schema()
    record_id = str(uuid.uuid4())
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO dbo.acc_fba_shipment_plan
            (id, quarter, marketplace_id, shipment_id, plan_week_start, planned_ship_date, planned_units,
             actual_ship_date, actual_units, tolerance_pct, status, owner, notes_json, updated_at)
            VALUES
            (CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME())
            """,
            (
                record_id,
                payload.get("quarter"),
                payload.get("marketplace_id"),
                payload.get("shipment_id"),
                payload.get("plan_week_start"),
                payload.get("planned_ship_date"),
                _to_int(payload.get("planned_units")),
                payload.get("actual_ship_date"),
                _to_int(payload.get("actual_units")) if payload.get("actual_units") is not None else None,
                _to_float(payload.get("tolerance_pct"), 0.10),
                payload.get("status") or "planned",
                payload.get("owner"),
                json.dumps(payload.get("notes_json") or {}, ensure_ascii=True),
            ),
        )
        conn.commit()
        return get_shipment_plan(record_id)
    finally:
        conn.close()


def get_shipment_plan(record_id: str) -> dict[str, Any]:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                quarter, marketplace_id, shipment_id, plan_week_start, planned_ship_date,
                planned_units, actual_ship_date, actual_units, tolerance_pct, status, owner, notes_json, updated_at
            FROM dbo.acc_fba_shipment_plan WITH (NOLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (record_id,),
        )
        row = _fetchone_dict(cur)
        if not row:
            raise ValueError("shipment plan not found")
        return {
            **row,
            "marketplace_code": _marketplace_code(row.get("marketplace_id")),
            "notes_json": _parse_json(row.get("notes_json"), {}),
        }
    finally:
        conn.close()


def update_shipment_plan(record_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_fba_schema()
    if not payload:
        return get_shipment_plan(record_id)
    conn = _connect()
    try:
        cur = conn.cursor()
        set_parts: list[str] = []
        params: list[Any] = []
        mapping = [
            ("shipment_id", "shipment_id"),
            ("plan_week_start", "plan_week_start"),
            ("planned_ship_date", "planned_ship_date"),
            ("planned_units", "planned_units"),
            ("actual_ship_date", "actual_ship_date"),
            ("actual_units", "actual_units"),
            ("tolerance_pct", "tolerance_pct"),
            ("status", "status"),
            ("owner", "owner"),
        ]
        for field, column in mapping:
            if field in payload:
                set_parts.append(f"{column} = ?")
                if field in {"planned_units", "actual_units"} and payload[field] is not None:
                    params.append(_to_int(payload[field]))
                elif field == "tolerance_pct" and payload[field] is not None:
                    params.append(_to_float(payload[field]))
                else:
                    params.append(payload[field])
        if "notes_json" in payload:
            set_parts.append("notes_json = ?")
            params.append(json.dumps(payload.get("notes_json") or {}, ensure_ascii=True))
        set_parts.append("updated_at = SYSUTCDATETIME()")
        params.append(record_id)
        cur.execute(
            "UPDATE dbo.acc_fba_shipment_plan SET " + ", ".join(set_parts) + " WHERE id = CAST(? AS UNIQUEIDENTIFIER)",
            params,
        )
        if _to_int(cur.rowcount) <= 0:
            raise ValueError("shipment plan not found")
        conn.commit()
        return get_shipment_plan(record_id)
    finally:
        conn.close()


def delete_shipment_plan(record_id: str) -> bool:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM dbo.acc_fba_shipment_plan WHERE id = CAST(? AS UNIQUEIDENTIFIER)", (record_id,))
        deleted = _to_int(cur.rowcount) > 0
        conn.commit()
        return deleted
    finally:
        conn.close()


# ──────────── Cases ────────────

def _insert_case_event(
    cur,
    *,
    case_id: str,
    event_type: str,
    actor: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    cur.execute(
        """
        INSERT INTO dbo.acc_fba_case_event
        (id, case_id, event_type, event_at, actor, payload_json)
        VALUES
        (CAST(? AS UNIQUEIDENTIFIER), CAST(? AS UNIQUEIDENTIFIER), ?, SYSUTCDATETIME(), ?, ?)
        """,
        (
            str(uuid.uuid4()),
            case_id,
            event_type,
            actor,
            json.dumps(payload or {}, ensure_ascii=True),
        ),
    )


def _get_case_comment_event(record_id: str, event_id: str) -> dict[str, Any]:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                CAST(case_id AS NVARCHAR(40)) AS case_id,
                event_type,
                event_at,
                actor,
                payload_json
            FROM dbo.acc_fba_case_event WITH (NOLOCK)
            WHERE case_id = CAST(? AS UNIQUEIDENTIFIER)
              AND id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (record_id, event_id),
        )
        row = _fetchone_dict(cur)
        if not row:
            raise ValueError("comment event not found")
        if row.get("event_type") != "comment":
            raise ValueError("event is not a comment")
        row["payload_json"] = _parse_json(row.get("payload_json"), {})
        return row
    finally:
        conn.close()


def list_cases(*, status: str | None = None, case_type: str | None = None, owner: str | None = None) -> dict[str, Any]:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        where = ["1=1"]
        params: list[Any] = []
        if status:
            where.append("status = ?")
            params.append(status)
        if case_type:
            where.append("case_type = ?")
            params.append(case_type)
        if owner:
            where.append("owner = ?")
            params.append(owner)
        cur.execute(
            f"""
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                case_type, marketplace_id, entity_type, entity_id, sku, detected_date, close_date,
                owner, status, root_cause, payload_json, created_at, updated_at
            FROM dbo.acc_fba_case WITH (NOLOCK)
            WHERE {" AND ".join(where)}
            ORDER BY detected_date DESC, created_at DESC
            """,
            params,
        )
        rows = _fetchall_dict(cur)
        items = [{**row, "marketplace_code": _marketplace_code(row.get("marketplace_id")), "payload_json": _parse_json(row.get("payload_json"), {})} for row in rows]
        return {"total": len(items), "items": items}
    finally:
        conn.close()


def create_case(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_fba_schema()
    record_id = str(uuid.uuid4())
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO dbo.acc_fba_case
            (id, case_type, marketplace_id, entity_type, entity_id, sku, detected_date, close_date, owner, status, root_cause, payload_json, created_at, updated_at)
            VALUES
            (CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME())
            """,
            (
                record_id,
                payload.get("case_type"),
                payload.get("marketplace_id"),
                payload.get("entity_type"),
                payload.get("entity_id"),
                payload.get("sku"),
                payload.get("detected_date"),
                payload.get("close_date"),
                payload.get("owner"),
                payload.get("status") or "open",
                payload.get("root_cause"),
                json.dumps(payload.get("payload_json") or {}, ensure_ascii=True),
            ),
        )
        _insert_case_event(
            cur,
            case_id=record_id,
            event_type="created",
            actor=payload.get("owner"),
            payload={"status": payload.get("status") or "open", "root_cause": payload.get("root_cause")},
        )
        conn.commit()
        return get_case(record_id)
    finally:
        conn.close()


def get_case(record_id: str) -> dict[str, Any]:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                case_type, marketplace_id, entity_type, entity_id, sku, detected_date, close_date,
                owner, status, root_cause, payload_json, created_at, updated_at
            FROM dbo.acc_fba_case WITH (NOLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (record_id,),
        )
        row = _fetchone_dict(cur)
        if not row:
            raise ValueError("case not found")
        return {
            **row,
            "marketplace_code": _marketplace_code(row.get("marketplace_id")),
            "payload_json": _parse_json(row.get("payload_json"), {}),
        }
    finally:
        conn.close()


def update_case(record_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_fba_schema()
    if not payload:
        return get_case(record_id)
    conn = _connect()
    try:
        cur = conn.cursor()
        before = get_case(record_id)
        set_parts: list[str] = []
        params: list[Any] = []
        for field in ("close_date", "owner", "status", "root_cause"):
            if field in payload:
                set_parts.append(f"{field} = ?")
                params.append(payload[field])
        if "payload_json" in payload:
            set_parts.append("payload_json = ?")
            params.append(json.dumps(payload.get("payload_json") or {}, ensure_ascii=True))
        set_parts.append("updated_at = SYSUTCDATETIME()")
        params.append(record_id)
        cur.execute("UPDATE dbo.acc_fba_case SET " + ", ".join(set_parts) + " WHERE id = CAST(? AS UNIQUEIDENTIFIER)", params)
        if _to_int(cur.rowcount) <= 0:
            raise ValueError("case not found")
        changed_fields = {
            key: {"before": before.get(key), "after": payload.get(key)}
            for key in ("close_date", "owner", "status", "root_cause")
            if key in payload and before.get(key) != payload.get(key)
        }
        if "payload_json" in payload:
            changed_fields["payload_json"] = {"before": before.get("payload_json"), "after": payload.get("payload_json")}
        if changed_fields:
            _insert_case_event(
                cur,
                case_id=record_id,
                event_type="updated",
                actor=payload.get("owner") or before.get("owner"),
                payload=changed_fields,
            )
        conn.commit()
        return get_case(record_id)
    finally:
        conn.close()


def delete_case(record_id: str) -> bool:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM dbo.acc_fba_case WHERE id = CAST(? AS UNIQUEIDENTIFIER)", (record_id,))
        deleted = _to_int(cur.rowcount) > 0
        conn.commit()
        return deleted
    finally:
        conn.close()


def get_case_timeline(record_id: str) -> dict[str, Any]:
    ensure_fba_schema()
    case_item = get_case(record_id)
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                CAST(case_id AS NVARCHAR(40)) AS case_id,
                event_type,
                event_at,
                actor,
                payload_json
            FROM dbo.acc_fba_case_event WITH (NOLOCK)
            WHERE case_id = CAST(? AS UNIQUEIDENTIFIER)
            ORDER BY event_at ASC
            """,
            (record_id,),
        )
        events = _fetchall_dict(cur)
    finally:
        conn.close()
    return {
        "case": case_item,
        "events": [
            {
                **row,
                "payload_json": _parse_json(row.get("payload_json"), {}),
            }
            for row in events
        ],
    }


def add_case_comment(record_id: str, comment: str, author: str | None = None) -> dict[str, Any]:
    ensure_fba_schema()
    text = str(comment or "").strip()
    if not text:
        raise ValueError("comment is required")
    case_item = get_case(record_id)
    conn = _connect()
    try:
        cur = conn.cursor()
        _insert_case_event(
            cur,
            case_id=record_id,
            event_type="comment",
            actor=author or case_item.get("owner"),
            payload={"comment": text},
        )
        conn.commit()
        return get_case_timeline(record_id)
    finally:
        conn.close()


def update_case_comment(record_id: str, event_id: str, comment: str, author: str | None = None) -> dict[str, Any]:
    ensure_fba_schema()
    text = str(comment or "").strip()
    if not text:
        raise ValueError("comment is required")
    event = _get_case_comment_event(record_id, event_id)
    payload = dict(event.get("payload_json") or {})
    if payload.get("deleted"):
        raise ValueError("comment was deleted")
    previous_comment = str(payload.get("comment") or "")
    payload["comment"] = text
    payload["edited_at"] = _iso_now()
    payload["edited_by"] = author or event.get("actor")
    if previous_comment and previous_comment != text and "original_comment" not in payload:
        payload["original_comment"] = previous_comment
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE dbo.acc_fba_case_event
            SET actor = ?, payload_json = ?
            WHERE case_id = CAST(? AS UNIQUEIDENTIFIER)
              AND id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (
                author or event.get("actor"),
                json.dumps(payload, ensure_ascii=True),
                record_id,
                event_id,
            ),
        )
        if _to_int(cur.rowcount) <= 0:
            raise ValueError("comment event not found")
        conn.commit()
        return get_case_timeline(record_id)
    finally:
        conn.close()


def delete_case_comment(record_id: str, event_id: str, author: str | None = None) -> dict[str, Any]:
    ensure_fba_schema()
    event = _get_case_comment_event(record_id, event_id)
    payload = dict(event.get("payload_json") or {})
    if payload.get("deleted"):
        return get_case_timeline(record_id)
    payload["deleted"] = True
    payload["deleted_at"] = _iso_now()
    payload["deleted_by"] = author or event.get("actor")
    payload["comment"] = ""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE dbo.acc_fba_case_event
            SET actor = ?, payload_json = ?
            WHERE case_id = CAST(? AS UNIQUEIDENTIFIER)
              AND id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (
                author or event.get("actor"),
                json.dumps(payload, ensure_ascii=True),
                record_id,
                event_id,
            ),
        )
        if _to_int(cur.rowcount) <= 0:
            raise ValueError("comment event not found")
        conn.commit()
        return get_case_timeline(record_id)
    finally:
        conn.close()


# ──────────── Launches ────────────

def list_launches(*, quarter: str | None = None, status: str | None = None) -> dict[str, Any]:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        where = ["1=1"]
        params: list[Any] = []
        if quarter:
            where.append("quarter = ?")
            params.append(quarter)
        if status:
            where.append("status = ?")
            params.append(status)
        cur.execute(
            f"""
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                quarter, launch_type, sku, CAST(bundle_id AS NVARCHAR(40)) AS bundle_id, marketplace_id,
                planned_go_live_date, actual_go_live_date, live_stable_at, incident_free, vine_eligible,
                vine_eligible_at, vine_submitted_at, owner, status, payload_json, created_at, updated_at
            FROM dbo.acc_fba_launch WITH (NOLOCK)
            WHERE {" AND ".join(where)}
            ORDER BY quarter DESC, planned_go_live_date ASC, created_at DESC
            """,
            params,
        )
        rows = _fetchall_dict(cur)
        items = [
            {
                **row,
                "marketplace_code": _marketplace_code(row.get("marketplace_id")),
                "incident_free": bool(row.get("incident_free")),
                "vine_eligible": bool(row.get("vine_eligible")),
                "payload_json": _parse_json(row.get("payload_json"), {}),
            }
            for row in rows
        ]
        return {"total": len(items), "items": items}
    finally:
        conn.close()


def create_launch(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_fba_schema()
    record_id = str(uuid.uuid4())
    conn = _connect()
    try:
        cur = conn.cursor()
        bundle_id = payload.get("bundle_id")
        cur.execute(
            """
            INSERT INTO dbo.acc_fba_launch
            (id, quarter, launch_type, sku, bundle_id, marketplace_id, planned_go_live_date, actual_go_live_date,
             live_stable_at, incident_free, vine_eligible, vine_eligible_at, vine_submitted_at, owner, status,
             payload_json, created_at, updated_at)
            VALUES
            (CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME())
            """,
            (
                record_id,
                payload.get("quarter"),
                payload.get("launch_type") or "new_sku",
                payload.get("sku"),
                bundle_id,
                payload.get("marketplace_id"),
                payload.get("planned_go_live_date"),
                payload.get("actual_go_live_date"),
                payload.get("live_stable_at"),
                1 if payload.get("incident_free", True) else 0,
                1 if payload.get("vine_eligible", False) else 0,
                payload.get("vine_eligible_at"),
                payload.get("vine_submitted_at"),
                payload.get("owner"),
                payload.get("status") or "planned",
                json.dumps(payload.get("payload_json") or {}, ensure_ascii=True),
            ),
        )
        conn.commit()
        return get_launch(record_id)
    finally:
        conn.close()


def get_launch(record_id: str) -> dict[str, Any]:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                quarter, launch_type, CAST(bundle_id AS NVARCHAR(40)) AS bundle_id, sku, marketplace_id,
                planned_go_live_date, actual_go_live_date, live_stable_at, incident_free, vine_eligible,
                vine_eligible_at, vine_submitted_at, owner, status, payload_json, created_at, updated_at
            FROM dbo.acc_fba_launch WITH (NOLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (record_id,),
        )
        row = _fetchone_dict(cur)
        if not row:
            raise ValueError("launch not found")
        return {
            **row,
            "marketplace_code": _marketplace_code(row.get("marketplace_id")),
            "payload_json": _parse_json(row.get("payload_json"), {}),
            "incident_free": bool(row.get("incident_free")),
            "vine_eligible": bool(row.get("vine_eligible")),
        }
    finally:
        conn.close()


def update_launch(record_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_fba_schema()
    if not payload:
        return get_launch(record_id)
    conn = _connect()
    try:
        cur = conn.cursor()
        set_parts: list[str] = []
        params: list[Any] = []
        for field in ("actual_go_live_date", "live_stable_at", "vine_eligible_at", "vine_submitted_at", "owner", "status"):
            if field in payload:
                set_parts.append(f"{field} = ?")
                params.append(payload[field])
        if "incident_free" in payload:
            set_parts.append("incident_free = ?")
            params.append(1 if payload.get("incident_free") else 0)
        if "vine_eligible" in payload:
            set_parts.append("vine_eligible = ?")
            params.append(1 if payload.get("vine_eligible") else 0)
        if "payload_json" in payload:
            set_parts.append("payload_json = ?")
            params.append(json.dumps(payload.get("payload_json") or {}, ensure_ascii=True))
        set_parts.append("updated_at = SYSUTCDATETIME()")
        params.append(record_id)
        cur.execute("UPDATE dbo.acc_fba_launch SET " + ", ".join(set_parts) + " WHERE id = CAST(? AS UNIQUEIDENTIFIER)", params)
        if _to_int(cur.rowcount) <= 0:
            raise ValueError("launch not found")
        conn.commit()
        return get_launch(record_id)
    finally:
        conn.close()


def delete_launch(record_id: str) -> bool:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM dbo.acc_fba_launch WHERE id = CAST(? AS UNIQUEIDENTIFIER)", (record_id,))
        deleted = _to_int(cur.rowcount) > 0
        conn.commit()
        return deleted
    finally:
        conn.close()


# ──────────── Initiatives ────────────

def list_initiatives(*, quarter: str | None = None, status: str | None = None) -> dict[str, Any]:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        where = ["1=1"]
        params: list[Any] = []
        if quarter:
            where.append("quarter = ?")
            params.append(quarter)
        if status:
            where.append("status = ?")
            params.append(status)
        cur.execute(
            f"""
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                quarter, initiative_type, title, sku, CAST(bundle_id AS NVARCHAR(40)) AS bundle_id,
                owner, status, planned, approved, live_stable_at, created_at, updated_at
            FROM dbo.acc_fba_initiative WITH (NOLOCK)
            WHERE {" AND ".join(where)}
            ORDER BY quarter DESC, created_at DESC
            """,
            params,
        )
        rows = _fetchall_dict(cur)
        items = [{**row, "planned": bool(row.get("planned")), "approved": bool(row.get("approved"))} for row in rows]
        return {"total": len(items), "items": items}
    finally:
        conn.close()


def create_initiative(payload: dict[str, Any]) -> dict[str, Any]:
    ensure_fba_schema()
    record_id = str(uuid.uuid4())
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO dbo.acc_fba_initiative
            (id, quarter, initiative_type, title, sku, bundle_id, owner, status, planned, approved, live_stable_at, created_at, updated_at)
            VALUES
            (CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME())
            """,
            (
                record_id,
                payload.get("quarter"),
                payload.get("initiative_type"),
                payload.get("title"),
                payload.get("sku"),
                payload.get("bundle_id"),
                payload.get("owner"),
                payload.get("status") or "planned",
                1 if payload.get("planned", True) else 0,
                1 if payload.get("approved", True) else 0,
                payload.get("live_stable_at"),
            ),
        )
        conn.commit()
        return get_initiative(record_id)
    finally:
        conn.close()


def get_initiative(record_id: str) -> dict[str, Any]:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                quarter, initiative_type, title, sku, CAST(bundle_id AS NVARCHAR(40)) AS bundle_id,
                owner, status, planned, approved, live_stable_at, created_at, updated_at
            FROM dbo.acc_fba_initiative WITH (NOLOCK)
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (record_id,),
        )
        row = _fetchone_dict(cur)
        if not row:
            raise ValueError("initiative not found")
        return {
            **row,
            "planned": bool(row.get("planned")),
            "approved": bool(row.get("approved")),
        }
    finally:
        conn.close()


def update_initiative(record_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_fba_schema()
    if not payload:
        return get_initiative(record_id)
    conn = _connect()
    try:
        cur = conn.cursor()
        set_parts: list[str] = []
        params: list[Any] = []
        for field in ("title", "owner", "status", "live_stable_at"):
            if field in payload:
                set_parts.append(f"{field} = ?")
                params.append(payload[field])
        for field in ("planned", "approved"):
            if field in payload:
                set_parts.append(f"{field} = ?")
                params.append(1 if payload.get(field) else 0)
        set_parts.append("updated_at = SYSUTCDATETIME()")
        params.append(record_id)
        cur.execute("UPDATE dbo.acc_fba_initiative SET " + ", ".join(set_parts) + " WHERE id = CAST(? AS UNIQUEIDENTIFIER)", params)
        if _to_int(cur.rowcount) <= 0:
            raise ValueError("initiative not found")
        conn.commit()
        return get_initiative(record_id)
    finally:
        conn.close()


def delete_initiative(record_id: str) -> bool:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM dbo.acc_fba_initiative WHERE id = CAST(? AS UNIQUEIDENTIFIER)", (record_id,))
        deleted = _to_int(cur.rowcount) > 0
        conn.commit()
        return deleted
    finally:
        conn.close()


# ──────────── Excel/CSV import for manual registers ────────────

def import_register_from_rows(
    *,
    register_type: str,
    rows: list[dict[str, Any]],
    quarter: str | None = None,
) -> dict[str, Any]:
    ensure_fba_schema()
    _importers = {
        "shipment_plan": _import_shipment_plans,
        "case": _import_cases,
        "launch": _import_launches,
        "initiative": _import_initiatives,
    }
    importer = _importers.get(register_type)
    if not importer:
        raise ValueError(f"Unknown register_type: {register_type}. Use: {', '.join(_importers.keys())}")
    return importer(rows=rows, quarter=quarter)


def _import_shipment_plans(*, rows: list[dict[str, Any]], quarter: str | None) -> dict[str, Any]:
    conn = _connect()
    imported, skipped, errors = 0, 0, []
    try:
        cur = conn.cursor()
        for i, row in enumerate(rows, 1):
            try:
                q = str(row.get("quarter") or quarter or "").strip()
                plan_week_start = row.get("plan_week_start")
                planned_units = _to_int(row.get("planned_units"))
                if not q or not plan_week_start:
                    skipped += 1
                    continue
                cur.execute(
                    """
                    INSERT INTO dbo.acc_fba_shipment_plan
                    (id, quarter, marketplace_id, shipment_id, plan_week_start, planned_ship_date,
                     planned_units, actual_ship_date, actual_units, tolerance_pct, status, owner, updated_at)
                    VALUES (NEWID(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME())
                    """,
                    (
                        q,
                        row.get("marketplace_id"),
                        row.get("shipment_id"),
                        plan_week_start,
                        row.get("planned_ship_date"),
                        planned_units,
                        row.get("actual_ship_date"),
                        _to_int(row.get("actual_units")) if row.get("actual_units") is not None else None,
                        _to_float(row.get("tolerance_pct"), 0.10),
                        str(row.get("status") or "planned").strip(),
                        row.get("owner"),
                    ),
                )
                imported += 1
            except Exception as exc:
                errors.append(f"Row {i}: {exc}")
        conn.commit()
    finally:
        conn.close()
    return {"imported": imported, "skipped": skipped, "errors": errors}


def _import_cases(*, rows: list[dict[str, Any]], quarter: str | None) -> dict[str, Any]:
    conn = _connect()
    imported, skipped, errors = 0, 0, []
    try:
        cur = conn.cursor()
        for i, row in enumerate(rows, 1):
            try:
                case_type = str(row.get("case_type") or "operations").strip()
                detected_date = row.get("detected_date")
                if not detected_date:
                    skipped += 1
                    continue
                cur.execute(
                    """
                    INSERT INTO dbo.acc_fba_case
                    (id, case_type, marketplace_id, entity_type, entity_id, sku,
                     detected_date, close_date, owner, status, root_cause, created_at, updated_at)
                    VALUES (NEWID(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME())
                    """,
                    (
                        case_type,
                        row.get("marketplace_id"),
                        row.get("entity_type"),
                        row.get("entity_id"),
                        row.get("sku"),
                        detected_date,
                        row.get("close_date"),
                        row.get("owner"),
                        str(row.get("status") or "open").strip(),
                        row.get("root_cause"),
                    ),
                )
                imported += 1
            except Exception as exc:
                errors.append(f"Row {i}: {exc}")
        conn.commit()
    finally:
        conn.close()
    return {"imported": imported, "skipped": skipped, "errors": errors}


def _import_launches(*, rows: list[dict[str, Any]], quarter: str | None) -> dict[str, Any]:
    conn = _connect()
    imported, skipped, errors = 0, 0, []
    try:
        cur = conn.cursor()
        for i, row in enumerate(rows, 1):
            try:
                q = str(row.get("quarter") or quarter or "").strip()
                if not q:
                    skipped += 1
                    continue
                cur.execute(
                    """
                    INSERT INTO dbo.acc_fba_launch
                    (id, quarter, launch_type, sku, marketplace_id, planned_go_live_date,
                     actual_go_live_date, live_stable_at, incident_free, vine_eligible,
                     vine_eligible_at, vine_submitted_at, owner, status, created_at, updated_at)
                    VALUES (NEWID(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME())
                    """,
                    (
                        q,
                        str(row.get("launch_type") or "new_sku").strip(),
                        row.get("sku"),
                        row.get("marketplace_id"),
                        row.get("planned_go_live_date"),
                        row.get("actual_go_live_date"),
                        row.get("live_stable_at"),
                        1 if str(row.get("incident_free", "1")).strip() in ("1", "true", "True", "yes") else 0,
                        1 if str(row.get("vine_eligible", "0")).strip() in ("1", "true", "True", "yes") else 0,
                        row.get("vine_eligible_at"),
                        row.get("vine_submitted_at"),
                        row.get("owner"),
                        str(row.get("status") or "planned").strip(),
                    ),
                )
                imported += 1
            except Exception as exc:
                errors.append(f"Row {i}: {exc}")
        conn.commit()
    finally:
        conn.close()
    return {"imported": imported, "skipped": skipped, "errors": errors}


def _import_initiatives(*, rows: list[dict[str, Any]], quarter: str | None) -> dict[str, Any]:
    conn = _connect()
    imported, skipped, errors = 0, 0, []
    try:
        cur = conn.cursor()
        for i, row in enumerate(rows, 1):
            try:
                q = str(row.get("quarter") or quarter or "").strip()
                title = str(row.get("title") or "").strip()
                if not q or not title:
                    skipped += 1
                    continue
                cur.execute(
                    """
                    INSERT INTO dbo.acc_fba_initiative
                    (id, quarter, initiative_type, title, sku, owner, status,
                     planned, approved, live_stable_at, created_at, updated_at)
                    VALUES (NEWID(), ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME())
                    """,
                    (
                        q,
                        str(row.get("initiative_type") or "operational").strip(),
                        title,
                        row.get("sku"),
                        row.get("owner"),
                        str(row.get("status") or "planned").strip(),
                        1 if str(row.get("planned", "1")).strip() in ("1", "true", "True", "yes") else 0,
                        1 if str(row.get("approved", "1")).strip() in ("1", "true", "True", "yes") else 0,
                        row.get("live_stable_at"),
                    ),
                )
                imported += 1
            except Exception as exc:
                errors.append(f"Row {i}: {exc}")
        conn.commit()
    finally:
        conn.close()
    return {"imported": imported, "skipped": skipped, "errors": errors}
