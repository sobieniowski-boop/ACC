"""Operator Console v2 — Sprint 23-24.

Unified alert feed, generic case management, and action queue with
approval workflow.  Consolidates data from ``acc_al_alerts``,
``acc_system_alert``, ``acc_refund_anomaly``, buybox loss alerts,
and adds generic operator cases + action queue.

Tables managed:
  acc_operator_case   — Generic case/ticket for operator follow-up
  acc_action_queue    — Pending actions requiring approval before execution

Reads from (not owned):
  acc_al_alerts          — Rule-based user alerts
  acc_system_alert       — System-level guardrail alerts
  acc_refund_anomaly     — Refund/fee anomaly feed items
  acc_action_log         — Completed action audit trail
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

import structlog

from app.connectors.mssql import connect_acc

log = structlog.get_logger(__name__)

# ── Constants ────────────────────────────────────────────────────────

# Case management
CASE_CATEGORIES = {
    "refund_anomaly",
    "fee_dispute",
    "inventory_discrepancy",
    "listing_issue",
    "buybox_loss",
    "content_quality",
    "compliance",
    "other",
}
CASE_PRIORITIES = {"critical", "high", "medium", "low"}
CASE_STATUSES = {"open", "in_progress", "waiting", "resolved", "closed"}

# Action queue
ACTION_QUEUE_STATUSES = {"pending_approval", "approved", "rejected", "executed", "failed", "expired"}
AUTO_APPROVE_LOW_RISK = True  # low-risk actions auto-approved


# ── Schema DDL ───────────────────────────────────────────────────────

_SCHEMA_STATEMENTS: list[str] = [
    # Operator cases
    """
    IF OBJECT_ID('dbo.acc_operator_case', 'U') IS NULL
    CREATE TABLE dbo.acc_operator_case (
        id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
        title               NVARCHAR(300)  NOT NULL,
        description         NVARCHAR(MAX)  NULL,
        category            VARCHAR(40)    NOT NULL DEFAULT 'other',
        priority            VARCHAR(20)    NOT NULL DEFAULT 'medium',
        status              VARCHAR(20)    NOT NULL DEFAULT 'open',
        marketplace_id      VARCHAR(20)    NULL,
        sku                 VARCHAR(50)    NULL,
        asin                VARCHAR(20)    NULL,
        source_type         VARCHAR(40)    NULL,
        source_id           VARCHAR(80)    NULL,
        assigned_to         NVARCHAR(120)  NULL,
        resolution_note     NVARCHAR(MAX)  NULL,
        resolved_by         NVARCHAR(120)  NULL,
        resolved_at         DATETIME2      NULL,
        due_date            DATE           NULL,
        tags                NVARCHAR(500)  NULL,
        created_at          DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at          DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_operator_case_status')
    CREATE INDEX ix_operator_case_status ON dbo.acc_operator_case (status, priority)
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_operator_case_category')
    CREATE INDEX ix_operator_case_category ON dbo.acc_operator_case (category, created_at DESC)
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_operator_case_assigned')
    CREATE INDEX ix_operator_case_assigned ON dbo.acc_operator_case (assigned_to, status)
    """,
    # Action queue
    """
    IF OBJECT_ID('dbo.acc_action_queue', 'U') IS NULL
    CREATE TABLE dbo.acc_action_queue (
        id                  BIGINT IDENTITY(1,1) PRIMARY KEY,
        action_type         VARCHAR(80)    NOT NULL,
        title               NVARCHAR(300)  NOT NULL,
        description         NVARCHAR(MAX)  NULL,
        marketplace_id      VARCHAR(20)    NULL,
        sku                 VARCHAR(50)    NULL,
        asin                VARCHAR(20)    NULL,
        payload             NVARCHAR(MAX)  NULL,
        risk_level          VARCHAR(20)    NOT NULL DEFAULT 'medium',
        requires_approval   BIT            NOT NULL DEFAULT 1,
        status              VARCHAR(30)    NOT NULL DEFAULT 'pending_approval',
        requested_by        NVARCHAR(120)  NOT NULL,
        approved_by         NVARCHAR(120)  NULL,
        approved_at         DATETIME2      NULL,
        rejected_by         NVARCHAR(120)  NULL,
        rejected_at         DATETIME2      NULL,
        rejection_reason    NVARCHAR(500)  NULL,
        executed_at         DATETIME2      NULL,
        execution_result    NVARCHAR(MAX)  NULL,
        error_message       NVARCHAR(500)  NULL,
        expires_at          DATETIME2      NULL,
        created_at          DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at          DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_action_queue_status')
    CREATE INDEX ix_action_queue_status ON dbo.acc_action_queue (status, created_at DESC)
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_action_queue_type')
    CREATE INDEX ix_action_queue_type ON dbo.acc_action_queue (action_type, status)
    """,
]


def ensure_operator_console_schema() -> None:
    conn = connect_acc(autocommit=True)
    try:
        cur = conn.cursor()
        for stmt in _SCHEMA_STATEMENTS:
            cur.execute(stmt)
        cur.close()
    finally:
        conn.close()


# ── Unified Alert Feed ───────────────────────────────────────────────

def get_unified_feed(
    *,
    days: int = 7,
    severity: str | None = None,
    marketplace_id: str | None = None,
    source: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """Return a unified feed combining multiple alert sources.

    Sources:
      1. acc_al_alerts          (source='alert')
      2. acc_system_alert       (source='system')
      3. acc_refund_anomaly     (source='anomaly')
    """
    conn = connect_acc()
    try:
        cur = conn.cursor()
        since = datetime.now(timezone.utc) - timedelta(days=days)
        offset = (page - 1) * page_size

        # Build UNION query across sources
        parts: list[str] = []
        params: list[Any] = []

        if source is None or source == "alert":
            frag = """
                SELECT 'alert' AS source, CAST(a.id AS VARCHAR(40)) AS source_id,
                       a.title, a.detail AS description, a.severity,
                       a.marketplace_id, NULL AS sku, NULL AS asin,
                       CASE WHEN a.is_resolved = 1 THEN 'resolved' ELSE 'open' END AS status,
                       a.created_at
                FROM dbo.acc_al_alerts a
                WHERE a.created_at >= %s
            """
            params.append(since)
            if severity:
                frag += " AND a.severity = %s"
                params.append(severity)
            if marketplace_id:
                frag += " AND a.marketplace_id = %s"
                params.append(marketplace_id)
            parts.append(frag)

        if source is None or source == "system":
            frag = """
                SELECT 'system' AS source, CAST(sa.id AS VARCHAR(40)) AS source_id,
                       sa.alert_type AS title, sa.message AS description,
                       sa.severity,
                       NULL AS marketplace_id, NULL AS sku, NULL AS asin,
                       'open' AS status,
                       sa.created_at
                FROM dbo.acc_system_alert sa
                WHERE sa.created_at >= %s
            """
            params.append(since)
            if severity:
                frag += " AND sa.severity = %s"
                params.append(severity)
            parts.append(frag)

        if source is None or source == "anomaly":
            frag = """
                SELECT 'anomaly' AS source, CAST(ra.id AS VARCHAR(40)) AS source_id,
                       ra.anomaly_type + ': ' + ra.sku AS title,
                       'Spike ratio ' + CAST(ra.spike_ratio AS VARCHAR(20)) AS description,
                       ra.severity,
                       ra.marketplace_id, ra.sku, ra.asin,
                       ra.status,
                       ra.created_at
                FROM dbo.acc_refund_anomaly ra
                WHERE ra.created_at >= %s
            """
            params.append(since)
            if severity:
                frag += " AND ra.severity = %s"
                params.append(severity)
            if marketplace_id:
                frag += " AND ra.marketplace_id = %s"
                params.append(marketplace_id)
            parts.append(frag)

        if not parts:
            return {"items": [], "total": 0, "page": page, "page_size": page_size}

        union_sql = " UNION ALL ".join(parts)

        # Count
        count_sql = f"SELECT COUNT(*) FROM ({union_sql}) AS feed"
        cur.execute(count_sql, tuple(params))
        row = cur.fetchone()
        total = row[0] if row else 0

        # Page
        page_sql = f"""
            SELECT * FROM ({union_sql}) AS feed
            ORDER BY feed.created_at DESC
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
        """
        page_params = tuple(params) + (offset, page_size)
        cur.execute(page_sql, page_params)
        rows = cur.fetchall()

        items = []
        for r in rows:
            items.append({
                "source": r[0],
                "source_id": r[1],
                "title": r[2],
                "description": r[3],
                "severity": r[4],
                "marketplace_id": r[5],
                "sku": r[6],
                "asin": r[7],
                "status": r[8],
                "created_at": r[9].isoformat() if isinstance(r[9], (date, datetime)) else r[9],
            })

        cur.close()
        return {"items": items, "total": total, "page": page, "page_size": page_size}
    finally:
        conn.close()


def get_feed_summary(*, days: int = 7) -> dict[str, Any]:
    """KPI summary across all feed sources."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        since = datetime.now(timezone.utc) - timedelta(days=days)

        cur.execute("""
            SELECT COUNT(*),
                   SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN is_resolved = 0 THEN 1 ELSE 0 END)
            FROM dbo.acc_al_alerts
            WHERE created_at >= %s
        """, (since,))
        al = cur.fetchone() or (0, 0, 0)

        cur.execute("""
            SELECT COUNT(*),
                   SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END)
            FROM dbo.acc_system_alert
            WHERE created_at >= %s
        """, (since,))
        sys_al = cur.fetchone() or (0, 0)

        cur.execute("""
            SELECT COUNT(*),
                   SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END)
            FROM dbo.acc_refund_anomaly
            WHERE created_at >= %s
        """, (since,))
        anom = cur.fetchone() or (0, 0, 0)

        cur.execute("""
            SELECT COUNT(*),
                   SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN priority = 'critical' THEN 1 ELSE 0 END)
            FROM dbo.acc_operator_case
            WHERE created_at >= %s
        """, (since,))
        cases = cur.fetchone() or (0, 0, 0)

        cur.execute("""
            SELECT COUNT(*),
                   SUM(CASE WHEN status = 'pending_approval' THEN 1 ELSE 0 END)
            FROM dbo.acc_action_queue
            WHERE created_at >= %s
        """, (since,))
        actions = cur.fetchone() or (0, 0)

        cur.close()
        return {
            "alerts": {"total": al[0] or 0, "critical": al[1] or 0, "unresolved": al[2] or 0},
            "system_alerts": {"total": sys_al[0] or 0, "critical": sys_al[1] or 0},
            "anomalies": {"total": anom[0] or 0, "critical": anom[1] or 0, "open": anom[2] or 0},
            "cases": {"total": cases[0] or 0, "open": cases[1] or 0, "critical": cases[2] or 0},
            "action_queue": {"total": actions[0] or 0, "pending_approval": actions[1] or 0},
        }
    finally:
        conn.close()


# ── Case Management ──────────────────────────────────────────────────

def _row_to_case(row) -> dict[str, Any]:
    return {
        "id": row[0],
        "title": row[1],
        "description": row[2],
        "category": row[3],
        "priority": row[4],
        "status": row[5],
        "marketplace_id": row[6],
        "sku": row[7],
        "asin": row[8],
        "source_type": row[9],
        "source_id": row[10],
        "assigned_to": row[11],
        "resolution_note": row[12],
        "resolved_by": row[13],
        "resolved_at": row[14].isoformat() if isinstance(row[14], (date, datetime)) else row[14],
        "due_date": row[15].isoformat() if isinstance(row[15], date) else row[15],
        "tags": row[16],
        "created_at": row[17].isoformat() if isinstance(row[17], (date, datetime)) else row[17],
        "updated_at": row[18].isoformat() if isinstance(row[18], (date, datetime)) else row[18],
    }


def list_operator_cases(
    *,
    status: str | None = None,
    category: str | None = None,
    priority: str | None = None,
    assigned_to: str | None = None,
    marketplace_id: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    conn = connect_acc()
    try:
        cur = conn.cursor()
        where: list[str] = []
        params: list[Any] = []

        if status:
            where.append("status = %s")
            params.append(status)
        if category:
            where.append("category = %s")
            params.append(category)
        if priority:
            where.append("priority = %s")
            params.append(priority)
        if assigned_to:
            where.append("assigned_to = %s")
            params.append(assigned_to)
        if marketplace_id:
            where.append("marketplace_id = %s")
            params.append(marketplace_id)

        where_clause = " AND ".join(where) if where else "1=1"
        offset = (page - 1) * page_size

        cur.execute(f"SELECT COUNT(*) FROM dbo.acc_operator_case WHERE {where_clause}", tuple(params))
        total = (cur.fetchone() or (0,))[0]

        cur.execute(f"""
            SELECT id, title, description, category, priority, status,
                   marketplace_id, sku, asin, source_type, source_id,
                   assigned_to, resolution_note, resolved_by, resolved_at,
                   due_date, tags, created_at, updated_at
            FROM dbo.acc_operator_case
            WHERE {where_clause}
            ORDER BY
                CASE priority WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                created_at DESC
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
        """, tuple(params) + (offset, page_size))
        rows = cur.fetchall()
        cur.close()

        return {
            "items": [_row_to_case(r) for r in rows],
            "total": total,
            "page": page,
            "page_size": page_size,
        }
    finally:
        conn.close()


def get_operator_case(case_id: int) -> dict[str, Any] | None:
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, title, description, category, priority, status,
                   marketplace_id, sku, asin, source_type, source_id,
                   assigned_to, resolution_note, resolved_by, resolved_at,
                   due_date, tags, created_at, updated_at
            FROM dbo.acc_operator_case WHERE id = %s
        """, (case_id,))
        row = cur.fetchone()
        cur.close()
        return _row_to_case(row) if row else None
    finally:
        conn.close()


def create_operator_case(
    *,
    title: str,
    description: str | None = None,
    category: str = "other",
    priority: str = "medium",
    marketplace_id: str | None = None,
    sku: str | None = None,
    asin: str | None = None,
    source_type: str | None = None,
    source_id: str | None = None,
    assigned_to: str | None = None,
    due_date: str | None = None,
    tags: str | None = None,
) -> dict[str, Any]:
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO dbo.acc_operator_case
                (title, description, category, priority, marketplace_id,
                 sku, asin, source_type, source_id, assigned_to, due_date, tags)
            OUTPUT INSERTED.id
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            title, description, category, priority, marketplace_id,
            sku, asin, source_type, source_id, assigned_to, due_date, tags,
        ))
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        log.info("operator_case.created", id=new_id, category=category)
        return {"id": new_id, "status": "open"}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_operator_case(
    case_id: int,
    *,
    status: str | None = None,
    priority: str | None = None,
    assigned_to: str | None = None,
    resolution_note: str | None = None,
    resolved_by: str | None = None,
) -> dict[str, Any] | None:
    conn = connect_acc()
    try:
        cur = conn.cursor()
        sets: list[str] = ["updated_at = SYSUTCDATETIME()"]
        params: list[Any] = []

        if status:
            sets.append("status = %s")
            params.append(status)
            if status in ("resolved", "closed") and resolved_by:
                sets.append("resolved_by = %s")
                params.append(resolved_by)
                sets.append("resolved_at = SYSUTCDATETIME()")
        if priority:
            sets.append("priority = %s")
            params.append(priority)
        if assigned_to is not None:
            sets.append("assigned_to = %s")
            params.append(assigned_to)
        if resolution_note is not None:
            sets.append("resolution_note = %s")
            params.append(resolution_note)

        params.append(case_id)
        cur.execute(f"""
            UPDATE dbo.acc_operator_case
            SET {', '.join(sets)}
            WHERE id = %s
        """, tuple(params))
        affected = cur.rowcount
        conn.commit()
        cur.close()

        if affected == 0:
            return None
        return {"id": case_id, "updated": True}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Action Queue ─────────────────────────────────────────────────────

def _row_to_action(row) -> dict[str, Any]:
    return {
        "id": row[0],
        "action_type": row[1],
        "title": row[2],
        "description": row[3],
        "marketplace_id": row[4],
        "sku": row[5],
        "asin": row[6],
        "payload": json.loads(row[7]) if row[7] else None,
        "risk_level": row[8],
        "requires_approval": bool(row[9]),
        "status": row[10],
        "requested_by": row[11],
        "approved_by": row[12],
        "approved_at": row[13].isoformat() if isinstance(row[13], (date, datetime)) else row[13],
        "rejected_by": row[14],
        "rejected_at": row[15].isoformat() if isinstance(row[15], (date, datetime)) else row[15],
        "rejection_reason": row[16],
        "executed_at": row[17].isoformat() if isinstance(row[17], (date, datetime)) else row[17],
        "execution_result": row[18],
        "error_message": row[19],
        "expires_at": row[20].isoformat() if isinstance(row[20], (date, datetime)) else row[20],
        "created_at": row[21].isoformat() if isinstance(row[21], (date, datetime)) else row[21],
        "updated_at": row[22].isoformat() if isinstance(row[22], (date, datetime)) else row[22],
    }


_ACTION_COLS = """
    id, action_type, title, description, marketplace_id, sku, asin,
    payload, risk_level, requires_approval, status, requested_by,
    approved_by, approved_at, rejected_by, rejected_at, rejection_reason,
    executed_at, execution_result, error_message, expires_at,
    created_at, updated_at
"""


def list_action_queue(
    *,
    status: str | None = None,
    action_type: str | None = None,
    marketplace_id: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    conn = connect_acc()
    try:
        cur = conn.cursor()
        where: list[str] = []
        params: list[Any] = []

        if status:
            where.append("status = %s")
            params.append(status)
        if action_type:
            where.append("action_type = %s")
            params.append(action_type)
        if marketplace_id:
            where.append("marketplace_id = %s")
            params.append(marketplace_id)

        where_clause = " AND ".join(where) if where else "1=1"
        offset = (page - 1) * page_size

        cur.execute(f"SELECT COUNT(*) FROM dbo.acc_action_queue WHERE {where_clause}", tuple(params))
        total = (cur.fetchone() or (0,))[0]

        cur.execute(f"""
            SELECT {_ACTION_COLS}
            FROM dbo.acc_action_queue
            WHERE {where_clause}
            ORDER BY created_at DESC
            OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
        """, tuple(params) + (offset, page_size))
        rows = cur.fetchall()
        cur.close()

        return {
            "items": [_row_to_action(r) for r in rows],
            "total": total,
            "page": page,
            "page_size": page_size,
        }
    finally:
        conn.close()


def get_action_queue_item(action_id: int) -> dict[str, Any] | None:
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT {_ACTION_COLS} FROM dbo.acc_action_queue WHERE id = %s", (action_id,))
        row = cur.fetchone()
        cur.close()
        return _row_to_action(row) if row else None
    finally:
        conn.close()


def submit_action(
    *,
    action_type: str,
    title: str,
    description: str | None = None,
    marketplace_id: str | None = None,
    sku: str | None = None,
    asin: str | None = None,
    payload: dict | None = None,
    risk_level: str = "medium",
    requested_by: str,
    expires_hours: int = 72,
) -> dict[str, Any]:
    requires_approval = not (AUTO_APPROVE_LOW_RISK and risk_level == "low")
    initial_status = "pending_approval" if requires_approval else "approved"

    conn = connect_acc()
    try:
        cur = conn.cursor()
        expires_at = datetime.now(timezone.utc) + timedelta(hours=expires_hours)
        payload_json = json.dumps(payload) if payload else None

        cur.execute("""
            INSERT INTO dbo.acc_action_queue
                (action_type, title, description, marketplace_id, sku, asin,
                 payload, risk_level, requires_approval, status, requested_by, expires_at)
            OUTPUT INSERTED.id
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            action_type, title, description, marketplace_id, sku, asin,
            payload_json, risk_level, 1 if requires_approval else 0,
            initial_status, requested_by, expires_at,
        ))
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        log.info("action_queue.submitted", id=new_id, action_type=action_type, status=initial_status)
        return {"id": new_id, "status": initial_status, "requires_approval": requires_approval}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def approve_action(action_id: int, *, approved_by: str) -> dict[str, Any] | None:
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE dbo.acc_action_queue
            SET status = 'approved', approved_by = %s, approved_at = SYSUTCDATETIME(),
                updated_at = SYSUTCDATETIME()
            WHERE id = %s AND status = 'pending_approval'
        """, (approved_by, action_id))
        affected = cur.rowcount
        conn.commit()
        cur.close()
        if affected == 0:
            return None
        log.info("action_queue.approved", id=action_id, by=approved_by)
        return {"id": action_id, "status": "approved"}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def reject_action(action_id: int, *, rejected_by: str, reason: str | None = None) -> dict[str, Any] | None:
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE dbo.acc_action_queue
            SET status = 'rejected', rejected_by = %s, rejected_at = SYSUTCDATETIME(),
                rejection_reason = %s, updated_at = SYSUTCDATETIME()
            WHERE id = %s AND status = 'pending_approval'
        """, (rejected_by, reason, action_id))
        affected = cur.rowcount
        conn.commit()
        cur.close()
        if affected == 0:
            return None
        log.info("action_queue.rejected", id=action_id, by=rejected_by)
        return {"id": action_id, "status": "rejected"}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def mark_action_executed(
    action_id: int,
    *,
    result: str | None = None,
    error: str | None = None,
) -> dict[str, Any] | None:
    new_status = "failed" if error else "executed"
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE dbo.acc_action_queue
            SET status = %s, executed_at = SYSUTCDATETIME(),
                execution_result = %s, error_message = %s,
                updated_at = SYSUTCDATETIME()
            WHERE id = %s AND status = 'approved'
        """, (new_status, result, error, action_id))
        affected = cur.rowcount
        conn.commit()
        cur.close()
        if affected == 0:
            return None
        return {"id": action_id, "status": new_status}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def expire_stale_actions() -> int:
    """Mark expired pending actions. Called by scheduler."""
    conn = connect_acc()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE dbo.acc_action_queue
            SET status = 'expired', updated_at = SYSUTCDATETIME()
            WHERE status = 'pending_approval'
              AND expires_at IS NOT NULL
              AND expires_at < SYSUTCDATETIME()
        """)
        count = cur.rowcount
        conn.commit()
        cur.close()
        if count:
            log.info("action_queue.expired_stale", count=count)
        return count
    finally:
        conn.close()


def get_operator_dashboard() -> dict[str, Any]:
    """Full dashboard KPIs for the operator console."""
    return get_feed_summary(days=7)
