"""SQS Queue Topology Engine — Sprint 19.

Multi-queue SQS topology management:
  - Per-domain queue registry (pricing, listing, order, inventory, report, feed)
  - Dead-letter queue (DLQ) strategy with max-receive-count policies
  - Multi-queue polling orchestration
  - DLQ entry tracking and resolution
  - Queue health monitoring and metrics

Tables:
  acc_sqs_queue_topology  — Per-domain queue configuration
  acc_dlq_entry           — Dead-letter tracking
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import structlog

from app.connectors.mssql import connect_acc

log = structlog.get_logger(__name__)

# ── Constants ────────────────────────────────────────────────────────

VALID_DOMAINS = {"pricing", "listing", "order", "inventory", "report", "feed"}

VALID_QUEUE_STATUSES = {"active", "paused", "error", "disabled"}

VALID_DLQ_STATUSES = {"unresolved", "replayed", "discarded", "investigating"}

VALID_DLQ_RESOLUTIONS = {"replayed", "discarded", "investigating"}

# Default queue configuration per domain
DEFAULT_QUEUE_CONFIG: dict[str, dict[str, Any]] = {
    "pricing": {
        "max_receive_count": 3,
        "visibility_timeout_seconds": 30,
        "message_retention_days": 4,
        "polling_interval_seconds": 120,
        "batch_size": 10,
    },
    "listing": {
        "max_receive_count": 3,
        "visibility_timeout_seconds": 60,
        "message_retention_days": 14,
        "polling_interval_seconds": 120,
        "batch_size": 10,
    },
    "order": {
        "max_receive_count": 5,
        "visibility_timeout_seconds": 30,
        "message_retention_days": 14,
        "polling_interval_seconds": 60,
        "batch_size": 10,
    },
    "inventory": {
        "max_receive_count": 3,
        "visibility_timeout_seconds": 30,
        "message_retention_days": 7,
        "polling_interval_seconds": 120,
        "batch_size": 10,
    },
    "report": {
        "max_receive_count": 3,
        "visibility_timeout_seconds": 120,
        "message_retention_days": 14,
        "polling_interval_seconds": 300,
        "batch_size": 5,
    },
    "feed": {
        "max_receive_count": 3,
        "visibility_timeout_seconds": 120,
        "message_retention_days": 14,
        "polling_interval_seconds": 300,
        "batch_size": 5,
    },
}

# Notification type → domain routing table
NOTIFICATION_ROUTING: dict[str, str] = {
    "ANY_OFFER_CHANGED": "pricing",
    "LISTINGS_ITEM_STATUS_CHANGE": "listing",
    "LISTINGS_ITEM_ISSUES_CHANGE": "listing",
    "REPORT_PROCESSING_FINISHED": "report",
    "FBA_INVENTORY_AVAILABILITY_CHANGES": "inventory",
    "ORDER_STATUS_CHANGE": "order",
    "FEED_PROCESSING_FINISHED": "feed",
    "ITEM_PRODUCT_TYPE_CHANGE": "listing",
    "BRANDED_ITEM_CONTENT_CHANGE": "listing",
}


# ── Schema DDL ───────────────────────────────────────────────────────

_TOPOLOGY_SCHEMA: list[str] = [
    """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_sqs_queue_topology')
    CREATE TABLE dbo.acc_sqs_queue_topology (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        domain          NVARCHAR(40)  NOT NULL,
        queue_url       NVARCHAR(500) NOT NULL,
        queue_arn       NVARCHAR(500) NULL,
        dlq_url         NVARCHAR(500) NULL,
        dlq_arn         NVARCHAR(500) NULL,
        region          NVARCHAR(20)  NOT NULL DEFAULT 'eu-west-1',
        max_receive_count INT         NOT NULL DEFAULT 3,
        visibility_timeout_seconds INT NOT NULL DEFAULT 30,
        message_retention_days INT    NOT NULL DEFAULT 14,
        polling_interval_seconds INT  NOT NULL DEFAULT 120,
        batch_size      INT           NOT NULL DEFAULT 10,
        enabled         BIT           NOT NULL DEFAULT 1,
        status          NVARCHAR(20)  NOT NULL DEFAULT 'active',
        messages_received BIGINT      NOT NULL DEFAULT 0,
        messages_processed BIGINT     NOT NULL DEFAULT 0,
        messages_failed BIGINT        NOT NULL DEFAULT 0,
        messages_dlq    BIGINT        NOT NULL DEFAULT 0,
        last_poll_at    DATETIME2     NULL,
        last_error      NVARCHAR(500) NULL,
        created_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_sqs_topology_domain UNIQUE (domain)
    );
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_dlq_entry')
    CREATE TABLE dbo.acc_dlq_entry (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        domain          NVARCHAR(40)  NOT NULL,
        queue_url       NVARCHAR(500) NOT NULL,
        message_id      NVARCHAR(200) NOT NULL,
        receipt_handle  NVARCHAR(2000) NULL,
        body            NVARCHAR(MAX) NULL,
        approximate_receive_count INT NOT NULL DEFAULT 0,
        original_event_id NVARCHAR(64) NULL,
        error_message   NVARCHAR(1000) NULL,
        status          NVARCHAR(20)  NOT NULL DEFAULT 'unresolved',
        resolution      NVARCHAR(20)  NULL,
        resolved_by     NVARCHAR(60)  NULL,
        resolved_at     DATETIME2     NULL,
        created_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );
    """,
]


def ensure_topology_schema() -> None:
    """Create tables if needed (idempotent)."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        for ddl in _TOPOLOGY_SCHEMA:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


# ── Helpers ──────────────────────────────────────────────────────────

def _safe_json(val: Any) -> list | dict:
    if val is None:
        return []
    if isinstance(val, (list, dict)):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return []


def _topology_row_to_dict(row: tuple) -> dict[str, Any]:
    """Map a topology row (22 columns) to a dictionary."""
    return {
        "id": row[0],
        "domain": row[1],
        "queue_url": row[2],
        "queue_arn": row[3],
        "dlq_url": row[4],
        "dlq_arn": row[5],
        "region": row[6],
        "max_receive_count": row[7],
        "visibility_timeout_seconds": row[8],
        "message_retention_days": row[9],
        "polling_interval_seconds": row[10],
        "batch_size": row[11],
        "enabled": bool(row[12]),
        "status": row[13],
        "messages_received": row[14],
        "messages_processed": row[15],
        "messages_failed": row[16],
        "messages_dlq": row[17],
        "last_poll_at": str(row[18]) if row[18] else None,
        "last_error": row[19],
        "created_at": str(row[20]) if row[20] else None,
        "updated_at": str(row[21]) if row[21] else None,
    }


def _dlq_row_to_dict(row: tuple) -> dict[str, Any]:
    """Map a DLQ entry row (14 columns) to a dictionary."""
    return {
        "id": row[0],
        "domain": row[1],
        "queue_url": row[2],
        "message_id": row[3],
        "receipt_handle": row[4],
        "body": row[5],
        "approximate_receive_count": row[6],
        "original_event_id": row[7],
        "error_message": row[8],
        "status": row[9],
        "resolution": row[10],
        "resolved_by": row[11],
        "resolved_at": str(row[12]) if row[12] else None,
        "created_at": str(row[13]) if row[13] else None,
    }


# ── Queue topology CRUD ─────────────────────────────────────────────

def register_queue(
    *,
    domain: str,
    queue_url: str,
    queue_arn: str | None = None,
    dlq_url: str | None = None,
    dlq_arn: str | None = None,
    region: str = "eu-west-1",
    max_receive_count: int | None = None,
    visibility_timeout_seconds: int | None = None,
    message_retention_days: int | None = None,
    polling_interval_seconds: int | None = None,
    batch_size: int | None = None,
) -> dict[str, Any]:
    """Register or update a queue in the topology."""
    if domain not in VALID_DOMAINS:
        raise ValueError(f"Invalid domain '{domain}'. Must be one of: {sorted(VALID_DOMAINS)}")

    defaults = DEFAULT_QUEUE_CONFIG.get(domain, {})
    mrc = max_receive_count or defaults.get("max_receive_count", 3)
    vts = visibility_timeout_seconds or defaults.get("visibility_timeout_seconds", 30)
    mrd = message_retention_days or defaults.get("message_retention_days", 14)
    pis = polling_interval_seconds or defaults.get("polling_interval_seconds", 120)
    bs = batch_size or defaults.get("batch_size", 10)

    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            MERGE dbo.acc_sqs_queue_topology AS t
            USING (SELECT ? AS domain) AS s ON t.domain = s.domain
            WHEN MATCHED THEN UPDATE SET
                queue_url = ?, queue_arn = ?, dlq_url = ?, dlq_arn = ?,
                region = ?, max_receive_count = ?, visibility_timeout_seconds = ?,
                message_retention_days = ?, polling_interval_seconds = ?,
                batch_size = ?, updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN INSERT
                (domain, queue_url, queue_arn, dlq_url, dlq_arn, region,
                 max_receive_count, visibility_timeout_seconds, message_retention_days,
                 polling_interval_seconds, batch_size)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            OUTPUT INSERTED.id;
        """, (
            domain, queue_url, queue_arn, dlq_url, dlq_arn, region,
            mrc, vts, mrd, pis, bs,
            domain, queue_url, queue_arn, dlq_url, dlq_arn, region,
            mrc, vts, mrd, pis, bs,
        ))
        row = cur.fetchone()
        conn.commit()
        log.info("queue.registered", domain=domain, queue_url=queue_url)
        return {
            "id": row[0] if row else None,
            "domain": domain,
            "queue_url": queue_url,
            "dlq_url": dlq_url,
            "status": "active",
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_queue_status(domain: str, *, enabled: bool | None = None, status: str | None = None) -> dict[str, Any]:
    """Enable/disable a queue or update its status."""
    if status and status not in VALID_QUEUE_STATUSES:
        raise ValueError(f"Invalid status '{status}'. Must be one of: {sorted(VALID_QUEUE_STATUSES)}")
    if domain not in VALID_DOMAINS:
        raise ValueError(f"Invalid domain '{domain}'")

    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        sets: list[str] = ["updated_at = SYSUTCDATETIME()"]
        params: list[Any] = []
        if enabled is not None:
            sets.append("enabled = ?")
            params.append(1 if enabled else 0)
        if status is not None:
            sets.append("status = ?")
            params.append(status)
        params.append(domain)
        cur.execute(
            f"UPDATE dbo.acc_sqs_queue_topology SET {', '.join(sets)} WHERE domain = ?",
            params,
        )
        conn.commit()
        return {"domain": domain, "updated": True, "enabled": enabled, "status": status}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_queue_topology() -> list[dict[str, Any]]:
    """Get all registered queues."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, domain, queue_url, queue_arn, dlq_url, dlq_arn, region,
                   max_receive_count, visibility_timeout_seconds, message_retention_days,
                   polling_interval_seconds, batch_size, enabled, status,
                   messages_received, messages_processed, messages_failed, messages_dlq,
                   last_poll_at, last_error, created_at, updated_at
            FROM dbo.acc_sqs_queue_topology WITH (NOLOCK)
            ORDER BY domain
        """)
        return [_topology_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_queue_for_domain(domain: str) -> dict[str, Any] | None:
    """Get queue config for specific domain."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, domain, queue_url, queue_arn, dlq_url, dlq_arn, region,
                   max_receive_count, visibility_timeout_seconds, message_retention_days,
                   polling_interval_seconds, batch_size, enabled, status,
                   messages_received, messages_processed, messages_failed, messages_dlq,
                   last_poll_at, last_error, created_at, updated_at
            FROM dbo.acc_sqs_queue_topology WITH (NOLOCK)
            WHERE domain = ?
        """, (domain,))
        row = cur.fetchone()
        return _topology_row_to_dict(row) if row else None
    finally:
        conn.close()


def get_enabled_queues() -> list[dict[str, Any]]:
    """Get all enabled queues for polling."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, domain, queue_url, queue_arn, dlq_url, dlq_arn, region,
                   max_receive_count, visibility_timeout_seconds, message_retention_days,
                   polling_interval_seconds, batch_size, enabled, status,
                   messages_received, messages_processed, messages_failed, messages_dlq,
                   last_poll_at, last_error, created_at, updated_at
            FROM dbo.acc_sqs_queue_topology WITH (NOLOCK)
            WHERE enabled = 1 AND status = 'active'
            ORDER BY domain
        """)
        return [_topology_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# ── Queue metrics ────────────────────────────────────────────────────

def record_poll_result(
    domain: str,
    *,
    messages_received: int = 0,
    messages_processed: int = 0,
    messages_failed: int = 0,
    messages_dlq: int = 0,
    error: str | None = None,
) -> None:
    """Update queue metrics after a poll cycle."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        sets = [
            "messages_received = messages_received + ?",
            "messages_processed = messages_processed + ?",
            "messages_failed = messages_failed + ?",
            "messages_dlq = messages_dlq + ?",
            "last_poll_at = SYSUTCDATETIME()",
            "updated_at = SYSUTCDATETIME()",
        ]
        params: list[Any] = [messages_received, messages_processed, messages_failed, messages_dlq]
        if error:
            sets.append("last_error = ?")
            sets.append("status = 'error'")
            params.append(error)
        else:
            sets.append("last_error = NULL")
        params.append(domain)
        cur.execute(
            f"UPDATE dbo.acc_sqs_queue_topology SET {', '.join(sets)} WHERE domain = ?",
            params,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_topology_health() -> dict[str, Any]:
    """Get overall topology health summary."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*) AS total_queues,
                SUM(CASE WHEN enabled = 1 AND status = 'active' THEN 1 ELSE 0 END) AS active_queues,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_queues,
                SUM(CASE WHEN enabled = 0 THEN 1 ELSE 0 END) AS disabled_queues,
                SUM(messages_received) AS total_received,
                SUM(messages_processed) AS total_processed,
                SUM(messages_failed) AS total_failed,
                SUM(messages_dlq) AS total_dlq
            FROM dbo.acc_sqs_queue_topology WITH (NOLOCK)
        """)
        row = cur.fetchone()
        if not row:
            return {
                "total_queues": 0, "active_queues": 0, "error_queues": 0,
                "disabled_queues": 0, "total_received": 0, "total_processed": 0,
                "total_failed": 0, "total_dlq": 0, "unresolved_dlq": 0,
            }

        # Also get DLQ unresolved count
        cur.execute("""
            SELECT COUNT(*) FROM dbo.acc_dlq_entry WITH (NOLOCK)
            WHERE status = 'unresolved'
        """)
        dlq_row = cur.fetchone()

        return {
            "total_queues": row[0] or 0,
            "active_queues": row[1] or 0,
            "error_queues": row[2] or 0,
            "disabled_queues": row[3] or 0,
            "total_received": row[4] or 0,
            "total_processed": row[5] or 0,
            "total_failed": row[6] or 0,
            "total_dlq": row[7] or 0,
            "unresolved_dlq": dlq_row[0] if dlq_row else 0,
        }
    finally:
        conn.close()


# ── DLQ management ───────────────────────────────────────────────────

def track_dlq_entry(
    *,
    domain: str,
    queue_url: str,
    message_id: str,
    body: str | None = None,
    approximate_receive_count: int = 0,
    original_event_id: str | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    """Track a message that arrived in DLQ."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            MERGE dbo.acc_dlq_entry AS t
            USING (SELECT ? AS message_id) AS s ON t.message_id = s.message_id
            WHEN MATCHED THEN UPDATE SET
                approximate_receive_count = ?,
                error_message = ?,
                status = CASE WHEN t.status = 'unresolved' THEN 'unresolved' ELSE t.status END
            WHEN NOT MATCHED THEN INSERT
                (domain, queue_url, message_id, body, approximate_receive_count,
                 original_event_id, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            OUTPUT INSERTED.id;
        """, (
            message_id, approximate_receive_count, error_message,
            domain, queue_url, message_id, body, approximate_receive_count,
            original_event_id, error_message,
        ))
        row = cur.fetchone()
        conn.commit()
        log.info("dlq.entry.tracked", domain=domain, message_id=message_id)
        return {"id": row[0] if row else None, "domain": domain, "message_id": message_id}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def resolve_dlq_entry(
    entry_id: int,
    *,
    resolution: str,
    resolved_by: str | None = None,
) -> dict[str, Any]:
    """Resolve a DLQ entry (replay, discard, or investigate)."""
    if resolution not in VALID_DLQ_RESOLUTIONS:
        raise ValueError(f"Invalid resolution '{resolution}'. Must be one of: {sorted(VALID_DLQ_RESOLUTIONS)}")

    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE dbo.acc_dlq_entry
            SET status = ?, resolution = ?, resolved_by = ?,
                resolved_at = SYSUTCDATETIME()
            WHERE id = ? AND status = 'unresolved'
        """, (resolution, resolution, resolved_by, entry_id))
        conn.commit()
        log.info("dlq.entry.resolved", id=entry_id, resolution=resolution)
        return {"id": entry_id, "resolution": resolution, "resolved": True}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_dlq_entries(
    domain: str | None = None,
    *,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """List DLQ entries with filters."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        where: list[str] = []
        params: list[Any] = []

        if domain:
            where.append("domain = ?")
            params.append(domain)
        if status:
            where.append("status = ?")
            params.append(status)

        where_sql = " AND ".join(where) if where else "1=1"

        cur.execute(f"SELECT COUNT(*) FROM dbo.acc_dlq_entry WITH (NOLOCK) WHERE {where_sql}", params)
        total = cur.fetchone()[0] or 0

        cur.execute(f"""
            SELECT id, domain, queue_url, message_id, receipt_handle, body,
                   approximate_receive_count, original_event_id, error_message,
                   status, resolution, resolved_by, resolved_at, created_at
            FROM dbo.acc_dlq_entry WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY created_at DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, params + [offset, limit])

        items = [_dlq_row_to_dict(r) for r in cur.fetchall()]
        return {"items": items, "total": total, "limit": limit, "offset": offset}
    finally:
        conn.close()


def get_dlq_summary() -> dict[str, Any]:
    """DLQ summary by domain and status."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status = 'unresolved' THEN 1 ELSE 0 END) AS unresolved,
                SUM(CASE WHEN status = 'replayed' THEN 1 ELSE 0 END) AS replayed,
                SUM(CASE WHEN status = 'discarded' THEN 1 ELSE 0 END) AS discarded,
                SUM(CASE WHEN status = 'investigating' THEN 1 ELSE 0 END) AS investigating
            FROM dbo.acc_dlq_entry WITH (NOLOCK)
        """)
        row = cur.fetchone()
        if not row:
            return {"total": 0, "unresolved": 0, "replayed": 0, "discarded": 0, "investigating": 0}
        return {
            "total": row[0] or 0,
            "unresolved": row[1] or 0,
            "replayed": row[2] or 0,
            "discarded": row[3] or 0,
            "investigating": row[4] or 0,
        }
    finally:
        conn.close()


# ── Event routing ────────────────────────────────────────────────────

def route_notification_type(notification_type: str) -> str:
    """Determine which domain queue a notification type should be routed to."""
    return NOTIFICATION_ROUTING.get(notification_type, "report")


def get_routing_table() -> dict[str, Any]:
    """Return the full notification → domain routing table."""
    # Build reverse: domain → notification types
    domain_types: dict[str, list[str]] = {}
    for ntype, domain in NOTIFICATION_ROUTING.items():
        domain_types.setdefault(domain, []).append(ntype)

    return {
        "routes": NOTIFICATION_ROUTING,
        "domains": {d: sorted(types) for d, types in sorted(domain_types.items())},
        "total_types": len(NOTIFICATION_ROUTING),
        "total_domains": len(domain_types),
    }


# ── Multi-queue polling orchestrator ─────────────────────────────────

def poll_domain_queue(
    domain: str,
    *,
    max_messages: int = 10,
) -> dict[str, Any]:
    """Poll a specific domain queue for messages.

    Uses the queue URL from topology config. Falls back to legacy
    single-queue mode if no topology entry exists.
    """
    queue_config = get_queue_for_domain(domain)
    if not queue_config:
        return {
            "domain": domain,
            "status": "no_config",
            "messages_received": 0,
            "error": f"No queue configured for domain '{domain}'",
        }

    if not queue_config["enabled"]:
        return {"domain": domain, "status": "disabled", "messages_received": 0}

    queue_url = queue_config["queue_url"]
    if not queue_url:
        return {"domain": domain, "status": "no_url", "messages_received": 0}

    # Delegate to event_backbone.poll_sqs with the domain-specific queue URL
    try:
        from app.services.event_backbone import poll_sqs
        result = poll_sqs(
            max_messages=min(max_messages, queue_config["batch_size"]),
            queue_url_override=queue_url,
        )
        received = result.get("messages_received", 0) if isinstance(result, dict) else 0
        processed = result.get("messages_processed", 0) if isinstance(result, dict) else 0
        failed = result.get("messages_failed", 0) if isinstance(result, dict) else 0

        record_poll_result(
            domain,
            messages_received=received,
            messages_processed=processed,
            messages_failed=failed,
        )

        return {
            "domain": domain,
            "status": "polled",
            "messages_received": received,
            "messages_processed": processed,
            "messages_failed": failed,
        }
    except Exception as exc:
        error_msg = str(exc)[:500]
        record_poll_result(domain, error=error_msg)
        log.error("queue.poll.error", domain=domain, error=error_msg)
        return {
            "domain": domain,
            "status": "error",
            "messages_received": 0,
            "error": error_msg,
        }


def poll_all_queues() -> dict[str, Any]:
    """Poll all enabled domain queues in sequence."""
    queues = get_enabled_queues()
    results: list[dict] = []
    total_received = 0
    total_errors = 0

    for q in queues:
        result = poll_domain_queue(q["domain"], max_messages=q["batch_size"])
        results.append(result)
        total_received += result.get("messages_received", 0)
        if result.get("status") == "error":
            total_errors += 1

    return {
        "queues_polled": len(results),
        "total_received": total_received,
        "total_errors": total_errors,
        "results": results,
    }


# ── Seed default topology ───────────────────────────────────────────

def seed_default_topology(*, base_queue_url: str = "", region: str = "eu-west-1") -> dict[str, Any]:
    """Seed default queue topology entries for all domains.

    If base_queue_url is provided, generates per-domain URLs using
    the pattern: {base}-{domain}. Otherwise registers with empty URLs
    (placeholder for manual configuration).
    """
    results: list[dict] = []
    for domain in sorted(VALID_DOMAINS):
        queue_url = f"{base_queue_url}-{domain}" if base_queue_url else ""
        dlq_url = f"{base_queue_url}-{domain}-dlq" if base_queue_url else ""
        try:
            r = register_queue(
                domain=domain,
                queue_url=queue_url,
                dlq_url=dlq_url,
                region=region,
            )
            results.append(r)
        except Exception as exc:
            results.append({"domain": domain, "error": str(exc)})

    return {"seeded": len(results), "results": results}
