"""Event Wiring & Replay Engine — Sprint 20.

Full SQS Topology phase 2: wire all modules into the event backbone,
manage wiring configuration, and provide operational replay capabilities.

Tables:
  acc_event_wire_config  — Module-to-domain-event wiring registry
  acc_replay_job         — Replay operation audit trail

Capabilities:
  - Register/list/toggle module-to-event wiring
  - Replay-and-process: reset events to received + immediately process
  - DLQ replay: re-ingest DLQ entries through event backbone
  - Wiring health dashboard: handler coverage per domain
  - Topology-aware polling bridge for scheduler
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import structlog

from app.connectors.mssql import connect_acc

log = structlog.get_logger(__name__)

# ── Constants ────────────────────────────────────────────────────────

VALID_REPLAY_TYPES = {"event_reset", "dlq_reingest", "bulk_domain", "selective"}
VALID_REPLAY_STATUSES = {"pending", "running", "completed", "failed", "cancelled"}
VALID_WIRE_MODULES = {
    "listing_state", "pricing_state", "profit", "order_pipeline",
    "inventory_sync", "report_processor", "feed_processor",
    "catalog_health", "content_ops", "sqs_topology",
}

# Default wiring: module → list of (domain, action, handler_name, description)
DEFAULT_WIRING: list[dict[str, str]] = [
    {
        "module_name": "listing_state",
        "event_domain": "listing",
        "event_action": "listing_status_changed",
        "handler_name": "listing_state.status",
        "description": "Process listing status change notifications",
    },
    {
        "module_name": "listing_state",
        "event_domain": "listing",
        "event_action": "listing_issues_changed",
        "handler_name": "listing_state.issues",
        "description": "Process listing issues change notifications",
    },
    {
        "module_name": "pricing_state",
        "event_domain": "pricing",
        "event_action": "offer_changed",
        "handler_name": "pricing_state.offer_changed",
        "description": "Process pricing offer change notifications",
    },
    {
        "module_name": "profit",
        "event_domain": "ads",
        "event_action": "synced",
        "handler_name": "profitability_dep_gate",
        "description": "Trigger profitability rollup after ads sync",
    },
    {
        "module_name": "profit",
        "event_domain": "finance",
        "event_action": "synced",
        "handler_name": "profitability_dep_gate_finance",
        "description": "Trigger profitability rollup after finance sync",
    },
    {
        "module_name": "order_pipeline",
        "event_domain": "order",
        "event_action": "*",
        "handler_name": "order_pipeline.status_handler",
        "description": "Process order status change events",
    },
    {
        "module_name": "inventory_sync",
        "event_domain": "inventory",
        "event_action": "*",
        "handler_name": "inventory_sync.availability_handler",
        "description": "Process FBA inventory availability changes",
    },
    {
        "module_name": "report_processor",
        "event_domain": "report",
        "event_action": "*",
        "handler_name": "report_processor.ready_handler",
        "description": "Process report processing finished events",
    },
    {
        "module_name": "feed_processor",
        "event_domain": "feed",
        "event_action": "*",
        "handler_name": "feed_processor.ready_handler",
        "description": "Process feed processing finished events",
    },
    {
        "module_name": "catalog_health",
        "event_domain": "listing",
        "event_action": "*",
        "handler_name": "catalog_health.listing_watcher",
        "description": "Update catalog health on listing changes",
    },
]

# Domain coverage: all domains that should have at least one handler
ALL_EVENT_DOMAINS = {"pricing", "listing", "order", "inventory", "report", "feed", "ads", "finance"}


# ── Schema DDL ───────────────────────────────────────────────────────

_WIRING_SCHEMA: list[str] = [
    """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_event_wire_config')
    CREATE TABLE dbo.acc_event_wire_config (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        module_name     NVARCHAR(80)  NOT NULL,
        event_domain    NVARCHAR(40)  NOT NULL,
        event_action    NVARCHAR(80)  NOT NULL DEFAULT '*',
        handler_name    NVARCHAR(120) NOT NULL,
        description     NVARCHAR(500) NULL,
        enabled         BIT           NOT NULL DEFAULT 1,
        priority        INT           NOT NULL DEFAULT 100,
        timeout_seconds INT           NOT NULL DEFAULT 30,
        created_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_event_wire_handler UNIQUE (handler_name)
    );
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'acc_replay_job')
    CREATE TABLE dbo.acc_replay_job (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        replay_type     NVARCHAR(40)  NOT NULL,
        scope_domain    NVARCHAR(40)  NULL,
        scope_action    NVARCHAR(80)  NULL,
        scope_event_ids NVARCHAR(MAX) NULL,
        scope_since     DATETIME2     NULL,
        scope_until     DATETIME2     NULL,
        events_matched  INT           NOT NULL DEFAULT 0,
        events_replayed INT           NOT NULL DEFAULT 0,
        events_processed INT          NOT NULL DEFAULT 0,
        events_failed   INT           NOT NULL DEFAULT 0,
        status          NVARCHAR(20)  NOT NULL DEFAULT 'pending',
        triggered_by    NVARCHAR(60)  NULL,
        error_message   NVARCHAR(1000) NULL,
        started_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        completed_at    DATETIME2     NULL
    );
    """,
]


def ensure_wiring_schema() -> None:
    """Create wiring+replay tables if needed (idempotent)."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        for ddl in _WIRING_SCHEMA:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


# ── Row mappers ──────────────────────────────────────────────────────

def _wire_row_to_dict(row: tuple) -> dict[str, Any]:
    """Map an acc_event_wire_config row (11 columns) to dict."""
    return {
        "id": row[0],
        "module_name": row[1],
        "event_domain": row[2],
        "event_action": row[3],
        "handler_name": row[4],
        "description": row[5],
        "enabled": bool(row[6]),
        "priority": row[7],
        "timeout_seconds": row[8],
        "created_at": str(row[9]) if row[9] else None,
        "updated_at": str(row[10]) if row[10] else None,
    }


def _replay_row_to_dict(row: tuple) -> dict[str, Any]:
    """Map an acc_replay_job row (15 columns) to dict."""
    return {
        "id": row[0],
        "replay_type": row[1],
        "scope_domain": row[2],
        "scope_action": row[3],
        "scope_event_ids": row[4],
        "scope_since": str(row[5]) if row[5] else None,
        "scope_until": str(row[6]) if row[6] else None,
        "events_matched": row[7],
        "events_replayed": row[8],
        "events_processed": row[9],
        "events_failed": row[10],
        "status": row[11],
        "triggered_by": row[12],
        "error_message": row[13],
        "started_at": str(row[14]) if row[14] else None,
        "completed_at": str(row[15]) if row[15] else None,
    }


# ── Wiring CRUD ──────────────────────────────────────────────────────

_WIRE_SELECT = """
    SELECT id, module_name, event_domain, event_action, handler_name,
           description, enabled, priority, timeout_seconds, created_at, updated_at
    FROM dbo.acc_event_wire_config WITH (NOLOCK)
"""


def register_wire(
    *,
    module_name: str,
    event_domain: str,
    event_action: str = "*",
    handler_name: str,
    description: str | None = None,
    enabled: bool = True,
    priority: int = 100,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    """Register or update an event wire config entry."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            MERGE dbo.acc_event_wire_config AS t
            USING (SELECT ? AS handler_name) AS s ON t.handler_name = s.handler_name
            WHEN MATCHED THEN UPDATE SET
                module_name = ?, event_domain = ?, event_action = ?,
                description = ?, enabled = ?, priority = ?,
                timeout_seconds = ?, updated_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN INSERT
                (module_name, event_domain, event_action, handler_name,
                 description, enabled, priority, timeout_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            OUTPUT INSERTED.id;
        """, (
            handler_name,
            module_name, event_domain, event_action, description,
            1 if enabled else 0, priority, timeout_seconds,
            module_name, event_domain, event_action, handler_name,
            description, 1 if enabled else 0, priority, timeout_seconds,
        ))
        row = cur.fetchone()
        conn.commit()
        log.info("wire.registered", handler=handler_name, domain=event_domain)
        return {"id": row[0] if row else None, "handler_name": handler_name, "event_domain": event_domain}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_wiring(*, module_name: str | None = None, event_domain: str | None = None, enabled_only: bool = False) -> list[dict[str, Any]]:
    """List wiring configurations with optional filters."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        where: list[str] = []
        params: list[Any] = []
        if module_name:
            where.append("module_name = ?")
            params.append(module_name)
        if event_domain:
            where.append("event_domain = ?")
            params.append(event_domain)
        if enabled_only:
            where.append("enabled = 1")

        where_sql = " AND ".join(where) if where else "1=1"
        cur.execute(f"{_WIRE_SELECT} WHERE {where_sql} ORDER BY event_domain, priority", params)
        return [_wire_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def toggle_wire(handler_name: str, *, enabled: bool) -> dict[str, Any]:
    """Enable or disable a wiring entry."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE dbo.acc_event_wire_config
            SET enabled = ?, updated_at = SYSUTCDATETIME()
            WHERE handler_name = ?
        """, (1 if enabled else 0, handler_name))
        conn.commit()
        return {"handler_name": handler_name, "enabled": enabled, "updated": True}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def delete_wire(handler_name: str) -> dict[str, Any]:
    """Remove a wiring entry."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM dbo.acc_event_wire_config WHERE handler_name = ?", (handler_name,))
        conn.commit()
        return {"handler_name": handler_name, "deleted": True}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def seed_default_wiring() -> dict[str, Any]:
    """Seed the default wiring configuration for all modules."""
    results: list[dict] = []
    for w in DEFAULT_WIRING:
        try:
            r = register_wire(
                module_name=w["module_name"],
                event_domain=w["event_domain"],
                event_action=w["event_action"],
                handler_name=w["handler_name"],
                description=w.get("description"),
            )
            results.append(r)
        except Exception as exc:
            results.append({"handler_name": w["handler_name"], "error": str(exc)})
    return {"seeded": len(results), "results": results}


# ── Wiring health / coverage ────────────────────────────────────────

def get_wiring_health() -> dict[str, Any]:
    """Get wiring health summary: coverage per domain, handler stats."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*) AS total_wires,
                SUM(CASE WHEN enabled = 1 THEN 1 ELSE 0 END) AS enabled_wires,
                SUM(CASE WHEN enabled = 0 THEN 1 ELSE 0 END) AS disabled_wires,
                COUNT(DISTINCT event_domain) AS domains_covered,
                COUNT(DISTINCT module_name) AS modules_wired
            FROM dbo.acc_event_wire_config WITH (NOLOCK)
        """)
        row = cur.fetchone()
        if not row:
            return {
                "total_wires": 0, "enabled_wires": 0, "disabled_wires": 0,
                "domains_covered": 0, "modules_wired": 0,
                "domain_coverage": [], "unwired_domains": sorted(ALL_EVENT_DOMAINS),
            }

        # Per-domain breakdown
        cur.execute("""
            SELECT event_domain,
                   COUNT(*) AS wire_count,
                   SUM(CASE WHEN enabled = 1 THEN 1 ELSE 0 END) AS enabled_count
            FROM dbo.acc_event_wire_config WITH (NOLOCK)
            GROUP BY event_domain
            ORDER BY event_domain
        """)
        domain_rows = cur.fetchall()
        covered = set()
        domain_coverage = []
        for dr in domain_rows:
            covered.add(dr[0])
            domain_coverage.append({
                "domain": dr[0],
                "wire_count": dr[1],
                "enabled_count": dr[2],
            })

        unwired = sorted(ALL_EVENT_DOMAINS - covered)

        return {
            "total_wires": row[0] or 0,
            "enabled_wires": row[1] or 0,
            "disabled_wires": row[2] or 0,
            "domains_covered": row[3] or 0,
            "modules_wired": row[4] or 0,
            "domain_coverage": domain_coverage,
            "unwired_domains": unwired,
        }
    finally:
        conn.close()


# ── Replay operations ────────────────────────────────────────────────

def create_replay_job(
    *,
    replay_type: str,
    scope_domain: str | None = None,
    scope_action: str | None = None,
    scope_event_ids: list[str] | None = None,
    scope_since: str | None = None,
    scope_until: str | None = None,
    triggered_by: str | None = None,
) -> dict[str, Any]:
    """Create a replay job record for audit trail."""
    if replay_type not in VALID_REPLAY_TYPES:
        raise ValueError(f"Invalid replay_type '{replay_type}'. Must be one of: {sorted(VALID_REPLAY_TYPES)}")

    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        event_ids_json = json.dumps(scope_event_ids) if scope_event_ids else None
        cur.execute("""
            INSERT INTO dbo.acc_replay_job
                (replay_type, scope_domain, scope_action, scope_event_ids,
                 scope_since, scope_until, triggered_by)
            OUTPUT INSERTED.id
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (replay_type, scope_domain, scope_action, event_ids_json,
              scope_since, scope_until, triggered_by))
        row = cur.fetchone()
        conn.commit()
        return {"id": row[0] if row else None, "replay_type": replay_type, "status": "pending"}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_replay_job(
    job_id: int,
    *,
    status: str | None = None,
    events_matched: int | None = None,
    events_replayed: int | None = None,
    events_processed: int | None = None,
    events_failed: int | None = None,
    error_message: str | None = None,
) -> None:
    """Update replay job progress."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        sets: list[str] = []
        params: list[Any] = []
        if status:
            sets.append("status = ?")
            params.append(status)
            if status in ("completed", "failed", "cancelled"):
                sets.append("completed_at = SYSUTCDATETIME()")
        if events_matched is not None:
            sets.append("events_matched = ?")
            params.append(events_matched)
        if events_replayed is not None:
            sets.append("events_replayed = ?")
            params.append(events_replayed)
        if events_processed is not None:
            sets.append("events_processed = ?")
            params.append(events_processed)
        if events_failed is not None:
            sets.append("events_failed = ?")
            params.append(events_failed)
        if error_message is not None:
            sets.append("error_message = ?")
            params.append(error_message)
        if not sets:
            return
        params.append(job_id)
        cur.execute(f"UPDATE dbo.acc_replay_job SET {', '.join(sets)} WHERE id = ?", params)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def replay_and_process(
    *,
    event_domain: str | None = None,
    notification_type: str | None = None,
    event_ids: list[str] | None = None,
    since: str | None = None,
    until: str | None = None,
    limit: int = 500,
    triggered_by: str | None = None,
) -> dict[str, Any]:
    """Replay events and immediately process them.

    1. Creates an audit trail entry (acc_replay_job)
    2. Resets matching events to 'received' via event_backbone.replay_events
    3. Immediately runs process_pending_events to handle them
    4. Returns comprehensive result
    """
    from app.services.event_backbone import replay_events, process_pending_events

    # Determine replay type
    if event_ids:
        replay_type = "selective"
    elif event_domain:
        replay_type = "bulk_domain"
    else:
        replay_type = "event_reset"

    job = create_replay_job(
        replay_type=replay_type,
        scope_domain=event_domain,
        scope_event_ids=event_ids,
        scope_since=since,
        scope_until=until,
        triggered_by=triggered_by,
    )
    job_id = job["id"]

    try:
        update_replay_job(job_id, status="running")

        # Step 1: Reset events to 'received'
        replay_result = replay_events(
            event_ids=event_ids,
            event_domain=event_domain,
            notification_type=notification_type,
            since=since,
            until=until,
            limit=limit,
        )
        replayed = replay_result.get("replayed", 0)
        update_replay_job(job_id, events_matched=replayed, events_replayed=replayed)

        if replayed == 0:
            update_replay_job(job_id, status="completed")
            return {
                "job_id": job_id,
                "replay_type": replay_type,
                "status": "completed",
                "events_replayed": 0,
                "events_processed": 0,
                "events_failed": 0,
            }

        # Step 2: Immediately process the replayed events
        process_result = process_pending_events(limit=replayed)
        proc = process_result.get("processed", 0)
        fail = process_result.get("failed", 0)

        update_replay_job(job_id, status="completed", events_processed=proc, events_failed=fail)

        log.info("replay.completed", job_id=job_id, replayed=replayed, processed=proc, failed=fail)
        return {
            "job_id": job_id,
            "replay_type": replay_type,
            "status": "completed",
            "events_replayed": replayed,
            "events_processed": proc,
            "events_failed": fail,
        }

    except Exception as exc:
        error_msg = str(exc)[:1000]
        update_replay_job(job_id, status="failed", error_message=error_msg)
        log.error("replay.failed", job_id=job_id, error=error_msg)
        return {
            "job_id": job_id,
            "replay_type": replay_type,
            "status": "failed",
            "error": error_msg,
        }


def replay_dlq_entries(
    *,
    domain: str | None = None,
    entry_ids: list[int] | None = None,
    triggered_by: str | None = None,
) -> dict[str, Any]:
    """Re-ingest DLQ entries through the event backbone.

    1. Queries matching DLQ entries (unresolved only)
    2. Calls ingest() for each entry's body
    3. Marks DLQ entries as replayed
    4. Returns result summary
    """
    from app.services.event_backbone import ingest
    from app.intelligence.sqs_topology import resolve_dlq_entry, get_dlq_entries

    job = create_replay_job(
        replay_type="dlq_reingest",
        scope_domain=domain,
        triggered_by=triggered_by,
    )
    job_id = job["id"]

    try:
        update_replay_job(job_id, status="running")

        # Get DLQ entries to replay
        if entry_ids:
            # Fetch specific entries
            conn = connect_acc(autocommit=False)
            try:
                cur = conn.cursor()
                placeholders = ",".join(["?"] * len(entry_ids))
                cur.execute(f"""
                    SELECT id, domain, queue_url, message_id, receipt_handle, body,
                           approximate_receive_count, original_event_id, error_message,
                           status, resolution, resolved_by, resolved_at, created_at
                    FROM dbo.acc_dlq_entry WITH (NOLOCK)
                    WHERE id IN ({placeholders}) AND status = 'unresolved'
                """, entry_ids)
                from app.intelligence.sqs_topology import _dlq_row_to_dict
                entries = [_dlq_row_to_dict(r) for r in cur.fetchall()]
            finally:
                conn.close()
        else:
            result = get_dlq_entries(domain, status="unresolved", limit=100)
            entries = result.get("items", [])

        update_replay_job(job_id, events_matched=len(entries))

        replayed = 0
        failed = 0
        for entry in entries:
            try:
                body = entry.get("body")
                if body and isinstance(body, str):
                    try:
                        payload = json.loads(body)
                    except (json.JSONDecodeError, TypeError):
                        payload = {"raw_body": body, "dlq_entry_id": entry["id"]}
                elif body and isinstance(body, dict):
                    payload = body
                else:
                    payload = {"dlq_entry_id": entry["id"], "domain": entry["domain"]}

                ingest(payload, source="dlq_replay")
                resolve_dlq_entry(entry["id"], resolution="replayed", resolved_by=triggered_by)
                replayed += 1
            except Exception as exc:
                log.warning("dlq.replay.entry_failed", entry_id=entry["id"], error=str(exc))
                failed += 1

        update_replay_job(
            job_id,
            status="completed",
            events_replayed=replayed,
            events_failed=failed,
        )

        log.info("dlq.replay.completed", job_id=job_id, replayed=replayed, failed=failed)
        return {
            "job_id": job_id,
            "replay_type": "dlq_reingest",
            "status": "completed",
            "entries_matched": len(entries),
            "entries_replayed": replayed,
            "entries_failed": failed,
        }
    except Exception as exc:
        error_msg = str(exc)[:1000]
        update_replay_job(job_id, status="failed", error_message=error_msg)
        return {"job_id": job_id, "status": "failed", "error": error_msg}


def get_replay_jobs(
    *,
    status: str | None = None,
    replay_type: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """List replay jobs with optional filters."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        where: list[str] = []
        params: list[Any] = []
        if status:
            where.append("status = ?")
            params.append(status)
        if replay_type:
            where.append("replay_type = ?")
            params.append(replay_type)

        where_sql = " AND ".join(where) if where else "1=1"

        cur.execute(f"SELECT COUNT(*) FROM dbo.acc_replay_job WITH (NOLOCK) WHERE {where_sql}", params)
        total = cur.fetchone()[0] or 0

        cur.execute(f"""
            SELECT id, replay_type, scope_domain, scope_action, scope_event_ids,
                   scope_since, scope_until, events_matched, events_replayed,
                   events_processed, events_failed, status, triggered_by,
                   error_message, started_at, completed_at
            FROM dbo.acc_replay_job WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY started_at DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, params + [offset, limit])

        items = [_replay_row_to_dict(r) for r in cur.fetchall()]
        return {"items": items, "total": total, "limit": limit, "offset": offset}
    finally:
        conn.close()


def get_replay_summary() -> dict[str, Any]:
    """Replay operations summary."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*) AS total_jobs,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
                SUM(events_replayed) AS total_events_replayed,
                SUM(events_processed) AS total_events_processed,
                SUM(events_failed) AS total_events_failed
            FROM dbo.acc_replay_job WITH (NOLOCK)
        """)
        row = cur.fetchone()
        if not row:
            return {
                "total_jobs": 0, "completed": 0, "failed": 0, "running": 0,
                "pending": 0, "total_events_replayed": 0,
                "total_events_processed": 0, "total_events_failed": 0,
            }
        return {
            "total_jobs": row[0] or 0,
            "completed": row[1] or 0,
            "failed": row[2] or 0,
            "running": row[3] or 0,
            "pending": row[4] or 0,
            "total_events_replayed": row[5] or 0,
            "total_events_processed": row[6] or 0,
            "total_events_failed": row[7] or 0,
        }
    finally:
        conn.close()


# ── Topology-aware polling bridge ────────────────────────────────────

def poll_topology_queues() -> dict[str, Any]:
    """Bridge function for the scheduler: poll all topology queues.

    Delegates to sqs_topology.poll_all_queues() with error handling
    and logging suitable for scheduler context.
    """
    try:
        from app.intelligence.sqs_topology import poll_all_queues
        result = poll_all_queues()
        log.info(
            "topology.poll.done",
            queues_polled=result.get("queues_polled", 0),
            total_received=result.get("total_received", 0),
        )
        return result
    except Exception as exc:
        log.error("topology.poll.failed", error=str(exc))
        return {"status": "error", "error": str(exc)}


# ── Event domain handler stubs ───────────────────────────────────────
# These are lightweight handler functions for domains that didn't have
# explicit handlers before Sprint 20. They log the event and can be
# extended with actual business logic.

def handle_order_event(event: dict) -> dict:
    """Handle order domain events (ORDER_STATUS_CHANGE)."""
    log.info(
        "handler.order",
        event_id=event.get("event_id", "")[:16],
        action=event.get("event_action"),
        order_id=event.get("amazon_order_id"),
    )
    return {"handled": True, "domain": "order"}


def handle_inventory_event(event: dict) -> dict:
    """Handle inventory domain events (FBA_INVENTORY_AVAILABILITY_CHANGES)."""
    log.info(
        "handler.inventory",
        event_id=event.get("event_id", "")[:16],
        action=event.get("event_action"),
        sku=event.get("sku"),
    )
    return {"handled": True, "domain": "inventory"}


def handle_report_event(event: dict) -> dict:
    """Handle report domain events (REPORT_PROCESSING_FINISHED)."""
    log.info(
        "handler.report",
        event_id=event.get("event_id", "")[:16],
        action=event.get("event_action"),
    )
    return {"handled": True, "domain": "report"}


def handle_feed_event(event: dict) -> dict:
    """Handle feed domain events (FEED_PROCESSING_FINISHED)."""
    log.info(
        "handler.feed",
        event_id=event.get("event_id", "")[:16],
        action=event.get("event_action"),
    )
    return {"handled": True, "domain": "feed"}


def register_all_domain_handlers() -> dict[str, Any]:
    """Register event handlers for all domains that lack handlers.

    This ensures every event domain has at least one handler registered
    in the event backbone, completing the "all modules event-wired" goal.
    """
    from app.services.event_backbone import register_handler, _HANDLER_REGISTRY

    registered: list[str] = []

    # Order domain handler
    key = "order:*"
    if key not in _HANDLER_REGISTRY:
        register_handler(
            "order", None,
            handler_name="order_pipeline.status_handler",
            handler_fn=handle_order_event,
        )
        registered.append("order:*")

    # Inventory domain handler
    key = "inventory:*"
    if key not in _HANDLER_REGISTRY:
        register_handler(
            "inventory", None,
            handler_name="inventory_sync.availability_handler",
            handler_fn=handle_inventory_event,
        )
        registered.append("inventory:*")

    # Report domain handler
    key = "report:*"
    if key not in _HANDLER_REGISTRY:
        register_handler(
            "report", None,
            handler_name="report_processor.ready_handler",
            handler_fn=handle_report_event,
        )
        registered.append("report:*")

    # Feed domain handler
    key = "feed:*"
    if key not in _HANDLER_REGISTRY:
        register_handler(
            "feed", None,
            handler_name="feed_processor.ready_handler",
            handler_fn=handle_feed_event,
        )
        registered.append("feed:*")

    log.info("domain_handlers.registered", handlers=registered)
    return {"registered": registered, "count": len(registered)}
