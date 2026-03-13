"""Unified platform health — /system/health.

Single endpoint that aggregates live metrics from every subsystem
into one JSON response.
"""
from __future__ import annotations

import time
from typing import Any

import structlog
from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool

from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)
router = APIRouter(prefix="/system", tags=["system"])


# ---------------------------------------------------------------------------
# Metric collectors (each runs a short SQL query or reads in-memory state)
# ---------------------------------------------------------------------------

def _collect_event_backbone() -> dict[str, Any]:
    conn = connect_acc(autocommit=False, timeout=15)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                SUM(CASE WHEN status = 'received' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status = 'failed'
                          AND received_at >= DATEADD(HOUR, -1, SYSUTCDATETIME()) THEN 1 ELSE 0 END),
                SUM(CASE WHEN status = 'processed'
                          AND processed_at >= DATEADD(HOUR, -1, SYSUTCDATETIME()) THEN 1 ELSE 0 END)
            FROM dbo.acc_event_log WITH (NOLOCK)
        """)
        row = cur.fetchone()
        return {
            "pending_events": row[0] or 0,
            "failed_last_hour": row[1] or 0,
            "processing_rate": row[2] or 0,
        }
    except Exception as exc:
        log.warning("system_health.event_backbone.error", error=str(exc))
        return {"pending_events": -1, "failed_last_hour": -1, "processing_rate": -1}
    finally:
        conn.close()


def _collect_sqs() -> dict[str, Any]:
    try:
        from app.services.event_backbone import get_sqs_metrics
        m = get_sqs_metrics()
        return {
            "queue_depth": m.get("sqs_messages_received", 0),
            "poll_rate": m.get("sqs_poll_loops", 0),
        }
    except Exception as exc:
        log.warning("system_health.sqs.error", error=str(exc))
        return {"queue_depth": -1, "poll_rate": -1}


def _collect_listing_state() -> dict[str, Any]:
    conn = connect_acc(autocommit=False, timeout=15)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                SUM(CASE WHEN is_suppressed = 1 THEN 1 ELSE 0 END),
                MAX(last_synced_at)
            FROM dbo.acc_listing_state WITH (NOLOCK)
        """)
        row = cur.fetchone()
        return {
            "suppressed_count": row[0] or 0,
            "last_update": str(row[1]) if row[1] else None,
        }
    except Exception as exc:
        log.warning("system_health.listing_state.error", error=str(exc))
        return {"suppressed_count": -1, "last_update": None}
    finally:
        conn.close()


def _collect_pricing() -> dict[str, Any]:
    conn = connect_acc(autocommit=False, timeout=15)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*)
            FROM dbo.acc_pricing_snapshot WITH (NOLOCK)
            WHERE observed_at >= DATEADD(HOUR, -1, SYSUTCDATETIME())
        """)
        snaps = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*)
            FROM dbo.acc_pricing_recommendation WITH (NOLOCK)
            WHERE status = 'pending'
              AND (expires_at IS NULL OR expires_at > SYSUTCDATETIME())
        """)
        pending = cur.fetchone()[0]

        return {
            "snapshots_last_hour": snaps,
            "pending_recommendations": pending,
        }
    except Exception as exc:
        log.warning("system_health.pricing.error", error=str(exc))
        return {"snapshots_last_hour": -1, "pending_recommendations": -1}
    finally:
        conn.close()


def _collect_database() -> dict[str, Any]:
    t0 = time.perf_counter()
    conn = None
    try:
        conn = connect_acc(autocommit=False, timeout=10)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        ok = True

        # Count currently executing queries running > 5 s
        cur.execute("""
            SELECT COUNT(*)
            FROM sys.dm_exec_requests WITH (NOLOCK)
            WHERE total_elapsed_time > 5000
              AND session_id > 50
        """)
        slow = cur.fetchone()[0]

        return {
            "connection_ok": ok,
            "latency_ms": round((time.perf_counter() - t0) * 1000, 1),
            "slow_queries": slow,
        }
    except Exception as exc:
        log.warning("system_health.database.error", error=str(exc))
        return {
            "connection_ok": False,
            "latency_ms": round((time.perf_counter() - t0) * 1000, 1),
            "slow_queries": -1,
        }
    finally:
        if conn:
            conn.close()


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get("/health")
async def system_health():
    """Unified platform health — aggregates live metrics from all subsystems."""
    import asyncio

    t0 = time.perf_counter()

    eb, sqs, ls, pr, db = await asyncio.gather(
        run_in_threadpool(_collect_event_backbone),
        run_in_threadpool(_collect_sqs),
        run_in_threadpool(_collect_listing_state),
        run_in_threadpool(_collect_pricing),
        run_in_threadpool(_collect_database),
    )

    return {
        "event_backbone": eb,
        "sqs": sqs,
        "listing_state": ls,
        "pricing": pr,
        "database": db,
        "collected_ms": round((time.perf_counter() - t0) * 1000, 1),
    }
