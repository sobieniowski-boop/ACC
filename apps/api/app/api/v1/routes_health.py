"""Health check endpoints — shallow + deep.

GET /health        → quick "ok" (load-balancer probe)
GET /health/deep   → checks Azure SQL, Redis, SP-API token
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from fastapi import APIRouter, Depends
from fastapi.concurrency import run_in_threadpool

from app.core.config import settings
from app.core.security import Role, get_current_user, require_role

_require_analyst = Depends(require_role(Role.ANALYST))

log = logging.getLogger("acc.health")

router = APIRouter(prefix="/health", tags=["health"])


# ── shallow (for LB / container probes) ────────────────────────
@router.get("")
async def health_check():
    return {"status": "ok"}


# ── deep (real dependency checks) ──────────────────────────────
@router.get("/deep", dependencies=[_require_analyst])
async def health_deep():
    """Check Azure SQL, Redis and SP-API token validity."""
    checks: dict[str, Any] = {}
    t0 = time.perf_counter()

    # Run independent checks concurrently
    sql_task = asyncio.create_task(_check_sql())
    redis_task = asyncio.create_task(_check_redis())
    spapi_task = asyncio.create_task(_check_spapi())

    checks["azure_sql"] = await sql_task
    checks["redis"] = await redis_task
    checks["sp_api"] = await spapi_task

    overall = all(c["ok"] for c in checks.values())
    return {
        "status": "healthy" if overall else "degraded",
        "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1),
        "env": settings.APP_ENV,
        "checks": checks,
    }


@router.get("/netfox-sessions", dependencies=[_require_analyst])
async def health_netfox_sessions():
    return await run_in_threadpool(_check_netfox_sessions)


@router.get("/order-sync", dependencies=[_require_analyst])
async def health_order_sync():
    return await run_in_threadpool(_check_order_sync)


@router.get("/sp-api-usage", dependencies=[_require_analyst])
async def health_sp_api_usage(
    days: int = 7,
    endpoint_name: str | None = None,
    marketplace_id: str | None = None,
    sync_profile: str | None = None,
):
    return await run_in_threadpool(
        _check_sp_api_usage,
        days,
        endpoint_name,
        marketplace_id,
        sync_profile,
    )


async def _check_sql() -> dict[str, Any]:
    """SELECT 1 via connect_acc with a short timeout."""
    try:
        from app.core.db_connection import connect_acc

        def _ping():
            conn = connect_acc(autocommit=True, timeout=5)
            try:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.fetchone()
            finally:
                conn.close()

        t0 = time.perf_counter()
        await run_in_threadpool(_ping)
        ms = round((time.perf_counter() - t0) * 1000, 1)
        return {"ok": True, "latency_ms": ms}
    except Exception as exc:
        log.warning("health.sql_fail: %s", exc)
        return {"ok": False, "error": str(exc)[:200]}


async def _check_redis() -> dict[str, Any]:
    """Redis PING."""
    try:
        from app.core.redis_client import get_redis

        r = await get_redis()
        t0 = time.perf_counter()
        pong = await asyncio.wait_for(r.ping(), timeout=3.0)
        ms = round((time.perf_counter() - t0) * 1000, 1)
        return {"ok": bool(pong), "latency_ms": ms}
    except Exception as exc:
        log.warning("health.redis_fail: %s", exc)
        return {"ok": False, "error": str(exc)[:200]}


async def _check_spapi() -> dict[str, Any]:
    """Try to obtain/refresh LWA access token."""
    if not settings.SP_API_CLIENT_ID:
        return {"ok": False, "error": "SP_API_CLIENT_ID not configured"}
    try:
        from app.connectors.amazon_sp_api.client import _auth

        t0 = time.perf_counter()
        token = await asyncio.wait_for(_auth.get_access_token(), timeout=10.0)
        ms = round((time.perf_counter() - t0) * 1000, 1)
        return {"ok": bool(token), "latency_ms": ms}
    except Exception as exc:
        log.warning("health.spapi_fail: %s", exc)
        return {"ok": False, "error": str(exc)[:200]}


def _check_netfox_sessions() -> dict[str, Any]:
    try:
        from app.core.db_connection import connect_netfox

        conn = connect_netfox(timeout=5)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                DECLARE @self_spid INT = @@SPID;
                SELECT
                    session_id,
                    login_name,
                    host_name,
                    program_name,
                    status,
                    DB_NAME(database_id) AS database_name,
                    login_time,
                    last_request_start_time,
                    last_request_end_time
                FROM sys.dm_exec_sessions
                WHERE is_user_process = 1
                  AND program_name = 'ACC-Netfox-RO'
                  AND session_id <> @self_spid
                ORDER BY login_time DESC
                """
            )
            rows = cur.fetchall()
            items: list[dict[str, Any]] = []
            for row in rows:
                items.append(
                    {
                        "session_id": int(row[0]),
                        "login_name": str(row[1]) if row[1] else None,
                        "host_name": str(row[2]) if row[2] else None,
                        "program_name": str(row[3]) if row[3] else None,
                        "status": str(row[4]) if row[4] else None,
                        "database_name": str(row[5]) if row[5] else None,
                        "login_time": row[6].isoformat() if row[6] else None,
                        "last_request_start_time": row[7].isoformat() if row[7] else None,
                        "last_request_end_time": row[8].isoformat() if row[8] else None,
                    }
                )
            return {
                "ok": True,
                "session_count": len(items),
                "items": items,
            }
        finally:
            conn.close()
    except Exception as exc:
        log.warning("health.netfox_sessions_fail: %s", exc)
        return {"ok": False, "error": str(exc)[:200], "session_count": None, "items": []}


def _check_order_sync() -> dict[str, Any]:
    try:
        from app.services.order_pipeline import _collect_order_sync_health

        summary = _collect_order_sync_health()
        return {
            "ok": str(summary.get("status") or "healthy") == "healthy",
            **summary,
        }
    except Exception as exc:
        log.warning("health.order_sync_fail: %s", exc)
        return {"ok": False, "status": "error", "error": str(exc)[:200], "items": []}


def _check_sp_api_usage(
    days: int,
    endpoint_name: str | None,
    marketplace_id: str | None,
    sync_profile: str | None,
) -> dict[str, Any]:
    try:
        from app.services.sp_api_usage import list_sp_api_usage_daily

        rows = list_sp_api_usage_daily(
            days=days,
            endpoint_name=endpoint_name,
            marketplace_id=marketplace_id,
            sync_profile=sync_profile,
        )
        totals = {
            "calls_count": sum(int(r.get("calls_count") or 0) for r in rows),
            "error_count": sum(int(r.get("error_count") or 0) for r in rows),
            "throttled_count": sum(int(r.get("throttled_count") or 0) for r in rows),
            "rows_returned": sum(int(r.get("rows_returned") or 0) for r in rows),
            "total_duration_ms": sum(int(r.get("total_duration_ms") or 0) for r in rows),
        }
        totals["avg_duration_ms"] = (
            round(totals["total_duration_ms"] / totals["calls_count"], 2)
            if totals["calls_count"] > 0
            else 0.0
        )
        return {
            "ok": True,
            "days": max(1, min(int(days or 7), 90)),
            "filters": {
                "endpoint_name": endpoint_name,
                "marketplace_id": marketplace_id,
                "sync_profile": sync_profile,
            },
            "totals": totals,
            "rows": rows,
        }
    except Exception as exc:
        log.warning("health.sp_api_usage_fail: %s", exc)
        return {"ok": False, "error": str(exc)[:200], "rows": []}
