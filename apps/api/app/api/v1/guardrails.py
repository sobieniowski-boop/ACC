"""Runtime guardrails API — production health & integrity checks.

GET  /guardrails              → run all checks, return structured report
GET  /guardrails/summary      → last persisted run from DB (fast, no re-run)
GET  /guardrails/check/{name} → run a single named check
GET  /guardrails/history      → trending data from acc_guardrail_results
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool

router = APIRouter(prefix="/guardrails", tags=["guardrails"])
log = logging.getLogger("acc.guardrails")


@router.get("")
async def run_all_guardrails():
    """Execute all runtime guardrail checks and return results."""
    from app.services.guardrails import run_guardrails
    return await run_guardrails(persist=True)


@router.get("/summary")
async def guardrails_summary(hours: int = Query(default=24, ge=1, le=168)):
    """Return the latest guardrail results from the DB (no re-run)."""
    from app.core.db_connection import connect_acc

    def _query():
        conn = connect_acc(timeout=10)
        try:
            cur = conn.cursor()
            cur.execute("""\
                SELECT check_name, severity, message, value, threshold,
                       elapsed_ms, checked_at
                FROM acc_guardrail_results WITH (NOLOCK)
                WHERE checked_at >= DATEADD(HOUR, ?, SYSUTCDATETIME())
                ORDER BY checked_at DESC
            """, (-hours,))
            rows = cur.fetchall()
            items = []
            for r in rows:
                items.append({
                    "check_name": str(r[0]),
                    "severity": str(r[1]),
                    "message": str(r[2]) if r[2] else None,
                    "value": float(r[3]) if r[3] is not None else None,
                    "threshold": float(r[4]) if r[4] is not None else None,
                    "elapsed_ms": float(r[5]) if r[5] is not None else None,
                    "checked_at": r[6].isoformat() if r[6] else None,
                })
            return items
        finally:
            conn.close()

    try:
        items = await run_in_threadpool(_query)
    except Exception as exc:
        raise HTTPException(500, f"Failed to read guardrail history: {exc!s}"[:300])

    # Group by latest per check
    latest: dict[str, Any] = {}
    for item in items:
        name = item["check_name"]
        if name not in latest:
            latest[name] = item

    by_sev: dict[str, int] = {}
    for v in latest.values():
        sev = v["severity"]
        by_sev[sev] = by_sev.get(sev, 0) + 1

    overall = "healthy"
    if by_sev.get("critical", 0) > 0:
        overall = "critical"
    elif by_sev.get("warning", 0) > 0:
        overall = "degraded"

    return {
        "status": overall,
        "hours": hours,
        "summary": by_sev,
        "latest_per_check": latest,
        "total_records": len(items),
    }


@router.get("/check/{check_name}")
async def run_single_check(check_name: str):
    """Run a single guardrail check by name."""
    from app.services import guardrails as g

    # Build lookup from sync + async checks
    all_checks = {}
    for fn in g._SYNC_CHECKS:
        key = fn.__name__.replace("check_", "")
        all_checks[key] = ("sync", fn)
    for fn in g._ASYNC_CHECKS:
        key = fn.__name__.replace("check_", "")
        all_checks[key] = ("async", fn)

    if check_name not in all_checks:
        raise HTTPException(404, f"Unknown check: {check_name}. Available: {sorted(all_checks)}")

    kind, fn = all_checks[check_name]
    if kind == "sync":
        from dataclasses import asdict
        result = await run_in_threadpool(fn)
        return asdict(result)
    else:
        from dataclasses import asdict
        result = await fn()
        return asdict(result)


@router.get("/history")
async def guardrails_history(
    check_name: str = Query(..., description="Name of the check to trend"),
    days: int = Query(default=7, ge=1, le=30),
):
    """Return historical trend for a specific guardrail check."""
    from app.core.db_connection import connect_acc

    def _query():
        conn = connect_acc(timeout=10)
        try:
            cur = conn.cursor()
            cur.execute("""\
                SELECT severity, value, threshold, elapsed_ms, checked_at
                FROM acc_guardrail_results WITH (NOLOCK)
                WHERE check_name = ?
                  AND checked_at >= DATEADD(DAY, ?, SYSUTCDATETIME())
                ORDER BY checked_at ASC
            """, (check_name, -days))
            return [{
                "severity": str(r[0]),
                "value": float(r[1]) if r[1] is not None else None,
                "threshold": float(r[2]) if r[2] is not None else None,
                "elapsed_ms": float(r[3]) if r[3] is not None else None,
                "checked_at": r[4].isoformat() if r[4] else None,
            } for r in cur.fetchall()]
        finally:
            conn.close()

    try:
        rows = await run_in_threadpool(_query)
    except Exception as exc:
        raise HTTPException(500, f"History query failed: {exc!s}"[:300])

    return {
        "check_name": check_name,
        "days": days,
        "data_points": len(rows),
        "history": rows,
    }
