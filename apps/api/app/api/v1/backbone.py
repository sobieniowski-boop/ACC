"""Event Backbone monitoring & health API.

Provides a dedicated ``/backbone`` prefix with:
- ``GET /backbone/health``   — aggregated health snapshot
- ``GET /backbone/alerts``   — recent system alerts
- ``POST /backbone/evaluate`` — trigger dead-letter evaluation + alert
"""
from __future__ import annotations

from typing import Optional

import structlog
from fastapi import APIRouter, Query
from starlette.concurrency import run_in_threadpool

from app.services.guardrails_backbone import (
    evaluate_and_alert,
    get_backbone_health_summary,
)
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)
router = APIRouter(prefix="/backbone", tags=["backbone"])


@router.get("/health")
async def backbone_health():
    """Aggregated backbone health snapshot (failed events, pending depth,
    processing rate, circuit-breaker status)."""
    return await run_in_threadpool(get_backbone_health_summary)


@router.post("/evaluate")
async def backbone_evaluate():
    """Run dead-letter evaluation and fire alerts if thresholds breached.

    Returns the list of guardrail results.
    """
    from dataclasses import asdict
    results = await run_in_threadpool(evaluate_and_alert)
    return {"results": [asdict(r) for r in results]}


@router.get("/alerts")
async def backbone_alerts(
    limit: int = Query(50, ge=1, le=500),
    alert_type: Optional[str] = None,
):
    """Query recent system alerts from ``acc_system_alert``."""

    def _fetch():
        try:
            conn = connect_acc(timeout=10)
            try:
                cur = conn.cursor()
                if alert_type:
                    cur.execute(
                        """
                        SELECT TOP(?) id, alert_type, severity, message, details, created_at
                        FROM acc_system_alert WITH (NOLOCK)
                        WHERE alert_type = ?
                        ORDER BY created_at DESC
                        """,
                        (limit, alert_type),
                    )
                else:
                    cur.execute(
                        """
                        SELECT TOP(?) id, alert_type, severity, message, details, created_at
                        FROM acc_system_alert WITH (NOLOCK)
                        ORDER BY created_at DESC
                        """,
                        (limit,),
                    )
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
                cur.close()
                return rows
            finally:
                conn.close()
        except Exception as exc:
            log.warning("backbone_alerts.fetch_failed", error=str(exc))
            return []

    alerts = await run_in_threadpool(_fetch)
    return {"alerts": alerts, "count": len(alerts)}
