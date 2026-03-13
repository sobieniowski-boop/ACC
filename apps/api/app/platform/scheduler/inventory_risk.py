"""Inventory Risk Engine domain — daily risk score computation,
replenishment plan generation, and risk alert scanning.

Sprint 13 – S13.4
Sprint 14 – Replenishment plan + risk alerts
"""
from __future__ import annotations

import asyncio

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.connectors.mssql import set_job_failure, set_job_success
from app.platform.scheduler.base import create_job_record

log = structlog.get_logger(__name__)


async def _compute_inventory_risk_scores() -> None:
    """Daily 05:00 — Compute per-SKU inventory risk scores, then generate
    replenishment plan and risk alerts."""
    job_id = create_job_record("inventory_risk_computation")
    log.info("scheduler.inventory_risk.start", job_id=job_id)
    try:
        from app.intelligence.inventory_risk import (
            compute_daily_risk_scores,
            compute_replenishment_plan,
            ensure_inventory_risk_schema,
            ensure_replenishment_schema,
            generate_risk_alerts,
        )
        await asyncio.to_thread(ensure_inventory_risk_schema)
        await asyncio.to_thread(ensure_replenishment_schema)
        upserted = await asyncio.to_thread(compute_daily_risk_scores)
        plan_count = await asyncio.to_thread(compute_replenishment_plan)
        alert_count = await asyncio.to_thread(generate_risk_alerts)
        set_job_success(
            job_id, records_processed=upserted,
            message=f"risk_scores={upserted} plan={plan_count} alerts={alert_count}",
        )
        log.info("scheduler.inventory_risk.done",
                 upserted=upserted, plan=plan_count, alerts=alert_count)
    except Exception as exc:
        log.error("scheduler.inventory_risk.error", error=str(exc))
        set_job_failure(job_id, str(exc))


def register(scheduler: AsyncIOScheduler) -> None:
    scheduler.add_job(
        _compute_inventory_risk_scores,
        trigger=CronTrigger(hour=5, minute=0),
        id="inventory-risk-computation",
        name="Inventory Risk Score Computation (05:00)",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=600,
    )
