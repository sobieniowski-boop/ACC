"""Catalog health domain — daily health score snapshot job.

Sprint 10 – S10.4
"""
from __future__ import annotations

import asyncio

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.connectors.mssql import set_job_failure, set_job_success
from app.platform.scheduler.base import create_job_record

log = structlog.get_logger(__name__)


async def _catalog_health_snapshot() -> None:
    """Daily 03:00 — Compute and persist health scores for all listings."""
    job_id = create_job_record("catalog_health_snapshot")
    log.info("scheduler.catalog_health_snapshot.start", job_id=job_id)
    try:
        from app.intelligence.catalog_health import (
            compute_and_persist_health_snapshots,
            ensure_catalog_health_schema,
        )
        await asyncio.to_thread(ensure_catalog_health_schema)
        result = await asyncio.to_thread(compute_and_persist_health_snapshots)
        set_job_success(
            job_id, records_processed=result.get("upserted", 0),
            message=f"snapshots={result.get('upserted', 0)}",
        )
        log.info("scheduler.catalog_health_snapshot.done", result=result)
    except Exception as exc:
        log.error("scheduler.catalog_health_snapshot.error", error=str(exc))
        set_job_failure(job_id, str(exc))


def register(scheduler: AsyncIOScheduler) -> None:
    scheduler.add_job(
        _catalog_health_snapshot,
        trigger=CronTrigger(hour=3, minute=0),
        id="catalog-health-snapshot-daily",
        name="Catalog Health Snapshot (03:00)",
        replace_existing=True, max_instances=1, misfire_grace_time=600,
    )
