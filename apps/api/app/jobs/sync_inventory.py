"""Sync FBA inventory snapshots from SP-API.

Delegates to ingestion/inventory.py (Sprint 7 S7.2) for the unified
ingestion path.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

import structlog
from app.worker import celery_app

log = structlog.get_logger(__name__)


@celery_app.task(name="app.jobs.sync_inventory.sync_inventory", bind=True)
def sync_inventory(self, job_run_id: str, marketplace_id: Optional[str] = None):
    asyncio.run(
        _sync_inventory_async(job_run_id, marketplace_id)
    )


async def _sync_inventory_async(job_run_id: str, marketplace_id: Optional[str]):
    from sqlalchemy import select
    from app.core.database import AsyncSessionLocal
    from app.models.job import JobRun
    from app.ingestion.inventory import ingest_inventory

    async with AsyncSessionLocal() as db:
        job_result = await db.execute(select(JobRun).where(JobRun.id == job_run_id))
        job = job_result.scalar_one_or_none()
        if job:
            job.status = "running"
            job.started_at = datetime.now(timezone.utc)
            await db.commit()

        result = await ingest_inventory(
            marketplace_id=marketplace_id,
            enrich=False,
            return_meta=True,
        )
        total = result.get("raw_total", 0)

        if job:
            job.status = "success"
            job.records_processed = total
            job.finished_at = datetime.now(timezone.utc)
            job.progress_pct = 100
            if job.started_at:
                job.duration_seconds = (job.finished_at - job.started_at).total_seconds()
        await db.commit()
        log.info("sync_inventory.complete", total=total)
