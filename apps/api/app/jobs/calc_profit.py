"""Recalculate profit for recent orders via the canonical V2 batch path."""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import structlog

from app.worker import celery_app

log = structlog.get_logger(__name__)


@celery_app.task(name="app.jobs.calc_profit.calc_profit", bind=True)
def calc_profit(self, job_run_id: str, marketplace_id: Optional[str] = None, days_back: int = 1):
    asyncio.run(
        _calc_profit_async(job_run_id, marketplace_id, days_back)
    )


async def _calc_profit_async(job_run_id: str, marketplace_id: Optional[str], days_back: int):
    import asyncio as _aio
    from sqlalchemy import select
    from app.core.database import AsyncSessionLocal
    from app.models.job import JobRun
    from app.connectors.mssql.mssql_store import (
        recalc_profit_orders,
        sync_profit_snapshot,
        evaluate_alert_rules,
    )

    async with AsyncSessionLocal() as db:
        job_result = await db.execute(select(JobRun).where(JobRun.id == job_run_id))
        job = job_result.scalar_one_or_none()
        if job:
            job.status = "running"
            job.started_at = datetime.now(timezone.utc)
            await db.commit()

        date_to = date.today()
        date_from = date_to - timedelta(days=days_back)

        try:
            count = await _aio.to_thread(
                recalc_profit_orders, date_from=date_from, date_to=date_to,
            )
            await _aio.to_thread(sync_profit_snapshot, date_from=date_from, date_to=date_to)
            await _aio.to_thread(evaluate_alert_rules)
            if job:
                job.status = "success"
                job.records_processed = count
                job.progress_pct = 100
        except Exception as exc:
            log.error("calc_profit.error", error=str(exc))
            if job:
                job.status = "failure"
                job.error_message = str(exc)
        finally:
            if job:
                job.finished_at = datetime.now(timezone.utc)
                if job.started_at:
                    job.duration_seconds = (job.finished_at - job.started_at).total_seconds()
            await db.commit()
