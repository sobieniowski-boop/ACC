"""Nightly sync of purchase prices from Holding FIFO + XLSX."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

import structlog

from app.worker import celery_app

log = structlog.get_logger(__name__)


@celery_app.task(
    name="app.jobs.sync_purchase_prices.sync_purchase_prices",
    bind=True,
)
def sync_purchase_prices(self, job_run_id: Optional[str] = None):
    """Celery wrapper — delegates to the async sync_service function."""
    asyncio.run(
        _sync_async(job_run_id)
    )


async def _sync_async(job_run_id: Optional[str]):
    from app.services.sync_service import sync_purchase_prices as _do_sync

    try:
        count = await _do_sync(job_id=job_run_id)
        log.info("jobs.sync_purchase_prices.done", count=count)
    except Exception as exc:
        log.error("jobs.sync_purchase_prices.error", error=str(exc))
        # Job status is already handled inside sync_purchase_prices()
        raise
