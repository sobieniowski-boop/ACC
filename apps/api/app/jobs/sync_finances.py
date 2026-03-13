"""Sync SP-API financial events — delegates to step_sync_finances (canonical).

Legacy v0 importer removed to prevent duplicate rows in acc_finance_transaction.
"""
from __future__ import annotations

import asyncio
from typing import Optional

import structlog
from app.worker import celery_app

log = structlog.get_logger(__name__)


@celery_app.task(name="app.jobs.sync_finances.sync_finances", bind=True)
def sync_finances(self, job_run_id: str | None = None, marketplace_id: Optional[str] = None, days_back: int = 7):
    """Celery-compatible wrapper — runs the canonical step_sync_finances."""
    asyncio.run(
        _sync_finances_async(marketplace_id, days_back)
    )


async def _sync_finances_async(marketplace_id: Optional[str], days_back: int):
    from app.services.order_pipeline import step_sync_finances

    log.warning("jobs.sync_finances: delegating to step_sync_finances (canonical)")
    result = await step_sync_finances(days_back=days_back, marketplace_id=marketplace_id)
    log.info("jobs.sync_finances.done", fee_rows=result.get("fee_rows", 0))
