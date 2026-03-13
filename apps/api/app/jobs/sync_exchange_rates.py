"""Celery task: sync FX exchange rates from NBP API."""
from __future__ import annotations

import asyncio

import structlog

from app.worker import celery_app

log = structlog.get_logger(__name__)


def _run_async(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


@celery_app.task(
    name="app.jobs.sync_exchange_rates.sync_exchange_rates",
    bind=True,
    max_retries=2,
    default_retry_delay=60,
)
def sync_exchange_rates(self, days_back: int = 7):
    """Sync FX rates from NBP API (daily at 1:30am)."""
    try:
        count = _run_async(_sync_fx(days_back))
        log.info("sync_exchange_rates.task_done", inserted=count)
        return count
    except Exception as exc:
        log.error("sync_exchange_rates.task_error", error=str(exc))
        raise self.retry(exc=exc)


async def _sync_fx(days_back: int):
    from app.services.sync_service import sync_exchange_rates as _sync
    return await _sync(days_back=days_back)
