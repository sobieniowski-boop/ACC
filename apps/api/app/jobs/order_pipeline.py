"""Celery task: order pipeline (every 15 min)."""
from __future__ import annotations

import asyncio
from typing import Optional

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
    name="app.jobs.order_pipeline.run_order_pipeline",
    bind=True,
    max_retries=2,
    default_retry_delay=60,
)
def run_order_pipeline(self, days_back: int = 1):
    """
    Run the 5-step order pipeline:
      1. Sync orders from SP-API
      2. Backfill missing products
      3. Link order lines → products
      4. Map internal SKU (EAN cascade)
      5. Stamp purchase prices

    Scheduled every 15 min via Celery beat.
    """
    try:
        result = _run_async(_run_pipeline(days_back))
        log.info("order_pipeline.task_done", result=result)
        return result
    except Exception as exc:
        log.error("order_pipeline.task_error", error=str(exc))
        raise self.retry(exc=exc)


async def _run_pipeline(days_back: int):
    from app.services.order_pipeline import run_order_pipeline as _pipeline
    return await _pipeline(days_back=days_back)
