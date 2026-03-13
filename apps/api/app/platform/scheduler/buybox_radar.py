"""Buy Box Radar domain — daily win-rate computation & loss alert detection.

Sprint 11 – S11.6
"""
from __future__ import annotations

import asyncio

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.connectors.mssql import set_job_failure, set_job_success
from app.platform.scheduler.base import create_job_record

log = structlog.get_logger(__name__)


async def _buybox_trend_computation() -> None:
    """Daily 03:30 — Compute BuyBox win-rate trends from pricing snapshots."""
    job_id = create_job_record("buybox_trend_computation")
    log.info("scheduler.buybox_trend_computation.start", job_id=job_id)
    try:
        from app.intelligence.buybox_radar import (
            capture_competitor_offers,
            compute_daily_buybox_trends,
            ensure_buybox_radar_schema,
            raise_sustained_loss_alerts,
        )
        await asyncio.to_thread(ensure_buybox_radar_schema)

        # Step 1: Capture fresh competitor offers
        from app.core.config import MARKETPLACE_REGISTRY
        capture_total = 0
        for mkt_id in MARKETPLACE_REGISTRY:
            try:
                result = await capture_competitor_offers(mkt_id, asin_limit=50)
                capture_total += result.get("offers_recorded", 0)
            except Exception as cap_exc:
                log.warning("scheduler.buybox_competitor_capture_error",
                            marketplace_id=mkt_id, error=str(cap_exc))

        # Step 2: Compute trends + raise alerts
        upserted = await asyncio.to_thread(compute_daily_buybox_trends)
        alerts = await asyncio.to_thread(raise_sustained_loss_alerts)
        set_job_success(
            job_id, records_processed=upserted,
            message=f"capture={capture_total}, trends={upserted}, alerts={alerts}",
        )
        log.info("scheduler.buybox_trend_computation.done",
                 capture=capture_total, upserted=upserted, alerts=alerts)
    except Exception as exc:
        log.error("scheduler.buybox_trend_computation.error", error=str(exc))
        set_job_failure(job_id, str(exc))


def register(scheduler: AsyncIOScheduler) -> None:
    scheduler.add_job(
        _buybox_trend_computation,
        trigger=CronTrigger(hour=3, minute=30),
        id="buybox-trend-computation",
        name="BuyBox Trend Computation (03:30)",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=600,
    )
