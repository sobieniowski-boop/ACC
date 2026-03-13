"""Ads domain — Amazon Advertising sync (profiles, campaigns, daily reports)."""
from __future__ import annotations

import asyncio

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.connectors.mssql import set_job_failure, set_job_success
from app.core.config import settings
from app.platform.scheduler.base import create_job_record, run_scheduled_job_type

log = structlog.get_logger(__name__)


async def _sync_ads() -> None:
    """Every 4h — sync Amazon Ads API data (profiles, campaigns, daily reports)."""
    if not settings.WORKER_EXECUTION_ENABLED:
        await asyncio.to_thread(run_scheduled_job_type, "sync_ads", {"days_back": 14})
        return
    job_id = create_job_record("sync_ads")
    log.info("scheduler.sync_ads.start", job_id=job_id)
    try:
        from app.services.ads_sync import run_full_ads_sync
        result = await run_full_ads_sync(days_back=14)
        status = result.get("status", "ok")
        rows = result.get("daily_rows_upserted", 0)
        campaigns = result.get("campaigns_upserted", 0)
        set_job_success(
            job_id, records_processed=rows,
            message=f"ads sync {status}: campaigns={campaigns}, daily_rows={rows}",
        )
        log.info("scheduler.sync_ads.done", **result)

        # Emit domain event for downstream triggers (profitability chain)
        from app.services.event_backbone import emit_domain_event
        from datetime import date
        emit_domain_event(
            "ads", "synced",
            {"daily_rows": rows, "campaigns": campaigns, "days_back": 14},
            idempotency_key=f"ads_sync_{date.today().isoformat()}",
        )
    except Exception as exc:
        log.error("scheduler.sync_ads.error", error=str(exc))
        set_job_failure(job_id, str(exc))


def register(scheduler: AsyncIOScheduler) -> None:
    scheduler.add_job(
        _sync_ads,
        trigger=IntervalTrigger(hours=4),
        id="sync-ads-4h",
        name="Sync Amazon Ads (every 4h)",
        replace_existing=True, max_instances=1, misfire_grace_time=600,
    )
