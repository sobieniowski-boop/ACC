"""Content domain — PTD cache, content publish queue, pricing state."""
from __future__ import annotations

import asyncio

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.connectors.mssql import set_job_failure, set_job_success
from app.core.config import settings
from app.platform.scheduler.base import create_job_record, run_scheduled_job_type

log = structlog.get_logger(__name__)


async def _sync_ptd_cache() -> None:
    """02:30 — refresh Product Type Definitions cache from SP-API."""
    if not settings.WORKER_EXECUTION_ENABLED:
        await asyncio.to_thread(run_scheduled_job_type, "sync_ptd_cache", {})
        return
    job_id = create_job_record("sync_ptd_cache")
    log.info("scheduler.sync_ptd_cache.start", job_id=job_id)
    try:
        from app.services.ptd_cache import sync_all_marketplaces
        result = await sync_all_marketplaces()
        set_job_success(
            job_id, records_processed=result.get("synced", 0),
            message=(
                f"PTD cache sync mkt={result.get('marketplaces')} "
                f"synced={result.get('synced')} skipped={result.get('skipped')} "
                f"errors={result.get('errors')}"
            ),
        )
        log.info("scheduler.sync_ptd_cache.done", **result)
    except Exception as exc:
        log.error("scheduler.sync_ptd_cache.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _sync_pricing_state() -> None:
    """03:00 — capture pricing snapshots + evaluate pricing rules."""
    job_id = create_job_record("sync_pricing_state")
    log.info("scheduler.sync_pricing_state.start", job_id=job_id)
    try:
        from app.services.pricing_state import capture_all_marketplaces
        from app.services.pricing_rules import evaluate_all_marketplaces

        capture_result = await capture_all_marketplaces()
        eval_result = await asyncio.to_thread(evaluate_all_marketplaces)
        total_snaps = capture_result.get("snapshots", 0)
        total_recs = sum(r.get("recommendations_created", 0) for r in eval_result.get("results", []))
        set_job_success(
            job_id, records_processed=total_snaps,
            message=f"Pricing state: {total_snaps} snapshots, {total_recs} recommendations",
        )
        log.info("scheduler.sync_pricing_state.done", snapshots=total_snaps, recommendations=total_recs)
    except Exception as exc:
        log.error("scheduler.sync_pricing_state.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _process_content_publish_queue() -> None:
    """Every 1 min — process queued Content Ops publish/push jobs."""
    try:
        from app.services.content_ops import process_queued_publish_jobs, evaluate_publish_queue_alerts
        result = await asyncio.to_thread(process_queued_publish_jobs, limit=5)
        alerts = await asyncio.to_thread(evaluate_publish_queue_alerts, stale_minutes=30, threshold_count=5)
        log.info("scheduler.content_publish_queue.done", **result, **alerts)
    except Exception as exc:
        log.error("scheduler.content_publish_queue.error", error=str(exc))


async def _content_scoring_run() -> None:
    """05:30 — daily content quality score computation for all active marketplaces."""
    job_id = create_job_record("content_scoring_run")
    log.info("scheduler.content_scoring_run.start", job_id=job_id)
    try:
        from app.intelligence.content_optimization import score_listings_for_marketplace
        from app.services.content_ops._helpers import _DEFAULT_CONTENT_MARKETS, _marketplace_to_id
        total_scored = 0
        for mkt_code in _DEFAULT_CONTENT_MARKETS:
            mkt_id = _marketplace_to_id(mkt_code)
            if not mkt_id:
                continue
            result = await asyncio.to_thread(
                score_listings_for_marketplace, mkt_id, limit=2000,
            )
            total_scored += result.get("listings_scored", 0)
        set_job_success(
            job_id, records_processed=total_scored,
            message=f"Content scoring: {total_scored} listings scored",
        )
        log.info("scheduler.content_scoring_run.done", total_scored=total_scored)
    except Exception as exc:
        log.error("scheduler.content_scoring_run.error", error=str(exc))
        set_job_failure(job_id, str(exc))


def register(scheduler: AsyncIOScheduler) -> None:
    scheduler.add_job(
        _sync_ptd_cache,
        trigger=CronTrigger(hour=2, minute=30),
        id="sync-ptd-cache-daily",
        name="Sync PTD Cache (02:30)",
        replace_existing=True, max_instances=1, misfire_grace_time=600,
    )
    scheduler.add_job(
        _sync_pricing_state,
        trigger=CronTrigger(hour=3, minute=0),
        id="sync-pricing-state-daily",
        name="Pricing State (03:00)",
        replace_existing=True, max_instances=1, misfire_grace_time=600,
    )
    scheduler.add_job(
        _process_content_publish_queue,
        trigger=IntervalTrigger(minutes=1),
        id="content-publish-queue-1m",
        name="Content Publish Queue (1 min)",
        replace_existing=True, max_instances=1, misfire_grace_time=30,
    )
    scheduler.add_job(
        _content_scoring_run,
        trigger=CronTrigger(hour=5, minute=30),
        id="content-scoring-daily",
        name="Content Quality Scoring (05:30)",
        replace_existing=True, max_instances=1, misfire_grace_time=600,
    )
