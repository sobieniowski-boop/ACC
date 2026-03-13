"""Seasonality domain — monthly build, profile recompute, opportunity detection."""
from __future__ import annotations

import asyncio

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.connectors.mssql import set_job_failure, set_job_success
from app.platform.scheduler.base import create_job_record

log = structlog.get_logger(__name__)


async def _seasonality_build_monthly() -> None:
    """Daily 04:30 — Aggregate monthly seasonality metrics from rollup data."""
    job_id = create_job_record("seasonality_build_monthly")
    log.info("scheduler.seasonality_monthly.start", job_id=job_id)
    try:
        from app.services.seasonality_service import build_monthly_metrics
        result = await asyncio.to_thread(build_monthly_metrics)
        set_job_success(
            job_id, records_processed=result.get("sku_rows", 0),
            message=f"sku={result.get('sku_rows',0)} cat={result.get('category_rows',0)}",
        )
        log.info("scheduler.seasonality_monthly.done", result=result)
    except Exception as exc:
        log.error("scheduler.seasonality_monthly.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _seasonality_recompute_profiles() -> None:
    """Weekly Sun 05:00 — Recompute indices and profiles."""
    job_id = create_job_record("seasonality_recompute_profiles")
    log.info("scheduler.seasonality_profiles.start", job_id=job_id)
    try:
        from app.services.seasonality_service import recompute_indices, recompute_profiles
        idx_result = await asyncio.to_thread(recompute_indices)
        prof_result = await asyncio.to_thread(recompute_profiles)
        total = idx_result.get("entities_processed", 0) + prof_result.get("entities_classified", 0)
        set_job_success(
            job_id, records_processed=total,
            message=f"indices={idx_result.get('entities_processed',0)} profiles={prof_result.get('entities_classified',0)}",
        )
        log.info("scheduler.seasonality_profiles.done", indices=idx_result, profiles=prof_result)
    except Exception as exc:
        log.error("scheduler.seasonality_profiles.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _seasonality_detect_opportunities() -> None:
    """Weekly Mon 05:30 — Detect seasonal opportunities."""
    job_id = create_job_record("seasonality_detect_opportunities")
    log.info("scheduler.seasonality_opps.start", job_id=job_id)
    try:
        from app.services.seasonality_opportunity_engine import detect_seasonality_opportunities
        result = await asyncio.to_thread(detect_seasonality_opportunities)
        set_job_success(
            job_id, records_processed=result.get("opportunities_created", 0),
            message=f"opps_created={result.get('opportunities_created',0)}",
        )
        log.info("scheduler.seasonality_opps.done", result=result)
    except Exception as exc:
        log.error("scheduler.seasonality_opps.error", error=str(exc))
        set_job_failure(job_id, str(exc))


def register(scheduler: AsyncIOScheduler) -> None:
    scheduler.add_job(
        _seasonality_build_monthly,
        trigger=CronTrigger(hour=4, minute=30),
        id="seasonality-build-monthly-daily",
        name="Seasonality Monthly Build (04:30)",
        replace_existing=True, max_instances=1, misfire_grace_time=600,
    )
    scheduler.add_job(
        _seasonality_recompute_profiles,
        trigger=CronTrigger(day_of_week="sun", hour=5, minute=0),
        id="seasonality-recompute-profiles-weekly",
        name="Seasonality Profiles Recompute (Sun 05:00)",
        replace_existing=True, max_instances=1, misfire_grace_time=1200,
    )
    scheduler.add_job(
        _seasonality_detect_opportunities,
        trigger=CronTrigger(day_of_week="mon", hour=5, minute=30),
        id="seasonality-detect-opps-weekly",
        name="Seasonality Opportunity Detection (Mon 05:30)",
        replace_existing=True, max_instances=1, misfire_grace_time=1200,
    )
