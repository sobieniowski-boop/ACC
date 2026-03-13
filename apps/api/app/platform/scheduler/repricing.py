"""Repricing Engine domain — daily proposal computation, execution, analytics.

Sprint 15 – S15.5 — Runs after BuyBox radar (03:30), scheduled at 04:00.
Sprint 16 – S16.4 — Auto-approve at 04:15, execute at 04:30, analytics at 05:00.
"""
from __future__ import annotations

import asyncio

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.connectors.mssql import set_job_failure, set_job_success
from app.platform.scheduler.base import create_job_record

log = structlog.get_logger(__name__)


async def _repricing_proposal_computation() -> None:
    """Daily 04:00 — Evaluate all active strategies, enforce guardrails,
    create execution proposals for human review."""
    job_id = create_job_record("repricing_proposal_computation")
    log.info("scheduler.repricing_proposal_computation.start", job_id=job_id)
    try:
        from app.intelligence.repricing_engine import (
            compute_repricing_proposals,
            ensure_repricing_schema,
        )
        await asyncio.to_thread(ensure_repricing_schema)

        from app.core.config import MARKETPLACE_REGISTRY
        total_proposals = 0
        for mkt_id in MARKETPLACE_REGISTRY:
            try:
                count = await asyncio.to_thread(
                    compute_repricing_proposals, mkt_id,
                )
                total_proposals += count
            except Exception as mkt_exc:
                log.warning(
                    "scheduler.repricing_proposal_computation.market_error",
                    marketplace_id=mkt_id, error=str(mkt_exc),
                )

        set_job_success(
            job_id,
            records_processed=total_proposals,
            message=f"proposals={total_proposals}",
        )
        log.info(
            "scheduler.repricing_proposal_computation.done",
            proposals=total_proposals,
        )
    except Exception as exc:
        log.error("scheduler.repricing_proposal_computation.error", error=str(exc))
        set_job_failure(job_id, str(exc))


def register(scheduler: AsyncIOScheduler) -> None:
    scheduler.add_job(
        _repricing_proposal_computation,
        trigger=CronTrigger(hour=4, minute=0),
        id="repricing-proposal-computation",
        name="Repricing Proposal Computation (04:00)",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=600,
    )
    scheduler.add_job(
        _repricing_auto_approve_and_execute,
        trigger=CronTrigger(hour=4, minute=15),
        id="repricing-auto-approve-execute",
        name="Repricing Auto-Approve & Execute (04:15)",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=600,
    )
    scheduler.add_job(
        _repricing_daily_analytics,
        trigger=CronTrigger(hour=5, minute=0),
        id="repricing-daily-analytics",
        name="Repricing Daily Analytics (05:00)",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=600,
    )


async def _repricing_auto_approve_and_execute() -> None:
    """04:15 — Auto-approve small changes, then execute all approved prices."""
    job_id = create_job_record("repricing_auto_approve_execute")
    log.info("scheduler.repricing_auto_approve_execute.start", job_id=job_id)
    try:
        from app.intelligence.repricing_engine import (
            auto_approve_proposals,
            execute_approved_prices,
        )
        from app.core.config import MARKETPLACE_REGISTRY

        # Phase 1: auto-approve
        auto_count = await asyncio.to_thread(auto_approve_proposals)

        # Phase 2: execute per marketplace
        total_submitted = 0
        for mkt_id in MARKETPLACE_REGISTRY:
            try:
                result = await asyncio.to_thread(execute_approved_prices, mkt_id)
                total_submitted += result.get("submitted", 0)
            except Exception as mkt_exc:
                log.warning(
                    "scheduler.repricing_execute.market_error",
                    marketplace_id=mkt_id, error=str(mkt_exc),
                )

        set_job_success(
            job_id,
            records_processed=total_submitted,
            message=f"auto_approved={auto_count}, executed={total_submitted}",
        )
        log.info(
            "scheduler.repricing_auto_approve_execute.done",
            auto_approved=auto_count, executed=total_submitted,
        )
    except Exception as exc:
        log.error("scheduler.repricing_auto_approve_execute.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _repricing_daily_analytics() -> None:
    """05:00 — Compute daily repricing analytics for all marketplaces."""
    job_id = create_job_record("repricing_daily_analytics")
    log.info("scheduler.repricing_daily_analytics.start", job_id=job_id)
    try:
        from app.intelligence.repricing_engine import compute_daily_analytics
        from app.core.config import MARKETPLACE_REGISTRY

        results = []
        for mkt_id in MARKETPLACE_REGISTRY:
            try:
                r = await asyncio.to_thread(compute_daily_analytics, marketplace_id=mkt_id)
                results.append(r)
            except Exception as mkt_exc:
                log.warning(
                    "scheduler.repricing_analytics.market_error",
                    marketplace_id=mkt_id, error=str(mkt_exc),
                )

        # Also compute global (no marketplace filter)
        try:
            global_r = await asyncio.to_thread(compute_daily_analytics)
            results.append(global_r)
        except Exception:
            pass

        total = sum(r.get("proposals_created", 0) for r in results)
        set_job_success(
            job_id,
            records_processed=total,
            message=f"analytics_records={len(results)}",
        )
        log.info("scheduler.repricing_daily_analytics.done", records=len(results))
    except Exception as exc:
        log.error("scheduler.repricing_daily_analytics.error", error=str(exc))
        set_job_failure(job_id, str(exc))
