"""Strategy & Decision Intelligence domain — executive, strategy, DI outcome/learning/recalibration, search terms."""
from __future__ import annotations

import asyncio

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.connectors.mssql import set_job_failure, set_job_success
from app.platform.scheduler.base import create_job_record

log = structlog.get_logger(__name__)


async def _decision_outcome_evaluation() -> None:
    """07:00 — Decision Intelligence: evaluate outcomes for matured monitoring windows."""
    job_id = create_job_record("decision_outcome_evaluation")
    log.info("scheduler.decision_outcome_eval.start", job_id=job_id)
    try:
        from app.services.decision_intelligence_service import run_outcome_monitoring
        result = await asyncio.to_thread(run_outcome_monitoring)
        set_job_success(
            job_id, records_processed=result.get("evaluated", 0),
            message=f"evaluated={result.get('evaluated',0)} expired={result.get('expired',0)} elapsed={result.get('elapsed_sec',0)}s",
        )
        log.info("scheduler.decision_outcome_eval.done", result=result)
    except Exception as exc:
        log.error("scheduler.decision_outcome_eval.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _decision_learning_aggregation() -> None:
    """Sun 08:00 — Decision Intelligence: aggregate per-type learning statistics."""
    job_id = create_job_record("decision_learning_aggregation")
    log.info("scheduler.decision_learning.start", job_id=job_id)
    try:
        from app.services.decision_intelligence_service import run_learning_aggregation
        result = await asyncio.to_thread(run_learning_aggregation)
        set_job_success(
            job_id, records_processed=result.get("types_updated", 0),
            message=f"types_updated={result.get('types_updated',0)} elapsed={result.get('elapsed_sec',0)}s",
        )
        log.info("scheduler.decision_learning.done", result=result)
    except Exception as exc:
        log.error("scheduler.decision_learning.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _decision_model_recalibration() -> None:
    """1st of month 09:00 — Decision Intelligence: recalibrate confidence & priority weights."""
    job_id = create_job_record("decision_model_recalibration")
    log.info("scheduler.decision_recalibration.start", job_id=job_id)
    try:
        from app.services.decision_intelligence_service import run_model_recalibration
        result = await asyncio.to_thread(run_model_recalibration)
        set_job_success(
            job_id, records_processed=result.get("types_adjusted", 0),
            message=f"types_adjusted={result.get('types_adjusted',0)} elapsed={result.get('elapsed_sec',0)}s",
        )
        log.info("scheduler.decision_recalibration.done", result=result)
    except Exception as exc:
        log.error("scheduler.decision_recalibration.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _sync_search_terms() -> None:
    """Weekly Wed 03:00 — Sync Brand Analytics search term reports."""
    job_id = create_job_record("sync_search_terms")
    log.info("scheduler.search_terms.start", job_id=job_id)
    try:
        from app.services.search_term_sync import sync_search_terms
        result = await sync_search_terms(months_back=3)
        set_job_success(
            job_id, records_processed=result.get("total_monthly_rows", 0),
            message=f"monthly={result.get('total_monthly_rows',0)} mkts={result.get('per_marketplace',{})}",
        )
        log.info("scheduler.search_terms.done", result=result)
    except Exception as exc:
        log.error("scheduler.search_terms.error", error=str(exc))
        set_job_failure(job_id, str(exc))


def register(scheduler: AsyncIOScheduler) -> None:
    # NOTE: executive_pipeline and strategy_detection are chained inside
    # _recompute_profitability in the profit domain (dependency-coupled).
    # Individual functions remain available for manual API triggers.

    scheduler.add_job(
        _decision_outcome_evaluation,
        trigger=CronTrigger(hour=7, minute=0),
        id="decision-outcome-evaluation-daily",
        name="Decision Outcome Evaluation (07:00)",
        replace_existing=True, max_instances=1, misfire_grace_time=600,
    )
    scheduler.add_job(
        _decision_learning_aggregation,
        trigger=CronTrigger(day_of_week="sun", hour=8, minute=0),
        id="decision-learning-weekly",
        name="Decision Learning Aggregation (Sun 08:00)",
        replace_existing=True, max_instances=1, misfire_grace_time=1200,
    )
    scheduler.add_job(
        _decision_model_recalibration,
        trigger=CronTrigger(day=1, hour=9, minute=0),
        id="decision-model-recalibration-monthly",
        name="Decision Model Recalibration (1st 09:00)",
        replace_existing=True, max_instances=1, misfire_grace_time=1200,
    )
    scheduler.add_job(
        _sync_search_terms,
        trigger=CronTrigger(day_of_week="wed", hour=3, minute=0),
        id="sync-search-terms-weekly",
        name="Sync Search Terms (Wed 03:00)",
        replace_existing=True, max_instances=1, misfire_grace_time=1200,
    )
