"""
In-process scheduler -- thin orchestrator.

Delegates all job definitions to domain-specific modules under
``app.platform.scheduler.*``.  This file retains the public API
(``start_scheduler``, ``stop_scheduler``, ``scheduler``) so that
main.py, guardrails.py and tests keep working without changes.

Architecture (Sprint 2 -- scheduler decomposition):
  app/platform/scheduler/
    base.py       -- shared helpers (create_job_record, run_scheduled_job_type)
    registry.py   -- diagnostic job registry
    orders.py     -- 3 jobs  (order pipeline, listings, listing registry)
    finance.py    -- 5 jobs  (purchase prices, ECB, finances, fee-gap, COGS import)
    inventory.py  -- 7 jobs  (inventory, sales traffic, FBA inv/inbound/recon/alerts, returns)
    ads.py        -- 1 job   (Amazon Ads sync)
    profit.py     -- 4 jobs  (TKL cache, calc profit, COGS audit, profitability chain)
    content.py    -- 3 jobs  (PTD cache, pricing state, content publish queue)
    logistics.py  -- 5 jobs  (GLS, DHL, courier estimation, billing verify, BL distribution)
    strategy.py   -- 4 jobs  (DI outcome/learning/recalibration, search terms)
    seasonality.py-- 3 jobs  (monthly build, profile recompute, opportunity detection)
    system.py     -- 7 jobs  (retries, alerts, guardrails, taxonomy, pricing archive, SQS, events)
"""
from __future__ import annotations

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.core.scheduler_lock import scheduler_lock
from app.platform.scheduler import register_all_domains
from app.platform.scheduler.base import ascii_safe

log = structlog.get_logger(__name__)

scheduler = AsyncIOScheduler(timezone="Europe/Warsaw")


# ---------------------------------------------------------------------------
# Public API -- called from FastAPI lifespan
# ---------------------------------------------------------------------------

def start_scheduler() -> None:
    """Register all scheduled jobs and start the APScheduler.

    Leader lock is acquired in main.py lifespan (async) BEFORE calling
    this function.  If we reach here it means we ARE the leader.
    """
    if not scheduler_lock.is_leader:
        log.warning("scheduler.skip_not_leader", worker_id=scheduler_lock.worker_id)
        return

    register_all_domains(scheduler)

    scheduler.start()

    jobs = scheduler.get_jobs()
    for j in jobs:
        log.info(
            "scheduler.job_registered",
            id=j.id,
            name=ascii_safe(j.name),
            next_run=str(j.next_run_time),
        )
    log.info("scheduler.started", total_jobs=len(jobs))


def stop_scheduler() -> None:
    """Gracefully shut down the scheduler.

    Lock release is handled in main.py lifespan (async) AFTER this call.
    """
    if scheduler.running:
        scheduler.shutdown(wait=False)
        log.info("scheduler.stopped")