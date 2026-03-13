"""Profit domain — TKL cache, calc profit, COGS audit, profitability chain.

The profitability chain is event-driven: each step triggers on the
predecessor's completion event.  A 05:45 safety-net cron job catches
any case where events were missed.
"""
from __future__ import annotations

import asyncio

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.connectors.mssql import set_job_failure, set_job_success
from app.core.config import settings
from app.platform.scheduler.base import create_job_record, run_scheduled_job_type

log = structlog.get_logger(__name__)


async def _sync_tkl_cache() -> None:
    """01:40 — prewarm/refresh TKL SQL cache from XLSX source files."""
    if not settings.WORKER_EXECUTION_ENABLED:
        await asyncio.to_thread(run_scheduled_job_type, "sync_tkl_cache", {"force": True})
        return
    job_id = create_job_record("sync_tkl_cache")
    log.info("scheduler.sync_tkl_cache.start", job_id=job_id)
    try:
        from app.services.profit_engine import refresh_tkl_sql_cache
        result = await asyncio.to_thread(refresh_tkl_sql_cache, force=True)
        records = int(result.get("country_pairs", 0) or 0) + int(result.get("sku_rows", 0) or 0)
        set_job_success(
            job_id, records_processed=records,
            message=(
                f"TKL cache refreshed, country_pairs={result.get('country_pairs', 0)}, "
                f"sku_rows={result.get('sku_rows', 0)}"
            ),
        )
        log.info("scheduler.sync_tkl_cache.done", **result)
    except Exception as exc:
        log.error("scheduler.sync_tkl_cache.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _calc_profit() -> None:
    """05:00 — recalculate contribution margin."""
    if not settings.WORKER_EXECUTION_ENABLED:
        await asyncio.to_thread(run_scheduled_job_type, "calc_profit", {"days_back": 7})
        return
    job_id = create_job_record("calc_profit")
    log.info("scheduler.calc_profit.start", job_id=job_id)
    try:
        from app.connectors.mssql import recalc_profit_orders
        from datetime import date as _date, timedelta as _td
        count = recalc_profit_orders(
            date_from=_date.today() - _td(days=7),
            date_to=_date.today(),
        )
        set_job_success(
            job_id, records_processed=count,
            message=f"Profit recalculated={count}",
        )
        log.info("scheduler.calc_profit.done", count=count)
    except Exception as exc:
        log.error("scheduler.calc_profit.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _nightly_audit() -> None:
    """05:30 — full COGS data quality audit with persistence."""
    job_id = create_job_record("cogs_audit")
    log.info("scheduler.nightly_audit.start", job_id=job_id)
    try:
        from app.services.cogs_audit import run_full_audit
        report = await asyncio.to_thread(
            run_full_audit, persist=True, trigger_source="scheduler",
        )
        status = report.get("overall_status", "ok")
        issues = report.get("total_issues", 0)
        cov = next(
            (c.get("cogs_coverage_pct") for c in report.get("checks", [])
             if c.get("check") == "coverage"),
            None,
        )
        set_job_success(
            job_id, records_processed=issues,
            message=f"Audit {status}: {issues} issues, coverage={cov}%",
        )
        log.info("scheduler.nightly_audit.done", status=status, issues=issues, coverage=cov)
    except Exception as exc:
        log.error("scheduler.nightly_audit.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _recompute_profitability() -> None:
    """05:45 — Safety-net: run the full profitability chain if events
    did not already trigger it today (e.g. ads or finance job failed).
    """
    from app.services.event_backbone import check_domain_events_today

    already_ran = check_domain_events_today("profitability", "chain_done")
    if already_ran:
        log.info("scheduler.profitability_chain.already_triggered_by_events")
        return

    log.info("scheduler.profitability_chain.safety_net_start")
    await _run_profitability_chain()


async def _run_profitability_chain() -> None:
    """Execute the full profitability chain: ads→finance→rollup→alerts→executive→strategy.

    Called either by the event-driven dependency gate (both ads.synced
    and finance.synced arrived today) OR by the 05:45 safety-net cron.
    """
    from app.services.event_backbone import emit_domain_event

    job_id = create_job_record("profitability_rollup")
    log.info("scheduler.profitability_chain.start", job_id=job_id)
    try:
        from datetime import date, timedelta

        # Step 1: Dependency syncs (ads + finance)
        dep_failed = False
        try:
            log.info("scheduler.profitability_chain.step_ads_sync")
            from app.services.ads_sync import run_full_ads_sync
            await run_full_ads_sync(days_back=3)
        except Exception as dep_exc:
            log.error("scheduler.profitability_chain.ads_sync_failed", error=str(dep_exc))
            dep_failed = True

        try:
            log.info("scheduler.profitability_chain.step_finance_sync")
            from app.services.order_pipeline import step_sync_finances
            await step_sync_finances(days_back=3, job_id=job_id)
        except Exception as dep_exc:
            log.error("scheduler.profitability_chain.finance_sync_failed", error=str(dep_exc))
            dep_failed = True

        if dep_failed:
            log.error("scheduler.profitability_chain.abort_dep_failure",
                      msg="Chain aborted — dependency sync failed.")
            set_job_failure(job_id, "Dependency sync (ads/finance) failed — full chain aborted")
            return

        log.info("scheduler.profitability_chain.deps_ok")

        # Step 2: Profitability rollup + alerts
        sku_rows, mkt_rows, alert_result = await _step_rollup_and_alerts()
        emit_domain_event("profitability", "rollup_done",
                          {"sku_rows": sku_rows, "mkt_rows": mkt_rows,
                           "alerts": alert_result.get("alerts_created", 0)},
                          idempotency_key=f"rollup_{date.today().isoformat()}")

        # Step 3: Executive pipeline
        exec_result = await _step_executive()
        exec_ok = exec_result is not None
        if exec_ok:
            emit_domain_event("profitability", "executive_done",
                              exec_result,
                              idempotency_key=f"executive_{date.today().isoformat()}")
        else:
            set_job_success(
                job_id, records_processed=sku_rows + mkt_rows,
                message=f"SKU={sku_rows} MKT={mkt_rows} alerts={alert_result.get('alerts_created', 0)} executive=FAILED strategy=SKIPPED",
            )
            return

        # Step 4: Strategy detection
        strat_opps = await _step_strategy()

        emit_domain_event("profitability", "chain_done",
                          {"sku_rows": sku_rows, "mkt_rows": mkt_rows,
                           "strategy_opps": strat_opps},
                          idempotency_key=f"chain_{date.today().isoformat()}")

        set_job_success(
            job_id, records_processed=sku_rows + mkt_rows,
            message=(
                f"SKU={sku_rows} MKT={mkt_rows} "
                f"alerts={alert_result.get('alerts_created', 0)} "
                f"exec_metrics={exec_result.get('metrics_rows', 0)} "
                f"risks={exec_result.get('risks_found', 0)} "
                f"strategy={strat_opps}"
            ),
        )
        log.info("scheduler.profitability_chain.complete")
    except Exception as exc:
        log.error("scheduler.profitability_chain.error", error=str(exc))
        set_job_failure(job_id, str(exc))


# ── Decomposed chain steps (reusable by event handlers) ───────────────────

async def _step_rollup_and_alerts() -> tuple:
    """Run profitability rollup + alerts. Returns (sku_rows, mkt_rows, alert_result)."""
    from datetime import date, timedelta
    from app.services.profitability_service import (
        evaluate_profitability_alerts,
        recompute_rollups,
    )
    result = await asyncio.to_thread(recompute_rollups, days_back=7)
    sku_rows = result.get("sku_rows_upserted", 0)
    mkt_rows = result.get("marketplace_rows_upserted", 0)

    alert_result = await asyncio.to_thread(
        evaluate_profitability_alerts,
        date_from=date.today() - timedelta(days=7),
        date_to=date.today(),
    )
    log.info("scheduler.profitability_chain.rollup_done",
             sku_rows=sku_rows, mkt_rows=mkt_rows,
             alerts=alert_result.get("alerts_created", 0))
    return sku_rows, mkt_rows, alert_result


async def _step_executive() -> dict | None:
    """Run executive pipeline. Returns result dict or None on failure."""
    try:
        log.info("scheduler.profitability_chain.step_executive")
        from app.services.executive_service import run_executive_pipeline
        exec_result = await asyncio.to_thread(run_executive_pipeline, days_back=7)
        log.info("scheduler.profitability_chain.executive_done",
                 metrics=exec_result.get("metrics_rows", 0),
                 risks=exec_result.get("risks_found", 0),
                 growth=exec_result.get("opportunities_found", 0))
        return exec_result
    except Exception as exc:
        log.error("scheduler.profitability_chain.executive_failed", error=str(exc))
        return None


async def _step_strategy() -> int | str:
    """Run strategy detection. Returns opportunities count or 'FAILED'."""
    try:
        log.info("scheduler.profitability_chain.step_strategy")
        from app.services.strategy_service import run_strategy_detection
        strat_result = await asyncio.to_thread(run_strategy_detection, days_back=30)
        opps = strat_result.get("opportunities_found", 0)
        log.info("scheduler.profitability_chain.strategy_done", opportunities=opps)
        return opps
    except Exception as exc:
        log.error("scheduler.profitability_chain.strategy_failed", error=str(exc))
        return "FAILED"


# ── Event-driven dependency gate ─────────────────────────────────────────

def _on_dependency_synced(event: dict) -> dict:
    """Event handler for ads.synced / finance.synced.

    When BOTH ads and finance have synced today, trigger the profitability
    chain via the event backbone rather than waiting for the 05:45 cron.
    """
    from app.services.event_backbone import check_domain_events_today, emit_domain_event

    ads_ok = check_domain_events_today("ads", "synced")
    finance_ok = check_domain_events_today("finance", "synced")

    if ads_ok and finance_ok:
        already_ran = check_domain_events_today("profitability", "chain_done")
        if already_ran:
            return {"action": "skipped", "reason": "chain_already_done_today"}

        log.info("scheduler.profitability_chain.event_triggered",
                 msg="Both ads + finance synced today → starting chain")

        # Run the chain synchronously (handler runs in a thread pool)
        import asyncio as _aio
        try:
            loop = _aio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # Already inside an event-loop — schedule as a task
            loop.create_task(_run_profitability_chain())
        else:
            _aio.run(_run_profitability_chain())

        return {"action": "chain_started"}

    waiting_for = []
    if not ads_ok:
        waiting_for.append("ads.synced")
    if not finance_ok:
        waiting_for.append("finance.synced")
    return {"action": "waiting", "missing": waiting_for}


def register_event_handlers() -> None:
    """Register profitability chain event handlers with the event backbone."""
    from app.services.event_backbone import register_handler

    register_handler(
        "ads", "synced",
        handler_name="profitability_dep_gate",
        handler_fn=_on_dependency_synced,
    )
    register_handler(
        "finance", "synced",
        handler_name="profitability_dep_gate_finance",
        handler_fn=_on_dependency_synced,
    )


def register(scheduler: AsyncIOScheduler) -> None:
    scheduler.add_job(
        _sync_tkl_cache,
        trigger=CronTrigger(hour=1, minute=40),
        id="sync-tkl-cache-daily",
        name="Sync TKL SQL Cache (01:40)",
        replace_existing=True, max_instances=1, misfire_grace_time=600,
    )
    scheduler.add_job(
        _calc_profit,
        trigger=CronTrigger(hour=5, minute=0),
        id="calc-profit-nightly",
        name="Calc Profit (05:00)",
        replace_existing=True, max_instances=1, misfire_grace_time=300,
    )
    scheduler.add_job(
        _nightly_audit,
        trigger=CronTrigger(hour=5, minute=30),
        id="cogs-audit-nightly",
        name="COGS Audit (05:30)",
        replace_existing=True, max_instances=1, misfire_grace_time=300,
    )
    scheduler.add_job(
        _recompute_profitability,
        trigger=CronTrigger(hour=5, minute=45),
        id="profitability-chain-daily",
        name="Profitability Chain Safety Net (05:45)",
        replace_existing=True, max_instances=1, misfire_grace_time=600,
    )

    # Register event-driven handlers for the profitability chain
    register_event_handlers()
