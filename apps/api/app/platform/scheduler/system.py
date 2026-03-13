"""System domain — retries, alerts, guardrails, pricing archive, taxonomy, SQS/events."""
from __future__ import annotations

import asyncio

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.connectors.mssql import dispatch_due_retry_jobs, set_job_failure, set_job_success
from app.core.config import settings
from app.platform.scheduler.base import create_job_record, run_scheduled_job_type

log = structlog.get_logger(__name__)


async def _dispatch_retryable_jobs() -> None:
    """Every 1 min — dispatch due retryable acc_al_jobs."""
    try:
        result = await asyncio.to_thread(dispatch_due_retry_jobs, 5)
        dispatched = int(result.get("dispatched", 0) or 0)
        if dispatched:
            log.info("scheduler.retry_dispatch.done", dispatched=dispatched,
                     job_ids=result.get("job_ids") or [])
    except Exception as exc:
        log.error("scheduler.retry_dispatch.error", error=str(exc))


async def _evaluate_alerts() -> None:
    """Every 60 min — evaluate alert rules + task SLA breaches."""
    job_id = create_job_record("evaluate_alerts")
    log.info("scheduler.evaluate_alerts.start", job_id=job_id)
    try:
        from app.connectors.mssql import evaluate_alert_rules
        from app.services.courier_alerts import evaluate_courier_alerts
        from app.services.finance_center import evaluate_finance_completeness_alerts
        from app.services.order_pipeline import evaluate_order_sync_gap_alerts

        created = evaluate_alert_rules(days=7)
        courier_health = evaluate_courier_alerts(window_days=7)
        finance_health = evaluate_finance_completeness_alerts(days_back=30)
        order_sync_health = evaluate_order_sync_gap_alerts()

        courier_created = int(courier_health.get("created", 0) or 0)
        courier_updated = int(courier_health.get("updated", 0) or 0)
        courier_resolved = int(courier_health.get("resolved", 0) or 0)
        courier_items = int(len(courier_health.get("items") or []))
        health_status = str(finance_health.get("status") or "unknown")
        health_created = int(finance_health.get("created", 0) or 0)
        health_updated = int(finance_health.get("updated", 0) or 0)
        order_sync_status = str(order_sync_health.get("status") or "unknown")
        order_sync_created = int(order_sync_health.get("created", 0) or 0)
        order_sync_updated = int(order_sync_health.get("updated", 0) or 0)

        set_job_success(
            job_id,
            records_processed=created + courier_items + health_created + order_sync_created,
            message=(
                f"alerts_created={created}, finance_completeness={health_status}, "
                f"courier_created={courier_created}, courier_updated={courier_updated}, courier_resolved={courier_resolved}, "
                f"health_created={health_created}, health_updated={health_updated}, "
                f"order_sync={order_sync_status}, order_sync_created={order_sync_created}, order_sync_updated={order_sync_updated}"
            ),
        )
        log.info("scheduler.evaluate_alerts.done",
                 created=created, courier_health=courier_health,
                 finance_health=finance_health, order_sync_health=order_sync_health)
    except Exception as exc:
        log.error("scheduler.evaluate_alerts.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _run_guardrails() -> None:
    """Hourly — pipeline freshness + financial integrity checks."""
    job_id = create_job_record("guardrails")
    log.info("scheduler.guardrails.start", job_id=job_id)
    try:
        from app.services.guardrails import run_guardrails
        result = await run_guardrails(persist=True)
        status = result.get("status", "unknown")
        total = result.get("total_checks", 0)
        summary = result.get("summary", {})
        set_job_success(
            job_id, records_processed=total,
            message=f"status={status} ok={summary.get('ok',0)} warn={summary.get('warning',0)} crit={summary.get('critical',0)}",
        )
        log.info("scheduler.guardrails.done", status=status, summary=summary)
    except Exception as exc:
        log.error("scheduler.guardrails.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _sync_taxonomy() -> None:
    """Nightly — build taxonomy predictions and optionally auto-apply."""
    limit = max(1, int(settings.TAXONOMY_SYNC_LIMIT or 10000))
    min_confidence = float(settings.TAXONOMY_SYNC_MIN_CONFIDENCE or 0.95)
    min_confidence = max(0.0, min(1.0, min_confidence))
    params = {
        "limit": limit,
        "auto_apply": bool(settings.TAXONOMY_SYNC_AUTO_APPLY),
        "min_auto_confidence": min_confidence,
    }
    log.info("scheduler.sync_taxonomy.start", **params)
    try:
        result = await asyncio.to_thread(run_scheduled_job_type, "sync_taxonomy", params)
        log.info("scheduler.sync_taxonomy.done", job_id=result.get("id"), **params)
    except Exception as exc:
        log.error("scheduler.sync_taxonomy.error", error=str(exc))


async def _archive_pricing_snapshots() -> None:
    """Daily 02:00 — archive pricing snapshots older than 30 days."""
    job_id = create_job_record("pricing_snapshot_archive")
    log.info("scheduler.pricing_snapshot_archive.start", job_id=job_id)
    try:
        from app.services.pricing_state import archive_old_snapshots
        result = await asyncio.to_thread(archive_old_snapshots)
        archived = result.get("archived", 0)
        set_job_success(
            job_id, records_processed=archived,
            message=f"Archived {archived} snapshots in {result.get('batches', 0)} batches",
        )
        log.info("scheduler.pricing_snapshot_archive.done", **result)
    except Exception as exc:
        log.error("scheduler.pricing_snapshot_archive.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _poll_sqs_notifications() -> None:
    """Every 2 min — poll SQS for SP-API notifications → Event Backbone."""
    job_id = create_job_record("poll_sqs_notifications")
    log.info("scheduler.poll_sqs.start", job_id=job_id)
    try:
        from app.services.event_backbone import poll_sqs
        result = await asyncio.to_thread(poll_sqs, max_messages=10)
        received = result.get("received", 0)
        set_job_success(job_id, records_processed=received, message=f"received={received}")
        if received:
            log.info("scheduler.poll_sqs.done", received=received)
    except Exception as exc:
        log.error("scheduler.poll_sqs.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _process_notification_events() -> None:
    """Every 5 min — process pending events from Event Backbone."""
    job_id = create_job_record("process_notification_events")
    log.info("scheduler.process_events.start", job_id=job_id)
    try:
        from app.services.event_backbone import process_pending_events
        result = await asyncio.to_thread(process_pending_events, limit=100)
        processed = result.get("processed", 0)
        set_job_success(job_id, records_processed=processed, message=f"processed={processed}")
        if processed:
            log.info("scheduler.process_events.done", processed=processed)
    except Exception as exc:
        log.error("scheduler.process_events.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _poll_topology_queues() -> None:
    """Every 3 min — poll all domain queues via SQS topology."""
    job_id = create_job_record("poll_topology_queues")
    log.info("scheduler.poll_topology.start", job_id=job_id)
    try:
        from app.intelligence.event_wiring import poll_topology_queues
        result = await asyncio.to_thread(poll_topology_queues)
        received = result.get("total_received", 0)
        polled = result.get("queues_polled", 0)
        set_job_success(job_id, records_processed=received, message=f"queues={polled} received={received}")
        if received:
            log.info("scheduler.poll_topology.done", queues=polled, received=received)
    except Exception as exc:
        log.error("scheduler.poll_topology.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _run_refund_anomaly_scan() -> None:
    """Nightly — detect refund spikes, serial returners, reimbursement opportunities."""
    job_id = create_job_record("refund_anomaly_scan")
    log.info("scheduler.refund_anomaly_scan.start", job_id=job_id)
    try:
        from app.intelligence.refund_anomaly import run_full_scan
        result = await asyncio.to_thread(run_full_scan)
        spikes = result.get("refund_spikes", {}).get("anomalies_created", 0)
        returners = result.get("serial_returners", {}).get("returners_flagged", 0)
        cases = result.get("reimbursement", {}).get("cases_created", 0)
        total = spikes + returners + cases
        set_job_success(job_id, records_processed=total,
                        message=f"spikes={spikes} returners={returners} cases={cases}")
        log.info("scheduler.refund_anomaly_scan.done",
                 spikes=spikes, returners=returners, cases=cases)
    except Exception as exc:
        log.error("scheduler.refund_anomaly_scan.error", error=str(exc))
        set_job_failure(job_id, str(exc))


def register(scheduler: AsyncIOScheduler) -> None:
    scheduler.add_job(
        _dispatch_retryable_jobs,
        trigger=IntervalTrigger(minutes=1),
        id="dispatch-job-retries-1m",
        name="Dispatch Job Retries (1 min)",
        replace_existing=True, max_instances=1, misfire_grace_time=30,
    )
    scheduler.add_job(
        _evaluate_alerts,
        trigger=IntervalTrigger(minutes=60),
        id="evaluate-alerts-hourly",
        name="Evaluate Alerts (60 min)",
        replace_existing=True, max_instances=1, misfire_grace_time=120,
    )
    scheduler.add_job(
        _run_guardrails,
        trigger=IntervalTrigger(minutes=60),
        id="guardrails-hourly",
        name="Runtime Guardrails (60 min)",
        replace_existing=True, max_instances=1, misfire_grace_time=120,
    )

    if settings.TAXONOMY_SYNC_ENABLED:
        scheduler.add_job(
            _sync_taxonomy,
            trigger=CronTrigger(
                hour=int(settings.TAXONOMY_SYNC_HOUR),
                minute=int(settings.TAXONOMY_SYNC_MINUTE),
            ),
            id="sync-taxonomy-nightly",
            name=f"Sync Taxonomy ({int(settings.TAXONOMY_SYNC_HOUR):02d}:{int(settings.TAXONOMY_SYNC_MINUTE):02d})",
            replace_existing=True, max_instances=1, misfire_grace_time=900,
        )
    else:
        log.info("scheduler.sync_taxonomy.disabled")

    scheduler.add_job(
        _archive_pricing_snapshots,
        trigger=CronTrigger(hour=2, minute=0),
        id="pricing-snapshot-archive-daily",
        name="Pricing Snapshot Archive (02:00)",
        replace_existing=True, max_instances=1, misfire_grace_time=600,
    )
    scheduler.add_job(
        _poll_sqs_notifications,
        trigger=IntervalTrigger(minutes=2),
        id="poll-sqs-notifications-2m",
        name="Poll SQS Notifications (2 min)",
        replace_existing=True, max_instances=1, misfire_grace_time=60,
    )
    scheduler.add_job(
        _process_notification_events,
        trigger=IntervalTrigger(minutes=5),
        id="process-notification-events-5m",
        name="Process Notification Events (5 min)",
        replace_existing=True, max_instances=1, misfire_grace_time=120,
    )
    scheduler.add_job(
        _poll_topology_queues,
        trigger=IntervalTrigger(minutes=3),
        id="poll-topology-queues-3m",
        name="Poll Topology Domain Queues (3 min)",
        replace_existing=True, max_instances=1, misfire_grace_time=120,
    )
    scheduler.add_job(
        _run_refund_anomaly_scan,
        trigger=CronTrigger(hour=3, minute=30),
        id="refund-anomaly-scan-nightly",
        name="Refund Anomaly Scan (03:30)",
        replace_existing=True, max_instances=1, misfire_grace_time=900,
    )
