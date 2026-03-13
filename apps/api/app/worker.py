"""Celery application setup."""
from __future__ import annotations

from celery import Celery
from celery.schedules import crontab

from app.core.config import settings

celery_app = Celery(
    "acc_worker",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=[
        "app.jobs.sync_finances",
        "app.jobs.sync_inventory",
        "app.jobs.sync_purchase_prices",
        "app.jobs.sync_exchange_rates",
        "app.jobs.calc_profit",
        "app.jobs.order_pipeline",
        "app.jobs.acc_job",
    ],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Europe/Warsaw",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    result_expires=86400,  # 24h
    task_default_queue="light.default",
    task_default_routing_key="light.default",
    task_routes={
        "app.jobs.acc_job.run_acc_job_task": {"queue": "light.default", "routing_key": "light.default"},
    },
)

# Scheduled tasks (beat)
celery_app.conf.beat_schedule = {
    # Order pipeline every 15 min (sync + backfill + map + stamp)
    "order-pipeline-15m": {
        "task": "app.jobs.order_pipeline.run_order_pipeline",
        "schedule": crontab(minute="*/15"),
        "kwargs": {"days_back": 1},
    },
    # Sync finances daily at 3am (delegates to step_sync_finances — dedup safe)
    "sync-finances-daily": {
        "task": "app.jobs.sync_finances.sync_finances",
        "schedule": crontab(minute=0, hour=3),
        "kwargs": {"days_back": 7},
    },
    # Sync inventory daily at 4am
    "sync-inventory-daily": {
        "task": "app.jobs.sync_inventory.sync_inventory",
        "schedule": crontab(minute=0, hour=4),
        "kwargs": {"marketplace_id": None},
    },
    # Sync purchase prices at 2am (before profit calc)
    "sync-purchase-prices-nightly": {
        "task": "app.jobs.sync_purchase_prices.sync_purchase_prices",
        "schedule": crontab(minute=0, hour=2),
    },
    # Recalculate profit nightly at 5am
    "calc-profit-nightly": {
        "task": "app.jobs.calc_profit.calc_profit",
        "schedule": crontab(minute=0, hour=5),
        "kwargs": {"days_back": 1},
    },
    # Sync FX rates daily at 1:30am (before purchase prices & profit)
    "sync-fx-rates-daily": {
        "task": "app.jobs.sync_exchange_rates.sync_exchange_rates",
        "schedule": crontab(minute=30, hour=1),
        "kwargs": {"days_back": 7},
    },
}

if __name__ == "__main__":
    celery_app.start()
