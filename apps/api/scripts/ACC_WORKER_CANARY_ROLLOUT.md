# ACC Worker Canary Rollout (RabbitMQ + Celery)

## Process split
- API process:
  - `SCHEDULER_ENABLED=false`
  - `WORKER_EXECUTION_ENABLED=false`
- Scheduler process:
  - `SCHEDULER_ENABLED=true`
  - `WORKER_EXECUTION_ENABLED=false`
  - run: `python scripts/run_scheduler_process.py`
- Worker process(es):
  - `SCHEDULER_ENABLED=false`
  - `WORKER_EXECUTION_ENABLED=true`

## Queue profiles (recommended)
- `courier.heavy`: concurrency 1
- `inventory.heavy`: concurrency 1
- `finance.heavy`: concurrency 1
- `fba.medium`: concurrency 2
- `core.medium`: concurrency 2
- `light.default`: concurrency 4

## Canary mode
- Day 1: `JOB_CANARY_MODE=courier_fba`
- Day 2: `JOB_CANARY_MODE=all`
- Backout: set `JOB_CANARY_MODE=off` (inline fallback).

## Worker examples
```powershell
celery -A app.worker.celery_app worker -Q courier.heavy --concurrency=1 --prefetch-multiplier=1 -n courier@%h
celery -A app.worker.celery_app worker -Q fba.medium --concurrency=2 --prefetch-multiplier=1 -n fba@%h
celery -A app.worker.celery_app worker -Q core.medium --concurrency=2 --prefetch-multiplier=1 -n core@%h
celery -A app.worker.celery_app worker -Q inventory.heavy --concurrency=1 --prefetch-multiplier=1 -n inventory@%h
celery -A app.worker.celery_app worker -Q finance.heavy --concurrency=1 --prefetch-multiplier=1 -n finance@%h
celery -A app.worker.celery_app worker -Q light.default --concurrency=4 --prefetch-multiplier=1 -n light@%h
```

## Operational checks
- Queue/health report: `python scripts/job_queue_observability_report.py`
- Orphan cleanup dry-run: `python scripts/cleanup_orphaned_pending_jobs.py --minutes-old 30`
- Orphan cleanup apply: `python scripts/cleanup_orphaned_pending_jobs.py --minutes-old 30 --apply`
