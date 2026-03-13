from __future__ import annotations

from app.connectors.mssql import run_job_type, set_job_failure
from app.core.config import settings
from app.worker import celery_app


@celery_app.task(name="app.jobs.acc_job.run_acc_job_task", bind=True, max_retries=0)
def run_acc_job_task(self, job_id: str) -> dict:
    if not settings.WORKER_EXECUTION_ENABLED:
        raise RuntimeError("WORKER_EXECUTION_ENABLED=false; refusing to execute job payload")
    try:
        return run_job_type(
            job_id,
            worker_id=f"celery:{self.request.hostname}:{self.request.id}",
            lease_seconds=900,
        )
    except Exception as exc:
        set_job_failure(job_id, exc)
        raise
