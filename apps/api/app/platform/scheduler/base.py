"""Shared helpers for all scheduler domain modules.

Centralises _create_job_record, _ascii_safe, _run_scheduled_job_type
so domain modules don't duplicate boilerplate.
"""
from __future__ import annotations

import uuid

import structlog

from app.connectors.mssql import (
    create_job,
    enqueue_job,
)


log = structlog.get_logger(__name__)


def create_job_record(job_type: str, *, params: dict | None = None) -> str:
    """Create a JobRun row so the UI can track progress."""
    try:
        job = create_job(
            job_type=job_type,
            trigger_source="scheduler",
            triggered_by="apscheduler",
            params=params,
        )
        return job["id"]
    except Exception as exc:
        log.warning("scheduler.create_job_failed", job_type=job_type, error=str(exc))
        return str(uuid.uuid4())


def ascii_safe(value: object) -> str:
    return str(value).encode("ascii", "replace").decode("ascii")


def run_scheduled_job_type(job_type: str, params: dict | None = None) -> dict:
    log.info("scheduler.job_type.enqueue", job_type=job_type, params=params or {})
    try:
        result = enqueue_job(
            job_type=job_type,
            trigger_source="scheduler",
            triggered_by="apscheduler",
            params=params or {},
        )
        log.info("scheduler.job_type.enqueued", job_type=job_type, job_id=result.get("id"))
        return result
    except Exception as exc:
        log.error("scheduler.job_type.error", job_type=job_type, error=str(exc))
        raise
