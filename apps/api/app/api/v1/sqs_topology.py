"""SQS Queue Topology API.

Sprint 19 – S19.3
"""
from __future__ import annotations

from typing import Optional

import structlog
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.intelligence import sqs_topology as topo

log = structlog.get_logger(__name__)
router = APIRouter(prefix="/sqs-topology", tags=["sqs-topology"])


# ── Request bodies ───────────────────────────────────────────────────

class RegisterQueueRequest(BaseModel):
    domain: str
    queue_url: str
    queue_arn: str | None = None
    dlq_url: str | None = None
    dlq_arn: str | None = None
    region: str = "eu-west-1"
    max_receive_count: int | None = None
    visibility_timeout_seconds: int | None = None
    message_retention_days: int | None = None
    polling_interval_seconds: int | None = None
    batch_size: int | None = None


class UpdateQueueStatusRequest(BaseModel):
    enabled: bool | None = None
    status: str | None = None


class ResolveDlqRequest(BaseModel):
    resolution: str
    resolved_by: str | None = None


class SeedTopologyRequest(BaseModel):
    base_queue_url: str = ""
    region: str = "eu-west-1"


# ── Topology endpoints ──────────────────────────────────────────────

@router.get("/queues")
def list_queues():
    """Get all registered queues in the topology."""
    return topo.get_queue_topology()


@router.get("/queues/{domain}")
def get_queue(domain: str):
    """Get queue config for a specific domain."""
    result = topo.get_queue_for_domain(domain)
    if not result:
        raise HTTPException(404, f"No queue configured for domain '{domain}'")
    return result


@router.post("/queues")
def register_queue(body: RegisterQueueRequest):
    """Register or update a domain queue."""
    try:
        return topo.register_queue(
            domain=body.domain,
            queue_url=body.queue_url,
            queue_arn=body.queue_arn,
            dlq_url=body.dlq_url,
            dlq_arn=body.dlq_arn,
            region=body.region,
            max_receive_count=body.max_receive_count,
            visibility_timeout_seconds=body.visibility_timeout_seconds,
            message_retention_days=body.message_retention_days,
            polling_interval_seconds=body.polling_interval_seconds,
            batch_size=body.batch_size,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@router.patch("/queues/{domain}/status")
def update_queue_status(domain: str, body: UpdateQueueStatusRequest):
    """Enable/disable a queue or change its status."""
    try:
        return topo.update_queue_status(
            domain, enabled=body.enabled, status=body.status,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@router.get("/health")
def topology_health():
    """Overall SQS topology health summary."""
    return topo.get_topology_health()


@router.get("/routing")
def routing_table():
    """Notification type → domain routing table."""
    return topo.get_routing_table()


@router.post("/poll/{domain}")
def poll_domain(domain: str, max_messages: int = Query(10, ge=1, le=100)):
    """Manually trigger a poll for a specific domain queue."""
    return topo.poll_domain_queue(domain, max_messages=max_messages)


@router.post("/poll-all")
def poll_all():
    """Poll all enabled domain queues."""
    return topo.poll_all_queues()


@router.post("/seed")
def seed_topology(body: SeedTopologyRequest):
    """Seed default topology entries for all 6 domains."""
    return topo.seed_default_topology(
        base_queue_url=body.base_queue_url, region=body.region,
    )


# ── DLQ endpoints ───────────────────────────────────────────────────

@router.get("/dlq")
def list_dlq_entries(
    domain: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """List dead-letter queue entries."""
    return topo.get_dlq_entries(domain, status=status, limit=limit, offset=offset)


@router.get("/dlq/summary")
def dlq_summary():
    """DLQ summary (total, unresolved, replayed, discarded)."""
    return topo.get_dlq_summary()


@router.post("/dlq/{entry_id}/resolve")
def resolve_dlq(entry_id: int, body: ResolveDlqRequest):
    """Resolve a DLQ entry."""
    try:
        return topo.resolve_dlq_entry(
            entry_id, resolution=body.resolution, resolved_by=body.resolved_by,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc))
