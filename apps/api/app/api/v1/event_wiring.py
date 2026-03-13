"""Event Wiring & Replay API.

Sprint 20 – S20.5
"""
from __future__ import annotations

from typing import Optional

import structlog
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.intelligence import event_wiring as ew

log = structlog.get_logger(__name__)
router = APIRouter(prefix="/event-wiring", tags=["event-wiring"])


# ── Request bodies ───────────────────────────────────────────────────

class RegisterWireRequest(BaseModel):
    module_name: str
    event_domain: str
    event_action: str = "*"
    handler_name: str
    description: str | None = None
    enabled: bool = True
    priority: int = 100
    timeout_seconds: int = 30


class ToggleWireRequest(BaseModel):
    enabled: bool


class ReplayRequest(BaseModel):
    event_domain: str | None = None
    notification_type: str | None = None
    event_ids: list[str] | None = None
    since: str | None = None
    until: str | None = None
    limit: int = 500
    triggered_by: str | None = None


class DlqReplayRequest(BaseModel):
    domain: str | None = None
    entry_ids: list[int] | None = None
    triggered_by: str | None = None


# ── Wiring endpoints ────────────────────────────────────────────────

@router.get("/wires")
def list_wires(
    module_name: Optional[str] = Query(None),
    event_domain: Optional[str] = Query(None),
    enabled_only: bool = Query(False),
):
    """List event wiring configurations."""
    return ew.get_wiring(module_name=module_name, event_domain=event_domain, enabled_only=enabled_only)


@router.post("/wires")
def register_wire(body: RegisterWireRequest):
    """Register or update an event wire."""
    return ew.register_wire(
        module_name=body.module_name,
        event_domain=body.event_domain,
        event_action=body.event_action,
        handler_name=body.handler_name,
        description=body.description,
        enabled=body.enabled,
        priority=body.priority,
        timeout_seconds=body.timeout_seconds,
    )


@router.patch("/wires/{handler_name}/toggle")
def toggle_wire(handler_name: str, body: ToggleWireRequest):
    """Enable or disable an event wire."""
    return ew.toggle_wire(handler_name, enabled=body.enabled)


@router.delete("/wires/{handler_name}")
def delete_wire(handler_name: str):
    """Delete an event wire."""
    return ew.delete_wire(handler_name)


@router.post("/wires/seed")
def seed_wiring():
    """Seed default wiring configuration for all modules."""
    return ew.seed_default_wiring()


@router.get("/health")
def wiring_health():
    """Event wiring health summary: coverage per domain, handler stats."""
    return ew.get_wiring_health()


@router.post("/register-handlers")
def register_handlers():
    """Register missing domain handlers in the event backbone."""
    return ew.register_all_domain_handlers()


# ── Replay endpoints ────────────────────────────────────────────────

@router.post("/replay")
def replay(body: ReplayRequest):
    """Replay events: reset to received and immediately process."""
    return ew.replay_and_process(
        event_domain=body.event_domain,
        notification_type=body.notification_type,
        event_ids=body.event_ids,
        since=body.since,
        until=body.until,
        limit=body.limit,
        triggered_by=body.triggered_by,
    )


@router.post("/replay/dlq")
def replay_dlq(body: DlqReplayRequest):
    """Re-ingest DLQ entries through the event backbone."""
    return ew.replay_dlq_entries(
        domain=body.domain,
        entry_ids=body.entry_ids,
        triggered_by=body.triggered_by,
    )


@router.get("/replay/jobs")
def list_replay_jobs(
    status: Optional[str] = Query(None),
    replay_type: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """List replay jobs with filters."""
    return ew.get_replay_jobs(status=status, replay_type=replay_type, limit=limit, offset=offset)


@router.get("/replay/summary")
def replay_summary():
    """Replay operations summary."""
    return ew.get_replay_summary()


# ── Topology polling bridge ─────────────────────────────────────────

@router.post("/poll-topology")
def poll_topology():
    """Trigger a topology-aware poll of all domain queues."""
    return ew.poll_topology_queues()
