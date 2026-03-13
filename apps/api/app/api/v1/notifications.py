"""Notifications & Event Backbone API.

Endpoints for managing SP-API notification destinations, subscriptions,
ingesting events, querying the event log, and backbone health.

All management endpoints require admin auth.  The intake endpoint uses
a separate HMAC secret for SQS → Lambda → ACC forwarding scenarios.
"""
from __future__ import annotations

import hmac
import hashlib
from typing import Optional

import structlog
from fastapi import APIRouter, HTTPException, Header, Query
from pydantic import BaseModel, Field

from app.connectors.amazon_sp_api.notifications import (
    NotificationsClient,
    SUPPORTED_NOTIFICATION_TYPES,
)
from app.services import event_backbone

log = structlog.get_logger(__name__)
router = APIRouter(prefix="/notifications", tags=["notifications"])


# ═══════════════════════════════════════════════════════════════════════════
#  Request / Response schemas
# ═══════════════════════════════════════════════════════════════════════════

class CreateDestinationRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    destination_type: str = Field("sqs", pattern="^(sqs|eventbridge)$")
    sqs_arn: Optional[str] = None
    account_id: Optional[str] = None
    region: Optional[str] = None


class CreateSubscriptionRequest(BaseModel):
    notification_type: str = Field(..., min_length=1, max_length=100)
    destination_id: str = Field(..., min_length=1)
    payload_version: str = "1.0"


class IntakeRequest(BaseModel):
    """Structured intake request.  ``payload`` holds the raw SP-API notification."""
    payload: dict
    source: str = "direct"
    correlation_id: Optional[str] = None


class IntakeBatchRequest(BaseModel):
    events: list[dict]
    source: str = "direct"
    correlation_id: Optional[str] = None


class ReplayRequest(BaseModel):
    event_ids: Optional[list[str]] = None
    event_domain: Optional[str] = None
    notification_type: Optional[str] = None
    since: Optional[str] = None
    until: Optional[str] = None
    limit: int = Field(500, ge=1, le=5000)


# ═══════════════════════════════════════════════════════════════════════════
#  Health
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/health")
def backbone_health():
    """Event backbone health summary."""
    return event_backbone.get_backbone_health()


# ═══════════════════════════════════════════════════════════════════════════
#  Destinations
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/destinations")
async def list_destinations():
    """List registered notification destinations (local DB + live from SP-API)."""
    client = NotificationsClient()
    try:
        remote = await client.get_destinations()
    except Exception as exc:
        log.warning("notifications.list_destinations_failed", error=str(exc))
        remote = []

    return {"destinations": remote}


@router.post("/destinations", status_code=201)
async def create_destination(req: CreateDestinationRequest):
    """Register a new SQS or EventBridge destination with Amazon."""
    client = NotificationsClient()

    if req.destination_type == "sqs":
        if not req.sqs_arn:
            raise HTTPException(400, "sqs_arn required for SQS destination")
        dest = await client.create_destination_sqs(req.name, req.sqs_arn)
    else:
        if not req.account_id or not req.region:
            raise HTTPException(400, "account_id and region required for EventBridge")
        dest = await client.create_destination_eventbridge(
            req.name, req.account_id, req.region
        )

    event_backbone.persist_destination(dest)
    return dest


@router.delete("/destinations/{destination_id}")
async def delete_destination(destination_id: str):
    """Delete a destination from Amazon and local DB."""
    client = NotificationsClient()
    await client.delete_destination(destination_id)
    event_backbone.remove_destination(destination_id)
    return {"status": "deleted", "destination_id": destination_id}


# ═══════════════════════════════════════════════════════════════════════════
#  Subscriptions
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/subscriptions")
async def list_subscriptions():
    """List current subscriptions for all supported notification types."""
    client = NotificationsClient()
    results = []
    for nt in SUPPORTED_NOTIFICATION_TYPES:
        try:
            sub = await client.get_subscription(nt)
            if sub:
                results.append({"notification_type": nt, "subscription": sub})
        except Exception as exc:
            results.append({"notification_type": nt, "error": str(exc)})
    return {"subscriptions": results}


@router.post("/subscriptions", status_code=201)
async def create_subscription(req: CreateSubscriptionRequest):
    """Create a subscription for a notification type."""
    if req.notification_type not in SUPPORTED_NOTIFICATION_TYPES:
        raise HTTPException(
            400,
            f"Unsupported type: {req.notification_type}. "
            f"Supported: {list(SUPPORTED_NOTIFICATION_TYPES.keys())}",
        )

    client = NotificationsClient()
    sub = await client.create_subscription(
        req.notification_type,
        req.destination_id,
        payload_version=req.payload_version,
    )

    domain = SUPPORTED_NOTIFICATION_TYPES[req.notification_type]
    event_backbone.persist_subscription(sub, req.notification_type, domain)
    return sub


@router.delete("/subscriptions/{notification_type}")
async def delete_subscription(notification_type: str):
    """Delete a subscription for a notification type."""
    client = NotificationsClient()
    existing = await client.get_subscription(notification_type)
    if not existing:
        raise HTTPException(404, f"No subscription for {notification_type}")

    sub_id = existing.get("subscriptionId")
    if sub_id:
        await client.delete_subscription(notification_type, sub_id)
    event_backbone.remove_subscription(notification_type)
    return {"status": "deleted", "notification_type": notification_type}


@router.get("/supported-types")
def supported_types():
    """List all notification types ACC can subscribe to."""
    return {
        "types": [
            {"notification_type": nt, "event_domain": domain}
            for nt, domain in SUPPORTED_NOTIFICATION_TYPES.items()
        ]
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Event intake
# ═══════════════════════════════════════════════════════════════════════════

def _verify_intake_signature(payload_bytes: bytes, signature: str | None) -> bool:
    """Verify HMAC-SHA256 signature for intake endpoint (if configured)."""
    from app.core.config import settings
    secret = settings.NOTIFICATION_INTAKE_SECRET
    if not secret:
        return True  # no secret configured → skip verification
    if not signature:
        return False
    expected = hmac.new(
        secret.encode(), payload_bytes, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


@router.post("/intake")
async def intake_event(
    req: IntakeRequest,
    x_acc_signature: Optional[str] = Header(None, alias="X-ACC-Signature"),
):
    """Ingest a single SP-API notification event.

    Accepts ``{"payload": {<SP-API notification JSON>}, "source": "...", "correlation_id": "..."}``.

    Idempotent — duplicate events are safely ignored.
    """
    if not _verify_intake_signature(req.model_dump_json().encode(), x_acc_signature):
        raise HTTPException(403, "Invalid or missing X-ACC-Signature")
    result = event_backbone.ingest(
        req.payload, source=req.source, correlation_id=req.correlation_id,
    )
    if result["status"] == "error":
        raise HTTPException(422, result.get("error", "Ingestion failed"))
    return result


@router.post("/intake/batch")
async def intake_batch(
    req: IntakeBatchRequest,
    x_acc_signature: Optional[str] = Header(None, alias="X-ACC-Signature"),
):
    """Ingest a batch of SP-API notification events.

    Each event is processed independently — partial success is possible.
    """
    if not _verify_intake_signature(req.model_dump_json().encode(), x_acc_signature):
        raise HTTPException(403, "Invalid or missing X-ACC-Signature")
    if len(req.events) > 100:
        raise HTTPException(400, "Maximum 100 events per batch")
    result = event_backbone.ingest_batch(
        req.events, source=req.source, correlation_id=req.correlation_id,
    )
    return result


# ═══════════════════════════════════════════════════════════════════════════
#  SQS polling trigger
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/poll-sqs")
def poll_sqs(max_messages: int = Query(10, ge=1, le=10)):
    """Manually trigger SQS polling (also called by scheduler)."""
    return event_backbone.poll_sqs(max_messages=max_messages)


# ═══════════════════════════════════════════════════════════════════════════
#  Event log query & replay
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/events")
def query_events(
    event_domain: Optional[str] = None,
    notification_type: Optional[str] = None,
    status: Optional[str] = None,
    severity: Optional[str] = None,
    asin: Optional[str] = None,
    sku: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
):
    """Query the event log with filters + pagination."""
    return event_backbone.get_event_log(
        event_domain=event_domain,
        notification_type=notification_type,
        status=status,
        severity=severity,
        asin=asin,
        sku=sku,
        since=since,
        until=until,
        page=page,
        page_size=page_size,
    )


@router.get("/events/{event_id}")
def get_event(event_id: str):
    """Get full event detail including processing log."""
    event = event_backbone.get_event_detail(event_id)
    if not event:
        raise HTTPException(404, "Event not found")
    return event


@router.post("/events/replay")
def replay_events(req: ReplayRequest):
    """Replay (re-process) events matching criteria."""
    return event_backbone.replay_events(
        event_ids=req.event_ids,
        event_domain=req.event_domain,
        notification_type=req.notification_type,
        since=req.since,
        until=req.until,
        limit=req.limit,
    )


@router.post("/events/process")
def process_events(limit: int = Query(100, ge=1, le=1000)):
    """Manually trigger processing of pending events."""
    return event_backbone.process_pending_events(limit=limit)


# ═══════════════════════════════════════════════════════════════════════════
#  SQS metrics
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/sqs-metrics")
def sqs_metrics():
    """In-process SQS polling metrics (messages received, loops, empty polls)."""
    return event_backbone.get_sqs_metrics()


# ═══════════════════════════════════════════════════════════════════════════
#  Domain stats
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/stats")
def event_stats():
    """Aggregate event statistics by domain and type."""
    from app.core.db_connection import connect_acc
    conn = connect_acc()
    try:
        cur = conn.cursor()

        # By domain
        cur.execute(
            """
            SELECT event_domain, status, COUNT(*) AS cnt
            FROM dbo.acc_event_log WITH (NOLOCK)
            GROUP BY event_domain, status
            ORDER BY event_domain, status
            """
        )
        domain_stats: dict[str, dict] = {}
        for row in cur.fetchall():
            domain = row[0]
            if domain not in domain_stats:
                domain_stats[domain] = {}
            domain_stats[domain][row[1]] = row[2]

        # By type (last 24h)
        cur.execute(
            """
            SELECT notification_type, COUNT(*) AS cnt
            FROM dbo.acc_event_log WITH (NOLOCK)
            WHERE received_at >= DATEADD(HOUR, -24, SYSUTCDATETIME())
            GROUP BY notification_type
            ORDER BY cnt DESC
            """
        )
        type_counts = {row[0]: row[1] for row in cur.fetchall()}

        cur.close()
        return {
            "by_domain": domain_stats,
            "last_24h_by_type": type_counts,
        }
    finally:
        conn.close()
