"""WebSocket endpoints for real-time job progress and alert notifications."""
from __future__ import annotations

import asyncio
import json
from typing import Any

import structlog
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy import select

from app.core.database import get_db
from app.core.redis_client import get_redis
from app.models.alert import Alert
from app.models.job import JobRun

log = structlog.get_logger(__name__)
ws_router = APIRouter(prefix="/ws", tags=["websocket"])


class ConnectionManager:
    """Manages active WebSocket connections."""

    def __init__(self):
        self.active: dict[str, list[WebSocket]] = {}  # channel → [ws]

    async def connect(self, channel: str, ws: WebSocket) -> None:
        await ws.accept()
        self.active.setdefault(channel, []).append(ws)
        log.info("ws.connected", channel=channel, total=len(self.active[channel]))

    def disconnect(self, channel: str, ws: WebSocket) -> None:
        conns = self.active.get(channel, [])
        if ws in conns:
            conns.remove(ws)
        log.info("ws.disconnected", channel=channel, remaining=len(conns))

    async def broadcast(self, channel: str, data: Any) -> None:
        message = json.dumps(data)
        dead: list[WebSocket] = []
        for ws in list(self.active.get(channel, [])):
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(channel, ws)


manager = ConnectionManager()


@ws_router.websocket("/jobs/{job_id}")
async def ws_job_progress(websocket: WebSocket, job_id: str):
    """Push job progress updates for a single job run."""
    await manager.connect(f"job:{job_id}", websocket)
    try:
        async for db in get_db():
            while True:
                result = await db.execute(select(JobRun).where(JobRun.id == job_id))
                job = result.scalar_one_or_none()
                if job is None:
                    await websocket.send_json({"error": "Job not found"})
                    break

                await websocket.send_json({
                    "job_id": str(job.id),
                    "status": job.status,
                    "progress_pct": job.progress_pct,
                    "progress_message": job.progress_message,
                    "records_processed": job.records_processed,
                    "error_message": job.error_message,
                })

                if job.status in ("success", "failure", "revoked"):
                    break

                await asyncio.sleep(2)
    except WebSocketDisconnect:
        manager.disconnect(f"job:{job_id}", websocket)


@ws_router.websocket("/alerts")
async def ws_alerts(websocket: WebSocket):
    """Push new alert notifications in real-time via Redis pub/sub."""
    await manager.connect("alerts", websocket)
    redis = await get_redis()
    pubsub = redis.pubsub()
    await pubsub.subscribe("acc:alerts")
    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                conns = manager.active.get("alerts", [])
                if not conns:
                    break
                await manager.broadcast("alerts", json.loads(message["data"]))
    except WebSocketDisconnect:
        pass
    finally:
        await pubsub.unsubscribe("acc:alerts")
        manager.disconnect("alerts", websocket)


@ws_router.websocket("/fba-alerts")
async def ws_fba_alerts(websocket: WebSocket):
    await ws_alerts(websocket)


@ws_router.websocket("/fba-jobs/{job_id}")
async def ws_fba_job_progress(websocket: WebSocket, job_id: str):
    await ws_job_progress(websocket, job_id)


async def publish_alert(alert: Alert) -> None:
    """Called from alert service to push new alerts via Redis pub/sub."""
    redis = await get_redis()
    await redis.publish(
        "acc:alerts",
        json.dumps({
            "id": str(alert.id),
            "title": alert.title,
            "severity": alert.severity,
            "marketplace_id": alert.marketplace_id,
            "sku": alert.sku,
            "triggered_at": alert.triggered_at.isoformat(),
        }),
    )
