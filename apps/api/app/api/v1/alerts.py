from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool

from app.connectors.mssql import (
    create_alert_rule,
    delete_alert_rule,
    evaluate_alert_rules,
    list_alert_rules,
    list_alerts,
    mark_alert_read,
    resolve_alert,
)
from app.core.config import settings
from app.schemas.alerts import AlertListResponse, AlertRuleCreate, AlertRuleOut

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("", response_model=AlertListResponse)
async def get_alerts(
    is_resolved: Optional[bool] = Query(default=False),
    severity: Optional[str] = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, le=200),
    evaluate: bool = Query(default=False),
):
    try:
        if evaluate:
            raise HTTPException(
                status_code=400,
                detail="Use a job endpoint to evaluate alerts; GET /alerts is read-only.",
            )
        return await run_in_threadpool(
            list_alerts,
            is_resolved=is_resolved,
            severity=severity,
            marketplace_id=marketplace_id,
            page=page,
            page_size=page_size,
        )
    except HTTPException:
        raise
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Alerts query failed: {exc}") from exc


@router.post("/{alert_id}/read")
async def set_alert_read(alert_id: str):
    try:
        ok = await run_in_threadpool(mark_alert_read, alert_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Alert not found")
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Mark read failed: {exc}") from exc


@router.post("/{alert_id}/resolve")
async def set_alert_resolved(alert_id: str):
    try:
        ok = await run_in_threadpool(resolve_alert, alert_id, settings.DEFAULT_ACTOR)
        if not ok:
            raise HTTPException(status_code=404, detail="Alert not found")
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Resolve failed: {exc}") from exc


@router.get("/rules", response_model=list[AlertRuleOut])
async def get_rules():
    try:
        return await run_in_threadpool(list_alert_rules)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Rules query failed: {exc}") from exc


@router.post("/rules", response_model=AlertRuleOut, status_code=201)
async def add_rule(payload: AlertRuleCreate):
    try:
        return await run_in_threadpool(create_alert_rule, payload.model_dump(), settings.DEFAULT_ACTOR)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Rule create failed: {exc}") from exc


@router.delete("/rules/{rule_id}", status_code=204)
async def remove_rule(rule_id: str):
    try:
        await run_in_threadpool(delete_alert_rule, rule_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Rule delete failed: {exc}") from exc
