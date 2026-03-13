"""Operator Console v2 API — Sprint 23-24.

Unified alert feed, case management CRUD, and action queue with
approve / reject / execute lifecycle.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from app.intelligence import operator_console as oc

router = APIRouter(prefix="/operator-console", tags=["operator-console"])


# ── Pydantic schemas ─────────────────────────────────────────────────

class CaseCreate(BaseModel):
    title: str = Field(..., max_length=300)
    description: Optional[str] = None
    category: str = Field(default="other", max_length=40)
    priority: str = Field(default="medium", max_length=20)
    marketplace_id: Optional[str] = None
    sku: Optional[str] = None
    asin: Optional[str] = None
    source_type: Optional[str] = None
    source_id: Optional[str] = None
    assigned_to: Optional[str] = None
    due_date: Optional[str] = None
    tags: Optional[str] = None


class CaseUpdate(BaseModel):
    status: Optional[str] = None
    priority: Optional[str] = None
    assigned_to: Optional[str] = None
    resolution_note: Optional[str] = None
    resolved_by: Optional[str] = None


class ActionSubmit(BaseModel):
    action_type: str = Field(..., max_length=80)
    title: str = Field(..., max_length=300)
    description: Optional[str] = None
    marketplace_id: Optional[str] = None
    sku: Optional[str] = None
    asin: Optional[str] = None
    payload: Optional[dict] = None
    risk_level: str = Field(default="medium", max_length=20)
    requested_by: str = Field(..., max_length=120)
    expires_hours: int = Field(default=72, ge=1, le=720)


class ActionApprove(BaseModel):
    approved_by: str = Field(..., max_length=120)


class ActionReject(BaseModel):
    rejected_by: str = Field(..., max_length=120)
    reason: Optional[str] = None


class ActionExecuted(BaseModel):
    result: Optional[str] = None
    error: Optional[str] = None


# ── Dashboard ────────────────────────────────────────────────────────

@router.get("/dashboard")
async def get_dashboard():
    try:
        return await run_in_threadpool(oc.get_operator_dashboard)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── Unified Feed ─────────────────────────────────────────────────────

@router.get("/feed")
async def get_feed(
    days: int = Query(default=7, ge=1, le=90),
    severity: Optional[str] = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    source: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    try:
        return await run_in_threadpool(
            oc.get_unified_feed,
            days=days,
            severity=severity,
            marketplace_id=marketplace_id,
            source=source,
            page=page,
            page_size=page_size,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/feed/summary")
async def get_feed_summary(days: int = Query(default=7, ge=1, le=90)):
    try:
        return await run_in_threadpool(oc.get_feed_summary, days=days)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── Case Management ──────────────────────────────────────────────────

@router.get("/cases")
async def get_cases(
    status: Optional[str] = Query(default=None),
    category: Optional[str] = Query(default=None),
    priority: Optional[str] = Query(default=None),
    assigned_to: Optional[str] = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    try:
        return await run_in_threadpool(
            oc.list_operator_cases,
            status=status,
            category=category,
            priority=priority,
            assigned_to=assigned_to,
            marketplace_id=marketplace_id,
            page=page,
            page_size=page_size,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/cases/{case_id}")
async def get_case(case_id: int):
    try:
        result = await run_in_threadpool(oc.get_operator_case, case_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Case not found")
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/cases", status_code=201)
async def create_case(body: CaseCreate):
    try:
        return await run_in_threadpool(
            oc.create_operator_case,
            title=body.title,
            description=body.description,
            category=body.category,
            priority=body.priority,
            marketplace_id=body.marketplace_id,
            sku=body.sku,
            asin=body.asin,
            source_type=body.source_type,
            source_id=body.source_id,
            assigned_to=body.assigned_to,
            due_date=body.due_date,
            tags=body.tags,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.patch("/cases/{case_id}")
async def update_case(case_id: int, body: CaseUpdate):
    try:
        result = await run_in_threadpool(
            oc.update_operator_case,
            case_id,
            status=body.status,
            priority=body.priority,
            assigned_to=body.assigned_to,
            resolution_note=body.resolution_note,
            resolved_by=body.resolved_by,
        )
        if result is None:
            raise HTTPException(status_code=404, detail="Case not found")
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── Action Queue ─────────────────────────────────────────────────────

@router.get("/actions")
async def get_actions(
    status: Optional[str] = Query(default=None),
    action_type: Optional[str] = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    try:
        return await run_in_threadpool(
            oc.list_action_queue,
            status=status,
            action_type=action_type,
            marketplace_id=marketplace_id,
            page=page,
            page_size=page_size,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/actions/{action_id}")
async def get_action(action_id: int):
    try:
        result = await run_in_threadpool(oc.get_action_queue_item, action_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Action not found")
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/actions", status_code=201)
async def submit_action(body: ActionSubmit):
    try:
        return await run_in_threadpool(
            oc.submit_action,
            action_type=body.action_type,
            title=body.title,
            description=body.description,
            marketplace_id=body.marketplace_id,
            sku=body.sku,
            asin=body.asin,
            payload=body.payload,
            risk_level=body.risk_level,
            requested_by=body.requested_by,
            expires_hours=body.expires_hours,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/actions/{action_id}/approve")
async def approve_action(action_id: int, body: ActionApprove):
    try:
        result = await run_in_threadpool(oc.approve_action, action_id, approved_by=body.approved_by)
        if result is None:
            raise HTTPException(status_code=404, detail="Action not found or not pending")
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/actions/{action_id}/reject")
async def reject_action(action_id: int, body: ActionReject):
    try:
        result = await run_in_threadpool(
            oc.reject_action, action_id, rejected_by=body.rejected_by, reason=body.reason,
        )
        if result is None:
            raise HTTPException(status_code=404, detail="Action not found or not pending")
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/actions/{action_id}/executed")
async def mark_executed(action_id: int, body: ActionExecuted):
    try:
        result = await run_in_threadpool(
            oc.mark_action_executed, action_id, result=body.result, error=body.error,
        )
        if result is None:
            raise HTTPException(status_code=404, detail="Action not found or not approved")
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
