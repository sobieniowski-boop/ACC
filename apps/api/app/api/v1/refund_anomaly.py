"""Refund Anomaly Engine API — Sprint 21-22.

Endpoints for refund spike anomalies, serial returners,
reimbursement cases, dashboard, scan trigger, detail views,
trend analysis, and CSV exports.
"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

router = APIRouter(prefix="/refund-anomaly", tags=["refund-anomaly"])


# ──────────────────── Request / Response Models ────────────────────────

class AnomalyStatusUpdate(BaseModel):
    status: str = Field(..., description="open | investigating | resolved | dismissed")
    resolution_note: str | None = Field(None)
    resolved_by: str | None = Field(None)


class ReturnerStatusUpdate(BaseModel):
    status: str = Field(..., description="flagged | monitoring | cleared | blocked")
    notes: str | None = Field(None)


class CaseStatusUpdate(BaseModel):
    status: str = Field(..., description="identified | filed | accepted | rejected | paid")
    amazon_case_id: str | None = Field(None)
    reimbursed_amount_pln: float | None = Field(None)
    resolution_note: str | None = Field(None)


class ScanRequest(BaseModel):
    marketplace_id: str | None = Field(None, description="Limit scan to one marketplace")


# ──────────────────── Dashboard ────────────────────────

@router.get("/dashboard")
async def anomaly_dashboard():
    """Get anomaly engine KPIs — anomalies, serial returners, reimbursements."""
    from app.intelligence.refund_anomaly import get_anomaly_dashboard

    try:
        data = await run_in_threadpool(get_anomaly_dashboard)
        return data
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)[:500])


# ──────────────────── Full scan trigger ────────────────────────

@router.post("/scan")
async def trigger_scan(body: ScanRequest | None = None):
    """Trigger full anomaly scan: refund spikes + serial returners + reimbursements."""
    from app.intelligence.refund_anomaly import run_full_scan

    try:
        mkt = body.marketplace_id if body else None
        result = await run_in_threadpool(run_full_scan, marketplace_id=mkt)
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)[:500])


# ──────────────────── Anomalies ────────────────────────

@router.get("/anomalies")
async def list_anomalies(
    anomaly_type: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    status: str | None = Query(default=None),
    marketplace_id: str | None = Query(default=None),
    sku: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    """List refund anomalies with optional filters."""
    from app.intelligence.refund_anomaly import get_anomalies

    try:
        return await run_in_threadpool(
            get_anomalies,
            anomaly_type=anomaly_type,
            severity=severity,
            status=status,
            marketplace_id=marketplace_id,
            sku=sku,
            limit=limit,
            offset=offset,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)[:500])


@router.put("/anomalies/{anomaly_id}/status")
async def update_anomaly(anomaly_id: int, body: AnomalyStatusUpdate):
    """Update anomaly status (e.g., investigating → resolved)."""
    from app.intelligence.refund_anomaly import update_anomaly_status

    try:
        return await run_in_threadpool(
            update_anomaly_status,
            anomaly_id,
            status=body.status,
            resolution_note=body.resolution_note,
            resolved_by=body.resolved_by,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)[:500])


# ──────────────────── Serial Returners ────────────────────────

@router.get("/serial-returners")
async def list_serial_returners(
    risk_tier: str | None = Query(default=None),
    status: str | None = Query(default=None),
    marketplace_id: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    """List identified serial returners with optional filters."""
    from app.intelligence.refund_anomaly import get_serial_returners

    try:
        return await run_in_threadpool(
            get_serial_returners,
            risk_tier=risk_tier,
            status=status,
            marketplace_id=marketplace_id,
            limit=limit,
            offset=offset,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)[:500])


@router.put("/serial-returners/{returner_id}/status")
async def update_serial_returner(returner_id: int, body: ReturnerStatusUpdate):
    """Update serial returner status."""
    from app.intelligence.refund_anomaly import update_returner_status

    try:
        return await run_in_threadpool(
            update_returner_status,
            returner_id,
            status=body.status,
            notes=body.notes,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)[:500])


# ──────────────────── Reimbursement Cases ────────────────────────

@router.get("/reimbursement-cases")
async def list_reimbursement_cases(
    case_type: str | None = Query(default=None),
    status: str | None = Query(default=None),
    marketplace_id: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    """List reimbursement cases with optional filters."""
    from app.intelligence.refund_anomaly import get_reimbursement_cases

    try:
        return await run_in_threadpool(
            get_reimbursement_cases,
            case_type=case_type,
            status=status,
            marketplace_id=marketplace_id,
            limit=limit,
            offset=offset,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)[:500])


@router.put("/reimbursement-cases/{case_id}/status")
async def update_reimbursement_case(case_id: int, body: CaseStatusUpdate):
    """Update reimbursement case status (filed → accepted → paid)."""
    from app.intelligence.refund_anomaly import update_case_status

    try:
        return await run_in_threadpool(
            update_case_status,
            case_id,
            status=body.status,
            amazon_case_id=body.amazon_case_id,
            reimbursed_amount_pln=body.reimbursed_amount_pln,
            resolution_note=body.resolution_note,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)[:500])


# ──────────────────── CSV Exports (before detail routes to avoid path conflicts) ──

def _to_csv_stream(rows: list[dict], filename: str) -> StreamingResponse:
    """Convert list of dicts to a CSV StreamingResponse."""
    if not rows:
        output = io.StringIO()
        output.write("")
    else:
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/anomalies/export/csv")
async def export_anomalies(
    anomaly_type: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    status: str | None = Query(default=None),
    marketplace_id: str | None = Query(default=None),
):
    """Export anomalies as CSV download."""
    from app.intelligence.refund_anomaly import export_anomalies_csv

    rows = await run_in_threadpool(
        export_anomalies_csv,
        anomaly_type=anomaly_type,
        severity=severity,
        status=status,
        marketplace_id=marketplace_id,
    )
    return _to_csv_stream(rows, "refund_anomalies.csv")


@router.get("/serial-returners/export/csv")
async def export_returners(
    risk_tier: str | None = Query(default=None),
    status: str | None = Query(default=None),
    marketplace_id: str | None = Query(default=None),
):
    """Export serial returners as CSV download."""
    from app.intelligence.refund_anomaly import export_returners_csv

    rows = await run_in_threadpool(
        export_returners_csv,
        risk_tier=risk_tier,
        status=status,
        marketplace_id=marketplace_id,
    )
    return _to_csv_stream(rows, "serial_returners.csv")


@router.get("/reimbursement-cases/export/csv")
async def export_cases(
    case_type: str | None = Query(default=None),
    status: str | None = Query(default=None),
    marketplace_id: str | None = Query(default=None),
):
    """Export reimbursement cases as CSV download."""
    from app.intelligence.refund_anomaly import export_cases_csv

    rows = await run_in_threadpool(
        export_cases_csv,
        case_type=case_type,
        status=status,
        marketplace_id=marketplace_id,
    )
    return _to_csv_stream(rows, "reimbursement_cases.csv")


# ──────────────────── Detail endpoints ────────────────────────

@router.get("/anomalies/{anomaly_id}")
async def get_anomaly_detail(anomaly_id: int):
    """Get a single anomaly by ID."""
    from app.intelligence.refund_anomaly import get_anomaly_by_id

    result = await run_in_threadpool(get_anomaly_by_id, anomaly_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Anomaly not found")
    return result


@router.get("/serial-returners/{returner_id}")
async def get_returner_detail(returner_id: int):
    """Get a single serial returner by ID."""
    from app.intelligence.refund_anomaly import get_returner_by_id

    result = await run_in_threadpool(get_returner_by_id, returner_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Serial returner not found")
    return result


@router.get("/reimbursement-cases/{case_id}")
async def get_case_detail(case_id: int):
    """Get a single reimbursement case by ID."""
    from app.intelligence.refund_anomaly import get_case_by_id

    result = await run_in_threadpool(get_case_by_id, case_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Reimbursement case not found")
    return result


# ──────────────────── Trends ────────────────────────

@router.get("/trends")
async def anomaly_trends(
    days: int = Query(default=90, ge=7, le=365),
    anomaly_type: str | None = Query(default=None),
    marketplace_id: str | None = Query(default=None),
):
    """Get anomaly count trends grouped by week."""
    from app.intelligence.refund_anomaly import get_anomaly_trends

    try:
        return await run_in_threadpool(
            get_anomaly_trends,
            days=days,
            anomaly_type=anomaly_type,
            marketplace_id=marketplace_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)[:500])
