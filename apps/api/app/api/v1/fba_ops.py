from __future__ import annotations

import csv
import io
from typing import Any, Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.concurrency import run_in_threadpool

from app.core.security import require_analyst, require_ops
from app.schemas.fba_ops import (
    FbaOverviewResponse,
    FbaReportDiagnosticsResponse,
    FbaInventoryListResponse,
    FbaInventoryDetailResponse,
    FbaReplenishmentResponse,
    FbaInboundShipmentListResponse,
    FbaAgedItem,
    FbaStrandedItem,
    FbaKpiScorecardResponse,
    FbaInboundShipmentDetailResponse,
    FbaCaseTimelineResponse,
    FbaShipmentPlanCreate,
    FbaShipmentPlanItem,
    FbaShipmentPlanListResponse,
    FbaShipmentPlanUpdate,
    FbaCaseCreate,
    FbaCaseCommentCreate,
    FbaCaseCommentUpdate,
    FbaCaseItem,
    FbaCaseListResponse,
    FbaCaseUpdate,
    FbaLaunchCreate,
    FbaLaunchItem,
    FbaLaunchListResponse,
    FbaLaunchUpdate,
    FbaInitiativeCreate,
    FbaInitiativeItem,
    FbaInitiativeListResponse,
    FbaInitiativeUpdate,
    # Fee audit schemas
    FbaFeeAnomalyResponse,
    FbaFeeTimelineResponse,
    FbaOverchargeSummaryResponse,
    FbaFeeReferenceResponse,
)

router = APIRouter(prefix="/fba", tags=["fba-ops"])


@router.get("/overview", response_model=FbaOverviewResponse, dependencies=[Depends(require_analyst)])
async def get_fba_overview():
    from app.services.fba_ops import get_overview

    try:
        return await run_in_threadpool(get_overview)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA overview failed: {exc}") from exc


@router.get("/diagnostics/report-status", response_model=FbaReportDiagnosticsResponse, dependencies=[Depends(require_analyst)])
async def get_fba_report_status(lookback_hours: int = Query(default=48, ge=1, le=168)):
    from app.services.fba_ops import get_report_diagnostics

    try:
        return await run_in_threadpool(get_report_diagnostics, lookback_hours=lookback_hours)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA diagnostics failed: {exc}") from exc


@router.get("/inventory", response_model=FbaInventoryListResponse, dependencies=[Depends(require_analyst)])
async def get_fba_inventory(
    marketplace_id: Optional[str] = Query(default=None),
    sku_search: Optional[str] = Query(default=None),
    risk_type: Optional[str] = Query(default=None),
    days_cover_max: Optional[int] = Query(default=None, ge=0),
):
    from app.services.fba_ops import get_inventory

    try:
        return await run_in_threadpool(
            get_inventory,
            marketplace_id=marketplace_id,
            sku_search=sku_search,
            risk_type=risk_type,
            days_cover_max=days_cover_max,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA inventory failed: {exc}") from exc


@router.get("/inventory/{sku}", response_model=FbaInventoryDetailResponse, dependencies=[Depends(require_analyst)])
async def get_fba_inventory_detail(sku: str, marketplace_id: Optional[str] = Query(default=None)):
    from app.services.fba_ops import get_inventory_detail

    try:
        return await run_in_threadpool(get_inventory_detail, sku=sku, marketplace_id=marketplace_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA inventory detail failed: {exc}") from exc


@router.get("/replenishment/suggestions", response_model=FbaReplenishmentResponse, dependencies=[Depends(require_analyst)])
async def get_fba_replenishment_suggestions(
    marketplace_id: Optional[str] = Query(default=None),
    sku_search: Optional[str] = Query(default=None),
):
    from app.services.fba_ops import get_replenishment_suggestions

    try:
        return await run_in_threadpool(
            get_replenishment_suggestions,
            marketplace_id=marketplace_id,
            sku_search=sku_search,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA replenishment failed: {exc}") from exc


@router.get("/inbound/shipments", response_model=FbaInboundShipmentListResponse, dependencies=[Depends(require_analyst)])
async def get_fba_inbound_shipments(
    marketplace_id: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
):
    from app.services.fba_ops import get_inbound_shipments

    try:
        return await run_in_threadpool(get_inbound_shipments, marketplace_id=marketplace_id, status=status)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA inbound failed: {exc}") from exc


@router.get("/inbound/shipments/{shipment_id}", response_model=FbaInboundShipmentDetailResponse, dependencies=[Depends(require_analyst)])
async def get_fba_inbound_shipment_detail(shipment_id: str):
    from app.services.fba_ops import get_inbound_shipment_detail

    try:
        return await run_in_threadpool(get_inbound_shipment_detail, shipment_id=shipment_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA inbound detail failed: {exc}") from exc


@router.get("/aged", response_model=list[FbaAgedItem], dependencies=[Depends(require_analyst)])
async def get_fba_aged():
    from app.services.fba_ops import get_aged_items

    try:
        return await run_in_threadpool(get_aged_items)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA aged failed: {exc}") from exc


@router.get("/stranded", response_model=list[FbaStrandedItem], dependencies=[Depends(require_analyst)])
async def get_fba_stranded():
    from app.services.fba_ops import get_stranded_items

    try:
        return await run_in_threadpool(get_stranded_items)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA stranded failed: {exc}") from exc


@router.get("/kpi/scorecard", response_model=FbaKpiScorecardResponse, dependencies=[Depends(require_analyst)])
async def get_fba_scorecard(quarter: str = Query(...)):
    from app.services.fba_ops import get_scorecard

    try:
        return await run_in_threadpool(get_scorecard, quarter=quarter)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA scorecard failed: {exc}") from exc


@router.post("/jobs/run", dependencies=[Depends(require_ops)])
async def run_fba_job(job_type: str = Query(..., pattern="^(sync_fba_inventory|sync_fba_inbound|run_fba_alerts|recompute_fba_replenishment|sync_fba_reconciliation)$")):
    from app.connectors.mssql import enqueue_job

    try:
        return await run_in_threadpool(
            enqueue_job,
            job_type=job_type,
            trigger_source="manual",
            triggered_by="system",
            params={},
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA job failed: {exc}") from exc


@router.get("/shipment-plans", response_model=FbaShipmentPlanListResponse, dependencies=[Depends(require_analyst)])
async def get_fba_shipment_plans(
    quarter: Optional[str] = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
):
    from app.services.fba_ops import list_shipment_plans

    try:
        return await run_in_threadpool(list_shipment_plans, quarter=quarter, marketplace_id=marketplace_id, status=status)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA shipment plans failed: {exc}") from exc


@router.post("/shipment-plans", response_model=FbaShipmentPlanItem, status_code=201, dependencies=[Depends(require_ops)])
async def post_fba_shipment_plan(payload: FbaShipmentPlanCreate):
    from app.services.fba_ops import create_shipment_plan

    try:
        return await run_in_threadpool(create_shipment_plan, payload.model_dump())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Create shipment plan failed: {exc}") from exc


@router.patch("/shipment-plans/{record_id}", response_model=FbaShipmentPlanItem, dependencies=[Depends(require_ops)])
async def patch_fba_shipment_plan(record_id: str, payload: FbaShipmentPlanUpdate):
    from app.services.fba_ops import update_shipment_plan

    try:
        return await run_in_threadpool(update_shipment_plan, record_id, payload.model_dump(exclude_none=False, exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Update shipment plan failed: {exc}") from exc


@router.delete("/shipment-plans/{record_id}", status_code=204, dependencies=[Depends(require_ops)])
async def delete_fba_shipment_plan(record_id: str):
    from app.services.fba_ops import delete_shipment_plan

    try:
        ok = await run_in_threadpool(delete_shipment_plan, record_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Shipment plan not found")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Delete shipment plan failed: {exc}") from exc


@router.get("/cases", response_model=FbaCaseListResponse, dependencies=[Depends(require_analyst)])
async def get_fba_cases(
    status: Optional[str] = Query(default=None),
    case_type: Optional[str] = Query(default=None),
    owner: Optional[str] = Query(default=None),
):
    from app.services.fba_ops import list_cases

    try:
        return await run_in_threadpool(list_cases, status=status, case_type=case_type, owner=owner)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA cases failed: {exc}") from exc


@router.post("/cases", response_model=FbaCaseItem, status_code=201, dependencies=[Depends(require_ops)])
async def post_fba_case(payload: FbaCaseCreate):
    from app.services.fba_ops import create_case

    try:
        return await run_in_threadpool(create_case, payload.model_dump())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Create case failed: {exc}") from exc


@router.patch("/cases/{record_id}", response_model=FbaCaseItem, dependencies=[Depends(require_ops)])
async def patch_fba_case(record_id: str, payload: FbaCaseUpdate):
    from app.services.fba_ops import update_case

    try:
        return await run_in_threadpool(update_case, record_id, payload.model_dump(exclude_none=False, exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Update case failed: {exc}") from exc


@router.delete("/cases/{record_id}", status_code=204, dependencies=[Depends(require_ops)])
async def delete_fba_case(record_id: str):
    from app.services.fba_ops import delete_case

    try:
        ok = await run_in_threadpool(delete_case, record_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Case not found")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Delete case failed: {exc}") from exc


@router.get("/cases/{record_id}/timeline", response_model=FbaCaseTimelineResponse, dependencies=[Depends(require_analyst)])
async def get_fba_case_timeline(record_id: str):
    from app.services.fba_ops import get_case_timeline

    try:
        return await run_in_threadpool(get_case_timeline, record_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA case timeline failed: {exc}") from exc


@router.post("/cases/{record_id}/comments", response_model=FbaCaseTimelineResponse, dependencies=[Depends(require_ops)])
async def post_fba_case_comment(record_id: str, payload: FbaCaseCommentCreate):
    from app.services.fba_ops import add_case_comment

    try:
        return await run_in_threadpool(add_case_comment, record_id, payload.comment, payload.author)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA case comment failed: {exc}") from exc


@router.put("/cases/{record_id}/comments/{event_id}", response_model=FbaCaseTimelineResponse, dependencies=[Depends(require_ops)])
async def put_fba_case_comment(record_id: str, event_id: str, payload: FbaCaseCommentUpdate):
    from app.services.fba_ops import update_case_comment

    try:
        return await run_in_threadpool(update_case_comment, record_id, event_id, payload.comment, payload.author)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA case comment update failed: {exc}") from exc


@router.delete("/cases/{record_id}/comments/{event_id}", response_model=FbaCaseTimelineResponse, dependencies=[Depends(require_ops)])
async def delete_fba_case_comment(record_id: str, event_id: str, author: Optional[str] = Query(default=None)):
    from app.services.fba_ops import delete_case_comment

    try:
        return await run_in_threadpool(delete_case_comment, record_id, event_id, author)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA case comment delete failed: {exc}") from exc


@router.get("/launches", response_model=FbaLaunchListResponse, dependencies=[Depends(require_analyst)])
async def get_fba_launches(
    quarter: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
):
    from app.services.fba_ops import list_launches

    try:
        return await run_in_threadpool(list_launches, quarter=quarter, status=status)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA launches failed: {exc}") from exc


@router.post("/launches", response_model=FbaLaunchItem, status_code=201, dependencies=[Depends(require_ops)])
async def post_fba_launch(payload: FbaLaunchCreate):
    from app.services.fba_ops import create_launch

    try:
        return await run_in_threadpool(create_launch, payload.model_dump())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Create launch failed: {exc}") from exc


@router.patch("/launches/{record_id}", response_model=FbaLaunchItem, dependencies=[Depends(require_ops)])
async def patch_fba_launch(record_id: str, payload: FbaLaunchUpdate):
    from app.services.fba_ops import update_launch

    try:
        return await run_in_threadpool(update_launch, record_id, payload.model_dump(exclude_none=False, exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Update launch failed: {exc}") from exc


@router.delete("/launches/{record_id}", status_code=204, dependencies=[Depends(require_ops)])
async def delete_fba_launch(record_id: str):
    from app.services.fba_ops import delete_launch

    try:
        ok = await run_in_threadpool(delete_launch, record_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Launch not found")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Delete launch failed: {exc}") from exc


@router.get("/initiatives", response_model=FbaInitiativeListResponse, dependencies=[Depends(require_analyst)])
async def get_fba_initiatives(
    quarter: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
):
    from app.services.fba_ops import list_initiatives

    try:
        return await run_in_threadpool(list_initiatives, quarter=quarter, status=status)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA initiatives failed: {exc}") from exc


@router.post("/initiatives", response_model=FbaInitiativeItem, status_code=201, dependencies=[Depends(require_ops)])
async def post_fba_initiative(payload: FbaInitiativeCreate):
    from app.services.fba_ops import create_initiative

    try:
        return await run_in_threadpool(create_initiative, payload.model_dump())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Create initiative failed: {exc}") from exc


@router.patch("/initiatives/{record_id}", response_model=FbaInitiativeItem, dependencies=[Depends(require_ops)])
async def patch_fba_initiative(record_id: str, payload: FbaInitiativeUpdate):
    from app.services.fba_ops import update_initiative

    try:
        return await run_in_threadpool(update_initiative, record_id, payload.model_dump(exclude_none=False, exclude_unset=True))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Update initiative failed: {exc}") from exc


@router.delete("/initiatives/{record_id}", status_code=204, dependencies=[Depends(require_ops)])
async def delete_fba_initiative(record_id: str):
    from app.services.fba_ops import delete_initiative

    try:
        ok = await run_in_threadpool(delete_initiative, record_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Initiative not found")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Delete initiative failed: {exc}") from exc


# ======================================================================
#  FBA Fee Audit — anomaly detection endpoints
# ======================================================================

@router.get("/fee-audit/anomalies", response_model=FbaFeeAnomalyResponse, dependencies=[Depends(require_analyst)])
async def get_fba_fee_anomalies(
    marketplace_id: Optional[str] = Query(default=None),
    min_ratio: float = Query(default=1.5, ge=1.1, le=10.0, description="Minimum fee ratio to flag as anomaly"),
    min_orders: int = Query(default=2, ge=1, le=100, description="Minimum orders per week to consider"),
    lookback_days: int = Query(default=90, ge=7, le=365, description="Number of days to look back"),
):
    """Detect FBA fee anomalies — week-over-week fee jumps per SKU.

    Useful for catching Amazon dimension reclassification errors.
    """
    from app.services.fba_ops.fba_fee_audit import get_fee_anomalies

    try:
        return await run_in_threadpool(
            get_fee_anomalies,
            marketplace_id=marketplace_id,
            min_ratio=min_ratio,
            min_orders=min_orders,
            lookback_days=lookback_days,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA fee anomaly scan failed: {exc}") from exc


@router.get("/fee-audit/timeline/{sku}", response_model=FbaFeeTimelineResponse, dependencies=[Depends(require_analyst)])
async def get_fba_fee_timeline(
    sku: str,
    lookback_days: int = Query(default=180, ge=7, le=365),
):
    """Get detailed FBA fee timeline for a specific SKU.

    Shows every individual charge, daily aggregates, statistics, and detected anomaly periods.
    """
    from app.services.fba_ops.fba_fee_audit import get_sku_fee_timeline

    try:
        return await run_in_threadpool(get_sku_fee_timeline, sku, lookback_days=lookback_days)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA fee timeline failed: {exc}") from exc


@router.get("/fee-audit/overcharges", response_model=FbaOverchargeSummaryResponse, dependencies=[Depends(require_analyst)])
async def get_fba_fee_overcharges(
    date_from: Optional[str] = Query(default=None, description="Start date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(default=None, description="End date (YYYY-MM-DD)"),
    marketplace_id: Optional[str] = Query(default=None),
    min_overcharge_eur: float = Query(default=1.0, ge=0, description="Minimum overcharge to include"),
):
    """Calculate estimated FBA overcharges per SKU — dispute-ready data.

    For each SKU, computes median fee, flags charges > 1.5× median,
    and sums estimated overcharge with individual order IDs.
    """
    from app.services.fba_ops.fba_fee_audit import get_overcharge_summary
    from datetime import date as date_type

    try:
        df = date_type.fromisoformat(date_from) if date_from else None
        dt = date_type.fromisoformat(date_to) if date_to else None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {exc}") from exc

    try:
        return await run_in_threadpool(
            get_overcharge_summary,
            date_from=df,
            date_to=dt,
            marketplace_id=marketplace_id,
            min_overcharge_eur=min_overcharge_eur,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA overcharge summary failed: {exc}") from exc


@router.get("/fee-audit/reference", response_model=FbaFeeReferenceResponse, dependencies=[Depends(require_analyst)])
async def get_fba_fee_reference(
    marketplace_id: Optional[str] = Query(default=None),
):
    """Compare actual FBA fees with reference rates from acc_fba_fee_reference.

    Reference rates should be imported from Amazon's published fee schedule.
    """
    from app.services.fba_ops.fba_fee_audit import get_fee_vs_reference

    try:
        return await run_in_threadpool(get_fee_vs_reference, marketplace_id=marketplace_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"FBA fee reference comparison failed: {exc}") from exc


# ──────────── Register CSV/Excel import ────────────


@router.post("/registers/import", dependencies=[Depends(require_ops)])
async def import_register_csv(
    register_type: str = Query(..., pattern="^(shipment_plan|case|launch|initiative)$"),
    quarter: Optional[str] = Query(default=None),
    file: UploadFile = File(...),
):
    """
    Bulk import rows from a CSV/TSV file into a manual register.

    Expected columns depend on register_type:
      - shipment_plan: quarter, marketplace_id, shipment_id, plan_week_start, planned_ship_date,
                       planned_units, actual_ship_date, actual_units, tolerance_pct, status, owner
      - case: case_type, marketplace_id, entity_type, entity_id, sku,
              detected_date, close_date, owner, status, root_cause
      - launch: quarter, launch_type, sku, marketplace_id, planned_go_live_date,
               actual_go_live_date, live_stable_at, incident_free, vine_eligible,
               vine_eligible_at, vine_submitted_at, owner, status
      - initiative: quarter, initiative_type, title, sku, owner, status, planned, approved, live_stable_at
    """
    from app.services.fba_ops import import_register_from_rows

    raw = await file.read()
    text = raw.decode("utf-8-sig")
    first_line = next((line for line in text.splitlines() if line.strip()), "")
    delimiter = "\t" if first_line.count("\t") >= first_line.count(",") else ","
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    rows: list[dict[str, Any]] = []
    for row in reader:
        clean = {k.strip().lower().replace("-", "_").replace(" ", "_"): (v.strip() if v else None) for k, v in row.items() if k}
        rows.append(clean)
    if not rows:
        raise HTTPException(status_code=400, detail="CSV file is empty or has no data rows")
    try:
        result = await run_in_threadpool(import_register_from_rows, register_type=register_type, rows=rows, quarter=quarter)
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Import failed: {exc}") from exc


@router.post("/reconciliation/sync", dependencies=[Depends(require_ops)])
async def trigger_reconciliation_sync():
    """Manually trigger receiving reconciliation sync + shipment plan auto-fill."""
    from app.services.fba_ops import sync_receiving_reconciliation, auto_fill_shipment_plan_actuals

    try:
        recon_rows = await run_in_threadpool(sync_receiving_reconciliation)
        plan_rows = await run_in_threadpool(auto_fill_shipment_plan_actuals)
        return {"reconciliation_rows": recon_rows, "plan_autofill_rows": plan_rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Reconciliation sync failed: {exc}") from exc
