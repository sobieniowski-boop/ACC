"""DHL24 WebAPI2 read-only diagnostics and health endpoints."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

from app.core.config import settings
from app.schemas.jobs import JobRunOut

router = APIRouter(prefix="/dhl", tags=["DHL"])


class DHLHealthResponse(BaseModel):
    ok: bool
    configured: bool
    base_url: str
    write_enabled: bool
    version: str | None = None
    shipments_probe_count: int | None = None
    latency_ms: float | None = None
    error: str | None = None


class DHLShipmentItemResponse(BaseModel):
    shipment_id: str
    created: str | None = None
    shipper_name: str | None = None
    receiver_name: str | None = None
    order_status: str | None = None


class DHLShipmentListResponse(BaseModel):
    created_from: date
    created_to: date
    offset: int
    count: int
    items: list[DHLShipmentItemResponse]


class DHLShipmentCountResponse(BaseModel):
    created_from: date
    created_to: date
    count: int


class DHLTrackingEventResponse(BaseModel):
    status: str | None = None
    description: str | None = None
    terminal: str | None = None
    timestamp: str | None = None


class DHLTrackResponse(BaseModel):
    shipment_id: str
    received_by: str | None = None
    events: list[DHLTrackingEventResponse]


class DHLBinaryDocumentResponse(BaseModel):
    shipment_id: str
    mime_type: str | None = None
    content_base64: str | None = None
    has_content: bool


class DHLPieceResponseItem(BaseModel):
    shipment_number: str | None = None
    cedex_number: str | None = None
    packages: list[dict[str, Any]]


class DHLPieceResponse(BaseModel):
    count: int
    items: list[DHLPieceResponseItem]


class DHLLabelPieceResponse(BaseModel):
    routing_barcode: str | None = None
    blp_piece_id: str | None = None
    piece_type: str | None = None
    weight: float | None = None
    quantity: int | None = None


class DHLLabelsDataResponse(BaseModel):
    shipment_id: str
    primary_waybill_number: str | None = None
    dispatch_notification_number: str | None = None
    label_header: str | None = None
    reference: str | None = None
    content: str | None = None
    comment: str | None = None
    service_product: str | None = None
    shipper_name: str | None = None
    shipper_country: str | None = None
    receiver_name: str | None = None
    receiver_country: str | None = None
    pieces: list[DHLLabelPieceResponse]


class DHLCostTraceResponse(BaseModel):
    count: int
    items: list[dict[str, Any]]


class DHLUnmatchedShipmentsResponse(BaseModel):
    count: int
    items: list[dict[str, Any]]


class DHLShadowDiffResponse(BaseModel):
    count: int
    items: list[dict[str, Any]]


@router.get("/health", response_model=DHLHealthResponse)
async def dhl_health():
    from app.connectors.dhl24_api import DHL24Client

    client = DHL24Client()
    result = await run_in_threadpool(client.health_check)
    return DHLHealthResponse(**result)


@router.get("/shipments", response_model=DHLShipmentListResponse)
async def dhl_shipments(
    created_from: date | None = Query(default=None),
    created_to: date | None = Query(default=None),
    offset: int = Query(default=0, ge=0),
):
    _ensure_configured()

    from app.connectors.dhl24_api import DHL24Client
    from app.connectors.dhl24_api.errors import DHL24Error

    date_to_value = created_to or date.today()
    date_from_value = created_from or (date_to_value - timedelta(days=7))
    client = DHL24Client()
    try:
        items = await run_in_threadpool(
            client.get_my_shipments,
            created_from=date_from_value,
            created_to=date_to_value,
            offset=offset,
        )
        return DHLShipmentListResponse(
            created_from=date_from_value,
            created_to=date_to_value,
            offset=offset,
            count=len(items),
            items=[DHLShipmentItemResponse(**item.to_dict()) for item in items],
        )
    except DHL24Error as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/shipments/count", response_model=DHLShipmentCountResponse)
async def dhl_shipments_count(
    created_from: date | None = Query(default=None),
    created_to: date | None = Query(default=None),
):
    _ensure_configured()

    from app.connectors.dhl24_api import DHL24Client
    from app.connectors.dhl24_api.errors import DHL24Error

    date_to_value = created_to or date.today()
    date_from_value = created_from or (date_to_value - timedelta(days=7))
    client = DHL24Client()
    try:
        count = await run_in_threadpool(
            client.get_my_shipments_count,
            created_from=date_from_value,
            created_to=date_to_value,
        )
        return DHLShipmentCountResponse(
            created_from=date_from_value,
            created_to=date_to_value,
            count=count,
        )
    except DHL24Error as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/shipments/{shipment_id}/track", response_model=DHLTrackResponse)
async def dhl_track(shipment_id: str):
    _ensure_configured()

    from app.connectors.dhl24_api import DHL24Client
    from app.connectors.dhl24_api.errors import DHL24Error

    client = DHL24Client()
    try:
        result = await run_in_threadpool(client.get_track_and_trace_info, shipment_id)
        payload = result.to_dict()
        return DHLTrackResponse(
            shipment_id=payload["shipment_id"],
            received_by=payload.get("received_by"),
            events=[DHLTrackingEventResponse(**item) for item in payload.get("events", [])],
        )
    except DHL24Error as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/shipments/{shipment_id}/scan", response_model=DHLBinaryDocumentResponse)
async def dhl_scan(shipment_id: str):
    _ensure_configured()

    from app.connectors.dhl24_api import DHL24Client
    from app.connectors.dhl24_api.errors import DHL24Error

    client = DHL24Client()
    try:
        result = await run_in_threadpool(client.get_shipment_scan, shipment_id)
        return DHLBinaryDocumentResponse(shipment_id=shipment_id, **result.to_dict())
    except DHL24Error as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/shipments/{shipment_id}/pod", response_model=DHLBinaryDocumentResponse)
async def dhl_pod(shipment_id: str):
    _ensure_configured()

    from app.connectors.dhl24_api import DHL24Client
    from app.connectors.dhl24_api.errors import DHL24Error

    client = DHL24Client()
    try:
        result = await run_in_threadpool(client.get_epod, shipment_id)
        return DHLBinaryDocumentResponse(shipment_id=shipment_id, **result.to_dict())
    except DHL24Error as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/shipments/{shipment_id}/labels-data", response_model=DHLLabelsDataResponse)
async def dhl_labels_data(shipment_id: str):
    _ensure_configured()

    from app.connectors.dhl24_api import DHL24Client
    from app.connectors.dhl24_api.errors import DHL24Error

    client = DHL24Client()
    try:
        items = await run_in_threadpool(client.get_labels_data, [shipment_id])
        if not items:
            raise HTTPException(status_code=404, detail="Labels data not found")
        payload = items[0].to_dict()
        return DHLLabelsDataResponse(
            **{
                **payload,
                "pieces": [DHLLabelPieceResponse(**piece) for piece in payload.get("pieces", [])],
            }
        )
    except DHL24Error as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/piece-id", response_model=DHLPieceResponse)
async def dhl_piece_id(
    shipment_number: str | None = Query(default=None),
    cedex_number: str | None = Query(default=None),
    package_number: str | None = Query(default=None),
):
    _ensure_configured()

    from app.connectors.dhl24_api import DHL24Client
    from app.connectors.dhl24_api.errors import DHL24Error

    client = DHL24Client()
    try:
        items = await run_in_threadpool(
            client.get_piece_id,
            shipment_number=shipment_number,
            cedex_number=cedex_number,
            package_number=package_number,
        )
        return DHLPieceResponse(
            count=len(items),
            items=[DHLPieceResponseItem(**item.to_dict()) for item in items],
        )
    except DHL24Error as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/cost-trace", response_model=DHLCostTraceResponse)
async def dhl_cost_trace(
    shipment_number: str | None = Query(default=None),
    tracking_number: str | None = Query(default=None),
    amazon_order_id: str | None = Query(default=None),
    limit_shipments: int = Query(default=20, ge=1, le=100),
):
    from app.services.dhl_observability import get_dhl_cost_trace

    try:
        result = await run_in_threadpool(
            get_dhl_cost_trace,
            shipment_number=shipment_number,
            tracking_number=tracking_number,
            amazon_order_id=amazon_order_id,
            limit_shipments=limit_shipments,
        )
        return DHLCostTraceResponse(**result)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/unmatched-shipments", response_model=DHLUnmatchedShipmentsResponse)
async def dhl_unmatched_shipments(
    created_from: date | None = Query(default=None),
    created_to: date | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
):
    from app.services.dhl_observability import list_unmatched_dhl_shipments

    result = await run_in_threadpool(
        list_unmatched_dhl_shipments,
        created_from=created_from,
        created_to=created_to,
        limit=limit,
    )
    return DHLUnmatchedShipmentsResponse(**result)


@router.get("/shadow-diff", response_model=DHLShadowDiffResponse)
async def dhl_shadow_diff(
    purchase_from: date | None = Query(default=None),
    purchase_to: date | None = Query(default=None),
    comparison_status: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
):
    from app.services.dhl_observability import get_dhl_shadow_diff_report

    result = await run_in_threadpool(
        get_dhl_shadow_diff_report,
        purchase_from=purchase_from,
        purchase_to=purchase_to,
        comparison_status=comparison_status,
        limit=limit,
    )
    return DHLShadowDiffResponse(**result)


@router.post("/jobs/backfill", response_model=JobRunOut)
async def run_dhl_backfill_job(
    created_from: date | None = Query(default=None),
    created_to: date | None = Query(default=None),
    include_events: bool = Query(default=True),
    limit_shipments: int | None = Query(default=None, ge=1, le=20000),
):
    _ensure_configured()
    params: dict[str, Any] = {
        "include_events": include_events,
    }
    if created_from:
        params["created_from"] = created_from.isoformat()
    if created_to:
        params["created_to"] = created_to.isoformat()
    if limit_shipments:
        params["limit_shipments"] = limit_shipments
    return await _run_dhl_job("dhl_backfill_shipments", params)


@router.post("/jobs/sync-events", response_model=JobRunOut)
async def run_dhl_tracking_sync_job(
    created_from: date | None = Query(default=None),
    created_to: date | None = Query(default=None),
    limit_shipments: int = Query(default=500, ge=1, le=20000),
):
    _ensure_configured()
    params: dict[str, Any] = {
        "limit_shipments": limit_shipments,
    }
    if created_from:
        params["created_from"] = created_from.isoformat()
    if created_to:
        params["created_to"] = created_to.isoformat()
    return await _run_dhl_job("dhl_sync_tracking_events", params)


@router.post("/jobs/import-billing-files", response_model=JobRunOut)
async def run_dhl_billing_import_job(
    invoice_root: str | None = Query(default=None),
    jj_root: str | None = Query(default=None),
    manifest_path: str | None = Query(default=None),
    include_shipment_seed: bool = Query(default=True),
    seed_all_existing: bool = Query(default=False),
    force_reimport: bool = Query(default=False),
    limit_invoice_files: int | None = Query(default=None, ge=1, le=1000),
    limit_jj_files: int | None = Query(default=None, ge=1, le=1000),
):
    params: dict[str, Any] = {
        "include_shipment_seed": include_shipment_seed,
        "seed_all_existing": seed_all_existing,
        "force_reimport": force_reimport,
    }
    if invoice_root:
        params["invoice_root"] = invoice_root
    if jj_root:
        params["jj_root"] = jj_root
    if manifest_path:
        params["manifest_path"] = manifest_path
    if limit_invoice_files:
        params["limit_invoice_files"] = limit_invoice_files
    if limit_jj_files:
        params["limit_jj_files"] = limit_jj_files
    return await _run_dhl_job("dhl_import_billing_files", params)


@router.post("/jobs/seed-shipments", response_model=JobRunOut)
async def run_dhl_seed_shipments_job(
    created_from: date | None = Query(default=None),
    created_to: date | None = Query(default=None),
    seed_all_existing: bool = Query(default=True),
    limit_parcels: int | None = Query(default=None, ge=1, le=500000),
):
    params: dict[str, Any] = {
        "seed_all_existing": seed_all_existing,
    }
    if created_from:
        params["created_from"] = created_from.isoformat()
    if created_to:
        params["created_to"] = created_to.isoformat()
    if limit_parcels:
        params["limit_parcels"] = limit_parcels
    return await _run_dhl_job("dhl_seed_shipments_from_staging", params)


@router.post("/jobs/sync-costs", response_model=JobRunOut)
async def run_dhl_cost_sync_job(
    created_from: date | None = Query(default=None),
    created_to: date | None = Query(default=None),
    limit_shipments: int = Query(default=500, ge=1, le=20000),
    allow_estimated: bool = Query(default=True),
    refresh_existing: bool = Query(default=False),
):
    if allow_estimated:
        _ensure_configured()
    params: dict[str, Any] = {
        "limit_shipments": limit_shipments,
        "allow_estimated": allow_estimated,
        "refresh_existing": refresh_existing,
    }
    if created_from:
        params["created_from"] = created_from.isoformat()
    if created_to:
        params["created_to"] = created_to.isoformat()
    return await _run_dhl_job("dhl_sync_costs", params)


@router.post("/jobs/aggregate-logistics", response_model=JobRunOut)
async def run_dhl_logistics_aggregate_job(
    created_from: date | None = Query(default=None),
    created_to: date | None = Query(default=None),
    limit_orders: int = Query(default=5000, ge=1, le=50000),
):
    params: dict[str, Any] = {
        "limit_orders": limit_orders,
    }
    if created_from:
        params["created_from"] = created_from.isoformat()
    if created_to:
        params["created_to"] = created_to.isoformat()
    return await _run_dhl_job("dhl_aggregate_logistics", params)


@router.post("/jobs/shadow-logistics", response_model=JobRunOut)
async def run_dhl_logistics_shadow_job(
    purchase_from: date | None = Query(default=None),
    purchase_to: date | None = Query(default=None),
    limit_orders: int = Query(default=10000, ge=1, le=100000),
):
    params: dict[str, Any] = {
        "limit_orders": limit_orders,
    }
    if purchase_from:
        params["purchase_from"] = purchase_from.isoformat()
    if purchase_to:
        params["purchase_to"] = purchase_to.isoformat()
    return await _run_dhl_job("dhl_shadow_logistics", params)


def _ensure_configured() -> None:
    if not settings.dhl24_api_enabled:
        raise HTTPException(
            status_code=503,
            detail="DHL24 API not configured - set DHL24_API_USERNAME + DHL24_API_PASSWORD in .env",
        )


async def _run_dhl_job(job_type: str, params: dict[str, Any]) -> JobRunOut:
    from app.connectors.mssql import enqueue_job

    try:
        job = await run_in_threadpool(
            enqueue_job,
            job_type=job_type,
            marketplace_id=None,
            trigger_source="manual",
            triggered_by=settings.DEFAULT_ACTOR,
            params=params,
        )
        return JobRunOut(**job)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"DHL job failed: {exc}") from exc
