"""
GLS API endpoints — Track And Trace V1 + Poland ADE WebAPI integration.

Prefix: /api/v1/gls
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

from app.core.config import settings
from app.schemas.jobs import JobRunOut

router = APIRouter(prefix="/gls", tags=["GLS"])


# ── Schemas ───────────────────────────────────────────────────────

class GLSHealthResponse(BaseModel):
    ok: bool
    sandbox: bool | None = None
    base_url: str | None = None
    event_codes_count: int | None = None
    error: str | None = None


class GLSTrackResponse(BaseModel):
    parcel_number: str
    unitno: str
    status: str
    status_datetime: str
    is_delivered: bool
    events_count: int = 0
    events: list[dict[str, Any]] = []
    error_code: str | None = None
    error_message: str | None = None


class GLSTrackBatchRequest(BaseModel):
    parcel_numbers: list[str]


class GLSTrackBatchResponse(BaseModel):
    results: dict[str, GLSTrackResponse | None]
    tracked: int
    errors: int


class GLSTrackByRefResponse(BaseModel):
    references: list[str]
    results: list[GLSTrackResponse]
    count: int


# ── Endpoints ─────────────────────────────────────────────────────

@router.get("/health", response_model=GLSHealthResponse)
async def gls_health():
    """Check GLS T&T API connectivity and credentials."""
    if not settings.gls_api_enabled:
        return GLSHealthResponse(
            ok=False,
            error="GLS API not configured — set GLS_CLIENT_ID + GLS_CLIENT_SECRET in .env",
        )

    from app.connectors.gls_api import GLSClient
    client = GLSClient()
    result = client.health_check()
    return GLSHealthResponse(**result)


@router.get("/track/{parcel_number}", response_model=GLSTrackResponse)
async def gls_track_parcel(parcel_number: str):
    """Track a single GLS parcel by its parcel number (unitno)."""
    _ensure_configured()

    from app.connectors.gls_api import GLSClient
    from app.connectors.gls_api.client import GLSAPIError

    client = GLSClient()
    try:
        result = client.track(parcel_number)
        return GLSTrackResponse(**result.to_dict())
    except GLSAPIError as exc:
        raise HTTPException(
            status_code=exc.status_code or 502,
            detail=str(exc),
        )


@router.post("/track/batch", response_model=GLSTrackBatchResponse)
async def gls_track_batch(body: GLSTrackBatchRequest):
    """Track multiple GLS parcels in one call (max 100)."""
    _ensure_configured()

    if len(body.parcel_numbers) > 100:
        raise HTTPException(status_code=400, detail="Max 100 parcels per batch")

    from app.connectors.gls_api import GLSClient

    client = GLSClient()
    raw_results = client.track_batch(body.parcel_numbers)

    results: dict[str, GLSTrackResponse | None] = {}
    tracked = 0
    errors = 0
    for pn, res in raw_results.items():
        if res and not res.error_code:
            results[pn] = GLSTrackResponse(**res.to_dict())
            tracked += 1
        else:
            results[pn] = GLSTrackResponse(**res.to_dict()) if res else None
            errors += 1

    return GLSTrackBatchResponse(results=results, tracked=tracked, errors=errors)


@router.get("/track-by-ref", response_model=GLSTrackByRefResponse)
async def gls_track_by_reference(
    reference: str = Query(..., description="Reference(s) comma-separated: parcel number, track ID, or notification card ID"),
):
    """Track parcels by reference (parcel number, track ID, notification card ID). Max 10."""
    _ensure_configured()

    from app.connectors.gls_api import GLSClient
    from app.connectors.gls_api.client import GLSAPIError

    refs = [r.strip() for r in reference.split(",") if r.strip()]
    if len(refs) > 10:
        raise HTTPException(status_code=400, detail="Max 10 references per request")

    client = GLSClient()
    try:
        results = client.track_by_reference(refs)
        items = [GLSTrackResponse(**r.to_dict()) for r in results]
        return GLSTrackByRefResponse(references=refs, results=items, count=len(items))
    except GLSAPIError as exc:
        raise HTTPException(
            status_code=exc.status_code or 502,
            detail=str(exc),
        )


@router.get("/event-codes")
async def gls_event_codes():
    """Get all GLS track & trace event codes with descriptions."""
    _ensure_configured()

    from app.connectors.gls_api import GLSClient
    from app.connectors.gls_api.client import GLSAPIError

    client = GLSClient()
    try:
        return {"event_codes": client.get_event_codes()}
    except GLSAPIError as exc:
        raise HTTPException(status_code=exc.status_code or 502, detail=str(exc))


# ── Cost Center Posting ──────────────────────────────────────────

class CostCenterItemSchema(BaseModel):
    sender_cost_center: str
    receiver_cost_center: str
    amount: str
    currency: str = "EUR"
    cost_element: str
    item_text: str = ""


class CostCenterPostRequest(BaseModel):
    transaction_id: str
    process_code: str = "CT"
    username: str
    doc_date: str
    posting_date: str
    items: list[CostCenterItemSchema]
    doc_header_text: str = ""


class CostCenterPostResponse(BaseModel):
    success: bool
    transaction_id: str
    request_id: str | None = None
    doc_no: str | None = None
    error: str | None = None


@router.get("/cost-center/health")
async def gls_cost_center_health():
    """Check Cost Center Posting API connectivity (CSRF token fetch)."""
    _ensure_configured()

    from app.connectors.gls_api import GLSCostCenterClient
    client = GLSCostCenterClient()
    return client.health_check()


@router.post("/cost-center/post", response_model=CostCenterPostResponse)
async def gls_cost_center_post(body: CostCenterPostRequest):
    """Create a Cost Center posting in SAP via GLS OData API."""
    _ensure_configured()

    from app.connectors.gls_api import GLSCostCenterClient
    from app.connectors.gls_api.cost_center import CostCenterItem, CostCenterAPIError

    client = GLSCostCenterClient()
    items = [
        CostCenterItem(
            sender_cost_center=i.sender_cost_center,
            receiver_cost_center=i.receiver_cost_center,
            amount=i.amount,
            currency=i.currency,
            cost_element=i.cost_element,
            item_text=i.item_text,
        )
        for i in body.items
    ]

    try:
        result = client.post_transaction(
            transaction_id=body.transaction_id,
            process_code=body.process_code,
            username=body.username,
            doc_date=body.doc_date,
            posting_date=body.posting_date,
            items=items,
            doc_header_text=body.doc_header_text,
        )
        return CostCenterPostResponse(**result.to_dict())
    except CostCenterAPIError as exc:
        raise HTTPException(status_code=exc.status_code or 502, detail=str(exc))


@router.post("/jobs/import-billing-files", response_model=JobRunOut)
async def run_gls_billing_import_job(
    invoice_root: str | None = Query(default=None),
    bl_map_path: str | None = Query(default=None),
    include_shipment_seed: bool = Query(default=True),
    seed_all_existing: bool = Query(default=False),
    force_reimport: bool = Query(default=False),
    limit_invoice_files: int | None = Query(default=None, ge=1, le=2000),
):
    params: dict[str, Any] = {
        "include_shipment_seed": include_shipment_seed,
        "seed_all_existing": seed_all_existing,
        "force_reimport": force_reimport,
    }
    if invoice_root:
        params["invoice_root"] = invoice_root
    if bl_map_path:
        params["bl_map_path"] = bl_map_path
    if limit_invoice_files:
        params["limit_invoice_files"] = limit_invoice_files
    return await _run_gls_job("gls_import_billing_files", params)


@router.post("/jobs/seed-shipments", response_model=JobRunOut)
async def run_gls_seed_shipments_job(
    created_from: str | None = Query(default=None),
    created_to: str | None = Query(default=None),
    seed_all_existing: bool = Query(default=True),
    limit_parcels: int | None = Query(default=None, ge=1, le=500000),
):
    params: dict[str, Any] = {
        "seed_all_existing": seed_all_existing,
    }
    if created_from:
        params["created_from"] = created_from
    if created_to:
        params["created_to"] = created_to
    if limit_parcels:
        params["limit_parcels"] = limit_parcels
    return await _run_gls_job("gls_seed_shipments_from_staging", params)


@router.post("/jobs/sync-costs", response_model=JobRunOut)
async def run_gls_sync_costs_job(
    created_from: str | None = Query(default=None),
    created_to: str | None = Query(default=None),
    billing_periods: list[str] | None = Query(default=None),
    limit_shipments: int = Query(default=5000, ge=1, le=100000),
    refresh_existing: bool = Query(default=False),
    seeded_only: bool = Query(default=False),
    only_primary_linked: bool = Query(default=False),
):
    params: dict[str, Any] = {
        "limit_shipments": limit_shipments,
        "refresh_existing": refresh_existing,
        "seeded_only": seeded_only,
        "only_primary_linked": only_primary_linked,
    }
    if created_from:
        params["created_from"] = created_from
    if created_to:
        params["created_to"] = created_to
    if billing_periods:
        params["billing_periods"] = [str(item).strip() for item in billing_periods if str(item).strip()]
    return await _run_gls_job("gls_sync_costs", params)


@router.post("/jobs/aggregate-logistics", response_model=JobRunOut)
async def run_gls_logistics_aggregate_job(
    created_from: str | None = Query(default=None),
    created_to: str | None = Query(default=None),
    limit_orders: int = Query(default=5000, ge=1, le=100000),
):
    params: dict[str, Any] = {
        "limit_orders": limit_orders,
    }
    if created_from:
        params["created_from"] = created_from
    if created_to:
        params["created_to"] = created_to
    return await _run_gls_job("gls_aggregate_logistics", params)


@router.post("/jobs/shadow-logistics", response_model=JobRunOut)
async def run_gls_logistics_shadow_job(
    purchase_from: str | None = Query(default=None),
    purchase_to: str | None = Query(default=None),
    limit_orders: int = Query(default=10000, ge=1, le=100000),
):
    params: dict[str, Any] = {
        "limit_orders": limit_orders,
    }
    if purchase_from:
        params["purchase_from"] = purchase_from
    if purchase_to:
        params["purchase_to"] = purchase_to
    return await _run_gls_job("gls_shadow_logistics", params)


# ── Helpers ───────────────────────────────────────────────────────

def _ensure_configured():
    if not settings.gls_api_enabled:
        raise HTTPException(
            status_code=503,
            detail="GLS API not configured — set GLS_CLIENT_ID + GLS_CLIENT_SECRET in .env",
        )


def _ensure_ade_configured():
    if not settings.gls_ade_enabled:
        raise HTTPException(
            status_code=503,
            detail="GLS ADE not configured — set GLS_ADE_USERNAME + GLS_ADE_PASSWORD in .env",
        )


# ── GLS Poland ADE WebAPI ────────────────────────────────────────

async def _run_gls_job(job_type: str, params: dict[str, Any]) -> JobRunOut:
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
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"GLS job run failed: {exc}") from exc


class ADEHealthResponse(BaseModel):
    ok: bool
    wsdl_url: str | None = None
    username: str | None = None
    session_active: bool | None = None
    services: Any = None
    error: str | None = None


class ADETrackResponse(BaseModel):
    parcel_number: str
    consignment: dict[str, Any] | None = None
    track_id: str | None = None
    error: str | None = None


class ADEPodResponse(BaseModel):
    parcel_number: str
    has_pod: bool
    file_pdf_base64: str | None = None


@router.get("/ade/health", response_model=ADEHealthResponse)
async def gls_ade_health():
    """Check GLS Poland ADE WebAPI connectivity (login + services query)."""
    if not settings.gls_ade_enabled:
        return ADEHealthResponse(
            ok=False,
            error="GLS ADE not configured — set GLS_ADE_USERNAME + GLS_ADE_PASSWORD in .env",
        )

    from app.connectors.gls_api import GLSADEClient
    client = GLSADEClient()
    result = client.health_check()
    return ADEHealthResponse(**result)


@router.get("/ade/track/{parcel_number}", response_model=ADETrackResponse)
async def gls_ade_track(parcel_number: str):
    """
    Track a Polish GLS parcel via ADE WebAPI.

    Uses adePickup_ParcelNumberSearch + adeTrackID_Get.
    """
    _ensure_ade_configured()

    from app.connectors.gls_api import GLSADEClient
    from app.connectors.gls_api.ade_client import ADEError

    client = GLSADEClient()
    try:
        consignment = client.search_parcel(parcel_number)
        track_id = None
        try:
            track_id = client.get_track_id(parcel_number)
        except ADEError:
            pass  # tracking ID optional
        return ADETrackResponse(
            parcel_number=parcel_number,
            consignment=consignment,
            track_id=track_id,
        )
    except ADEError as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@router.get("/ade/pod/{parcel_number}", response_model=ADEPodResponse)
async def gls_ade_pod(parcel_number: str):
    """Get Proof of Delivery PDF (base64) for a Polish GLS parcel."""
    _ensure_ade_configured()

    from app.connectors.gls_api import GLSADEClient
    from app.connectors.gls_api.ade_client import ADEError

    client = GLSADEClient()
    try:
        pdf_b64 = client.get_pod(parcel_number)
        return ADEPodResponse(
            parcel_number=parcel_number,
            has_pod=pdf_b64 is not None,
            file_pdf_base64=pdf_b64,
        )
    except ADEError as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@router.get("/ade/services")
async def gls_ade_services():
    """Get allowed services for this ADE account."""
    _ensure_ade_configured()

    from app.connectors.gls_api import GLSADEClient
    from app.connectors.gls_api.ade_client import ADEError

    client = GLSADEClient()
    try:
        return client.get_allowed_services()
    except ADEError as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@router.get("/ade/preparing-box/consignments")
async def gls_ade_preparing_box_list(id_start: int = 0):
    """List consignment IDs in the preparing box."""
    _ensure_ade_configured()

    from app.connectors.gls_api import GLSADEClient
    from app.connectors.gls_api.ade_client import ADEError

    client = GLSADEClient()
    try:
        ids = client.preparing_box_get_consign_ids(id_start=id_start)
        return {"consignment_ids": ids, "count": len(ids)}
    except ADEError as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@router.get("/ade/preparing-box/consignment/{consign_id}")
async def gls_ade_preparing_box_get(consign_id: int):
    """Get consignment details from preparing box."""
    _ensure_ade_configured()

    from app.connectors.gls_api import GLSADEClient
    from app.connectors.gls_api.ade_client import ADEError

    client = GLSADEClient()
    try:
        return client.preparing_box_get_consign(consign_id)
    except ADEError as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@router.get("/ade/preparing-box/labels/{consign_id}")
async def gls_ade_preparing_box_labels(consign_id: int, mode: str = "one_label_on_a4_lt_pdf"):
    """Get labels for a consignment in preparing box (base64 PDF)."""
    _ensure_ade_configured()

    from app.connectors.gls_api import GLSADEClient
    from app.connectors.gls_api.ade_client import ADEError

    client = GLSADEClient()
    try:
        return client.preparing_box_get_labels(consign_id, mode=mode)
    except ADEError as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@router.get("/ade/pickups")
async def gls_ade_pickup_list(id_start: int = 0):
    """List pickup IDs."""
    _ensure_ade_configured()

    from app.connectors.gls_api import GLSADEClient
    from app.connectors.gls_api.ade_client import ADEError

    client = GLSADEClient()
    try:
        ids = client.pickup_get_ids(id_start=id_start)
        return {"pickup_ids": ids, "count": len(ids)}
    except ADEError as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@router.get("/ade/pickup/{pickup_id}")
async def gls_ade_pickup_get(pickup_id: int):
    """Get pickup details."""
    _ensure_ade_configured()

    from app.connectors.gls_api import GLSADEClient
    from app.connectors.gls_api.ade_client import ADEError

    client = GLSADEClient()
    try:
        return client.pickup_get(pickup_id)
    except ADEError as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@router.get("/ade/pickup/{pickup_id}/labels")
async def gls_ade_pickup_labels(pickup_id: int, mode: str = "one_label_on_a4_lt_pdf"):
    """Get all labels for a pickup (base64 PDF)."""
    _ensure_ade_configured()

    from app.connectors.gls_api import GLSADEClient
    from app.connectors.gls_api.ade_client import ADEError

    client = GLSADEClient()
    try:
        return client.pickup_get_labels(pickup_id, mode=mode)
    except ADEError as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@router.get("/ade/pickup/{pickup_id}/receipt")
async def gls_ade_pickup_receipt(pickup_id: int):
    """Get pickup receipt (base64 PDF)."""
    _ensure_ade_configured()

    from app.connectors.gls_api import GLSADEClient
    from app.connectors.gls_api.ade_client import ADEError

    client = GLSADEClient()
    try:
        return client.pickup_get_receipt(pickup_id)
    except ADEError as exc:
        raise HTTPException(status_code=502, detail=str(exc))
