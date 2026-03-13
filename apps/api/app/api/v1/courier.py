from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Query
from fastapi.concurrency import run_in_threadpool

from app.core.config import settings
from app.schemas.jobs import JobRunOut

router = APIRouter(prefix="/courier", tags=["courier"])


@router.get("/readiness")
async def get_courier_readiness(
    months: list[str] | None = Query(default=None),
    carriers: list[str] | None = Query(default=None),
):
    from app.services.courier_readiness import get_courier_readiness_snapshot

    return await run_in_threadpool(
        get_courier_readiness_snapshot,
        months=months,
        carriers=carriers,
    )


@router.get("/coverage-matrix")
async def get_courier_coverage_matrix_view(
    months: list[str] | None = Query(default=None),
    carriers: list[str] | None = Query(default=None),
):
    from app.services.courier_readiness import get_courier_coverage_matrix

    return await run_in_threadpool(
        get_courier_coverage_matrix,
        months=months,
        carriers=carriers,
    )


@router.get("/monthly-kpis")
async def get_courier_monthly_kpis_view(
    months: list[str] | None = Query(default=None),
    carriers: list[str] | None = Query(default=None),
):
    from app.services.courier_monthly_kpi import get_courier_monthly_kpi_snapshot

    return await run_in_threadpool(
        get_courier_monthly_kpi_snapshot,
        months=months,
        carriers=carriers,
    )


@router.get("/order-relations")
async def get_courier_order_relations_view(
    months: list[str] | None = Query(default=None),
    carriers: list[str] | None = Query(default=None),
    only_strong: bool = Query(default=False),
    limit: int = Query(default=200, ge=1, le=1000),
):
    from app.services.courier_order_relations import get_courier_order_relations

    return await run_in_threadpool(
        get_courier_order_relations,
        months=months,
        carriers=carriers,
        only_strong=only_strong,
        limit=limit,
    )


@router.get("/shipment-outcomes")
async def get_courier_shipment_outcomes_view(
    months: list[str] | None = Query(default=None),
    carriers: list[str] | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
):
    from app.services.courier_shipment_semantics import get_courier_shipment_outcomes

    return await run_in_threadpool(
        get_courier_shipment_outcomes,
        months=months,
        carriers=carriers,
        limit=limit,
    )


@router.get("/link-gap-diagnostics")
async def get_courier_link_gap_diagnostics_view(
    months: list[str] | None = Query(default=None),
    carriers: list[str] | None = Query(default=None),
    created_to_buffer_days: int = Query(default=31, ge=0, le=120),
    sample_limit: int = Query(default=20, ge=1, le=200),
):
    from app.services.courier_link_diagnostics import get_courier_link_gap_diagnostics

    return await run_in_threadpool(
        get_courier_link_gap_diagnostics,
        months=months,
        carriers=carriers,
        created_to_buffer_days=created_to_buffer_days,
        sample_limit=sample_limit,
    )


@router.get("/link-gap-summary")
async def get_courier_link_gap_summary_view(
    months: list[str] | None = Query(default=None),
    carriers: list[str] | None = Query(default=None),
    created_to_buffer_days: int = Query(default=31, ge=0, le=120),
):
    from app.services.courier_link_diagnostics import get_courier_link_gap_summary

    return await run_in_threadpool(
        get_courier_link_gap_summary,
        months=months,
        carriers=carriers,
        created_to_buffer_days=created_to_buffer_days,
    )


@router.get("/identifier-source-gaps")
async def get_courier_identifier_source_gaps_view(
    months: list[str] | None = Query(default=None),
    carriers: list[str] | None = Query(default=None),
    created_to_buffer_days: int = Query(default=31, ge=0, le=120),
):
    from app.services.courier_link_diagnostics import get_courier_identifier_source_gap_summary

    return await run_in_threadpool(
        get_courier_identifier_source_gap_summary,
        months=months,
        carriers=carriers,
        created_to_buffer_days=created_to_buffer_days,
    )


@router.get("/order-identity-gaps")
async def get_courier_order_identity_gaps_view(
    months: list[str] | None = Query(default=None),
    carriers: list[str] | None = Query(default=None),
    created_to_buffer_days: int = Query(default=31, ge=0, le=120),
    sample_limit: int = Query(default=10, ge=0, le=100),
):
    from app.services.courier_link_diagnostics import get_courier_order_identity_gap_summary

    return await run_in_threadpool(
        get_courier_order_identity_gap_summary,
        months=months,
        carriers=carriers,
        created_to_buffer_days=created_to_buffer_days,
        sample_limit=sample_limit,
    )


@router.get("/closed-month-readiness")
async def get_courier_closed_month_readiness_view(
    months: list[str] | None = Query(default=None),
    carriers: list[str] | None = Query(default=None),
    buffer_days: int = Query(default=45, ge=0, le=180),
    as_of: date | None = Query(default=None),
):
    from app.services.courier_readiness import get_courier_closed_month_readiness

    return await run_in_threadpool(
        get_courier_closed_month_readiness,
        months=months,
        carriers=carriers,
        buffer_days=buffer_days,
        as_of=as_of,
    )


@router.post("/jobs/evaluate-alerts", response_model=JobRunOut)
async def run_courier_alerts_job(
    window_days: int = Query(default=7, ge=1, le=90),
    cost_coverage_min_pct: float = Query(default=95.0, ge=0, le=100),
    link_coverage_min_pct: float = Query(default=95.0, ge=0, le=100),
    shadow_delta_max_pct: float = Query(default=10.0, ge=0, le=100),
    estimation_mape_max_pct: float = Query(default=25.0, ge=0, le=500),
    estimation_mae_max_pln: float = Query(default=3.0, ge=0, le=1000),
    estimation_min_samples: int = Query(default=30, ge=1, le=10000),
    estimation_days_back: int = Query(default=30, ge=1, le=365),
):
    params: dict[str, Any] = {
        "window_days": window_days,
        "cost_coverage_min_pct": cost_coverage_min_pct,
        "link_coverage_min_pct": link_coverage_min_pct,
        "shadow_delta_max_pct": shadow_delta_max_pct,
        "estimation_mape_max_pct": estimation_mape_max_pct,
        "estimation_mae_max_pln": estimation_mae_max_pln,
        "estimation_min_samples": estimation_min_samples,
        "estimation_days_back": estimation_days_back,
    }
    return await _run_courier_job("courier_evaluate_alerts", params)


@router.post("/jobs/verify-billing-completeness", response_model=JobRunOut)
async def run_courier_billing_verification_job(
    carrier: str | None = Query(default=None),
    billing_period: str | None = Query(default=None),
):
    params: dict[str, Any] = {}
    if carrier:
        params["carrier"] = carrier
    if billing_period:
        params["billing_period"] = billing_period
    return await _run_courier_job("courier_verify_billing_completeness", params)


@router.post("/jobs/refresh-monthly-kpis", response_model=JobRunOut)
async def run_courier_monthly_kpi_refresh_job(
    months: list[str] | None = Query(default=None),
    carriers: list[str] | None = Query(default=None),
    buffer_days: int = Query(default=45, ge=0, le=180),
):
    params: dict[str, Any] = {
        "buffer_days": int(buffer_days),
    }
    if months:
        params["months"] = [str(item).strip() for item in months if str(item).strip()]
    if carriers:
        params["carriers"] = [str(item).strip().upper() for item in carriers if str(item).strip()]
    return await _run_courier_job("courier_refresh_monthly_kpis", params)


@router.post("/jobs/refresh-order-relations", response_model=JobRunOut)
async def run_courier_order_relations_job(
    months: list[str] | None = Query(default=None),
    carriers: list[str] | None = Query(default=None),
    lookahead_days: int = Query(default=30, ge=1, le=90),
):
    params: dict[str, Any] = {
        "lookahead_days": int(lookahead_days),
    }
    if months:
        params["months"] = [str(item).strip() for item in months if str(item).strip()]
    if carriers:
        params["carriers"] = [str(item).strip().upper() for item in carriers if str(item).strip()]
    return await _run_courier_job("courier_refresh_order_relations", params)


@router.post("/jobs/refresh-shipment-outcomes", response_model=JobRunOut)
async def run_courier_shipment_outcomes_job(
    months: list[str] | None = Query(default=None),
    carriers: list[str] | None = Query(default=None),
):
    params: dict[str, Any] = {}
    if months:
        params["months"] = [str(item).strip() for item in months if str(item).strip()]
    if carriers:
        params["carriers"] = [str(item).strip().upper() for item in carriers if str(item).strip()]
    return await _run_courier_job("courier_refresh_shipment_outcomes", params)


@router.post("/jobs/sync-bl-distribution-cache", response_model=JobRunOut)
async def run_bl_distribution_cache_sync_job(
    date_confirmed_from: date | None = Query(default=None),
    date_confirmed_to: date | None = Query(default=None),
    source_ids: list[int] | None = Query(default=None),
    tracking_numbers: list[str] | None = Query(default=None),
    include_packages: bool = Query(default=True),
    limit_orders: int | None = Query(default=None, ge=1, le=5000),
):
    params: dict[str, Any] = {
        "include_packages": include_packages,
    }
    if date_confirmed_from:
        params["date_confirmed_from"] = date_confirmed_from.isoformat()
    if date_confirmed_to:
        params["date_confirmed_to"] = date_confirmed_to.isoformat()
    if source_ids:
        params["source_ids"] = [int(item) for item in source_ids]
    if tracking_numbers:
        params["tracking_numbers"] = [str(item).strip() for item in tracking_numbers if str(item).strip()]
    if limit_orders is not None:
        params["limit_orders"] = int(limit_orders)
    return await _run_courier_job("sync_bl_distribution_order_cache", params)


@router.post("/jobs/backfill-identifier-sources", response_model=JobRunOut)
async def run_courier_identifier_backfill_job(
    mode: str = Query(...),
    months: list[str] | None = Query(default=None),
    created_to_buffer_days: int = Query(default=31, ge=0, le=120),
    limit_values: int = Query(default=200, ge=1, le=1000),
    include_packages: bool = Query(default=True),
    include_bl_orders: bool = Query(default=True),
    include_dis_map: bool = Query(default=True),
    include_dhl_parcel_map: bool = Query(default=True),
):
    params: dict[str, Any] = {
        "mode": str(mode or "").strip().lower(),
        "created_to_buffer_days": int(created_to_buffer_days),
        "limit_values": int(limit_values),
        "include_packages": bool(include_packages),
        "include_bl_orders": bool(include_bl_orders),
        "include_dis_map": bool(include_dis_map),
        "include_dhl_parcel_map": bool(include_dhl_parcel_map),
    }
    if months:
        params["months"] = [str(item).strip() for item in months if str(item).strip()]
    return await _run_courier_job("courier_backfill_identifier_sources", params)


@router.post("/jobs/order-universe-linking", response_model=JobRunOut)
async def run_order_universe_linking_job(
    months: list[str] | None = Query(default=None),
    carriers: list[str] | None = Query(default=None),
    reset_existing_in_scope: bool = Query(default=False),
    run_aggregate_shadow: bool = Query(default=False),
    limit_orders: int = Query(default=3_000_000, ge=1, le=10_000_000),
    created_to_buffer_days: int = Query(default=31, ge=0, le=120),
):
    params: dict[str, Any] = {
        "reset_existing_in_scope": bool(reset_existing_in_scope),
        "run_aggregate_shadow": bool(run_aggregate_shadow),
        "limit_orders": int(limit_orders),
        "created_to_buffer_days": int(created_to_buffer_days),
    }
    if months:
        params["months"] = [str(item).strip() for item in months if str(item).strip()]
    if carriers:
        params["carriers"] = [str(item).strip().upper() for item in carriers if str(item).strip()]
    return await _run_courier_job("courier_order_universe_linking", params)


@router.post("/jobs/estimate-preinvoice-costs", response_model=JobRunOut)
async def run_preinvoice_estimator_job(
    carriers: list[str] | None = Query(default=None),
    created_from: date | None = Query(default=None),
    created_to: date | None = Query(default=None),
    horizon_days: int = Query(default=180, ge=30, le=365),
    min_samples: int = Query(default=10, ge=1, le=200),
    limit_shipments: int = Query(default=20000, ge=1, le=500000),
    refresh_existing: bool = Query(default=False),
):
    params: dict[str, Any] = {
        "horizon_days": int(horizon_days),
        "min_samples": int(min_samples),
        "limit_shipments": int(limit_shipments),
        "refresh_existing": bool(refresh_existing),
    }
    if carriers:
        params["carriers"] = [str(item).strip().upper() for item in carriers if str(item).strip()]
    if created_from:
        params["created_from"] = created_from.isoformat()
    if created_to:
        params["created_to"] = created_to.isoformat()
    return await _run_courier_job("courier_estimate_preinvoice_costs", params)


@router.post("/jobs/reconcile-estimated-costs", response_model=JobRunOut)
async def run_reconcile_estimates_job(
    carriers: list[str] | None = Query(default=None),
    limit_shipments: int = Query(default=50000, ge=1, le=500000),
):
    params: dict[str, Any] = {
        "limit_shipments": int(limit_shipments),
    }
    if carriers:
        params["carriers"] = [str(item).strip().upper() for item in carriers if str(item).strip()]
    return await _run_courier_job("courier_reconcile_estimated_costs", params)


@router.post("/jobs/compute-estimation-kpis", response_model=JobRunOut)
async def run_estimation_kpis_job(
    carriers: list[str] | None = Query(default=None),
    days_back: int = Query(default=30, ge=1, le=365),
):
    params: dict[str, Any] = {
        "days_back": int(days_back),
    }
    if carriers:
        params["carriers"] = [str(item).strip().upper() for item in carriers if str(item).strip()]
    return await _run_courier_job("courier_compute_estimation_kpis", params)


async def _run_courier_job(job_type: str, params: dict[str, Any]) -> JobRunOut:
    from app.connectors.mssql import enqueue_job

    job = await run_in_threadpool(
        enqueue_job,
        job_type=job_type,
        marketplace_id=None,
        trigger_source="manual",
        triggered_by=settings.DEFAULT_ACTOR,
        params=params,
    )
    return JobRunOut(**job)
