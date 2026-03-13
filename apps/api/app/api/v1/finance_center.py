from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.concurrency import run_in_threadpool

from app.core.security import get_current_user, require_admin, require_analyst
from app.schemas.finance_center import (
    FinanceAccountItem,
    FinanceAutoMatchOut,
    FinanceBankImportOut,
    FinanceCreateOut,
    FinanceCompletenessResponse,
    FinanceDashboardResponse,
    FinanceGapDiagnosticsResponse,
    FinanceImportRequest,
    FinanceLedgerListResponse,
    FinanceManualLedgerCreate,
    FinancePayoutReconciliationListResponse,
    FinanceRevenueIntegrityResponse,
    FinanceSyncDiagnosticsResponse,
    FinanceTaxCodeItem,
)
from app.schemas.jobs import JobListResponse, JobRunOut

router = APIRouter(prefix="/finance", tags=["finance-center"])


@router.get("/dashboard", response_model=FinanceDashboardResponse, dependencies=[Depends(require_analyst)])
async def finance_dashboard(
    date_from: Optional[date] = Query(default=None, alias="from"),
    date_to: Optional[date] = Query(default=None, alias="to"),
):
    from app.services.finance_center import get_finance_dashboard

    try:
        return await run_in_threadpool(get_finance_dashboard, date_from, date_to)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Finance dashboard query failed: {exc}") from exc


@router.post("/import/amazon/transactions", response_model=JobRunOut, dependencies=[Depends(require_admin)], status_code=202)
async def finance_import_amazon_transactions(payload: FinanceImportRequest):
    from app.services.finance_center import queue_finance_job

    try:
        return await run_in_threadpool(
            queue_finance_job,
            "finance_sync_transactions",
            {"days_back": payload.days_back},
            payload.marketplace_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Finance transaction import failed: {exc}") from exc


@router.post("/import/amazon/settlements", response_model=JobRunOut, dependencies=[Depends(require_admin)], status_code=202)
async def finance_prepare_amazon_settlements():
    from app.services.finance_center import queue_finance_job

    try:
        return await run_in_threadpool(queue_finance_job, "finance_prepare_settlements", {})
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Settlement preparation failed: {exc}") from exc


@router.post("/import/bank/csv", response_model=FinanceBankImportOut, dependencies=[Depends(require_admin)])
async def finance_import_bank_csv(file: UploadFile = File(...)):
    from app.services.finance_center import import_bank_csv

    try:
        content = await file.read()
        return await run_in_threadpool(import_bank_csv, content, file.filename or "bank.csv")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Bank CSV import failed: {exc}") from exc


@router.get("/jobs", response_model=JobListResponse, dependencies=[Depends(require_analyst)])
async def finance_jobs(page: int = Query(default=1, ge=1), page_size: int = Query(default=30, ge=1, le=200)):
    from app.services.finance_center import list_finance_jobs

    try:
        return await run_in_threadpool(list_finance_jobs, page, page_size)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Finance jobs query failed: {exc}") from exc


@router.get("/jobs/{job_id}", response_model=JobRunOut, dependencies=[Depends(require_analyst)])
async def finance_job(job_id: str):
    from app.services.finance_center import get_finance_job

    job = await run_in_threadpool(get_finance_job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Finance job not found")
    return job


@router.get("/sync/diagnostics", response_model=FinanceSyncDiagnosticsResponse, dependencies=[Depends(require_analyst)])
async def finance_sync_diagnostics(
    limit: int = Query(default=30, ge=1, le=100),
    marketplace_id: Optional[str] = Query(default=None),
):
    from app.services.finance_center import get_finance_sync_diagnostics

    try:
        return await run_in_threadpool(get_finance_sync_diagnostics, limit, marketplace_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Finance sync diagnostics query failed: {exc}") from exc


@router.get("/sync/completeness", response_model=FinanceCompletenessResponse, dependencies=[Depends(require_analyst)])
async def finance_sync_completeness(days_back: int = Query(default=30, ge=1, le=180)):
    from app.services.finance_center import get_finance_data_completeness

    try:
        return await run_in_threadpool(get_finance_data_completeness, days_back)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Finance completeness query failed: {exc}") from exc


@router.get("/sync/gap-diagnostics", response_model=FinanceGapDiagnosticsResponse, dependencies=[Depends(require_analyst)])
async def finance_sync_gap_diagnostics(days_back: int = Query(default=30, ge=1, le=365)):
    from app.services.finance_center import get_finance_marketplace_gap_diagnostics

    try:
        return await run_in_threadpool(get_finance_marketplace_gap_diagnostics, days_back)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Finance gap diagnostics query failed: {exc}") from exc


@router.get("/sync/order-revenue-integrity", response_model=FinanceRevenueIntegrityResponse, dependencies=[Depends(require_analyst)])
async def finance_order_revenue_integrity(
    date_from: date = Query(alias="from"),
    date_to: date = Query(alias="to"),
):
    from app.services.finance_center import get_order_revenue_integrity

    try:
        return await run_in_threadpool(get_order_revenue_integrity, date_from, date_to)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Finance order revenue integrity query failed: {exc}") from exc


@router.get("/ledger", response_model=FinanceLedgerListResponse, dependencies=[Depends(require_analyst)])
async def finance_ledger(
    date_from: Optional[date] = Query(default=None, alias="from"),
    date_to: Optional[date] = Query(default=None, alias="to"),
    marketplace_id: Optional[str] = Query(default=None),
    account_code: Optional[str] = Query(default=None),
    sku: Optional[str] = Query(default=None),
    country: Optional[str] = Query(default=None),
    source: Optional[str] = Query(default=None),
):
    from app.services.finance_center import list_ledger

    try:
        return await run_in_threadpool(
            list_ledger,
            date_from=date_from,
            date_to=date_to,
            marketplace_id=marketplace_id,
            account_code=account_code,
            sku=sku,
            country=country,
            source=source,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Finance ledger query failed: {exc}") from exc


@router.post("/ledger/manual", response_model=FinanceCreateOut, dependencies=[Depends(require_admin)])
async def finance_manual_ledger_entry(payload: FinanceManualLedgerCreate, current_user: dict = Depends(get_current_user)):
    from app.services.finance_center import create_manual_ledger_entry

    try:
        return await run_in_threadpool(create_manual_ledger_entry, payload.model_dump(), str(current_user.get("user_id")))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Manual ledger entry failed: {exc}") from exc


@router.post("/ledger/reverse/{entry_id}", response_model=FinanceCreateOut, dependencies=[Depends(require_admin)])
async def finance_reverse_ledger_entry(entry_id: str, current_user: dict = Depends(get_current_user)):
    from app.services.finance_center import reverse_ledger_entry

    try:
        return await run_in_threadpool(reverse_ledger_entry, entry_id, str(current_user.get("user_id")))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Ledger reverse failed: {exc}") from exc


@router.get("/accounts", response_model=list[FinanceAccountItem], dependencies=[Depends(require_analyst)])
async def finance_accounts():
    from app.services.finance_center import list_accounts

    return await run_in_threadpool(list_accounts)


@router.put("/accounts", response_model=FinanceAccountItem, dependencies=[Depends(require_admin)])
async def finance_put_account(payload: FinanceAccountItem):
    from app.services.finance_center import upsert_account

    return await run_in_threadpool(upsert_account, payload.account_code, payload.name, payload.account_type, payload.parent_code)


@router.get("/tax-codes", response_model=list[FinanceTaxCodeItem], dependencies=[Depends(require_analyst)])
async def finance_tax_codes():
    from app.services.finance_center import list_tax_codes

    return await run_in_threadpool(list_tax_codes)


@router.put("/tax-codes", response_model=FinanceTaxCodeItem, dependencies=[Depends(require_admin)])
async def finance_put_tax_code(payload: FinanceTaxCodeItem):
    from app.services.finance_center import upsert_tax_code

    return await run_in_threadpool(upsert_tax_code, payload.code, payload.vat_rate, payload.oss_flag, payload.country, payload.description)


@router.get("/reconcile/payouts", response_model=FinancePayoutReconciliationListResponse, dependencies=[Depends(require_analyst)])
async def finance_reconcile_payouts(status: Optional[str] = Query(default=None)):
    from app.services.finance_center import list_payout_reconciliation

    return await run_in_threadpool(list_payout_reconciliation, status)


@router.post("/reconcile/payouts/auto-match", response_model=FinanceAutoMatchOut, dependencies=[Depends(require_admin)])
async def finance_auto_match_payouts():
    from app.services.finance_center import auto_match_payouts

    return await run_in_threadpool(auto_match_payouts)


@router.post("/jobs/run-ledger", response_model=JobRunOut, dependencies=[Depends(require_admin)], status_code=202)
async def finance_generate_ledger_job(payload: FinanceImportRequest):
    from app.services.finance_center import queue_finance_job

    return await run_in_threadpool(queue_finance_job, "finance_generate_ledger", {"days_back": payload.days_back}, payload.marketplace_id)


@router.post("/jobs/run-reconciliation", response_model=JobRunOut, dependencies=[Depends(require_admin)], status_code=202)
async def finance_reconcile_job():
    from app.services.finance_center import queue_finance_job

    return await run_in_threadpool(queue_finance_job, "finance_reconcile_payouts", {})
