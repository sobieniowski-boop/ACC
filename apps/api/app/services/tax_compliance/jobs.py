"""
Background Jobs for Tax Compliance.

Idempotent scheduling wrappers for all compliance background tasks.
Called from the APScheduler or manual trigger via API.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import structlog

log = structlog.get_logger(__name__)


async def job_classify_vat_events(days_back: int = 30) -> dict[str, Any]:
    """Classify VAT events for recent orders."""
    from starlette.concurrency import run_in_threadpool
    from app.services.tax_compliance.classification_engine import classify_vat_events

    d_from = date.today() - timedelta(days=days_back)
    d_to = date.today()
    result = await run_in_threadpool(classify_vat_events, d_from, d_to, False)
    log.info("job.classify_vat_events.done", **result)
    return result


async def job_sync_transport_evidence(days_back: int = 30) -> dict[str, Any]:
    from starlette.concurrency import run_in_threadpool
    from app.services.tax_compliance.evidence_control import sync_transport_evidence

    d_from = date.today() - timedelta(days=days_back)
    d_to = date.today()
    result = await run_in_threadpool(sync_transport_evidence, d_from, d_to)
    log.info("job.sync_transport_evidence.done", **result)
    return result


async def job_build_oss_period(year: int | None = None, quarter: int | None = None) -> dict[str, Any]:
    from starlette.concurrency import run_in_threadpool
    from app.services.tax_compliance.oss_center import build_oss_period

    if year is None or quarter is None:
        today = date.today()
        quarter = (today.month - 1) // 3 + 1
        year = today.year
    result = await run_in_threadpool(build_oss_period, year, quarter)
    log.info("job.build_oss_period.done", **result)
    return result


async def job_sync_fba_movements(days_back: int = 60) -> dict[str, Any]:
    from starlette.concurrency import run_in_threadpool
    from app.services.tax_compliance.fba_movements import sync_fba_movements

    d_from = date.today() - timedelta(days=days_back)
    d_to = date.today()
    result = await run_in_threadpool(sync_fba_movements, d_from, d_to)
    log.info("job.sync_fba_movements.done", **result)
    return result


async def job_build_local_vat_ledger(days_back: int = 30) -> dict[str, Any]:
    from starlette.concurrency import run_in_threadpool
    from app.services.tax_compliance.local_vat import build_local_vat_ledger

    d_from = date.today() - timedelta(days=days_back)
    d_to = date.today()
    result = await run_in_threadpool(build_local_vat_ledger, d_from, d_to)
    log.info("job.build_local_vat_ledger.done", **result)
    return result


async def job_reconcile_amazon_clearing(days_back: int = 60) -> dict[str, Any]:
    from starlette.concurrency import run_in_threadpool
    from app.services.tax_compliance.amazon_clearing import reconcile_amazon_clearing

    result = await run_in_threadpool(reconcile_amazon_clearing, days_back)
    log.info("job.reconcile_amazon_clearing.done", **result)
    return result


async def job_build_filing_readiness(period_ref: str | None = None) -> dict[str, Any]:
    from starlette.concurrency import run_in_threadpool
    from app.services.tax_compliance.filing_readiness import build_filing_readiness_snapshot

    if period_ref is None:
        today = date.today()
        q = (today.month - 1) // 3 + 1
        period_ref = f"{today.year}-Q{q}"
    result = await run_in_threadpool(build_filing_readiness_snapshot, "quarter", period_ref)
    log.info("job.build_filing_readiness.done", **result)
    return result


async def job_detect_compliance_issues(days_back: int = 90) -> dict[str, Any]:
    from starlette.concurrency import run_in_threadpool
    from app.services.tax_compliance.alert_rules import detect_compliance_issues

    result = await run_in_threadpool(detect_compliance_issues, days_back)
    log.info("job.detect_compliance_issues.done", **result)
    return result


async def job_generate_audit_pack(period_ref: str | None = None) -> dict[str, Any]:
    from starlette.concurrency import run_in_threadpool
    from app.services.tax_compliance.audit_archive import generate_audit_pack

    if period_ref is None:
        today = date.today()
        q = (today.month - 1) // 3 + 1
        period_ref = f"{today.year}-Q{q}"
    result = await run_in_threadpool(generate_audit_pack, "quarter", period_ref)
    log.info("job.generate_audit_pack.done", **result)
    return result


async def job_sync_ecb_rates(days_back: int = 30) -> dict[str, Any]:
    from starlette.concurrency import run_in_threadpool
    from app.services.tax_compliance.oss_center import sync_ecb_rates

    result = await run_in_threadpool(sync_ecb_rates, days_back)
    log.info("job.sync_ecb_rates.done", **result)
    return result


async def job_classify_refunds(days_back: int = 30) -> dict[str, Any]:
    from starlette.concurrency import run_in_threadpool
    from app.services.tax_compliance.classification_engine import classify_refunds

    d_from = date.today() - timedelta(days=days_back)
    d_to = date.today()
    result = await run_in_threadpool(classify_refunds, d_from, d_to)
    log.info("job.classify_refunds.done", **result)
    return result


async def run_full_compliance_pipeline(days_back: int = 30) -> dict[str, Any]:
    """Run the full compliance pipeline in the correct order."""
    results = {}

    # 1. ECB rates first (needed for EUR conversion)
    results["ecb_rates"] = await job_sync_ecb_rates(days_back)

    # 2. Classify VAT events
    results["classify"] = await job_classify_vat_events(days_back)

    # 3. Classify refunds
    results["refunds"] = await job_classify_refunds(days_back)

    # 4. Parallel-safe tasks
    results["evidence"] = await job_sync_transport_evidence(days_back)
    results["fba_movements"] = await job_sync_fba_movements(max(days_back, 60))
    results["local_vat"] = await job_build_local_vat_ledger(days_back)
    results["reconciliation"] = await job_reconcile_amazon_clearing(max(days_back, 60))

    # 5. OSS period build
    results["oss"] = await job_build_oss_period()

    # 6. Filing readiness (depends on all above)
    results["filing"] = await job_build_filing_readiness()

    # 7. Detect issues (depends on all above)
    results["issues"] = await job_detect_compliance_issues()

    log.info("full_compliance_pipeline.done")
    return results
