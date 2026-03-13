"""Tax Compliance API Router — /tax prefix."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool

router = APIRouter(prefix="/tax", tags=["tax-compliance"])


# ── GET /tax/overview ───────────────────────────────────────────────

@router.get("/overview")
async def tax_overview():
    """Aggregated overview across all compliance modules."""
    try:
        from app.services.tax_compliance.alert_rules import list_compliance_issues
        from app.services.tax_compliance.oss_center import get_oss_overview
        from app.services.tax_compliance.fba_movements import get_fba_movements_summary
        from app.services.tax_compliance.local_vat import get_local_vat_summary
        from app.services.tax_compliance.evidence_control import get_evidence_summary
        from app.services.tax_compliance.amazon_clearing import get_reconciliation_summary
        from app.services.tax_compliance.filing_readiness import get_filing_readiness

        cutoff = date.today() - timedelta(days=90)

        classification_summary = await run_in_threadpool(
            _get_classification_summary, cutoff
        )
        oss_summary = await run_in_threadpool(get_oss_overview)
        local_vat = await run_in_threadpool(get_local_vat_summary)
        evidence = await run_in_threadpool(get_evidence_summary, cutoff, date.today())
        movements = await run_in_threadpool(get_fba_movements_summary)
        reconciliation = await run_in_threadpool(get_reconciliation_summary)
        filing = await run_in_threadpool(get_filing_readiness, None)
        issues = await run_in_threadpool(list_compliance_issues, status="open")

        return {
            "classification_summary": classification_summary,
            "oss_summary": oss_summary,
            "local_vat_summary": local_vat,
            "evidence_summary": evidence,
            "movements_summary": movements,
            "reconciliation_summary": reconciliation,
            "filing_readiness": filing,
            "open_issues": issues.get("total", 0),
            "p1_issues": sum(
                1 for i in issues.get("items", []) if i.get("severity") == "P1"
            ),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _get_classification_summary(cutoff: date) -> dict:
    from app.core.db_connection import connect_acc

    conn = connect_acc(autocommit=False, timeout=30)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT vat_classification, COUNT(*) as cnt,
                   SUM(amount_gross) as total_gross
            FROM dbo.vat_event_ledger WITH (NOLOCK)
            WHERE event_date >= ?
            GROUP BY vat_classification
        """, (cutoff,))
        cols = [c[0] for c in cur.description] if cur.description else []
        rows = [{cols[i]: r[i] for i in range(len(cols))} for r in cur.fetchall()]
        return {"by_classification": rows, "total": sum(r.get("cnt", 0) for r in rows)}
    finally:
        conn.close()


# ── GET /tax/vat-events ─────────────────────────────────────────────

@router.get("/vat-events")
async def list_vat_events(
    date_from: Optional[date] = Query(default=None),
    date_to: Optional[date] = Query(default=None),
    classification: Optional[str] = Query(default=None),
    marketplace: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    try:
        result = await run_in_threadpool(
            _query_vat_events, date_from, date_to, classification, marketplace, page, page_size
        )
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _query_vat_events(
    date_from, date_to, classification, marketplace, page, page_size
) -> dict:
    from app.core.db_connection import connect_acc

    conn = connect_acc(autocommit=False, timeout=30)
    cur = conn.cursor()
    try:
        where = ["1=1"]
        params: list = []
        if date_from:
            where.append("vel.event_date >= ?")
            params.append(date_from)
        if date_to:
            where.append("vel.event_date <= ?")
            params.append(date_to)
        if classification:
            where.append("vel.vat_classification = ?")
            params.append(classification)
        if marketplace:
            where.append("vel.marketplace = ?")
            params.append(marketplace)

        w = " AND ".join(where)
        offset = (page - 1) * page_size

        cur.execute(f"SELECT COUNT(*) FROM dbo.vat_event_ledger vel WITH (NOLOCK) WHERE {w}", tuple(params))
        total = cur.fetchone()[0] or 0

        cur.execute(f"""
            SELECT vel.* FROM dbo.vat_event_ledger vel WITH (NOLOCK)
            WHERE {w}
            ORDER BY vel.event_date DESC, vel.id DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, tuple(params) + (offset, page_size))

        cols = [c[0] for c in cur.description] if cur.description else []
        items = [{cols[i]: r[i] for i in range(len(cols))} for r in cur.fetchall()]
        return {"items": items, "total": total, "page": page, "page_size": page_size}
    finally:
        conn.close()


# ── POST /tax/classification/recompute ──────────────────────────────

@router.post("/classification/recompute")
async def recompute_classification(
    date_from: Optional[date] = Query(default=None),
    date_to: Optional[date] = Query(default=None),
    reprocess: bool = Query(default=False),
):
    try:
        from app.services.tax_compliance.classification_engine import classify_vat_events

        d_to = date_to or date.today()
        d_from = date_from or (d_to - timedelta(days=30))
        result = await run_in_threadpool(classify_vat_events, d_from, d_to, reprocess)
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── POST /tax/vat-events/{event_id}/override-classification ────────

@router.post("/vat-events/{event_id}/override-classification")
async def override_classification(event_id: int, new_classification: str = Query(...), reviewer: Optional[str] = Query(default=None)):
    try:
        from app.services.tax_compliance.classification_engine import override_classification as do_override

        result = await run_in_threadpool(do_override, event_id, new_classification, reviewer)
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── GET /tax/oss/overview ───────────────────────────────────────────

@router.get("/oss/overview")
async def oss_overview():
    try:
        from app.services.tax_compliance.oss_center import get_oss_overview

        return await run_in_threadpool(get_oss_overview)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── GET /tax/oss/period/{year}/{quarter} ────────────────────────────

@router.get("/oss/period/{year}/{quarter}")
async def oss_period_detail(year: int, quarter: int):
    try:
        from app.services.tax_compliance.oss_center import get_oss_period_detail

        return await run_in_threadpool(get_oss_period_detail, year, quarter)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── POST /tax/oss/build-period ──────────────────────────────────────

@router.post("/oss/build-period")
async def build_oss_period(year: Optional[int] = Query(default=None), quarter: Optional[int] = Query(default=None)):
    try:
        from app.services.tax_compliance.oss_center import build_oss_period as do_build

        y = year or date.today().year
        q = quarter or ((date.today().month - 1) // 3 + 1)
        return await run_in_threadpool(do_build, y, q)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── GET /tax/oss/corrections ────────────────────────────────────────

@router.get("/oss/corrections")
async def oss_corrections(year: Optional[int] = Query(default=None)):
    try:
        from app.services.tax_compliance.oss_center import get_oss_corrections

        return await run_in_threadpool(get_oss_corrections, year or date.today().year)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── GET /tax/evidence ───────────────────────────────────────────────

@router.get("/evidence")
async def evidence_list(
    status: Optional[str] = Query(default=None),
    marketplace: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    try:
        from app.services.tax_compliance.evidence_control import list_evidence_records

        return await run_in_threadpool(list_evidence_records, status, marketplace, page, page_size)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── GET /tax/evidence/summary ───────────────────────────────────────

@router.get("/evidence/summary")
async def evidence_summary(
    date_from: Optional[date] = Query(default=None),
    date_to: Optional[date] = Query(default=None),
):
    try:
        from app.services.tax_compliance.evidence_control import get_evidence_summary

        d_to = date_to or date.today()
        d_from = date_from or (d_to - timedelta(days=90))
        return await run_in_threadpool(get_evidence_summary, d_from, d_to)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── GET /tax/local-vat ──────────────────────────────────────────────

@router.get("/local-vat")
async def local_vat_list(
    country: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    try:
        from app.services.tax_compliance.local_vat import list_local_vat

        return await run_in_threadpool(list_local_vat, country, status, page, page_size)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── GET /tax/local-vat/summary ──────────────────────────────────────

@router.get("/local-vat/summary")
async def local_vat_summary():
    try:
        from app.services.tax_compliance.local_vat import get_local_vat_summary

        return await run_in_threadpool(get_local_vat_summary)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── GET /tax/fba-movements ──────────────────────────────────────────

@router.get("/fba-movements")
async def fba_movements_list(
    from_country: Optional[str] = Query(default=None),
    to_country: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    try:
        from app.services.tax_compliance.fba_movements import list_fba_movements

        return await run_in_threadpool(list_fba_movements, from_country, to_country, status, page, page_size)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── GET /tax/fba-movements/summary ──────────────────────────────────

@router.get("/fba-movements/summary")
async def fba_movements_summary():
    try:
        from app.services.tax_compliance.fba_movements import get_fba_movements_summary

        return await run_in_threadpool(get_fba_movements_summary)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── GET /tax/reconciliation/amazon ──────────────────────────────────

@router.get("/reconciliation/amazon")
async def reconciliation_list(
    status: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    try:
        from app.services.tax_compliance.amazon_clearing import list_reconciliations

        return await run_in_threadpool(list_reconciliations, status, page, page_size)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── POST /tax/reconciliation/run ────────────────────────────────────

@router.post("/reconciliation/run")
async def run_reconciliation(days_back: int = Query(default=60)):
    try:
        from app.services.tax_compliance.amazon_clearing import reconcile_amazon_clearing

        return await run_in_threadpool(reconcile_amazon_clearing, days_back)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── GET /tax/reconciliation/summary ─────────────────────────────────

@router.get("/reconciliation/summary")
async def reconciliation_summary():
    try:
        from app.services.tax_compliance.amazon_clearing import get_reconciliation_summary

        return await run_in_threadpool(get_reconciliation_summary)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── GET /tax/filing-readiness ───────────────────────────────────────

@router.get("/filing-readiness")
async def filing_readiness(period_ref: Optional[str] = Query(default=None)):
    try:
        from app.services.tax_compliance.filing_readiness import get_filing_readiness

        return await run_in_threadpool(get_filing_readiness, period_ref)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── GET /tax/filing-readiness/blockers ──────────────────────────────

@router.get("/filing-readiness/blockers")
async def filing_blockers(
    filing_type: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    try:
        from app.services.tax_compliance.filing_readiness import get_filing_blockers

        return await run_in_threadpool(get_filing_blockers, filing_type, page, page_size)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── GET /tax/audit-archive ──────────────────────────────────────────

@router.get("/audit-archive")
async def audit_archive_list():
    try:
        from app.services.tax_compliance.audit_archive import list_audit_packs

        return await run_in_threadpool(list_audit_packs)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── POST /tax/audit-pack/generate ───────────────────────────────────

@router.post("/audit-pack/generate")
async def generate_audit_pack(
    period_type: str = Query(default="quarter"),
    period_ref: Optional[str] = Query(default=None),
):
    try:
        from app.services.tax_compliance.audit_archive import generate_audit_pack as do_gen

        if not period_ref:
            today = date.today()
            q = (today.month - 1) // 3 + 1
            period_ref = f"{today.year}-Q{q}"
        return await run_in_threadpool(do_gen, period_type, period_ref)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── GET /tax/compliance-issues ──────────────────────────────────────

@router.get("/compliance-issues")
async def compliance_issues(
    issue_type: Optional[str] = Query(default=None),
    severity: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    country: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    try:
        from app.services.tax_compliance.alert_rules import list_compliance_issues

        return await run_in_threadpool(
            list_compliance_issues, issue_type, severity, status, country, page, page_size
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── POST /tax/compliance-issues/{id}/assign ─────────────────────────

@router.post("/compliance-issues/{issue_id}/assign")
async def assign_issue(issue_id: int, owner: str = Query(...)):
    try:
        from app.services.tax_compliance.alert_rules import assign_issue as do_assign

        return await run_in_threadpool(do_assign, issue_id, owner)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── POST /tax/compliance-issues/{id}/resolve ────────────────────────

@router.post("/compliance-issues/{issue_id}/resolve")
async def resolve_issue(issue_id: int, resolver: Optional[str] = Query(default=None)):
    try:
        from app.services.tax_compliance.alert_rules import resolve_issue as do_resolve

        return await run_in_threadpool(do_resolve, issue_id, resolver)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── POST /tax/detect-issues ─────────────────────────────────────────

@router.post("/detect-issues")
async def detect_issues(days_back: int = Query(default=90)):
    try:
        from app.services.tax_compliance.alert_rules import detect_compliance_issues

        return await run_in_threadpool(detect_compliance_issues, days_back)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── GET /tax/vat-rates ───────────────────────────────────────────────

@router.get("/vat-rates")
async def get_vat_rates():
    try:
        return await run_in_threadpool(_get_vat_rates)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _get_vat_rates() -> dict:
    from app.core.db_connection import connect_acc

    conn = connect_acc(autocommit=False, timeout=30)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT country, rate, valid_from, valid_to, is_default, source, product_type
            FROM dbo.vat_rate_mapping WITH (NOLOCK)
            WHERE is_default = 1
              AND (valid_to IS NULL OR valid_to >= CAST(GETDATE() AS DATE))
            ORDER BY country
        """)
        cols = [c[0] for c in cur.description] if cur.description else []
        items = [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]
        return {"items": items}
    finally:
        conn.close()


# ── POST /tax/vat-rates/upsert ──────────────────────────────────────

@router.post("/vat-rates/upsert")
async def upsert_vat_rate(
    country: str = Query(...),
    rate_type: str = Query(default="standard"),
    rate: float = Query(...),
    valid_from: Optional[date] = Query(default=None),
):
    try:
        result = await run_in_threadpool(_upsert_vat_rate, country, rate_type, rate, valid_from)
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _upsert_vat_rate(country: str, rate_type: str, rate: float, valid_from) -> dict:
    from app.core.db_connection import connect_acc

    conn = connect_acc(autocommit=False, timeout=30)
    cur = conn.cursor()
    try:
        cur.execute("""
            MERGE dbo.vat_rate_mapping AS t
            USING (SELECT ? AS country, ? AS pt) AS s
            ON t.country = s.country AND ISNULL(t.product_type, 'standard') = s.pt AND t.is_default = 1
            WHEN MATCHED THEN UPDATE SET rate = ?, valid_from = ISNULL(?, t.valid_from)
            WHEN NOT MATCHED THEN INSERT (country, product_type, rate, valid_from, is_default, source)
                VALUES (?, ?, ?, ISNULL(?, CAST(GETDATE() AS DATE)), 1, 'manual');
        """, (country, rate_type, rate, valid_from, country, rate_type, rate, valid_from))
        conn.commit()
        return {"upserted": True, "country": country, "rate_type": rate_type, "rate": rate}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── POST /tax/ecb-rates/sync ────────────────────────────────────────

@router.post("/ecb-rates/sync")
async def sync_ecb_rates(days_back: int = Query(default=30)):
    try:
        from app.services.tax_compliance.oss_center import sync_ecb_rates

        return await run_in_threadpool(sync_ecb_rates, days_back)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── POST /tax/pipeline/run ──────────────────────────────────────────

@router.post("/pipeline/run")
async def run_pipeline(days_back: int = Query(default=30)):
    """Run the full compliance pipeline end-to-end."""
    try:
        from app.services.tax_compliance.jobs import run_full_compliance_pipeline

        return await run_full_compliance_pipeline(days_back)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
