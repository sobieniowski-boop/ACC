"""
Family Mapper API routes — DE Canonical → EU variation family mapping.

13 endpoints covering:
  - Family list / detail / search
  - Children + marketplace links
  - Matching triggers
  - Coverage & issues
  - Fix packages (CRUD + approve/apply)
  - Review queue
"""
from __future__ import annotations

import asyncio
import json
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

from app.connectors.mssql import enqueue_job
from app.core.config import MARKETPLACE_REGISTRY, settings
from app.core.security import require_analyst, require_director, require_ops
from app.schemas.jobs import JobRunOut
from app.services.family_mapper import (
    analyze_restructure,
    analyze_restructure_all_marketplaces,
    create_restructure_run,
    execute_restructure,
    finish_restructure_run,
    generate_fix_package,
    get_restructure_run_status,
    get_rebuild_status,
    rebuild_de_canonical,
    recompute_coverage,
    run_matching,
    sync_marketplace_listings,
    update_restructure_run_progress,
)

router = APIRouter(prefix="/families", tags=["families"])

DE_MARKETPLACE = settings.SP_API_PRIMARY_MARKETPLACE


# ---------------------------------------------------------------------------
# Shared DB helper
# ---------------------------------------------------------------------------

def _connect():
    from app.core.db_connection import connect_acc
    return connect_acc(autocommit=False)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class FamilySummary(BaseModel):
    id: int
    de_parent_asin: str
    brand: str | None = None
    category: str | None = None
    product_type: str | None = None
    variation_theme_de: str | None = None
    children_count: int = 0
    marketplaces_mapped: int = 0
    de_sales_qty: int = 0


class FamilyListResponse(BaseModel):
    items: list[FamilySummary]
    total: int
    page: int
    page_size: int


class ChildOut(BaseModel):
    id: int
    master_key: str
    key_type: str
    de_child_asin: str
    sku_de: str | None = None
    ean_de: str | None = None
    attributes: dict | None = None


class ChildMarketLinkOut(BaseModel):
    global_family_id: int
    master_key: str
    marketplace: str
    target_child_asin: str | None = None
    current_parent_asin: str | None = None
    match_type: str
    confidence: int
    status: str
    reasons: list[str] | None = None


class CoverageOut(BaseModel):
    global_family_id: int
    marketplace: str
    de_children_count: int
    matched_children_count: int
    coverage_pct: int
    missing_children_count: int
    extra_children_count: int
    theme_mismatch: bool
    confidence_avg: int


class IssueOut(BaseModel):
    id: int
    global_family_id: int
    marketplace: str | None = None
    issue_type: str
    severity: str
    payload: dict | None = None


class FixPackageOut(BaseModel):
    id: int
    marketplace: str
    global_family_id: int
    action_plan: dict
    status: str
    generated_at: str | None = None
    approved_by: str | None = None
    approved_at: str | None = None
    applied_at: str | None = None


class FixPackageListResponse(BaseModel):
    items: list[FixPackageOut]
    total: int
    page: int
    page_size: int


class ReviewQueueItem(BaseModel):
    global_family_id: int
    de_parent_asin: str
    brand: str | None = None
    marketplace: str
    master_key: str
    de_child_asin: str | None = None
    target_child_asin: str | None = None
    match_type: str
    confidence: int
    status: str


class ReviewQueueResponse(BaseModel):
    items: list[ReviewQueueItem]
    total: int
    page: int
    page_size: int


class TriggerResponse(BaseModel):
    status: str
    result: dict


class ApproveRequest(BaseModel):
    approved_by: str


class StatusUpdateRequest(BaseModel):
    status: str
    master_key: str
    marketplace: str


# ---------------------------------------------------------------------------
# 1) GET /families — list with pagination + filters
# ---------------------------------------------------------------------------
# Allowed sort options for the family list
_SORT_OPTIONS = {
    "id": "gf.id",
    "brand": "gf.brand",
    "children": "ISNULL(cc.children_count, 0)",
    "sales_de": "ISNULL(s.qty, 0)",
    "marketplaces": "ISNULL(mc.mp_count, 0)",
}


@router.get("", response_model=FamilyListResponse)
async def list_families(
    page: int = Query(1, ge=1),
    page_size: int = Query(30, ge=1, le=200),
    brand: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    sort_by: str = Query("sales_de", description="Sort column: id, brand, children, sales_de, marketplaces"),
    sort_dir: str = Query("desc", description="Sort direction: asc or desc"),
    _user=Depends(require_analyst),
):
    de_marketplace = settings.SP_API_PRIMARY_MARKETPLACE

    def _query():
        conn = _connect()
        cur = conn.cursor()

        where_parts: list[str] = []
        params: list = []

        if brand:
            where_parts.append("gf.brand LIKE ?")
            params.append(f"%{brand}%")
        if category:
            where_parts.append("gf.category LIKE ?")
            params.append(f"%{category}%")
        if search:
            where_parts.append("(gf.de_parent_asin LIKE ? OR gf.brand LIKE ?)")
            params.extend([f"%{search}%", f"%{search}%"])

        where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        cur.execute(f"""
            SELECT COUNT(*) FROM dbo.global_family gf {where_clause}
        """, *params)
        total = cur.fetchone()[0]

        # Validate sort
        order_col = _SORT_OPTIONS.get(sort_by, _SORT_OPTIONS["sales_de"])
        direction = "ASC" if sort_dir.lower() == "asc" else "DESC"

        offset = (page - 1) * page_size
        cur.execute(f"""
            SELECT gf.id, gf.de_parent_asin, gf.brand, gf.category,
                   gf.product_type, gf.variation_theme_de,
                   ISNULL(cc.children_count, 0),
                   ISNULL(mc.mp_count, 0),
                   ISNULL(s.qty, 0) AS de_sales_qty
            FROM dbo.global_family gf
            LEFT JOIN (
                SELECT global_family_id, COUNT(*) AS children_count
                FROM dbo.global_family_child
                GROUP BY global_family_id
            ) cc ON cc.global_family_id = gf.id
            LEFT JOIN (
                SELECT global_family_id,
                       COUNT(DISTINCT marketplace) AS mp_count
                FROM dbo.global_family_market_link
                WHERE status <> 'unmapped'
                GROUP BY global_family_id
            ) mc ON mc.global_family_id = gf.id
            LEFT JOIN (
                SELECT p.parent_asin,
                       SUM(ol.quantity_ordered) AS qty
                FROM dbo.acc_order_line ol WITH (NOLOCK)
                JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
                JOIN dbo.acc_product p WITH (NOLOCK) ON p.asin = ol.asin
                WHERE o.marketplace_id = ?
                  AND p.parent_asin IS NOT NULL
                  AND ol.sku NOT LIKE 'amzn.gr.%%'
                GROUP BY p.parent_asin
            ) s ON s.parent_asin = gf.de_parent_asin
            {where_clause}
            ORDER BY {order_col} {direction}
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, de_marketplace, *params, offset, page_size)

        items = [
            FamilySummary(
                id=r[0], de_parent_asin=r[1], brand=r[2], category=r[3],
                product_type=r[4], variation_theme_de=r[5],
                children_count=r[6], marketplaces_mapped=r[7],
                de_sales_qty=r[8],
            )
            for r in cur.fetchall()
        ]
        conn.close()
        return FamilyListResponse(items=items, total=total, page=page, page_size=page_size)

    return await run_in_threadpool(_query)


# ---------------------------------------------------------------------------
# GET /families/marketplaces — list available marketplaces
# (static route — MUST be defined before /{family_id})
# ---------------------------------------------------------------------------
@router.get("/marketplaces")
async def list_marketplaces(_user=Depends(require_analyst)):
    return [
        {"marketplace_id": mp_id, **info}
        for mp_id, info in MARKETPLACE_REGISTRY.items()
    ]


# ---------------------------------------------------------------------------
# GET /families/review — review queue (proposed + needs_review links)
# (static route — MUST be defined before /{family_id})
# ---------------------------------------------------------------------------
@router.get("/review", response_model=ReviewQueueResponse)
async def review_queue(
    marketplace: Optional[str] = Query(None),
    status_filter: Optional[str] = Query(None, alias="status"),
    min_confidence: int = Query(0, ge=0),
    max_confidence: int = Query(100, le=100),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    _user=Depends(require_analyst),
):
    def _query():
        conn = _connect()
        cur = conn.cursor()

        where_parts = [
            "l.status IN ('proposed', 'needs_review')",
            "l.confidence BETWEEN ? AND ?",
        ]
        params: list = [min_confidence, max_confidence]

        if marketplace:
            where_parts.append("l.marketplace = ?")
            params.append(marketplace)
        if status_filter:
            where_parts[0] = "l.status = ?"
            params.insert(0, status_filter)

        where_clause = " AND ".join(where_parts)

        cur.execute(f"""
            SELECT COUNT(*)
            FROM dbo.global_family_child_market_link l
            WHERE {where_clause}
        """, *params)
        total = cur.fetchone()[0]

        offset = (page - 1) * page_size
        cur.execute(f"""
            SELECT l.global_family_id, gf.de_parent_asin, gf.brand,
                   l.marketplace, l.master_key,
                   gfc.de_child_asin, l.target_child_asin,
                   l.match_type, l.confidence, l.status
            FROM dbo.global_family_child_market_link l
            JOIN dbo.global_family gf ON gf.id = l.global_family_id
            LEFT JOIN dbo.global_family_child gfc
                ON gfc.global_family_id = l.global_family_id
               AND gfc.master_key = l.master_key
            WHERE {where_clause}
            ORDER BY l.confidence ASC, l.marketplace
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, *params, offset, page_size)

        items = [
            ReviewQueueItem(
                global_family_id=r[0], de_parent_asin=r[1], brand=r[2],
                marketplace=r[3], master_key=r[4],
                de_child_asin=r[5], target_child_asin=r[6],
                match_type=r[7], confidence=r[8], status=r[9],
            )
            for r in cur.fetchall()
        ]
        conn.close()
        return ReviewQueueResponse(items=items, total=total, page=page, page_size=page_size)

    return await run_in_threadpool(_query)


# ---------------------------------------------------------------------------
# GET /families/fix-packages — list fix packages
# (static route — MUST be defined before /{family_id})
# ---------------------------------------------------------------------------
@router.get("/fix-packages", response_model=FixPackageListResponse)
async def list_fix_packages(
    marketplace: Optional[str] = Query(None),
    status_filter: Optional[str] = Query(None, alias="status"),
    page: int = Query(1, ge=1),
    page_size: int = Query(30, ge=1, le=200),
    _user=Depends(require_analyst),
):
    def _query():
        conn = _connect()
        cur = conn.cursor()

        where_parts: list[str] = []
        params: list = []

        if marketplace:
            where_parts.append("marketplace = ?")
            params.append(marketplace)
        if status_filter:
            where_parts.append("status = ?")
            params.append(status_filter)

        where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        cur.execute(f"SELECT COUNT(*) FROM dbo.family_fix_package {where_clause}", *params)
        total = cur.fetchone()[0]

        offset = (page - 1) * page_size
        cur.execute(f"""
            SELECT id, marketplace, global_family_id, action_plan_json,
                   status, generated_at, approved_by, approved_at, applied_at
            FROM dbo.family_fix_package
            {where_clause}
            ORDER BY generated_at DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, *params, offset, page_size)

        items = [
            FixPackageOut(
                id=r[0], marketplace=r[1], global_family_id=r[2],
                action_plan=json.loads(r[3]) if r[3] else {},
                status=r[4], generated_at=str(r[5]) if r[5] else None,
                approved_by=r[6], approved_at=str(r[7]) if r[7] else None,
                applied_at=str(r[8]) if r[8] else None,
            )
            for r in cur.fetchall()
        ]
        conn.close()
        return FixPackageListResponse(items=items, total=total, page=page, page_size=page_size)

    return await run_in_threadpool(_query)


# ---------------------------------------------------------------------------
# 2) GET /families/{family_id} — single family detail
# ---------------------------------------------------------------------------
@router.get("/{family_id}")
async def get_family(family_id: int, _user=Depends(require_analyst)):
    def _query():
        conn = _connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, de_parent_asin, brand, category, product_type,
                   variation_theme_de, created_at
            FROM dbo.global_family WHERE id = ?
        """, family_id)
        r = cur.fetchone()
        if not r:
            conn.close()
            raise HTTPException(404, detail="Family not found")

        family = {
            "id": r[0], "de_parent_asin": r[1], "brand": r[2],
            "category": r[3], "product_type": r[4],
            "variation_theme_de": r[5], "created_at": str(r[6]),
        }

        # Children
        cur.execute("""
            SELECT id, master_key, key_type, de_child_asin,
                   sku_de, ean_de, attributes_json
            FROM dbo.global_family_child WHERE global_family_id = ?
        """, family_id)
        family["children"] = [
            {
                "id": c[0], "master_key": c[1], "key_type": c[2],
                "de_child_asin": c[3], "sku_de": c[4], "ean_de": c[5],
                "attributes": json.loads(c[6]) if c[6] else None,
            }
            for c in cur.fetchall()
        ]

        # Market links
        cur.execute("""
            SELECT global_family_id, marketplace, target_parent_asin,
                   status, confidence_avg
            FROM dbo.global_family_market_link WHERE global_family_id = ?
        """, family_id)
        family["market_links"] = [
            {
                "marketplace": m[1], "target_parent_asin": m[2],
                "status": m[3], "confidence_avg": m[4],
            }
            for m in cur.fetchall()
        ]

        conn.close()
        return family

    return await run_in_threadpool(_query)


# ---------------------------------------------------------------------------
# 3) GET /families/{family_id}/children — children with market links
# ---------------------------------------------------------------------------
@router.get("/{family_id}/children", response_model=list[ChildOut])
async def get_family_children(family_id: int, _user=Depends(require_analyst)):
    def _query():
        conn = _connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, master_key, key_type, de_child_asin,
                   sku_de, ean_de, attributes_json
            FROM dbo.global_family_child WHERE global_family_id = ?
        """, family_id)
        items = [
            ChildOut(
                id=r[0], master_key=r[1], key_type=r[2],
                de_child_asin=r[3], sku_de=r[4], ean_de=r[5],
                attributes=json.loads(r[6]) if r[6] else None,
            )
            for r in cur.fetchall()
        ]
        conn.close()
        return items

    return await run_in_threadpool(_query)


# ---------------------------------------------------------------------------
# 4) GET /families/{family_id}/links — child market links
# ---------------------------------------------------------------------------
@router.get("/{family_id}/links", response_model=list[ChildMarketLinkOut])
async def get_child_market_links(
    family_id: int,
    marketplace: Optional[str] = Query(None),
    _user=Depends(require_analyst),
):
    def _query():
        conn = _connect()
        cur = conn.cursor()
        q = """
            SELECT global_family_id, master_key, marketplace,
                   target_child_asin, current_parent_asin,
                   match_type, confidence, status, reason_json
            FROM dbo.global_family_child_market_link
            WHERE global_family_id = ?
        """
        params = [family_id]
        if marketplace:
            q += " AND marketplace = ?"
            params.append(marketplace)
        q += " ORDER BY confidence DESC"

        cur.execute(q, *params)
        items = [
            ChildMarketLinkOut(
                global_family_id=r[0], master_key=r[1], marketplace=r[2],
                target_child_asin=r[3], current_parent_asin=r[4],
                match_type=r[5], confidence=r[6], status=r[7],
                reasons=(json.loads(r[8]) or {}).get("reasons") if r[8] else None,
            )
            for r in cur.fetchall()
        ]
        conn.close()
        return items

    return await run_in_threadpool(_query)


# ---------------------------------------------------------------------------
# 5) PUT /families/{family_id}/links/status — manual override
# ---------------------------------------------------------------------------
@router.put("/{family_id}/links/status")
async def update_link_status(
    family_id: int,
    body: StatusUpdateRequest,
    _user=Depends(require_ops),
):
    def _update():
        conn = _connect()
        cur = conn.cursor()
        cur.execute("""
            UPDATE dbo.global_family_child_market_link
            SET status = ?, updated_at = SYSUTCDATETIME()
            WHERE global_family_id = ?
              AND master_key = ? AND marketplace = ?
        """, body.status, family_id, body.master_key, body.marketplace)
        conn.commit()
        affected = cur.rowcount
        conn.close()
        if affected == 0:
            raise HTTPException(404, "Link not found")
        return {"updated": affected}

    return await run_in_threadpool(_update)


# ---------------------------------------------------------------------------
# 6) GET /families/{family_id}/coverage — coverage cache
# ---------------------------------------------------------------------------
@router.get("/{family_id}/coverage", response_model=list[CoverageOut])
async def get_family_coverage(family_id: int, _user=Depends(require_analyst)):
    def _query():
        conn = _connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT global_family_id, marketplace,
                   de_children_count, matched_children_count,
                   coverage_pct, missing_children_count,
                   extra_children_count, theme_mismatch, confidence_avg
            FROM dbo.family_coverage_cache
            WHERE global_family_id = ?
        """, family_id)
        items = [
            CoverageOut(
                global_family_id=r[0], marketplace=r[1],
                de_children_count=r[2], matched_children_count=r[3],
                coverage_pct=r[4], missing_children_count=r[5],
                extra_children_count=r[6], theme_mismatch=bool(r[7]),
                confidence_avg=r[8],
            )
            for r in cur.fetchall()
        ]
        conn.close()
        return items

    return await run_in_threadpool(_query)


# ---------------------------------------------------------------------------
# 7) GET /families/{family_id}/issues — issues for family
# ---------------------------------------------------------------------------
@router.get("/{family_id}/issues", response_model=list[IssueOut])
async def get_family_issues(family_id: int, _user=Depends(require_analyst)):
    def _query():
        conn = _connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, global_family_id, marketplace, issue_type,
                   severity, payload_json
            FROM dbo.family_issues_cache
            WHERE global_family_id = ?
            ORDER BY severity, issue_type
        """, family_id)
        items = [
            IssueOut(
                id=r[0], global_family_id=r[1], marketplace=r[2],
                issue_type=r[3], severity=r[4],
                payload=json.loads(r[5]) if r[5] else None,
            )
            for r in cur.fetchall()
        ]
        conn.close()
        return items

    return await run_in_threadpool(_query)


# ---------------------------------------------------------------------------
# 8) POST /families/trigger/rebuild-de — trigger DE canonical rebuild
# ---------------------------------------------------------------------------
@router.post("/trigger/rebuild-de", response_model=TriggerResponse)
async def trigger_rebuild_de(
    background_tasks: BackgroundTasks,
    max_parents: int = Query(200, ge=1, le=1000),
    brand_filter: Optional[str] = Query(None, description="Filter products by brand/SKU (e.g. KADAX)"),
    only_missing: bool = Query(False, description="Skip parent ASINs already in global_family"),
    _user=Depends(require_ops),
):
    status = get_rebuild_status()
    if status["running"]:
        return TriggerResponse(status="already_running", result=status)

    async def _run():
        await rebuild_de_canonical(
            max_parents=max_parents,
            brand_filter=brand_filter,
            only_missing=only_missing,
        )

    background_tasks.add_task(_run)
    return TriggerResponse(status="started", result={"message": "Rebuild started in background"})


@router.get("/trigger/rebuild-de/status")
async def rebuild_de_status(_user=Depends(require_ops)):
    return get_rebuild_status()


# ---------------------------------------------------------------------------
# 9) POST /families/trigger/sync-mp — trigger marketplace sync
# ---------------------------------------------------------------------------
@router.post("/trigger/sync-mp", response_model=JobRunOut, status_code=202)
async def trigger_sync_mp(
    marketplace_ids: Optional[str] = Query(None, description="Comma-separated marketplace IDs"),
    family_ids: Optional[str] = Query(None, description="Comma-separated family IDs"),
    _user=Depends(require_ops),
):
    mp_list = marketplace_ids.split(",") if marketplace_ids else None
    fam_list = [int(x) for x in family_ids.split(",")] if family_ids else None
    return await run_in_threadpool(
        enqueue_job,
        job_type="family_sync_marketplace_listings",
        marketplace_id=None,
        trigger_source="manual",
        triggered_by=settings.DEFAULT_ACTOR,
        params={
            "marketplace_ids": mp_list or [],
            "family_ids": fam_list or [],
        },
    )


# ---------------------------------------------------------------------------
# 10) POST /families/trigger/matching — trigger matching engine
# ---------------------------------------------------------------------------
@router.post("/trigger/matching", response_model=JobRunOut, status_code=202)
async def trigger_matching(
    marketplace_ids: Optional[str] = Query(None),
    family_ids: Optional[str] = Query(None, description="Comma-separated family IDs"),
    _user=Depends(require_ops),
):
    mp_list = marketplace_ids.split(",") if marketplace_ids else None
    fam_list = [int(x) for x in family_ids.split(",")] if family_ids else None
    return await run_in_threadpool(
        enqueue_job,
        job_type="family_matching_pipeline",
        marketplace_id=None,
        trigger_source="manual",
        triggered_by=settings.DEFAULT_ACTOR,
        params={
            "marketplace_ids": mp_list or [],
            "family_ids": fam_list or [],
        },
    )


@router.post("/jobs/recompute-coverage", response_model=JobRunOut, status_code=202)
async def trigger_recompute_coverage(_user=Depends(require_ops)):
    return await run_in_threadpool(
        enqueue_job,
        job_type="family_recompute_coverage",
        marketplace_id=None,
        trigger_source="manual",
        triggered_by=settings.DEFAULT_ACTOR,
        params={},
    )


# ---------------------------------------------------------------------------
# 13) POST /families/fix-packages/generate — generate fix packages
# ---------------------------------------------------------------------------
@router.post("/fix-packages/generate", response_model=TriggerResponse)
async def generate_fix_packages(
    family_id: Optional[int] = Query(None),
    marketplace: Optional[str] = Query(None),
    _user=Depends(require_ops),
):
    result = await generate_fix_package(family_id=family_id, marketplace=marketplace)
    return TriggerResponse(status="ok", result=result)


# ---------------------------------------------------------------------------
# 14) POST /families/fix-packages/{pkg_id}/approve — approve package
# ---------------------------------------------------------------------------
@router.post("/fix-packages/{pkg_id}/approve")
async def approve_fix_package(
    pkg_id: int,
    body: ApproveRequest,
    _user=Depends(require_director),
):
    def _approve():
        conn = _connect()
        cur = conn.cursor()
        cur.execute("""
            UPDATE dbo.family_fix_package
            SET status = 'approved',
                approved_by = ?,
                approved_at = SYSUTCDATETIME()
            WHERE id = ? AND status IN ('draft', 'pending_approve')
        """, body.approved_by, pkg_id)
        conn.commit()
        if cur.rowcount == 0:
            conn.close()
            raise HTTPException(404, "Package not found or already approved")
        conn.close()
        return {"status": "approved", "id": pkg_id}

    return await run_in_threadpool(_approve)


# ---------------------------------------------------------------------------
# 15) POST /families/{family_id}/analyze-restructure — dry-run analysis
# ---------------------------------------------------------------------------
@router.post("/{family_id}/analyze-restructure")
async def analyze_restructure_endpoint(
    family_id: int,
    marketplace_id: str = Query(..., description="Target marketplace ID"),
    _user=Depends(require_analyst),
):
    """Analyse family structure on target MP vs DE canonical (dry-run)."""
    result = await analyze_restructure(family_id, marketplace_id)
    if "error" in result:
        raise HTTPException(400, detail=result["error"])
    return result


# ---------------------------------------------------------------------------
# 16) POST /families/{family_id}/analyze-restructure-all — all MPs
# ---------------------------------------------------------------------------
@router.post("/{family_id}/analyze-restructure-all")
async def analyze_restructure_all_endpoint(
    family_id: int,
    _user=Depends(require_analyst),
):
    """Analyse family structure vs DE canonical on ALL non-DE marketplaces."""
    return await analyze_restructure_all_marketplaces(family_id)


# ---------------------------------------------------------------------------
# 17) POST /families/{family_id}/execute-restructure — execute restructure
# ---------------------------------------------------------------------------
@router.post("/{family_id}/execute-restructure")
async def execute_restructure_endpoint(
    family_id: int,
    marketplace_id: str = Query(..., description="Target marketplace ID"),
    dry_run: bool = Query(False, description="Dry-run mode (no actual SP-API calls)"),
    _user=Depends(require_director),
):
    """Execute family restructure on target MP (PATCH children, optionally DELETE foreign parent)."""
    result = await execute_restructure(family_id, marketplace_id, dry_run=dry_run)
    if result.get("status") == "error":
        raise HTTPException(400, detail=result.get("error", "Execution failed"))
    return result


@router.post("/{family_id}/execute-restructure/start")
async def execute_restructure_start_endpoint(
    family_id: int,
    marketplace_id: str = Query(..., description="Target marketplace ID"),
    dry_run: bool = Query(False, description="Dry-run mode (no actual SP-API calls)"),
    _user=Depends(require_director),
):
    """Start restructure in background and return run_id for polling."""
    mp_code = MARKETPLACE_REGISTRY.get(marketplace_id, {}).get("code", marketplace_id)
    run_id = await run_in_threadpool(
        create_restructure_run,
        family_id,
        marketplace_id,
        mp_code,
        dry_run=dry_run,
    )

    async def _progress(done: int, total: int, message: str):
        pct = int(round((done / total) * 100)) if total > 0 else 0
        await run_in_threadpool(
            update_restructure_run_progress,
            run_id,
            progress_pct=pct,
            children_done=done,
            children_total=total,
            message=message,
        )

    async def _runner():
        try:
            result = await execute_restructure(
                family_id,
                marketplace_id,
                dry_run=dry_run,
                progress_hook=_progress,
            )
            result_status = str(result.get("status") or "completed")
            final_status = (
                result_status
                if result_status in {
                    "completed",
                    "completed_with_errors",
                    "already_aligned",
                    "nothing_to_do",
                    "no_data",
                }
                else "failed"
            )
            await run_in_threadpool(
                finish_restructure_run,
                run_id,
                status=final_status,
                result=result,
                error_message=result.get("error"),
            )
        except Exception as exc:
            await run_in_threadpool(
                finish_restructure_run,
                run_id,
                status="failed",
                result=None,
                error_message=str(exc),
            )

    asyncio.create_task(_runner())
    return {"status": "started", "run_id": run_id}


@router.get("/{family_id}/execute-restructure/status")
async def execute_restructure_status_endpoint(
    family_id: int,
    marketplace_id: str = Query(..., description="Target marketplace ID"),
    run_id: Optional[str] = Query(None, description="Specific run_id to fetch"),
    _user=Depends(require_director),
):
    """Get latest or specific execute-restructure run status."""
    status = await run_in_threadpool(
        get_restructure_run_status,
        family_id=family_id,
        marketplace_id=marketplace_id,
        run_id=run_id,
    )
    if not status:
        raise HTTPException(status_code=404, detail="No restructure run found")
    return status
