"""Profit Engine v2 API — CM1/CM2/NP profit analysis endpoints."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse

from app.connectors.mssql import enqueue_job
from app.core.config import settings
from app.core.security import require_analyst
from app.schemas.jobs import JobRunOut
from app.schemas.profit_v2 import (
    ProductProfitTableResponse,
    ProductWhatIfResponse,
    ProductDrilldownResponse,
    LossOrdersResponse,
    FeeBreakdownResponse,
    DataQualityResponse,
    FeeGapDiagnosticsResponse,
    FeeGapWatchSeedResponse,
    FeeGapRecheckResponse,
    ProfitKPIResponse,
    ProductTaskCreate,
    ProductTaskItem,
    ProductTaskListResponse,
    ProductTaskUpdate,
    ProductTaskCommentCreate,
    ProductTaskCommentItem,
    TaskOwnerRuleCreate,
    TaskOwnerRuleItem,
    PurchasePriceUpsertRequest,
    PurchasePriceUpsertResponse,
    MapAndPriceRequest,
    MapAndPriceResponse,
    AIMatchRunResponse,
    AIMatchSuggestionsResponse,
    AIMatchActionResponse,
)
from app.schemas.profitability import (
    MarketplaceProfitabilityResponse,
    PriceSimulatorRequest,
    PriceSimulatorResult,
    ProfitabilityOrdersResponse,
    ProfitabilityOverviewResponse,
    ProfitabilityProductsResponse,
    RollupJobResult,
)

router = APIRouter(prefix="/profit/v2", tags=["profit"])


@router.get("/products", response_model=ProductProfitTableResponse)
async def product_profit_table(
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    brand: Optional[str] = Query(default=None),
    sku_search: Optional[str] = Query(default=None, max_length=100),
    fulfillment: Optional[str] = Query(default=None),
    parent_asin: Optional[str] = Query(default=None, max_length=20),
    profit_mode: str = Query(default="cm1", pattern="^(cm1|cm2|np)$"),
    include_cost_components: bool = Query(default=False),
    only_loss: bool = Query(default=False),
    only_low_confidence: bool = Query(default=False),
    sort_by: str = Query(default="cm1_profit", pattern="^[a-z0-9_]{1,40}$"),
    sort_dir: str = Query(default="desc", pattern="^(asc|desc)$"),
    group_by: str = Query(default="asin_marketplace", pattern="^[a-z_]{1,30}$"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    use_rollup: bool = Query(default=True, description="Use pre-computed rollup (fast, may be stale) instead of live order data"),
):
    """Product-level profit aggregation — ASIN-first with optional parent rollup.

    When use_rollup=True (default), reads from acc_sku_profitability_rollup (fast,
    refreshed daily at 05:45). Set use_rollup=False for live data from acc_order + acc_order_line.
    """
    from app.services.profit_engine import get_product_profit_table
    from app.services.return_tracker import get_return_cogs_adjustments

    end = date_to or date.today()
    start = date_from or (end - timedelta(days=29))

    # ── Phase 4: rollup path — delegate to profitability_service ──
    # Returns ProfitabilityProductsResponse shape (NOT ProductProfitTableResponse),
    # so we use response_model_exclude_unset via JSONResponse to skip validation.
    if use_rollup:
        from app.services.profitability_service import get_profitability_products
        from starlette.responses import JSONResponse
        try:
            # Map live sort columns to rollup equivalents
            _SORT_LIVE_TO_ROLLUP = {
                "cm1_profit": "cm1_pln",
                "cm2_profit": "cm2_pln",
                "np_profit": "profit_pln",
                "ads_cost_pln": "ad_spend_pln",
            }
            rollup_sort = _SORT_LIVE_TO_ROLLUP.get(sort_by, sort_by)
            rollup_result = await run_in_threadpool(
                get_profitability_products,
                date_from=start,
                date_to=end,
                marketplace_id=marketplace_id,
                sku=sku_search,
                sort_by=rollup_sort,
                sort_dir=sort_dir,
                page=page,
                page_size=page_size,
            )
            rollup_result["data_source"] = "rollup"
            return JSONResponse(content=rollup_result)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Rollup query failed: {exc}") from exc

    try:
        result = await run_in_threadpool(
            get_product_profit_table,
            date_from=start,
            date_to=end,
            marketplace_id=marketplace_id,
            brand=brand,
            sku_search=sku_search,
            fulfillment=fulfillment,
            parent_asin=parent_asin,
            profit_mode=profit_mode,
            include_cost_components=include_cost_components,
            only_loss=only_loss,
            only_low_confidence=only_low_confidence,
            sort_by=sort_by,
            sort_dir=sort_dir,
            group_by=group_by,
            page=page,
            page_size=page_size,
        )
        if isinstance(result, dict):
            result["data_source"] = "live"

        # Enrich with return tracker COGS adjustments
        try:
            adjustments = await run_in_threadpool(
                get_return_cogs_adjustments,
                date_from=start,
                date_to=end,
                marketplace_id=marketplace_id,
            )
            if adjustments:
                tot_recovered = 0.0
                tot_writeoff = 0.0
                tot_pending = 0.0
                for item in result.get("items", []) if isinstance(result, dict) else getattr(result, "items", []):
                    sku = item.get("sku", "") if isinstance(item, dict) else getattr(item, "sku", "")
                    mkt = item.get("marketplace_id", "") if isinstance(item, dict) else getattr(item, "marketplace_id", "")
                    key = f"{sku}|{mkt}"
                    adj = adjustments.get(key)
                    if adj:
                        recovered = adj["cogs_recovered_pln"]
                        writeoff = adj["write_off_pln"]
                        pending = adj["pending_cogs_pln"]
                        cm1 = item.get("cm1_profit", 0) if isinstance(item, dict) else getattr(item, "cm1_profit", 0)
                        if isinstance(item, dict):
                            item["return_cogs_recovered_pln"] = recovered
                            item["return_cogs_write_off_pln"] = writeoff
                            item["return_cogs_pending_pln"] = pending
                            item["cm1_adjusted"] = round(cm1 + recovered, 2)
                        else:
                            item.return_cogs_recovered_pln = recovered
                            item.return_cogs_write_off_pln = writeoff
                            item.return_cogs_pending_pln = pending
                            item.cm1_adjusted = round(cm1 + recovered, 2)
                        tot_recovered += recovered
                        tot_writeoff += writeoff
                        tot_pending += pending

                # Update summary
                summary = result.get("summary", {}) if isinstance(result, dict) else getattr(result, "summary", None)
                if summary:
                    if isinstance(summary, dict):
                        summary["total_return_cogs_recovered_pln"] = round(tot_recovered, 2)
                        summary["total_return_cogs_write_off_pln"] = round(tot_writeoff, 2)
                        summary["total_return_cogs_pending_pln"] = round(tot_pending, 2)
                    else:
                        summary.total_return_cogs_recovered_pln = round(tot_recovered, 2)
                        summary.total_return_cogs_write_off_pln = round(tot_writeoff, 2)
                        summary.total_return_cogs_pending_pln = round(tot_pending, 2)
        except Exception as exc:
            import structlog
            structlog.get_logger(__name__).warning(
                "profit_v2.return_cogs_enrichment_failed", error=str(exc),
            )
            warnings = result.get("warnings", []) if isinstance(result, dict) else []
            warnings.append("Return COGS enrichment unavailable — cm1_adjusted values may be missing")
            if isinstance(result, dict):
                result["warnings"] = warnings

        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Product profit query failed: {exc}") from exc


@router.get("/what-if", response_model=ProductWhatIfResponse)
async def product_profit_what_if(
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    marketplace_ids: Optional[str] = Query(default=None),
    sku_search: Optional[str] = Query(default=None),
    fulfillment_channels: Optional[str] = Query(default=None),
    parent_asin: Optional[str] = Query(default=None),
    profit_mode: str = Query(default="cm1"),
    include_cost_components: bool = Query(default=False),
    quantity: int = Query(default=1, ge=1, le=200),
    include_shipping_charge: bool = Query(default=True),
    only_open: bool = Query(default=True),
    group_by: str = Query(default="offer"),
    sort_by: str = Query(default="cm2_profit"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    """What-if simulation for open offers (price+fees+shipping/logistics)."""
    from app.services.profit_engine import get_product_what_if_table

    end = date_to or date.today()
    start = date_from or (end - timedelta(days=89))
    try:
        return await run_in_threadpool(
            get_product_what_if_table,
            date_from=start,
            date_to=end,
            marketplace_id=marketplace_id,
            marketplace_ids=marketplace_ids,
            sku_search=sku_search,
            fulfillment_channels=fulfillment_channels,
            parent_asin=parent_asin,
            profit_mode=profit_mode,
            include_cost_components=include_cost_components,
            quantity=quantity,
            include_shipping_charge=include_shipping_charge,
            only_open=only_open,
            group_by=group_by,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            page_size=page_size,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"What-if query failed: {exc}") from exc


@router.get("/products/export.xlsx")
async def product_profit_export_xlsx(
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    brand: Optional[str] = Query(default=None),
    sku_search: Optional[str] = Query(default=None),
    fulfillment: Optional[str] = Query(default=None),
    profit_mode: str = Query(default="cm1"),
    sort_by: str = Query(default="cm1_profit"),
    sort_dir: str = Query(default="desc"),
    group_by: str = Query(default="asin_marketplace"),
    columns: Optional[str] = Query(default=None, description="Comma-separated column keys"),
):
    """Server-side XLSX export for product profit table."""
    from app.services.profit_engine import export_product_profit_xlsx

    end = date_to or date.today()
    start = date_from or (end - timedelta(days=29))
    column_list = [c.strip() for c in (columns or "").split(",") if c.strip()]
    try:
        file_bytes, filename = await run_in_threadpool(
            export_product_profit_xlsx,
            date_from=start,
            date_to=end,
            marketplace_id=marketplace_id,
            brand=brand,
            sku_search=sku_search,
            fulfillment=fulfillment,
            profit_mode=profit_mode,
            sort_by=sort_by,
            sort_dir=sort_dir,
            group_by=group_by,
            columns=column_list or None,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"XLSX export failed: {exc}") from exc

    return StreamingResponse(
        iter([file_bytes]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/drilldown", response_model=ProductDrilldownResponse)
async def product_drilldown(
    sku: str = Query(..., description="SKU to drill down into"),
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    """Order-line level detail for a specific SKU with CM1 waterfall."""
    from app.services.profit_engine import get_product_drilldown

    end = date_to or date.today()
    start = date_from or (end - timedelta(days=29))
    try:
        return await run_in_threadpool(
            get_product_drilldown,
            sku=sku,
            date_from=start,
            date_to=end,
            marketplace_id=marketplace_id,
            page=page,
            page_size=page_size,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Drilldown query failed: {exc}") from exc


@router.get("/loss-orders", response_model=LossOrdersResponse)
async def loss_orders(
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    sku_search: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    """Order lines with negative CM1 — loss-making transactions."""
    from app.services.profit_engine import get_loss_orders

    end = date_to or date.today()
    start = date_from or (end - timedelta(days=29))
    try:
        return await run_in_threadpool(
            get_loss_orders,
            date_from=start,
            date_to=end,
            marketplace_id=marketplace_id,
            sku_search=sku_search,
            page=page,
            page_size=page_size,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Loss orders query failed: {exc}") from exc


@router.get("/fee-breakdown", response_model=FeeBreakdownResponse)
async def fee_breakdown(
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    sku: Optional[str] = Query(default=None),
):
    """Granular P&L — every fee type from finance transactions, ~50+ lines."""
    from app.services.profit_engine import get_fee_breakdown

    end = date_to or date.today()
    start = date_from or (end - timedelta(days=29))
    try:
        return await run_in_threadpool(
            get_fee_breakdown,
            date_from=start,
            date_to=end,
            marketplace_id=marketplace_id,
            sku=sku,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Fee breakdown query failed: {exc}") from exc


@router.get("/data-quality", response_model=DataQualityResponse)
async def data_quality(
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
):
    """Data quality & coverage metrics — trust dashboard for CFO."""
    from app.services.profit_engine import get_data_quality

    end = date_to or date.today()
    start = date_from or (end - timedelta(days=29))
    try:
        return await run_in_threadpool(
            get_data_quality,
            date_from=start,
            date_to=end,
            marketplace_id=marketplace_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Data quality query failed: {exc}") from exc


@router.get("/fee-gap-diagnostics", response_model=FeeGapDiagnosticsResponse)
async def fee_gap_diagnostics(
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
):
    """Explain missing FBA/referral fee gaps by marketplace and root reason."""
    from app.services.profit_engine import get_fee_gap_diagnostics

    end = date_to or date.today()
    start = date_from or (end - timedelta(days=29))
    try:
        return await run_in_threadpool(
            get_fee_gap_diagnostics,
            date_from=start,
            date_to=end,
            marketplace_id=marketplace_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Fee-gap diagnostics failed: {exc}") from exc


@router.post("/fee-gap-watch/seed", response_model=FeeGapWatchSeedResponse)
async def fee_gap_watch_seed(
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
):
    """Seed/refresh watchlist of open fee gaps for follow-up checks against Amazon."""
    from app.services.profit_engine import seed_fee_gap_watch

    end = date_to or date.today()
    start = date_from or (end - timedelta(days=29))
    try:
        return await run_in_threadpool(
            seed_fee_gap_watch,
            date_from=start,
            date_to=end,
            marketplace_id=marketplace_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Fee-gap watch seed failed: {exc}") from exc


@router.post("/fee-gap-watch/recheck", response_model=FeeGapRecheckResponse)
async def fee_gap_watch_recheck(
    limit: int = Query(default=25, ge=1, le=500),
    marketplace_id: Optional[str] = Query(default=None),
):
    """Recheck tracked fee gaps by querying Amazon finances by order id."""
    from app.services.profit_engine import recheck_fee_gap_watch

    try:
        return await run_in_threadpool(
            recheck_fee_gap_watch,
            limit=limit,
            marketplace_id=marketplace_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Fee-gap watch recheck failed: {exc}") from exc


@router.post("/purchase-price", response_model=PurchasePriceUpsertResponse)
async def upsert_purchase_price(payload: PurchasePriceUpsertRequest):
    """Manually set purchase price for an internal_sku from Data Quality UI."""
    from app.services.profit_engine import upsert_purchase_price as _upsert

    if payload.netto_price_pln <= 0:
        raise HTTPException(status_code=400, detail="Price must be positive")
    if payload.netto_price_pln > 2000:
        raise HTTPException(status_code=400, detail=f"Price {payload.netto_price_pln} PLN exceeds safety cap (2000 PLN). Likely data error.")
    try:
        return await run_in_threadpool(
            _upsert,
            internal_sku=payload.internal_sku,
            netto_price_pln=payload.netto_price_pln,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Save price failed: {exc}") from exc


@router.post("/map-and-price", response_model=MapAndPriceResponse)
async def map_and_price_endpoint(payload: MapAndPriceRequest):
    """Map product SKU to internal_sku and set purchase price in one call."""
    from app.services.profit_engine import map_and_price as _map_and_price

    if payload.netto_price_pln <= 0:
        raise HTTPException(status_code=400, detail="Price must be positive")
    if payload.netto_price_pln > 2000:
        raise HTTPException(status_code=400, detail=f"Price {payload.netto_price_pln} PLN exceeds safety cap (2000 PLN). Likely data error.")
    if not payload.internal_sku.strip():
        raise HTTPException(status_code=400, detail="internal_sku cannot be empty")
    try:
        return await run_in_threadpool(
            _map_and_price,
            sku=payload.sku.strip(),
            internal_sku=payload.internal_sku.strip(),
            netto_price_pln=payload.netto_price_pln,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Map & price failed: {exc}") from exc


@router.get("/kpis", response_model=ProfitKPIResponse)
async def profit_kpis(
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
):
    """CM1-based executive KPIs with period-over-period comparison."""
    from app.services.profit_engine import get_profit_kpis

    end = date_to or date.today()
    start = date_from or (end - timedelta(days=29))
    try:
        return await run_in_threadpool(
            get_profit_kpis,
            date_from=start,
            date_to=end,
            marketplace_id=marketplace_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Profit KPIs query failed: {exc}") from exc


@router.post("/tasks", response_model=ProductTaskItem)
async def create_task(payload: ProductTaskCreate):
    """Create product task from product-profit actions."""
    from app.services.profit_engine import create_product_task

    try:
        return await run_in_threadpool(
            create_product_task,
            task_type=payload.task_type,
            sku=payload.sku,
            marketplace_id=payload.marketplace_id,
            title=payload.title,
            note=payload.note,
            owner=payload.owner,
            source_page=payload.source_page,
            payload_json=payload.payload_json,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Create task failed: {exc}") from exc


@router.get("/tasks", response_model=ProductTaskListResponse)
async def list_tasks(
    status: Optional[str] = Query(default=None),
    task_type: Optional[str] = Query(default=None),
    owner: Optional[str] = Query(default=None),
    sku_search: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    from app.services.profit_engine import list_product_tasks

    try:
        return await run_in_threadpool(
            list_product_tasks,
            status=status,
            task_type=task_type,
            owner=owner,
            sku_search=sku_search,
            page=page,
            page_size=page_size,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"List tasks failed: {exc}") from exc


@router.patch("/tasks/{task_id}", response_model=ProductTaskItem)
async def update_task(task_id: str, payload: ProductTaskUpdate):
    from app.services.profit_engine import update_product_task

    try:
        return await run_in_threadpool(
            update_product_task,
            task_id=task_id,
            status=payload.status,
            owner=payload.owner,
            title=payload.title,
            note=payload.note,
        )
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Update task failed: {exc}") from exc


@router.get("/tasks/{task_id}/comments", response_model=list[ProductTaskCommentItem])
async def list_task_comments(task_id: str):
    from app.services.profit_engine import list_product_task_comments

    try:
        return await run_in_threadpool(list_product_task_comments, task_id=task_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"List task comments failed: {exc}") from exc


@router.post("/tasks/{task_id}/comments", response_model=ProductTaskCommentItem)
async def add_task_comment(task_id: str, payload: ProductTaskCommentCreate):
    from app.services.profit_engine import add_product_task_comment

    try:
        return await run_in_threadpool(
            add_product_task_comment,
            task_id=task_id,
            comment=payload.comment,
            author=payload.author,
        )
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Add comment failed: {exc}") from exc


@router.get("/tasks/owner-rules", response_model=list[TaskOwnerRuleItem])
async def list_owner_rules():
    from app.services.profit_engine import list_task_owner_rules

    try:
        return await run_in_threadpool(list_task_owner_rules)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"List owner rules failed: {exc}") from exc


@router.post("/tasks/owner-rules", response_model=TaskOwnerRuleItem)
async def create_owner_rule(payload: TaskOwnerRuleCreate):
    from app.services.profit_engine import create_task_owner_rule

    try:
        return await run_in_threadpool(
            create_task_owner_rule,
            owner=payload.owner,
            priority=payload.priority,
            task_type=payload.task_type,
            marketplace_id=payload.marketplace_id,
            brand=payload.brand,
            is_active=payload.is_active,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Create owner rule failed: {exc}") from exc


@router.delete("/tasks/owner-rules/{rule_id}", status_code=204)
async def delete_owner_rule(rule_id: int):
    from app.services.profit_engine import delete_task_owner_rule

    try:
        ok = await run_in_threadpool(delete_task_owner_rule, rule_id=rule_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Owner rule not found")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Delete owner rule failed: {exc}") from exc


# ---------------------------------------------------------------------------
# AI Product Match Suggestions
# ---------------------------------------------------------------------------

@router.post("/ai-match/run", response_model=JobRunOut, status_code=202)
async def run_ai_matching():
    """Trigger AI matching pipeline — generates suggestions, does NOT apply them."""
    try:
        return await run_in_threadpool(
            enqueue_job,
            job_type="profit_ai_match_run",
            marketplace_id=None,
            trigger_source="manual",
            triggered_by=settings.DEFAULT_ACTOR,
            params={},
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"AI matching failed: {exc}") from exc


@router.get("/ai-match/suggestions", response_model=AIMatchSuggestionsResponse)
async def list_ai_suggestions(
    status: str = Query(default="pending"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    """List AI match suggestions — pending/approved/rejected."""
    from app.services.ai_product_matcher import get_match_suggestions

    try:
        return await run_in_threadpool(
            get_match_suggestions,
            status=status,
            page=page,
            page_size=page_size,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Get suggestions failed: {exc}") from exc


@router.post("/ai-match/{suggestion_id}/approve", response_model=AIMatchActionResponse)
async def approve_ai_suggestion(suggestion_id: int):
    """Approve a suggestion — maps product + sets price."""
    from app.services.ai_product_matcher import approve_suggestion

    try:
        return await run_in_threadpool(approve_suggestion, suggestion_id)
    except ValueError as exc:
        msg = str(exc)
        code = 404 if "not found" in msg else 400
        raise HTTPException(status_code=code, detail=msg) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Approve failed: {exc}") from exc


@router.post("/ai-match/{suggestion_id}/reject", response_model=AIMatchActionResponse)
async def reject_ai_suggestion(suggestion_id: int):
    """Reject a suggestion — no changes applied."""
    from app.services.ai_product_matcher import reject_suggestion

    try:
        return await run_in_threadpool(reject_suggestion, suggestion_id)
    except ValueError as exc:
        msg = str(exc)
        code = 404 if "not found" in msg else 400
        raise HTTPException(status_code=code, detail=msg) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Reject failed: {exc}") from exc


# ---------------------------------------------------------------------------
# Profitability endpoints (migrated from /profitability/* to /profit/v2/*)
# ---------------------------------------------------------------------------

@router.get(
    "/overview",
    response_model=ProfitabilityOverviewResponse,
    dependencies=[Depends(require_analyst)],
)
async def profitability_overview(
    date_from: date = Query(default=None, alias="from"),
    date_to: date = Query(default=None, alias="to"),
    marketplace_id: Optional[str] = Query(default=None),
):
    """Profitability dashboard: KPIs, best/worst SKUs, loss orders, data freshness."""
    from app.services.profitability_service import get_profitability_overview

    if date_from is None:
        date_from = date.today() - timedelta(days=30)
    if date_to is None:
        date_to = date.today()
    try:
        return await run_in_threadpool(
            get_profitability_overview,
            date_from=date_from,
            date_to=date_to,
            marketplace_id=marketplace_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/orders",
    response_model=ProfitabilityOrdersResponse,
    dependencies=[Depends(require_analyst)],
)
async def profitability_orders(
    date_from: date = Query(..., alias="from"),
    date_to: date = Query(..., alias="to"),
    marketplace_id: Optional[str] = Query(default=None),
    sku: Optional[str] = Query(default=None),
    loss_only: bool = Query(default=False),
    min_margin: Optional[float] = Query(default=None),
    max_margin: Optional[float] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    """NP-level order table with loss / margin filtering."""
    from app.services.profitability_service import get_profitability_orders

    try:
        return await run_in_threadpool(
            get_profitability_orders,
            date_from=date_from,
            date_to=date_to,
            marketplace_id=marketplace_id,
            sku=sku,
            loss_only=loss_only,
            min_margin=min_margin,
            max_margin=max_margin,
            page=page,
            page_size=page_size,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/order-detail",
    dependencies=[Depends(require_analyst)],
)
async def order_detail(
    order_id: str = Query(..., description="Amazon order ID"),
):
    """Return order lines for a specific amazon_order_id."""
    from app.intelligence.profit.rollup import get_order_lines

    try:
        return await run_in_threadpool(get_order_lines, amazon_order_id=order_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/sku-rollup",
    response_model=ProfitabilityProductsResponse,
    dependencies=[Depends(require_analyst)],
)
async def profitability_sku_rollup(
    date_from: date = Query(..., alias="from"),
    date_to: date = Query(..., alias="to"),
    marketplace_id: Optional[str] = Query(default=None),
    sku: Optional[str] = Query(default=None),
    sort_by: str = Query(default="profit_pln"),
    sort_dir: str = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    """SKU-level profitability rollup (from acc_sku_profitability_rollup)."""
    from app.services.profitability_service import get_profitability_products

    try:
        return await run_in_threadpool(
            get_profitability_products,
            date_from=date_from,
            date_to=date_to,
            marketplace_id=marketplace_id,
            sku=sku,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            page_size=page_size,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get(
    "/marketplace-rollup",
    response_model=MarketplaceProfitabilityResponse,
    dependencies=[Depends(require_analyst)],
)
async def profitability_marketplace_rollup(
    date_from: date = Query(..., alias="from"),
    date_to: date = Query(..., alias="to"),
):
    """Profitability breakdown by marketplace."""
    from app.services.profitability_service import get_marketplace_profitability

    try:
        items = await run_in_threadpool(
            get_marketplace_profitability,
            date_from=date_from,
            date_to=date_to,
        )
        return {"items": items}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post(
    "/simulate",
    response_model=PriceSimulatorResult,
    dependencies=[Depends(require_analyst)],
)
async def profitability_simulate(payload: PriceSimulatorRequest):
    """Price what-if simulator — estimate margins for a given price point."""
    from app.services.profitability_service import simulate_price

    try:
        return simulate_price(
            sale_price=payload.sale_price,
            purchase_cost=payload.purchase_cost,
            shipping_cost=payload.shipping_cost,
            amazon_fee_pct=payload.amazon_fee_pct,
            fba_fee=payload.fba_fee,
            ad_cost=payload.ad_cost,
            currency=payload.currency,
            fx_rate=payload.fx_rate,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post(
    "/recompute",
    response_model=RollupJobResult,
    dependencies=[Depends(require_analyst)],
)
async def profitability_recompute(
    days_back: int = Query(default=7, ge=1, le=365),
):
    """Trigger manual recompute of profitability rollup tables."""
    from app.services.profitability_service import recompute_rollups

    try:
        return await run_in_threadpool(recompute_rollups, days_back=days_back)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
