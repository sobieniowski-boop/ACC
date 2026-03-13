"""V1 profit routes — now powered by the unified V2 profit engine.

All CM calculation uses the canonical CM1/CM2/NP formulas from profit_engine.py.
The pre-calculated fields in acc_order (written by legacy profit_service.py)
are no longer used.
"""
from __future__ import annotations

import csv
import io
import math
from datetime import date, timedelta
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from starlette.responses import Response

from app.core.config import MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc

_DEPRECATION_HEADERS = {
    "Deprecation": "true",
    "Sunset": "2026-05-01",
    "Link": '</api/v1/profit/v2/products>; rel="successor-version"',
}
from app.services.order_logistics_source import (
    profit_logistics_join_sql,
    profit_logistics_value_sql,
)

router = APIRouter(prefix="/profit", tags=["profit"])


# ---------------------------------------------------------------------------
# Helpers (shared V2 FX / revenue formulas)
# ---------------------------------------------------------------------------
def _fx_fallback_sql(currency_col: str = "o.currency") -> str:
    from app.core.fx_service import build_fx_case_sql
    return f"ISNULL(fx.rate_to_pln, {build_fx_case_sql(currency_col)})"

_FX_JOIN_SQL = """
    OUTER APPLY (
        SELECT TOP 1 rate_to_pln
        FROM dbo.acc_exchange_rate er WITH (NOLOCK)
        WHERE er.currency = o.currency
          AND er.rate_date <= o.purchase_date
        ORDER BY er.rate_date DESC
    ) fx
"""

_OLT_JOIN_SQL = """
    OUTER APPLY (
        SELECT
            ISNULL(SUM(ISNULL(ol2.item_price, 0) - ISNULL(ol2.item_tax, 0) - ISNULL(ol2.promotion_discount, 0)), 0) AS order_line_total,
            ISNULL(SUM(ISNULL(ol2.quantity_ordered, 0)), 0) AS order_units_total
        FROM dbo.acc_order_line ol2 WITH (NOLOCK)
        WHERE ol2.order_id = o.id
    ) olt
"""


def _mkt_code(marketplace_id: str | None) -> str:
    if not marketplace_id:
        return ""
    return MARKETPLACE_REGISTRY.get(marketplace_id, {}).get("code", marketplace_id[-2:])


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _fetchall_dict(cur) -> list[dict[str, Any]]:
    cols = [c[0] for c in cur.description] if cur.description else []
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]


def _get_profit_orders_unified(
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
    sku: str | None = None,
    fulfillment_channel: str | None = None,
    min_cm_pct: float | None = None,
    max_cm_pct: float | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """Order-level profit using V2 engine formulas (netto revenue, on-the-fly CM1)."""
    conn = connect_acc(autocommit=False, timeout=20)
    try:
        cur = conn.cursor()
        logistics_join = profit_logistics_join_sql(order_alias="o", fact_alias="olf")
        logistics_val = profit_logistics_value_sql(order_alias="o", fact_alias="olf")

        # Revenue = SUM over lines: (item_price - item_tax - promotion_discount) * FX
        # NOTE: FX multiplied OUTSIDE SUM to avoid SQL Server error 8124
        # (outer reference + inner columns in same aggregate).
        # All lines in an order share the same currency → safe to factor out.
        fx_expr = _fx_fallback_sql()
        rev_sql = f"""
            ISNULL((
                SELECT SUM(
                    ISNULL(ol_r.item_price, 0) - ISNULL(ol_r.item_tax, 0)
                     - ISNULL(ol_r.promotion_discount, 0)
                )
                FROM dbo.acc_order_line ol_r WITH (NOLOCK) WHERE ol_r.order_id = o.id
            ) * {fx_expr}, 0)
        """
        cogs_sql = """
            ISNULL((
                SELECT SUM(ISNULL(ol_c.cogs_pln, 0))
                FROM dbo.acc_order_line ol_c WITH (NOLOCK) WHERE ol_c.order_id = o.id
            ), 0)
        """
        fees_sql = """
            ISNULL((
                SELECT SUM(ISNULL(ol_f.fba_fee_pln, 0) + ISNULL(ol_f.referral_fee_pln, 0))
                FROM dbo.acc_order_line ol_f WITH (NOLOCK) WHERE ol_f.order_id = o.id
            ), 0)
            + ISNULL(o.shipping_surcharge_pln, 0)
            + ISNULL(o.promo_order_fee_pln, 0)
            + ISNULL(o.refund_commission_pln, 0)
        """
        cm_sql = f"({rev_sql} - {cogs_sql} - {fees_sql} - {logistics_val})"
        cm_pct_sql = f"CASE WHEN {rev_sql} > 0 THEN ({cm_sql}) / NULLIF({rev_sql}, 0) * 100.0 ELSE 0 END"

        wheres = [
            "o.purchase_date >= CAST(? AS DATE)",
            "o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))",
            "o.status = 'Shipped'",
            "ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'",
            "o.amazon_order_id NOT LIKE 'S02-%%'",
        ]
        params: list[Any] = [date_from.isoformat(), date_to.isoformat()]

        if marketplace_id:
            wheres.append("o.marketplace_id = ?")
            params.append(marketplace_id)
        if sku:
            wheres.append("o.id IN (SELECT ol_s.order_id FROM dbo.acc_order_line ol_s WHERE ol_s.sku = ?)")
            params.append(sku)
        if fulfillment_channel:
            wheres.append("o.fulfillment_channel = ?")
            params.append(fulfillment_channel)
        if min_cm_pct is not None:
            wheres.append(f"({cm_pct_sql}) >= ?")
            params.append(min_cm_pct)
        if max_cm_pct is not None:
            wheres.append(f"({cm_pct_sql}) <= ?")
            params.append(max_cm_pct)

        where_sql = " AND ".join(wheres)

        cur.execute(
            f"SELECT COUNT(*) FROM dbo.acc_order o WITH (NOLOCK) {logistics_join} {_FX_JOIN_SQL} WHERE {where_sql}",
            params,
        )
        total = int(cur.fetchone()[0] or 0)

        offset = (max(1, page) - 1) * max(1, page_size)
        cur.execute(f"""
            SELECT
                CAST(o.id AS NVARCHAR(40)) AS id,
                o.amazon_order_id,
                o.marketplace_id,
                o.purchase_date,
                o.status,
                o.fulfillment_channel,
                o.order_total,
                o.currency,
                CAST({rev_sql} AS FLOAT) AS revenue_pln,
                CAST({cogs_sql} AS FLOAT) AS cogs_pln,
                CAST({fees_sql} AS FLOAT) AS amazon_fees_pln,
                CAST({logistics_val} AS FLOAT) AS logistics_pln,
                CAST({cm_sql} AS FLOAT) AS contribution_margin_pln,
                CAST({cm_pct_sql} AS FLOAT) AS cm_percent
            FROM dbo.acc_order o WITH (NOLOCK)
            {logistics_join}
            {_FX_JOIN_SQL}
            WHERE {where_sql}
            ORDER BY o.purchase_date DESC, o.amazon_order_id DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, (*params, offset, max(1, page_size)))
        order_rows = _fetchall_dict(cur)

        if not order_rows:
            return {"total": total, "page": page, "page_size": page_size,
                    "pages": math.ceil(total / max(1, page_size)) if total else 0, "items": []}

        order_ids = [r["id"] for r in order_rows]
        ph = ",".join(["?"] * len(order_ids))
        cur.execute(f"""
            SELECT
                CAST(ol.order_id AS NVARCHAR(40)) AS order_id,
                ol.sku, ol.asin, ol.title,
                p.title AS title_pl,
                ISNULL(ol.quantity_ordered, 0) AS quantity,
                ol.item_price, ol.currency,
                ISNULL(ol.purchase_price_pln, 0) AS purchase_price_pln,
                ISNULL(ol.cogs_pln, 0) AS cogs_pln,
                ISNULL(ol.fba_fee_pln, 0) AS fba_fee_pln,
                ISNULL(ol.referral_fee_pln, 0) AS referral_fee_pln
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            LEFT JOIN dbo.acc_product p WITH (NOLOCK) ON p.id = ol.product_id
            WHERE CAST(ol.order_id AS NVARCHAR(40)) IN ({ph})
            ORDER BY ol.sku
        """, order_ids)
        line_rows = _fetchall_dict(cur)

        lines_by_order: dict[str, list[dict]] = {}
        for lr in line_rows:
            oid = lr.pop("order_id", "")
            lines_by_order.setdefault(oid, []).append(lr)

        from datetime import datetime, timezone
        items = []
        for row in order_rows:
            oid = row["id"]
            pd = row.get("purchase_date")
            if isinstance(pd, str):
                try:
                    pd = datetime.fromisoformat(pd)
                except Exception:
                    pd = datetime.now(timezone.utc)
            elif not isinstance(pd, datetime):
                pd = datetime.now(timezone.utc)
            items.append({
                "id": oid,
                "amazon_order_id": row.get("amazon_order_id") or oid,
                "marketplace_id": row.get("marketplace_id") or "",
                "marketplace_code": _mkt_code(row.get("marketplace_id")),
                "purchase_date": pd,
                "status": row.get("status") or "Unknown",
                "fulfillment_channel": row.get("fulfillment_channel") or "Amazon",
                "order_total": _f(row.get("order_total")),
                "currency": row.get("currency") or "PLN",
                "revenue_pln": round(_f(row.get("revenue_pln")), 2),
                "cogs_pln": round(_f(row.get("cogs_pln")), 2),
                "amazon_fees_pln": round(_f(row.get("amazon_fees_pln")), 2),
                "ads_cost_pln": 0.0,
                "logistics_pln": round(_f(row.get("logistics_pln")), 2),
                "contribution_margin_pln": round(_f(row.get("contribution_margin_pln")), 2),
                "cm_percent": round(_f(row.get("cm_percent")), 2),
                "lines": lines_by_order.get(oid, []),
            })

        return {
            "total": total,
            "page": max(1, page),
            "page_size": max(1, page_size),
            "pages": math.ceil(total / max(1, page_size)) if total else 0,
            "items": items,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/orders", deprecated=True)
async def profit_orders(
    response: Response,
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    sku: Optional[str] = Query(default=None),
    fulfillment_channel: Optional[str] = Query(default=None),
    min_cm_pct: Optional[float] = Query(default=None),
    max_cm_pct: Optional[float] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    for k, v in _DEPRECATION_HEADERS.items():
        response.headers[k] = v
    end = date_to or date.today()
    start = date_from or (end - timedelta(days=29))
    try:
        return await run_in_threadpool(
            _get_profit_orders_unified,
            date_from=start,
            date_to=end,
            marketplace_id=marketplace_id,
            sku=sku,
            fulfillment_channel=fulfillment_channel,
            min_cm_pct=min_cm_pct,
            max_cm_pct=max_cm_pct,
            page=page,
            page_size=page_size,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Profit query failed: {exc}") from exc


@router.get("/by-sku", deprecated=True)
async def profit_by_sku(
    response: Response,
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
):
    """SKU-level profit — delegates to V2 product profit table.

    .. deprecated:: Use GET /profit/v2/products instead.
    """
    for k, v in _DEPRECATION_HEADERS.items():
        response.headers[k] = v
    from app.services.profit_engine import get_product_profit_table

    end = date_to or date.today()
    start = date_from or (end - timedelta(days=29))
    try:
        v2 = await run_in_threadpool(
            get_product_profit_table,
            date_from=start,
            date_to=end,
            marketplace_id=marketplace_id,
            group_by="sku",
            page=1,
            page_size=5000,
        )
        items = []
        for p in v2.get("items", []):
            rev = _f(p.get("revenue_pln"))
            cogs = _f(p.get("cogs_pln"))
            fees = _f(p.get("amazon_fees_pln"))
            logistics = _f(p.get("logistics_pln"))
            cm = _f(p.get("cm1_profit"))
            items.append({
                "sku": p.get("sku") or "",
                "asin": p.get("asin"),
                "title": p.get("title"),
                "units": int(p.get("units") or 0),
                "revenue_pln": round(rev, 2),
                "cogs_pln": round(cogs, 2),
                "amazon_fees_pln": round(fees, 2),
                "logistics_pln": round(logistics, 2),
                "contribution_margin_pln": round(cm, 2),
                "cm_percent": round(_f(p.get("cm1_percent")), 2),
                "orders": int(p.get("order_count") or 0),
            })
        return {
            "date_from": start,
            "date_to": end,
            "marketplace_id": marketplace_id,
            "total_skus": len(items),
            "items": items,
        }
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Profit by SKU failed: {exc}") from exc


@router.get("/export", deprecated=True)
async def profit_export_csv(
    response: Response,
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    marketplace_id: Optional[str] = Query(default=None),
    sku: Optional[str] = Query(default=None),
    fulfillment_channel: Optional[str] = Query(default=None),
    min_cm_pct: Optional[float] = Query(default=None),
    max_cm_pct: Optional[float] = Query(default=None),
):
    """Export profit orders as CSV download.

    .. deprecated:: Use GET /profit/v2/products/export.xlsx instead.
    """
    for k, v in _DEPRECATION_HEADERS.items():
        response.headers[k] = v
    end = date_to or date.today()
    start = date_from or (end - timedelta(days=29))

    try:
        result = await run_in_threadpool(
            _get_profit_orders_unified,
            date_from=start,
            date_to=end,
            marketplace_id=marketplace_id,
            sku=sku,
            fulfillment_channel=fulfillment_channel,
            min_cm_pct=min_cm_pct,
            max_cm_pct=max_cm_pct,
            page=1,
            page_size=10000,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Export failed: {exc}") from exc

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")

    writer.writerow([
        "Order ID", "Marketplace", "Date", "Status", "Fulfillment",
        "Revenue PLN", "COGS PLN", "Amazon Fees PLN", "Logistics PLN",
        "CM1 PLN", "CM1 %", "Currency", "Order Total",
    ])

    for order in result.get("items", []):
        writer.writerow([
            order.get("amazon_order_id", ""),
            order.get("marketplace_code", ""),
            order.get("purchase_date", ""),
            order.get("status", ""),
            order.get("fulfillment_channel", ""),
            order.get("revenue_pln", ""),
            order.get("cogs_pln", ""),
            order.get("amazon_fees_pln", ""),
            order.get("logistics_pln", ""),
            order.get("contribution_margin_pln", ""),
            order.get("cm_percent", ""),
            order.get("currency", ""),
            order.get("order_total", ""),
        ])

    output.seek(0)
    filename = f"profit_orders_{start}_{end}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
