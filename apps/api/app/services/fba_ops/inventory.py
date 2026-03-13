from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any

from ._helpers import (
    _connect,
    _fba_cache_get,
    _fba_cache_set,
    _fetchall_dict,
    _get_defaults,
    _is_amzn_grade_sku,
    _latest_inventory_snapshot_date,
    _lookup_product_context,
    _marketplace_code,
    _to_float,
    _to_int,
    ensure_fba_schema,
)


def _load_inventory_rows(
    *,
    marketplace_id: str | None = None,
    sku_search: str | None = None,
    risk_type: str | None = None,
    days_cover_max: int | None = None,
) -> tuple[date | None, list[dict[str, Any]]]:
    conn = _connect()
    try:
        cur = conn.cursor()
        latest = _latest_inventory_snapshot_date(cur)
        if not latest:
            return None, []
        defaults = _get_defaults(cur)
        where = ["inv.snapshot_date = ?"]
        params: list[Any] = [latest]
        if marketplace_id:
            where.append("inv.marketplace_id = ?")
            params.append(marketplace_id)
        if sku_search:
            where.append("(inv.sku LIKE ? OR inv.asin LIKE ?)")
            params.extend([f"%{sku_search}%", f"%{sku_search}%"])

        sql = f"""
            WITH velocity AS (
                SELECT
                    o.marketplace_id,
                    ISNULL(ol.sku, p.sku) AS sku,
                    SUM(CASE
                        WHEN CAST(o.purchase_date AS DATE) >= DATEADD(day, -7, CAST(GETUTCDATE() AS DATE))
                        THEN CASE WHEN ISNULL(ol.quantity_shipped, 0) > 0 THEN ISNULL(ol.quantity_shipped, 0) ELSE ISNULL(ol.quantity_ordered, 0) END
                        ELSE 0
                    END) / 7.0 AS velocity_7d,
                    SUM(CASE
                        WHEN CAST(o.purchase_date AS DATE) >= DATEADD(day, -30, CAST(GETUTCDATE() AS DATE))
                        THEN CASE WHEN ISNULL(ol.quantity_shipped, 0) > 0 THEN ISNULL(ol.quantity_shipped, 0) ELSE ISNULL(ol.quantity_ordered, 0) END
                        ELSE 0
                    END) / 30.0 AS velocity_30d,
                    SUM(CASE
                        WHEN CAST(o.purchase_date AS DATE) >= DATEADD(day, -90, CAST(GETUTCDATE() AS DATE))
                        THEN ISNULL(ol.item_price, 0) * CASE WHEN ISNULL(ol.quantity_ordered, 0) > 0 THEN ISNULL(ol.quantity_ordered, 0) ELSE 1 END
                        ELSE 0
                    END) AS revenue_90d
                FROM dbo.acc_order_line ol WITH (NOLOCK)
                JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
                LEFT JOIN dbo.acc_product p WITH (NOLOCK) ON p.id = ol.product_id
                WHERE o.status = 'Shipped'
                  AND CAST(o.purchase_date AS DATE) >= DATEADD(day, -90, CAST(GETUTCDATE() AS DATE))
                GROUP BY o.marketplace_id, ISNULL(ol.sku, p.sku)
            )
            SELECT
                inv.marketplace_id,
                inv.sku,
                inv.asin,
                ISNULL(inv.on_hand, 0) AS on_hand,
                ISNULL(inv.inbound, 0) AS inbound,
                ISNULL(inv.reserved, 0) AS reserved,
                ISNULL(v.velocity_7d, 0) AS velocity_7d,
                ISNULL(v.velocity_30d, 0) AS velocity_30d,
                ISNULL(v.revenue_90d, 0) AS revenue_90d
            FROM dbo.acc_fba_inventory_snapshot inv WITH (NOLOCK)
            LEFT JOIN velocity v
                ON v.marketplace_id = inv.marketplace_id
               AND v.sku = inv.sku
            WHERE {" AND ".join(where)}
        """
        cur.execute(sql, params)
        base_rows = _fetchall_dict(cur)
        if not base_rows:
            return latest, []

        all_skus = {r["sku"] for r in base_rows if r.get("sku")}
        all_asins = {r["asin"] for r in base_rows if r.get("asin")}

        def _in_placeholders(items: set[str]) -> tuple[str, list[str]]:
            lst = list(items)
            return ", ".join("?" for _ in lst), lst

        prod_by_sku: dict[str, dict[str, Any]] = {}
        prod_by_asin: dict[str, dict[str, Any]] = {}
        if all_skus or all_asins:
            clauses, p_params = [], []  # type: ignore[var-annotated]
            if all_skus:
                ph, vals = _in_placeholders(all_skus)
                clauses.append(f"sku IN ({ph})")
                p_params.extend(vals)
            if all_asins:
                ph, vals = _in_placeholders(all_asins)
                clauses.append(f"asin IN ({ph})")
                p_params.extend(vals)
            cur.execute(
                f"SELECT sku, asin, title, brand, category, internal_sku, ean, parent_asin "
                f"FROM dbo.acc_product WITH (NOLOCK) WHERE {' OR '.join(clauses)}",
                p_params,
            )
            for pr in _fetchall_dict(cur):
                if pr.get("sku"):
                    prod_by_sku.setdefault(pr["sku"], pr)
                if pr.get("asin"):
                    prod_by_asin.setdefault(pr["asin"], pr)

        reg_by_sku: dict[str, dict[str, Any]] = {}
        reg_by_asin: dict[str, dict[str, Any]] = {}
        if all_skus or all_asins:
            clauses, r_params = [], []  # type: ignore[var-annotated]
            if all_skus:
                ph, vals = _in_placeholders(all_skus)
                clauses.append(f"merchant_sku IN ({ph})")
                r_params.extend(vals)
                clauses.append(f"merchant_sku_alt IN ({ph})")
                r_params.extend(vals)
            if all_asins:
                ph, vals = _in_placeholders(all_asins)
                clauses.append(f"asin IN ({ph})")
                r_params.extend(vals)
            cur.execute(
                f"SELECT merchant_sku, merchant_sku_alt, asin, internal_sku, ean, parent_asin, "
                f"brand, product_name, category_1, category_2 "
                f"FROM dbo.acc_amazon_listing_registry WITH (NOLOCK) WHERE {' OR '.join(clauses)}",
                r_params,
            )
            for rr in _fetchall_dict(cur):
                if rr.get("merchant_sku"):
                    reg_by_sku.setdefault(rr["merchant_sku"], rr)
                if rr.get("merchant_sku_alt"):
                    reg_by_sku.setdefault(rr["merchant_sku_alt"], rr)
                if rr.get("asin"):
                    reg_by_asin.setdefault(rr["asin"], rr)

        ipm_by_sku: dict[str, str] = {}
        lookup_skus = set()
        for r in base_rows:
            sku = r.get("sku") or ""
            p_info = prod_by_sku.get(sku) or prod_by_asin.get(r.get("asin") or "")
            rg_info = reg_by_sku.get(sku) or reg_by_asin.get(r.get("asin") or "")
            int_sku = (p_info or {}).get("internal_sku") or (rg_info or {}).get("internal_sku") or ""
            candidate = int_sku.strip() if int_sku else ""
            if not candidate:
                candidate = sku
            if candidate:
                lookup_skus.add(candidate)
            if sku.startswith("FBA_") and len(sku) > 4:
                lookup_skus.add(sku[4:])
        if lookup_skus:
            ph, vals = _in_placeholders(lookup_skus)
            cur.execute(
                f"SELECT sku, nazwa_pelna FROM dbo.acc_import_products WITH (NOLOCK) WHERE sku IN ({ph})",
                vals,
            )
            for ir in _fetchall_dict(cur):
                if ir.get("sku") and ir.get("nazwa_pelna"):
                    ipm_by_sku[ir["sku"]] = ir["nazwa_pelna"]

        title_by_sku: dict[str, str] = {}
        if all_skus:
            ph, vals = _in_placeholders(all_skus)
            cur.execute(
                f"SELECT sku, MAX(title) AS title FROM dbo.acc_order_line WITH (NOLOCK) "
                f"WHERE sku IN ({ph}) AND title IS NOT NULL AND LTRIM(RTRIM(title)) <> '' "
                f"GROUP BY sku",
                vals,
            )
            for tr in _fetchall_dict(cur):
                if tr.get("sku") and tr.get("title"):
                    title_by_sku[tr["sku"]] = tr["title"]

        items: list[dict[str, Any]] = []
        for row in base_rows:
            sku = row.get("sku") or ""
            asin = row.get("asin") or ""
            p_info = prod_by_sku.get(sku) or prod_by_asin.get(asin) or {}
            rg_info = reg_by_sku.get(sku) or reg_by_asin.get(asin) or {}

            int_sku_resolved = (p_info.get("internal_sku") or "").strip() or (rg_info.get("internal_sku") or "").strip() or sku
            fba_alt = sku[4:] if sku.startswith("FBA_") and len(sku) > 4 else ""
            title_preferred = (
                ipm_by_sku.get(int_sku_resolved)
                or ipm_by_sku.get(fba_alt)
                or rg_info.get("product_name")
                or p_info.get("title")
                or title_by_sku.get(sku)
            )
            brand = (p_info.get("brand") or "").strip() or rg_info.get("brand") or ""
            category = (p_info.get("category") or "").strip() or rg_info.get("category_1") or rg_info.get("category_2") or ""
            internal_sku = (p_info.get("internal_sku") or "").strip() or rg_info.get("internal_sku") or None
            ean = (p_info.get("ean") or "").strip() or rg_info.get("ean") or None
            parent_asin = (p_info.get("parent_asin") or "").strip() or rg_info.get("parent_asin") or None

            on_hand = _to_int(row.get("on_hand"))
            inbound = _to_int(row.get("inbound"))
            reserved = _to_int(row.get("reserved"))
            velocity_7d = round(_to_float(row.get("velocity_7d")), 2)
            velocity_30d = round(_to_float(row.get("velocity_30d")), 2)
            units_available = max(on_hand - reserved, 0)
            days_cover = round(units_available / velocity_30d, 1) if velocity_30d > 0 else None
            target_days = _to_int(defaults.get("target_days"), 45)
            stockout_risk = "critical" if days_cover is not None and days_cover < 7 else ("warning" if days_cover is not None and days_cover < 14 else "ok")
            overstock_risk = "high" if days_cover is not None and days_cover > 120 else ("warning" if days_cover is not None and days_cover > 90 else "ok")
            if risk_type == "stockout" and stockout_risk == "ok":
                continue
            if risk_type == "overstock" and overstock_risk == "ok":
                continue
            if days_cover_max is not None and days_cover is not None and days_cover > days_cover_max:
                continue
            items.append(
                {
                    "sku": sku,
                    "asin": row.get("asin"),
                    "title_preferred": title_preferred or None,
                    "marketplace_id": row.get("marketplace_id") or "",
                    "marketplace_code": _marketplace_code(row.get("marketplace_id")),
                    "brand": brand or None,
                    "category": category or None,
                    "internal_sku": internal_sku,
                    "ean": ean,
                    "parent_asin": parent_asin,
                    "on_hand": on_hand,
                    "inbound": inbound,
                    "reserved": reserved,
                    "units_available": units_available,
                    "velocity_7d": velocity_7d,
                    "velocity_30d": velocity_30d,
                    "days_cover": days_cover,
                    "target_days": target_days,
                    "stockout_risk": stockout_risk,
                    "overstock_risk": overstock_risk,
                    "aged_90_plus_units": 0,
                    "aged_90_plus_value_pln": 0.0,
                    "stranded_units": 0,
                    "stranded_value_pln": 0.0,
                    "last_restock_date": None,
                    "next_inbound_eta": None,
                    "revenue_90d": round(_to_float(row.get("revenue_90d")), 2),
                }
            )
        return latest, items
    finally:
        conn.close()


def get_overview() -> dict[str, Any]:
    from .inbound import get_inbound_shipments

    cached = _fba_cache_get("fba_overview")
    if cached is not None:
        return cached
    ensure_fba_schema()
    snapshot_date, inventory = _load_inventory_rows()
    inventory_for_stockout = [item for item in inventory if not _is_amzn_grade_sku(item.get("sku"))]
    inbound = get_inbound_shipments()
    aged = get_aged_items()
    stranded = get_stranded_items()
    top100 = sorted(inventory_for_stockout, key=lambda item: item.get("revenue_90d", 0), reverse=True)[:100]
    oos_top100 = round(sum(1 for item in top100 if item.get("days_cover") is not None and item["days_cover"] < 7) / max(len(top100), 1) * 100, 1) if top100 else 0.0
    metrics = [
        {"label": "OOS% Top100", "value": oos_top100, "unit": "%", "status": "warning" if oos_top100 >= 5 else "ok"},
        {"label": "Stockout Risk <7d", "value": sum(1 for item in inventory_for_stockout if item.get("days_cover") is not None and item["days_cover"] < 7), "status": "critical"},
        {"label": "Stockout Risk <14d", "value": sum(1 for item in inventory_for_stockout if item.get("days_cover") is not None and item["days_cover"] < 14), "status": "warning"},
        {"label": "Aged 90+ Value", "value": round(sum(item["aged_90_plus_value_pln"] for item in aged), 2), "unit": "PLN"},
        {"label": "Stranded Value", "value": round(sum(item["stranded_value_pln"] for item in stranded), 2), "unit": "PLN"},
        {"label": "Inbound Open", "value": sum(inbound.get("by_status", {}).values()), "status": "ok"},
    ]
    top_stockout = sorted(
        [item for item in inventory_for_stockout if item["stockout_risk"] != "ok"],
        key=lambda item: (item.get("days_cover") if item.get("days_cover") is not None else 9999, -item.get("revenue_90d", 0)),
    )[:20]
    top_aged = sorted(aged, key=lambda item: item.get("aged_90_plus_value_pln", 0), reverse=True)[:20]
    inbound_delays = [item for item in inbound.get("items", []) if item.get("days_in_status", 0) >= 7][:20]
    result = {
        "metrics": metrics,
        "top_stockout_risks": top_stockout,
        "top_aged_value_skus": top_aged,
        "inbound_delays": inbound_delays,
        "snapshot_date": snapshot_date,
    }
    _fba_cache_set("fba_overview", result, ttl=600)
    return result


def get_inventory(*, marketplace_id: str | None = None, sku_search: str | None = None, risk_type: str | None = None, days_cover_max: int | None = None) -> dict[str, Any]:
    ensure_fba_schema()
    snapshot_date, items = _load_inventory_rows(
        marketplace_id=marketplace_id,
        sku_search=sku_search,
        risk_type=risk_type,
        days_cover_max=days_cover_max,
    )
    items.sort(key=lambda item: (item.get("days_cover") if item.get("days_cover") is not None else 9999, -item.get("revenue_90d", 0)))
    return {"items": items, "total": len(items), "snapshot_date": snapshot_date}


def get_inventory_detail(*, sku: str, marketplace_id: str | None = None) -> dict[str, Any]:
    ensure_fba_schema()
    _, items = _load_inventory_rows(marketplace_id=marketplace_id, sku_search=sku)
    item = next((entry for entry in items if entry["sku"] == sku and (not marketplace_id or entry["marketplace_id"] == marketplace_id)), None)
    if not item:
        raise ValueError("inventory sku not found")
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 30
                inv.snapshot_date AS [date],
                SUM(ISNULL(inv.qty_fulfillable, 0)) AS on_hand,
                SUM(ISNULL(inv.qty_inbound, 0)) AS inbound,
                SUM(ISNULL(inv.qty_reserved, 0)) AS reserved
            FROM dbo.acc_inventory_snapshot inv WITH (NOLOCK)
            WHERE inv.sku = ?
              AND (? IS NULL OR inv.marketplace_id = ?)
            GROUP BY inv.snapshot_date
            ORDER BY inv.snapshot_date DESC
            """,
            (sku, marketplace_id, marketplace_id),
        )
        inventory_timeline = _fetchall_dict(cur)
        cur.execute(
            """
            SELECT TOP 30
                CAST(o.purchase_date AS DATE) AS [date],
                SUM(CASE WHEN ISNULL(ol.quantity_shipped, 0) > 0 THEN ISNULL(ol.quantity_shipped, 0) ELSE ISNULL(ol.quantity_ordered, 0) END) AS units_sold
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            WHERE ol.sku = ?
              AND o.status = 'Shipped'
              AND (? IS NULL OR o.marketplace_id = ?)
            GROUP BY CAST(o.purchase_date AS DATE)
            ORDER BY [date] DESC
            """,
            (sku, marketplace_id, marketplace_id),
        )
        sales_rows = _fetchall_dict(cur)
    finally:
        conn.close()
    sales_map = {row["date"]: _to_int(row["units_sold"]) for row in sales_rows}
    from datetime import datetime, timezone
    return {
        "item": item,
        "inventory_timeline": [
            {
                "date": row["date"],
                "on_hand": _to_int(row["on_hand"]),
                "inbound": _to_int(row["inbound"]),
                "reserved": _to_int(row["reserved"]),
                "units_sold": sales_map.get(row["date"], 0),
            }
            for row in reversed(inventory_timeline)
        ],
        "sales_timeline": [
            {"date": row["date"], "on_hand": 0, "inbound": 0, "reserved": 0, "units_sold": _to_int(row["units_sold"])}
            for row in reversed(sales_rows)
        ],
        "notes": [
            {
                "at": datetime.now(timezone.utc),
                "type": "info",
                "message": "Detailed inbound / aged / stranded data available from latest FBA inventory snapshot.",
            }
        ],
    }


def get_replenishment_suggestions(*, marketplace_id: str | None = None, sku_search: str | None = None) -> dict[str, Any]:
    ensure_fba_schema()
    _, items = _load_inventory_rows(marketplace_id=marketplace_id, sku_search=sku_search)
    conn = _connect()
    try:
        cur = conn.cursor()
        defaults = _get_defaults(cur)
        target_days = _to_int(defaults.get("target_days"), 45)
        safety_stock_days = _to_int(defaults.get("safety_stock_days"), 14)
        lead_time_days = _to_int(defaults.get("lead_time_days"), 21)
    finally:
        conn.close()
    suggestions: list[dict[str, Any]] = []
    for item in items:
        sku = str(item.get("sku") or "").strip()
        if _is_amzn_grade_sku(sku):
            continue
        velocity_30d = _to_float(item.get("velocity_30d"))
        days_cover = item.get("days_cover")
        if velocity_30d <= 0 or days_cover is None or days_cover >= target_days:
            continue
        suggested_qty = max(0, math.ceil((target_days + safety_stock_days - days_cover) * velocity_30d))
        if suggested_qty <= 0:
            continue
        exceptions: list[str] = []
        if item["brand"] is None:
            exceptions.append("missing_brand")
        if velocity_30d < 0.5:
            exceptions.append("low_velocity_confidence")
        urgency = "critical" if days_cover < 7 else ("high" if days_cover < 14 else "medium")
        suggestions.append(
            {
                "sku": sku,
                "asin": item.get("asin"),
                "title_preferred": item.get("title_preferred"),
                "marketplace_id": item["marketplace_id"],
                "marketplace_code": item["marketplace_code"],
                "brand": item.get("brand"),
                "category": item.get("category"),
                "current_days_cover": days_cover,
                "target_days_cover": target_days,
                "lead_time_days": lead_time_days,
                "safety_stock_days": safety_stock_days,
                "suggested_qty": suggested_qty,
                "suggested_ship_week": date.today() + timedelta(days=max(0, int(days_cover) - lead_time_days)),
                "urgency": urgency,
                "exceptions": exceptions,
            }
        )
    suggestions.sort(key=lambda item: ({"critical": 0, "high": 1, "medium": 2}.get(item["urgency"], 9), item["current_days_cover"]))
    return {"items": suggestions, "total": len(suggestions)}


def get_aged_items() -> list[dict[str, Any]]:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT inv.marketplace_id, inv.sku, inv.asin,
                   inv.aged_90_plus,
                   CAST(ISNULL(aged_90_plus, 0) * ISNULL(p.netto_purchase_price_pln, 0) AS DECIMAL(18,4)) AS aged_value,
                   p.title AS p_title
            FROM dbo.acc_fba_inventory_snapshot inv WITH (NOLOCK)
            LEFT JOIN dbo.acc_product p WITH (NOLOCK) ON p.sku = inv.sku
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM dbo.acc_fba_inventory_snapshot WITH (NOLOCK))
              AND ISNULL(aged_90_plus, 0) > 0
            ORDER BY aged_value DESC
            """
        )
        rows = _fetchall_dict(cur)
    finally:
        conn.close()
    return [
        {
            "sku": row["sku"],
            "asin": row.get("asin"),
            "internal_sku": None,
            "ean": None,
            "title_preferred": row.get("p_title") or None,
            "marketplace_id": row["marketplace_id"],
            "marketplace_code": _marketplace_code(row.get("marketplace_id")),
            "aged_90_plus_units": _to_int(row.get("aged_90_plus")),
            "aged_90_plus_value_pln": round(_to_float(row.get("aged_value")), 2),
            "storage_fee_impact_estimate_pln": round(_to_float(row.get("aged_value")) * 0.08, 2),
            "recommended_action": "Create removal order or run promotion to liquidate aged inventory.",
        }
        for row in rows
    ]


def get_stranded_items() -> list[dict[str, Any]]:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT inv.marketplace_id, inv.sku, inv.asin,
                   inv.stranded_units,
                   CAST(ISNULL(stranded_units, 0) * ISNULL(p.netto_purchase_price_pln, 0) AS DECIMAL(18,4)) AS stranded_value,
                   p.title AS p_title
            FROM dbo.acc_fba_inventory_snapshot inv WITH (NOLOCK)
            LEFT JOIN dbo.acc_product p WITH (NOLOCK) ON p.sku = inv.sku
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM dbo.acc_fba_inventory_snapshot WITH (NOLOCK))
              AND ISNULL(stranded_units, 0) > 0
            ORDER BY stranded_value DESC
            """
        )
        rows = _fetchall_dict(cur)
    finally:
        conn.close()
    return [
        {
            "sku": row["sku"],
            "asin": row.get("asin"),
            "internal_sku": None,
            "ean": None,
            "title_preferred": row.get("p_title") or None,
            "marketplace_id": row["marketplace_id"],
            "marketplace_code": _marketplace_code(row.get("marketplace_id")),
            "stranded_units": _to_int(row.get("stranded_units")),
            "stranded_value_pln": round(_to_float(row.get("stranded_value")), 2),
            "reason": None,
            "recommended_action": "Review listing status and fix compliance issues in Seller Central.",
        }
        for row in rows
    ]
