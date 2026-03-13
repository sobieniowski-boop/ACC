from __future__ import annotations

import json
from datetime import date
from typing import Any

from app.core.db_connection import connect_acc


def _connect():
    return connect_acc(autocommit=False, timeout=30)


def _normalize_token(value: Any) -> str:
    return str(value or "").strip().upper().replace(" ", "")


def _fetchall_dict(cur) -> list[dict[str, Any]]:
    columns = [col[0] for col in cur.description] if cur.description else []
    return [{columns[idx]: row[idx] for idx in range(len(columns))} for row in cur.fetchall()]


def _load_json(value: Any) -> dict[str, Any] | list[Any] | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def _build_unmatched_reasons(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not int(row.get("has_primary_link") or 0):
        reasons.append("missing_order_link")
    if not int(row.get("parcel_map_rows") or 0):
        reasons.append("missing_parcel_map")
    if not int(row.get("billing_line_rows") or 0):
        reasons.append("missing_billing_lines")
    if not row.get("cost_source"):
        reasons.append("missing_shipment_cost")
    elif bool(row.get("is_estimated")):
        reasons.append("estimated_only")
    return reasons


def get_dhl_cost_trace(
    *,
    shipment_number: str | None = None,
    tracking_number: str | None = None,
    amazon_order_id: str | None = None,
    limit_shipments: int = 20,
) -> dict[str, Any]:
    if not shipment_number and not tracking_number and not amazon_order_id:
        raise ValueError("Provide shipment_number, tracking_number, or amazon_order_id")

    conn = _connect()
    try:
        cur = conn.cursor()
        where = ["s.carrier = 'DHL'"]
        params: list[Any] = []
        if shipment_number:
            where.append("s.shipment_number = ?")
            params.append(shipment_number)
        if tracking_number:
            where.append("(s.tracking_number = ? OR s.piece_id = ?)")
            params.extend([tracking_number, tracking_number])
        if amazon_order_id:
            where.append(
                """
                EXISTS (
                    SELECT 1
                    FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
                    WHERE l.shipment_id = s.id
                      AND l.amazon_order_id = ?
                )
                """
            )
            params.append(amazon_order_id)
        sql = f"""
            SELECT TOP {int(limit_shipments)}
                CAST(s.id AS NVARCHAR(40)) AS shipment_id,
                s.shipment_number,
                s.tracking_number,
                s.piece_id,
                s.cedex_number,
                s.source_system,
                s.status_code,
                s.status_label,
                s.is_delivered,
                s.delivered_at,
                s.created_at_carrier,
                s.source_payload_json
            FROM dbo.acc_shipment s WITH (NOLOCK)
            WHERE {' AND '.join(where)}
            ORDER BY s.created_at_carrier DESC, s.last_sync_at DESC
        """
        cur.execute(sql, params)
        shipments = _fetchall_dict(cur)
        if not shipments:
            return {"count": 0, "items": []}

        shipment_ids = [item["shipment_id"] for item in shipments]
        shipment_numbers = [str(item.get("shipment_number") or "") for item in shipments if item.get("shipment_number")]
        tracking_values = {
            str(item.get("tracking_number") or "")
            for item in shipments
            if item.get("tracking_number")
        } | {
            str(item.get("piece_id") or "")
            for item in shipments
            if item.get("piece_id")
        }
        amazon_order_ids: set[str] = set()

        placeholders = ",".join("?" for _ in shipment_ids)
        cur.execute(
            f"""
            SELECT
                CAST(shipment_id AS NVARCHAR(40)) AS shipment_id,
                amazon_order_id,
                CAST(acc_order_id AS NVARCHAR(40)) AS acc_order_id,
                bl_order_id,
                link_method,
                link_confidence,
                is_primary
            FROM dbo.acc_shipment_order_link WITH (NOLOCK)
            WHERE CAST(shipment_id AS NVARCHAR(40)) IN ({placeholders})
            ORDER BY is_primary DESC, link_confidence DESC, amazon_order_id
            """,
            shipment_ids,
        )
        links_by_shipment: dict[str, list[dict[str, Any]]] = {}
        for row in _fetchall_dict(cur):
            links_by_shipment.setdefault(row["shipment_id"], []).append(row)
            if row.get("amazon_order_id"):
                amazon_order_ids.add(str(row["amazon_order_id"]))

        cur.execute(
            f"""
            SELECT
                CAST(shipment_id AS NVARCHAR(40)) AS shipment_id,
                cost_source,
                currency,
                CAST(net_amount AS FLOAT) AS net_amount,
                CAST(fuel_amount AS FLOAT) AS fuel_amount,
                CAST(toll_amount AS FLOAT) AS toll_amount,
                CAST(gross_amount AS FLOAT) AS gross_amount,
                invoice_number,
                invoice_date,
                billing_period,
                is_estimated,
                raw_payload_json,
                updated_at
            FROM dbo.acc_shipment_cost WITH (NOLOCK)
            WHERE CAST(shipment_id AS NVARCHAR(40)) IN ({placeholders})
            ORDER BY is_estimated ASC, updated_at DESC
            """,
            shipment_ids,
        )
        costs_by_shipment: dict[str, list[dict[str, Any]]] = {}
        for row in _fetchall_dict(cur):
            row["raw_payload"] = _load_json(row.pop("raw_payload_json", None))
            costs_by_shipment.setdefault(row["shipment_id"], []).append(row)

        parcel_placeholders = ",".join("?" for _ in shipment_numbers) if shipment_numbers else ""
        tracking_list = [value for value in tracking_values if value]
        tracking_placeholders = ",".join("?" for _ in tracking_list) if tracking_list else ""
        parcel_sql_parts: list[str] = []
        parcel_params: list[Any] = []
        if shipment_numbers:
            parcel_sql_parts.append(f"parcel_number_base IN ({parcel_placeholders})")
            parcel_params.extend(shipment_numbers)
        if tracking_list:
            parcel_sql_parts.append(f"jjd_number IN ({tracking_placeholders})")
            parcel_params.extend(tracking_list)

        parcel_map_by_shipment_number: dict[str, list[dict[str, Any]]] = {}
        parcel_map_by_tracking: dict[str, list[dict[str, Any]]] = {}
        if parcel_sql_parts:
            cur.execute(
                f"""
                SELECT
                    parcel_number,
                    parcel_number_base,
                    parcel_number_suffix,
                    jjd_number,
                    shipment_type,
                    ship_date,
                    delivery_date,
                    last_event_code,
                    last_event_at,
                    source_file
                FROM dbo.acc_dhl_parcel_map WITH (NOLOCK)
                WHERE {' OR '.join(parcel_sql_parts)}
                ORDER BY delivery_date DESC, last_event_at DESC
                """,
                parcel_params,
            )
            for row in _fetchall_dict(cur):
                base = _normalize_token(row.get("parcel_number_base"))
                if base:
                    parcel_map_by_shipment_number.setdefault(base, []).append(row)
                jjd = _normalize_token(row.get("jjd_number"))
                if jjd:
                    parcel_map_by_tracking.setdefault(jjd, []).append(row)

        billing_lines_by_parcel: dict[str, list[dict[str, Any]]] = {}
        if shipment_numbers:
            cur.execute(
                f"""
                SELECT
                    document_number,
                    parcel_number,
                    parcel_number_base,
                    parcel_number_suffix,
                    issue_date,
                    sales_date,
                    delivery_date,
                    product_code,
                    description,
                    CAST(net_amount AS FLOAT) AS net_amount,
                    CAST(base_fee AS FLOAT) AS base_fee,
                    CAST(base_discount AS FLOAT) AS base_discount,
                    CAST(fuel_road_fee AS FLOAT) AS fuel_road_fee,
                    source_file
                FROM dbo.acc_dhl_billing_line WITH (NOLOCK)
                WHERE parcel_number_base IN ({parcel_placeholders})
                ORDER BY issue_date DESC, document_number DESC
                """,
                shipment_numbers,
            )
            for row in _fetchall_dict(cur):
                key = _normalize_token(row.get("parcel_number_base"))
                billing_lines_by_parcel.setdefault(key, []).append(row)

        fact_by_order: dict[str, dict[str, Any]] = {}
        shadow_by_order: dict[str, dict[str, Any]] = {}
        if amazon_order_ids:
            order_list = sorted(amazon_order_ids)
            order_placeholders = ",".join("?" for _ in order_list)
            cur.execute(
                f"""
                SELECT
                    amazon_order_id,
                    CAST(total_logistics_pln AS FLOAT) AS total_logistics_pln,
                    shipments_count,
                    actual_shipments_count,
                    estimated_shipments_count,
                    calculated_at
                FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
                WHERE amazon_order_id IN ({order_placeholders})
                """,
                order_list,
            )
            fact_by_order = {str(row["amazon_order_id"]): row for row in _fetchall_dict(cur)}

            cur.execute(
                f"""
                SELECT
                    amazon_order_id,
                    CAST(legacy_logistics_pln AS FLOAT) AS legacy_logistics_pln,
                    CAST(shadow_logistics_pln AS FLOAT) AS shadow_logistics_pln,
                    CAST(delta_pln AS FLOAT) AS delta_pln,
                    CAST(delta_abs_pln AS FLOAT) AS delta_abs_pln,
                    comparison_status,
                    calculated_at
                FROM dbo.acc_order_logistics_shadow WITH (NOLOCK)
                WHERE amazon_order_id IN ({order_placeholders})
                """,
                order_list,
            )
            shadow_by_order = {str(row["amazon_order_id"]): row for row in _fetchall_dict(cur)}

        items: list[dict[str, Any]] = []
        for shipment in shipments:
            shipment_number_key = _normalize_token(shipment.get("shipment_number"))
            tracking_key = _normalize_token(shipment.get("tracking_number"))
            piece_key = _normalize_token(shipment.get("piece_id"))
            links = links_by_shipment.get(shipment["shipment_id"], [])
            primary_order_id = next(
                (str(item.get("amazon_order_id")) for item in links if item.get("is_primary")),
                None,
            )
            item = {
                **shipment,
                "source_payload": _load_json(shipment.pop("source_payload_json", None)),
                "links": links,
                "costs": costs_by_shipment.get(shipment["shipment_id"], []),
                "parcel_maps": (
                    parcel_map_by_shipment_number.get(shipment_number_key, [])
                    + parcel_map_by_tracking.get(tracking_key, [])
                    + parcel_map_by_tracking.get(piece_key, [])
                ),
                "billing_lines": billing_lines_by_parcel.get(shipment_number_key, []),
                "primary_amazon_order_id": primary_order_id,
                "fact": fact_by_order.get(primary_order_id or ""),
                "shadow": shadow_by_order.get(primary_order_id or ""),
            }
            items.append(item)
        return {"count": len(items), "items": items}
    finally:
        conn.close()


def list_unmatched_dhl_shipments(
    *,
    created_from: date | None = None,
    created_to: date | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    conn = _connect()
    try:
        cur = conn.cursor()
        where = ["s.carrier = 'DHL'"]
        params: list[Any] = []
        if created_from:
            where.append("CAST(ISNULL(s.created_at_carrier, s.first_seen_at) AS DATE) >= ?")
            params.append(created_from.isoformat())
        if created_to:
            where.append("CAST(ISNULL(s.created_at_carrier, s.first_seen_at) AS DATE) <= ?")
            params.append(created_to.isoformat())
        sql = f"""
WITH ranked_costs AS (
    SELECT
        CAST(c.shipment_id AS NVARCHAR(40)) AS shipment_id,
        c.cost_source,
        c.is_estimated,
        ROW_NUMBER() OVER (
            PARTITION BY c.shipment_id
            ORDER BY CASE WHEN c.is_estimated = 0 THEN 0 ELSE 1 END, c.updated_at DESC
        ) AS rn
    FROM dbo.acc_shipment_cost c WITH (NOLOCK)
),
link_stats AS (
    SELECT
        CAST(shipment_id AS NVARCHAR(40)) AS shipment_id,
        MAX(CASE WHEN is_primary = 1 THEN 1 ELSE 0 END) AS has_primary_link,
        COUNT(*) AS link_rows
    FROM dbo.acc_shipment_order_link WITH (NOLOCK)
    GROUP BY shipment_id
),
parcel_map_stats AS (
    SELECT parcel_number_base, COUNT(*) AS parcel_map_rows
    FROM dbo.acc_dhl_parcel_map WITH (NOLOCK)
    GROUP BY parcel_number_base
),
billing_line_stats AS (
    SELECT parcel_number_base, COUNT(*) AS billing_line_rows
    FROM dbo.acc_dhl_billing_line WITH (NOLOCK)
    GROUP BY parcel_number_base
)
SELECT TOP {int(limit)}
    CAST(s.id AS NVARCHAR(40)) AS shipment_id,
    s.shipment_number,
    s.tracking_number,
    s.piece_id,
    s.source_system,
    s.status_code,
    s.status_label,
    s.created_at_carrier,
    s.is_delivered,
    ISNULL(ls.has_primary_link, 0) AS has_primary_link,
    ISNULL(ls.link_rows, 0) AS link_rows,
    rc.cost_source,
    ISNULL(rc.is_estimated, 0) AS is_estimated,
    ISNULL(pm.parcel_map_rows, 0) AS parcel_map_rows,
    ISNULL(bl.billing_line_rows, 0) AS billing_line_rows
FROM dbo.acc_shipment s WITH (NOLOCK)
LEFT JOIN link_stats ls
  ON ls.shipment_id = CAST(s.id AS NVARCHAR(40))
LEFT JOIN ranked_costs rc
  ON rc.shipment_id = CAST(s.id AS NVARCHAR(40))
 AND rc.rn = 1
LEFT JOIN parcel_map_stats pm
  ON pm.parcel_number_base = s.shipment_number
LEFT JOIN billing_line_stats bl
  ON bl.parcel_number_base = s.shipment_number
WHERE {' AND '.join(where)}
  AND (
        ISNULL(ls.has_primary_link, 0) = 0
        OR rc.cost_source IS NULL
        OR ISNULL(rc.is_estimated, 0) = 1
        OR ISNULL(pm.parcel_map_rows, 0) = 0
        OR ISNULL(bl.billing_line_rows, 0) = 0
      )
ORDER BY ISNULL(s.created_at_carrier, s.first_seen_at) DESC, s.shipment_number DESC
        """
        cur.execute(sql, params)
        items = _fetchall_dict(cur)
        for item in items:
            item["reasons"] = _build_unmatched_reasons(item)
        return {"count": len(items), "items": items}
    finally:
        conn.close()


def get_dhl_shadow_diff_report(
    *,
    purchase_from: date | None = None,
    purchase_to: date | None = None,
    comparison_status: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    conn = _connect()
    try:
        cur = conn.cursor()
        where = ["o.fulfillment_channel = 'MFN'"]
        params: list[Any] = []
        if purchase_from:
            where.append("CAST(o.purchase_date AS DATE) >= ?")
            params.append(purchase_from.isoformat())
        if purchase_to:
            where.append("CAST(o.purchase_date AS DATE) <= ?")
            params.append(purchase_to.isoformat())
        if comparison_status:
            where.append("s.comparison_status = ?")
            params.append(comparison_status)
        sql = f"""
            SELECT TOP {int(limit)}
                s.amazon_order_id,
                CAST(o.id AS NVARCHAR(40)) AS acc_order_id,
                o.purchase_date,
                o.marketplace_id,
                CAST(s.legacy_logistics_pln AS FLOAT) AS legacy_logistics_pln,
                CAST(s.shadow_logistics_pln AS FLOAT) AS shadow_logistics_pln,
                CAST(s.delta_pln AS FLOAT) AS delta_pln,
                CAST(s.delta_abs_pln AS FLOAT) AS delta_abs_pln,
                s.shipments_count,
                s.actual_shipments_count,
                s.estimated_shipments_count,
                s.comparison_status,
                s.calculated_at
            FROM dbo.acc_order_logistics_shadow s WITH (NOLOCK)
            JOIN dbo.acc_order o WITH (NOLOCK)
              ON o.amazon_order_id = s.amazon_order_id
            WHERE {' AND '.join(where)}
            ORDER BY s.delta_abs_pln DESC, o.purchase_date DESC, s.amazon_order_id DESC
        """
        cur.execute(sql, params)
        items = _fetchall_dict(cur)
        return {"count": len(items), "items": items}
    finally:
        conn.close()
