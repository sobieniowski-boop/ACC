from __future__ import annotations

import json
import uuid
from datetime import date
from typing import Any

import pyodbc

from ._helpers import (
    _connect,
    _fetchall_dict,
    _get_defaults,
    _is_amzn_grade_sku,
    _lookup_product_context,
    _marketplace_code,
    _to_float,
    _to_int,
    _truncate_text,
    ensure_fba_schema,
)
from .inventory import _load_inventory_rows


def _resolve_fba_task_owner(cur: pyodbc.Cursor, sku: str, marketplace_id: str | None) -> str | None:
    cur.execute("SELECT TOP 1 brand FROM dbo.acc_product WITH (NOLOCK) WHERE sku = ? AND brand IS NOT NULL", (sku,))
    brand_row = cur.fetchone()
    brand = str(brand_row[0]).strip() if brand_row and brand_row[0] else None
    cur.execute(
        """
        SELECT TOP 1 owner
        FROM dbo.acc_al_task_owner_rules WITH (NOLOCK)
        WHERE is_active = 1
          AND (task_type IS NULL OR task_type = 'fba_replenishment')
          AND (marketplace_id IS NULL OR marketplace_id = ?)
          AND (brand IS NULL OR brand = ?)
        ORDER BY priority ASC,
                 CASE WHEN brand IS NOT NULL THEN 0 ELSE 1 END,
                 CASE WHEN marketplace_id IS NOT NULL THEN 0 ELSE 1 END
        """,
        (marketplace_id, brand),
    )
    owner = cur.fetchone()
    return str(owner[0]).strip() if owner and owner[0] else None


def _ensure_alert_rule(cur: pyodbc.Cursor, *, rule_type: str, name: str, description: str, severity: str) -> str:
    cur.execute(
        """
        IF NOT EXISTS (
            SELECT 1 FROM dbo.acc_al_alert_rules WITH (NOLOCK)
            WHERE rule_type = ?
        )
        INSERT INTO dbo.acc_al_alert_rules
        (id, name, description, rule_type, severity, is_active, created_by, created_at)
        VALUES
        (NEWID(), ?, ?, ?, ?, 1, 'system', SYSUTCDATETIME())
        """,
        (rule_type, name, description, rule_type, severity),
    )
    cur.execute("SELECT TOP 1 CAST(id AS NVARCHAR(40)) FROM dbo.acc_al_alert_rules WITH (NOLOCK) WHERE rule_type = ?", (rule_type,))
    row = cur.fetchone()
    if not row or not row[0]:
        raise RuntimeError(f"alert rule not available: {rule_type}")
    return str(row[0])


def _alert_exists_recent(cur: pyodbc.Cursor, *, rule_id: str, sku: str | None, marketplace_id: str | None, title: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*)
        FROM dbo.acc_al_alerts WITH (NOLOCK)
        WHERE rule_id = CAST(? AS UNIQUEIDENTIFIER)
          AND ISNULL(sku, '') = ISNULL(?, '')
          AND ISNULL(marketplace_id, '') = ISNULL(?, '')
          AND title = ?
          AND is_resolved = 0
          AND triggered_at >= DATEADD(hour, -24, SYSUTCDATETIME())
        """,
        (rule_id, sku, marketplace_id, title),
    )
    return _to_int(cur.fetchone()[0]) > 0


def _insert_fba_alert(
    cur: pyodbc.Cursor,
    *,
    rule_id: str,
    marketplace_id: str | None,
    sku: str | None,
    title: str,
    detail: str,
    detail_json: dict[str, Any] | None,
    context_json: dict[str, Any] | None,
    severity: str,
    current_value: float | int | None,
) -> None:
    cur.execute(
        """
        INSERT INTO dbo.acc_al_alerts
        (id, rule_id, marketplace_id, sku, title, detail, detail_json, context_json, severity, current_value, is_read, is_resolved, triggered_at)
        VALUES
        (NEWID(), CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, SYSUTCDATETIME())
        """,
        (
            rule_id,
            marketplace_id,
            sku,
            title,
            detail,
            json.dumps(detail_json or {}, ensure_ascii=True),
            json.dumps(context_json or {}, ensure_ascii=True),
            severity,
            current_value,
        ),
    )


def _shipment_top_variance_lines(cur: pyodbc.Cursor, shipment_id: str, *, limit: int = 3) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT TOP (?)
            l.sku,
            l.asin,
            SUM(ISNULL(l.qty_planned, 0)) AS qty_planned,
            SUM(ISNULL(l.qty_received, 0)) AS qty_received,
            SUM(ISNULL(l.qty_planned, 0) - ISNULL(l.qty_received, 0)) AS variance_units
        FROM dbo.acc_fba_inbound_shipment_line l WITH (NOLOCK)
        WHERE l.shipment_id = ?
        GROUP BY l.sku, l.asin
        HAVING SUM(ISNULL(l.qty_planned, 0) - ISNULL(l.qty_received, 0)) > 0
        ORDER BY SUM(ISNULL(l.qty_planned, 0) - ISNULL(l.qty_received, 0)) DESC, l.sku ASC
        """,
        (limit, shipment_id),
    )
    rows = _fetchall_dict(cur)
    enriched: list[dict[str, Any]] = []
    for row in rows:
        product = _lookup_product_context(cur, sku=row.get("sku"), asin=row.get("asin"))
        enriched.append(
            {
                **row,
                "title_preferred": product.get("title_preferred"),
                "brand": product.get("brand"),
            }
        )
    return enriched


def _format_variance_lines(lines: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for line in lines:
        label = _truncate_text(line.get("title_preferred") or line.get("sku") or "-", 48)
        variance_units = _to_int(line.get("variance_units"))
        parts.append(f"{line.get('sku')}: {label} (-{variance_units})")
    return "; ".join(parts)


def _top_inventory_issue_skus(cur: pyodbc.Cursor, *, snapshot_date: date | None, mode: str, limit: int = 3) -> list[str]:
    if not snapshot_date:
        return []
    if mode == "stranded":
        metric_sql = "CAST(ISNULL(inv.stranded_units, 0) * ISNULL(p.netto_purchase_price_pln, 0) AS DECIMAL(18,4))"
        where_sql = "ISNULL(inv.stranded_units, 0) > 0"
    else:
        metric_sql = "CAST((ISNULL(inv.aged_90_plus, 0) + ISNULL(inv.excess_units, 0)) * ISNULL(p.netto_purchase_price_pln, 0) AS DECIMAL(18,4))"
        where_sql = "(ISNULL(inv.aged_90_plus, 0) + ISNULL(inv.excess_units, 0)) > 0"
    cur.execute(
        f"""
        SELECT TOP ({limit})
            inv.sku,
            inv.asin,
            {metric_sql} AS metric_value
        FROM dbo.acc_fba_inventory_snapshot inv WITH (NOLOCK)
        LEFT JOIN dbo.acc_product p WITH (NOLOCK)
          ON p.sku = inv.sku OR (p.asin = inv.asin AND inv.asin IS NOT NULL)
        WHERE inv.snapshot_date = ?
          AND {where_sql}
        ORDER BY metric_value DESC, inv.sku ASC
        """,
        (snapshot_date,),
    )
    rows = _fetchall_dict(cur)
    labels: list[str] = []
    for row in rows:
        product = _lookup_product_context(cur, sku=row.get("sku"), asin=row.get("asin"))
        title = _truncate_text(product.get("title_preferred") or row.get("sku") or "-", 42)
        labels.append(f"{row.get('sku')}: {title}")
    return labels


def _ensure_fba_task(
    cur: pyodbc.Cursor,
    *,
    task_type: str,
    title: str,
    note: str,
    source_page: str,
    sku: str | None = None,
    marketplace_id: str | None = None,
    owner: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    cur.execute(
        """
        SELECT COUNT(*)
        FROM dbo.acc_al_product_tasks WITH (NOLOCK)
        WHERE task_type = ?
          AND ISNULL(sku, '') = ISNULL(?, '')
          AND ISNULL(marketplace_id, '') = ISNULL(?, '')
          AND title = ?
          AND status IN ('open', 'investigating')
        """,
        (task_type, sku, marketplace_id, title),
    )
    if _to_int(cur.fetchone()[0]) > 0:
        return
    task_sku = sku or f"__{task_type}__"
    cur.execute(
        """
        INSERT INTO dbo.acc_al_product_tasks
        (id, task_type, sku, marketplace_id, status, title, note, owner, source_page, payload_json, created_by, created_at, updated_at)
        VALUES
        (CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, 'open', ?, ?, ?, ?, ?, 'system', SYSUTCDATETIME(), SYSUTCDATETIME())
        """,
        (
            str(uuid.uuid4()),
            task_type,
            task_sku,
            marketplace_id,
            title,
            note,
            owner,
            source_page,
            json.dumps(payload or {}, ensure_ascii=True),
        ),
    )


# ──────────── Alert scan entry‑point ────────────


def run_alert_scan() -> int:
    ensure_fba_schema()
    snapshot_date, inventory = _load_inventory_rows()
    conn = _connect()
    try:
        cur = conn.cursor()
        created = 0
        if snapshot_date:
            stockout_inventory = [item for item in inventory if not _is_amzn_grade_sku(item.get("sku"))]
            top100 = sorted(stockout_inventory, key=lambda item: item.get("revenue_90d", 0), reverse=True)[:100]
            risky = [item for item in top100 if item.get("days_cover") is not None and item["days_cover"] < 7 and item["inbound"] == 0]
            rule_id = _ensure_alert_rule(
                cur,
                rule_type="fba_stockout_top_sku",
                name="FBA Top SKU Stockout Risk",
                description="Generated by FBA Ops alert scan",
                severity="critical",
            )
            for item in risky:
                product = _lookup_product_context(cur, sku=item["sku"], asin=item.get("asin"))
                product_label = _truncate_text(product.get("title_preferred") or item["sku"], 70)
                title = f"P1 Stockout Top SKU: {item['sku']} | {product_label}"
                if _alert_exists_recent(cur, rule_id=rule_id, sku=item["sku"], marketplace_id=item["marketplace_id"], title=title):
                    continue
                detail_parts = [
                    f"Produkt: {product_label}",
                    f"Marketplace: {_marketplace_code(item['marketplace_id'])} | SKU: {item['sku']}",
                    f"Why: days cover {item['days_cover']}, on hand {item['on_hand']}, inbound {item['inbound']}, vel.30d {item['velocity_30d']}",
                ]
                if product.get("brand") or product.get("category"):
                    detail_parts.append(f"Kontekst: brand {product.get('brand') or '-'} | category {product.get('category') or '-'}")
                detail_parts.append("Next step: sprawdz replenishment plan, lead time i czy da sie uruchomic inbound dzis.")
                detail = "\n".join(detail_parts)
                _insert_fba_alert(
                    cur,
                    rule_id=rule_id,
                    marketplace_id=item["marketplace_id"],
                    sku=item["sku"],
                    title=title,
                    detail=detail,
                    detail_json={
                        "product": product,
                        "marketplace_code": _marketplace_code(item["marketplace_id"]),
                        "metrics": {
                            "days_cover": item["days_cover"],
                            "on_hand": item["on_hand"],
                            "inbound": item["inbound"],
                            "velocity_30d": item["velocity_30d"],
                            "revenue_90d": item.get("revenue_90d"),
                        },
                    },
                    context_json={
                        "module": "fba_ops",
                        "entity_type": "sku",
                        "sku": item["sku"],
                        "marketplace_id": item["marketplace_id"],
                        "route": "/fba/inventory",
                        "query": {"sku_search": item["sku"], "marketplace_id": item["marketplace_id"]},
                        "source_page": "fba_overview",
                    },
                    severity="critical",
                    current_value=item["days_cover"] or 0,
                )
                _ensure_fba_task(
                    cur,
                    task_type="fba_replenishment",
                    title=title,
                    note=detail,
                    source_page="fba_overview",
                    sku=item["sku"],
                    marketplace_id=item["marketplace_id"],
                    owner=_resolve_fba_task_owner(cur, item["sku"], item["marketplace_id"]),
                    payload={"days_cover": item["days_cover"], "velocity_30d": item["velocity_30d"], "snapshot_date": snapshot_date.isoformat()},
                )
                created += 1

        defaults = _get_defaults(cur)
        inbound_stuck_days = _to_int(defaults.get("inbound_stuck_days"), 7)
        cur.execute(
            """
            SELECT shipment_id, marketplace_id, status, DATEDIFF(day, ISNULL(last_update_at, created_at), SYSUTCDATETIME()) AS days_in_status,
                   units_planned, units_received, owner
            FROM dbo.acc_fba_inbound_shipment WITH (NOLOCK)
            WHERE status IN ('WORKING','SHIPPED','IN_TRANSIT','DELIVERED','CHECKED_IN','RECEIVING')
            """
        )
        inbound_rule_id = _ensure_alert_rule(
            cur,
            rule_type="fba_inbound_stuck",
            name="FBA Inbound Stuck",
            description="Shipment stuck in the same status above threshold",
            severity="warning",
        )
        for row in _fetchall_dict(cur):
            days_in_status = _to_int(row.get("days_in_status"))
            if days_in_status < inbound_stuck_days:
                continue
            title = f"P2 Inbound Stuck: {row['shipment_id']}"
            if _alert_exists_recent(cur, rule_id=inbound_rule_id, sku=None, marketplace_id=row.get("marketplace_id"), title=title):
                continue
            top_lines = _shipment_top_variance_lines(cur, row["shipment_id"])
            detail_parts = [
                f"Shipment: {row['shipment_id']} | MP: {_marketplace_code(row.get('marketplace_id'))}",
                f"Why: status {row['status']}, days in status {days_in_status}, planned {_to_int(row.get('units_planned'))}, received {_to_int(row.get('units_received'))}",
            ]
            if top_lines:
                detail_parts.append(f"Najbardziej opoznione linie: {_format_variance_lines(top_lines)}")
            detail_parts.append("Next step: sprawdz appointment / receiving block / case log i eskaluj do Amazon jesli brak ruchu.")
            detail = "\n".join(detail_parts)
            _insert_fba_alert(
                cur,
                rule_id=inbound_rule_id,
                marketplace_id=row.get("marketplace_id"),
                sku=None,
                title=title,
                detail=detail,
                detail_json={
                    "shipment_id": row["shipment_id"],
                    "status": row["status"],
                    "days_in_status": days_in_status,
                    "units_planned": _to_int(row.get("units_planned")),
                    "units_received": _to_int(row.get("units_received")),
                    "top_lines": top_lines,
                },
                context_json={
                    "module": "fba_ops",
                    "entity_type": "shipment",
                    "shipment_id": row["shipment_id"],
                    "marketplace_id": row.get("marketplace_id"),
                    "route": "/fba/inbound",
                    "query": {"shipment_id": row["shipment_id"]},
                    "source_page": "fba_inbound",
                },
                severity="warning",
                current_value=days_in_status,
            )
            _ensure_fba_task(
                cur,
                task_type="fba_inbound",
                title=title,
                note=detail,
                source_page="fba_inbound",
                sku=None,
                marketplace_id=row.get("marketplace_id"),
                owner=row.get("owner"),
                payload={"shipment_id": row["shipment_id"], "status": row["status"], "days_in_status": days_in_status},
            )
            created += 1

        cur.execute(
            """
            SELECT shipment_id, marketplace_id, units_planned, units_received
            FROM dbo.acc_fba_inbound_shipment WITH (NOLOCK)
            WHERE status = 'CLOSED'
              AND units_planned > 0
              AND (CAST(units_received AS FLOAT) / NULLIF(CAST(units_planned AS FLOAT), 0)) < 0.95
            """
        )
        variance_rule_id = _ensure_alert_rule(
            cur,
            rule_type="fba_receiving_variance",
            name="FBA Receiving Variance",
            description="Closed shipment received below 95% of plan",
            severity="warning",
        )
        for row in _fetchall_dict(cur):
            top_lines = _shipment_top_variance_lines(cur, row["shipment_id"])
            shipment_label = row["shipment_id"]
            if top_lines:
                shipment_label = f"{shipment_label} | {_truncate_text(top_lines[0].get('title_preferred') or top_lines[0].get('sku'), 48)}"
            title = f"P2 Receiving Variance: {shipment_label}"
            if _alert_exists_recent(cur, rule_id=variance_rule_id, sku=None, marketplace_id=row.get("marketplace_id"), title=title):
                continue
            ratio = round(_to_int(row.get("units_received")) / max(_to_int(row.get("units_planned")), 1) * 100, 1)
            detail_parts = [
                f"Shipment: {row['shipment_id']} | MP: {_marketplace_code(row.get('marketplace_id'))}",
                f"Why: received {ratio}% | planned {_to_int(row.get('units_planned'))} | received {_to_int(row.get('units_received'))}",
            ]
            if top_lines:
                detail_parts.append(f"Top missing lines: {_format_variance_lines(top_lines)}")
            detail_parts.append("Next step: porownaj line items z shipment planem i otworz case reimbursement / discrepancy jesli shipment jest domkniety.")
            detail = "\n".join(detail_parts)
            _insert_fba_alert(
                cur,
                rule_id=variance_rule_id,
                marketplace_id=row.get("marketplace_id"),
                sku=None,
                title=title,
                detail=detail,
                detail_json={
                    "shipment_id": row["shipment_id"],
                    "received_pct": ratio,
                    "units_planned": _to_int(row.get("units_planned")),
                    "units_received": _to_int(row.get("units_received")),
                    "top_lines": top_lines,
                },
                context_json={
                    "module": "fba_ops",
                    "entity_type": "shipment",
                    "shipment_id": row["shipment_id"],
                    "marketplace_id": row.get("marketplace_id"),
                    "route": "/fba/inbound",
                    "query": {"shipment_id": row["shipment_id"]},
                    "source_page": "fba_inbound",
                },
                severity="warning",
                current_value=ratio,
            )
            _ensure_fba_task(
                cur,
                task_type="fba_inbound_variance",
                title=title,
                note=detail,
                source_page="fba_inbound",
                marketplace_id=row.get("marketplace_id"),
                payload={"shipment_id": row["shipment_id"], "received_pct": ratio},
            )
            created += 1

        cur.execute(
            """
            WITH snapshots AS (
                SELECT snapshot_date,
                       SUM(CAST(ISNULL(stranded_units, 0) * ISNULL(p.netto_purchase_price_pln, 0) AS DECIMAL(18,4))) AS stranded_value,
                       SUM(CAST((ISNULL(on_hand, 0) + ISNULL(inbound, 0)) * ISNULL(p.netto_purchase_price_pln, 0) AS DECIMAL(18,4))) AS total_value,
                       SUM(CAST((ISNULL(aged_90_plus, 0) + ISNULL(excess_units, 0)) * ISNULL(p.netto_purchase_price_pln, 0) AS DECIMAL(18,4))) AS aging_value
                FROM dbo.acc_fba_inventory_snapshot inv WITH (NOLOCK)
                LEFT JOIN dbo.acc_product p WITH (NOLOCK)
                  ON p.sku = inv.sku OR (p.asin = inv.asin AND inv.asin IS NOT NULL)
                GROUP BY snapshot_date
            )
            SELECT TOP 2 snapshot_date, stranded_value, total_value, aging_value
            FROM snapshots
            ORDER BY snapshot_date DESC
            """
        )
        snapshot_rows = _fetchall_dict(cur)
        if snapshot_rows:
            latest = snapshot_rows[0]
            previous = snapshot_rows[1] if len(snapshot_rows) > 1 else None
            latest_total_value = max(_to_float(latest.get("total_value")), 0.0)
            latest_stranded_value = _to_float(latest.get("stranded_value"))
            latest_aging_value = _to_float(latest.get("aging_value"))
            latest_aging_pct = round(latest_aging_value / latest_total_value * 100, 2) if latest_total_value > 0 else 0.0
            prev_total_value = max(_to_float(previous.get("total_value")) if previous else 0.0, 0.0)
            prev_stranded_value = _to_float(previous.get("stranded_value")) if previous else 0.0
            prev_aging_value = _to_float(previous.get("aging_value")) if previous else 0.0
            prev_aging_pct = round(prev_aging_value / prev_total_value * 100, 2) if prev_total_value > 0 else 0.0

            stranded_rule_id = _ensure_alert_rule(
                cur,
                rule_type="fba_stranded_spike",
                name="FBA Stranded Spike",
                description="Stranded value threshold or week-over-week spike exceeded",
                severity="critical",
            )
            stranded_threshold = 10000.0
            stranded_wow = ((latest_stranded_value - prev_stranded_value) / prev_stranded_value * 100) if prev_stranded_value > 0 else 0.0
            if latest_stranded_value > stranded_threshold or stranded_wow >= 30.0:
                title = "P1 Stranded Value Spike"
                if not _alert_exists_recent(cur, rule_id=stranded_rule_id, sku=None, marketplace_id=None, title=title):
                    top_stranded = _top_inventory_issue_skus(cur, snapshot_date=latest.get("snapshot_date"), mode="stranded")
                    detail_parts = [
                        f"Snapshot: {latest.get('snapshot_date')}",
                        f"Why: stranded value {round(latest_stranded_value, 2)} PLN | WoW {round(stranded_wow, 1)}%",
                    ]
                    if top_stranded:
                        detail_parts.append(f"Top SKU: {'; '.join(top_stranded)}")
                    detail_parts.append("Next step: sprawdz reason codes, listing/compliance issues i zdecyduj fix vs removal.")
                    detail = "\n".join(detail_parts)
                    _insert_fba_alert(
                        cur,
                        rule_id=stranded_rule_id,
                        marketplace_id=None,
                        sku=None,
                        title=title,
                        detail=detail,
                        detail_json={
                            "snapshot_date": str(latest.get("snapshot_date")),
                            "stranded_value": round(latest_stranded_value, 2),
                            "wow_change_pct": round(stranded_wow, 1),
                            "top_skus": top_stranded,
                        },
                        context_json={
                            "module": "fba_ops",
                            "entity_type": "snapshot",
                            "route": "/fba/aged-stranded",
                            "source_page": "fba_aged_stranded",
                            "query": {"tab": "stranded"},
                        },
                        severity="critical",
                        current_value=latest_stranded_value,
                    )
                    _ensure_fba_task(cur, task_type="fba_stranded", title=title, note=detail, source_page="fba_aged_stranded", payload={"snapshot_date": str(latest.get("snapshot_date")), "stranded_value": latest_stranded_value, "wow_change_pct": round(stranded_wow, 1)})
                    created += 1

            aging_rule_id = _ensure_alert_rule(
                cur,
                rule_type="fba_aging_spike",
                name="FBA Aging Spike",
                description="Aging/excess share threshold or month-over-month spike exceeded",
                severity="warning",
            )
            if latest_aging_pct > 25.0 or (latest_aging_pct - prev_aging_pct) >= 5.0:
                title = "P2 Aged Spike"
                if not _alert_exists_recent(cur, rule_id=aging_rule_id, sku=None, marketplace_id=None, title=title):
                    top_aged = _top_inventory_issue_skus(cur, snapshot_date=latest.get("snapshot_date"), mode="aging")
                    detail_parts = [
                        f"Snapshot: {latest.get('snapshot_date')}",
                        f"Why: aging/excess {latest_aging_pct}% | previous {prev_aging_pct}%",
                    ]
                    if top_aged:
                        detail_parts.append(f"Top SKU: {'; '.join(top_aged)}")
                    detail_parts.append("Next step: sprawdz promo / bundle / removal dla najdrozszych pozycji 90+ dni.")
                    detail = "\n".join(detail_parts)
                    _insert_fba_alert(
                        cur,
                        rule_id=aging_rule_id,
                        marketplace_id=None,
                        sku=None,
                        title=title,
                        detail=detail,
                        detail_json={
                            "snapshot_date": str(latest.get("snapshot_date")),
                            "aging_excess_pct": latest_aging_pct,
                            "prev_pct": prev_aging_pct,
                            "top_skus": top_aged,
                        },
                        context_json={
                            "module": "fba_ops",
                            "entity_type": "snapshot",
                            "route": "/fba/aged-stranded",
                            "source_page": "fba_aged_stranded",
                            "query": {"tab": "aged"},
                        },
                        severity="warning",
                        current_value=latest_aging_pct,
                    )
                    _ensure_fba_task(cur, task_type="fba_aging", title=title, note=detail, source_page="fba_aged_stranded", payload={"snapshot_date": str(latest.get("snapshot_date")), "aging_excess_pct": latest_aging_pct, "prev_pct": prev_aging_pct})
                    created += 1
        conn.commit()
        return created
    finally:
        conn.close()
