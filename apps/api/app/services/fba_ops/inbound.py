from __future__ import annotations

import asyncio
import json
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

import structlog

from app.connectors.amazon_sp_api.inbound import InboundClient
from app.connectors.amazon_sp_api.inventory import InventoryClient
from app.connectors.amazon_sp_api.reports import ReportsClient, ReportType
from app.core.config import MARKETPLACE_REGISTRY

from ._helpers import (
    _connect,
    _fetchall_dict,
    _first_int,
    _first_value,
    _inventory_api_rows_to_normalized,
    _latest_raw_inventory_snapshot_date,
    _lookup_product_context,
    _marketplace_code,
    _parse_json,
    _planning_report_cooldown_state,
    _report_rows_to_normalized,
    _to_int,
    _to_float,
    ensure_fba_schema,
)

log = structlog.get_logger(__name__)


def get_inbound_shipments(*, marketplace_id: str | None = None, status: str | None = None) -> dict[str, Any]:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        where = ["1=1"]
        params: list[Any] = []
        if marketplace_id:
            where.append("marketplace_id = ?")
            params.append(marketplace_id)
        if status:
            where.append("status = ?")
            params.append(status)
        cur.execute(
            f"""
            SELECT
                shipment_id, shipment_name, marketplace_id, status, created_at, last_update_at,
                from_warehouse, units_planned, units_received, first_receive_at, closed_at,
                DATEDIFF(day, ISNULL(last_update_at, created_at), SYSUTCDATETIME()) AS days_in_status,
                payload_json
            FROM dbo.acc_fba_inbound_shipment WITH (NOLOCK)
            WHERE {" AND ".join(where)}
            ORDER BY ISNULL(last_update_at, created_at) DESC
            """,
            params,
        )
        rows = _fetchall_dict(cur)
        cur.execute("SELECT status, COUNT(*) FROM dbo.acc_fba_inbound_shipment WITH (NOLOCK) GROUP BY status")
        by_status = {str(row[0]): _to_int(row[1]) for row in cur.fetchall()}
    finally:
        conn.close()
    items = []
    for row in rows:
        payload = {}
        if row.get("payload_json"):
            try:
                payload = json.loads(row["payload_json"])
            except Exception:
                payload = {}
        items.append(
            {
                "shipment_id": row["shipment_id"],
                "shipment_name": row.get("shipment_name"),
                "marketplace_id": row.get("marketplace_id"),
                "marketplace_code": _marketplace_code(row.get("marketplace_id")),
                "from_warehouse": row.get("from_warehouse"),
                "status": row.get("status") or "UNKNOWN",
                "created_at": row.get("created_at"),
                "last_update_at": row.get("last_update_at"),
                "units_planned": _to_int(row.get("units_planned")),
                "units_received": _to_int(row.get("units_received")),
                "variance_units": _to_int(row.get("units_received")) - _to_int(row.get("units_planned")),
                "first_receive_at": row.get("first_receive_at"),
                "closed_at": row.get("closed_at"),
                "days_in_status": _to_int(row.get("days_in_status")),
                "problems": payload.get("problems", []) if isinstance(payload, dict) else [],
            }
        )
    return {"items": items, "total": len(items), "by_status": by_status}


def get_inbound_shipment_detail(*, shipment_id: str) -> dict[str, Any]:
    shipments = get_inbound_shipments()
    shipment = next((item for item in shipments["items"] if item["shipment_id"] == shipment_id), None)
    if not shipment:
        raise ValueError("shipment not found")
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT sku, asin, qty_planned, qty_received, payload_json
            FROM dbo.acc_fba_inbound_shipment_line WITH (NOLOCK)
            WHERE shipment_id = ?
            ORDER BY sku
            """,
            (shipment_id,),
        )
        lines = _fetchall_dict(cur)
        product_context_cache: dict[tuple[str | None, str | None], dict[str, Any]] = {}

        def _line_context(sku: str | None, asin: str | None) -> dict[str, Any]:
            key = (sku or None, asin or None)
            cached = product_context_cache.get(key)
            if cached is not None:
                return cached
            context = _lookup_product_context(cur, sku=sku, asin=asin)
            product_context_cache[key] = context
            return context

        line_items = [
            {
                "sku": row.get("sku") or "",
                "asin": row.get("asin"),
                "internal_sku": _line_context(row.get("sku"), row.get("asin")).get("internal_sku"),
                "ean": _line_context(row.get("sku"), row.get("asin")).get("ean"),
                "parent_asin": _line_context(row.get("sku"), row.get("asin")).get("parent_asin"),
                "title_preferred": _line_context(row.get("sku"), row.get("asin")).get("title_preferred"),
                "qty_planned": _to_int(row.get("qty_planned")),
                "qty_received": _to_int(row.get("qty_received")),
                "variance_units": _to_int(row.get("qty_received")) - _to_int(row.get("qty_planned")),
                "payload_json": _parse_json(row.get("payload_json"), {}),
            }
            for row in lines
        ]
    finally:
        conn.close()
    return {
        "shipment": shipment,
        "lines": line_items,
    }


# ──────────── Report diagnostics ────────────

def _persist_report_diagnostics(cur, diagnostics: list[dict[str, Any]], *, sync_scope: str) -> None:
    for item in diagnostics:
        cur.execute(
            """
            INSERT INTO dbo.acc_fba_report_diagnostic
            (
                id, sync_scope, marketplace_id, report_type, fetch_mode,
                request_report_id, request_status, selected_report_id, selected_status,
                selected_document_id, fallback_source, detail_json, created_at
            )
            VALUES
            (
                CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME()
            )
            """,
            (
                str(uuid.uuid4()),
                sync_scope,
                item.get("marketplace_id"),
                item.get("report_type"),
                item.get("fetch_mode") or "unknown",
                item.get("request_report_id"),
                item.get("request_status"),
                item.get("selected_report_id"),
                item.get("selected_status"),
                item.get("selected_document_id"),
                item.get("fallback_source"),
                json.dumps(item, ensure_ascii=True),
            ),
        )


def _report_diagnostics_summary(diagnostics: list[dict[str, Any]]) -> str:
    if not diagnostics:
        return "reports=n/a"
    parts: list[str] = []
    for item in diagnostics:
        marketplace = _marketplace_code(item.get("marketplace_id")) or str(item.get("marketplace_id") or "-")
        if item.get("report_type") == ReportType.STRANDED_INVENTORY:
            report_name = "stranded"
        elif item.get("report_type") == "FBA_INVENTORY_API_SUMMARIES":
            report_name = "inventory_api"
        else:
            report_name = "planning"
        mode = str(item.get("fetch_mode") or "unknown")
        status = str(item.get("request_status") or item.get("selected_status") or "-")
        fallback = str(item.get("fallback_source") or "")
        parts.append(f"{marketplace}:{report_name}={mode}:{status}{('/' + fallback) if fallback else ''}")
    return "; ".join(parts)


def get_report_diagnostics(*, lookback_hours: int = 48) -> dict[str, Any]:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            WITH ranked AS (
                SELECT
                    marketplace_id,
                    report_type,
                    fetch_mode,
                    request_status,
                    selected_status,
                    fallback_source,
                    detail_json,
                    created_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY marketplace_id, report_type
                        ORDER BY created_at DESC
                    ) AS rn
                FROM dbo.acc_fba_report_diagnostic WITH (NOLOCK)
                WHERE sync_scope = 'inventory'
                  AND created_at >= DATEADD(HOUR, ?, SYSUTCDATETIME())
            )
            SELECT marketplace_id, report_type, fetch_mode, request_status, selected_status,
                   fallback_source, detail_json, created_at
            FROM ranked
            WHERE rn = 1
            ORDER BY marketplace_id, report_type
            """,
            (-abs(int(lookback_hours)),),
        )
        rows = _fetchall_dict(cur)
        by_marketplace: dict[str, dict[str, Any]] = {}
        bucket_map = {
            ReportType.FBA_INVENTORY_PLANNING: "planning",
            ReportType.STRANDED_INVENTORY: "stranded",
            "FBA_INVENTORY_API_SUMMARIES": "inventory_api",
        }
        for row in rows:
            marketplace_id = str(row.get("marketplace_id") or "")
            if not marketplace_id:
                continue
            bucket = bucket_map.get(str(row.get("report_type") or ""))
            if not bucket:
                continue
            item = by_marketplace.setdefault(
                marketplace_id,
                {
                    "marketplace_id": marketplace_id,
                    "marketplace_code": _marketplace_code(marketplace_id),
                    "planning": None,
                    "stranded": None,
                    "inventory_api": None,
                },
            )
            item[bucket] = {
                "report_type": row.get("report_type"),
                "fetch_mode": row.get("fetch_mode") or "unknown",
                "request_status": row.get("request_status"),
                "selected_status": row.get("selected_status"),
                "fallback_source": row.get("fallback_source"),
                "detail_json": _parse_json(row.get("detail_json"), {}),
                "created_at": row.get("created_at"),
            }
        return {
            "generated_at": datetime.now(timezone.utc),
            "items": sorted(by_marketplace.values(), key=lambda item: item["marketplace_code"] or item["marketplace_id"]),
        }
    finally:
        conn.close()


# ──────────── Async report fetching ────────────

async def _fetch_inventory_api_rows_with_diagnostics(marketplace_id: str) -> dict[str, Any]:
    try:
        client = InventoryClient(marketplace_id=marketplace_id)
        items = await client.get_inventory_summaries()
        return {
            "rows": _inventory_api_rows_to_normalized(items),
            "diagnostic": {
                "marketplace_id": marketplace_id,
                "report_type": "FBA_INVENTORY_API_SUMMARIES",
                "fetch_mode": "fallback_inventory_api",
                "request_report_id": None,
                "request_status": "DONE",
                "selected_report_id": None,
                "selected_status": "DONE",
                "selected_document_id": None,
                "fallback_source": "inventory_api",
                "detail": {
                    "item_count": len(items),
                    "coverage": "inventory_levels_only",
                    "aged_excess_available": False,
                },
            },
            "error": None,
        }
    except Exception as exc:
        return {
            "rows": [],
            "diagnostic": {
                "marketplace_id": marketplace_id,
                "report_type": "FBA_INVENTORY_API_SUMMARIES",
                "fetch_mode": "failed_inventory_api",
                "request_report_id": None,
                "request_status": "FAILED",
                "selected_report_id": None,
                "selected_status": None,
                "selected_document_id": None,
                "fallback_source": "inventory_api",
                "error": str(exc),
            },
            "error": str(exc),
        }


def _merge_inventory_report_data(
    inventory_map: dict[tuple[str, str], dict[str, Any]],
    marketplace_id: str,
    planning_rows: list[dict[str, Any]],
    stranded_rows: list[dict[str, Any]],
) -> None:
    for row in planning_rows:
        sku = _first_value(row, "msku", "sku", "merchant_sku", "seller_sku")
        asin = _first_value(row, "asin", "fnsku_asin")
        if not sku:
            continue
        key = (marketplace_id, sku)
        entry = inventory_map.setdefault(
            key,
            {
                "marketplace_id": marketplace_id,
                "sku": sku,
                "asin": asin or None,
                "on_hand": 0,
                "inbound": 0,
                "reserved": 0,
                "stranded_units": 0,
                "aged_0_30": 0,
                "aged_31_60": 0,
                "aged_61_90": 0,
                "aged_90_plus": 0,
                "excess_units": 0,
            },
        )
        if asin and not entry.get("asin"):
            entry["asin"] = asin
        entry["on_hand"] = max(entry["on_hand"], _first_int(row, "available"))
        age_0_30 = _first_int(row, "inv_age_0_to_30_days")
        age_31_60 = _first_int(row, "inv_age_31_to_60_days")
        age_61_90 = _first_int(row, "inv_age_61_to_90_days")
        age_91_180 = _first_int(row, "inv_age_91_to_180_days", "inventory_age_91_to_180_days")
        age_181_270 = _first_int(row, "inv_age_181_to_270_days", "inventory_age_181_to_270_days")
        age_271_365 = _first_int(row, "inv_age_271_to_365_days", "inventory_age_271_to_365_days")
        age_366_455 = _first_int(row, "inv_age_366_to_455_days")
        age_456 = _first_int(row, "inv_age_456_plus_days")
        age_365 = _first_int(row, "inv_age_365_plus_days", "inventory_age_365_plus_days")
        entry["aged_0_30"] = max(entry["aged_0_30"], age_0_30)
        entry["aged_31_60"] = max(entry["aged_31_60"], age_31_60)
        entry["aged_61_90"] = max(entry["aged_61_90"], age_61_90)
        entry["aged_90_plus"] = max(entry["aged_90_plus"], age_91_180 + age_181_270 + age_271_365 + age_366_455 + age_456 + age_365)
        entry["excess_units"] = max(
            entry["excess_units"],
            _first_int(row, "estimated_excess_quantity", "excess_units", "qty_to_be_charged_ltsf_12_mo"),
        )
        entry["inbound"] = max(entry["inbound"], _first_int(row, "inbound_quantity", "inbound_working") + _first_int(row, "inbound_shipped") + _first_int(row, "inbound_received"))
        entry["reserved"] = max(entry["reserved"], _first_int(row, "total_reserved_quantity"))

    for row in stranded_rows:
        sku = _first_value(row, "sku", "merchant_sku", "msku")
        asin = _first_value(row, "asin")
        if not sku:
            continue
        key = (marketplace_id, sku)
        entry = inventory_map.setdefault(
            key,
            {
                "marketplace_id": marketplace_id,
                "sku": sku,
                "asin": asin or None,
                "on_hand": 0,
                "inbound": 0,
                "reserved": 0,
                "stranded_units": 0,
                "aged_0_30": 0,
                "aged_31_60": 0,
                "aged_61_90": 0,
                "aged_90_plus": 0,
                "excess_units": 0,
            },
        )
        if asin and not entry.get("asin"):
            entry["asin"] = asin
        stranded_units = _first_int(
            row,
            "total_quantity",
            "quantity",
            "stranded_quantity",
            "fulfillable_qty",
            "unfulfillable_qty",
        )
        if stranded_units <= 0:
            stranded_units = _first_int(row, "fulfillable_qty") + _first_int(row, "unfulfillable_qty")
        entry["stranded_units"] = max(entry["stranded_units"], stranded_units)


async def _fetch_report_content_with_diagnostics(
    *,
    client: ReportsClient,
    marketplace_id: str,
    report_type: str,
    max_age_minutes: int,
    poll_interval: float,
    allow_last_done_fallback: bool = False,
    skip_new_request: bool = False,
    skip_reason: str | None = None,
    skip_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    recent_reports_error: str | None = None
    try:
        reports = await client.get_existing_reports(
            report_types=[report_type],
            processing_statuses=["DONE"],
            page_size=10,
        )
    except Exception as exc:
        reports = []
        recent_reports_error = str(exc)
    now = datetime.now(timezone.utc)
    for report in reports:
        created_raw = report.get("createdTime")
        document_id = report.get("reportDocumentId")
        if not created_raw or not document_id:
            continue
        try:
            created_at = datetime.fromisoformat(str(created_raw).replace("Z", "+00:00"))
        except Exception:
            continue
        age_minutes = (now - created_at).total_seconds() / 60.0
        if age_minutes <= max_age_minutes:
            content = await client.download_report_content(document_id)
            return {
                "content": content,
                "diagnostic": {
                    "marketplace_id": marketplace_id,
                    "report_type": report_type,
                    "fetch_mode": "reuse_recent_done",
                    "request_report_id": report.get("reportId"),
                    "request_status": "DONE",
                    "selected_report_id": report.get("reportId"),
                    "selected_status": "DONE",
                    "selected_document_id": document_id,
                    "fallback_source": None,
                    "created_time": created_raw,
                    "age_minutes": round(age_minutes, 1),
                },
                "error": None,
            }

    if skip_new_request:
        return {
            "content": None,
            "diagnostic": {
                "marketplace_id": marketplace_id,
                "report_type": report_type,
                "fetch_mode": "skip_recent_fatal_cooldown",
                "request_report_id": None,
                "request_status": "COOLDOWN",
                "selected_report_id": None,
                "selected_status": None,
                "selected_document_id": None,
                "fallback_source": skip_reason or "cooldown",
                "detail": skip_context or {},
                "error": recent_reports_error,
            },
            "error": f"Skipped new {report_type} request due to cooldown",
        }

    request_report_id: str | None = None
    request_status: str | None = None
    try:
        request_report_id = await client.create_report(
            report_type=report_type,
            marketplace_ids=[marketplace_id],
        )
        report = await client.wait_for_report(request_report_id, poll_interval=poll_interval)
        request_status = str(report.get("processingStatus") or "DONE")
        document_id = report.get("reportDocumentId")
        if not document_id:
            raise RuntimeError(f"Report {request_report_id} DONE but no reportDocumentId")
        content = await client.download_report_content(document_id)
        return {
            "content": content,
            "diagnostic": {
                "marketplace_id": marketplace_id,
                "report_type": report_type,
                "fetch_mode": "requested_new",
                "request_report_id": request_report_id,
                "request_status": request_status,
                "selected_report_id": request_report_id,
                "selected_status": request_status,
                "selected_document_id": document_id,
                "fallback_source": None,
            },
            "error": None,
        }
    except Exception as exc:
        if request_report_id:
            try:
                failed_report = await client.get_report(request_report_id)
                request_status = str(failed_report.get("processingStatus") or request_status or "UNKNOWN")
            except Exception:
                pass
        if allow_last_done_fallback:
            try:
                done_reports = await client.get_existing_reports(
                    report_types=[report_type],
                    processing_statuses=["DONE"],
                    page_size=20,
                )
                for report in done_reports:
                    document_id = report.get("reportDocumentId")
                    if not document_id:
                        continue
                    content = await client.download_report_content(document_id)
                    return {
                        "content": content,
                        "diagnostic": {
                            "marketplace_id": marketplace_id,
                            "report_type": report_type,
                            "fetch_mode": "fallback_last_done",
                            "request_report_id": request_report_id,
                            "request_status": request_status or "UNKNOWN",
                            "selected_report_id": report.get("reportId"),
                            "selected_status": "DONE",
                            "selected_document_id": document_id,
                            "fallback_source": "last_done",
                            "error": str(exc),
                        },
                        "error": None,
                    }
            except Exception as fallback_exc:
                return {
                    "content": None,
                    "diagnostic": {
                        "marketplace_id": marketplace_id,
                        "report_type": report_type,
                        "fetch_mode": "failed",
                        "request_report_id": request_report_id,
                        "request_status": request_status or "UNKNOWN",
                        "selected_report_id": None,
                        "selected_status": None,
                        "selected_document_id": None,
                        "fallback_source": "last_done_lookup_failed",
                        "error": f"{exc}; fallback lookup failed: {fallback_exc}",
                    },
                    "error": str(exc),
                }
        return {
            "content": None,
            "diagnostic": {
                "marketplace_id": marketplace_id,
                "report_type": report_type,
                "fetch_mode": "failed",
                "request_report_id": request_report_id,
                "request_status": request_status or "UNKNOWN",
                "selected_report_id": None,
                "selected_status": None,
                "selected_document_id": None,
                "fallback_source": None,
                "error": str(exc),
            },
            "error": str(exc),
        }


async def _fetch_inventory_enrichment_for_marketplace(
    marketplace_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    client = ReportsClient(marketplace_id=marketplace_id)
    diagnostics: list[dict[str, Any]] = []
    cooldown = _planning_report_cooldown_state(marketplace_id)

    planning_result = await _fetch_report_content_with_diagnostics(
        client=client,
        marketplace_id=marketplace_id,
        report_type=ReportType.FBA_INVENTORY_PLANNING,
        max_age_minutes=180,
        poll_interval=15.0,
        skip_new_request=bool(cooldown.get("active")),
        skip_reason=str(cooldown.get("reason") or "cooldown"),
        skip_context={
            "failure_count": _to_int(cooldown.get("failure_count")),
            "last_failure_at": str(cooldown.get("last_failure_at") or ""),
        },
    )
    diagnostics.append(planning_result["diagnostic"])
    planning_rows: list[dict[str, Any]] = []
    planning_source = "planning_report"
    if planning_result.get("content"):
        planning_rows = _report_rows_to_normalized(planning_result["content"])
    else:
        inventory_api_result = await _fetch_inventory_api_rows_with_diagnostics(marketplace_id)
        diagnostics.append(inventory_api_result["diagnostic"])
        planning_rows = inventory_api_result.get("rows") or []
        if planning_rows:
            planning_source = "inventory_api"

    stranded_result = await _fetch_report_content_with_diagnostics(
        client=client,
        marketplace_id=marketplace_id,
        report_type=ReportType.STRANDED_INVENTORY,
        max_age_minutes=360,
        poll_interval=15.0,
        allow_last_done_fallback=True,
    )
    diagnostics.append(stranded_result["diagnostic"])
    if stranded_result.get("content"):
        stranded_rows = _report_rows_to_normalized(stranded_result["content"])
    else:
        stranded_rows = []
        for row in planning_rows:
            if _first_int(row, "unfulfillable_quantity") > 0:
                stranded_rows.append(
                    {
                        "sku": _first_value(row, "sku"),
                        "asin": _first_value(row, "asin"),
                        "unfulfillable_qty": _first_int(row, "unfulfillable_quantity"),
                        "source": "planning_report_unfulfillable_proxy" if planning_source == "planning_report" else "inventory_api_unfulfillable_proxy",
                    }
                )
        diagnostics[-1]["fetch_mode"] = (
            "fallback_planning_unfulfillable"
            if planning_source == "planning_report"
            else "fallback_inventory_api_unfulfillable"
        )
        diagnostics[-1]["fallback_source"] = (
            "planning_unfulfillable_proxy"
            if planning_source == "planning_report"
            else "inventory_api_unfulfillable_proxy"
        )
    return planning_rows, stranded_rows, diagnostics


# ──────────── Sync operations ────────────

async def _sync_inventory_cache_async(*, return_meta: bool = False) -> int | dict[str, Any]:
    ensure_fba_schema()
    inventory_map: dict[tuple[str, str], dict[str, Any]] = {}
    diagnostics: list[dict[str, Any]] = []
    latest: date = date.today()
    conn = _connect()
    try:
        cur = conn.cursor()
        acc_latest = _latest_raw_inventory_snapshot_date(cur)
        if acc_latest:
            cur.execute(
                """
                SELECT marketplace_id, sku, asin,
                       ISNULL(qty_fulfillable, 0) AS on_hand,
                       ISNULL(qty_inbound, 0) AS inbound,
                       ISNULL(qty_reserved, 0) AS reserved
                FROM dbo.acc_inventory_snapshot WITH (NOLOCK)
                WHERE snapshot_date = ?
                """,
                (acc_latest,),
            )
            for row in _fetchall_dict(cur):
                inventory_map[(row["marketplace_id"], row["sku"])] = {
                    "marketplace_id": row["marketplace_id"],
                    "sku": row["sku"],
                    "asin": row.get("asin"),
                    "on_hand": _to_int(row.get("on_hand")),
                    "inbound": _to_int(row.get("inbound")),
                    "reserved": _to_int(row.get("reserved")),
                    "stranded_units": 0,
                    "aged_0_30": 0,
                    "aged_31_60": 0,
                    "aged_61_90": 0,
                    "aged_90_plus": 0,
                    "excess_units": 0,
                }
    finally:
        conn.close()

    marketplaces = sorted(MARKETPLACE_REGISTRY.keys())
    for marketplace_id in marketplaces:
        try:
            planning_rows, stranded_rows, marketplace_diagnostics = await _fetch_inventory_enrichment_for_marketplace(marketplace_id)
            diagnostics.extend(marketplace_diagnostics)
            _merge_inventory_report_data(inventory_map, marketplace_id, planning_rows, stranded_rows)
        except Exception:
            continue

    conn = _connect()
    try:
        cur = conn.cursor()
        rows = list(inventory_map.values())
        if diagnostics:
            _persist_report_diagnostics(cur, diagnostics, sync_scope="inventory")
        if not rows:
            conn.commit()
            result = {"rows": 0, "report_diagnostics": diagnostics, "report_diagnostics_summary": _report_diagnostics_summary(diagnostics)}
            return result if return_meta else 0
        for item in rows:
            cur.execute(
                """
                MERGE dbo.acc_fba_inventory_snapshot AS target
                USING (
                    SELECT ? AS marketplace_id, ? AS sku, ? AS asin,
                           ? AS on_hand, ? AS inbound, ? AS reserved,
                           ? AS stranded_units, ? AS aged_0_30, ? AS aged_31_60,
                           ? AS aged_61_90, ? AS aged_90_plus, ? AS excess_units,
                           ? AS snapshot_date
                ) AS source
                ON target.marketplace_id = source.marketplace_id
                   AND target.sku = source.sku
                   AND target.snapshot_date = source.snapshot_date
                WHEN MATCHED THEN
                    UPDATE SET asin = source.asin,
                               on_hand = source.on_hand, inbound = source.inbound,
                               reserved = source.reserved, stranded_units = source.stranded_units,
                               aged_0_30 = source.aged_0_30, aged_31_60 = source.aged_31_60,
                               aged_61_90 = source.aged_61_90, aged_90_plus = source.aged_90_plus,
                               excess_units = source.excess_units,
                               created_at = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN
                    INSERT (id, marketplace_id, sku, asin, on_hand, inbound, reserved,
                            stranded_units, aged_0_30, aged_31_60, aged_61_90, aged_90_plus,
                            excess_units, snapshot_date, created_at)
                    VALUES (NEWID(), source.marketplace_id, source.sku, source.asin,
                            source.on_hand, source.inbound, source.reserved,
                            source.stranded_units, source.aged_0_30, source.aged_31_60,
                            source.aged_61_90, source.aged_90_plus,
                            source.excess_units, source.snapshot_date, SYSUTCDATETIME());
                """,
                (
                    item["marketplace_id"],
                    item["sku"],
                    item.get("asin"),
                    _to_int(item.get("on_hand")),
                    _to_int(item.get("inbound")),
                    _to_int(item.get("reserved")),
                    _to_int(item.get("stranded_units")),
                    _to_int(item.get("aged_0_30")),
                    _to_int(item.get("aged_31_60")),
                    _to_int(item.get("aged_61_90")),
                    _to_int(item.get("aged_90_plus")),
                    _to_int(item.get("excess_units")),
                    latest,
                ),
            )
        conn.commit()
        log.info("fba_ops.sync_inventory_cache.saved", rows=len(rows), snapshot_date=str(latest))
        result = {
            "rows": len(rows),
            "report_diagnostics": diagnostics,
            "report_diagnostics_summary": _report_diagnostics_summary(diagnostics),
        }
        return result if return_meta else len(rows)
    finally:
        conn.close()


async def _sync_inbound_async() -> int:
    ensure_fba_schema()
    statuses = ["WORKING", "SHIPPED", "IN_TRANSIT", "DELIVERED", "CHECKED_IN", "RECEIVING", "CLOSED"]
    shipments_buffer: list[dict[str, Any]] = []
    lines_buffer: list[dict[str, Any]] = []
    updated_after = datetime.now(timezone.utc) - timedelta(days=120)
    updated_before = datetime.now(timezone.utc)

    for marketplace_id in MARKETPLACE_REGISTRY.keys():
        try:
            client = InboundClient(marketplace_id=marketplace_id)
            shipments = await client.get_shipments(
                statuses=statuses,
                last_updated_after=updated_after,
                last_updated_before=updated_before,
            )
            for shipment in shipments:
                shipment_id = _first_value(shipment, "ShipmentId", "shipmentId")
                if not shipment_id:
                    continue
                shipment_name = _first_value(shipment, "ShipmentName", "shipmentName")
                status = _first_value(shipment, "ShipmentStatus", "shipmentStatus") or "UNKNOWN"
                item = {
                    "marketplace_id": marketplace_id,
                    "shipment_id": shipment_id,
                    "shipment_name": shipment_name or None,
                    "status": status,
                    "created_at": None,
                    "last_update_at": updated_before,
                    "from_warehouse": _first_value(shipment, "DestinationFulfillmentCenterId", "destinationFulfillmentCenterId"),
                    "units_planned": _to_int(shipment.get("QuantityShipped")),
                    "units_received": 0,
                    "first_receive_at": None,
                    "closed_at": updated_before if status == "CLOSED" else None,
                    "payload_json": shipment,
                }
                try:
                    line_items = await client.get_shipment_items(shipment_id)
                except Exception:
                    line_items = []
                units_planned = 0
                units_received = 0
                first_receive_at = None
                for line in line_items:
                    qty_planned = _to_int(line.get("QuantityShipped"))
                    qty_received = _to_int(line.get("QuantityReceived"))
                    units_planned += qty_planned
                    units_received += qty_received
                    lines_buffer.append(
                        {
                            "shipment_id": shipment_id,
                            "sku": _first_value(line, "SellerSKU", "sellerSku"),
                            "asin": _first_value(line, "ASIN", "asin") or None,
                            "qty_planned": qty_planned,
                            "qty_received": qty_received,
                            "payload_json": line,
                        }
                    )
                item["units_planned"] = units_planned
                item["units_received"] = units_received
                item["first_receive_at"] = first_receive_at
                shipments_buffer.append(item)
        except Exception:
            continue

    conn = _connect()
    try:
        cur = conn.cursor()
        seen_shipments = {row["shipment_id"] for row in shipments_buffer}
        if seen_shipments:
            placeholders = ",".join("?" for _ in seen_shipments)
            cur.execute(f"DELETE FROM dbo.acc_fba_inbound_shipment_line WHERE shipment_id IN ({placeholders})", tuple(seen_shipments))
        for shipment in shipments_buffer:
            cur.execute(
                """
                MERGE dbo.acc_fba_inbound_shipment AS target
                USING (
                    SELECT
                        ? AS marketplace_id, ? AS shipment_id, ? AS shipment_name, ? AS status, ? AS created_at,
                        ? AS last_update_at, ? AS from_warehouse, ? AS units_planned, ? AS units_received,
                        ? AS first_receive_at, ? AS closed_at, ? AS payload_json
                ) AS source
                ON target.shipment_id = source.shipment_id
                WHEN MATCHED THEN
                    UPDATE SET marketplace_id = source.marketplace_id, shipment_name = source.shipment_name,
                               status = source.status,
                               last_update_at = CASE WHEN ISNULL(target.status, '') = ISNULL(source.status, '') THEN ISNULL(target.last_update_at, source.last_update_at) ELSE source.last_update_at END,
                               from_warehouse = source.from_warehouse,
                               units_planned = source.units_planned, units_received = source.units_received,
                               first_receive_at = source.first_receive_at, closed_at = source.closed_at, payload_json = source.payload_json
                WHEN NOT MATCHED THEN
                    INSERT (id, marketplace_id, shipment_id, shipment_name, status, created_at, last_update_at, from_warehouse,
                            units_planned, units_received, first_receive_at, closed_at, payload_json)
                    VALUES (NEWID(), source.marketplace_id, source.shipment_id, source.shipment_name, source.status, source.created_at,
                            source.last_update_at, source.from_warehouse, source.units_planned, source.units_received,
                            source.first_receive_at, source.closed_at, source.payload_json);
                """,
                (
                    shipment["marketplace_id"],
                    shipment["shipment_id"],
                    shipment.get("shipment_name"),
                    shipment["status"],
                    shipment.get("created_at"),
                    shipment.get("last_update_at"),
                    shipment.get("from_warehouse"),
                    shipment.get("units_planned"),
                    shipment.get("units_received"),
                    shipment.get("first_receive_at"),
                    shipment.get("closed_at"),
                    json.dumps(shipment.get("payload_json") or {}, ensure_ascii=True),
                ),
            )
        for line in lines_buffer:
            cur.execute(
                """
                INSERT INTO dbo.acc_fba_inbound_shipment_line
                (id, shipment_id, sku, asin, qty_planned, qty_received, payload_json)
                VALUES
                (NEWID(), ?, ?, ?, ?, ?, ?)
                """,
                (
                    line["shipment_id"],
                    line["sku"],
                    line.get("asin"),
                    _to_int(line.get("qty_planned")),
                    _to_int(line.get("qty_received")),
                    json.dumps(line.get("payload_json") or {}, ensure_ascii=True),
                ),
            )
        conn.commit()
        return len(shipments_buffer)
    finally:
        conn.close()


def sync_inventory_cache(*, return_meta: bool = False) -> int | dict[str, Any]:
    """Delegate to unified ingestion (Sprint 7 S7.2).

    Runs enrichment-only (raw snapshots are written by the inventory
    ingestion module separately).  Callers that need the full pipeline
    should use ``app.ingestion.inventory.ingest_inventory_sync``.
    """
    from app.ingestion.inventory import _enrich_with_fba_reports
    result = asyncio.run(_enrich_with_fba_reports(return_meta=return_meta))
    if return_meta:
        return result
    return int(result.get("rows", 0) or 0)


def sync_inbound_stub() -> int:
    return asyncio.run(_sync_inbound_async())


# ──────────── FC Receiving Reconciliation Sync ────────────

def sync_receiving_reconciliation() -> int:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                s.marketplace_id,
                s.shipment_id,
                sl.sku,
                ISNULL(sl.qty_planned, 0) AS qty_planned,
                ISNULL(sl.qty_received, 0) AS qty_received,
                COALESCE(s.closed_at, s.last_update_at, SYSUTCDATETIME()) AS event_dt
            FROM dbo.acc_fba_inbound_shipment s WITH (NOLOCK)
            JOIN dbo.acc_fba_inbound_shipment_line sl WITH (NOLOCK)
              ON sl.shipment_id = s.shipment_id
            WHERE s.status IN ('CLOSED', 'RECEIVING', 'CHECKED_IN')
              AND sl.sku IS NOT NULL
              AND ISNULL(sl.qty_planned, 0) > 0
            """
        )
        rows = _fetchall_dict(cur)
        if not rows:
            return 0
        upserted = 0
        for row in rows:
            shortage = max(0, _to_int(row.get("qty_planned")) - _to_int(row.get("qty_received")))
            event_date = row.get("event_dt")
            if hasattr(event_date, "date"):
                event_date = event_date.date()
            elif not isinstance(event_date, date):
                event_date = date.today()
            cur.execute(
                """
                MERGE dbo.acc_fba_receiving_reconciliation AS target
                USING (
                    SELECT ? AS shipment_id, ? AS marketplace_id, ? AS sku,
                           ? AS event_date, ? AS shipped_units, ? AS shortage_units
                ) AS source
                ON target.shipment_id = source.shipment_id
                   AND target.sku = source.sku
                WHEN MATCHED THEN
                    UPDATE SET marketplace_id = source.marketplace_id,
                               event_date = source.event_date,
                               shipped_units = source.shipped_units,
                               shortage_units = source.shortage_units,
                               created_at = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN
                    INSERT (id, shipment_id, marketplace_id, sku, event_date,
                            shipped_units, shortage_units, damage_units, reimbursement_units, created_at)
                    VALUES (NEWID(), source.shipment_id, source.marketplace_id, source.sku,
                            source.event_date, source.shipped_units, source.shortage_units, 0, 0, SYSUTCDATETIME());
                """,
                (
                    row.get("shipment_id"),
                    row.get("marketplace_id"),
                    row.get("sku"),
                    event_date,
                    _to_int(row.get("qty_planned")),
                    shortage,
                ),
            )
            upserted += 1
        conn.commit()
        log.info("fba_ops.sync_receiving_reconciliation.done", upserted=upserted)
        return upserted
    finally:
        conn.close()


# ──────────── Auto-fill Shipment Plan Actuals ────────────

def auto_fill_shipment_plan_actuals(*, quarter: str | None = None) -> int:
    ensure_fba_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        where_quarter = "AND sp.quarter = ?" if quarter else ""
        params = (quarter,) if quarter else ()
        cur.execute(
            f"""
            UPDATE sp
            SET
                sp.actual_units = s.units_planned,
                sp.actual_ship_date = CAST(COALESCE(s.closed_at, s.last_update_at) AS DATE),
                sp.status = CASE
                    WHEN s.status = 'CLOSED' THEN 'completed'
                    WHEN s.status IN ('RECEIVING', 'CHECKED_IN', 'DELIVERED') THEN 'receiving'
                    WHEN s.status IN ('SHIPPED', 'IN_TRANSIT') THEN 'shipped'
                    ELSE sp.status
                END,
                sp.updated_at = SYSUTCDATETIME()
            FROM dbo.acc_fba_shipment_plan sp
            JOIN dbo.acc_fba_inbound_shipment s
              ON s.shipment_id = sp.shipment_id
            WHERE sp.shipment_id IS NOT NULL
              AND sp.actual_units IS NULL
              {where_quarter}
            """,
            params,
        )
        updated = cur.rowcount or 0
        conn.commit()
        log.info("fba_ops.auto_fill_shipment_plan_actuals.done", updated=updated, quarter=quarter)
        return updated
    finally:
        conn.close()
