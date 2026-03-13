from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable

import structlog

from app.connectors.dhl24_api import DHL24Client
from app.connectors.dhl24_api.models import DHL24LabelDataResult
from app.core.db_connection import connect_acc
from app.services.dhl_integration import ensure_dhl_schema

log = structlog.get_logger(__name__)

_BATCH_SIZE = 500


@dataclass
class ShipmentIdentifier:
    kind: str
    value: str
    priority: int


@dataclass
class ShipmentCostTarget:
    shipment_id: str
    shipment_number: str | None
    tracking_number: str | None
    piece_id: str | None
    cedex_number: str | None
    source_payload_json: str | None


def _connect():
    return connect_acc(autocommit=False, timeout=30)


def _normalize_token(value: Any) -> str:
    return str(value or "").strip().upper().replace(" ", "")


def _json_dump(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _chunks(items: list[Any], size: int) -> Iterable[list[Any]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _load_json(payload: str | None) -> dict[str, Any]:
    if not payload:
        return {}
    try:
        data = json.loads(payload)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _append_identifier(
    bucket: dict[str, ShipmentIdentifier],
    *,
    kind: str,
    value: Any,
    priority: int,
) -> None:
    raw = str(value or "").strip()
    if not raw:
        return
    key = _normalize_token(raw)
    if not key:
        return
    existing = bucket.get(key)
    if existing is None or priority > existing.priority:
        bucket[key] = ShipmentIdentifier(kind=kind, value=raw, priority=priority)


def _extract_identifiers(shipment: ShipmentCostTarget) -> list[ShipmentIdentifier]:
    bucket: dict[str, ShipmentIdentifier] = {}
    _append_identifier(bucket, kind="shipment_number", value=shipment.shipment_number, priority=100)
    _append_identifier(bucket, kind="tracking_number", value=shipment.tracking_number, priority=99)
    _append_identifier(bucket, kind="piece_id", value=shipment.piece_id, priority=97)
    _append_identifier(bucket, kind="cedex_number", value=shipment.cedex_number, priority=94)

    payload = _load_json(shipment.source_payload_json)
    basic = payload.get("basic") if isinstance(payload.get("basic"), dict) else {}
    label_data = payload.get("label_data") if isinstance(payload.get("label_data"), dict) else {}
    piece_shipments = payload.get("piece_shipments") if isinstance(payload.get("piece_shipments"), list) else []

    _append_identifier(bucket, kind="basic_shipment_id", value=basic.get("shipment_id"), priority=100)
    _append_identifier(bucket, kind="label_primary_waybill", value=label_data.get("primary_waybill_number"), priority=98)

    for piece in label_data.get("pieces") or []:
        if not isinstance(piece, dict):
            continue
        _append_identifier(bucket, kind="label_routing_barcode", value=piece.get("routing_barcode"), priority=98)
        _append_identifier(bucket, kind="label_blp_piece_id", value=piece.get("blp_piece_id"), priority=96)

    for piece_shipment in piece_shipments:
        if not isinstance(piece_shipment, dict):
            continue
        _append_identifier(bucket, kind="piece_shipment_cedex", value=piece_shipment.get("cedex_number"), priority=95)
        for package in piece_shipment.get("packages") or []:
            if not isinstance(package, dict):
                continue
            _append_identifier(bucket, kind="piece_package_number", value=package.get("package_number"), priority=97)

    return sorted(bucket.values(), key=lambda item: (-item.priority, item.kind))


def _load_cost_targets(
    *,
    created_from: date | None,
    created_to: date | None,
    limit_shipments: int,
    refresh_existing: bool,
) -> list[ShipmentCostTarget]:
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
        if not refresh_existing:
            where.append(
                """
                NOT EXISTS (
                    SELECT 1
                    FROM dbo.acc_shipment_cost c WITH (NOLOCK)
                    WHERE c.shipment_id = s.id
                      AND c.is_estimated = 0
                )
                """
            )

        sql = f"""
            SELECT TOP {int(limit_shipments)}
                CAST(s.id AS NVARCHAR(40)) AS shipment_id,
                s.shipment_number,
                s.tracking_number,
                s.piece_id,
                s.cedex_number,
                s.source_payload_json
            FROM dbo.acc_shipment s WITH (NOLOCK)
            WHERE {' AND '.join(where)}
            ORDER BY s.last_sync_at ASC, s.first_seen_at ASC
        """
        cur.execute(sql, params)
        rows = cur.fetchall()
        return [
            ShipmentCostTarget(
                shipment_id=str(row[0]),
                shipment_number=str(row[1] or "") or None,
                tracking_number=str(row[2] or "") or None,
                piece_id=str(row[3] or "") or None,
                cedex_number=str(row[4] or "") or None,
                source_payload_json=str(row[5]) if row[5] is not None else None,
            )
            for row in rows
        ]
    finally:
        conn.close()


def _query_imported_parcel_lookup(cur, values: list[str]) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    if not values:
        return result
    for batch in _chunks(values, _BATCH_SIZE):
        placeholders = ",".join("?" for _ in batch)
        params = batch + batch + batch
        cur.execute(
            f"""
            SELECT
                jjd_number,
                parcel_number_base,
                parcel_number
            FROM dbo.acc_dhl_parcel_map WITH (NOLOCK)
            WHERE jjd_number IN ({placeholders})
               OR parcel_number_base IN ({placeholders})
               OR parcel_number IN ({placeholders})
            """,
            params,
        )
        for row in cur.fetchall():
            parcel_base = _normalize_token(row[1])
            if not parcel_base:
                continue
            for candidate in (row[0], row[1], row[2]):
                key = _normalize_token(candidate)
                if key:
                    result.setdefault(key, set()).add(parcel_base)
    return result


def _query_imported_billing_costs(cur, parcel_bases: list[str]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    if not parcel_bases:
        return result
    for batch in _chunks(parcel_bases, _BATCH_SIZE):
        placeholders = ",".join("?" for _ in batch)
        cur.execute(
            f"""
            SELECT
                parcel_number_base,
                SUM(ISNULL(net_amount, 0)) AS total_net_amount,
                SUM(ISNULL(fuel_road_fee, 0)) AS fuel_amount,
                MAX(issue_date) AS latest_issue_date,
                MIN(sales_date) AS first_sales_date,
                MAX(sales_date) AS last_sales_date,
                COUNT(*) AS line_count,
                COUNT(DISTINCT document_number) AS doc_count
            FROM dbo.acc_dhl_billing_line WITH (NOLOCK)
            WHERE parcel_number_base IN ({placeholders})
            GROUP BY parcel_number_base
            """,
            batch,
        )
        for row in cur.fetchall():
            key = _normalize_token(row[0])
            result[key] = {
                "parcel_number_base": str(row[0] or "") or None,
                "net_amount": float(row[1] or 0),
                "fuel_amount": float(row[2] or 0),
                # SF-09: gross = net + fuel (not just net). Explicit basis flag.
                "gross_amount": round(float(row[1] or 0) + float(row[2] or 0), 4),
                "cost_basis": "NET",
                "invoice_date": row[3],
                "first_sales_date": row[4],
                "last_sales_date": row[5],
                "line_count": int(row[6] or 0),
                "doc_count": int(row[7] or 0),
            }
    return result


def _select_imported_actual_cost(
    *,
    identifiers: list[ShipmentIdentifier],
    parcel_lookup: dict[str, set[str]],
    billing_costs: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    for identifier in identifiers:
        key = _normalize_token(identifier.value)
        if not key:
            continue
        parcel_candidates: list[str] = []
        if key in billing_costs:
            parcel_candidates.append(key)
        parcel_candidates.extend(sorted(parcel_lookup.get(key, set())))
        seen: set[str] = set()
        for parcel_base in parcel_candidates:
            parcel_key = _normalize_token(parcel_base)
            if not parcel_key or parcel_key in seen:
                continue
            seen.add(parcel_key)
            billing = billing_costs.get(parcel_key)
            if not billing:
                continue
            return {
                **billing,
                "cost_source": "dhl_billing_files",
                "match_kind": identifier.kind,
                "match_value": identifier.value,
                "resolved_via": "billing_line.parcel_number_base" if parcel_key == key else "parcel_map",
                "matched_parcel_base": billing.get("parcel_number_base"),
            }
    return None


def _query_direct_invoices(cur, values: list[str]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    if not values:
        return result
    for batch in _chunks(values, _BATCH_SIZE):
        placeholders = ",".join("?" for _ in batch)
        cur.execute(
            f"""
            SELECT
                parcel_num,
                SUM(ISNULL(netto, 0)) AS net_amount,
                SUM(ISNULL(toll, 0)) AS toll_amount,
                SUM(ISNULL(fuel_surcharge, 0)) AS fuel_amount,
                MAX(invoice_num) AS invoice_number,
                MAX(invoice_date) AS invoice_date,
                MAX(rcountry) AS country
            FROM dbo.acc_cache_invoices WITH (NOLOCK)
            WHERE courier = 'DHL'
              AND parcel_num IN ({placeholders})
            GROUP BY parcel_num
            """,
            batch,
        )
        for row in cur.fetchall():
            key = _normalize_token(row[0])
            result[key] = {
                "parcel_num_invoice": str(row[0] or "") or None,
                "net_amount": float(row[1] or 0),
                "toll_amount": float(row[2] or 0),
                "fuel_amount": float(row[3] or 0),
                "gross_amount": float((row[1] or 0) + (row[2] or 0) + (row[3] or 0)),
                "invoice_number": str(row[4] or "") or None,
                "invoice_date": row[5],
                "country": str(row[6] or "") or None,
            }
    return result


def _query_extras_map(cur, values: list[str]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    if not values:
        return result
    for batch in _chunks(values, _BATCH_SIZE):
        placeholders = ",".join("?" for _ in batch)
        cur.execute(
            f"""
            SELECT parcel_num_other, parcel_num
            FROM dbo.acc_cache_extras WITH (NOLOCK)
            WHERE parcel_num_other IN ({placeholders})
            """,
            batch,
        )
        for row in cur.fetchall():
            key = _normalize_token(row[0])
            parcel = str(row[1] or "").strip()
            if key and parcel:
                result.setdefault(key, [])
                if parcel not in result[key]:
                    result[key].append(parcel)
    return result


def _select_actual_cost(
    *,
    identifiers: list[ShipmentIdentifier],
    direct_invoices: dict[str, dict[str, Any]],
    extras_map: dict[str, list[str]],
    invoice_by_parcel: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    for identifier in identifiers:
        key = _normalize_token(identifier.value)
        direct = direct_invoices.get(key)
        if direct:
            return {
                **direct,
                "cost_source": "invoice_direct",
                "match_kind": identifier.kind,
                "match_value": identifier.value,
                "resolved_via": "parcel_num",
            }
        for parcel in extras_map.get(key, []):
            invoice = invoice_by_parcel.get(_normalize_token(parcel))
            if invoice:
                return {
                    **invoice,
                    "cost_source": "invoice_extras",
                    "match_kind": identifier.kind,
                    "match_value": identifier.value,
                    "resolved_via": "parcel_num_other",
                }
    return None


def _load_cached_label_data(shipment: ShipmentCostTarget) -> DHL24LabelDataResult | None:
    payload = _load_json(shipment.source_payload_json)
    label_payload = payload.get("label_data")
    if not isinstance(label_payload, dict):
        return None
    return DHL24LabelDataResult.from_dict(label_payload)


def _label_data_priceable(label_data: DHL24LabelDataResult | None) -> bool:
    if label_data is None or label_data.service is None:
        return False
    if label_data.billing is None:
        return False
    payer_type = (
        label_data.billing.shipping_payment_type
        or label_data.billing.payment_type
    )
    if not payer_type or not label_data.billing.billing_account_number:
        return False
    if label_data.shipper is None or label_data.receiver is None:
        return False
    required_shipper = [
        label_data.shipper.country,
        label_data.shipper.postal_code,
        label_data.shipper.city,
        label_data.shipper.street,
        label_data.shipper.house_number,
    ]
    required_receiver = [
        label_data.receiver.country,
        label_data.receiver.postal_code,
        label_data.receiver.city,
        label_data.receiver.street,
        label_data.receiver.house_number,
    ]
    if not all(required_shipper) or not all(required_receiver):
        return False
    if not label_data.service.product or not label_data.pieces:
        return False
    for piece in label_data.pieces:
        if piece.piece_type and piece.weight is not None:
            return True
    return False


def _upsert_shipment_cost(
    cur,
    *,
    shipment_id: str,
    cost_source: str,
    currency: str,
    net_amount: float | None,
    fuel_amount: float | None,
    toll_amount: float | None,
    gross_amount: float | None,
    invoice_number: str | None,
    invoice_date: Any,
    billing_period: str | None,
    is_estimated: bool,
    raw_payload_json: str,
) -> None:
    cur.execute(
        """
        SELECT CAST(id AS NVARCHAR(40))
        FROM dbo.acc_shipment_cost WITH (NOLOCK)
        WHERE shipment_id = CAST(? AS UNIQUEIDENTIFIER)
          AND cost_source = ?
        """,
        [shipment_id, cost_source],
    )
    row = cur.fetchone()
    if row:
        cur.execute(
            """
            UPDATE dbo.acc_shipment_cost
            SET currency = ?,
                net_amount = ?,
                fuel_amount = ?,
                toll_amount = ?,
                gross_amount = ?,
                invoice_number = ?,
                invoice_date = ?,
                billing_period = ?,
                is_estimated = ?,
                raw_payload_json = ?,
                updated_at = SYSUTCDATETIME()
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            [
                currency,
                net_amount,
                fuel_amount,
                toll_amount,
                gross_amount,
                invoice_number,
                invoice_date,
                billing_period,
                1 if is_estimated else 0,
                raw_payload_json,
                str(row[0]),
            ],
        )
        return

    cur.execute(
        """
        INSERT INTO dbo.acc_shipment_cost (
            id, shipment_id, cost_source, currency, net_amount, fuel_amount,
            toll_amount, gross_amount, invoice_number, invoice_date, billing_period,
            is_estimated, raw_payload_json, created_at, updated_at
        )
        VALUES (
            NEWID(), CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME()
        )
        """,
        [
            shipment_id,
            cost_source,
            currency,
            net_amount,
            fuel_amount,
            toll_amount,
            gross_amount,
            invoice_number,
            invoice_date,
            billing_period,
            1 if is_estimated else 0,
            raw_payload_json,
        ],
    )


def _delete_other_actual_rows(cur, *, shipment_id: str, keep_source: str) -> None:
    cur.execute(
        """
        DELETE FROM dbo.acc_shipment_cost
        WHERE shipment_id = CAST(? AS UNIQUEIDENTIFIER)
          AND is_estimated = 0
          AND cost_source <> ?
        """,
        [shipment_id, keep_source],
    )


def sync_dhl_shipment_costs(
    *,
    created_from: date | None = None,
    created_to: date | None = None,
    limit_shipments: int = 500,
    allow_estimated: bool = True,
    refresh_existing: bool = False,
    job_id: str | None = None,
) -> dict[str, Any]:
    ensure_dhl_schema()

    from app.connectors.mssql.mssql_store import set_job_progress

    stats = {
        "shipments_selected": 0,
        "shipments_processed": 0,
        "actual_costs_written": 0,
        "estimated_costs_written": 0,
        "actual_source_billing_files": 0,
        "estimate_requests": 0,
        "estimate_success": 0,
        "estimate_skipped_not_priceable": 0,
        "estimate_failures": 0,
        "no_cost_match": 0,
    }

    shipments = _load_cost_targets(
        created_from=created_from,
        created_to=created_to,
        limit_shipments=limit_shipments,
        refresh_existing=refresh_existing,
    )
    stats["shipments_selected"] = len(shipments)
    if not shipments:
        return stats

    if job_id:
        set_job_progress(job_id, progress_pct=15, records_processed=0, message=f"DHL cost sync count={len(shipments)}")

    shipment_identifiers: dict[str, list[ShipmentIdentifier]] = {}
    unique_values: dict[str, str] = {}
    for shipment in shipments:
        identifiers = _extract_identifiers(shipment)
        shipment_identifiers[shipment.shipment_id] = identifiers
        for identifier in identifiers:
            unique_values.setdefault(_normalize_token(identifier.value), identifier.value)

    parcel_lookup: dict[str, set[str]] = {}
    billing_costs: dict[str, dict[str, Any]] = {}

    conn = _connect()
    client = DHL24Client() if allow_estimated else None
    try:
        cur = conn.cursor()
        raw_values = list(unique_values.values())
        parcel_lookup = _query_imported_parcel_lookup(cur, raw_values)
        billing_parcels = set(parcel_lookup.keys()) | {key for key in unique_values.keys()}
        for parcel_bases in parcel_lookup.values():
            billing_parcels.update(parcel_bases)
        billing_costs = _query_imported_billing_costs(cur, sorted(billing_parcels))

        for idx, shipment in enumerate(shipments, start=1):
            identifiers = shipment_identifiers.get(shipment.shipment_id, [])
            actual = _select_imported_actual_cost(
                identifiers=identifiers,
                parcel_lookup=parcel_lookup,
                billing_costs=billing_costs,
            )

            if actual:
                payload = {
                    "match_kind": actual["match_kind"],
                    "match_value": actual["match_value"],
                    "resolved_via": actual["resolved_via"],
                    "parcel_number_base": actual.get("matched_parcel_base"),
                    "doc_count": actual.get("doc_count"),
                    "line_count": actual.get("line_count"),
                    "invoice_date": str(actual.get("invoice_date") or "") or None,
                    "first_sales_date": str(actual.get("first_sales_date") or "") or None,
                    "last_sales_date": str(actual.get("last_sales_date") or "") or None,
                }
                _upsert_shipment_cost(
                    cur,
                    shipment_id=shipment.shipment_id,
                    cost_source=str(actual["cost_source"]),
                    currency="PLN",
                    net_amount=actual.get("net_amount"),
                    fuel_amount=actual.get("fuel_amount"),
                    toll_amount=None,
                    gross_amount=actual.get("gross_amount"),
                    invoice_number=None,
                    invoice_date=actual.get("invoice_date"),
                    billing_period=str(actual.get("last_sales_date") or "")[:7] or None,
                    is_estimated=False,
                    raw_payload_json=_json_dump(payload),
                )
                _delete_other_actual_rows(cur, shipment_id=shipment.shipment_id, keep_source=str(actual["cost_source"]))
                stats["actual_costs_written"] += 1
                stats["actual_source_billing_files"] += 1
            elif allow_estimated and client and client.is_configured:
                stats["estimate_requests"] += 1
                label_data = _load_cached_label_data(shipment)
                if label_data is None and shipment.shipment_number:
                    try:
                        items = client.get_labels_data([shipment.shipment_number])
                        label_data = items[0] if items else None
                    except Exception as exc:
                        log.warning(
                            "dhl_cost_sync.labels_data_failed",
                            shipment_id=shipment.shipment_id,
                            shipment_number=shipment.shipment_number,
                            error=str(exc),
                        )
                        label_data = None

                if not _label_data_priceable(label_data):
                    stats["estimate_skipped_not_priceable"] += 1
                else:
                    try:
                        price = client.get_price(label_data)
                        fuel_pct = float(price.fuel_surcharge or 0)
                        base_price = float(price.price or 0)
                        fuel_amount = None
                        gross = None
                        if price.price is not None or price.fuel_surcharge is not None:
                            fuel_amount = round(base_price * fuel_pct / 100.0, 4)
                            gross = round(base_price + fuel_amount, 4)
                        payload = {
                            "source": "getPrice",
                            "price": price.to_dict(),
                            "gross_formula": "price + (price * fuel_surcharge_pct / 100)",
                            "label_reference": label_data.reference,
                            "service_product": label_data.service_product,
                        }
                        _upsert_shipment_cost(
                            cur,
                            shipment_id=shipment.shipment_id,
                            cost_source="dhl_get_price",
                            currency="PLN",
                            net_amount=price.price,
                            fuel_amount=fuel_amount,
                            toll_amount=None,
                            gross_amount=gross,
                            invoice_number=None,
                            invoice_date=None,
                            billing_period=None,
                            is_estimated=True,
                            raw_payload_json=_json_dump(payload),
                        )
                        stats["estimated_costs_written"] += 1
                        stats["estimate_success"] += 1
                    except Exception as exc:
                        stats["estimate_failures"] += 1
                        log.warning(
                            "dhl_cost_sync.price_failed",
                            shipment_id=shipment.shipment_id,
                            shipment_number=shipment.shipment_number,
                            error=str(exc),
                        )
            else:
                stats["no_cost_match"] += 1

            stats["shipments_processed"] += 1

            if idx % 25 == 0:
                conn.commit()
                log.info(
                    "dhl_cost_sync.progress",
                    processed=idx,
                    total=len(shipments),
                    actual_costs_written=stats["actual_costs_written"],
                    estimated_costs_written=stats["estimated_costs_written"],
                    no_cost_match=stats["no_cost_match"],
                )
                if job_id:
                    pct = 15 + int((idx / max(len(shipments), 1)) * 75)
                    set_job_progress(
                        job_id,
                        progress_pct=min(pct, 95),
                        records_processed=idx,
                        message=f"DHL cost sync processed={idx}/{len(shipments)}",
                    )

        conn.commit()
        log.info(
            "dhl_cost_sync.done",
            shipments_selected=stats["shipments_selected"],
            shipments_processed=stats["shipments_processed"],
            actual_costs_written=stats["actual_costs_written"],
            estimated_costs_written=stats["estimated_costs_written"],
            no_cost_match=stats["no_cost_match"],
        )
        if job_id:
            set_job_progress(
                job_id,
                progress_pct=95,
                records_processed=stats["shipments_processed"],
                message="DHL cost sync finished",
            )
        return stats
    finally:
        conn.close()
