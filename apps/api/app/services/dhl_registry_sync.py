from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import structlog

from app.connectors.dhl24_api import DHL24Client
from app.connectors.dhl24_api.models import (
    DHL24LabelDataResult,
    DHL24PieceShipment,
    DHL24ShipmentBasic,
    DHL24TrackAndTraceResult,
)
from app.core.config import settings
from app.core.db_connection import connect_acc
from app.services.dhl_integration import ensure_dhl_schema

log = structlog.get_logger(__name__)

_MY_SHIPMENTS_PAGE_SIZE = 100
_LABELS_BATCH_SIZE = 25
_DEFAULT_LINK_LOOKBACK_DAYS = 45


@dataclass
class LocalPackageCandidate:
    amazon_order_id: str
    acc_order_id: str | None
    bl_order_id: int | None
    package_order_id: int | None
    courier_package_nr: str | None
    courier_inner_number: str | None


@dataclass
class LocalLinkContext:
    by_tracking: dict[str, list[LocalPackageCandidate]]
    by_inner: dict[str, list[LocalPackageCandidate]]
    by_amazon_ref: dict[str, list[LocalPackageCandidate]]
    by_bl_ref: dict[str, list[LocalPackageCandidate]]


def _connect():
    return connect_acc(autocommit=False, timeout=30)


def _normalize_token(value: Any) -> str:
    return str(value or "").strip().upper().replace(" ", "")


def _json_dump(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _hash_payload(payload: Any) -> str:
    return hashlib.sha256(_json_dump(payload).encode("utf-8")).hexdigest()


def _parse_carrier_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _parse_carrier_date(value: Any) -> date | None:
    dt = _parse_carrier_datetime(value)
    return dt.date() if dt else None


def _is_dhl_package(courier_code: Any, courier_other_name: Any) -> bool:
    code = str(courier_code or "").strip().lower()
    other = str(courier_other_name or "").strip().lower()
    if code == "blconnectpackages":
        code = other
    return "dhl" in code or "dhl" in other


def _latest_track_event(track: DHL24TrackAndTraceResult) -> dict[str, Any] | None:
    latest_payload: dict[str, Any] | None = None
    latest_dt: datetime | None = None
    for event in track.events:
        payload = event.to_dict()
        ts = _parse_carrier_datetime(payload.get("timestamp"))
        if ts is None and latest_payload is None:
            latest_payload = payload
            continue
        if ts is not None and (latest_dt is None or ts > latest_dt):
            latest_dt = ts
            latest_payload = payload
    return latest_payload


def _is_delivered(track: DHL24TrackAndTraceResult, order_status: str | None) -> tuple[bool, datetime | None]:
    delivered_at: datetime | None = None
    delivered = False
    for event in track.events:
        payload = event.to_dict()
        code = str(payload.get("status") or "").strip().upper()
        desc = str(payload.get("description") or "").strip().lower()
        if code in {"DOR", "DELIVERED"} or "deliv" in desc or "dorecz" in desc:
            delivered = True
            event_dt = _parse_carrier_datetime(payload.get("timestamp"))
            if event_dt and (delivered_at is None or event_dt > delivered_at):
                delivered_at = event_dt
    status_text = str(order_status or "").strip().lower()
    if not delivered and ("deliv" in status_text or "dorecz" in status_text):
        delivered = True
    return delivered, delivered_at


def _load_local_link_context(*, created_from: date, created_to: date) -> LocalLinkContext:
    purchase_from = created_from - timedelta(days=_DEFAULT_LINK_LOOKBACK_DAYS)
    purchase_to = created_to + timedelta(days=7)
    by_tracking: dict[str, list[LocalPackageCandidate]] = {}
    by_inner: dict[str, list[LocalPackageCandidate]] = {}
    by_amazon_ref: dict[str, list[LocalPackageCandidate]] = {}
    by_bl_ref: dict[str, list[LocalPackageCandidate]] = {}

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
WITH base_orders AS (
    SELECT
        CAST(o.id AS NVARCHAR(40)) AS acc_order_id,
        o.amazon_order_id,
        bo.order_id AS bl_order_id
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
      ON bo.external_order_id = o.amazon_order_id
    WHERE o.fulfillment_channel = 'MFN'
      AND CAST(o.purchase_date AS DATE) >= ?
      AND CAST(o.purchase_date AS DATE) <= ?
),
direct_packages AS (
    SELECT
        b.acc_order_id,
        b.amazon_order_id,
        b.bl_order_id,
        p.order_id AS package_order_id,
        p.courier_package_nr,
        p.courier_inner_number,
        p.courier_code,
        p.courier_other_name
    FROM base_orders b
    JOIN dbo.acc_cache_packages p WITH (NOLOCK)
      ON p.order_id = b.bl_order_id
),
dis_packages AS (
    SELECT
        b.acc_order_id,
        b.amazon_order_id,
        b.bl_order_id,
        p.order_id AS package_order_id,
        p.courier_package_nr,
        p.courier_inner_number,
        p.courier_code,
        p.courier_other_name
    FROM base_orders b
    JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
      ON dm.holding_order_id = b.bl_order_id
    JOIN dbo.acc_cache_packages p WITH (NOLOCK)
      ON p.order_id = dm.dis_order_id
)
SELECT DISTINCT
    x.acc_order_id,
    x.amazon_order_id,
    x.bl_order_id,
    x.package_order_id,
    x.courier_package_nr,
    x.courier_inner_number,
    x.courier_code,
    x.courier_other_name
FROM (
    SELECT * FROM direct_packages
    UNION ALL
    SELECT * FROM dis_packages
) x
            """,
            [purchase_from.isoformat(), purchase_to.isoformat()],
        )
        for row in cur.fetchall():
            if not _is_dhl_package(row[6], row[7]):
                continue
            candidate = LocalPackageCandidate(
                amazon_order_id=str(row[1] or ""),
                acc_order_id=str(row[0]) if row[0] else None,
                bl_order_id=int(row[2]) if row[2] is not None else None,
                package_order_id=int(row[3]) if row[3] is not None else None,
                courier_package_nr=str(row[4] or "") or None,
                courier_inner_number=str(row[5] or "") or None,
            )
            if not candidate.amazon_order_id:
                continue
            by_amazon_ref.setdefault(_normalize_token(candidate.amazon_order_id), []).append(candidate)
            if candidate.bl_order_id is not None:
                by_bl_ref.setdefault(str(candidate.bl_order_id), []).append(candidate)
            tracking_key = _normalize_token(candidate.courier_package_nr)
            inner_key = _normalize_token(candidate.courier_inner_number)
            if tracking_key:
                by_tracking.setdefault(tracking_key, []).append(candidate)
            if inner_key:
                by_inner.setdefault(inner_key, []).append(candidate)
    finally:
        conn.close()

    return LocalLinkContext(
        by_tracking=by_tracking,
        by_inner=by_inner,
        by_amazon_ref=by_amazon_ref,
        by_bl_ref=by_bl_ref,
    )


def _append_candidate(
    bucket: dict[tuple[str, str, str], dict[str, Any]],
    *,
    candidate: LocalPackageCandidate,
    link_method: str,
    confidence: float,
) -> None:
    key = (candidate.amazon_order_id, link_method, candidate.acc_order_id or "")
    existing = bucket.get(key)
    payload = {
        "amazon_order_id": candidate.amazon_order_id,
        "acc_order_id": candidate.acc_order_id,
        "bl_order_id": candidate.bl_order_id,
        "link_method": link_method,
        "link_confidence": confidence,
        "is_primary": False,
    }
    if existing is None or confidence > float(existing.get("link_confidence") or 0):
        bucket[key] = payload


def _collect_link_candidates(
    *,
    basic: DHL24ShipmentBasic,
    label_data: DHL24LabelDataResult | None,
    piece_shipments: list[DHL24PieceShipment],
    local_ctx: LocalLinkContext,
) -> list[dict[str, Any]]:
    bucket: dict[tuple[str, str, str], dict[str, Any]] = {}

    def match_by_tracking(value: Any, method: str, confidence: float) -> None:
        for candidate in local_ctx.by_tracking.get(_normalize_token(value), []):
            _append_candidate(bucket, candidate=candidate, link_method=method, confidence=confidence)

    def match_by_inner(value: Any, method: str, confidence: float) -> None:
        for candidate in local_ctx.by_inner.get(_normalize_token(value), []):
            _append_candidate(bucket, candidate=candidate, link_method=method, confidence=confidence)

    if label_data:
        reference = _normalize_token(label_data.reference)
        if reference:
            for candidate in local_ctx.by_amazon_ref.get(reference, []):
                _append_candidate(bucket, candidate=candidate, link_method="reference_amazon_order_id", confidence=1.0)
            for candidate in local_ctx.by_bl_ref.get(reference, []):
                _append_candidate(bucket, candidate=candidate, link_method="reference_bl_order_id", confidence=0.99)

        match_by_tracking(label_data.primary_waybill_number, "label_primary_waybill", 0.97)
        match_by_tracking(label_data.dispatch_notification_number, "label_dispatch_number", 0.9)
        for piece in label_data.pieces:
            match_by_tracking(piece.routing_barcode, "label_routing_barcode", 0.95)
            match_by_inner(piece.blp_piece_id, "label_blp_piece_id", 0.93)
            match_by_tracking(piece.blp_piece_id, "label_blp_piece_tracking_fallback", 0.88)

    for piece_shipment in piece_shipments:
        match_by_tracking(piece_shipment.cedex_number, "piece_cedex_tracking", 0.84)
        for package in piece_shipment.packages:
            match_by_tracking(package.package_number, "piece_package_tracking", 0.92)
            match_by_inner(package.package_number, "piece_package_inner", 0.9)

    candidates = list(bucket.values())
    if not candidates:
        return candidates

    best_confidence = max(float(item.get("link_confidence") or 0) for item in candidates)
    best_orders = {
        str(item.get("amazon_order_id") or "")
        for item in candidates
        if float(item.get("link_confidence") or 0) == best_confidence
    }
    if len(best_orders) == 1:
        primary_order = next(iter(best_orders))
        primary_set = False
        for item in candidates:
            if not primary_set and str(item.get("amazon_order_id") or "") == primary_order and float(item.get("link_confidence") or 0) == best_confidence:
                item["is_primary"] = True
                primary_set = True
    return candidates


def _upsert_shipment(cur, payload: dict[str, Any]) -> str:
    cur.execute(
        """
        SELECT CAST(id AS NVARCHAR(40))
        FROM dbo.acc_shipment WITH (NOLOCK)
        WHERE carrier = ? AND shipment_number = ?
        """,
        [payload["carrier"], payload["shipment_number"]],
    )
    row = cur.fetchone()
    if row:
        shipment_row_id = str(row[0])
        cur.execute(
            """
            UPDATE dbo.acc_shipment
            SET carrier_account = ?,
                piece_id = ?,
                tracking_number = ?,
                cedex_number = ?,
                service_code = ?,
                ship_date = ?,
                created_at_carrier = ?,
                status_code = ?,
                status_label = ?,
                received_by = ?,
                is_delivered = ?,
                delivered_at = ?,
                recipient_name = ?,
                recipient_country = ?,
                shipper_name = ?,
                shipper_country = ?,
                source_system = COALESCE(?, source_system),
                source_payload_json = ?,
                source_payload_hash = ?,
                last_seen_at = SYSUTCDATETIME(),
                last_sync_at = SYSUTCDATETIME()
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            [
                payload.get("carrier_account"),
                payload.get("piece_id"),
                payload.get("tracking_number"),
                payload.get("cedex_number"),
                payload.get("service_code"),
                payload.get("ship_date"),
                payload.get("created_at_carrier"),
                payload.get("status_code"),
                payload.get("status_label"),
                payload.get("received_by"),
                1 if payload.get("is_delivered") else 0,
                payload.get("delivered_at"),
                payload.get("recipient_name"),
                payload.get("recipient_country"),
                payload.get("shipper_name"),
                payload.get("shipper_country"),
                payload.get("source_system"),
                payload.get("source_payload_json"),
                payload.get("source_payload_hash"),
                shipment_row_id,
            ],
        )
        return shipment_row_id

    shipment_row_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO dbo.acc_shipment (
            id, carrier, carrier_account, shipment_number, piece_id, tracking_number, cedex_number,
            service_code, ship_date, created_at_carrier, status_code, status_label, received_by,
            is_delivered, delivered_at, recipient_name, recipient_country, shipper_name,
            shipper_country, source_system, source_payload_json, source_payload_hash,
            first_seen_at, last_seen_at, last_sync_at
        )
        VALUES (
            CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME(), SYSUTCDATETIME()
        )
        """,
        [
            shipment_row_id,
            payload["carrier"],
            payload.get("carrier_account"),
            payload["shipment_number"],
            payload.get("piece_id"),
            payload.get("tracking_number"),
            payload.get("cedex_number"),
            payload.get("service_code"),
            payload.get("ship_date"),
            payload.get("created_at_carrier"),
            payload.get("status_code"),
            payload.get("status_label"),
            payload.get("received_by"),
            1 if payload.get("is_delivered") else 0,
            payload.get("delivered_at"),
            payload.get("recipient_name"),
            payload.get("recipient_country"),
            payload.get("shipper_name"),
            payload.get("shipper_country"),
            payload.get("source_system") or "dhl_webapi2",
            payload.get("source_payload_json"),
            payload.get("source_payload_hash"),
        ],
    )
    return shipment_row_id


def _upsert_shipment_links(cur, *, shipment_id: str, links: list[dict[str, Any]]) -> int:
    written = 0
    for link in links:
        cur.execute(
            """
            SELECT CAST(id AS NVARCHAR(40))
            FROM dbo.acc_shipment_order_link WITH (NOLOCK)
            WHERE shipment_id = CAST(? AS UNIQUEIDENTIFIER)
              AND amazon_order_id = ?
              AND link_method = ?
            """,
            [shipment_id, link.get("amazon_order_id"), link.get("link_method")],
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                """
                UPDATE dbo.acc_shipment_order_link
                SET acc_order_id = CASE WHEN ? IS NULL OR ? = '' THEN acc_order_id ELSE CAST(? AS UNIQUEIDENTIFIER) END,
                    bl_order_id = COALESCE(?, bl_order_id),
                    link_confidence = ?,
                    is_primary = ?,
                    updated_at = SYSUTCDATETIME()
                WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                """,
                [
                    link.get("acc_order_id"),
                    link.get("acc_order_id"),
                    link.get("acc_order_id"),
                    link.get("bl_order_id"),
                    link.get("link_confidence"),
                    1 if link.get("is_primary") else 0,
                    str(row[0]),
                ],
            )
        else:
            cur.execute(
                """
                INSERT INTO dbo.acc_shipment_order_link (
                    id, shipment_id, amazon_order_id, acc_order_id, bl_order_id,
                    link_method, link_confidence, is_primary, created_at, updated_at
                )
                VALUES (
                    CAST(? AS UNIQUEIDENTIFIER), CAST(? AS UNIQUEIDENTIFIER), ?, 
                    CASE WHEN ? IS NULL OR ? = '' THEN NULL ELSE CAST(? AS UNIQUEIDENTIFIER) END,
                    ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME()
                )
                """,
                [
                    str(uuid.uuid4()),
                    shipment_id,
                    link.get("amazon_order_id"),
                    link.get("acc_order_id"),
                    link.get("acc_order_id"),
                    link.get("acc_order_id"),
                    link.get("bl_order_id"),
                    link.get("link_method"),
                    link.get("link_confidence"),
                    1 if link.get("is_primary") else 0,
                ],
            )
        written += 1
    return written


def _upsert_tracking_events(cur, *, shipment_id: str, track: DHL24TrackAndTraceResult) -> int:
    written = 0
    for event in track.events:
        payload = event.to_dict()
        event_at = _parse_carrier_datetime(payload.get("timestamp"))
        event_code = payload.get("status")
        event_label = payload.get("description")
        event_terminal = payload.get("terminal")
        cur.execute(
            """
            IF NOT EXISTS (
                SELECT 1
                FROM dbo.acc_shipment_event WITH (NOLOCK)
                WHERE shipment_id = CAST(? AS UNIQUEIDENTIFIER)
                  AND ISNULL(event_code, '') = ISNULL(?, '')
                  AND ISNULL(event_label, '') = ISNULL(?, '')
                  AND (
                        (event_at IS NULL AND ? IS NULL)
                        OR event_at = ?
                      )
            )
            BEGIN
                INSERT INTO dbo.acc_shipment_event (
                    shipment_id, event_code, event_label, event_terminal,
                    event_at, location_city, location_country, raw_payload_json, created_at
                )
                VALUES (
                    CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, NULL, ?, SYSUTCDATETIME()
                )
            END
            """,
            [
                shipment_id,
                event_code,
                event_label,
                event_at,
                event_at,
                shipment_id,
                event_code,
                event_label,
                event_terminal,
                event_at,
                event_terminal,
                _json_dump(payload),
            ],
        )
        written += 1
    return written


def _build_shipment_payload(
    *,
    basic: DHL24ShipmentBasic,
    label_data: DHL24LabelDataResult | None,
    piece_shipments: list[DHL24PieceShipment],
    track: DHL24TrackAndTraceResult | None,
) -> dict[str, Any]:
    latest_event = _latest_track_event(track) if track else None
    delivered, delivered_at = _is_delivered(track, basic.order_status) if track else (False, None)

    primary_waybill = label_data.primary_waybill_number if label_data else None
    label_piece_id = None
    label_routing = None
    if label_data and label_data.pieces:
        label_piece_id = label_data.pieces[0].blp_piece_id
        label_routing = label_data.pieces[0].routing_barcode

    piece_package_number = None
    piece_cedex = None
    if piece_shipments:
        piece_cedex = piece_shipments[0].cedex_number
        if piece_shipments[0].packages:
            piece_package_number = piece_shipments[0].packages[0].package_number

    raw_payload = {
        "basic": basic.to_dict(),
        "label_data": label_data.to_dict() if label_data else None,
        "piece_shipments": [item.to_dict() for item in piece_shipments],
        "track": track.to_dict() if track else None,
    }

    return {
        "carrier": "DHL",
        "carrier_account": settings.DHL24_API_USERNAME or None,
        "shipment_number": basic.shipment_id,
        "piece_id": label_piece_id or piece_package_number,
        "tracking_number": primary_waybill or label_routing or piece_package_number,
        "cedex_number": piece_cedex,
        "service_code": label_data.service_product if label_data else None,
        "ship_date": _parse_carrier_date(basic.created),
        "created_at_carrier": _parse_carrier_datetime(basic.created),
        "status_code": (latest_event or {}).get("status") or basic.order_status,
        "status_label": (latest_event or {}).get("description") or basic.order_status,
        "received_by": track.received_by if track else None,
        "is_delivered": delivered,
        "delivered_at": delivered_at,
        "recipient_name": (label_data.receiver_name if label_data else None) or basic.receiver_name,
        "recipient_country": label_data.receiver_country if label_data else None,
        "shipper_name": (label_data.shipper_name if label_data else None) or basic.shipper_name,
        "shipper_country": label_data.shipper_country if label_data else None,
        "source_payload_json": _json_dump(raw_payload),
        "source_payload_hash": _hash_payload(raw_payload),
    }


def _load_labels_data(client: DHL24Client, shipment_ids: list[str]) -> dict[str, DHL24LabelDataResult]:
    result: dict[str, DHL24LabelDataResult] = {}
    for idx in range(0, len(shipment_ids), _LABELS_BATCH_SIZE):
        batch = shipment_ids[idx : idx + _LABELS_BATCH_SIZE]
        for item in client.get_labels_data(batch):
            result[item.shipment_id] = item
    return result


def backfill_dhl_shipments(
    *,
    created_from: date,
    created_to: date,
    include_events: bool = True,
    limit_shipments: int | None = None,
    job_id: str | None = None,
) -> dict[str, Any]:
    ensure_dhl_schema()
    client = DHL24Client()
    if not client.is_configured:
        raise RuntimeError("DHL24 API not configured")

    from app.connectors.mssql.mssql_store import set_job_progress

    total = client.get_my_shipments_count(created_from=created_from, created_to=created_to)
    target_total = min(total, int(limit_shipments or total))
    local_ctx = _load_local_link_context(created_from=created_from, created_to=created_to)
    labels_map: dict[str, DHL24LabelDataResult] = {}
    stats = {
        "created_from": created_from.isoformat(),
        "created_to": created_to.isoformat(),
        "remote_total": total,
        "planned_total": target_total,
        "shipments_processed": 0,
        "shipments_inserted_or_updated": 0,
        "labels_data_hits": 0,
        "piece_lookup_hits": 0,
        "events_written": 0,
        "links_written": 0,
        "shipments_linked": 0,
        "shipments_unlinked": 0,
        "delivered_shipments": 0,
    }

    if job_id:
        set_job_progress(job_id, progress_pct=15, records_processed=0, message=f"DHL backfill count={target_total}")

    processed = 0
    offset = 0
    conn = _connect()
    try:
        cur = conn.cursor()
        while processed < target_total:
            page = client.get_my_shipments(created_from=created_from, created_to=created_to, offset=offset)
            if not page:
                break
            remaining = target_total - processed
            current_page = page[:remaining]
            labels_map.update(_load_labels_data(client, [item.shipment_id for item in current_page]))

            for basic in current_page:
                label_data = labels_map.get(basic.shipment_id)
                if label_data:
                    stats["labels_data_hits"] += 1

                piece_shipments: list[DHL24PieceShipment] = []
                needs_piece_lookup = not label_data or (
                    not label_data.primary_waybill_number and not any(piece.blp_piece_id or piece.routing_barcode for piece in label_data.pieces)
                )
                if needs_piece_lookup:
                    try:
                        piece_shipments = client.get_piece_id(shipment_number=basic.shipment_id)
                    except Exception:
                        piece_shipments = []
                    if piece_shipments:
                        stats["piece_lookup_hits"] += 1

                track = None
                if include_events:
                    track = client.get_track_and_trace_info(basic.shipment_id)

                shipment_payload = _build_shipment_payload(
                    basic=basic,
                    label_data=label_data,
                    piece_shipments=piece_shipments,
                    track=track,
                )
                shipment_row_id = _upsert_shipment(cur, shipment_payload)
                stats["shipments_inserted_or_updated"] += 1

                link_candidates = _collect_link_candidates(
                    basic=basic,
                    label_data=label_data,
                    piece_shipments=piece_shipments,
                    local_ctx=local_ctx,
                )
                if link_candidates:
                    stats["links_written"] += _upsert_shipment_links(cur, shipment_id=shipment_row_id, links=link_candidates)
                    stats["shipments_linked"] += 1
                else:
                    stats["shipments_unlinked"] += 1

                if include_events and track:
                    stats["events_written"] += _upsert_tracking_events(cur, shipment_id=shipment_row_id, track=track)
                if shipment_payload.get("is_delivered"):
                    stats["delivered_shipments"] += 1

                processed += 1
                stats["shipments_processed"] = processed

                if processed % 25 == 0:
                    conn.commit()
                    if job_id and target_total:
                        pct = 15 + int((processed / max(target_total, 1)) * 75)
                        set_job_progress(
                            job_id,
                            progress_pct=min(pct, 95),
                            records_processed=processed,
                            message=f"DHL backfill processed={processed}/{target_total}",
                        )

            offset += _MY_SHIPMENTS_PAGE_SIZE
            if len(page) < _MY_SHIPMENTS_PAGE_SIZE:
                break

        conn.commit()
        if job_id:
            set_job_progress(job_id, progress_pct=95, records_processed=processed, message="DHL backfill finished")
        return stats
    finally:
        conn.close()


def sync_dhl_tracking_events(
    *,
    created_from: date | None = None,
    created_to: date | None = None,
    limit_shipments: int = 500,
    job_id: str | None = None,
) -> dict[str, Any]:
    ensure_dhl_schema()
    client = DHL24Client()
    if not client.is_configured:
        raise RuntimeError("DHL24 API not configured")

    from app.connectors.mssql.mssql_store import set_job_progress

    stats = {
        "shipments_selected": 0,
        "shipments_processed": 0,
        "events_written": 0,
        "delivered_shipments": 0,
    }
    conn = _connect()
    try:
        cur = conn.cursor()
        where = ["carrier = 'DHL'"]
        params: list[Any] = []
        if created_from:
            where.append("CAST(ISNULL(created_at_carrier, first_seen_at) AS DATE) >= ?")
            params.append(created_from.isoformat())
        if created_to:
            where.append("CAST(ISNULL(created_at_carrier, first_seen_at) AS DATE) <= ?")
            params.append(created_to.isoformat())
        sql = f"""
            SELECT TOP {int(limit_shipments)}
                CAST(id AS NVARCHAR(40)) AS id,
                shipment_number,
                status_code
            FROM dbo.acc_shipment WITH (NOLOCK)
            WHERE {' AND '.join(where)}
            ORDER BY last_sync_at ASC, first_seen_at ASC
        """
        cur.execute(sql, params)
        rows = cur.fetchall()
        stats["shipments_selected"] = len(rows)

        for idx, row in enumerate(rows, start=1):
            shipment_row_id = str(row[0])
            shipment_number = str(row[1] or "")
            if not shipment_number:
                continue
            track = client.get_track_and_trace_info(shipment_number)
            stats["events_written"] += _upsert_tracking_events(cur, shipment_id=shipment_row_id, track=track)
            latest_event = _latest_track_event(track)
            delivered, delivered_at = _is_delivered(track, str(row[2] or ""))
            cur.execute(
                """
                UPDATE dbo.acc_shipment
                SET status_code = ?,
                    status_label = ?,
                    received_by = ?,
                    is_delivered = ?,
                    delivered_at = ?,
                    last_sync_at = SYSUTCDATETIME(),
                    last_seen_at = SYSUTCDATETIME()
                WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                """,
                [
                    (latest_event or {}).get("status"),
                    (latest_event or {}).get("description"),
                    track.received_by,
                    1 if delivered else 0,
                    delivered_at,
                    shipment_row_id,
                ],
            )
            stats["shipments_processed"] += 1
            if delivered:
                stats["delivered_shipments"] += 1

            if idx % 25 == 0:
                conn.commit()
                if job_id and rows:
                    pct = 15 + int((idx / max(len(rows), 1)) * 75)
                    set_job_progress(
                        job_id,
                        progress_pct=min(pct, 95),
                        records_processed=idx,
                        message=f"DHL event sync processed={idx}/{len(rows)}",
                    )

        conn.commit()
        return stats
    finally:
        conn.close()
