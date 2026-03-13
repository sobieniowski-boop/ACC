from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable

from app.core.db_connection import connect_acc
from app.services.dhl_integration import ensure_dhl_schema
from app.services.gls_integration import ensure_gls_schema

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
    source_payload_json: str | None


def _connect():
    return connect_acc(autocommit=False, timeout=30)


def _normalize_token(value: Any) -> str:
    return str(value or "").strip().upper().replace(" ", "")


def _normalize_billing_period(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    return raw.replace(".", "-")


def _normalize_billing_period_filters(values: Iterable[Any] | None) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        token = _normalize_billing_period(value)
        if not token:
            continue
        parts = token.split("-", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid billing period '{value}'")
        year_value = int(parts[0])
        month_value = int(parts[1])
        normalized = f"{year_value:04d}-{month_value:02d}"
        date(year_value, month_value, 1)
        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def _json_dump(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _chunks(items: list[Any], size: int) -> Iterable[list[Any]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


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
    _append_identifier(bucket, kind="piece_id", value=shipment.piece_id, priority=95)
    return sorted(bucket.values(), key=lambda item: (-item.priority, item.kind))


def _load_cost_targets(
    *,
    created_from: date | None,
    created_to: date | None,
    limit_shipments: int,
    refresh_existing: bool,
    billing_periods: list[str] | None = None,
    seeded_only: bool = False,
    only_primary_linked: bool = False,
) -> list[ShipmentCostTarget]:
    # GLS billing import can legitimately leave seeded gls_billing_files shipments
    # without acc_shipment_cost until this sync resolves parcel_number -> billing
    # lines and writes the actual cost row.
    conn = _connect()
    try:
        cur = conn.cursor()
        where = ["s.carrier = 'GLS'"]
        params: list[Any] = []
        billing_period_filters = _normalize_billing_period_filters(billing_periods)
        if created_from:
            where.append("CAST(ISNULL(s.created_at_carrier, s.first_seen_at) AS DATE) >= ?")
            params.append(created_from.isoformat())
        if created_to:
            where.append("CAST(ISNULL(s.created_at_carrier, s.first_seen_at) AS DATE) <= ?")
            params.append(created_to.isoformat())
        if billing_period_filters:
            placeholders = ",".join("?" for _ in billing_period_filters)
            where.append(
                f"""
                REPLACE(
                    LTRIM(RTRIM(ISNULL(JSON_VALUE(s.source_payload_json, '$.billing_period'), ''))),
                    '.',
                    '-'
                ) IN ({placeholders})
                """
            )
            params.extend(billing_period_filters)
        if seeded_only:
            where.append("s.source_system = 'gls_billing_files'")
        if only_primary_linked:
            where.append(
                """
                EXISTS (
                    SELECT 1
                    FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
                    WHERE l.shipment_id = s.id
                      AND l.is_primary = 1
                )
                """
            )
        if not refresh_existing:
            where.append(
                """
                (
                    NOT EXISTS (
                        SELECT 1
                        FROM dbo.acc_shipment_cost c WITH (NOLOCK)
                        WHERE c.shipment_id = s.id
                          AND c.is_estimated = 0
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM dbo.acc_gls_billing_correction_line gc WITH (NOLOCK)
                        WHERE gc.parcel_number IN (
                            NULLIF(UPPER(REPLACE(LTRIM(RTRIM(ISNULL(s.shipment_number, ''))), ' ', '')), ''),
                            NULLIF(UPPER(REPLACE(LTRIM(RTRIM(ISNULL(s.tracking_number, ''))), ' ', '')), ''),
                            NULLIF(UPPER(REPLACE(LTRIM(RTRIM(ISNULL(s.piece_id, ''))), ' ', '')), '')
                        )
                          AND gc.imported_at > ISNULL(
                              (
                                  SELECT MAX(c2.updated_at)
                                  FROM dbo.acc_shipment_cost c2 WITH (NOLOCK)
                                  WHERE c2.shipment_id = s.id
                                    AND c2.is_estimated = 0
                              ),
                              CAST('1900-01-01' AS DATETIME2)
                          )
                    )
                )
                """
            )

        sql = f"""
            SELECT TOP {int(limit_shipments)}
                CAST(s.id AS NVARCHAR(40)) AS shipment_id,
                s.shipment_number,
                s.tracking_number,
                s.piece_id,
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
                source_payload_json=str(row[4]) if row[4] is not None else None,
            )
            for row in rows
        ]
    finally:
        conn.close()


def _query_billing_costs(cur, values: list[str]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    if not values:
        return result
    for batch in _chunks(values, _BATCH_SIZE):
        placeholders = ",".join("?" for _ in batch)
        cur.execute(
            f"""
            SELECT
                l.parcel_number,
                COUNT(*) AS line_count,
                COUNT(DISTINCT l.document_number) AS doc_count,
                MIN(l.document_number) AS first_document_number,
                MAX(l.document_number) AS last_document_number,
                MAX(l.billing_period) AS billing_period,
                MIN(l.row_date) AS first_row_date,
                MAX(l.row_date) AS last_row_date,
                MAX(l.delivery_date) AS last_delivery_date,
                CAST(SUM(ISNULL(l.net_amount, 0)) AS FLOAT) AS net_amount,
                CAST(SUM(ISNULL(l.toll_amount, 0)) AS FLOAT) AS toll_amount,
                CAST(SUM(ISNULL(l.fuel_amount, 0)) AS FLOAT) AS fuel_amount,
                CAST(SUM(ISNULL(l.storewarehouse_amount, 0)) AS FLOAT) AS storewarehouse_amount,
                CAST(SUM(ISNULL(l.surcharge_amount, 0)) AS FLOAT) AS surcharge_amount
            FROM dbo.acc_gls_billing_line l WITH (NOLOCK)
            WHERE l.parcel_number IN ({placeholders})
            GROUP BY l.parcel_number
            """,
            batch,
        )
        for row in cur.fetchall():
            parcel_number = str(row[0] or "").strip()
            if not parcel_number:
                continue
            net_amount = float(row[9] or 0)
            toll_amount = float(row[10] or 0)
            fuel_amount = float(row[11] or 0)
            storewarehouse_amount = float(row[12] or 0)
            surcharge_amount = float(row[13] or 0)
            result[_normalize_token(parcel_number)] = {
                "parcel_number": parcel_number,
                "line_count": int(row[1] or 0),
                "doc_count": int(row[2] or 0),
                "first_document_number": str(row[3] or "") or None,
                "last_document_number": str(row[4] or "") or None,
                "billing_period": _normalize_billing_period(row[5]),
                "first_row_date": row[6],
                "last_row_date": row[7],
                "last_delivery_date": row[8],
                "net_amount": net_amount,
                "toll_amount": toll_amount,
                "fuel_amount": fuel_amount,
                "storewarehouse_amount": storewarehouse_amount,
                "surcharge_amount": surcharge_amount,
                "gross_amount": round(
                    net_amount
                    + toll_amount
                    + fuel_amount
                    + storewarehouse_amount
                    + surcharge_amount,
                    4,
                ),
            }
    corrections = _query_billing_corrections(cur, values)
    return _apply_billing_corrections(result, corrections)


def _query_billing_corrections(cur, values: list[str]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    normalized_values = sorted({_normalize_token(value) for value in values if _normalize_token(value)})
    if not normalized_values:
        return result
    for batch in _chunks(normalized_values, _BATCH_SIZE):
        placeholders = ",".join("?" for _ in batch)
        cur.execute(
            f"""
            WITH ranked AS (
                SELECT
                    parcel_number,
                    document_number,
                    issue_date,
                    sales_date,
                    recipient_name,
                    recipient_postal_code,
                    recipient_city,
                    recipient_country,
                    original_net_amount,
                    corrected_net_amount,
                    net_delta_amount,
                    fuel_rate_pct,
                    fuel_correction_amount,
                    toll_amount,
                    source_file,
                    ROW_NUMBER() OVER (
                        PARTITION BY parcel_number
                        ORDER BY
                            ISNULL(issue_date, CAST('1900-01-01' AS DATE)) DESC,
                            imported_at DESC,
                            source_row_no DESC
                    ) AS rn
                FROM dbo.acc_gls_billing_correction_line WITH (NOLOCK)
                WHERE parcel_number IN ({placeholders})
            )
            SELECT
                parcel_number,
                document_number,
                issue_date,
                sales_date,
                recipient_name,
                recipient_postal_code,
                recipient_city,
                recipient_country,
                original_net_amount,
                corrected_net_amount,
                net_delta_amount,
                fuel_rate_pct,
                fuel_correction_amount,
                toll_amount,
                source_file
            FROM ranked
            WHERE rn = 1
            """,
            batch,
        )
        for row in cur.fetchall():
            key = _normalize_token(row[0])
            if not key:
                continue
            result[key] = {
                "parcel_number": str(row[0] or "") or None,
                "document_number": str(row[1] or "") or None,
                "issue_date": row[2],
                "sales_date": row[3],
                "recipient_name": str(row[4] or "") or None,
                "recipient_postal_code": str(row[5] or "") or None,
                "recipient_city": str(row[6] or "") or None,
                "recipient_country": str(row[7] or "") or None,
                "original_net_amount": float(row[8] or 0),
                "corrected_net_amount": float(row[9] or 0),
                "net_delta_amount": float(row[10] or 0),
                "fuel_rate_pct": float(row[11] or 0),
                "fuel_correction_amount": float(row[12] or 0),
                "toll_amount": float(row[13] or 0) if row[13] is not None else None,
                "source_file": str(row[14] or "") or None,
            }
    return result


def _apply_billing_corrections(
    billing_costs: dict[str, dict[str, Any]],
    corrections: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    if not corrections:
        return billing_costs
    result = {key: dict(value) for key, value in billing_costs.items()}
    for key, correction in corrections.items():
        corrected_net_amount = float(correction.get("corrected_net_amount") or 0)
        fuel_rate_pct = float(correction.get("fuel_rate_pct") or 0)
        toll_amount = (
            float(correction["toll_amount"])
            if correction.get("toll_amount") is not None
            else float(result.get(key, {}).get("toll_amount") or 0)
        )
        base = result.get(
            key,
            {
                "parcel_number": correction.get("parcel_number"),
                "line_count": 0,
                "doc_count": 0,
                "first_document_number": correction.get("document_number"),
                "last_document_number": correction.get("document_number"),
                "billing_period": None,
                "first_row_date": None,
                "last_row_date": None,
                "last_delivery_date": None,
                "net_amount": 0.0,
                "toll_amount": toll_amount,
                "fuel_amount": 0.0,
                "storewarehouse_amount": 0.0,
                "surcharge_amount": 0.0,
                "gross_amount": 0.0,
            },
        )
        storewarehouse_amount = float(base.get("storewarehouse_amount") or 0)
        surcharge_amount = float(base.get("surcharge_amount") or 0)
        corrected_fuel_amount = round(corrected_net_amount * fuel_rate_pct, 4)
        corrected_gross_amount = round(
            corrected_net_amount
            + toll_amount
            + corrected_fuel_amount
            + storewarehouse_amount
            + surcharge_amount,
            4,
        )
        original_gross_amount = float(base.get("gross_amount") or 0)
        original_net_amount = float(base.get("net_amount") or 0)
        result[key] = {
            **base,
            "parcel_number": correction.get("parcel_number") or base.get("parcel_number"),
            "first_document_number": correction.get("document_number") or base.get("first_document_number"),
            "last_document_number": correction.get("document_number") or base.get("last_document_number"),
            "net_amount": corrected_net_amount,
            "toll_amount": toll_amount,
            "fuel_amount": corrected_fuel_amount,
            "gross_amount": corrected_gross_amount,
            "correction_applied": True,
            "correction_document_number": correction.get("document_number"),
            "correction_issue_date": correction.get("issue_date"),
            "correction_sales_date": correction.get("sales_date"),
            "correction_source_file": correction.get("source_file"),
            "correction_original_net_amount": correction.get("original_net_amount") or original_net_amount,
            "correction_net_delta_amount": correction.get("net_delta_amount"),
            "correction_fuel_rate_pct": fuel_rate_pct,
            "correction_fuel_delta_amount": correction.get("fuel_correction_amount"),
            "original_gross_amount": original_gross_amount,
            "correction_delta_gross_amount": round(original_gross_amount - corrected_gross_amount, 4)
            if original_gross_amount
            else None,
        }
    return result


def _select_actual_cost(
    *,
    identifiers: list[ShipmentIdentifier],
    billing_costs: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    for identifier in identifiers:
        key = _normalize_token(identifier.value)
        if not key:
            continue
        billing = billing_costs.get(key)
        if billing:
            return {
                **billing,
                "cost_source": "gls_billing_files",
                "match_kind": identifier.kind,
                "match_value": identifier.value,
                "resolved_via": "parcel_number",
            }
    return None


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


def sync_gls_shipment_costs(
    *,
    created_from: date | None = None,
    created_to: date | None = None,
    limit_shipments: int = 5000,
    refresh_existing: bool = False,
    billing_periods: list[str] | None = None,
    seeded_only: bool = False,
    only_primary_linked: bool = False,
    job_id: str | None = None,
) -> dict[str, Any]:
    ensure_gls_schema()
    ensure_dhl_schema()

    from app.connectors.mssql.mssql_store import set_job_progress

    stats = {
        "shipments_selected": 0,
        "shipments_processed": 0,
        "actual_costs_written": 0,
        "actual_source_billing_files": 0,
        "no_cost_match": 0,
        "billing_periods": _normalize_billing_period_filters(billing_periods),
        "seeded_only": bool(seeded_only),
        "only_primary_linked": bool(only_primary_linked),
    }

    shipments = _load_cost_targets(
        created_from=created_from,
        created_to=created_to,
        limit_shipments=limit_shipments,
        refresh_existing=refresh_existing,
        billing_periods=stats["billing_periods"],
        seeded_only=bool(seeded_only),
        only_primary_linked=bool(only_primary_linked),
    )
    stats["shipments_selected"] = len(shipments)
    if not shipments:
        return stats

    if job_id:
        set_job_progress(job_id, progress_pct=15, records_processed=0, message=f"GLS cost sync count={len(shipments)}")

    shipment_identifiers: dict[str, list[ShipmentIdentifier]] = {}
    unique_values: dict[str, str] = {}
    for shipment in shipments:
        identifiers = _extract_identifiers(shipment)
        shipment_identifiers[shipment.shipment_id] = identifiers
        for identifier in identifiers:
            unique_values.setdefault(_normalize_token(identifier.value), identifier.value)

    conn = _connect()
    try:
        cur = conn.cursor()
        billing_costs = _query_billing_costs(cur, list(unique_values.values()))

        for idx, shipment in enumerate(shipments, start=1):
            identifiers = shipment_identifiers.get(shipment.shipment_id, [])
            actual = _select_actual_cost(identifiers=identifiers, billing_costs=billing_costs)

            if actual:
                invoice_number = (
                    actual.get("first_document_number")
                    if int(actual.get("doc_count") or 0) == 1
                    else None
                )
                payload = {
                    "match_kind": actual["match_kind"],
                    "match_value": actual["match_value"],
                    "resolved_via": actual["resolved_via"],
                    "parcel_number": actual.get("parcel_number"),
                    "doc_count": actual.get("doc_count"),
                    "line_count": actual.get("line_count"),
                    "first_document_number": actual.get("first_document_number"),
                    "last_document_number": actual.get("last_document_number"),
                    "first_row_date": str(actual.get("first_row_date") or "") or None,
                    "last_row_date": str(actual.get("last_row_date") or "") or None,
                    "last_delivery_date": str(actual.get("last_delivery_date") or "") or None,
                    "storewarehouse_amount": actual.get("storewarehouse_amount"),
                    "surcharge_amount": actual.get("surcharge_amount"),
                    "correction_applied": bool(actual.get("correction_applied")),
                    "correction_document_number": actual.get("correction_document_number"),
                    "correction_issue_date": str(actual.get("correction_issue_date") or "") or None,
                    "correction_sales_date": str(actual.get("correction_sales_date") or "") or None,
                    "correction_source_file": actual.get("correction_source_file"),
                    "correction_original_net_amount": actual.get("correction_original_net_amount"),
                    "correction_net_delta_amount": actual.get("correction_net_delta_amount"),
                    "correction_fuel_rate_pct": actual.get("correction_fuel_rate_pct"),
                    "correction_fuel_delta_amount": actual.get("correction_fuel_delta_amount"),
                    "original_gross_amount": actual.get("original_gross_amount"),
                    "correction_delta_gross_amount": actual.get("correction_delta_gross_amount"),
                }
                _upsert_shipment_cost(
                    cur,
                    shipment_id=shipment.shipment_id,
                    cost_source=str(actual["cost_source"]),
                    currency="PLN",
                    net_amount=actual.get("net_amount"),
                    fuel_amount=actual.get("fuel_amount"),
                    toll_amount=actual.get("toll_amount"),
                    gross_amount=actual.get("gross_amount"),
                    invoice_number=invoice_number,
                    invoice_date=None,
                    billing_period=actual.get("billing_period"),
                    is_estimated=False,
                    raw_payload_json=_json_dump(payload),
                )
                _delete_other_actual_rows(cur, shipment_id=shipment.shipment_id, keep_source=str(actual["cost_source"]))
                stats["actual_costs_written"] += 1
                stats["actual_source_billing_files"] += 1
            else:
                stats["no_cost_match"] += 1

            stats["shipments_processed"] += 1

            if idx % 100 == 0:
                conn.commit()
                if job_id:
                    pct = 15 + int((idx / max(len(shipments), 1)) * 75)
                    set_job_progress(
                        job_id,
                        progress_pct=min(pct, 95),
                        records_processed=idx,
                        message=f"GLS cost sync processed={idx}/{len(shipments)}",
                    )

        conn.commit()
        if job_id:
            set_job_progress(
                job_id,
                progress_pct=95,
                records_processed=stats["shipments_processed"],
                message="GLS cost sync finished",
            )
        return stats
    finally:
        conn.close()
