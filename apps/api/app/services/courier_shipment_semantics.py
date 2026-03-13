from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any

from app.core.db_connection import connect_acc
from app.services.bl_distribution_cache import ensure_bl_distribution_cache_schema
from app.services.courier_order_relations import _ascii_text, _month_start, _next_month, _normalize_carriers, _normalize_months, _to_float
from app.services.dhl_integration import ensure_dhl_schema

_CLASSIFIER_VERSION = "courier_semantics_v1"
_RETURN_KEYWORDS = (
    "return to sender",
    "returned",
    "zwrot",
    "odeslan",
    "return",
    "sender",
)
_FAILED_KEYWORDS = (
    "refused",
    "odmowa",
    "nieodebr",
    "unclaimed",
    "not collected",
    "failed",
    "cannot deliver",
    "adresat",
)
_DELIVERED_KEYWORDS = (
    "delivered",
    "dostarcz",
    "dorecz",
    "odebran",
    "proof of delivery",
)
_IN_TRANSIT_KEYWORDS = (
    "in transit",
    "transit",
    "sorting",
    "out for delivery",
    "w doreczeniu",
    "route",
)


def _connect():
    return connect_acc(autocommit=False, timeout=90)


def _load_shipment_rows(
    cur,
    *,
    carrier: str,
    month_start_value: date,
    month_end_value: date,
) -> list[tuple[Any, ...]]:
    cur.execute(
        """
IF OBJECT_ID('tempdb..#shipment_scope') IS NOT NULL DROP TABLE #shipment_scope;
IF OBJECT_ID('tempdb..#scope_bl_orders') IS NOT NULL DROP TABLE #scope_bl_orders;
IF OBJECT_ID('tempdb..#scope_package_candidates') IS NOT NULL DROP TABLE #scope_package_candidates;
IF OBJECT_ID('tempdb..#scope_relation_candidates') IS NOT NULL DROP TABLE #scope_relation_candidates;
IF OBJECT_ID('tempdb..#scope_event_text') IS NOT NULL DROP TABLE #scope_event_text;

CREATE TABLE #shipment_scope (
    shipment_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    carrier NVARCHAR(16) NOT NULL,
    ship_month DATE NOT NULL,
    amazon_order_id NVARCHAR(80) NULL,
    acc_order_id NVARCHAR(40) NULL,
    bl_order_id BIGINT NULL,
    primary_link_method NVARCHAR(64) NULL,
    status_code NVARCHAR(64) NULL,
    status_label NVARCHAR(255) NULL,
    is_delivered BIT NOT NULL
);

CREATE INDEX IX_shipment_scope_bl_order_id ON #shipment_scope(bl_order_id);
CREATE INDEX IX_shipment_scope_order_ref ON #shipment_scope(amazon_order_id, bl_order_id);

CREATE TABLE #scope_bl_orders (
    bl_order_id BIGINT NOT NULL PRIMARY KEY
);

CREATE TABLE #scope_package_candidates (
    bl_order_id BIGINT NOT NULL,
    tracking_status NVARCHAR(255) NULL,
    package_type NVARCHAR(255) NULL,
    is_return BIT NULL,
    match_rank INT NOT NULL,
    package_id BIGINT NOT NULL
);

CREATE TABLE #scope_relation_candidates (
    shipment_id UNIQUEIDENTIFIER NOT NULL,
    relation_type NVARCHAR(32) NULL,
    relation_confidence FLOAT NULL,
    updated_at DATETIME2 NULL
);

CREATE TABLE #scope_event_text (
    shipment_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    event_text NVARCHAR(MAX) NULL
);
        """
    )

    cur.execute(
        """
INSERT INTO #shipment_scope (
    shipment_id, carrier, ship_month, amazon_order_id, acc_order_id, bl_order_id,
    primary_link_method, status_code, status_label, is_delivered
)
SELECT
    s.id,
    s.carrier,
    ?,
    l.amazon_order_id,
    CAST(l.acc_order_id AS NVARCHAR(40)) AS acc_order_id,
    l.bl_order_id,
    l.link_method,
    s.status_code,
    s.status_label,
    s.is_delivered
FROM dbo.acc_shipment s WITH (NOLOCK)
LEFT JOIN dbo.acc_shipment_order_link l WITH (NOLOCK)
  ON l.shipment_id = s.id
 AND l.is_primary = 1
WHERE s.carrier = ?
  AND CAST(
        COALESCE(
            s.ship_date,
            CAST(s.created_at_carrier AS DATE),
            CAST(s.first_seen_at AS DATE)
        ) AS DATE
      ) >= ?
  AND CAST(
        COALESCE(
            s.ship_date,
            CAST(s.created_at_carrier AS DATE),
            CAST(s.first_seen_at AS DATE)
        ) AS DATE
      ) < ?
        """,
        [month_start_value.isoformat(), carrier, month_start_value.isoformat(), month_end_value.isoformat()],
    )

    cur.execute(
        """
INSERT INTO #scope_bl_orders (bl_order_id)
SELECT DISTINCT bl_order_id
FROM #shipment_scope
WHERE bl_order_id IS NOT NULL
        """
    )

    cur.execute(
        """
INSERT INTO #scope_package_candidates (
    bl_order_id, tracking_status, package_type, is_return, match_rank, package_id
)
SELECT
    sbo.bl_order_id,
    dp.tracking_status,
    dp.package_type,
    dp.is_return,
    0 AS match_rank,
    CAST(dp.package_id AS BIGINT) AS package_id
FROM #scope_bl_orders sbo
JOIN dbo.acc_bl_distribution_package_cache dp WITH (NOLOCK)
  ON dp.order_id = sbo.bl_order_id
WHERE dp.package_id IS NOT NULL

UNION ALL

SELECT
    sbo.bl_order_id,
    dp.tracking_status,
    dp.package_type,
    dp.is_return,
    1 AS match_rank,
    CAST(dp.package_id AS BIGINT) AS package_id
FROM #scope_bl_orders sbo
JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
  ON dm.holding_order_id = sbo.bl_order_id
JOIN dbo.acc_bl_distribution_package_cache dp WITH (NOLOCK)
  ON dp.order_id = dm.dis_order_id
WHERE dp.package_id IS NOT NULL
        """
    )

    cur.execute(
        """
INSERT INTO #scope_relation_candidates (
    shipment_id, relation_type, relation_confidence, updated_at
)
SELECT
    sc.shipment_id,
    r.relation_type,
    CAST(r.confidence AS FLOAT) AS relation_confidence,
    r.updated_at
FROM #shipment_scope sc
JOIN dbo.acc_order_courier_relation r WITH (NOLOCK)
  ON r.carrier = sc.carrier
 AND r.is_strong = 1
 AND r.source_amazon_order_id = sc.amazon_order_id
 AND r.related_bl_order_id = sc.bl_order_id
WHERE sc.amazon_order_id IS NOT NULL
  AND sc.bl_order_id IS NOT NULL

UNION ALL

SELECT
    sc.shipment_id,
    r.relation_type,
    CAST(r.confidence AS FLOAT) AS relation_confidence,
    r.updated_at
FROM #shipment_scope sc
JOIN dbo.acc_order_courier_relation r WITH (NOLOCK)
  ON r.carrier = sc.carrier
 AND r.is_strong = 1
 AND r.source_amazon_order_id = sc.amazon_order_id
 AND r.related_distribution_order_id = sc.bl_order_id
WHERE sc.amazon_order_id IS NOT NULL
  AND sc.bl_order_id IS NOT NULL
        """
    )

    cur.execute(
        """
INSERT INTO #scope_event_text (shipment_id, event_text)
SELECT
    se.shipment_id,
    STRING_AGG(LOWER(ISNULL(se.event_label, '')), ' | ') AS event_text
FROM dbo.acc_shipment_event se WITH (NOLOCK)
JOIN #shipment_scope sc
  ON sc.shipment_id = se.shipment_id
GROUP BY se.shipment_id
        """
    )

    cur.execute(
        """
WITH best_package AS (
    SELECT
        bl_order_id,
        tracking_status,
        package_type,
        is_return,
        ROW_NUMBER() OVER (
            PARTITION BY bl_order_id
            ORDER BY match_rank ASC, package_id DESC
        ) AS rn
    FROM #scope_package_candidates
),
best_relation AS (
    SELECT
        shipment_id,
        relation_type,
        relation_confidence,
        ROW_NUMBER() OVER (
            PARTITION BY shipment_id
            ORDER BY relation_confidence DESC, updated_at DESC
        ) AS rn
    FROM #scope_relation_candidates
)
SELECT
    CAST(sc.shipment_id AS NVARCHAR(36)) AS shipment_id,
    sc.amazon_order_id,
    sc.acc_order_id,
    sc.bl_order_id,
    sc.primary_link_method,
    sc.status_code,
    sc.status_label,
    sc.is_delivered,
    pkg.tracking_status,
    pkg.package_type,
    pkg.is_return,
    ev.event_text,
    rel.relation_type,
    rel.relation_confidence
FROM #shipment_scope sc
LEFT JOIN best_package pkg
  ON pkg.bl_order_id = sc.bl_order_id
 AND pkg.rn = 1
LEFT JOIN #scope_event_text ev
  ON ev.shipment_id = sc.shipment_id
LEFT JOIN best_relation rel
  ON rel.shipment_id = sc.shipment_id
 AND rel.rn = 1
ORDER BY sc.shipment_id
        """
    )
    return cur.fetchall()


@dataclass(frozen=True)
class ShipmentSemanticsInput:
    carrier: str
    amazon_order_id: str | None
    acc_order_id: str | None
    bl_order_id: int | None
    primary_link_method: str | None
    status_code: str | None
    status_label: str | None
    is_delivered: bool
    tracking_status: str | None
    package_type: str | None
    package_is_return: bool
    event_text: str | None
    relation_type: str | None
    relation_confidence: float


def _classify_shipment_semantics(payload: ShipmentSemanticsInput) -> dict[str, Any]:
    status_text = _ascii_text(
        " ".join(
            [
                str(payload.status_code or ""),
                str(payload.status_label or ""),
                str(payload.tracking_status or ""),
                str(payload.package_type or ""),
                str(payload.event_text or ""),
            ]
        )
    )

    if payload.package_is_return or any(keyword in status_text for keyword in _RETURN_KEYWORDS):
        outcome_code = "return_to_sender"
        outcome_confidence = 0.96 if payload.package_is_return else 0.92
    elif any(keyword in status_text for keyword in _FAILED_KEYWORDS):
        outcome_code = "failed_delivery"
        outcome_confidence = 0.91
    elif payload.is_delivered or any(keyword in status_text for keyword in _DELIVERED_KEYWORDS):
        outcome_code = "delivered"
        outcome_confidence = 0.99 if payload.is_delivered else 0.94
    elif any(keyword in status_text for keyword in _IN_TRANSIT_KEYWORDS) or status_text:
        outcome_code = "in_transit"
        outcome_confidence = 0.80 if status_text else 0.60
    else:
        outcome_code = "unknown"
        outcome_confidence = 0.50

    if payload.relation_type == "replacement_order" and payload.relation_confidence >= 0.95:
        cost_reason = "replacement_shipment"
        cost_reason_confidence = 0.98
    elif payload.relation_type == "reshipment" and payload.relation_confidence >= 0.95:
        cost_reason = "supplemental_reshipment"
        cost_reason_confidence = 0.97
    elif outcome_code in {"return_to_sender", "failed_delivery"}:
        cost_reason = "failed_delivery_or_return"
        cost_reason_confidence = 0.94
    elif payload.amazon_order_id:
        cost_reason = "primary_delivery"
        cost_reason_confidence = 0.90
    else:
        cost_reason = "unknown"
        cost_reason_confidence = 0.50

    return {
        "outcome_code": outcome_code,
        "outcome_confidence": round(outcome_confidence, 4),
        "cost_reason": cost_reason,
        "cost_reason_confidence": round(cost_reason_confidence, 4),
        "evidence": {
            "classifier_version": _CLASSIFIER_VERSION,
            "status_text": status_text,
            "package_is_return": bool(payload.package_is_return),
            "relation_type": payload.relation_type,
            "relation_confidence": round(payload.relation_confidence, 4),
            "primary_link_method": payload.primary_link_method,
        },
    }


def refresh_courier_shipment_outcomes(
    *,
    months: list[str] | None = None,
    carriers: list[str] | None = None,
    job_id: str | None = None,
) -> dict[str, Any]:
    ensure_dhl_schema()
    ensure_bl_distribution_cache_schema()

    from app.connectors.mssql.mssql_store import set_job_progress

    months_norm = _normalize_months(months)
    carriers_norm = _normalize_carriers(carriers)
    total_pairs = max(1, len(months_norm) * len(carriers_norm))

    result: dict[str, Any] = {
        "months": months_norm,
        "carriers": carriers_norm,
        "rows_written": 0,
        "rows_deleted": 0,
        "items": [],
        "matrix": {month: {} for month in months_norm},
    }

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SET DEADLOCK_PRIORITY LOW")
        cur.execute("SET LOCK_TIMEOUT 10000")

        pair_index = 0
        for month_token in months_norm:
            month_start_value = _month_start(month_token)
            month_end_value = _next_month(month_start_value)
            for carrier in carriers_norm:
                pair_index += 1
                shipment_rows = _load_shipment_rows(
                    cur,
                    carrier=carrier,
                    month_start_value=month_start_value,
                    month_end_value=month_end_value,
                )
                facts: list[list[Any]] = []
                by_outcome: dict[str, int] = {}
                by_cost_reason: dict[str, int] = {}
                for row in shipment_rows:
                    payload = ShipmentSemanticsInput(
                        carrier=carrier,
                        amazon_order_id=str(row[1]).strip() if row[1] else None,
                        acc_order_id=str(row[2]).strip() if row[2] else None,
                        bl_order_id=int(row[3]) if row[3] is not None else None,
                        primary_link_method=str(row[4]).strip() if row[4] else None,
                        status_code=str(row[5]).strip() if row[5] else None,
                        status_label=str(row[6]).strip() if row[6] else None,
                        is_delivered=bool(row[7]),
                        tracking_status=str(row[8]).strip() if row[8] else None,
                        package_type=str(row[9]).strip() if row[9] else None,
                        package_is_return=bool(row[10]),
                        event_text=str(row[11]).strip() if row[11] else None,
                        relation_type=str(row[12]).strip() if row[12] else None,
                        relation_confidence=_to_float(row[13]),
                    )
                    classified = _classify_shipment_semantics(payload)
                    by_outcome[classified["outcome_code"]] = by_outcome.get(classified["outcome_code"], 0) + 1
                    by_cost_reason[classified["cost_reason"]] = by_cost_reason.get(classified["cost_reason"], 0) + 1
                    facts.append(
                        [
                            row[0],
                            carrier,
                            month_start_value.isoformat(),
                            payload.amazon_order_id,
                            payload.acc_order_id,
                            payload.acc_order_id,
                            payload.acc_order_id,
                            payload.bl_order_id,
                            payload.primary_link_method,
                            payload.relation_type,
                            payload.relation_confidence if payload.relation_type else None,
                            classified["outcome_code"],
                            classified["outcome_confidence"],
                            classified["cost_reason"],
                            classified["cost_reason_confidence"],
                            _CLASSIFIER_VERSION,
                            json.dumps(classified["evidence"], ensure_ascii=True),
                        ]
                    )

                cur.execute(
                    """
DELETE FROM dbo.acc_shipment_outcome_fact
WHERE carrier = ?
  AND ship_month = ?
                    """,
                    [carrier, month_start_value.isoformat()],
                )
                rows_deleted = max(0, int(cur.rowcount or 0))
                if facts:
                    insert_sql = """
INSERT INTO dbo.acc_shipment_outcome_fact (
    shipment_id, carrier, ship_month, amazon_order_id, acc_order_id, bl_order_id,
    primary_link_method, relation_type, relation_confidence,
    outcome_code, outcome_confidence, cost_reason, cost_reason_confidence,
    classifier_version, evidence_json, created_at, updated_at
)
VALUES (
    CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?,
    CASE WHEN ? IS NULL OR ? = '' THEN NULL ELSE CAST(? AS UNIQUEIDENTIFIER) END,
    ?, ?, ?, ?,
    ?, ?, ?, ?,
    ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME()
)
                    """
                    if hasattr(cur, "fast_executemany"):
                        cur.fast_executemany = True
                    cur.executemany(insert_sql, facts)
                    if hasattr(cur, "fast_executemany"):
                        cur.fast_executemany = False
                conn.commit()

                summary = {
                    "month": month_token,
                    "carrier": carrier,
                    "shipments_total": len(shipment_rows),
                    "outcomes": by_outcome,
                    "cost_reasons": by_cost_reason,
                }
                result["rows_deleted"] += rows_deleted
                result["rows_written"] += len(facts)
                result["items"].append(summary)
                result["matrix"][month_token][carrier] = summary

                if job_id:
                    set_job_progress(
                        job_id,
                        progress_pct=min(95, 10 + int((pair_index / total_pairs) * 80)),
                        records_processed=result["rows_written"],
                        message=f"Courier shipment outcomes {month_token} {carrier}",
                    )
        return result
    finally:
        conn.close()


def get_courier_shipment_outcomes(
    *,
    months: list[str] | None = None,
    carriers: list[str] | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    ensure_dhl_schema()

    months_norm = _normalize_months(months)
    carriers_norm = _normalize_carriers(carriers)
    month_start_values = [_month_start(token) for token in months_norm]
    month_placeholders = ",".join("?" for _ in month_start_values)
    carrier_placeholders = ",".join("?" for _ in carriers_norm)

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
SELECT
    ship_month,
    carrier,
    shipment_id,
    amazon_order_id,
    CAST(acc_order_id AS NVARCHAR(40)) AS acc_order_id,
    bl_order_id,
    primary_link_method,
    relation_type,
    CAST(relation_confidence AS FLOAT) AS relation_confidence,
    outcome_code,
    CAST(outcome_confidence AS FLOAT) AS outcome_confidence,
    cost_reason,
    CAST(cost_reason_confidence AS FLOAT) AS cost_reason_confidence,
    evidence_json
FROM dbo.acc_shipment_outcome_fact WITH (NOLOCK)
WHERE ship_month IN ({month_placeholders})
  AND carrier IN ({carrier_placeholders})
ORDER BY ship_month ASC, carrier ASC, outcome_code ASC, shipment_id ASC
            """,
            [*(item.isoformat() for item in month_start_values), *carriers_norm],
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    items: list[dict[str, Any]] = []
    matrix: dict[str, dict[str, Any]] = {month: {} for month in months_norm}
    by_scope: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        month_token = f"{row[0].year}-{row[0].month:02d}"
        try:
            evidence = json.loads(row[13]) if row[13] else {}
        except Exception:
            evidence = {}
        item = {
            "month": month_token,
            "carrier": str(row[1]),
            "shipment_id": str(row[2]),
            "amazon_order_id": str(row[3]).strip() if row[3] else None,
            "acc_order_id": str(row[4]).strip() if row[4] else None,
            "bl_order_id": int(row[5]) if row[5] is not None else None,
            "primary_link_method": str(row[6]).strip() if row[6] else None,
            "relation_type": str(row[7]).strip() if row[7] else None,
            "relation_confidence": round(_to_float(row[8]), 4),
            "outcome_code": str(row[9]),
            "outcome_confidence": round(_to_float(row[10]), 4),
            "cost_reason": str(row[11]),
            "cost_reason_confidence": round(_to_float(row[12]), 4),
            "evidence": evidence if isinstance(evidence, dict) else {},
        }
        items.append(item)
        scope = by_scope.setdefault(
            (month_token, item["carrier"]),
            {
                "month": month_token,
                "carrier": item["carrier"],
                "shipments_total": 0,
                "outcomes": {},
                "cost_reasons": {},
            },
        )
        scope["shipments_total"] += 1
        scope["outcomes"][item["outcome_code"]] = scope["outcomes"].get(item["outcome_code"], 0) + 1
        scope["cost_reasons"][item["cost_reason"]] = scope["cost_reasons"].get(item["cost_reason"], 0) + 1

    summary: list[dict[str, Any]] = []
    for month in months_norm:
        for carrier in carriers_norm:
            scope = by_scope.get(
                (month, carrier),
                {"month": month, "carrier": carrier, "shipments_total": 0, "outcomes": {}, "cost_reasons": {}},
            )
            matrix[month][carrier] = scope
            summary.append(scope)

    return {
        "months": months_norm,
        "carriers": carriers_norm,
        "rows": len(items),
        "summary": summary,
        "items": items[: max(1, int(limit or 1))],
        "matrix": matrix,
    }
