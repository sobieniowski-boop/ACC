from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from app.core.db_connection import connect_acc, connect_netfox
from app.services.dhl_integration import ensure_dhl_schema

_ESTIMATOR_NAME = "courier_hist_v1"
_ESTIMATE_COST_SOURCE = "courier_hist_estimate"


@dataclass(frozen=True)
class ShipmentTarget:
    shipment_id: str
    carrier: str
    amazon_order_id: str
    recipient_country: str | None


@dataclass(frozen=True)
class HistoricalSample:
    carrier: str
    amazon_order_id: str
    recipient_country: str | None
    actual_amount: float


def _connect_acc():
    return connect_acc(autocommit=False, timeout=60)


def _norm_text(value: Any, *, max_len: int = 40, fallback: str = "UNK") -> str:
    text = str(value or "").strip().lower()
    if not text:
        return fallback
    text = " ".join(text.split())
    return text[:max_len]


def _weight_bucket(weight_value: Any) -> str:
    try:
        weight = float(weight_value or 0)
    except Exception:
        return "w_unk"
    if weight <= 0:
        return "w_unk"
    if weight <= 1:
        return "w_0_1"
    if weight <= 3:
        return "w_1_3"
    if weight <= 5:
        return "w_3_5"
    if weight <= 10:
        return "w_5_10"
    if weight <= 20:
        return "w_10_20"
    return "w_20_plus"


def _bucket_keys(
    *,
    carrier: str,
    country: str,
    method: str,
    module: str,
    carton: str,
    weight_band: str,
) -> list[str]:
    return [
        f"{carrier}|{country}|{method}|{module}|{carton}|{weight_band}",
        f"{carrier}|{country}|{method}|{module}|any|{weight_band}",
        f"{carrier}|{country}|{method}|any|any|{weight_band}",
        f"{carrier}|{country}|any|any|any|{weight_band}",
        f"{carrier}|{country}|any|any|any|w_unk",
        f"{carrier}|unk|any|any|any|w_unk",
    ]


def _chunks(items: list[str], size: int = 500) -> list[list[str]]:
    if not items:
        return []
    out: list[list[str]] = []
    for idx in range(0, len(items), max(1, int(size))):
        out.append(items[idx : idx + size])
    return out


def _load_netfox_order_features(order_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not order_ids:
        return {}
    rows_by_order: dict[str, dict[str, Any]] = {}
    conn = connect_netfox(timeout=20)
    try:
        cur = conn.cursor()
        for batch in _chunks(order_ids, size=500):
            placeholders = ",".join("?" for _ in batch)
            cur.execute(
                f"""
WITH ranked AS (
    SELECT
        external_order_id,
        delivery_country_code,
        delivery_method,
        delivery_package_module,
        exf_KartonPakowanie,
        weight,
        CzasSynch,
        ROW_NUMBER() OVER (
            PARTITION BY external_order_id
            ORDER BY CzasSynch DESC
        ) AS rn
    FROM dbo.ITJK_ZamowieniaBaselinkerAPI WITH (NOLOCK)
    WHERE external_order_id IN ({placeholders})
)
SELECT
    external_order_id,
    delivery_country_code,
    delivery_method,
    delivery_package_module,
    exf_KartonPakowanie,
    weight
FROM ranked
WHERE rn = 1
                """,
                batch,
            )
            for row in cur.fetchall():
                order_id = str(row[0] or "").strip()
                if not order_id:
                    continue
                rows_by_order[order_id] = {
                    "delivery_country_code": row[1],
                    "delivery_method": row[2],
                    "delivery_package_module": row[3],
                    "exf_KartonPakowanie": row[4],
                    "weight": row[5],
                }
    finally:
        conn.close()
    return rows_by_order


def _load_target_shipments(
    cur,
    *,
    carriers: list[str],
    created_from: date | None,
    created_to: date | None,
    limit_shipments: int,
    refresh_existing: bool,
) -> list[ShipmentTarget]:
    where = [
        "l.is_primary = 1",
        "l.amazon_order_id IS NOT NULL",
        "NOT EXISTS (SELECT 1 FROM dbo.acc_shipment_cost c WITH (NOLOCK) WHERE c.shipment_id = s.id AND c.is_estimated = 0)",
    ]
    params: list[Any] = []
    carrier_placeholders = ",".join("?" for _ in carriers)
    where.append(f"s.carrier IN ({carrier_placeholders})")
    params.extend(carriers)
    if not refresh_existing:
        where.append(
            "NOT EXISTS (SELECT 1 FROM dbo.acc_shipment_cost c WITH (NOLOCK) WHERE c.shipment_id = s.id AND c.is_estimated = 1)"
        )
    if created_from:
        where.append("CAST(COALESCE(s.ship_date, CAST(s.created_at_carrier AS DATE), CAST(s.first_seen_at AS DATE)) AS DATE) >= ?")
        params.append(created_from.isoformat())
    if created_to:
        where.append("CAST(COALESCE(s.ship_date, CAST(s.created_at_carrier AS DATE), CAST(s.first_seen_at AS DATE)) AS DATE) <= ?")
        params.append(created_to.isoformat())

    cur.execute(
        f"""
SELECT TOP {int(limit_shipments)}
    CAST(s.id AS NVARCHAR(40)) AS shipment_id,
    s.carrier,
    l.amazon_order_id,
    s.recipient_country
FROM dbo.acc_shipment s WITH (NOLOCK)
JOIN dbo.acc_shipment_order_link l WITH (NOLOCK)
  ON l.shipment_id = s.id
WHERE {" AND ".join(where)}
ORDER BY s.first_seen_at ASC
        """,
        params,
    )
    out: list[ShipmentTarget] = []
    for row in cur.fetchall():
        shipment_id = str(row[0] or "").strip()
        carrier = str(row[1] or "").strip().upper()
        order_id = str(row[2] or "").strip()
        if not shipment_id or not carrier or not order_id:
            continue
        out.append(
            ShipmentTarget(
                shipment_id=shipment_id,
                carrier=carrier,
                amazon_order_id=order_id,
                recipient_country=str(row[3] or "").strip() or None,
            )
        )
    return out


def _load_historical_actual_samples(
    cur,
    *,
    carriers: list[str],
    lookback_from: date,
    lookback_to: date,
    limit_samples: int,
) -> list[HistoricalSample]:
    carrier_placeholders = ",".join("?" for _ in carriers)
    params: list[Any] = list(carriers) + [lookback_from.isoformat(), lookback_to.isoformat()]
    cur.execute(
        f"""
WITH ranked_costs AS (
    SELECT
        c.shipment_id,
        c.cost_source,
        COALESCE(
            c.gross_amount,
            ISNULL(c.net_amount, 0) + ISNULL(c.fuel_amount, 0) + ISNULL(c.toll_amount, 0)
        ) AS resolved_amount,
        ROW_NUMBER() OVER (
            PARTITION BY c.shipment_id
            ORDER BY
                CASE c.cost_source
                    WHEN 'dhl_billing_files' THEN 0
                    WHEN 'invoice_direct' THEN 0
                    WHEN 'gls_billing_files' THEN 0
                    WHEN 'invoice_extras' THEN 1
                    ELSE 9
                END,
                ISNULL(c.invoice_date, CAST('1900-01-01' AS DATE)) DESC,
                c.updated_at DESC
        ) AS rn
    FROM dbo.acc_shipment_cost c WITH (NOLOCK)
    WHERE c.is_estimated = 0
),
base AS (
    SELECT
        s.carrier,
        l.amazon_order_id,
        s.recipient_country,
        CAST(rc.resolved_amount AS FLOAT) AS actual_amount,
        CAST(COALESCE(s.ship_date, CAST(s.created_at_carrier AS DATE), CAST(s.first_seen_at AS DATE)) AS DATE) AS event_date
    FROM dbo.acc_shipment s WITH (NOLOCK)
    JOIN dbo.acc_shipment_order_link l WITH (NOLOCK)
      ON l.shipment_id = s.id
     AND l.is_primary = 1
    JOIN ranked_costs rc
      ON rc.shipment_id = s.id
     AND rc.rn = 1
    WHERE s.carrier IN ({carrier_placeholders})
      AND l.amazon_order_id IS NOT NULL
      AND rc.resolved_amount IS NOT NULL
)
SELECT TOP {int(limit_samples)}
    carrier,
    amazon_order_id,
    recipient_country,
    actual_amount
FROM base
WHERE event_date >= ?
  AND event_date <= ?
ORDER BY event_date DESC
        """,
        params,
    )
    out: list[HistoricalSample] = []
    for row in cur.fetchall():
        carrier = str(row[0] or "").strip().upper()
        order_id = str(row[1] or "").strip()
        if not carrier or not order_id:
            continue
        try:
            amount = float(row[3] or 0)
        except Exception:
            continue
        if amount <= 0:
            continue
        out.append(
            HistoricalSample(
                carrier=carrier,
                amazon_order_id=order_id,
                recipient_country=str(row[2] or "").strip() or None,
                actual_amount=amount,
            )
        )
    return out


def _upsert_shipment_cost_estimate(
    cur,
    *,
    shipment_id: str,
    carrier: str,
    amazon_order_id: str,
    bucket_key: str,
    sample_count: int,
    estimated_amount_pln: float,
    horizon_days: int,
    min_samples: int,
    model_version: str,
    payload: dict[str, Any],
) -> None:
    cur.execute(
        """
SELECT CAST(id AS NVARCHAR(40))
FROM dbo.acc_courier_cost_estimate WITH (NOLOCK)
WHERE shipment_id = CAST(? AS UNIQUEIDENTIFIER)
  AND estimator_name = ?
        """,
        [shipment_id, _ESTIMATOR_NAME],
    )
    row = cur.fetchone()
    if row:
        cur.execute(
            """
UPDATE dbo.acc_courier_cost_estimate
SET carrier = ?,
    amazon_order_id = ?,
    model_version = ?,
    horizon_days = ?,
    min_samples = ?,
    bucket_key = ?,
    sample_count = ?,
    estimated_amount_pln = ?,
    estimated_at = SYSUTCDATETIME(),
    status = 'estimated',
    reconciled_at = NULL,
    actual_amount_pln = NULL,
    abs_error_pln = NULL,
    ape_pct = NULL,
    replaced_by_cost_source = NULL,
    raw_payload_json = ?
WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            [
                carrier,
                amazon_order_id,
                model_version,
                int(horizon_days),
                int(min_samples),
                bucket_key,
                int(sample_count),
                float(estimated_amount_pln),
                json.dumps(payload, ensure_ascii=True),
                str(row[0]),
            ],
        )
        return

    cur.execute(
        """
INSERT INTO dbo.acc_courier_cost_estimate
(
    id, shipment_id, carrier, amazon_order_id, estimator_name, model_version,
    horizon_days, min_samples, bucket_key, sample_count, estimated_amount_pln,
    estimated_at, status, raw_payload_json
)
VALUES
(
    NEWID(), CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), 'estimated', ?
)
        """,
        [
            shipment_id,
            carrier,
            amazon_order_id,
            _ESTIMATOR_NAME,
            model_version,
            int(horizon_days),
            int(min_samples),
            bucket_key,
            int(sample_count),
            float(estimated_amount_pln),
            json.dumps(payload, ensure_ascii=True),
        ],
    )


def _upsert_estimated_cost(cur, *, shipment_id: str, estimated_amount_pln: float, payload: dict[str, Any]) -> None:
    cur.execute(
        """
SELECT CAST(id AS NVARCHAR(40))
FROM dbo.acc_shipment_cost WITH (NOLOCK)
WHERE shipment_id = CAST(? AS UNIQUEIDENTIFIER)
  AND cost_source = ?
        """,
        [shipment_id, _ESTIMATE_COST_SOURCE],
    )
    row = cur.fetchone()
    if row:
        cur.execute(
            """
UPDATE dbo.acc_shipment_cost
SET currency = 'PLN',
    net_amount = ?,
    fuel_amount = 0,
    toll_amount = 0,
    gross_amount = ?,
    invoice_number = NULL,
    invoice_date = NULL,
    billing_period = NULL,
    is_estimated = 1,
    raw_payload_json = ?,
    updated_at = SYSUTCDATETIME()
WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            [estimated_amount_pln, estimated_amount_pln, json.dumps(payload, ensure_ascii=True), str(row[0])],
        )
        return

    cur.execute(
        """
INSERT INTO dbo.acc_shipment_cost
(
    id, shipment_id, cost_source, currency, net_amount, fuel_amount, toll_amount,
    gross_amount, invoice_number, invoice_date, billing_period, is_estimated,
    raw_payload_json, created_at, updated_at
)
VALUES
(
    NEWID(), CAST(? AS UNIQUEIDENTIFIER), ?, 'PLN', ?, 0, 0, ?, NULL, NULL, NULL, 1, ?, SYSUTCDATETIME(), SYSUTCDATETIME()
)
        """,
        [
            shipment_id,
            _ESTIMATE_COST_SOURCE,
            estimated_amount_pln,
            estimated_amount_pln,
            json.dumps(payload, ensure_ascii=True),
        ],
    )


def estimate_preinvoice_courier_costs(
    *,
    carriers: list[str] | None = None,
    created_from: date | None = None,
    created_to: date | None = None,
    horizon_days: int = 180,
    min_samples: int = 10,
    limit_shipments: int = 20000,
    refresh_existing: bool = False,
    job_id: str | None = None,
) -> dict[str, Any]:
    ensure_dhl_schema()
    from app.connectors.mssql.mssql_store import set_job_progress

    carriers_norm = [str(c or "").strip().upper() for c in (carriers or ["DHL", "GLS"]) if str(c or "").strip()]
    carriers_norm = [c for c in carriers_norm if c in {"DHL", "GLS"}]
    if not carriers_norm:
        raise ValueError("No supported carriers selected")

    today = date.today()
    lookback_to = today
    lookback_from = today - timedelta(days=max(1, int(horizon_days or 180)))

    stats = {
        "carriers": carriers_norm,
        "shipments_selected": 0,
        "historical_samples": 0,
        "estimated_written": 0,
        "skipped_no_bucket": 0,
        "skipped_missing_features": 0,
        "horizon_days": int(horizon_days),
        "min_samples": int(min_samples),
    }

    conn = _connect_acc()
    try:
        cur = conn.cursor()
        targets = _load_target_shipments(
            cur,
            carriers=carriers_norm,
            created_from=created_from,
            created_to=created_to,
            limit_shipments=max(1, int(limit_shipments)),
            refresh_existing=bool(refresh_existing),
        )
        stats["shipments_selected"] = len(targets)
        if not targets:
            return stats

        history = _load_historical_actual_samples(
            cur,
            carriers=carriers_norm,
            lookback_from=lookback_from,
            lookback_to=lookback_to,
            limit_samples=max(5000, int(limit_shipments) * 20),
        )
        stats["historical_samples"] = len(history)
        if not history:
            return stats

        all_order_ids = list({item.amazon_order_id for item in targets} | {item.amazon_order_id for item in history})
        netfox_features = _load_netfox_order_features(all_order_ids)

        bucket_stats: dict[str, dict[str, float]] = {}
        for sample in history:
            feat = netfox_features.get(sample.amazon_order_id) or {}
            country = _norm_text(feat.get("delivery_country_code") or sample.recipient_country, max_len=8)
            method = _norm_text(feat.get("delivery_method"), max_len=30, fallback="any")
            module = _norm_text(feat.get("delivery_package_module"), max_len=30, fallback="any")
            carton = "carton" if str(feat.get("exf_KartonPakowanie") or "").strip() else "no_carton"
            weight_band = _weight_bucket(feat.get("weight"))
            for key in _bucket_keys(
                carrier=sample.carrier,
                country=country,
                method=method,
                module=module,
                carton=carton,
                weight_band=weight_band,
            ):
                entry = bucket_stats.setdefault(key, {"sum": 0.0, "count": 0.0})
                entry["sum"] += float(sample.actual_amount)
                entry["count"] += 1.0

        if job_id:
            set_job_progress(job_id, progress_pct=30, records_processed=0, message=f"Estimator targets={len(targets)} history={len(history)}")

        for idx, target in enumerate(targets, start=1):
            feat = netfox_features.get(target.amazon_order_id) or {}
            country = _norm_text(feat.get("delivery_country_code") or target.recipient_country, max_len=8)
            method = _norm_text(feat.get("delivery_method"), max_len=30, fallback="any")
            module = _norm_text(feat.get("delivery_package_module"), max_len=30, fallback="any")
            carton = "carton" if str(feat.get("exf_KartonPakowanie") or "").strip() else "no_carton"
            weight_band = _weight_bucket(feat.get("weight"))

            best_key = ""
            best_count = 0
            best_value = 0.0
            for key in _bucket_keys(
                carrier=target.carrier,
                country=country,
                method=method,
                module=module,
                carton=carton,
                weight_band=weight_band,
            ):
                entry = bucket_stats.get(key)
                if not entry:
                    continue
                count = int(entry["count"] or 0)
                if count < max(1, int(min_samples)):
                    continue
                best_key = key
                best_count = count
                best_value = float(entry["sum"] / max(entry["count"], 1))
                break

            if not best_key:
                stats["skipped_no_bucket"] += 1
                continue

            estimate = round(best_value, 4)
            payload = {
                "estimator_name": _ESTIMATOR_NAME,
                "model_version": _ESTIMATOR_NAME,
                "bucket_key": best_key,
                "sample_count": best_count,
                "features": {
                    "country": country,
                    "method": method,
                    "module": module,
                    "carton": carton,
                    "weight_band": weight_band,
                },
            }
            _upsert_shipment_cost_estimate(
                cur,
                shipment_id=target.shipment_id,
                carrier=target.carrier,
                amazon_order_id=target.amazon_order_id,
                bucket_key=best_key,
                sample_count=best_count,
                estimated_amount_pln=estimate,
                horizon_days=int(horizon_days),
                min_samples=int(min_samples),
                model_version=_ESTIMATOR_NAME,
                payload=payload,
            )
            _upsert_estimated_cost(cur, shipment_id=target.shipment_id, estimated_amount_pln=estimate, payload=payload)
            stats["estimated_written"] += 1

            if idx % 100 == 0:
                conn.commit()
                if job_id:
                    pct = 30 + int((idx / max(len(targets), 1)) * 60)
                    set_job_progress(
                        job_id,
                        progress_pct=min(95, pct),
                        records_processed=idx,
                        message=f"Estimator processed={idx}/{len(targets)}",
                    )

        conn.commit()
        return stats
    finally:
        conn.close()


def reconcile_estimated_costs(
    *,
    carriers: list[str] | None = None,
    limit_shipments: int = 50000,
    job_id: str | None = None,
) -> dict[str, Any]:
    ensure_dhl_schema()
    from app.connectors.mssql.mssql_store import set_job_progress

    carriers_norm = [str(c or "").strip().upper() for c in (carriers or ["DHL", "GLS"]) if str(c or "").strip()]
    carriers_norm = [c for c in carriers_norm if c in {"DHL", "GLS"}]
    if not carriers_norm:
        raise ValueError("No supported carriers selected")

    stats = {
        "carriers": carriers_norm,
        "estimated_rows_checked": 0,
        "reconciled_rows": 0,
        "estimated_cost_rows_deleted": 0,
    }

    carrier_placeholders = ",".join("?" for _ in carriers_norm)
    conn = _connect_acc()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
WITH ranked_actual AS (
    SELECT
        c.shipment_id,
        c.cost_source,
        COALESCE(
            c.gross_amount,
            ISNULL(c.net_amount, 0) + ISNULL(c.fuel_amount, 0) + ISNULL(c.toll_amount, 0)
        ) AS actual_amount,
        ROW_NUMBER() OVER (
            PARTITION BY c.shipment_id
            ORDER BY
                CASE c.cost_source
                    WHEN 'dhl_billing_files' THEN 0
                    WHEN 'invoice_direct' THEN 0
                    WHEN 'gls_billing_files' THEN 0
                    WHEN 'invoice_extras' THEN 1
                    ELSE 9
                END,
                ISNULL(c.invoice_date, CAST('1900-01-01' AS DATE)) DESC,
                c.updated_at DESC
        ) AS rn
    FROM dbo.acc_shipment_cost c WITH (NOLOCK)
    WHERE c.is_estimated = 0
),
base AS (
    SELECT TOP {int(limit_shipments)}
        CAST(e.id AS NVARCHAR(40)) AS estimate_id,
        CAST(e.shipment_id AS NVARCHAR(40)) AS shipment_id,
        CAST(e.estimated_amount_pln AS FLOAT) AS estimated_amount,
        CAST(a.actual_amount AS FLOAT) AS actual_amount,
        a.cost_source
    FROM dbo.acc_courier_cost_estimate e WITH (NOLOCK)
    JOIN ranked_actual a
      ON a.shipment_id = e.shipment_id
     AND a.rn = 1
    WHERE e.status = 'estimated'
      AND e.carrier IN ({carrier_placeholders})
      AND a.actual_amount IS NOT NULL
)
SELECT estimate_id, shipment_id, estimated_amount, actual_amount, cost_source
FROM base
ORDER BY estimate_id
            """,
            carriers_norm,
        )
        rows = cur.fetchall()
        stats["estimated_rows_checked"] = len(rows)
        if not rows:
            return stats

        if job_id:
            set_job_progress(job_id, progress_pct=20, records_processed=0, message=f"Reconcile candidates={len(rows)}")

        for idx, row in enumerate(rows, start=1):
            estimate_id = str(row[0])
            shipment_id = str(row[1])
            estimated_amount = float(row[2] or 0)
            actual_amount = float(row[3] or 0)
            src = str(row[4] or "")
            abs_error = abs(actual_amount - estimated_amount)
            ape_pct = (abs_error / actual_amount * 100.0) if actual_amount > 0 else None
            cur.execute(
                """
UPDATE dbo.acc_courier_cost_estimate
SET status = 'reconciled',
    reconciled_at = SYSUTCDATETIME(),
    actual_amount_pln = ?,
    abs_error_pln = ?,
    ape_pct = ?,
    replaced_by_cost_source = ?
WHERE id = CAST(? AS UNIQUEIDENTIFIER)
                """,
                [actual_amount, abs_error, ape_pct, src, estimate_id],
            )
            cur.execute(
                """
DELETE FROM dbo.acc_shipment_cost
WHERE shipment_id = CAST(? AS UNIQUEIDENTIFIER)
  AND cost_source = ?
  AND is_estimated = 1
                """,
                [shipment_id, _ESTIMATE_COST_SOURCE],
            )
            stats["estimated_cost_rows_deleted"] += int(cur.rowcount or 0)
            stats["reconciled_rows"] += 1

            if idx % 200 == 0:
                conn.commit()
                if job_id:
                    pct = 20 + int((idx / max(len(rows), 1)) * 75)
                    set_job_progress(
                        job_id,
                        progress_pct=min(95, pct),
                        records_processed=idx,
                        message=f"Reconciled {idx}/{len(rows)}",
                    )
        conn.commit()
        return stats
    finally:
        conn.close()


def compute_courier_estimation_kpis(
    *,
    days_back: int = 30,
    carriers: list[str] | None = None,
    model_version: str = _ESTIMATOR_NAME,
    job_id: str | None = None,
) -> dict[str, Any]:
    ensure_dhl_schema()
    from app.connectors.mssql.mssql_store import set_job_progress

    carriers_norm = [str(c or "").strip().upper() for c in (carriers or ["DHL", "GLS"]) if str(c or "").strip()]
    carriers_norm = [c for c in carriers_norm if c in {"DHL", "GLS"}]
    if not carriers_norm:
        raise ValueError("No supported carriers selected")
    days_safe = max(1, int(days_back or 30))
    from_dt = datetime.now(timezone.utc) - timedelta(days=days_safe)

    stats = {
        "days_back": days_safe,
        "carriers": carriers_norm,
        "rows_source": 0,
        "kpi_rows_upserted": 0,
        "items": [],
    }

    conn = _connect_acc()
    try:
        cur = conn.cursor()
        placeholders = ",".join("?" for _ in carriers_norm)
        cur.execute(
            f"""
SELECT
    CAST(reconciled_at AS DATE) AS kpi_date,
    carrier,
    model_version,
    CAST(abs_error_pln AS FLOAT) AS abs_error_pln,
    CAST(ape_pct AS FLOAT) AS ape_pct
FROM dbo.acc_courier_cost_estimate WITH (NOLOCK)
WHERE status = 'reconciled'
  AND reconciled_at >= ?
  AND carrier IN ({placeholders})
  AND model_version = ?
            """,
            [from_dt] + carriers_norm + [model_version],
        )
        rows = cur.fetchall()
        stats["rows_source"] = len(rows)
        if not rows:
            return stats

        groups: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in rows:
            kpi_date = str(row[0])
            carrier = str(row[1])
            mver = str(row[2])
            abs_error = float(row[3] or 0)
            ape = float(row[4]) if row[4] is not None else None
            key = (kpi_date, carrier, mver)
            g = groups.setdefault(key, {"abs_errors": [], "apes": []})
            g["abs_errors"].append(abs_error)
            if ape is not None:
                g["apes"].append(ape)

        if job_id:
            set_job_progress(job_id, progress_pct=35, records_processed=0, message=f"KPI groups={len(groups)}")

        for idx, (key, payload) in enumerate(groups.items(), start=1):
            kpi_date, carrier, mver = key
            abs_errors = payload["abs_errors"]
            apes = sorted(payload["apes"])
            samples = len(abs_errors)
            mae = (sum(abs_errors) / samples) if samples else 0.0
            mape = (sum(apes) / len(apes)) if apes else None
            p95 = apes[min(len(apes) - 1, max(0, int(round(0.95 * (len(apes) - 1)))))] if apes else None

            cur.execute(
                """
SELECT 1
FROM dbo.acc_courier_estimation_kpi_daily WITH (NOLOCK)
WHERE kpi_date = CAST(? AS DATE)
  AND carrier = ?
  AND model_version = ?
                """,
                [kpi_date, carrier, mver],
            )
            exists = cur.fetchone() is not None
            if exists:
                cur.execute(
                    """
UPDATE dbo.acc_courier_estimation_kpi_daily
SET samples_count = ?,
    mape_pct = ?,
    mae_pln = ?,
    p95_ape_pct = ?,
    calculated_at = SYSUTCDATETIME()
WHERE kpi_date = CAST(? AS DATE)
  AND carrier = ?
  AND model_version = ?
                    """,
                    [samples, mape, mae, p95, kpi_date, carrier, mver],
                )
            else:
                cur.execute(
                    """
INSERT INTO dbo.acc_courier_estimation_kpi_daily
(kpi_date, carrier, model_version, samples_count, mape_pct, mae_pln, p95_ape_pct, calculated_at)
VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?, SYSUTCDATETIME())
                    """,
                    [kpi_date, carrier, mver, samples, mape, mae, p95],
                )
            stats["kpi_rows_upserted"] += 1
            stats["items"].append(
                {
                    "kpi_date": kpi_date,
                    "carrier": carrier,
                    "model_version": mver,
                    "samples_count": samples,
                    "mape_pct": round(mape, 4) if mape is not None else None,
                    "mae_pln": round(mae, 4),
                    "p95_ape_pct": round(p95, 4) if p95 is not None else None,
                }
            )

            if idx % 50 == 0:
                conn.commit()
                if job_id:
                    pct = 35 + int((idx / max(len(groups), 1)) * 60)
                    set_job_progress(
                        job_id,
                        progress_pct=min(95, pct),
                        records_processed=idx,
                        message=f"KPI upsert {idx}/{len(groups)}",
                    )

        conn.commit()
        return stats
    finally:
        conn.close()
