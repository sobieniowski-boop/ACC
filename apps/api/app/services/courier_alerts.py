from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from app.core.config import settings
from app.core.db_connection import connect_acc
from app.services.courier_readiness import get_courier_closed_month_readiness
from app.services.dhl_integration import ensure_dhl_schema


def _connect():
    return connect_acc(autocommit=False, timeout=30)


@dataclass(frozen=True)
class CarrierAlertSpec:
    carrier: str
    source_system: str
    calc_version: str


_CARRIER_SPECS = [
    CarrierAlertSpec(carrier="DHL", source_system="dhl_billing_files", calc_version="dhl_v1"),
    CarrierAlertSpec(carrier="GLS", source_system="gls_billing_files", calc_version="gls_v1"),
]


def _ensure_rule(
    cur,
    *,
    name: str,
    rule_type: str,
    severity: str,
    description: str,
) -> str:
    cur.execute(
        """
        SELECT TOP 1 CAST(id AS NVARCHAR(40))
        FROM dbo.acc_al_alert_rules WITH (NOLOCK)
        WHERE name = ? AND rule_type = ?
        ORDER BY created_at DESC
        """,
        (name, rule_type),
    )
    row = cur.fetchone()
    if row and row[0]:
        return str(row[0])

    rule_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO dbo.acc_al_alert_rules
        (
            id, name, description, rule_type, severity, is_active, created_by
        )
        VALUES
        (
            CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, 1, ?
        )
        """,
        (
            rule_id,
            name,
            description,
            rule_type,
            severity,
            settings.DEFAULT_ACTOR,
        ),
    )
    return rule_id


def _upsert_singleton_alert(
    cur,
    *,
    rule_id: str,
    severity: str,
    title: str,
    detail: str,
    current_value: float,
    detail_json: dict[str, Any],
    context_json: dict[str, Any],
) -> dict[str, int]:
    cur.execute(
        """
        SELECT TOP 1 CAST(id AS NVARCHAR(40))
        FROM dbo.acc_al_alerts WITH (UPDLOCK, ROWLOCK)
        WHERE rule_id = CAST(? AS UNIQUEIDENTIFIER)
          AND is_resolved = 0
        ORDER BY triggered_at DESC
        """,
        (rule_id,),
    )
    row = cur.fetchone()
    if row and row[0]:
        cur.execute(
            """
            UPDATE dbo.acc_al_alerts
            SET
                title = ?,
                detail = ?,
                detail_json = ?,
                context_json = ?,
                severity = ?,
                current_value = ?,
                is_read = 0,
                triggered_at = SYSUTCDATETIME()
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (
                title,
                detail,
                json.dumps(detail_json, ensure_ascii=True),
                json.dumps(context_json, ensure_ascii=True),
                severity,
                current_value,
                str(row[0]),
            ),
        )
        return {"created": 0, "updated": 1, "resolved": 0}

    cur.execute(
        """
        INSERT INTO dbo.acc_al_alerts
        (
            id, rule_id, marketplace_id, sku, title, detail, detail_json, context_json,
            severity, current_value, is_read, is_resolved, triggered_at
        )
        VALUES
        (
            CAST(? AS UNIQUEIDENTIFIER), CAST(? AS UNIQUEIDENTIFIER), NULL, NULL, ?, ?, ?, ?,
            ?, ?, 0, 0, SYSUTCDATETIME()
        )
        """,
        (
            str(uuid.uuid4()),
            rule_id,
            title,
            detail,
            json.dumps(detail_json, ensure_ascii=True),
            json.dumps(context_json, ensure_ascii=True),
            severity,
            current_value,
        ),
    )
    return {"created": 1, "updated": 0, "resolved": 0}


def _resolve_singleton_alerts(cur, *, rule_id: str) -> int:
    cur.execute(
        """
        UPDATE dbo.acc_al_alerts
        SET
            is_resolved = 1,
            resolved_at = SYSUTCDATETIME(),
            resolved_by = ?
        WHERE rule_id = CAST(? AS UNIQUEIDENTIFIER)
          AND is_resolved = 0
        """,
        (settings.DEFAULT_ACTOR, rule_id),
    )
    return int(cur.rowcount or 0)


def _carrier_metrics(
    cur,
    *,
    spec: CarrierAlertSpec,
    window_from: date,
    window_to: date,
) -> dict[str, float]:
    cur.execute(
        """
        WITH scope AS (
            SELECT s.id
            FROM dbo.acc_shipment s WITH (NOLOCK)
            WHERE s.carrier = ?
              AND s.source_system = ?
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
                  ) <= ?
        )
        SELECT
            COUNT_BIG(*) AS shipments_total,
            COUNT_BIG(DISTINCT CASE WHEN l.shipment_id IS NOT NULL THEN scope.id END) AS linked_shipments,
            COUNT_BIG(DISTINCT CASE WHEN c.shipment_id IS NOT NULL THEN scope.id END) AS costed_shipments
        FROM scope
        LEFT JOIN dbo.acc_shipment_order_link l WITH (NOLOCK)
          ON l.shipment_id = scope.id
         AND l.is_primary = 1
        LEFT JOIN dbo.acc_shipment_cost c WITH (NOLOCK)
          ON c.shipment_id = scope.id
         AND c.is_estimated = 0
        """,
        (
            spec.carrier,
            spec.source_system,
            window_from.isoformat(),
            window_to.isoformat(),
        ),
    )
    ship_row = cur.fetchone() or (0, 0, 0)
    shipments_total = int(ship_row[0] or 0)
    linked_shipments = int(ship_row[1] or 0)
    costed_shipments = int(ship_row[2] or 0)

    cur.execute(
        """
        SELECT
            COUNT_BIG(*) AS shadow_rows,
            SUM(CASE WHEN sh.comparison_status = 'delta' THEN 1 ELSE 0 END) AS delta_rows,
            SUM(CASE WHEN sh.comparison_status = 'shadow_only' THEN 1 ELSE 0 END) AS shadow_only_rows,
            SUM(CASE WHEN sh.comparison_status = 'legacy_only' THEN 1 ELSE 0 END) AS legacy_only_rows
        FROM dbo.acc_order_logistics_shadow sh WITH (NOLOCK)
        JOIN dbo.acc_order o WITH (NOLOCK)
          ON o.amazon_order_id = sh.amazon_order_id
        WHERE sh.calc_version = ?
          AND CAST(o.purchase_date AS DATE) >= ?
          AND CAST(o.purchase_date AS DATE) <= ?
        """,
        (
            spec.calc_version,
            window_from.isoformat(),
            window_to.isoformat(),
        ),
    )
    shadow_row = cur.fetchone() or (0, 0, 0, 0)
    shadow_rows = int(shadow_row[0] or 0)
    delta_rows = int(shadow_row[1] or 0)
    shadow_only_rows = int(shadow_row[2] or 0)
    legacy_only_rows = int(shadow_row[3] or 0)

    return {
        "shipments_total": shipments_total,
        "linked_shipments": linked_shipments,
        "costed_shipments": costed_shipments,
        "link_coverage_pct": round((linked_shipments / shipments_total) * 100, 2) if shipments_total else 100.0,
        "cost_coverage_pct": round((costed_shipments / shipments_total) * 100, 2) if shipments_total else 100.0,
        "shadow_rows": shadow_rows,
        "delta_rows": delta_rows,
        "shadow_only_rows": shadow_only_rows,
        "legacy_only_rows": legacy_only_rows,
        "shadow_delta_pct": round((delta_rows / shadow_rows) * 100, 2) if shadow_rows else 0.0,
    }


def _carrier_estimation_kpi(
    cur,
    *,
    carrier: str,
    days_back: int,
) -> dict[str, float]:
    cur.execute(
        """
        SELECT
            SUM(ISNULL(samples_count, 0)) AS samples_total,
            SUM(ISNULL(samples_count, 0) * ISNULL(mape_pct, 0)) AS weighted_mape_sum,
            SUM(ISNULL(samples_count, 0) * ISNULL(mae_pln, 0)) AS weighted_mae_sum
        FROM dbo.acc_courier_estimation_kpi_daily WITH (NOLOCK)
        WHERE carrier = ?
          AND kpi_date >= DATEADD(day, -?, CAST(SYSUTCDATETIME() AS DATE))
        """,
        (carrier, max(1, int(days_back))),
    )
    row = cur.fetchone() or (0, 0, 0)
    samples = int(row[0] or 0)
    if samples <= 0:
        return {"samples_count": 0, "mape_pct": 0.0, "mae_pln": 0.0}
    weighted_mape = float(row[1] or 0) / max(samples, 1)
    weighted_mae = float(row[2] or 0) / max(samples, 1)
    return {
        "samples_count": samples,
        "mape_pct": round(weighted_mape, 2),
        "mae_pln": round(weighted_mae, 2),
    }


def evaluate_courier_alerts(
    *,
    window_days: int = 7,
    cost_coverage_min_pct: float = 95.0,
    link_coverage_min_pct: float = 95.0,
    shadow_delta_max_pct: float = 10.0,
    estimation_mape_max_pct: float = 25.0,
    estimation_mae_max_pln: float = 3.0,
    estimation_min_samples: int = 30,
    estimation_days_back: int = 30,
) -> dict[str, Any]:
    ensure_dhl_schema()
    try:
        from app.services.courier_cost_estimation import compute_courier_estimation_kpis

        compute_courier_estimation_kpis(days_back=max(1, int(estimation_days_back or 30)), carriers=["DHL", "GLS"])
    except Exception:
        # KPI refresh should not block core courier alerts.
        pass
    today = date.today()
    window_from = today - timedelta(days=max(1, int(window_days or 7)) - 1)
    window_to = today

    stats: dict[str, Any] = {
        "status": "ok",
        "window_from": window_from.isoformat(),
        "window_to": window_to.isoformat(),
        "created": 0,
        "updated": 0,
        "resolved": 0,
        "items": [],
    }

    conn = _connect()
    try:
        cur = conn.cursor()
        for spec in _CARRIER_SPECS:
            metrics = _carrier_metrics(cur, spec=spec, window_from=window_from, window_to=window_to)
            estimation_kpi = _carrier_estimation_kpi(
                cur,
                carrier=spec.carrier,
                days_back=max(1, int(estimation_days_back or 30)),
            )

            rule_defs = [
                {
                    "name": f"Courier {spec.carrier} cost coverage",
                    "rule_type": "courier_cost_coverage",
                    "severity": "warning",
                    "description": f"{spec.carrier} shipment cost coverage below threshold.",
                    "current_value": metrics["cost_coverage_pct"],
                    "is_triggered": metrics["shipments_total"] > 0 and metrics["cost_coverage_pct"] < cost_coverage_min_pct,
                    "title": f"Courier {spec.carrier}: cost coverage degraded",
                    "detail": (
                        f"{spec.carrier} ma koszt tylko dla {metrics['costed_shipments']} / {metrics['shipments_total']} "
                        f"przesylek w oknie {window_from}..{window_to} ({metrics['cost_coverage_pct']:.2f}%)."
                    ),
                    "detail_json": {
                        "carrier": spec.carrier,
                        "metric": "cost_coverage_pct",
                        "threshold_pct": cost_coverage_min_pct,
                        "metrics": metrics,
                    },
                },
                {
                    "name": f"Courier {spec.carrier} link coverage",
                    "rule_type": "courier_link_coverage",
                    "severity": "warning",
                    "description": f"{spec.carrier} shipment-to-order link coverage below threshold.",
                    "current_value": metrics["link_coverage_pct"],
                    "is_triggered": metrics["shipments_total"] > 0 and metrics["link_coverage_pct"] < link_coverage_min_pct,
                    "title": f"Courier {spec.carrier}: order-link coverage degraded",
                    "detail": (
                        f"{spec.carrier} ma link do zamowienia dla {metrics['linked_shipments']} / {metrics['shipments_total']} "
                        f"przesylek w oknie {window_from}..{window_to} ({metrics['link_coverage_pct']:.2f}%)."
                    ),
                    "detail_json": {
                        "carrier": spec.carrier,
                        "metric": "link_coverage_pct",
                        "threshold_pct": link_coverage_min_pct,
                        "metrics": metrics,
                    },
                },
                {
                    "name": f"Courier {spec.carrier} shadow health",
                    "rule_type": "courier_shadow_health",
                    "severity": "warning",
                    "description": f"{spec.carrier} shadow drift exceeded threshold or shadow coverage is missing.",
                    "current_value": metrics["shadow_delta_pct"],
                    "is_triggered": (
                        (metrics["shipments_total"] > 0 and metrics["shadow_rows"] == 0)
                        or metrics["shadow_delta_pct"] > shadow_delta_max_pct
                        or metrics["legacy_only_rows"] > 0
                    ),
                    "title": f"Courier {spec.carrier}: shadow drift requires review",
                    "detail": (
                        f"{spec.carrier} shadow rows={metrics['shadow_rows']}, delta={metrics['delta_rows']}, "
                        f"shadow_only={metrics['shadow_only_rows']}, legacy_only={metrics['legacy_only_rows']} "
                        f"w oknie {window_from}..{window_to}."
                    ),
                    "detail_json": {
                        "carrier": spec.carrier,
                        "metric": "shadow_delta_pct",
                        "threshold_pct": shadow_delta_max_pct,
                        "metrics": metrics,
                    },
                },
                {
                    "name": f"Courier {spec.carrier} estimation quality",
                    "rule_type": "courier_estimation_quality",
                    "severity": "warning",
                    "description": f"{spec.carrier} preinvoice estimation quality is below threshold.",
                    "current_value": estimation_kpi["mape_pct"],
                    "is_triggered": (
                        estimation_kpi["samples_count"] < max(1, int(estimation_min_samples))
                        or estimation_kpi["mape_pct"] > float(estimation_mape_max_pct)
                        or estimation_kpi["mae_pln"] > float(estimation_mae_max_pln)
                    ),
                    "title": f"Courier {spec.carrier}: estimation quality degraded",
                    "detail": (
                        f"{spec.carrier} estymacja: samples={estimation_kpi['samples_count']}, "
                        f"MAPE={estimation_kpi['mape_pct']:.2f}%, MAE={estimation_kpi['mae_pln']:.2f} PLN "
                        f"(okno {max(1, int(estimation_days_back or 30))} dni)."
                    ),
                    "detail_json": {
                        "carrier": spec.carrier,
                        "metric": "mape_pct",
                        "threshold_mape_pct": float(estimation_mape_max_pct),
                        "threshold_mae_pln": float(estimation_mae_max_pln),
                        "threshold_min_samples": int(estimation_min_samples),
                        "metrics": estimation_kpi,
                    },
                },
            ]

            for rule_def in rule_defs:
                rule_id = _ensure_rule(
                    cur,
                    name=rule_def["name"],
                    rule_type=rule_def["rule_type"],
                    severity=rule_def["severity"],
                    description=rule_def["description"],
                )
                context_json = {
                    "module": "courier",
                    "carrier": spec.carrier,
                    "route": f"/api/v1/{spec.carrier.lower()}/shadow-diff" if spec.carrier == "DHL" else "/api/v1/gls/jobs/shadow-logistics",
                    "window_from": window_from.isoformat(),
                    "window_to": window_to.isoformat(),
                    "calc_version": spec.calc_version,
                }
                if rule_def["is_triggered"]:
                    result = _upsert_singleton_alert(
                        cur,
                        rule_id=rule_id,
                        severity=rule_def["severity"],
                        title=rule_def["title"],
                        detail=rule_def["detail"],
                        current_value=float(rule_def["current_value"] or 0),
                        detail_json=rule_def["detail_json"],
                        context_json=context_json,
                    )
                else:
                    result = {
                        "created": 0,
                        "updated": 0,
                        "resolved": _resolve_singleton_alerts(cur, rule_id=rule_id),
                    }
                stats["created"] += int(result["created"])
                stats["updated"] += int(result["updated"])
                stats["resolved"] += int(result["resolved"])
                stats["items"].append(
                    {
                        "carrier": spec.carrier,
                        "rule_type": rule_def["rule_type"],
                        "triggered": bool(rule_def["is_triggered"]),
                        "current_value": float(rule_def["current_value"] or 0),
                        "metrics": metrics,
                        "estimation_kpi": estimation_kpi,
                    }
                )

        stale_rule_id = _ensure_rule(
            cur,
            name="Courier pipeline stale run",
            rule_type="courier_pipeline_stale_run",
            severity="critical",
            description="Courier order-universe pipeline run appears stale.",
        )
        stale_threshold_min = 20
        cur.execute(
            """
            SELECT TOP 1
                CAST(id AS NVARCHAR(40)) AS id,
                DATEDIFF(minute, ISNULL(last_heartbeat_at, started_at), SYSUTCDATETIME()) AS stale_minutes,
                progress_message
            FROM dbo.acc_al_jobs WITH (NOLOCK)
            WHERE job_type = 'courier_order_universe_linking'
              AND status = 'running'
            ORDER BY created_at DESC
            """
        )
        stale_row = cur.fetchone()
        stale_triggered = bool(stale_row and int(stale_row[1] or 0) >= stale_threshold_min)
        stale_metrics = {
            "stale_threshold_min": stale_threshold_min,
            "stale_minutes": int(stale_row[1] or 0) if stale_row else 0,
            "job_id": str(stale_row[0] or "") if stale_row else "",
            "progress_message": str(stale_row[2] or "") if stale_row else "",
        }
        if stale_triggered:
            result = _upsert_singleton_alert(
                cur,
                rule_id=stale_rule_id,
                severity="critical",
                title="Courier pipeline: stale run detected",
                detail=(
                    f"Run {stale_metrics['job_id']} nie raportuje postepu od "
                    f"{stale_metrics['stale_minutes']} min (threshold={stale_threshold_min} min)."
                ),
                current_value=float(stale_metrics["stale_minutes"]),
                detail_json={"metric": "stale_minutes", "metrics": stale_metrics},
                context_json={"module": "courier", "route": "/api/v1/jobs", "job_type": "courier_order_universe_linking"},
            )
        else:
            result = {
                "created": 0,
                "updated": 0,
                "resolved": _resolve_singleton_alerts(cur, rule_id=stale_rule_id),
            }
        stats["created"] += int(result["created"])
        stats["updated"] += int(result["updated"])
        stats["resolved"] += int(result["resolved"])
        stats["items"].append(
            {
                "carrier": "GLOBAL",
                "rule_type": "courier_pipeline_stale_run",
                "triggered": stale_triggered,
                "current_value": float(stale_metrics["stale_minutes"]),
                "metrics": stale_metrics,
            }
        )

        if bool(settings.COURIER_READINESS_SLA_ENABLED):
            readiness_rule_id = _ensure_rule(
                cur,
                name="Courier readiness closed months",
                rule_type="courier_readiness_closed_months",
                severity="critical",
                description="Courier readiness is NO_GO for closed months (must be 100% coverage).",
            )
            readiness = get_courier_closed_month_readiness(
                months=None,
                carriers=["DHL", "GLS"],
                buffer_days=max(0, int(settings.COURIER_READINESS_SLA_BUFFER_DAYS or 45)),
                as_of=today,
            )
            readiness_triggered = str(readiness.get("overall_go_no_go") or "NO_GO") == "NO_GO"
            summary = readiness.get("summary") or {}
            scopes_go = int(summary.get("scopes_go", 0) or 0)
            scopes_total_closed = int(summary.get("scopes_total_closed", 0) or 0)
            scopes_no_go = int(summary.get("scopes_no_go", 0) or 0)
            if readiness_triggered:
                result = _upsert_singleton_alert(
                    cur,
                    rule_id=readiness_rule_id,
                    severity="critical",
                    title="Courier readiness SLA: NO_GO",
                    detail=(
                        f"Closed months readiness NO_GO (buffer={int(settings.COURIER_READINESS_SLA_BUFFER_DAYS or 45)}d). "
                        f"scopes_go={scopes_go}/{scopes_total_closed}."
                    ),
                    current_value=float(scopes_no_go),
                    detail_json={"metric": "scopes_no_go", "readiness": readiness},
                    context_json={"module": "courier", "route": "/api/v1/courier/closed-month-readiness"},
                )
            else:
                result = {
                    "created": 0,
                    "updated": 0,
                    "resolved": _resolve_singleton_alerts(cur, rule_id=readiness_rule_id),
                }
            stats["created"] += int(result["created"])
            stats["updated"] += int(result["updated"])
            stats["resolved"] += int(result["resolved"])
            stats["items"].append(
                {
                    "carrier": "GLOBAL",
                    "rule_type": "courier_readiness_closed_months",
                    "triggered": readiness_triggered,
                    "current_value": float(scopes_no_go),
                    "metrics": summary,
                }
            )

        conn.commit()
        if stats["created"] or stats["updated"]:
            stats["status"] = "warning"
        return stats
    finally:
        conn.close()
