from __future__ import annotations

from datetime import date
from typing import Any

from app.core.db_connection import connect_acc
from app.services.dhl_integration import ensure_dhl_schema

_SHADOW_TOLERANCE = 0.05


def _connect():
    return connect_acc(autocommit=False, timeout=30)


def _upsert_logistics_fact(cur, payload: dict[str, Any]) -> None:
    cur.execute(
        """
        SELECT amazon_order_id
        FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
        WHERE amazon_order_id = ?
          AND calc_version = ?
        """,
        [payload["amazon_order_id"], payload["calc_version"]],
    )
    row = cur.fetchone()
    if row:
        cur.execute(
            """
            UPDATE dbo.acc_order_logistics_fact
            SET acc_order_id = CASE WHEN ? IS NULL OR ? = '' THEN acc_order_id ELSE CAST(? AS UNIQUEIDENTIFIER) END,
                shipments_count = ?,
                delivered_shipments_count = ?,
                actual_shipments_count = ?,
                estimated_shipments_count = ?,
                total_logistics_pln = ?,
                last_delivery_at = ?,
                calc_version = ?,
                source_system = ?,
                calculated_at = SYSUTCDATETIME()
            WHERE amazon_order_id = ?
              AND calc_version = ?
            """,
            [
                payload.get("acc_order_id"),
                payload.get("acc_order_id"),
                payload.get("acc_order_id"),
                payload["shipments_count"],
                payload["delivered_shipments_count"],
                payload["actual_shipments_count"],
                payload["estimated_shipments_count"],
                payload["total_logistics_pln"],
                payload.get("last_delivery_at"),
                payload["calc_version"],
                payload["source_system"],
                payload["amazon_order_id"],
                payload["calc_version"],
            ],
        )
        return

    cur.execute(
        """
        INSERT INTO dbo.acc_order_logistics_fact (
            amazon_order_id, acc_order_id, shipments_count, delivered_shipments_count,
            actual_shipments_count, estimated_shipments_count, total_logistics_pln,
            last_delivery_at, calc_version, source_system, calculated_at
        )
        VALUES (
            ?,
            CASE WHEN ? IS NULL OR ? = '' THEN NULL ELSE CAST(? AS UNIQUEIDENTIFIER) END,
            ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME()
        )
        """,
        [
            payload["amazon_order_id"],
            payload.get("acc_order_id"),
            payload.get("acc_order_id"),
            payload.get("acc_order_id"),
            payload["shipments_count"],
            payload["delivered_shipments_count"],
            payload["actual_shipments_count"],
            payload["estimated_shipments_count"],
            payload["total_logistics_pln"],
            payload.get("last_delivery_at"),
            payload["calc_version"],
            payload["source_system"],
        ],
    )


def _classify_shadow_status(legacy_logistics_pln: float, shadow_logistics_pln: float) -> str:
    if abs(legacy_logistics_pln) <= _SHADOW_TOLERANCE and abs(shadow_logistics_pln) <= _SHADOW_TOLERANCE:
        return "match_zero"
    if abs(legacy_logistics_pln - shadow_logistics_pln) <= _SHADOW_TOLERANCE:
        return "match"
    if abs(legacy_logistics_pln) <= _SHADOW_TOLERANCE and shadow_logistics_pln > _SHADOW_TOLERANCE:
        return "shadow_only"
    if abs(shadow_logistics_pln) <= _SHADOW_TOLERANCE and legacy_logistics_pln > _SHADOW_TOLERANCE:
        return "legacy_only"
    return "delta"


def _upsert_shadow_row(cur, payload: dict[str, Any]) -> None:
    cur.execute(
        """
        SELECT amazon_order_id
        FROM dbo.acc_order_logistics_shadow WITH (NOLOCK)
        WHERE amazon_order_id = ?
          AND calc_version = ?
        """,
        [payload["amazon_order_id"], payload["calc_version"]],
    )
    row = cur.fetchone()
    if row:
        cur.execute(
            """
            UPDATE dbo.acc_order_logistics_shadow
            SET acc_order_id = CASE WHEN ? IS NULL OR ? = '' THEN acc_order_id ELSE CAST(? AS UNIQUEIDENTIFIER) END,
                legacy_logistics_pln = ?,
                shadow_logistics_pln = ?,
                delta_pln = ?,
                delta_abs_pln = ?,
                shipments_count = ?,
                actual_shipments_count = ?,
                estimated_shipments_count = ?,
                comparison_status = ?,
                calc_version = ?,
                calculated_at = SYSUTCDATETIME()
            WHERE amazon_order_id = ?
              AND calc_version = ?
            """,
            [
                payload.get("acc_order_id"),
                payload.get("acc_order_id"),
                payload.get("acc_order_id"),
                payload["legacy_logistics_pln"],
                payload["shadow_logistics_pln"],
                payload["delta_pln"],
                payload["delta_abs_pln"],
                payload["shipments_count"],
                payload["actual_shipments_count"],
                payload["estimated_shipments_count"],
                payload["comparison_status"],
                payload["calc_version"],
                payload["amazon_order_id"],
                payload["calc_version"],
            ],
        )
        return

    cur.execute(
        """
        INSERT INTO dbo.acc_order_logistics_shadow (
            amazon_order_id, acc_order_id, legacy_logistics_pln, shadow_logistics_pln,
            delta_pln, delta_abs_pln, shipments_count, actual_shipments_count,
            estimated_shipments_count, comparison_status, calc_version, calculated_at
        )
        VALUES (
            ?,
            CASE WHEN ? IS NULL OR ? = '' THEN NULL ELSE CAST(? AS UNIQUEIDENTIFIER) END,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME()
        )
        """,
        [
            payload["amazon_order_id"],
            payload.get("acc_order_id"),
            payload.get("acc_order_id"),
            payload.get("acc_order_id"),
            payload["legacy_logistics_pln"],
            payload["shadow_logistics_pln"],
            payload["delta_pln"],
            payload["delta_abs_pln"],
            payload["shipments_count"],
            payload["actual_shipments_count"],
            payload["estimated_shipments_count"],
            payload["comparison_status"],
            payload["calc_version"],
        ],
    )


def aggregate_dhl_order_logistics(
    *,
    created_from: date | None = None,
    created_to: date | None = None,
    limit_orders: int = 5000,
    job_id: str | None = None,
) -> dict[str, Any]:
    ensure_dhl_schema()

    from app.connectors.mssql.mssql_store import set_job_progress

    stats = {
        "orders_aggregated": 0,
        "shipments_aggregated": 0,
        "actual_shipments_count": 0,
        "estimated_shipments_count": 0,
    }

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            DELETE f
            FROM dbo.acc_order_logistics_fact f
            WHERE NOT EXISTS (
                SELECT 1
                FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
                JOIN dbo.acc_shipment s WITH (NOLOCK)
                  ON s.id = l.shipment_id
                WHERE l.amazon_order_id = f.amazon_order_id
                  AND l.is_primary = 1
                  AND s.carrier = 'DHL'
            )
              AND f.calc_version = 'dhl_v1'
            """
        )

        where = [
            "s.carrier = 'DHL'",
            "l.is_primary = 1",
            "l.amazon_order_id IS NOT NULL",
        ]
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
                    c.shipment_id,
                    c.is_estimated,
                    COALESCE(
                        c.gross_amount,
                        ISNULL(c.net_amount, 0) + ISNULL(c.fuel_amount, 0) + ISNULL(c.toll_amount, 0)
                    ) AS resolved_amount,
                    ROW_NUMBER() OVER (
                        PARTITION BY c.shipment_id
                        ORDER BY
                            CASE WHEN c.is_estimated = 0 THEN 0 ELSE 1 END,
                            CASE c.cost_source
                                WHEN 'dhl_billing_files' THEN 0
                                WHEN 'invoice_direct' THEN 0
                                WHEN 'invoice_extras' THEN 1
                                WHEN 'dhl_get_price' THEN 2
                                ELSE 9
                            END,
                            ISNULL(c.invoice_date, CAST('1900-01-01' AS DATE)) DESC,
                            c.updated_at DESC
                    ) AS rn
                FROM dbo.acc_shipment_cost c WITH (NOLOCK)
            ),
            base AS (
                SELECT
                    l.amazon_order_id,
                    MAX(CAST(l.acc_order_id AS NVARCHAR(40))) AS acc_order_id,
                    COUNT(DISTINCT CAST(s.id AS NVARCHAR(40))) AS shipments_count,
                    SUM(CASE WHEN s.is_delivered = 1 THEN 1 ELSE 0 END) AS delivered_shipments_count,
                    SUM(CASE WHEN rc.resolved_amount IS NOT NULL THEN CAST(rc.resolved_amount AS FLOAT) ELSE 0 END) AS total_logistics_pln,
                    MAX(CASE WHEN s.is_delivered = 1 THEN s.delivered_at END) AS last_delivery_at,
                    SUM(CASE WHEN rc.resolved_amount IS NOT NULL AND rc.is_estimated = 0 THEN 1 ELSE 0 END) AS actual_shipments_count,
                    SUM(CASE WHEN rc.resolved_amount IS NOT NULL AND rc.is_estimated = 1 THEN 1 ELSE 0 END) AS estimated_shipments_count
                FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
                INNER JOIN dbo.acc_shipment s WITH (NOLOCK)
                    ON s.id = l.shipment_id
                LEFT JOIN ranked_costs rc
                    ON rc.shipment_id = s.id
                   AND rc.rn = 1
                WHERE {' AND '.join(where)}
                GROUP BY l.amazon_order_id
            )
            SELECT TOP {int(limit_orders)}
                amazon_order_id,
                acc_order_id,
                shipments_count,
                delivered_shipments_count,
                actual_shipments_count,
                estimated_shipments_count,
                total_logistics_pln,
                last_delivery_at
            FROM base
            ORDER BY amazon_order_id
        """
        cur.execute(sql, params)
        rows = cur.fetchall()

        if job_id:
            set_job_progress(job_id, progress_pct=20, records_processed=0, message=f"DHL logistics aggregate count={len(rows)}")

        for idx, row in enumerate(rows, start=1):
            payload = {
                "amazon_order_id": str(row[0]),
                "acc_order_id": str(row[1]) if row[1] else None,
                "shipments_count": int(row[2] or 0),
                "delivered_shipments_count": int(row[3] or 0),
                "actual_shipments_count": int(row[4] or 0),
                "estimated_shipments_count": int(row[5] or 0),
                "total_logistics_pln": float(row[6] or 0),
                "last_delivery_at": row[7],
                "calc_version": "dhl_v1",
                "source_system": "shipment_aggregate",
            }
            _upsert_logistics_fact(cur, payload)

            stats["orders_aggregated"] += 1
            stats["shipments_aggregated"] += payload["shipments_count"]
            stats["actual_shipments_count"] += payload["actual_shipments_count"]
            stats["estimated_shipments_count"] += payload["estimated_shipments_count"]

            if idx % 100 == 0:
                conn.commit()
                if job_id:
                    pct = 20 + int((idx / max(len(rows), 1)) * 70)
                    set_job_progress(
                        job_id,
                        progress_pct=min(pct, 95),
                        records_processed=idx,
                        message=f"DHL logistics aggregate processed={idx}/{len(rows)}",
                    )

        conn.commit()
        if job_id:
            set_job_progress(
                job_id,
                progress_pct=95,
                records_processed=stats["orders_aggregated"],
                message="DHL logistics aggregate finished",
            )
        return stats
    finally:
        conn.close()


def build_dhl_logistics_shadow(
    *,
    purchase_from: date | None = None,
    purchase_to: date | None = None,
    limit_orders: int = 10000,
    replace_all_existing: bool = False,
    job_id: str | None = None,
) -> dict[str, Any]:
    ensure_dhl_schema()

    from app.connectors.mssql.mssql_store import set_job_progress

    stats = {
        "orders_compared": 0,
        "match_zero": 0,
        "match": 0,
        "legacy_only": 0,
        "shadow_only": 0,
        "delta": 0,
    }

    conn = _connect()
    try:
        cur = conn.cursor()
        if replace_all_existing:
            cur.execute(
                """
                DELETE FROM dbo.acc_order_logistics_shadow
                WHERE calc_version = 'dhl_v1'
                """
            )
            conn.commit()

        where = [
            "o.fulfillment_channel = 'MFN'",
            """
            EXISTS (
                SELECT 1
                FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
                JOIN dbo.acc_shipment s WITH (NOLOCK)
                  ON s.id = l.shipment_id
                WHERE l.amazon_order_id = o.amazon_order_id
                  AND l.is_primary = 1
                  AND s.carrier = 'DHL'
            )
            """,
            "(ISNULL(o.logistics_pln, 0) <> 0 OR ISNULL(f.total_logistics_pln, 0) <> 0 OR ISNULL(f.shipments_count, 0) > 0)",
        ]
        params: list[Any] = []
        if purchase_from:
            where.append("CAST(o.purchase_date AS DATE) >= ?")
            params.append(purchase_from.isoformat())
        if purchase_to:
            where.append("CAST(o.purchase_date AS DATE) <= ?")
            params.append(purchase_to.isoformat())

        sql = f"""
            SELECT TOP {int(limit_orders)}
                o.amazon_order_id,
                CAST(o.id AS NVARCHAR(40)) AS acc_order_id,
                CAST(ISNULL(o.logistics_pln, 0) AS FLOAT) AS legacy_logistics_pln,
                CAST(ISNULL(f.total_logistics_pln, 0) AS FLOAT) AS shadow_logistics_pln,
                ISNULL(f.shipments_count, 0) AS shipments_count,
                ISNULL(f.actual_shipments_count, 0) AS actual_shipments_count,
                ISNULL(f.estimated_shipments_count, 0) AS estimated_shipments_count
            FROM dbo.acc_order o WITH (NOLOCK)
            LEFT JOIN dbo.acc_order_logistics_fact f WITH (NOLOCK)
              ON f.amazon_order_id = o.amazon_order_id
             AND f.calc_version = 'dhl_v1'
            WHERE {' AND '.join(where)}
            ORDER BY o.purchase_date DESC, o.amazon_order_id DESC
        """
        cur.execute(sql, params)
        rows = cur.fetchall()

        if job_id:
            set_job_progress(job_id, progress_pct=20, records_processed=0, message=f"DHL logistics shadow count={len(rows)}")

        for idx, row in enumerate(rows, start=1):
            legacy = float(row[2] or 0)
            shadow = float(row[3] or 0)
            status = _classify_shadow_status(legacy, shadow)
            payload = {
                "amazon_order_id": str(row[0]),
                "acc_order_id": str(row[1]) if row[1] else None,
                "legacy_logistics_pln": legacy,
                "shadow_logistics_pln": shadow,
                "delta_pln": shadow - legacy,
                "delta_abs_pln": abs(shadow - legacy),
                "shipments_count": int(row[4] or 0),
                "actual_shipments_count": int(row[5] or 0),
                "estimated_shipments_count": int(row[6] or 0),
                "comparison_status": status,
                "calc_version": "dhl_v1",
            }
            _upsert_shadow_row(cur, payload)
            stats["orders_compared"] += 1
            stats[status] += 1

            if idx % 200 == 0:
                conn.commit()
                if job_id:
                    pct = 20 + int((idx / max(len(rows), 1)) * 70)
                    set_job_progress(
                        job_id,
                        progress_pct=min(pct, 95),
                        records_processed=idx,
                        message=f"DHL logistics shadow processed={idx}/{len(rows)}",
                    )

        conn.commit()
        if job_id:
            set_job_progress(
                job_id,
                progress_pct=95,
                records_processed=stats["orders_compared"],
                message="DHL logistics shadow finished",
            )
        return stats
    finally:
        conn.close()
