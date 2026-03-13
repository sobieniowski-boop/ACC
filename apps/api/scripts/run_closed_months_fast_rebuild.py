from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.db_connection import connect_acc
from app.services.dhl_integration import ensure_dhl_schema
from app.services.gls_integration import ensure_gls_schema


def _connect():
    return connect_acc(autocommit=False, timeout=60)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _next_day(value: date) -> date:
    return value + timedelta(days=1)


def _dump(label: str, payload: dict) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {label}")
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    sys.stdout.flush()


def _scalar(cur, sql: str, params: list | None = None) -> int:
    cur.execute(sql, params or [])
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def _drop_temp_tables(cur, names: list[str]) -> None:
    for name in names:
        cur.execute(f"IF OBJECT_ID('tempdb..{name}') IS NOT NULL DROP TABLE {name};")


def _classify_shadow_case() -> str:
    return """
CASE
    WHEN ABS(ISNULL(o.logistics_pln, 0)) <= 0.05 AND ABS(ISNULL(f.total_logistics_pln, 0)) <= 0.05 THEN 'match_zero'
    WHEN ABS(ISNULL(o.logistics_pln, 0) - ISNULL(f.total_logistics_pln, 0)) <= 0.05 THEN 'match'
    WHEN ABS(ISNULL(o.logistics_pln, 0)) <= 0.05 AND ISNULL(f.total_logistics_pln, 0) > 0.05 THEN 'shadow_only'
    WHEN ABS(ISNULL(f.total_logistics_pln, 0)) <= 0.05 AND ISNULL(o.logistics_pln, 0) > 0.05 THEN 'legacy_only'
    ELSE 'delta'
END
"""


def rebuild_dhl_closed_months(*, purchase_from: date, purchase_to: date) -> dict:
    ensure_dhl_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        purchase_to_exclusive = _next_day(purchase_to)
        _drop_temp_tables(
            cur,
            [
                "#dhl_source",
                "#dhl_orders",
                "#dhl_tracking_tokens",
                "#dhl_inner_tokens",
                "#dhl_seed",
                "#dhl_scope_shipments",
                "#dhl_candidates",
                "#dhl_cost_rows",
            ],
        )

        cur.execute(
            """
CREATE TABLE #dhl_source (
    amazon_order_id NVARCHAR(80) NOT NULL,
    acc_order_id UNIQUEIDENTIFIER NULL,
    resolved_bl_order_id BIGINT NULL,
    raw_bl_order_id BIGINT NULL,
    courier_package_nr NVARCHAR(255) NULL,
    courier_inner_number NVARCHAR(255) NULL
);
            """
        )
        cur.execute(
            """
INSERT INTO #dhl_source (
    amazon_order_id,
    acc_order_id,
    resolved_bl_order_id,
    raw_bl_order_id,
    courier_package_nr,
    courier_inner_number
)
SELECT DISTINCT
    o.amazon_order_id,
    o.id,
    CAST(COALESCE(dm.holding_order_id, p.order_id) AS BIGINT) AS resolved_bl_order_id,
    CAST(p.order_id AS BIGINT) AS raw_bl_order_id,
    NULLIF(LTRIM(RTRIM(p.courier_package_nr)), '') AS courier_package_nr,
    NULLIF(LTRIM(RTRIM(p.courier_inner_number)), '') AS courier_inner_number
FROM dbo.acc_cache_packages p WITH (NOLOCK)
LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
  ON dm.dis_order_id = p.order_id
JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
  ON bo.order_id = COALESCE(dm.holding_order_id, p.order_id)
JOIN dbo.acc_order o WITH (NOLOCK)
  ON o.amazon_order_id = bo.external_order_id
WHERE o.fulfillment_channel = 'MFN'
  AND CAST(o.purchase_date AS DATE) >= ?
  AND CAST(o.purchase_date AS DATE) < ?
  AND (
        LOWER(ISNULL(p.courier_code, '')) LIKE '%dhl%'
        OR LOWER(ISNULL(p.courier_other_name, '')) LIKE '%dhl%'
      )

UNION ALL

SELECT DISTINCT
    o.amazon_order_id,
    o.id,
    CAST(COALESCE(dm.holding_order_id, p.order_id) AS BIGINT) AS resolved_bl_order_id,
    CAST(p.order_id AS BIGINT) AS raw_bl_order_id,
    NULLIF(LTRIM(RTRIM(p.courier_package_nr)), '') AS courier_package_nr,
    NULLIF(LTRIM(RTRIM(p.courier_inner_number)), '') AS courier_inner_number
FROM dbo.acc_bl_distribution_package_cache p WITH (NOLOCK)
LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
  ON dm.dis_order_id = p.order_id
JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
  ON bo.order_id = COALESCE(dm.holding_order_id, p.order_id)
JOIN dbo.acc_order o WITH (NOLOCK)
  ON o.amazon_order_id = bo.external_order_id
WHERE o.fulfillment_channel = 'MFN'
  AND CAST(o.purchase_date AS DATE) >= ?
  AND CAST(o.purchase_date AS DATE) < ?
  AND (
        LOWER(ISNULL(p.courier_code, '')) LIKE '%dhl%'
        OR LOWER(ISNULL(p.courier_other_name, '')) LIKE '%dhl%'
      )

UNION ALL

SELECT DISTINCT
    o.amazon_order_id,
    o.id,
    CAST(COALESCE(dm.holding_order_id, d.order_id) AS BIGINT) AS resolved_bl_order_id,
    CAST(d.order_id AS BIGINT) AS raw_bl_order_id,
    NULLIF(LTRIM(RTRIM(d.delivery_package_nr)), '') AS courier_package_nr,
    CAST(NULL AS NVARCHAR(255)) AS courier_inner_number
FROM dbo.acc_bl_distribution_order_cache d WITH (NOLOCK)
LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
  ON dm.dis_order_id = d.order_id
JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
  ON bo.order_id = COALESCE(dm.holding_order_id, d.order_id)
JOIN dbo.acc_order o WITH (NOLOCK)
  ON o.amazon_order_id = bo.external_order_id
WHERE o.fulfillment_channel = 'MFN'
  AND CAST(o.purchase_date AS DATE) >= ?
  AND CAST(o.purchase_date AS DATE) < ?
  AND (
        LOWER(ISNULL(d.delivery_package_module, '')) LIKE '%dhl%'
        OR LOWER(ISNULL(d.delivery_method, '')) LIKE '%dhl%'
      );
            """,
            [
                purchase_from.isoformat(),
                purchase_to_exclusive.isoformat(),
                purchase_from.isoformat(),
                purchase_to_exclusive.isoformat(),
                purchase_from.isoformat(),
                purchase_to_exclusive.isoformat(),
            ],
        )

        cur.execute(
            """
SELECT DISTINCT
    amazon_order_id,
    acc_order_id,
    resolved_bl_order_id
INTO #dhl_orders
FROM #dhl_source;
CREATE INDEX IX_dhl_orders_order ON #dhl_orders(amazon_order_id, resolved_bl_order_id);

SELECT DISTINCT
    courier_package_nr AS token,
    amazon_order_id,
    acc_order_id,
    resolved_bl_order_id
INTO #dhl_tracking_tokens
FROM #dhl_source
WHERE courier_package_nr IS NOT NULL;
CREATE INDEX IX_dhl_tracking_tokens_token ON #dhl_tracking_tokens(token);

SELECT DISTINCT
    courier_inner_number AS token,
    amazon_order_id,
    acc_order_id,
    resolved_bl_order_id
INTO #dhl_inner_tokens
FROM #dhl_source
WHERE courier_inner_number IS NOT NULL;
CREATE INDEX IX_dhl_inner_tokens_token ON #dhl_inner_tokens(token);
            """
        )
        conn.commit()

        cur.execute(
            """
WITH latest_map AS (
    SELECT
        m.parcel_number,
        m.parcel_number_base,
        m.jjd_number,
        m.shipment_type,
        m.ship_date,
        m.delivery_date,
        m.last_event_code,
        m.last_event_at,
        ROW_NUMBER() OVER (
            PARTITION BY m.parcel_number_base
            ORDER BY
                CASE WHEN m.delivery_date IS NOT NULL THEN 0 ELSE 1 END,
                ISNULL(m.last_event_at, ISNULL(m.delivery_date, m.ship_date)) DESC,
                m.imported_at DESC
        ) AS rn
    FROM dbo.acc_dhl_parcel_map m WITH (NOLOCK)
),
billing AS (
    SELECT
        l.parcel_number_base,
        MAX(l.parcel_number) AS parcel_number,
        MAX(l.product_code) AS product_code,
        MAX(l.description) AS description,
        MAX(l.issue_date) AS issue_date,
        MAX(l.sales_date) AS sales_date,
        MAX(l.delivery_date) AS delivery_date,
        CAST(SUM(ISNULL(l.net_amount, 0)) AS FLOAT) AS total_net_amount,
        COUNT(*) AS line_count
    FROM dbo.acc_dhl_billing_line l WITH (NOLOCK)
    GROUP BY l.parcel_number_base
)
SELECT
    b.parcel_number,
    b.parcel_number_base,
    NULLIF(LTRIM(RTRIM(m.jjd_number)), '') AS jjd_number,
    NULLIF(LTRIM(RTRIM(m.shipment_type)), '') AS shipment_type,
    m.ship_date,
    COALESCE(m.delivery_date, CAST(b.delivery_date AS DATETIME2)) AS delivery_date,
    NULLIF(LTRIM(RTRIM(m.last_event_code)), '') AS last_event_code,
    m.last_event_at,
    NULLIF(LTRIM(RTRIM(b.product_code)), '') AS product_code,
    NULLIF(LTRIM(RTRIM(b.description)), '') AS description,
    b.issue_date,
    b.sales_date,
    b.total_net_amount,
    b.line_count
INTO #dhl_seed
FROM billing b
LEFT JOIN latest_map m
  ON m.parcel_number_base = b.parcel_number_base
 AND m.rn = 1
WHERE EXISTS (
        SELECT 1
        FROM #dhl_tracking_tokens t
        WHERE t.token = b.parcel_number_base
           OR t.token = b.parcel_number
           OR (m.jjd_number IS NOT NULL AND t.token = m.jjd_number)
    )
   OR EXISTS (
        SELECT 1
        FROM #dhl_inner_tokens t
        WHERE t.token = b.parcel_number_base
           OR t.token = b.parcel_number
    );

CREATE INDEX IX_dhl_seed_parcel_base ON #dhl_seed(parcel_number_base);
            """
        )
        conn.commit()

        cur.execute(
            """
MERGE dbo.acc_shipment AS target
USING (
    SELECT
        parcel_number_base,
        parcel_number,
        jjd_number,
        shipment_type,
        ship_date,
        delivery_date,
        last_event_code,
        product_code,
        sales_date
    FROM #dhl_seed
) AS src
ON target.carrier = 'DHL' AND target.shipment_number = src.parcel_number_base
WHEN MATCHED THEN
    UPDATE SET
        piece_id = COALESCE(src.jjd_number, src.parcel_number_base),
        tracking_number = COALESCE(src.jjd_number, src.parcel_number_base),
        service_code = COALESCE(src.product_code, src.shipment_type),
        ship_date = COALESCE(CAST(src.ship_date AS DATE), src.sales_date),
        created_at_carrier = COALESCE(src.ship_date, CAST(src.sales_date AS DATETIME2)),
        status_code = COALESCE(src.last_event_code, CASE WHEN src.delivery_date IS NOT NULL THEN 'DELIVERED' ELSE 'BILLING_IMPORTED' END),
        status_label = COALESCE(src.last_event_code, CASE WHEN src.delivery_date IS NOT NULL THEN 'Delivered from billing files' ELSE 'Imported from billing files' END),
        is_delivered = CASE WHEN src.delivery_date IS NOT NULL THEN 1 ELSE 0 END,
        delivered_at = src.delivery_date,
        source_system = 'dhl_billing_files',
        last_seen_at = SYSUTCDATETIME(),
        last_sync_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        id, carrier, carrier_account, shipment_number, piece_id, tracking_number, cedex_number,
        service_code, ship_date, created_at_carrier, status_code, status_label, received_by,
        is_delivered, delivered_at, recipient_name, recipient_country, shipper_name, shipper_country,
        source_system, source_payload_json, source_payload_hash, first_seen_at, last_seen_at, last_sync_at
    )
    VALUES (
        NEWID(), 'DHL', NULL, src.parcel_number_base, COALESCE(src.jjd_number, src.parcel_number_base),
        COALESCE(src.jjd_number, src.parcel_number_base), NULL, COALESCE(src.product_code, src.shipment_type),
        COALESCE(CAST(src.ship_date AS DATE), src.sales_date), COALESCE(src.ship_date, CAST(src.sales_date AS DATETIME2)),
        COALESCE(src.last_event_code, CASE WHEN src.delivery_date IS NOT NULL THEN 'DELIVERED' ELSE 'BILLING_IMPORTED' END),
        COALESCE(src.last_event_code, CASE WHEN src.delivery_date IS NOT NULL THEN 'Delivered from billing files' ELSE 'Imported from billing files' END),
        NULL, CASE WHEN src.delivery_date IS NOT NULL THEN 1 ELSE 0 END, src.delivery_date,
        NULL, NULL, NULL, NULL, 'dhl_billing_files', NULL, NULL, SYSUTCDATETIME(), SYSUTCDATETIME(), SYSUTCDATETIME()
    );
            """
        )
        conn.commit()

        cur.execute(
            """
SELECT
    CAST(s.id AS UNIQUEIDENTIFIER) AS shipment_id,
    sd.parcel_number_base,
    sd.parcel_number,
    sd.jjd_number
INTO #dhl_scope_shipments
FROM #dhl_seed sd
JOIN dbo.acc_shipment s WITH (NOLOCK)
  ON s.carrier = 'DHL'
 AND s.shipment_number = sd.parcel_number_base;

CREATE INDEX IX_dhl_scope_shipments_id ON #dhl_scope_shipments(shipment_id);
CREATE INDEX IX_dhl_scope_shipments_base ON #dhl_scope_shipments(parcel_number_base);
            """
        )
        cur.execute(
            """
DELETE l
FROM dbo.acc_shipment_order_link l
JOIN #dhl_scope_shipments s
  ON s.shipment_id = l.shipment_id;
            """
        )
        cur.execute(
            """
CREATE TABLE #dhl_candidates (
    shipment_id UNIQUEIDENTIFIER NOT NULL,
    amazon_order_id NVARCHAR(80) NOT NULL,
    acc_order_id UNIQUEIDENTIFIER NULL,
    bl_order_id BIGINT NULL,
    link_method NVARCHAR(64) NOT NULL,
    link_confidence DECIMAL(9,4) NOT NULL
);
            """
        )
        cur.execute(
            """
INSERT INTO #dhl_candidates (shipment_id, amazon_order_id, acc_order_id, bl_order_id, link_method, link_confidence)
SELECT s.shipment_id, t.amazon_order_id, t.acc_order_id, t.resolved_bl_order_id, 'billing_jjd', CAST(1.0 AS DECIMAL(9,4))
FROM #dhl_scope_shipments s
JOIN #dhl_seed sd
  ON sd.parcel_number_base = s.parcel_number_base
JOIN #dhl_tracking_tokens t
  ON sd.jjd_number IS NOT NULL
 AND t.token = sd.jjd_number

UNION ALL

SELECT s.shipment_id, t.amazon_order_id, t.acc_order_id, t.resolved_bl_order_id, 'billing_parcel_inner', CAST(0.98 AS DECIMAL(9,4))
FROM #dhl_scope_shipments s
JOIN #dhl_seed sd
  ON sd.parcel_number_base = s.parcel_number_base
JOIN #dhl_inner_tokens t
  ON t.token = sd.parcel_number_base

UNION ALL

SELECT s.shipment_id, t.amazon_order_id, t.acc_order_id, t.resolved_bl_order_id, 'billing_parcel_inner_raw', CAST(0.94 AS DECIMAL(9,4))
FROM #dhl_scope_shipments s
JOIN #dhl_seed sd
  ON sd.parcel_number_base = s.parcel_number_base
JOIN #dhl_inner_tokens t
  ON sd.parcel_number IS NOT NULL
 AND sd.parcel_number <> sd.parcel_number_base
 AND t.token = sd.parcel_number

UNION ALL

SELECT s.shipment_id, t.amazon_order_id, t.acc_order_id, t.resolved_bl_order_id, 'billing_parcel_tracking', CAST(0.90 AS DECIMAL(9,4))
FROM #dhl_scope_shipments s
JOIN #dhl_seed sd
  ON sd.parcel_number_base = s.parcel_number_base
JOIN #dhl_tracking_tokens t
  ON t.token = sd.parcel_number_base

UNION ALL

SELECT s.shipment_id, t.amazon_order_id, t.acc_order_id, t.resolved_bl_order_id, 'billing_parcel_tracking_raw', CAST(0.86 AS DECIMAL(9,4))
FROM #dhl_scope_shipments s
JOIN #dhl_seed sd
  ON sd.parcel_number_base = s.parcel_number_base
JOIN #dhl_tracking_tokens t
  ON sd.parcel_number IS NOT NULL
 AND sd.parcel_number <> sd.parcel_number_base
 AND t.token = sd.parcel_number;
            """
        )
        cur.execute(
            """
;WITH dedup AS (
    SELECT
        shipment_id,
        amazon_order_id,
        acc_order_id,
        bl_order_id,
        link_method,
        MAX(link_confidence) AS link_confidence
    FROM #dhl_candidates
    GROUP BY shipment_id, amazon_order_id, acc_order_id, bl_order_id, link_method
),
ranked AS (
    SELECT
        d.*,
        MAX(link_confidence) OVER (PARTITION BY shipment_id) AS max_confidence,
        COUNT(*) OVER (PARTITION BY shipment_id, link_confidence) AS same_conf_count,
        ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY link_confidence DESC, amazon_order_id ASC, link_method ASC) AS row_num
    FROM dedup d
)
INSERT INTO dbo.acc_shipment_order_link (
    id, shipment_id, amazon_order_id, acc_order_id, bl_order_id,
    link_method, link_confidence, is_primary, created_at, updated_at
)
SELECT
    NEWID(),
    shipment_id,
    amazon_order_id,
    acc_order_id,
    CAST(bl_order_id AS INT),
    link_method,
    link_confidence,
    CASE WHEN link_confidence = max_confidence AND same_conf_count = 1 AND row_num = 1 THEN 1 ELSE 0 END,
    SYSUTCDATETIME(),
    SYSUTCDATETIME()
FROM ranked;
            """
        )
        conn.commit()

        cur.execute(
            """
WITH billing_costs AS (
    SELECT
        parcel_number_base,
        CAST(SUM(ISNULL(net_amount, 0)) AS DECIMAL(18,4)) AS net_amount,
        CAST(SUM(ISNULL(fuel_road_fee, 0)) AS DECIMAL(18,4)) AS fuel_amount,
        CAST(SUM(ISNULL(net_amount, 0)) AS DECIMAL(18,4)) AS gross_amount,
        MAX(issue_date) AS invoice_date,
        LEFT(CONVERT(NVARCHAR(10), MAX(COALESCE(sales_date, issue_date)), 126), 7) AS billing_period
    FROM dbo.acc_dhl_billing_line WITH (NOLOCK)
    GROUP BY parcel_number_base
)
SELECT
    s.shipment_id,
    b.net_amount,
    b.fuel_amount,
    b.gross_amount,
    b.invoice_date,
    b.billing_period
INTO #dhl_cost_rows
FROM #dhl_scope_shipments s
JOIN billing_costs b
  ON b.parcel_number_base = s.parcel_number_base;

CREATE INDEX IX_dhl_cost_rows_shipment ON #dhl_cost_rows(shipment_id);
            """
        )
        cur.execute(
            """
MERGE dbo.acc_shipment_cost AS target
USING #dhl_cost_rows AS src
ON target.shipment_id = src.shipment_id
AND target.cost_source = 'dhl_billing_files'
WHEN MATCHED THEN
    UPDATE SET
        currency = 'PLN',
        net_amount = src.net_amount,
        fuel_amount = src.fuel_amount,
        toll_amount = NULL,
        gross_amount = src.gross_amount,
        invoice_number = NULL,
        invoice_date = src.invoice_date,
        billing_period = src.billing_period,
        is_estimated = 0,
        raw_payload_json = NULL,
        updated_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        id, shipment_id, cost_source, currency, net_amount, fuel_amount, toll_amount,
        gross_amount, invoice_number, invoice_date, billing_period, is_estimated,
        raw_payload_json, created_at, updated_at
    )
    VALUES (
        NEWID(), src.shipment_id, 'dhl_billing_files', 'PLN', src.net_amount, src.fuel_amount, NULL,
        src.gross_amount, NULL, src.invoice_date, src.billing_period, 0, NULL, SYSUTCDATETIME(), SYSUTCDATETIME()
    );
            """
        )
        conn.commit()

        cur.execute(
            """
DELETE f
FROM dbo.acc_order_logistics_fact f
JOIN #gls_orders o
  ON o.amazon_order_id = f.amazon_order_id;

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
                    WHEN 'gls_billing_files' THEN 0
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
    JOIN dbo.acc_shipment s WITH (NOLOCK)
      ON s.id = l.shipment_id
    LEFT JOIN ranked_costs rc
      ON rc.shipment_id = s.id
     AND rc.rn = 1
    JOIN #gls_orders o
      ON o.amazon_order_id = l.amazon_order_id
    WHERE l.is_primary = 1
      AND l.amazon_order_id IS NOT NULL
      AND s.carrier = 'GLS'
    GROUP BY l.amazon_order_id
)
INSERT INTO dbo.acc_order_logistics_fact (
    amazon_order_id,
    acc_order_id,
    shipments_count,
    delivered_shipments_count,
    actual_shipments_count,
    estimated_shipments_count,
    total_logistics_pln,
    last_delivery_at,
    calc_version,
    source_system,
    calculated_at
)
SELECT
    amazon_order_id,
    CASE WHEN acc_order_id IS NULL OR acc_order_id = '' THEN NULL ELSE CAST(acc_order_id AS UNIQUEIDENTIFIER) END,
    shipments_count,
    delivered_shipments_count,
    actual_shipments_count,
    estimated_shipments_count,
    total_logistics_pln,
    last_delivery_at,
    'gls_v1',
    'shipment_aggregate_fast',
    SYSUTCDATETIME()
FROM base;
            """
        )
        conn.commit()

        cur.execute(
            f"""
DELETE s
FROM dbo.acc_order_logistics_shadow s
JOIN #gls_orders o
  ON o.amazon_order_id = s.amazon_order_id;

INSERT INTO dbo.acc_order_logistics_shadow (
    amazon_order_id,
    acc_order_id,
    legacy_logistics_pln,
    shadow_logistics_pln,
    delta_pln,
    delta_abs_pln,
    shipments_count,
    actual_shipments_count,
    estimated_shipments_count,
    comparison_status,
    calc_version,
    calculated_at
)
SELECT
    o.amazon_order_id,
    o.id,
    CAST(ISNULL(o.logistics_pln, 0) AS DECIMAL(18,4)),
    CAST(ISNULL(f.total_logistics_pln, 0) AS DECIMAL(18,4)),
    CAST(ISNULL(f.total_logistics_pln, 0) - ISNULL(o.logistics_pln, 0) AS DECIMAL(18,4)),
    CAST(ABS(ISNULL(f.total_logistics_pln, 0) - ISNULL(o.logistics_pln, 0)) AS DECIMAL(18,4)),
    ISNULL(f.shipments_count, 0),
    ISNULL(f.actual_shipments_count, 0),
    ISNULL(f.estimated_shipments_count, 0),
    {_classify_shadow_case()},
    'gls_v1',
    SYSUTCDATETIME()
FROM dbo.acc_order o WITH (NOLOCK)
JOIN #gls_orders target
  ON target.amazon_order_id = o.amazon_order_id
LEFT JOIN dbo.acc_order_logistics_fact f WITH (NOLOCK)
  ON f.amazon_order_id = o.amazon_order_id
WHERE o.fulfillment_channel = 'MFN';
            """
        )
        conn.commit()

        stats = {
            "orders_universe": _scalar(cur, "SELECT COUNT(DISTINCT amazon_order_id) FROM #gls_orders"),
            "tracking_tokens": _scalar(cur, "SELECT COUNT(*) FROM #gls_tracking_tokens"),
            "blmap_tokens": _scalar(cur, "SELECT COUNT(*) FROM #gls_blmap_tokens"),
            "note1_tokens": _scalar(cur, "SELECT COUNT(*) FROM #gls_note1_tokens"),
            "seed_rows": _scalar(cur, "SELECT COUNT(*) FROM #gls_seed"),
            "scope_shipments": _scalar(cur, "SELECT COUNT(*) FROM #gls_scope_shipments"),
            "linked_shipments": _scalar(
                cur,
                """
SELECT COUNT(DISTINCT l.shipment_id)
FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
JOIN #gls_scope_shipments s
  ON s.shipment_id = l.shipment_id
WHERE l.amazon_order_id IS NOT NULL
                """,
            ),
            "costed_shipments": _scalar(cur, "SELECT COUNT(*) FROM #gls_cost_rows"),
            "orders_with_fact": _scalar(
                cur,
                """
SELECT COUNT(*)
FROM dbo.acc_order_logistics_fact f WITH (NOLOCK)
JOIN #gls_orders o
  ON o.amazon_order_id = f.amazon_order_id
WHERE f.calc_version = 'gls_v1'
                """,
            ),
            "orders_in_shadow": _scalar(
                cur,
                """
SELECT COUNT(*)
FROM dbo.acc_order_logistics_shadow s WITH (NOLOCK)
JOIN #gls_orders o
  ON o.amazon_order_id = s.amazon_order_id
WHERE s.calc_version = 'gls_v1'
                """,
            ),
        }
        return stats
    finally:
        conn.close()

        cur.execute(
            """
DELETE f
FROM dbo.acc_order_logistics_fact f
JOIN #dhl_orders o
  ON o.amazon_order_id = f.amazon_order_id;

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
    JOIN dbo.acc_shipment s WITH (NOLOCK)
      ON s.id = l.shipment_id
    LEFT JOIN ranked_costs rc
      ON rc.shipment_id = s.id
     AND rc.rn = 1
    JOIN #dhl_orders o
      ON o.amazon_order_id = l.amazon_order_id
    WHERE l.is_primary = 1
      AND l.amazon_order_id IS NOT NULL
      AND s.carrier = 'DHL'
    GROUP BY l.amazon_order_id
)
INSERT INTO dbo.acc_order_logistics_fact (
    amazon_order_id,
    acc_order_id,
    shipments_count,
    delivered_shipments_count,
    actual_shipments_count,
    estimated_shipments_count,
    total_logistics_pln,
    last_delivery_at,
    calc_version,
    source_system,
    calculated_at
)
SELECT
    amazon_order_id,
    CASE WHEN acc_order_id IS NULL OR acc_order_id = '' THEN NULL ELSE CAST(acc_order_id AS UNIQUEIDENTIFIER) END,
    shipments_count,
    delivered_shipments_count,
    actual_shipments_count,
    estimated_shipments_count,
    total_logistics_pln,
    last_delivery_at,
    'dhl_v1',
    'shipment_aggregate_fast',
    SYSUTCDATETIME()
FROM base;
            """
        )
        conn.commit()

        cur.execute(
            f"""
DELETE s
FROM dbo.acc_order_logistics_shadow s
JOIN #dhl_orders o
  ON o.amazon_order_id = s.amazon_order_id;

INSERT INTO dbo.acc_order_logistics_shadow (
    amazon_order_id,
    acc_order_id,
    legacy_logistics_pln,
    shadow_logistics_pln,
    delta_pln,
    delta_abs_pln,
    shipments_count,
    actual_shipments_count,
    estimated_shipments_count,
    comparison_status,
    calc_version,
    calculated_at
)
SELECT
    o.amazon_order_id,
    o.id,
    CAST(ISNULL(o.logistics_pln, 0) AS DECIMAL(18,4)),
    CAST(ISNULL(f.total_logistics_pln, 0) AS DECIMAL(18,4)),
    CAST(ISNULL(f.total_logistics_pln, 0) - ISNULL(o.logistics_pln, 0) AS DECIMAL(18,4)),
    CAST(ABS(ISNULL(f.total_logistics_pln, 0) - ISNULL(o.logistics_pln, 0)) AS DECIMAL(18,4)),
    ISNULL(f.shipments_count, 0),
    ISNULL(f.actual_shipments_count, 0),
    ISNULL(f.estimated_shipments_count, 0),
    {_classify_shadow_case()},
    'dhl_v1',
    SYSUTCDATETIME()
FROM dbo.acc_order o WITH (NOLOCK)
JOIN #dhl_orders target
  ON target.amazon_order_id = o.amazon_order_id
LEFT JOIN dbo.acc_order_logistics_fact f WITH (NOLOCK)
  ON f.amazon_order_id = o.amazon_order_id
WHERE o.fulfillment_channel = 'MFN';
            """
        )
        conn.commit()

        stats = {
            "orders_universe": _scalar(cur, "SELECT COUNT(DISTINCT amazon_order_id) FROM #dhl_orders"),
            "tracking_tokens": _scalar(cur, "SELECT COUNT(*) FROM #dhl_tracking_tokens"),
            "inner_tokens": _scalar(cur, "SELECT COUNT(*) FROM #dhl_inner_tokens"),
            "seed_rows": _scalar(cur, "SELECT COUNT(*) FROM #dhl_seed"),
            "scope_shipments": _scalar(cur, "SELECT COUNT(*) FROM #dhl_scope_shipments"),
            "linked_shipments": _scalar(
                cur,
                """
SELECT COUNT(DISTINCT l.shipment_id)
FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
JOIN #dhl_scope_shipments s
  ON s.shipment_id = l.shipment_id
WHERE l.amazon_order_id IS NOT NULL
                """,
            ),
            "costed_shipments": _scalar(cur, "SELECT COUNT(*) FROM #dhl_cost_rows"),
            "orders_with_fact": _scalar(
                cur,
                """
SELECT COUNT(*)
FROM dbo.acc_order_logistics_fact f WITH (NOLOCK)
JOIN #dhl_orders o
  ON o.amazon_order_id = f.amazon_order_id
WHERE f.calc_version = 'dhl_v1'
                """,
            ),
            "orders_in_shadow": _scalar(
                cur,
                """
SELECT COUNT(*)
FROM dbo.acc_order_logistics_shadow s WITH (NOLOCK)
JOIN #dhl_orders o
  ON o.amazon_order_id = s.amazon_order_id
WHERE s.calc_version = 'dhl_v1'
                """,
            ),
        }
        return stats
    finally:
        conn.close()


def rebuild_gls_closed_months(*, purchase_from: date, purchase_to: date) -> dict:
    ensure_gls_schema()
    ensure_dhl_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        purchase_to_exclusive = _next_day(purchase_to)
        _drop_temp_tables(
            cur,
            [
                "#gls_source",
                "#gls_orders",
                "#gls_tracking_tokens",
                "#gls_blmap_tokens",
                "#gls_note1_tokens",
                "#gls_seed",
                "#gls_scope_shipments",
                "#gls_candidates",
                "#gls_cost_rows",
            ],
        )

        cur.execute(
            """
CREATE TABLE #gls_source (
    amazon_order_id NVARCHAR(80) NOT NULL,
    acc_order_id UNIQUEIDENTIFIER NULL,
    resolved_bl_order_id BIGINT NULL,
    raw_bl_order_id BIGINT NULL,
    courier_package_nr NVARCHAR(255) NULL,
    courier_inner_number NVARCHAR(255) NULL
);
            """
        )
        cur.execute(
            """
INSERT INTO #gls_source (
    amazon_order_id,
    acc_order_id,
    resolved_bl_order_id,
    raw_bl_order_id,
    courier_package_nr,
    courier_inner_number
)
SELECT DISTINCT
    o.amazon_order_id,
    o.id,
    CAST(COALESCE(dm.holding_order_id, p.order_id) AS BIGINT) AS resolved_bl_order_id,
    CAST(p.order_id AS BIGINT) AS raw_bl_order_id,
    NULLIF(LTRIM(RTRIM(p.courier_package_nr)), '') AS courier_package_nr,
    NULLIF(LTRIM(RTRIM(p.courier_inner_number)), '') AS courier_inner_number
FROM dbo.acc_cache_packages p WITH (NOLOCK)
LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
  ON dm.dis_order_id = p.order_id
JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
  ON bo.order_id = COALESCE(dm.holding_order_id, p.order_id)
JOIN dbo.acc_order o WITH (NOLOCK)
  ON o.amazon_order_id = bo.external_order_id
WHERE o.fulfillment_channel = 'MFN'
  AND CAST(o.purchase_date AS DATE) >= ?
  AND CAST(o.purchase_date AS DATE) < ?
  AND (
        LOWER(ISNULL(p.courier_code, '')) LIKE '%gls%'
        OR LOWER(ISNULL(p.courier_other_name, '')) LIKE '%gls%'
      )

UNION ALL

SELECT DISTINCT
    o.amazon_order_id,
    o.id,
    CAST(COALESCE(dm.holding_order_id, p.order_id) AS BIGINT) AS resolved_bl_order_id,
    CAST(p.order_id AS BIGINT) AS raw_bl_order_id,
    NULLIF(LTRIM(RTRIM(p.courier_package_nr)), '') AS courier_package_nr,
    NULLIF(LTRIM(RTRIM(p.courier_inner_number)), '') AS courier_inner_number
FROM dbo.acc_bl_distribution_package_cache p WITH (NOLOCK)
LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
  ON dm.dis_order_id = p.order_id
JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
  ON bo.order_id = COALESCE(dm.holding_order_id, p.order_id)
JOIN dbo.acc_order o WITH (NOLOCK)
  ON o.amazon_order_id = bo.external_order_id
WHERE o.fulfillment_channel = 'MFN'
  AND CAST(o.purchase_date AS DATE) >= ?
  AND CAST(o.purchase_date AS DATE) < ?
  AND (
        LOWER(ISNULL(p.courier_code, '')) LIKE '%gls%'
        OR LOWER(ISNULL(p.courier_other_name, '')) LIKE '%gls%'
      )

UNION ALL

SELECT DISTINCT
    o.amazon_order_id,
    o.id,
    CAST(COALESCE(dm.holding_order_id, d.order_id) AS BIGINT) AS resolved_bl_order_id,
    CAST(d.order_id AS BIGINT) AS raw_bl_order_id,
    NULLIF(LTRIM(RTRIM(d.delivery_package_nr)), '') AS courier_package_nr,
    CAST(NULL AS NVARCHAR(255)) AS courier_inner_number
FROM dbo.acc_bl_distribution_order_cache d WITH (NOLOCK)
LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
  ON dm.dis_order_id = d.order_id
JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
  ON bo.order_id = COALESCE(dm.holding_order_id, d.order_id)
JOIN dbo.acc_order o WITH (NOLOCK)
  ON o.amazon_order_id = bo.external_order_id
WHERE o.fulfillment_channel = 'MFN'
  AND CAST(o.purchase_date AS DATE) >= ?
  AND CAST(o.purchase_date AS DATE) < ?
  AND (
        LOWER(ISNULL(d.delivery_package_module, '')) LIKE '%gls%'
        OR LOWER(ISNULL(d.delivery_method, '')) LIKE '%gls%'
      )

UNION ALL

SELECT DISTINCT
    o.amazon_order_id,
    o.id,
    CAST(m.bl_order_id AS BIGINT) AS resolved_bl_order_id,
    CAST(m.bl_order_id AS BIGINT) AS raw_bl_order_id,
    NULLIF(LTRIM(RTRIM(m.tracking_number)), '') AS courier_package_nr,
    CAST(NULL AS NVARCHAR(255)) AS courier_inner_number
FROM dbo.acc_gls_bl_map m WITH (NOLOCK)
JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
  ON bo.order_id = m.bl_order_id
JOIN dbo.acc_order o WITH (NOLOCK)
  ON o.amazon_order_id = bo.external_order_id
WHERE o.fulfillment_channel = 'MFN'
  AND CAST(o.purchase_date AS DATE) >= ?
  AND CAST(o.purchase_date AS DATE) < ?;
            """,
            [
                purchase_from.isoformat(),
                purchase_to_exclusive.isoformat(),
                purchase_from.isoformat(),
                purchase_to_exclusive.isoformat(),
                purchase_from.isoformat(),
                purchase_to_exclusive.isoformat(),
                purchase_from.isoformat(),
                purchase_to_exclusive.isoformat(),
            ],
        )
        cur.execute(
            """
SELECT DISTINCT amazon_order_id, acc_order_id, resolved_bl_order_id
INTO #gls_orders
FROM #gls_source;
CREATE INDEX IX_gls_orders_order ON #gls_orders(amazon_order_id, resolved_bl_order_id);

SELECT DISTINCT courier_package_nr AS token, amazon_order_id, acc_order_id, resolved_bl_order_id
INTO #gls_tracking_tokens
FROM #gls_source
WHERE courier_package_nr IS NOT NULL;
CREATE INDEX IX_gls_tracking_tokens_token ON #gls_tracking_tokens(token);

SELECT DISTINCT
    NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(64), raw_bl_order_id))), '') AS token,
    amazon_order_id,
    acc_order_id,
    resolved_bl_order_id
INTO #gls_note1_tokens
FROM #gls_source
WHERE raw_bl_order_id IS NOT NULL;
INSERT INTO #gls_note1_tokens (token, amazon_order_id, acc_order_id, resolved_bl_order_id)
SELECT DISTINCT
    NULLIF(LTRIM(RTRIM(CONVERT(NVARCHAR(64), resolved_bl_order_id))), '') AS token,
    amazon_order_id,
    acc_order_id,
    resolved_bl_order_id
FROM #gls_source
WHERE resolved_bl_order_id IS NOT NULL;
CREATE INDEX IX_gls_note1_tokens_token ON #gls_note1_tokens(token);

SELECT DISTINCT
    m.tracking_number AS token,
    o.amazon_order_id,
    o.acc_order_id,
    o.resolved_bl_order_id
INTO #gls_blmap_tokens
FROM dbo.acc_gls_bl_map m WITH (NOLOCK)
JOIN #gls_orders o
  ON o.resolved_bl_order_id = CAST(m.bl_order_id AS BIGINT)
WHERE m.tracking_number IS NOT NULL
  AND LTRIM(RTRIM(m.tracking_number)) <> '';
CREATE INDEX IX_gls_blmap_tokens_token ON #gls_blmap_tokens(token);
            """
        )
        conn.commit()

        cur.execute(
            """
SELECT
    l.parcel_number,
    MIN(l.row_date) AS row_date,
    MAX(l.delivery_date) AS delivery_date,
    MAX(NULLIF(LTRIM(RTRIM(l.parcel_status)), '')) AS parcel_status,
    MAX(NULLIF(LTRIM(RTRIM(l.service_code)), '')) AS service_code,
    MAX(NULLIF(LTRIM(RTRIM(l.note1)), '')) AS note1,
    MAX(NULLIF(LTRIM(RTRIM(l.recipient_name)), '')) AS recipient_name,
    MAX(NULLIF(LTRIM(RTRIM(l.recipient_country)), '')) AS recipient_country,
    MAX(l.billing_period) AS billing_period,
    CAST(
        SUM(
            ISNULL(l.net_amount, 0)
            + ISNULL(l.toll_amount, 0)
            + ISNULL(l.fuel_amount, 0)
            + ISNULL(l.storewarehouse_amount, 0)
            + ISNULL(l.surcharge_amount, 0)
        ) AS FLOAT
    ) AS total_amount,
    COUNT(*) AS line_count
INTO #gls_seed
FROM dbo.acc_gls_billing_line l WITH (NOLOCK)
WHERE EXISTS (SELECT 1 FROM #gls_tracking_tokens t WHERE t.token = l.parcel_number)
   OR EXISTS (SELECT 1 FROM #gls_blmap_tokens t WHERE t.token = l.parcel_number)
   OR EXISTS (SELECT 1 FROM #gls_note1_tokens t WHERE t.token = l.note1)
GROUP BY l.parcel_number;

CREATE INDEX IX_gls_seed_parcel ON #gls_seed(parcel_number);
            """
        )
        conn.commit()

        cur.execute(
            """
MERGE dbo.acc_shipment AS target
USING #gls_seed AS src
ON target.carrier = 'GLS' AND target.shipment_number = src.parcel_number
WHEN MATCHED THEN
    UPDATE SET
        piece_id = src.parcel_number,
        tracking_number = src.parcel_number,
        service_code = src.service_code,
        ship_date = src.row_date,
        created_at_carrier = CAST(src.row_date AS DATETIME2),
        status_code = CASE WHEN src.delivery_date IS NOT NULL THEN 'DELIVERED' ELSE 'BILLING_IMPORTED' END,
        status_label = COALESCE(src.parcel_status, CASE WHEN src.delivery_date IS NOT NULL THEN 'Delivered from billing files' ELSE 'Imported from billing files' END),
        is_delivered = CASE WHEN src.delivery_date IS NOT NULL THEN 1 ELSE 0 END,
        delivered_at = CAST(src.delivery_date AS DATETIME2),
        recipient_name = src.recipient_name,
        recipient_country = src.recipient_country,
        source_system = 'gls_billing_files',
        last_seen_at = SYSUTCDATETIME(),
        last_sync_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        id, carrier, carrier_account, shipment_number, piece_id, tracking_number, cedex_number,
        service_code, ship_date, created_at_carrier, status_code, status_label, received_by,
        is_delivered, delivered_at, recipient_name, recipient_country, shipper_name, shipper_country,
        source_system, source_payload_json, source_payload_hash, first_seen_at, last_seen_at, last_sync_at
    )
    VALUES (
        NEWID(), 'GLS', NULL, src.parcel_number, src.parcel_number, src.parcel_number, NULL,
        src.service_code, src.row_date, CAST(src.row_date AS DATETIME2),
        CASE WHEN src.delivery_date IS NOT NULL THEN 'DELIVERED' ELSE 'BILLING_IMPORTED' END,
        COALESCE(src.parcel_status, CASE WHEN src.delivery_date IS NOT NULL THEN 'Delivered from billing files' ELSE 'Imported from billing files' END),
        NULL, CASE WHEN src.delivery_date IS NOT NULL THEN 1 ELSE 0 END, CAST(src.delivery_date AS DATETIME2),
        src.recipient_name, src.recipient_country, NULL, NULL, 'gls_billing_files', NULL, NULL,
        SYSUTCDATETIME(), SYSUTCDATETIME(), SYSUTCDATETIME()
    );
            """
        )
        conn.commit()

        cur.execute(
            """
SELECT
    CAST(s.id AS UNIQUEIDENTIFIER) AS shipment_id,
    sd.parcel_number,
    sd.note1
INTO #gls_scope_shipments
FROM #gls_seed sd
JOIN dbo.acc_shipment s WITH (NOLOCK)
  ON s.carrier = 'GLS'
 AND s.shipment_number = sd.parcel_number;

CREATE INDEX IX_gls_scope_shipments_id ON #gls_scope_shipments(shipment_id);
CREATE INDEX IX_gls_scope_shipments_parcel ON #gls_scope_shipments(parcel_number);
            """
        )
        cur.execute(
            """
DELETE l
FROM dbo.acc_shipment_order_link l
JOIN #gls_scope_shipments s
  ON s.shipment_id = l.shipment_id;
            """
        )
        cur.execute(
            """
CREATE TABLE #gls_candidates (
    shipment_id UNIQUEIDENTIFIER NOT NULL,
    amazon_order_id NVARCHAR(80) NOT NULL,
    acc_order_id UNIQUEIDENTIFIER NULL,
    bl_order_id BIGINT NULL,
    link_method NVARCHAR(64) NOT NULL,
    link_confidence DECIMAL(9,4) NOT NULL
);
            """
        )
        cur.execute(
            """
INSERT INTO #gls_candidates (shipment_id, amazon_order_id, acc_order_id, bl_order_id, link_method, link_confidence)
SELECT s.shipment_id, t.amazon_order_id, t.acc_order_id, t.resolved_bl_order_id, 'billing_parcel_tracking', CAST(1.0 AS DECIMAL(9,4))
FROM #gls_scope_shipments s
JOIN #gls_tracking_tokens t
  ON t.token = s.parcel_number

UNION ALL

SELECT s.shipment_id, t.amazon_order_id, t.acc_order_id, t.resolved_bl_order_id, 'billing_tracking_bl_map', CAST(0.96 AS DECIMAL(9,4))
FROM #gls_scope_shipments s
JOIN #gls_blmap_tokens t
  ON t.token = s.parcel_number

UNION ALL

SELECT s.shipment_id, t.amazon_order_id, t.acc_order_id, t.resolved_bl_order_id, 'billing_note1_bl_order', CAST(0.92 AS DECIMAL(9,4))
FROM #gls_scope_shipments s
JOIN #gls_seed sd
  ON sd.parcel_number = s.parcel_number
JOIN #gls_note1_tokens t
  ON sd.note1 IS NOT NULL
 AND t.token = sd.note1;
            """
        )
        cur.execute(
            """
;WITH dedup AS (
    SELECT
        shipment_id,
        amazon_order_id,
        acc_order_id,
        bl_order_id,
        link_method,
        MAX(link_confidence) AS link_confidence
    FROM #gls_candidates
    GROUP BY shipment_id, amazon_order_id, acc_order_id, bl_order_id, link_method
),
ranked AS (
    SELECT
        d.*,
        MAX(link_confidence) OVER (PARTITION BY shipment_id) AS max_confidence,
        COUNT(*) OVER (PARTITION BY shipment_id, link_confidence) AS same_conf_count,
        ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY link_confidence DESC, amazon_order_id ASC, link_method ASC) AS row_num
    FROM dedup d
)
INSERT INTO dbo.acc_shipment_order_link (
    id, shipment_id, amazon_order_id, acc_order_id, bl_order_id,
    link_method, link_confidence, is_primary, created_at, updated_at
)
SELECT
    NEWID(),
    shipment_id,
    amazon_order_id,
    acc_order_id,
    CAST(bl_order_id AS INT),
    link_method,
    link_confidence,
    CASE WHEN link_confidence = max_confidence AND same_conf_count = 1 AND row_num = 1 THEN 1 ELSE 0 END,
    SYSUTCDATETIME(),
    SYSUTCDATETIME()
FROM ranked;
            """
        )
        conn.commit()

        cur.execute(
            """
WITH billing_costs AS (
    SELECT
        parcel_number,
        CAST(SUM(ISNULL(net_amount, 0)) AS DECIMAL(18,4)) AS net_amount,
        CAST(SUM(ISNULL(toll_amount, 0)) AS DECIMAL(18,4)) AS toll_amount,
        CAST(SUM(ISNULL(fuel_amount, 0)) AS DECIMAL(18,4)) AS fuel_amount,
        CAST(SUM(ISNULL(storewarehouse_amount, 0)) AS DECIMAL(18,4)) AS storewarehouse_amount,
        CAST(SUM(ISNULL(surcharge_amount, 0)) AS DECIMAL(18,4)) AS surcharge_amount,
        CAST(
            SUM(
                ISNULL(net_amount, 0)
                + ISNULL(toll_amount, 0)
                + ISNULL(fuel_amount, 0)
                + ISNULL(storewarehouse_amount, 0)
                + ISNULL(surcharge_amount, 0)
            ) AS DECIMAL(18,4)
        ) AS gross_amount,
        MAX(billing_period) AS billing_period,
        MAX(delivery_date) AS invoice_date
    FROM dbo.acc_gls_billing_line WITH (NOLOCK)
    GROUP BY parcel_number
)
SELECT
    s.shipment_id,
    b.net_amount,
    b.fuel_amount,
    b.toll_amount,
    b.gross_amount,
    b.invoice_date,
    b.billing_period
INTO #gls_cost_rows
FROM #gls_scope_shipments s
JOIN billing_costs b
  ON b.parcel_number = s.parcel_number;

CREATE INDEX IX_gls_cost_rows_shipment ON #gls_cost_rows(shipment_id);
            """
        )
        cur.execute(
            """
MERGE dbo.acc_shipment_cost AS target
USING #gls_cost_rows AS src
ON target.shipment_id = src.shipment_id
AND target.cost_source = 'gls_billing_files'
WHEN MATCHED THEN
    UPDATE SET
        currency = 'PLN',
        net_amount = src.net_amount,
        fuel_amount = src.fuel_amount,
        toll_amount = src.toll_amount,
        gross_amount = src.gross_amount,
        invoice_number = NULL,
        invoice_date = src.invoice_date,
        billing_period = src.billing_period,
        is_estimated = 0,
        raw_payload_json = NULL,
        updated_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        id, shipment_id, cost_source, currency, net_amount, fuel_amount, toll_amount,
        gross_amount, invoice_number, invoice_date, billing_period, is_estimated,
        raw_payload_json, created_at, updated_at
    )
    VALUES (
        NEWID(), src.shipment_id, 'gls_billing_files', 'PLN', src.net_amount, src.fuel_amount, src.toll_amount,
        src.gross_amount, NULL, src.invoice_date, src.billing_period, 0, NULL, SYSUTCDATETIME(), SYSUTCDATETIME()
    );
            """
        )
        conn.commit()
