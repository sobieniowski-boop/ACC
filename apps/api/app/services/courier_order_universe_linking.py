from __future__ import annotations

from datetime import date
from typing import Any

from app.core.db_connection import connect_acc


def _connect():
    return connect_acc(autocommit=False, timeout=900)


def _contains_ci(expression: str, needle: str) -> str:
    return f"CHARINDEX('{needle}', LOWER(ISNULL({expression}, ''))) > 0"


def _carrier_predicate(alias: str, carrier: str) -> str:
    key = carrier.strip().upper()
    carrier_expr = (
        f"CASE WHEN {alias}.courier_code = 'blconnectpackages' "
        f"THEN {alias}.courier_other_name ELSE {alias}.courier_code END"
    )
    if key == "DHL":
        return (
            f"({_contains_ci(carrier_expr, 'dhl')} "
            f"OR {_contains_ci(f'{alias}.courier_other_name', 'dhl')})"
        )
    if key == "GLS":
        return (
            f"({_contains_ci(carrier_expr, 'gls')} "
            f"OR {_contains_ci(f'{alias}.courier_other_name', 'gls')})"
        )
    raise ValueError(f"Unsupported carrier '{carrier}'")


def _distribution_order_carrier_predicate(alias: str, carrier: str) -> str:
    key = carrier.strip().upper()
    if key == "DHL":
        return (
            f"({_contains_ci(f'{alias}.delivery_method', 'dhl')} "
            f"OR {_contains_ci(f'{alias}.delivery_package_module', 'dhl')})"
        )
    if key == "GLS":
        return (
            f"({_contains_ci(f'{alias}.delivery_method', 'gls')} "
            f"OR {_contains_ci(f'{alias}.delivery_package_module', 'gls')})"
        )
    raise ValueError(f"Unsupported carrier '{carrier}'")


def backfill_order_links_order_universe(
    *,
    carrier: str,
    purchase_from: date,
    purchase_to: date,
    created_from: date | None = None,
    created_to: date | None = None,
    reset_existing_in_scope: bool = False,
) -> dict[str, Any]:
    carrier_key = carrier.strip().upper()
    if carrier_key not in {"DHL", "GLS"}:
        raise ValueError("carrier must be DHL or GLS")

    created_from_value = created_from or purchase_from
    created_to_value = created_to or purchase_to
    package_carrier_pred = _carrier_predicate("p", carrier_key)
    package_dis_carrier_pred = _carrier_predicate("dp", carrier_key)
    package_do_carrier_pred = _distribution_order_carrier_predicate("dco", carrier_key)

    conn = _connect()
    try:
        cur = conn.cursor()

        cur.execute(
            """
IF OBJECT_ID('tempdb..#order_universe') IS NOT NULL DROP TABLE #order_universe;
CREATE TABLE #order_universe (
    amazon_order_id NVARCHAR(80) NOT NULL,
    acc_order_id NVARCHAR(40) NULL,
    bl_order_id BIGINT NOT NULL,
    universe_role NVARCHAR(24) NOT NULL,
    relation_type NVARCHAR(32) NULL,
    relation_confidence FLOAT NOT NULL
);

IF OBJECT_ID('tempdb..#package_tokens') IS NOT NULL DROP TABLE #package_tokens;
CREATE TABLE #package_tokens (
    token NVARCHAR(120) NOT NULL,
    amazon_order_id NVARCHAR(80) NOT NULL,
    acc_order_id NVARCHAR(40) NULL,
    bl_order_id BIGINT NOT NULL,
    token_source NVARCHAR(32) NOT NULL,
    token_confidence FLOAT NOT NULL,
    universe_role NVARCHAR(24) NOT NULL,
    relation_type NVARCHAR(32) NULL,
    universe_confidence FLOAT NOT NULL
);

IF OBJECT_ID('tempdb..#package_tokens_any') IS NOT NULL DROP TABLE #package_tokens_any;
CREATE TABLE #package_tokens_any (
    token NVARCHAR(120) NOT NULL,
    amazon_order_id NVARCHAR(80) NOT NULL,
    acc_order_id NVARCHAR(40) NULL,
    bl_order_id BIGINT NOT NULL,
    token_source NVARCHAR(32) NOT NULL,
    token_confidence FLOAT NOT NULL,
    universe_role NVARCHAR(24) NOT NULL,
    relation_type NVARCHAR(32) NULL,
    universe_confidence FLOAT NOT NULL
);

IF OBJECT_ID('tempdb..#ship_scope') IS NOT NULL DROP TABLE #ship_scope;
CREATE TABLE #ship_scope (
    shipment_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    shipment_number NVARCHAR(120) NULL,
    tracking_number NVARCHAR(120) NULL,
    piece_id NVARCHAR(120) NULL,
    cedex_number NVARCHAR(120) NULL,
    source_payload_json NVARCHAR(MAX) NULL
);

IF OBJECT_ID('tempdb..#ship_tokens') IS NOT NULL DROP TABLE #ship_tokens;
CREATE TABLE #ship_tokens (
    shipment_id UNIQUEIDENTIFIER NOT NULL,
    token NVARCHAR(120) NOT NULL,
    token_source NVARCHAR(32) NOT NULL,
    token_confidence FLOAT NOT NULL
);

IF OBJECT_ID('tempdb..#candidates') IS NOT NULL DROP TABLE #candidates;
CREATE TABLE #candidates (
    shipment_id UNIQUEIDENTIFIER NOT NULL,
    amazon_order_id NVARCHAR(80) NOT NULL,
    acc_order_id NVARCHAR(40) NULL,
    bl_order_id BIGINT NULL,
    link_method NVARCHAR(64) NOT NULL,
    link_confidence FLOAT NOT NULL
);
            """
        )

        cur.execute(
            """
WITH direct_orders AS (
    SELECT
        o.amazon_order_id,
        CAST(o.id AS NVARCHAR(40)) AS acc_order_id,
        CAST(bo.order_id AS BIGINT) AS bl_order_id
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
      ON bo.external_order_id = o.amazon_order_id
    WHERE o.fulfillment_channel = 'MFN'
      AND CAST(o.purchase_date AS DATE) >= ?
      AND CAST(o.purchase_date AS DATE) <= ?
),
distribution_orders AS (
    SELECT
        o.amazon_order_id,
        CAST(o.id AS NVARCHAR(40)) AS acc_order_id,
        CAST(COALESCE(dm.holding_order_id, dco.order_id) AS BIGINT) AS bl_order_id
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_bl_distribution_order_cache dco WITH (NOLOCK)
      ON dco.external_order_id = o.amazon_order_id
    LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
      ON dm.dis_order_id = dco.order_id
    WHERE o.fulfillment_channel = 'MFN'
      AND CAST(o.purchase_date AS DATE) >= ?
      AND CAST(o.purchase_date AS DATE) <= ?
)
INSERT INTO #order_universe (amazon_order_id, acc_order_id, bl_order_id, universe_role, relation_type, relation_confidence)
SELECT DISTINCT amazon_order_id, acc_order_id, bl_order_id, 'direct', NULL, 1.0
FROM (
    SELECT * FROM direct_orders
    UNION ALL
    SELECT * FROM distribution_orders
) u
WHERE amazon_order_id IS NOT NULL
  AND LTRIM(RTRIM(amazon_order_id)) <> ''
  AND bl_order_id IS NOT NULL;

INSERT INTO #order_universe (amazon_order_id, acc_order_id, bl_order_id, universe_role, relation_type, relation_confidence)
SELECT DISTINCT
    r.source_amazon_order_id,
    COALESCE(CAST(r.source_acc_order_id AS NVARCHAR(40)), u.acc_order_id),
    CAST(COALESCE(r.related_bl_order_id, r.related_distribution_order_id) AS BIGINT),
    'related',
    r.relation_type,
    CASE
        WHEN CAST(r.confidence AS FLOAT) > 1.0 THEN 1.0
        ELSE CAST(r.confidence AS FLOAT)
    END AS relation_confidence
FROM dbo.acc_order_courier_relation r WITH (NOLOCK)
LEFT JOIN #order_universe u
  ON u.amazon_order_id = r.source_amazon_order_id
WHERE r.carrier = ?
  AND r.is_strong = 1
  AND r.source_purchase_date >= ?
  AND r.source_purchase_date <= ?
  AND COALESCE(r.related_bl_order_id, r.related_distribution_order_id) IS NOT NULL
  AND NOT EXISTS (
        SELECT 1
        FROM #order_universe existing
        WHERE existing.amazon_order_id = r.source_amazon_order_id
          AND existing.bl_order_id = CAST(COALESCE(r.related_bl_order_id, r.related_distribution_order_id) AS BIGINT)
    );
            """,
            [
                purchase_from.isoformat(),
                purchase_to.isoformat(),
                purchase_from.isoformat(),
                purchase_to.isoformat(),
                carrier_key,
                purchase_from.isoformat(),
                purchase_to.isoformat(),
            ],
        )

        cur.execute(
            f"""
INSERT INTO #package_tokens (
    token, amazon_order_id, acc_order_id, bl_order_id,
    token_source, token_confidence, universe_role, relation_type, universe_confidence
)
SELECT DISTINCT
    tok.token,
    u.amazon_order_id,
    u.acc_order_id,
    u.bl_order_id,
    tok.token_source,
    tok.token_confidence,
    u.universe_role,
    u.relation_type,
    u.relation_confidence
FROM #order_universe u
JOIN (
    SELECT
        COALESCE(dm.holding_order_id, p.order_id) AS resolved_bl_order_id,
        p.courier_package_nr,
        p.courier_inner_number,
        p.courier_code,
        p.courier_other_name
    FROM dbo.acc_cache_packages p WITH (NOLOCK)
    LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
      ON dm.dis_order_id = p.order_id
) p
  ON p.resolved_bl_order_id = u.bl_order_id
CROSS APPLY (
    VALUES
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(p.courier_package_nr, ''))), ' ', '')), 'courier_package_nr', 1.0),
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(p.courier_inner_number, ''))), ' ', '')), 'courier_inner_number', 0.98)
) tok(token, token_source, token_confidence)
WHERE {package_carrier_pred}
  AND tok.token <> '';

INSERT INTO #package_tokens (
    token, amazon_order_id, acc_order_id, bl_order_id,
    token_source, token_confidence, universe_role, relation_type, universe_confidence
)
SELECT DISTINCT
    tok.token,
    u.amazon_order_id,
    u.acc_order_id,
    u.bl_order_id,
    tok.token_source,
    tok.token_confidence,
    u.universe_role,
    u.relation_type,
    u.relation_confidence
FROM #order_universe u
JOIN (
    SELECT
        COALESCE(dm.holding_order_id, dp.order_id) AS resolved_bl_order_id,
        dp.order_id,
        dp.courier_package_nr,
        dp.courier_inner_number,
        dp.courier_code,
        dp.courier_other_name
    FROM dbo.acc_bl_distribution_package_cache dp WITH (NOLOCK)
    LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
      ON dm.dis_order_id = dp.order_id
) dp
  ON dp.resolved_bl_order_id = u.bl_order_id
CROSS APPLY (
    VALUES
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(dp.courier_package_nr, ''))), ' ', '')), 'dis_courier_package_nr', 0.97),
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(dp.courier_inner_number, ''))), ' ', '')), 'dis_courier_inner_number', 0.95)
) tok(token, token_source, token_confidence)
WHERE {package_dis_carrier_pred}
  AND tok.token <> '';

INSERT INTO #package_tokens (
    token, amazon_order_id, acc_order_id, bl_order_id,
    token_source, token_confidence, universe_role, relation_type, universe_confidence
)
SELECT DISTINCT
    tok.token,
    u.amazon_order_id,
    u.acc_order_id,
    u.bl_order_id,
    tok.token_source,
    tok.token_confidence,
    u.universe_role,
    u.relation_type,
    u.relation_confidence
FROM #order_universe u
JOIN (
    SELECT
        COALESCE(dm.holding_order_id, dco.order_id) AS resolved_bl_order_id,
        dco.order_id,
        dco.delivery_method,
        dco.delivery_package_module,
        dco.delivery_package_nr
    FROM dbo.acc_bl_distribution_order_cache dco WITH (NOLOCK)
    LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
      ON dm.dis_order_id = dco.order_id
) dco
  ON dco.resolved_bl_order_id = u.bl_order_id
CROSS APPLY (
    VALUES
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(dco.delivery_package_nr, ''))), ' ', '')), 'dis_delivery_package_nr', 0.94)
) tok(token, token_source, token_confidence)
WHERE {package_do_carrier_pred}
  AND tok.token <> '';

INSERT INTO #package_tokens_any (
    token, amazon_order_id, acc_order_id, bl_order_id,
    token_source, token_confidence, universe_role, relation_type, universe_confidence
)
SELECT DISTINCT
    tok.token,
    u.amazon_order_id,
    u.acc_order_id,
    u.bl_order_id,
    tok.token_source,
    tok.token_confidence,
    u.universe_role,
    u.relation_type,
    u.relation_confidence
FROM #order_universe u
JOIN (
    SELECT
        COALESCE(dm.holding_order_id, p.order_id) AS resolved_bl_order_id,
        p.courier_package_nr,
        p.courier_inner_number,
        p.courier_code,
        p.courier_other_name
    FROM dbo.acc_cache_packages p WITH (NOLOCK)
    LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
      ON dm.dis_order_id = p.order_id
) p
  ON p.resolved_bl_order_id = u.bl_order_id
CROSS APPLY (
    VALUES
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(p.courier_package_nr, ''))), ' ', '')), 'any_courier_package_nr', 1.0),
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(p.courier_inner_number, ''))), ' ', '')), 'any_courier_inner_number', 0.98)
) tok(token, token_source, token_confidence)
WHERE tok.token <> '';

INSERT INTO #package_tokens_any (
    token, amazon_order_id, acc_order_id, bl_order_id,
    token_source, token_confidence, universe_role, relation_type, universe_confidence
)
SELECT DISTINCT
    tok.token,
    u.amazon_order_id,
    u.acc_order_id,
    u.bl_order_id,
    tok.token_source,
    tok.token_confidence,
    u.universe_role,
    u.relation_type,
    u.relation_confidence
FROM #order_universe u
JOIN (
    SELECT
        COALESCE(dm.holding_order_id, dp.order_id) AS resolved_bl_order_id,
        dp.order_id,
        dp.courier_package_nr,
        dp.courier_inner_number
    FROM dbo.acc_bl_distribution_package_cache dp WITH (NOLOCK)
    LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
      ON dm.dis_order_id = dp.order_id
) dp
  ON dp.resolved_bl_order_id = u.bl_order_id
CROSS APPLY (
    VALUES
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(dp.courier_package_nr, ''))), ' ', '')), 'any_dis_courier_package_nr', 0.97),
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(dp.courier_inner_number, ''))), ' ', '')), 'any_dis_courier_inner_number', 0.95)
) tok(token, token_source, token_confidence)
WHERE tok.token <> '';

INSERT INTO #package_tokens_any (
    token, amazon_order_id, acc_order_id, bl_order_id,
    token_source, token_confidence, universe_role, relation_type, universe_confidence
)
SELECT DISTINCT
    tok.token,
    u.amazon_order_id,
    u.acc_order_id,
    u.bl_order_id,
    tok.token_source,
    tok.token_confidence,
    u.universe_role,
    u.relation_type,
    u.relation_confidence
FROM #order_universe u
JOIN (
    SELECT
        COALESCE(dm.holding_order_id, dco.order_id) AS resolved_bl_order_id,
        dco.order_id,
        dco.delivery_package_nr
    FROM dbo.acc_bl_distribution_order_cache dco WITH (NOLOCK)
    LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
      ON dm.dis_order_id = dco.order_id
) dco
  ON dco.resolved_bl_order_id = u.bl_order_id
CROSS APPLY (
    VALUES
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(dco.delivery_package_nr, ''))), ' ', '')), 'any_dis_delivery_package_nr', 0.94)
) tok(token, token_source, token_confidence)
WHERE tok.token <> '';
            """
        )

        cur.execute(
            """
INSERT INTO #ship_scope (shipment_id, shipment_number, tracking_number, piece_id, cedex_number, source_payload_json)
SELECT
    s.id,
    s.shipment_number,
    s.tracking_number,
    s.piece_id,
    s.cedex_number,
    s.source_payload_json
FROM dbo.acc_shipment s WITH (NOLOCK)
WHERE s.carrier = ?
  AND CAST(COALESCE(s.ship_date, CAST(s.created_at_carrier AS DATE), CAST(s.first_seen_at AS DATE)) AS DATE) >= ?
  AND CAST(COALESCE(s.ship_date, CAST(s.created_at_carrier AS DATE), CAST(s.first_seen_at AS DATE)) AS DATE) <= ?;
            """,
            [carrier_key, created_from_value.isoformat(), created_to_value.isoformat()],
        )

        cur.execute(
            """
INSERT INTO #ship_tokens (shipment_id, token, token_source, token_confidence)
SELECT
    ss.shipment_id,
    tok.token,
    tok.token_source,
    tok.token_confidence
FROM #ship_scope ss
CROSS APPLY (
    VALUES
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(ss.tracking_number, ''))), ' ', '')), 'tracking_number', 1.00),
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(ss.shipment_number, ''))), ' ', '')), 'shipment_number', 0.97),
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(ss.piece_id, ''))), ' ', '')), 'piece_id', 0.95),
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(ss.cedex_number, ''))), ' ', '')), 'cedex_number', 0.92)
) tok(token, token_source, token_confidence)
WHERE tok.token <> '';
            """
        )

        if carrier_key == "DHL":
            cur.execute(
                """
INSERT INTO #ship_tokens (shipment_id, token, token_source, token_confidence)
SELECT DISTINCT
    st.shipment_id,
    UPPER(REPLACE(LTRIM(RTRIM(ISNULL(m.parcel_number_base, ''))), ' ', '')) AS token,
    'dhl_jjd_parcel_base' AS token_source,
    CAST(st.token_confidence * 0.96 AS FLOAT) AS token_confidence
FROM #ship_tokens st
JOIN dbo.acc_dhl_parcel_map m WITH (NOLOCK)
  ON m.jjd_number = st.token
WHERE st.token LIKE 'JJD%'
  AND m.parcel_number_base IS NOT NULL
  AND LTRIM(RTRIM(m.parcel_number_base)) <> '';
                """
            )

        cur.execute(
            """
WITH matched AS (
    SELECT
        st.shipment_id,
        pt.amazon_order_id,
        pt.acc_order_id,
        pt.bl_order_id,
        CASE
            WHEN pt.relation_type IS NULL THEN CONCAT('order_universe_', st.token_source, '_', pt.token_source)
            WHEN pt.relation_type = 'replacement_order' THEN CONCAT('order_rel_repl_', st.token_source, '_', pt.token_source)
            ELSE CONCAT('order_rel_reship_', st.token_source, '_', pt.token_source)
        END AS link_method,
        CAST(st.token_confidence * pt.token_confidence * pt.universe_confidence AS FLOAT) AS link_confidence,
        ROW_NUMBER() OVER (
            PARTITION BY
                st.shipment_id,
                pt.amazon_order_id,
                CASE
                    WHEN pt.relation_type IS NULL THEN CONCAT('order_universe_', st.token_source, '_', pt.token_source)
                    WHEN pt.relation_type = 'replacement_order' THEN CONCAT('order_rel_repl_', st.token_source, '_', pt.token_source)
                    ELSE CONCAT('order_rel_reship_', st.token_source, '_', pt.token_source)
                END
            ORDER BY st.token_confidence * pt.token_confidence * pt.universe_confidence DESC
        ) AS rn
    FROM #ship_tokens st
    JOIN #package_tokens pt
      ON pt.token = st.token
)
INSERT INTO #candidates (shipment_id, amazon_order_id, acc_order_id, bl_order_id, link_method, link_confidence)
SELECT shipment_id, amazon_order_id, acc_order_id, bl_order_id, link_method, link_confidence
FROM matched
WHERE rn = 1;
            """
        )

        if carrier_key == "GLS":
            cur.execute(
                """
INSERT INTO #candidates (shipment_id, amazon_order_id, acc_order_id, bl_order_id, link_method, link_confidence)
SELECT
    ss.shipment_id,
    u.amazon_order_id,
    u.acc_order_id,
    u.bl_order_id,
    CASE
        WHEN u.relation_type IS NULL THEN 'order_universe_note1_bl_order'
        WHEN u.relation_type = 'replacement_order' THEN 'order_rel_repl_note1_bl_order'
        ELSE 'order_rel_reship_note1_bl_order'
    END AS link_method,
    CAST(0.93 * u.relation_confidence AS FLOAT) AS link_confidence
FROM #ship_scope ss
LEFT JOIN dbo.acc_cache_dis_map dm_note1 WITH (NOLOCK)
  ON dm_note1.dis_order_id = TRY_CAST(JSON_VALUE(ss.source_payload_json, '$.note1') AS BIGINT)
JOIN #order_universe u
  ON u.bl_order_id = COALESCE(
        dm_note1.holding_order_id,
        TRY_CAST(JSON_VALUE(ss.source_payload_json, '$.note1') AS BIGINT)
     )
WHERE JSON_VALUE(ss.source_payload_json, '$.note1') IS NOT NULL
  AND LTRIM(RTRIM(JSON_VALUE(ss.source_payload_json, '$.note1'))) <> '';

INSERT INTO #candidates (shipment_id, amazon_order_id, acc_order_id, bl_order_id, link_method, link_confidence)
SELECT
    st.shipment_id,
    u.amazon_order_id,
    u.acc_order_id,
    u.bl_order_id,
    CASE
        WHEN u.relation_type IS NULL THEN 'order_universe_gls_bl_map'
        WHEN u.relation_type = 'replacement_order' THEN 'order_rel_repl_gls_bl_map'
        ELSE 'order_rel_reship_gls_bl_map'
    END AS link_method,
    CAST(0.96 * st.token_confidence * u.relation_confidence AS FLOAT) AS link_confidence
FROM #ship_tokens st
JOIN dbo.acc_gls_bl_map gm WITH (NOLOCK)
  ON gm.tracking_number = st.token
LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
  ON dm.dis_order_id = gm.bl_order_id
JOIN #order_universe u
  ON u.bl_order_id = COALESCE(dm.holding_order_id, gm.bl_order_id)
WHERE st.token_source IN ('tracking_number', 'shipment_number', 'piece_id');
                """
            )

        if reset_existing_in_scope:
            cur.execute(
                """
DELETE l
FROM dbo.acc_shipment_order_link l
JOIN #ship_scope ss
  ON ss.shipment_id = l.shipment_id;
                """
            )

        cur.execute(
            """
WITH dedup AS (
    SELECT
        c.shipment_id,
        c.amazon_order_id,
        c.acc_order_id,
        c.bl_order_id,
        c.link_method,
        MAX(c.link_confidence) AS link_confidence
    FROM #candidates c
    GROUP BY
        c.shipment_id,
        c.amazon_order_id,
        c.acc_order_id,
        c.bl_order_id,
        c.link_method
)
MERGE dbo.acc_shipment_order_link AS target
USING dedup AS src
   ON target.shipment_id = src.shipment_id
  AND target.amazon_order_id = src.amazon_order_id
  AND target.link_method = src.link_method
WHEN MATCHED THEN
    UPDATE SET
        acc_order_id = CASE WHEN src.acc_order_id IS NULL OR src.acc_order_id = '' THEN target.acc_order_id ELSE CAST(src.acc_order_id AS UNIQUEIDENTIFIER) END,
        bl_order_id = COALESCE(src.bl_order_id, target.bl_order_id),
        link_confidence = src.link_confidence,
        updated_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        id, shipment_id, amazon_order_id, acc_order_id, bl_order_id,
        link_method, link_confidence, is_primary, created_at, updated_at
    )
    VALUES (
        NEWID(), src.shipment_id, src.amazon_order_id,
        CASE WHEN src.acc_order_id IS NULL OR src.acc_order_id = '' THEN NULL ELSE CAST(src.acc_order_id AS UNIQUEIDENTIFIER) END,
        src.bl_order_id,
        src.link_method, src.link_confidence, 0, SYSUTCDATETIME(), SYSUTCDATETIME()
    );
            """
        )

        cur.execute(
            """
UPDATE l
SET is_primary = 0,
    updated_at = SYSUTCDATETIME()
FROM dbo.acc_shipment_order_link l
JOIN #ship_scope ss
  ON ss.shipment_id = l.shipment_id;

WITH ranked AS (
    SELECT
        l.id,
        ROW_NUMBER() OVER (
            PARTITION BY l.shipment_id
            ORDER BY l.link_confidence DESC, l.amazon_order_id ASC, l.link_method ASC
        ) AS rn
    FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
    JOIN #ship_scope ss
      ON ss.shipment_id = l.shipment_id
)
UPDATE l
SET is_primary = CASE WHEN r.rn = 1 THEN 1 ELSE 0 END,
    updated_at = SYSUTCDATETIME()
FROM dbo.acc_shipment_order_link l
JOIN ranked r
  ON r.id = l.id;
            """
        )

        gls_summary_sql = (
            "(SELECT COUNT(DISTINCT us.shipment_id) FROM unlinked_shipments us "
            " JOIN #ship_tokens st ON st.shipment_id = us.shipment_id"
            " AND st.token_source IN ('tracking_number', 'shipment_number', 'piece_id')"
            " JOIN dbo.acc_gls_bl_map gm WITH (NOLOCK) ON gm.tracking_number = st.token)"
            if carrier_key == "GLS"
            else "CAST(0 AS INT)"
        )
        note1_numeric_sql = (
            "(SELECT COUNT(*) FROM unlinked_shipments us "
            " WHERE TRY_CAST(JSON_VALUE(us.source_payload_json, '$.note1') AS BIGINT) IS NOT NULL)"
            if carrier_key == "GLS"
            else "CAST(0 AS INT)"
        )
        note1_mapped_sql = (
            "(SELECT COUNT(*) FROM unlinked_shipments us "
            " JOIN #order_universe ou "
            "   ON ou.bl_order_id = TRY_CAST(JSON_VALUE(us.source_payload_json, '$.note1') AS BIGINT))"
            if carrier_key == "GLS"
            else "CAST(0 AS INT)"
        )

        cur.execute(
            f"""
WITH unlinked_shipments AS (
    SELECT ss.shipment_id, ss.shipment_number, ss.source_payload_json
    FROM #ship_scope ss
    LEFT JOIN dbo.acc_shipment_order_link l WITH (NOLOCK)
      ON l.shipment_id = ss.shipment_id
     AND l.is_primary = 1
    WHERE l.shipment_id IS NULL
),
unlinked_ship_token AS (
    SELECT DISTINCT st.shipment_id
    FROM #ship_tokens st
    JOIN unlinked_shipments us
      ON us.shipment_id = st.shipment_id
),
unlinked_any_package_match AS (
    SELECT DISTINCT st.shipment_id
    FROM #ship_tokens st
    JOIN unlinked_shipments us
      ON us.shipment_id = st.shipment_id
    JOIN #package_tokens_any pt
      ON pt.token = st.token
),
unlinked_carrier_filtered_match AS (
    SELECT DISTINCT st.shipment_id
    FROM #ship_tokens st
    JOIN unlinked_shipments us
      ON us.shipment_id = st.shipment_id
    JOIN #package_tokens pt
      ON pt.token = st.token
)
SELECT
    (SELECT COUNT(*) FROM #ship_scope) AS shipments_in_scope,
    (SELECT COUNT(DISTINCT l.shipment_id)
     FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
     JOIN #ship_scope ss ON ss.shipment_id = l.shipment_id) AS shipments_with_any_link,
    (SELECT COUNT(DISTINCT l.shipment_id)
     FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
     JOIN #ship_scope ss ON ss.shipment_id = l.shipment_id
     WHERE l.is_primary = 1) AS shipments_with_primary_link,
    (SELECT COUNT(*)
     FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
     JOIN #ship_scope ss ON ss.shipment_id = l.shipment_id) AS links_total_in_scope,
    (SELECT COUNT(*) FROM #candidates) AS candidate_rows,
    (SELECT COUNT(*) FROM unlinked_shipments) AS shipments_unlinked,
    (SELECT COUNT(*) FROM unlinked_ship_token) AS unlinked_shipments_with_core_token,
    (SELECT COUNT(*) FROM unlinked_any_package_match) AS unlinked_shipments_with_any_package_token_match,
    (SELECT COUNT(*) FROM unlinked_carrier_filtered_match) AS unlinked_shipments_with_carrier_filtered_package_token_match,
    {gls_summary_sql} AS unlinked_shipments_present_in_gls_bl_map,
    {note1_numeric_sql} AS unlinked_shipments_with_numeric_note1,
    {note1_mapped_sql} AS unlinked_shipments_note1_mapped_to_order_universe;
            """
        )
        row = cur.fetchone() or (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        conn.commit()
        shipments_unlinked = int(row[5] or 0)
        unlinked_with_any_package_match = int(row[7] or 0)
        unlinked_with_carrier_filtered_package_match = int(row[8] or 0)
        return {
            "carrier": carrier_key,
            "purchase_from": purchase_from.isoformat(),
            "purchase_to": purchase_to.isoformat(),
            "created_from": created_from_value.isoformat(),
            "created_to": created_to_value.isoformat(),
            "reset_existing_in_scope": bool(reset_existing_in_scope),
            "shipments_in_scope": int(row[0] or 0),
            "shipments_with_any_link": int(row[1] or 0),
            "shipments_with_primary_link": int(row[2] or 0),
            "links_total_in_scope": int(row[3] or 0),
            "candidate_rows": int(row[4] or 0),
            "shipments_unlinked": shipments_unlinked,
            "unlinked_shipments_with_core_token": int(row[6] or 0),
            "unlinked_shipments_with_any_package_token_match": unlinked_with_any_package_match,
            "unlinked_shipments_with_carrier_filtered_package_token_match": unlinked_with_carrier_filtered_package_match,
            "unlinked_shipments_suspected_carrier_label_mismatch": max(
                0,
                unlinked_with_any_package_match - unlinked_with_carrier_filtered_package_match,
            ),
            "unlinked_shipments_present_in_gls_bl_map": int(row[9] or 0),
            "unlinked_shipments_with_numeric_note1": int(row[10] or 0),
            "unlinked_shipments_note1_mapped_to_order_universe": int(row[11] or 0),
        }
    finally:
        conn.close()
