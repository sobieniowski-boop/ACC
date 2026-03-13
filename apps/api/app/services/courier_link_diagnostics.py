from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from app.core.db_connection import connect_acc
from app.services.courier_order_universe_pipeline import (
    _carrier_predicate,
    _distribution_order_carrier_predicate,
)


def _connect():
    # Diagnostics are read-only but can be materially heavier on production-sized
    # months than a standard API read. Keep a longer timeout to avoid false timeouts.
    return connect_acc(timeout=300)


def _dynamic_default_months(count: int = 1) -> list[str]:
    today = date.today()
    months: list[str] = []
    for offset in range(count, 0, -1):
        month_value = today.month - offset
        year_value = today.year
        while month_value <= 0:
            month_value += 12
            year_value -= 1
        months.append(f"{year_value}-{month_value:02d}")
    return months


def _month_start(token: str) -> date:
    year_str, month_str = token.split("-", 1)
    return date(int(year_str), int(month_str), 1)


def _next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _normalize_months(months: list[str] | None) -> list[str]:
    result: list[str] = []
    for token in months or _dynamic_default_months():
        value = str(token or "").strip()
        if not value:
            continue
        _month_start(value)
        result.append(value)
    if not result:
        raise ValueError("months list cannot be empty")
    return result


def _normalize_carriers(carriers: list[str] | None) -> list[str]:
    result = [str(item or "").strip().upper() for item in (carriers or ["DHL", "GLS"]) if str(item or "").strip()]
    if not result:
        raise ValueError("carriers list cannot be empty")
    for carrier in result:
        if carrier not in {"DHL", "GLS"}:
            raise ValueError(f"Unsupported carrier '{carrier}'")
    return result


def _fetchall_dict(cur) -> list[dict[str, Any]]:
    columns = [col[0] for col in cur.description] if cur.description else []
    return [{columns[idx]: row[idx] for idx in range(len(columns))} for row in cur.fetchall()]


def _setup_scope_tables(
    cur,
    *,
    carrier_key: str,
    purchase_from: date,
    purchase_to: date,
    created_from: date,
    created_to: date,
) -> None:
    package_carrier_pred = _carrier_predicate("p", carrier_key)
    package_dis_carrier_pred = _carrier_predicate("dp", carrier_key)
    package_do_carrier_pred = _distribution_order_carrier_predicate("dco", carrier_key)

    cur.execute(
        """
IF OBJECT_ID('tempdb..#order_universe') IS NOT NULL DROP TABLE #order_universe;
CREATE TABLE #order_universe (
    amazon_order_id NVARCHAR(80) NOT NULL,
    acc_order_id NVARCHAR(40) NULL,
    bl_order_id BIGINT NOT NULL
);

IF OBJECT_ID('tempdb..#package_tokens') IS NOT NULL DROP TABLE #package_tokens;
CREATE TABLE #package_tokens (
    token NVARCHAR(120) NOT NULL,
    amazon_order_id NVARCHAR(80) NOT NULL,
    acc_order_id NVARCHAR(40) NULL,
    bl_order_id BIGINT NOT NULL,
    token_source NVARCHAR(32) NOT NULL,
    token_confidence FLOAT NOT NULL
);

IF OBJECT_ID('tempdb..#package_tokens_any') IS NOT NULL DROP TABLE #package_tokens_any;
CREATE TABLE #package_tokens_any (
    token NVARCHAR(120) NOT NULL,
    amazon_order_id NVARCHAR(80) NOT NULL,
    acc_order_id NVARCHAR(40) NULL,
    bl_order_id BIGINT NOT NULL,
    token_source NVARCHAR(32) NOT NULL,
    token_confidence FLOAT NOT NULL
);

IF OBJECT_ID('tempdb..#ship_scope') IS NOT NULL DROP TABLE #ship_scope;
CREATE TABLE #ship_scope (
    shipment_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
    shipment_number NVARCHAR(120) NULL,
    tracking_number NVARCHAR(120) NULL,
    piece_id NVARCHAR(120) NULL,
    cedex_number NVARCHAR(120) NULL,
    source_system NVARCHAR(64) NULL,
    source_payload_json NVARCHAR(MAX) NULL,
    observed_at DATETIME2 NULL
);

IF OBJECT_ID('tempdb..#ship_tokens') IS NOT NULL DROP TABLE #ship_tokens;
CREATE TABLE #ship_tokens (
    shipment_id UNIQUEIDENTIFIER NOT NULL,
    token NVARCHAR(120) NOT NULL,
    token_source NVARCHAR(32) NOT NULL,
    token_confidence FLOAT NOT NULL
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
INSERT INTO #order_universe (amazon_order_id, acc_order_id, bl_order_id)
SELECT DISTINCT amazon_order_id, acc_order_id, bl_order_id
FROM (
    SELECT * FROM direct_orders
    UNION ALL
    SELECT * FROM distribution_orders
) u
WHERE amazon_order_id IS NOT NULL
  AND LTRIM(RTRIM(amazon_order_id)) <> ''
  AND bl_order_id IS NOT NULL;
        """,
        [
            purchase_from.isoformat(),
            purchase_to.isoformat(),
            purchase_from.isoformat(),
            purchase_to.isoformat(),
        ],
    )

    cur.execute(
        f"""
INSERT INTO #package_tokens (token, amazon_order_id, acc_order_id, bl_order_id, token_source, token_confidence)
SELECT DISTINCT
    tok.token,
    u.amazon_order_id,
    u.acc_order_id,
    u.bl_order_id,
    tok.token_source,
    tok.token_confidence
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

INSERT INTO #package_tokens (token, amazon_order_id, acc_order_id, bl_order_id, token_source, token_confidence)
SELECT DISTINCT
    tok.token,
    u.amazon_order_id,
    u.acc_order_id,
    u.bl_order_id,
    tok.token_source,
    tok.token_confidence
FROM #order_universe u
JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
  ON dm.holding_order_id = u.bl_order_id
JOIN dbo.acc_bl_distribution_package_cache dp WITH (NOLOCK)
  ON dp.order_id = dm.dis_order_id
CROSS APPLY (
    VALUES
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(dp.courier_package_nr, ''))), ' ', '')), 'dis_courier_package_nr', 0.97),
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(dp.courier_inner_number, ''))), ' ', '')), 'dis_courier_inner_number', 0.95)
) tok(token, token_source, token_confidence)
WHERE {package_dis_carrier_pred}
  AND tok.token <> '';

INSERT INTO #package_tokens (token, amazon_order_id, acc_order_id, bl_order_id, token_source, token_confidence)
SELECT DISTINCT
    tok.token,
    u.amazon_order_id,
    u.acc_order_id,
    u.bl_order_id,
    tok.token_source,
    tok.token_confidence
FROM #order_universe u
JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
  ON dm.holding_order_id = u.bl_order_id
JOIN dbo.acc_bl_distribution_order_cache dco WITH (NOLOCK)
  ON dco.order_id = dm.dis_order_id
CROSS APPLY (
    VALUES
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(dco.delivery_package_nr, ''))), ' ', '')), 'dis_delivery_package_nr', 0.94)
) tok(token, token_source, token_confidence)
WHERE {package_do_carrier_pred}
  AND tok.token <> '';

INSERT INTO #package_tokens_any (token, amazon_order_id, acc_order_id, bl_order_id, token_source, token_confidence)
SELECT DISTINCT
    tok.token,
    u.amazon_order_id,
    u.acc_order_id,
    u.bl_order_id,
    tok.token_source,
    tok.token_confidence
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

INSERT INTO #package_tokens_any (token, amazon_order_id, acc_order_id, bl_order_id, token_source, token_confidence)
SELECT DISTINCT
    tok.token,
    u.amazon_order_id,
    u.acc_order_id,
    u.bl_order_id,
    tok.token_source,
    tok.token_confidence
FROM #order_universe u
JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
  ON dm.holding_order_id = u.bl_order_id
JOIN dbo.acc_bl_distribution_package_cache dp WITH (NOLOCK)
  ON dp.order_id = dm.dis_order_id
CROSS APPLY (
    VALUES
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(dp.courier_package_nr, ''))), ' ', '')), 'any_dis_courier_package_nr', 0.97),
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(dp.courier_inner_number, ''))), ' ', '')), 'any_dis_courier_inner_number', 0.95)
) tok(token, token_source, token_confidence)
WHERE tok.token <> '';

INSERT INTO #package_tokens_any (token, amazon_order_id, acc_order_id, bl_order_id, token_source, token_confidence)
SELECT DISTINCT
    tok.token,
    u.amazon_order_id,
    u.acc_order_id,
    u.bl_order_id,
    tok.token_source,
    tok.token_confidence
FROM #order_universe u
JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
  ON dm.holding_order_id = u.bl_order_id
JOIN dbo.acc_bl_distribution_order_cache dco WITH (NOLOCK)
  ON dco.order_id = dm.dis_order_id
CROSS APPLY (
    VALUES
        (UPPER(REPLACE(LTRIM(RTRIM(ISNULL(dco.delivery_package_nr, ''))), ' ', '')), 'any_dis_delivery_package_nr', 0.94)
) tok(token, token_source, token_confidence)
WHERE tok.token <> '';
        """
    )

    cur.execute(
        """
INSERT INTO #ship_scope (
    shipment_id,
    shipment_number,
    tracking_number,
    piece_id,
    cedex_number,
    source_system,
    source_payload_json,
    observed_at
)
SELECT
    s.id,
    s.shipment_number,
    s.tracking_number,
    s.piece_id,
    s.cedex_number,
    s.source_system,
    s.source_payload_json,
    COALESCE(CAST(s.ship_date AS DATETIME2), s.created_at_carrier, s.first_seen_at)
FROM dbo.acc_shipment s WITH (NOLOCK)
WHERE s.carrier = ?
  AND CAST(COALESCE(s.ship_date, CAST(s.created_at_carrier AS DATE), CAST(s.first_seen_at AS DATE)) AS DATE) >= ?
  AND CAST(COALESCE(s.ship_date, CAST(s.created_at_carrier AS DATE), CAST(s.first_seen_at AS DATE)) AS DATE) <= ?;
        """,
        [carrier_key, created_from.isoformat(), created_to.isoformat()],
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
CREATE INDEX IX_order_universe_bl_order_id
    ON #order_universe (bl_order_id);

CREATE INDEX IX_order_universe_amazon_order_id
    ON #order_universe (amazon_order_id);

CREATE INDEX IX_package_tokens_token
    ON #package_tokens (token);

CREATE INDEX IX_package_tokens_bl_order_id
    ON #package_tokens (bl_order_id);

CREATE INDEX IX_package_tokens_any_token
    ON #package_tokens_any (token);

CREATE INDEX IX_package_tokens_any_bl_order_id
    ON #package_tokens_any (bl_order_id);

CREATE INDEX IX_ship_tokens_token
    ON #ship_tokens (token);

CREATE INDEX IX_ship_tokens_shipment_id
    ON #ship_tokens (shipment_id);
        """
    )


def _unlinked_classification_cte_sql(carrier_key: str) -> str:
    base_cte = """
WITH unlinked_shipments AS (
    SELECT
        ss.shipment_id,
        ss.shipment_number,
        ss.tracking_number,
        ss.piece_id,
        ss.cedex_number,
        ss.source_system,
        ss.source_payload_json,
        ss.observed_at,
        CASE WHEN EXISTS (
            SELECT 1
            FROM #ship_tokens st
            WHERE st.shipment_id = ss.shipment_id
        ) THEN 1 ELSE 0 END AS has_core_identifier,
        CASE WHEN EXISTS (
            SELECT 1
            FROM #ship_tokens st
            JOIN #package_tokens pt
              ON pt.token = st.token
            WHERE st.shipment_id = ss.shipment_id
        ) THEN 1 ELSE 0 END AS has_carrier_package_match,
        CASE WHEN EXISTS (
            SELECT 1
            FROM #ship_tokens st
            JOIN #package_tokens_any pt
              ON pt.token = st.token
            WHERE st.shipment_id = ss.shipment_id
        ) THEN 1 ELSE 0 END AS has_any_package_match,
        CASE WHEN EXISTS (
            SELECT 1
            FROM dbo.acc_shipment_cost c WITH (NOLOCK)
            WHERE c.shipment_id = ss.shipment_id
              AND c.is_estimated = 0
        ) THEN 1 ELSE 0 END AS has_actual_cost,
        CASE WHEN EXISTS (
            SELECT 1
            FROM dbo.acc_shipment_cost c WITH (NOLOCK)
            WHERE c.shipment_id = ss.shipment_id
              AND c.is_estimated = 1
        ) THEN 1 ELSE 0 END AS has_estimated_cost
    FROM #ship_scope ss
    WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
        WHERE l.shipment_id = ss.shipment_id
          AND l.is_primary = 1
    )
)
"""
    if carrier_key != "GLS":
        return (
            base_cte
            + """
, classified AS (
    SELECT
        us.*,
        CASE
            WHEN us.has_carrier_package_match = 1 THEN 'carrier_package_token_match'
            WHEN us.has_any_package_match = 1 THEN 'carrier_label_mismatch_suspected'
            WHEN us.has_core_identifier = 0 THEN 'missing_all_core_identifiers'
            ELSE 'core_identifiers_present_no_order_match'
        END AS gap_bucket
    FROM unlinked_shipments us
)
"""
        )

    return (
        base_cte
        + """
, gls_note1 AS (
    SELECT
        us.shipment_id,
        CASE WHEN TRY_CAST(JSON_VALUE(us.source_payload_json, '$.note1') AS BIGINT) IS NOT NULL THEN 1 ELSE 0 END AS note1_numeric,
        CASE WHEN EXISTS (
            SELECT 1
            FROM #order_universe ou
            LEFT JOIN dbo.acc_cache_dis_map dm_note1 WITH (NOLOCK)
              ON dm_note1.dis_order_id = TRY_CAST(JSON_VALUE(us.source_payload_json, '$.note1') AS BIGINT)
            WHERE ou.bl_order_id = COALESCE(
                dm_note1.holding_order_id,
                TRY_CAST(JSON_VALUE(us.source_payload_json, '$.note1') AS BIGINT)
            )
        ) THEN 1 ELSE 0 END AS note1_mapped
    FROM unlinked_shipments us
),
gls_bl_map_matches AS (
    SELECT
        us.shipment_id,
        st.token_source,
        ROW_NUMBER() OVER (
            PARTITION BY us.shipment_id
            ORDER BY CASE st.token_source
                WHEN 'tracking_number' THEN 1
                WHEN 'shipment_number' THEN 2
                WHEN 'piece_id' THEN 3
                ELSE 9
            END
        ) AS rn
    FROM unlinked_shipments us
    JOIN #ship_tokens st
      ON st.shipment_id = us.shipment_id
     AND st.token_source IN ('tracking_number', 'shipment_number', 'piece_id')
    JOIN dbo.acc_gls_bl_map gm WITH (NOLOCK)
      ON gm.tracking_number = st.token
    LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
      ON dm.dis_order_id = gm.bl_order_id
    JOIN #order_universe ou
      ON ou.bl_order_id = COALESCE(dm.holding_order_id, gm.bl_order_id)
),
gls_bl_map_best AS (
    SELECT shipment_id, token_source
    FROM gls_bl_map_matches
    WHERE rn = 1
),
classified AS (
    SELECT
        us.*,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM gls_note1 n
                WHERE n.shipment_id = us.shipment_id
                  AND n.note1_mapped = 1
            ) THEN 'gls_note1_bl_order'
            WHEN EXISTS (
                SELECT 1
                FROM gls_bl_map_best b
                WHERE b.shipment_id = us.shipment_id
                  AND b.token_source = 'tracking_number'
            ) THEN 'gls_bl_map_tracking_number'
            WHEN EXISTS (
                SELECT 1
                FROM gls_bl_map_best b
                WHERE b.shipment_id = us.shipment_id
                  AND b.token_source = 'shipment_number'
            ) THEN 'gls_bl_map_shipment_number'
            WHEN EXISTS (
                SELECT 1
                FROM gls_bl_map_best b
                WHERE b.shipment_id = us.shipment_id
                  AND b.token_source = 'piece_id'
            ) THEN 'gls_bl_map_piece_id'
            WHEN us.has_carrier_package_match = 1 THEN 'carrier_package_token_match'
            WHEN us.has_any_package_match = 1 THEN 'carrier_label_mismatch_suspected'
            WHEN EXISTS (
                SELECT 1
                FROM gls_note1 n
                WHERE n.shipment_id = us.shipment_id
                  AND n.note1_numeric = 1
            ) THEN 'gls_note1_numeric_unmapped'
            WHEN us.has_core_identifier = 0 THEN 'missing_all_core_identifiers'
            ELSE 'core_identifiers_present_no_order_match'
        END AS gap_bucket
    FROM unlinked_shipments us
)
"""
    )


def _linked_missing_actual_cte_sql() -> str:
    return """
WITH linked_missing_actual AS (
    SELECT
        ss.shipment_id,
        ss.shipment_number,
        ss.tracking_number,
        ss.piece_id,
        ss.source_system,
        ss.source_payload_json,
        ss.observed_at,
        l.amazon_order_id,
        CASE WHEN EXISTS (
            SELECT 1
            FROM dbo.acc_shipment_cost c WITH (NOLOCK)
            WHERE c.shipment_id = ss.shipment_id
              AND c.is_estimated = 1
        ) THEN 1 ELSE 0 END AS has_estimated_cost
    FROM #ship_scope ss
    JOIN dbo.acc_shipment_order_link l WITH (NOLOCK)
      ON l.shipment_id = ss.shipment_id
     AND l.is_primary = 1
    WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.acc_shipment_cost c WITH (NOLOCK)
        WHERE c.shipment_id = ss.shipment_id
          AND c.is_estimated = 0
    )
),
classified AS (
    SELECT
        lm.*,
        CASE
            WHEN lm.has_estimated_cost = 1 THEN 'estimated_only'
            WHEN ISNULL(lm.source_system, '') IN ('gls_billing_files', 'dhl_billing_files') THEN 'seeded_from_billing_source'
            ELSE 'linked_no_cost_row'
        END AS gap_bucket
    FROM linked_missing_actual lm
)
"""


def _identifier_pattern_sql(carrier_key: str) -> str:
    if carrier_key == "GLS":
        identifier_case = """
        CASE
            WHEN c.gap_bucket = 'gls_note1_numeric_unmapped' THEN 'gls_note1_numeric_unmapped'
            WHEN ISNULL(tf.has_jjd_token, 0) = 1 THEN 'jjd_like_core_token'
            WHEN ISNULL(tf.has_numeric_token, 0) = 1 THEN 'numeric_core_token'
            WHEN c.has_core_identifier = 0 THEN 'missing_core_identifier'
            ELSE 'non_numeric_core_token'
        END
"""
    else:
        identifier_case = """
        CASE
            WHEN ISNULL(tf.has_jjd_token, 0) = 1 THEN 'jjd_like_core_token'
            WHEN ISNULL(tf.has_numeric_token, 0) = 1 THEN 'numeric_core_token'
            WHEN c.has_core_identifier = 0 THEN 'missing_core_identifier'
            ELSE 'non_numeric_core_token'
        END
"""

    return (
        _unlinked_classification_cte_sql(carrier_key)
        + f"""
, token_flags AS (
    SELECT
        st.shipment_id,
        MAX(CASE WHEN st.token LIKE 'JJD%' THEN 1 ELSE 0 END) AS has_jjd_token,
        MAX(CASE WHEN PATINDEX('%[^0-9]%', st.token) = 0 THEN 1 ELSE 0 END) AS has_numeric_token
    FROM #ship_tokens st
    GROUP BY st.shipment_id
),
patterned AS (
    SELECT
        c.shipment_id,
        c.has_actual_cost,
        c.has_estimated_cost,
        {identifier_case} AS identifier_pattern
    FROM classified c
    LEFT JOIN token_flags tf
      ON tf.shipment_id = c.shipment_id
)
-- courier_link_gap_bucket_identifier_patterns
SELECT
    identifier_pattern,
    COUNT(*) AS shipments,
    SUM(CASE WHEN has_actual_cost = 1 THEN 1 ELSE 0 END) AS shipments_with_actual_cost,
    SUM(CASE WHEN has_actual_cost = 0 AND has_estimated_cost = 1 THEN 1 ELSE 0 END) AS shipments_with_estimated_only
FROM patterned
GROUP BY identifier_pattern
ORDER BY COUNT(*) DESC, identifier_pattern;
"""
    )


def _gls_note1_source_gap_sql() -> str:
    return (
        _unlinked_classification_cte_sql("GLS")
        + """
, focus_shipments AS (
    SELECT
        c.shipment_id,
        TRY_CAST(JSON_VALUE(c.source_payload_json, '$.note1') AS BIGINT) AS note1_bl_order_id
    FROM classified c
    WHERE c.gap_bucket = 'gls_note1_numeric_unmapped'
      AND TRY_CAST(JSON_VALUE(c.source_payload_json, '$.note1') AS BIGINT) IS NOT NULL
),
focus_values AS (
    SELECT DISTINCT note1_bl_order_id
    FROM focus_shipments
),
bl_cache AS (
    SELECT DISTINCT fv.note1_bl_order_id
    FROM focus_values fv
    JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
      ON bo.order_id = fv.note1_bl_order_id
),
dist_cache AS (
    SELECT DISTINCT fv.note1_bl_order_id, dco.external_order_id
    FROM focus_values fv
    JOIN dbo.acc_bl_distribution_order_cache dco WITH (NOLOCK)
      ON dco.order_id = fv.note1_bl_order_id
),
dis_map_raw AS (
    SELECT DISTINCT fv.note1_bl_order_id, dm.holding_order_id
    FROM focus_values fv
    JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
      ON dm.dis_order_id = fv.note1_bl_order_id
    UNION
    SELECT DISTINCT fv.note1_bl_order_id, dm.holding_order_id
    FROM focus_values fv
    JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
      ON dm.holding_order_id = fv.note1_bl_order_id
),
order_universe_hits AS (
    SELECT DISTINCT fv.note1_bl_order_id
    FROM focus_values fv
    JOIN #order_universe ou
      ON ou.bl_order_id = fv.note1_bl_order_id
),
resolved_to_acc_order AS (
    SELECT DISTINCT bc.note1_bl_order_id
    FROM bl_cache bc
    JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
      ON bo.order_id = bc.note1_bl_order_id
    JOIN dbo.acc_order o WITH (NOLOCK)
      ON o.amazon_order_id = bo.external_order_id
     AND o.fulfillment_channel = 'MFN'
    UNION
    SELECT DISTINCT dc.note1_bl_order_id
    FROM dist_cache dc
    JOIN dbo.acc_order o WITH (NOLOCK)
      ON o.amazon_order_id = dc.external_order_id
     AND o.fulfillment_channel = 'MFN'
    UNION
    SELECT DISTINCT dm.note1_bl_order_id
    FROM dis_map_raw dm
    JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
      ON bo.order_id = dm.holding_order_id
    JOIN dbo.acc_order o WITH (NOLOCK)
      ON o.amazon_order_id = bo.external_order_id
     AND o.fulfillment_channel = 'MFN'
)
-- courier_source_gap_gls_note1
SELECT
    (SELECT COUNT(*) FROM focus_shipments) AS shipments,
    (SELECT COUNT(*) FROM focus_values) AS distinct_values,
    (SELECT COUNT(*) FROM bl_cache) AS values_in_bl_orders_cache,
    (SELECT COUNT(*) FROM dist_cache) AS values_in_distribution_order_cache,
    (SELECT COUNT(DISTINCT note1_bl_order_id) FROM dis_map_raw) AS values_in_dis_map,
    (SELECT COUNT(*) FROM order_universe_hits) AS values_in_order_universe,
    (SELECT COUNT(*) FROM resolved_to_acc_order) AS values_resolved_to_acc_order;
"""
    )


def _gls_tracking_source_gap_sql() -> str:
    return (
        _unlinked_classification_cte_sql("GLS")
        + """
, focus_shipments AS (
    SELECT DISTINCT c.shipment_id
    FROM classified c
    WHERE c.gap_bucket = 'core_identifiers_present_no_order_match'
),
focus_tokens AS (
    SELECT DISTINCT
        fs.shipment_id,
        st.token
    FROM focus_shipments fs
    JOIN #ship_tokens st
      ON st.shipment_id = fs.shipment_id
     AND st.token_source IN ('tracking_number', 'shipment_number', 'piece_id')
    WHERE PATINDEX('%[^0-9]%', st.token) = 0
),
focus_values AS (
    SELECT DISTINCT token
    FROM focus_tokens
),
map_raw AS (
    SELECT DISTINCT fv.token, gm.bl_order_id
    FROM focus_values fv
    JOIN dbo.acc_gls_bl_map gm WITH (NOLOCK)
      ON gm.tracking_number = fv.token
),
map_bl_orders_cache AS (
    SELECT DISTINCT mr.token
    FROM map_raw mr
    JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
      ON bo.order_id = mr.bl_order_id
),
map_distribution_cache AS (
    SELECT DISTINCT mr.token, dco.external_order_id
    FROM map_raw mr
    JOIN dbo.acc_bl_distribution_order_cache dco WITH (NOLOCK)
      ON dco.order_id = mr.bl_order_id
),
map_dis_map AS (
    SELECT DISTINCT mr.token, dm.holding_order_id
    FROM map_raw mr
    JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
      ON dm.dis_order_id = mr.bl_order_id
    UNION
    SELECT DISTINCT mr.token, dm.holding_order_id
    FROM map_raw mr
    JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
      ON dm.holding_order_id = mr.bl_order_id
),
map_resolved_to_acc_order AS (
    SELECT DISTINCT mb.token
    FROM map_bl_orders_cache mb
    JOIN map_raw mr
      ON mr.token = mb.token
    JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
      ON bo.order_id = mr.bl_order_id
    JOIN dbo.acc_order o WITH (NOLOCK)
      ON o.amazon_order_id = bo.external_order_id
     AND o.fulfillment_channel = 'MFN'
    UNION
    SELECT DISTINCT md.token
    FROM map_distribution_cache md
    JOIN dbo.acc_order o WITH (NOLOCK)
      ON o.amazon_order_id = md.external_order_id
     AND o.fulfillment_channel = 'MFN'
    UNION
    SELECT DISTINCT mm.token
    FROM map_dis_map mm
    JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
      ON bo.order_id = mm.holding_order_id
    JOIN dbo.acc_order o WITH (NOLOCK)
      ON o.amazon_order_id = bo.external_order_id
     AND o.fulfillment_channel = 'MFN'
)
-- courier_source_gap_gls_tracking
SELECT
    (SELECT COUNT(*) FROM focus_shipments) AS shipments,
    (SELECT COUNT(*) FROM focus_values) AS distinct_values,
    (SELECT COUNT(DISTINCT token) FROM map_raw) AS values_in_gls_bl_map,
    (SELECT COUNT(*) FROM map_bl_orders_cache) AS values_map_bl_order_in_bl_orders_cache,
    (SELECT COUNT(DISTINCT token) FROM map_distribution_cache) AS values_map_bl_order_in_distribution_order_cache,
    (SELECT COUNT(DISTINCT token) FROM map_dis_map) AS values_map_bl_order_in_dis_map,
    (SELECT COUNT(*) FROM map_resolved_to_acc_order) AS values_map_resolved_to_acc_order;
"""
    )


def _dhl_jjd_source_gap_sql() -> str:
    normalized_base = "UPPER(REPLACE(LTRIM(RTRIM(ISNULL(m.parcel_number_base, ''))), ' ', ''))"
    return (
        _unlinked_classification_cte_sql("DHL")
        + f"""
, focus_shipments AS (
    SELECT DISTINCT c.shipment_id
    FROM classified c
    JOIN #ship_tokens st
      ON st.shipment_id = c.shipment_id
     AND st.token LIKE 'JJD%%'
    WHERE c.gap_bucket = 'core_identifiers_present_no_order_match'
),
focus_tokens AS (
    SELECT DISTINCT
        fs.shipment_id,
        st.token
    FROM focus_shipments fs
    JOIN #ship_tokens st
      ON st.shipment_id = fs.shipment_id
     AND st.token LIKE 'JJD%%'
),
focus_values AS (
    SELECT DISTINCT token
    FROM focus_tokens
),
flags AS (
    SELECT
        fv.token,
        CASE WHEN EXISTS (
            SELECT 1
            FROM dbo.acc_dhl_parcel_map m WITH (NOLOCK)
            WHERE m.jjd_number = fv.token
        ) THEN 1 ELSE 0 END AS in_dhl_parcel_map,
        CASE WHEN EXISTS (
            SELECT 1
            FROM dbo.acc_dhl_parcel_map m WITH (NOLOCK)
            JOIN #package_tokens_any pt
              ON pt.token = {normalized_base}
            WHERE m.jjd_number = fv.token
        ) THEN 1 ELSE 0 END AS parcel_map_base_in_package_tokens_any,
        CASE WHEN EXISTS (
            SELECT 1
            FROM dbo.acc_dhl_parcel_map m WITH (NOLOCK)
            JOIN #package_tokens pt
              ON pt.token = {normalized_base}
            WHERE m.jjd_number = fv.token
        ) THEN 1 ELSE 0 END AS parcel_map_base_in_package_tokens_carrier
    FROM focus_values fv
)
-- courier_source_gap_dhl_jjd
SELECT
    (SELECT COUNT(*) FROM focus_shipments) AS shipments,
    COUNT(*) AS distinct_values,
    SUM(in_dhl_parcel_map) AS values_in_dhl_parcel_map,
    SUM(parcel_map_base_in_package_tokens_any) AS values_parcel_map_base_in_package_tokens_any,
    SUM(parcel_map_base_in_package_tokens_carrier) AS values_parcel_map_base_in_package_tokens_carrier
FROM flags;
"""
    )


def _dhl_numeric_source_gap_sql() -> str:
    normalized_base = "UPPER(REPLACE(LTRIM(RTRIM(ISNULL(m.parcel_number_base, ''))), ' ', ''))"
    return (
        _unlinked_classification_cte_sql("DHL")
        + f"""
, focus_shipments AS (
    SELECT DISTINCT c.shipment_id
    FROM classified c
    WHERE c.gap_bucket = 'core_identifiers_present_no_order_match'
      AND EXISTS (
            SELECT 1
            FROM #ship_tokens st
            WHERE st.shipment_id = c.shipment_id
              AND PATINDEX('%[^0-9]%', st.token) = 0
      )
      AND NOT EXISTS (
            SELECT 1
            FROM #ship_tokens st
            WHERE st.shipment_id = c.shipment_id
              AND st.token LIKE 'JJD%%'
      )
),
focus_tokens AS (
    SELECT DISTINCT
        fs.shipment_id,
        st.token
    FROM focus_shipments fs
    JOIN #ship_tokens st
      ON st.shipment_id = fs.shipment_id
    WHERE PATINDEX('%[^0-9]%', st.token) = 0
),
focus_values AS (
    SELECT DISTINCT token
    FROM focus_tokens
),
flags AS (
    SELECT
        fv.token,
        CASE WHEN EXISTS (
            SELECT 1
            FROM dbo.acc_dhl_parcel_map m WITH (NOLOCK)
            WHERE {normalized_base} = fv.token
        ) THEN 1 ELSE 0 END AS in_dhl_parcel_map_base,
        CASE WHEN EXISTS (
            SELECT 1
            FROM #package_tokens_any pt
            WHERE pt.token = fv.token
        ) THEN 1 ELSE 0 END AS in_package_tokens_any,
        CASE WHEN EXISTS (
            SELECT 1
            FROM #package_tokens pt
            WHERE pt.token = fv.token
        ) THEN 1 ELSE 0 END AS in_package_tokens_carrier
    FROM focus_values fv
)
-- courier_source_gap_dhl_numeric
SELECT
    (SELECT COUNT(*) FROM focus_shipments) AS shipments,
    COUNT(*) AS distinct_values,
    SUM(in_dhl_parcel_map_base) AS values_in_dhl_parcel_map_base,
    SUM(in_package_tokens_any) AS values_in_package_tokens_any,
    SUM(in_package_tokens_carrier) AS values_in_package_tokens_carrier
FROM flags;
"""
    )


def _load_pair_identifier_source_gap_summary(
    *,
    month_token: str,
    carrier_key: str,
    created_to_buffer_days: int,
) -> dict[str, Any]:
    month_start = _month_start(month_token)
    month_end = _next_month(month_start)
    purchase_to = month_end - timedelta(days=1)
    created_from = month_start
    created_to = purchase_to + timedelta(days=created_to_buffer_days)

    conn = _connect()
    try:
        cur = conn.cursor()
        _setup_scope_tables(
            cur,
            carrier_key=carrier_key,
            purchase_from=month_start,
            purchase_to=purchase_to,
            created_from=created_from,
            created_to=created_to,
        )

        focus_areas: list[dict[str, Any]] = []
        if carrier_key == "GLS":
            cur.execute(_gls_note1_source_gap_sql())
            note1_row = cur.fetchone() or (0, 0, 0, 0, 0, 0, 0)
            focus_areas.append(
                {
                    "focus_area": "gls_note1_numeric_unmapped",
                    "shipments": int(note1_row[0] or 0),
                    "distinct_values": int(note1_row[1] or 0),
                    "values_in_bl_orders_cache": int(note1_row[2] or 0),
                    "values_in_distribution_order_cache": int(note1_row[3] or 0),
                    "values_in_dis_map": int(note1_row[4] or 0),
                    "values_in_order_universe": int(note1_row[5] or 0),
                    "values_resolved_to_acc_order": int(note1_row[6] or 0),
                }
            )

            cur.execute(_gls_tracking_source_gap_sql())
            tracking_row = cur.fetchone() or (0, 0, 0, 0, 0, 0, 0)
            focus_areas.append(
                {
                    "focus_area": "gls_tracking_numeric_unresolved",
                    "shipments": int(tracking_row[0] or 0),
                    "distinct_values": int(tracking_row[1] or 0),
                    "values_in_gls_bl_map": int(tracking_row[2] or 0),
                    "values_map_bl_order_in_bl_orders_cache": int(tracking_row[3] or 0),
                    "values_map_bl_order_in_distribution_order_cache": int(tracking_row[4] or 0),
                    "values_map_bl_order_in_dis_map": int(tracking_row[5] or 0),
                    "values_map_resolved_to_acc_order": int(tracking_row[6] or 0),
                }
            )
        else:
            cur.execute(_dhl_jjd_source_gap_sql())
            jjd_row = cur.fetchone() or (0, 0, 0, 0, 0)
            focus_areas.append(
                {
                    "focus_area": "dhl_jjd_like",
                    "shipments": int(jjd_row[0] or 0),
                    "distinct_values": int(jjd_row[1] or 0),
                    "values_in_dhl_parcel_map": int(jjd_row[2] or 0),
                    "values_parcel_map_base_in_package_tokens_any": int(jjd_row[3] or 0),
                    "values_parcel_map_base_in_package_tokens_carrier": int(jjd_row[4] or 0),
                }
            )

            cur.execute(_dhl_numeric_source_gap_sql())
            numeric_row = cur.fetchone() or (0, 0, 0, 0, 0)
            focus_areas.append(
                {
                    "focus_area": "dhl_numeric_core",
                    "shipments": int(numeric_row[0] or 0),
                    "distinct_values": int(numeric_row[1] or 0),
                    "values_in_dhl_parcel_map_base": int(numeric_row[2] or 0),
                    "values_in_package_tokens_any": int(numeric_row[3] or 0),
                    "values_in_package_tokens_carrier": int(numeric_row[4] or 0),
                }
            )
    finally:
        conn.close()

    return {
        "month": month_token,
        "carrier": carrier_key,
        "scope": {
            "scope_type": "purchase_month_with_shipment_buffer",
            "purchase_from": month_start.isoformat(),
            "purchase_to": purchase_to.isoformat(),
            "created_from": created_from.isoformat(),
            "created_to": created_to.isoformat(),
            "created_to_buffer_days": int(created_to_buffer_days),
        },
        "focus_areas": focus_areas,
    }


def _gls_note1_order_identity_gap_cte_sql() -> str:
    return """
WITH focus_shipments AS (
    SELECT
        ss.shipment_id,
        TRY_CAST(JSON_VALUE(ss.source_payload_json, '$.note1') AS BIGINT) AS candidate_order_id
    FROM #ship_scope ss
    WHERE TRY_CAST(JSON_VALUE(ss.source_payload_json, '$.note1') AS BIGINT) IS NOT NULL
      AND NOT EXISTS (
            SELECT 1
            FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
            WHERE l.shipment_id = ss.shipment_id
              AND l.is_primary = 1
      )
),
focus_values AS (
    SELECT
        CAST(candidate_order_id AS NVARCHAR(40)) AS candidate_value,
        candidate_order_id,
        COUNT(*) AS shipments
    FROM focus_shipments
    GROUP BY candidate_order_id
),
resolved AS (
    SELECT
        fv.candidate_value,
        fv.shipments,
        CASE WHEN dm.holding_order_id IS NOT NULL THEN 1 ELSE 0 END AS via_dis_map,
        COALESCE(dm.holding_order_id, fv.candidate_order_id) AS resolved_bl_order_id,
        bo.external_order_id,
        ao.acc_order_id,
        CASE
            WHEN dm.holding_order_id IS NOT NULL THEN 'via_dis_map'
            ELSE 'direct_bl_order'
        END AS resolution_path
    FROM focus_values fv
    OUTER APPLY (
        SELECT TOP 1 dm.holding_order_id
        FROM dbo.acc_cache_dis_map dm WITH (NOLOCK)
        WHERE dm.dis_order_id = fv.candidate_order_id
        ORDER BY dm.holding_order_id
    ) dm
    OUTER APPLY (
        SELECT TOP 1 NULLIF(LTRIM(RTRIM(bo.external_order_id)), '') AS external_order_id
        FROM dbo.acc_cache_bl_orders bo WITH (NOLOCK)
        WHERE bo.order_id = COALESCE(dm.holding_order_id, fv.candidate_order_id)
        ORDER BY bo.external_order_id
    ) bo
    OUTER APPLY (
        SELECT TOP 1 CAST(o.id AS NVARCHAR(40)) AS acc_order_id
        FROM dbo.acc_order o WITH (NOLOCK)
        WHERE o.amazon_order_id = bo.external_order_id
        ORDER BY o.purchase_date DESC, o.id DESC
    ) ao
)
"""


def _gls_note1_order_identity_gap_summary_sql() -> str:
    return (
        _gls_note1_order_identity_gap_cte_sql()
        + """
-- courier_order_identity_gap_gls_note1_summary
SELECT
    (SELECT COUNT(*) FROM focus_shipments) AS shipments,
    COUNT(*) AS distinct_values,
    SUM(CASE WHEN via_dis_map = 1 THEN 1 ELSE 0 END) AS values_via_dis_map,
    SUM(CASE WHEN external_order_id IS NOT NULL THEN 1 ELSE 0 END) AS values_with_external_order_id,
    SUM(CASE WHEN acc_order_id IS NOT NULL THEN 1 ELSE 0 END) AS values_resolved_to_acc_order,
    SUM(CASE WHEN external_order_id IS NULL THEN 1 ELSE 0 END) AS values_missing_external_order_id,
    SUM(CASE WHEN external_order_id IS NOT NULL AND acc_order_id IS NULL THEN 1 ELSE 0 END) AS values_missing_acc_order,
    SUM(CASE WHEN external_order_id IS NOT NULL AND acc_order_id IS NULL THEN shipments ELSE 0 END) AS shipments_missing_acc_order
FROM resolved;
"""
    )


def _gls_note1_order_identity_gap_samples_sql(sample_limit: int) -> str:
    return (
        _gls_note1_order_identity_gap_cte_sql()
        + f"""
-- courier_order_identity_gap_gls_note1_samples
SELECT TOP {int(sample_limit)}
    candidate_value,
    shipments,
    CASE
        WHEN external_order_id IS NULL THEN 'missing_external_order_id'
        ELSE 'missing_acc_order'
    END AS break_stage,
    resolution_path,
    resolved_bl_order_id,
    external_order_id,
    acc_order_id
FROM resolved
WHERE external_order_id IS NULL
   OR acc_order_id IS NULL
ORDER BY
    CASE WHEN external_order_id IS NOT NULL AND acc_order_id IS NULL THEN 0 ELSE 1 END,
    shipments DESC,
    candidate_value ASC;
"""
    )


def _dhl_numeric_order_identity_gap_cte_sql() -> str:
    package_carrier_pred = _carrier_predicate("p", "DHL")
    return f"""
WITH focus_shipments AS (
    SELECT DISTINCT ss.shipment_id
    FROM #ship_scope ss
    WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
            WHERE l.shipment_id = ss.shipment_id
              AND l.is_primary = 1
    )
      AND EXISTS (
            SELECT 1
            FROM #ship_tokens st
            WHERE st.shipment_id = ss.shipment_id
              AND PATINDEX('%[^0-9]%', st.token) = 0
      )
      AND NOT EXISTS (
            SELECT 1
            FROM #ship_tokens st
            WHERE st.shipment_id = ss.shipment_id
              AND st.token LIKE 'JJD%'
      )
),
focus_values AS (
    SELECT
        st.token AS candidate_value,
        COUNT(DISTINCT st.shipment_id) AS shipments
    FROM focus_shipments fs
    JOIN #ship_tokens st
      ON st.shipment_id = fs.shipment_id
    WHERE PATINDEX('%[^0-9]%', st.token) = 0
      AND st.token NOT LIKE 'JJD%'
    GROUP BY st.token
),
package_matches AS (
    SELECT
        fv.candidate_value,
        p.order_id
    FROM focus_values fv
    JOIN dbo.acc_cache_packages p WITH (NOLOCK)
      ON p.courier_package_nr = fv.candidate_value
     AND {package_carrier_pred}
    UNION
    SELECT
        fv.candidate_value,
        p.order_id
    FROM focus_values fv
    JOIN dbo.acc_cache_packages p WITH (NOLOCK)
      ON p.courier_inner_number = fv.candidate_value
     AND {package_carrier_pred}
),
match_flags AS (
    SELECT
        fv.candidate_value,
        COUNT(DISTINCT pm.order_id) AS package_order_matches,
        COUNT(DISTINCT NULLIF(LTRIM(RTRIM(bo.external_order_id)), '')) AS external_order_id_matches,
        COUNT(DISTINCT o.id) AS acc_order_matches,
        MIN(pm.order_id) AS sample_package_order_id,
        MIN(COALESCE(dm.holding_order_id, pm.order_id)) AS sample_resolved_bl_order_id,
        MIN(NULLIF(LTRIM(RTRIM(bo.external_order_id)), '')) AS sample_external_order_id,
        MIN(CAST(o.id AS NVARCHAR(40))) AS sample_acc_order_id
    FROM focus_values fv
    LEFT JOIN package_matches pm
      ON pm.candidate_value = fv.candidate_value
    LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
      ON dm.dis_order_id = pm.order_id
    LEFT JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
      ON bo.order_id = COALESCE(dm.holding_order_id, pm.order_id)
    LEFT JOIN dbo.acc_order o WITH (NOLOCK)
      ON o.amazon_order_id = NULLIF(LTRIM(RTRIM(bo.external_order_id)), '')
    GROUP BY fv.candidate_value
),
resolved AS (
    SELECT
        fv.candidate_value,
        fv.shipments,
        ISNULL(mf.package_order_matches, 0) AS package_order_matches,
        ISNULL(mf.external_order_id_matches, 0) AS external_order_id_matches,
        ISNULL(mf.acc_order_matches, 0) AS acc_order_matches,
        mf.sample_package_order_id,
        mf.sample_resolved_bl_order_id,
        mf.sample_external_order_id,
        mf.sample_acc_order_id,
        CASE
            WHEN ISNULL(mf.package_order_matches, 0) = 0 THEN 'missing_package_match'
            WHEN ISNULL(mf.external_order_id_matches, 0) = 0 THEN 'missing_external_order_id'
            WHEN ISNULL(mf.acc_order_matches, 0) = 0 THEN 'missing_acc_order'
            ELSE 'resolved_to_acc_order'
        END AS resolution_path
    FROM focus_values fv
    LEFT JOIN match_flags mf
      ON mf.candidate_value = fv.candidate_value
)
"""


def _dhl_numeric_order_identity_gap_summary_sql() -> str:
    return (
        _dhl_numeric_order_identity_gap_cte_sql()
        + """
-- courier_order_identity_gap_dhl_numeric_summary
SELECT
    (SELECT COUNT(*) FROM focus_shipments) AS shipments,
    COUNT(*) AS distinct_values,
    SUM(CASE WHEN package_order_matches > 0 THEN 1 ELSE 0 END) AS values_with_package_match,
    SUM(CASE WHEN external_order_id_matches > 0 THEN 1 ELSE 0 END) AS values_with_external_order_id,
    SUM(CASE WHEN acc_order_matches > 0 THEN 1 ELSE 0 END) AS values_resolved_to_acc_order,
    SUM(CASE WHEN package_order_matches > 0 AND external_order_id_matches = 0 THEN 1 ELSE 0 END) AS values_missing_external_order_id,
    SUM(CASE WHEN external_order_id_matches > 0 AND acc_order_matches = 0 THEN 1 ELSE 0 END) AS values_missing_acc_order,
    SUM(CASE WHEN external_order_id_matches > 0 AND acc_order_matches = 0 THEN shipments ELSE 0 END) AS shipments_missing_acc_order
FROM resolved;
"""
    )


def _dhl_numeric_order_identity_gap_samples_sql(sample_limit: int) -> str:
    return (
        _dhl_numeric_order_identity_gap_cte_sql()
        + f"""
-- courier_order_identity_gap_dhl_numeric_samples
SELECT TOP {int(sample_limit)}
    candidate_value,
    shipments,
    resolution_path AS break_stage,
    sample_package_order_id,
    sample_resolved_bl_order_id,
    sample_external_order_id,
    sample_acc_order_id,
    package_order_matches,
    external_order_id_matches,
    acc_order_matches
FROM resolved
WHERE acc_order_matches = 0
ORDER BY
    CASE
        WHEN external_order_id_matches > 0 AND acc_order_matches = 0 THEN 0
        WHEN package_order_matches > 0 AND external_order_id_matches = 0 THEN 1
        ELSE 2
    END,
    shipments DESC,
    candidate_value ASC;
"""
    )


def _dhl_jjd_order_identity_gap_cte_sql() -> str:
    package_carrier_pred = _carrier_predicate("p", "DHL")
    return f"""
WITH focus_shipments AS (
    SELECT DISTINCT ss.shipment_id
    FROM #ship_scope ss
    WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
            WHERE l.shipment_id = ss.shipment_id
              AND l.is_primary = 1
    )
      AND EXISTS (
            SELECT 1
            FROM #ship_tokens st
            WHERE st.shipment_id = ss.shipment_id
              AND st.token LIKE 'JJD%'
      )
),
focus_values AS (
    SELECT
        st.token AS candidate_value,
        COUNT(DISTINCT st.shipment_id) AS shipments
    FROM focus_shipments fs
    JOIN #ship_tokens st
      ON st.shipment_id = fs.shipment_id
    WHERE st.token LIKE 'JJD%'
    GROUP BY st.token
),
parcel_map_matches AS (
    SELECT
        fv.candidate_value,
        NULLIF(LTRIM(RTRIM(m.parcel_number_base)), '') AS parcel_number_base
    FROM focus_values fv
    JOIN dbo.acc_dhl_parcel_map m WITH (NOLOCK)
      ON m.jjd_number = fv.candidate_value
    WHERE m.parcel_number_base IS NOT NULL
      AND LTRIM(RTRIM(m.parcel_number_base)) <> ''
),
package_matches AS (
    SELECT
        pm.candidate_value,
        p.order_id
    FROM parcel_map_matches pm
    JOIN dbo.acc_cache_packages p WITH (NOLOCK)
      ON p.courier_package_nr = pm.parcel_number_base
     AND {package_carrier_pred}
    UNION
    SELECT
        pm.candidate_value,
        p.order_id
    FROM parcel_map_matches pm
    JOIN dbo.acc_cache_packages p WITH (NOLOCK)
      ON p.courier_inner_number = pm.parcel_number_base
     AND {package_carrier_pred}
),
match_flags AS (
    SELECT
        fv.candidate_value,
        COUNT(DISTINCT pm.parcel_number_base) AS parcel_map_matches,
        COUNT(DISTINCT pkg.order_id) AS package_order_matches,
        COUNT(DISTINCT NULLIF(LTRIM(RTRIM(bo.external_order_id)), '')) AS external_order_id_matches,
        COUNT(DISTINCT o.id) AS acc_order_matches,
        MIN(pm.parcel_number_base) AS sample_parcel_number_base,
        MIN(pkg.order_id) AS sample_package_order_id,
        MIN(COALESCE(dm.holding_order_id, pkg.order_id)) AS sample_resolved_bl_order_id,
        MIN(NULLIF(LTRIM(RTRIM(bo.external_order_id)), '')) AS sample_external_order_id,
        MIN(CAST(o.id AS NVARCHAR(40))) AS sample_acc_order_id
    FROM focus_values fv
    LEFT JOIN parcel_map_matches pm
      ON pm.candidate_value = fv.candidate_value
    LEFT JOIN package_matches pkg
      ON pkg.candidate_value = fv.candidate_value
    LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
      ON dm.dis_order_id = pkg.order_id
    LEFT JOIN dbo.acc_cache_bl_orders bo WITH (NOLOCK)
      ON bo.order_id = COALESCE(dm.holding_order_id, pkg.order_id)
    LEFT JOIN dbo.acc_order o WITH (NOLOCK)
      ON o.amazon_order_id = NULLIF(LTRIM(RTRIM(bo.external_order_id)), '')
    GROUP BY fv.candidate_value
),
resolved AS (
    SELECT
        fv.candidate_value,
        fv.shipments,
        ISNULL(mf.parcel_map_matches, 0) AS parcel_map_matches,
        ISNULL(mf.package_order_matches, 0) AS package_order_matches,
        ISNULL(mf.external_order_id_matches, 0) AS external_order_id_matches,
        ISNULL(mf.acc_order_matches, 0) AS acc_order_matches,
        mf.sample_parcel_number_base,
        mf.sample_package_order_id,
        mf.sample_resolved_bl_order_id,
        mf.sample_external_order_id,
        mf.sample_acc_order_id,
        CASE
            WHEN ISNULL(mf.parcel_map_matches, 0) = 0 THEN 'missing_parcel_map'
            WHEN ISNULL(mf.package_order_matches, 0) = 0 THEN 'missing_package_match'
            WHEN ISNULL(mf.external_order_id_matches, 0) = 0 THEN 'missing_external_order_id'
            WHEN ISNULL(mf.acc_order_matches, 0) = 0 THEN 'missing_acc_order'
            ELSE 'resolved_to_acc_order'
        END AS resolution_path
    FROM focus_values fv
    LEFT JOIN match_flags mf
      ON mf.candidate_value = fv.candidate_value
)
"""


def _dhl_jjd_order_identity_gap_summary_sql() -> str:
    return (
        _dhl_jjd_order_identity_gap_cte_sql()
        + """
-- courier_order_identity_gap_dhl_jjd_summary
SELECT
    (SELECT COUNT(*) FROM focus_shipments) AS shipments,
    COUNT(*) AS distinct_values,
    SUM(CASE WHEN parcel_map_matches > 0 THEN 1 ELSE 0 END) AS values_with_parcel_map,
    SUM(CASE WHEN package_order_matches > 0 THEN 1 ELSE 0 END) AS values_with_package_match,
    SUM(CASE WHEN external_order_id_matches > 0 THEN 1 ELSE 0 END) AS values_with_external_order_id,
    SUM(CASE WHEN acc_order_matches > 0 THEN 1 ELSE 0 END) AS values_resolved_to_acc_order,
    SUM(CASE WHEN package_order_matches > 0 AND external_order_id_matches = 0 THEN 1 ELSE 0 END) AS values_missing_external_order_id,
    SUM(CASE WHEN external_order_id_matches > 0 AND acc_order_matches = 0 THEN 1 ELSE 0 END) AS values_missing_acc_order,
    SUM(CASE WHEN external_order_id_matches > 0 AND acc_order_matches = 0 THEN shipments ELSE 0 END) AS shipments_missing_acc_order
FROM resolved;
"""
    )


def _dhl_jjd_order_identity_gap_samples_sql(sample_limit: int) -> str:
    return (
        _dhl_jjd_order_identity_gap_cte_sql()
        + f"""
-- courier_order_identity_gap_dhl_jjd_samples
SELECT TOP {int(sample_limit)}
    candidate_value,
    shipments,
    resolution_path AS break_stage,
    sample_parcel_number_base,
    sample_package_order_id,
    sample_resolved_bl_order_id,
    sample_external_order_id,
    sample_acc_order_id,
    parcel_map_matches,
    package_order_matches,
    external_order_id_matches,
    acc_order_matches
FROM resolved
WHERE acc_order_matches = 0
ORDER BY
    CASE
        WHEN external_order_id_matches > 0 AND acc_order_matches = 0 THEN 0
        WHEN package_order_matches > 0 AND external_order_id_matches = 0 THEN 1
        WHEN parcel_map_matches > 0 AND package_order_matches = 0 THEN 2
        ELSE 3
    END,
    shipments DESC,
    candidate_value ASC;
"""
    )


def _load_pair_order_identity_gap_summary(
    *,
    month_token: str,
    carrier_key: str,
    created_to_buffer_days: int,
    sample_limit: int,
) -> dict[str, Any]:
    month_start = _month_start(month_token)
    month_end = _next_month(month_start)
    purchase_to = month_end - timedelta(days=1)
    created_from = month_start
    created_to = purchase_to + timedelta(days=created_to_buffer_days)

    conn = _connect()
    try:
        cur = conn.cursor()
        _setup_scope_tables(
            cur,
            carrier_key=carrier_key,
            purchase_from=month_start,
            purchase_to=purchase_to,
            created_from=created_from,
            created_to=created_to,
        )

        focus_areas: list[dict[str, Any]] = []
        if carrier_key == "GLS":
            cur.execute(_gls_note1_order_identity_gap_summary_sql())
            summary_row = cur.fetchone() or (0, 0, 0, 0, 0, 0, 0, 0)
            sample_rows: list[dict[str, Any]] = []
            if int(sample_limit or 0) > 0:
                cur.execute(_gls_note1_order_identity_gap_samples_sql(sample_limit))
                sample_rows = _fetchall_dict(cur)
            focus_areas.append(
                {
                    "focus_area": "gls_note1_order_identity",
                    "shipments": int(summary_row[0] or 0),
                    "distinct_values": int(summary_row[1] or 0),
                    "values_via_dis_map": int(summary_row[2] or 0),
                    "values_with_external_order_id": int(summary_row[3] or 0),
                    "values_resolved_to_acc_order": int(summary_row[4] or 0),
                    "values_missing_external_order_id": int(summary_row[5] or 0),
                    "values_missing_acc_order": int(summary_row[6] or 0),
                    "shipments_missing_acc_order": int(summary_row[7] or 0),
                    "broken_identity_samples": [
                        {
                            "candidate_value": str(row.get("candidate_value") or ""),
                            "shipments": int(row.get("shipments") or 0),
                            "break_stage": str(row.get("break_stage") or ""),
                            "resolution_path": str(row.get("resolution_path") or ""),
                            "resolved_bl_order_id": int(row.get("resolved_bl_order_id") or 0)
                            if row.get("resolved_bl_order_id") is not None
                            else None,
                            "external_order_id": row.get("external_order_id"),
                            "acc_order_id": row.get("acc_order_id"),
                        }
                        for row in sample_rows
                    ],
                }
            )
        else:
            cur.execute(_dhl_numeric_order_identity_gap_summary_sql())
            numeric_row = cur.fetchone() or (0, 0, 0, 0, 0, 0, 0, 0)
            numeric_samples: list[dict[str, Any]] = []
            if int(sample_limit or 0) > 0:
                cur.execute(_dhl_numeric_order_identity_gap_samples_sql(sample_limit))
                numeric_samples = _fetchall_dict(cur)
            focus_areas.append(
                {
                    "focus_area": "dhl_numeric_order_identity",
                    "shipments": int(numeric_row[0] or 0),
                    "distinct_values": int(numeric_row[1] or 0),
                    "values_with_package_match": int(numeric_row[2] or 0),
                    "values_with_external_order_id": int(numeric_row[3] or 0),
                    "values_resolved_to_acc_order": int(numeric_row[4] or 0),
                    "values_missing_external_order_id": int(numeric_row[5] or 0),
                    "values_missing_acc_order": int(numeric_row[6] or 0),
                    "shipments_missing_acc_order": int(numeric_row[7] or 0),
                    "broken_identity_samples": [
                        {
                            "candidate_value": str(row.get("candidate_value") or ""),
                            "shipments": int(row.get("shipments") or 0),
                            "break_stage": str(row.get("break_stage") or ""),
                            "sample_package_order_id": int(row.get("sample_package_order_id") or 0)
                            if row.get("sample_package_order_id") is not None
                            else None,
                            "sample_resolved_bl_order_id": int(row.get("sample_resolved_bl_order_id") or 0)
                            if row.get("sample_resolved_bl_order_id") is not None
                            else None,
                            "sample_external_order_id": row.get("sample_external_order_id"),
                            "sample_acc_order_id": row.get("sample_acc_order_id"),
                            "package_order_matches": int(row.get("package_order_matches") or 0),
                            "external_order_id_matches": int(row.get("external_order_id_matches") or 0),
                            "acc_order_matches": int(row.get("acc_order_matches") or 0),
                        }
                        for row in numeric_samples
                    ],
                }
            )

            cur.execute(_dhl_jjd_order_identity_gap_summary_sql())
            jjd_row = cur.fetchone() or (0, 0, 0, 0, 0, 0, 0, 0, 0)
            jjd_samples: list[dict[str, Any]] = []
            if int(sample_limit or 0) > 0:
                cur.execute(_dhl_jjd_order_identity_gap_samples_sql(sample_limit))
                jjd_samples = _fetchall_dict(cur)
            focus_areas.append(
                {
                    "focus_area": "dhl_jjd_order_identity",
                    "shipments": int(jjd_row[0] or 0),
                    "distinct_values": int(jjd_row[1] or 0),
                    "values_with_parcel_map": int(jjd_row[2] or 0),
                    "values_with_package_match": int(jjd_row[3] or 0),
                    "values_with_external_order_id": int(jjd_row[4] or 0),
                    "values_resolved_to_acc_order": int(jjd_row[5] or 0),
                    "values_missing_external_order_id": int(jjd_row[6] or 0),
                    "values_missing_acc_order": int(jjd_row[7] or 0),
                    "shipments_missing_acc_order": int(jjd_row[8] or 0),
                    "broken_identity_samples": [
                        {
                            "candidate_value": str(row.get("candidate_value") or ""),
                            "shipments": int(row.get("shipments") or 0),
                            "break_stage": str(row.get("break_stage") or ""),
                            "sample_parcel_number_base": row.get("sample_parcel_number_base"),
                            "sample_package_order_id": int(row.get("sample_package_order_id") or 0)
                            if row.get("sample_package_order_id") is not None
                            else None,
                            "sample_resolved_bl_order_id": int(row.get("sample_resolved_bl_order_id") or 0)
                            if row.get("sample_resolved_bl_order_id") is not None
                            else None,
                            "sample_external_order_id": row.get("sample_external_order_id"),
                            "sample_acc_order_id": row.get("sample_acc_order_id"),
                            "parcel_map_matches": int(row.get("parcel_map_matches") or 0),
                            "package_order_matches": int(row.get("package_order_matches") or 0),
                            "external_order_id_matches": int(row.get("external_order_id_matches") or 0),
                            "acc_order_matches": int(row.get("acc_order_matches") or 0),
                        }
                        for row in jjd_samples
                    ],
                }
            )
    finally:
        conn.close()

    return {
        "month": month_token,
        "carrier": carrier_key,
        "scope": {
            "scope_type": "purchase_month_with_shipment_buffer",
            "purchase_from": month_start.isoformat(),
            "purchase_to": purchase_to.isoformat(),
            "created_from": created_from.isoformat(),
            "created_to": created_to.isoformat(),
            "created_to_buffer_days": int(created_to_buffer_days),
        },
        "focus_areas": focus_areas,
    }


def _load_pair_diagnostics(
    *,
    month_token: str,
    carrier_key: str,
    created_to_buffer_days: int,
    sample_limit: int,
) -> dict[str, Any]:
    month_start = _month_start(month_token)
    month_end = _next_month(month_start)
    purchase_to = month_end - timedelta(days=1)
    created_from = month_start
    created_to = purchase_to + timedelta(days=created_to_buffer_days)

    conn = _connect()
    try:
        cur = conn.cursor()
        _setup_scope_tables(
            cur,
            carrier_key=carrier_key,
            purchase_from=month_start,
            purchase_to=purchase_to,
            created_from=created_from,
            created_to=created_to,
        )

        cur.execute(
            """
-- courier_link_gap_summary
WITH flags AS (
    SELECT
        ss.shipment_id,
        CASE WHEN EXISTS (
            SELECT 1
            FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
            WHERE l.shipment_id = ss.shipment_id
              AND l.is_primary = 1
        ) THEN 1 ELSE 0 END AS has_primary_link,
        CASE WHEN EXISTS (
            SELECT 1
            FROM dbo.acc_shipment_cost c WITH (NOLOCK)
            WHERE c.shipment_id = ss.shipment_id
              AND c.is_estimated = 0
        ) THEN 1 ELSE 0 END AS has_actual_cost,
        CASE WHEN EXISTS (
            SELECT 1
            FROM dbo.acc_shipment_cost c WITH (NOLOCK)
            WHERE c.shipment_id = ss.shipment_id
              AND c.is_estimated = 1
        ) THEN 1 ELSE 0 END AS has_estimated_cost
    FROM #ship_scope ss
)
SELECT
    (SELECT COUNT(*) FROM (SELECT DISTINCT amazon_order_id FROM #package_tokens) u) AS orders_universe,
    COUNT(*) AS shipments_in_scope,
    SUM(CASE WHEN has_primary_link = 1 THEN 1 ELSE 0 END) AS shipments_with_primary_link,
    SUM(CASE WHEN has_primary_link = 0 THEN 1 ELSE 0 END) AS shipments_without_primary_link,
    SUM(CASE WHEN has_primary_link = 0 AND has_actual_cost = 1 THEN 1 ELSE 0 END) AS shipments_unlinked_with_actual_cost,
    SUM(CASE WHEN has_primary_link = 0 AND has_actual_cost = 0 AND has_estimated_cost = 1 THEN 1 ELSE 0 END) AS shipments_unlinked_with_estimated_only,
    SUM(CASE WHEN has_primary_link = 1 AND has_actual_cost = 1 THEN 1 ELSE 0 END) AS shipments_linked_with_actual_cost,
    SUM(CASE WHEN has_primary_link = 1 AND has_actual_cost = 0 THEN 1 ELSE 0 END) AS shipments_linked_but_no_actual_cost,
    SUM(CASE WHEN has_primary_link = 1 AND has_actual_cost = 0 AND has_estimated_cost = 1 THEN 1 ELSE 0 END) AS shipments_linked_estimated_only
FROM flags;
            """
        )
        summary_row = cur.fetchone() or (0, 0, 0, 0, 0, 0, 0, 0, 0)

        cur.execute(
            _unlinked_classification_cte_sql(carrier_key)
            + """
-- courier_link_gap_unlinked_buckets
SELECT
    gap_bucket AS bucket,
    COUNT(*) AS shipments,
    SUM(CASE WHEN has_actual_cost = 1 THEN 1 ELSE 0 END) AS shipments_with_actual_cost,
    SUM(CASE WHEN has_actual_cost = 0 AND has_estimated_cost = 1 THEN 1 ELSE 0 END) AS shipments_with_estimated_only
FROM classified
GROUP BY gap_bucket
ORDER BY COUNT(*) DESC, gap_bucket;
"""
        )
        unlinked_bucket_rows = _fetchall_dict(cur)

        cur.execute(
            _linked_missing_actual_cte_sql()
            + """
-- courier_link_gap_cost_buckets
SELECT
    gap_bucket AS bucket,
    COUNT(*) AS shipments
FROM classified
GROUP BY gap_bucket
ORDER BY COUNT(*) DESC, gap_bucket;
"""
        )
        cost_bucket_rows = _fetchall_dict(cur)

        cur.execute(
            _unlinked_classification_cte_sql(carrier_key)
            + f"""
-- courier_link_gap_unlinked_samples
SELECT TOP {int(sample_limit)}
    CAST(shipment_id AS NVARCHAR(40)) AS shipment_id,
    gap_bucket AS bucket,
    shipment_number,
    tracking_number,
    piece_id,
    source_system,
    JSON_VALUE(source_payload_json, '$.note1') AS note1,
    has_actual_cost,
    has_estimated_cost,
    observed_at
FROM classified
ORDER BY has_actual_cost DESC, has_estimated_cost DESC, observed_at DESC, shipment_number DESC;
"""
        )
        unlinked_samples = _fetchall_dict(cur)

        cur.execute(
            _linked_missing_actual_cte_sql()
            + f"""
-- courier_link_gap_cost_samples
SELECT TOP {int(sample_limit)}
    CAST(shipment_id AS NVARCHAR(40)) AS shipment_id,
    amazon_order_id,
    gap_bucket AS bucket,
    shipment_number,
    tracking_number,
    piece_id,
    source_system,
    JSON_VALUE(source_payload_json, '$.note1') AS note1,
    has_estimated_cost,
    observed_at
FROM classified
ORDER BY has_estimated_cost DESC, observed_at DESC, shipment_number DESC;
"""
        )
        cost_samples = _fetchall_dict(cur)
    finally:
        conn.close()

    summary = {
        "orders_universe": int(summary_row[0] or 0),
        "shipments_in_scope": int(summary_row[1] or 0),
        "shipments_with_primary_link": int(summary_row[2] or 0),
        "shipments_without_primary_link": int(summary_row[3] or 0),
        "shipments_unlinked_with_actual_cost": int(summary_row[4] or 0),
        "shipments_unlinked_with_estimated_only": int(summary_row[5] or 0),
        "shipments_linked_with_actual_cost": int(summary_row[6] or 0),
        "shipments_linked_but_no_actual_cost": int(summary_row[7] or 0),
        "shipments_linked_estimated_only": int(summary_row[8] or 0),
    }

    return {
        "month": month_token,
        "carrier": carrier_key,
        "scope": {
            "scope_type": "purchase_month_with_shipment_buffer",
            "purchase_from": month_start.isoformat(),
            "purchase_to": purchase_to.isoformat(),
            "created_from": created_from.isoformat(),
            "created_to": created_to.isoformat(),
            "created_to_buffer_days": int(created_to_buffer_days),
        },
        "summary": summary,
        "unlinked_buckets": [
            {
                "bucket": str(row.get("bucket") or ""),
                "shipments": int(row.get("shipments") or 0),
                "shipments_with_actual_cost": int(row.get("shipments_with_actual_cost") or 0),
                "shipments_with_estimated_only": int(row.get("shipments_with_estimated_only") or 0),
            }
            for row in unlinked_bucket_rows
            if int(row.get("shipments") or 0) > 0
        ],
        "cost_gap_buckets": [
            {
                "bucket": str(row.get("bucket") or ""),
                "shipments": int(row.get("shipments") or 0),
            }
            for row in cost_bucket_rows
            if int(row.get("shipments") or 0) > 0
        ],
        "sample_unlinked_shipments": [
            {
                "shipment_id": str(row.get("shipment_id") or ""),
                "bucket": str(row.get("bucket") or ""),
                "shipment_number": row.get("shipment_number"),
                "tracking_number": row.get("tracking_number"),
                "piece_id": row.get("piece_id"),
                "source_system": row.get("source_system"),
                "note1": row.get("note1"),
                "has_actual_cost": bool(int(row.get("has_actual_cost") or 0)),
                "has_estimated_cost": bool(int(row.get("has_estimated_cost") or 0)),
                "observed_at": row.get("observed_at"),
            }
            for row in unlinked_samples
        ],
        "sample_linked_no_actual_cost_shipments": [
            {
                "shipment_id": str(row.get("shipment_id") or ""),
                "amazon_order_id": row.get("amazon_order_id"),
                "bucket": str(row.get("bucket") or ""),
                "shipment_number": row.get("shipment_number"),
                "tracking_number": row.get("tracking_number"),
                "piece_id": row.get("piece_id"),
                "source_system": row.get("source_system"),
                "note1": row.get("note1"),
                "has_estimated_cost": bool(int(row.get("has_estimated_cost") or 0)),
                "observed_at": row.get("observed_at"),
            }
            for row in cost_samples
        ],
    }


def _load_pair_link_gap_summary(
    *,
    month_token: str,
    carrier_key: str,
    created_to_buffer_days: int,
) -> dict[str, Any]:
    month_start = _month_start(month_token)
    month_end = _next_month(month_start)
    purchase_to = month_end - timedelta(days=1)
    created_from = month_start
    created_to = purchase_to + timedelta(days=created_to_buffer_days)

    conn = _connect()
    try:
        cur = conn.cursor()
        _setup_scope_tables(
            cur,
            carrier_key=carrier_key,
            purchase_from=month_start,
            purchase_to=purchase_to,
            created_from=created_from,
            created_to=created_to,
        )

        # Keep this path sample-free so it stays usable on ACC production-sized months.
        cur.execute(
            """
-- courier_link_gap_bucket_summary
WITH flags AS (
    SELECT
        ss.shipment_id,
        CASE WHEN EXISTS (
            SELECT 1
            FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
            WHERE l.shipment_id = ss.shipment_id
              AND l.is_primary = 1
        ) THEN 1 ELSE 0 END AS has_primary_link,
        CASE WHEN EXISTS (
            SELECT 1
            FROM dbo.acc_shipment_cost c WITH (NOLOCK)
            WHERE c.shipment_id = ss.shipment_id
              AND c.is_estimated = 0
        ) THEN 1 ELSE 0 END AS has_actual_cost,
        CASE WHEN EXISTS (
            SELECT 1
            FROM dbo.acc_shipment_cost c WITH (NOLOCK)
            WHERE c.shipment_id = ss.shipment_id
              AND c.is_estimated = 1
        ) THEN 1 ELSE 0 END AS has_estimated_cost
    FROM #ship_scope ss
)
SELECT
    (SELECT COUNT(*) FROM (SELECT DISTINCT amazon_order_id FROM #package_tokens) u) AS orders_universe,
    COUNT(*) AS shipments_in_scope,
    SUM(CASE WHEN has_primary_link = 1 THEN 1 ELSE 0 END) AS shipments_with_primary_link,
    SUM(CASE WHEN has_primary_link = 0 THEN 1 ELSE 0 END) AS shipments_without_primary_link,
    SUM(CASE WHEN has_primary_link = 0 AND has_actual_cost = 1 THEN 1 ELSE 0 END) AS shipments_unlinked_with_actual_cost,
    SUM(CASE WHEN has_primary_link = 0 AND has_actual_cost = 0 AND has_estimated_cost = 1 THEN 1 ELSE 0 END) AS shipments_unlinked_with_estimated_only,
    SUM(CASE WHEN has_primary_link = 1 AND has_actual_cost = 1 THEN 1 ELSE 0 END) AS shipments_linked_with_actual_cost,
    SUM(CASE WHEN has_primary_link = 1 AND has_actual_cost = 0 THEN 1 ELSE 0 END) AS shipments_linked_but_no_actual_cost,
    SUM(CASE WHEN has_primary_link = 1 AND has_actual_cost = 0 AND has_estimated_cost = 1 THEN 1 ELSE 0 END) AS shipments_linked_estimated_only
FROM flags;
            """
        )
        summary_row = cur.fetchone() or (0, 0, 0, 0, 0, 0, 0, 0, 0)

        cur.execute(
            _unlinked_classification_cte_sql(carrier_key)
            + """
-- courier_link_gap_bucket_unlinked_buckets
SELECT
    gap_bucket AS bucket,
    COUNT(*) AS shipments,
    SUM(CASE WHEN has_actual_cost = 1 THEN 1 ELSE 0 END) AS shipments_with_actual_cost,
    SUM(CASE WHEN has_actual_cost = 0 AND has_estimated_cost = 1 THEN 1 ELSE 0 END) AS shipments_with_estimated_only
FROM classified
GROUP BY gap_bucket
ORDER BY COUNT(*) DESC, gap_bucket;
"""
        )
        bucket_rows = _fetchall_dict(cur)

        cur.execute(
            _unlinked_classification_cte_sql(carrier_key)
            + """
-- courier_link_gap_bucket_source_systems
SELECT
    ISNULL(NULLIF(LTRIM(RTRIM(source_system)), ''), 'unknown') AS source_system,
    COUNT(*) AS shipments,
    SUM(CASE WHEN has_actual_cost = 1 THEN 1 ELSE 0 END) AS shipments_with_actual_cost,
    SUM(CASE WHEN has_actual_cost = 0 AND has_estimated_cost = 1 THEN 1 ELSE 0 END) AS shipments_with_estimated_only
FROM classified
GROUP BY ISNULL(NULLIF(LTRIM(RTRIM(source_system)), ''), 'unknown')
ORDER BY COUNT(*) DESC, source_system;
"""
        )
        source_rows = _fetchall_dict(cur)

        cur.execute(_identifier_pattern_sql(carrier_key))
        pattern_rows = _fetchall_dict(cur)
    finally:
        conn.close()

    shipments_in_scope = int(summary_row[1] or 0)
    shipments_without_primary_link = int(summary_row[3] or 0)
    summary = {
        "orders_universe": int(summary_row[0] or 0),
        "shipments_in_scope": shipments_in_scope,
        "shipments_with_primary_link": int(summary_row[2] or 0),
        "shipments_without_primary_link": shipments_without_primary_link,
        "shipments_without_primary_link_pct": round((shipments_without_primary_link / shipments_in_scope) * 100, 2)
        if shipments_in_scope
        else 0.0,
        "shipments_unlinked_with_actual_cost": int(summary_row[4] or 0),
        "shipments_unlinked_with_estimated_only": int(summary_row[5] or 0),
        "shipments_linked_with_actual_cost": int(summary_row[6] or 0),
        "shipments_linked_but_no_actual_cost": int(summary_row[7] or 0),
        "shipments_linked_estimated_only": int(summary_row[8] or 0),
    }

    return {
        "month": month_token,
        "carrier": carrier_key,
        "scope": {
            "scope_type": "purchase_month_with_shipment_buffer",
            "purchase_from": month_start.isoformat(),
            "purchase_to": purchase_to.isoformat(),
            "created_from": created_from.isoformat(),
            "created_to": created_to.isoformat(),
            "created_to_buffer_days": int(created_to_buffer_days),
        },
        "summary": summary,
        "unlinked_buckets": [
            {
                "bucket": str(row.get("bucket") or ""),
                "shipments": int(row.get("shipments") or 0),
                "shipments_with_actual_cost": int(row.get("shipments_with_actual_cost") or 0),
                "shipments_with_estimated_only": int(row.get("shipments_with_estimated_only") or 0),
            }
            for row in bucket_rows
            if int(row.get("shipments") or 0) > 0
        ],
        "unlinked_source_systems": [
            {
                "source_system": str(row.get("source_system") or "unknown"),
                "shipments": int(row.get("shipments") or 0),
                "shipments_with_actual_cost": int(row.get("shipments_with_actual_cost") or 0),
                "shipments_with_estimated_only": int(row.get("shipments_with_estimated_only") or 0),
            }
            for row in source_rows
            if int(row.get("shipments") or 0) > 0
        ],
        "unlinked_identifier_patterns": [
            {
                "identifier_pattern": str(row.get("identifier_pattern") or ""),
                "shipments": int(row.get("shipments") or 0),
                "shipments_with_actual_cost": int(row.get("shipments_with_actual_cost") or 0),
                "shipments_with_estimated_only": int(row.get("shipments_with_estimated_only") or 0),
            }
            for row in pattern_rows
            if int(row.get("shipments") or 0) > 0
        ],
    }


def get_courier_link_gap_diagnostics(
    *,
    months: list[str] | None = None,
    carriers: list[str] | None = None,
    created_to_buffer_days: int = 31,
    sample_limit: int = 20,
) -> dict[str, Any]:
    months_norm = _normalize_months(months)
    carriers_norm = _normalize_carriers(carriers)
    buffer_days_safe = max(0, int(created_to_buffer_days or 0))
    sample_limit_safe = max(1, int(sample_limit or 1))

    items: list[dict[str, Any]] = []
    matrix: dict[str, dict[str, Any]] = {}
    for month_token in months_norm:
        month_bucket: dict[str, Any] = {}
        for carrier_key in carriers_norm:
            item = _load_pair_diagnostics(
                month_token=month_token,
                carrier_key=carrier_key,
                created_to_buffer_days=buffer_days_safe,
                sample_limit=sample_limit_safe,
            )
            summary = item["summary"]
            month_bucket[carrier_key] = {
                "shipments_in_scope": summary["shipments_in_scope"],
                "shipments_without_primary_link": summary["shipments_without_primary_link"],
                "shipments_unlinked_with_actual_cost": summary["shipments_unlinked_with_actual_cost"],
                "shipments_linked_but_no_actual_cost": summary["shipments_linked_but_no_actual_cost"],
            }
            items.append(item)
        matrix[month_token] = month_bucket

    return {
        "months": months_norm,
        "carriers": carriers_norm,
        "rows": len(items),
        "scope_type": "purchase_month_with_shipment_buffer",
        "items": items,
        "matrix": matrix,
    }


def get_courier_link_gap_summary(
    *,
    months: list[str] | None = None,
    carriers: list[str] | None = None,
    created_to_buffer_days: int = 31,
) -> dict[str, Any]:
    months_norm = _normalize_months(months)
    carriers_norm = _normalize_carriers(carriers)
    buffer_days_safe = max(0, int(created_to_buffer_days or 0))

    items: list[dict[str, Any]] = []
    matrix: dict[str, dict[str, Any]] = {}
    for month_token in months_norm:
        month_bucket: dict[str, Any] = {}
        for carrier_key in carriers_norm:
            item = _load_pair_link_gap_summary(
                month_token=month_token,
                carrier_key=carrier_key,
                created_to_buffer_days=buffer_days_safe,
            )
            summary = item["summary"]
            month_bucket[carrier_key] = {
                "shipments_in_scope": summary["shipments_in_scope"],
                "shipments_without_primary_link": summary["shipments_without_primary_link"],
                "shipments_without_primary_link_pct": summary["shipments_without_primary_link_pct"],
                "shipments_unlinked_with_actual_cost": summary["shipments_unlinked_with_actual_cost"],
            }
            items.append(item)
        matrix[month_token] = month_bucket

    return {
        "months": months_norm,
        "carriers": carriers_norm,
        "rows": len(items),
        "scope_type": "purchase_month_with_shipment_buffer",
        "items": items,
        "matrix": matrix,
    }


def get_courier_identifier_source_gap_summary(
    *,
    months: list[str] | None = None,
    carriers: list[str] | None = None,
    created_to_buffer_days: int = 31,
) -> dict[str, Any]:
    months_norm = _normalize_months(months)
    carriers_norm = _normalize_carriers(carriers)
    buffer_days_safe = max(0, int(created_to_buffer_days or 0))

    items: list[dict[str, Any]] = []
    matrix: dict[str, dict[str, Any]] = {}
    for month_token in months_norm:
        month_bucket: dict[str, Any] = {}
        for carrier_key in carriers_norm:
            item = _load_pair_identifier_source_gap_summary(
                month_token=month_token,
                carrier_key=carrier_key,
                created_to_buffer_days=buffer_days_safe,
            )
            month_bucket[carrier_key] = {
                focus["focus_area"]: {
                    "shipments": int(focus.get("shipments") or 0),
                    "distinct_values": int(focus.get("distinct_values") or 0),
                }
                for focus in item["focus_areas"]
            }
            items.append(item)
        matrix[month_token] = month_bucket

    return {
        "months": months_norm,
        "carriers": carriers_norm,
        "rows": len(items),
        "scope_type": "purchase_month_with_shipment_buffer",
        "items": items,
        "matrix": matrix,
    }


def get_courier_order_identity_gap_summary(
    *,
    months: list[str] | None = None,
    carriers: list[str] | None = None,
    created_to_buffer_days: int = 31,
    sample_limit: int = 10,
) -> dict[str, Any]:
    months_norm = _normalize_months(months)
    carriers_norm = _normalize_carriers(carriers)
    buffer_days_safe = max(0, int(created_to_buffer_days or 0))
    sample_limit_safe = max(0, int(sample_limit or 0))

    items: list[dict[str, Any]] = []
    matrix: dict[str, dict[str, Any]] = {}
    for month_token in months_norm:
        month_bucket: dict[str, Any] = {}
        for carrier_key in carriers_norm:
            item = _load_pair_order_identity_gap_summary(
                month_token=month_token,
                carrier_key=carrier_key,
                created_to_buffer_days=buffer_days_safe,
                sample_limit=sample_limit_safe,
            )
            month_bucket[carrier_key] = {
                focus["focus_area"]: {
                    "shipments": int(focus.get("shipments") or 0),
                    "distinct_values": int(focus.get("distinct_values") or 0),
                    "values_missing_external_order_id": int(focus.get("values_missing_external_order_id") or 0),
                    "values_missing_acc_order": int(focus.get("values_missing_acc_order") or 0),
                    "shipments_missing_acc_order": int(focus.get("shipments_missing_acc_order") or 0),
                }
                for focus in item["focus_areas"]
            }
            items.append(item)
        matrix[month_token] = month_bucket

    return {
        "months": months_norm,
        "carriers": carriers_norm,
        "rows": len(items),
        "scope_type": "purchase_month_with_shipment_buffer",
        "items": items,
        "matrix": matrix,
    }
