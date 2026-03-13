from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import structlog

from app.core.db_connection import connect_acc, connect_netfox
from app.services.courier_link_diagnostics import (
    _dynamic_default_months,
    _month_start,
    _next_month,
    _setup_scope_tables,
)

log = structlog.get_logger(__name__)

_NETFOX_BATCH_SIZE = 100
_ACC_WRITE_COMMIT_EVERY = 100
_DEFAULT_LIMIT_VALUES = 200
_ALLOWED_MODES = {
    "gls_note1",
    "gls_tracking_map",
    "gls_note1_external_order",
    "dhl_numeric",
    "dhl_jjd",
    "dhl_numeric_external_order",
    "dhl_jjd_external_order",
}
_EXTERNAL_ORDER_ONLY_MODES = {
    "gls_note1_external_order",
    "dhl_numeric_external_order",
    "dhl_jjd_external_order",
}


@dataclass
class CandidateValue:
    value: str
    shipments: int
    shipments_with_actual_cost: int


@dataclass
class NetfoxPackageRow:
    order_id: int
    courier_package_nr: str | None
    courier_inner_number: str | None
    courier_code: str | None
    courier_other_name: str | None


@dataclass
class NetfoxDisMapRow:
    holding_order_id: int
    dis_order_id: int


@dataclass
class NetfoxBlOrderRow:
    order_id: int
    external_order_id: str


@dataclass
class NetfoxJjdRow:
    jjd_number: str
    parcel_number_base: str
    order_id: int


def _connect_acc():
    # This batch path builds temp scopes for one month+carrier and may legitimately
    # run longer than the generic API read timeout.
    return connect_acc(autocommit=False, timeout=300)


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_token(value: Any) -> str:
    return _normalize_text(value).upper().replace(" ", "")


def _normalize_int(value: Any) -> int | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def _chunks(items: list[Any], size: int) -> list[list[Any]]:
    return [items[idx : idx + size] for idx in range(0, len(items), size)]


def _mode_to_carrier(mode: str) -> str:
    mode_key = str(mode or "").strip().lower()
    if mode_key.startswith("gls_"):
        return "GLS"
    if mode_key.startswith("dhl_"):
        return "DHL"
    raise ValueError(f"Unsupported backfill mode '{mode}'")


def _normalize_months(months: list[str] | None) -> list[str]:
    raw = months or _dynamic_default_months()
    result: list[str] = []
    for token in raw:
        value = str(token or "").strip()
        if not value:
            continue
        _month_start(value)
        result.append(value)
    if not result:
        raise ValueError("months list cannot be empty")
    return result


def _normalize_mode(mode: str) -> str:
    value = str(mode or "").strip().lower()
    if value not in _ALLOWED_MODES:
        raise ValueError(f"Unsupported backfill mode '{mode}'")
    return value


def _candidate_sql(mode: str, *, limit_values: int) -> str:
    # Batch backfill only needs unresolved shipment identifiers, not the full
    # diagnostics classification tree. Keep this path lighter to avoid ACC timeouts.
    unlinked_cte = """
WITH unlinked AS (
    SELECT
        ss.shipment_id,
        ss.source_payload_json,
        CASE WHEN EXISTS (
            SELECT 1
            FROM dbo.acc_shipment_cost c WITH (NOLOCK)
            WHERE c.shipment_id = ss.shipment_id
              AND c.is_estimated = 0
        ) THEN 1 ELSE 0 END AS has_actual_cost
    FROM #ship_scope ss
    WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.acc_shipment_order_link l WITH (NOLOCK)
        WHERE l.shipment_id = ss.shipment_id
          AND l.is_primary = 1
    )
)
"""
    if mode == "gls_note1":
        return (
            unlinked_cte
            + f"""
, candidates AS (
    SELECT
        TRY_CAST(JSON_VALUE(u.source_payload_json, '$.note1') AS BIGINT) AS candidate_value,
        COUNT(*) AS shipments,
        SUM(CASE WHEN u.has_actual_cost = 1 THEN 1 ELSE 0 END) AS shipments_with_actual_cost
    FROM unlinked u
    WHERE TRY_CAST(JSON_VALUE(u.source_payload_json, '$.note1') AS BIGINT) IS NOT NULL
    GROUP BY TRY_CAST(JSON_VALUE(u.source_payload_json, '$.note1') AS BIGINT)
)
-- courier_identifier_backfill_gls_note1_candidates
SELECT TOP {int(limit_values)}
    CAST(candidate_value AS NVARCHAR(40)) AS candidate_value,
    shipments,
    shipments_with_actual_cost
FROM candidates
ORDER BY shipments_with_actual_cost DESC, shipments DESC, candidate_value ASC;
"""
        )
    if mode == "gls_note1_external_order":
        return (
            unlinked_cte
            + f"""
, candidate_values AS (
    SELECT
        TRY_CAST(JSON_VALUE(u.source_payload_json, '$.note1') AS BIGINT) AS candidate_order_id,
        COUNT(*) AS shipments,
        SUM(CASE WHEN u.has_actual_cost = 1 THEN 1 ELSE 0 END) AS shipments_with_actual_cost
    FROM unlinked u
    WHERE TRY_CAST(JSON_VALUE(u.source_payload_json, '$.note1') AS BIGINT) IS NOT NULL
    GROUP BY TRY_CAST(JSON_VALUE(u.source_payload_json, '$.note1') AS BIGINT)
),
resolved_values AS (
    SELECT
        COALESCE(dm.holding_order_id, cv.candidate_order_id) AS resolved_bl_order_id,
        SUM(cv.shipments) AS shipments,
        SUM(cv.shipments_with_actual_cost) AS shipments_with_actual_cost
    FROM candidate_values cv
    OUTER APPLY (
        SELECT TOP 1 dm.holding_order_id
        FROM dbo.acc_cache_dis_map dm WITH (NOLOCK)
        WHERE dm.dis_order_id = cv.candidate_order_id
        ORDER BY dm.holding_order_id
    ) dm
    OUTER APPLY (
        SELECT TOP 1 NULLIF(LTRIM(RTRIM(bo.external_order_id)), '') AS external_order_id
        FROM dbo.acc_cache_bl_orders bo WITH (NOLOCK)
        WHERE bo.order_id = COALESCE(dm.holding_order_id, cv.candidate_order_id)
        ORDER BY bo.external_order_id
    ) bo
    WHERE COALESCE(dm.holding_order_id, cv.candidate_order_id) IS NOT NULL
      AND bo.external_order_id IS NULL
    GROUP BY COALESCE(dm.holding_order_id, cv.candidate_order_id)
)
-- courier_identifier_backfill_gls_note1_external_order_candidates
SELECT TOP {int(limit_values)}
    CAST(resolved_bl_order_id AS NVARCHAR(40)) AS candidate_value,
    shipments,
    shipments_with_actual_cost
FROM resolved_values
ORDER BY shipments_with_actual_cost DESC, shipments DESC, candidate_value ASC;
"""
        )
    if mode == "gls_tracking_map":
        return (
            unlinked_cte
            + f"""
, candidate_matches AS (
    SELECT DISTINCT
        u.shipment_id,
        gm.bl_order_id AS candidate_value,
        u.has_actual_cost
    FROM unlinked u
    JOIN #ship_tokens st
      ON st.shipment_id = u.shipment_id
     AND st.token_source IN ('tracking_number', 'shipment_number', 'piece_id')
     AND PATINDEX('%[^0-9]%', st.token) = 0
    JOIN dbo.acc_gls_bl_map gm WITH (NOLOCK)
      ON gm.tracking_number = st.token
    WHERE gm.bl_order_id IS NOT NULL
),
candidates AS (
    SELECT
        candidate_value,
        COUNT(*) AS shipments,
        SUM(CASE WHEN has_actual_cost = 1 THEN 1 ELSE 0 END) AS shipments_with_actual_cost
    FROM candidate_matches
    GROUP BY candidate_value
)
-- courier_identifier_backfill_gls_tracking_candidates
SELECT TOP {int(limit_values)}
    CAST(candidate_value AS NVARCHAR(40)) AS candidate_value,
    shipments,
    shipments_with_actual_cost
FROM candidates
ORDER BY shipments_with_actual_cost DESC, shipments DESC, candidate_value ASC;
"""
        )
    if mode == "dhl_numeric":
        return (
            unlinked_cte
            + f"""
, candidate_tokens AS (
    SELECT DISTINCT
        u.shipment_id,
        st.token AS candidate_value,
        u.has_actual_cost
    FROM unlinked u
    JOIN #ship_tokens st
      ON st.shipment_id = u.shipment_id
    WHERE PATINDEX('%[^0-9]%', st.token) = 0
      AND st.token NOT LIKE 'JJD%%'
),
candidates AS (
    SELECT
        candidate_value,
        COUNT(*) AS shipments,
        SUM(CASE WHEN has_actual_cost = 1 THEN 1 ELSE 0 END) AS shipments_with_actual_cost
    FROM candidate_tokens
    GROUP BY candidate_value
)
-- courier_identifier_backfill_dhl_numeric_candidates
SELECT TOP {int(limit_values)}
    candidate_value,
    shipments,
    shipments_with_actual_cost
FROM candidates
ORDER BY shipments_with_actual_cost DESC, shipments DESC, candidate_value ASC;
"""
        )
    if mode == "dhl_numeric_external_order":
        return (
            unlinked_cte
            + f"""
, focus_tokens AS (
    SELECT DISTINCT
        u.shipment_id,
        st.token,
        u.has_actual_cost
    FROM unlinked u
    JOIN #ship_tokens st
      ON st.shipment_id = u.shipment_id
    WHERE PATINDEX('%[^0-9]%', st.token) = 0
      AND st.token NOT LIKE 'JJD%%'
),
matched_orders AS (
    SELECT DISTINCT
        ft.shipment_id,
        COALESCE(dm.holding_order_id, p.order_id) AS resolved_bl_order_id,
        ft.has_actual_cost
    FROM focus_tokens ft
    JOIN dbo.acc_cache_packages p WITH (NOLOCK)
      ON (p.courier_package_nr = ft.token OR p.courier_inner_number = ft.token)
     AND (
            CHARINDEX('dhl', LOWER(CASE WHEN p.courier_code = 'blconnectpackages'
                THEN ISNULL(p.courier_other_name, '') ELSE ISNULL(p.courier_code, '') END)) > 0
         OR CHARINDEX('dhl', LOWER(ISNULL(p.courier_other_name, ''))) > 0
     )
    LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
      ON dm.dis_order_id = p.order_id
    OUTER APPLY (
        SELECT TOP 1 NULLIF(LTRIM(RTRIM(bo.external_order_id)), '') AS external_order_id
        FROM dbo.acc_cache_bl_orders bo WITH (NOLOCK)
        WHERE bo.order_id = COALESCE(dm.holding_order_id, p.order_id)
        ORDER BY bo.external_order_id
    ) bo
    WHERE COALESCE(dm.holding_order_id, p.order_id) IS NOT NULL
      AND bo.external_order_id IS NULL
),
resolved_values AS (
    SELECT
        resolved_bl_order_id,
        COUNT(*) AS shipments,
        SUM(CASE WHEN has_actual_cost = 1 THEN 1 ELSE 0 END) AS shipments_with_actual_cost
    FROM matched_orders
    GROUP BY resolved_bl_order_id
)
-- courier_identifier_backfill_dhl_numeric_external_order_candidates
SELECT TOP {int(limit_values)}
    CAST(resolved_bl_order_id AS NVARCHAR(40)) AS candidate_value,
    shipments,
    shipments_with_actual_cost
FROM resolved_values
ORDER BY shipments_with_actual_cost DESC, shipments DESC, candidate_value ASC;
"""
        )
    if mode == "dhl_jjd":
        return (
            unlinked_cte
            + f"""
, candidate_tokens AS (
    SELECT DISTINCT
        u.shipment_id,
        st.token AS candidate_value,
        u.has_actual_cost
    FROM unlinked u
    JOIN #ship_tokens st
      ON st.shipment_id = u.shipment_id
    WHERE st.token LIKE 'JJD%%'
),
candidates AS (
    SELECT
        candidate_value,
        COUNT(*) AS shipments,
        SUM(CASE WHEN has_actual_cost = 1 THEN 1 ELSE 0 END) AS shipments_with_actual_cost
    FROM candidate_tokens
    GROUP BY candidate_value
)
-- courier_identifier_backfill_dhl_jjd_candidates
SELECT TOP {int(limit_values)}
    candidate_value,
    shipments,
    shipments_with_actual_cost
FROM candidates
ORDER BY shipments_with_actual_cost DESC, shipments DESC, candidate_value ASC;
"""
        )
    if mode == "dhl_jjd_external_order":
        return (
            unlinked_cte
            + f"""
, focus_tokens AS (
    SELECT DISTINCT
        u.shipment_id,
        st.token,
        u.has_actual_cost
    FROM unlinked u
    JOIN #ship_tokens st
      ON st.shipment_id = u.shipment_id
    WHERE st.token LIKE 'JJD%%'
),
parcel_map_matches AS (
    SELECT DISTINCT
        ft.shipment_id,
        ft.has_actual_cost,
        NULLIF(LTRIM(RTRIM(m.parcel_number_base)), '') AS parcel_number_base
    FROM focus_tokens ft
    JOIN dbo.acc_dhl_parcel_map m WITH (NOLOCK)
      ON m.jjd_number = ft.token
    WHERE m.parcel_number_base IS NOT NULL
      AND LTRIM(RTRIM(m.parcel_number_base)) <> ''
),
matched_orders AS (
    SELECT DISTINCT
        pm.shipment_id,
        COALESCE(dm.holding_order_id, p.order_id) AS resolved_bl_order_id,
        pm.has_actual_cost
    FROM parcel_map_matches pm
    JOIN dbo.acc_cache_packages p WITH (NOLOCK)
      ON (p.courier_package_nr = pm.parcel_number_base OR p.courier_inner_number = pm.parcel_number_base)
     AND (
            CHARINDEX('dhl', LOWER(CASE WHEN p.courier_code = 'blconnectpackages'
                THEN ISNULL(p.courier_other_name, '') ELSE ISNULL(p.courier_code, '') END)) > 0
         OR CHARINDEX('dhl', LOWER(ISNULL(p.courier_other_name, ''))) > 0
     )
    LEFT JOIN dbo.acc_cache_dis_map dm WITH (NOLOCK)
      ON dm.dis_order_id = p.order_id
    OUTER APPLY (
        SELECT TOP 1 NULLIF(LTRIM(RTRIM(bo.external_order_id)), '') AS external_order_id
        FROM dbo.acc_cache_bl_orders bo WITH (NOLOCK)
        WHERE bo.order_id = COALESCE(dm.holding_order_id, p.order_id)
        ORDER BY bo.external_order_id
    ) bo
    WHERE COALESCE(dm.holding_order_id, p.order_id) IS NOT NULL
      AND bo.external_order_id IS NULL
),
resolved_values AS (
    SELECT
        resolved_bl_order_id,
        COUNT(*) AS shipments,
        SUM(CASE WHEN has_actual_cost = 1 THEN 1 ELSE 0 END) AS shipments_with_actual_cost
    FROM matched_orders
    GROUP BY resolved_bl_order_id
)
-- courier_identifier_backfill_dhl_jjd_external_order_candidates
SELECT TOP {int(limit_values)}
    CAST(resolved_bl_order_id AS NVARCHAR(40)) AS candidate_value,
    shipments,
    shipments_with_actual_cost
FROM resolved_values
ORDER BY shipments_with_actual_cost DESC, shipments DESC, candidate_value ASC;
"""
        )
    raise ValueError(f"Unsupported backfill mode '{mode}'")


def _load_candidates_for_month(
    *,
    month_token: str,
    mode: str,
    created_to_buffer_days: int,
    limit_values: int,
) -> list[CandidateValue]:
    carrier_key = _mode_to_carrier(mode)
    month_start = _month_start(month_token)
    month_end = _next_month(month_start)
    purchase_to = month_end - timedelta(days=1)
    created_from = month_start
    created_to = purchase_to + timedelta(days=int(created_to_buffer_days or 0))

    conn = _connect_acc()
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
        cur.execute(_candidate_sql(mode, limit_values=max(1, int(limit_values or _DEFAULT_LIMIT_VALUES))))
        rows = cur.fetchall()
    finally:
        conn.close()

    result: list[CandidateValue] = []
    for row in rows:
        value = _normalize_text(row[0])
        if not value:
            continue
        result.append(
            CandidateValue(
                value=value,
                shipments=int(row[1] or 0),
                shipments_with_actual_cost=int(row[2] or 0),
            )
        )
    return result


def ensure_courier_identifier_cache_schema() -> None:
    conn = _connect_acc()
    try:
        cur = conn.cursor()
        cur.execute(
            """
IF OBJECT_ID('dbo.acc_cache_bl_orders', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_cache_bl_orders (
        external_order_id NVARCHAR(50) NOT NULL,
        order_id INT NOT NULL
    );
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_cache_bl_extorder'
      AND object_id = OBJECT_ID('dbo.acc_cache_bl_orders')
)
BEGIN
    CREATE INDEX ix_cache_bl_extorder ON dbo.acc_cache_bl_orders(external_order_id);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_cache_bl_orderid'
      AND object_id = OBJECT_ID('dbo.acc_cache_bl_orders')
)
BEGIN
    CREATE INDEX ix_cache_bl_orderid ON dbo.acc_cache_bl_orders(order_id);
END;

IF OBJECT_ID('dbo.acc_cache_packages', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_cache_packages (
        order_id INT NOT NULL,
        courier_package_nr NVARCHAR(120) NULL,
        courier_inner_number NVARCHAR(120) NULL,
        courier_code NVARCHAR(60) NULL,
        courier_other_name NVARCHAR(120) NULL
    );
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_cache_pkg_order'
      AND object_id = OBJECT_ID('dbo.acc_cache_packages')
)
BEGIN
    CREATE INDEX ix_cache_pkg_order ON dbo.acc_cache_packages(order_id);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_cache_pkg_tracking'
      AND object_id = OBJECT_ID('dbo.acc_cache_packages')
)
BEGIN
    CREATE INDEX ix_cache_pkg_tracking ON dbo.acc_cache_packages(courier_package_nr);
END;

IF OBJECT_ID('dbo.acc_cache_dis_map', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_cache_dis_map (
        holding_order_id INT NOT NULL,
        dis_order_id INT NOT NULL
    );
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_cache_dis_holding'
      AND object_id = OBJECT_ID('dbo.acc_cache_dis_map')
)
BEGIN
    CREATE INDEX ix_cache_dis_holding ON dbo.acc_cache_dis_map(holding_order_id);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_cache_dis_dis'
      AND object_id = OBJECT_ID('dbo.acc_cache_dis_map')
)
BEGIN
    CREATE INDEX ix_cache_dis_dis ON dbo.acc_cache_dis_map(dis_order_id);
END;
            """
        )
        conn.commit()
    finally:
        conn.close()


def _fetch_netfox_dis_map_rows(ncur, *, order_ids: list[int]) -> list[NetfoxDisMapRow]:
    result: list[NetfoxDisMapRow] = []
    normalized = sorted({_normalize_int(value) for value in order_ids if _normalize_int(value) is not None})
    if not normalized:
        return result
    for batch in _chunks(normalized, _NETFOX_BATCH_SIZE):
        placeholders = ",".join("?" for _ in batch)
        params = [*batch, *batch]
        ncur.execute(
            f"""
-- courier_identifier_backfill_netfox_dis_map
SELECT DISTINCT
    NumerHolding,
    NumerDIS
FROM dbo.ITJK_MapBLDISHolding_v2 WITH (NOLOCK)
WHERE NumerHolding IN ({placeholders})
   OR NumerDIS IN ({placeholders})
            """,
            params,
        )
        for row in ncur.fetchall():
            holding_order_id = _normalize_int(row[0])
            dis_order_id = _normalize_int(row[1])
            if holding_order_id is None or dis_order_id is None:
                continue
            result.append(NetfoxDisMapRow(holding_order_id=holding_order_id, dis_order_id=dis_order_id))
    dedup = {(row.holding_order_id, row.dis_order_id): row for row in result}
    return list(dedup.values())


def _fetch_netfox_bl_order_rows(ncur, *, order_ids: list[int]) -> list[NetfoxBlOrderRow]:
    result: list[NetfoxBlOrderRow] = []
    normalized = sorted({_normalize_int(value) for value in order_ids if _normalize_int(value) is not None})
    if not normalized:
        return result
    for batch in _chunks(normalized, _NETFOX_BATCH_SIZE):
        placeholders = ",".join("?" for _ in batch)
        ncur.execute(
            f"""
-- courier_identifier_backfill_netfox_bl_orders
SELECT DISTINCT
    order_id,
    external_order_id
FROM dbo.ITJK_ZamowieniaBaselinkerAPI WITH (NOLOCK)
WHERE order_id IN ({placeholders})
  AND external_order_id IS NOT NULL
  AND LTRIM(RTRIM(external_order_id)) <> ''
            """,
            batch,
        )
        for row in ncur.fetchall():
            order_id = _normalize_int(row[0])
            external_order_id = _normalize_text(row[1])
            if order_id is None or not external_order_id:
                continue
            result.append(NetfoxBlOrderRow(order_id=order_id, external_order_id=external_order_id))
    dedup = {(row.order_id, row.external_order_id): row for row in result}
    return list(dedup.values())


def _fetch_netfox_package_rows_by_order_ids(ncur, *, order_ids: list[int]) -> list[NetfoxPackageRow]:
    result: list[NetfoxPackageRow] = []
    normalized = sorted({_normalize_int(value) for value in order_ids if _normalize_int(value) is not None})
    if not normalized:
        return result
    for batch in _chunks(normalized, _NETFOX_BATCH_SIZE):
        placeholders = ",".join("?" for _ in batch)
        ncur.execute(
            f"""
-- courier_identifier_backfill_netfox_packages_by_order
SELECT DISTINCT
    order_id,
    courier_package_nr,
    courier_inner_number,
    courier_code,
    courier_other_name
FROM dbo.ITJK_BaselinkerOrderPackages WITH (NOLOCK)
WHERE order_id IN ({placeholders})
            """,
            batch,
        )
        for row in ncur.fetchall():
            order_id = _normalize_int(row[0])
            if order_id is None:
                continue
            result.append(
                NetfoxPackageRow(
                    order_id=order_id,
                    courier_package_nr=_normalize_text(row[1]) or None,
                    courier_inner_number=_normalize_text(row[2]) or None,
                    courier_code=_normalize_text(row[3]) or None,
                    courier_other_name=_normalize_text(row[4]) or None,
                )
            )
    dedup = {
        (
            row.order_id,
            row.courier_package_nr or "",
            row.courier_inner_number or "",
            row.courier_code or "",
            row.courier_other_name or "",
        ): row
        for row in result
    }
    return list(dedup.values())


def _fetch_dhl_numeric_resolution(ncur, *, tokens: list[str]) -> tuple[set[int], int]:
    normalized = sorted({_normalize_token(value) for value in tokens if _normalize_token(value)})
    if not normalized:
        return set(), 0
    order_ids: set[int] = set()
    matched_tokens: set[str] = set()
    for batch in _chunks(normalized, _NETFOX_BATCH_SIZE):
        placeholders = ",".join("?" for _ in batch)
        params = [*batch, *batch]
        ncur.execute(
            f"""
WITH matched AS (
    SELECT DISTINCT
        UPPER(REPLACE(LTRIM(RTRIM(ISNULL(courier_package_nr, ''))), ' ', '')) AS matched_token,
        order_id
    FROM dbo.ITJK_BaselinkerOrderPackages WITH (NOLOCK)
    WHERE UPPER(REPLACE(LTRIM(RTRIM(ISNULL(courier_package_nr, ''))), ' ', '')) IN ({placeholders})
    UNION
    SELECT DISTINCT
        UPPER(REPLACE(LTRIM(RTRIM(ISNULL(courier_inner_number, ''))), ' ', '')) AS matched_token,
        order_id
    FROM dbo.ITJK_BaselinkerOrderPackages WITH (NOLOCK)
    WHERE UPPER(REPLACE(LTRIM(RTRIM(ISNULL(courier_inner_number, ''))), ' ', '')) IN ({placeholders})
)
-- courier_identifier_backfill_dhl_numeric_netfox_lookup
SELECT matched_token, order_id
FROM matched
WHERE order_id IS NOT NULL
            """,
            params,
        )
        for row in ncur.fetchall():
            matched_token = _normalize_token(row[0])
            order_id = _normalize_int(row[1])
            if not matched_token or order_id is None:
                continue
            matched_tokens.add(matched_token)
            order_ids.add(order_id)
    return order_ids, len(matched_tokens)


def _fetch_dhl_jjd_resolution(ncur, *, tokens: list[str]) -> list[NetfoxJjdRow]:
    normalized = sorted({
        _normalize_token(value)
        for value in tokens
        if _normalize_token(value).startswith("JJD")
    })
    if not normalized:
        return []
    result: list[NetfoxJjdRow] = []
    for batch in _chunks(normalized, _NETFOX_BATCH_SIZE):
        placeholders = ",".join("?" for _ in batch)
        ncur.execute(
            f"""
WITH jjd_map AS (
    SELECT DISTINCT
        UPPER(REPLACE(LTRIM(RTRIM(ISNULL(e.parcel_num_other, ''))), ' ', '')) AS jjd_number,
        UPPER(REPLACE(LTRIM(RTRIM(ISNULL(e.parcel_num, ''))), ' ', '')) AS parcel_number_base
    FROM dbo.ITJK_CouriersInvoicesDetails_Extras e WITH (NOLOCK)
    WHERE UPPER(REPLACE(LTRIM(RTRIM(ISNULL(e.parcel_num_other, ''))), ' ', '')) IN ({placeholders})
      AND e.parcel_num IS NOT NULL
      AND LTRIM(RTRIM(e.parcel_num)) <> ''
),
matched AS (
    SELECT DISTINCT
        jm.jjd_number,
        jm.parcel_number_base,
        p.order_id
    FROM jjd_map jm
    JOIN dbo.ITJK_BaselinkerOrderPackages p WITH (NOLOCK)
      ON UPPER(REPLACE(LTRIM(RTRIM(ISNULL(p.courier_package_nr, ''))), ' ', '')) = jm.parcel_number_base
    UNION
    SELECT DISTINCT
        jm.jjd_number,
        jm.parcel_number_base,
        p.order_id
    FROM jjd_map jm
    JOIN dbo.ITJK_BaselinkerOrderPackages p WITH (NOLOCK)
      ON UPPER(REPLACE(LTRIM(RTRIM(ISNULL(p.courier_inner_number, ''))), ' ', '')) = jm.parcel_number_base
)
-- courier_identifier_backfill_dhl_jjd_netfox_lookup
SELECT jjd_number, parcel_number_base, order_id
FROM matched
WHERE order_id IS NOT NULL
            """,
            batch,
        )
        for row in ncur.fetchall():
            jjd_number = _normalize_token(row[0])
            parcel_number_base = _normalize_token(row[1])
            order_id = _normalize_int(row[2])
            if not jjd_number or not parcel_number_base or order_id is None:
                continue
            result.append(
                NetfoxJjdRow(
                    jjd_number=jjd_number,
                    parcel_number_base=parcel_number_base,
                    order_id=order_id,
                )
            )
    dedup = {(row.jjd_number, row.parcel_number_base, row.order_id): row for row in result}
    return list(dedup.values())


def _write_bl_order_rows(cur, rows: list[NetfoxBlOrderRow]) -> int:
    processed = 0
    for row in rows:
        cur.execute(
            """
MERGE dbo.acc_cache_bl_orders AS target
USING (
    SELECT ? AS order_id, ? AS external_order_id
) AS src
   ON target.order_id = src.order_id
WHEN MATCHED THEN
    UPDATE SET external_order_id = src.external_order_id
WHEN NOT MATCHED THEN
    INSERT (external_order_id, order_id)
    VALUES (src.external_order_id, src.order_id);
            """,
            [row.order_id, row.external_order_id],
        )
        processed += 1
    return processed


def _write_dis_map_rows(cur, rows: list[NetfoxDisMapRow]) -> int:
    processed = 0
    for row in rows:
        cur.execute(
            """
MERGE dbo.acc_cache_dis_map AS target
USING (
    SELECT ? AS holding_order_id, ? AS dis_order_id
) AS src
   ON target.holding_order_id = src.holding_order_id
  AND target.dis_order_id = src.dis_order_id
WHEN NOT MATCHED THEN
    INSERT (holding_order_id, dis_order_id)
    VALUES (src.holding_order_id, src.dis_order_id);
            """,
            [row.holding_order_id, row.dis_order_id],
        )
        processed += 1
    return processed


def _write_package_rows(cur, rows: list[NetfoxPackageRow]) -> int:
    processed = 0
    for row in rows:
        cur.execute(
            """
MERGE dbo.acc_cache_packages AS target
USING (
    SELECT
        ? AS order_id,
        ? AS courier_package_nr,
        ? AS courier_inner_number,
        ? AS courier_code,
        ? AS courier_other_name
) AS src
   ON target.order_id = src.order_id
  AND ISNULL(target.courier_package_nr, '') = ISNULL(src.courier_package_nr, '')
  AND ISNULL(target.courier_inner_number, '') = ISNULL(src.courier_inner_number, '')
WHEN MATCHED THEN
    UPDATE SET
        courier_code = src.courier_code,
        courier_other_name = src.courier_other_name
WHEN NOT MATCHED THEN
    INSERT (
        order_id,
        courier_package_nr,
        courier_inner_number,
        courier_code,
        courier_other_name
    )
    VALUES (
        src.order_id,
        src.courier_package_nr,
        src.courier_inner_number,
        src.courier_code,
        src.courier_other_name
    );
            """,
            [
                row.order_id,
                row.courier_package_nr,
                row.courier_inner_number,
                row.courier_code,
                row.courier_other_name,
            ],
        )
        processed += 1
    return processed


def _write_dhl_jjd_rows(cur, rows: list[NetfoxJjdRow]) -> int:
    processed = 0
    for row in rows:
        source_file = f"netfox_jjd_backfill/{row.jjd_number}"
        source_hash = hashlib.sha256(f"{row.jjd_number}|{row.parcel_number_base}".encode("utf-8")).hexdigest()
        cur.execute(
            """
MERGE dbo.acc_dhl_parcel_map AS target
USING (
    SELECT
        ? AS parcel_number,
        ? AS parcel_number_base,
        ? AS jjd_number,
        ? AS source_file,
        ? AS source_hash
) AS src
   ON target.jjd_number = src.jjd_number
WHEN MATCHED THEN
    UPDATE SET
        parcel_number = src.parcel_number,
        parcel_number_base = src.parcel_number_base,
        source_file = src.source_file,
        source_row_no = 1,
        source_hash = src.source_hash,
        imported_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        parcel_number,
        parcel_number_base,
        parcel_number_suffix,
        jjd_number,
        shipment_type,
        ship_date,
        delivery_date,
        last_event_code,
        last_event_at,
        source_file,
        source_row_no,
        source_hash,
        imported_at
    )
    VALUES (
        src.parcel_number,
        src.parcel_number_base,
        NULL,
        src.jjd_number,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        src.source_file,
        1,
        src.source_hash,
        SYSUTCDATETIME()
    );
            """,
            [
                row.parcel_number_base,
                row.parcel_number_base,
                row.jjd_number,
                source_file,
                source_hash,
            ],
        )
        processed += 1
    return processed


def _commit_in_chunks(conn, cur, rows: list[Any], writer) -> int:
    processed = 0
    for batch in _chunks(rows, _ACC_WRITE_COMMIT_EVERY):
        processed += int(writer(cur, batch) or 0)
        conn.commit()
    return processed


def _set_job_progress_if_needed(job_id: str | None, *, pct: int, message: str, records_processed: int = 0) -> None:
    if not job_id:
        return
    from app.connectors.mssql.mssql_store import set_job_progress

    set_job_progress(
        job_id,
        progress_pct=max(1, min(95, int(pct))),
        records_processed=int(records_processed or 0),
        message=message,
    )


def run_courier_identifier_backfill(
    *,
    mode: str,
    months: list[str] | None = None,
    created_to_buffer_days: int = 31,
    limit_values: int = _DEFAULT_LIMIT_VALUES,
    include_packages: bool = True,
    include_bl_orders: bool = True,
    include_dis_map: bool = True,
    include_dhl_parcel_map: bool = True,
    job_id: str | None = None,
) -> dict[str, Any]:
    mode_key = _normalize_mode(mode)
    month_tokens = _normalize_months(months)
    carrier_key = _mode_to_carrier(mode_key)
    ensure_courier_identifier_cache_schema()

    if mode_key == "dhl_jjd" and include_dhl_parcel_map:
        from app.services.dhl_integration import ensure_dhl_schema

        ensure_dhl_schema()

    results: list[dict[str, Any]] = []
    totals = {
        "candidate_values": 0,
        "candidate_shipments": 0,
        "candidate_shipments_with_actual_cost": 0,
        "resolved_order_ids": 0,
        "netfox_package_rows": 0,
        "netfox_bl_order_rows": 0,
        "netfox_dis_map_rows": 0,
        "netfox_jjd_rows": 0,
        "acc_package_rows_written": 0,
        "acc_bl_order_rows_written": 0,
        "acc_dis_map_rows_written": 0,
        "acc_dhl_parcel_map_rows_written": 0,
    }

    for idx, month_token in enumerate(month_tokens, start=1):
        _set_job_progress_if_needed(
            job_id,
            pct=5 + ((idx - 1) * 70 // max(1, len(month_tokens))),
            message=f"Courier identifier backfill selecting candidates for {month_token} ({mode_key})",
            records_processed=totals["candidate_values"],
        )
        candidates = _load_candidates_for_month(
            month_token=month_token,
            mode=mode_key,
            created_to_buffer_days=created_to_buffer_days,
            limit_values=limit_values,
        )
        candidate_shipments = sum(item.shipments for item in candidates)
        candidate_actual = sum(item.shipments_with_actual_cost for item in candidates)
        base_order_ids: set[int] = set()
        jjd_rows: list[NetfoxJjdRow] = []
        resolved_candidate_values = 0

        netfox_conn = connect_netfox(timeout=15)
        try:
            ncur = netfox_conn.cursor()
            if mode_key in {"gls_note1", "gls_tracking_map"}:
                base_order_ids = {
                    int(value)
                    for value in [_normalize_int(item.value) for item in candidates]
                    if value is not None
                }
                resolved_candidate_values = len(base_order_ids)
            elif mode_key in _EXTERNAL_ORDER_ONLY_MODES:
                base_order_ids = {
                    int(value)
                    for value in [_normalize_int(item.value) for item in candidates]
                    if value is not None
                }
                resolved_candidate_values = len(base_order_ids)
            elif mode_key == "dhl_numeric":
                base_order_ids, resolved_candidate_values = _fetch_dhl_numeric_resolution(
                    ncur,
                    tokens=[item.value for item in candidates],
                )
            elif mode_key == "dhl_jjd":
                jjd_rows = _fetch_dhl_jjd_resolution(
                    ncur,
                    tokens=[item.value for item in candidates],
                )
                base_order_ids = {row.order_id for row in jjd_rows}
                resolved_candidate_values = len({row.jjd_number for row in jjd_rows})

            expanded_order_ids = set(base_order_ids)
            if mode_key in _EXTERNAL_ORDER_ONLY_MODES:
                dis_map_rows = []
                bl_order_rows = _fetch_netfox_bl_order_rows(ncur, order_ids=sorted(expanded_order_ids)) if include_bl_orders else []
                package_rows = []
            else:
                dis_map_rows = _fetch_netfox_dis_map_rows(ncur, order_ids=sorted(base_order_ids)) if include_dis_map else []
                for row in dis_map_rows:
                    expanded_order_ids.add(row.holding_order_id)
                    expanded_order_ids.add(row.dis_order_id)

                bl_order_rows = _fetch_netfox_bl_order_rows(ncur, order_ids=sorted(expanded_order_ids)) if include_bl_orders else []
                package_rows = _fetch_netfox_package_rows_by_order_ids(ncur, order_ids=sorted(expanded_order_ids)) if include_packages else []
        finally:
            netfox_conn.close()

        acc_stats = {
            "acc_package_rows_written": 0,
            "acc_bl_order_rows_written": 0,
            "acc_dis_map_rows_written": 0,
            "acc_dhl_parcel_map_rows_written": 0,
        }
        if package_rows or bl_order_rows or dis_map_rows or (jjd_rows and include_dhl_parcel_map):
            acc_conn = _connect_acc()
            try:
                cur = acc_conn.cursor()
                if include_dis_map and dis_map_rows:
                    acc_stats["acc_dis_map_rows_written"] = _commit_in_chunks(
                        acc_conn,
                        cur,
                        dis_map_rows,
                        _write_dis_map_rows,
                    )
                if include_bl_orders and bl_order_rows:
                    acc_stats["acc_bl_order_rows_written"] = _commit_in_chunks(
                        acc_conn,
                        cur,
                        bl_order_rows,
                        _write_bl_order_rows,
                    )
                if include_packages and package_rows:
                    acc_stats["acc_package_rows_written"] = _commit_in_chunks(
                        acc_conn,
                        cur,
                        package_rows,
                        _write_package_rows,
                    )
                if mode_key == "dhl_jjd" and include_dhl_parcel_map and jjd_rows:
                    acc_stats["acc_dhl_parcel_map_rows_written"] = _commit_in_chunks(
                        acc_conn,
                        cur,
                        jjd_rows,
                        _write_dhl_jjd_rows,
                    )
            finally:
                acc_conn.close()

        item = {
            "month": month_token,
            "carrier": carrier_key,
            "mode": mode_key,
            "candidate_values": len(candidates),
            "candidate_preview": [item.value for item in candidates[:10]],
            "candidate_shipments": candidate_shipments,
            "candidate_shipments_with_actual_cost": candidate_actual,
            "resolved_candidate_values": resolved_candidate_values,
            "resolved_order_ids": len(expanded_order_ids),
            "netfox_package_rows": len(package_rows),
            "netfox_bl_order_rows": len(bl_order_rows),
            "netfox_dis_map_rows": len(dis_map_rows),
            "netfox_jjd_rows": len(jjd_rows),
            **acc_stats,
        }
        results.append(item)

        totals["candidate_values"] += item["candidate_values"]
        totals["candidate_shipments"] += item["candidate_shipments"]
        totals["candidate_shipments_with_actual_cost"] += item["candidate_shipments_with_actual_cost"]
        totals["resolved_order_ids"] += item["resolved_order_ids"]
        totals["netfox_package_rows"] += item["netfox_package_rows"]
        totals["netfox_bl_order_rows"] += item["netfox_bl_order_rows"]
        totals["netfox_dis_map_rows"] += item["netfox_dis_map_rows"]
        totals["netfox_jjd_rows"] += item["netfox_jjd_rows"]
        totals["acc_package_rows_written"] += item["acc_package_rows_written"]
        totals["acc_bl_order_rows_written"] += item["acc_bl_order_rows_written"]
        totals["acc_dis_map_rows_written"] += item["acc_dis_map_rows_written"]
        totals["acc_dhl_parcel_map_rows_written"] += item["acc_dhl_parcel_map_rows_written"]

        _set_job_progress_if_needed(
            job_id,
            pct=15 + (idx * 75 // max(1, len(month_tokens))),
            message=(
                f"Courier identifier backfill {month_token} ({mode_key}) "
                f"resolved_orders={item['resolved_order_ids']} packages={item['acc_package_rows_written']}"
            ),
            records_processed=totals["candidate_values"],
        )
        log.info(
            "courier_identifier_backfill.month_done",
            month=month_token,
            mode=mode_key,
            candidate_values=item["candidate_values"],
            resolved_order_ids=item["resolved_order_ids"],
            package_rows=item["acc_package_rows_written"],
            bl_order_rows=item["acc_bl_order_rows_written"],
            dis_map_rows=item["acc_dis_map_rows_written"],
            dhl_parcel_map_rows=item["acc_dhl_parcel_map_rows_written"],
        )

    return {
        "mode": mode_key,
        "carrier": carrier_key,
        "months": month_tokens,
        "created_to_buffer_days": int(created_to_buffer_days or 0),
        "limit_values": int(limit_values or _DEFAULT_LIMIT_VALUES),
        "items": results,
        "totals": totals,
    }
