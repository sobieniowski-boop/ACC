from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
import math
import uuid
from typing import Any

import pyodbc

from app.core.config import settings
from app.core.db_connection import connect_acc

_SALES_COLUMN_CACHE: dict[str, dict[str, str]] = {}

ALLOWED_JOB_TYPES = {
    "sync_orders",
    "sync_finances",
    "sync_inventory",
    "sync_pricing",
    "sync_offer_fee_estimates",
    "sync_purchase_prices",
    "calc_profit",
    "generate_ai_report",
}

_MONTH_NAMES_PL = [
    "",
    "Styczen",
    "Luty",
    "Marzec",
    "Kwiecien",
    "Maj",
    "Czerwiec",
    "Lipiec",
    "Sierpien",
    "Wrzesien",
    "Pazdziernik",
    "Listopad",
    "Grudzien",
]


def _assert_mssql_enabled() -> None:
    if not settings.mssql_enabled:
        raise RuntimeError("MSSQL is not configured (missing MSSQL_USER/MSSQL_PASSWORD).")


def _connect() -> pyodbc.Connection:
    _assert_mssql_enabled()
    return connect_acc(autocommit=False, timeout=20)


def _split_table_name(raw_table: str) -> tuple[str, str]:
    table = (raw_table or "").strip()
    if not table:
        table = "dbo.ITJK_BazaDanychSprzedazHolding"
    if "." in table:
        schema, name = table.split(".", 1)
    else:
        schema, name = "dbo", table
    return schema.strip(" []"), name.strip(" []")


def _q(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def _full_table_name(raw_table: str) -> str:
    schema, table = _split_table_name(raw_table)
    return f"{_q(schema)}.{_q(table)}"


def _fetchall_dict(cur: pyodbc.Cursor) -> list[dict[str, Any]]:
    cols = [c[0] for c in cur.description] if cur.description else []
    out: list[dict[str, Any]] = []
    for row in cur.fetchall():
        out.append({cols[i]: row[i] for i in range(len(cols))})
    return out


def _to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    text = str(value).strip()
    if not text:
        return None
    for candidate in (text, text[:19], text[:10]):
        try:
            dt = datetime.fromisoformat(candidate)
            if isinstance(dt, datetime):
                return dt
        except ValueError:
            continue
    return None


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def _find_column(cur: pyodbc.Cursor, schema: str, table: str, patterns: list[str]) -> str | None:
    if not patterns:
        return None
    like_block = " OR ".join("c.name COLLATE Latin1_General_CI_AI LIKE ?" for _ in patterns)
    order_block = ", ".join(
        f"CASE WHEN c.name COLLATE Latin1_General_CI_AI LIKE ? THEN {idx} ELSE 999 END"
        for idx in range(len(patterns))
    )
    sql = f"""
        SELECT TOP 1 c.name
        FROM sys.columns c
        JOIN sys.tables t ON t.object_id = c.object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = ? AND t.name = ? AND ({like_block})
        ORDER BY {order_block}, c.column_id
    """
    params: list[Any] = [schema, table]
    params.extend(patterns)
    params.extend(patterns)
    cur.execute(sql, params)
    row = cur.fetchone()
    return str(row[0]) if row else None


def _resolve_sales_columns(cur: pyodbc.Cursor, raw_table: str) -> dict[str, str]:
    schema, table = _split_table_name(raw_table)
    cache_key = f"{schema}.{table}".lower()
    cached = _SALES_COLUMN_CACHE.get(cache_key)
    if cached:
        return cached

    required_patterns: dict[str, list[str]] = {
        "date_col": ["%data%", "%date%"],
        "order_col": ["%numer%zamow%", "%order%number%", "%order%id%"],
        "sku_col": ["%numer%artyk%", "%sku%", "%symbol%"],
        "title_col": ["%nazwa%artyk%", "%title%", "%produkt%", "%nazwa%tow%"],
        "qty_col": ["%ilosc%", "%quantity%", "%qty%"],
        "cogs_col": ["%cena%zakup%", "%purchase%price%", "%cogs%", "%cost%"],
        "revenue_net_col": ["%cena%sprzed%netto%", "%sales%net%", "%revenue%net%"],
        "revenue_gross_col": ["%cena%sprzed%brutto%", "%sales%gross%", "%revenue%gross%"],
        "transport_col": ["%transport%", "%shipping%", "%delivery%"],
        "channel_col": ["%kanal%sprzed%", "%nazwa%kanal%sprzed%", "%channel%sales%"],
    }

    resolved: dict[str, str] = {}
    missing: list[str] = []
    for key, patterns in required_patterns.items():
        col = _find_column(cur, schema, table, patterns)
        if not col:
            missing.append(key)
        else:
            resolved[key] = col

    if missing:
        raise RuntimeError(
            f"Could not resolve required sales columns in {schema}.{table}: {', '.join(missing)}."
        )

    _SALES_COLUMN_CACHE[cache_key] = resolved
    return resolved


def _month_label(year: int, month: int) -> str:
    if month < 1 or month > 12:
        return f"{year}-{month:02d}"
    return f"{_MONTH_NAMES_PL[month]} {year}"


def _expr(parts: dict[str, str]) -> dict[str, str]:
    qty = f"TRY_CONVERT(float, {_q(parts['qty_col'])})"
    rev_net = f"TRY_CONVERT(float, {_q(parts['revenue_net_col'])})"
    rev_gross = f"TRY_CONVERT(float, {_q(parts['revenue_gross_col'])})"
    cogs_unit = f"TRY_CONVERT(float, {_q(parts['cogs_col'])})"
    cogs_total = f"({qty} * {cogs_unit})"
    transport = f"TRY_CONVERT(float, {_q(parts['transport_col'])})"
    cm = f"({rev_net} - {cogs_total} - {transport})"
    cm_pct = (
        f"CASE WHEN NULLIF({rev_net}, 0) IS NULL THEN 0.0 "
        f"ELSE ({cm} * 100.0 / NULLIF({rev_net}, 0)) END"
    )
    return {
        "qty": qty,
        "rev_net": rev_net,
        "rev_gross": rev_gross,
        "cogs_unit": cogs_unit,
        "cogs_total": cogs_total,
        "transport": transport,
        "cm": cm,
        "cm_pct": cm_pct,
    }


def ensure_v2_schema() -> None:
    _assert_mssql_enabled()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
IF OBJECT_ID('dbo.acc_al_alert_rules', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_alert_rules (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        name NVARCHAR(200) NOT NULL,
        description NVARCHAR(500) NULL,
        rule_type NVARCHAR(80) NOT NULL,
        marketplace_id NVARCHAR(160) NULL,
        sku NVARCHAR(120) NULL,
        category NVARCHAR(120) NULL,
        threshold_value DECIMAL(18,4) NULL,
        threshold_operator NVARCHAR(8) NULL,
        severity NVARCHAR(20) NOT NULL DEFAULT 'warning',
        is_active BIT NOT NULL DEFAULT 1,
        created_by NVARCHAR(120) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_al_alert_rules_type ON dbo.acc_al_alert_rules(rule_type, is_active);
END

IF OBJECT_ID('dbo.acc_al_alerts', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_alerts (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        rule_id UNIQUEIDENTIFIER NOT NULL,
        marketplace_id NVARCHAR(160) NULL,
        sku NVARCHAR(120) NULL,
        title NVARCHAR(300) NOT NULL,
        detail NVARCHAR(MAX) NULL,
        severity NVARCHAR(20) NOT NULL,
        current_value DECIMAL(18,4) NULL,
        is_read BIT NOT NULL DEFAULT 0,
        is_resolved BIT NOT NULL DEFAULT 0,
        triggered_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        resolved_at DATETIME2 NULL,
        resolved_by NVARCHAR(120) NULL
    );
    CREATE INDEX IX_acc_al_alerts_state ON dbo.acc_al_alerts(is_resolved, severity, triggered_at);
    CREATE INDEX IX_acc_al_alerts_rule ON dbo.acc_al_alerts(rule_id, is_resolved);
END

IF OBJECT_ID('dbo.acc_al_jobs', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_jobs (
        id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        celery_task_id NVARCHAR(80) NULL,
        job_type NVARCHAR(80) NOT NULL,
        marketplace_id NVARCHAR(160) NULL,
        trigger_source NVARCHAR(20) NOT NULL DEFAULT 'manual',
        triggered_by NVARCHAR(120) NULL,
        status NVARCHAR(20) NOT NULL DEFAULT 'pending',
        progress_pct INT NOT NULL DEFAULT 0,
        progress_message NVARCHAR(300) NULL,
        records_processed INT NULL,
        error_message NVARCHAR(MAX) NULL,
        started_at DATETIME2 NULL,
        finished_at DATETIME2 NULL,
        duration_seconds FLOAT NULL,
        params_json NVARCHAR(MAX) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_al_jobs_main ON dbo.acc_al_jobs(job_type, status, created_at);
END

IF OBJECT_ID('dbo.acc_al_plans', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_plans (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [year] INT NOT NULL,
        [month] INT NOT NULL,
        status NVARCHAR(20) NOT NULL DEFAULT 'draft',
        created_by NVARCHAR(120) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_acc_al_plan_month UNIQUE([year], [month])
    );
END

IF OBJECT_ID('dbo.acc_al_plan_lines', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_plan_lines (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        plan_id INT NOT NULL,
        marketplace_id NVARCHAR(160) NOT NULL,
        target_revenue_pln DECIMAL(18,2) NOT NULL DEFAULT 0,
        target_orders INT NOT NULL DEFAULT 0,
        target_acos_pct DECIMAL(9,2) NOT NULL DEFAULT 0,
        target_cm_pct DECIMAL(9,2) NOT NULL DEFAULT 0,
        budget_ads_pln DECIMAL(18,2) NOT NULL DEFAULT 0,
        actual_revenue_pln DECIMAL(18,2) NULL,
        actual_orders INT NULL,
        actual_acos_pct DECIMAL(9,2) NULL,
        actual_cm_pct DECIMAL(9,2) NULL,
        CONSTRAINT FK_acc_al_plan_lines_plan FOREIGN KEY (plan_id) REFERENCES dbo.acc_al_plans(id)
    );
    CREATE INDEX IX_acc_al_plan_lines_plan ON dbo.acc_al_plan_lines(plan_id, marketplace_id);
END

IF OBJECT_ID('dbo.acc_al_profit_snapshot', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.acc_al_profit_snapshot (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        sales_date DATE NOT NULL,
        order_number NVARCHAR(180) NULL,
        sku NVARCHAR(120) NULL,
        title NVARCHAR(300) NULL,
        quantity FLOAT NOT NULL DEFAULT 0,
        revenue_net DECIMAL(18,2) NOT NULL DEFAULT 0,
        revenue_gross DECIMAL(18,2) NOT NULL DEFAULT 0,
        cogs DECIMAL(18,2) NOT NULL DEFAULT 0,
        transport DECIMAL(18,2) NOT NULL DEFAULT 0,
        channel NVARCHAR(180) NULL,
        source_table NVARCHAR(180) NOT NULL,
        synced_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    CREATE INDEX IX_acc_al_profit_snapshot_date ON dbo.acc_al_profit_snapshot(sales_date, channel);
END
            """
        )
        conn.commit()
    finally:
        conn.close()


def test_connection() -> bool:
    try:
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            return True
        finally:
            conn.close()
    except Exception:
        return False


def list_channels(limit: int = 100) -> list[dict[str, Any]]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cols = _resolve_sales_columns(cur, settings.ALLEGRO_SALES_TABLE)
        channel = _q(cols["channel_col"])
        table = _full_table_name(settings.ALLEGRO_SALES_TABLE)
        sql = f"""
            SELECT TOP 500
                CAST({channel} AS NVARCHAR(180)) AS marketplace_id,
                COUNT(*) AS records
            FROM {table}
            WHERE CAST({channel} AS NVARCHAR(180)) LIKE ?
            GROUP BY CAST({channel} AS NVARCHAR(180))
            ORDER BY COUNT(*) DESC
        """
        cur.execute(sql, (settings.ALLEGRO_CHANNEL_PATTERN,))
        rows = _fetchall_dict(cur)
        return rows[: max(1, min(limit, 500))]
    finally:
        conn.close()


def get_mssql_status() -> dict[str, Any]:
    status: dict[str, Any] = {
        "configured": settings.mssql_enabled,
        "server": settings.MSSQL_SERVER,
        "port": settings.MSSQL_PORT,
        "database": settings.MSSQL_DATABASE,
        "driver": settings.odbc_driver,
        "salesTable": settings.ALLEGRO_SALES_TABLE,
        "channelPattern": settings.ALLEGRO_CHANNEL_PATTERN,
    }
    if not settings.mssql_enabled:
        status["ok"] = False
        status["error"] = "MSSQL credentials missing in .env"
        return status

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cols = _resolve_sales_columns(cur, settings.ALLEGRO_SALES_TABLE)
        status["ok"] = True
        status["resolvedColumns"] = cols
        status["channels"] = list_channels(limit=20)
        return status
    except Exception as exc:
        status["ok"] = False
        status["error"] = str(exc)
        return status
    finally:
        conn.close()


def _sales_where(
    cols: dict[str, str],
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
    sku: str | None = None,
    min_cm_pct: float | None = None,
    max_cm_pct: float | None = None,
) -> tuple[str, list[Any]]:
    e = _expr(cols)
    channel = f"CAST({_q(cols['channel_col'])} AS NVARCHAR(180))"
    sku_sql = f"CAST({_q(cols['sku_col'])} AS NVARCHAR(120))"
    day_sql = f"TRY_CONVERT(date, {_q(cols['date_col'])})"

    where_parts = [
        f"{day_sql} >= ?",
        f"{day_sql} <= ?",
        f"{channel} LIKE ?",
    ]
    params: list[Any] = [date_from.isoformat(), date_to.isoformat(), settings.ALLEGRO_CHANNEL_PATTERN]

    if marketplace_id:
        if "%" in marketplace_id or "_" in marketplace_id:
            where_parts.append(f"{channel} LIKE ?")
        else:
            where_parts.append(f"{channel} = ?")
        params.append(marketplace_id)
    if sku:
        where_parts.append(f"{sku_sql} = ?")
        params.append(sku)
    if min_cm_pct is not None:
        where_parts.append(f"{e['cm_pct']} >= ?")
        params.append(min_cm_pct)
    if max_cm_pct is not None:
        where_parts.append(f"{e['cm_pct']} <= ?")
        params.append(max_cm_pct)

    return " AND ".join(where_parts), params


def get_profit_orders(
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
    sku: str | None = None,
    min_cm_pct: float | None = None,
    max_cm_pct: float | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cols = _resolve_sales_columns(cur, settings.ALLEGRO_SALES_TABLE)
        where_sql, params = _sales_where(
            cols,
            date_from=date_from,
            date_to=date_to,
            marketplace_id=marketplace_id,
            sku=sku,
            min_cm_pct=min_cm_pct,
            max_cm_pct=max_cm_pct,
        )
        e = _expr(cols)
        table = _full_table_name(settings.ALLEGRO_SALES_TABLE)
        order_no = f"CAST({_q(cols['order_col'])} AS NVARCHAR(180))"
        channel = f"CAST({_q(cols['channel_col'])} AS NVARCHAR(180))"
        date_dt = f"TRY_CONVERT(datetime2, {_q(cols['date_col'])})"
        sku_col = f"CAST({_q(cols['sku_col'])} AS NVARCHAR(120))"
        title_col = f"CAST({_q(cols['title_col'])} AS NVARCHAR(300))"

        grouped_sql = f"""
            SELECT
                {order_no} AS order_no,
                {channel} AS marketplace_id,
                MAX({date_dt}) AS purchase_date,
                MIN({sku_col}) AS sample_sku,
                MIN({title_col}) AS sample_title,
                SUM({e['qty']}) AS units,
                SUM({e['rev_net']}) AS revenue_pln,
                SUM({e['rev_gross']}) AS order_total,
                SUM({e['cogs_total']}) AS cogs_pln,
                SUM({e['transport']}) AS logistics_pln,
                SUM({e['cm']}) AS cm_pln,
                CASE
                    WHEN NULLIF(SUM({e['rev_net']}), 0) IS NULL THEN 0.0
                    ELSE SUM({e['cm']}) * 100.0 / NULLIF(SUM({e['rev_net']}), 0)
                END AS cm_pct
            FROM {table}
            WHERE {where_sql}
            GROUP BY {order_no}, {channel}
        """

        count_sql = f"SELECT COUNT(*) FROM ({grouped_sql}) q"
        cur.execute(count_sql, params)
        total = _to_int(cur.fetchone()[0], 0)

        start_row = (max(1, page) - 1) * max(1, page_size) + 1
        end_row = start_row + max(1, page_size) - 1
        paged_sql = f"""
            SELECT *
            FROM (
                SELECT
                    ROW_NUMBER() OVER (ORDER BY purchase_date DESC, order_no DESC) AS rn,
                    q.*
                FROM ({grouped_sql}) q
            ) x
            WHERE x.rn BETWEEN ? AND ?
            ORDER BY x.rn
        """
        cur.execute(paged_sql, (*params, start_row, end_row))
        rows = _fetchall_dict(cur)

        items: list[dict[str, Any]] = []
        for row in rows:
            purchase_dt = _to_datetime(row.get("purchase_date"))
            revenue = _to_float(row.get("revenue_pln"))
            cogs = _to_float(row.get("cogs_pln"))
            logistics = _to_float(row.get("logistics_pln"))
            cm = _to_float(row.get("cm_pln"))
            cm_pct = _to_float(row.get("cm_pct"))
            sample_sku = (row.get("sample_sku") or "") if row.get("sample_sku") is not None else ""
            sample_title = row.get("sample_title")
            items.append(
                {
                    "id": f"{row.get('order_no')}",
                    "amazon_order_id": f"{row.get('order_no')}",
                    "marketplace_id": f"{row.get('marketplace_id')}",
                    "marketplace_code": f"{row.get('marketplace_id')}",
                    "purchase_date": purchase_dt or datetime.now(timezone.utc),
                    "status": "Shipped",
                    "fulfillment_channel": "Allegro",
                    "order_total": _to_float(row.get("order_total")),
                    "currency": "PLN",
                    "revenue_pln": revenue,
                    "cogs_pln": cogs,
                    "amazon_fees_pln": logistics,
                    "ads_cost_pln": 0.0,
                    "logistics_pln": logistics,
                    "contribution_margin_pln": cm,
                    "cm_percent": cm_pct,
                    "lines": [
                        {
                            "sku": sample_sku or None,
                            "asin": None,
                            "title": sample_title,
                            "quantity": max(1, _to_int(row.get("units"), 1)),
                            "item_price": revenue,
                            "currency": "PLN",
                            "cogs_pln": cogs,
                            "fba_fee_pln": logistics,
                            "referral_fee_pln": 0.0,
                        }
                    ],
                }
            )

        pages = math.ceil(total / max(1, page_size)) if total else 0
        return {
            "total": total,
            "page": max(1, page),
            "page_size": max(1, page_size),
            "pages": pages,
            "items": items,
        }
    finally:
        conn.close()


def get_profit_by_sku(
    *,
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cols = _resolve_sales_columns(cur, settings.ALLEGRO_SALES_TABLE)
        where_sql, params = _sales_where(
            cols,
            date_from=date_from,
            date_to=date_to,
            marketplace_id=marketplace_id,
        )
        e = _expr(cols)
        table = _full_table_name(settings.ALLEGRO_SALES_TABLE)
        sku_col = f"CAST({_q(cols['sku_col'])} AS NVARCHAR(120))"
        title_col = f"CAST({_q(cols['title_col'])} AS NVARCHAR(300))"
        order_col = f"CAST({_q(cols['order_col'])} AS NVARCHAR(180))"
        sql = f"""
            SELECT
                {sku_col} AS sku,
                MIN({title_col}) AS title,
                SUM({e['qty']}) AS units,
                SUM({e['rev_net']}) AS revenue_pln,
                SUM({e['cogs_total']}) AS cogs_pln,
                SUM({e['transport']}) AS fees_pln,
                SUM({e['cm']}) AS cm_pln,
                COUNT(DISTINCT {order_col}) AS orders
            FROM {table}
            WHERE {where_sql}
            GROUP BY {sku_col}
            ORDER BY SUM({e['rev_net']}) DESC
        """
        cur.execute(sql, params)
        rows = _fetchall_dict(cur)

        items = []
        for row in rows:
            revenue = _to_float(row.get("revenue_pln"))
            cm = _to_float(row.get("cm_pln"))
            items.append(
                {
                    "sku": str(row.get("sku") or ""),
                    "asin": None,
                    "title": row.get("title"),
                    "units": _to_int(row.get("units")),
                    "revenue_pln": revenue,
                    "cogs_pln": _to_float(row.get("cogs_pln")),
                    "amazon_fees_pln": _to_float(row.get("fees_pln")),
                    "contribution_margin_pln": cm,
                    "cm_percent": round((cm * 100.0 / revenue), 2) if revenue else 0.0,
                    "orders": _to_int(row.get("orders")),
                }
            )

        return {
            "date_from": date_from,
            "date_to": date_to,
            "marketplace_id": marketplace_id,
            "total_skus": len(items),
            "items": items,
        }
    finally:
        conn.close()


def sync_profit_snapshot(*, date_from: date, date_to: date) -> int:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cols = _resolve_sales_columns(cur, settings.ALLEGRO_SALES_TABLE)
        e = _expr(cols)
        table = _full_table_name(settings.ALLEGRO_SALES_TABLE)

        date_col = f"TRY_CONVERT(date, {_q(cols['date_col'])})"
        order_col = f"CAST({_q(cols['order_col'])} AS NVARCHAR(180))"
        sku_col = f"CAST({_q(cols['sku_col'])} AS NVARCHAR(120))"
        title_col = f"CAST({_q(cols['title_col'])} AS NVARCHAR(300))"
        channel_col = f"CAST({_q(cols['channel_col'])} AS NVARCHAR(180))"

        cur.execute(
            """
            DELETE FROM dbo.acc_al_profit_snapshot
            WHERE sales_date >= ? AND sales_date <= ? AND source_table = ?
            """,
            (date_from.isoformat(), date_to.isoformat(), settings.ALLEGRO_SALES_TABLE),
        )
        conn.commit()

        insert_sql = f"""
            INSERT INTO dbo.acc_al_profit_snapshot
            (
                sales_date,
                order_number,
                sku,
                title,
                quantity,
                revenue_net,
                revenue_gross,
                cogs,
                transport,
                channel,
                source_table
            )
            SELECT
                {date_col} AS sales_date,
                {order_col} AS order_number,
                {sku_col} AS sku,
                {title_col} AS title,
                {e['qty']} AS quantity,
                {e['rev_net']} AS revenue_net,
                {e['rev_gross']} AS revenue_gross,
                {e['cogs_total']} AS cogs,
                {e['transport']} AS transport,
                {channel_col} AS channel,
                ? AS source_table
            FROM {table}
            WHERE
                {date_col} >= ?
                AND {date_col} <= ?
                AND {channel_col} LIKE ?
        """
        cur.execute(
            insert_sql,
            (
                settings.ALLEGRO_SALES_TABLE,
                date_from.isoformat(),
                date_to.isoformat(),
                settings.ALLEGRO_CHANNEL_PATTERN,
            ),
        )
        inserted = _to_int(cur.rowcount, 0)
        conn.commit()
        return inserted
    finally:
        conn.close()


def compare_value(operator: str | None, current_value: float, threshold_value: float | None) -> bool:
    if threshold_value is None:
        return False
    op = (operator or "<=").strip().lower()
    if op in {"<", "lt"}:
        return current_value < threshold_value
    if op in {"<=", "lte"}:
        return current_value <= threshold_value
    if op in {">", "gt"}:
        return current_value > threshold_value
    if op in {">=", "gte"}:
        return current_value >= threshold_value
    if op in {"=", "==", "eq"}:
        return current_value == threshold_value
    if op in {"!=", "<>", "ne"}:
        return current_value != threshold_value
    return current_value <= threshold_value


def list_alert_rules() -> list[dict[str, Any]]:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                name,
                description,
                rule_type,
                marketplace_id,
                sku,
                category,
                threshold_value,
                threshold_operator,
                severity,
                is_active,
                created_by,
                created_at
            FROM dbo.acc_al_alert_rules
            ORDER BY name
            """
        )
        rows = _fetchall_dict(cur)
        for row in rows:
            row["is_active"] = bool(row.get("is_active"))
        return rows
    finally:
        conn.close()


def create_alert_rule(payload: dict[str, Any], actor: str | None = None) -> dict[str, Any]:
    ensure_v2_schema()
    rule_id = str(uuid.uuid4())
    created_by = actor or settings.DEFAULT_ACTOR
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO dbo.acc_al_alert_rules
            (
                id, name, description, rule_type, marketplace_id, sku, category,
                threshold_value, threshold_operator, severity, is_active, created_by
            )
            VALUES
            (
                CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?
            )
            """,
            (
                rule_id,
                payload.get("name"),
                payload.get("description"),
                payload.get("rule_type"),
                payload.get("marketplace_id"),
                payload.get("sku"),
                payload.get("category"),
                payload.get("threshold_value"),
                payload.get("threshold_operator"),
                payload.get("severity", "warning"),
                1 if payload.get("is_active", True) else 0,
                created_by,
            ),
        )
        conn.commit()
        cur.execute(
            """
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                name, description, rule_type, marketplace_id, sku, category,
                threshold_value, threshold_operator, severity, is_active, created_by, created_at
            FROM dbo.acc_al_alert_rules
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (rule_id,),
        )
        row = _fetchall_dict(cur)[0]
        row["is_active"] = bool(row.get("is_active"))
        return row
    finally:
        conn.close()


def delete_alert_rule(rule_id: str) -> bool:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM dbo.acc_al_alert_rules WHERE id = CAST(? AS UNIQUEIDENTIFIER)", (rule_id,))
        deleted = _to_int(cur.rowcount, 0) > 0
        conn.commit()
        return deleted
    finally:
        conn.close()


def _metric_for_rule(
    cur: pyodbc.Cursor,
    cols: dict[str, str],
    *,
    rule_type: str,
    marketplace_id: str | None,
    sku: str | None,
    days: int,
) -> float:
    e = _expr(cols)
    table = _full_table_name(settings.ALLEGRO_SALES_TABLE)
    date_col = f"TRY_CONVERT(date, {_q(cols['date_col'])})"
    channel_col = f"CAST({_q(cols['channel_col'])} AS NVARCHAR(180))"
    sku_col = f"CAST({_q(cols['sku_col'])} AS NVARCHAR(120))"
    order_col = f"CAST({_q(cols['order_col'])} AS NVARCHAR(180))"

    where = [f"{date_col} >= ?", f"{date_col} <= ?", f"{channel_col} LIKE ?"]
    since = (date.today() - timedelta(days=max(1, days))).isoformat()
    until = date.today().isoformat()
    params: list[Any] = [since, until, settings.ALLEGRO_CHANNEL_PATTERN]
    if marketplace_id:
        if "%" in marketplace_id or "_" in marketplace_id:
            where.append(f"{channel_col} LIKE ?")
        else:
            where.append(f"{channel_col} = ?")
        params.append(marketplace_id)
    if sku:
        where.append(f"{sku_col} = ?")
        params.append(sku)

    rt = (rule_type or "").lower()
    if "order" in rt:
        measure = f"COUNT(DISTINCT {order_col})"
    elif "transport" in rt:
        measure = (
            f"CASE WHEN NULLIF(SUM({e['rev_net']}),0) IS NULL THEN 0.0 "
            f"ELSE SUM({e['transport']}) * 100.0 / NULLIF(SUM({e['rev_net']}),0) END"
        )
    elif "cm" in rt:
        measure = (
            f"CASE WHEN NULLIF(SUM({e['rev_net']}),0) IS NULL THEN 0.0 "
            f"ELSE SUM({e['cm']}) * 100.0 / NULLIF(SUM({e['rev_net']}),0) END"
        )
    else:
        measure = f"SUM({e['rev_net']})"

    sql = f"SELECT {measure} AS metric FROM {table} WHERE " + " AND ".join(where)
    cur.execute(sql, params)
    row = cur.fetchone()
    return _to_float(row[0] if row else 0.0)


def evaluate_alert_rules(days: int = 7) -> int:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cols = _resolve_sales_columns(cur, settings.ALLEGRO_SALES_TABLE)
        cur.execute(
            """
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                name,
                rule_type,
                marketplace_id,
                sku,
                threshold_value,
                threshold_operator,
                severity
            FROM dbo.acc_al_alert_rules
            WHERE is_active = 1
            """
        )
        rules = _fetchall_dict(cur)
        created = 0

        for rule in rules:
            current = _metric_for_rule(
                cur,
                cols,
                rule_type=str(rule.get("rule_type") or ""),
                marketplace_id=rule.get("marketplace_id"),
                sku=rule.get("sku"),
                days=days,
            )
            threshold = rule.get("threshold_value")
            op = rule.get("threshold_operator") or "<="
            threshold_num = _to_float(threshold) if threshold is not None else None
            if not compare_value(str(op), current, threshold_num):
                continue

            cur.execute(
                """
                SELECT COUNT(*)
                FROM dbo.acc_al_alerts
                WHERE rule_id = CAST(? AS UNIQUEIDENTIFIER)
                  AND is_resolved = 0
                  AND triggered_at >= DATEADD(hour, -24, SYSUTCDATETIME())
                """,
                (rule["id"],),
            )
            if _to_int(cur.fetchone()[0], 0) > 0:
                continue

            alert_id = str(uuid.uuid4())
            threshold_text = f"{threshold}" if threshold is not None else "n/a"
            title = f"Alert: {rule.get('name')}"
            detail = (
                f"Rule '{rule.get('name')}' triggered. "
                f"Current value={round(current, 4)}, operator={op}, threshold={threshold_text}."
            )
            cur.execute(
                """
                INSERT INTO dbo.acc_al_alerts
                (
                    id, rule_id, marketplace_id, sku, title, detail, severity, current_value
                )
                VALUES
                (
                    CAST(? AS UNIQUEIDENTIFIER), CAST(? AS UNIQUEIDENTIFIER), ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    alert_id,
                    rule["id"],
                    rule.get("marketplace_id"),
                    rule.get("sku"),
                    title,
                    detail,
                    rule.get("severity") or "warning",
                    current,
                ),
            )
            created += 1

        conn.commit()
        return created
    finally:
        conn.close()


def list_alerts(
    *,
    is_resolved: bool | None = False,
    severity: str | None = None,
    marketplace_id: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        where_parts = ["1=1"]
        params: list[Any] = []

        if is_resolved is not None:
            where_parts.append("a.is_resolved = ?")
            params.append(1 if is_resolved else 0)
        if severity:
            where_parts.append("a.severity = ?")
            params.append(severity)
        if marketplace_id:
            where_parts.append("a.marketplace_id = ?")
            params.append(marketplace_id)

        where_sql = " AND ".join(where_parts)
        base_sql = f"""
            SELECT
                CAST(a.id AS NVARCHAR(40)) AS id,
                CAST(a.rule_id AS NVARCHAR(40)) AS rule_id,
                r.rule_type AS rule_type,
                a.marketplace_id,
                a.sku,
                a.title,
                a.detail,
                a.severity,
                a.current_value,
                a.is_read,
                a.is_resolved,
                a.triggered_at
            FROM dbo.acc_al_alerts a
            LEFT JOIN dbo.acc_al_alert_rules r ON r.id = a.rule_id
            WHERE {where_sql}
        """

        cur.execute(f"SELECT COUNT(*) FROM ({base_sql}) q", params)
        total = _to_int(cur.fetchone()[0], 0)

        cur.execute(
            "SELECT COUNT(*) FROM dbo.acc_al_alerts WHERE is_read = 0 AND is_resolved = 0"
        )
        unread = _to_int(cur.fetchone()[0], 0)
        cur.execute(
            "SELECT COUNT(*) FROM dbo.acc_al_alerts WHERE severity = 'critical' AND is_resolved = 0"
        )
        critical = _to_int(cur.fetchone()[0], 0)

        start_row = (max(1, page) - 1) * max(1, page_size) + 1
        end_row = start_row + max(1, page_size) - 1
        paged_sql = f"""
            SELECT *
            FROM (
                SELECT
                    ROW_NUMBER() OVER (ORDER BY triggered_at DESC) AS rn,
                    q.*
                FROM ({base_sql}) q
            ) x
            WHERE x.rn BETWEEN ? AND ?
            ORDER BY x.rn
        """
        cur.execute(paged_sql, (*params, start_row, end_row))
        rows = _fetchall_dict(cur)
        for row in rows:
            row["is_read"] = bool(row.get("is_read"))
            row["is_resolved"] = bool(row.get("is_resolved"))

        return {
            "total": total,
            "unread": unread,
            "critical_count": critical,
            "items": rows,
        }
    finally:
        conn.close()


def mark_alert_read(alert_id: str) -> bool:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE dbo.acc_al_alerts
            SET is_read = 1
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (alert_id,),
        )
        changed = _to_int(cur.rowcount, 0) > 0
        conn.commit()
        return changed
    finally:
        conn.close()


def resolve_alert(alert_id: str, resolved_by: str | None = None) -> bool:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE dbo.acc_al_alerts
            SET
                is_resolved = 1,
                resolved_at = SYSUTCDATETIME(),
                resolved_by = ?
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (resolved_by or settings.DEFAULT_ACTOR, alert_id),
        )
        changed = _to_int(cur.rowcount, 0) > 0
        conn.commit()
        return changed
    finally:
        conn.close()


def create_job(
    *,
    job_type: str,
    marketplace_id: str | None = None,
    trigger_source: str = "manual",
    triggered_by: str | None = None,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_v2_schema()
    if job_type not in ALLOWED_JOB_TYPES:
        raise RuntimeError(f"Unknown job_type '{job_type}'. Allowed: {sorted(ALLOWED_JOB_TYPES)}")

    job_id = str(uuid.uuid4())
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO dbo.acc_al_jobs
            (
                id, celery_task_id, job_type, marketplace_id, trigger_source, triggered_by,
                status, progress_pct, params_json
            )
            VALUES
            (
                CAST(? AS UNIQUEIDENTIFIER), NULL, ?, ?, ?, ?,
                'pending', 0, ?
            )
            """,
            (
                job_id,
                job_type,
                marketplace_id,
                trigger_source,
                triggered_by or settings.DEFAULT_ACTOR,
                json.dumps(params or {}, ensure_ascii=True),
            ),
        )
        conn.commit()
        return get_job(job_id) or {}
    finally:
        conn.close()


def _update_job(
    job_id: str,
    *,
    status: str | None = None,
    progress_pct: int | None = None,
    progress_message: str | None = None,
    records_processed: int | None = None,
    error_message: str | None = None,
    started_at_now: bool = False,
    finished_at_now: bool = False,
) -> bool:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        set_parts: list[str] = []
        params: list[Any] = []
        if status is not None:
            set_parts.append("status = ?")
            params.append(status)
        if progress_pct is not None:
            set_parts.append("progress_pct = ?")
            params.append(progress_pct)
        if progress_message is not None:
            set_parts.append("progress_message = ?")
            params.append(progress_message)
        if records_processed is not None:
            set_parts.append("records_processed = ?")
            params.append(records_processed)
        if error_message is not None:
            set_parts.append("error_message = ?")
            params.append(error_message)
        if started_at_now:
            set_parts.append("started_at = ISNULL(started_at, SYSUTCDATETIME())")
        if finished_at_now:
            set_parts.append("finished_at = SYSUTCDATETIME()")
            set_parts.append(
                "duration_seconds = CASE WHEN started_at IS NULL THEN NULL ELSE DATEDIFF(second, started_at, SYSUTCDATETIME()) END"
            )
        if not set_parts:
            return False

        sql = "UPDATE dbo.acc_al_jobs SET " + ", ".join(set_parts) + " WHERE id = CAST(? AS UNIQUEIDENTIFIER)"
        params.append(job_id)
        cur.execute(sql, params)
        changed = _to_int(cur.rowcount, 0) > 0
        conn.commit()
        return changed
    finally:
        conn.close()


def set_job_running(job_id: str, message: str | None = None) -> bool:
    return _update_job(
        job_id,
        status="running",
        progress_pct=10,
        progress_message=message or "Running",
        started_at_now=True,
    )


def set_job_success(job_id: str, records_processed: int = 0, message: str | None = None) -> bool:
    return _update_job(
        job_id,
        status="completed",
        progress_pct=100,
        progress_message=message or "Completed",
        records_processed=records_processed,
        finished_at_now=True,
    )


def set_job_failure(job_id: str, error_message: str) -> bool:
    return _update_job(
        job_id,
        status="failure",
        progress_pct=100,
        progress_message="Failed",
        error_message=error_message[:4000],
        finished_at_now=True,
    )


def get_job(job_id: str) -> dict[str, Any] | None:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                celery_task_id,
                job_type,
                marketplace_id,
                trigger_source,
                status,
                progress_pct,
                progress_message,
                records_processed,
                error_message,
                started_at,
                finished_at,
                duration_seconds,
                created_at
            FROM dbo.acc_al_jobs
            WHERE id = CAST(? AS UNIQUEIDENTIFIER)
            """,
            (job_id,),
        )
        rows = _fetchall_dict(cur)
        return rows[0] if rows else None
    finally:
        conn.close()


def list_jobs(
    *,
    job_type: str | None = None,
    status: str | None = None,
    page: int = 1,
    page_size: int = 30,
) -> dict[str, Any]:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        where = ["1=1"]
        params: list[Any] = []
        if job_type:
            where.append("job_type = ?")
            params.append(job_type)
        if status:
            where.append("status = ?")
            params.append(status)
        where_sql = " AND ".join(where)

        base_sql = f"""
            SELECT
                CAST(id AS NVARCHAR(40)) AS id,
                celery_task_id,
                job_type,
                marketplace_id,
                trigger_source,
                status,
                progress_pct,
                progress_message,
                records_processed,
                error_message,
                started_at,
                finished_at,
                duration_seconds,
                created_at
            FROM dbo.acc_al_jobs
            WHERE {where_sql}
        """

        cur.execute(f"SELECT COUNT(*) FROM ({base_sql}) q", params)
        total = _to_int(cur.fetchone()[0], 0)

        start_row = (max(1, page) - 1) * max(1, page_size) + 1
        end_row = start_row + max(1, page_size) - 1
        paged_sql = f"""
            SELECT *
            FROM (
                SELECT ROW_NUMBER() OVER (ORDER BY created_at DESC) AS rn, q.*
                FROM ({base_sql}) q
            ) x
            WHERE x.rn BETWEEN ? AND ?
            ORDER BY x.rn
        """
        cur.execute(paged_sql, (*params, start_row, end_row))
        rows = _fetchall_dict(cur)
        return {"total": total, "items": rows}
    finally:
        conn.close()


def run_job_type(job_id: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    job = get_job(job_id)
    if not job:
        raise RuntimeError("Job not found")

    job_type = str(job.get("job_type") or "")
    payload = params or {}
    set_job_running(job_id, f"Starting {job_type}")

    try:
        today = date.today()
        if job_type == "calc_profit":
            from_date = date.fromisoformat(payload["date_from"]) if payload.get("date_from") else (today - timedelta(days=30))
            to_date = date.fromisoformat(payload["date_to"]) if payload.get("date_to") else today
            synced = sync_profit_snapshot(date_from=from_date, date_to=to_date)
            generated = evaluate_alert_rules(days=7)
            set_job_success(job_id, records_processed=synced, message=f"Profit synced={synced}, alerts={generated}")
        elif job_type in {"sync_orders", "sync_finances", "sync_inventory"}:
            from_date = date.fromisoformat(payload["date_from"]) if payload.get("date_from") else (today - timedelta(days=7))
            to_date = date.fromisoformat(payload["date_to"]) if payload.get("date_to") else today
            synced = sync_profit_snapshot(date_from=from_date, date_to=to_date)
            set_job_success(job_id, records_processed=synced, message=f"{job_type} synced snapshot rows={synced}")
        elif job_type == "sync_pricing":
            created = evaluate_alert_rules(days=3)
            set_job_success(job_id, records_processed=created, message=f"Pricing checks done, alerts={created}")
        elif job_type == "sync_offer_fee_estimates":
            import asyncio
            from app.services.sync_service import sync_offer_fee_estimates as _sync_offer_fee_estimates

            only_missing_raw = payload.get("only_missing", True)
            only_missing = (
                only_missing_raw
                if isinstance(only_missing_raw, bool)
                else str(only_missing_raw).strip().lower() in {"1", "true", "yes", "y"}
            )
            result = asyncio.run(
                _sync_offer_fee_estimates(
                    marketplace_id=job.get("marketplace_id"),
                    job_id=job_id,
                    max_offers=int(payload.get("max_offers", 600)),
                    only_missing=only_missing,
                )
            )
            synced = int((result or {}).get("synced", 0) or 0)
            errors = int((result or {}).get("errors", 0) or 0)
            processed = int((result or {}).get("processed", 0) or 0)
            set_job_success(
                job_id,
                records_processed=synced,
                message=(
                    f"Expected fees synced={synced}, "
                    f"processed={processed}, errors={errors}"
                ),
            )
        elif job_type == "generate_ai_report":
            summary = get_profit_by_sku(date_from=today - timedelta(days=30), date_to=today, marketplace_id=None)
            set_job_success(
                job_id,
                records_processed=summary.get("total_skus", 0),
                message=f"AI report source prepared for {summary.get('total_skus', 0)} skus",
            )
        elif job_type == "sync_purchase_prices":
            import asyncio
            from app.services.sync_service import sync_purchase_prices as _spp
            updated = asyncio.run(_spp(job_id=None))
            set_job_success(job_id, records_processed=updated, message=f"Purchase prices synced={updated}")
        else:
            raise RuntimeError(f"Unsupported job type '{job_type}'")
    except Exception as exc:
        set_job_failure(job_id, str(exc))

    return get_job(job_id) or {}


def _plan_lines_for_month(cur: pyodbc.Cursor, plan_id: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
            id,
            plan_id,
            marketplace_id,
            target_revenue_pln,
            target_orders,
            target_acos_pct,
            target_cm_pct,
            budget_ads_pln,
            actual_revenue_pln,
            actual_orders,
            actual_acos_pct,
            actual_cm_pct
        FROM dbo.acc_al_plan_lines
        WHERE plan_id = ?
        ORDER BY marketplace_id
        """,
        (plan_id,),
    )
    rows = _fetchall_dict(cur)
    out: list[dict[str, Any]] = []
    for row in rows:
        target_rev = _to_float(row.get("target_revenue_pln"))
        actual_rev = _to_float(row.get("actual_revenue_pln"), default=0.0) if row.get("actual_revenue_pln") is not None else None
        out.append(
            {
                "id": _to_int(row.get("id")),
                "plan_id": _to_int(row.get("plan_id")),
                "marketplace_id": row.get("marketplace_id"),
                "marketplace_code": row.get("marketplace_id"),
                "target_revenue_pln": target_rev,
                "target_orders": _to_int(row.get("target_orders")),
                "target_acos_pct": _to_float(row.get("target_acos_pct")),
                "target_cm_pct": _to_float(row.get("target_cm_pct")),
                "budget_ads_pln": _to_float(row.get("budget_ads_pln")),
                "actual_revenue_pln": actual_rev,
                "actual_orders": _to_int(row.get("actual_orders")) if row.get("actual_orders") is not None else None,
                "actual_acos_pct": _to_float(row.get("actual_acos_pct")) if row.get("actual_acos_pct") is not None else None,
                "actual_cm_pct": _to_float(row.get("actual_cm_pct")) if row.get("actual_cm_pct") is not None else None,
                "revenue_attainment_pct": (round((actual_rev * 100.0 / target_rev), 1) if actual_rev is not None and target_rev else None),
            }
        )
    return out


def create_plan_month(payload: dict[str, Any], actor: str | None = None) -> dict[str, Any]:
    ensure_v2_schema()
    year = int(payload["year"])
    month = int(payload["month"])
    lines = payload.get("lines") or []
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM dbo.acc_al_plans WHERE [year] = ? AND [month] = ?", (year, month))
        exists = cur.fetchone()
        if exists:
            raise RuntimeError(f"Plan for {year}/{month:02d} already exists")

        cur.execute(
            """
            INSERT INTO dbo.acc_al_plans([year], [month], status, created_by)
            VALUES(?, ?, 'draft', ?)
            """,
            (year, month, actor or settings.DEFAULT_ACTOR),
        )
        cur.execute("SELECT CAST(SCOPE_IDENTITY() AS INT)")
        plan_id = _to_int(cur.fetchone()[0], 0)

        for line in lines:
            cur.execute(
                """
                INSERT INTO dbo.acc_al_plan_lines
                (
                    plan_id, marketplace_id, target_revenue_pln, target_orders,
                    target_acos_pct, target_cm_pct, budget_ads_pln
                )
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    plan_id,
                    line.get("marketplace_id"),
                    _to_float(line.get("target_revenue_pln")),
                    _to_int(line.get("target_orders")),
                    _to_float(line.get("target_acos_pct")),
                    _to_float(line.get("target_cm_pct")),
                    _to_float(line.get("budget_ads_pln")),
                ),
            )
        conn.commit()

        refresh_plan_actuals(plan_id=plan_id)
        plans = list_plan_months(year=year)
        for plan in plans:
            if plan["id"] == plan_id:
                return plan
        raise RuntimeError("Created plan was not found")
    finally:
        conn.close()


def update_plan_status(plan_id: int, status: str) -> dict[str, Any]:
    ensure_v2_schema()
    if status not in {"draft", "approved", "locked"}:
        raise RuntimeError("Status must be one of: draft, approved, locked")

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT [year], [month], status FROM dbo.acc_al_plans WHERE id = ?", (plan_id,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError("Plan not found")
        existing_status = str(row[2] or "")
        if existing_status == "locked" and status != "locked":
            raise RuntimeError("Locked plans cannot be modified")
        cur.execute("UPDATE dbo.acc_al_plans SET status = ? WHERE id = ?", (status, plan_id))
        conn.commit()
    finally:
        conn.close()

    year = _to_int(row[0], 0)  # type: ignore[name-defined]
    plans = list_plan_months(year=year)
    for plan in plans:
        if plan["id"] == plan_id:
            return plan
    raise RuntimeError("Plan not found after status update")


def refresh_plan_actuals(*, plan_id: int | None = None, year: int | None = None) -> int:
    ensure_v2_schema()
    conn = _connect()
    try:
        cur = conn.cursor()
        cols = _resolve_sales_columns(cur, settings.ALLEGRO_SALES_TABLE)
        e = _expr(cols)
        table = _full_table_name(settings.ALLEGRO_SALES_TABLE)
        date_col = f"TRY_CONVERT(date, {_q(cols['date_col'])})"
        channel_col = f"CAST({_q(cols['channel_col'])} AS NVARCHAR(180))"
        order_col = f"CAST({_q(cols['order_col'])} AS NVARCHAR(180))"

        filter_parts = ["1=1"]
        params: list[Any] = []
        if plan_id is not None:
            filter_parts.append("p.id = ?")
            params.append(plan_id)
        if year is not None:
            filter_parts.append("p.[year] = ?")
            params.append(year)

        sql = f"""
            SELECT
                l.id AS line_id,
                l.marketplace_id,
                p.[year],
                p.[month]
            FROM dbo.acc_al_plan_lines l
            JOIN dbo.acc_al_plans p ON p.id = l.plan_id
            WHERE {' AND '.join(filter_parts)}
        """
        cur.execute(sql, params)
        rows = _fetchall_dict(cur)
        updated = 0
        for row in rows:
            y = _to_int(row["year"])
            m = _to_int(row["month"])
            start = date(y, m, 1)
            if m == 12:
                end = date(y + 1, 1, 1) - timedelta(days=1)
            else:
                end = date(y, m + 1, 1) - timedelta(days=1)

            market = str(row.get("marketplace_id") or "")
            metric_sql = f"""
                SELECT
                    SUM({e['rev_net']}) AS revenue,
                    COUNT(DISTINCT {order_col}) AS orders_cnt,
                    CASE
                        WHEN NULLIF(SUM({e['rev_net']}), 0) IS NULL THEN 0.0
                        ELSE SUM({e['cm']}) * 100.0 / NULLIF(SUM({e['rev_net']}), 0)
                    END AS cm_pct
                FROM {table}
                WHERE
                    {date_col} >= ?
                    AND {date_col} <= ?
                    AND {channel_col} LIKE ?
                    AND ({channel_col} = ? OR ? = '')
            """
            cur.execute(
                metric_sql,
                (
                    start.isoformat(),
                    end.isoformat(),
                    settings.ALLEGRO_CHANNEL_PATTERN,
                    market,
                    market,
                ),
            )
            met = cur.fetchone()
            revenue = _to_float(met[0] if met else 0.0)
            orders_cnt = _to_int(met[1] if met else 0)
            cm_pct = _to_float(met[2] if met else 0.0)

            cur.execute(
                """
                UPDATE dbo.acc_al_plan_lines
                SET
                    actual_revenue_pln = ?,
                    actual_orders = ?,
                    actual_acos_pct = NULL,
                    actual_cm_pct = ?
                WHERE id = ?
                """,
                (revenue, orders_cnt, cm_pct, _to_int(row["line_id"])),
            )
            updated += 1

        conn.commit()
        return updated
    finally:
        conn.close()


def list_plan_months(year: int | None = None) -> list[dict[str, Any]]:
    ensure_v2_schema()
    if year is not None:
        refresh_plan_actuals(year=year)
    else:
        refresh_plan_actuals()

    conn = _connect()
    try:
        cur = conn.cursor()
        if year is None:
            cur.execute(
                """
                SELECT id, [year], [month], status, created_by, created_at
                FROM dbo.acc_al_plans
                ORDER BY [year] DESC, [month] DESC
                """
            )
        else:
            cur.execute(
                """
                SELECT id, [year], [month], status, created_by, created_at
                FROM dbo.acc_al_plans
                WHERE [year] = ?
                ORDER BY [month] DESC
                """,
                (year,),
            )
        months = _fetchall_dict(cur)
        out: list[dict[str, Any]] = []
        for m in months:
            plan_id = _to_int(m["id"])
            lines = _plan_lines_for_month(cur, plan_id)
            total_target = sum(_to_float(line.get("target_revenue_pln")) for line in lines)
            total_budget = sum(_to_float(line.get("budget_ads_pln")) for line in lines)
            total_actual = sum(_to_float(line.get("actual_revenue_pln")) for line in lines if line.get("actual_revenue_pln") is not None)
            out.append(
                {
                    "id": plan_id,
                    "year": _to_int(m.get("year")),
                    "month": _to_int(m.get("month")),
                    "month_label": _month_label(_to_int(m.get("year")), _to_int(m.get("month"))),
                    "status": m.get("status"),
                    "total_target_revenue_pln": round(total_target, 2),
                    "total_target_budget_ads_pln": round(total_budget, 2),
                    "total_actual_revenue_pln": round(total_actual, 2) if total_actual else 0.0,
                    "revenue_attainment_pct": (round(total_actual * 100.0 / total_target, 1) if total_target else None),
                    "lines": lines,
                    "created_by": m.get("created_by"),
                    "created_at": _to_datetime(m.get("created_at")) or datetime.now(timezone.utc),
                }
            )
        return out
    finally:
        conn.close()


def get_plan_vs_actual(year: int) -> dict[str, Any]:
    plans = list_plan_months(year=year)
    rows: list[dict[str, Any]] = []
    ytd_target = 0.0
    ytd_actual = 0.0
    for m in sorted(plans, key=lambda x: x["month"]):
        lines = m.get("lines") or []
        target_revenue = sum(_to_float(line.get("target_revenue_pln")) for line in lines)
        actual_revenue = sum(_to_float(line.get("actual_revenue_pln")) for line in lines if line.get("actual_revenue_pln") is not None)
        t_cm = (
            sum(_to_float(line.get("target_cm_pct")) for line in lines) / len(lines)
            if lines
            else 0.0
        )
        a_cm = (
            sum(_to_float(line.get("actual_cm_pct")) for line in lines if line.get("actual_cm_pct") is not None) / len(lines)
            if lines
            else 0.0
        )
        t_acos = (
            sum(_to_float(line.get("target_acos_pct")) for line in lines) / len(lines)
            if lines
            else 0.0
        )
        a_acos = (
            sum(_to_float(line.get("actual_acos_pct")) for line in lines if line.get("actual_acos_pct") is not None) / len(lines)
            if lines
            else 0.0
        )
        ytd_target += target_revenue
        ytd_actual += actual_revenue
        rows.append(
            {
                "month_label": m["month_label"],
                "target_revenue_pln": round(target_revenue, 2),
                "actual_revenue_pln": round(actual_revenue, 2),
                "revenue_attainment_pct": round(actual_revenue * 100.0 / target_revenue, 1) if target_revenue else 0.0,
                "target_cm_pct": round(t_cm, 2),
                "actual_cm_pct": round(a_cm, 2),
                "target_acos_pct": round(t_acos, 2),
                "actual_acos_pct": round(a_acos, 2),
            }
        )
    return {
        "rows": rows,
        "ytd_target_revenue_pln": round(ytd_target, 2),
        "ytd_actual_revenue_pln": round(ytd_actual, 2),
        "ytd_attainment_pct": round(ytd_actual * 100.0 / ytd_target, 1) if ytd_target else 0.0,
    }
