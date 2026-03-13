"""Centralized database connection factory.

Provides two connection functions:
- ``connect_acc()``    → ACC's own database (Azure SQL via pymssql, or local via pyodbc)
- ``connect_netfox()`` → Netfox ERP database (always pyodbc, old driver OK)

Every module that needs a DB connection should import from here instead of
calling pyodbc.connect() or pymssql.connect() directly.

Safety:
- Every connection gets ``SET LOCK_TIMEOUT 30000`` (30 s anti-deadlock).
- ``connect_netfox()`` connections are READ-ONLY by convention.
"""
from __future__ import annotations

import pyodbc
import structlog

from app.core.config import settings

log = structlog.get_logger(__name__)


def _append_app_name(conn_str: str, app_name: str) -> str:
    text = str(conn_str or "")
    upper = text.upper()
    if "APP=" in upper or "APPLICATION NAME=" in upper:
        return text
    if text and not text.endswith(";"):
        text += ";"
    return text + f"APP={app_name};"


def connect_acc(
    *,
    autocommit: bool = False,
    timeout: int = 20,
    isolation_level: str | None = None,
) -> "pyodbc.Connection | CompatConnection":
    """Open connection to ACC's own database (acc_* tables, full read+write).

    Automatically uses pymssql for Azure SQL (TLS 1.2) or pyodbc for local MSSQL.
    Returns a pyodbc-compatible connection object in both cases.

    Parameters:
        isolation_level: If provided, executes
            ``SET TRANSACTION ISOLATION LEVEL <level>`` immediately.
            Allowed values: ``READ COMMITTED``, ``READ UNCOMMITTED``,
            ``REPEATABLE READ``, ``SERIALIZABLE``, ``SNAPSHOT``.
    """
    _ALLOWED_ISOLATION = {
        "READ COMMITTED",
        "READ UNCOMMITTED",
        "REPEATABLE READ",
        "SERIALIZABLE",
        "SNAPSHOT",
    }
    if isolation_level and isolation_level.upper() not in _ALLOWED_ISOLATION:
        raise ValueError(f"Invalid isolation_level: {isolation_level!r}")

    if not settings.mssql_enabled:
        raise RuntimeError("MSSQL not configured — set MSSQL_USER + MSSQL_PASSWORD in .env")

    if settings.use_pymssql:
        # Azure SQL → pymssql (TLS 1.2 compatible, no ODBC driver needed)
        from app.core.pymssql_compat import connect as pymssql_connect
        log.debug("db.connect_acc.pymssql", server=settings.MSSQL_SERVER)
        conn = pymssql_connect(
            server=settings.MSSQL_SERVER,
            port=settings.MSSQL_PORT,
            database=settings.MSSQL_DATABASE,
            user=settings.MSSQL_USER,
            password=settings.MSSQL_PASSWORD,
            autocommit=autocommit,
            timeout=timeout,
        )
    else:
        # Local/on-prem MSSQL → pyodbc (old "SQL Server" driver works fine)
        log.debug("db.connect_acc.pyodbc", server=settings.MSSQL_SERVER)
        conn = pyodbc.connect(
            settings.mssql_connection_string,
            timeout=timeout,
            autocommit=autocommit,
        )

    # Safety: SET LOCK_TIMEOUT on every connection
    cur = conn.cursor()
    cur.execute("SET LOCK_TIMEOUT 30000")
    if isolation_level:
        cur.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level.upper()}")
    cur.close()
    if not autocommit:
        conn.commit()

    return conn


def connect_netfox(
    *,
    database: str | None = None,
    autocommit: bool = True,
    timeout: int = 15,
) -> pyodbc.Connection:
    """Open READ-ONLY connection to Netfox ERP (pyodbc, old driver).

    Parameters:
        database: Override database name (e.g. 'NetfoxDistribution' for Subiekt GT).
                  Default: NETFOX_MSSQL_DATABASE or MSSQL_DATABASE.
        autocommit: Default True (read-only, no transactions needed).

    Safety:
        - Uses NETFOX_MSSQL_* config if set, falls back to MSSQL_* for backward compat.
        - SET LOCK_TIMEOUT 30000 on every connection.
        - NEVER run INSERT/UPDATE/DELETE on this connection!
    """
    if not settings.netfox_enabled:
        raise RuntimeError("Netfox ERP not configured — set NETFOX_MSSQL_* or MSSQL_* in .env")

    if database:
        # Build custom connection string with overridden database
        server = settings.NETFOX_MSSQL_SERVER or settings.MSSQL_SERVER
        port = settings.NETFOX_MSSQL_PORT or settings.MSSQL_PORT
        user = settings.NETFOX_MSSQL_USER or settings.MSSQL_USER
        pwd = settings.NETFOX_MSSQL_PASSWORD or settings.MSSQL_PASSWORD
        driver = settings._odbc_driver
        trust = "TrustServerCertificate=yes;" if "18" in driver or "17" in driver else ""
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={pwd};"
            f"{trust}"
        )
    else:
        conn_str = settings.netfox_connection_string

    log.debug("db.connect_netfox", database=database or "default")
    conn_str = _append_app_name(conn_str, "ACC-Netfox-RO")
    conn = pyodbc.connect(conn_str, timeout=timeout, autocommit=autocommit)

    # Safety: keep Netfox sessions low-impact and easier to trace.
    cur = conn.cursor()
    cur.execute("SET LOCK_TIMEOUT 10000")
    cur.execute("SET DEADLOCK_PRIORITY LOW")
    cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
    cur.close()
    if not autocommit:
        conn.commit()

    return conn
