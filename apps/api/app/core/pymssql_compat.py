"""pymssql compatibility layer — drop-in replacement for pyodbc connections.

Azure SQL requires TLS 1.2+ which the old "SQL Server" ODBC driver doesn't
support. pymssql uses FreeTDS natively and handles TLS 1.2 fine.

This module wraps pymssql connections and cursors so they behave identically
to pyodbc — including `?` parameter placeholders, autocommit kwarg, timeouts,
and the cursor open/close pattern used throughout the codebase.

Usage:
    from app.core.pymssql_compat import connect

    # Drop-in replacement for: pyodbc.connect(conn_str, autocommit=False)
    conn = connect(server, port, database, user, password, autocommit=False)
    cur = conn.cursor()
    cur.execute("SELECT * FROM acc_order WHERE amazon_order_id = ?", ["111-111"])
    rows = cur.fetchall()
    cur.close()
    conn.commit()
    conn.close()
"""
from __future__ import annotations

import re
from typing import Any, Sequence

import pymssql


class CompatCursor:
    """Wraps pymssql cursor to accept ``?`` parameter placeholders (pyodbc-style)."""

    def __init__(self, cursor: pymssql.Cursor) -> None:  # type: ignore[name-defined]
        self._cursor = cursor

    # --- pyodbc-compatible properties ---

    @property
    def description(self):
        return self._cursor.description

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    # --- execute / executemany ---

    @staticmethod
    def _convert_sql(sql: str) -> str:
        """Convert ``?`` parameter markers to ``%s`` (pymssql style).

        Strategy:
        1. Escape all existing ``%`` as ``%%`` so pymssql doesn't interpret them
           as format specs (e.g. ``LIKE '%test%'`` → ``LIKE '%%test%%'``).
        2. Replace ``?`` with ``%s``.

        This is safe because our codebase never uses ``%s`` as a literal in SQL
        and ``?`` outside of string literals is always a parameter marker.
        """
        sql = sql.replace("%", "%%")
        sql = sql.replace("?", "%s")
        return sql

    def execute(self, sql: str, *args: Any, **kwargs: Any) -> "CompatCursor":
        """Execute SQL with pyodbc-compatible calling conventions.

        pyodbc supports TWO patterns:
          1. cur.execute(sql, [p1, p2, p3])        — params as single sequence
          2. cur.execute(sql, p1, p2, p3)           — params as *args

        This method normalises both into a tuple for pymssql.
        """
        converted = self._convert_sql(sql)

        # Determine params from the various calling patterns
        params: tuple[Any, ...] | None = None
        if args:
            if len(args) == 1 and isinstance(args[0], (list, tuple)):
                # Pattern 1: execute(sql, [p1, p2]) or execute(sql, (p1, p2))
                params = tuple(args[0])
            else:
                # Pattern 2: execute(sql, p1, p2, p3, ...)
                params = args

        if params is not None:
            self._cursor.execute(converted, params)
        else:
            # No params — but sql might contain %% that should be left alone
            # When no params, we must NOT pass through %-formatting
            # pymssql.execute(sql) without params doesn't do %-formatting
            self._cursor.execute(converted.replace("%%", "%"))
        return self

    def executemany(self, sql: str, params_seq: Sequence[Sequence[Any]]) -> "CompatCursor":
        converted = self._convert_sql(sql)
        self._cursor.executemany(converted, [tuple(p) for p in params_seq])
        return self

    # --- fetch ---

    def fetchall(self) -> list[tuple]:
        return self._cursor.fetchall()

    def fetchone(self) -> tuple | None:
        return self._cursor.fetchone()

    def fetchmany(self, size: int = 1) -> list[tuple]:
        return self._cursor.fetchmany(size)

    # --- lifecycle ---

    def close(self) -> None:
        self._cursor.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __iter__(self):
        return iter(self._cursor)


class CompatConnection:
    """Wraps pymssql connection to match pyodbc.Connection API."""

    def __init__(self, conn: pymssql.Connection, autocommit: bool = False) -> None:  # type: ignore[name-defined]
        self._conn = conn
        self._autocommit = autocommit
        if autocommit:
            self._conn.autocommit(True)

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self._autocommit = value
        self._conn.autocommit(value)

    @property
    def timeout(self) -> int:
        """Read-only — pymssql doesn't expose query timeout this way."""
        return 0

    @timeout.setter
    def timeout(self, value: int) -> None:
        """No-op — pymssql handles timeout at connect-time only."""
        pass

    def cursor(self) -> CompatCursor:
        return CompatCursor(self._conn.cursor())

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def connect(
    server: str,
    port: int | str,
    database: str,
    user: str,
    password: str,
    *,
    autocommit: bool = False,
    timeout: int = 20,
) -> CompatConnection:
    """Create pymssql connection with pyodbc-compatible wrapper.

    Parameters match individual components (not a connection string) because
    pymssql doesn't use ODBC connection strings.
    """
    # Keep a safe floor (120s), but allow callers to request longer query timeouts.
    query_timeout = max(120, int(timeout or 120))
    conn = pymssql.connect(
        server=server,
        port=int(port),
        database=database,
        user=user,
        password=password,
        login_timeout=timeout,
        timeout=query_timeout,
        tds_version="7.3",
        charset="UTF-8",
        appname="ACC",
    )
    return CompatConnection(conn, autocommit=autocommit)


# ---------------------------------------------------------------------------
# Error types — re-export pymssql errors so callers can catch them
# alongside pyodbc.Error without importing both modules.
# ---------------------------------------------------------------------------
Error = pymssql.Error
OperationalError = pymssql.OperationalError
InterfaceError = pymssql.InterfaceError
DatabaseError = pymssql.DatabaseError
