"""Shared database utilities — single source of truth.

Consolidates _connect(), _fetchall_dict(), _f(), _i(), _mkt_code() that were
duplicated across 50+ modules into one canonical implementation.

Usage:
    from app.platform.shared.db import connect, fetchall_dict, f, i, mkt_code
"""
from __future__ import annotations

from typing import Any

import structlog

from app.core.config import MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

# ── Connection ──────────────────────────────────────────────────────────────

def connect(*, autocommit: bool = False, timeout: int = 20):
    """Return a pyodbc/pymssql connection to the ACC database.

    Standard timeout is 20s.  Override for long-running batch operations.
    """
    return connect_acc(autocommit=autocommit, timeout=timeout)


# ── Cursor → dict ───────────────────────────────────────────────────────────

def fetchall_dict(cur, rows=None) -> list[dict[str, Any]]:
    """Convert a DB-API cursor result into a list of dicts.

    If *rows* is passed (pre-fetched), those are used instead of cur.fetchall().
    """
    cols = [c[0] for c in cur.description] if cur.description else []
    data = rows if rows is not None else cur.fetchall()
    return [{cols[idx]: row[idx] for idx in range(len(cols))} for row in data]


# ── Safe numeric coercion ───────────────────────────────────────────────────

def f(v: Any, default: float = 0.0, *, field: str | None = None, precision: int = 2) -> float:
    """Coerce *v* to float, returning *default* on None/error.

    If *field* is given and *v* is None, a structured warning is emitted to
    help detect missing financial data during profit calculation.
    """
    if v is None:
        if field:
            log.warning("shared.db.null_coercion", field=field,
                        msg=f"NULL financial value coerced to {default}")
        return default
    try:
        return round(float(v), precision)
    except (ValueError, TypeError):
        return default


def i(v: Any, default: int = 0) -> int:
    """Coerce *v* to int, returning *default* on None/error."""
    if v is None:
        return default
    try:
        return int(v)
    except (ValueError, TypeError):
        return default


# ── Marketplace helpers ─────────────────────────────────────────────────────

def mkt_code(marketplace_id: str | None) -> str:
    """Return 2-letter marketplace code (e.g. 'DE', 'FR') from registry."""
    if not marketplace_id:
        return ""
    info = MARKETPLACE_REGISTRY.get(marketplace_id)
    return info["code"] if info else marketplace_id[:5]
