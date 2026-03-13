"""Profit Engine — shared helpers.

Extracted from the monolithic profit_engine.py during Sprint 3.
Provides utility functions, caching, and SQL fragment builders used
by all other profit sub-modules.
"""
from __future__ import annotations

import re
import threading
import time
from typing import Any

import pyodbc
import structlog

from app.core.config import MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Thread-local warnings collector
# ---------------------------------------------------------------------------
_thread_warnings = threading.local()


def _warnings_reset() -> None:
    _thread_warnings.items = []


def _warnings_append(msg: str) -> None:
    items = getattr(_thread_warnings, "items", None)
    if items is None:
        _thread_warnings.items = [msg]
    else:
        items.append(msg)


def _warnings_collect() -> list[str]:
    items = getattr(_thread_warnings, "items", None) or []
    _thread_warnings.items = []
    return items


# ---------------------------------------------------------------------------
# SKU filter constant
# ---------------------------------------------------------------------------
RENEWED_SKU_FILTER = "ol.sku NOT LIKE 'amzn.gr.%%'"


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def _connect():
    """Connect to ACC database (acc_* tables)."""
    return connect_acc(autocommit=False, timeout=20)


def _fetchall_dict(cur: pyodbc.Cursor) -> list[dict[str, Any]]:
    cols = [c[0] for c in cur.description] if cur.description else []
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]


def _f(v: Any, default: float = 0.0, *, field: str | None = None) -> float:
    if v is None:
        if field:
            log.warning("profit_engine._f_null_coercion", field=field,
                        msg=f"NULL financial value coerced to {default}")
        return default
    try:
        return round(float(v), 2)
    except (ValueError, TypeError):
        return default


def _f_strict(v: Any, field: str) -> float:
    if v is None:
        raise ValueError(f"NULL value for required financial field: {field}")
    try:
        return round(float(v), 2)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Cannot convert {v!r} to float for field {field}") from exc


def _i(v: Any, default: int = 0) -> int:
    if v is None:
        return default
    try:
        return int(v)
    except (ValueError, TypeError):
        return default


def _mkt_code(marketplace_id: str | None) -> str:
    if not marketplace_id:
        return ""
    info = MARKETPLACE_REGISTRY.get(marketplace_id)
    return info["code"] if info else marketplace_id[:5]


def _norm_text(v: Any) -> str:
    return str(v or "").replace("\n", " ").replace("\r", " ").strip()


def _norm_internal_sku(v: Any) -> str:
    txt = _norm_text(v)
    if txt.endswith(".0"):
        txt = txt[:-2]
    return txt


# ---------------------------------------------------------------------------
# SQL fragment builders
# ---------------------------------------------------------------------------

def _cm1_direct_order_fee_total_sql(order_alias: str = "o") -> str:
    return (
        f"(ISNULL({order_alias}.shipping_surcharge_pln, 0)"
        f" + ISNULL({order_alias}.promo_order_fee_pln, 0)"
        f" + ISNULL({order_alias}.refund_commission_pln, 0))"
    )


def _cm1_direct_order_fee_alloc_sql(order_alias: str, line_share_sql: str) -> str:
    return f"({_cm1_direct_order_fee_total_sql(order_alias)} * ({line_share_sql}))"


def _parse_csv_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [part.strip() for part in str(raw).split(",") if part and part.strip()]


def _parse_search_tokens(raw: str | None, *, limit: int = 12) -> list[str]:
    if not raw:
        return []
    tokens = [t.strip() for t in re.split(r"[\s,;]+", str(raw)) if t and t.strip()]
    return tokens[: max(1, int(limit))]


# ---------------------------------------------------------------------------
# In-memory result cache
# ---------------------------------------------------------------------------
_RESULT_CACHE: dict[str, tuple[float, Any]] = {}
_RESULT_CACHE_TTL = 180  # 3 minutes
_RESULT_CACHE_MAX = 50


def _result_cache_get(key: str) -> Any | None:
    entry = _RESULT_CACHE.get(key)
    if entry is None:
        return None
    expires_at, value = entry
    if time.monotonic() > expires_at:
        _RESULT_CACHE.pop(key, None)
        return None
    return value


def _result_cache_set(key: str, value: Any, ttl: int | None = None) -> None:
    effective_ttl = ttl if ttl is not None else _RESULT_CACHE_TTL
    if len(_RESULT_CACHE) > _RESULT_CACHE_MAX:
        now = time.monotonic()
        expired = [k for k, (exp, _) in _RESULT_CACHE.items() if now > exp]
        for k in expired:
            _RESULT_CACHE.pop(k, None)
    _RESULT_CACHE[key] = (time.monotonic() + effective_ttl, value)


def _result_cache_invalidate(prefix: str | None = None) -> None:
    if prefix is None:
        _RESULT_CACHE.clear()
        return
    for key in list(_RESULT_CACHE.keys()):
        if key.startswith(prefix):
            _RESULT_CACHE.pop(key, None)
