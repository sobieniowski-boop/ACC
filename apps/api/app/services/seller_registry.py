"""Seller registry — loads known Amazon sellers from production DB."""
from __future__ import annotations

from typing import Any

import structlog
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

_SELLERS_CACHE: list[dict[str, Any]] | None = None


def _load_sellers() -> list[dict[str, Any]]:
    """Load sellers from acc_ads_profile (unique account entries)."""
    global _SELLERS_CACHE
    if _SELLERS_CACHE is not None:
        return _SELLERS_CACHE
    try:
        conn = connect_acc(timeout=10)
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT account_id, account_name, account_type,
                   marketplace_id, country_code
            FROM dbo.acc_ads_profile WITH (NOLOCK)
            WHERE account_id IS NOT NULL
        """)
        rows = cur.fetchall()
        conn.close()
        _SELLERS_CACHE = [
            {
                "id": r[0],
                "name": r[1],
                "type": r[2],
                "marketplace_id": r[3],
                "country_code": r[4],
            }
            for r in rows
        ]
    except Exception as exc:
        log.warning("seller_registry.load_failed", error=str(exc))
        _SELLERS_CACHE = []
    return _SELLERS_CACHE


def seller_dict() -> dict[str, dict[str, Any]]:
    """Return sellers indexed by ID."""
    return {s.get("id", ""): s for s in _load_sellers()}
