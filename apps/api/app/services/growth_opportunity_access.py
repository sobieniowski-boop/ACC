"""Unified Growth-Opportunity Access Layer.

Single entry-point for all writes to the ``growth_opportunity`` table.
Eliminates the previous split between ``strategy_service`` (growth_opportunity)
and ``executive_service`` (executive_opportunities) by routing every detection
engine through one INSERT helper and one deactivation helper.

Read helpers provide filtered/paginated queries used by executive and
strategy API layers.

Sprint 8 – S8.1
"""
from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import structlog

log = structlog.get_logger(__name__)

# ── Priority mapping from P-labels to numeric scores ───────────────
_PRIORITY_MAP: Dict[str, float] = {"P1": 90.0, "P2": 70.0, "P3": 50.0}


def _f(v: Any) -> float:
    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    return float(v)


# ═══════════════════════════════════════════════════════════════════
#  WRITE HELPERS
# ═══════════════════════════════════════════════════════════════════

def insert_opportunity(
    cur,
    *,
    opportunity_type: str,
    marketplace_id: str | None = None,
    sku: str | None = None,
    asin: str | None = None,
    parent_asin: str | None = None,
    family_id: int | None = None,
    title: str,
    description: str | None = None,
    root_cause: str | None = None,
    recommendation: str | None = None,
    priority_score: float = 0,
    confidence_score: float = 50,
    revenue_uplift: float | None = None,
    profit_uplift: float | None = None,
    margin_uplift: float | None = None,
    units_uplift: int | None = None,
    effort: float | None = None,
    owner_role: str | None = None,
    blockers: list | None = None,
    signals: dict | None = None,
) -> None:
    """Insert a single opportunity into ``growth_opportunity``.

    This is the **only** sanctioned write path for opportunity data.
    Both strategy-service detection and executive-service risk/growth
    detection must funnel through here.
    """
    cur.execute(
        """INSERT INTO growth_opportunity
           (opportunity_type, marketplace_id, sku, asin, parent_asin, family_id,
            title, description, root_cause, recommendation,
            priority_score, confidence_score,
            estimated_revenue_uplift, estimated_profit_uplift,
            estimated_margin_uplift, estimated_units_uplift,
            effort_score, owner_role, blocker_json, source_signals_json,
            status, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'new',SYSUTCDATETIME(),SYSUTCDATETIME())""",
        (
            opportunity_type, marketplace_id, sku, asin, parent_asin, family_id,
            title, description, root_cause, recommendation,
            priority_score, confidence_score,
            revenue_uplift, profit_uplift, margin_uplift, units_uplift,
            effort, owner_role,
            json.dumps(blockers) if blockers else None,
            json.dumps(signals) if signals else None,
        ),
    )


def deactivate_by_types(
    cur,
    opportunity_types: List[str],
    *,
    status_from: str = "new",
    status_to: str = "rejected",
) -> int:
    """Mark old opportunities of the given types as *status_to*.

    Returns the number of rows affected.
    """
    if not opportunity_types:
        return 0
    placeholders = ",".join(["?"] * len(opportunity_types))
    cur.execute(
        f"""UPDATE growth_opportunity
            SET status = ?, updated_at = SYSUTCDATETIME()
            WHERE status = ?
              AND opportunity_type IN ({placeholders})""",
        (status_to, status_from, *opportunity_types),
    )
    return cur.rowcount


# ═══════════════════════════════════════════════════════════════════
#  READ HELPERS
# ═══════════════════════════════════════════════════════════════════

def query_active(
    cur,
    *,
    opportunity_types: List[str] | None = None,
    marketplace_id: str | None = None,
    limit: int = 20,
    order: str = "priority_score DESC",
) -> List[Dict[str, Any]]:
    """Return active opportunities (status new/in_review/accepted) as dicts."""
    clauses = ["status IN ('new','in_review','accepted')"]
    params: list = []

    if opportunity_types:
        placeholders = ",".join(["?"] * len(opportunity_types))
        clauses.append(f"opportunity_type IN ({placeholders})")
        params.extend(opportunity_types)

    if marketplace_id:
        clauses.append("marketplace_id = ?")
        params.append(marketplace_id)

    where = " AND ".join(clauses)
    # TOP (?) is the first parameter, followed by WHERE-clause params
    all_params = [limit] + params

    cur.execute(
        f"""SELECT TOP (?)
                id, opportunity_type, marketplace_id, sku, asin,
                title, description,
                priority_score, confidence_score,
                estimated_revenue_uplift, estimated_profit_uplift,
                created_at
            FROM growth_opportunity
            WHERE {where}
            ORDER BY {order}""",
        tuple(all_params),
    )

    from app.core.config import MARKETPLACE_REGISTRY

    rows: list[dict] = []
    for r in cur.fetchall():
        mkt = r[2]
        rows.append({
            "id": r[0],
            "opportunity_type": r[1],
            "marketplace_id": mkt,
            "marketplace_code": MARKETPLACE_REGISTRY.get(mkt, {}).get("code", mkt[-2:] if mkt else ""),
            "sku": r[3],
            "asin": r[4],
            "title": r[5],
            "description": r[6],
            "priority_score": _f(r[7]),
            "confidence_score": _f(r[8]),
            "impact_estimate": _f(r[9]) if r[9] is not None else _f(r[10]),
            "is_active": True,
            "created_at": r[11].isoformat() if r[11] else None,
        })
    return rows


def priority_from_label(label: str) -> float:
    """Convert P1/P2/P3 label → numeric priority score."""
    return _PRIORITY_MAP.get(label, 50.0)
