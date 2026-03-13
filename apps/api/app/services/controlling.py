"""
Controlling Module — Mapping & Price Change Audit Trail
========================================================
Provides:
  1. Source priority system — prevents low-confidence sources from overwriting
  2. Mapping change log — audit trail for every internal_sku change
  3. Price change log — audit trail for every purchase price change
  4. Stale price detection — finds products with outdated prices
  5. Hook functions to integrate into existing pipelines

Tables created (if not exist):
  - acc_mapping_change_log  — before/after for internal_sku changes
  - acc_price_change_log    — before/after for purchase price changes
"""
from __future__ import annotations

import json
import structlog
from datetime import date, datetime, timezone
from typing import Any

from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# SOURCE PRIORITY — higher number = more trustworthy
# A source can only overwrite another source of EQUAL or LOWER priority.
# ---------------------------------------------------------------------------
SOURCE_PRIORITY: dict[str, int] = {
    "spapi_listing_report": 10,   # auto-match from SP-API listing report
    "sp_api":               10,   # auto-match from SP-API orders
    "ai_match":             20,   # AI-based product matching
    "baselinker":           25,   # Baselinker sync
    "amazon_listing_registry": 30,  # Google Sheet listings
    "import_csv":           35,   # CSV import
    "spapi_ergonode":       40,   # SP-API enriched via Ergonode ASIN
    "cogs_xlsx":            45,   # purchase dept XLSX
    "ergonode_asin":        50,   # Ergonode match by ASIN
    "ergonode":             60,   # Ergonode PIM (master data)
    "manual_dq":            70,   # manual data quality override
    "manual":               70,   # manual override
}

DEFAULT_PRIORITY = 15  # unknown sources get low priority


def get_source_priority(source: str | None) -> int:
    """Return priority level for a mapping_source value."""
    if not source:
        return 0
    return SOURCE_PRIORITY.get(source, DEFAULT_PRIORITY)


def is_overwrite_allowed(
    current_source: str | None,
    new_source: str | None,
    *,
    force: bool = False,
) -> bool:
    """Check if new_source is allowed to overwrite current_source mapping.

    Returns True if:
      - force=True (always allow)
      - current_source is empty/null
      - new_source priority >= current_source priority
    """
    if force:
        return True
    if not current_source:
        return True
    return get_source_priority(new_source) >= get_source_priority(current_source)


# ---------------------------------------------------------------------------
# TABLE SETUP
# ---------------------------------------------------------------------------

def ensure_controlling_tables() -> None:
    """Create controlling tables if they don't exist."""
    conn = connect_acc(autocommit=True)
    cur = conn.cursor()

    cur.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'acc_mapping_change_log'
        )
        CREATE TABLE dbo.acc_mapping_change_log (
            id              INT IDENTITY(1,1) PRIMARY KEY,
            product_id      NVARCHAR(36)   NOT NULL,
            sku             NVARCHAR(128),
            asin            NVARCHAR(20),
            old_internal_sku NVARCHAR(50),
            new_internal_sku NVARCHAR(50),
            old_source      NVARCHAR(50),
            new_source      NVARCHAR(50),
            change_type     NVARCHAR(20)   NOT NULL,   -- 'set', 'update', 'clear', 'blocked'
            reason          NVARCHAR(500),
            created_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
        )
    """)

    cur.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'acc_price_change_log'
        )
        CREATE TABLE dbo.acc_price_change_log (
            id              INT IDENTITY(1,1) PRIMARY KEY,
            internal_sku    NVARCHAR(50)   NOT NULL,
            old_price_pln   DECIMAL(12,4),
            new_price_pln   DECIMAL(12,4)  NOT NULL,
            pct_change      DECIMAL(8,2),
            source          NVARCHAR(50),
            source_document NVARCHAR(300),
            change_type     NVARCHAR(20)   NOT NULL,   -- 'new', 'update', 'correction'
            flagged         BIT            NOT NULL DEFAULT 0,
            flag_reason     NVARCHAR(200),
            created_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
        )
    """)

    # Indexes
    cur.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_mapping_change_sku')
        CREATE INDEX IX_mapping_change_sku
            ON dbo.acc_mapping_change_log (sku, created_at DESC)
    """)
    cur.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_mapping_change_date')
        CREATE INDEX IX_mapping_change_date
            ON dbo.acc_mapping_change_log (created_at DESC)
    """)
    cur.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_price_change_sku')
        CREATE INDEX IX_price_change_sku
            ON dbo.acc_price_change_log (internal_sku, created_at DESC)
    """)
    cur.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_price_change_flagged')
        CREATE INDEX IX_price_change_flagged
            ON dbo.acc_price_change_log (flagged, created_at DESC)
            WHERE flagged = 1
    """)

    conn.close()
    log.info("controlling.tables_ensured")


# ---------------------------------------------------------------------------
# MAPPING CHANGE LOGGING
# ---------------------------------------------------------------------------

def log_mapping_change(
    conn,
    *,
    product_id: str,
    sku: str | None = None,
    asin: str | None = None,
    old_internal_sku: str | None = None,
    new_internal_sku: str | None = None,
    old_source: str | None = None,
    new_source: str | None = None,
    change_type: str = "update",
    reason: str | None = None,
) -> None:
    """Log a mapping change to acc_mapping_change_log.

    Should be called from any code that modifies acc_product.internal_sku.
    Uses the caller's connection (no separate commit).
    """
    if old_internal_sku == new_internal_sku:
        return  # no actual change

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO dbo.acc_mapping_change_log
            (product_id, sku, asin, old_internal_sku, new_internal_sku,
             old_source, new_source, change_type, reason)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        product_id, sku, asin,
        old_internal_sku, new_internal_sku,
        old_source, new_source,
        change_type, reason,
    )
    cur.close()

    log.info(
        "controlling.mapping_changed",
        sku=sku,
        old=old_internal_sku,
        new=new_internal_sku,
        source=new_source,
        type=change_type,
    )


def check_and_log_mapping(
    conn,
    *,
    product_id: str,
    sku: str | None = None,
    asin: str | None = None,
    new_internal_sku: str | None,
    new_source: str | None,
    force: bool = False,
) -> dict[str, Any]:
    """Check if mapping overwrite is allowed, log it, and return result.

    Returns:
        {
            "allowed": bool,
            "change_type": str,  # 'set'|'update'|'blocked'|'no_change'
            "old_internal_sku": str|None,
            "old_source": str|None,
            "reason": str|None,
        }
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT internal_sku, mapping_source FROM acc_product WITH (NOLOCK) WHERE id = ?",
        product_id,
    )
    row = cur.fetchone()
    cur.close()

    if not row:
        return {"allowed": False, "change_type": "blocked",
                "reason": "product not found"}

    old_sku = row[0]
    old_source = row[1]

    # No actual change
    if old_sku == new_internal_sku:
        return {"allowed": True, "change_type": "no_change",
                "old_internal_sku": old_sku, "old_source": old_source}

    # Setting for the first time
    if not old_sku:
        log_mapping_change(
            conn,
            product_id=product_id, sku=sku, asin=asin,
            old_internal_sku=None, new_internal_sku=new_internal_sku,
            old_source=None, new_source=new_source,
            change_type="set",
        )
        return {"allowed": True, "change_type": "set",
                "old_internal_sku": None, "old_source": None}

    # Overwrite check
    if not is_overwrite_allowed(old_source, new_source, force=force):
        reason = (
            f"Source '{new_source}' (priority {get_source_priority(new_source)}) "
            f"cannot overwrite '{old_source}' (priority {get_source_priority(old_source)})"
        )
        log_mapping_change(
            conn,
            product_id=product_id, sku=sku, asin=asin,
            old_internal_sku=old_sku, new_internal_sku=new_internal_sku,
            old_source=old_source, new_source=new_source,
            change_type="blocked",
            reason=reason,
        )
        log.warning(
            "controlling.mapping_blocked",
            sku=sku,
            old=old_sku,
            new=new_internal_sku,
            reason=reason,
        )
        return {"allowed": False, "change_type": "blocked",
                "old_internal_sku": old_sku, "old_source": old_source,
                "reason": reason}

    # Allowed overwrite
    log_mapping_change(
        conn,
        product_id=product_id, sku=sku, asin=asin,
        old_internal_sku=old_sku, new_internal_sku=new_internal_sku,
        old_source=old_source, new_source=new_source,
        change_type="update",
        reason=f"Overwrite by {new_source} (prio {get_source_priority(new_source)})"
               f" replacing {old_source} (prio {get_source_priority(old_source)})",
    )
    return {"allowed": True, "change_type": "update",
            "old_internal_sku": old_sku, "old_source": old_source}


# ---------------------------------------------------------------------------
# PRICE CHANGE LOGGING
# ---------------------------------------------------------------------------

# Thresholds for flagging price changes
PRICE_FLAG_MIN_PLN = 0.10      # below this → suspiciously low
PRICE_FLAG_MAX_PLN = 2000.0    # above this → suspiciously high
PRICE_FLAG_JUMP_PCT = 50.0     # >50% change → flag for review


def log_price_change(
    conn,
    *,
    internal_sku: str,
    old_price: float | None,
    new_price: float,
    source: str | None = None,
    source_document: str | None = None,
) -> dict[str, Any]:
    """Log a purchase price change and flag anomalies.

    Returns:
        {"flagged": bool, "flag_reason": str|None, "pct_change": float|None}
    """
    # Calculate percentage change
    pct_change = None
    if old_price and old_price > 0:
        pct_change = round((new_price - old_price) / old_price * 100, 2)

    # Determine change type
    if old_price is None:
        change_type = "new"
    elif abs(new_price - old_price) < 0.005:
        return {"flagged": False, "flag_reason": None, "pct_change": 0}
    else:
        change_type = "update"

    # Flag anomalies
    flagged = False
    flag_reason = None
    reasons = []

    if new_price < PRICE_FLAG_MIN_PLN:
        reasons.append(f"price {new_price:.2f} PLN below minimum {PRICE_FLAG_MIN_PLN}")
    if new_price > PRICE_FLAG_MAX_PLN:
        reasons.append(f"price {new_price:.2f} PLN above maximum {PRICE_FLAG_MAX_PLN}")
    if pct_change is not None and abs(pct_change) > PRICE_FLAG_JUMP_PCT:
        reasons.append(f"{pct_change:+.1f}% change (threshold: ±{PRICE_FLAG_JUMP_PCT}%)")

    if reasons:
        flagged = True
        flag_reason = "; ".join(reasons)

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO dbo.acc_price_change_log
            (internal_sku, old_price_pln, new_price_pln, pct_change,
             source, source_document, change_type, flagged, flag_reason)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        internal_sku, old_price, new_price, pct_change,
        source, source_document, change_type,
        1 if flagged else 0, flag_reason,
    )
    cur.close()

    if flagged:
        log.warning(
            "controlling.price_flagged",
            internal_sku=internal_sku,
            old=old_price,
            new=new_price,
            reason=flag_reason,
        )

    return {"flagged": flagged, "flag_reason": flag_reason, "pct_change": pct_change}


# ---------------------------------------------------------------------------
# STALE PRICE DETECTION
# ---------------------------------------------------------------------------

def check_stale_prices(*, max_age_days: int = 90) -> dict[str, Any]:
    """Find mapped products with active orders but no price update in max_age_days.

    Returns summary + list of stale SKUs.
    """
    conn = connect_acc(autocommit=True)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            p.internal_sku,
            p.sku,
            p.netto_purchase_price_pln,
            pp.latest_update,
            recent.order_cnt
        FROM acc_product p WITH (NOLOCK)
        INNER JOIN (
            SELECT ol.product_id, COUNT(*) AS order_cnt
            FROM acc_order_line ol WITH (NOLOCK)
            INNER JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            WHERE o.purchase_date >= DATEADD(DAY, -30, GETUTCDATE())
              AND o.status NOT IN ('Cancelled', 'Canceled')
              AND ol.sku NOT LIKE 'amzn.gr.%%'
            GROUP BY ol.product_id
        ) recent ON recent.product_id = p.id
        LEFT JOIN (
            SELECT internal_sku, MAX(updated_at) AS latest_update
            FROM acc_purchase_price WITH (NOLOCK)
            GROUP BY internal_sku
        ) pp ON pp.internal_sku = p.internal_sku
        WHERE p.internal_sku IS NOT NULL
          AND (
              pp.latest_update IS NULL
              OR pp.latest_update < DATEADD(DAY, -?, GETUTCDATE())
          )
        ORDER BY recent.order_cnt DESC
    """, max_age_days)

    stale = []
    for r in cur.fetchall():
        stale.append({
            "internal_sku": str(r[0]),
            "sku": str(r[1]) if r[1] else None,
            "current_price": float(r[2]) if r[2] else None,
            "last_price_update": r[3].isoformat() if r[3] else None,
            "recent_orders_30d": r[4],
        })

    conn.close()

    return {
        "check": "stale_prices",
        "max_age_days": max_age_days,
        "stale_count": len(stale),
        "stale_products": stale[:50],  # top 50 by order volume
        "status": "warning" if len(stale) > 10 else "ok",
        "issues": [f"{len(stale)} products with stale prices (>{max_age_days}d) but recent orders"]
                  if len(stale) > 0 else [],
    }


# ---------------------------------------------------------------------------
# CONTROLLING DASHBOARD — aggregated view
# ---------------------------------------------------------------------------

def get_controlling_summary() -> dict[str, Any]:
    """Return a summary of controlling status for dashboard display."""
    conn = connect_acc(autocommit=True)
    cur = conn.cursor()

    # Recent mapping changes (last 7 days)
    cur.execute("""
        SELECT
            change_type,
            COUNT(*) AS cnt
        FROM acc_mapping_change_log WITH (NOLOCK)
        WHERE created_at >= DATEADD(DAY, -7, GETUTCDATE())
        GROUP BY change_type
    """)
    mapping_changes = {}
    for r in cur.fetchall():
        mapping_changes[r[0]] = r[1]

    # Recent flagged price changes (last 7 days)
    cur.execute("""
        SELECT COUNT(*) AS total_changes,
               SUM(CASE WHEN flagged = 1 THEN 1 ELSE 0 END) AS flagged_changes
        FROM acc_price_change_log WITH (NOLOCK)
        WHERE created_at >= DATEADD(DAY, -7, GETUTCDATE())
    """)
    pr = cur.fetchone()
    price_changes_7d = pr[0] or 0
    flagged_prices_7d = pr[1] or 0

    # Blocked mapping overwrites (last 7 days)
    cur.execute("""
        SELECT TOP 10 sku, old_internal_sku, new_internal_sku,
               old_source, new_source, reason, created_at
        FROM acc_mapping_change_log WITH (NOLOCK)
        WHERE change_type = 'blocked'
          AND created_at >= DATEADD(DAY, -7, GETUTCDATE())
        ORDER BY created_at DESC
    """)
    blocked = []
    for r in cur.fetchall():
        blocked.append({
            "sku": str(r[0]) if r[0] else None,
            "old_internal_sku": str(r[1]) if r[1] else None,
            "new_internal_sku": str(r[2]) if r[2] else None,
            "old_source": str(r[3]) if r[3] else None,
            "new_source": str(r[4]) if r[4] else None,
            "reason": str(r[5]) if r[5] else None,
            "at": r[6].isoformat() if r[6] else None,
        })

    # Flagged price changes (last 7 days)
    cur.execute("""
        SELECT TOP 10 internal_sku, old_price_pln, new_price_pln,
               pct_change, source, flag_reason, created_at
        FROM acc_price_change_log WITH (NOLOCK)
        WHERE flagged = 1
          AND created_at >= DATEADD(DAY, -7, GETUTCDATE())
        ORDER BY created_at DESC
    """)
    flagged_list = []
    for r in cur.fetchall():
        flagged_list.append({
            "internal_sku": str(r[0]),
            "old_price": float(r[1]) if r[1] else None,
            "new_price": float(r[2]) if r[2] else None,
            "pct_change": float(r[3]) if r[3] else None,
            "source": str(r[4]) if r[4] else None,
            "flag_reason": str(r[5]) if r[5] else None,
            "at": r[6].isoformat() if r[6] else None,
        })

    conn.close()

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "mapping_changes_7d": mapping_changes,
        "total_mapping_changes_7d": sum(mapping_changes.values()),
        "blocked_overwrites_7d": len(blocked),
        "blocked_details": blocked,
        "price_changes_7d": price_changes_7d,
        "flagged_prices_7d": flagged_prices_7d,
        "flagged_details": flagged_list,
    }


def get_mapping_history(
    sku: str | None = None,
    product_id: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Get mapping change history for a specific SKU or product."""
    conn = connect_acc(autocommit=True)
    cur = conn.cursor()

    if sku:
        cur.execute("""
            SELECT TOP (?) id, product_id, sku, asin,
                   old_internal_sku, new_internal_sku,
                   old_source, new_source, change_type, reason, created_at
            FROM acc_mapping_change_log WITH (NOLOCK)
            WHERE sku = ?
            ORDER BY created_at DESC
        """, limit, sku)
    elif product_id:
        cur.execute("""
            SELECT TOP (?) id, product_id, sku, asin,
                   old_internal_sku, new_internal_sku,
                   old_source, new_source, change_type, reason, created_at
            FROM acc_mapping_change_log WITH (NOLOCK)
            WHERE product_id = ?
            ORDER BY created_at DESC
        """, limit, product_id)
    else:
        cur.execute("""
            SELECT TOP (?) id, product_id, sku, asin,
                   old_internal_sku, new_internal_sku,
                   old_source, new_source, change_type, reason, created_at
            FROM acc_mapping_change_log WITH (NOLOCK)
            ORDER BY created_at DESC
        """, limit)

    results = []
    for r in cur.fetchall():
        results.append({
            "id": r[0],
            "product_id": str(r[1]),
            "sku": str(r[2]) if r[2] else None,
            "asin": str(r[3]) if r[3] else None,
            "old_internal_sku": str(r[4]) if r[4] else None,
            "new_internal_sku": str(r[5]) if r[5] else None,
            "old_source": str(r[6]) if r[6] else None,
            "new_source": str(r[7]) if r[7] else None,
            "change_type": str(r[8]),
            "reason": str(r[9]) if r[9] else None,
            "at": r[10].isoformat() if r[10] else None,
        })

    conn.close()
    return results


def get_price_history(
    internal_sku: str | None = None,
    flagged_only: bool = False,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Get price change history, optionally filtered."""
    conn = connect_acc(autocommit=True)
    cur = conn.cursor()

    where_parts = ["1=1"]
    params: list[Any] = [limit]

    if internal_sku:
        where_parts.append("internal_sku = ?")
        params.append(internal_sku)
    if flagged_only:
        where_parts.append("flagged = 1")

    where_sql = " AND ".join(where_parts)
    cur.execute(f"""
        SELECT TOP (?) id, internal_sku, old_price_pln, new_price_pln,
               pct_change, source, source_document, change_type,
               flagged, flag_reason, created_at
        FROM acc_price_change_log WITH (NOLOCK)
        WHERE {where_sql}
        ORDER BY created_at DESC
    """, *params)

    results = []
    for r in cur.fetchall():
        results.append({
            "id": r[0],
            "internal_sku": str(r[1]),
            "old_price": float(r[2]) if r[2] else None,
            "new_price": float(r[3]) if r[3] else None,
            "pct_change": float(r[4]) if r[4] else None,
            "source": str(r[5]) if r[5] else None,
            "source_document": str(r[6]) if r[6] else None,
            "change_type": str(r[7]),
            "flagged": bool(r[8]),
            "flag_reason": str(r[9]) if r[9] else None,
            "at": r[10].isoformat() if r[10] else None,
        })

    conn.close()
    return results
