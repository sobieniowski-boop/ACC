"""
Return Tracker Module — Service Layer.

Tracks the full lifecycle of Amazon returns:
  1. Refund event detected (from acc_order.is_refund = 1)
  2. FBA Customer Returns report synced (physical return data)
  3. Financial classification:
     - sellable_return  → COGS recovered (WZ reversal)
     - damaged_return   → COGS write-off
     - pending          → awaiting physical return
     - lost_in_transit  → never arrived  
     - reimbursed       → Amazon reimbursed

P&L integration:
  Revenue:  always deducted by refund_amount_pln
  COGS:     recovered only if item returned sellable (inventory re-entry)
  Loss:     COGS written off if damaged/defective/lost
"""
from __future__ import annotations

import csv
import io
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import structlog

from app.core.config import settings, MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

# ──────────────────────── Constants ────────────────────────

# Amazon FBA disposition codes → financial status mapping
DISPOSITION_MAP: dict[str, str] = {
    "SELLABLE":            "sellable_return",
    "DAMAGED":             "damaged_return",
    "DEFECTIVE":           "damaged_return",
    "CUSTOMER_DAMAGED":    "damaged_return",
    "CARRIER_DAMAGED":     "damaged_return",
    "EXPIRED":             "damaged_return",
    "DISTRIBUTOR_DAMAGED": "damaged_return",
    "WAREHOUSE_DAMAGED":   "damaged_return",
}

# EU marketplace IDs that have FBA returns reports
EU_FBA_MARKETPLACES = [
    mid for mid, info in MARKETPLACE_REGISTRY.items()
    if info["code"] in ("DE", "FR", "IT", "ES", "NL", "PL", "SE", "BE", "GB")
]


def _connect():
    """Get DB connection."""
    return connect_acc(autocommit=False, timeout=20)


def _dictrow(cur) -> list[dict]:
    cols = [d[0] for d in cur.description]
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]


def _f(v, default=0.0) -> float:
    try:
        return round(float(v), 4) if v is not None else default
    except (TypeError, ValueError):
        return default


def _mkt_code(marketplace_id: str | None) -> str:
    if not marketplace_id:
        return ""
    info = MARKETPLACE_REGISTRY.get(marketplace_id)
    return info["code"] if info else marketplace_id[:5]


# ──────────────────── 1. Refund → Return Item Seeding ──────────────────────

def seed_return_items_from_orders(
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, int]:
    """
    Scan acc_order for refunds and seed acc_return_item for any
    that don't yet exist. This is idempotent — only inserts new rows.

    Returns: {"scanned": N, "inserted": N, "skipped": N}
    """
    if date_from is None:
        date_from = date.today() - timedelta(days=90)
    if date_to is None:
        date_to = date.today()

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 30000")

        # Single INSERT...SELECT — much faster than row-by-row for 5K+ rows
        cur.execute("""
            INSERT INTO dbo.acc_return_item (
                amazon_order_id, order_line_id, order_id,
                marketplace_id, sku, asin,
                refund_date, refund_type, refund_amount_pln,
                quantity, financial_status,
                cogs_pln, cogs_recovered_pln, write_off_pln,
                source
            )
            SELECT
                o.amazon_order_id,
                CAST(ol.id AS NVARCHAR(50)),
                CAST(o.id AS NVARCHAR(50)),
                o.marketplace_id,
                ol.sku,
                ol.asin,
                o.refund_date,
                ISNULL(o.refund_type, 'unknown'),
                ISNULL(o.refund_amount_pln, 0)
                    * ISNULL(ol.item_price, 0)
                    / NULLIF((
                        SELECT SUM(ISNULL(ol2.item_price, 0))
                        FROM dbo.acc_order_line ol2 WITH (NOLOCK)
                        WHERE ol2.order_id = o.id
                    ), 0),
                ISNULL(ol.quantity_ordered, 1),
                'pending',
                ISNULL(ol.cogs_pln, 0),
                0, 0,
                'auto'
            FROM dbo.acc_order o WITH (NOLOCK)
            JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
            WHERE o.is_refund = 1
              AND o.purchase_date >= CAST(? AS DATE)
              AND o.purchase_date < DATEADD(day, 1, CAST(? AS DATE))
              AND NOT EXISTS (
                  SELECT 1 FROM dbo.acc_return_item ri WITH (NOLOCK)
                  WHERE ri.amazon_order_id = o.amazon_order_id
                    AND ri.sku = ol.sku
              )
        """, [date_from.isoformat(), date_to.isoformat()])

        inserted = cur.rowcount or 0
        conn.commit()
        log.info("return_tracker.seed_complete", inserted=inserted)
        return {"scanned": inserted, "inserted": inserted, "skipped": 0}

    finally:
        conn.close()


# ──────────────────── 2. FBA Returns Report Sync ──────────────────────

def parse_fba_returns_report(content: str, marketplace_id: str) -> list[dict]:
    """
    Parse GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA TSV/CSV content.

    Amazon report columns:
      return-date, order-id, sku, asin, fnsku, product-name, quantity,
      fulfillment-center-id, detailed-disposition, reason, status,
      license-plate-number, customer-comments
    """
    if content.startswith("\ufeff"):
        content = content[1:]

    first_line = next((line for line in content.splitlines() if line.strip()), "")
    delimiter = "\t" if first_line.count("\t") >= first_line.count(",") else ","

    reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
    rows = []
    for row in reader:
        clean = {k.strip().lower().replace("-", "_"): (v.strip() if v else "") for k, v in row.items() if k}
        clean["marketplace_id"] = marketplace_id
        rows.append(clean)

    log.info("return_tracker.parsed_report", marketplace=_mkt_code(marketplace_id), rows=len(rows))
    return rows


def upsert_fba_returns(rows: list[dict]) -> dict[str, int]:
    """
    Upsert parsed FBA returns into acc_fba_customer_return.
    Returns {"inserted": N, "updated": N, "errors": N}
    """
    if not rows:
        return {"inserted": 0, "updated": 0, "errors": 0}

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 30000")

        inserted = 0
        updated = 0
        errors = 0

        for r in rows:
            return_date = r.get("return_date") or r.get("return date") or ""
            order_id = r.get("order_id") or r.get("order id") or ""
            sku = r.get("sku") or ""
            if not order_id or not sku:
                errors += 1
                continue

            try:
                cur.execute("""
                    MERGE dbo.acc_fba_customer_return AS tgt
                    USING (SELECT ? AS order_id, ? AS sku, ? AS return_date,
                                  ? AS fulfillment_center_id, ? AS quantity) AS src
                    ON tgt.order_id = src.order_id
                       AND tgt.sku = src.sku
                       AND tgt.return_date = src.return_date
                       AND ISNULL(tgt.fulfillment_center_id, '') = ISNULL(src.fulfillment_center_id, '')
                       AND tgt.quantity = src.quantity
                    WHEN MATCHED THEN UPDATE SET
                        detailed_disposition = ?,
                        reason = ?,
                        status = ?,
                        customer_comments = ?,
                        synced_at = GETUTCDATE()
                    WHEN NOT MATCHED THEN INSERT (
                        return_date, order_id, sku, asin, fnsku, product_name,
                        quantity, fulfillment_center_id, detailed_disposition,
                        reason, status, license_plate_number, customer_comments,
                        marketplace_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, [
                    # USING src params
                    order_id, sku, return_date,
                    r.get("fulfillment_center_id", ""),
                    int(r.get("quantity") or 1),
                    # WHEN MATCHED UPDATE params
                    r.get("detailed_disposition", ""),
                    r.get("reason", ""),
                    r.get("status", ""),
                    r.get("customer_comments", ""),
                    # WHEN NOT MATCHED INSERT params
                    return_date, order_id, sku,
                    r.get("asin", ""),
                    r.get("fnsku", ""),
                    r.get("product_name", ""),
                    int(r.get("quantity") or 1),
                    r.get("fulfillment_center_id", ""),
                    r.get("detailed_disposition", ""),
                    r.get("reason", ""),
                    r.get("status", ""),
                    r.get("license_plate_number", ""),
                    r.get("customer_comments", ""),
                    r.get("marketplace_id", ""),
                ])
                # MERGE doesn't easily tell us if it was insert vs update,
                # so we count based on rowcount
                inserted += 1
            except Exception as exc:
                errors += 1
                if errors <= 5:
                    log.warning("return_tracker.upsert_error", order=order_id, error=str(exc)[:200])

        conn.commit()
        log.info("return_tracker.upsert_complete", inserted=inserted, errors=errors)
        return {"inserted": inserted, "updated": updated, "errors": errors}

    finally:
        conn.close()


# ──────────────────── 3. Reconciliation ──────────────────────

def reconcile_returns() -> dict[str, int]:
    """
    Match FBA customer returns (physical) with return items (financial).
    Updates disposition and financial_status based on FBA report data.

    Flow:
      1. For each acc_return_item with financial_status='pending':
         - Look up acc_fba_customer_return by order_id + sku
         - If found → set disposition, return_date
         - Classify: SELLABLE → cogs_recovered, others → write_off
      2. Items with no FBA return after 45 days → mark 'lost_in_transit'

    Returns: {"matched": N, "sellable": N, "damaged": N, "lost": N}
    """
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 30000")

        # Step 1: Match pending returns with FBA return report data
        cur.execute("""
            UPDATE ri SET
                ri.disposition = fcr.detailed_disposition,
                ri.return_date = fcr.return_date,
                ri.return_reason = fcr.reason,
                ri.return_reason_detail = fcr.customer_comments,
                ri.financial_status = CASE
                    WHEN fcr.detailed_disposition = 'SELLABLE' THEN 'sellable_return'
                    ELSE 'damaged_return'
                END,
                ri.cogs_recovered_pln = CASE
                    WHEN fcr.detailed_disposition = 'SELLABLE' THEN ISNULL(ri.cogs_pln, 0)
                    ELSE 0
                END,
                ri.write_off_pln = CASE
                    WHEN fcr.detailed_disposition != 'SELLABLE' THEN ISNULL(ri.cogs_pln, 0)
                    ELSE 0
                END,
                ri.updated_at = GETUTCDATE(),
                ri.source = 'fba_report'
            FROM dbo.acc_return_item ri
            INNER JOIN dbo.acc_fba_customer_return fcr WITH (NOLOCK)
                ON fcr.order_id = ri.amazon_order_id
                AND fcr.sku = ri.sku
            WHERE ri.financial_status = 'pending'
              AND ri.manual_status IS NULL
        """)
        matched = cur.rowcount

        # Get breakdown
        cur.execute("""
            SELECT financial_status, COUNT(*) as cnt
            FROM dbo.acc_return_item WITH (NOLOCK)
            WHERE source = 'fba_report'
              AND updated_at > DATEADD(minute, -5, GETUTCDATE())
            GROUP BY financial_status
        """)
        breakdown = {r[0]: r[1] for r in cur.fetchall()}

        # Step 2: Mark old pending items as lost (45+ days with no physical return)
        cur.execute("""
            UPDATE dbo.acc_return_item SET
                financial_status = 'lost_in_transit',
                write_off_pln = ISNULL(cogs_pln, 0),
                cogs_recovered_pln = 0,
                updated_at = GETUTCDATE()
            WHERE financial_status = 'pending'
              AND manual_status IS NULL
              AND refund_date < DATEADD(day, -45, GETUTCDATE())
        """)
        lost = cur.rowcount

        conn.commit()
        result = {
            "matched": matched,
            "sellable": breakdown.get("sellable_return", 0),
            "damaged": breakdown.get("damaged_return", 0),
            "lost": lost,
        }
        log.info("return_tracker.reconcile_complete", **result)
        return result

    finally:
        conn.close()


# ──────────────────── 4. Manual Override ──────────────────────

def update_return_status(
    return_item_id: int,
    financial_status: str,
    note: str | None = None,
    updated_by: str = "admin",
) -> dict[str, Any]:
    """
    Manual override of return item financial status.
    Warehouse team confirms: sellable_return / damaged_return / reimbursed / lost_in_transit.
    """
    valid_statuses = {"sellable_return", "damaged_return", "lost_in_transit", "reimbursed", "pending"}
    if financial_status not in valid_statuses:
        raise ValueError(f"Invalid status: {financial_status}. Must be one of {valid_statuses}")

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 30000")

        # Get current item
        cur.execute("SELECT id, cogs_pln, financial_status FROM dbo.acc_return_item WHERE id = ?", [return_item_id])
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Return item {return_item_id} not found")

        cogs = _f(row[1])

        cogs_recovered = cogs if financial_status == "sellable_return" else 0.0
        write_off = cogs if financial_status in ("damaged_return", "lost_in_transit") else 0.0

        cur.execute("""
            UPDATE dbo.acc_return_item SET
                financial_status = ?,
                manual_status = ?,
                manual_note = ?,
                manual_updated_by = ?,
                manual_updated_at = GETUTCDATE(),
                cogs_recovered_pln = ?,
                write_off_pln = ?,
                updated_at = GETUTCDATE()
            WHERE id = ?
        """, [financial_status, financial_status, note, updated_by, cogs_recovered, write_off, return_item_id])

        conn.commit()
        log.info("return_tracker.manual_update", id=return_item_id, status=financial_status, by=updated_by)
        return {"id": return_item_id, "financial_status": financial_status, "cogs_recovered_pln": cogs_recovered, "write_off_pln": write_off}

    finally:
        conn.close()


# ──────────────────── 5. Dashboard & Analytics ──────────────────────

def get_return_dashboard(
    date_from: date | None = None,
    date_to: date | None = None,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    """
    Return tracker dashboard data:
    - Summary KPIs (total refunds, sellable rate, COGS recovered vs written off)
    - Breakdown by marketplace
    - Top returned products
    - Pending items needing attention
    """
    if date_from is None:
        date_from = date.today() - timedelta(days=30)
    if date_to is None:
        date_to = date.today()

    conn = _connect()
    try:
        cur = conn.cursor()

        mkt_filter = ""
        params: list[Any] = [date_from.isoformat(), date_to.isoformat()]
        if marketplace_id:
            mkt_filter = " AND ri.marketplace_id = ?"
            params.append(marketplace_id)

        # Summary KPIs
        cur.execute(f"""
            SELECT
                COUNT(*)                                            AS total_items,
                SUM(ri.quantity)                                    AS total_units,
                COUNT(DISTINCT ri.amazon_order_id)                  AS total_orders,
                SUM(ISNULL(ri.refund_amount_pln, 0))               AS total_refund_pln,
                SUM(ISNULL(ri.cogs_pln, 0))                        AS total_cogs_pln,
                SUM(ISNULL(ri.cogs_recovered_pln, 0))              AS total_cogs_recovered_pln,
                SUM(ISNULL(ri.write_off_pln, 0))                   AS total_write_off_pln,
                SUM(CASE WHEN ri.financial_status = 'pending'
                    THEN ISNULL(ri.cogs_pln, 0) ELSE 0 END)        AS pending_cogs_pln,
                SUM(CASE WHEN ri.financial_status = 'sellable_return' THEN 1 ELSE 0 END) AS sellable_count,
                SUM(CASE WHEN ri.financial_status = 'damaged_return' THEN 1 ELSE 0 END)  AS damaged_count,
                SUM(CASE WHEN ri.financial_status = 'pending' THEN 1 ELSE 0 END)         AS pending_count,
                SUM(CASE WHEN ri.financial_status = 'lost_in_transit' THEN 1 ELSE 0 END) AS lost_count,
                SUM(CASE WHEN ri.financial_status = 'reimbursed' THEN 1 ELSE 0 END)      AS reimbursed_count
            FROM dbo.acc_return_item ri WITH (NOLOCK)
            WHERE ri.refund_date >= CAST(? AS DATE)
              AND ri.refund_date < DATEADD(day, 1, CAST(? AS DATE))
              {mkt_filter}
        """, params)
        sr = cur.fetchone()

        total_items = sr[0] or 0
        sellable_count = sr[8] or 0
        damaged_count = sr[9] or 0
        returned_count = sellable_count + damaged_count
        sellable_rate = round(sellable_count / returned_count * 100, 1) if returned_count > 0 else 0

        summary = {
            "total_items": total_items,
            "total_units": sr[1] or 0,
            "total_orders": sr[2] or 0,
            "total_refund_pln": _f(sr[3]),
            "total_cogs_at_risk_pln": _f(sr[4]),
            "cogs_recovered_pln": _f(sr[5]),
            "cogs_write_off_pln": _f(sr[6]),
            "cogs_pending_pln": _f(sr[7]),
            "sellable_count": sellable_count,
            "damaged_count": damaged_count,
            "pending_count": sr[10] or 0,
            "lost_count": sr[11] or 0,
            "reimbursed_count": sr[12] or 0,
            "sellable_rate_pct": sellable_rate,
            "net_loss_pln": _f(sr[6]) + _f(sr[7]),  # write_off + pending
        }

        # By marketplace breakdown
        params2: list[Any] = [date_from.isoformat(), date_to.isoformat()]
        mkt_filter2 = ""
        if marketplace_id:
            mkt_filter2 = " AND ri.marketplace_id = ?"
            params2.append(marketplace_id)

        cur.execute(f"""
            SELECT
                ri.marketplace_id,
                COUNT(*) AS items,
                SUM(ri.quantity) AS units,
                SUM(ISNULL(ri.refund_amount_pln, 0)) AS refund_pln,
                SUM(ISNULL(ri.cogs_recovered_pln, 0)) AS recovered_pln,
                SUM(ISNULL(ri.write_off_pln, 0)) AS write_off_pln,
                SUM(CASE WHEN ri.financial_status = 'sellable_return' THEN 1 ELSE 0 END) AS sellable,
                SUM(CASE WHEN ri.financial_status = 'damaged_return' THEN 1 ELSE 0 END) AS damaged,
                SUM(CASE WHEN ri.financial_status = 'pending' THEN 1 ELSE 0 END) AS pending
            FROM dbo.acc_return_item ri WITH (NOLOCK)
            WHERE ri.refund_date >= CAST(? AS DATE)
              AND ri.refund_date < DATEADD(day, 1, CAST(? AS DATE))
              {mkt_filter2}
            GROUP BY ri.marketplace_id
            ORDER BY SUM(ri.quantity) DESC
        """, params2)
        by_marketplace = []
        for r in cur.fetchall():
            mkt = r[0]
            by_marketplace.append({
                "marketplace_id": mkt,
                "marketplace_code": _mkt_code(mkt),
                "items": r[1],
                "units": r[2],
                "refund_pln": _f(r[3]),
                "cogs_recovered_pln": _f(r[4]),
                "write_off_pln": _f(r[5]),
                "sellable": r[6],
                "damaged": r[7],
                "pending": r[8],
            })

        # Top returned SKUs
        params3: list[Any] = [date_from.isoformat(), date_to.isoformat()]
        mkt_filter3 = ""
        if marketplace_id:
            mkt_filter3 = " AND ri.marketplace_id = ?"
            params3.append(marketplace_id)

        cur.execute(f"""
            SELECT TOP 20
                ri.sku, ri.asin, ri.marketplace_id,
                SUM(ri.quantity) AS return_units,
                SUM(ISNULL(ri.cogs_pln, 0)) AS cogs_at_risk_pln,
                SUM(ISNULL(ri.cogs_recovered_pln, 0)) AS recovered_pln,
                SUM(ISNULL(ri.write_off_pln, 0)) AS write_off_pln,
                SUM(CASE WHEN ri.financial_status = 'sellable_return' THEN 1 ELSE 0 END) AS sellable,
                SUM(CASE WHEN ri.financial_status = 'damaged_return' THEN 1 ELSE 0 END) AS damaged,
                SUM(CASE WHEN ri.financial_status = 'pending' THEN 1 ELSE 0 END) AS pending,
                MAX(ri.return_reason) AS top_reason
            FROM dbo.acc_return_item ri WITH (NOLOCK)
            WHERE ri.refund_date >= CAST(? AS DATE)
              AND ri.refund_date < DATEADD(day, 1, CAST(? AS DATE))
              {mkt_filter3}
            GROUP BY ri.sku, ri.asin, ri.marketplace_id
            ORDER BY SUM(ri.quantity) DESC
        """, params3)
        top_skus = []
        for r in cur.fetchall():
            top_skus.append({
                "sku": r[0],
                "asin": r[1],
                "marketplace_id": r[2],
                "marketplace_code": _mkt_code(r[2]),
                "return_units": r[3],
                "cogs_at_risk_pln": _f(r[4]),
                "cogs_recovered_pln": _f(r[5]),
                "write_off_pln": _f(r[6]),
                "sellable": r[7],
                "damaged": r[8],
                "pending": r[9],
                "top_reason": r[10] or "",
            })

        # Pending items needing attention (oldest first)
        params4: list[Any] = []
        mkt_filter4 = ""
        if marketplace_id:
            mkt_filter4 = " AND ri.marketplace_id = ?"
            params4.append(marketplace_id)

        cur.execute(f"""
            SELECT TOP 50
                ri.id, ri.amazon_order_id, ri.sku, ri.asin, ri.marketplace_id,
                ri.refund_date, ri.refund_type, ri.quantity,
                ri.cogs_pln, ri.financial_status,
                DATEDIFF(day, ri.refund_date, GETUTCDATE()) AS days_since_refund
            FROM dbo.acc_return_item ri WITH (NOLOCK)
            WHERE ri.financial_status = 'pending'
              AND ri.manual_status IS NULL
              {mkt_filter4}
            ORDER BY ri.refund_date ASC
        """, params4)
        pending_items = []
        for r in cur.fetchall():
            pending_items.append({
                "id": r[0],
                "amazon_order_id": r[1],
                "sku": r[2],
                "asin": r[3],
                "marketplace_id": r[4],
                "marketplace_code": _mkt_code(r[4]),
                "refund_date": r[5].isoformat() if r[5] else None,
                "refund_type": r[6],
                "quantity": r[7],
                "cogs_pln": _f(r[8]),
                "financial_status": r[9],
                "days_since_refund": r[10],
            })

        return {
            "period": {"date_from": date_from.isoformat(), "date_to": date_to.isoformat()},
            "summary": summary,
            "by_marketplace": by_marketplace,
            "top_returned_skus": top_skus,
            "pending_items": pending_items,
        }

    finally:
        conn.close()


# ──────────────────── 6. Return Items List (Detailed) ──────────────────────

def get_return_items(
    date_from: date | None = None,
    date_to: date | None = None,
    marketplace_id: str | None = None,
    financial_status: str | None = None,
    sku_search: str | None = None,
    sort_by: str = "refund_date",
    sort_dir: str = "desc",
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """Paginated list of return items with filters."""
    if date_from is None:
        date_from = date.today() - timedelta(days=30)
    if date_to is None:
        date_to = date.today()

    conn = _connect()
    try:
        cur = conn.cursor()

        wheres = [
            "ri.refund_date >= CAST(? AS DATE)",
            "ri.refund_date < DATEADD(day, 1, CAST(? AS DATE))",
        ]
        params: list[Any] = [date_from.isoformat(), date_to.isoformat()]

        if marketplace_id:
            wheres.append("ri.marketplace_id = ?")
            params.append(marketplace_id)
        if financial_status:
            wheres.append("ri.financial_status = ?")
            params.append(financial_status)
        if sku_search:
            wheres.append("(ri.sku LIKE ? OR ri.asin LIKE ?)")
            params.extend([f"%{sku_search}%", f"%{sku_search}%"])

        where_sql = " AND ".join(wheres)

        # Count
        cur.execute(f"SELECT COUNT(*) FROM dbo.acc_return_item ri WITH (NOLOCK) WHERE {where_sql}", params)
        total = cur.fetchone()[0] or 0

        # Validate sort
        allowed_sort = {"refund_date", "return_date", "sku", "cogs_pln", "financial_status", "quantity", "marketplace_id"}
        if sort_by not in allowed_sort:
            sort_by = "refund_date"
        sort_dir_sql = "DESC" if sort_dir.lower() == "desc" else "ASC"

        pages = max(1, (total + page_size - 1) // page_size)
        offset = (max(1, page) - 1) * page_size

        cur.execute(f"""
            SELECT
                ri.id, ri.amazon_order_id, ri.sku, ri.asin, ri.marketplace_id,
                ri.refund_date, ri.refund_type, ri.refund_amount_pln,
                ri.return_date, ri.return_reason, ri.disposition,
                ri.quantity, ri.financial_status,
                ri.cogs_pln, ri.cogs_recovered_pln, ri.write_off_pln,
                ri.manual_status, ri.manual_note, ri.manual_updated_by,
                ri.source, ri.created_at
            FROM dbo.acc_return_item ri WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY ri.{sort_by} {sort_dir_sql}
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, params + [offset, page_size])

        items = []
        for r in cur.fetchall():
            items.append({
                "id": r[0],
                "amazon_order_id": r[1],
                "sku": r[2],
                "asin": r[3],
                "marketplace_id": r[4],
                "marketplace_code": _mkt_code(r[4]),
                "refund_date": r[5].isoformat() if r[5] else None,
                "refund_type": r[6],
                "refund_amount_pln": _f(r[7]),
                "return_date": r[8].isoformat() if r[8] else None,
                "return_reason": r[9],
                "disposition": r[10],
                "quantity": r[11],
                "financial_status": r[12],
                "cogs_pln": _f(r[13]),
                "cogs_recovered_pln": _f(r[14]),
                "write_off_pln": _f(r[15]),
                "manual_status": r[16],
                "manual_note": r[17],
                "manual_updated_by": r[18],
                "source": r[19],
                "created_at": r[20].isoformat() if r[20] else None,
            })

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": pages,
            "items": items,
        }

    finally:
        conn.close()


# ──────────────────── 7. Profit Engine Integration ──────────────────────

def get_return_cogs_adjustments(
    date_from: date,
    date_to: date,
    marketplace_id: str | None = None,
) -> dict[str, dict[str, float]]:
    """
    Get COGS adjustments per SKU+marketplace for profit engine integration.

    Returns dict keyed by (sku, marketplace_id):
      {
        "sku|marketplace_id": {
            "cogs_recovered_pln": float,  # positive = recovered (WZ reversal)
            "write_off_pln": float,       # positive = loss
            "pending_cogs_pln": float,    # awaiting resolution
        }
      }
    """
    conn = _connect()
    try:
        cur = conn.cursor()
        params: list[Any] = [date_from.isoformat(), date_to.isoformat()]
        mkt_filter = ""
        if marketplace_id:
            mkt_filter = " AND ri.marketplace_id = ?"
            params.append(marketplace_id)

        cur.execute(f"""
            SELECT
                ri.sku, ri.marketplace_id,
                SUM(ISNULL(ri.cogs_recovered_pln, 0)) AS cogs_recovered,
                SUM(ISNULL(ri.write_off_pln, 0)) AS write_off,
                SUM(CASE WHEN ri.financial_status = 'pending'
                    THEN ISNULL(ri.cogs_pln, 0) ELSE 0 END) AS pending_cogs
            FROM dbo.acc_return_item ri WITH (NOLOCK)
            WHERE ri.refund_date >= CAST(? AS DATE)
              AND ri.refund_date < DATEADD(day, 1, CAST(? AS DATE))
              {mkt_filter}
            GROUP BY ri.sku, ri.marketplace_id
        """, params)

        result = {}
        for r in cur.fetchall():
            key = f"{r[0]}|{r[1]}"
            result[key] = {
                "cogs_recovered_pln": _f(r[2]),
                "write_off_pln": _f(r[3]),
                "pending_cogs_pln": _f(r[4]),
            }
        return result

    finally:
        conn.close()


# ──────────────────── 8. Daily Summary Rebuild ──────────────────────

def rebuild_daily_summary(
    date_from: date | None = None,
    date_to: date | None = None,
) -> int:
    """
    Rebuild acc_return_daily_summary from acc_return_item.
    Useful after reconciliation or manual updates.
    Returns number of rows upserted.
    """
    if date_from is None:
        date_from = date.today() - timedelta(days=90)
    if date_to is None:
        date_to = date.today()

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 30000")

        cur.execute("""
            MERGE dbo.acc_return_daily_summary AS tgt
            USING (
                SELECT
                    CAST(ri.refund_date AS DATE) AS report_date,
                    ri.marketplace_id,
                    COUNT(DISTINCT ri.amazon_order_id) AS refund_orders,
                    SUM(ri.quantity) AS refund_units,
                    SUM(CASE WHEN ri.financial_status IN ('sellable_return','damaged_return') THEN ri.quantity ELSE 0 END) AS return_received_units,
                    SUM(CASE WHEN ri.financial_status = 'sellable_return' THEN ri.quantity ELSE 0 END) AS sellable_units,
                    SUM(CASE WHEN ri.financial_status = 'damaged_return' THEN ri.quantity ELSE 0 END) AS damaged_units,
                    SUM(CASE WHEN ri.financial_status = 'pending' THEN ri.quantity ELSE 0 END) AS pending_units,
                    SUM(CASE WHEN ri.financial_status = 'reimbursed' THEN ri.quantity ELSE 0 END) AS reimbursed_units,
                    SUM(ISNULL(ri.refund_amount_pln, 0)) AS refund_amount_pln,
                    SUM(ISNULL(ri.cogs_pln, 0)) AS cogs_total_pln,
                    SUM(ISNULL(ri.cogs_recovered_pln, 0)) AS cogs_recovered_pln,
                    SUM(ISNULL(ri.write_off_pln, 0)) AS cogs_write_off_pln,
                    SUM(CASE WHEN ri.financial_status = 'pending' THEN ISNULL(ri.cogs_pln, 0) ELSE 0 END) AS cogs_pending_pln
                FROM dbo.acc_return_item ri WITH (NOLOCK)
                WHERE ri.refund_date >= CAST(? AS DATE)
                  AND ri.refund_date < DATEADD(day, 1, CAST(? AS DATE))
                GROUP BY CAST(ri.refund_date AS DATE), ri.marketplace_id
            ) AS src
            ON tgt.report_date = src.report_date AND tgt.marketplace_id = src.marketplace_id
            WHEN MATCHED THEN UPDATE SET
                tgt.refund_orders = src.refund_orders,
                tgt.refund_units = src.refund_units,
                tgt.return_received_units = src.return_received_units,
                tgt.sellable_units = src.sellable_units,
                tgt.damaged_units = src.damaged_units,
                tgt.pending_units = src.pending_units,
                tgt.reimbursed_units = src.reimbursed_units,
                tgt.refund_amount_pln = src.refund_amount_pln,
                tgt.cogs_total_pln = src.cogs_total_pln,
                tgt.cogs_recovered_pln = src.cogs_recovered_pln,
                tgt.cogs_write_off_pln = src.cogs_write_off_pln,
                tgt.cogs_pending_pln = src.cogs_pending_pln,
                tgt.sellable_rate_pct = CASE
                    WHEN src.return_received_units > 0
                    THEN ROUND(src.sellable_units * 100.0 / src.return_received_units, 2)
                    ELSE NULL END,
                tgt.updated_at = GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT (
                report_date, marketplace_id,
                refund_orders, refund_units, return_received_units,
                sellable_units, damaged_units, pending_units, reimbursed_units,
                refund_amount_pln, cogs_total_pln, cogs_recovered_pln,
                cogs_write_off_pln, cogs_pending_pln, sellable_rate_pct
            ) VALUES (
                src.report_date, src.marketplace_id,
                src.refund_orders, src.refund_units, src.return_received_units,
                src.sellable_units, src.damaged_units, src.pending_units, src.reimbursed_units,
                src.refund_amount_pln, src.cogs_total_pln, src.cogs_recovered_pln,
                src.cogs_write_off_pln, src.cogs_pending_pln,
                CASE WHEN src.return_received_units > 0
                    THEN ROUND(src.sellable_units * 100.0 / src.return_received_units, 2)
                    ELSE NULL END
            );
        """, [date_from.isoformat(), date_to.isoformat()])

        affected = cur.rowcount
        conn.commit()
        log.info("return_tracker.daily_summary_rebuilt", date_from=date_from, date_to=date_to, rows=affected)
        return affected

    finally:
        conn.close()


# ──────────────────── 9. Sync State Watermark ──────────────────────

def _get_sync_watermark(marketplace_id: str) -> datetime | None:
    """Get last synced timestamp for a marketplace from acc_return_sync_state."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT last_synced_to FROM dbo.acc_return_sync_state WITH (NOLOCK) WHERE marketplace_id = ?",
            [marketplace_id],
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _update_sync_watermark(
    marketplace_id: str, synced_to: datetime, rows_synced: int, status: str = "ok", error_msg: str | None = None,
) -> None:
    """Update or insert sync watermark for a marketplace."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 30000")

        # Check if row exists
        cur.execute(
            "SELECT id FROM dbo.acc_return_sync_state WITH (NOLOCK) WHERE marketplace_id = ?",
            [marketplace_id],
        )
        exists = cur.fetchone()

        if exists:
            cur.execute("""
                UPDATE dbo.acc_return_sync_state SET
                    last_synced_to = ?,
                    last_sync_at = GETUTCDATE(),
                    rows_synced = ?,
                    status = ?,
                    error_message = ?
                WHERE marketplace_id = ?
            """, [synced_to, rows_synced, status, error_msg, marketplace_id])
        else:
            cur.execute("""
                INSERT INTO dbo.acc_return_sync_state
                    (marketplace_id, last_synced_to, last_sync_at, rows_synced, status, error_message)
                VALUES (?, ?, GETUTCDATE(), ?, ?, ?)
            """, [marketplace_id, synced_to, rows_synced, status, error_msg])

        conn.commit()
    finally:
        conn.close()


# ──────────────────── 10. Full Sync Pipeline ──────────────────────

# Primary marketplace for report fetching — EU unified account returns
# ALL returns across all EU marketplaces in a single report
_PRIMARY_REPORT_MARKETPLACE = "A1PA6795UKMFR9"  # DE


def _fix_marketplace_ids_from_orders() -> int:
    """
    Fix marketplace_id in acc_fba_customer_return by joining with acc_order.
    
    The FBA Customer Returns report doesn't include marketplace info —
    all rows come back without it. We determine the actual marketplace
    from the order's marketplace_id in acc_order.
    
    Returns: number of rows updated.
    """
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 30000")
        cur.execute("""
            UPDATE fcr SET
                fcr.marketplace_id = o.marketplace_id
            FROM dbo.acc_fba_customer_return fcr
            INNER JOIN dbo.acc_order o WITH (NOLOCK)
                ON o.amazon_order_id = fcr.order_id
            WHERE fcr.marketplace_id IS NULL
               OR fcr.marketplace_id = ''
               OR fcr.marketplace_id != o.marketplace_id
        """)
        updated = cur.rowcount
        conn.commit()
        if updated > 0:
            log.info("return_tracker.fix_marketplace_ids", updated=updated)
        return updated
    finally:
        conn.close()


async def sync_fba_returns(
    days_back: int = 30,
    marketplace_ids: list[str] | None = None,
    use_watermark: bool = True,
) -> dict[str, Any]:
    """
    Full sync pipeline:
      1. Download FBA Customer Returns report (once from primary marketplace —
         EU unified account returns ALL returns in a single report)
      2. Parse and upsert raw return data
      3. Fix marketplace_id from acc_order (report doesn't include it)
      4. Update sync watermark
      5. Seed return items from refunded orders
      6. Reconcile physical returns with financial items
      7. Rebuild daily summary

    Called by scheduler (daily at 06:30) and manually via API.

    Args:
        days_back: fallback range if no watermark exists
        marketplace_ids: ignored (kept for API compatibility) — always fetches from primary
        use_watermark: if True, use acc_return_sync_state to determine start date
    """
    from app.connectors.amazon_sp_api.reports import ReportsClient, ReportType

    mkt_id = _PRIMARY_REPORT_MARKETPLACE
    mkt_code = _mkt_code(mkt_id)
    end = datetime.now(timezone.utc)
    fallback_start = end - timedelta(days=days_back)

    log.info("return_tracker.sync_start", marketplace=mkt_code, days_back=days_back, use_watermark=use_watermark)

    results: dict[str, Any] = {"marketplaces": {}, "totals": {"reports": 0, "rows": 0, "errors": 0}}

    # Determine start date from watermark
    mkt_start = fallback_start
    if use_watermark:
        wm = _get_sync_watermark(mkt_id)
        if wm:
            # Ensure watermark is offset-aware (MSSQL returns naive datetimes)
            if wm.tzinfo is None:
                wm = wm.replace(tzinfo=timezone.utc)
            # Overlap by 2 days to catch late-arriving returns
            mkt_start = max(fallback_start, wm - timedelta(days=2))
            log.info("return_tracker.watermark_found", watermark=wm.isoformat(), start=mkt_start.isoformat())

    try:
        client = ReportsClient(marketplace_id=mkt_id)

        log.info("return_tracker.requesting_report", mkt=mkt_code, start=mkt_start.isoformat(), end=end.isoformat())
        content = await client.request_and_download(
            report_type=ReportType.FBA_CUSTOMER_RETURNS,
            marketplace_ids=[mkt_id],
            data_start_time=mkt_start,
            data_end_time=end,
            poll_interval=30.0,
        )

        parsed = parse_fba_returns_report(content, marketplace_id=mkt_id)
        upsert_result = upsert_fba_returns(parsed)

        # Fix marketplace_id from acc_order (report doesn't distinguish marketplaces)
        mkt_fixed = _fix_marketplace_ids_from_orders()

        # Update watermark on success
        _update_sync_watermark(mkt_id, end, len(parsed), status="ok")

        results["marketplaces"][mkt_code] = {
            "rows_downloaded": len(parsed),
            "period": f"{mkt_start.date()}..{end.date()}",
            "marketplace_ids_fixed": mkt_fixed,
            **upsert_result,
        }
        results["totals"]["reports"] = 1
        results["totals"]["rows"] = len(parsed)

        log.info("return_tracker.sync_report_done", mkt=mkt_code, rows=len(parsed), mkt_fixed=mkt_fixed)

    except Exception as exc:
        _update_sync_watermark(mkt_id, mkt_start, 0, status="error", error_msg=str(exc)[:500])
        results["marketplaces"][mkt_code] = {"error": str(exc)[:300]}
        results["totals"]["errors"] = 1
        log.error("return_tracker.sync_report_error", mkt=mkt_code, error=str(exc)[:300])

    # Seed + reconcile + rebuild
    seed_start = mkt_start.date()
    seed_end = end.date()

    seed_result = seed_return_items_from_orders(date_from=seed_start, date_to=seed_end)
    results["seed"] = seed_result

    reconcile_result = reconcile_returns()
    results["reconcile"] = reconcile_result

    summary_rows = rebuild_daily_summary(date_from=seed_start, date_to=seed_end)
    results["daily_summary_rows"] = summary_rows

    log.info("return_tracker.sync_complete", **results["totals"])
    return results


async def backfill_fba_returns(
    days_back: int = 90,
    marketplace_ids: list[str] | None = None,
    chunk_days: int = 30,
) -> dict[str, Any]:
    """
    Backfill historical FBA Customer Returns data.
    Splits into chunks (max 30 days per report request) to avoid timeouts.
    
    Fetches from primary marketplace only (EU unified account returns all data).
    This is for one-time historical fill, not daily use.
    """
    import asyncio as _asyncio
    from app.connectors.amazon_sp_api.reports import ReportsClient, ReportType

    mkt_id = _PRIMARY_REPORT_MARKETPLACE
    mkt_code = _mkt_code(mkt_id)
    end = datetime.now(timezone.utc)
    overall_start = end - timedelta(days=days_back)

    # Build date chunks
    chunks: list[tuple[datetime, datetime]] = []
    cs = overall_start
    while cs < end:
        ce = min(cs + timedelta(days=chunk_days), end)
        chunks.append((cs, ce))
        cs = ce

    log.info("return_tracker.backfill_start", marketplace=mkt_code, days_back=days_back, chunks=len(chunks))

    results: dict[str, Any] = {"marketplaces": {}, "totals": {"reports": 0, "rows": 0, "errors": 0}}
    total_rows = 0
    chunk_errors = 0

    for ci, (chunk_start, chunk_end) in enumerate(chunks):
        try:
            client = ReportsClient(marketplace_id=mkt_id)

            log.info("return_tracker.backfill_chunk",
                     mkt=mkt_code, chunk=f"{ci+1}/{len(chunks)}",
                     start=chunk_start.date().isoformat(), end=chunk_end.date().isoformat())

            content = await client.request_and_download(
                report_type=ReportType.FBA_CUSTOMER_RETURNS,
                marketplace_ids=[mkt_id],
                data_start_time=chunk_start,
                data_end_time=chunk_end,
                poll_interval=30.0,
            )

            parsed = parse_fba_returns_report(content, marketplace_id=mkt_id)
            upsert_fba_returns(parsed)
            total_rows += len(parsed)

            log.info("return_tracker.backfill_chunk_done", mkt=mkt_code, chunk=ci+1, rows=len(parsed))
            # Rate limit: 15s between report requests
            await _asyncio.sleep(15)

        except Exception as exc:
            chunk_errors += 1
            log.error("return_tracker.backfill_chunk_error",
                      mkt=mkt_code, chunk=ci+1, error=str(exc)[:300])

    # Fix marketplace_id from acc_order
    mkt_fixed = _fix_marketplace_ids_from_orders()
    log.info("return_tracker.backfill_marketplace_fix", fixed=mkt_fixed)

    # Update watermark to latest date after backfill
    if total_rows > 0:
        _update_sync_watermark(mkt_id, end, total_rows, status="ok")

    results["marketplaces"][mkt_code] = {"total_rows": total_rows, "errors": chunk_errors, "marketplace_ids_fixed": mkt_fixed}
    results["totals"]["reports"] = len(chunks) - chunk_errors
    results["totals"]["rows"] = total_rows
    results["totals"]["errors"] = chunk_errors

    # After backfill: seed + reconcile + rebuild
    seed_result = seed_return_items_from_orders(date_from=overall_start.date(), date_to=end.date())
    results["seed"] = seed_result

    reconcile_result = reconcile_returns()
    results["reconcile"] = reconcile_result

    summary_rows = rebuild_daily_summary(date_from=overall_start.date(), date_to=end.date())
    results["daily_summary_rows"] = summary_rows

    log.info("return_tracker.backfill_complete", **results["totals"])
    return results
