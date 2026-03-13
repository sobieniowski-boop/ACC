"""
Local VAT Ledger — sales in FBA warehouse countries.

Tracks local VAT obligations in countries where FBA stocks are held
and local sales occur (warehouse_country == ship_to_country).
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import structlog

from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)


def _connect():
    return connect_acc(autocommit=False, timeout=30)


def _fetchall_dict(cur) -> list[dict[str, Any]]:
    cols = [c[0] for c in cur.description] if cur.description else []
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]


def build_local_vat_ledger(
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, Any]:
    """
    Build local VAT ledger from LOCAL_VAT events in vat_event_ledger.

    Populates local_vat_ledger table.
    """
    conn = _connect()
    cur = conn.cursor()
    stats = {"processed": 0, "created": 0, "errors": 0}

    try:
        if not date_from:
            date_from = date.today() - timedelta(days=90)
        if not date_to:
            date_to = date.today()

        # Get LOCAL_VAT events not yet in local ledger
        cur.execute("""
            SELECT
                vel.id, vel.consumption_country, vel.source_ref,
                vel.event_type, vel.tax_base_amount, vel.tax_amount,
                vel.currency, vel.event_date
            FROM dbo.vat_event_ledger vel WITH (NOLOCK)
            WHERE vel.vat_classification = 'LOCAL_VAT'
              AND vel.consumption_country != 'PL'
              AND vel.event_date >= ? AND vel.event_date <= ?
              AND vel.source_ref NOT IN (
                  SELECT source_ref FROM dbo.local_vat_ledger WITH (NOLOCK)
              )
        """, (date_from, date_to))

        rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if cur.description else []

        for row in rows:
            rec = dict(zip(cols, row))
            stats["processed"] += 1

            try:
                cur.execute("""
                    INSERT INTO dbo.local_vat_ledger(
                        country, source_ref, event_type,
                        tax_base, tax_amount, currency,
                        event_date, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'open')
                """, (
                    rec.get("consumption_country"),
                    rec.get("source_ref"),
                    rec.get("event_type"),
                    rec.get("tax_base_amount") or 0,
                    rec.get("tax_amount") or 0,
                    rec.get("currency") or "EUR",
                    rec.get("event_date"),
                ))
                stats["created"] += 1

            except Exception as e:
                stats["errors"] += 1
                log.warning("local_vat.insert_error", error=str(e))

            if stats["processed"] % 500 == 0:
                conn.commit()

        conn.commit()
        log.info("build_local_vat_ledger.done", **stats)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return stats


def get_local_vat_summary(
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, Any]:
    """Summary of local VAT obligations by country."""
    conn = _connect()
    cur = conn.cursor()
    try:
        if not date_from:
            date_from = date.today() - timedelta(days=90)
        if not date_to:
            date_to = date.today()

        cur.execute("""
            SELECT
                country,
                COUNT(*) as event_count,
                SUM(tax_base) as total_base,
                SUM(tax_amount) as total_tax,
                currency,
                MIN(event_date) as first_date,
                MAX(event_date) as last_date,
                SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) as open_count,
                SUM(CASE WHEN status = 'filed' THEN 1 ELSE 0 END) as filed_count
            FROM dbo.local_vat_ledger WITH (NOLOCK)
            WHERE event_date >= ? AND event_date <= ?
            GROUP BY country, currency
            ORDER BY total_base DESC
        """, (date_from, date_to))

        by_country = _fetchall_dict(cur)

        cur.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) as open_count,
                   SUM(tax_base) as total_base,
                   SUM(tax_amount) as total_tax
            FROM dbo.local_vat_ledger WITH (NOLOCK)
            WHERE event_date >= ? AND event_date <= ?
        """, (date_from, date_to))
        totals = _fetchall_dict(cur)

        return {
            "by_country": by_country,
            "totals": totals[0] if totals else {},
        }
    finally:
        conn.close()


def list_local_vat(
    country: str | None = None,
    status: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """List local VAT entries with filters."""
    conn = _connect()
    cur = conn.cursor()
    try:
        if not date_from:
            date_from = date.today() - timedelta(days=90)
        if not date_to:
            date_to = date.today()

        where_parts = ["l.event_date >= ?", "l.event_date <= ?"]
        params: list = [date_from, date_to]

        if country:
            where_parts.append("l.country = ?")
            params.append(country)
        if status:
            where_parts.append("l.status = ?")
            params.append(status)

        where_sql = " AND ".join(where_parts)
        offset = (page - 1) * page_size

        cur.execute(f"""
            SELECT COUNT(*) FROM dbo.local_vat_ledger l WITH (NOLOCK)
            WHERE {where_sql}
        """, tuple(params))
        total = cur.fetchone()[0] or 0

        cur.execute(f"""
            SELECT l.id, l.country, l.source_ref, l.event_type,
                   l.tax_base, l.tax_amount, l.currency,
                   l.event_date, l.status
            FROM dbo.local_vat_ledger l WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY l.event_date DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, tuple(params) + (offset, page_size))

        items = _fetchall_dict(cur)
        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
        }
    finally:
        conn.close()
