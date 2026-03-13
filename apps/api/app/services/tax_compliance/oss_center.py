"""
OSS Center — WSTO aggregation, EUR conversion, period building, VIU-DO support.

Handles:
  - Aggregating WSTO events per consumption country + VAT rate
  - ECB rate conversion to EUR
  - Building OSS quarterly return periods
  - Correction tracking
  - VIU-DO data export support
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

import structlog

from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)


def _connect():
    return connect_acc(autocommit=False, timeout=30)


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def _fetchall_dict(cur) -> list[dict[str, Any]]:
    cols = [c[0] for c in cur.description] if cur.description else []
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]


def _quarter_bounds(year: int, quarter: int) -> tuple[date, date]:
    """Return (start_date, end_date) for a given year/quarter."""
    start_month = (quarter - 1) * 3 + 1
    start = date(year, start_month, 1)
    if quarter == 4:
        end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(year, start_month + 3, 1) - timedelta(days=1)
    return start, end


# ═══════════════════════════════════════════════════════════════════
# ECB Rate sync
# ═══════════════════════════════════════════════════════════════════

def sync_ecb_rates(days_back: int = 90) -> dict[str, Any]:
    """
    Fetch ECB exchange rates and store them.
    Uses the ECB Statistical Data Warehouse XML feed.
    Falls back to NBP for PLN rates if needed.
    """
    import urllib.request
    import xml.etree.ElementTree as ET

    conn = _connect()
    cur = conn.cursor()
    stats = {"fetched": 0, "inserted": 0, "errors": 0}

    try:
        # Fetch from ECB (last 90 days)
        url = f"https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist-90d.xml"
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                xml_data = resp.read()
        except Exception as e:
            log.warning("ecb_rates.fetch_failed", error=str(e))
            return {"fetched": 0, "inserted": 0, "errors": 1, "error": str(e)}

        root = ET.fromstring(xml_data)
        ns = {"gesmes": "http://www.gesmes.org/xml/2002-08-01",
              "eurofxref": "http://www.ecb.int/vocabulary/2002-08-01/eurofxref"}

        for cube_time in root.findall(".//eurofxref:Cube[@time]", ns):
            rate_date_str = cube_time.get("time")
            if not rate_date_str:
                continue
            rate_date = date.fromisoformat(rate_date_str)
            stats["fetched"] += 1

            for cube_rate in cube_time.findall("eurofxref:Cube", ns):
                currency = cube_rate.get("currency")
                rate_val = cube_rate.get("rate")
                if not currency or not rate_val:
                    continue

                try:
                    cur.execute("""
                        MERGE dbo.ecb_exchange_rate AS tgt
                        USING (SELECT ? AS rd, ? AS sc, 'EUR' AS tc, ? AS r) AS src
                        ON tgt.rate_date = src.rd
                           AND tgt.source_currency = src.sc
                           AND tgt.target_currency = src.tc
                        WHEN MATCHED THEN
                            UPDATE SET rate = src.r
                        WHEN NOT MATCHED THEN
                            INSERT (rate_date, source_currency, target_currency, rate)
                            VALUES (src.rd, src.sc, src.tc, src.r);
                    """, (rate_date, currency, float(rate_val)))
                    stats["inserted"] += 1
                except Exception as e:
                    stats["errors"] += 1

            conn.commit()

        log.info("sync_ecb_rates.done", **stats)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return stats


def _get_ecb_rate(cur, currency: str, rate_date: date) -> float | None:
    """Get exchange rate for currency->EUR on given date (or nearest prior).
    
    Tries ecb_exchange_rate first, then falls back to acc_exchange_rate
    (NBP rates via PLN cross-rate).
    """
    if currency == "EUR":
        return 1.0

    # Try compliance-owned ECB table first
    cur.execute("""
        SELECT TOP 1 rate FROM dbo.ecb_exchange_rate WITH (NOLOCK)
        WHERE source_currency = ? AND target_currency = 'EUR'
          AND rate_date <= ?
        ORDER BY rate_date DESC
    """, (currency, rate_date))
    row = cur.fetchone()
    if row:
        return float(row[0])

    # Fallback: derive EUR rate from acc_exchange_rate via PLN cross-rate
    # currency_to_eur = currency_to_pln / eur_to_pln
    cur.execute("""
        SELECT TOP 1 rate_to_pln FROM dbo.acc_exchange_rate WITH (NOLOCK)
        WHERE currency = ? AND rate_date <= ?
        ORDER BY rate_date DESC
    """, (currency, rate_date))
    src_row = cur.fetchone()

    cur.execute("""
        SELECT TOP 1 rate_to_pln FROM dbo.acc_exchange_rate WITH (NOLOCK)
        WHERE currency = 'EUR' AND rate_date <= ?
        ORDER BY rate_date DESC
    """, (rate_date,))
    eur_row = cur.fetchone()

    if src_row and eur_row and float(eur_row[0]) > 0:
        return float(src_row[0]) / float(eur_row[0])

    return None


# ═══════════════════════════════════════════════════════════════════
# Build OSS period
# ═══════════════════════════════════════════════════════════════════

def build_oss_period(year: int, quarter: int) -> dict[str, Any]:
    """
    Build OSS return period from WSTO events in vat_event_ledger.

    Aggregates per consumption_country + VAT rate, converts to EUR.
    Creates oss_return_period + oss_return_line records.
    """
    conn = _connect()
    cur = conn.cursor()

    try:
        start, end = _quarter_bounds(year, quarter)

        # Get or create period
        cur.execute("""
            SELECT id, status FROM dbo.oss_return_period
            WHERE year = ? AND quarter = ?
        """, (year, quarter))
        period_row = cur.fetchone()

        if period_row and period_row[1] == "filed":
            return {"error": "Period already filed", "period_id": period_row[0]}

        if period_row:
            period_id = period_row[0]
            # Clear existing lines for rebuild
            cur.execute("DELETE FROM dbo.oss_return_line WHERE oss_period_id = ?", (period_id,))
        else:
            cur.execute("""
                INSERT INTO dbo.oss_return_period(year, quarter, status)
                VALUES (?, ?, 'draft');
                SELECT SCOPE_IDENTITY();
            """, (year, quarter))
            period_id = int(cur.fetchone()[0])

        # Aggregate WSTO events
        cur.execute("""
            SELECT
                vel.consumption_country,
                ISNULL(vel.tax_rate, vrm.rate) as effective_rate,
                COUNT(*) as source_count,
                SUM(vel.tax_base_amount) as total_base,
                SUM(vel.tax_amount) as total_tax,
                vel.currency
            FROM dbo.vat_event_ledger vel WITH (NOLOCK)
            LEFT JOIN dbo.vat_rate_mapping vrm WITH (NOLOCK)
                ON vrm.country = vel.consumption_country
                AND vrm.is_default = 1
                AND vrm.valid_from <= vel.event_date
                AND (vrm.valid_to IS NULL OR vrm.valid_to >= vel.event_date)
            WHERE vel.vat_classification = 'WSTO'
              AND vel.event_date >= ? AND vel.event_date <= ?
            GROUP BY vel.consumption_country,
                     ISNULL(vel.tax_rate, vrm.rate),
                     vel.currency
        """, (start, end))

        lines = cur.fetchall()
        total_base_eur = Decimal(0)
        total_tax_eur = Decimal(0)
        line_count = 0
        corrections_count = 0

        for line in lines:
            country = line[0]
            vat_rate = _to_float(line[1])
            source_count = int(line[2] or 0)
            total_base = _to_float(line[3])
            total_tax = _to_float(line[4])
            currency = str(line[5] or "EUR")

            # Convert to EUR using ECB rate (use mid-quarter date)
            mid_date = start + (end - start) / 2
            ecb_rate = _get_ecb_rate(cur, currency, mid_date)

            if ecb_rate and ecb_rate != 0:
                base_eur = total_base / ecb_rate
                tax_eur = total_tax / ecb_rate if total_tax else (base_eur * vat_rate / 100 if vat_rate else 0)
            else:
                base_eur = total_base  # Assume EUR if no rate
                tax_eur = total_tax or (total_base * vat_rate / 100 if vat_rate else 0)

            cur.execute("""
                INSERT INTO dbo.oss_return_line(
                    oss_period_id, consumption_country, vat_rate,
                    tax_base_eur, tax_amount_eur, correction_flag, source_count
                ) VALUES (?, ?, ?, ?, ?, 0, ?)
            """, (period_id, country, vat_rate, round(base_eur, 4), round(tax_eur, 4), source_count))

            total_base_eur += Decimal(str(round(base_eur, 4)))
            total_tax_eur += Decimal(str(round(tax_eur, 4)))
            line_count += 1

        # Handle corrections from previous quarters
        cur.execute("""
            SELECT
                vel.consumption_country,
                ISNULL(vel.tax_rate, vrm.rate) as effective_rate,
                COUNT(*) as source_count,
                SUM(vel.tax_base_amount) as total_base,
                SUM(vel.tax_amount) as total_tax,
                vel.currency
            FROM dbo.vat_event_ledger vel WITH (NOLOCK)
            LEFT JOIN dbo.vat_rate_mapping vrm WITH (NOLOCK)
                ON vrm.country = vel.consumption_country
                AND vrm.is_default = 1
                AND vrm.valid_from <= vel.event_date
                AND (vrm.valid_to IS NULL OR vrm.valid_to >= vel.event_date)
            WHERE vel.event_type = 'correction'
              AND vel.vat_classification = 'WSTO'
              AND vel.event_date >= ? AND vel.event_date <= ?
            GROUP BY vel.consumption_country,
                     ISNULL(vel.tax_rate, vrm.rate),
                     vel.currency
        """, (start, end))

        for line in cur.fetchall():
            country = line[0]
            vat_rate = _to_float(line[1])
            source_count = int(line[2] or 0)
            total_base = _to_float(line[3])
            currency = str(line[5] or "EUR")

            mid_date = start + (end - start) / 2
            ecb_rate = _get_ecb_rate(cur, currency, mid_date)

            if ecb_rate and ecb_rate != 0:
                base_eur = total_base / ecb_rate
                tax_eur = base_eur * vat_rate / 100 if vat_rate else 0
            else:
                base_eur = total_base
                tax_eur = total_base * vat_rate / 100 if vat_rate else 0

            cur.execute("""
                INSERT INTO dbo.oss_return_line(
                    oss_period_id, consumption_country, vat_rate,
                    tax_base_eur, tax_amount_eur, correction_flag, source_count
                ) VALUES (?, ?, ?, ?, ?, 1, ?)
            """, (period_id, country, vat_rate, round(base_eur, 4), round(tax_eur, 4), source_count))

            total_base_eur += Decimal(str(round(base_eur, 4)))
            total_tax_eur += Decimal(str(round(tax_eur, 4)))
            corrections_count += 1

        # Update period totals
        cur.execute("""
            UPDATE dbo.oss_return_period
            SET total_base_eur = ?, total_tax_eur = ?,
                corrections_count = ?, status = 'draft'
            WHERE id = ?
        """, (float(total_base_eur), float(total_tax_eur), corrections_count, period_id))

        conn.commit()

        return {
            "period_id": period_id,
            "year": year,
            "quarter": quarter,
            "status": "draft",
            "lines": line_count,
            "corrections": corrections_count,
            "total_base_eur": float(total_base_eur),
            "total_tax_eur": float(total_tax_eur),
        }

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════
# OSS Queries
# ═══════════════════════════════════════════════════════════════════

def get_oss_overview() -> dict[str, Any]:
    """Get overview of all OSS periods."""
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT
                p.id, p.year, p.quarter, p.status,
                p.total_base_eur, p.total_tax_eur,
                p.corrections_count, p.filed_at, p.created_at,
                (SELECT COUNT(*) FROM dbo.oss_return_line WHERE oss_period_id = p.id) as line_count
            FROM dbo.oss_return_period p WITH (NOLOCK)
            ORDER BY p.year DESC, p.quarter DESC
        """)

        periods = _fetchall_dict(cur)

        # Current quarter WSTO totals
        today = date.today()
        current_q = (today.month - 1) // 3 + 1
        start, end = _quarter_bounds(today.year, current_q)

        cur.execute("""
            SELECT COUNT(*) as event_count,
                   SUM(tax_base_amount) as total_base,
                   SUM(tax_amount) as total_tax,
                   COUNT(DISTINCT consumption_country) as countries
            FROM dbo.vat_event_ledger WITH (NOLOCK)
            WHERE vat_classification = 'WSTO'
              AND event_date >= ? AND event_date <= ?
        """, (start, end))
        current = _fetchall_dict(cur)

        return {
            "periods": periods,
            "current_quarter": {
                "year": today.year,
                "quarter": current_q,
                **(current[0] if current else {}),
            },
        }
    finally:
        conn.close()


def get_oss_period_detail(year: int, quarter: int) -> dict[str, Any]:
    """Get detailed OSS period with all lines."""
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, year, quarter, status, total_base_eur, total_tax_eur,
                   corrections_count, filed_at, created_at
            FROM dbo.oss_return_period WITH (NOLOCK)
            WHERE year = ? AND quarter = ?
        """, (year, quarter))
        period = _fetchall_dict(cur)
        if not period:
            return {"error": f"Period {year}-Q{quarter} not found"}

        period_data = period[0]

        cur.execute("""
            SELECT l.id, l.consumption_country, l.vat_rate,
                   l.tax_base_eur, l.tax_amount_eur,
                   l.correction_flag, l.source_count
            FROM dbo.oss_return_line l WITH (NOLOCK)
            WHERE l.oss_period_id = ?
            ORDER BY l.consumption_country, l.vat_rate
        """, (period_data["id"],))

        lines = _fetchall_dict(cur)

        return {
            "period": period_data,
            "lines": lines,
        }
    finally:
        conn.close()


def get_oss_corrections(year: int | None = None) -> dict[str, Any]:
    """Get all corrections across OSS periods."""
    conn = _connect()
    cur = conn.cursor()
    try:
        where = "WHERE l.correction_flag = 1"
        params: list = []
        if year:
            where += " AND p.year = ?"
            params.append(year)

        cur.execute(f"""
            SELECT p.year, p.quarter, l.consumption_country, l.vat_rate,
                   l.tax_base_eur, l.tax_amount_eur, l.source_count
            FROM dbo.oss_return_line l WITH (NOLOCK)
            JOIN dbo.oss_return_period p WITH (NOLOCK) ON p.id = l.oss_period_id
            {where}
            ORDER BY p.year DESC, p.quarter DESC, l.consumption_country
        """, tuple(params))

        items = _fetchall_dict(cur)
        return {"corrections": items, "total": len(items)}
    finally:
        conn.close()
