"""
FBA Movements Ledger — tracking own-goods transfers for WDT/WNT.

Handles:
  - Syncing FBA stock movements from Amazon reports
  - Classifying as WDT_OWN_GOODS / WNT_OWN_GOODS
  - Matching WDT-WNT pairs
  - Transport evidence tracking for movements
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import structlog

from app.core.db_connection import connect_acc
from app.services.tax_compliance.classification_engine import _classify_movement, EU_COUNTRIES

log = structlog.get_logger(__name__)


def _connect():
    return connect_acc(autocommit=False, timeout=30)


def _fetchall_dict(cur) -> list[dict[str, Any]]:
    cols = [c[0] for c in cur.description] if cur.description else []
    return [{cols[i]: row[i] for i in range(len(cols))} for row in cur.fetchall()]


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


# ═══════════════════════════════════════════════════════════════════
# Sync movements from FBA data
# ═══════════════════════════════════════════════════════════════════

def sync_fba_movements(
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, Any]:
    """
    Sync FBA inventory movements into fba_stock_movement_ledger.

    Sources:
    - acc_fba_inbound_shipment — cross-border shipments from payload_json
      (ShipFromAddress.CountryCode → DestinationFulfillmentCenterId)
    """
    conn = _connect()
    cur = conn.cursor()
    stats = {"processed": 0, "created": 0, "skipped": 0, "errors": 0}

    try:
        import json as _json

        if not date_from:
            date_from = date.today() - timedelta(days=90)
        if not date_to:
            date_to = date.today()

        # Pull inbound shipments with payload containing address info
        cur.execute("""
            SELECT
                s.shipment_id,
                s.marketplace_id,
                s.from_warehouse,
                s.status,
                ISNULL(s.last_update_at, s.created_at) as event_date,
                s.payload_json,
                s.units_received
            FROM dbo.acc_fba_inbound_shipment s WITH (NOLOCK)
            WHERE ISNULL(s.last_update_at, s.created_at) >= ?
              AND ISNULL(s.last_update_at, s.created_at) <= ?
              AND s.status IN ('CLOSED', 'RECEIVING', 'SHIPPED', 'IN_TRANSIT')
              AND NOT EXISTS (
                  SELECT 1 FROM dbo.fba_stock_movement_ledger m WITH (NOLOCK)
                  WHERE m.movement_ref = s.shipment_id
              )
        """, (date_from, date_to))

        rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if cur.description else []

        for row in rows:
            rec = dict(zip(cols, row))
            stats["processed"] += 1

            try:
                payload = rec.get("payload_json")
                if not payload:
                    stats["skipped"] += 1
                    continue

                payload_data = _json.loads(payload) if isinstance(payload, str) else payload

                # Extract origin country from ShipFromAddress
                ship_from_addr = payload_data.get("ShipFromAddress") or {}
                from_country = (ship_from_addr.get("CountryCode") or "").upper().strip()

                # Extract destination country from DestinationFulfillmentCenterId
                dest_fc = payload_data.get("DestinationFulfillmentCenterId") or rec.get("from_warehouse") or ""
                to_country = _warehouse_to_country(dest_fc)

                if not from_country or not to_country:
                    stats["skipped"] += 1
                    continue

                if from_country == to_country:
                    stats["skipped"] += 1
                    continue

                result = _classify_movement(from_country, to_country, "movement")

                cur.execute("""
                    INSERT INTO dbo.fba_stock_movement_ledger(
                        movement_ref, sku, asin, quantity,
                        movement_date, from_country, to_country,
                        movement_type, vat_treatment,
                        matching_pair_status, transport_evidence_status
                    ) VALUES (?, 'SHIPMENT', NULL, ?, ?, ?, ?, 'own_goods_transfer', ?, 'unmatched', 'missing')
                """, (
                    rec.get("shipment_id"),
                    rec.get("units_received") or 0,
                    rec.get("event_date"),
                    from_country,
                    to_country,
                    result["classification"],
                ))
                stats["created"] += 1

                # Also insert individual SKU-level movements from shipment lines
                cur.execute("""
                    SELECT sl.sku, sl.asin, sl.qty_received
                    FROM dbo.acc_fba_inbound_shipment_line sl WITH (NOLOCK)
                    WHERE sl.shipment_id = ?
                      AND sl.qty_received > 0
                """, (rec.get("shipment_id"),))
                lines = cur.fetchall()

                for line in lines:
                    line_sku = line[0]
                    line_asin = line[1]
                    line_qty = line[2] or 0
                    movement_ref = f"{rec.get('shipment_id')}_{line_sku}"

                    # Check if already exists
                    cur.execute("""
                        SELECT 1 FROM dbo.fba_stock_movement_ledger WITH (NOLOCK)
                        WHERE movement_ref = ?
                    """, (movement_ref,))
                    if cur.fetchone():
                        continue

                    cur.execute("""
                        INSERT INTO dbo.fba_stock_movement_ledger(
                            movement_ref, sku, asin, quantity,
                            movement_date, from_country, to_country,
                            movement_type, vat_treatment,
                            matching_pair_status, transport_evidence_status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'own_goods_transfer', ?, 'unmatched', 'missing')
                    """, (
                        movement_ref,
                        line_sku, line_asin, line_qty,
                        rec.get("event_date"),
                        from_country, to_country,
                        result["classification"],
                    ))

            except Exception as e:
                stats["errors"] += 1
                log.warning("sync_fba_movement.error", error=str(e))

            if stats["processed"] % 500 == 0:
                conn.commit()

        conn.commit()

        # Match WDT/WNT pairs
        matched = _match_movement_pairs(cur, conn)
        stats["pairs_matched"] = matched

        conn.commit()
        log.info("sync_fba_movements.done", **stats)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return stats


def _warehouse_to_country(warehouse_code: str) -> str | None:
    """Map Amazon warehouse code prefix to country."""
    prefix_map = {
        "FRA": "FR", "LIL": "FR", "MRS": "FR", "ORY": "FR",
        "BER": "DE", "CGN": "DE", "DUS": "DE", "FRA": "DE", "HAM": "DE",
        "LEJ": "DE", "MUC": "DE", "STR": "DE", "DTM": "DE", "EDE": "DE",
        "MIL": "IT", "FCO": "IT", "BLQ": "IT",
        "MAD": "ES", "BCN": "ES", "SVQ": "ES",
        "WRO": "PL", "KTW": "PL", "POZ": "PL", "SZZ": "PL",
        "PRG": "CZ", "BRQ": "CZ",
        "AMS": "NL", "RTM": "NL",
        "ARN": "SE", "GOT": "SE",
        "BRU": "BE", "LGG": "BE",
    }
    wh = warehouse_code.upper().strip()

    # Try 3-letter prefix
    for prefix, country in prefix_map.items():
        if wh.startswith(prefix):
            return country

    # Try 2-letter country code at start
    if len(wh) >= 2 and wh[:2] in EU_COUNTRIES:
        return wh[:2]

    return None


def _match_movement_pairs(cur, conn) -> int:
    """Match WDT outbound with WNT inbound movements."""
    cur.execute("""
        SELECT m1.id, m1.sku, m1.from_country, m1.to_country,
               m1.quantity, m1.movement_date
        FROM dbo.fba_stock_movement_ledger m1 WITH (NOLOCK)
        WHERE m1.matching_pair_status = 'unmatched'
          AND m1.vat_treatment = 'WDT_OWN_GOODS'
    """)
    wdt_rows = cur.fetchall()

    matched = 0
    for wdt in wdt_rows:
        wdt_id, sku, from_c, to_c, qty, mvdate = wdt

        # Find matching WNT (same SKU, reversed countries, similar date/quantity)
        cur.execute("""
            SELECT TOP 1 id
            FROM dbo.fba_stock_movement_ledger WITH (NOLOCK)
            WHERE sku = ? AND from_country = ? AND to_country = ?
              AND vat_treatment = 'WNT_OWN_GOODS'
              AND matching_pair_status = 'unmatched'
              AND ABS(quantity - ?) < 0.01
              AND ABS(DATEDIFF(day, movement_date, ?)) <= 7
        """, (sku, to_c, from_c, qty, mvdate))
        wnt = cur.fetchone()

        if wnt:
            wnt_id = wnt[0]
            cur.execute("""
                UPDATE dbo.fba_stock_movement_ledger
                SET matching_pair_status = 'matched'
                WHERE id IN (?, ?)
            """, (wdt_id, wnt_id))
            matched += 1

    conn.commit()
    return matched


# ═══════════════════════════════════════════════════════════════════
# Movement queries
# ═══════════════════════════════════════════════════════════════════

def get_fba_movements_summary(
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, Any]:
    """Overview of FBA movements and matching status."""
    conn = _connect()
    cur = conn.cursor()
    try:
        if not date_from:
            date_from = date.today() - timedelta(days=90)
        if not date_to:
            date_to = date.today()

        cur.execute("""
            SELECT
                vat_treatment,
                matching_pair_status,
                transport_evidence_status,
                COUNT(*) as cnt,
                SUM(quantity) as total_qty
            FROM dbo.fba_stock_movement_ledger WITH (NOLOCK)
            WHERE movement_date >= ? AND movement_date <= ?
            GROUP BY vat_treatment, matching_pair_status, transport_evidence_status
        """, (date_from, date_to))

        breakdown = _fetchall_dict(cur)

        cur.execute("""
            SELECT
                from_country, to_country,
                COUNT(*) as cnt,
                SUM(quantity) as total_qty
            FROM dbo.fba_stock_movement_ledger WITH (NOLOCK)
            WHERE movement_date >= ? AND movement_date <= ?
            GROUP BY from_country, to_country
            ORDER BY cnt DESC
        """, (date_from, date_to))

        routes = _fetchall_dict(cur)

        # Alert counts
        cur.execute("""
            SELECT COUNT(*) FROM dbo.fba_stock_movement_ledger WITH (NOLOCK)
            WHERE movement_date >= ? AND movement_date <= ?
              AND matching_pair_status = 'unmatched'
        """, (date_from, date_to))
        unmatched = cur.fetchone()[0] or 0

        cur.execute("""
            SELECT COUNT(*) FROM dbo.fba_stock_movement_ledger WITH (NOLOCK)
            WHERE movement_date >= ? AND movement_date <= ?
              AND transport_evidence_status = 'missing'
        """, (date_from, date_to))
        no_evidence = cur.fetchone()[0] or 0

        return {
            "breakdown": breakdown,
            "routes": routes,
            "alerts": {
                "unmatched_pairs": unmatched,
                "missing_evidence": no_evidence,
            },
        }
    finally:
        conn.close()


def list_fba_movements(
    date_from: date | None = None,
    date_to: date | None = None,
    vat_treatment: str | None = None,
    matching_status: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """List FBA movements with filters and pagination."""
    conn = _connect()
    cur = conn.cursor()
    try:
        if not date_from:
            date_from = date.today() - timedelta(days=90)
        if not date_to:
            date_to = date.today()

        where_parts = ["m.movement_date >= ?", "m.movement_date <= ?"]
        params: list = [date_from, date_to]

        if vat_treatment:
            where_parts.append("m.vat_treatment = ?")
            params.append(vat_treatment)
        if matching_status:
            where_parts.append("m.matching_pair_status = ?")
            params.append(matching_status)

        where_sql = " AND ".join(where_parts)
        offset = (page - 1) * page_size

        cur.execute(f"""
            SELECT COUNT(*) FROM dbo.fba_stock_movement_ledger m WITH (NOLOCK)
            WHERE {where_sql}
        """, tuple(params))
        total = cur.fetchone()[0] or 0

        cur.execute(f"""
            SELECT m.id, m.movement_ref, m.sku, m.asin,
                   m.quantity, m.movement_date,
                   m.from_country, m.to_country,
                   m.movement_type, m.vat_treatment,
                   m.matching_pair_status, m.transport_evidence_status,
                   m.created_at
            FROM dbo.fba_stock_movement_ledger m WITH (NOLOCK)
            WHERE {where_sql}
            ORDER BY m.movement_date DESC
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
