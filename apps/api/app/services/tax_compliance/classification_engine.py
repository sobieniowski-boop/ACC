"""
VAT Classification Engine.

Classifies every Amazon event into the correct VAT treatment:
  WSTO | LOCAL_VAT | WDT_OWN_GOODS | WNT_OWN_GOODS | B2B_WDT | OUT_OF_SCOPE | UNCLASSIFIED

Works with data already in acc_order_line / acc_finance_transaction.
"""
from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import structlog

from app.core.config import MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

# ── Marketplace → country mapping ────────────────────────────────
MKT_COUNTRY: dict[str, str] = {}
for _mid, _info in MARKETPLACE_REGISTRY.items():
    MKT_COUNTRY[_mid] = _info.get("code", "")

EU_COUNTRIES = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
    "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL",
    "PL", "PT", "RO", "SK", "SI", "ES", "SE",
}

# Amazon FBA warehouse countries we know about
FBA_WAREHOUSE_COUNTRIES = {"DE", "FR", "IT", "ES", "PL", "CZ", "NL", "SE", "BE"}


def _connect():
    return connect_acc(autocommit=False, timeout=30)


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


# ═══════════════════════════════════════════════════════════════════
# Classification logic
# ═══════════════════════════════════════════════════════════════════

def classify_event(
    *,
    event_type: str,
    ship_from_country: str | None,
    ship_to_country: str | None,
    warehouse_country: str | None,
    marketplace: str | None,
    is_b2b: bool = False,
    is_fba: bool = False,
    buyer_vat_number: str | None = None,
) -> dict[str, Any]:
    """
    Classify a single event.

    Returns dict with:
      classification, tax_jurisdiction, consumption_country,
      confidence_score, reason
    """
    sfrom = (ship_from_country or "").upper().strip()
    sto = (ship_to_country or "").upper().strip()
    wh = (warehouse_country or "").upper().strip()
    mkt_country = MKT_COUNTRY.get(marketplace or "", "").upper()

    # Infer missing fields
    if not sfrom and wh:
        sfrom = wh
    if not sfrom and is_fba and mkt_country:
        sfrom = mkt_country
    if not sfrom:
        sfrom = "PL"  # FBM default
    if not sto and mkt_country:
        sto = mkt_country
    if not wh and is_fba:
        wh = sfrom

    # ── Movement events ──────────────────────────────────────────
    if event_type in ("movement", "wdt_nontransactional", "wnt_nontransactional"):
        return _classify_movement(sfrom, sto, event_type)

    # ── Service fees (not a taxable sale) ────────────────────────
    if event_type == "service_fee":
        return {
            "classification": "OUT_OF_SCOPE",
            "tax_jurisdiction": None,
            "consumption_country": None,
            "confidence_score": 1.0,
            "reason": "Service fee – not a B2C/B2B sale event",
        }

    # ── B2B with VAT number → reverse charge / WDT ──────────────
    if is_b2b and buyer_vat_number and sfrom != sto:
        if sfrom in EU_COUNTRIES and sto in EU_COUNTRIES:
            return {
                "classification": "B2B_WDT",
                "tax_jurisdiction": f"B2B_{sfrom}",
                "consumption_country": sto,
                "confidence_score": 0.9,
                "reason": f"B2B intra-EU {sfrom}->{sto}, buyer VAT present, reverse charge applies",
            }

    # ── FBA: warehouse country == ship-to country → LOCAL_VAT ───
    if is_fba and wh and sto and wh == sto and wh != "PL":
        return {
            "classification": "LOCAL_VAT",
            "tax_jurisdiction": f"LOCAL_{wh}",
            "consumption_country": sto,
            "confidence_score": 0.95,
            "reason": f"FBA local sale: warehouse {wh} = destination {sto}",
        }

    # ── Cross-border B2C within EU → WSTO / OSS ─────────────────
    if sfrom in EU_COUNTRIES and sto in EU_COUNTRIES and sfrom != sto:
        return {
            "classification": "WSTO",
            "tax_jurisdiction": "OSS",
            "consumption_country": sto,
            "confidence_score": 0.95,
            "reason": f"Intra-EU B2C cross-border {sfrom}->{sto}, OSS applies",
        }

    # ── Domestic PL sale (FBM from PL to PL) ────────────────────
    if sfrom == "PL" and sto == "PL":
        return {
            "classification": "LOCAL_VAT",
            "tax_jurisdiction": "PL",
            "consumption_country": "PL",
            "confidence_score": 1.0,
            "reason": "Domestic PL sale",
        }

    # ── FBA local sale in warehouse country ──────────────────────
    if is_fba and wh and sto and wh == sto:
        return {
            "classification": "LOCAL_VAT",
            "tax_jurisdiction": f"LOCAL_{wh}",
            "consumption_country": sto,
            "confidence_score": 0.9,
            "reason": f"FBA local sale in {wh}",
        }

    # ── Fallback: same country ───────────────────────────────────
    if sfrom == sto and sfrom:
        return {
            "classification": "LOCAL_VAT",
            "tax_jurisdiction": f"LOCAL_{sfrom}",
            "consumption_country": sto,
            "confidence_score": 0.7,
            "reason": f"Same country {sfrom}, assumed local",
        }

    # ── Non-EU destination → out of scope ────────────────────────
    if sto and sto not in EU_COUNTRIES:
        return {
            "classification": "OUT_OF_SCOPE",
            "tax_jurisdiction": None,
            "consumption_country": sto,
            "confidence_score": 0.85,
            "reason": f"Destination {sto} is outside EU",
        }

    # ── Cannot classify ──────────────────────────────────────────
    return {
        "classification": "UNCLASSIFIED",
        "tax_jurisdiction": None,
        "consumption_country": sto or None,
        "confidence_score": 0.0,
        "reason": f"Insufficient data: from={sfrom} to={sto} wh={wh} fba={is_fba}",
    }


def _classify_movement(from_country: str, to_country: str, event_type: str) -> dict[str, Any]:
    """Classify FBA stock movement as WDT/WNT pair."""
    if from_country == to_country:
        return {
            "classification": "OUT_OF_SCOPE",
            "tax_jurisdiction": None,
            "consumption_country": None,
            "confidence_score": 0.9,
            "reason": f"Internal movement within {from_country}",
        }

    if from_country == "PL" and to_country in EU_COUNTRIES:
        return {
            "classification": "WDT_OWN_GOODS",
            "tax_jurisdiction": "PL",
            "consumption_country": to_country,
            "confidence_score": 0.95,
            "reason": f"Own goods transfer PL->{to_country}, WDT in PL",
        }

    if from_country in EU_COUNTRIES and to_country == "PL":
        return {
            "classification": "WNT_OWN_GOODS",
            "tax_jurisdiction": "PL",
            "consumption_country": "PL",
            "confidence_score": 0.95,
            "reason": f"Own goods return {from_country}->PL, WNT in PL",
        }

    if from_country in EU_COUNTRIES and to_country in EU_COUNTRIES:
        # Cross-border between two non-PL countries
        return {
            "classification": "WDT_OWN_GOODS",
            "tax_jurisdiction": f"LOCAL_{from_country}",
            "consumption_country": to_country,
            "confidence_score": 0.8,
            "reason": f"Own goods transfer {from_country}->{to_country}",
        }

    return {
        "classification": "UNCLASSIFIED",
        "tax_jurisdiction": None,
        "consumption_country": to_country or None,
        "confidence_score": 0.0,
        "reason": f"Cannot classify movement {from_country}->{to_country}",
    }


# ═══════════════════════════════════════════════════════════════════
# Batch classification job
# ═══════════════════════════════════════════════════════════════════

def get_vat_rate(country: str, event_date: date, cur) -> float | None:
    """Lookup default VAT rate for a country on a given date."""
    cur.execute("""
        SELECT TOP 1 rate FROM dbo.vat_rate_mapping WITH (NOLOCK)
        WHERE country = ? AND is_default = 1
          AND valid_from <= ?
          AND (valid_to IS NULL OR valid_to >= ?)
        ORDER BY valid_from DESC
    """, (country, event_date, event_date))
    row = cur.fetchone()
    return float(row[0]) if row else None


def classify_vat_events(
    date_from: date | None = None,
    date_to: date | None = None,
    reprocess: bool = False,
) -> dict[str, Any]:
    """
    Main classification job.

    Reads orders from acc_order_line, classifies each one,
    writes results to vat_event_ledger + vat_transaction_classification.

    Returns stats dict.
    """
    conn = _connect()
    cur = conn.cursor()
    stats = {
        "processed": 0, "classified": 0, "unclassified": 0,
        "wsto": 0, "local_vat": 0, "wdt": 0, "wnt": 0,
        "out_of_scope": 0, "b2b": 0, "errors": 0,
    }

    try:
        # Default: last 90 days
        if not date_from:
            date_from = date.today().replace(day=1)
            date_from = date_from.replace(month=max(1, date_from.month - 3))
        if not date_to:
            date_to = date.today()

        # If not reprocessing, skip already classified
        existing_filter = ""
        if not reprocess:
            existing_filter = """
                AND o.amazon_order_id NOT IN (
                    SELECT source_ref FROM dbo.vat_transaction_classification WITH (NOLOCK)
                    WHERE source_type = 'order'
                )
            """

        # Pull order data with shipping info
        cur.execute(f"""
            SELECT DISTINCT
                o.amazon_order_id,
                o.marketplace_id,
                ol.sku,
                ol.asin,
                ol.quantity_ordered as quantity,
                o.ship_country,
                o.buyer_country,
                o.purchase_date as order_date,
                ol.item_price,
                ol.item_tax,
                ol.currency,
                o.fulfillment_channel
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            WHERE o.purchase_date >= ? AND o.purchase_date <= ?
              AND o.status NOT IN ('Cancelled', 'Pending')
              {existing_filter}
            ORDER BY o.purchase_date
        """, (date_from, date_to))

        rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if cur.description else []

        for row in rows:
            rec = dict(zip(cols, row))
            stats["processed"] += 1

            try:
                order_id = str(rec.get("amazon_order_id") or "")
                mkt_id = str(rec.get("marketplace_id") or "")
                mkt_code = MKT_COUNTRY.get(mkt_id, "")
                # Use buyer_country first, then ship_country, then marketplace as fallback
                ship_to = str(rec.get("buyer_country") or rec.get("ship_country") or mkt_code).upper().strip()
                is_fba = str(rec.get("fulfillment_channel") or "").upper().startswith("AFN")
                wh_country = mkt_code if is_fba else "PL"
                ship_from = wh_country

                result = classify_event(
                    event_type="sale",
                    ship_from_country=ship_from,
                    ship_to_country=ship_to,
                    warehouse_country=wh_country,
                    marketplace=mkt_id,
                    is_b2b=False,
                    is_fba=is_fba,
                )

                classification = result["classification"]
                consumption_country = result.get("consumption_country")
                tax_jurisdiction = result.get("tax_jurisdiction")

                # Lookup VAT rate
                tax_rate = None
                if consumption_country:
                    tax_rate = get_vat_rate(consumption_country, rec.get("order_date") or date.today(), cur)

                item_price = _to_float(rec.get("item_price"))
                item_tax = _to_float(rec.get("item_tax"))
                gross = item_price + item_tax
                tax_base = item_price

                # Insert into vat_event_ledger
                cur.execute("""
                    INSERT INTO dbo.vat_event_ledger(
                        event_type, source_system, source_ref, order_id,
                        marketplace, sku, asin, quantity,
                        ship_from_country, ship_to_country,
                        warehouse_country, consumption_country,
                        vat_classification, tax_jurisdiction,
                        tax_rate, tax_base_amount, tax_amount, gross_amount,
                        currency, event_date, evidence_status,
                        confidence_score, notes
                    ) VALUES (
                        'sale', 'amazon', ?, ?,
                        ?, ?, ?, ?,
                        ?, ?,
                        ?, ?,
                        ?, ?,
                        ?, ?, ?, ?,
                        ?, ?, 'missing',
                        ?, ?
                    )
                """, (
                    order_id, order_id,
                    mkt_code, rec.get("sku"), rec.get("asin"), rec.get("quantity"),
                    ship_from, ship_to,
                    wh_country, consumption_country,
                    classification, tax_jurisdiction,
                    tax_rate, tax_base, item_tax, gross,
                    rec.get("currency"), rec.get("order_date"),
                    result.get("confidence_score", 0),
                    result.get("reason"),
                ))

                # Insert / update classification cache
                if reprocess:
                    cur.execute(
                        "DELETE FROM dbo.vat_transaction_classification WHERE source_ref = ? AND source_type = 'order'",
                        (order_id,),
                    )

                cur.execute("""
                    INSERT INTO dbo.vat_transaction_classification(
                        source_ref, source_type, classification,
                        reason_json, confidence_score, status
                    ) VALUES (?, 'order', ?, ?, ?, 'auto')
                """, (
                    order_id,
                    classification,
                    json.dumps(result, default=str),
                    result.get("confidence_score", 0),
                ))

                # Track stats
                stats["classified"] += 1
                key = classification.lower()
                if key == "wsto":
                    stats["wsto"] += 1
                elif key == "local_vat":
                    stats["local_vat"] += 1
                elif key == "wdt_own_goods":
                    stats["wdt"] += 1
                elif key == "wnt_own_goods":
                    stats["wnt"] += 1
                elif key == "out_of_scope":
                    stats["out_of_scope"] += 1
                elif key == "b2b_wdt":
                    stats["b2b"] += 1
                elif key == "unclassified":
                    stats["unclassified"] += 1

                    # Create compliance issue for unclassified
                    cur.execute("""
                        INSERT INTO dbo.compliance_issue(
                            issue_type, severity, source_ref,
                            marketplace, description, status
                        ) VALUES (
                            'vat_unclassified', 'P2', ?,
                            ?, ?, 'open'
                        )
                    """, (
                        order_id,
                        mkt_code,
                        f"Cannot classify VAT for order {order_id}: {result.get('reason')}",
                    ))

            except Exception as e:
                stats["errors"] += 1
                log.warning("classify_vat.row_error", order=rec.get("amazon_order_id"), error=str(e))

            # Commit in batches of 500
            if stats["processed"] % 500 == 0:
                conn.commit()

        conn.commit()
        log.info("classify_vat_events.done", **stats)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return stats


def classify_refunds(
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, Any]:
    """Classify refund events — inherit jurisdiction from original sale."""
    conn = _connect()
    cur = conn.cursor()
    stats = {"processed": 0, "classified": 0, "errors": 0}

    try:
        if not date_from:
            date_from = date.today().replace(day=1)
        if not date_to:
            date_to = date.today()

        cur.execute("""
            SELECT
                ft.amazon_order_id,
                ft.marketplace_id,
                ft.sku,
                ft.amount,
                ft.amount_pln,
                ft.currency,
                ft.posted_date,
                ft.charge_type
            FROM dbo.acc_finance_transaction ft WITH (NOLOCK)
            WHERE ft.transaction_type LIKE '%Refund%'
              AND ft.posted_date >= ? AND ft.posted_date <= ?
              AND ft.amazon_order_id IS NOT NULL
              AND ft.amazon_order_id != ''
              AND ft.amazon_order_id NOT IN (
                  SELECT source_ref FROM dbo.vat_event_ledger WITH (NOLOCK)
                  WHERE event_type = 'refund' AND source_ref = ft.amazon_order_id
              )
        """, (date_from, date_to))

        rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if cur.description else []

        for row in rows:
            rec = dict(zip(cols, row))
            stats["processed"] += 1

            try:
                order_id = str(rec.get("amazon_order_id") or "")
                mkt_code = MKT_COUNTRY.get(str(rec.get("marketplace_id") or ""), "")

                # Try to find original classification
                cur.execute("""
                    SELECT TOP 1 vat_classification, tax_jurisdiction, consumption_country,
                           ship_from_country, ship_to_country, warehouse_country
                    FROM dbo.vat_event_ledger WITH (NOLOCK)
                    WHERE order_id = ? AND event_type = 'sale'
                """, (order_id,))
                original = cur.fetchone()

                if original:
                    classification = original[0]
                    jurisdiction = original[1]
                    consumption = original[2]
                    sfrom = original[3]
                    sto = original[4]
                    wh = original[5]
                    confidence = 0.95
                    reason = f"Refund inherits classification from original sale: {classification}"
                else:
                    classification = "UNCLASSIFIED"
                    jurisdiction = None
                    consumption = None
                    sfrom = "PL"
                    sto = mkt_code
                    wh = None
                    confidence = 0.3
                    reason = f"Refund for {order_id} — original sale not found in VAT ledger"

                amount = _to_float(rec.get("amount"))
                posted = rec.get("posted_date")
                if hasattr(posted, "date"):
                    posted = posted.date()

                cur.execute("""
                    INSERT INTO dbo.vat_event_ledger(
                        event_type, source_system, source_ref, order_id,
                        marketplace, sku,
                        ship_from_country, ship_to_country,
                        warehouse_country, consumption_country,
                        vat_classification, tax_jurisdiction,
                        tax_base_amount, gross_amount,
                        currency, event_date,
                        confidence_score, notes
                    ) VALUES (
                        'refund', 'amazon', ?, ?,
                        ?, ?,
                        ?, ?,
                        ?, ?,
                        ?, ?,
                        ?, ?,
                        ?, ?,
                        ?, ?
                    )
                """, (
                    order_id, order_id,
                    mkt_code, rec.get("sku"),
                    sfrom, sto,
                    wh, consumption,
                    classification, jurisdiction,
                    amount, amount,
                    rec.get("currency"), posted,
                    confidence, reason,
                ))

                stats["classified"] += 1

            except Exception as e:
                stats["errors"] += 1
                log.warning("classify_refund.error", order=rec.get("amazon_order_id"), error=str(e))

            if stats["processed"] % 500 == 0:
                conn.commit()

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return stats


def override_classification(
    event_id: int,
    new_classification: str,
    reviewer: str,
) -> dict[str, Any]:
    """Manual override of a VAT event classification."""
    conn = _connect()
    cur = conn.cursor()

    try:
        cur.execute("""
            UPDATE dbo.vat_event_ledger
            SET vat_classification = ?,
                notes = CONCAT(ISNULL(notes, ''), ' | Override by ', ?, ' at ', CONVERT(NVARCHAR, SYSUTCDATETIME(), 126))
            WHERE id = ?
        """, (new_classification, reviewer, event_id))

        # Also update classification cache
        cur.execute("""
            SELECT source_ref FROM dbo.vat_event_ledger WHERE id = ?
        """, (event_id,))
        row = cur.fetchone()
        if row:
            source_ref = row[0]
            cur.execute("""
                UPDATE dbo.vat_transaction_classification
                SET classification = ?, status = 'overridden',
                    reviewed_by = ?, reviewed_at = SYSUTCDATETIME()
                WHERE source_ref = ?
            """, (new_classification, reviewer, source_ref))

        conn.commit()
        return {"updated": True, "event_id": event_id, "classification": new_classification}

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
