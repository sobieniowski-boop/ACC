"""ECB (European Central Bank) exchange rate connector — backup source.

Fetches EUR-based exchange rates from ECB's free XML feed.
Primary source is NBP (acc_exchange_rate). This feeds ecb_exchange_rate
as a secondary/backup source for cross-validation.

Reference: https://www.ecb.europa.eu/stats/eurofxref/
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import date
from typing import Optional

import httpx
import structlog

log = structlog.get_logger(__name__)

ECB_HIST_90D_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist-90d.xml"
ECB_DAILY_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"

# ECB XML namespace
NS = {"gesmes": "http://www.gesmes.org/xml/2002-08-01",
      "ecb": "http://www.ecb.int/vocabulary/2002-08-01/eurofxref"}


async def fetch_ecb_rates(
    days_back: int = 90,
) -> list[dict]:
    """
    Fetch EUR-based exchange rates from ECB XML feed.

    Returns list of dicts: {rate_date, source_currency, target_currency, rate}
    where source_currency is always "EUR" and target_currency is the foreign currency.

    Each rate represents: 1 EUR = N target_currency.

    Args:
        days_back: If <= 90, uses the 90-day feed. Otherwise uses daily (latest only).

    Returns:
        List of rate dicts, or empty list on error.
    """
    url = ECB_HIST_90D_URL if days_back <= 90 else ECB_DAILY_URL

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(url)
            resp.raise_for_status()
    except Exception as e:
        log.error("ecb.fetch_error", url=url, error=str(e))
        return []

    return _parse_ecb_xml(resp.text)


def _parse_ecb_xml(xml_text: str) -> list[dict]:
    """Parse ECB XML response into rate records."""
    rates: list[dict] = []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        log.error("ecb.xml_parse_error", error=str(e))
        return []

    # ECB XML structure:
    # <Cube>
    #   <Cube time="2026-03-07">
    #     <Cube currency="USD" rate="1.0832"/>
    #     <Cube currency="GBP" rate="0.84"/>
    #     ...
    #   </Cube>
    # </Cube>
    envelope = root.find(".//ecb:Cube", NS)
    if envelope is None:
        log.warning("ecb.no_cube_element")
        return []

    for day_cube in envelope.findall("ecb:Cube[@time]", NS):
        time_str = day_cube.get("time", "")
        try:
            rate_date = date.fromisoformat(time_str)
        except ValueError:
            continue

        for rate_cube in day_cube.findall("ecb:Cube[@currency]", NS):
            currency = rate_cube.get("currency", "").strip().upper()
            rate_str = rate_cube.get("rate", "")
            if not currency or not rate_str:
                continue
            try:
                rate_val = float(rate_str)
            except ValueError:
                continue

            rates.append({
                "rate_date": rate_date,
                "source_currency": "EUR",
                "target_currency": currency,
                "rate": round(rate_val, 6),
            })

    log.info("ecb.parsed", days=len(set(r["rate_date"] for r in rates)),
             currencies=len(set(r["target_currency"] for r in rates)),
             total_rates=len(rates))
    return rates
