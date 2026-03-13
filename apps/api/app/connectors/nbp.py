"""NBP (Narodowy Bank Polski) exchange rates API connector.

Fetches official PLN exchange rates from NBP Table A (mid rates).
Free API, no auth required. Rate limit: reasonable (~10 req/s).

Reference: https://api.nbp.pl/
"""
from __future__ import annotations

import asyncio
from datetime import date, timedelta
from typing import Optional

import httpx
import structlog

log = structlog.get_logger(__name__)

NBP_API_BASE = "https://api.nbp.pl/api/exchangerates"


async def fetch_nbp_rate(
    currency: str,
    rate_date: Optional[date] = None,
) -> Optional[float]:
    """
    Fetch single exchange rate: 1 currency = N PLN (mid rate, Table A).

    If the exact date is not available (weekend, holiday),
    tries up to 5 previous business days.

    Args:
        currency: ISO 4217 code (EUR, GBP, SEK, etc.)
        rate_date: Date for which to fetch rate. Defaults to today.

    Returns:
        Exchange rate (float) or None if not found.
    """
    if currency == "PLN":
        return 1.0

    if rate_date is None:
        rate_date = date.today()

    # Try the requested date, fall back up to 5 days
    for delta in range(6):
        check_date = rate_date - timedelta(days=delta)
        url = f"{NBP_API_BASE}/rates/a/{currency.lower()}/{check_date}/"

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url, params={"format": "json"})

                if resp.status_code == 200:
                    data = resp.json()
                    rates = data.get("rates", [])
                    if rates:
                        mid = rates[0].get("mid")
                        log.debug("nbp.rate_ok", currency=currency, date=str(check_date), rate=mid)
                        return float(mid)

                elif resp.status_code == 404:
                    # No rate for this date (weekend/holiday) — try previous day
                    continue
                else:
                    log.warning("nbp.unexpected_status", status=resp.status_code, currency=currency)

        except Exception as e:
            log.error("nbp.fetch_error", currency=currency, date=str(check_date), error=str(e))

    log.warning("nbp.rate_not_found", currency=currency, date=str(rate_date))
    return None


async def fetch_nbp_rates_range(
    currency: str,
    date_from: date,
    date_to: date,
) -> list[tuple[date, float]]:
    """
    Fetch exchange rates for a date range.

    NBP API supports up to 367 days per request.
    Returns list of (date, rate) tuples.
    """
    if currency == "PLN":
        return [(date_from, 1.0)]

    results: list[tuple[date, float]] = []
    chunk_start = date_from

    while chunk_start <= date_to:
        chunk_end = min(chunk_start + timedelta(days=365), date_to)
        url = (
            f"{NBP_API_BASE}/rates/a/{currency.lower()}"
            f"/{chunk_start}/{chunk_end}/"
        )

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(url, params={"format": "json"})

                if resp.status_code == 200:
                    data = resp.json()
                    for rate_entry in data.get("rates", []):
                        rd = date.fromisoformat(rate_entry["effectiveDate"])
                        results.append((rd, float(rate_entry["mid"])))

                elif resp.status_code == 404:
                    log.debug("nbp.no_data_range", currency=currency,
                              start=str(chunk_start), end=str(chunk_end))

        except Exception as e:
            log.error("nbp.range_error", currency=currency, error=str(e))

        chunk_start = chunk_end + timedelta(days=1)
        await asyncio.sleep(0.2)  # Rate limit courtesy

    log.info("nbp.range_fetched", currency=currency, count=len(results))
    return results


async def fetch_all_currencies(
    currencies: list[str],
    rate_date: Optional[date] = None,
) -> dict[str, float]:
    """
    Fetch today's rates for multiple currencies.

    Returns dict: {currency: rate_to_pln}
    Always includes PLN=1.0.
    """
    if rate_date is None:
        rate_date = date.today()

    rates: dict[str, float] = {"PLN": 1.0}

    for currency in currencies:
        if currency == "PLN":
            continue
        rate = await fetch_nbp_rate(currency, rate_date)
        if rate is not None:
            rates[currency] = rate
        else:
            from app.core.fx_service import get_rate_safe
            rates[currency] = get_rate_safe(currency, rate_date)
            log.warning("nbp.using_db_fallback", currency=currency, rate=rates[currency])

        await asyncio.sleep(0.1)

    return rates
