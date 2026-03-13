"""Central FX (foreign exchange) service.

Single source of truth for currency → PLN conversion rates.
Replaces all hardcoded fallback dicts scattered across modules.

Features:
    - Loads rates from ``acc_exchange_rate`` (cached in-process, 1-hour TTL).
    - Returns the most recent rate for a given currency / date.
    - Circuit-breaker: warns when the latest rate is > 24 h old,
      raises ``StaleFxRateError`` when > 7 days old.
    - Generates the SQL ``CASE currency WHEN …`` fragment dynamically
      so raw-SQL queries always use DB-sourced rates.
"""
from __future__ import annotations

import bisect
import time
from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

import structlog

from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)

# ── Circuit-breaker thresholds ──────────────────────────────────────
_WARN_STALENESS_DAYS = 1     # log WARNING if newest rate > 1 day old
_BREAK_STALENESS_DAYS = 7    # treat as stale → raise if no override


class StaleFxRateError(Exception):
    """Raised when FX rates are older than the circuit-breaker threshold."""


# ── In-process cache ────────────────────────────────────────────────
_cache: Dict[str, Any] = {"loaded_at": 0.0, "rates": {}, "latest_dates": {}}
_CACHE_TTL = 3600  # 1 hour


def _load_cache() -> Dict[str, List[Tuple[str, float]]]:
    """Load all exchange rates into memory, grouped by currency.

    Returns ``{currency: [(date_str, rate), …]}`` sorted ASC by date.
    """
    now = time.monotonic()
    if now - _cache["loaded_at"] < _CACHE_TTL and _cache["rates"]:
        return _cache["rates"]

    conn = connect_acc(timeout=15)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT currency,
                   CONVERT(VARCHAR(10), rate_date, 120) AS rate_date,
                   rate_to_pln
            FROM dbo.acc_exchange_rate WITH (NOLOCK)
            ORDER BY currency, rate_date
        """)
        rates: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
        latest: Dict[str, str] = {}
        for row in cur.fetchall():
            ccy = str(row[0])
            d = str(row[1])
            r = float(row[2])
            rates[ccy].append((d, r))
            latest[ccy] = d
        _cache["rates"] = dict(rates)
        _cache["latest_dates"] = latest
        _cache["loaded_at"] = now
        log.info("fx_service.cache_loaded", currencies=len(rates),
                 total_rates=sum(len(v) for v in rates.values()))
        return _cache["rates"]
    except Exception as exc:
        log.error("fx_service.cache_load_failed", error=str(exc))
        # Return whatever we had before (possibly stale but better than nothing)
        return _cache["rates"]
    finally:
        conn.close()


def invalidate_cache() -> None:
    """Force cache reload on next call (e.g. after sync_exchange_rates)."""
    _cache["loaded_at"] = 0.0


# ── Public API ──────────────────────────────────────────────────────

def get_rate(currency: str, for_date: str | date | None = None) -> float:
    """Return PLN exchange rate for *currency* on or before *for_date*.

    Uses binary search on the cached rate series.

    Raises ``StaleFxRateError`` if the newest available rate is older
    than ``_BREAK_STALENESS_DAYS``.
    """
    if not currency or currency == "PLN":
        return 1.0

    cache = _load_cache()
    series = cache.get(currency, [])

    if not series:
        log.error("fx_service.no_rates", currency=currency)
        raise StaleFxRateError(
            f"No exchange rates found for {currency} in acc_exchange_rate"
        )

    # Check staleness against today regardless of for_date
    latest_date_str = _cache["latest_dates"].get(currency, "")
    if latest_date_str:
        _check_staleness(currency, latest_date_str)

    # Resolve target date
    if for_date is None:
        date_str = date.today().isoformat()
    elif isinstance(for_date, date):
        date_str = for_date.isoformat()
    else:
        date_str = str(for_date)[:10]

    # Binary search for largest date <= date_str (series sorted ASC)
    idx = bisect.bisect_right(series, (date_str, float("inf"))) - 1
    if idx >= 0:
        return series[idx][1]

    # for_date is before the earliest rate → use earliest
    log.warning("fx_service.before_earliest_rate",
                currency=currency, requested=date_str,
                earliest=series[0][0])
    return series[0][1]


def get_latest_rate(currency: str) -> float:
    """Return the most recent rate on file for *currency*."""
    if not currency or currency == "PLN":
        return 1.0

    cache = _load_cache()
    series = cache.get(currency, [])
    if not series:
        raise StaleFxRateError(
            f"No exchange rates found for {currency}"
        )
    _check_staleness(currency, series[-1][0])
    return series[-1][1]


class FxRateMissingError(Exception):
    """Raised when no FX rate exists and no fallback is possible."""


def get_rate_safe(currency: str, for_date: str | date | None = None) -> float:
    """Like ``get_rate`` but never raises — returns last known rate or raises clearly.

    Uses the last known rate when the current date is stale, logging a
    warning. Raises ``FxRateMissingError`` when absolutely no rate data
    exists for a currency — this replaces the silent 1.0 fallback (SF-02 fix).
    """
    if not currency or currency == "PLN":
        return 1.0
    try:
        return get_rate(currency, for_date)
    except StaleFxRateError:
        cache = _load_cache()
        series = cache.get(currency, [])
        if series:
            rate = series[-1][1]
            log.warning("fx_service.stale_fallback",
                        currency=currency, rate=rate,
                        last_date=series[-1][0])
            return rate
        log.error("fx_service.no_rate_data",
                  currency=currency,
                  for_date=str(for_date),
                  msg="No FX rate data found. Refusing to use 1.0 fallback — this would "
                      "silently corrupt financial data (SF-02).")
        raise FxRateMissingError(
            f"No exchange rate data for {currency}. Cannot convert to PLN."
        )


def build_fx_case_sql(currency_column: str = "o.currency") -> str:
    """Generate a SQL CASE expression using the latest DB rates.

    Returns something like::

        CASE o.currency
            WHEN 'EUR' THEN 4.3012 WHEN 'GBP' THEN 5.2150 ...
            WHEN 'PLN' THEN 1.0 ELSE 1.0
        END

    This replaces the old hardcoded CASE blocks.
    """
    cache = _load_cache()
    parts = [f"CASE {currency_column}"]
    for ccy, series in sorted(cache.items()):
        if series:
            rate = series[-1][1]  # latest rate
            parts.append(f"WHEN '{ccy}' THEN {rate}")
    parts.append("WHEN 'PLN' THEN 1.0 ELSE NULL END")  # NULL forces callers to handle unknown currencies (SF-02)
    return "\n                            ".join(parts)


# ── Internal helpers ────────────────────────────────────────────────

def _check_staleness(currency: str, latest_date_str: str) -> None:
    """Warn or raise depending on how old the latest rate is."""
    try:
        latest = date.fromisoformat(latest_date_str)
    except (ValueError, TypeError):
        return
    age_days = (date.today() - latest).days
    if age_days > _BREAK_STALENESS_DAYS:
        log.error("fx_service.circuit_breaker",
                  currency=currency, last_rate_date=latest_date_str,
                  age_days=age_days)
        raise StaleFxRateError(
            f"FX rate for {currency} is {age_days} days old "
            f"(last: {latest_date_str}). Circuit breaker triggered."
        )
    if age_days > _WARN_STALENESS_DAYS:
        log.warning("fx_service.stale_rate",
                    currency=currency, last_rate_date=latest_date_str,
                    age_days=age_days)
