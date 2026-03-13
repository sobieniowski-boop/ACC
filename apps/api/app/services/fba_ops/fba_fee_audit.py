"""FBA Fee Audit — anomaly detection, overcharge estimation, dispute support.

Analyses ``acc_finance_transaction`` (charge_type = FBAPerUnitFulfillmentFee)
to detect sudden fee jumps per SKU that likely indicate Amazon dimension
reclassification errors.

Key features:
- Weekly sliding-window comparison of avg FBA fee per SKU
- Overcharge estimation vs. "normal" (historical baseline) fee
- Timeline view per SKU showing every individual charge
- Optional reference-rate comparison from ``acc_fba_fee_reference``
- Aggregated summary for dispute preparation

All SQL uses ``WITH (NOLOCK)`` — read-only, no writes.
"""
from __future__ import annotations

import structlog
from datetime import date, datetime
from typing import Any

from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)


def _load_fx_rates(cur) -> dict[str, float]:
    """Load latest exchange rates to EUR for currency normalisation."""
    rates: dict[str, float] = {"EUR": 1.0}
    try:
        cur.execute("""
            SELECT currency, rate_to_pln
            FROM (
                SELECT currency, rate_to_pln,
                       ROW_NUMBER() OVER (PARTITION BY currency ORDER BY rate_date DESC) rn
                FROM acc_exchange_rate WITH (NOLOCK)
            ) x WHERE rn = 1
        """)
        to_pln: dict[str, float] = {}
        for row in cur.fetchall():
            to_pln[row[0]] = float(row[1]) if row[1] else 0
        eur_to_pln = to_pln.get("EUR", 4.30)  # fallback
        for curr, pln_rate in to_pln.items():
            if pln_rate > 0 and eur_to_pln > 0:
                rates[curr] = pln_rate / eur_to_pln  # how many EUR is 1 unit of curr
    except Exception:
        pass
    return rates


def _to_eur(amount: float, currency: str, fx: dict[str, float]) -> float:
    """Convert amount in *currency* to EUR equivalent."""
    rate = fx.get(currency, 1.0)
    return amount * rate

# ---------------------------------------------------------------------------
#  FBA fee charge types (same list as profit_engine.py and order_pipeline.py)
# ---------------------------------------------------------------------------
_FBA_CHARGE_TYPES = (
    "FBAPerUnitFulfillmentFee",
    "FBAPerOrderFulfillmentFee",
    "FBAWeightBasedFee",
    "FBAPickAndPackFee",
)

_FBA_TYPES_SQL = ",".join(f"'{t}'" for t in _FBA_CHARGE_TYPES)


def _dictrow(cursor) -> list[dict[str, Any]]:
    """Convert cursor rows to list of dicts."""
    if not cursor.description:
        return []
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _dictone(cursor) -> dict[str, Any] | None:
    if not cursor.description:
        return None
    cols = [d[0] for d in cursor.description]
    row = cursor.fetchone()
    return dict(zip(cols, row)) if row else None


# ======================================================================
#  1. ANOMALY DETECTION — week-over-week fee jumps
# ======================================================================

def get_fee_anomalies(
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    marketplace_id: str | None = None,
    min_ratio: float = 1.5,
    min_orders: int = 2,
    lookback_days: int = 90,
) -> dict[str, Any]:
    """Detect SKUs where FBA fee jumped >min_ratio× week-over-week.

    Returns:
        {
          "anomalies": [...],
          "total_anomalies": int,
          "total_estimated_overcharge_eur": float,
          "scan_period": {"from": ..., "to": ...},
        }
    """
    conn = connect_acc()
    try:
        cur = conn.cursor()

        # Date bounds
        if not date_from:
            date_from = date.today()
            # go back lookback_days
        if not date_to:
            date_to = date.today()

        date_from_str = date_from.isoformat() if date_from else ""

        # Build optional marketplace filter
        market_filter = ""
        params: list[Any] = []

        # We'll use inline dates since compat cursor ? doesn't work well in CTEs
        # Use parameterised values where possible

        market_join = ""
        market_where = ""
        if marketplace_id:
            market_join = """
                INNER JOIN acc_order o WITH (NOLOCK)
                    ON o.amazon_order_id = ft.amazon_order_id
            """
            market_where = f"AND o.marketplace_id = '{marketplace_id}'"

        sql = f"""
            WITH weekly AS (
                SELECT
                    ft.sku,
                    DATEPART(ISO_WEEK, ft.posted_date) as wk,
                    DATEPART(YEAR, ft.posted_date) as yr,
                    MIN(CAST(ft.posted_date AS DATE)) as week_start,
                    MAX(CAST(ft.posted_date AS DATE)) as week_end,
                    COUNT(*) as order_cnt,
                    AVG(ABS(ft.amount)) as avg_fee,
                    MIN(ABS(ft.amount)) as min_fee,
                    MAX(ABS(ft.amount)) as max_fee,
                    ft.currency
                FROM acc_finance_transaction ft WITH (NOLOCK)
                {market_join}
                WHERE ft.charge_type IN ({_FBA_TYPES_SQL})
                  AND ft.sku IS NOT NULL
                  AND ft.posted_date >= DATEADD(DAY, -{lookback_days}, GETUTCDATE())
                  {market_where}
                GROUP BY ft.sku, DATEPART(ISO_WEEK, ft.posted_date),
                         DATEPART(YEAR, ft.posted_date), ft.currency
                HAVING COUNT(*) >= {min_orders}
            ),
            with_prev AS (
                SELECT *,
                    LAG(avg_fee) OVER (PARTITION BY sku, currency ORDER BY yr, wk) as prev_avg_fee,
                    LAG(week_start) OVER (PARTITION BY sku, currency ORDER BY yr, wk) as prev_week_start,
                    LAG(week_end) OVER (PARTITION BY sku, currency ORDER BY yr, wk) as prev_week_end,
                    LAG(order_cnt) OVER (PARTITION BY sku, currency ORDER BY yr, wk) as prev_order_cnt,
                    LAG(min_fee) OVER (PARTITION BY sku, currency ORDER BY yr, wk) as prev_min_fee,
                    LAG(max_fee) OVER (PARTITION BY sku, currency ORDER BY yr, wk) as prev_max_fee
                FROM weekly
            )
            SELECT
                sku,
                week_start,
                week_end,
                prev_week_start,
                prev_week_end,
                order_cnt,
                prev_order_cnt,
                avg_fee,
                prev_avg_fee,
                CASE WHEN prev_avg_fee > 0 THEN avg_fee / prev_avg_fee ELSE NULL END as fee_ratio,
                min_fee,
                max_fee,
                prev_min_fee,
                prev_max_fee,
                currency,
                -- estimated overcharge for this week's orders
                (avg_fee - prev_avg_fee) * order_cnt as overcharge_amount
            FROM with_prev
            WHERE prev_avg_fee IS NOT NULL
              AND prev_avg_fee > 0
              AND avg_fee / prev_avg_fee > {min_ratio}
            ORDER BY avg_fee / prev_avg_fee DESC
        """

        cur.execute(sql)
        anomalies_raw = _dictrow(cur)

        # Load FX rates for currency normalisation
        fx_rates = _load_fx_rates(cur)

        # Enrich with product info
        skus = list({a["sku"] for a in anomalies_raw})
        product_map: dict[str, dict] = {}
        if skus:
            # Batch lookup product info
            sku_list = ",".join(f"'{s}'" for s in skus[:200])
            cur.execute(f"""
                SELECT sku, asin, title, internal_sku, parent_asin
                FROM acc_product WITH (NOLOCK)
                WHERE sku IN ({sku_list})
            """)
            for r in _dictrow(cur):
                product_map[r["sku"]] = r

        # Build response — normalise overcharge to EUR
        total_overcharge_eur = 0.0
        per_currency_overcharge: dict[str, float] = {}
        anomalies = []
        for a in anomalies_raw:
            oc = float(a["overcharge_amount"] or 0)
            curr = str(a.get("currency") or "EUR")
            if oc > 0:
                per_currency_overcharge[curr] = per_currency_overcharge.get(curr, 0.0) + oc
                total_overcharge_eur += _to_eur(oc, curr, fx_rates)

            prod = product_map.get(a["sku"], {})
            anomalies.append({
                "sku": a["sku"],
                "asin": prod.get("asin"),
                "title": prod.get("title"),
                "internal_sku": prod.get("internal_sku"),
                "parent_asin": prod.get("parent_asin"),
                "currency": a["currency"],
                "current_period": {
                    "week_start": str(a["week_start"]),
                    "week_end": str(a["week_end"]),
                    "order_count": a["order_cnt"],
                    "avg_fee": round(float(a["avg_fee"]), 2),
                    "min_fee": round(float(a["min_fee"]), 2),
                    "max_fee": round(float(a["max_fee"]), 2),
                },
                "previous_period": {
                    "week_start": str(a["prev_week_start"]),
                    "week_end": str(a["prev_week_end"]) if a.get("prev_week_end") else None,
                    "order_count": a["prev_order_cnt"],
                    "avg_fee": round(float(a["prev_avg_fee"]), 2),
                    "min_fee": round(float(a["prev_min_fee"]), 2) if a.get("prev_min_fee") else None,
                    "max_fee": round(float(a["prev_max_fee"]), 2) if a.get("prev_max_fee") else None,
                },
                "fee_ratio": round(float(a["fee_ratio"]), 2),
                "estimated_overcharge": round(max(oc, 0), 2),
                "severity": (
                    "critical" if float(a["fee_ratio"]) > 3.0
                    else "high" if float(a["fee_ratio"]) > 2.0
                    else "medium"
                ),
                "recommendation": _build_recommendation(a),
            })

        return {
            "anomalies": anomalies,
            "total_anomalies": len(anomalies),
            "total_estimated_overcharge_eur": round(total_overcharge_eur, 2),
            "overcharge_by_currency": {
                k: round(v, 2) for k, v in sorted(per_currency_overcharge.items())
            },
            "scan_period": {
                "from": str(date.today() - __import__("datetime").timedelta(days=lookback_days)),
                "to": str(date.today()),
            },
        }
    finally:
        conn.close()


def _build_recommendation(anomaly: dict) -> str:
    """Generate actionable recommendation text."""
    ratio = float(anomaly["fee_ratio"])
    sku = anomaly["sku"]
    old = float(anomaly["prev_avg_fee"])
    new = float(anomaly["avg_fee"])
    curr = anomaly["currency"]

    if ratio > 3.0:
        return (
            f"KRYTYCZNE: Opłata FBA dla {sku} wzrosła {ratio:.1f}x "
            f"({old:.2f} → {new:.2f} {curr}). "
            f"Prawdopodobna przeklasyfikacja wymiarowa. "
            f"Otwórz case: Seller Central → FBA fulfillment fee dispute."
        )
    elif ratio > 2.0:
        return (
            f"WYSOKIE: Opłata FBA wzrosła {ratio:.1f}x ({old:.2f} → {new:.2f} {curr}). "
            f"Sprawdź wymiary produktu w Manage FBA Inventory. "
            f"Jeśli wymiary są błędne — złóż dispute."
        )
    else:
        return (
            f"ŚREDNIE: Opłata FBA wzrosła {ratio:.1f}x ({old:.2f} → {new:.2f} {curr}). "
            f"Monitoruj — może to sezonowa zmiana stawek."
        )


# ======================================================================
#  2. SKU TIMELINE — detailed fee history for a specific SKU
# ======================================================================

def get_sku_fee_timeline(
    sku: str,
    *,
    lookback_days: int = 180,
) -> dict[str, Any]:
    """Return chronological FBA fee history for a single SKU.

    Groups by day and shows individual orders, plus statistical summary.
    """
    conn = connect_acc()
    try:
        cur = conn.cursor()

        # Use inline SQL (no params) because compat cursor has issues with params in some contexts
        safe_sku = sku.replace("'", "''")

        cur.execute(f"""
            SELECT
                ft.amazon_order_id,
                CAST(ft.posted_date AS DATE) as posted_date,
                ft.charge_type,
                ABS(ft.amount) as fee_amount,
                ft.currency
            FROM acc_finance_transaction ft WITH (NOLOCK)
            WHERE ft.charge_type IN ({_FBA_TYPES_SQL})
              AND ft.sku = '{safe_sku}'
              AND ft.posted_date >= DATEADD(DAY, -{lookback_days}, GETUTCDATE())
            ORDER BY ft.posted_date
        """)
        charges = _dictrow(cur)

        if not charges:
            return {
                "sku": sku,
                "charges": [],
                "daily_summary": [],
                "statistics": None,
                "anomaly_periods": [],
            }

        # Product info
        cur.execute(f"""
            SELECT asin, title, internal_sku, parent_asin
            FROM acc_product WITH (NOLOCK)
            WHERE sku = '{safe_sku}'
        """)
        prod = _dictone(cur) or {}

        # Daily aggregation
        from collections import defaultdict
        daily: dict[str, list[float]] = defaultdict(list)
        for c in charges:
            day = str(c["posted_date"])
            daily[day].append(float(c["fee_amount"]))

        daily_summary = []
        for day in sorted(daily.keys()):
            fees = daily[day]
            daily_summary.append({
                "date": day,
                "order_count": len(fees),
                "avg_fee": round(sum(fees) / len(fees), 4),
                "min_fee": round(min(fees), 4),
                "max_fee": round(max(fees), 4),
            })

        # Statistics
        all_fees = [float(c["fee_amount"]) for c in charges]
        all_fees_sorted = sorted(all_fees)
        n = len(all_fees_sorted)
        median = all_fees_sorted[n // 2] if n % 2 == 1 else (all_fees_sorted[n // 2 - 1] + all_fees_sorted[n // 2]) / 2
        mean = sum(all_fees) / n
        p25 = all_fees_sorted[int(n * 0.25)]
        p75 = all_fees_sorted[int(n * 0.75)]
        iqr = p75 - p25

        # Detect anomaly periods: fees > p75 + 1.5*IQR (IQR method)
        upper_fence = p75 + 1.5 * iqr if iqr > 0 else p75 * 1.5
        anomaly_periods = []
        in_anomaly = False
        anom_start = None
        anom_fees: list[float] = []

        for ds in daily_summary:
            is_anomalous = ds["avg_fee"] > upper_fence and upper_fence > 0
            if is_anomalous and not in_anomaly:
                in_anomaly = True
                anom_start = ds["date"]
                anom_fees = [ds["avg_fee"]]
            elif is_anomalous and in_anomaly:
                anom_fees.append(ds["avg_fee"])
            elif not is_anomalous and in_anomaly:
                anomaly_periods.append({
                    "start_date": anom_start,
                    "end_date": ds["date"],
                    "days": len(anom_fees),
                    "avg_anomaly_fee": round(sum(anom_fees) / len(anom_fees), 2),
                    "normal_fee": round(median, 2),
                    "overcharge_per_unit": round(sum(anom_fees) / len(anom_fees) - median, 2),
                })
                in_anomaly = False
                anom_fees = []

        # Close open anomaly period
        if in_anomaly and anom_fees:
            anomaly_periods.append({
                "start_date": anom_start,
                "end_date": daily_summary[-1]["date"],
                "days": len(anom_fees),
                "avg_anomaly_fee": round(sum(anom_fees) / len(anom_fees), 2),
                "normal_fee": round(median, 2),
                "overcharge_per_unit": round(sum(anom_fees) / len(anom_fees) - median, 2),
                "ongoing": True,
            })

        return {
            "sku": sku,
            "asin": prod.get("asin"),
            "title": prod.get("title"),
            "internal_sku": prod.get("internal_sku"),
            "currency": charges[0]["currency"] if charges else "EUR",
            "charges": [
                {
                    "date": str(c["posted_date"]),
                    "order_id": c["amazon_order_id"],
                    "charge_type": c["charge_type"],
                    "fee": round(float(c["fee_amount"]), 4),
                }
                for c in charges
            ],
            "daily_summary": daily_summary,
            "statistics": {
                "total_orders": n,
                "mean_fee": round(mean, 4),
                "median_fee": round(median, 4),
                "p25_fee": round(p25, 4),
                "p75_fee": round(p75, 4),
                "iqr": round(iqr, 4),
                "upper_fence": round(upper_fence, 4),
                "min_fee": round(all_fees_sorted[0], 4),
                "max_fee": round(all_fees_sorted[-1], 4),
                "anomalous_orders": sum(1 for f in all_fees if f > upper_fence),
            },
            "anomaly_periods": anomaly_periods,
        }
    finally:
        conn.close()


# ======================================================================
#  3. OVERCHARGE SUMMARY — aggregate for dispute filing
# ======================================================================

def get_overcharge_summary(
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    marketplace_id: str | None = None,
    min_overcharge_eur: float = 1.0,
) -> dict[str, Any]:
    """Calculate total estimated FBA overcharges per SKU.

    For each SKU, the "normal" fee is the median of all charges.
    Any charge > median * 1.5 is considered an overcharge.
    The overcharge amount is (actual - median) summed.

    Returns dispute-ready data with order IDs.

    Uses a single-pass CTE approach to avoid N+1 per-SKU queries.
    """
    conn = connect_acc()
    try:
        cur = conn.cursor()

        # Date filters
        date_filter = ""
        if date_from:
            date_filter += f" AND ft.posted_date >= '{date_from.isoformat()}'"
        if date_to:
            date_filter += f" AND ft.posted_date <= '{date_to.isoformat()} 23:59:59'"

        market_join = ""
        market_where = ""
        if marketplace_id:
            market_join = """
                INNER JOIN acc_order o WITH (NOLOCK)
                    ON o.amazon_order_id = ft.amazon_order_id
            """
            market_where = f"AND o.marketplace_id = '{marketplace_id}'"

        # Load FX rates early
        fx_rates = _load_fx_rates(cur)

        # ── Single-pass: compute median per SKU via PERCENTILE_CONT,
        #    then identify overcharged orders (fee > median * 1.5) ──
        cur.execute(f"""
            WITH base AS (
                SELECT
                    ft.sku,
                    ft.currency,
                    ft.amazon_order_id,
                    CAST(ft.posted_date AS DATE) AS posted_date,
                    ABS(ft.amount)               AS fee_amount,
                    COUNT(*) OVER (PARTITION BY ft.sku, ft.currency) AS total_charges,
                    MIN(ABS(ft.amount)) OVER (PARTITION BY ft.sku, ft.currency)   AS min_fee,
                    MAX(ABS(ft.amount)) OVER (PARTITION BY ft.sku, ft.currency)   AS max_fee,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ABS(ft.amount))
                        OVER (PARTITION BY ft.sku, ft.currency)                   AS median_fee
                FROM acc_finance_transaction ft WITH (NOLOCK)
                {market_join}
                WHERE ft.charge_type IN ({_FBA_TYPES_SQL})
                  AND ft.sku IS NOT NULL
                  {date_filter}
                  {market_where}
            ),
            suspicious AS (
                SELECT *
                FROM base
                WHERE total_charges >= 5
                  AND max_fee > min_fee * 1.5
            )
            SELECT
                sku, currency, amazon_order_id, posted_date,
                fee_amount, total_charges, median_fee,
                median_fee * 1.5 AS threshold,
                CASE WHEN fee_amount > median_fee * 1.5
                     THEN fee_amount - median_fee ELSE 0 END AS excess
            FROM suspicious
            ORDER BY sku, posted_date
        """)
        rows = _dictrow(cur)

        if not rows:
            return {
                "items": [],
                "total_skus_affected": 0,
                "total_affected_orders": 0,
                "total_estimated_overcharge_eur": 0,
                "overcharge_by_currency": {},
                "scan_date": str(date.today()),
                "filters": {
                    "date_from": str(date_from) if date_from else None,
                    "date_to": str(date_to) if date_to else None,
                    "marketplace_id": marketplace_id,
                    "min_overcharge_eur": min_overcharge_eur,
                },
            }

        # Batch product lookup
        all_skus = list({r["sku"] for r in rows})
        product_map: dict[str, dict] = {}
        for i in range(0, len(all_skus), 200):
            batch = all_skus[i : i + 200]
            sku_list = ",".join(f"'{s.replace(chr(39), chr(39)+chr(39))}'" for s in batch)
            cur.execute(f"""
                SELECT sku, asin, title, internal_sku, parent_asin
                FROM acc_product WITH (NOLOCK) WHERE sku IN ({sku_list})
            """)
            for p in _dictrow(cur):
                product_map[p["sku"]] = p

        # Aggregate per SKU in Python (rows are already sorted by sku)
        from itertools import groupby
        results = []
        total_overcharge_eur = 0.0
        per_currency_overcharge: dict[str, float] = {}
        total_affected_orders = 0

        for (sku, currency), group in groupby(rows, key=lambda r: (r["sku"], r["currency"])):
            items = list(group)
            median_fee = float(items[0]["median_fee"])
            threshold = float(items[0]["threshold"])
            total_charges = int(items[0]["total_charges"])

            overcharged = [i for i in items if float(i["excess"]) > 0]
            sku_overcharge = sum(float(i["excess"]) for i in overcharged)
            sku_overcharge_eur = _to_eur(sku_overcharge, currency, fx_rates)

            if sku_overcharge_eur < min_overcharge_eur:
                continue

            per_currency_overcharge[currency] = per_currency_overcharge.get(currency, 0.0) + sku_overcharge
            total_overcharge_eur += sku_overcharge_eur
            total_affected_orders += len(overcharged)

            prod = product_map.get(sku, {})
            results.append({
                "sku": sku,
                "asin": prod.get("asin"),
                "title": prod.get("title"),
                "internal_sku": prod.get("internal_sku"),
                "currency": currency,
                "total_charges": total_charges,
                "median_fee": round(median_fee, 2),
                "threshold": round(threshold, 2),
                "overcharged_order_count": len(overcharged),
                "estimated_overcharge": round(sku_overcharge, 2),
                "estimated_overcharge_eur": round(sku_overcharge_eur, 2),
                "overcharged_orders": [
                    {
                        "order_id": i["amazon_order_id"],
                        "date": str(i["posted_date"]),
                        "actual_fee": round(float(i["fee_amount"]), 2),
                        "expected_fee": round(median_fee, 2),
                        "overcharge": round(float(i["excess"]), 2),
                    }
                    for i in overcharged[:50]
                ],
                "severity": (
                    "critical" if sku_overcharge_eur > 100
                    else "high" if sku_overcharge_eur > 20
                    else "medium"
                ),
            })

        results.sort(key=lambda x: x["estimated_overcharge_eur"], reverse=True)

        return {
            "items": results,
            "total_skus_affected": len(results),
            "total_affected_orders": total_affected_orders,
            "total_estimated_overcharge_eur": round(total_overcharge_eur, 2),
            "overcharge_by_currency": {
                k: round(v, 2) for k, v in sorted(per_currency_overcharge.items())
            },
            "scan_date": str(date.today()),
            "filters": {
                "date_from": str(date_from) if date_from else None,
                "date_to": str(date_to) if date_to else None,
                "marketplace_id": marketplace_id,
                "min_overcharge_eur": min_overcharge_eur,
            },
        }
    finally:
        conn.close()


# ======================================================================
#  4. FEE REFERENCE RATES — compare actual vs Amazon published rates
# ======================================================================

def get_fee_vs_reference(
    *,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    """Compare actual FBA fees with reference rates from acc_fba_fee_reference.

    Reference rates can be populated from Amazon's fee schedule
    (e.g. from a Google Sheets import).
    """
    conn = connect_acc()
    try:
        cur = conn.cursor()

        # Check if reference table exists
        cur.execute("""
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'acc_fba_fee_reference'
        """)
        if not cur.fetchone():
            return {
                "available": False,
                "message": "Tabela acc_fba_fee_reference nie istnieje. "
                           "Zaimportuj stawki referencyjne z Google Sheets.",
                "items": [],
            }

        market_filter = ""
        if marketplace_id:
            market_filter = f"AND ref.marketplace_id = '{marketplace_id}'"

        cur.execute(f"""
            SELECT
                ref.sku,
                ref.marketplace_id,
                ref.size_tier,
                ref.expected_fee_eur,
                ref.valid_from,
                ref.valid_to,
                ref.source,
                stats.avg_actual_fee,
                stats.min_actual_fee,
                stats.max_actual_fee,
                stats.total_orders,
                stats.avg_actual_fee - ref.expected_fee_eur as fee_delta,
                CASE WHEN ref.expected_fee_eur > 0
                     THEN (stats.avg_actual_fee - ref.expected_fee_eur) / ref.expected_fee_eur * 100
                     ELSE 0 END as delta_pct
            FROM acc_fba_fee_reference ref WITH (NOLOCK)
            CROSS APPLY (
                SELECT
                    AVG(ABS(ft.amount)) as avg_actual_fee,
                    MIN(ABS(ft.amount)) as min_actual_fee,
                    MAX(ABS(ft.amount)) as max_actual_fee,
                    COUNT(*) as total_orders
                FROM acc_finance_transaction ft WITH (NOLOCK)
                WHERE ft.charge_type IN ({_FBA_TYPES_SQL})
                  AND ft.sku = ref.sku
                  AND ft.posted_date >= ISNULL(ref.valid_from, DATEADD(DAY, -90, GETUTCDATE()))
                  AND ft.posted_date <= ISNULL(ref.valid_to, GETUTCDATE())
            ) stats
            WHERE stats.total_orders > 0
              {market_filter}
            ORDER BY ABS(stats.avg_actual_fee - ref.expected_fee_eur) DESC
        """)
        items = _dictrow(cur)

        return {
            "available": True,
            "items": [
                {
                    "sku": r["sku"],
                    "marketplace_id": r["marketplace_id"],
                    "size_tier": r["size_tier"],
                    "expected_fee_eur": round(float(r["expected_fee_eur"]), 2),
                    "avg_actual_fee": round(float(r["avg_actual_fee"]), 2),
                    "min_actual_fee": round(float(r["min_actual_fee"]), 2),
                    "max_actual_fee": round(float(r["max_actual_fee"]), 2),
                    "total_orders": r["total_orders"],
                    "fee_delta": round(float(r["fee_delta"]), 2),
                    "delta_pct": round(float(r["delta_pct"]), 1),
                }
                for r in items
            ],
            "total": len(items),
        }
    finally:
        conn.close()


# ======================================================================
#  5. ENSURE SCHEMA — create reference table if needed
# ======================================================================

def ensure_fee_audit_schema() -> None:
    """No-op — schema managed by Alembic migration eb022."""
