"""Buy Box Radar — competitor intelligence & BuyBox win-rate analytics.

Captures competitor offer snapshots per ASIN, computes daily BuyBox
win-rate trends, and detects sustained BuyBox loss for alerting.

Sprint 11 – S11.2 / S11.3 / S11.4
Sprint 12 – S12.1 / S12.2 / S12.4

Tables:
  ``acc_competitor_offer``  — per-seller offer snapshots (price, FBA, BB winner)
  ``acc_buybox_trend``      — daily win-rate aggregation per SKU/marketplace

Win-rate model:
  For each (seller_sku, marketplace_id, date):
    snapshots_total = # pricing snapshots that day
    snapshots_won   = # where has_buybox = 1
    win_rate        = snapshots_won / snapshots_total * 100
"""
from __future__ import annotations

import json
from datetime import datetime, date, timezone, timedelta
from typing import Any

import structlog

from app.core.config import MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc

log = structlog.get_logger(__name__)


# ── Schema DDL ──────────────────────────────────────────────────────────

_SCHEMA_STATEMENTS: list[str] = [
    # 1. Competitor offer snapshots
    """
    IF OBJECT_ID('dbo.acc_competitor_offer', 'U') IS NULL
    CREATE TABLE dbo.acc_competitor_offer (
        id              BIGINT        IDENTITY(1,1) PRIMARY KEY,
        asin            VARCHAR(20)   NOT NULL,
        marketplace_id  VARCHAR(20)   NOT NULL,
        seller_id       VARCHAR(20)   NULL,
        is_our_offer    BIT           DEFAULT 0,
        listing_price   DECIMAL(12,2) NULL,
        shipping_price  DECIMAL(12,2) NULL,
        landed_price    DECIMAL(12,2) NULL,
        currency        VARCHAR(5)    DEFAULT 'EUR',
        is_buybox_winner BIT          DEFAULT 0,
        is_fba          BIT           DEFAULT 0,
        condition_type  VARCHAR(20)   DEFAULT 'New',
        seller_feedback_rating DECIMAL(4,2) NULL,
        seller_feedback_count  INT    NULL,
        observed_at     DATETIME2     DEFAULT SYSUTCDATETIME(),
        created_at      DATETIME2     DEFAULT SYSUTCDATETIME()
    )
    """,
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_comp_offer_asin_mkt'
          AND object_id = OBJECT_ID('dbo.acc_competitor_offer')
    )
    CREATE INDEX IX_comp_offer_asin_mkt
        ON dbo.acc_competitor_offer (asin, marketplace_id, observed_at DESC)
    """,
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_comp_offer_seller'
          AND object_id = OBJECT_ID('dbo.acc_competitor_offer')
    )
    CREATE INDEX IX_comp_offer_seller
        ON dbo.acc_competitor_offer (seller_id)
        WHERE seller_id IS NOT NULL
    """,
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_comp_offer_bb_winner'
          AND object_id = OBJECT_ID('dbo.acc_competitor_offer')
    )
    CREATE INDEX IX_comp_offer_bb_winner
        ON dbo.acc_competitor_offer (marketplace_id, is_buybox_winner)
        INCLUDE (asin, landed_price, seller_id)
    """,

    # 2. Daily BuyBox trend aggregation
    """
    IF OBJECT_ID('dbo.acc_buybox_trend', 'U') IS NULL
    CREATE TABLE dbo.acc_buybox_trend (
        id              BIGINT        IDENTITY(1,1) PRIMARY KEY,
        seller_sku      NVARCHAR(100) NOT NULL,
        asin            VARCHAR(20)   NULL,
        marketplace_id  VARCHAR(20)   NOT NULL,
        trend_date      DATE          NOT NULL,
        snapshots_total INT           DEFAULT 0,
        snapshots_won   INT           DEFAULT 0,
        win_rate        AS CASE WHEN snapshots_total > 0
                           THEN CAST(snapshots_won AS DECIMAL(5,2))
                                / snapshots_total * 100
                           ELSE 0 END PERSISTED,
        avg_our_price       DECIMAL(12,2) NULL,
        avg_buybox_price    DECIMAL(12,2) NULL,
        avg_price_gap_pct   DECIMAL(8,2)  NULL,
        num_competitors     INT           NULL,
        lowest_competitor_price DECIMAL(12,2) NULL,
        computed_at     DATETIME2     DEFAULT SYSUTCDATETIME(),

        CONSTRAINT UQ_buybox_trend_sku_mkt_date
            UNIQUE (seller_sku, marketplace_id, trend_date)
    )
    """,
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_bb_trend_date'
          AND object_id = OBJECT_ID('dbo.acc_buybox_trend')
    )
    CREATE INDEX IX_bb_trend_date
        ON dbo.acc_buybox_trend (trend_date)
    """,
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_bb_trend_mkt_winrate'
          AND object_id = OBJECT_ID('dbo.acc_buybox_trend')
    )
    CREATE INDEX IX_bb_trend_mkt_winrate
        ON dbo.acc_buybox_trend (marketplace_id, win_rate)
    """,
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_bb_trend_sku_mkt_date'
          AND object_id = OBJECT_ID('dbo.acc_buybox_trend')
    )
    CREATE INDEX IX_bb_trend_sku_mkt_date
        ON dbo.acc_buybox_trend (seller_sku, marketplace_id, trend_date DESC)
    """,
]


def ensure_buybox_radar_schema() -> None:
    """Create Buy Box Radar tables (idempotent)."""
    conn = connect_acc(autocommit=True)
    try:
        cur = conn.cursor()
        for stmt in _SCHEMA_STATEMENTS:
            cur.execute(stmt)
        log.info("buybox_radar.schema_ensured")
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Competitor offer storage
# ═══════════════════════════════════════════════════════════════════════════

def record_competitor_offers(
    asin: str,
    marketplace_id: str,
    offers: list[dict],
    *,
    our_seller_id: str | None = None,
) -> int:
    """Persist a batch of competitor offers for an ASIN.

    Each item in *offers* should have keys matching SP-API GetItemOffers
    response: SubCondition, ListingPrice, Shipping, IsBuyBoxWinner,
    IsFulfilledByAmazon, SellerId, SellerFeedbackRating.

    Returns the number of rows inserted.
    """
    if not offers:
        return 0

    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        inserted = 0
        for o in offers:
            seller_id = o.get("SellerId") or o.get("seller_id")
            listing_price = _extract_amount(o, "ListingPrice", "listing_price")
            shipping_price = _extract_amount(o, "Shipping", "shipping_price")
            landed = None
            if listing_price is not None:
                landed = listing_price + (shipping_price or 0)

            cur.execute(
                """
                INSERT INTO dbo.acc_competitor_offer
                    (asin, marketplace_id, seller_id, is_our_offer,
                     listing_price, shipping_price, landed_price, currency,
                     is_buybox_winner, is_fba, condition_type,
                     seller_feedback_rating, seller_feedback_count)
                VALUES (%s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s)
                """,
                (
                    asin, marketplace_id, seller_id,
                    1 if (our_seller_id and seller_id == our_seller_id) else 0,
                    listing_price, shipping_price, landed,
                    o.get("currency", "EUR"),
                    1 if o.get("IsBuyBoxWinner") or o.get("is_buybox_winner") else 0,
                    1 if o.get("IsFulfilledByAmazon") or o.get("is_fba") else 0,
                    o.get("SubCondition") or o.get("condition_type") or "New",
                    _safe_float(o.get("SellerFeedbackRating", {}).get("SellerPositiveFeedbackRating")
                                if isinstance(o.get("SellerFeedbackRating"), dict)
                                else o.get("seller_feedback_rating")),
                    _safe_int(o.get("SellerFeedbackRating", {}).get("FeedbackCount")
                              if isinstance(o.get("SellerFeedbackRating"), dict)
                              else o.get("seller_feedback_count")),
                ),
            )
            inserted += 1
        conn.commit()
        log.info("buybox_radar.competitor_offers_recorded",
                 asin=asin, marketplace_id=marketplace_id, count=inserted)
        return inserted
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_competitor_landscape(
    asin: str,
    marketplace_id: str,
    *,
    hours: int = 24,
) -> dict[str, Any]:
    """Return competitive landscape for an ASIN.

    Uses the most recent snapshot window (default 24h) to show:
    - Total unique sellers
    - FBA vs FBM split
    - Price distribution (min, max, avg, median)
    - Current BuyBox winner
    - Our position (if present)
    """
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            WITH latest AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY seller_id
                    ORDER BY observed_at DESC
                ) AS rn
                FROM dbo.acc_competitor_offer
                WHERE asin = %s
                  AND marketplace_id = %s
                  AND observed_at >= DATEADD(HOUR, -%s, SYSUTCDATETIME())
            )
            SELECT seller_id, is_our_offer, listing_price, shipping_price,
                   landed_price, is_buybox_winner, is_fba, condition_type,
                   seller_feedback_rating, seller_feedback_count, observed_at
            FROM latest
            WHERE rn = 1
            ORDER BY landed_price ASC
            """,
            (asin, marketplace_id, hours),
        )
        rows = cur.fetchall()

        if not rows:
            return {
                "asin": asin,
                "marketplace_id": marketplace_id,
                "total_sellers": 0,
                "sellers": [],
            }

        sellers = []
        prices = []
        buybox_winner = None
        our_position = None
        fba_count = 0

        for i, r in enumerate(rows):
            seller = {
                "seller_id": r[0],
                "is_our_offer": bool(r[1]),
                "listing_price": float(r[2]) if r[2] else None,
                "shipping_price": float(r[3]) if r[3] else None,
                "landed_price": float(r[4]) if r[4] else None,
                "is_buybox_winner": bool(r[5]),
                "is_fba": bool(r[6]),
                "condition_type": r[7],
                "seller_feedback_rating": float(r[8]) if r[8] else None,
                "seller_feedback_count": r[9],
                "observed_at": r[10].isoformat() if r[10] else None,
            }
            sellers.append(seller)
            if r[4] is not None:
                prices.append(float(r[4]))
            if r[5]:
                buybox_winner = seller
            if r[1]:
                our_position = i + 1
            if r[6]:
                fba_count += 1

        price_stats = {}
        if prices:
            prices_sorted = sorted(prices)
            mid = len(prices_sorted) // 2
            price_stats = {
                "min": prices_sorted[0],
                "max": prices_sorted[-1],
                "avg": round(sum(prices_sorted) / len(prices_sorted), 2),
                "median": prices_sorted[mid] if len(prices_sorted) % 2
                    else round((prices_sorted[mid - 1] + prices_sorted[mid]) / 2, 2),
            }

        return {
            "asin": asin,
            "marketplace_id": marketplace_id,
            "total_sellers": len(sellers),
            "fba_sellers": fba_count,
            "fbm_sellers": len(sellers) - fba_count,
            "price_stats": price_stats,
            "buybox_winner": buybox_winner,
            "our_position": our_position,
            "sellers": sellers,
        }
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  BuyBox win-rate trend computation
# ═══════════════════════════════════════════════════════════════════════════

def compute_daily_buybox_trends(
    target_date: date | None = None,
    marketplace_id: str | None = None,
) -> int:
    """Compute daily BuyBox win-rate per SKU from pricing snapshots.

    Aggregates acc_pricing_snapshot rows for *target_date* (default:
    yesterday) into acc_buybox_trend via MERGE upsert.

    Returns the number of SKU-marketplace rows upserted.
    """
    if target_date is None:
        target_date = (datetime.now(timezone.utc) - timedelta(days=1)).date()

    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = [str(target_date), str(target_date)]
        if marketplace_id:
            mkt_filter = "AND ps.marketplace_id = %s"
            params.append(marketplace_id)

        sql = f"""
        MERGE dbo.acc_buybox_trend AS tgt
        USING (
            SELECT
                ps.seller_sku,
                ps.asin,
                ps.marketplace_id,
                CAST(ps.observed_at AS DATE)              AS trend_date,
                COUNT(*)                                  AS snapshots_total,
                SUM(CAST(ISNULL(ps.has_buybox, 0) AS INT)) AS snapshots_won,
                AVG(ps.our_price)                         AS avg_our_price,
                AVG(ps.buybox_price)                      AS avg_buybox_price,
                AVG(ps.price_vs_buybox_pct)               AS avg_price_gap_pct,
                MAX(ps.num_offers_new)                    AS num_competitors,
                MIN(ps.lowest_price_new)                  AS lowest_competitor_price
            FROM dbo.acc_pricing_snapshot ps
            WHERE CAST(ps.observed_at AS DATE) = %s
              {mkt_filter}
            GROUP BY ps.seller_sku, ps.asin, ps.marketplace_id,
                     CAST(ps.observed_at AS DATE)
        ) AS src
        ON  tgt.seller_sku     = src.seller_sku
        AND tgt.marketplace_id = src.marketplace_id
        AND tgt.trend_date     = src.trend_date
        WHEN MATCHED THEN UPDATE SET
            tgt.asin                   = src.asin,
            tgt.snapshots_total        = src.snapshots_total,
            tgt.snapshots_won          = src.snapshots_won,
            tgt.avg_our_price          = src.avg_our_price,
            tgt.avg_buybox_price       = src.avg_buybox_price,
            tgt.avg_price_gap_pct      = src.avg_price_gap_pct,
            tgt.num_competitors        = src.num_competitors,
            tgt.lowest_competitor_price = src.lowest_competitor_price,
            tgt.computed_at            = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (
            seller_sku, asin, marketplace_id, trend_date,
            snapshots_total, snapshots_won,
            avg_our_price, avg_buybox_price, avg_price_gap_pct,
            num_competitors, lowest_competitor_price
        ) VALUES (
            src.seller_sku, src.asin, src.marketplace_id, src.trend_date,
            src.snapshots_total, src.snapshots_won,
            src.avg_our_price, src.avg_buybox_price, src.avg_price_gap_pct,
            src.num_competitors, src.lowest_competitor_price
        );
        SELECT @@ROWCOUNT;
        """

        cur.execute(sql, params)
        row = cur.fetchone()
        upserted = row[0] if row else 0
        conn.commit()
        log.info("buybox_radar.trends_computed",
                 date=str(target_date), upserted=upserted)
        return upserted
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_buybox_trends(
    seller_sku: str,
    marketplace_id: str,
    *,
    days: int = 30,
) -> list[dict]:
    """Return daily BuyBox win-rate trend for a specific SKU."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT trend_date, snapshots_total, snapshots_won, win_rate,
                   avg_our_price, avg_buybox_price, avg_price_gap_pct,
                   num_competitors, lowest_competitor_price
            FROM dbo.acc_buybox_trend
            WHERE seller_sku = %s
              AND marketplace_id = %s
              AND trend_date >= DATEADD(DAY, -%s, CAST(SYSUTCDATETIME() AS DATE))
            ORDER BY trend_date ASC
            """,
            (seller_sku, marketplace_id, days),
        )
        cols = [
            "trend_date", "snapshots_total", "snapshots_won", "win_rate",
            "avg_our_price", "avg_buybox_price", "avg_price_gap_pct",
            "num_competitors", "lowest_competitor_price",
        ]
        results = []
        for r in cur.fetchall():
            row = {}
            for i, c in enumerate(cols):
                v = r[i]
                if isinstance(v, date):
                    v = v.isoformat()
                elif isinstance(v, (float, int)):
                    v = float(v) if not isinstance(v, int) else v
                row[c] = v
            results.append(row)
        return results
    finally:
        conn.close()


def get_buybox_dashboard(
    marketplace_id: str | None = None,
    *,
    days: int = 7,
) -> dict[str, Any]:
    """Aggregate BuyBox stats across all SKUs.

    Returns:
      - overall_win_rate (avg across SKUs for the period)
      - total_skus, winning_skus, losing_skus
      - top_winners (best win rate), top_losers (worst)
      - trend direction (improving / declining / stable)
    """
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = [days]
        if marketplace_id:
            mkt_filter = "AND marketplace_id = %s"
            params.append(marketplace_id)

        # Per-SKU averages over the window
        cur.execute(
            f"""
            SELECT seller_sku, marketplace_id, asin,
                   AVG(win_rate) AS avg_win_rate,
                   SUM(snapshots_total) AS total_snaps,
                   SUM(snapshots_won) AS total_won
            FROM dbo.acc_buybox_trend
            WHERE trend_date >= DATEADD(DAY, -%s, CAST(SYSUTCDATETIME() AS DATE))
              {mkt_filter}
            GROUP BY seller_sku, marketplace_id, asin
            """,
            params,
        )
        sku_rows = cur.fetchall()

        total_skus = len(sku_rows)
        if total_skus == 0:
            return {
                "marketplace_id": marketplace_id,
                "days": days,
                "total_skus": 0,
                "overall_win_rate": 0,
                "winning_skus": 0,
                "losing_skus": 0,
                "at_risk_skus": 0,
                "top_winners": [],
                "top_losers": [],
                "trend_direction": "stable",
            }

        sku_data = []
        for r in sku_rows:
            sku_data.append({
                "seller_sku": r[0],
                "marketplace_id": r[1],
                "asin": r[2],
                "avg_win_rate": float(r[3]) if r[3] else 0,
                "total_snaps": r[4] or 0,
                "total_won": r[5] or 0,
            })

        all_rates = [s["avg_win_rate"] for s in sku_data]
        overall = round(sum(all_rates) / len(all_rates), 2) if all_rates else 0
        winning = sum(1 for r in all_rates if r >= 70)
        losing = sum(1 for r in all_rates if r < 30)
        at_risk = sum(1 for r in all_rates if 30 <= r < 70)

        sorted_by_rate = sorted(sku_data, key=lambda x: x["avg_win_rate"], reverse=True)
        top_winners = sorted_by_rate[:5]
        top_losers = sorted_by_rate[-5:][::-1] if len(sorted_by_rate) > 5 else []

        # Trend direction: compare first half vs second half of window
        trend_direction = _compute_trend_direction(cur, days, marketplace_id)

        return {
            "marketplace_id": marketplace_id,
            "days": days,
            "total_skus": total_skus,
            "overall_win_rate": overall,
            "winning_skus": winning,
            "losing_skus": losing,
            "at_risk_skus": at_risk,
            "top_winners": top_winners,
            "top_losers": top_losers,
            "trend_direction": trend_direction,
        }
    finally:
        conn.close()


def _compute_trend_direction(
    cur: Any,
    days: int,
    marketplace_id: str | None,
) -> str:
    """Compare avg win rate of first vs second half of window."""
    half = days // 2
    mkt_filter = ""
    params: list[Any] = [days, half]
    if marketplace_id:
        mkt_filter = "AND marketplace_id = %s"
        params.extend([marketplace_id, marketplace_id])

    cur.execute(
        f"""
        SELECT
            AVG(CASE WHEN trend_date < DATEADD(DAY, -%s, CAST(SYSUTCDATETIME() AS DATE))
                     THEN win_rate END) AS first_half,
            AVG(CASE WHEN trend_date >= DATEADD(DAY, -%s, CAST(SYSUTCDATETIME() AS DATE))
                     THEN win_rate END) AS second_half
        FROM dbo.acc_buybox_trend
        WHERE trend_date >= DATEADD(DAY, -%s, CAST(SYSUTCDATETIME() AS DATE))
          {mkt_filter}
        """,
        [days, half, days] + ([marketplace_id] if marketplace_id else []),
    )
    row = cur.fetchone()
    if not row or row[0] is None or row[1] is None:
        return "stable"
    first_half = float(row[0])
    second_half = float(row[1])
    diff = second_half - first_half
    if diff > 3:
        return "improving"
    elif diff < -3:
        return "declining"
    return "stable"


# ═══════════════════════════════════════════════════════════════════════════
#  Sustained BuyBox loss detection & alerting
# ═══════════════════════════════════════════════════════════════════════════

SUSTAINED_LOSS_THRESHOLD_DAYS = 3


def detect_sustained_buybox_losses(
    marketplace_id: str | None = None,
    *,
    threshold_days: int = SUSTAINED_LOSS_THRESHOLD_DAYS,
) -> list[dict]:
    """Find SKUs that have lost BuyBox for *threshold_days* consecutive days.

    Queries acc_buybox_trend for SKUs where win_rate < 5 for the last
    N days continuously. Returns list of dicts with SKU details.
    """
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = [threshold_days, threshold_days]
        if marketplace_id:
            mkt_filter = "AND t.marketplace_id = %s"
            params.append(marketplace_id)

        cur.execute(
            f"""
            WITH recent AS (
                SELECT seller_sku, marketplace_id, asin, trend_date, win_rate,
                       ROW_NUMBER() OVER (
                           PARTITION BY seller_sku, marketplace_id
                           ORDER BY trend_date DESC
                       ) AS rn
                FROM dbo.acc_buybox_trend
                WHERE trend_date >= DATEADD(DAY, -%s, CAST(SYSUTCDATETIME() AS DATE))
            ),
            consecutive_losses AS (
                SELECT seller_sku, marketplace_id, asin,
                       COUNT(*) AS loss_days,
                       MIN(trend_date) AS loss_start,
                       MAX(trend_date) AS loss_end,
                       AVG(win_rate) AS avg_win_rate
                FROM recent
                WHERE rn <= %s AND win_rate < 5
                {mkt_filter}
                GROUP BY seller_sku, marketplace_id, asin
                HAVING COUNT(*) >= %s
            )
            SELECT cl.seller_sku, cl.marketplace_id, cl.asin,
                   cl.loss_days, cl.loss_start, cl.loss_end, cl.avg_win_rate
            FROM consecutive_losses cl
            ORDER BY cl.loss_days DESC, cl.avg_win_rate ASC
            """,
            params + [threshold_days],
        )
        results = []
        for r in cur.fetchall():
            results.append({
                "seller_sku": r[0],
                "marketplace_id": r[1],
                "asin": r[2],
                "consecutive_loss_days": r[3],
                "loss_start": r[4].isoformat() if r[4] else None,
                "loss_end": r[5].isoformat() if r[5] else None,
                "avg_win_rate": float(r[6]) if r[6] is not None else 0,
            })
        return results
    finally:
        conn.close()


def raise_sustained_loss_alerts(
    marketplace_id: str | None = None,
    *,
    threshold_days: int = SUSTAINED_LOSS_THRESHOLD_DAYS,
) -> int:
    """Detect sustained BB losses and write to acc_system_alert.

    Returns number of alerts raised.
    """
    losses = detect_sustained_buybox_losses(
        marketplace_id, threshold_days=threshold_days,
    )
    if not losses:
        return 0

    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        count = 0
        for loss in losses:
            # Skip if an unresolved alert already exists for this SKU
            cur.execute(
                """
                SELECT TOP 1 id FROM dbo.acc_system_alert
                WHERE alert_type = 'buybox_sustained_loss'
                  AND details LIKE %s
                  AND created_at >= DATEADD(DAY, -1, SYSUTCDATETIME())
                """,
                (f'%"seller_sku": "{loss["seller_sku"]}"%',),
            )
            if cur.fetchone():
                continue

            detail = json.dumps({
                "seller_sku": loss["seller_sku"],
                "marketplace_id": loss["marketplace_id"],
                "asin": loss["asin"],
                "consecutive_loss_days": loss["consecutive_loss_days"],
                "loss_start": loss["loss_start"],
                "loss_end": loss["loss_end"],
                "avg_win_rate": loss["avg_win_rate"],
            })

            cur.execute(
                """
                INSERT INTO dbo.acc_system_alert
                    (alert_type, severity, message, details)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    "buybox_sustained_loss",
                    "warning" if loss["consecutive_loss_days"] < 5 else "critical",
                    f"BuyBox lost for {loss['consecutive_loss_days']} consecutive days: "
                    f"{loss['seller_sku']} on {loss['marketplace_id']}",
                    detail,
                ),
            )
            count += 1
        conn.commit()
        log.info("buybox_radar.sustained_loss_alerts", count=count)
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_buybox_alerts(
    marketplace_id: str | None = None,
    *,
    days: int = 7,
    limit: int = 50,
) -> list[dict]:
    """Return recent BuyBox loss alerts from acc_system_alert."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        mkt_clause = ""
        params: list[Any] = [days, limit]
        if marketplace_id:
            mkt_clause = "AND details LIKE %s"
            params.insert(1, f'%"{marketplace_id}"%')

        cur.execute(
            f"""
            SELECT TOP (%s) id, alert_type, severity, message, details, created_at
            FROM dbo.acc_system_alert
            WHERE alert_type = 'buybox_sustained_loss'
              AND created_at >= DATEADD(DAY, -%s, SYSUTCDATETIME())
              {mkt_clause}
            ORDER BY created_at DESC
            """,
            [limit, days] + ([f'%"{marketplace_id}"%'] if marketplace_id else []),
        )
        results = []
        for r in cur.fetchall():
            detail_parsed = None
            if r[4]:
                try:
                    detail_parsed = json.loads(r[4])
                except (json.JSONDecodeError, TypeError):
                    detail_parsed = r[4]

            results.append({
                "id": r[0],
                "alert_type": r[1],
                "severity": r[2],
                "message": r[3],
                "details": detail_parsed,
                "created_at": r[5].isoformat() if r[5] else None,
            })
        return results
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Rolling win-rate summaries (7d / 30d / 90d)
# ═══════════════════════════════════════════════════════════════════════════

def get_rolling_win_rates(
    seller_sku: str,
    marketplace_id: str,
) -> dict[str, float | None]:
    """Return rolling BuyBox win-rate for 7, 30, 90 day windows."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                AVG(CASE WHEN trend_date >= DATEADD(DAY, -7, CAST(SYSUTCDATETIME() AS DATE))
                         THEN win_rate END) AS win_rate_7d,
                AVG(CASE WHEN trend_date >= DATEADD(DAY, -30, CAST(SYSUTCDATETIME() AS DATE))
                         THEN win_rate END) AS win_rate_30d,
                AVG(CASE WHEN trend_date >= DATEADD(DAY, -90, CAST(SYSUTCDATETIME() AS DATE))
                         THEN win_rate END) AS win_rate_90d
            FROM dbo.acc_buybox_trend
            WHERE seller_sku = %s AND marketplace_id = %s
            """,
            (seller_sku, marketplace_id),
        )
        row = cur.fetchone()
        if not row:
            return {"win_rate_7d": None, "win_rate_30d": None, "win_rate_90d": None}
        return {
            "win_rate_7d": round(float(row[0]), 2) if row[0] is not None else None,
            "win_rate_30d": round(float(row[1]), 2) if row[1] is not None else None,
            "win_rate_90d": round(float(row[2]), 2) if row[2] is not None else None,
        }
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _extract_amount(
    offer: dict, sp_api_key: str, flat_key: str,
) -> float | None:
    """Extract price amount from SP-API nested dict or flat key."""
    val = offer.get(sp_api_key)
    if isinstance(val, dict):
        amt = val.get("Amount") or val.get("amount")
        return float(amt) if amt is not None else None
    val = offer.get(flat_key)
    return float(val) if val is not None else None


def _safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_int(val: Any) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 12 — Async competitor capture service
# ═══════════════════════════════════════════════════════════════════════════

async def capture_competitor_offers(
    marketplace_id: str,
    *,
    asin_limit: int = 50,
    our_seller_id: str | None = None,
) -> dict[str, Any]:
    """Fetch individual offers for top ASINs and store competitor data.

    Calls SP-API ``GetItemOffers`` for up to *asin_limit* ASINs with
    the most offers (highest competitive pressure) and records each
    seller's offer in ``acc_competitor_offer``.

    Returns summary dict with counts.
    """
    import asyncio
    from app.connectors.amazon_sp_api.pricing_api import PricingClient

    # Get top ASINs by offer count from recent pricing snapshots
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP (%s) asin, MAX(num_offers_new) AS max_offers
            FROM dbo.acc_pricing_snapshot
            WHERE marketplace_id = %s
              AND asin IS NOT NULL AND asin != ''
              AND observed_at >= DATEADD(DAY, -7, SYSUTCDATETIME())
            GROUP BY asin
            ORDER BY MAX(num_offers_new) DESC
            """,
            (asin_limit, marketplace_id),
        )
        asins = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    if not asins:
        return {"marketplace_id": marketplace_id, "asins_sampled": 0,
                "offers_recorded": 0, "status": "no_asins"}

    client = PricingClient(marketplace_id=marketplace_id)
    total_offers = 0
    errors = 0

    for asin in asins:
        try:
            result = await client.get_item_offers(asin)
            offers_list = result.get("Offers", [])
            if offers_list:
                recorded = await asyncio.to_thread(
                    record_competitor_offers,
                    asin, marketplace_id, offers_list,
                    our_seller_id=our_seller_id,
                )
                total_offers += recorded
            # Respect SP-API rate limit (0.5 req/s)
            await asyncio.sleep(2.0)
        except Exception as exc:
            log.warning("buybox_radar.capture_error", asin=asin, error=str(exc))
            errors += 1

    log.info("buybox_radar.capture_complete",
             marketplace_id=marketplace_id,
             asins=len(asins), offers=total_offers, errors=errors)
    return {
        "marketplace_id": marketplace_id,
        "asins_sampled": len(asins),
        "offers_recorded": total_offers,
        "errors": errors,
        "status": "ok",
    }


def record_competitor_offers_from_notification(
    asin: str,
    marketplace_id: str,
    notification_payload: dict,
    *,
    our_seller_id: str | None = None,
) -> int:
    """Extract individual offers from ANY_OFFER_CHANGED and record them.

    The notification payload may contain an ``offers`` array with per-seller
    offer details. This function converts them to our storage format and
    calls ``record_competitor_offers``.
    """
    raw_offers = notification_payload.get("offers") or notification_payload.get("Offers") or []
    if not raw_offers:
        return 0
    return record_competitor_offers(
        asin, marketplace_id, raw_offers,
        our_seller_id=our_seller_id,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  Sprint 12 — Landscape API enhancements
# ═══════════════════════════════════════════════════════════════════════════

def get_competitor_price_history(
    asin: str,
    marketplace_id: str,
    *,
    days: int = 30,
    seller_id: str | None = None,
) -> list[dict]:
    """Return daily aggregated competitor price snapshots for an ASIN."""
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        seller_filter = ""
        params: list[Any] = [asin, marketplace_id, days]
        if seller_id:
            seller_filter = "AND seller_id = %s"
            params.append(seller_id)

        cur.execute(
            f"""
            SELECT
                CAST(observed_at AS DATE) AS snap_date,
                COUNT(DISTINCT seller_id) AS unique_sellers,
                MIN(landed_price) AS min_price,
                MAX(landed_price) AS max_price,
                AVG(landed_price) AS avg_price,
                SUM(CASE WHEN is_fba = 1 THEN 1 ELSE 0 END) AS fba_offers,
                SUM(CASE WHEN is_fba = 0 THEN 1 ELSE 0 END) AS fbm_offers
            FROM dbo.acc_competitor_offer
            WHERE asin = %s
              AND marketplace_id = %s
              AND observed_at >= DATEADD(DAY, -%s, SYSUTCDATETIME())
              {seller_filter}
            GROUP BY CAST(observed_at AS DATE)
            ORDER BY snap_date ASC
            """,
            params,
        )
        results = []
        for r in cur.fetchall():
            results.append({
                "date": r[0].isoformat() if r[0] else None,
                "unique_sellers": r[1],
                "min_price": float(r[2]) if r[2] is not None else None,
                "max_price": float(r[3]) if r[3] is not None else None,
                "avg_price": round(float(r[4]), 2) if r[4] is not None else None,
                "fba_offers": r[5],
                "fbm_offers": r[6],
            })
        return results
    finally:
        conn.close()


def get_landscape_overview(
    marketplace_id: str | None = None,
    *,
    hours: int = 24,
    limit: int = 50,
) -> list[dict]:
    """Return competitive landscape summary across all tracked ASINs.

    For each ASIN: total sellers, FBA/FBM split, price range, BB winner.
    Sorted by total sellers descending (most competitive first).
    """
    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        mkt_filter = ""
        params: list[Any] = [hours, limit]
        if marketplace_id:
            mkt_filter = "AND co.marketplace_id = %s"
            params.append(marketplace_id)

        cur.execute(
            f"""
            WITH recent AS (
                SELECT co.asin, co.marketplace_id, co.seller_id,
                       co.landed_price, co.is_fba, co.is_buybox_winner,
                       ROW_NUMBER() OVER (
                           PARTITION BY co.asin, co.marketplace_id, co.seller_id
                           ORDER BY co.observed_at DESC
                       ) AS rn
                FROM dbo.acc_competitor_offer co
                WHERE co.observed_at >= DATEADD(HOUR, -%s, SYSUTCDATETIME())
                  {mkt_filter}
            )
            SELECT TOP (%s)
                r.asin, r.marketplace_id,
                COUNT(DISTINCT r.seller_id) AS total_sellers,
                SUM(CASE WHEN r.is_fba = 1 THEN 1 ELSE 0 END) AS fba_count,
                MIN(r.landed_price) AS min_price,
                MAX(r.landed_price) AS max_price,
                AVG(r.landed_price) AS avg_price,
                MAX(CASE WHEN r.is_buybox_winner = 1 THEN r.seller_id END) AS bb_winner
            FROM recent r
            WHERE r.rn = 1
            GROUP BY r.asin, r.marketplace_id
            ORDER BY COUNT(DISTINCT r.seller_id) DESC
            """,
            [hours] + ([marketplace_id] if marketplace_id else []) + [limit],
        )
        results = []
        for r in cur.fetchall():
            results.append({
                "asin": r[0],
                "marketplace_id": r[1],
                "total_sellers": r[2],
                "fba_sellers": r[3],
                "fbm_sellers": r[2] - r[3],
                "min_price": float(r[4]) if r[4] is not None else None,
                "max_price": float(r[5]) if r[5] is not None else None,
                "avg_price": round(float(r[6]), 2) if r[6] is not None else None,
                "buybox_winner_seller_id": r[7],
            })
        return results
    finally:
        conn.close()
