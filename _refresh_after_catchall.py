"""
After catch-all: recalculate CM2/NP in rollup, refresh marketplace rollup + EDM.
"""
import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env")

from datetime import date
from app.core.db_connection import connect_acc

date_from = date(2026, 1, 1)
date_to = date(2026, 3, 10)

conn = connect_acc(autocommit=False, timeout=120)
cur = conn.cursor()

# ── EDM BEFORE ──
cur.execute("""
    SELECT CONVERT(VARCHAR(7), period_date, 120) m,
           SUM(CAST(ad_spend_pln AS FLOAT)) ads,
           SUM(CAST(cm2_pln AS FLOAT)) cm2,
           SUM(CAST(profit_pln AS FLOAT)) np
    FROM dbo.executive_daily_metrics WITH (NOLOCK)
    WHERE period_date >= ? AND period_date <= ?
    GROUP BY CONVERT(VARCHAR(7), period_date, 120)
    ORDER BY m
""", (date_from, date_to))
print("EDM BEFORE:")
for r in cur.fetchall():
    print(f"  {r[0]}: ads={float(r[1]):>10,.0f} | cm2={float(r[2]):>10,.0f} | np={float(r[3]):>10,.0f}")

# ── Step 1: Recalculate profitability columns in SKU rollup ──
print("\nRecalculating cm1/cm2/profit_pln in SKU rollup...")
cur.execute("""
    UPDATE dbo.acc_sku_profitability_rollup SET
        cm1_pln = revenue_pln - cogs_pln - amazon_fees_pln
                  - fba_fees_pln - logistics_pln,
        cm2_pln = revenue_pln - cogs_pln - amazon_fees_pln
                  - fba_fees_pln - logistics_pln
                  - ad_spend_pln - refund_pln - storage_fee_pln - other_fees_pln,
        profit_pln = revenue_pln - cogs_pln - amazon_fees_pln
                     - fba_fees_pln - logistics_pln - ad_spend_pln
                     - refund_pln - storage_fee_pln - other_fees_pln,
        margin_pct = CASE WHEN revenue_pln <> 0
            THEN (revenue_pln - cogs_pln - amazon_fees_pln
                  - fba_fees_pln - logistics_pln - ad_spend_pln
                  - refund_pln - storage_fee_pln - other_fees_pln)
                 / revenue_pln * 100
            ELSE 0 END,
        computed_at = SYSUTCDATETIME()
    WHERE period_date >= ? AND period_date <= ?
""", (date_from, date_to))
print(f"  SKU rollup rows updated: {cur.rowcount}")
conn.commit()

# ── Step 2: Refresh marketplace rollup ──
print("\nRefreshing marketplace rollup...")
cur.execute("""
    UPDATE tgt SET
        tgt.ad_spend_pln = src.ad_spend_pln,
        tgt.cm1_pln = src.cm1_pln,
        tgt.cm2_pln = src.cm2_pln,
        tgt.profit_pln = src.profit_pln,
        tgt.margin_pct = CASE WHEN src.revenue_pln <> 0
            THEN src.profit_pln / src.revenue_pln * 100 ELSE 0 END,
        tgt.acos_pct = CASE WHEN src.revenue_pln <> 0
            THEN src.ad_spend_pln / src.revenue_pln * 100 ELSE NULL END,
        tgt.computed_at = SYSUTCDATETIME()
    FROM dbo.acc_marketplace_profitability_rollup tgt
    JOIN (
        SELECT
            r.period_date,
            r.marketplace_id,
            SUM(r.revenue_pln) as revenue_pln,
            SUM(r.ad_spend_pln) as ad_spend_pln,
            SUM(r.cm1_pln) as cm1_pln,
            SUM(r.cm2_pln) as cm2_pln,
            SUM(r.profit_pln) as profit_pln
        FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
        WHERE r.period_date >= ? AND r.period_date <= ?
        GROUP BY r.period_date, r.marketplace_id
    ) src
      ON src.period_date = tgt.period_date
     AND src.marketplace_id = tgt.marketplace_id
    WHERE tgt.period_date >= ? AND tgt.period_date <= ?
""", (date_from, date_to, date_from, date_to))
print(f"  Marketplace rollup rows updated: {cur.rowcount}")
conn.commit()

# ── Step 3: Refresh EDM from rollup ──
print("\nRefreshing executive_daily_metrics...")
cur.execute("""
    UPDATE tgt SET
        tgt.ad_spend_pln = src.ad_spend_pln,
        tgt.cm1_pln = src.cm1_pln,
        tgt.cm2_pln = src.cm2_pln,
        tgt.profit_pln = src.profit_pln,
        tgt.margin_pct = CASE WHEN src.revenue_pln <> 0
            THEN src.profit_pln / src.revenue_pln * 100 ELSE 0 END,
        tgt.acos_pct = CASE WHEN src.revenue_pln > 0
            THEN src.ad_spend_pln / src.revenue_pln * 100 ELSE NULL END,
        tgt.computed_at = SYSUTCDATETIME()
    FROM dbo.executive_daily_metrics tgt
    JOIN (
        SELECT period_date, marketplace_id,
               SUM(revenue_pln) as revenue_pln,
               SUM(ad_spend_pln) as ad_spend_pln,
               SUM(cm1_pln) as cm1_pln,
               SUM(cm2_pln) as cm2_pln,
               SUM(profit_pln) as profit_pln
        FROM dbo.acc_sku_profitability_rollup WITH (NOLOCK)
        WHERE period_date >= ? AND period_date <= ?
        GROUP BY period_date, marketplace_id
    ) src
      ON src.period_date = tgt.period_date
     AND src.marketplace_id = tgt.marketplace_id
    WHERE tgt.period_date >= ? AND tgt.period_date <= ?
""", (date_from, date_to, date_from, date_to))
print(f"  EDM rows updated: {cur.rowcount}")
conn.commit()

# ── EDM AFTER ──
cur.execute("""
    SELECT CONVERT(VARCHAR(7), period_date, 120) m,
           SUM(CAST(ad_spend_pln AS FLOAT)) ads,
           SUM(CAST(cm2_pln AS FLOAT)) cm2,
           SUM(CAST(profit_pln AS FLOAT)) np
    FROM dbo.executive_daily_metrics WITH (NOLOCK)
    WHERE period_date >= ? AND period_date <= ?
    GROUP BY CONVERT(VARCHAR(7), period_date, 120)
    ORDER BY m
""", (date_from, date_to))
print("\nEDM AFTER:")
for r in cur.fetchall():
    print(f"  {r[0]}: ads={float(r[1]):>10,.0f} | cm2={float(r[2]):>10,.0f} | np={float(r[3]):>10,.0f}")

cur.close()
conn.close()
print("\nDONE")
