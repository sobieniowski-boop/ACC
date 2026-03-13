"""Check ads flow through rollup to EDM."""
import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env")
from app.core.db_connection import connect_acc

c = connect_acc(autocommit=False, timeout=30)
cur = c.cursor()

def q(sql):
    cur.execute(sql)
    return cur.fetchall()

# 1. Rollup columns
print("=== ACC_SKU_PROFITABILITY_ROLLUP columns ===")
cols = [r[0] for r in q("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='acc_sku_profitability_rollup' ORDER BY ORDINAL_POSITION")]
print(f"  {cols}")

# 2. Rollup ads totals
print("\n=== ROLLUP ADS BY MONTH ===")
for r in q("""
    SELECT CONVERT(VARCHAR(7), period_date, 120) m,
           SUM(CAST(ISNULL(ad_spend_pln, 0) AS FLOAT)) ads,
           SUM(CAST(ISNULL(revenue_pln, 0) AS FLOAT)) rev,
           COUNT(DISTINCT asin) asins
    FROM acc_sku_profitability_rollup WITH (NOLOCK)
    WHERE period_date >= '2026-01-01' AND period_date < '2026-04-01'
    GROUP BY CONVERT(VARCHAR(7), period_date, 120) ORDER BY m
"""):
    rev = float(r[2] or 0)
    ads = float(r[1] or 0)
    print(f"  {r[0]}: ads={ads:>12,.0f} | rev={rev:>12,.0f} | asins={r[3]} | ads/rev={ads/max(rev,1)*100:.1f}%")

# 3. What table sources rollup? Check where it's written
print("\n=== ROLLUP sample row ===")
for r in q("SELECT TOP 3 * FROM acc_sku_profitability_rollup WITH (NOLOCK) WHERE ad_spend_pln > 0 ORDER BY period_date DESC"):
    print(f"  {dict(zip(cols, r))}")

# 4. Compare: product_day total vs rollup total vs EDM total for Jan
print("\n=== JAN 2026 ADS: 3 SOURCES ===")
pd_total = q("SELECT SUM(CAST(spend_pln AS FLOAT)) FROM acc_ads_product_day WITH (NOLOCK) WHERE report_date >= '2026-01-01' AND report_date < '2026-02-01'")[0][0]
cd_total = q("SELECT SUM(CAST(spend_pln AS FLOAT)) FROM acc_ads_campaign_day WITH (NOLOCK) WHERE report_date >= '2026-01-01' AND report_date < '2026-02-01'")[0][0]
rollup_total = q("SELECT SUM(CAST(ad_spend_pln AS FLOAT)) FROM acc_sku_profitability_rollup WITH (NOLOCK) WHERE period_date >= '2026-01-01' AND period_date < '2026-02-01'")[0][0]
edm_total = q("SELECT SUM(CAST(ad_spend_pln AS FLOAT)) FROM executive_daily_metrics WITH (NOLOCK) WHERE period_date >= '2026-01-01' AND period_date < '2026-02-01'")[0][0]

print(f"  campaign_day:  {float(cd_total or 0):>12,.0f}")
print(f"  product_day:   {float(pd_total or 0):>12,.0f}")
print(f"  rollup:        {float(rollup_total or 0):>12,.0f}")
print(f"  EDM:           {float(edm_total or 0):>12,.0f}")
print(f"  Gap (cd-edm):  {float(cd_total or 0)-float(edm_total or 0):>12,.0f}")

# 5. Which ASINs have ads but no orders (or vice versa)?
print("\n=== ASINS WITH ADS BUT LOW ROLLUP ASSIGNMENT (Jan) ===")
for r in q("""
    SELECT TOP 10 pd.asin, pd.marketplace_id,
           SUM(CAST(pd.spend_pln AS FLOAT)) product_day_spend,
           ISNULL((SELECT SUM(CAST(r.ad_spend_pln AS FLOAT))
                   FROM acc_sku_profitability_rollup r WITH (NOLOCK)
                   WHERE r.asin = pd.asin AND r.marketplace_id = pd.marketplace_id
                     AND r.period_date >= '2026-01-01' AND r.period_date < '2026-02-01'), 0) rollup_spend
    FROM acc_ads_product_day pd WITH (NOLOCK)
    WHERE pd.report_date >= '2026-01-01' AND pd.report_date < '2026-02-01'
    GROUP BY pd.asin, pd.marketplace_id
    HAVING SUM(CAST(pd.spend_pln AS FLOAT)) > 100
    ORDER BY SUM(CAST(pd.spend_pln AS FLOAT)) -
             ISNULL((SELECT SUM(CAST(r.ad_spend_pln AS FLOAT))
                     FROM acc_sku_profitability_rollup r WITH (NOLOCK)
                     WHERE r.asin = pd.asin AND r.marketplace_id = pd.marketplace_id
                       AND r.period_date >= '2026-01-01' AND r.period_date < '2026-02-01'), 0) DESC
"""):
    gap = float(r[2] or 0) - float(r[3] or 0)
    print(f"  {r[0]} ({r[1]}): product_day={float(r[2] or 0):>8,.0f} | rollup={float(r[3] or 0):>8,.0f} | gap={gap:>8,.0f}")

c.close()
print("\nDONE")
