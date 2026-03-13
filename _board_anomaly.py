"""Investigate CM1 anomaly: why does margin vary so much across months?"""
import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env")
from app.core.db_connection import connect_acc

c = connect_acc(autocommit=False, timeout=60)
cur = c.cursor()

def q(sql):
    cur.execute(sql)
    return cur.fetchall()

# 1. Break down CM1 components per month from acc_order_line
print("=== ORDER LINE FEE BREAKDOWN (Jan-Mar 2026, Shipped) ===")
for r in q("""
    SELECT CONVERT(VARCHAR(7), o.purchase_date, 120) m,
           COUNT(*) lines,
           SUM(CAST(ISNULL(ol.fba_fee_pln, 0) AS FLOAT)) fba,
           SUM(CAST(ISNULL(ol.referral_fee_pln, 0) AS FLOAT)) ref
    FROM acc_order_line ol WITH (NOLOCK)
    JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    WHERE o.purchase_date >= '2026-01-01' AND o.purchase_date < '2026-04-01'
      AND o.status = 'Shipped'
    GROUP BY CONVERT(VARCHAR(7), o.purchase_date, 120) ORDER BY m
"""):
    print(f"  {r[0]}: lines={r[1]:>6,} | fba_fee={float(r[2] or 0):>12,.0f} | referral_fee={float(r[3] or 0):>12,.0f} | total_fees={float(r[2] or 0) + float(r[3] or 0):>12,.0f}")

# 2. Logistics per order average
print("\n=== LOGISTICS PER ORDER AVERAGE ===")
for r in q("""
    SELECT CONVERT(VARCHAR(7), o.purchase_date, 120) m,
           COUNT(DISTINCT CASE WHEN olf.amazon_order_id IS NOT NULL THEN o.amazon_order_id END) matched,
           COUNT(DISTINCT o.amazon_order_id) total,
           SUM(CAST(ISNULL(olf.total_logistics_pln, 0) AS FLOAT)) logi,
           CASE WHEN COUNT(DISTINCT CASE WHEN olf.amazon_order_id IS NOT NULL THEN o.amazon_order_id END) > 0
                THEN SUM(CAST(ISNULL(olf.total_logistics_pln, 0) AS FLOAT)) / COUNT(DISTINCT CASE WHEN olf.amazon_order_id IS NOT NULL THEN o.amazon_order_id END)
                ELSE 0 END avg_logi
    FROM acc_order o WITH (NOLOCK)
    LEFT JOIN acc_order_logistics_fact olf WITH (NOLOCK) ON o.amazon_order_id = olf.amazon_order_id
    WHERE o.purchase_date >= '2026-01-01' AND o.purchase_date < '2026-04-01'
      AND o.status = 'Shipped'
    GROUP BY CONVERT(VARCHAR(7), o.purchase_date, 120) ORDER BY m
"""):
    matched = int(r[1] or 0)
    total = int(r[2] or 0)
    logi = float(r[3] or 0)
    avg = float(r[4] or 0)
    fba_orders = total - matched if matched > 0 else 0
    estimated_full = avg * total if avg > 0 else 0
    print(f"  {r[0]}: matched={matched:>6,}/{total:>6,} | logi_actual={logi:>10,.0f} | avg/order={avg:>6,.1f} | est_full={estimated_full:>10,.0f}")

# 3. What % are FBA vs MFN orders?
print("\n=== FBA vs MFN ORDERS ===")
for r in q("""
    SELECT CONVERT(VARCHAR(7), o.purchase_date, 120) m,
           COUNT(*) total_orders,
           SUM(CASE WHEN o.fulfillment_channel = 'AFN' THEN 1 ELSE 0 END) fba,
           SUM(CASE WHEN o.fulfillment_channel = 'MFN' THEN 1 ELSE 0 END) mfn
    FROM acc_order o WITH (NOLOCK)
    WHERE o.purchase_date >= '2026-01-01' AND o.purchase_date < '2026-04-01'
      AND o.status = 'Shipped'
    GROUP BY CONVERT(VARCHAR(7), o.purchase_date, 120) ORDER BY m
"""):
    total = max(int(r[1] or 1), 1)
    fba = int(r[2] or 0)
    mfn = int(r[3] or 0)
    print(f"  {r[0]}: total={total:>6,} | FBA={fba:>6,} ({fba*100//total}%) | MFN={mfn:>6,} ({mfn*100//total}%)")

# 4. Check marketplace distribution
print("\n=== MARKETPLACE REVENUE DISTRIBUTION ===")
for r in q("""
    SELECT CONVERT(VARCHAR(7), period_date, 120) m,
           marketplace_id,
           SUM(CAST(revenue_pln AS FLOAT)) rev,
           SUM(CAST(cm1_pln AS FLOAT)) cm1,
           SUM(CAST(cm2_pln AS FLOAT)) cm2,
           SUM(CAST(ad_spend_pln AS FLOAT)) ads
    FROM executive_daily_metrics WITH (NOLOCK)
    WHERE period_date >= '2026-01-01' AND period_date < '2026-04-01'
    GROUP BY CONVERT(VARCHAR(7), period_date, 120), marketplace_id
    ORDER BY CONVERT(VARCHAR(7), period_date, 120), marketplace_id
"""):
    rev = float(r[2] or 0)
    cm1 = float(r[3] or 0)
    cm1_pct = cm1/max(rev,1)*100
    print(f"  {r[0]} | mkt={r[1]} | rev={rev:>10,.0f} | cm1={cm1:>10,.0f} ({cm1_pct:.1f}%) | cm2={float(r[4] or 0):>10,.0f} | ads={float(r[5] or 0):>8,.0f}")

# 5. Check acc_order columns for any fee fields
print("\n=== ACC_ORDER fee-related columns ===")
for r in q("""
    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME='acc_order' 
    AND (COLUMN_NAME LIKE '%fee%' OR COLUMN_NAME LIKE '%pln%' OR COLUMN_NAME LIKE '%cost%' OR COLUMN_NAME LIKE '%logistics%')
    ORDER BY ORDINAL_POSITION
"""):
    print(f"  {r[0]}")

# 6. Check acc_order fee totals
print("\n=== ACC_ORDER fee totals per month ===")
for r in q("""
    SELECT CONVERT(VARCHAR(7), purchase_date, 120) m,
           SUM(CAST(ISNULL(revenue_pln, 0) AS FLOAT)) rev,
           SUM(CAST(ISNULL(cogs_pln, 0) AS FLOAT)) cogs,
           SUM(CAST(ISNULL(amazon_fees_pln, 0) AS FLOAT)) amz_fees,
           SUM(CAST(ISNULL(logistics_cost_pln, 0) AS FLOAT)) logi
    FROM acc_order WITH (NOLOCK)
    WHERE purchase_date >= '2026-01-01' AND purchase_date < '2026-04-01'
      AND status = 'Shipped'
    GROUP BY CONVERT(VARCHAR(7), purchase_date, 120) ORDER BY m
"""):
    rev = float(r[1] or 0)
    cogs = float(r[2] or 0)
    fees = float(r[3] or 0)
    logi = float(r[4] or 0)
    cm1 = rev - cogs - fees - logi
    print(f"  {r[0]}: rev={rev:>10,.0f} | cogs={cogs:>10,.0f} | amz_fees={fees:>10,.0f} | logi={logi:>10,.0f} | cm1={cm1:>10,.0f} ({cm1/max(rev,1)*100:.1f}%)")

c.close()
print("\nDONE")
