"""Board-ready P&L audit: data availability for Jan-Mar 2026."""
import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env")
from app.core.db_connection import connect_acc

c = connect_acc(autocommit=False, timeout=30)
cur = c.cursor()

def q(sql, params=None):
    cur.execute(sql, params or [])
    return cur.fetchall()

# 1. Orders
print("=== ORDERS by month (Jan-Mar 2026) ===")
for r in q("""
    SELECT CONVERT(VARCHAR(7), purchase_date, 120) m,
           COUNT(*) orders,
           SUM(CASE WHEN status = 'Shipped' THEN 1 ELSE 0 END) shipped,
           SUM(CASE WHEN revenue_pln IS NOT NULL AND revenue_pln <> 0 THEN 1 ELSE 0 END) has_rev
    FROM acc_order WITH (NOLOCK)
    WHERE purchase_date >= '2026-01-01' AND purchase_date < '2026-04-01'
    GROUP BY CONVERT(VARCHAR(7), purchase_date, 120) ORDER BY m
"""):
    print(f"  {r[0]}: {r[1]:>7,} orders | shipped={r[2]:>7,} | has_rev={r[3]:>7,}")

# 2. Order lines quality
print("\n=== ORDER LINES quality (Shipped, Jan-Mar 2026) ===")
for r in q("""
    SELECT CONVERT(VARCHAR(7), o.purchase_date, 120) m,
           COUNT(*) lines,
           SUM(CASE WHEN ol.cogs_pln IS NOT NULL AND ol.cogs_pln <> 0 THEN 1 ELSE 0 END) cogs,
           SUM(CASE WHEN ol.fba_fee_pln IS NOT NULL AND ol.fba_fee_pln <> 0 THEN 1 ELSE 0 END) fba,
           SUM(CASE WHEN ol.referral_fee_pln IS NOT NULL AND ol.referral_fee_pln <> 0 THEN 1 ELSE 0 END) ref_fee
    FROM acc_order_line ol WITH (NOLOCK)
    JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    WHERE o.purchase_date >= '2026-01-01' AND o.purchase_date < '2026-04-01'
      AND o.status = 'Shipped'
    GROUP BY CONVERT(VARCHAR(7), o.purchase_date, 120) ORDER BY m
"""):
    t = max(r[1], 1)
    print(f"  {r[0]}: {r[1]:>7,} lines | cogs={r[2]:>6,} ({r[2]*100//t}%) | fba={r[3]:>6,} ({r[3]*100//t}%) | ref={r[4]:>6,} ({r[4]*100//t}%)")

# 3. Finance transactions
print("\n=== FINANCE TRANSACTIONS (Jan-Mar 2026) ===")
for r in q("""
    SELECT CONVERT(VARCHAR(7), posted_date, 120) m, COUNT(*) c
    FROM acc_finance_transaction WITH (NOLOCK)
    WHERE posted_date >= '2026-01-01' AND posted_date < '2026-04-01'
    GROUP BY CONVERT(VARCHAR(7), posted_date, 120) ORDER BY m
"""):
    print(f"  {r[0]}: {r[1]:>8,} txns")

# 4. Ads data
print("\n=== ADS CAMPAIGN DAY (Jan-Mar 2026) ===")
try:
    for r in q("""
        SELECT CONVERT(VARCHAR(7), report_date, 120) m, COUNT(*) rows,
               SUM(CAST(cost_pln AS FLOAT)) total_cost_pln
        FROM acc_ads_campaign_day WITH (NOLOCK)
        WHERE report_date >= '2026-01-01' AND report_date < '2026-04-01'
        GROUP BY CONVERT(VARCHAR(7), report_date, 120) ORDER BY m
    """):
        print(f"  {r[0]}: {r[1]:>6,} rows | cost_pln={float(r[2] or 0):>10,.2f}")
    if not cur.rowcount:
        print("  (brak danych)")
except Exception as e:
    print(f"  Error: {e}")

# 5. Ads product day
print("\n=== ADS PRODUCT DAY (Jan-Mar 2026) ===")
try:
    for r in q("""
        SELECT CONVERT(VARCHAR(7), report_date, 120) m, COUNT(*) rows,
               SUM(CAST(cost_pln AS FLOAT)) total_cost_pln
        FROM acc_ads_product_day WITH (NOLOCK)
        WHERE report_date >= '2026-01-01' AND report_date < '2026-04-01'
        GROUP BY CONVERT(VARCHAR(7), report_date, 120) ORDER BY m
    """):
        print(f"  {r[0]}: {r[1]:>6,} rows | cost_pln={float(r[2] or 0):>10,.2f}")
    if not cur.rowcount:
        print("  (brak danych)")
except Exception as e:
    print(f"  Error/no table: {e}")

# 6. Logistics
print("\n=== LOGISTICS / COURIER COST (Jan-Mar 2026) ===")
try:
    for r in q("""
        SELECT CONVERT(VARCHAR(7), o.purchase_date, 120) m,
               COUNT(*) matched,
               SUM(CAST(olf.total_cost_pln AS FLOAT)) logi_pln
        FROM acc_order_logistics_fact olf WITH (NOLOCK)
        JOIN acc_order o WITH (NOLOCK) ON o.id = olf.order_id
        WHERE o.purchase_date >= '2026-01-01' AND o.purchase_date < '2026-04-01'
        GROUP BY CONVERT(VARCHAR(7), o.purchase_date, 120) ORDER BY m
    """):
        print(f"  {r[0]}: {r[1]:>6,} orders | total_pln={float(r[2] or 0):>12,.2f}")
    if not cur.rowcount:
        print("  (brak danych)")
except Exception as e:
    print(f"  Error/no table: {e}")

# 7. Executive daily metrics
print("\n=== EXECUTIVE DAILY METRICS (Jan-Mar 2026) ===")
try:
    for r in q("""
        SELECT CONVERT(VARCHAR(7), period_date, 120) m, COUNT(*) days,
               SUM(CAST(revenue_pln AS FLOAT)) rev,
               SUM(CAST(cm1_pln AS FLOAT)) cm1,
               SUM(CAST(cm2_pln AS FLOAT)) cm2,
               SUM(CAST(profit_pln AS FLOAT)) np
        FROM executive_daily_metrics WITH (NOLOCK)
        WHERE period_date >= '2026-01-01' AND period_date < '2026-04-01'
        GROUP BY CONVERT(VARCHAR(7), period_date, 120) ORDER BY m
    """):
        print(f"  {r[0]}: {r[1]:>3} days | rev={float(r[2] or 0):>12,.0f} | cm1={float(r[3] or 0):>10,.0f} | cm2={float(r[4] or 0):>10,.0f} | np={float(r[5] or 0):>10,.0f}")
    if not cur.rowcount:
        print("  (brak danych)")
except Exception as e:
    print(f"  Error: {e}")

# 8. March day-by-day (for extrapolation)
print("\n=== MARCH 2026 day-by-day orders ===")
for r in q("""
    SELECT CAST(purchase_date AS DATE) d, COUNT(*) c
    FROM acc_order WITH (NOLOCK)
    WHERE purchase_date >= '2026-03-01' AND purchase_date < '2026-03-11'
      AND status = 'Shipped'
    GROUP BY CAST(purchase_date AS DATE) ORDER BY d
"""):
    print(f"  {r[0]}: {r[1]:>5,} orders")

c.close()
print("\nDONE")
