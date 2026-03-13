"""Deep P&L data for board meeting: Jan-Mar 2026."""
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

def cols(table):
    return [r[0] for r in q(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table}' ORDER BY ORDINAL_POSITION")]

# 1. Check schema of problem tables
print("=== ADS CAMPAIGN DAY columns ===")
try:
    c1 = cols('acc_ads_campaign_day')
    print(f"  {c1}")
except: print("  table missing")

print("\n=== ADS PRODUCT DAY columns ===")
try:
    c2 = cols('acc_ads_product_day')
    print(f"  {c2}")
except: print("  table missing")

print("\n=== LOGISTICS FACT columns ===")
try:
    c3 = cols('acc_order_logistics_fact')
    print(f"  {c3}")
except: print("  table missing")

print("\n=== EXECUTIVE DAILY METRICS columns ===")
c4 = cols('executive_daily_metrics')
print(f"  {c4}")

# 2. Executive daily metrics - actual unique days + totals
print("\n=== EXECUTIVE DAILY METRICS detail (Jan-Mar 2026) ===")
for r in q("""
    SELECT CONVERT(VARCHAR(7), period_date, 120) m,
           COUNT(*) total_rows,
           COUNT(DISTINCT period_date) unique_days,
           SUM(CAST(revenue_pln AS FLOAT)) rev,
           SUM(CAST(cogs_pln AS FLOAT)) cogs,
           SUM(CAST(cm1_pln AS FLOAT)) cm1,
           SUM(CAST(cm2_pln AS FLOAT)) cm2,
           SUM(CAST(profit_pln AS FLOAT)) np
    FROM executive_daily_metrics WITH (NOLOCK)
    WHERE period_date >= '2026-01-01' AND period_date < '2026-04-01'
    GROUP BY CONVERT(VARCHAR(7), period_date, 120) ORDER BY m
"""):
    print(f"  {r[0]}: rows={r[1]:>5} | days={r[2]:>3} | rev={float(r[3] or 0):>12,.0f} | cogs={float(r[4] or 0):>12,.0f} | cm1={float(r[5] or 0):>10,.0f} | cm2={float(r[6] or 0):>10,.0f} | np={float(r[7] or 0):>10,.0f}")

# 3. All columns available in exec metrics - get sum for each numeric column
print("\n=== EXECUTIVE DAILY METRICS - ALL numeric columns Jan 2026 ===")
numeric_cols = [c for c in c4 if c.endswith('_pln') or c in ('margin_pct',)]
for col in numeric_cols:
    try:
        val = q(f"SELECT SUM(CAST({col} AS FLOAT)) FROM executive_daily_metrics WITH (NOLOCK) WHERE period_date >= '2026-01-01' AND period_date < '2026-02-01'")[0][0]
        print(f"  Jan {col}: {float(val or 0):>12,.2f}")
    except Exception as e:
        print(f"  Jan {col}: ERROR {e}")

print()
for col in numeric_cols:
    try:
        val = q(f"SELECT SUM(CAST({col} AS FLOAT)) FROM executive_daily_metrics WITH (NOLOCK) WHERE period_date >= '2026-02-01' AND period_date < '2026-03-01'")[0][0]
        print(f"  Feb {col}: {float(val or 0):>12,.2f}")
    except Exception as e:
        print(f"  Feb {col}: ERROR {e}")

print()
for col in numeric_cols:
    try:
        val = q(f"SELECT SUM(CAST({col} AS FLOAT)) FROM executive_daily_metrics WITH (NOLOCK) WHERE period_date >= '2026-03-01' AND period_date < '2026-03-11'")[0][0]
        print(f"  Mar1-10 {col}: {float(val or 0):>12,.2f}")
    except Exception as e:
        print(f"  Mar1-10 {col}: ERROR {e}")

# 4. Ads - use actual column names
print("\n=== ADS COST (Jan-Mar 2026) ===")
try:
    ads_cols = cols('acc_ads_campaign_day')
    cost_col = [c for c in ads_cols if 'cost' in c.lower()][0] if any('cost' in c.lower() for c in ads_cols) else None
    if cost_col:
        print(f"  Using column: {cost_col}")
        for r in q(f"""
            SELECT CONVERT(VARCHAR(7), report_date, 120) m, COUNT(*) rows,
                   SUM(CAST({cost_col} AS FLOAT)) total_cost
            FROM acc_ads_campaign_day WITH (NOLOCK)
            WHERE report_date >= '2026-01-01' AND report_date < '2026-04-01'
            GROUP BY CONVERT(VARCHAR(7), report_date, 120) ORDER BY m
        """):
            print(f"  {r[0]}: {r[1]:>6,} rows | cost={float(r[2] or 0):>12,.2f}")
    else:
        print(f"  No cost column found among: {ads_cols}")
except Exception as e:
    print(f"  Error: {e}")

# 5. Direct SQL P&L from order lines (for validation)
print("\n=== DIRECT SQL P&L from acc_order_line (Jan-Mar 2026) ===")
for r in q("""
    SELECT CONVERT(VARCHAR(7), o.purchase_date, 120) m,
           COUNT(*) lines,
           SUM(CAST(ol.revenue_pln AS FLOAT)) rev,
           SUM(CAST(ol.cogs_pln AS FLOAT)) cogs,
           SUM(CAST(ol.fba_fee_pln AS FLOAT)) fba,
           SUM(CAST(ol.referral_fee_pln AS FLOAT)) ref_fee,
           SUM(CAST(ol.amazon_fees_pln AS FLOAT)) amz_fees,
           SUM(CAST(ol.revenue_pln AS FLOAT))
             - SUM(CAST(ISNULL(ol.cogs_pln,0) AS FLOAT))
             - SUM(CAST(ISNULL(ol.amazon_fees_pln,0) AS FLOAT)) as raw_cm1
    FROM acc_order_line ol WITH (NOLOCK)
    JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    WHERE o.purchase_date >= '2026-01-01' AND o.purchase_date < '2026-04-01'
      AND o.status = 'Shipped'
    GROUP BY CONVERT(VARCHAR(7), o.purchase_date, 120) ORDER BY m
"""):
    print(f"  {r[0]}: lines={r[1]:>6,} | rev={float(r[2] or 0):>12,.0f} | cogs={float(r[3] or 0):>12,.0f} | fba={float(r[4] or 0):>10,.0f} | ref={float(r[5] or 0):>10,.0f} | amz_fees={float(r[6] or 0):>10,.0f} | raw_CM1_no_logi={float(r[7] or 0):>10,.0f}")

# 6. Logistics - check what the table actually looks like  
print("\n=== LOGISTICS FACT first 5 rows ===")
try:
    logi_cols = cols('acc_order_logistics_fact')
    for r in q(f"SELECT TOP 5 * FROM acc_order_logistics_fact WITH (NOLOCK)"):
        print(f"  {dict(zip(logi_cols, r))}")
except Exception as e:
    print(f"  Error: {e}")

# 7. Finance transactions breakdown by type for Jan-Mar 
print("\n=== FINANCE TRANSACTION types (Feb-Mar 2026, top 15 by abs amount) ===")
for r in q("""
    SELECT TOP 15 transaction_type,
           COUNT(*) cnt,
           SUM(CAST(amount_pln AS FLOAT)) total_pln
    FROM acc_finance_transaction WITH (NOLOCK)
    WHERE posted_date >= '2026-02-01' AND posted_date < '2026-04-01'
    GROUP BY transaction_type
    ORDER BY ABS(SUM(CAST(amount_pln AS FLOAT))) DESC
"""):
    print(f"  {r[0]:>30}: {r[1]:>6,} txns | total_pln={float(r[2] or 0):>12,.2f}")

c.close()
print("\nDONE")
