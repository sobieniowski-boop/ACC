"""Final Board P&L Report: Jan-Mar 2026 with March extrapolation."""
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

# 1. acc_order_line columns
print("=== ACC_ORDER_LINE columns ===")
ol_cols = cols('acc_order_line')
print(f"  {ol_cols}")

# 2. Direct P&L from order lines using actual column names
# Need to find revenue, cogs, fees columns
rev_col = [c for c in ol_cols if 'revenue' in c.lower() or 'item_price' in c.lower()]
print(f"\n  Revenue candidates: {rev_col}")

# 3. Ads WITH correct column (spend_pln)
print("\n=== ADS SPEND (Jan-Mar 2026) ===")
for r in q("""
    SELECT CONVERT(VARCHAR(7), report_date, 120) m,
           COUNT(*) rows,
           COUNT(DISTINCT report_date) days,
           SUM(CAST(spend_pln AS FLOAT)) total_spend_pln
    FROM acc_ads_campaign_day WITH (NOLOCK)
    WHERE report_date >= '2026-01-01' AND report_date < '2026-04-01'
    GROUP BY CONVERT(VARCHAR(7), report_date, 120) ORDER BY m
"""):
    print(f"  {r[0]}: {r[1]:>6,} rows | {r[2]:>3} days | spend_pln={float(r[3] or 0):>12,.2f}")

# 4. Logistics with correct column
print("\n=== LOGISTICS (Jan-Mar 2026) ===")
for r in q("""
    SELECT CONVERT(VARCHAR(7), o.purchase_date, 120) m,
           COUNT(*) matched,
           SUM(CAST(olf.total_logistics_pln AS FLOAT)) logi_pln
    FROM acc_order_logistics_fact olf WITH (NOLOCK)
    JOIN acc_order o WITH (NOLOCK) ON o.amazon_order_id = olf.amazon_order_id
    WHERE o.purchase_date >= '2026-01-01' AND o.purchase_date < '2026-04-01'
      AND o.status = 'Shipped'
    GROUP BY CONVERT(VARCHAR(7), o.purchase_date, 120) ORDER BY m
"""):
    print(f"  {r[0]}: {r[1]:>6,} orders | logistics_pln={float(r[2] or 0):>12,.2f}")

# 5. Finance breakdown for Feb-Mar (since Jan has no data)
print("\n=== FINANCE TRANSACTIONS by type (Feb 2026) ===")
for r in q("""
    SELECT TOP 20 transaction_type,
           COUNT(*) cnt,
           SUM(CAST(amount_pln AS FLOAT)) total_pln
    FROM acc_finance_transaction WITH (NOLOCK)
    WHERE posted_date >= '2026-02-01' AND posted_date < '2026-03-01'
    GROUP BY transaction_type
    ORDER BY ABS(SUM(CAST(amount_pln AS FLOAT))) DESC
"""):
    print(f"  {r[0]:>35}: {r[1]:>6,} txns | {float(r[2] or 0):>12,.2f} PLN")

print("\n=== FINANCE TRANSACTIONS by type (Mar 1-10 2026) ===")
for r in q("""
    SELECT TOP 20 transaction_type,
           COUNT(*) cnt,
           SUM(CAST(amount_pln AS FLOAT)) total_pln
    FROM acc_finance_transaction WITH (NOLOCK)
    WHERE posted_date >= '2026-03-01' AND posted_date < '2026-03-11'
    GROUP BY transaction_type
    ORDER BY ABS(SUM(CAST(amount_pln AS FLOAT))) DESC
"""):
    print(f"  {r[0]:>35}: {r[1]:>6,} txns | {float(r[2] or 0):>12,.2f} PLN")

# 6. Direct order-level P&L (check actual available columns)
print("\n=== DIRECT ORDER LINE P&L (using actual columns) ===")
# First check what we actually have
sample = q("SELECT TOP 1 * FROM acc_order_line WITH (NOLOCK)")
print(f"  Columns: {ol_cols}")

# Find the right revenue/price/total columns
for col in ol_cols:
    if any(kw in col.lower() for kw in ['revenue', 'price', 'total', 'amount', 'cogs', 'fba', 'referral', 'fee', 'shipping']):
        val = q(f"""
            SELECT SUM(CAST(ISNULL({col}, 0) AS FLOAT)) 
            FROM acc_order_line ol WITH (NOLOCK)
            JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            WHERE o.purchase_date >= '2026-01-01' AND o.purchase_date < '2026-02-01'
              AND o.status = 'Shipped'
        """)[0][0]
        print(f"  Jan {col}: {float(val or 0):>14,.2f}")

# 7. Same for Feb
print()
for col in ol_cols:
    if any(kw in col.lower() for kw in ['revenue', 'price', 'total', 'amount', 'cogs', 'fba', 'referral', 'fee', 'shipping']):
        val = q(f"""
            SELECT SUM(CAST(ISNULL({col}, 0) AS FLOAT)) 
            FROM acc_order_line ol WITH (NOLOCK)
            JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            WHERE o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-01'
              AND o.status = 'Shipped'
        """)[0][0]
        print(f"  Feb {col}: {float(val or 0):>14,.2f}")

# 8. Same for Mar 1-10
print()
for col in ol_cols:
    if any(kw in col.lower() for kw in ['revenue', 'price', 'total', 'amount', 'cogs', 'fba', 'referral', 'fee', 'shipping']):
        val = q(f"""
            SELECT SUM(CAST(ISNULL({col}, 0) AS FLOAT)) 
            FROM acc_order_line ol WITH (NOLOCK)
            JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
            WHERE o.purchase_date >= '2026-03-01' AND o.purchase_date < '2026-03-11'
              AND o.status = 'Shipped'
        """)[0][0]
        print(f"  Mar1-10 {col}: {float(val or 0):>14,.2f}")

c.close()
print("\nDONE")
