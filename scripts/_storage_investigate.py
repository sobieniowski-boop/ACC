"""Deep investigation of FBAStorageFee discrepancy.
Seller Central: -1,107.52 EUR (Mar 1-6)
Our DB:         -54,064.48 EUR (Mar 3-7)
"""
import pymssql, os
from dotenv import load_dotenv
load_dotenv("C:/ACC/.env")

conn = pymssql.connect(
    server=os.getenv("MSSQL_SERVER"), user=os.getenv("MSSQL_USER"),
    password=os.getenv("MSSQL_PASSWORD"), database=os.getenv("MSSQL_DATABASE"),
    port=1433, login_timeout=60, timeout=180, tds_version="7.3",
)
cur = conn.cursor()

# 1) Check transaction_type for FBAStorageFee records
print("=== 1. transaction_type for FBAStorageFee ===")
cur.execute("""
SELECT transaction_type, COUNT(*), SUM(CAST(amount AS FLOAT))
FROM acc_finance_transaction
WHERE charge_type = 'FBAStorageFee'
GROUP BY transaction_type
""")
for r in cur.fetchall():
    print(f"  {r[0]:<30} | {r[1]:>5} recs | {r[2]:>12,.2f} EUR")

# 2) Sample 10 records with all fields
print("\n=== 2. Sample FBAStorageFee records (first 10) ===")
cur.execute("""
SELECT TOP 10 id, marketplace_id, transaction_type, amazon_order_id, 
       shipment_id, sku, CAST(posted_date AS DATE), 
       CAST(amount AS FLOAT), currency, financial_event_group_id
FROM acc_finance_transaction
WHERE charge_type = 'FBAStorageFee'
ORDER BY CAST(amount AS FLOAT)
""")
cols = [d[0] for d in cur.description]
for r in cur.fetchall():
    print("  ---")
    for c, v in zip(cols, r):
        print(f"    {c}: {v}")

# 3) Check if these are per-ASIN breakdowns (sku=NULL means aggregate?)
print("\n=== 3. SKU distribution in FBAStorageFee ===")
cur.execute("""
SELECT CASE WHEN sku IS NULL THEN 'NULL' ELSE 'has_sku' END,
       COUNT(*), SUM(CAST(amount AS FLOAT))
FROM acc_finance_transaction
WHERE charge_type = 'FBAStorageFee'
GROUP BY CASE WHEN sku IS NULL THEN 'NULL' ELSE 'has_sku' END
""")
for r in cur.fetchall():
    print(f"  {r[0]:<10} | {r[1]:>5} recs | {r[2]:>12,.2f} EUR")

# 4) Check what OTHER ServiceFeeEventList charge_types exist
print("\n=== 4. All charge_types in ServiceFeeEventList ===")
cur.execute("""
SELECT charge_type, COUNT(*), SUM(CAST(amount AS FLOAT)) total_eur
FROM acc_finance_transaction
WHERE transaction_type = 'ServiceFeeEventList'
GROUP BY charge_type
ORDER BY SUM(CAST(amount AS FLOAT))
""")
for r in cur.fetchall():
    print(f"  {r[0]:<40} | {r[1]:>6} recs | {r[2]:>12,.2f} EUR")

# 5) Compare: total ServiceFeeEventList vs Seller Central's "Amazon fees"
print("\n=== 5. Total ServiceFeeEventList by month ===")
cur.execute("""
SELECT FORMAT(posted_date, 'yyyy-MM') m, 
       COUNT(*), SUM(CAST(amount AS FLOAT))
FROM acc_finance_transaction
WHERE transaction_type = 'ServiceFeeEventList'
GROUP BY FORMAT(posted_date, 'yyyy-MM')
ORDER BY m
""")
for r in cur.fetchall():
    print(f"  {r[0]} | {r[1]:>6} recs | {r[2]:>12,.2f} EUR")

# 6) Check: is FBAStorageFee coming from ServiceFeeEventList or ShipmentEventList?
print("\n=== 6. FBAStorageFee per financial_event_group_id (top 5 by amount) ===")
cur.execute("""
SELECT TOP 5 financial_event_group_id, 
       MIN(CAST(posted_date AS DATE)) min_date,
       MAX(CAST(posted_date AS DATE)) max_date,
       COUNT(*) cnt,
       SUM(CAST(amount AS FLOAT)) eur
FROM acc_finance_transaction
WHERE charge_type = 'FBAStorageFee'
GROUP BY financial_event_group_id
ORDER BY SUM(CAST(amount AS FLOAT))
""")
for r in cur.fetchall():
    print(f"  group={r[0][:30]} | {r[1]} to {r[2]} | {r[3]:>4} recs | {r[4]:>12,.2f} EUR")

# 7) Check date range for these specific settlement groups - what else is in them?
print("\n=== 7. Biggest FBAStorageFee settlement group - all charge_types ===")
cur.execute("""
SELECT TOP 1 financial_event_group_id
FROM acc_finance_transaction
WHERE charge_type = 'FBAStorageFee'
GROUP BY financial_event_group_id
ORDER BY SUM(CAST(amount AS FLOAT))
""")
biggest_group = cur.fetchone()[0]
cur.execute("""
SELECT charge_type, COUNT(*), SUM(CAST(amount AS FLOAT))
FROM acc_finance_transaction
WHERE financial_event_group_id = %s
GROUP BY charge_type
ORDER BY SUM(CAST(amount AS FLOAT))
""", (biggest_group,))
print(f"  Settlement group: {biggest_group}")
for r in cur.fetchall():
    print(f"    {r[0]:<40} | {r[1]:>5} recs | {r[2]:>12,.2f} EUR")

conn.close()
print("\nDONE")
