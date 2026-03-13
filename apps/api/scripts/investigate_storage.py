"""Investigate FBAStorageFee amounts."""
import pymssql, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv("C:/ACC/.env")

conn = pymssql.connect(
    server=os.getenv("MSSQL_SERVER"),
    user=os.getenv("MSSQL_USER"),
    password=os.getenv("MSSQL_PASSWORD"),
    database=os.getenv("MSSQL_DATABASE"),
    port=int(os.getenv("MSSQL_PORT", "1433")),
    tds_version="7.3",
)
c = conn.cursor()

# 1. FBAStorageFee breakdown by date and transaction_type
print("=== FBAStorageFee by date ===")
c.execute("""
    SELECT CAST(posted_date AS DATE) as d, transaction_type, 
           COUNT(*) as cnt, 
           SUM(amount_pln) as total_pln,
           SUM(CAST(amount AS FLOAT)) as total_orig,
           MIN(currency) as curr
    FROM acc_finance_transaction
    WHERE charge_type = 'FBAStorageFee'
    GROUP BY CAST(posted_date AS DATE), transaction_type
    ORDER BY d
""")
for r in c.fetchall():
    tt = str(r[1] or "")
    pln = r[3] or 0
    orig = r[4] or 0
    curr = r[5] or ""
    print(f"  {r[0]} | {tt:30s} | cnt={r[2]:5d} | PLN={pln:12.2f} | orig={orig:12.2f} | {curr}")

# 2. Top 20 individual records by amount
print("\n=== FBAStorageFee top 20 by abs(amount) ===")
c.execute("""
    SELECT TOP 20 posted_date, transaction_type, 
           CAST(amount AS FLOAT) as amount, currency, amount_pln,
           marketplace_id, sku
    FROM acc_finance_transaction
    WHERE charge_type = 'FBAStorageFee'
    ORDER BY ABS(CAST(amount AS FLOAT)) DESC
""")
for r in c.fetchall():
    tt = str(r[1] or "")
    pln = r[4] or 0
    print(f"  {r[0]} | {tt:20s} | amt={r[2]:10.2f} {r[3]} | pln={pln:10.2f} | mp={r[5]} | sku={r[6]}")

# 3. Check description field if exists
print("\n=== FBAStorageFee - check what the fee covers ===")
c.execute("""
    SELECT DISTINCT transaction_type 
    FROM acc_finance_transaction 
    WHERE charge_type = 'FBAStorageFee'
""")
for r in c.fetchall():
    print(f"  transaction_type: {r[0]}")

# 4. Is this monthly storage fee posted on one date?
print("\n=== Monthly storage pattern check ===")
c.execute("""
    SELECT CAST(posted_date AS DATE) as d, COUNT(*) as cnt, 
           SUM(amount_pln) as total,
           COUNT(DISTINCT sku) as distinct_skus
    FROM acc_finance_transaction
    WHERE charge_type = 'FBAStorageFee'
    GROUP BY CAST(posted_date AS DATE)
    ORDER BY d
""")
for r in c.fetchall():
    print(f"  {r[0]} | records={r[1]:5d} | PLN={r[2]:12.2f} | distinct SKUs={r[3]}")

# 5. Also check all storage-related charge types
print("\n=== All storage-related charge_types ===")
c.execute("""
    SELECT charge_type, COUNT(*) as cnt, SUM(amount_pln) as total_pln,
           MIN(CAST(posted_date AS DATE)) as first_date,
           MAX(CAST(posted_date AS DATE)) as last_date
    FROM acc_finance_transaction
    WHERE charge_type LIKE '%Storage%' OR charge_type LIKE '%storage%'
    GROUP BY charge_type
""")
for r in c.fetchall():
    print(f"  {r[0]:40s} | cnt={r[1]:5d} | PLN={r[2]:12.2f} | {r[3]} - {r[4]}")

conn.close()
