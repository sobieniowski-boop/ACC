"""More investigation on FBAStorageFee - check description/period."""
import pymssql, os
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

# Check all columns on acc_finance_transaction
print("=== Table columns ===")
c.execute("""
    SELECT COLUMN_NAME, DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'acc_finance_transaction'
    ORDER BY ORDINAL_POSITION
""")
for r in c.fetchall():
    print(f"  {r[0]:40s} {r[1]}")

# Check if there's a description or fee_type for storage
print("\n=== FBAStorageFee - sample full row ===")
c.execute("""
    SELECT TOP 3 * 
    FROM acc_finance_transaction
    WHERE charge_type = 'FBAStorageFee'
    ORDER BY ABS(CAST(amount AS FLOAT)) DESC
""")
cols = [d[0] for d in c.description]
for row in c.fetchall():
    print("---")
    for col, val in zip(cols, row):
        if val is not None and val != "" and val != 0:
            print(f"  {col}: {val}")

# 2026-03-07 records have PLN=0 - check exchange rate for that date
print("\n=== Exchange rate check for 2026-03-01+ ===")
c.execute("""
    SELECT TOP 10 rate_date, currency, rate_to_pln
    FROM acc_exchange_rate 
    WHERE rate_date >= '2026-03-01' AND currency = 'EUR'
    ORDER BY rate_date DESC
""")
for r in c.fetchall():
    print(f"  {r[0]} | EUR/PLN={r[1]}")

# Total storage cost properly converted
print("\n=== Total FBAStorageFee in EUR ===")
c.execute("""
    SELECT SUM(CAST(amount AS FLOAT)) as total_eur, COUNT(*) as cnt
    FROM acc_finance_transaction 
    WHERE charge_type = 'FBAStorageFee'
""")
r = c.fetchone()
print(f"  Total: {r[0]:.2f} EUR across {r[1]} records")

# Check ALL charge_types in the system
print("\n=== All distinct charge_types with counts ===")
c.execute("""
    SELECT charge_type, transaction_type, COUNT(*) as cnt,
           SUM(amount_pln) as total_pln,
           SUM(CAST(amount AS FLOAT)) as total_orig,
           MIN(currency) as curr
    FROM acc_finance_transaction
    GROUP BY charge_type, transaction_type
    ORDER BY ABS(SUM(CAST(amount AS FLOAT))) DESC
""")
for r in c.fetchall():
    ct = str(r[0] or "NULL")
    tt = str(r[1] or "NULL")
    pln = r[3] or 0
    orig = r[4] or 0
    curr = r[5] or ""
    print(f"  {ct:45s} | {tt:30s} | cnt={r[2]:6d} | {curr} {orig:>14.2f} | PLN {pln:>14.2f}")

conn.close()
