"""List all charge_types, check exchange rates, and FBA storage per marketplace."""
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

# 1. Exchange rates
print("=== EUR Exchange rates (March) ===")
c.execute("""
    SELECT rate_date, rate_to_pln
    FROM acc_exchange_rate 
    WHERE rate_date >= '2026-03-01' AND currency = 'EUR'
    ORDER BY rate_date
""")
for r in c.fetchall():
    print(f"  {r[0]} | {r[1]}")

# 2. FBAStorageFee per marketplace
print("\n=== FBAStorageFee per marketplace ===")
c.execute("""
    SELECT marketplace_id, COUNT(*) as cnt,
           SUM(CAST(amount AS FLOAT)) as total_eur,
           SUM(amount_pln) as total_pln
    FROM acc_finance_transaction
    WHERE charge_type = 'FBAStorageFee'
    GROUP BY marketplace_id
""")
for r in c.fetchall():
    pln = r[3] or 0
    print(f"  {r[0]:20s} | cnt={r[1]:5d} | EUR={r[2]:12.2f} | PLN={pln:12.2f}")

# 3. Check if there's duplicate data (same settlement_id posted multiple times)
print("\n=== FBAStorageFee - check for duplicates ===")
c.execute("""
    SELECT settlement_id, COUNT(*) as cnt, SUM(CAST(amount AS FLOAT)) as total
    FROM acc_finance_transaction
    WHERE charge_type = 'FBAStorageFee'
    GROUP BY settlement_id
    ORDER BY cnt DESC
""")
for r in c.fetchall():
    print(f"  {r[0][:50]:50s} | cnt={r[1]:5d} | EUR={r[2]:12.2f}")

# 4. ALL charge_types with totals  
print("\n=== ALL charge_types in acc_finance_transaction ===")
c.execute("""
    SELECT charge_type, COUNT(*) as cnt,
           SUM(CAST(amount AS FLOAT)) as total_orig,
           SUM(COALESCE(amount_pln, 0)) as total_pln,
           MIN(currency) as curr,
           MIN(CAST(posted_date AS DATE)) as first_dt,
           MAX(CAST(posted_date AS DATE)) as last_dt
    FROM acc_finance_transaction
    GROUP BY charge_type
    ORDER BY ABS(SUM(CAST(amount AS FLOAT))) DESC
""")
for r in c.fetchall():
    ct = str(r[0] or "NULL")
    pln = r[3] or 0
    orig = r[2] or 0
    curr = r[4] or ""
    print(f"  {ct:45s} | cnt={r[1]:7d} | {curr:3s} {orig:>14.2f} | PLN {pln:>14.2f} | {r[5]} - {r[6]}")

conn.close()
