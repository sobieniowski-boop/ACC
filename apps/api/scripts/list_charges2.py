"""List ALL charge_types - output to file."""
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

lines = []

# 1. FBA Storage per marketplace
c.execute("""
    SELECT marketplace_id, COUNT(*) as cnt,
           SUM(CAST(amount AS FLOAT)) as total_eur,
           SUM(COALESCE(amount_pln, 0)) as total_pln
    FROM acc_finance_transaction
    WHERE charge_type = 'FBAStorageFee'
    GROUP BY marketplace_id
""")
lines.append("=== FBAStorageFee per marketplace ===")
for r in c.fetchall():
    mp = str(r[0] or "NULL")
    pln = r[3] or 0
    eur = r[2] or 0
    lines.append(f"  {mp:20s} | cnt={r[1]:5d} | EUR={eur:12.2f} | PLN={pln:12.2f}")

# 2. Check for duplicates
c.execute("""
    SELECT settlement_id, COUNT(*) as cnt, SUM(CAST(amount AS FLOAT)) as total
    FROM acc_finance_transaction
    WHERE charge_type = 'FBAStorageFee'
    GROUP BY settlement_id
    ORDER BY cnt DESC
""")
lines.append("\n=== FBAStorageFee settlements (check dupes) ===")
for r in c.fetchall():
    sid = str(r[0] or "NULL")[:50]
    lines.append(f"  {sid:50s} | cnt={r[1]:5d} | EUR={r[2]:12.2f}")

# 3. ALL charge_types
c.execute("""
    SELECT charge_type, transaction_type, COUNT(*) as cnt,
           SUM(CAST(amount AS FLOAT)) as total_orig,
           SUM(COALESCE(amount_pln, 0)) as total_pln,
           MIN(currency) as curr,
           MIN(CAST(posted_date AS DATE)) as first_dt,
           MAX(CAST(posted_date AS DATE)) as last_dt
    FROM acc_finance_transaction
    GROUP BY charge_type, transaction_type
    ORDER BY ABS(SUM(CAST(amount AS FLOAT))) DESC
""")
lines.append("\n=== ALL charge_types in DB ===")
for r in c.fetchall():
    ct = str(r[0] or "NULL")
    tt = str(r[1] or "NULL")
    orig = r[3] or 0
    pln = r[4] or 0
    curr = r[5] or ""
    lines.append(f"  {ct:45s} | {tt:30s} | cnt={r[2]:7d} | {curr:3s} {orig:>14.2f} | PLN {pln:>14.2f} | {r[6]} - {r[7]}")

# 4. Exchange rates
c.execute("""
    SELECT rate_date, rate_to_pln
    FROM acc_exchange_rate 
    WHERE rate_date >= '2026-03-01' AND currency = 'EUR'
    ORDER BY rate_date
""")
lines.append("\n=== EUR/PLN exchange rates (March 2026) ===")
for r in c.fetchall():
    lines.append(f"  {r[0]} | {r[1]}")

conn.close()

with open("scripts/charge_types_output.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(lines))
print(f"Output written to scripts/charge_types_output.txt ({len(lines)} lines)")
