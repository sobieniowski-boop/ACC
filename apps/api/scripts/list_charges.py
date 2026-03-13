"""List ALL charge_types only."""
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
for r in c.fetchall():
    ct = str(r[0] or "NULL")
    tt = str(r[1] or "NULL")
    orig = r[3] or 0
    pln = r[4] or 0
    curr = r[5] or ""
    print(f"{ct:45s} | {tt:30s} | cnt={r[2]:7d} | {curr:3s} {orig:>14.2f} | PLN {pln:>14.2f} | {r[6]} - {r[7]}")

conn.close()
