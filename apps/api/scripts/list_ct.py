"""Minimal: list all charge_types."""
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
    login_timeout=30,
    timeout=60,
)
c = conn.cursor()
c.execute("""
    SELECT COALESCE(charge_type,'NULL') as ct, COUNT(*) as cnt,
           SUM(CAST(amount AS FLOAT)) as total
    FROM acc_finance_transaction
    GROUP BY charge_type
    ORDER BY ABS(SUM(CAST(amount AS FLOAT))) DESC
""")
rows = c.fetchall()
with open("C:/ACC/charge_types.txt", "w") as f:
    for r in rows:
        line = f"{r[0]:45s} | cnt={r[1]:7d} | {r[2]:>14.2f}"
        f.write(line + "\n")
        print(line)
print(f"\n--- {len(rows)} charge_types total ---")
conn.close()
