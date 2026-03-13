"""Quick marketplace breakdown - single simple query."""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import pymssql

conn = pymssql.connect(
    server=os.getenv("MSSQL_SERVER"),
    user=os.getenv("MSSQL_USER"),
    password=os.getenv("MSSQL_PASSWORD"),
    database=os.getenv("MSSQL_DATABASE"),
    port=1433,
    login_timeout=60,
    timeout=120,
    tds_version="7.3",
)
cur = conn.cursor()
cur.execute("SELECT DISTINCT marketplace_id FROM acc_finance_transaction WHERE marketplace_id IS NOT NULL")
rows = cur.fetchall()
conn.close()

with open("C:/ACC/mp3_result.txt", "w") as f:
    f.write("marketplace_id\n")
    f.write("-" * 30 + "\n")
    for r in rows:
        line = f"{r[0]}"
        print(line)
        f.write(line + "\n")
print("DONE")
