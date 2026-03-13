import pymssql, os
from dotenv import load_dotenv
load_dotenv("C:/ACC/.env")
conn = pymssql.connect(
    server=os.getenv("MSSQL_SERVER"), user=os.getenv("MSSQL_USER"),
    password=os.getenv("MSSQL_PASSWORD"), database=os.getenv("MSSQL_DATABASE"),
    port=1433, login_timeout=60, timeout=120, tds_version="7.3",
)
cur = conn.cursor()
cur.execute("""
SELECT CAST(posted_date AS DATE) d, COUNT(*) cnt,
       SUM(CAST(amount AS FLOAT)) eur,
       SUM(CAST(amount_pln AS FLOAT)) pln
FROM acc_finance_transaction
WHERE charge_type = 'FBAStorageFee'
GROUP BY CAST(posted_date AS DATE)
ORDER BY d
""")
rows = cur.fetchall()
conn.close()
print(f"{'Date':<12} | {'Cnt':>5} | {'EUR':>12} | {'PLN':>12}")
print("-" * 50)
t_eur = t_pln = 0
for r in rows:
    print(f"{str(r[0]):<12} | {r[1]:>5} | {r[2]:>12,.2f} | {r[3]:>12,.2f}")
    t_eur += r[2]
    t_pln += r[3]
print("-" * 50)
print(f"{'TOTAL':<12} | {sum(r[1] for r in rows):>5} | {t_eur:>12,.2f} | {t_pln:>12,.2f}")
