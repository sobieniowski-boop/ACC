import pymssql, os
from dotenv import load_dotenv
load_dotenv("C:/ACC/.env")
conn = pymssql.connect(
    server=os.getenv("MSSQL_SERVER"), user=os.getenv("MSSQL_USER"),
    password=os.getenv("MSSQL_PASSWORD"), database=os.getenv("MSSQL_DATABASE"),
    port=int(os.getenv("MSSQL_PORT", "1433")), tds_version="7.3",
    login_timeout=30, timeout=120,
)
c = conn.cursor()
sql = """
SELECT ISNULL(charge_type, 'NULL') as ct, COUNT(*) as cnt,
       SUM(CAST(amount AS FLOAT)) as total
FROM acc_finance_transaction
GROUP BY charge_type
ORDER BY ABS(SUM(CAST(amount AS FLOAT))) DESC
"""
c.execute(sql)
for r in c.fetchall():
    t = r[2] or 0
    print(f"{str(r[0]):45s}|{r[1]:7d}|{t:14.2f}")
conn.close()
