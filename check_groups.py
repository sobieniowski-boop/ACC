"""Check how many rows lack financial_event_group_id."""
import pymssql, os
from dotenv import load_dotenv
load_dotenv('C:/ACC/.env')

conn = pymssql.connect(
    server=os.getenv('MSSQL_SERVER'),
    user=os.getenv('MSSQL_USER'),
    password=os.getenv('MSSQL_PASSWORD'),
    database=os.getenv('MSSQL_DATABASE'),
    port=1433, tds_version='7.3'
)
cur = conn.cursor()

cur.execute("""
    SELECT 
        CASE WHEN financial_event_group_id IS NULL OR financial_event_group_id = '' THEN 'NO_GROUP' ELSE 'HAS_GROUP' END AS grp,
        COUNT(*) as cnt,
        SUM(CASE WHEN charge_type='FBAStorageFee' THEN 1 ELSE 0 END) as fba_cnt,
        SUM(CASE WHEN charge_type='FBAStorageFee' THEN amount ELSE 0 END) as fba_sum
    FROM acc_finance_transaction
    GROUP BY CASE WHEN financial_event_group_id IS NULL OR financial_event_group_id = '' THEN 'NO_GROUP' ELSE 'HAS_GROUP' END
""")
rows = cur.fetchall()
result = "Group status breakdown:\n"
for r in rows:
    result += f"  {r[0]}: {r[1]:,} rows, FBA={r[2]} rows sum={r[3]}\n"
print(result)
with open('C:/ACC/group_check.txt', 'w') as f:
    f.write(result)
conn.close()
