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

lines = []

cur.execute('SELECT MIN(posted_date), MAX(posted_date), COUNT(*) FROM acc_finance_transaction')
r = cur.fetchone()
lines.append(f'Range: {r[0]} to {r[1]}')
lines.append(f'Total: {r[2]}')

cur.execute("SELECT charge_type, COUNT(*), SUM(amount) FROM acc_finance_transaction WHERE charge_type='FBAStorageFee' GROUP BY charge_type")
r2 = cur.fetchone()
if r2:
    lines.append(f'FBAStorageFee: {r2[1]} rows, sum={r2[2]}')

cur.execute("SELECT COUNT(DISTINCT synced_at) FROM acc_finance_transaction")
r3 = cur.fetchone()
lines.append(f'Distinct synced_at: {r3[0]}')

conn.close()

with open('C:/ACC/db_check_result.txt', 'w') as f:
    f.write('\n'.join(lines) + '\n')
print('WROTE C:/ACC/db_check_result.txt')
