import pymssql, os
from dotenv import load_dotenv
load_dotenv()
conn = pymssql.connect(
    server=os.getenv('MSSQL_SERVER'),
    user=os.getenv('MSSQL_USER'),
    password=os.getenv('MSSQL_PASSWORD'),
    database=os.getenv('MSSQL_DATABASE'),
    port=int(os.getenv('MSSQL_PORT','1433')),
    tds_version='7.3'
)
cur = conn.cursor()

cur.execute('SELECT COUNT(*) FROM acc_fin_event_group_sync')
print(f'Total groups in sync table: {cur.fetchone()[0]}')

cur.execute("""SELECT COUNT(*) FROM acc_fin_event_group_sync 
    WHERE processing_status='Closed' AND fund_transfer_status IN ('Succeeded','Transferred','Transfered')""")
print(f'Terminal groups: {cur.fetchone()[0]}')

cur.execute('SELECT COUNT(*) FROM acc_fin_event_group_sync WHERE ISNULL(last_row_count,0) > 0')
print(f'Groups with last_row_count > 0 (force_repair triggers): {cur.fetchone()[0]}')

cur.execute("""SELECT COUNT(*) FROM acc_fin_event_group_sync 
    WHERE ISNULL(last_row_count,0) = 0 
    AND processing_status='Closed' 
    AND fund_transfer_status IN ('Succeeded','Transferred','Transfered')""")
print(f'Terminal with row_count=0 (SKIPPED!): {cur.fetchone()[0]}')

cur.execute('SELECT COUNT(*) FROM acc_finance_transaction')
print(f'Current transaction rows: {cur.fetchone()[0]}')

cur.execute("""SELECT TOP 10 financial_event_group_id, processing_status, 
    fund_transfer_status, last_synced_at, last_row_count
    FROM acc_fin_event_group_sync ORDER BY last_synced_at DESC""")
print('\nRecently synced groups:')
for r in cur.fetchall():
    print(f'  gid={str(r[0])[:24]}  status={r[1]}  fund={r[2]}  synced={r[3]}  rows={r[4]}')

conn.close()
