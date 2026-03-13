from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env", override=True)
import sys, os
sys.path.insert(0, r"C:\ACC\apps\api")
from app.core.db_connection import connect_acc
conn = connect_acc()
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
print("acc_finance_transaction:", cur.fetchone()[0])
cur.execute("SELECT request_id, session_id, resource_type, resource_description FROM sys.dm_tran_locks WHERE resource_type='APPLICATION' AND request_mode='Exclusive'")
rows = cur.fetchall()
print("App locks:", len(rows))
for r in rows:
    print(f"  session={r[1]} resource={r[3]}")
conn.close()
