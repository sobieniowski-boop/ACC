import sys,os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc
conn = connect_acc(timeout=30)
cur = conn.cursor()
cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='acc_dhl_billing_line' ORDER BY ORDINAL_POSITION")
for r in cur.fetchall():
    print(r[0])
conn.close()
