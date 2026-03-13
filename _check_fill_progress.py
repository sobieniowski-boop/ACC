import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc
conn = connect_acc(timeout=30)
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM dbo.acc_order_logistics_fact WITH (NOLOCK) WHERE calc_version = 'hist_country_v1'")
print('Rows filled:', cur.fetchone()[0])
conn.close()
