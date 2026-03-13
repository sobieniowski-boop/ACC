import os, sys
sys.path.insert(0, r'C:\ACC\apps\api')
os.chdir(r'C:\ACC\apps\api')
from dotenv import load_dotenv
load_dotenv(r'C:\ACC\.env')
from app.core.db_connection import connect_acc

c = connect_acc(timeout=30)
cur = c.cursor()
cur.execute("SELECT MIN(purchase_date), MAX(purchase_date), COUNT(*) FROM dbo.acc_order WITH (NOLOCK) WHERE status = 'Shipped'")
r = cur.fetchone()
print(f"Shipped orders: min={r[0]}, max={r[1]}, count={r[2]}")
cur.execute("SELECT TOP 5 CAST(purchase_date AS DATE) AS d, COUNT(*) AS cnt FROM dbo.acc_order WITH (NOLOCK) WHERE status = 'Shipped' GROUP BY CAST(purchase_date AS DATE) ORDER BY d DESC")
for r2 in cur.fetchall():
    print(f"  {r2[0]}: {r2[1]} orders")
c.close()
