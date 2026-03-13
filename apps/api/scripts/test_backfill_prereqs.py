import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.core.config import MARKETPLACE_REGISTRY
from app.connectors.amazon_sp_api.orders import OrdersClient
import pyodbc
from app.core.config import settings
print(f"OK: {len(MARKETPLACE_REGISTRY)} marketplaces")
conn = pyodbc.connect(settings.mssql_connection_string, autocommit=False)
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM acc_order")
print(f"Orders in DB: {cur.fetchone()[0]}")
cur.execute("SELECT MIN(purchase_date), MAX(purchase_date) FROM acc_order")
r = cur.fetchone()
print(f"Date range: {r[0]} → {r[1]}")
cur.close()
conn.close()
print("All imports and DB OK — ready to backfill!")
