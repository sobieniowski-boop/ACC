import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv; load_dotenv(r"C:\ACC\.env")
from app.core.db_connection import connect_acc
c = connect_acc(autocommit=False, timeout=10)
cur = c.cursor()
cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='acc_order' AND COLUMN_NAME IN ('shipping_surcharge_pln','promo_order_fee_pln','refund_commission_pln')")
rows = cur.fetchall()
for r in rows:
    print(r[0])
print(f"Found: {len(rows)}/3")
c.close()
