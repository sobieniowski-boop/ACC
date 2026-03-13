import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv; load_dotenv(r"C:\ACC\.env")
from app.core.db_connection import connect_acc

c = connect_acc(autocommit=False, timeout=10)
cur = c.cursor()
cur.execute("""
    SELECT COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'acc_order' 
      AND (COLUMN_NAME LIKE '%surcharge%' 
           OR COLUMN_NAME LIKE '%promo_order%'
           OR COLUMN_NAME LIKE '%refund_commission%')
    ORDER BY COLUMN_NAME
""")
rows = cur.fetchall()
if rows:
    for r in rows:
        print(f"EXISTS: {r[0]}")
else:
    print("NONE of shipping_surcharge_pln / promo_order_fee_pln / refund_commission_pln exist in acc_order")

# Also show all acc_order columns for reference
cur.execute("""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'acc_order'
    ORDER BY ORDINAL_POSITION
""")
print("\nAll acc_order columns:")
for r in cur.fetchall():
    print(f"  {r[0]} ({r[1]})")
c.close()
