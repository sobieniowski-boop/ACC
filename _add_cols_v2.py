"""Add missing columns to acc_order via raw pymssql (no compat layer)."""
import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv; load_dotenv(r"C:\ACC\.env")
from app.core.config import settings

import pymssql

print(f"Connecting to {settings.MSSQL_SERVER}:{settings.MSSQL_PORT}...")
c = pymssql.connect(
    server=settings.MSSQL_SERVER,
    port=settings.MSSQL_PORT,
    user=settings.MSSQL_USER,
    password=settings.MSSQL_PASSWORD,
    database=settings.MSSQL_DATABASE,
    tds_version="7.4",
    login_timeout=15,
    timeout=30,
    autocommit=True,
)
cur = c.cursor()
print("Connected!")

columns = ['shipping_surcharge_pln', 'promo_order_fee_pln', 'refund_commission_pln']

for col in columns:
    # Check existence
    cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='acc_order' AND COLUMN_NAME=%s", (col,))
    exists = cur.fetchone()[0] > 0
    if not exists:
        ddl = "ALTER TABLE dbo.acc_order ADD " + col + " DECIMAL(18,4) NULL"
        print(f"  Executing: {ddl}")
        try:
            cur.execute(ddl)
            print(f"  Column {col}: ADDED")
        except Exception as e:
            print(f"  Column {col}: ERROR - {e}")
    else:
        print(f"  Column {col}: already exists")

# Verify
cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='acc_order' AND COLUMN_NAME IN ('shipping_surcharge_pln','promo_order_fee_pln','refund_commission_pln')")
rows = cur.fetchall()
for r in rows:
    print(f"  VERIFIED: {r[0]}")
print(f"Total: {len(rows)}/3")
c.close()
print("DONE")
