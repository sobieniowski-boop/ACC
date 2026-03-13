import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv; load_dotenv(r"C:\ACC\.env")
from app.core.db_connection import connect_acc

c = connect_acc(autocommit=True, timeout=15)
cur = c.cursor()
cur.execute("""
DECLARE @result INT;
EXEC @result = sp_getapplock
    @Resource = 'acc_finance_sync_transactions',
    @LockMode = 'Exclusive',
    @LockOwner = 'Session',
    @LockTimeout = 1000;
SELECT @result;
""")
r = cur.fetchone()
print(f"Lock result: {r[0]} (0=granted, 1=granted after wait, <0=failed)")
cur.execute("EXEC sp_releaseapplock @Resource = 'acc_finance_sync_transactions', @LockOwner = 'Session';")
cur.close()
c.close()
print("Lock released, ready to restart backfill")
