import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv; load_dotenv(r"C:\ACC\.env")
from app.core.db_connection import connect_acc

c = connect_acc(autocommit=False, timeout=15)
cur = c.cursor()

# 1. acc_job_run - all recent
cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='acc_job_run' ORDER BY ORDINAL_POSITION")
print("=== acc_job_run COLUMNS ===")
for r in cur.fetchall():
    print(f"  {r[0]}")

cur.execute("SELECT TOP 30 * FROM acc_job_run WITH (NOLOCK) ORDER BY 1 DESC")
cols = [d[0] for d in cur.description]
print(f"\n=== acc_job_run RECENT (cols: {cols}) ===")
for r in cur.fetchall():
    print(f"  {list(r)}")

# 2. acc_fin_event_group_sync
print("\n=== acc_fin_event_group_sync COLUMNS ===")
cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='acc_fin_event_group_sync' ORDER BY ORDINAL_POSITION")
for r in cur.fetchall():
    print(f"  {r[0]}")

cur.execute("SELECT TOP 10 * FROM acc_fin_event_group_sync WITH (NOLOCK) ORDER BY 1 DESC")
cols2 = [d[0] for d in cur.description]
print(f"\n=== acc_fin_event_group_sync RECENT (cols: {cols2}) ===")
for r in cur.fetchall():
    vals = []
    for v in r:
        s = str(v)
        if len(s) > 60: s = s[:60] + "..."
        vals.append(s)
    print(f"  {vals}")

c.close()
print("\nDONE")
