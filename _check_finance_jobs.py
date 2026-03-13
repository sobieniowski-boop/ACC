import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv; load_dotenv(r"C:\ACC\.env")
from app.core.db_connection import connect_acc

c = connect_acc(autocommit=False, timeout=15)
cur = c.cursor()

# 1. Find tables
cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='dbo' AND (TABLE_NAME LIKE '%sync%' OR TABLE_NAME LIKE '%job%' OR TABLE_NAME LIKE '%scheduler%' OR TABLE_NAME LIKE '%finance%') ORDER BY TABLE_NAME")
print("=== SYNC/JOB/FINANCE TABLES ===")
for r in cur.fetchall():
    print(f"  {r[0]}")

# 2. Check acc_job if exists
try:
    cur.execute("SELECT TOP 20 job_id, job_type, status, started_at, finished_at, SUBSTRING(CAST(error_message AS VARCHAR(200)),1,100) FROM acc_job WITH (NOLOCK) WHERE job_type LIKE '%inance%' OR job_type LIKE '%finance%' ORDER BY started_at DESC")
    print("\n=== FINANCE JOBS (last 20) ===")
    rows = cur.fetchall()
    if not rows:
        print("  (no finance jobs found)")
    for r in rows:
        dur = ""
        if r[3] and r[4]:
            dur = f"{(r[4]-r[3]).total_seconds():.0f}s"
        print(f"  id={r[0]} | type={r[1]} | status={r[2]} | start={r[3]} | dur={dur} | err={r[5]}")
except Exception as e:
    print(f"  acc_job query failed: {e}")

# 3. Check scheduler jobs
try:
    cur.execute("SELECT TOP 20 job_id, job_type, status, started_at, finished_at FROM acc_job WITH (NOLOCK) ORDER BY started_at DESC")
    print("\n=== ALL RECENT JOBS (last 20) ===")
    for r in cur.fetchall():
        dur = ""
        if r[3] and r[4]:
            dur = f"{(r[4]-r[3]).total_seconds():.0f}s"
        print(f"  id={r[0]} | type={r[1]} | status={r[2]} | start={r[3]} | dur={dur}")
except Exception as e:
    print(f"  acc_job all query failed: {e}")

# 4. Finance transaction freshness
cur.execute("SELECT COUNT(*), MIN(posted_date), MAX(posted_date), MAX(synced_at) FROM acc_finance_transaction WITH (NOLOCK)")
r = cur.fetchone()
print(f"\n=== FINANCE TXNS: {r[0]:,} rows | range: {r[1]} -> {r[2]} | last_synced: {r[3]} ===")

# 5. Last 5 dates
cur.execute("SELECT TOP 5 CAST(posted_date AS DATE) d, COUNT(*) cnt FROM acc_finance_transaction WITH (NOLOCK) GROUP BY CAST(posted_date AS DATE) ORDER BY d DESC")
print("\n=== LATEST POSTED DATES ===")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:,} txns")

# 6. Check order sync state
try:
    cur.execute("SELECT TOP 10 marketplace_id, last_window_from, last_window_to, updated_at FROM acc_order_sync_state WITH (NOLOCK) ORDER BY updated_at DESC")
    print("\n=== ORDER SYNC STATE ===")
    for r in cur.fetchall():
        print(f"  mkt={str(r[0])[:14]} | from={r[1]} | to={r[2]} | updated={r[3]}")
except Exception as e:
    print(f"  order sync state: {e}")

c.close()
print("\nDONE")
