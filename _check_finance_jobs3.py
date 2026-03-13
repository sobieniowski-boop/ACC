import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv; load_dotenv(r"C:\ACC\.env")
from app.core.db_connection import connect_acc

c = connect_acc(autocommit=False, timeout=15)
cur = c.cursor()

# 1. Finance event group sync summary
cur.execute("""
SELECT marketplace_id, processing_status, COUNT(*) cnt,
       MAX(last_synced_at) last_sync,
       MAX(last_posted_at) last_posted,
       SUM(last_row_count) total_rows
FROM acc_fin_event_group_sync WITH (NOLOCK) 
GROUP BY marketplace_id, processing_status
ORDER BY marketplace_id, processing_status
""")
print("=== FINANCE EVENT GROUP SYNC SUMMARY ===")
print(f"  {'marketplace':<16} {'status':<10} {'groups':>6} {'rows':>8} {'last_sync':<22} {'last_posted':<22}")
for r in cur.fetchall():
    print(f"  {str(r[0]):<16} {r[1]:<10} {r[2]:>6} {r[3]:>8} {str(r[4])[:21]:<22} {str(r[5])[:21]:<22}")

# 2. Open groups (not yet Closed)
cur.execute("""
SELECT marketplace_id, financial_event_group_id, processing_status, 
       fund_transfer_status, group_start, group_end, last_synced_at, last_row_count
FROM acc_fin_event_group_sync WITH (NOLOCK) 
WHERE processing_status != 'Closed'
ORDER BY last_synced_at DESC
""")
print("\n=== OPEN (non-Closed) FINANCE GROUPS ===")
rows = cur.fetchall()
if not rows:
    print("  (all groups are Closed)")
for r in rows:
    print(f"  mkt={str(r[0])[:14]} | {r[2]}/{r[3]} | start={r[4]} end={r[5]} | sync={r[6]} | rows={r[7]}")

# 3. acc_job_run - check if there's any data
cur.execute("SELECT COUNT(*) FROM acc_job_run WITH (NOLOCK)")
cnt = cur.fetchone()[0]
print(f"\n=== acc_job_run: {cnt} total rows ===")
if cnt > 0:
    cur.execute("SELECT TOP 10 job_type, status, started_at, finished_at, records_processed, trigger_source FROM acc_job_run WITH (NOLOCK) ORDER BY started_at DESC")
    for r in cur.fetchall():
        print(f"  type={r[0]} | status={r[1]} | start={r[2]} | end={r[3]} | records={r[4]} | trigger={r[5]}")

# 4. Last finance transaction sync times
cur.execute("""
SELECT TOP 10 CAST(synced_at AS DATE) sync_date, COUNT(*) cnt, 
       MIN(posted_date) min_posted, MAX(posted_date) max_posted
FROM acc_finance_transaction WITH (NOLOCK)
GROUP BY CAST(synced_at AS DATE)
ORDER BY sync_date DESC
""")
print("\n=== FINANCE TXN SYNC DATES (when were rows written) ===")
for r in cur.fetchall():
    print(f"  synced={r[0]}: {r[1]:,} rows | posted range: {str(r[2])[:10]} -> {str(r[3])[:10]}")

# 5. Scheduler-related check: is server running?
cur.execute("SELECT COUNT(*) FROM acc_fin_event_group_sync WITH (NOLOCK) WHERE last_synced_at > DATEADD(hour, -1, GETUTCDATE())")
recent = cur.fetchone()[0]
print(f"\n=== GROUPS SYNCED IN LAST HOUR: {recent} ===")

cur.execute("SELECT COUNT(*) FROM acc_fin_event_group_sync WITH (NOLOCK) WHERE last_synced_at > DATEADD(hour, -24, GETUTCDATE())")
day = cur.fetchone()[0]
print(f"=== GROUPS SYNCED IN LAST 24H: {day} ===")

c.close()
print("\nDONE")
