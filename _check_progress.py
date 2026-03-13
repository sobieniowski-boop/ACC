"""Quick check: finance reimport progress."""
import pymssql, os
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env")

conn = pymssql.connect(
    server=os.getenv("MSSQL_SERVER", "acc-sql-kadax.database.windows.net"),
    port=int(os.getenv("MSSQL_PORT", "1433")),
    user=os.getenv("MSSQL_USER"),
    password=os.getenv("MSSQL_PASSWORD"),
    database=os.getenv("MSSQL_DATABASE", "ACC"),
    tds_version="7.3",
)
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
print("acc_finance_transaction rows:", cur.fetchone()[0])

cur.execute("""
    SELECT resource_description, request_mode, request_status, request_session_id
    FROM sys.dm_tran_locks WHERE resource_type = 'APPLICATION'
""")
locks = cur.fetchall()
print("App locks:", len(locks))
for r in locks:
    print("  ", r)

cur.execute("""
    SELECT TOP 5 s.session_id, s.status, s.last_request_start_time
    FROM sys.dm_exec_sessions s
    WHERE s.is_user_process = 1
    ORDER BY s.last_request_start_time DESC
""")
for s in cur.fetchall():
    print("  session:", s)

# Check finance_group_sync counts
cur.execute("""
    SELECT COUNT(*) total,
           SUM(CASE WHEN processing_status='Open' THEN 1 ELSE 0 END) as open_groups,
           SUM(CASE WHEN processing_status='Closed' THEN 1 ELSE 0 END) as closed_groups,
           SUM(ISNULL(last_row_count,0)) as total_rows_tracked
    FROM acc_finance_group_sync
""")
row = cur.fetchone()
print(f"Group sync: total={row[0]}, open={row[1]}, closed={row[2]}, tracked_rows={row[3]}")

cur.close()
conn.close()
