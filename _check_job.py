from dotenv import load_dotenv
load_dotenv("C:/ACC/.env")
import sys; sys.path.insert(0, "C:/ACC/apps/api")
from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()

# Check for app locks
cur.execute(
    "SELECT resource_type, resource_description, request_mode, request_status, request_session_id "
    "FROM sys.dm_tran_locks WHERE resource_type = 'APPLICATION'"
)
locks = cur.fetchall()
print(f"Application locks: {len(locks)}")
for l in locks:
    print(f"  {l}")

# Check sleeping sessions with open transactions
cur.execute(
    "SELECT s.session_id, s.status, s.login_name, s.last_request_start_time "
    "FROM sys.dm_exec_sessions s "
    "JOIN sys.dm_tran_session_transactions st ON s.session_id = st.session_id "
    "WHERE s.session_id != @@SPID ORDER BY s.last_request_start_time"
)
sessions = cur.fetchall()
print(f"\nSessions with open transactions: {len(sessions)}")
for s in sessions:
    print(f"  sid={s[0]} status={s[1]} login={s[2]} last_req={s[3]}")

# Count finance transactions after truncate
cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
print(f"\nacc_finance_transaction rows: {cur.fetchone()[0]}")

conn.close()

