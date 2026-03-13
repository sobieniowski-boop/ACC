from app.core.db_connection import connect_acc
conn = connect_acc()
cur = conn.cursor()
cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME IN ('acc_ptd_cache', 'acc_ptd_sync_state')")
for r in cur.fetchall():
    print(r[0])
conn.close()
print("DONE")
