"""Quick check: new table + columns exist."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
os.chdir(os.path.join(os.path.dirname(__file__), "apps", "api"))

from app.core.db_connection import connect_acc
conn = connect_acc(autocommit=True)
cur = conn.cursor()

cur.execute("""
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'acc_event_handler_health'
ORDER BY ORDINAL_POSITION
""")
print("=== acc_event_handler_health ===")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}")

cur.execute("""
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'acc_event_processing_log'
  AND COLUMN_NAME IN ('handler_timeout', 'circuit_open')
""")
print("=== new columns on acc_event_processing_log ===")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}")

cur.close()
conn.close()
print("OK")
