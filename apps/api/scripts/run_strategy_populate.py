"""Run strategy detection engines to populate initial opportunities."""
import sys, os
sys.path.insert(0, ".")
os.chdir(r"C:\ACC\apps\api")

from app.services.strategy_service import run_strategy_detection

print("Starting strategy detection (days_back=30) ...")
result = run_strategy_detection(days_back=30)
print(f"Detection result: {result}")

# Verify counts
from app.core.db_connection import connect_acc
conn = connect_acc()
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM growth_opportunity")
total = cur.fetchone()[0]
print(f"Total opportunities: {total}")
cur.execute("SELECT opportunity_type, COUNT(*) cnt FROM growth_opportunity GROUP BY opportunity_type ORDER BY cnt DESC")
for row in cur.fetchall():
    print(f"  {row[0]:30s} {row[1]}")
cur.execute("SELECT status, COUNT(*) cnt FROM growth_opportunity GROUP BY status ORDER BY cnt DESC")
for row in cur.fetchall():
    print(f"  Status {row[0]:15s} {row[1]}")
cur.execute("SELECT COUNT(*) FROM growth_opportunity_log")
print(f"Log entries: {cur.fetchone()[0]}")
conn.close()
print("DONE")
