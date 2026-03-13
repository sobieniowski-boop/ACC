import sys
sys.path.insert(0, 'C:/ACC/apps/api')
from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()
cur.execute("""
    SELECT TOP 3 run_id, status, dry_run, created_at, finished_at 
    FROM dbo.family_restructure_run 
    WHERE family_id=1367 AND marketplace_id='A13V1IB3VIYZZH' 
    ORDER BY created_at DESC
""")
rows = cur.fetchall()

print("Last 3 runs:")
for i, r in enumerate(rows, 1):
    print(f"{i}. Status: {r[1]}, DryRun: {r[2]}, Created: {r[3]}")

conn.close()
