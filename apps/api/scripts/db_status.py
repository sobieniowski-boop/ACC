"""Quick DB status check."""
from app.core.db_connection import connect_acc

c = connect_acc()
cur = c.cursor()

cur.execute(
    "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
    "WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE' "
    "ORDER BY TABLE_NAME"
)
tables = [r[0] for r in cur.fetchall()]

print("=== DATABASE STATUS ===")
for t in tables:
    try:
        cur.execute(f"SELECT COUNT(*) FROM dbo.[{t}] WITH (NOLOCK)")
        cnt = cur.fetchone()[0]
        status = f"{cnt} rows" if cnt > 0 else "empty"
    except Exception as e:
        status = f"ERROR: {e}"
    print(f"  {t}: {status}")

# Order range
try:
    cur.execute(
        "SELECT MIN(purchase_date), MAX(purchase_date), "
        "COUNT(DISTINCT marketplace_id) FROM acc_order WITH (NOLOCK)"
    )
    r = cur.fetchone()
    print(f"\nOrders range: {r[0]} -> {r[1]} ({r[2]} marketplaces)")
except:
    pass

# Backfill checkpoint
import json, os
for f in ["backfill_checkpoint.json", "backfill_progress.json"]:
    if os.path.exists(f):
        with open(f) as fh:
            data = json.load(fh)
            print(f"\n=== {f} ===")
            if "last_completed" in data:
                print(f"  last_completed: {data['last_completed']}")
            if "stats" in data:
                print(f"  stats: {data['stats']}")

c.close()
print("\nDone.")
