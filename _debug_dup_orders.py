"""Debug: check for duplicate amazon_order_ids causing PK violations."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc

conn = connect_acc(timeout=30)
cur = conn.cursor()

# Check specific order
for oid in ['028-0087537-4033972', '171-1049278-9522723']:
    cur.execute(
        "SELECT amazon_order_id, id, status, is_refund, purchase_date "
        "FROM dbo.acc_order WITH (NOLOCK) WHERE amazon_order_id = ?", (oid,)
    )
    rows = cur.fetchall()
    print(f"\n{oid}: {len(rows)} rows")
    for r in rows:
        print(f"  {r}")

    cur.execute(
        "SELECT amazon_order_id, calc_version, total_logistics_pln "
        "FROM dbo.acc_order_logistics_fact WITH (NOLOCK) WHERE amazon_order_id = ?", (oid,)
    )
    facts = cur.fetchall()
    print(f"  Facts: {len(facts)}")
    for f in facts:
        print(f"    {f}")

# General check: how many amazon_order_ids appear multiple times in Feb-Mar?
cur.execute("""
    SELECT amazon_order_id, COUNT(*) AS cnt
    FROM dbo.acc_order WITH (NOLOCK)
    WHERE purchase_date >= '2026-02-01' AND purchase_date < '2026-04-01'
    GROUP BY amazon_order_id
    HAVING COUNT(*) > 1
""")
dups = cur.fetchall()
print(f"\nDuplicate amazon_order_ids in Feb-Mar 2026: {len(dups)}")
if dups[:5]:
    for d in dups[:5]:
        print(f"  {d}")

conn.close()
