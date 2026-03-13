"""Validate F1 fix: OUTER APPLY TOP 1 eliminates row multiplication.

Runs the NEW OUTER APPLY pattern against live DB and compares with
the OLD LEFT JOIN to prove deduplication works.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))

from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()

# 1. Find orders with multiple logistics fact rows that also have order lines
print("=== Orders with duplicate logistics facts AND order lines ===")
cur.execute("""
    SELECT olf.amazon_order_id, COUNT(DISTINCT olf.calc_version) as cnt
    FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = olf.amazon_order_id
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE o.status = 'Shipped'
    GROUP BY olf.amazon_order_id
    HAVING COUNT(DISTINCT olf.calc_version) > 1
""")
dup_orders = cur.fetchall()
print(f"Total orders with >1 calc_version: {len(dup_orders)}")
if not dup_orders:
    print("No duplicates found — F1 not reproducible. Exiting.")
    conn.close()
    sys.exit(0)

sample_order = dup_orders[0][0]
print(f"\nSample order: {sample_order} ({dup_orders[0][1]} versions)")

# 2. OLD pattern: LEFT JOIN (causes multiplication)
print("\n=== OLD: LEFT JOIN (buggy) ===")
cur.execute("""
    SELECT COUNT(*) as row_count
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    LEFT JOIN dbo.acc_order_logistics_fact olf WITH (NOLOCK)
        ON olf.amazon_order_id = o.amazon_order_id
    WHERE o.amazon_order_id = ?
""", (sample_order,))
old_rows = cur.fetchone()[0]
print(f"  Rows returned: {old_rows}")

# 3. NEW pattern: OUTER APPLY TOP 1 (fixed)
print("\n=== NEW: OUTER APPLY TOP 1 (fixed) ===")
cur.execute("""
    SELECT COUNT(*) as row_count
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    OUTER APPLY (
        SELECT TOP 1 olf_inner.total_logistics_pln
        FROM dbo.acc_order_logistics_fact olf_inner WITH (NOLOCK)
        WHERE olf_inner.amazon_order_id = o.amazon_order_id
        ORDER BY olf_inner.calculated_at DESC
    ) olf
    WHERE o.amazon_order_id = ?
""", (sample_order,))
new_rows = cur.fetchone()[0]
print(f"  Rows returned: {new_rows}")

# 4. Actual line count
cur.execute("""
    SELECT COUNT(*)
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    WHERE o.amazon_order_id = ?
""", (sample_order,))
actual_lines = cur.fetchone()[0]
print(f"\n  Actual order lines: {actual_lines}")

assert old_rows > actual_lines, f"Expected multiplication: old_rows={old_rows} should be > actual_lines={actual_lines}"
assert new_rows == actual_lines, f"Fix failed: new_rows={new_rows} should equal actual_lines={actual_lines}"
print(f"\n  OLD multiplier: {old_rows / actual_lines:.1f}x")
print(f"  NEW multiplier: {new_rows / actual_lines:.1f}x")

# 5. Verify across ALL duplicate orders
print(f"\n=== Bulk validation: all {len(dup_orders)} dup orders ===")
all_ok = True
for order_id, cnt in dup_orders:
    cur.execute("""
        SELECT COUNT(*)
        FROM dbo.acc_order_line ol WITH (NOLOCK)
        JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        OUTER APPLY (
            SELECT TOP 1 olf_inner.total_logistics_pln
            FROM dbo.acc_order_logistics_fact olf_inner WITH (NOLOCK)
            WHERE olf_inner.amazon_order_id = o.amazon_order_id
            ORDER BY olf_inner.calculated_at DESC
        ) olf
        WHERE o.amazon_order_id = ?
    """, (order_id,))
    new_cnt = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM dbo.acc_order_line ol WITH (NOLOCK)
        JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        WHERE o.amazon_order_id = ?
    """, (order_id,))
    expected = cur.fetchone()[0]

    if new_cnt != expected:
        print(f"  FAIL: {order_id}: got {new_cnt}, expected {expected}")
        all_ok = False

if all_ok:
    print(f"  ALL {len(dup_orders)} orders: OUTER APPLY = exact line count (no multiplication)")
else:
    print("  SOME ORDERS STILL MULTIPLIED — fix incomplete")

# 6. Revenue impact comparison
print("\n=== Revenue impact: OLD vs NEW ===")
cur.execute("""
    WITH old_join AS (
        SELECT
            o.amazon_order_id,
            SUM(ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0)) as rev
        FROM dbo.acc_order_line ol WITH (NOLOCK)
        JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        LEFT JOIN dbo.acc_order_logistics_fact olf WITH (NOLOCK)
            ON olf.amazon_order_id = o.amazon_order_id
        WHERE o.amazon_order_id IN (
            SELECT amazon_order_id FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
            GROUP BY amazon_order_id HAVING COUNT(*) > 1
        )
        AND o.status = 'Shipped'
        GROUP BY o.amazon_order_id
    ),
    new_join AS (
        SELECT
            o.amazon_order_id,
            SUM(ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0)) as rev
        FROM dbo.acc_order_line ol WITH (NOLOCK)
        JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        OUTER APPLY (
            SELECT TOP 1 olf_inner.total_logistics_pln
            FROM dbo.acc_order_logistics_fact olf_inner WITH (NOLOCK)
            WHERE olf_inner.amazon_order_id = o.amazon_order_id
            ORDER BY olf_inner.calculated_at DESC
        ) olf
        WHERE o.amazon_order_id IN (
            SELECT amazon_order_id FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
            GROUP BY amazon_order_id HAVING COUNT(*) > 1
        )
        AND o.status = 'Shipped'
        GROUP BY o.amazon_order_id
    )
    SELECT
        SUM(o.rev) as old_total_rev,
        SUM(n.rev) as new_total_rev,
        SUM(o.rev) - SUM(n.rev) as inflated_by
    FROM old_join o
    JOIN new_join n ON n.amazon_order_id = o.amazon_order_id
""")
row = cur.fetchone()
if row:
    print(f"  OLD total revenue:    {row[0]:,.2f} PLN")
    print(f"  NEW total revenue:    {row[1]:,.2f} PLN")
    print(f"  Revenue was inflated: {row[2]:,.2f} PLN")

print("\n=== VALIDATION COMPLETE ===")
conn.close()
