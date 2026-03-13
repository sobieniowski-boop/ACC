"""Multi-line order audit queries."""
import sys
sys.path.insert(0, "apps/api")
from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()
results = []

# Q1: Multi-line order stats
cur.execute("""
SELECT ml.multi_line_orders, tc.total_orders, ml.multi_line_total_lines
FROM (
    SELECT COUNT(*) as multi_line_orders, SUM(line_count) as multi_line_total_lines
    FROM (
        SELECT o.id, COUNT(*) as line_count
        FROM acc_order o WITH (NOLOCK)
        JOIN acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
        WHERE o.status = 'Shipped'
        GROUP BY o.id
        HAVING COUNT(*) >= 2
    ) x
) ml
CROSS JOIN (
    SELECT COUNT(DISTINCT o2.id) as total_orders
    FROM acc_order o2 WITH (NOLOCK)
    WHERE o2.status = 'Shipped'
) tc
""")
row = cur.fetchone()
results.append(f"Multi-line orders: {row[0]}, Total shipped orders: {row[1]}, Lines in multi-line: {row[2]}")

# Q2: Logistics fact cardinality duplicates
try:
    cur.execute("""
    SELECT TOP 5 amazon_order_id, COUNT(*) as cnt
    FROM acc_order_logistics_fact WITH (NOLOCK)
    GROUP BY amazon_order_id
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
    """)
    dups = cur.fetchall()
    results.append(f"Logistics fact duplicates (top 5): {dups}")
except Exception as e:
    results.append(f"Logistics fact query error: {e}")

# Q3: Multi-line refund orders
cur.execute("""
SELECT COUNT(*) FROM (
    SELECT o.id
    FROM acc_order o WITH (NOLOCK)
    JOIN acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE o.status = 'Shipped' AND o.is_refund = 1
    GROUP BY o.id
    HAVING COUNT(*) >= 2
) x
""")
results.append(f"Multi-line refund orders: {cur.fetchone()[0]}")

# Q4: Sample multi-line refund orders
cur.execute("""
SELECT TOP 5
    o.amazon_order_id, o.refund_amount_pln, o.refund_type,
    COUNT(*) as lines,
    SUM(ISNULL(ol.item_price, 0)) as total_item_price
FROM acc_order o WITH (NOLOCK)
JOIN acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
WHERE o.is_refund = 1 AND o.status = 'Shipped'
GROUP BY o.amazon_order_id, o.refund_amount_pln, o.refund_type
HAVING COUNT(*) >= 2
ORDER BY o.refund_amount_pln DESC
""")
for r in cur.fetchall():
    results.append(f"  Refund order {r[0]}: refund={r[1]} PLN, type={r[2]}, lines={r[3]}, total_price={r[4]}")

# Q5: Line distribution in multi-line orders
cur.execute("""
SELECT line_count, COUNT(*) as order_count
FROM (
    SELECT o.id, COUNT(*) as line_count
    FROM acc_order o WITH (NOLOCK)
    JOIN acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE o.status = 'Shipped'
    GROUP BY o.id
    HAVING COUNT(*) >= 2
) x
GROUP BY line_count
ORDER BY line_count
""")
results.append("Line count distribution (multi-line only):")
for r in cur.fetchall():
    results.append(f"  {r[0]} lines: {r[1]} orders")

# Q6: Check refund allocation precision loss
# For multi-line refund orders, verify sum(line_share) == 1.0
cur.execute("""
SELECT TOP 10
    o.amazon_order_id,
    o.refund_amount_pln,
    COUNT(*) as lines,
    SUM(ISNULL(ol.item_price, 0)) as sum_item_price,
    SUM(CASE
        WHEN ISNULL(olt.order_line_total, 0) > 0
            THEN CAST(ISNULL(ol.item_price, 0) AS FLOAT) / olt.order_line_total
        WHEN ISNULL(olt.order_units_total, 0) > 0
            THEN CAST(ISNULL(ol.quantity_ordered, 0) AS FLOAT) / olt.order_units_total
        ELSE 0
    END) as sum_shares,
    ABS(1.0 - SUM(CASE
        WHEN ISNULL(olt.order_line_total, 0) > 0
            THEN CAST(ISNULL(ol.item_price, 0) AS FLOAT) / olt.order_line_total
        WHEN ISNULL(olt.order_units_total, 0) > 0
            THEN CAST(ISNULL(ol.quantity_ordered, 0) AS FLOAT) / olt.order_units_total
        ELSE 0
    END)) as share_error
FROM acc_order o WITH (NOLOCK)
JOIN acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
CROSS APPLY (
    SELECT
        ISNULL(SUM(ISNULL(ol2.item_price, 0)), 0) AS order_line_total,
        ISNULL(SUM(ISNULL(ol2.quantity_ordered, 0)), 0) AS order_units_total
    FROM acc_order_line ol2 WITH (NOLOCK)
    WHERE ol2.order_id = o.id
) olt
WHERE o.is_refund = 1 AND o.status = 'Shipped'
GROUP BY o.amazon_order_id, o.refund_amount_pln
HAVING COUNT(*) >= 2
ORDER BY ABS(1.0 - SUM(CASE
    WHEN ISNULL(olt.order_line_total, 0) > 0
        THEN CAST(ISNULL(ol.item_price, 0) AS FLOAT) / olt.order_line_total
    WHEN ISNULL(olt.order_units_total, 0) > 0
        THEN CAST(ISNULL(ol.quantity_ordered, 0) AS FLOAT) / olt.order_units_total
    ELSE 0
END)) DESC
""")
results.append("Refund share allocation precision (worst 10 multi-line refunds):")
for r in cur.fetchall():
    results.append(f"  {r[0]}: refund={r[1]}, lines={r[2]}, sum_price={r[3]}, sum_shares={r[4]:.6f}, share_error={r[5]:.6f}")

# Q7: profitability_service orders view — does TOP 1 miss multi-line SKUs?
cur.execute("""
SELECT COUNT(*) as affected_orders
FROM acc_order o WITH (NOLOCK)
WHERE o.status = 'Shipped'
AND (SELECT COUNT(DISTINCT ol.sku)
     FROM acc_order_line ol WITH (NOLOCK)
     WHERE ol.order_id = o.id) >= 2
""")
results.append(f"Orders with 2+ distinct SKUs (multi-SKU): {cur.fetchone()[0]}")

# Q8: Does profit_service.py's order-level CM match profit_engine line sums?
# Sample 5 multi-line orders and compare
cur.execute("""
SELECT TOP 5
    o.amazon_order_id,
    o.contribution_margin_pln as order_cm,
    o.revenue_pln as order_rev,
    o.cogs_pln as order_cogs,
    o.amazon_fees_pln as order_fees,
    SUM((ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))) as sum_line_rev_raw,
    SUM(ISNULL(ol.cogs_pln, 0)) as sum_line_cogs,
    SUM(ISNULL(ol.fba_fee_pln, 0) + ISNULL(ol.referral_fee_pln, 0)) as sum_line_fees,
    COUNT(*) as lines
FROM acc_order o WITH (NOLOCK)
JOIN acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
WHERE o.status = 'Shipped' AND o.contribution_margin_pln IS NOT NULL
GROUP BY o.amazon_order_id, o.contribution_margin_pln, o.revenue_pln, o.cogs_pln, o.amazon_fees_pln
HAVING COUNT(*) >= 3
ORDER BY ABS(o.contribution_margin_pln) DESC
""")
results.append("Sample multi-line orders (order CM vs line sums):")
for r in cur.fetchall():
    results.append(f"  {r[0]}: order_cm={r[1]}, order_rev={r[2]}, order_cogs={r[3]}, order_fees={r[4]} | line_rev_raw={r[5]}, line_cogs={r[6]}, line_fees={r[7]}, lines={r[8]}")

conn.close()

with open("_ml_audit_results.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(results))
