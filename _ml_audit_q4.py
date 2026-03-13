"""Final audit: PK structure + summary impact."""
import sys
sys.path.insert(0, "apps/api")
from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()
results = []

# Q1: What's the PK of acc_order_logistics_fact?
cur.execute("""
SELECT kcu.COLUMN_NAME
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc WITH (NOLOCK)
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu WITH (NOLOCK)
    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
WHERE tc.TABLE_NAME = 'acc_order_logistics_fact'
  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
ORDER BY kcu.ORDINAL_POSITION
""")
results.append("PK columns: " + str([r[0] for r in cur.fetchall()]))

# Q2: Get IX_acc_order_logistics_fact_order columns
cur.execute("""
SELECT c.name
FROM sys.indexes i
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE i.name = 'IX_acc_order_logistics_fact_order'
""")
results.append("IX_acc_order_logistics_fact_order columns: " + str([r[0] for r in cur.fetchall()]))

# Q3: What does the PK look like for duplicates?
cur.execute("""
SELECT TOP 4 olf.*
FROM acc_order_logistics_fact olf WITH (NOLOCK)
WHERE olf.amazon_order_id = '028-0400778-4486722'
""")
desc = [d[0] for d in cur.description]
results.append(f"Columns: {desc}")
for r in cur.fetchall():
    results.append(f"  {dict(zip(desc, r))}")

# Q4: Total monetary impact of logistics fact duplicates
cur.execute("""
;WITH dup_orders AS (
    SELECT amazon_order_id
    FROM acc_order_logistics_fact WITH (NOLOCK)
    GROUP BY amazon_order_id
    HAVING COUNT(*) > 1
),
affected AS (
    SELECT
        o.amazon_order_id,
        SUM(ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0)) as rev_raw,
        SUM(ISNULL(ol.cogs_pln, 0)) as cogs,
        COUNT(*) as lines
    FROM acc_order o WITH (NOLOCK)
    JOIN acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE o.status = 'Shipped'
      AND o.amazon_order_id IN (SELECT amazon_order_id FROM dup_orders)
    GROUP BY o.amazon_order_id
)
SELECT COUNT(*), SUM(rev_raw), SUM(cogs), SUM(lines)
FROM affected
""")
r = cur.fetchone()
results.append(f"Impact of logistics dups: {r[0]} orders, rev_raw={r[1]}, cogs={r[2]}, total_lines={r[3]}")
results.append("These orders have ALL line values doubled due to row multiplication")

# Q5: Does the drilldown also have this problem?
# Yes — same LEFT JOIN pattern used in get_product_drilldown and get_loss_orders

# Q6: Summary — what % of multi-line orders are also affected by logistics dups?
cur.execute("""
SELECT COUNT(*)
FROM (
    SELECT o.id
    FROM acc_order o WITH (NOLOCK)
    JOIN acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE o.status = 'Shipped'
      AND o.amazon_order_id IN (
          SELECT amazon_order_id FROM acc_order_logistics_fact WITH (NOLOCK)
          GROUP BY amazon_order_id HAVING COUNT(*) > 1
      )
    GROUP BY o.id
    HAVING COUNT(*) >= 2
) x
""")
results.append(f"Multi-line + logistics-dup overlap: {cur.fetchone()[0]} orders")

conn.close()
with open("_ml_audit_results4.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(results))
