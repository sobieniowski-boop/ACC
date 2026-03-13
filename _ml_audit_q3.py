"""Check logistics configuration and schema."""
import sys
sys.path.insert(0, "apps/api")
from app.core.db_connection import connect_acc
from app.services.order_logistics_source import profit_uses_logistics_fact

results = []
results.append(f"PROFIT_USE_LOGISTICS_FACT: {profit_uses_logistics_fact()}")

conn = connect_acc()
cur = conn.cursor()

# Schema
cur.execute("""
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS WITH (NOLOCK)
WHERE TABLE_NAME = 'acc_order_logistics_fact'
ORDER BY ORDINAL_POSITION
""")
results.append("acc_order_logistics_fact schema:")
for r in cur.fetchall():
    results.append(f"  {r[0]}: {r[1]}")

# Indexes
cur.execute("""
SELECT i.name, i.is_unique, i.is_primary_key
FROM sys.indexes i
WHERE OBJECT_NAME(i.object_id) = 'acc_order_logistics_fact'
AND i.name IS NOT NULL
""")
results.append("Indexes:")
for r in cur.fetchall():
    results.append(f"  {r[0]}: unique={r[1]}, pk={r[2]}")

# Rollup logistics
cur.execute("""
SELECT COUNT(*) FROM acc_sku_profitability_rollup WITH (NOLOCK) WHERE logistics_pln > 0
""")
results.append(f"Rollup rows with logistics>0: {cur.fetchone()[0]}")

# Get the exact duplicate logistics fact data
cur.execute("""
SELECT TOP 5 olf.amazon_order_id, olf.total_logistics_pln
FROM acc_order_logistics_fact olf WITH (NOLOCK)
WHERE olf.amazon_order_id IN (
    SELECT amazon_order_id FROM acc_order_logistics_fact WITH (NOLOCK)
    GROUP BY amazon_order_id HAVING COUNT(*) > 1
)
ORDER BY olf.amazon_order_id
""")
results.append("Duplicate logistics fact samples:")
for r in cur.fetchall():
    results.append(f"  order={r[0]}, logistics_pln={r[1]}")

# Revenue divergence quantification — total impact for multi-line orders
cur.execute("""
SELECT
    SUM(ABS(o.revenue_pln - eng.engine_rev)) as total_rev_delta,
    AVG(ABS(o.revenue_pln - eng.engine_rev)) as avg_rev_delta,
    COUNT(*) as order_count
FROM acc_order o WITH (NOLOCK)
CROSS APPLY (
    SELECT
        SUM(
            (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
            * ISNULL(fx.rate_to_pln, 1.0)
        ) AS engine_rev
    FROM acc_order_line ol WITH (NOLOCK)
    OUTER APPLY (
        SELECT TOP 1 er.rate_to_pln
        FROM acc_exchange_rate er WITH (NOLOCK)
        WHERE er.currency = o.currency AND er.rate_date <= o.purchase_date
        ORDER BY er.rate_date DESC
    ) fx
    WHERE ol.order_id = o.id
) eng
WHERE o.status = 'Shipped'
  AND o.revenue_pln IS NOT NULL
  AND eng.engine_rev IS NOT NULL
  AND (SELECT COUNT(*) FROM acc_order_line ol3 WITH (NOLOCK) WHERE ol3.order_id = o.id) >= 2
""")
r = cur.fetchone()
results.append(f"V1 vs V2 revenue delta (multi-line only): total={r[0]:.2f} PLN, avg={r[1]:.2f} PLN, orders={r[2]}")

conn.close()
with open("_ml_audit_results3.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(results))
