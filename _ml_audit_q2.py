"""Quantify impact of logistics fact duplicates and revenue consistency."""
import sys
sys.path.insert(0, "apps/api")
from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()
results = []

# Q1: How many orders are affected by logistics_fact duplicates
try:
    cur.execute("""
    SELECT COUNT(*) as dup_orders
    FROM (
        SELECT amazon_order_id
        FROM acc_order_logistics_fact WITH (NOLOCK)
        GROUP BY amazon_order_id
        HAVING COUNT(*) > 1
    ) x
    """)
    results.append(f"Orders with duplicate logistics facts: {cur.fetchone()[0]}")
    
    # Check if those dups are in shipped orders with multi-lines
    cur.execute("""
    SELECT COUNT(DISTINCT o.id)
    FROM acc_order o WITH (NOLOCK)
    JOIN acc_order_logistics_fact olf WITH (NOLOCK) ON olf.amazon_order_id = o.amazon_order_id
    WHERE o.status = 'Shipped'
    AND o.amazon_order_id IN (
        SELECT amazon_order_id
        FROM acc_order_logistics_fact WITH (NOLOCK)
        GROUP BY amazon_order_id
        HAVING COUNT(*) > 1
    )
    """)
    results.append(f"Of which shipped: {cur.fetchone()[0]}")
    
    # Sample a dup to see values
    cur.execute("""
    SELECT TOP 3
        olf.amazon_order_id,
        olf.total_logistics_pln,
        olf.courier,
        olf.tracking_number
    FROM acc_order_logistics_fact olf WITH (NOLOCK)
    WHERE olf.amazon_order_id IN (
        SELECT TOP 1 amazon_order_id
        FROM acc_order_logistics_fact WITH (NOLOCK)
        GROUP BY amazon_order_id
        HAVING COUNT(*) > 1
    )
    """)
    for r in cur.fetchall():
        results.append(f"  Dup sample: order={r[0]}, logistics={r[1]}, courier={r[2]}, tracking={r[3]}")
    
except Exception as e:
    results.append(f"Logistics fact analysis error: {e}")

# Q2: Verify row multiplication impact
# For orders with logistics_fact duplicates, check if GROUP BY in profit_engine 
# would produce wrong results  
try:
    cur.execute("""
    SELECT
        o.amazon_order_id,
        COUNT(*) as raw_row_count,
        (SELECT COUNT(*) FROM acc_order_line ol2 WITH (NOLOCK) WHERE ol2.order_id = o.id) as actual_lines,
        (SELECT COUNT(*) FROM acc_order_logistics_fact olf2 WITH (NOLOCK) WHERE olf2.amazon_order_id = o.amazon_order_id) as fact_rows
    FROM acc_order o WITH (NOLOCK)
    JOIN acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    LEFT JOIN acc_order_logistics_fact olf WITH (NOLOCK) ON olf.amazon_order_id = o.amazon_order_id
    WHERE o.amazon_order_id IN (
        SELECT TOP 5 amazon_order_id
        FROM acc_order_logistics_fact WITH (NOLOCK)
        GROUP BY amazon_order_id
        HAVING COUNT(*) > 1
    )
    GROUP BY o.amazon_order_id, o.id
    """)
    results.append("Row multiplication check (orders with dup logistics facts):")
    for r in cur.fetchall():
        results.append(f"  {r[0]}: JOIN produces {r[1]} rows, actual lines={r[2]}, fact rows={r[3]}, multiplier={r[1]/r[2]:.1f}x")
except Exception as e:
    results.append(f"Multiplication check error: {e}")

# Q3: profitability_service.py TOP 1 SKU loss analysis
# For multi-SKU orders, how much revenue is misattributed?
cur.execute("""
SELECT 
    COUNT(*) as orders_affected,
    SUM(ISNULL(o.revenue_pln, 0)) as total_revenue_pln,
    SUM(ISNULL(o.contribution_margin_pln, 0)) as total_cm_pln
FROM acc_order o WITH (NOLOCK)
WHERE o.status = 'Shipped'
AND (SELECT COUNT(DISTINCT ol.sku) FROM acc_order_line ol WITH (NOLOCK) WHERE ol.order_id = o.id) >= 2
""")
r = cur.fetchone()
results.append(f"Multi-SKU orders: {r[0]} orders, rev={r[1]} PLN, cm={r[2]} PLN")

# Q4: profit_service order-level CM vs profit_engine line-level CM (with FX)
# Re-do comparison with FX applied
cur.execute("""
SELECT TOP 5
    o.amazon_order_id,
    ISNULL(o.revenue_pln, 0) as order_rev_pln,
    ISNULL(o.contribution_margin_pln, 0) as order_cm_pln,
    SUM(
        (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
        * ISNULL(fx.rate_to_pln, 1.0)
    ) as engine_rev_pln,
    SUM(ISNULL(ol.cogs_pln, 0)) as engine_cogs_pln,
    SUM(ISNULL(ol.fba_fee_pln, 0) + ISNULL(ol.referral_fee_pln, 0)) as engine_fees_pln,
    COUNT(*) as line_count
FROM acc_order o WITH (NOLOCK)
JOIN acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
OUTER APPLY (
    SELECT TOP 1 er.rate_to_pln
    FROM acc_exchange_rate er WITH (NOLOCK)
    WHERE er.currency = o.currency
      AND er.rate_date <= o.purchase_date
    ORDER BY er.rate_date DESC
) fx
WHERE o.status = 'Shipped'
  AND o.contribution_margin_pln IS NOT NULL
GROUP BY o.amazon_order_id, o.revenue_pln, o.contribution_margin_pln
HAVING COUNT(*) >= 3
ORDER BY ABS(ISNULL(o.revenue_pln, 0) - SUM(
    (ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0))
    * ISNULL(fx.rate_to_pln, 1.0)
)) DESC
""")
results.append("Order-level vs engine-level revenue comparison (top divergences, 3+ lines):")
for r in cur.fetchall():
    delta = abs((r[1] or 0) - (r[3] or 0))
    results.append(f"  {r[0]}: order_rev={r[1]:.2f}, engine_rev={r[3]:.2f}, delta={delta:.2f} PLN, lines={r[6]}")

# Q5: Check if refund_amount_pln is negative (means it's stored as loss)
cur.execute("""
SELECT
    SUM(CASE WHEN refund_amount_pln > 0 THEN 1 ELSE 0 END) as positive,
    SUM(CASE WHEN refund_amount_pln < 0 THEN 1 ELSE 0 END) as negative,
    SUM(CASE WHEN refund_amount_pln = 0 THEN 1 ELSE 0 END) as zero,
    COUNT(*) as total
FROM acc_order WITH (NOLOCK)
WHERE is_refund = 1
""")
r = cur.fetchone()
results.append(f"Refund amounts: positive={r[0]}, negative={r[1]}, zero={r[2]}, total={r[3]}")

# Q6: Drilldown revenue for refund order — does adding refund to revenue make sense?
cur.execute("""
SELECT TOP 3
    o.amazon_order_id,
    o.refund_amount_pln,
    o.refund_type,
    SUM(ISNULL(ol.item_price, 0) - ISNULL(ol.item_tax, 0) - ISNULL(ol.promotion_discount, 0)) as line_rev_raw,
    COUNT(*) as lines
FROM acc_order o WITH (NOLOCK)
JOIN acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
WHERE o.is_refund = 1 AND o.status = 'Shipped'
GROUP BY o.amazon_order_id, o.refund_amount_pln, o.refund_type
HAVING COUNT(*) >= 2
ORDER BY ABS(o.refund_amount_pln) DESC
""")
results.append("Refund orders — refund_amount vs line revenue (multi-line):")
for r in cur.fetchall():
    results.append(f"  {r[0]}: refund_pln={r[1]}, type={r[2]}, line_rev_raw={r[3]}, lines={r[4]}")

conn.close()
with open("_ml_audit_results2.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(results))
