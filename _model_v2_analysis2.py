"""Model v2 analysis part 2 — shipment_cost coverage + existing calc_versions."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc

conn = connect_acc(timeout=60)
cur = conn.cursor()

# 1. calc_versions in logistics_fact
print("=== calc_versions in acc_order_logistics_fact ===")
cur.execute("""
    SELECT calc_version, COUNT(*) cnt, SUM(total_logistics_pln) total_pln
    FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
    GROUP BY calc_version
    ORDER BY cnt DESC
""")
for r in cur.fetchall():
    print(f"  {str(r[0]):<30} {r[1]:>8,} rows  {float(r[2]):>15,.2f} PLN")

# 2. How many MFN Feb-Mar orders have shipment_cost already?
print("\n=== MFN Feb-Mar orders with acc_shipment_cost ===")
cur.execute("""
    SELECT COUNT(DISTINCT o.amazon_order_id)
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = o.amazon_order_id
    JOIN dbo.acc_shipment_cost sc WITH (NOLOCK) ON sc.shipment_id = sol.shipment_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-12'
""")
print(f"  Orders with shipment_cost: {cur.fetchone()[0]:,}")

# 3. Cost source distribution in shipment_cost for Feb-Mar MFN
print("\n=== acc_shipment_cost sources for Feb-Mar MFN ===")
cur.execute("""
    SELECT sc.cost_source, COUNT(DISTINCT o.amazon_order_id) orders, 
           SUM(sc.net_amount) total_net
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = o.amazon_order_id
    JOIN dbo.acc_shipment_cost sc WITH (NOLOCK) ON sc.shipment_id = sol.shipment_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-12'
    GROUP BY sc.cost_source
    ORDER BY orders DESC
""")
for r in cur.fetchall():
    print(f"  {str(r[0]):<25} {r[1]:>8,} orders  {float(r[2]):>12,.2f} PLN")

# 4. Gap analysis: how many MFN Feb-Mar orders have NO cost at all?
print("\n=== MFN Feb-Mar gap analysis ===")
cur.execute("""
    SELECT 
        COUNT(DISTINCT o.amazon_order_id) total_mfn,
        COUNT(DISTINCT CASE WHEN f.amazon_order_id IS NOT NULL THEN o.amazon_order_id END) has_logistics_fact,
        COUNT(DISTINCT CASE WHEN sc.shipment_id IS NOT NULL THEN o.amazon_order_id END) has_shipment_cost,
        COUNT(DISTINCT CASE WHEN f.amazon_order_id IS NOT NULL OR sc.shipment_id IS NOT NULL THEN o.amazon_order_id END) has_any_cost
    FROM dbo.acc_order o WITH (NOLOCK)
    LEFT JOIN dbo.acc_order_logistics_fact f WITH (NOLOCK) ON f.amazon_order_id = o.amazon_order_id
    LEFT JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = o.amazon_order_id
    LEFT JOIN dbo.acc_shipment_cost sc WITH (NOLOCK) ON sc.shipment_id = sol.shipment_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-12'
""")
total, has_lf, has_sc, has_any = cur.fetchone()
print(f"  Total MFN orders: {total:,}")
print(f"  Has logistics_fact: {has_lf:,} ({100*has_lf/max(total,1):.1f}%)")
print(f"  Has shipment_cost: {has_sc:,} ({100*has_sc/max(total,1):.1f}%)")
print(f"  Has any cost: {has_any:,} ({100*has_any/max(total,1):.1f}%)")
print(f"  NO cost at all: {total - has_any:,} ({100*(total-has_any)/max(total,1):.1f}%)")

# 5. Check overlap: orders that have BOTH logistics_fact AND shipment_cost
print("\n=== Overlap: orders with both logistics_fact and shipment_cost ===")
cur.execute("""
    SELECT COUNT(DISTINCT o.amazon_order_id)
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_logistics_fact f WITH (NOLOCK) ON f.amazon_order_id = o.amazon_order_id
    JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = o.amazon_order_id
    JOIN dbo.acc_shipment_cost sc WITH (NOLOCK) ON sc.shipment_id = sol.shipment_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-12'
""")
print(f"  Orders with BOTH: {cur.fetchone()[0]:,}")

# 6. SKU weight lookup: top 30 SKUs by median billing_weight (single-line GLS orders)
print("\n=== Top 30 SKUs by shipment count (single-line GLS orders, all time) ===")
cur.execute("""
    ;WITH single_line_orders AS (
        SELECT ol.order_id, MIN(ol.seller_sku) sku, SUM(ol.quantity_ordered) qty
        FROM dbo.acc_order_line ol WITH (NOLOCK)
        GROUP BY ol.order_id
        HAVING COUNT(*) = 1
    ),
    sku_gls AS (
        SELECT slo.sku, g.billing_weight, g.net_amount
        FROM single_line_orders slo
        JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = slo.order_id
        JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = o.amazon_order_id
        JOIN dbo.acc_shipment s WITH (NOLOCK) ON s.id = sol.shipment_id
        JOIN dbo.acc_gls_billing_line g WITH (NOLOCK) ON g.parcel_number = s.tracking_number
        WHERE g.billing_weight > 0 AND o.fulfillment_channel = 'MFN'
          AND slo.qty = 1
    )
    SELECT sku, COUNT(*) cnt,
           AVG(billing_weight) avg_wt,
           MIN(billing_weight) min_wt,
           MAX(billing_weight) max_wt,
           AVG(net_amount) avg_cost
    FROM sku_gls
    GROUP BY sku
    HAVING COUNT(*) >= 5
    ORDER BY cnt DESC
""")
print(f"  {'SKU':<35} {'N':>5} {'Avg Wt':>8} {'Min':>6} {'Max':>6} {'Avg$':>8}")
rows = cur.fetchall()
for r in rows[:30]:
    print(f"  {str(r[0]):<35} {r[1]:>5} {float(r[2]):>8.2f} {float(r[3]):>6.1f} {float(r[4]):>6.1f} {float(r[5]):>8.2f}")
print(f"  ... total SKUs with data: {len(rows)}")

# 7. Same for DHL
print("\n=== Top 30 SKUs from DHL billing (single-line orders, all time) ===")
cur.execute("""
    ;WITH single_line_orders AS (
        SELECT ol.order_id, MIN(ol.seller_sku) sku, SUM(ol.quantity_ordered) qty
        FROM dbo.acc_order_line ol WITH (NOLOCK)
        GROUP BY ol.order_id
        HAVING COUNT(*) = 1
    ),
    sku_dhl AS (
        SELECT slo.sku, d.weight, d.net_amount
        FROM single_line_orders slo
        JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = slo.order_id
        JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = o.amazon_order_id
        JOIN dbo.acc_shipment s WITH (NOLOCK) ON s.id = sol.shipment_id
        JOIN dbo.acc_dhl_billing_line d WITH (NOLOCK) ON d.parcel_number = s.tracking_number
        WHERE d.weight > 0 AND o.fulfillment_channel = 'MFN'
          AND slo.qty = 1
    )
    SELECT sku, COUNT(*) cnt,
           AVG(weight) avg_wt,
           MIN(weight) min_wt,
           MAX(weight) max_wt,
           AVG(net_amount) avg_cost
    FROM sku_dhl
    GROUP BY sku
    HAVING COUNT(*) >= 5
    ORDER BY cnt DESC
""")
print(f"  {'SKU':<35} {'N':>5} {'Avg Wt':>8} {'Min':>6} {'Max':>6} {'Avg$':>8}")
rows = cur.fetchall()
for r in rows[:30]:
    print(f"  {str(r[0]):<35} {r[1]:>5} {float(r[2]):>8.2f} {float(r[3]):>6.1f} {float(r[4]):>6.1f} {float(r[5]):>8.2f}")
print(f"  ... total SKUs with data: {len(rows)}")

# 8. How many DISTINCT SKUs total in Feb-Mar MFN orders?
print("\n=== Distinct SKUs in Feb-Mar MFN orders ===")
cur.execute("""
    SELECT COUNT(DISTINCT ol.seller_sku)
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-12'
""")
print(f"  Distinct SKUs: {cur.fetchone()[0]:,}")

conn.close()
print("\nDone!")
