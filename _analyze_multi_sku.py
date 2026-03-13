"""
Analyze multi-SKU orders: how many parcels per order?
Are items combined or shipped separately?
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc
from collections import defaultdict

conn = connect_acc(timeout=120)
cur = conn.cursor()

# 1. Multi-line orders: how many parcels per order?
print("=" * 70)
print("MULTI-LINE MFN ORDERS (Jan-Mar 2026): PARCELS PER ORDER")
print("=" * 70)

cur.execute("""
    SELECT o.amazon_order_id,
           COUNT(DISTINCT ol.id) as line_count,
           COUNT(DISTINCT sol.shipment_id) as parcel_count
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    LEFT JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) 
        ON sol.amazon_order_id = o.amazon_order_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-01-01'
    GROUP BY o.amazon_order_id
    HAVING COUNT(DISTINCT ol.id) > 1 AND COUNT(DISTINCT sol.shipment_id) > 0
""")

rows = cur.fetchall()
print(f"Total multi-line orders with shipment data: {len(rows):,}\n")

# Bucket analysis
one_parcel = []
multi_parcel = []
for amazon_id, lines, parcels in rows:
    if parcels == 1:
        one_parcel.append((amazon_id, lines, parcels))
    else:
        multi_parcel.append((amazon_id, lines, parcels))

print(f"  1 paczka na zamowienie:  {len(one_parcel):,} ({100*len(one_parcel)/len(rows):.1f}%)")
print(f"  Wiele paczek:            {len(multi_parcel):,} ({100*len(multi_parcel)/len(rows):.1f}%)")

# Detail by line count
print("\nBreakdown by line count:")
from collections import Counter
for lines in sorted(set(r[1] for r in rows)):
    subset = [r for r in rows if r[1] == lines]
    one = sum(1 for r in subset if r[2] == 1)
    print(f"  {lines} linii: {len(subset):,} zamowien | 1 paczka: {one} ({100*one/len(subset):.0f}%) | wiele: {len(subset)-one}")

# 2. Cost comparison: 1-parcel vs multi-parcel for multi-line orders
print("\n" + "=" * 70)
print("KOSZT: 1 PACZKA vs WIELE PACZEK (multi-line orders)")
print("=" * 70)

cur.execute("""
    SELECT 
        sub.amazon_order_id,
        sub.line_count,
        sub.parcel_count,
        ISNULL(costs.total_cost, 0) as total_cost
    FROM (
        SELECT o.amazon_order_id,
               COUNT(DISTINCT ol.id) as line_count,
               COUNT(DISTINCT sol.shipment_id) as parcel_count
        FROM dbo.acc_order o WITH (NOLOCK)
        JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
        JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) 
            ON sol.amazon_order_id = o.amazon_order_id
        WHERE o.fulfillment_channel = 'MFN'
          AND o.purchase_date >= '2026-01-01'
        GROUP BY o.amazon_order_id
        HAVING COUNT(DISTINCT ol.id) > 1
    ) sub
    OUTER APPLY (
        SELECT SUM(sc.net_amount + ISNULL(sc.fuel_amount, 0) + ISNULL(sc.toll_amount, 0)) as total_cost
        FROM dbo.acc_shipment_order_link sol2 WITH (NOLOCK)
        JOIN dbo.acc_shipment_cost sc WITH (NOLOCK) ON sc.shipment_id = sol2.shipment_id
        WHERE sol2.amazon_order_id = sub.amazon_order_id
          AND sc.net_amount > 0
    ) costs
""")

rows2 = cur.fetchall()
one_costs = [float(r[3]) for r in rows2 if r[2] == 1 and r[3] > 0]
multi_costs = [float(r[3]) for r in rows2 if r[2] > 1 and r[3] > 0]

import statistics
if one_costs:
    print(f"\n  1 paczka:   {len(one_costs):,} zamowien | median={statistics.median(one_costs):.2f} | avg={sum(one_costs)/len(one_costs):.2f} | total={sum(one_costs):,.2f} PLN")
if multi_costs:
    print(f"  Wiele paczek: {len(multi_costs):,} zamowien | median={statistics.median(multi_costs):.2f} | avg={sum(multi_costs)/len(multi_costs):.2f} | total={sum(multi_costs):,.2f} PLN")

# 3. Specific products: doniczki, balkonowki
print("\n" + "=" * 70)
print("PRZYKLAD: SKU z 'DON' (doniczki) i 'BAL' (balkonowki) w multi-line")
print("=" * 70)

cur.execute("""
    SELECT ol.sku, ol.asin, COUNT(DISTINCT o.amazon_order_id) as orders
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-01-01'
      AND (ol.sku LIKE '%DON%' OR ol.sku LIKE '%BAL%' OR ol.sku LIKE '%DONICZK%' 
           OR ol.sku LIKE '%BALKON%' OR ol.sku LIKE '%POT%' OR ol.sku LIKE '%PLANTER%')
    GROUP BY ol.sku, ol.asin
    ORDER BY orders DESC
""")
pot_rows = cur.fetchall()
if pot_rows:
    for sku, asin, orders in pot_rows[:20]:
        print(f"  SKU={sku} ASIN={asin} orders={orders}")
else:
    print("  No matching SKUs found. Trying broader search...")
    # Show top SKUs in multi-line orders
    cur.execute("""
        SELECT TOP 30 ol.sku, COUNT(DISTINCT o.amazon_order_id) as multi_orders
        FROM dbo.acc_order o WITH (NOLOCK)
        JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
        WHERE o.fulfillment_channel = 'MFN'
          AND o.purchase_date >= '2026-01-01'
          AND EXISTS (
              SELECT 1 FROM dbo.acc_order_line ol2 WITH (NOLOCK) 
              WHERE ol2.order_id = o.id AND ol2.id != ol.id
          )
        GROUP BY ol.sku
        ORDER BY multi_orders DESC
    """)
    for sku, cnt in cur.fetchall():
        print(f"  SKU={sku} multi_orders={cnt}")

# 4. Model v2 check: what did we assign to multi-line orders?
print("\n" + "=" * 70)
print("MODEL V2: ESTYMACJA vs RZECZYWISTOSC dla multi-line orders")
print("=" * 70)

cur.execute("""
    SELECT TOP 20
        o.amazon_order_id,
        o.ship_country,
        (SELECT COUNT(*) FROM dbo.acc_order_line ol WITH (NOLOCK) WHERE ol.order_id = o.id) as lines,
        f_est.total_logistics_pln as estimated,
        f_act.total_logistics_pln as actual,
        (SELECT COUNT(DISTINCT sol.shipment_id) FROM dbo.acc_shipment_order_link sol WITH (NOLOCK) WHERE sol.amazon_order_id = o.amazon_order_id) as parcels
    FROM dbo.acc_order o WITH (NOLOCK)
    CROSS APPLY (
        SELECT TOP 1 total_logistics_pln 
        FROM dbo.acc_order_logistics_fact WITH (NOLOCK) 
        WHERE amazon_order_id = o.amazon_order_id AND calc_version = 'sku_country_v2'
    ) f_est
    OUTER APPLY (
        SELECT SUM(sc.net_amount + ISNULL(sc.fuel_amount,0) + ISNULL(sc.toll_amount,0)) as total_logistics_pln
        FROM dbo.acc_shipment_order_link sol WITH (NOLOCK)
        JOIN dbo.acc_shipment_cost sc WITH (NOLOCK) ON sc.shipment_id = sol.shipment_id
        WHERE sol.amazon_order_id = o.amazon_order_id AND sc.net_amount > 0
    ) f_act
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01'
      AND (SELECT COUNT(*) FROM dbo.acc_order_line ol WITH (NOLOCK) WHERE ol.order_id = o.id) > 1
    ORDER BY ABS(ISNULL(f_est.total_logistics_pln,0) - ISNULL(f_act.total_logistics_pln,0)) DESC
""")

print(f"{'Order ID':<25} {'Kraj':<5} {'Linii':<6} {'Estym':<10} {'Rzeczyw':<10} {'Paczek':<7} {'Diff':<8}")
print("-" * 75)
for r in cur.fetchall():
    est = float(r[3]) if r[3] else 0
    act = float(r[4]) if r[4] else 0
    diff = est - act
    print(f"{r[0]:<25} {r[1] or '?':<5} {r[2]:<6} {est:<10.2f} {act:<10.2f} {r[5]:<7} {diff:+.2f}")

conn.close()
print("\nDone.")
