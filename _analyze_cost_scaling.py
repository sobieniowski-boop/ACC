# Analyze actual cost scaling for multi-quantity orders
# Key question: when customer buys 2-3 small items (doniczki, balkonowki),
# does cost scale linearly or stay flat (fit in 1 box)?
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc
from collections import defaultdict
import statistics

conn = connect_acc(timeout=120)
cur = conn.cursor()

# 1. Find SKU-level cost scaling: same SKU, qty=1 vs qty=2 vs qty=3
print("=" * 80)
print("COST SCALING: same SKU, different quantities (from actual GLS/DHL billing)")
print("=" * 80)

# Get orders where a single SKU appears with qty > 1 or multiple lines same SKU
# vs orders with qty=1 of same SKU → compare actual shipping cost
cur.execute("""
    SELECT 
        ol.sku,
        SUM(ol.quantity_ordered) as total_qty,
        COUNT(DISTINCT ol.id) as line_count,
        costs.total_cost,
        gbl.billing_weight,
        gbl.weight as real_weight,
        o.ship_country,
        COUNT(DISTINCT sol.shipment_id) as parcels
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = o.amazon_order_id
    OUTER APPLY (
        SELECT SUM(sc.net_amount + ISNULL(sc.fuel_amount, 0) + ISNULL(sc.toll_amount, 0)) as total_cost
        FROM dbo.acc_shipment_order_link sol2 WITH (NOLOCK)
        JOIN dbo.acc_shipment_cost sc WITH (NOLOCK) ON sc.shipment_id = sol2.shipment_id
        WHERE sol2.amazon_order_id = o.amazon_order_id AND sc.net_amount > 0
    ) costs
    OUTER APPLY (
        SELECT TOP 1 gbl2.billing_weight, gbl2.weight
        FROM dbo.acc_shipment s WITH (NOLOCK)
        JOIN dbo.acc_gls_billing_line gbl2 WITH (NOLOCK) ON gbl2.parcel_number = s.tracking_number
        WHERE s.id = sol.shipment_id AND gbl2.billing_weight > 0
    ) gbl
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-01-01'
      AND NOT EXISTS (
          SELECT 1 FROM dbo.acc_order_line ol2 WITH (NOLOCK) 
          WHERE ol2.order_id = o.id AND ol2.sku != ol.sku
      )
    GROUP BY ol.sku, costs.total_cost, gbl.billing_weight, gbl.weight,
             o.ship_country, o.amazon_order_id
    HAVING costs.total_cost > 0
""")

# Build: SKU → {country → {qty → [cost, weight]}}
sku_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
for row in cur.fetchall():
    sku, qty, lines, cost, bw, rw, country, parcels = row
    if sku and cost and country:
        total_qty = int(qty)
        sku_data[sku][country][total_qty].append({
            'cost': float(cost),
            'bw': float(bw) if bw else None,
            'rw': float(rw) if rw else None,
            'parcels': parcels
        })

# Find SKUs that have BOTH qty=1 and qty>1 data for same country
print(f"\nSKUs with multi-qty data: ", end="")
scaling_data = []
for sku, countries in sku_data.items():
    for country, qtys in countries.items():
        if 1 in qtys and any(q > 1 for q in qtys):
            med_1 = statistics.median([d['cost'] for d in qtys[1]])
            bw_1 = statistics.median([d['bw'] for d in qtys[1] if d['bw']]) if any(d['bw'] for d in qtys[1]) else None
            for q in sorted(qtys.keys()):
                if q > 1:
                    med_q = statistics.median([d['cost'] for d in qtys[q]])
                    bw_q = statistics.median([d['bw'] for d in qtys[q] if d['bw']]) if any(d['bw'] for d in qtys[q]) else None
                    scaling_data.append({
                        'sku': sku, 'country': country,
                        'qty': q, 'n_single': len(qtys[1]), 'n_multi': len(qtys[q]),
                        'cost_1': med_1, 'cost_q': med_q,
                        'ratio': med_q / med_1 if med_1 > 0 else None,
                        'bw_1': bw_1, 'bw_q': bw_q
                    })

print(f"{len(scaling_data)} SKU×country×qty combos\n")

if scaling_data:
    # Sort by sample size
    scaling_data.sort(key=lambda x: x['n_multi'], reverse=True)
    
    print(f"{'SKU':<25} {'Kraj':<5} {'Q':<4} {'N(1)':<6} {'N(q)':<6} {'Cost(1)':<10} {'Cost(q)':<10} {'Ratio':<8} {'BW(1)':<8} {'BW(q)':<8}")
    print("-" * 95)
    for d in scaling_data[:40]:
        bw1 = f"{d['bw_1']:.1f}" if d['bw_1'] else "?"
        bwq = f"{d['bw_q']:.1f}" if d['bw_q'] else "?"
        ratio = f"{d['ratio']:.2f}x" if d['ratio'] else "?"
        print(f"{d['sku']:<25} {d['country']:<5} {d['qty']:<4} {d['n_single']:<6} {d['n_multi']:<6} {d['cost_1']:<10.2f} {d['cost_q']:<10.2f} {ratio:<8} {bw1:<8} {bwq:<8}")

    # Summary stats
    ratios = [d['ratio'] for d in scaling_data if d['ratio'] and d['n_multi'] >= 2]
    if ratios:
        print(f"\n--- Cost scaling ratios (qty>1 / qty=1), N≥2 samples ---")
        print(f"  Median ratio:  {statistics.median(ratios):.2f}x")
        print(f"  Mean ratio:    {sum(ratios)/len(ratios):.2f}x")
        print(f"  Min ratio:     {min(ratios):.2f}x")
        print(f"  Max ratio:     {max(ratios):.2f}x")
        print(f"  <=1.0x (same): {sum(1 for r in ratios if r <= 1.0)} ({100*sum(1 for r in ratios if r <= 1.0)/len(ratios):.0f}%)")
        print(f"  1.0-1.5x:     {sum(1 for r in ratios if 1.0 < r <= 1.5)} ({100*sum(1 for r in ratios if 1.0 < r <= 1.5)/len(ratios):.0f}%)")
        print(f"  >1.5x:        {sum(1 for r in ratios if r > 1.5)} ({100*sum(1 for r in ratios if r > 1.5)/len(ratios):.0f}%)")

# 2. Also look at multi-different-SKU orders: actual billing weight
print(f"\n\n{'='*80}")
print("MULTI-LINE ORDERS: actual billing weight distribution")
print("=" * 80)

cur.execute("""
    SELECT 
        sub.line_count,
        AVG(gbl.billing_weight) as avg_bw,
        AVG(gbl.weight) as avg_rw,
        AVG(sub.total_cost) as avg_cost,
        COUNT(*) as n
    FROM (
        SELECT o.amazon_order_id,
               COUNT(DISTINCT ol.id) as line_count,
               SUM(sc.net_amount + ISNULL(sc.fuel_amount, 0) + ISNULL(sc.toll_amount, 0)) as total_cost
        FROM dbo.acc_order o WITH (NOLOCK)
        JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
        JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = o.amazon_order_id
        JOIN dbo.acc_shipment_cost sc WITH (NOLOCK) ON sc.shipment_id = sol.shipment_id
        WHERE o.fulfillment_channel = 'MFN'
          AND o.purchase_date >= '2026-01-01'
          AND sc.net_amount > 0
        GROUP BY o.amazon_order_id
        HAVING COUNT(DISTINCT sol.shipment_id) = 1
    ) sub
    JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = sub.amazon_order_id
    JOIN dbo.acc_shipment s WITH (NOLOCK) ON s.id = sol.shipment_id
    JOIN dbo.acc_gls_billing_line gbl WITH (NOLOCK) ON gbl.parcel_number = s.tracking_number
    WHERE gbl.billing_weight > 0
    GROUP BY sub.line_count
    ORDER BY sub.line_count
""")

print(f"{'Lines':<8} {'Avg BW (kg)':<14} {'Avg Real (kg)':<16} {'Avg Cost (PLN)':<16} {'N':<8}")
print("-" * 60)
for row in cur.fetchall():
    lines, avg_bw, avg_rw, avg_cost, n = row
    print(f"{lines:<8} {float(avg_bw):<14.2f} {float(avg_rw):<16.2f} {float(avg_cost):<16.2f} {n:<8}")

conn.close()
print("\nDone.")
