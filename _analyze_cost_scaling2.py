# Simplified cost scaling analysis
# Q: when customer orders qty=2 of same SKU, does shipping cost double or stay similar?
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc
from collections import defaultdict
import statistics

conn = connect_acc(timeout=120)
cur = conn.cursor()

print("=" * 80)
print("COST SCALING: same SKU only, qty=1 vs qty>1")
print("=" * 80)

# Step 1: Orders with ONLY 1 unique SKU (but possibly qty>1)
# that have actual billing via gls_v1/dhl_v1
cur.execute("""
    SELECT 
        sub.sku, sub.total_qty, sub.ship_country,
        f.total_logistics_pln
    FROM (
        SELECT o.amazon_order_id, o.ship_country,
               MIN(ol.sku) as sku,
               SUM(ol.quantity_ordered) as total_qty,
               COUNT(DISTINCT ol.sku) as unique_skus
        FROM dbo.acc_order o WITH (NOLOCK)
        JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
        WHERE o.fulfillment_channel = 'MFN'
          AND o.purchase_date >= '2025-11-01'
        GROUP BY o.amazon_order_id, o.ship_country
        HAVING COUNT(DISTINCT ol.sku) = 1
    ) sub
    JOIN dbo.acc_order_logistics_fact f WITH (NOLOCK) 
        ON f.amazon_order_id = sub.amazon_order_id
        AND f.calc_version IN ('gls_v1', 'dhl_v1')
""")

# Build: SKU → country → qty → [cost]
sku_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
total_rows = 0
for sku, qty, country, cost in cur.fetchall():
    if sku and cost and country:
        sku_data[sku][country][int(qty)].append(float(cost))
        total_rows += 1

print(f"Total records: {total_rows:,}")
print(f"Unique SKUs: {len(sku_data):,}\n")

# Find SKUs with both qty=1 AND qty>=2 data
scaling = []
for sku, countries in sku_data.items():
    for country, qtys in countries.items():
        if 1 in qtys and any(q >= 2 for q in qtys):
            med_1 = statistics.median(qtys[1])
            n_1 = len(qtys[1])
            for q in sorted(qtys.keys()):
                if q >= 2:
                    med_q = statistics.median(qtys[q])
                    n_q = len(qtys[q])
                    scaling.append({
                        'sku': sku, 'country': country, 'qty': q,
                        'n1': n_1, 'nq': n_q,
                        'cost1': med_1, 'costq': med_q,
                        'ratio': med_q / med_1 if med_1 > 0 else 0
                    })

scaling.sort(key=lambda x: x['nq'], reverse=True)
print(f"SKU x country x qty combos with both qty=1 and qty>=2: {len(scaling)}\n")

print(f"{'SKU':<28} {'Kraj':<5} {'Qty':<5} {'N(1)':<6} {'N(q)':<6} {'Cost(1)':<10} {'Cost(q)':<10} {'Ratio':<8}")
print("-" * 85)
for d in scaling[:50]:
    print(f"{d['sku']:<28} {d['country']:<5} {d['qty']:<5} {d['n1']:<6} {d['nq']:<6} {d['cost1']:<10.2f} {d['costq']:<10.2f} {d['ratio']:.2f}x")

# Summary
if scaling:
    # Only with N>=2 for reliability
    reliable = [s for s in scaling if s['nq'] >= 2]
    if reliable:
        ratios = [s['ratio'] for s in reliable]
        print(f"\n--- Summary (N(q)>=2 samples: {len(reliable)} combos) ---")
        print(f"  Median ratio (costQ / cost1): {statistics.median(ratios):.2f}x")
        print(f"  Mean ratio:                   {sum(ratios)/len(ratios):.2f}x")
        
        # Bucket by ratio
        for lo, hi, label in [(0, 0.8, "cheaper than 1"), (0.8, 1.1, "~same"), (1.1, 1.5, "slightly more"), (1.5, 2.0, "1.5-2x"), (2.0, 99, ">2x")]:
            n = sum(1 for r in ratios if lo <= r < hi)
            print(f"  {label:<20}: {n} ({100*n/len(ratios):.0f}%)")

# Multi-different-SKU: use line_count bucket for actual billing
print(f"\n\n{'='*80}")
print("MULTI-DIFFERENT-SKU ORDERS: billing cost by line_count")
print("=" * 80)

cur.execute("""
    SELECT sub.line_count, sub.ship_country,
           AVG(f.total_logistics_pln) as avg_cost,
           COUNT(*) as n
    FROM (
        SELECT o.amazon_order_id, o.ship_country,
               COUNT(DISTINCT ol.sku) as line_count
        FROM dbo.acc_order o WITH (NOLOCK)
        JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
        WHERE o.fulfillment_channel = 'MFN'
          AND o.purchase_date >= '2025-11-01'
        GROUP BY o.amazon_order_id, o.ship_country
    ) sub
    JOIN dbo.acc_order_logistics_fact f WITH (NOLOCK)
        ON f.amazon_order_id = sub.amazon_order_id
        AND f.calc_version IN ('gls_v1', 'dhl_v1')
    GROUP BY sub.line_count, sub.ship_country
    HAVING COUNT(*) >= 5
    ORDER BY sub.ship_country, sub.line_count
""")

current_country = None
for lines, country, avg_cost, n in cur.fetchall():
    if country != current_country:
        print(f"\n  {country}:")
        current_country = country
    print(f"    {lines} unique SKU: avg={float(avg_cost):.2f} PLN (N={n})")

conn.close()
print("\nDone.")
