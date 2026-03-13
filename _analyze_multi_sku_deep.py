"""
Deep dive: multi-line orders — model accuracy vs actual billing.
Focus: how much does 1-parcel multi-line order cost vs sum/max of individual SKU costs?
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc
from collections import defaultdict
import statistics

conn = connect_acc(timeout=120)
cur = conn.cursor()

# Get multi-line orders with ACTUAL billing
print("=" * 70)
print("MULTI-LINE ORDERS: ACTUAL COST vs MODEL APPROACHES")
print("=" * 70)

cur.execute("""
    SELECT o.amazon_order_id, o.ship_country, ol.sku, ol.quantity_ordered
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-01-01'
      AND EXISTS (
          SELECT 1 FROM dbo.acc_shipment_order_link sol WITH (NOLOCK)
          JOIN dbo.acc_shipment_cost sc WITH (NOLOCK) ON sc.shipment_id = sol.shipment_id
          WHERE sol.amazon_order_id = o.amazon_order_id AND sc.net_amount > 0
      )
    ORDER BY o.amazon_order_id
""")

# Build order structures
orders = defaultdict(lambda: {"country": None, "lines": []})
for amazon_id, country, sku, qty in cur.fetchall():
    orders[amazon_id]["country"] = country
    orders[amazon_id]["lines"].append((sku, int(qty) if qty else 1))

multi_orders = {k: v for k, v in orders.items() if len(v["lines"]) > 1}
print(f"Multi-line orders with actual billing: {len(multi_orders):,}")

# Get actual costs
cur.execute("""
    SELECT sol.amazon_order_id,
           SUM(sc.net_amount + ISNULL(sc.fuel_amount, 0) + ISNULL(sc.toll_amount, 0)) as actual_cost,
           COUNT(DISTINCT sc.shipment_id) as parcels
    FROM dbo.acc_shipment_order_link sol WITH (NOLOCK)
    JOIN dbo.acc_shipment_cost sc WITH (NOLOCK) ON sc.shipment_id = sol.shipment_id
    WHERE sc.net_amount > 0
    GROUP BY sol.amazon_order_id
""")
actual_costs = {}
actual_parcels = {}
for amazon_id, cost, parcels in cur.fetchall():
    actual_costs[amazon_id] = float(cost)
    actual_parcels[amazon_id] = parcels

# Build SKU×country → median cost lookup (from single-SKU orders only)
cur.execute("""
    SELECT ol.sku, o.ship_country,
           sc.net_amount + ISNULL(sc.fuel_amount, 0) + ISNULL(sc.toll_amount, 0) AS cost
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = o.amazon_order_id
    JOIN dbo.acc_shipment_cost sc WITH (NOLOCK) ON sc.shipment_id = sol.shipment_id
    WHERE o.fulfillment_channel = 'MFN'
      AND sc.net_amount > 0 AND sc.is_estimated = 0
      AND NOT EXISTS (
          SELECT 1 FROM dbo.acc_order_line ol2 WITH (NOLOCK) 
          WHERE ol2.order_id = o.id AND ol2.id != ol.id
      )
""")
sku_costs_raw = defaultdict(list)
for sku, country, cost in cur.fetchall():
    if sku and country:
        sku_costs_raw[(sku, country)].append(float(cost))
sku_median = {k: statistics.median(v) for k, v in sku_costs_raw.items()}

# Country fallback
country_agg = defaultdict(list)
for (sku, c), costs in sku_costs_raw.items():
    country_agg[c].extend(costs)
country_median = {c: statistics.median(v) for c, v in country_agg.items()}

# Compare approaches for each multi-line order
results = []
for amazon_id, info in multi_orders.items():
    if amazon_id not in actual_costs:
        continue
    actual = actual_costs[amazon_id]
    country = info["country"]
    lines = info["lines"]
    parcels = actual_parcels.get(amazon_id, 0)
    
    # Individual SKU cost lookups
    sku_est = []
    for sku, qty in lines:
        key = (sku, country)
        if key in sku_median:
            sku_est.append(sku_median[key])
        elif country in country_median:
            sku_est.append(country_median[country])
    
    if not sku_est:
        continue
    
    approach_max = max(sku_est)
    approach_sum = sum(sku_est)
    approach_avg = sum(sku_est) / len(sku_est)
    
    results.append({
        "id": amazon_id, "country": country, "lines": len(lines),
        "parcels": parcels, "actual": actual,
        "max": approach_max, "sum": approach_sum, "avg": approach_avg,
    })

print(f"Comparable orders (have both actual + SKU estimates): {len(results):,}\n")

# Overall stats
if results:
    print(f"{'Approach':<20} {'Median Error':<15} {'Avg Error':<15} {'MAPE':<10} {'Overest%':<10}")
    print("-" * 70)
    
    for name, key in [("MAX (current v2)", "max"), ("SUM", "sum"), ("AVG", "avg")]:
        errors = [r[key] - r["actual"] for r in results]
        abs_errors = [abs(e) for e in errors]
        pct_errors = [abs(r[key] - r["actual"]) / r["actual"] * 100 for r in results if r["actual"] > 0]
        over = sum(1 for e in errors if e > 0)
        
        med_err = statistics.median(errors)
        avg_err = sum(errors) / len(errors)
        mape = statistics.median(pct_errors) if pct_errors else 0
        
        print(f"{name:<20} {med_err:>+10.2f} PLN {avg_err:>+10.2f} PLN {mape:>7.1f}%  {100*over/len(errors):>6.1f}%")
    
    # 1-parcel only (98% of cases)
    one_parcel = [r for r in results if r["parcels"] == 1]
    if one_parcel:
        print(f"\n--- Only 1-parcel orders ({len(one_parcel):,}) ---")
        print(f"{'Approach':<20} {'Median Error':<15} {'Avg Error':<15} {'MAPE':<10}")
        print("-" * 60)
        for name, key in [("MAX (current v2)", "max"), ("SUM", "sum"), ("AVG", "avg")]:
            errors = [r[key] - r["actual"] for r in one_parcel]
            pct_errors = [abs(r[key] - r["actual"]) / r["actual"] * 100 for r in one_parcel if r["actual"] > 0]
            med_err = statistics.median(errors)
            avg_err = sum(errors) / len(errors)
            mape = statistics.median(pct_errors) if pct_errors else 0
            print(f"{name:<20} {med_err:>+10.2f} PLN {avg_err:>+10.2f} PLN {mape:>7.1f}%")

    # By line count
    print(f"\n--- MAPE by line count ---")
    for lc in sorted(set(r["lines"] for r in results)):
        subset = [r for r in results if r["lines"] == lc]
        if len(subset) < 5:
            continue
        for name, key in [("MAX", "max"), ("SUM", "sum")]:
            pct_errors = [abs(r[key] - r["actual"]) / r["actual"] * 100 for r in subset if r["actual"] > 0]
            mape = statistics.median(pct_errors)
            avg_actual = sum(r["actual"] for r in subset) / len(subset)
            avg_est = sum(r[key] for r in subset) / len(subset)
            print(f"  {lc} linii ({len(subset):>4} orders) | {name}: MAPE={mape:.0f}% avg_act={avg_actual:.2f} avg_est={avg_est:.2f}")

# Sample orders: show the worst MAX errors
print(f"\n--- Top 10 worst MAX errors (1-parcel only) ---")
one_parcel_sorted = sorted([r for r in results if r["parcels"] == 1], key=lambda r: abs(r["max"] - r["actual"]), reverse=True)
print(f"{'Order ID':<25} {'Kraj':<5} {'Linii':<6} {'Actual':<10} {'MAX est':<10} {'SUM est':<10} {'Diff':<10}")
print("-" * 80)
for r in one_parcel_sorted[:10]:
    diff = r["max"] - r["actual"]
    print(f"{r['id']:<25} {r['country']:<5} {r['lines']:<6} {r['actual']:<10.2f} {r['max']:<10.2f} {r['sum']:<10.2f} {diff:>+8.2f}")

# Billing weight analysis for multi-line 1-parcel orders
print(f"\n" + "=" * 70)
print("BILLING WEIGHT: multi-line 1-parcel vs single-line")
print("=" * 70)

cur.execute("""
    SELECT 
        CASE WHEN sub.line_count = 1 THEN 'single' ELSE 'multi' END as type,
        AVG(gbl.billing_weight) as avg_bw,
        AVG(gbl.weight) as avg_weight,
        COUNT(*) as cnt
    FROM (
        SELECT o.amazon_order_id,
               COUNT(DISTINCT ol.id) as line_count
        FROM dbo.acc_order o WITH (NOLOCK)
        JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
        WHERE o.fulfillment_channel = 'MFN'
          AND o.purchase_date >= '2026-01-01'
        GROUP BY o.amazon_order_id
    ) sub
    JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = sub.amazon_order_id
    JOIN dbo.acc_shipment s WITH (NOLOCK) ON s.id = sol.shipment_id
    JOIN dbo.acc_gls_billing_line gbl WITH (NOLOCK) ON gbl.parcel_number = s.tracking_number
    WHERE gbl.billing_weight > 0
    GROUP BY CASE WHEN sub.line_count = 1 THEN 'single' ELSE 'multi' END
""")
for row in cur.fetchall():
    print(f"  {row[0]}: avg_billing_weight={row[1]:.2f}kg, avg_real_weight={row[2]:.2f}kg, N={row[3]:,}")

conn.close()
print("\nDone.")
