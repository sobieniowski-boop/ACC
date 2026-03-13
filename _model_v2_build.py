"""
Weight-Based Logistics Model v2

Strategy (3 tiers):
  Tier 0: Order already has ACTUAL billing cost (gls_v1 / dhl_v1) → skip
  Tier 1: SKU × country → median actual shipping cost (from billing history)
  Tier 2: Country × weight_bracket → median cost (fallback)
  Tier 3: Country → median cost (last resort fallback)

Data source: acc_shipment_cost (net_amount) joined through
  order → shipment_order_link → shipment → shipment_cost
  
This replaces hist_country_v1 estimates with more accurate per-SKU estimates.
"""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc
from collections import defaultdict
import statistics

print("=" * 70)
print("MODEL V2 — Building lookup tables from actual billing data")
print("=" * 70)

conn = connect_acc(timeout=120)
cur = conn.cursor()

# ── STEP 1: Build SKU × country → actual cost lookup ──
# From single-line orders that have actual shipment costs
print("\n[1] Building SKU × country → cost lookup (from actual billing)...")
t0 = time.time()

cur.execute("""
    SELECT ol.sku, o.ship_country, sc.net_amount + ISNULL(sc.fuel_amount, 0) + ISNULL(sc.toll_amount, 0) AS total_cost
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = o.amazon_order_id
    JOIN dbo.acc_shipment_cost sc WITH (NOLOCK) ON sc.shipment_id = sol.shipment_id
    WHERE o.fulfillment_channel = 'MFN'
      AND sc.net_amount > 0
      AND sc.is_estimated = 0
    -- Only single-line orders for clean SKU attribution
    AND NOT EXISTS (
        SELECT 1 FROM dbo.acc_order_line ol2 WITH (NOLOCK) 
        WHERE ol2.order_id = o.id AND ol2.id != ol.id
    )
""")

sku_country_costs = defaultdict(list)
rows_read = 0
for sku, country, cost in cur.fetchall():
    if sku and country and cost:
        sku_country_costs[(sku, country)].append(float(cost))
        rows_read += 1

# Compute medians
sku_country_median = {}
for key, costs in sku_country_costs.items():
    sku_country_median[key] = statistics.median(costs)

print(f"  Read {rows_read:,} single-line billing records")
print(f"  Unique (SKU, country) pairs: {len(sku_country_median):,}")
print(f"  Took {time.time()-t0:.1f}s")

# Show top 10 by data points
top10 = sorted(sku_country_costs.items(), key=lambda x: -len(x[1]))[:10]
print(f"\n  Top 10 (SKU × country) by data count:")
for (sku, country), costs in top10:
    print(f"    {sku:<30} {country:<4} N={len(costs):>5} median={statistics.median(costs):>8.2f} PLN")

# ── STEP 2: Build country → cost lookup (fallback) ──
print("\n[2] Building country → cost lookup (fallback)...")
t1 = time.time()

country_costs = defaultdict(list)
for (sku, country), costs in sku_country_costs.items():
    country_costs[country].extend(costs)

country_median = {}
for country, costs in country_costs.items():
    country_median[country] = statistics.median(costs)

print(f"  Countries with data: {len(country_median)}")
for country in sorted(country_median.keys(), key=lambda c: -len(country_costs[c]))[:15]:
    costs = country_costs[country]
    print(f"    {country:<4} N={len(costs):>6} median={country_median[country]:>8.2f} PLN")

# ── STEP 3: Identify orders to fill ──
print("\n[3] Identifying MFN orders needing v2 estimates (Feb-Mar 2026)...")

# Get MFN orders that currently have hist_country_v1 OR no cost at all
cur.execute("""
    SELECT o.amazon_order_id, o.ship_country
    FROM dbo.acc_order o WITH (NOLOCK)
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-12'
      AND NOT EXISTS (
        SELECT 1 FROM dbo.acc_order_logistics_fact f WITH (NOLOCK)
        WHERE f.amazon_order_id = o.amazon_order_id
          AND f.calc_version IN ('gls_v1', 'dhl_v1')
      )
""")
orders_to_fill = [(r[0], r[1]) for r in cur.fetchall()]
print(f"  Orders without actual billing: {len(orders_to_fill):,}")

# Get order lines for these orders
order_ids = [o[0] for o in orders_to_fill]
order_country = {o[0]: o[1] for o in orders_to_fill}

# Build order → SKUs lookup
print("\n[4] Loading order lines for these orders...")
cur.execute("""
    SELECT o.amazon_order_id, ol.sku, ol.quantity_ordered
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-12'
      AND NOT EXISTS (
        SELECT 1 FROM dbo.acc_order_logistics_fact f WITH (NOLOCK)
        WHERE f.amazon_order_id = o.amazon_order_id
          AND f.calc_version IN ('gls_v1', 'dhl_v1')
      )
""")
order_lines = defaultdict(list)
for amazon_id, sku, qty in cur.fetchall():
    order_lines[amazon_id].append((sku, int(qty) if qty else 1))

print(f"  Orders with lines: {len(order_lines):,}")

# ── STEP 4: Estimate costs ──
print("\n[5] Estimating costs with v2 model...")

results = []
tier_counts = {1: 0, 2: 0, 3: 0, 'no_data': 0}
global_median = statistics.median([c for costs in country_costs.values() for c in costs])

for amazon_id, country in orders_to_fill:
    lines = order_lines.get(amazon_id, [])
    if not lines:
        # No lines → use country median
        cost = country_median.get(country, global_median)
        tier = 3
    else:
        # For single-line orders: straightforward lookup
        # For multi-line: THIS is one shipment, so take max(SKU costs) or sum-based approach
        # Actually: shipping cost = cost of the ONE parcel, not per-item
        # For multi-line, look up the "heaviest" item's cost as base
        sku_costs = []
        tier = None
        for sku, qty in lines:
            key = (sku, country)
            if key in sku_country_median:
                sku_costs.append(sku_country_median[key])
                if tier is None or tier > 1:
                    tier = 1
            elif country in country_median:
                sku_costs.append(country_median[country])
                if tier is None or tier > 2:
                    tier = 2
            else:
                sku_costs.append(global_median)
                if tier is None or tier > 3:
                    tier = 3
        
        if len(lines) == 1:
            cost = sku_costs[0]
        else:
            # Multi-line: one shipment, cost ≈ max(individual SKU costs) 
            # (heaviest item drives the parcel cost)
            cost = max(sku_costs)
        
        if tier is None:
            tier = 3

    results.append((amazon_id, cost, tier))
    tier_counts[tier] = tier_counts.get(tier, 0) + 1

print(f"  Tier 1 (SKU×country): {tier_counts[1]:,}")
print(f"  Tier 2 (country only): {tier_counts[2]:,}") 
print(f"  Tier 3 (global median): {tier_counts[3]:,}")
print(f"  Total to write: {len(results):,}")
print(f"  Total estimated cost: {sum(r[1] for r in results):,.2f} PLN")

# Compare with current hist_country_v1
cur.execute("""
    SELECT COUNT(*), SUM(total_logistics_pln)
    FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
    WHERE calc_version = 'hist_country_v1'
      AND amazon_order_id IN (
        SELECT amazon_order_id FROM dbo.acc_order WITH (NOLOCK)
        WHERE purchase_date >= '2026-02-01' AND purchase_date < '2026-03-12'
          AND fulfillment_channel = 'MFN'
      )
""")
old_count, old_total = cur.fetchone()
print(f"\n  Current hist_country_v1: {old_count:,} orders, {float(old_total or 0):,.2f} PLN")
print(f"  New v2 estimate:         {len(results):,} orders, {sum(r[1] for r in results):,.2f} PLN")
diff = sum(r[1] for r in results) - float(old_total or 0)
print(f"  Difference: {diff:+,.2f} PLN ({100*diff/float(old_total or 1):+.1f}%)")

# ── STEP 5: Show sample comparisons ──
print("\n[6] Sample comparisons (10 random orders)...")
import random
sample = random.sample(results, min(10, len(results)))
for amazon_id, new_cost, tier in sample:
    cur.execute("""
        SELECT total_logistics_pln FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
        WHERE amazon_order_id = ? AND calc_version = 'hist_country_v1'
    """, (amazon_id,))
    row = cur.fetchone()
    old_cost = float(row[0]) if row else 0
    country = order_country.get(amazon_id, '?')
    lines = order_lines.get(amazon_id, [])
    skus = [l[0] for l in lines]
    print(f"  {amazon_id[:20]}.. {country} sku={skus[0] if skus else '?'} old={old_cost:.2f} new={new_cost:.2f} tier={tier}")

conn.close()
print(f"\nAnalysis done! Run with --apply to write to DB.")
print(f"Total time: {time.time()-t0:.0f}s")
