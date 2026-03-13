"""
Apply Model V2: Replace hist_country_v1 estimates with SKU×country-based estimates.

Strategy:
  1. Build SKU × country → median actual cost from billing history
  2. For orders without actual billing (no gls_v1/dhl_v1):
     - Tier 1: SKU×country median from billing history
     - Tier 2: Country median (fallback)
     - Tier 3: Global median (last resort)
  3. DELETE old hist_country_v1 rows for Feb-Mar
  4. INSERT new sku_country_v2 rows
"""
import sys, os, time, uuid
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc
from collections import defaultdict
import statistics

APPLY = True  # Set to False for dry-run
BATCH_SIZE = 500

print("=" * 70)
print("MODEL V2 — APPLY" if APPLY else "MODEL V2 — DRY RUN")
print("=" * 70)

conn = connect_acc(timeout=120)
cur = conn.cursor()

# ── STEP 1: Build SKU × country → actual cost lookup ──
print("\n[1] Building SKU × country → cost lookup...")
t0 = time.time()

cur.execute("""
    SELECT ol.sku, o.ship_country, 
           sc.net_amount + ISNULL(sc.fuel_amount, 0) + ISNULL(sc.toll_amount, 0) AS total_cost
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = o.amazon_order_id
    JOIN dbo.acc_shipment_cost sc WITH (NOLOCK) ON sc.shipment_id = sol.shipment_id
    WHERE o.fulfillment_channel = 'MFN'
      AND sc.net_amount > 0
      AND sc.is_estimated = 0
      AND NOT EXISTS (
          SELECT 1 FROM dbo.acc_order_line ol2 WITH (NOLOCK) 
          WHERE ol2.order_id = o.id AND ol2.id != ol.id
      )
""")

sku_country_costs = defaultdict(list)
for sku, country, cost in cur.fetchall():
    if sku and country and cost:
        sku_country_costs[(sku, country)].append(float(cost))

sku_country_median = {k: statistics.median(v) for k, v in sku_country_costs.items()}
print(f"  {len(sku_country_median):,} (SKU, country) pairs from {sum(len(v) for v in sku_country_costs.values()):,} records")

# Country fallback
country_costs = defaultdict(list)
for (sku, country), costs in sku_country_costs.items():
    country_costs[country].extend(costs)
country_median = {c: statistics.median(v) for c, v in country_costs.items()}
global_median = statistics.median([c for costs in country_costs.values() for c in costs])
print(f"  {len(country_median)} countries, global median={global_median:.2f} PLN")

# ── STEP 2: Get orders to fill ──
print("\n[2] Loading orders needing estimates...")
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
order_country = {o[0]: o[1] for o in orders_to_fill}
print(f"  {len(orders_to_fill):,} orders without actual billing")

# Get order lines
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

# ── STEP 3: Calculate estimates ──
print("\n[3] Calculating v2 estimates...")
results = []
tier_counts = {1: 0, 2: 0, 3: 0}

for amazon_id, country in orders_to_fill:
    lines = order_lines.get(amazon_id, [])
    if not lines:
        cost = country_median.get(country, global_median)
        tier = 3 if country not in country_median else 2
    else:
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
        
        # One shipment per order: cost = max(individual SKU costs)
        cost = max(sku_costs) if len(lines) > 1 else sku_costs[0]
        if tier is None:
            tier = 3

    results.append((amazon_id, round(cost, 2), f"tier{tier}"))
    tier_counts[tier] = tier_counts.get(tier, 0) + 1

total_cost = sum(r[1] for r in results)
print(f"  Tier 1 (SKU×country): {tier_counts[1]:,}")
print(f"  Tier 2 (country):     {tier_counts[2]:,}")
print(f"  Tier 3 (global):      {tier_counts[3]:,}")
print(f"  Total: {len(results):,} orders = {total_cost:,.2f} PLN")

if not APPLY:
    print("\nDRY RUN — no changes applied.")
    conn.close()
    sys.exit(0)

# ── STEP 4: Delete old hist_country_v1 for Feb-Mar ──
conn.close()

# ── STEP 4: Delete old hist_country_v1 for Feb-Mar ──
print("\n[4] Deleting old hist_country_v1 estimates for Feb-Mar...")
conn = connect_acc(autocommit=True, timeout=120)
cur = conn.cursor()
cur.execute("SET LOCK_TIMEOUT 30000")
cur.execute("""
    DELETE f FROM dbo.acc_order_logistics_fact f
    WHERE f.calc_version = 'hist_country_v1'
      AND f.amazon_order_id IN (
          SELECT amazon_order_id FROM dbo.acc_order WITH (NOLOCK)
          WHERE purchase_date >= '2026-02-01' AND purchase_date < '2026-03-12'
            AND fulfillment_channel = 'MFN'
      )
""")
deleted = cur.rowcount
print(f"  Deleted: {deleted:,} hist_country_v1 rows")

# ── STEP 5: Insert sku_country_v2 rows ──
print(f"\n[5] Inserting {len(results):,} sku_country_v2 rows ({BATCH_SIZE}/batch)...")
now = datetime.utcnow()
inserted = 0
errors = 0

for i in range(0, len(results), BATCH_SIZE):
    batch = results[i:i+BATCH_SIZE]
    values_parts = []
    params = []
    for amazon_id, cost, tier_label in batch:
        values_parts.append("(?, ?, ?, ?, ?)")
        params.extend([amazon_id, 'sku_country_v2', cost, now, tier_label])
    
    sql = f"""
        INSERT INTO dbo.acc_order_logistics_fact 
        (amazon_order_id, calc_version, total_logistics_pln, calculated_at, source_system)
        VALUES {', '.join(values_parts)}
    """
    try:
        cur.execute(sql, tuple(params))
        inserted += len(batch)
    except Exception as e:
        print(f"  ERROR batch {i}: {e}")
        errors += len(batch)
    
    if (i // BATCH_SIZE) % 10 == 0:
        print(f"  ... {inserted:,} inserted", flush=True)

print(f"  Inserted: {inserted:,}, Errors: {errors}")

# ── Verify ──
print("\n[6] Verification...")
cur.execute("""
    SELECT calc_version, COUNT(*) cnt, SUM(total_logistics_pln) total_pln
    FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
    GROUP BY calc_version
    ORDER BY cnt DESC
""")
for r in cur.fetchall():
    print(f"  {str(r[0]):<30} {r[1]:>8,} rows  {float(r[2]):>15,.2f} PLN")

conn.close()
print(f"\nDone in {time.time()-t0:.0f}s")
