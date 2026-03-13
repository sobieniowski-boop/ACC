"""
Model v2 Analysis: Map actual billing costs (GLS/DHL) to Amazon orders.
Chain: acc_order → acc_shipment_order_link → acc_shipment → acc_gls_billing_line / acc_dhl_billing_line
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc

conn = connect_acc(timeout=60)
cur = conn.cursor()

# ── 1. Check full chain: order → shipment_link → shipment → GLS billing ──
print("=" * 70)
print("1. FULL CHAIN: Order → Shipment → GLS Billing (MFN orders, Feb-Mar 2026)")
print("=" * 70)

cur.execute("""
    SELECT COUNT(DISTINCT o.amazon_order_id) 
    FROM dbo.acc_order o WITH (NOLOCK)
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-12'
""")
total_mfn = cur.fetchone()[0]
print(f"  Total MFN orders Feb-Mar: {total_mfn:,}")

cur.execute("""
    SELECT COUNT(DISTINCT o.amazon_order_id)
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = o.amazon_order_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-12'
""")
with_link = cur.fetchone()[0]
print(f"  MFN orders with shipment link: {with_link:,} ({100*with_link/max(total_mfn,1):.1f}%)")

cur.execute("""
    SELECT COUNT(DISTINCT o.amazon_order_id)
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = o.amazon_order_id
    JOIN dbo.acc_shipment s WITH (NOLOCK) ON s.id = sol.shipment_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-12'
""")
with_shipment = cur.fetchone()[0]
print(f"  MFN orders with shipment: {with_shipment:,} ({100*with_shipment/max(total_mfn,1):.1f}%)")

cur.execute("""
    SELECT COUNT(DISTINCT o.amazon_order_id)
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = o.amazon_order_id
    JOIN dbo.acc_shipment s WITH (NOLOCK) ON s.id = sol.shipment_id
    JOIN dbo.acc_gls_billing_line g WITH (NOLOCK) ON g.parcel_number = s.tracking_number
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-12'
""")
with_gls = cur.fetchone()[0]
print(f"  MFN orders → GLS billing: {with_gls:,} ({100*with_gls/max(total_mfn,1):.1f}%)")

# DHL link
cur.execute("""
    SELECT COUNT(DISTINCT o.amazon_order_id)
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = o.amazon_order_id
    JOIN dbo.acc_shipment s WITH (NOLOCK) ON s.id = sol.shipment_id
    JOIN dbo.acc_dhl_billing_line d WITH (NOLOCK) ON d.parcel_number = s.tracking_number
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-12'
""")
with_dhl = cur.fetchone()[0]
print(f"  MFN orders → DHL billing: {with_dhl:,} ({100*with_dhl/max(total_mfn,1):.1f}%)")

print(f"\n  TOTAL with actual billing cost: {with_gls + with_dhl:,} / {total_mfn:,} = {100*(with_gls+with_dhl)/max(total_mfn,1):.1f}%")

# ── 2. What about acc_shipment_cost? ──
print("\n" + "=" * 70)
print("2. acc_shipment_cost coverage")
print("=" * 70)

cur.execute("""
    SELECT COLUMN_NAME, DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME='acc_shipment_cost' 
    ORDER BY ORDINAL_POSITION
""")
for r in cur.fetchall():
    print(f"  {r[0]} ({r[1]})")

cur.execute("SELECT COUNT(*) FROM dbo.acc_shipment_cost WITH (NOLOCK)")
print(f"\n  Total rows: {cur.fetchone()[0]:,}")

cur.execute("SELECT TOP 10 * FROM dbo.acc_shipment_cost WITH (NOLOCK) ORDER BY NEWID()")
cols = [d[0] for d in cur.description]
print(f"  Sample: {cols}")
for r in cur.fetchall():
    print(f"  {[str(x)[:40] for x in r]}")

# ── 3. GLS billing: billing_weight stats per country ──
print("\n" + "=" * 70)
print("3. GLS billing_weight stats per country (top 10 countries)")
print("=" * 70)

cur.execute("""
    SELECT recipient_country, 
           COUNT(*) cnt,
           AVG(billing_weight) avg_wt,
           MIN(billing_weight) min_wt,
           MAX(billing_weight) max_wt,
           AVG(net_amount) avg_cost
    FROM dbo.acc_gls_billing_line WITH (NOLOCK)
    WHERE billing_weight > 0 AND net_amount > 0
    GROUP BY recipient_country
    ORDER BY cnt DESC
""")
print(f"  {'Country':<8} {'Count':>8} {'Avg Wt':>8} {'Min Wt':>8} {'Max Wt':>8} {'Avg Cost':>10}")
for r in cur.fetchall():
    print(f"  {str(r[0]):<8} {r[1]:>8,} {float(r[2]):>8.2f} {float(r[3]):>8.2f} {float(r[4]):>8.2f} {float(r[5]):>10.2f}")

# ── 4. Carrier distribution in shipments for Feb-Mar 2026 ──
print("\n" + "=" * 70)
print("4. Carrier distribution for MFN shipments (Feb-Mar 2026)")
print("=" * 70)

cur.execute("""
    SELECT s.carrier, COUNT(DISTINCT o.amazon_order_id) cnt
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = o.amazon_order_id
    JOIN dbo.acc_shipment s WITH (NOLOCK) ON s.id = sol.shipment_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-12'
    GROUP BY s.carrier
    ORDER BY cnt DESC
""")
for r in cur.fetchall():
    print(f"  {str(r[0]):<20} {r[1]:>8,} orders")

# ── 5. Sample: order → shipment → GLS billing cost ──
print("\n" + "=" * 70)
print("5. Sample: MFN order → shipment → GLS actual cost (10 random)")
print("=" * 70)

cur.execute("""
    SELECT TOP 10
        o.amazon_order_id,
        o.ship_country,
        s.tracking_number,
        g.billing_weight,
        g.net_amount AS gls_net_pln,
        g.recipient_country
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = o.amazon_order_id
    JOIN dbo.acc_shipment s WITH (NOLOCK) ON s.id = sol.shipment_id
    JOIN dbo.acc_gls_billing_line g WITH (NOLOCK) ON g.parcel_number = s.tracking_number
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-12'
      AND g.net_amount > 0
    ORDER BY NEWID()
""")
for r in cur.fetchall():
    print(f"  Order={r[0][:20]}.. country={r[1]} track={r[2]} wt={r[3]:.2f}kg cost={float(r[4]):.2f}PLN gls_country={r[5]}")

# ── 6. Current hist_country_v1 stats ──
print("\n" + "=" * 70)
print("6. Current logistics_fact: hist_country_v1 MFN stats")
print("=" * 70)

cur.execute("""
    SELECT o.ship_country, COUNT(*) cnt,
           AVG(f.total_logistics_pln) avg_cost, MIN(f.total_logistics_pln) min_cost, MAX(f.total_logistics_pln) max_cost
    FROM dbo.acc_order_logistics_fact f WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = f.amazon_order_id
    WHERE f.calc_version = 'hist_country_v1'
      AND o.fulfillment_channel = 'MFN'
    GROUP BY o.ship_country
    ORDER BY cnt DESC
""")
print(f"  {'Country':<8} {'Count':>8} {'Avg Cost':>10} {'Min':>8} {'Max':>8}")
for r in cur.fetchall():
    print(f"  {str(r[0]):<8} {r[1]:>8,} {float(r[2]):>10.2f} {float(r[3]):>8.2f} {float(r[4]):>8.2f}")

# ── 7. Check how many orders already have actual billing costs (not estimated) ──
print("\n" + "=" * 70)
print("7. All calc_versions in logistics_fact")
print("=" * 70)
cur.execute("""
    SELECT calc_version, COUNT(*) cnt, SUM(total_logistics_pln) total_pln
    FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
    GROUP BY calc_version
    ORDER BY cnt DESC
""")
for r in cur.fetchall():
    print(f"  {str(r[0]):<30} {r[1]:>8,} rows  {float(r[2]):>15,.2f} PLN")

# ── 8. SKU to weight mapping from order_line → shipment → GLS ──
print("\n" + "=" * 70)
print("8. SKU → median billing_weight (from GLS, single-line orders only)")
print("=" * 70)

cur.execute("""
    ;WITH single_line AS (
        SELECT ol.order_id, ol.seller_sku
        FROM dbo.acc_order_line ol WITH (NOLOCK)
        GROUP BY ol.order_id, ol.seller_sku
        HAVING COUNT(*) = 1
    ),
    sku_weights AS (
        SELECT sl.seller_sku,
               g.billing_weight
        FROM single_line sl
        JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = sl.order_id
        JOIN dbo.acc_shipment_order_link sol WITH (NOLOCK) ON sol.amazon_order_id = o.amazon_order_id
        JOIN dbo.acc_shipment s WITH (NOLOCK) ON s.id = sol.shipment_id
        JOIN dbo.acc_gls_billing_line g WITH (NOLOCK) ON g.parcel_number = s.tracking_number
        WHERE g.billing_weight > 0 AND o.fulfillment_channel = 'MFN'
    )
    SELECT TOP 30 seller_sku, 
           COUNT(*) cnt,
           AVG(billing_weight) avg_wt,
           -- percentile_cont not available, use min/max spread
           MIN(billing_weight) min_wt,
           MAX(billing_weight) max_wt
    FROM sku_weights
    GROUP BY seller_sku
    HAVING COUNT(*) >= 5
    ORDER BY cnt DESC
""")
print(f"  {'SKU':<30} {'N':>5} {'Avg Wt':>8} {'Min':>8} {'Max':>8}")
for r in cur.fetchall():
    print(f"  {str(r[0]):<30} {r[1]:>5} {float(r[2]):>8.2f} {float(r[3]):>8.2f} {float(r[4]):>8.2f}")

conn.close()
print("\nDone!")
