# Model V3: Weight-based logistics estimation
# Step 1: Check SKU mapping coverage
# Step 2: Build product→weight lookup from TKL
# Step 3: Calculate per-order logistics using carrier price lists
# Step 4: Clean up old v1/v2, insert v3
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc
from collections import defaultdict
import statistics

conn = connect_acc(timeout=120)
cur = conn.cursor()

# ── STEP 1: Check registry mapping coverage ──
print("=" * 80)
print("STEP 1: Mapping coverage — acc_amazon_listing_registry")
print("=" * 80)

# How many distinct merchant_sku in orders?
cur.execute("""
    SELECT COUNT(DISTINCT ol.sku) 
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-12'
""")
total_skus = cur.fetchone()[0]
print(f"Unique Amazon SKUs in Feb-Mar MFN orders: {total_skus}")

# How many have internal_sku mapping?
cur.execute("""
    SELECT COUNT(DISTINCT ol.sku)
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    JOIN dbo.acc_amazon_listing_registry r WITH (NOLOCK) 
        ON r.merchant_sku = ol.sku OR r.merchant_sku_alt = ol.sku
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-12'
      AND r.internal_sku IS NOT NULL AND r.internal_sku != ''
""")
mapped_skus = cur.fetchone()[0]
print(f"Mapped to internal_sku: {mapped_skus} ({100*mapped_skus/total_skus:.0f}%)")

# What does internal_sku look like?
cur.execute("""
    SELECT TOP 20 r.merchant_sku, r.internal_sku, r.product_name
    FROM dbo.acc_amazon_listing_registry r WITH (NOLOCK)
    WHERE r.internal_sku IS NOT NULL AND r.internal_sku != ''
    ORDER BY r.synced_at DESC
""")
print(f"\nSample mappings:")
for row in cur.fetchall():
    print(f"  merchant_sku={row[0]} → internal_sku={row[1]} | {str(row[2])[:50]}")

# Check: does internal_sku match the Nr artykulu (numeric) from TKL?
cur.execute("""
    SELECT TOP 10 r.internal_sku, 
           CASE WHEN TRY_CAST(r.internal_sku AS INT) IS NOT NULL THEN 'numeric' ELSE 'text' END as type
    FROM dbo.acc_amazon_listing_registry r WITH (NOLOCK)
    WHERE r.internal_sku IS NOT NULL AND r.internal_sku != ''
    GROUP BY r.internal_sku, CASE WHEN TRY_CAST(r.internal_sku AS INT) IS NOT NULL THEN 'numeric' ELSE 'text' END
""")
print(f"\nInternal SKU format:")
for row in cur.fetchall():
    print(f"  {row[0]} ({row[1]})")

# Also check: order_line.sku format (MAG_xxx vs numeric)
cur.execute("""
    SELECT TOP 10 ol.sku
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01'
""")
print(f"\nOrder line SKU format:")
for row in cur.fetchall():
    print(f"  {row[0]}")

# Check merchant_sku format in registry
cur.execute("""
    SELECT TOP 10 r.merchant_sku
    FROM dbo.acc_amazon_listing_registry r WITH (NOLOCK)
    WHERE r.merchant_sku IS NOT NULL
""")
print(f"\nRegistry merchant_sku format:")
for row in cur.fetchall():
    print(f"  {row[0]}")

# How many orders need estimation (no gls_v1/dhl_v1)?
cur.execute("""
    SELECT COUNT(DISTINCT o.amazon_order_id)
    FROM dbo.acc_order o WITH (NOLOCK)
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-12'
      AND NOT EXISTS (
          SELECT 1 FROM dbo.acc_order_logistics_fact f WITH (NOLOCK)
          WHERE f.amazon_order_id = o.amazon_order_id
            AND f.calc_version IN ('gls_v1', 'dhl_v1')
      )
""")
orders_needing_est = cur.fetchone()[0]
print(f"\nOrders needing estimation (no actual billing): {orders_needing_est}")

# Current estimates breakdown
cur.execute("""
    SELECT f.calc_version, COUNT(*) as cnt, SUM(f.total_logistics_pln) as total
    FROM dbo.acc_order_logistics_fact f WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = f.amazon_order_id
    WHERE o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-03-12'
    GROUP BY f.calc_version
    ORDER BY cnt DESC
""")
print(f"\nCurrent logistics_fact breakdown (Feb-Mar):")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]:,} rows = {float(row[2]):,.2f} PLN")

conn.close()
print("\nDone.")
