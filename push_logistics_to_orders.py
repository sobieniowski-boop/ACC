"""
Push acc_shipping_cost → acc_order.logistics_pln  AND  recalculate CM1.

Steps:
  1. UPDATE acc_order.logistics_pln = acc_shipping_cost.cost_total_pln
  2. Recalculate contribution_margin_pln and cm_percent for affected rows
"""
import pymssql

conn = pymssql.connect(
    server='acc-sql-kadax.database.windows.net',
    port=1433,
    user='accadmin',
    password='Mil53$SobAdhd',
    database='ACC'
)
cur = conn.cursor()

# ── Step 0: Current state ──
cur.execute("""
    SELECT 
        COUNT(*) as total_mfn,
        SUM(CASE WHEN logistics_pln IS NOT NULL THEN 1 ELSE 0 END) as has_logistics,
        SUM(CASE WHEN logistics_pln IS NULL THEN 1 ELSE 0 END) as missing_logistics,
        SUM(CASE WHEN contribution_margin_pln IS NOT NULL THEN 1 ELSE 0 END) as has_cm1
    FROM acc_order WITH (NOLOCK)
    WHERE fulfillment_channel = 'MFN'
      AND purchase_date >= '2025-03-01'
""")
row = cur.fetchone()
print(f"BEFORE: MFN orders={row[0]:,}  has_logistics={row[1]:,}  missing={row[2]:,}  has_cm1={row[3]:,}")

# ── Step 1: UPDATE logistics_pln from acc_shipping_cost ──
print("\nStep 1: Updating acc_order.logistics_pln from acc_shipping_cost...")
cur.execute("SET LOCK_TIMEOUT 30000")
cur.execute("""
    UPDATE o
    SET o.logistics_pln = s.cost_total_pln
    FROM acc_order o
    JOIN acc_shipping_cost s WITH (NOLOCK) ON s.amazon_order_id = o.amazon_order_id
    WHERE o.fulfillment_channel = 'MFN'
      AND o.purchase_date >= '2025-03-01'
""")
updated_logistics = cur.rowcount
conn.commit()
print(f"  Updated logistics_pln: {updated_logistics:,} rows")

# ── Step 2: Recalculate CM1 for all MFN orders with logistics_pln ──
print("\nStep 2: Recalculating CM1...")
cur.execute("SET LOCK_TIMEOUT 30000")
cur.execute("""
    UPDATE acc_order
    SET contribution_margin_pln = ROUND(
            ISNULL(revenue_pln, 0) 
            - ISNULL(cogs_pln, 0)
            - ISNULL(amazon_fees_pln, 0) 
            - ISNULL(ads_cost_pln, 0)
            - ISNULL(logistics_pln, 0), 2),
        cm_percent = CASE
            WHEN ISNULL(revenue_pln, 0) > 0 THEN
                ROUND(
                    (ISNULL(revenue_pln, 0) 
                     - ISNULL(cogs_pln, 0)
                     - ISNULL(amazon_fees_pln, 0) 
                     - ISNULL(ads_cost_pln, 0)
                     - ISNULL(logistics_pln, 0)
                    ) * 100.0 / revenue_pln, 2)
            ELSE NULL
        END
    WHERE fulfillment_channel = 'MFN'
      AND purchase_date >= '2025-03-01'
      AND logistics_pln IS NOT NULL
""")
updated_cm1 = cur.rowcount
conn.commit()
print(f"  Recalculated CM1: {updated_cm1:,} rows")

# ── Step 3: Verify ──
cur.execute("""
    SELECT 
        COUNT(*) as total_mfn,
        SUM(CASE WHEN logistics_pln IS NOT NULL THEN 1 ELSE 0 END) as has_logistics,
        SUM(CASE WHEN logistics_pln IS NULL THEN 1 ELSE 0 END) as missing_logistics,
        SUM(CASE WHEN contribution_margin_pln IS NOT NULL THEN 1 ELSE 0 END) as has_cm1
    FROM acc_order WITH (NOLOCK)
    WHERE fulfillment_channel = 'MFN'
      AND purchase_date >= '2025-03-01'
""")
row = cur.fetchone()
print(f"\nAFTER:  MFN orders={row[0]:,}  has_logistics={row[1]:,}  missing={row[2]:,}  has_cm1={row[3]:,}")

# ── Step 4: Sample CM1 values ──
cur.execute("""
    SELECT TOP 10
        amazon_order_id,
        CAST(revenue_pln AS FLOAT) as rev,
        CAST(cogs_pln AS FLOAT) as cogs,
        CAST(amazon_fees_pln AS FLOAT) as fees,
        CAST(ads_cost_pln AS FLOAT) as ads,
        CAST(logistics_pln AS FLOAT) as logistics,
        CAST(contribution_margin_pln AS FLOAT) as cm1,
        CAST(cm_percent AS FLOAT) as cm_pct
    FROM acc_order WITH (NOLOCK)
    WHERE fulfillment_channel = 'MFN'
      AND logistics_pln IS NOT NULL
      AND revenue_pln IS NOT NULL
      AND purchase_date >= '2025-08-01'
    ORDER BY purchase_date DESC
""")
print(f"\n{'Order ID':>22} {'Revenue':>8} {'COGS':>7} {'Fees':>7} {'Ads':>6} {'Ship':>7} {'CM1':>8} {'CM%':>6}")
print('-' * 80)
for r in cur.fetchall():
    oid = r[0]
    vals = [float(v) if v is not None else 0.0 for v in r[1:]]
    rev, cogs, fees, ads, ship, cm1, cm_pct = vals
    print(f'{oid:>22} {rev:>8.2f} {cogs:>7.2f} {fees:>7.2f} {ads:>6.2f} {ship:>7.2f} {cm1:>8.2f} {cm_pct:>5.1f}%')

# Monthly CM1 averages
cur.execute("""
    SELECT 
        FORMAT(purchase_date, 'yyyy-MM') as month,
        COUNT(*) as orders,
        AVG(CAST(contribution_margin_pln AS FLOAT)) as avg_cm1,
        AVG(CAST(cm_percent AS FLOAT)) as avg_cm_pct,
        AVG(CAST(logistics_pln AS FLOAT)) as avg_ship
    FROM acc_order WITH (NOLOCK)
    WHERE fulfillment_channel = 'MFN'
      AND logistics_pln IS NOT NULL
      AND purchase_date >= '2025-03-01'
    GROUP BY FORMAT(purchase_date, 'yyyy-MM')
    ORDER BY month
""")
print(f"\n{'Month':>10} {'Orders':>8} {'Avg CM1':>10} {'Avg CM%':>8} {'Avg Ship':>10}")
print('-' * 50)
for r in cur.fetchall():
    m = r[0]
    cnt = int(r[1])
    avg_cm = float(r[2]) if r[2] else 0
    avg_pct = float(r[3]) if r[3] else 0
    avg_ship = float(r[4]) if r[4] else 0
    print(f'{m:>10} {cnt:>8,} {avg_cm:>10.2f} {avg_pct:>7.1f}% {avg_ship:>10.2f}')

conn.close()
print("\nDone!")
