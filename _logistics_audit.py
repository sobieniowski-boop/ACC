"""
Audit: logistics data availability for Feb-Mar 2026.
- How many orders have logistics costs vs don't
- What data we have to estimate (country, weight, carrier)
- Historical cost patterns by country/carrier
"""
import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env")

from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=False, timeout=30)
cur = conn.cursor()

print("="*70)
print("1. ORDER LOGISTICS COVERAGE — Feb-Mar 2026")
print("="*70)
cur.execute("""
    SELECT
        CONVERT(VARCHAR(7), o.purchase_date, 120) AS m,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN f.total_logistics_pln IS NOT NULL AND f.total_logistics_pln > 0 THEN 1 ELSE 0 END) AS has_fact,
        SUM(CASE WHEN ISNULL(o.logistics_pln, 0) > 0 THEN 1 ELSE 0 END) AS has_legacy,
        SUM(CASE WHEN COALESCE(f.total_logistics_pln, o.logistics_pln, 0) > 0 THEN 1 ELSE 0 END) AS has_any_cost,
        SUM(CASE WHEN COALESCE(f.total_logistics_pln, o.logistics_pln, 0) = 0 THEN 1 ELSE 0 END) AS missing_cost
    FROM dbo.acc_order o WITH (NOLOCK)
    OUTER APPLY (
        SELECT TOP 1 olf.total_logistics_pln
        FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
        WHERE olf.amazon_order_id = o.amazon_order_id
          AND olf.total_logistics_pln > 0
        ORDER BY olf.calculated_at DESC
    ) f
    WHERE o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-04-01'
      AND o.status IN ('Shipped', 'Unshipped')
    GROUP BY CONVERT(VARCHAR(7), o.purchase_date, 120)
    ORDER BY m
""")
for r in cur.fetchall():
    m, total, fact, legacy, any_cost, missing = r[0], int(r[1]), int(r[2]), int(r[3]), int(r[4]), int(r[5])
    pct = any_cost / max(total, 1) * 100
    print(f"  {m}: total={total:,} | has_fact={fact:,} | has_legacy={legacy:,} | has_any={any_cost:,} ({pct:.1f}%) | MISSING={missing:,}")

print("\n" + "="*70)
print("2. SHIP COUNTRY DISTRIBUTION — orders missing logistics (Feb-Mar)")
print("="*70)
cur.execute("""
    SELECT TOP 20
        o.ship_country,
        COUNT(*) AS cnt,
        SUM(CAST(o.revenue_pln AS FLOAT)) AS rev
    FROM dbo.acc_order o WITH (NOLOCK)
    OUTER APPLY (
        SELECT TOP 1 olf.total_logistics_pln
        FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
        WHERE olf.amazon_order_id = o.amazon_order_id
          AND olf.total_logistics_pln > 0
        ORDER BY olf.calculated_at DESC
    ) f
    WHERE o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-04-01'
      AND o.status IN ('Shipped', 'Unshipped')
      AND COALESCE(f.total_logistics_pln, o.logistics_pln, 0) = 0
    GROUP BY o.ship_country
    ORDER BY cnt DESC
""")
print(f"  {'Country':<10} {'Orders':>8} {'Revenue PLN':>12}")
for r in cur.fetchall():
    print(f"  {str(r[0] or 'NULL'):<10} {int(r[1]):>8,} {float(r[2] or 0):>12,.0f}")

print("\n" + "="*70)
print("3. EXISTING LOGISTICS FACTS — calc_version distribution")
print("="*70)
cur.execute("""
    SELECT
        olf.calc_version,
        olf.source_system,
        COUNT(*) AS cnt,
        AVG(CAST(olf.total_logistics_pln AS FLOAT)) AS avg_cost,
        MIN(CAST(olf.total_logistics_pln AS FLOAT)) AS min_cost,
        MAX(CAST(olf.total_logistics_pln AS FLOAT)) AS max_cost
    FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = olf.amazon_order_id
    WHERE o.purchase_date >= '2026-01-01' AND o.purchase_date < '2026-04-01'
    GROUP BY olf.calc_version, olf.source_system
    ORDER BY cnt DESC
""")
for r in cur.fetchall():
    print(f"  {str(r[0]):<20} {str(r[1]):<30} cnt={int(r[2]):>6,} avg={float(r[3]):.2f} min={float(r[4]):.2f} max={float(r[5]):.2f}")

print("\n" + "="*70)
print("4. HISTORICAL COST BY COUNTRY — from matched Jan 2026 orders")
print("="*70)
cur.execute("""
    SELECT TOP 25
        o.ship_country,
        COUNT(*) AS cnt,
        AVG(CAST(f.total_logistics_pln AS FLOAT)) AS avg_cost,
        STDEV(CAST(f.total_logistics_pln AS FLOAT)) AS std_cost,
        MIN(CAST(f.total_logistics_pln AS FLOAT)) AS min_cost,
        MAX(CAST(f.total_logistics_pln AS FLOAT)) AS max_cost
    FROM dbo.acc_order o WITH (NOLOCK)
    CROSS APPLY (
        SELECT TOP 1 olf.total_logistics_pln
        FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
        WHERE olf.amazon_order_id = o.amazon_order_id
          AND olf.total_logistics_pln > 0
          AND olf.actual_shipments_count > 0
        ORDER BY olf.calculated_at DESC
    ) f
    WHERE o.purchase_date >= '2026-01-01' AND o.purchase_date < '2026-02-01'
      AND o.status = 'Shipped'
    GROUP BY o.ship_country
    ORDER BY cnt DESC
""")
print(f"  {'Country':<8} {'Orders':>7} {'AVG':>8} {'STD':>8} {'MIN':>8} {'MAX':>8}")
for r in cur.fetchall():
    std = float(r[3]) if r[3] is not None else 0
    print(f"  {str(r[0] or '?'):<8} {int(r[1]):>7,} {float(r[2]):>8.2f} {std:>8.2f} {float(r[4]):>8.2f} {float(r[5]):>8.2f}")

print("\n" + "="*70)
print("5. WEIGHT DATA AVAILABILITY — do we have item weights?")
print("="*70)
# Check if acc_order_line has any weight-related columns
cur.execute("""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS WITH (NOLOCK)
    WHERE TABLE_NAME = 'acc_order_line'
      AND COLUMN_NAME LIKE '%weight%'
    ORDER BY COLUMN_NAME
""")
weight_cols = [r[0] for r in cur.fetchall()]
print(f"  acc_order_line weight columns: {weight_cols or 'NONE'}")

# Check acc_catalog or similar
cur.execute("""
    SELECT TABLE_NAME, COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS WITH (NOLOCK)
    WHERE COLUMN_NAME LIKE '%weight%'
      AND TABLE_SCHEMA = 'dbo'
    ORDER BY TABLE_NAME, COLUMN_NAME
""")
all_weight = [(r[0], r[1]) for r in cur.fetchall()]
print(f"  All weight columns in DB:")
for t, c in all_weight:
    print(f"    {t}.{c}")

print("\n" + "="*70)
print("6. ORDER LINE COUNT DISTRIBUTION — orders missing logistics")
print("="*70)
cur.execute("""
    SELECT
        line_count_bucket,
        COUNT(*) AS orders
    FROM (
        SELECT
            CASE WHEN lc.lines <= 1 THEN '1'
                 WHEN lc.lines <= 2 THEN '2'
                 WHEN lc.lines <= 3 THEN '3'
                 WHEN lc.lines <= 5 THEN '4-5'
                 ELSE '6+'
            END AS line_count_bucket
        FROM dbo.acc_order o WITH (NOLOCK)
        OUTER APPLY (
            SELECT TOP 1 olf.total_logistics_pln
            FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
            WHERE olf.amazon_order_id = o.amazon_order_id
              AND olf.total_logistics_pln > 0
            ORDER BY olf.calculated_at DESC
        ) f
        CROSS APPLY (
            SELECT COUNT(*) AS lines
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            WHERE ol.order_id = o.id
        ) lc
        WHERE o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-04-01'
          AND o.status IN ('Shipped', 'Unshipped')
          AND COALESCE(f.total_logistics_pln, o.logistics_pln, 0) = 0
    ) sub
    GROUP BY line_count_bucket
    ORDER BY line_count_bucket
""")
for r in cur.fetchall():
    print(f"  {r[0]} lines: {int(r[1]):>8,} orders")

print("\n" + "="*70)
print("7. MARKETPLACE DISTRIBUTION — orders missing logistics")
print("="*70)
cur.execute("""
    SELECT
        o.marketplace_id,
        COUNT(*) AS cnt
    FROM dbo.acc_order o WITH (NOLOCK)
    OUTER APPLY (
        SELECT TOP 1 olf.total_logistics_pln
        FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
        WHERE olf.amazon_order_id = o.amazon_order_id
          AND olf.total_logistics_pln > 0
        ORDER BY olf.calculated_at DESC
    ) f
    WHERE o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-04-01'
      AND o.status IN ('Shipped', 'Unshipped')
      AND COALESCE(f.total_logistics_pln, o.logistics_pln, 0) = 0
    GROUP BY o.marketplace_id
    ORDER BY cnt DESC
""")
for r in cur.fetchall():
    print(f"  {r[0]}: {int(r[1]):>6,}")

print("\n" + "="*70)
print("8. EXISTING ESTIMATE calc_versions for Feb-Mar")
print("="*70)
cur.execute("""
    SELECT olf.calc_version, COUNT(*)
    FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = olf.amazon_order_id
    WHERE o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-04-01'
      AND olf.calc_version LIKE '%estimate%'
    GROUP BY olf.calc_version
""")
est_rows = cur.fetchall()
if est_rows:
    for r in est_rows:
        print(f"  {r[0]}: {int(r[1]):,}")
else:
    print("  No estimate rows found")

conn.close()
print("\nDONE")
