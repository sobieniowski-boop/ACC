"""
Extract historical logistics cost statistics per (ship_country, line_count_bucket, marketplace_id).
Uses Jan 2026 matched data (69% coverage) as training set.
Also extracts per-SKU cost data where available.
"""
import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env")

from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=False, timeout=60)
cur = conn.cursor()

# ── 1. Country + lines bucket stats (primary model) ──
print("="*70)
print("COST MODEL: country × line_count_bucket (Jan 2026 actuals)")
print("="*70)
cur.execute("""
    ;WITH matched AS (
        SELECT
            o.ship_country,
            o.marketplace_id,
            lc.lines,
            CASE WHEN lc.lines = 1 THEN 1
                 WHEN lc.lines = 2 THEN 2
                 WHEN lc.lines <= 3 THEN 3
                 ELSE 4
            END AS line_bucket,
            CAST(f.total_logistics_pln AS FLOAT) AS cost
        FROM dbo.acc_order o WITH (NOLOCK)
        CROSS APPLY (
            SELECT TOP 1 olf.total_logistics_pln
            FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
            WHERE olf.amazon_order_id = o.amazon_order_id
              AND olf.total_logistics_pln > 0
              AND olf.actual_shipments_count > 0
            ORDER BY olf.calculated_at DESC
        ) f
        CROSS APPLY (
            SELECT COUNT(*) AS lines
            FROM dbo.acc_order_line ol WITH (NOLOCK)
            WHERE ol.order_id = o.id
        ) lc
        WHERE o.purchase_date >= '2025-12-01' AND o.purchase_date < '2026-03-01'
          AND o.status = 'Shipped'
          AND o.ship_country IS NOT NULL
    )
    SELECT
        ship_country,
        line_bucket,
        COUNT(*) AS samples,
        CAST(AVG(cost) AS DECIMAL(10,2)) AS avg_cost,
        CAST(STDEV(cost) AS DECIMAL(10,2)) AS std_cost,
        (SELECT DISTINCT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY m2.cost)
             OVER () FROM matched m2
         WHERE m2.ship_country = m.ship_country
           AND m2.line_bucket = m.line_bucket) AS median_cost,
        CAST(MIN(cost) AS DECIMAL(10,2)) AS min_cost,
        CAST(MAX(cost) AS DECIMAL(10,2)) AS max_cost
    FROM matched m
    GROUP BY ship_country, line_bucket
    HAVING COUNT(*) >= 3
    ORDER BY ship_country, line_bucket
""")
print(f"  {'Country':<6} {'Bucket':>6} {'N':>7} {'AVG':>8} {'STD':>8} {'MED':>8} {'MIN':>8} {'MAX':>8}")
country_stats = {}
for r in cur.fetchall():
    country = str(r[0])
    bucket = int(r[1])
    n = int(r[2])
    avg = float(r[3])
    std = float(r[4]) if r[4] else 0
    med = float(r[5]) if r[5] else avg
    mn = float(r[6])
    mx = float(r[7])
    print(f"  {country:<6} {bucket:>6} {n:>7,} {avg:>8.2f} {std:>8.2f} {med:>8.2f} {mn:>8.2f} {mx:>8.2f}")
    country_stats[(country, bucket)] = {"avg": avg, "median": med, "samples": n}

# ── 2. Country-only fallback (ignoring line bucket) ──
print("\n" + "="*70)
print("FALLBACK: country-only averages")
print("="*70)
cur.execute("""
    ;WITH matched AS (
        SELECT
            o.ship_country,
            CAST(f.total_logistics_pln AS FLOAT) AS cost
        FROM dbo.acc_order o WITH (NOLOCK)
        CROSS APPLY (
            SELECT TOP 1 olf.total_logistics_pln
            FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
            WHERE olf.amazon_order_id = o.amazon_order_id
              AND olf.total_logistics_pln > 0
              AND olf.actual_shipments_count > 0
            ORDER BY olf.calculated_at DESC
        ) f
        WHERE o.purchase_date >= '2025-12-01' AND o.purchase_date < '2026-03-01'
          AND o.status = 'Shipped'
          AND o.ship_country IS NOT NULL
    )
    SELECT
        ship_country,
        COUNT(*) AS samples,
        CAST(AVG(cost) AS DECIMAL(10,2)) AS avg_cost,
        (SELECT DISTINCT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY m2.cost)
             OVER () FROM matched m2
         WHERE m2.ship_country = m.ship_country) AS median_cost
    FROM matched m
    GROUP BY ship_country
    ORDER BY samples DESC
""")
print(f"  {'Country':<8} {'N':>7} {'AVG':>8} {'MED':>8}")
for r in cur.fetchall():
    print(f"  {str(r[0]):<8} {int(r[1]):>7,} {float(r[2]):>8.2f} {float(r[3]):>8.2f}")

# ── 3. Per-SKU cost data (for recurring SKUs) ──
print("\n" + "="*70)
print("TOP SKUs: per-SKU median cost (n>=10 shipments)")
print("="*70)
cur.execute("""
    SELECT TOP 30
        ol.sku,
        o.ship_country,
        COUNT(*) AS samples,
        CAST(AVG(CAST(f.total_logistics_pln AS FLOAT)) AS DECIMAL(10,2)) AS avg_cost,
        CAST(MIN(CAST(f.total_logistics_pln AS FLOAT)) AS DECIMAL(10,2)) AS min_cost,
        CAST(MAX(CAST(f.total_logistics_pln AS FLOAT)) AS DECIMAL(10,2)) AS max_cost
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    CROSS APPLY (
        SELECT TOP 1 olf.total_logistics_pln
        FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
        WHERE olf.amazon_order_id = o.amazon_order_id
          AND olf.total_logistics_pln > 0
          AND olf.actual_shipments_count > 0
        ORDER BY olf.calculated_at DESC
    ) f
    CROSS APPLY (
        SELECT COUNT(*) AS lines
        FROM dbo.acc_order_line ol2 WITH (NOLOCK)
        WHERE ol2.order_id = o.id
    ) lc
    WHERE o.purchase_date >= '2025-12-01' AND o.purchase_date < '2026-03-01'
      AND o.status = 'Shipped'
      AND lc.lines = 1
    GROUP BY ol.sku, o.ship_country
    HAVING COUNT(*) >= 10
    ORDER BY COUNT(*) DESC
""")
print(f"  {'SKU':<25} {'Country':<6} {'N':>5} {'AVG':>8} {'MIN':>8} {'MAX':>8}")
for r in cur.fetchall():
    print(f"  {str(r[0])[:24]:<25} {str(r[1]):<6} {int(r[2]):>5} {float(r[3]):>8.2f} {float(r[4]):>8.2f} {float(r[5]):>8.2f}")

# ── 4. Check how many orders to fill ──
print("\n" + "="*70)
print("ORDERS TO FILL: Feb-Mar 2026 missing logistics, by country")
print("="*70)
cur.execute("""
    SELECT
        o.ship_country,
        CASE WHEN lc.lines = 1 THEN 1
             WHEN lc.lines = 2 THEN 2
             WHEN lc.lines <= 3 THEN 3
             ELSE 4
        END AS line_bucket,
        COUNT(*) AS cnt
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
      AND o.ship_country IS NOT NULL
    GROUP BY o.ship_country,
        CASE WHEN lc.lines = 1 THEN 1
             WHEN lc.lines = 2 THEN 2
             WHEN lc.lines <= 3 THEN 3
             ELSE 4
        END
    ORDER BY cnt DESC
""")
print(f"  {'Country':<8} {'Bucket':>6} {'Orders':>8}")
total_to_fill = 0
for r in cur.fetchall():
    n = int(r[2])
    total_to_fill += n
    print(f"  {str(r[0]):<8} {int(r[1]):>6} {n:>8,}")
print(f"\n  TOTAL TO FILL: {total_to_fill:,}")

conn.close()
print("\nDONE")
