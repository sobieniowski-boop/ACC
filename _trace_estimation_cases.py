"""Trace estimation logic for 3 specific order cases."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc

conn = connect_acc(timeout=30)
cur = conn.cursor()

# ============================================================
# THE ESTIMATION MODEL
# ============================================================
print("=" * 70)
print("HOW THE ESTIMATION WORKS")
print("=" * 70)
print("""
Model: historical MEDIAN cost per (ship_country, line_count_bucket)
- line_count_bucket = number of acc_order_line rows per order, capped at 4
- Based on Jan 2026 actual DHL/GLS invoices
- NO weight, NO quantity, NO product type consideration

Key medians used:
  DE bucket 1 = 23.95 PLN    DE bucket 2 = 23.95 PLN
  FR bucket 1 = 39.75 PLN    FR bucket 2 = 39.75 PLN
  IT bucket 1 = 44.55 PLN    IT bucket 2 = 47.30 PLN
""")

# ============================================================
# CASE 1: DE, 2x10 jars
# ============================================================
print("=" * 70)
print("CASE 1: DE order with 2x10 jars (słoiki)")
print("=" * 70)

# Find real example
cur.execute("""
    SELECT TOP 3
        o.amazon_order_id, o.ship_country,
        ol.sku, ol.quantity_ordered, LEFT(ol.title, 70) AS title
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE o.ship_country = 'DE'
      AND o.purchase_date >= '2026-02-01'
      AND ol.quantity_ordered >= 10
      AND ol.title LIKE '%Einmachgl%'
    ORDER BY o.purchase_date DESC
""")
examples = cur.fetchall()
print(f"\nReal examples found: {len(examples)}")
for r in examples:
    print(f"  order={r[0]} qty={r[3]} sku={r[2]}")
    print(f"  title={r[4]}")

    # Check this order's full line structure
    cur.execute("""
        SELECT ol.sku, ol.quantity_ordered, LEFT(ol.title, 60)
        FROM dbo.acc_order o WITH (NOLOCK)
        JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
        WHERE o.amazon_order_id = ?
    """, (r[0],))
    lines = cur.fetchall()
    print(f"  Order has {len(lines)} line(s):")
    for l in lines:
        print(f"    SKU={l[0]} qty={l[1]} | {l[2]}")

    # What cost did we assign?
    cur.execute("""
        SELECT calc_version, total_logistics_pln, source_system
        FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
        WHERE amazon_order_id = ?
    """, (r[0],))
    facts = cur.fetchall()
    for f in facts:
        print(f"  → COST: {float(f[1]):.2f} PLN (version={f[0]}, source={f[2]})")

    # What would actual cost look like from DHL billing?
    cur.execute("""
        SELECT AVG(b.total_amount_pln), MIN(b.total_amount_pln), MAX(b.total_amount_pln), COUNT(*)
        FROM dbo.acc_dhl_billing_line b WITH (NOLOCK)
        WHERE b.weight >= 5 AND b.weight <= 15
          AND b.destination_country = 'DE'
    """)
    row = cur.fetchone()
    if row and row[3]:
        print(f"  → DHL actual DE 5-15kg: avg={float(row[0]):.2f}, min={float(row[1]):.2f}, max={float(row[2]):.2f} (n={row[3]})")
    print()

# ============================================================
# CASE 2: FR, 1x suszarka stabilo
# ============================================================
print("=" * 70)
print("CASE 2: FR order with 1x suszarka (ceiling dryer)")
print("=" * 70)

cur.execute("""
    SELECT TOP 3
        o.amazon_order_id, o.ship_country,
        ol.sku, ol.quantity_ordered, LEFT(ol.title, 80) AS title
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE o.ship_country = 'FR'
      AND o.purchase_date >= '2026-02-01'
      AND ol.sku LIKE '%4415%'
    ORDER BY o.purchase_date DESC
""")
examples = cur.fetchall()
print(f"\nReal examples: {len(examples)}")
for r in examples:
    print(f"  order={r[0]} qty={r[3]} sku={r[2]}")
    print(f"  title={r[4]}")

    cur.execute("""
        SELECT ol.sku, ol.quantity_ordered, LEFT(ol.title, 60)
        FROM dbo.acc_order o WITH (NOLOCK)
        JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
        WHERE o.amazon_order_id = ?
    """, (r[0],))
    lines = cur.fetchall()
    print(f"  Order {len(lines)} line(s):")
    for l in lines:
        print(f"    SKU={l[0]} qty={l[1]} | {l[2]}")

    cur.execute("""
        SELECT calc_version, total_logistics_pln, source_system
        FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
        WHERE amazon_order_id = ?
    """, (r[0],))
    for f in cur.fetchall():
        print(f"  → COST: {float(f[1]):.2f} PLN ({f[0]}, {f[2]})")
    print()

# ============================================================
# CASE 3: IT, doniczka zestaw (3 pcs)
# ============================================================
print("=" * 70)
print("CASE 3: IT order with doniczka zestaw (3 pots)")
print("=" * 70)

cur.execute("""
    SELECT TOP 3
        o.amazon_order_id, o.ship_country,
        ol.sku, ol.quantity_ordered, LEFT(ol.title, 80) AS title
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE o.ship_country = 'IT'
      AND o.purchase_date >= '2026-02-01'
      AND (ol.title LIKE '%doniczk%' OR ol.title LIKE '%Blumentopf%' OR ol.title LIKE '%vaso%')
      AND (ol.title LIKE '%3%' OR ol.title LIKE '%set%' OR ol.title LIKE '%zestaw%')
    ORDER BY o.purchase_date DESC
""")
examples = cur.fetchall()
print(f"\nReal examples: {len(examples)}")
for r in examples:
    print(f"  order={r[0]} qty={r[3]} sku={r[2]}")
    print(f"  title={r[4]}")

    cur.execute("""
        SELECT ol.sku, ol.quantity_ordered, LEFT(ol.title, 60)
        FROM dbo.acc_order o WITH (NOLOCK)
        JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
        WHERE o.amazon_order_id = ?
    """, (r[0],))
    lines = cur.fetchall()
    print(f"  Order {len(lines)} line(s):")
    for l in lines:
        print(f"    SKU={l[0]} qty={l[1]} | {l[2]}")

    cur.execute("""
        SELECT calc_version, total_logistics_pln, source_system
        FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
        WHERE amazon_order_id = ?
    """, (r[0],))
    for f in cur.fetchall():
        print(f"  → COST: {float(f[1]):.2f} PLN ({f[0]}, {f[2]})")
    print()

# ============================================================
# KEY PROBLEM: same cost regardless of weight/size
# ============================================================
print("=" * 70)
print("THE PROBLEM: No weight/size differentiation")
print("=" * 70)

# Show actual DHL costs by weight range for DE
cur.execute("""
    SELECT
        CASE
            WHEN weight < 2 THEN '0-2kg'
            WHEN weight < 5 THEN '2-5kg'
            WHEN weight < 10 THEN '5-10kg'
            WHEN weight < 20 THEN '10-20kg'
            WHEN weight < 31.5 THEN '20-31.5kg'
            ELSE '31.5kg+'
        END AS weight_range,
        COUNT(*) AS n,
        AVG(total_amount_pln) AS avg_cost,
        MIN(total_amount_pln) AS min_cost,
        MAX(total_amount_pln) AS max_cost
    FROM dbo.acc_dhl_billing_line WITH (NOLOCK)
    WHERE destination_country = 'DE'
      AND total_amount_pln > 0
      AND weight > 0
    GROUP BY
        CASE
            WHEN weight < 2 THEN '0-2kg'
            WHEN weight < 5 THEN '2-5kg'
            WHEN weight < 10 THEN '5-10kg'
            WHEN weight < 20 THEN '10-20kg'
            WHEN weight < 31.5 THEN '20-31.5kg'
            ELSE '31.5kg+'
        END
    ORDER BY MIN(weight)
""")
print("\nActual DHL costs by weight (DE):")
print(f"  {'Range':<12} {'N':>6} {'AVG':>8} {'MIN':>8} {'MAX':>8}")
for r in cur.fetchall():
    print(f"  {r[0]:<12} {r[1]:>6,} {float(r[2]):>8.2f} {float(r[3]):>8.2f} {float(r[4]):>8.2f}")

# Same for FR
cur.execute("""
    SELECT
        CASE
            WHEN weight < 2 THEN '0-2kg'
            WHEN weight < 5 THEN '2-5kg'
            WHEN weight < 10 THEN '5-10kg'
            WHEN weight < 20 THEN '10-20kg'
            WHEN weight < 31.5 THEN '20-31.5kg'
            ELSE '31.5kg+'
        END AS weight_range,
        COUNT(*) AS n,
        AVG(total_amount_pln) AS avg_cost
    FROM dbo.acc_dhl_billing_line WITH (NOLOCK)
    WHERE destination_country = 'FR'
      AND total_amount_pln > 0
      AND weight > 0
    GROUP BY
        CASE
            WHEN weight < 2 THEN '0-2kg'
            WHEN weight < 5 THEN '2-5kg'
            WHEN weight < 10 THEN '5-10kg'
            WHEN weight < 20 THEN '10-20kg'
            WHEN weight < 31.5 THEN '20-31.5kg'
            ELSE '31.5kg+'
        END
    ORDER BY MIN(weight)
""")
print("\nActual DHL costs by weight (FR):")
for r in cur.fetchall():
    print(f"  {r[0]:<12} {r[1]:>6,} {float(r[2]):>8.2f}")

# Same for IT
cur.execute("""
    SELECT
        CASE
            WHEN weight < 2 THEN '0-2kg'
            WHEN weight < 5 THEN '2-5kg'
            WHEN weight < 10 THEN '5-10kg'
            WHEN weight < 20 THEN '10-20kg'
            WHEN weight < 31.5 THEN '20-31.5kg'
            ELSE '31.5kg+'
        END AS weight_range,
        COUNT(*) AS n,
        AVG(total_amount_pln) AS avg_cost
    FROM dbo.acc_dhl_billing_line WITH (NOLOCK)
    WHERE destination_country = 'IT'
      AND total_amount_pln > 0
      AND weight > 0
    GROUP BY
        CASE
            WHEN weight < 2 THEN '0-2kg'
            WHEN weight < 5 THEN '2-5kg'
            WHEN weight < 10 THEN '5-10kg'
            WHEN weight < 20 THEN '10-20kg'
            WHEN weight < 31.5 THEN '20-31.5kg'
            ELSE '31.5kg+'
        END
    ORDER BY MIN(weight)
""")
print("\nActual DHL costs by weight (IT):")
for r in cur.fetchall():
    print(f"  {r[0]:<12} {r[1]:>6,} {float(r[2]):>8.2f}")

# What's the per-order cost we currently assign?
print("\n" + "=" * 70)
print("ESTIMATION vs REALITY summary:")
print("=" * 70)
print("""
Nasz model przypisuje JEDEN koszt per zamówienie, niezależnie od wagi/rozmiaru:
  DE bucket 1 (1 linia) → 23.95 PLN (mediana)
  DE bucket 2 (2 linie) → 23.95 PLN
  FR bucket 1            → 39.75 PLN
  IT bucket 1            → 44.55 PLN

Problem: 10 szt. słoików waży ~5-8 kg, a lekka doniczka ~0.5 kg.
Koszt DHL różni się 2-3x w zależności od wagi!
""")

conn.close()
