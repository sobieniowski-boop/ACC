"""Trace estimation logic for 3 specific order cases - v2."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
from app.core.db_connection import connect_acc

conn = connect_acc(timeout=30)
cur = conn.cursor()

print("=" * 70)
print("JAK DZIAŁA ESTYMACJA KOSZTÓW LOGISTYKI")
print("=" * 70)
print("""
Model: mediana historycznego kosztu per (kraj, liczba_linii_zamówienia)
  - line_count_bucket = ile LINII (SKU) ma zamówienie (max 4)
  - Dane wzorcowe: styczeń 2026, faktyczne faktury DHL + GLS
  - Model NIE uwzględnia: wagi, ilości sztuk, typu produktu

Kluczowe mediany:
  DE bucket 1 (1 linia)  = 23.95 PLN
  DE bucket 2 (2 linie)  = 23.95 PLN
  FR bucket 1            = 39.75 PLN
  IT bucket 1            = 44.55 PLN
  IT bucket 2            = 47.30 PLN
""")

# ============================================================
# CASE 1: DE, 2x10 jars
# ============================================================
print("=" * 70)
print("PRZYPADEK 1: DE — zamówienie 2×10 szt. słoików")
print("=" * 70)
print("""
Scenariusz użytkownika: klient DE kupuje w jednym zamówieniu
  - 10 szt. słoików typu A
  - 10 szt. słoików typu B
""")

# Find such order
cur.execute("""
    SELECT TOP 1 o.amazon_order_id, o.id
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE o.ship_country = 'DE'
      AND o.purchase_date >= '2026-02-01'
      AND ol.quantity_ordered >= 5
      AND ol.title LIKE '%Einmachgl%'
    ORDER BY o.purchase_date DESC
""")
ex = cur.fetchone()
if ex:
    oid = ex[0]
    cur.execute("""
        SELECT ol.sku, ol.quantity_ordered, LEFT(ol.title, 60)
        FROM dbo.acc_order_line ol WITH (NOLOCK)
        JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        WHERE o.amazon_order_id = ?
    """, (oid,))
    lines = cur.fetchall()
    print(f"Przykład: {oid}")
    print(f"  Linie zamówienia: {len(lines)}")
    for l in lines:
        print(f"    SKU={l[0]} qty={l[1]} | {l[2]}")

    cur.execute("""
        SELECT calc_version, total_logistics_pln, source_system
        FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
        WHERE amazon_order_id = ?
    """, (oid,))
    for f in cur.fetchall():
        print(f"  → Przypisany koszt: {float(f[1]):.2f} PLN")
        print(f"    calc_version={f[0]}, source={f[2]}")

print("""
Jak to działa:
  1. Zliczamy linie zamówienia → np. 1 linia (bo 10 szt. tego samego SKU = 1 linia)
     LUB 2 linie (jeśli 2 różne SKU po 10 szt.)
  2. Bucket = min(line_count, 4)
  3. Lookup: (DE, bucket=1) → mediana 23.95 PLN
             (DE, bucket=2) → mediana 23.95 PLN
  
  PROBLEM: 20 słoików waży ~6-10 kg → faktyczna paczka DHL/GLS kosztuje
  prawdopodobnie ~35-50 PLN. Ale nasz model daje 23.95 PLN bo to mediana
  WSZYSTKICH zamówień DE (w tym lekkie pokrywki, doniczki plastikowe).
  
  → Estymacja jest tu ZANIŻONA (~24 vs ~40 PLN faktycznych)
""")

# ============================================================
# CASE 2: FR, 1x suszarka
# ============================================================
print("=" * 70)
print("PRZYPADEK 2: FR — 1 szt. suszarki sufitowej")
print("=" * 70)

cur.execute("""
    SELECT TOP 1 o.amazon_order_id
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE o.ship_country = 'FR'
      AND o.purchase_date >= '2026-02-01'
      AND ol.quantity_ordered = 1
      AND ol.sku LIKE '%4415%'
    ORDER BY o.purchase_date DESC
""")
ex = cur.fetchone()
if ex:
    oid = ex[0]
    cur.execute("""
        SELECT ol.sku, ol.quantity_ordered, LEFT(ol.title, 70)
        FROM dbo.acc_order_line ol WITH (NOLOCK)
        JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        WHERE o.amazon_order_id = ?
    """, (oid,))
    lines = cur.fetchall()
    print(f"\nPrzykład: {oid}")
    for l in lines:
        print(f"  SKU={l[0]} qty={l[1]} | {l[2]}")

    cur.execute("""
        SELECT calc_version, total_logistics_pln, source_system
        FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
        WHERE amazon_order_id = ?
    """, (oid,))
    for f in cur.fetchall():
        print(f"  → Przypisany koszt: {float(f[1]):.2f} PLN ({f[0]}, {f[2]})")

print("""
Jak to działa:
  1. Zamówienie ma 1 linię (1 szt. suszarki) → bucket = 1
  2. Lookup: (FR, bucket=1) → mediana 39.75 PLN
  
  Suszarka sufitowa to DUŻA, lekka paczka (~120-160 cm, 3-5 kg).
  Faktyczny koszt DHL do FR prawdopodobnie ~45-65 PLN (gabaryt!).
  
  → Estymacja jest tu ZANIŻONA (~40 vs ~55 PLN faktycznych)
""")

# ============================================================
# CASE 3: IT, zestaw finezja (3 doniczki)
# ============================================================
print("=" * 70)
print("PRZYPADEK 3: IT — zestaw 3 doniczek (Finezja)")
print("=" * 70)

cur.execute("""
    SELECT TOP 1 o.amazon_order_id
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE o.ship_country = 'IT'
      AND o.purchase_date >= '2026-02-01'
      AND ol.quantity_ordered = 1
      AND (ol.title LIKE '%Blumentopf%' OR ol.title LIKE '%vaso%')
    ORDER BY o.purchase_date DESC
""")
ex = cur.fetchone()
if ex:
    oid = ex[0]
    cur.execute("""
        SELECT ol.sku, ol.quantity_ordered, LEFT(ol.title, 70)
        FROM dbo.acc_order_line ol WITH (NOLOCK)
        JOIN dbo.acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        WHERE o.amazon_order_id = ?
    """, (oid,))
    lines = cur.fetchall()
    print(f"\nPrzykład: {oid}")
    for l in lines:
        print(f"  SKU={l[0]} qty={l[1]} | {l[2]}")

    cur.execute("""
        SELECT calc_version, total_logistics_pln, source_system
        FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
        WHERE amazon_order_id = ?
    """, (oid,))
    for f in cur.fetchall():
        print(f"  → Przypisany koszt: {float(f[1]):.2f} PLN ({f[0]}, {f[2]})")

print("""
Jak to działa:
  1. "Zestaw Finezja 3 doniczki" = 1 SKU → 1 linia → bucket = 1
  2. Lookup: (IT, bucket=1) → mediana 44.55 PLN
  
  Zestaw 3 doniczek plastikowych to raczej lekka paczka (~1-3 kg).
  Faktyczny koszt DHL do IT dla lekkiego pakunku ~30-40 PLN.
  
  → Estymacja jest tu ZAWYŻONA (~45 vs ~35 PLN faktycznych)
""")

# ============================================================
# AGGREGATE IMPACT: too high or too low?
# ============================================================
print("=" * 70)
print("WPŁYW AGREGATOWY")
print("=" * 70)

# Compare: estimated avg vs actual avg by country
cur.execute("""
    SELECT
        o.ship_country,
        f.calc_version,
        COUNT(*) AS n,
        AVG(f.total_logistics_pln) AS avg_cost
    FROM dbo.acc_order_logistics_fact f WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = f.amazon_order_id
    WHERE o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-04-01'
      AND f.calc_version IN ('hist_country_v1', 'dhl_v1', 'gls_v1')
      AND o.ship_country IN ('DE', 'FR', 'IT', 'AT', 'PL')
    GROUP BY o.ship_country, f.calc_version
    ORDER BY o.ship_country, f.calc_version
""")
print(f"\n{'Country':<8} {'Version':<18} {'Count':>8} {'Avg PLN':>10}")
print("-" * 50)
for r in cur.fetchall():
    print(f"{r[0]:<8} {r[1]:<18} {r[2]:>8,} {float(r[3]):>10.2f}")

# Total logistics cost comparison
print("\nŁączny koszt logistyki Feb-Mar 2026:")
cur.execute("""
    SELECT
        f.calc_version,
        COUNT(*) AS orders,
        SUM(f.total_logistics_pln) AS total_pln
    FROM dbo.acc_order_logistics_fact f WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = f.amazon_order_id
    WHERE o.purchase_date >= '2026-02-01' AND o.purchase_date < '2026-04-01'
    GROUP BY f.calc_version
    ORDER BY f.calc_version
""")
for r in cur.fetchall():
    print(f"  {r[0]:<18}: {r[1]:>8,} zamówień × avg {float(r[2])/r[1]:.2f} = {float(r[2]):>12,.0f} PLN")

conn.close()
