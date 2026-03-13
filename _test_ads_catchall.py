"""
Test: ads catch-all allocation.
1. Show current ads totals in rollup vs product_day (the gap).
2. Trigger _enrich_rollup_from_finance for Jan-Mar 2026.
3. Show new totals and the gap reduction.
"""
import sys, os
sys.path.insert(0, r"c:\ACC\apps\api")
os.chdir(r"c:\ACC\apps\api")
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env")

from datetime import date
from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=False, timeout=60)
cur = conn.cursor()

date_from = date(2026, 1, 1)
date_to = date(2026, 3, 10)

# ── BEFORE: current state ──
print("="*70)
print("BEFORE catch-all recompute")
print("="*70)

# Total ads from product_day
cur.execute("""
    SELECT
        CONVERT(VARCHAR(7), report_date, 120) AS m,
        SUM(ISNULL(spend_pln, 0)) AS total_spend
    FROM dbo.acc_ads_product_day WITH (NOLOCK)
    WHERE report_date >= ? AND report_date <= ?
      AND sku IS NOT NULL AND sku != ''
    GROUP BY CONVERT(VARCHAR(7), report_date, 120)
    ORDER BY m
""", (date_from, date_to))
product_day = {}
print("\n[product_day spend by month]")
for r in cur.fetchall():
    m, s = str(r[0]), float(r[1])
    product_day[m] = s
    print(f"  {m}: {s:>12,.2f} PLN")

# Total ads in rollup 
cur.execute("""
    SELECT
        CONVERT(VARCHAR(7), period_date, 120) AS m,
        SUM(ISNULL(ad_spend_pln, 0)) AS total_ads
    FROM dbo.acc_sku_profitability_rollup WITH (NOLOCK)
    WHERE period_date >= ? AND period_date <= ?
    GROUP BY CONVERT(VARCHAR(7), period_date, 120)
    ORDER BY m
""", (date_from, date_to))
rollup_before = {}
print("\n[rollup ad_spend_pln by month - BEFORE]")
for r in cur.fetchall():
    m, s = str(r[0]), float(r[1])
    rollup_before[m] = s
    print(f"  {m}: {s:>12,.2f} PLN")

print("\n[GAP BEFORE]")
for m in sorted(set(product_day) | set(rollup_before)):
    pd_val = product_day.get(m, 0)
    ro_val = rollup_before.get(m, 0)
    gap = pd_val - ro_val
    pct = (gap / pd_val * 100) if pd_val > 0 else 0
    print(f"  {m}: product_day={pd_val:>10,.0f}  rollup={ro_val:>10,.0f}  gap={gap:>10,.0f} ({pct:.1f}%)")

conn.commit()  # release locks

# ── RUN enrichment ──
print("\n" + "="*70)
print("RUNNING _enrich_rollup_from_finance ...")
print("="*70)

conn2 = connect_acc(autocommit=False, timeout=120)
cur2 = conn2.cursor()

from app.services.profitability_service import _enrich_rollup_from_finance
stats = _enrich_rollup_from_finance(cur2, conn2, date_from, date_to)
conn2.commit()
print(f"Stats: {stats}")
cur2.close()
conn2.close()

# ── AFTER ──
print("\n" + "="*70)
print("AFTER catch-all recompute")
print("="*70)

cur.execute("""
    SELECT
        CONVERT(VARCHAR(7), period_date, 120) AS m,
        SUM(ISNULL(ad_spend_pln, 0)) AS total_ads
    FROM dbo.acc_sku_profitability_rollup WITH (NOLOCK)
    WHERE period_date >= ? AND period_date <= ?
    GROUP BY CONVERT(VARCHAR(7), period_date, 120)
    ORDER BY m
""", (date_from, date_to))
rollup_after = {}
print("\n[rollup ad_spend_pln by month - AFTER]")
for r in cur.fetchall():
    m, s = str(r[0]), float(r[1])
    rollup_after[m] = s
    print(f"  {m}: {s:>12,.2f} PLN")

print("\n[GAP AFTER]")
for m in sorted(set(product_day) | set(rollup_after)):
    pd_val = product_day.get(m, 0)
    ro_val = rollup_after.get(m, 0)
    gap = pd_val - ro_val
    pct = (gap / pd_val * 100) if pd_val > 0 else 0
    print(f"  {m}: product_day={pd_val:>10,.0f}  rollup={ro_val:>10,.0f}  gap={gap:>10,.0f} ({pct:.1f}%)")

print("\n[IMPROVEMENT]")
for m in sorted(set(rollup_before) | set(rollup_after)):
    before = rollup_before.get(m, 0)
    after = rollup_after.get(m, 0)
    delta = after - before
    print(f"  {m}: before={before:>10,.0f}  after={after:>10,.0f}  recovered={delta:>+10,.0f}")

conn.close()
print("\nDONE")
