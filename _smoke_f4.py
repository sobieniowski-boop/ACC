"""Live smoke test for F4 logistics enrichment in rollup.

Run: python _smoke_f4.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))

from datetime import date, timedelta

# --- 1. Check baseline: how many rollup rows have logistics_pln = 0 ---
from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=False, timeout=30)
cur = conn.cursor()

date_from = date(2026, 1, 1)
date_to = date(2026, 3, 9)

cur.execute("""
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN logistics_pln = 0 THEN 1 ELSE 0 END) AS zero_logistics,
        SUM(CASE WHEN logistics_pln > 0 THEN 1 ELSE 0 END) AS nonzero_logistics,
        ROUND(SUM(logistics_pln), 2) AS total_logistics_pln
    FROM dbo.acc_sku_profitability_rollup WITH (NOLOCK)
    WHERE period_date >= ? AND period_date <= ?
""", (date_from, date_to))
row = cur.fetchone()
print(f"BEFORE: total={row[0]}, zero_logistics={row[1]}, nonzero={row[2]}, sum={row[3]}")
conn.close()

# --- 2. Run enrichment ---
from app.services.profitability_service import _enrich_rollup_from_finance

conn2 = connect_acc(autocommit=False, timeout=120)
cur2 = conn2.cursor()

print("\nRunning enrichment...")
stats = _enrich_rollup_from_finance(cur2, conn2, date_from, date_to)
conn2.commit()
conn2.close()
print(f"Enrichment stats: {stats}")

# --- 3. Check after ---
conn3 = connect_acc(autocommit=False, timeout=30)
cur3 = conn3.cursor()

cur3.execute("""
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN logistics_pln = 0 THEN 1 ELSE 0 END) AS zero_logistics,
        SUM(CASE WHEN logistics_pln > 0 THEN 1 ELSE 0 END) AS nonzero_logistics,
        ROUND(SUM(logistics_pln), 2) AS total_logistics_pln
    FROM dbo.acc_sku_profitability_rollup WITH (NOLOCK)
    WHERE period_date >= ? AND period_date <= ?
""", (date_from, date_to))
row3 = cur3.fetchone()
print(f"\nAFTER:  total={row3[0]}, zero_logistics={row3[1]}, nonzero={row3[2]}, sum={row3[3]}")

# --- 4. Spot check: compare a few SKU rollup logistics with order-level ---
cur3.execute("""
    SELECT TOP 5
        r.sku, r.marketplace_id, r.period_date,
        r.logistics_pln AS rollup_logistics,
        r.revenue_pln,
        r.profit_pln
    FROM dbo.acc_sku_profitability_rollup r WITH (NOLOCK)
    WHERE r.period_date >= ? AND r.period_date <= ?
      AND r.logistics_pln > 0
    ORDER BY r.logistics_pln DESC
""", (date_from, date_to))
print("\nTop 5 SKU rows by logistics_pln:")
for r in cur3.fetchall():
    print(f"  {r[0]} | {r[1]} | {r[2]} | logistics={r[3]} | rev={r[4]} | profit={r[5]}")

# --- 5. Idempotency check: run again, verify same count ---
conn4 = connect_acc(autocommit=False, timeout=120)
cur4 = conn4.cursor()
print("\nIdempotency check: re-running enrichment...")
stats2 = _enrich_rollup_from_finance(cur4, conn4, date_from, date_to)
conn4.commit()
conn4.close()

conn5 = connect_acc(autocommit=False, timeout=30)
cur5 = conn5.cursor()
cur5.execute("""
    SELECT
        ROUND(SUM(logistics_pln), 2) AS total_logistics_pln
    FROM dbo.acc_sku_profitability_rollup WITH (NOLOCK)
    WHERE period_date >= ? AND period_date <= ?
""", (date_from, date_to))
row5 = cur5.fetchone()
conn5.close()

print(f"After re-run: sum={row5[0]} (should match {row3[3]})")
if abs((row5[0] or 0) - (row3[3] or 0)) < 0.01:
    print("IDEMPOTENT OK")
else:
    print(f"WARNING: NOT idempotent! delta={abs((row5[0] or 0) - (row3[3] or 0))}")

print("\nF4 smoke test complete")
