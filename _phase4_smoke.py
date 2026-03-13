"""Phase 4: Recompute executive metrics (populate cm1/cm2) + smoke tests."""
import sys, requests

# 1) Recompute executive metrics (30 days back to fill cm1/cm2)
print("=== Recomputing executive metrics ===")
from app.services.executive_service import recompute_executive_metrics
result = recompute_executive_metrics(days_back=90)
print(f"  Metrics rows: {result['metrics_rows']}, elapsed: {result['elapsed']}s")

# 2) Verify cm1/cm2 in executive_daily_metrics
from app.core.db_connection import connect_acc
conn = connect_acc(autocommit=False, timeout=15)
cur = conn.cursor()
cur.execute("""
    SELECT TOP 5 period_date, marketplace_id,
           revenue_pln, cm1_pln, cm2_pln, profit_pln
    FROM dbo.executive_daily_metrics WITH (NOLOCK)
    WHERE cm1_pln IS NOT NULL AND cm1_pln <> 0
    ORDER BY period_date DESC
""")
rows = cur.fetchall()
print(f"\n=== Sample executive_daily_metrics with cm1/cm2 ({len(rows)} rows) ===")
for r in rows:
    print(f"  {r[0]} | {r[1][:8]}.. | rev={r[2]:.2f} | cm1={r[3]:.2f} | cm2={r[4]:.2f} | np={r[5]:.2f}")
conn.close()

# 3) Smoke test: executive overview API
print("\n=== Smoke tests ===")
BASE = "http://localhost:8000/api/v1"

tests_passed = 0
tests_failed = 0

def check(name: str, condition: bool, detail: str = ""):
    global tests_passed, tests_failed
    if condition:
        tests_passed += 1
        print(f"  PASS: {name} {detail}")
    else:
        tests_failed += 1
        print(f"  FAIL: {name} {detail}")

try:
    # Test 1: Executive overview has cm1/cm2
    r = requests.get(f"{BASE}/executive/overview", params={"from": "2025-12-01", "to": "2026-03-10"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    kpi = data["kpi"]
    check("ExecOverview KPI has cm1_pln", "cm1_pln" in kpi, f"cm1={kpi.get('cm1_pln')}")
    check("ExecOverview KPI has cm2_pln", "cm2_pln" in kpi, f"cm2={kpi.get('cm2_pln')}")
    check("ExecOverview best_skus has cm1", len(data["best_skus"]) > 0 and "cm1_pln" in data["best_skus"][0],
          f"n={len(data['best_skus'])}")

    # Test 2: Executive products has cm1/cm2
    r = requests.get(f"{BASE}/executive/products", params={"from": "2025-12-01", "to": "2026-03-10"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data["items"]:
        item = data["items"][0]
        check("ExecProducts has cm1_pln", "cm1_pln" in item, f"cm1={item.get('cm1_pln')}")
        check("ExecProducts has cm2_pln", "cm2_pln" in item, f"cm2={item.get('cm2_pln')}")
    else:
        check("ExecProducts has items", False, "empty list")

    # Test 3: Executive marketplaces has cm1/cm2
    r = requests.get(f"{BASE}/executive/marketplaces", params={"from": "2025-12-01", "to": "2026-03-10"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data["items"]:
        item = data["items"][0]
        check("ExecMarketplaces has cm1_pln", "cm1_pln" in item, f"cm1={item.get('cm1_pln')}")
        check("ExecMarketplaces has cm2_pln", "cm2_pln" in item, f"cm2={item.get('cm2_pln')}")
    else:
        check("ExecMarketplaces has items", False, "empty list")

    # Test 4: Profitability overview has cm1/cm2
    r = requests.get(f"{BASE}/profitability/overview", params={"from": "2025-12-01", "to": "2026-03-10"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    kpi = data["kpi"]
    check("ProfitOverview KPI has cm1", "total_cm1_pln" in kpi, f"cm1={kpi.get('total_cm1_pln')}")
    check("ProfitOverview KPI has cm2", "total_cm2_pln" in kpi, f"cm2={kpi.get('total_cm2_pln')}")

    # Test 5: Profitability products has cm1/cm2
    r = requests.get(f"{BASE}/profitability/products", params={"from": "2025-12-01", "to": "2026-03-10"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data["items"]:
        item = data["items"][0]
        check("ProfitProducts has cm1_pln", "cm1_pln" in item, f"cm1={item.get('cm1_pln')}")
        check("ProfitProducts has cm2_pln", "cm2_pln" in item, f"cm2={item.get('cm2_pln')}")
    else:
        check("ProfitProducts has items", False, "empty list")

    # Test 6: Invariant: profit_pln == cm2_pln (no overhead allocated)
    r = requests.get(f"{BASE}/profitability/products", params={"from": "2026-01-01", "to": "2026-03-10", "page_size": 5}, timeout=30)
    r.raise_for_status()
    items = r.json()["items"]
    all_match = all(abs(it["profit_pln"] - it["cm2_pln"]) < 0.01 for it in items if "cm2_pln" in it)
    check("Invariant: NP == CM2", all_match, f"checked {len(items)} items")

except Exception as e:
    print(f"  ERROR: {e}")
    tests_failed += 1

print(f"\n=== Results: {tests_passed} passed, {tests_failed} failed ===")
sys.exit(1 if tests_failed > 0 else 0)
