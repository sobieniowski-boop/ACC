"""Test fixed endpoints."""
import sys, time
sys.path.insert(0, r"C:\ACC\apps\api")
import requests
from app.core.security import create_access_token

token = create_access_token("smoke", "admin")
h = {"Authorization": f"Bearer {token}"}

print("=== KPI SUMMARY ===")
t0 = time.time()
try:
    r = requests.get(
        "http://localhost:8000/api/v1/kpi/summary?date_from=2026-03-01&date_to=2026-03-11",
        headers=h, timeout=120,
    )
    elapsed = time.time() - t0
    print(f"STATUS: {r.status_code} in {elapsed:.1f}s")
    if r.status_code == 200:
        d = r.json()
        print(f"  Revenue: {d.get('total_revenue_pln')}")
        print(f"  Orders: {d.get('total_orders')}")
        print(f"  Units: {d.get('total_units')}")
        print(f"  CM1: {d.get('total_cm1_pln')}")
        print(f"  ACoS: {d.get('total_acos')}")
        print(f"  TACoS: {d.get('total_tacos')}")
        print(f"  FBA orders: {d.get('fba_orders')}")
        print(f"  FBM orders: {d.get('fbm_orders')}")
        print(f"  Marketplaces: {len(d.get('by_marketplace', []))}")
    else:
        print(r.text[:500])
except Exception as e:
    print(f"ERROR: {e}")

print()
print("=== FBA FEE OVERCHARGES ===")
t0 = time.time()
try:
    r2 = requests.get(
        "http://localhost:8000/api/v1/fba/fee-audit/overcharges?date_from=2025-12-12&date_to=2026-03-11",
        headers=h, timeout=120,
    )
    elapsed = time.time() - t0
    print(f"STATUS: {r2.status_code} in {elapsed:.1f}s")
    if r2.status_code == 200:
        d2 = r2.json()
        print(f"  SKUs affected: {d2.get('total_skus_affected')}")
        print(f"  Total overcharge EUR: {d2.get('total_estimated_overcharge_eur')}")
        print(f"  Items: {len(d2.get('items', []))}")
        if d2.get("items"):
            top = d2["items"][0]
            print(f"  Top: {top['sku']} - {top['estimated_overcharge_eur']} EUR ({top['severity']})")
    else:
        print(r2.text[:500])
except Exception as e:
    print(f"ERROR: {e}")
