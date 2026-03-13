import requests, time, json, sys

tok = open(r"C:\ACC\_token.txt").read().strip()
headers = {"Authorization": f"Bearer {tok}"}

print("=== KPI Summary ===")
t0 = time.time()
r = requests.get("http://localhost:8000/api/v1/kpi/summary", headers=headers, timeout=120)
print(f"Status: {r.status_code} ({time.time()-t0:.1f}s)")
if r.status_code == 200:
    d = r.json()
    print(f"Revenue: {d.get('total_revenue_pln')}")
    print(f"ACoS: {d.get('total_acos')}")
    print(f"TACoS: {d.get('total_tacos')}")
    print(f"FBA: {d.get('fba_orders')}  FBM: {d.get('fbm_orders')}")
else:
    print(r.text[:300])

print()
print("=== FBA Fee Audit ===")
t0 = time.time()
r2 = requests.get("http://localhost:8000/api/v1/fba/fee-audit/overcharges", headers=headers, timeout=180)
print(f"Status: {r2.status_code} ({time.time()-t0:.1f}s)")
if r2.status_code == 200:
    d2 = r2.json()
    if d2.get("items"):
        first = d2["items"][0]
        print(f"First item keys: {sorted(first.keys())}")
        print(f"estimated_overcharge_eur: {first.get('estimated_overcharge_eur')}")
    print(f"total_skus_affected: {d2.get('total_skus_affected')}")
    print(f"total_estimated_overcharge_eur: {d2.get('total_estimated_overcharge_eur')}")
    print(f"overcharge_by_currency: {d2.get('overcharge_by_currency')}")
else:
    print(r2.text[:500])
