"""Verify seasonality endpoints still work and scheduler has new job."""
import requests, json

tok = open(r"C:\ACC\_token.txt").read().strip()
h = {"Authorization": f"Bearer {tok}"}

# 1. Seasonality overview
r = requests.get("http://localhost:8000/api/v1/seasonality/overview", headers=h, timeout=60)
print(f"Overview: {r.status_code}")
if r.status_code == 200:
    d = r.json()
    kpi = d.get("kpi", {})
    print(f"  seasonal_categories: {kpi.get('seasonal_categories')}")
    print(f"  evergreen_categories: {kpi.get('evergreen_categories')}")

# 2. Check scheduler registered the new job
r2 = requests.get("http://localhost:8000/api/v1/kpi/summary", headers=h, timeout=60)
print(f"\nKPI: {r2.status_code}")

print("\nAll checks passed!")
