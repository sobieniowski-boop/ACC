"""Minimal smoke test to debug execution."""
import sys, json, urllib.request, urllib.error, traceback

out_lines = []
def log(msg):
    out_lines.append(msg)

try:
    sys.path.insert(0, "apps/api")
    log("step 1: import")
    from app.core.security import create_access_token
    log("step 2: token")
    token = create_access_token("smoke-test", "admin")
    log(f"step 3: token ok len={len(token)}")
    
    BASE = "http://localhost:8000/api/v1"
    headers = {"Authorization": f"Bearer {token}"}
    
    log("step 4: health check")
    r = urllib.request.Request(f"{BASE}/health", headers=headers)
    resp = urllib.request.urlopen(r, timeout=10)
    log(f"step 5: health={resp.status}")
    
    log("step 6: profitability overview")
    r = urllib.request.Request(f"{BASE}/profitability/overview?date_from=2026-02-01&date_to=2026-03-09", headers=headers)
    resp = urllib.request.urlopen(r, timeout=20)
    data = json.loads(resp.read())
    kpi = data.get("kpi", {})
    log(f"step 7: kpi keys={list(kpi.keys())}")
    log(f"step 8: ad_spend_share_pct={kpi.get('ad_spend_share_pct')}")
    
    log("DONE - all ok")
except Exception:
    log(f"EXCEPTION:\n{traceback.format_exc()}")

with open("_smoke_debug.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(out_lines))
