"""Comprehensive smoke test — all checklists from recent cleanups."""
import sys, json, urllib.request, urllib.error, os, traceback

sys.path.insert(0, "apps/api")
from app.core.security import create_access_token

BASE = "http://localhost:8000/api/v1"
TOKEN = create_access_token("smoke-test", "admin")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
FROM = "2026-02-01"
TO = "2026-03-09"
OUT = "c:\\ACC\\_smoke_final.txt"

results = []

def req(label, path):
    url = f"{BASE}{path}"
    try:
        r = urllib.request.Request(url, headers=HEADERS)
        resp = urllib.request.urlopen(r, timeout=20)
        return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:200]
        results.append(f"FAIL {label}: HTTP {e.code} — {body}")
        return None
    except Exception as e:
        results.append(f"FAIL {label}: {e}")
        return None

def check(label, ok, detail=""):
    status = "PASS" if ok else "FAIL"
    msg = f"{status} | {label}"
    if detail:
        msg += f" — {detail}"
    results.append(msg)

try:
    # ===== GROUP 1: Profit / Executive =====
    d = req("ProfitOverview", f"/profitability/overview?date_from={FROM}&date_to={TO}")
    if d:
        kpi = d.get("kpi", {})
        check("ProfitOverview: KPI returned", bool(kpi))
        check("ProfitOverview: ad_spend_share_pct exists", "ad_spend_share_pct" in kpi, f"val={kpi.get('ad_spend_share_pct')}")
        check("ProfitOverview: best_skus array", isinstance(d.get("best_skus"), list), f"cnt={len(d.get('best_skus', []))}")
        check("ProfitOverview: worst_skus array", isinstance(d.get("worst_skus"), list), f"cnt={len(d.get('worst_skus', []))}")
        check("ProfitOverview: loss_orders array", isinstance(d.get("loss_orders"), list), f"cnt={len(d.get('loss_orders', []))}")

    d = req("ExecProducts", f"/executive/products?date_from={FROM}&date_to={TO}&page=1&page_size=5")
    if d:
        check("ExecProducts: items", isinstance(d.get("items"), list), f"cnt={len(d.get('items', []))}")
        check("ExecProducts: pages", "pages" in d, f"pages={d.get('pages')}")
        check("ExecProducts: total", "total" in d, f"total={d.get('total')}")

    d = req("ProfitabilityProducts", f"/profitability/products?date_from={FROM}&date_to={TO}&page=1&page_size=5")
    if d:
        check("ProfitabilityProducts: items", isinstance(d.get("items"), list), f"cnt={len(d.get('items', []))}")
        check("ProfitabilityProducts: pages", "pages" in d, f"pages={d.get('pages')}")

    d = req("ProfitabilityOrders", f"/profitability/orders?date_from={FROM}&date_to={TO}&page=1&page_size=5")
    if d:
        check("ProfitabilityOrders: items", isinstance(d.get("items"), list), f"cnt={len(d.get('items', []))}")
        check("ProfitabilityOrders: pages", "pages" in d, f"pages={d.get('pages')}")

    d = req("ExecMarketplaces", f"/executive/marketplaces?date_from={FROM}&date_to={TO}")
    if d:
        items = d.get("items") or d.get("marketplaces") or []
        check("ExecMarketplaces: data returned", len(items) > 0, f"cnt={len(items)}")
        if items:
            f0 = items[0]
            check("ExecMarketplaces: no ad_spend_pln phantom", "ad_spend_pln" not in f0)
            check("ExecMarketplaces: no active_skus phantom", "active_skus" not in f0)

    d = req("ProductDrilldown", f"/profit/v2/drilldown?sku=TEST&marketplace_id=A1PA6795UKMFR9&date_from={FROM}&date_to={TO}&page=1&page_size=5")
    if d:
        items = d.get("items", [])
        if items:
            check("Drilldown: cm1_percent field", "cm1_percent" in items[0])
            check("Drilldown: is_refund field", "is_refund" in items[0])
            check("Drilldown: refund_type field", "refund_type" in items[0])
        else:
            check("Drilldown: empty for TEST sku (expected)", True)

    # ===== GROUP 2: Strategy =====
    d = req("StrategyOverview", "/strategy/overview")
    if d:
        check("StrategyOverview: kpi", "kpi" in d)
        check("StrategyOverview: by_type", "by_type" in d)

    d = req("StrategyOpportunities", "/strategy/opportunities?page=1&page_size=5")
    if d:
        check("StrategyOpps: items", isinstance(d.get("items"), list), f"cnt={len(d.get('items', []))}")
        check("StrategyOpps: pages", "pages" in d, f"pages={d.get('pages')}")
        check("StrategyOpps: total", "total" in d, f"total={d.get('total')}")
        items = d.get("items", [])
        if items:
            opp = items[0]
            check("StrategyOpp: source_signals_json", "source_signals_json" in opp)
            check("StrategyOpp: blocker_json", "blocker_json" in opp)

    d = req("StrategyPlaybooks", "/strategy/playbooks")
    if d:
        pbs = d.get("playbooks", [])
        check("Playbooks: array", isinstance(pbs, list), f"cnt={len(pbs)}")
        if pbs:
            check("Playbooks: steps", "steps" in pbs[0])
            if pbs[0].get("steps"):
                check("Playbooks: step.owner_role", "owner_role" in pbs[0]["steps"][0])

    d = req("StrategyBundles", "/strategy/bundles")
    if d:
        bundles = d.get("bundles", [])
        check("Bundles: array", isinstance(bundles, list), f"cnt={len(bundles)}")
        if bundles:
            check("Bundles: sku_a", "sku_a" in bundles[0])
            check("Bundles: confidence", "confidence" in bundles[0])
        check("Bundles: variant_gaps", isinstance(d.get("variant_gaps"), list), f"cnt={len(d.get('variant_gaps', []))}")

    d = req("MarketExpansion", "/strategy/market-expansion")
    if d:
        items = d.get("items", [])
        check("MarketExpansion: items", isinstance(items, list), f"cnt={len(items)}")
        if items:
            check("MarketExpansion: confidence", "confidence" in items[0])
            check("MarketExpansion: missing_components is list", isinstance(items[0].get("missing_components"), (list, type(None))))

    d = req("StrategyExperiments", "/strategy/experiments")
    if d:
        check("Experiments: items", isinstance(d.get("items"), list), f"cnt={len(d.get('items', []))}")

    d = req("SeasonalityOpps", "/seasonality/opportunities?page=1&page_size=5")
    if d:
        check("SeasonalityOpps: items", isinstance(d.get("items"), list), f"cnt={len(d.get('items', []))}")
        check("SeasonalityOpps: page_size in response", "page_size" in d, f"val={d.get('page_size')}")
        items = d.get("items", [])
        if items:
            check("SeasonalityOpps: marketplace field", "marketplace" in items[0])

    # ===== GROUP 3: Static checks =====
    api_ts = os.path.join("apps", "web", "src", "lib", "api.ts")
    if os.path.exists(api_ts):
        with open(api_ts, "r", encoding="utf-8") as f:
            content = f.read()
        check("api.ts: 1x LossOrderItem", content.count("export interface LossOrderItem") == 1, f"found {content.count('export interface LossOrderItem')}")
        check("api.ts: 1x ProfitabilityLossOrderItem", content.count("export interface ProfitabilityLossOrderItem") == 1, f"found {content.count('export interface ProfitabilityLossOrderItem')}")

except Exception:
    results.append(f"EXCEPTION: {traceback.format_exc()}")

# Write results
with open(OUT, "w", encoding="utf-8") as f:
    passes = sum(1 for r in results if r.startswith("PASS"))
    fails = sum(1 for r in results if r.startswith("FAIL"))
    f.write(f"TOTAL: {passes} passed, {fails} failed out of {len(results)} checks\n\n")
    for r in results:
        f.write(r + "\n")
    if fails:
        f.write("\n--- FAILED ---\n")
        for r in results:
            if r.startswith("FAIL"):
                f.write(f"  X {r}\n")
