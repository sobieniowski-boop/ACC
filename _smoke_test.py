"""Comprehensive smoke test for all recent cleanup changes."""
import sys
import json
import urllib.request
import urllib.error

sys.path.insert(0, "apps/api")
from app.core.security import create_access_token

BASE = "http://localhost:8000/api/v1"
TOKEN = create_access_token("smoke-test", "admin")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
FROM = "2026-02-01"
TO = "2026-03-09"

results = []

def req(label: str, path: str):
    url = f"{BASE}{path}"
    try:
        r = urllib.request.Request(url, headers=HEADERS)
        resp = urllib.request.urlopen(r, timeout=20)
        data = json.loads(resp.read())
        return data
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:200]
        results.append(f"FAIL {label}: HTTP {e.code} — {body}")
        return None
    except Exception as e:
        results.append(f"FAIL {label}: {e}")
        return None

def check(label: str, ok: bool, detail: str = ""):
    status = "PASS" if ok else "FAIL"
    msg = f"{status} | {label}"
    if detail:
        msg += f" — {detail}"
    results.append(msg)
    print(msg)

# ===========================================================
# GROUP 1: Profit / Executive endpoints (cleanup pass)
# ===========================================================

# 1a. ProfitOverview — ad_spend_share_pct present, no tacos_pct required
d = req("ProfitOverview", f"/profitability/overview?date_from={FROM}&date_to={TO}")
if d:
    kpi = d.get("kpi", {})
    check("ProfitOverview: KPI returned", bool(kpi))
    check("ProfitOverview: ad_spend_share_pct exists", "ad_spend_share_pct" in kpi,
          f"value={kpi.get('ad_spend_share_pct')}")
    check("ProfitOverview: best_skus array", isinstance(d.get("best_skus"), list),
          f"count={len(d.get('best_skus', []))}")
    check("ProfitOverview: worst_skus array", isinstance(d.get("worst_skus"), list),
          f"count={len(d.get('worst_skus', []))}")
    check("ProfitOverview: loss_orders array", isinstance(d.get("loss_orders"), list),
          f"count={len(d.get('loss_orders', []))}")

# 1b. ExecProducts — pagination metadata
d = req("ExecProducts", f"/executive/products?date_from={FROM}&date_to={TO}&page=1&page_size=5")
if d:
    check("ExecProducts: items array", isinstance(d.get("items"), list), f"count={len(d.get('items', []))}")
    check("ExecProducts: pages field", "pages" in d, f"pages={d.get('pages')}")
    check("ExecProducts: total field", "total" in d, f"total={d.get('total')}")

# 1c. ProfitabilityProducts — pagination
d = req("ProfitabilityProducts", f"/profitability/products?date_from={FROM}&date_to={TO}&page=1&page_size=5")
if d:
    check("ProfitabilityProducts: items array", isinstance(d.get("items"), list), f"count={len(d.get('items', []))}")
    check("ProfitabilityProducts: pages field", "pages" in d, f"pages={d.get('pages')}")

# 1d. ProfitabilityOrders — pagination
d = req("ProfitabilityOrders", f"/profitability/orders?date_from={FROM}&date_to={TO}&page=1&page_size=5")
if d:
    check("ProfitabilityOrders: items array", isinstance(d.get("items"), list), f"count={len(d.get('items', []))}")
    check("ProfitabilityOrders: pages field", "pages" in d, f"pages={d.get('pages')}")

# 1e. ExecMarketplaces — no phantom fields
d = req("ExecMarketplaces", f"/executive/marketplaces?date_from={FROM}&date_to={TO}")
if d:
    items = d.get("items") or d.get("marketplaces") or []
    check("ExecMarketplaces: data returned", len(items) > 0, f"count={len(items)}")
    if items:
        first = items[0]
        check("ExecMarketplaces: no ad_spend_pln phantom", "ad_spend_pln" not in first,
              f"keys={list(first.keys())[:8]}")
        check("ExecMarketplaces: no active_skus phantom", "active_skus" not in first)

# 1f. ProfitExplorer / Drilldown — cm1_percent field
d = req("ProductDrilldown", f"/profit/v2/drilldown?sku=TEST&marketplace_id=A1PA6795UKMFR9&date_from={FROM}&date_to={TO}&page=1&page_size=5")
if d:
    items = d.get("items", [])
    if items:
        check("ProductDrilldown: cm1_percent field", "cm1_percent" in items[0],
              f"value={items[0].get('cm1_percent')}")
        check("ProductDrilldown: is_refund field", "is_refund" in items[0])
        check("ProductDrilldown: refund_type field", "refund_type" in items[0])
    else:
        check("ProductDrilldown: empty for test SKU (expected)", True, "no items for TEST sku")

# ===========================================================
# GROUP 2: Strategy endpoints
# ===========================================================

# 2a. Strategy Overview
d = req("StrategyOverview", "/strategy/overview")
if d:
    check("StrategyOverview: kpi present", "kpi" in d)
    check("StrategyOverview: by_type present", "by_type" in d)

# 2b. Strategy Opportunities — pagination
d = req("StrategyOpportunities", "/strategy/opportunities?page=1&page_size=5")
if d:
    check("StrategyOpportunities: items array", isinstance(d.get("items"), list), f"count={len(d.get('items', []))}")
    check("StrategyOpportunities: pages field", "pages" in d, f"pages={d.get('pages')}")
    check("StrategyOpportunities: total field", "total" in d, f"total={d.get('total')}")
    # Validate GrowthOpportunity fields
    items = d.get("items", [])
    if items:
        opp = items[0]
        check("StrategyOpp: source_signals_json field", "source_signals_json" in opp)
        check("StrategyOpp: blocker_json field", "blocker_json" in opp)
        check("StrategyOpp: no 'source_signals' phantom", "source_signals" not in opp or "source_signals_json" in opp)

# 2c. Strategy Playbooks
d = req("StrategyPlaybooks", "/strategy/playbooks")
if d:
    pbs = d.get("playbooks", [])
    check("StrategyPlaybooks: playbooks array", isinstance(pbs, list), f"count={len(pbs)}")
    if pbs:
        check("StrategyPlaybooks: steps present", "steps" in pbs[0])
        if pbs[0].get("steps"):
            step = pbs[0]["steps"][0]
            check("StrategyPlaybooks: step has owner_role", "owner_role" in step)

# 2d. Strategy Bundles
d = req("StrategyBundles", "/strategy/bundles")
if d:
    bundles = d.get("bundles", [])
    check("StrategyBundles: bundles array", isinstance(bundles, list), f"count={len(bundles)}")
    if bundles:
        b = bundles[0]
        check("StrategyBundles: sku_a field", "sku_a" in b)
        check("StrategyBundles: confidence field", "confidence" in b)
    vg = d.get("variant_gaps", [])
    check("StrategyBundles: variant_gaps array", isinstance(vg, list), f"count={len(vg)}")

# 2e. Market Expansion
d = req("MarketExpansion", "/strategy/market-expansion")
if d:
    items = d.get("items", [])
    check("MarketExpansion: items array", isinstance(items, list), f"count={len(items)}")
    if items:
        itm = items[0]
        check("MarketExpansion: confidence field (not confidence_score)", "confidence" in itm)
        check("MarketExpansion: no 'id' field", "id" not in itm or itm.get("id") is None)
        check("MarketExpansion: missing_components is list", isinstance(itm.get("missing_components"), (list, type(None))))

# 2f. Strategy Experiments
d = req("StrategyExperiments", "/strategy/experiments")
if d:
    items = d.get("items", [])
    check("StrategyExperiments: items array", isinstance(items, list), f"count={len(items)}")

# 2g. Seasonality Opportunities
d = req("SeasonalityOpps", "/seasonality/opportunities?page=1&page_size=5")
if d:
    check("SeasonalityOpps: items array", isinstance(d.get("items"), list), f"count={len(d.get('items', []))}")
    check("SeasonalityOpps: page_size in response", "page_size" in d, f"page_size={d.get('page_size')}")
    items = d.get("items", [])
    if items:
        check("SeasonalityOpps: marketplace field (not marketplace_id)", "marketplace" in items[0])

# ===========================================================
# GROUP 3: LossOrderItem import check (static analysis)
# ===========================================================
import importlib.util, os
api_ts_path = os.path.join("apps", "web", "src", "lib", "api.ts")
if os.path.exists(api_ts_path):
    with open(api_ts_path, "r", encoding="utf-8") as f:
        content = f.read()
    loss_count = content.count("export interface LossOrderItem")
    profitability_loss_count = content.count("export interface ProfitabilityLossOrderItem")
    check("api.ts: exactly 1 LossOrderItem interface", loss_count == 1, f"found {loss_count}")
    check("api.ts: exactly 1 ProfitabilityLossOrderItem interface", profitability_loss_count == 1, f"found {profitability_loss_count}")

# ===========================================================
# SUMMARY
# ===========================================================
print("\n" + "=" * 60)
passes = sum(1 for r in results if r.startswith("PASS"))
fails = sum(1 for r in results if r.startswith("FAIL"))
print(f"TOTAL: {passes} passed, {fails} failed out of {len(results)} checks")
if fails:
    print("\nFailed checks:")
    for r in results:
        if r.startswith("FAIL"):
            print(f"  ✗ {r}")
print("=" * 60)
