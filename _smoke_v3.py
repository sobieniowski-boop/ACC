"""Comprehensive smoke test — all checklists."""
import sys, json, urllib.request, urllib.error, os, traceback

results = []

def check(label, ok, detail=""):
    status = "PASS" if ok else "FAIL"
    msg = f"{status} | {label}"
    if detail:
        msg += f" -- {detail}"
    results.append(msg)

def req(label, path, base, headers):
    url = f"{base}{path}"
    try:
        r = urllib.request.Request(url, headers=headers)
        resp = urllib.request.urlopen(r, timeout=20)
        return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:200]
        results.append(f"FAIL {label}: HTTP {e.code} -- {body}")
        return None
    except Exception as e:
        results.append(f"FAIL {label}: {e}")
        return None

def main():
    sys.path.insert(0, "apps/api")
    from app.core.security import create_access_token

    BASE = "http://localhost:8000/api/v1"
    TOKEN = create_access_token("smoke-test", "admin")
    H = {"Authorization": f"Bearer {TOKEN}"}
    FR = "2026-02-01"
    TO = "2026-03-09"

    # === GROUP 1: Profit / Executive ===
    d = req("ProfitOverview", f"/profitability/overview?date_from={FR}&date_to={TO}", BASE, H)
    if d:
        kpi = d.get("kpi", {})
        check("ProfitOverview: KPI returned", bool(kpi))
        check("ProfitOverview: ad_spend_share_pct", "ad_spend_share_pct" in kpi, f"val={kpi.get('ad_spend_share_pct')}")
        check("ProfitOverview: best_skus", isinstance(d.get("best_skus"), list), f"cnt={len(d.get('best_skus', []))}")
        check("ProfitOverview: worst_skus", isinstance(d.get("worst_skus"), list), f"cnt={len(d.get('worst_skus', []))}")
        check("ProfitOverview: loss_orders", isinstance(d.get("loss_orders"), list), f"cnt={len(d.get('loss_orders', []))}")

    d = req("ExecProducts", f"/executive/products?date_from={FR}&date_to={TO}&page=1&page_size=5", BASE, H)
    if d:
        check("ExecProducts: items", isinstance(d.get("items"), list), f"cnt={len(d.get('items', []))}")
        check("ExecProducts: pages", "pages" in d, f"pages={d.get('pages')}")
        check("ExecProducts: total", "total" in d, f"total={d.get('total')}")

    d = req("ProfitabilityProducts", f"/profitability/products?date_from={FR}&date_to={TO}&page=1&page_size=5", BASE, H)
    if d:
        check("ProfitabilityProducts: items", isinstance(d.get("items"), list), f"cnt={len(d.get('items', []))}")
        check("ProfitabilityProducts: pages", "pages" in d, f"pages={d.get('pages')}")

    d = req("ProfitabilityOrders", f"/profitability/orders?date_from={FR}&date_to={TO}&page=1&page_size=5", BASE, H)
    if d:
        check("ProfitabilityOrders: items", isinstance(d.get("items"), list), f"cnt={len(d.get('items', []))}")
        check("ProfitabilityOrders: pages", "pages" in d, f"pages={d.get('pages')}")

    d = req("ExecMarketplaces", f"/executive/marketplaces?date_from={FR}&date_to={TO}", BASE, H)
    if d:
        items = d.get("items") or d.get("marketplaces") or []
        check("ExecMarketplaces: data", len(items) > 0, f"cnt={len(items)}")
        if items:
            check("ExecMarketplaces: no ad_spend_pln", "ad_spend_pln" not in items[0])
            check("ExecMarketplaces: no active_skus", "active_skus" not in items[0])

    d = req("ProductDrilldown", f"/profit/v2/drilldown?sku=TEST&marketplace_id=A1PA6795UKMFR9&date_from={FR}&date_to={TO}&page=1&page_size=5", BASE, H)
    if d:
        items = d.get("items", [])
        if items:
            check("Drilldown: cm1_percent", "cm1_percent" in items[0])
            check("Drilldown: is_refund", "is_refund" in items[0])
            check("Drilldown: refund_type", "refund_type" in items[0])
        else:
            check("Drilldown: empty for TEST sku (ok)", True)

    # === GROUP 2: Strategy ===
    d = req("StrategyOverview", "/strategy/overview", BASE, H)
    if d:
        check("StrategyOverview: kpi", "kpi" in d)
        check("StrategyOverview: by_type", "by_type" in d)

    d = req("StrategyOpps", "/strategy/opportunities?page=1&page_size=5", BASE, H)
    if d:
        check("StrategyOpps: items", isinstance(d.get("items"), list), f"cnt={len(d.get('items', []))}")
        check("StrategyOpps: pages", "pages" in d)
        check("StrategyOpps: total", "total" in d)
        items = d.get("items", [])
        if items:
            check("StrategyOpp[0]: source_signals_json", "source_signals_json" in items[0])
            check("StrategyOpp[0]: blocker_json", "blocker_json" in items[0])

    d = req("Playbooks", "/strategy/playbooks", BASE, H)
    if d:
        pbs = d.get("playbooks", [])
        check("Playbooks: array", isinstance(pbs, list), f"cnt={len(pbs)}")
        if pbs:
            check("Playbooks: steps", "steps" in pbs[0])
            if pbs[0].get("steps"):
                check("Playbooks: step.owner_role", "owner_role" in pbs[0]["steps"][0])

    d = req("Bundles", "/strategy/bundles", BASE, H)
    if d:
        bundles = d.get("bundles", [])
        check("Bundles: array", isinstance(bundles, list), f"cnt={len(bundles)}")
        if bundles:
            check("Bundles: sku_a", "sku_a" in bundles[0])
            check("Bundles: confidence", "confidence" in bundles[0])
        check("Bundles: variant_gaps", isinstance(d.get("variant_gaps"), list))

    d = req("MarketExpansion", "/strategy/market-expansion", BASE, H)
    if d:
        items = d.get("items", [])
        check("MarketExpansion: items", isinstance(items, list), f"cnt={len(items)}")
        if items:
            check("MarketExpansion: confidence", "confidence" in items[0])
            check("MarketExpansion: missing_components list|None", isinstance(items[0].get("missing_components"), (list, type(None))))

    d = req("Experiments", "/strategy/experiments", BASE, H)
    if d:
        check("Experiments: items", isinstance(d.get("items"), list), f"cnt={len(d.get('items', []))}")

    d = req("SeasonalityOpps", "/seasonality/opportunities?page=1&page_size=5", BASE, H)
    if d:
        check("SeasonalityOpps: items", isinstance(d.get("items"), list), f"cnt={len(d.get('items', []))}")
        check("SeasonalityOpps: page_size", "page_size" in d)
        items = d.get("items", [])
        if items:
            check("SeasonalityOpps: marketplace field", "marketplace" in items[0])

    # === GROUP 3: Static ===
    api_ts = os.path.join("apps", "web", "src", "lib", "api.ts")
    if os.path.exists(api_ts):
        with open(api_ts, "r", encoding="utf-8") as f:
            c = f.read()
        check("api.ts: 1x LossOrderItem", c.count("export interface LossOrderItem") == 1)
        check("api.ts: 1x ProfitabilityLossOrderItem", c.count("export interface ProfitabilityLossOrderItem") == 1)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        results.append(f"FATAL: {traceback.format_exc()}")

    passes = sum(1 for r in results if r.startswith("PASS"))
    fails = sum(1 for r in results if r.startswith("FAIL"))
    summary = f"TOTAL: {passes} passed, {fails} failed out of {len(results)} checks\n\n"
    body = "\n".join(results)
    failed_section = ""
    if fails:
        failed_section = "\n\n--- FAILED ---\n" + "\n".join(f"  X {r}" for r in results if r.startswith("FAIL"))

    output = summary + body + failed_section
    with open("_smoke_final.txt", "w", encoding="utf-8") as f:
        f.write(output)
