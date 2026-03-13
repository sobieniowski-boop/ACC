"""Test fixes: Profit Explorer and Fee Breakdown duplicates."""
import requests, json

BASE = "http://127.0.0.1:8000/api/v1"

# Test 1: Profit Explorer (was SQL 8124)
print("=== TEST 1: Profit Explorer ===")
try:
    r = requests.get(f"{BASE}/profit/orders", params={
        "date_from": "2026-02-09", "date_to": "2026-03-10",
        "page": 1, "page_size": 5,
    }, timeout=60)
    print(f"Status: {r.status_code}")
    if r.status_code == 200:
        d = r.json()
        print(f"Total: {d.get('total')}, Items: {len(d.get('items', []))}")
        if d.get("items"):
            item = d["items"][0]
            print(f"First: order={item.get('amazon_order_id')} rev={item.get('revenue_pln')} cm={item.get('contribution_margin_pln')}")
    else:
        print(f"Error: {r.text[:500]}")
except Exception as e:
    print(f"EXCEPTION: {e}")

# Test 2: Fee Breakdown duplicates
print("\n=== TEST 2: Fee Breakdown Duplicates ===")
try:
    r = requests.get(f"{BASE}/profit/v2/fee-breakdown", params={
        "date_from": "2026-02-09", "date_to": "2026-03-10",
    }, timeout=60)
    if r.status_code == 200:
        d = r.json()
        lines = d.get("lines", [])
        charge_types = [l.get("charge_type") for l in lines if l.get("line_type") != "subtotal"]
        dupes = [ct for ct in charge_types if charge_types.count(ct) > 1]
        if dupes:
            print(f"STILL DUPLICATES: {set(dupes)}")
            for l in lines:
                if l.get("charge_type") in dupes and l.get("line_type") != "subtotal":
                    print(f"  {l['charge_type']:30s} | {l['amount_pln']:>12.2f} | cnt={l['txn_count']:>5d}")
        else:
            print("NO DUPLICATES - all charge_types are unique!")

        # Print summary
        summary = d.get("summary", {})
        print(f"Summary: Rev={summary.get('revenue_pln')}, CM1={summary.get('cm1_pln')}, CM2={summary.get('cm2_pln')}, NP={summary.get('np_pln')}")
        print(f"Total lines: {len(lines)} (incl. subtotals)")
    else:
        print(f"Error: {r.text[:500]}")
except Exception as e:
    print(f"EXCEPTION: {e}")

# Test 3: Product Profit Table
print("\n=== TEST 3: Product Profit Table ===")
try:
    import time
    t0 = time.time()
    r = requests.get(f"{BASE}/profit/v2/products", params={
        "date_from": "2026-02-09", "date_to": "2026-03-10",
        "page": 1, "page_size": 5,
        "profit_mode": "cm1", "group_by": "asin_marketplace",
    }, timeout=120)
    elapsed = time.time() - t0
    print(f"Status: {r.status_code} in {elapsed:.1f}s")
    if r.status_code == 200:
        d = r.json()
        print(f"Total: {d.get('total')}, Items: {len(d.get('items', []))}")
    else:
        print(f"Error: {r.text[:300]}")
except Exception as e:
    print(f"EXCEPTION: {e}")

print("\nDONE")
