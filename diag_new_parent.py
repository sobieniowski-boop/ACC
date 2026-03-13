"""Deep diagnosis: check all issues on new parent + check if children have issues."""
import asyncio, json, os, sys
from collections import Counter
sys.path.insert(0, r'C:\ACC\apps\api')
os.environ['PYTHONPATH'] = r'C:\ACC\apps\api'

from app.connectors.amazon_sp_api.listings import ListingsClient

FR_MP = "A13V1IB3VIYZZH"
NEW_PARENT_SKU = "FR-PARENT-1367-CEA8F738"


async def main():
    client = ListingsClient(marketplace_id=FR_MP)
    sid = client.seller_id

    # 1. Full parent dump with issues
    print("=== PARENT FULL ISSUES ===")
    parent = await client.get_listings_item(
        sid, NEW_PARENT_SKU, included_data="summaries,attributes,issues",
    )
    
    summaries = parent.get("summaries", [{}])
    if summaries:
        s = summaries[0]
        print(f"ASIN: {s.get('asin')}")
        print(f"Status: {s.get('status', [])}")
        print(f"Condition: {s.get('conditionType')}")
        print(f"Product type: {s.get('productType')}")

    issues = parent.get("issues", [])
    print(f"\nTotal issues: {len(issues)}")
    
    # Group by code
    codes = Counter(i.get("code") for i in issues)
    print("\nBy code:")
    for code, cnt in codes.most_common():
        print(f"  {code}: {cnt}x")
    
    # Show unique messages per code
    print("\nUnique messages per code:")
    seen = set()
    for iss in issues:
        code = iss.get("code")
        msg = iss.get("message", "")
        severity = iss.get("severity", "")
        key = f"{code}|{msg[:80]}"
        if key not in seen:
            seen.add(key)
            print(f"  [{severity}] {code}: {msg[:200]}")
            attr_names = iss.get("attributeNames", [])
            if attr_names:
                print(f"    attributeNames: {attr_names}")

    # 2. Check a few children's issues
    print("\n=== SAMPLE CHILD ISSUES ===")
    sample = [
        "FBA_5903699409312",  # Acier 14
        "FBA_5902730382072",  # Plastique 14
        "FBA_5903699409480",  # Beige 30
    ]
    for sku in sample:
        child = await client.get_listings_item(
            sid, sku, included_data="summaries,issues",
        )
        s = (child.get("summaries") or [{}])[0]
        child_issues = child.get("issues", [])
        status = s.get("status", [])
        print(f"\n{sku}: status={status}, issues={len(child_issues)}")
        for iss in child_issues[:5]:
            print(f"  [{iss.get('severity')}] {iss.get('code')}: {iss.get('message','')[:150]}")


asyncio.run(main())
