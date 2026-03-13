"""Claim & delete ghost catalog parents.

Ghost parents (no seller SKU, only catalog-level):
  - B08KH6GCMW (Acier line, 9 children)
  - B08KKTKHHG (Silicone/Beige line, 9 children)

Strategy: PUT a temp seller SKU with merchant_suggested_asin → DELETE it.
"""
import asyncio, json, os, sys
sys.path.insert(0, r'C:\ACC\apps\api')
os.environ['PYTHONPATH'] = r'C:\ACC\apps\api'

from app.connectors.amazon_sp_api.listings import ListingsClient

FR_MP = "A13V1IB3VIYZZH"
PRODUCT_TYPE = "CONTAINER_LID"

GHOSTS = [
    {"asin": "B08KH6GCMW", "temp_sku": "GHOST-DEL-B08KH6GCMW", "label": "Acier parent"},
    {"asin": "B08KKTKHHG", "temp_sku": "GHOST-DEL-B08KKTKHHG", "label": "Silicone parent"},
]


async def main():
    client = ListingsClient(marketplace_id=FR_MP)
    sid = client.seller_id

    for g in GHOSTS:
        print(f"\n{'='*60}")
        print(f"GHOST: {g['asin']} ({g['label']})")
        print(f"Temp SKU: {g['temp_sku']}")

        # Step 1: PUT — claim the ASIN
        put_body = {
            "productType": PRODUCT_TYPE,
            "requirements": "LISTING_OFFER_ONLY",
            "attributes": {
                "merchant_suggested_asin": [{
                    "value": g["asin"],
                    "marketplace_id": FR_MP,
                }],
                "condition_type": [{
                    "value": "new_new",
                    "marketplace_id": FR_MP,
                }],
            },
        }

        print(f"\n>>> PUT (claim ASIN)...")
        try:
            put_result = await client.put_listings_item(sid, g["temp_sku"], put_body)
            put_status = put_result.get("status", "UNKNOWN")
            print(f"    Status: {put_status}")
            if put_result.get("issues"):
                for iss in put_result["issues"][:5]:
                    print(f"    [{iss.get('severity')}] {iss.get('code')}: {iss.get('message','')[:150]}")
        except Exception as e:
            print(f"    ERROR: {e}")
            continue

        # Small pause for Amazon to process
        await asyncio.sleep(2)

        # Step 2: DELETE — kill the ghost
        print(f"\n>>> DELETE (kill ghost)...")
        try:
            del_result = await client.delete_listings_item(sid, g["temp_sku"])
            del_status = del_result.get("status", "UNKNOWN")
            print(f"    Status: {del_status}")
            if del_result.get("issues"):
                for iss in del_result["issues"][:5]:
                    print(f"    [{iss.get('severity')}] {iss.get('code')}: {iss.get('message','')[:150]}")
        except Exception as e:
            print(f"    ERROR: {e}")

        await asyncio.sleep(1)

    # Verify: check parent issues after
    print(f"\n{'='*60}")
    print("Waiting 5s then checking new parent issues...")
    await asyncio.sleep(5)

    parent = await client.get_listings_item(
        sid, "FR-PARENT-1367-CEA8F738", included_data="summaries,issues",
    )
    issues = parent.get("issues", [])
    from collections import Counter
    codes = Counter(i.get("code") for i in issues)
    summaries = parent.get("summaries", [{}])
    status = summaries[0].get("status", []) if summaries else []
    print(f"Parent status: {status}")
    print(f"Total issues: {len(issues)}")
    for code, cnt in codes.most_common():
        print(f"  {code}: {cnt}x")


asyncio.run(main())
