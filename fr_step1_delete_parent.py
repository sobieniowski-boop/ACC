"""
KROK 1: Usuń stary parent 7P-HO4I-IM4E z FR marketplace.
         Po usunięciu ~100 dzieci osieroci.
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, r'C:\ACC\apps\api')
os.environ['PYTHONPATH'] = r'C:\ACC\apps\api'

from app.connectors.amazon_sp_api.listings import ListingsClient

FR_MARKETPLACE = "A13V1IB3VIYZZH"
OLD_PARENT_SKU = "7P-HO4I-IM4E"


async def main():
    client = ListingsClient(marketplace_id=FR_MARKETPLACE)

    print(f"=== KROK 1: Usunięcie starego parenta ===")
    print(f"SKU: {OLD_PARENT_SKU}")
    print(f"Marketplace: FR ({FR_MARKETPLACE})")
    print()

    # Confirm parent exists
    try:
        listing = await client.get_listings_item(
            client.seller_id, OLD_PARENT_SKU,
            included_data="summaries,attributes",
        )
        parentage = listing.get("attributes", {}).get("parentage_level", [])
        print(f"Parent exists. parentage_level = {json.dumps(parentage)}")
    except Exception as e:
        print(f"❌ Cannot fetch parent: {e}")
        return

    print()
    print(">>> Deleting parent...")
    result = await client.delete_listings_item(
        client.seller_id, OLD_PARENT_SKU,
    )
    print(f"Delete result: {json.dumps(result, indent=2)}")

    status = result.get("status", "UNKNOWN")
    if status == "ACCEPTED":
        print("\n✅ Parent deleted successfully. Children are now orphans.")
    else:
        print(f"\n⚠️  Delete status: {status}")
        if result.get("issues"):
            for i in result["issues"]:
                print(f"   - {i.get('code')}: {i.get('message')}")


if __name__ == "__main__":
    asyncio.run(main())
