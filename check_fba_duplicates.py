"""
Investigate the 4 FBA_ SKU pairs causing 8801 (duplicate variation attrs) errors.
Check what product/attributes each SKU has.
"""
import asyncio
import json
import sys
sys.path.insert(0, r'C:\ACC\apps\api')

from app.connectors.amazon_sp_api.listings import ListingsClient

FR_MARKETPLACE = "A13V1IB3VIYZZH"

# The duplicate pairs from 8801 errors:
# FBA_5903699409312 conflicts with FBA_5902730382072
# FBA_5903699409336 conflicts with FBA_5902730382096
# FBA_5903699409343 conflicts with FBA_5902730382102
# FBA_5903699409329 conflicts with FBA_5902730382089

ALL_SKUS = [
    "FBA_5903699409312", "FBA_5902730382072",
    "FBA_5903699409336", "FBA_5902730382096",
    "FBA_5903699409343", "FBA_5902730382102",
    "FBA_5903699409329", "FBA_5902730382089",
]

async def main():
    fr = ListingsClient(marketplace_id=FR_MARKETPLACE)
    
    for sku in ALL_SKUS:
        print(f"\n=== {sku} ===")
        try:
            listing = await fr.get_listings_item(
                fr.seller_id, sku,
                included_data="summaries,attributes",
            )
            for s in listing.get("summaries", []):
                print(f"  ASIN: {s.get('asin')}")
                print(f"  Status: {s.get('status')}")
                print(f"  Title: {s.get('itemName', 'N/A')[:80]}")
            
            attrs = listing.get("attributes", {})
            color = attrs.get("color", [])
            size = attrs.get("size", [])
            color_name = attrs.get("color_name", [])
            size_name = attrs.get("size_name", [])
            parentage = attrs.get("parentage_level", [])
            parent_rel = attrs.get("child_parent_sku_relationship", [])
            
            print(f"  color: {json.dumps(color)}")
            print(f"  size: {json.dumps(size)}")
            print(f"  color_name: {json.dumps(color_name)}")
            print(f"  size_name: {json.dumps(size_name)}")
            print(f"  parentage_level: {json.dumps(parentage)}")
            if parent_rel:
                for pr in parent_rel:
                    print(f"  parent_sku: {pr.get('parent_sku')}")
        except Exception as e:
            print(f"  ERROR: {e}")

if __name__ == "__main__":
    asyncio.run(main())
