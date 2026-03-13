"""
Naprawa parent_relationship_type na parentie FR
"""
import asyncio
import json
import os
import sys

# Set PYTHONPATH
sys.path.insert(0, r'C:\ACC\apps\api')
os.environ['PYTHONPATH'] = r'C:\ACC\apps\api'

from app.connectors.amazon_sp_api.listings import ListingsClient

async def main():
    marketplace_id = "A13V1IB3VIYZZH"  # FR
    parent_sku = "7P-HO4I-IM4E"
    product_type = "CONTAINER_LID"
    
    client = ListingsClient(marketplace_id=marketplace_id)
    
    print(f"Fixing parent relationship type for SKU: {parent_sku} on {marketplace_id}\n")
    
    # Correct structures for parent
    patches = [
        {
            "op": "replace",
            "path": "/attributes/parentage_level",
            "value": [
                {
                    "marketplace_id": marketplace_id,
                    "value": "parent"
                }
            ]
        },
        {
            "op": "replace",
            "path": "/attributes/child_parent_sku_relationship",
            "value": [
                {
                    "marketplace_id": marketplace_id,
                    "parent_relationship_type": "variation"
                }
            ]
        },
        {
            "op": "replace",
            "path": "/attributes/variation_theme",
            "value": [
                {
                    "name": "COLOR/SIZE"
                }
            ]
        }
    ]
    
    print("=== PATCH PAYLOAD ===")
    print(json.dumps(patches, indent=2, ensure_ascii=False))
    
    print("\n=== Executing PATCH ===")
    result = await client.patch_listings_item(
        seller_id=client.seller_id,
        sku=parent_sku,
        patches=patches,
        product_type=product_type
    )
    
    print("\n=== PATCH RESULT ===")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    
    status = result.get("status")
    if status == "ACCEPTED":
        print("\n✅ SUCCESS: Parent relationship type corrected")
    elif status == "INVALID":
        print("\n❌ FAILED: Patch was rejected")
        issues = result.get("issues", [])
        for issue in issues:
            print(f"   - {issue.get('code')}: {issue.get('message')}")
    else:
        print(f"\n⚠️  UNKNOWN STATUS: {status}")

if __name__ == "__main__":
    asyncio.run(main())
