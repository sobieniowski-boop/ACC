"""
Sprawdzenie struktury child_parent_sku_relationship na parentie FR
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
    
    client = ListingsClient(marketplace_id=marketplace_id)
    
    print(f"Fetching parent SKU: {parent_sku} on {marketplace_id}\n")
    
    listing = await client.get_listings_item(
        seller_id=client.seller_id,
        sku=parent_sku
    )
    
    # Extract child_parent_sku_relationship
    attrs = listing.get("attributes", {})
    rel = attrs.get("child_parent_sku_relationship", [])
    
    print("=== CURRENT child_parent_sku_relationship ===")
    print(json.dumps(rel, indent=2, ensure_ascii=False))
    
    # Check parentage_level
    parentage = attrs.get("parentage_level", [])
    print("\n=== parentage_level ===")
    print(json.dumps(parentage, indent=2, ensure_ascii=False))
    
    # Check variation_theme
    theme = attrs.get("variation_theme", [])
    print("\n=== variation_theme ===")
    print(json.dumps(theme, indent=2, ensure_ascii=False))
    
    print("\n=== DIAGNOSIS ===")
    if rel:
        rel_item = rel[0] if isinstance(rel, list) else rel
        if "child_relationship_type" in json.dumps(rel_item):
            print("❌ PROBLEM: Parent has 'child_relationship_type' instead of 'parent_relationship_type'")
            print("   This is non-standard and may affect UX in Seller Central")
        elif "parent_relationship_type" in json.dumps(rel_item):
            print("✅ OK: Parent has correct 'parent_relationship_type'")
        else:
            print("⚠️  UNKNOWN: No relationship type found")
    else:
        print("⚠️  No child_parent_sku_relationship found")

if __name__ == "__main__":
    asyncio.run(main())
