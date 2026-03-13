"""
Sprawdzenie struktury na DE parentie
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, r'C:\ACC\apps\api')
os.environ['PYTHONPATH'] = r'C:\ACC\apps\api'

from app.connectors.amazon_sp_api.listings import ListingsClient
from app.core.db_connection import connect_acc

async def main():
    # Try the same SKU as FR parent
    de_parent_sku = "7P-HO4I-IM4E"
    
    print(f"Checking DE Parent: SKU={de_parent_sku}\n")
    
    # Fetch from API
    client = ListingsClient(marketplace_id="A1PA6795UKMFR9")
    
    listing = await client.get_listings_item(
        seller_id=client.seller_id,
        sku=de_parent_sku
    )
    
    attrs = listing.get("attributes", {})
    rel = attrs.get("child_parent_sku_relationship", [])
    parentage = attrs.get("parentage_level", [])
    theme = attrs.get("variation_theme", [])
    
    print("=== DE PARENT child_parent_sku_relationship ===")
    print(json.dumps(rel, indent=2, ensure_ascii=False))
    
    print("\n=== DE PARENT parentage_level ===")
    print(json.dumps(parentage, indent=2, ensure_ascii=False))
    
    print("\n=== DE PARENT variation_theme ===")
    print(json.dumps(theme, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(main())
