"""
Check how the "Acier" (steel handle) products look on DE vs FR.
Were they in separate families on DE? What variation attrs did they have originally?
"""
import asyncio
import json
import sys
sys.path.insert(0, r'C:\ACC\apps\api')

from app.connectors.amazon_sp_api.listings import ListingsClient
from app.connectors.amazon_sp_api.catalog import CatalogClient
from app.core.db_connection import connect_acc

DE_MARKETPLACE = "A1PA6795UKMFR9"
FR_MARKETPLACE = "A13V1IB3VIYZZH"

# Acier (steel handle) children ASINs - from catalog parent B08KH6GCMW
ACIER_ASINS = ["B08JVL9DVD", "B08JVJSP2L", "B08JVKSN2M", "B08JVNDCFK",
               "B08JVKH5B5", "B08JVKSH4G", "B08JVKZMCX", "B08JVMF8CD", "B08JVN8BT5"]

# Standard children ASINs - their conflicting counterparts
STANDARD_ASINS = ["B07YC4RQ9N", "B07YC4Z6QY", "B07YC4PGQW", "B07YC444C8"]

async def main():
    de_list = ListingsClient(marketplace_id=DE_MARKETPLACE)
    de_cat = CatalogClient(marketplace_id=DE_MARKETPLACE)
    
    # 1. Check catalog parents on DE
    print("=== CATALOG: ACIER PARENT B08KH6GCMW on DE ===")
    try:
        item = await de_cat.get_item("B08KH6GCMW", included_data="summaries,relationships")
        for s in item.get("summaries", []):
            print(f"  Name: {s.get('itemName', 'N/A')[:100]}")
        for r in item.get("relationships", []):
            for mr in r.get("relationships", []):
                print(f"  Type: {mr.get('type')}, children: {len(mr.get('childAsins', []))}")
                if mr.get("childAsins"):
                    print(f"  childAsins: {mr['childAsins'][:5]}")
    except Exception as e:
        print(f"  Error: {e}")

    print("\n=== CATALOG: STANDARD PARENT B07YL989KJ on DE ===")
    try:
        item = await de_cat.get_item("B07YL989KJ", included_data="summaries,relationships")
        for s in item.get("summaries", []):
            print(f"  Name: {s.get('itemName', 'N/A')[:100]}")
        for r in item.get("relationships", []):
            for mr in r.get("relationships", []):
                print(f"  Type: {mr.get('type')}, children: {len(mr.get('childAsins', []))}")
    except Exception as e:
        print(f"  Error: {e}")

    # 2. Check DE seller-level listing attributes for Acier children
    print("\n=== DE LISTING ATTRIBUTES: ACIER CHILDREN ===")
    conn = connect_acc(autocommit=True)
    cur = conn.cursor()
    
    # Find DE SKUs for acier ASINs
    for asin in ACIER_ASINS[:4]:  # Just the 4 conflicting ones
        cur.execute(
            "SELECT merchant_sku FROM dbo.acc_amazon_listing_registry WHERE asin = ?",
            [asin]
        )
        rows = cur.fetchall()
        de_sku = rows[0][0] if rows else None
        print(f"\n  ASIN: {asin}, DE SKU: {de_sku}")
        
        if de_sku:
            try:
                listing = await de_list.get_listings_item(
                    de_list.seller_id, de_sku,
                    included_data="summaries,attributes",
                )
                attrs = listing.get("attributes", {})
                print(f"    color: {json.dumps(attrs.get('color', []))}")
                print(f"    size: {json.dumps(attrs.get('size', []))}")
                print(f"    color_name: {json.dumps(attrs.get('color_name', []))}")
                print(f"    size_name: {json.dumps(attrs.get('size_name', []))}")
                parent_rel = attrs.get("child_parent_sku_relationship", [])
                if parent_rel:
                    print(f"    parent_sku: {parent_rel[0].get('parent_sku')}")
                for s in listing.get("summaries", []):
                    print(f"    Title: {s.get('itemName', '')[:80]}")
            except Exception as e:
                print(f"    Listing error: {e}")

    # 3. Check DE seller-level listing attributes for standard children 
    print("\n=== DE LISTING ATTRIBUTES: STANDARD CHILDREN (counterparts) ===")
    for asin in STANDARD_ASINS:
        cur.execute(
            "SELECT merchant_sku FROM dbo.acc_amazon_listing_registry WHERE asin = ?",
            [asin]
        )
        rows = cur.fetchall()
        de_sku = rows[0][0] if rows else None
        print(f"\n  ASIN: {asin}, DE SKU: {de_sku}")
        
        if de_sku:
            try:
                listing = await de_list.get_listings_item(
                    de_list.seller_id, de_sku,
                    included_data="summaries,attributes",
                )
                attrs = listing.get("attributes", {})
                print(f"    color: {json.dumps(attrs.get('color', []))}")
                print(f"    size: {json.dumps(attrs.get('size', []))}")
                print(f"    color_name: {json.dumps(attrs.get('color_name', []))}")
                print(f"    size_name: {json.dumps(attrs.get('size_name', []))}")
                parent_rel = attrs.get("child_parent_sku_relationship", [])
                if parent_rel:
                    print(f"    parent_sku: {parent_rel[0].get('parent_sku')}")
                for s in listing.get("summaries", []):
                    print(f"    Title: {s.get('itemName', '')[:80]}")
            except Exception as e:
                print(f"    Listing error: {e}")

    # 4. Check if acier children belong to same global_family_id
    print("\n=== GLOBAL FAMILY CHECK ===")
    for asin in ACIER_ASINS[:4]:
        cur.execute(
            "SELECT global_family_id FROM dbo.global_family_child WHERE de_child_asin = ?",
            [asin]
        )
        rows = cur.fetchall()
        fam = rows[0][0] if rows else "NOT FOUND"
        print(f"  {asin} -> family: {fam}")
    
    conn.close()

if __name__ == "__main__":
    asyncio.run(main())
