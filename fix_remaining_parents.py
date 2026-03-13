"""
Find SKUs for old parent ASINs B08KH6GCMW and B08KKTKHHG, then delete them.
Also dump all 8801 issues from new parent to understand duplicate variation attr conflicts.
"""
import asyncio
import json
import sys
sys.path.insert(0, r'C:\ACC\apps\api')

from app.connectors.amazon_sp_api.listings import ListingsClient
from app.connectors.amazon_sp_api.catalog import CatalogClient
from app.core.db_connection import connect_acc

FR_MARKETPLACE = "A13V1IB3VIYZZH"

async def main():
    fr_list = ListingsClient(marketplace_id=FR_MARKETPLACE)
    fr_cat = CatalogClient(marketplace_id=FR_MARKETPLACE)

    # 1. Find SKUs for old parent ASINs via registry
    print("=== FINDING SKUs FOR OLD PARENT ASINs ===")
    conn = connect_acc(autocommit=True)
    cur = conn.cursor()
    
    for asin in ["B08KH6GCMW", "B08KKTKHHG"]:
        cur.execute("""
            SELECT merchant_sku
            FROM dbo.acc_amazon_listing_registry WITH (NOLOCK)
            WHERE asin = %s AND marketplace_id = %s
        """, (asin, FR_MARKETPLACE))
        rows = cur.fetchall()
        if rows:
            for r in rows:
                print(f"  {asin} → SKU: {r[0]}")
        else:
            print(f"  {asin} → NOT FOUND in registry, trying catalog...")
            # try search by ASIN in catalog
            try:
                item = await fr_cat.get_item(asin, included_data="summaries,relationships")
                for s in item.get("summaries", []):
                    print(f"    Catalog: {s.get('itemName', 'N/A')[:80]}")
            except Exception as e:
                print(f"    Catalog error: {e}")

    # Also search by product table
    print("\n=== CHECKING acc_product for parent SKUs ===")
    cur.execute("""
        SELECT sku, asin, marketplace_id
        FROM dbo.acc_amazon_listing_registry WITH (NOLOCK)
        WHERE asin IN ('B08KH6GCMW', 'B08KKTKHHG')
    """)
    for row in cur.fetchall():
        print(f"  SKU: {row[0]}, ASIN: {row[1]}, MP: {row[2]}")

    # If not found in registry, try to get all parent SKUs from marketplace_listing_child
    print("\n=== CHECK marketplace_listing_child for parent SKU info ===")
    cur.execute("""
        SELECT DISTINCT mlc.current_parent_asin, ar.merchant_sku
        FROM dbo.marketplace_listing_child mlc WITH (NOLOCK)
        LEFT JOIN dbo.acc_amazon_listing_registry ar WITH (NOLOCK)
            ON ar.asin = mlc.current_parent_asin AND ar.marketplace_id = %s
        WHERE mlc.marketplace = 'FR'
          AND mlc.current_parent_asin IN ('B08KH6GCMW', 'B08KKTKHHG')
    """, (FR_MARKETPLACE,))
    for row in cur.fetchall():
        print(f"  Parent ASIN: {row[0]} → Registry SKU: {row[1] or 'NOT IN REGISTRY'}")

    conn.close()

    # 2. Dump all issues with code 8801 from new parent
    print("\n=== ALL ISSUES ON NEW PARENT ===")
    new_sku = "FR-PARENT-1367-2A0A63DE"
    try:
        listing = await fr_list.get_listings_item(
            fr_list.seller_id, new_sku,
            included_data="issues",
        )
        issues = listing.get("issues", [])
        print(f"Total issues: {len(issues)}")
        
        issue_counts = {}
        for iss in issues:
            code = iss.get("code", "?")
            issue_counts[code] = issue_counts.get(code, 0) + 1
        print(f"Issue counts: {json.dumps(issue_counts)}")
        
        print("\n--- Issue 8801 (duplicate attrs) ---")
        for iss in issues:
            if iss.get("code") == "8801":
                msg = iss.get("message", "")
                print(f"  {msg[:200]}")
        
        print("\n--- Issue 8032 (multi-parent, first 5) ---")
        for iss in issues[:5]:
            if iss.get("code") == "8032":
                msg = iss.get("message", "")
                print(f"  {msg[:200]}")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    asyncio.run(main())
