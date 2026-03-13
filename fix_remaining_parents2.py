"""
Find SKUs for old parent ASINs B08KH6GCMW and B08KKTKHHG, then delete them.
Also dump all issues from new parent.
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

    # 1. Find SKUs for old parent ASINs via registry
    print("=== FINDING SKUs FOR OLD PARENT ASINs ===")
    conn = connect_acc(autocommit=True)
    cur = conn.cursor()
    
    for asin in ["B08KH6GCMW", "B08KKTKHHG"]:
        cur.execute(
            "SELECT merchant_sku FROM dbo.acc_amazon_listing_registry WITH (NOLOCK) WHERE asin = %s AND marketplace_id = %s",
            (asin, FR_MARKETPLACE),
        )
        rows = cur.fetchall()
        if rows:
            for r in rows:
                print(f"  {asin} -> SKU: {r[0]}")
        else:
            print(f"  {asin} -> NOT FOUND in registry")

    # Broader search - any marketplace
    print("\n=== REGISTRY SEARCH (any marketplace) ===")
    for asin in ["B08KH6GCMW", "B08KKTKHHG"]:
        cur.execute(
            "SELECT merchant_sku, marketplace_id FROM dbo.acc_amazon_listing_registry WITH (NOLOCK) WHERE asin = %s",
            (asin,),
        )
        rows = cur.fetchall()
        if rows:
            for r in rows:
                print(f"  {asin} -> SKU: {r[0]}, MP: {r[1]}")
        else:
            print(f"  {asin} -> NOT FOUND anywhere in registry")

    # Check children of these parents to understand what they are
    print("\n=== CHILDREN OF OLD PARENTS ===")
    for parent_asin in ["B08KH6GCMW", "B08KKTKHHG"]:
        cur.execute("""
            SELECT TOP 5 mlc.asin, mlc.current_parent_asin, ar.merchant_sku
            FROM dbo.marketplace_listing_child mlc WITH (NOLOCK)
            LEFT JOIN dbo.acc_amazon_listing_registry ar WITH (NOLOCK)
                ON ar.asin = mlc.asin AND ar.marketplace_id = %s
            WHERE mlc.marketplace = 'FR'
              AND mlc.current_parent_asin = %s
        """, (FR_MARKETPLACE, parent_asin))
        rows = cur.fetchall()
        print(f"\n  Parent {parent_asin} children:")
        for r in rows:
            print(f"    Child ASIN: {r[0]}, SKU: {r[2] or 'N/A'}")

    conn.close()

    # 2. Try to get listing for these parent ASINs by searching
    # Use Catalog API to check if they're real parents
    fr_cat = CatalogClient(marketplace_id=FR_MARKETPLACE)
    print("\n=== CATALOG CHECK ===")
    for asin in ["B08KH6GCMW", "B08KKTKHHG"]:
        try:
            item = await fr_cat.get_item(asin, included_data="summaries,relationships")
            rels = item.get("relationships", [])
            for s in item.get("summaries", []):
                print(f"  {asin}: {s.get('itemName', 'N/A')[:80]} | Classification: {s.get('classificationId', 'N/A')}")
            if rels:
                for r in rels:
                    mp_rels = r.get("relationships", [])
                    for mr in mp_rels[:2]:
                        print(f"    Type: {mr.get('type')}, childAsins: {len(mr.get('childAsins', []))}")
        except Exception as e:
            print(f"  {asin}: ERROR {e}")

    # 3. Dump all issues on new parent
    print("\n=== ALL ISSUES ON NEW PARENT ===")
    new_sku = "FR-PARENT-1367-2A0A63DE"
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
    
    print("\n--- ALL 8801 issues (duplicate attrs) ---")
    for iss in issues:
        if iss.get("code") == "8801":
            msg = iss.get("message", "")
            print(f"  {msg}")
    
    print("\n--- First 5 of 8032 issues (multi-parent) ---")
    count = 0
    for iss in issues:
        if iss.get("code") == "8032" and count < 5:
            msg = iss.get("message", "")
            print(f"  {msg}")
            count += 1

if __name__ == "__main__":
    asyncio.run(main())
