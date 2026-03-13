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
    conn = connect_acc(autocommit=True)
    cur = conn.cursor()
    
    # 1. Find SKUs for old parent ASINs via registry
    print("=== FINDING SKUs FOR OLD PARENT ASINs ===")
    for asin in ["B08KH6GCMW", "B08KKTKHHG"]:
        cur.execute(
            "SELECT merchant_sku FROM dbo.acc_amazon_listing_registry WITH (NOLOCK) WHERE asin = ? AND marketplace_id = ?",
            [asin, FR_MARKETPLACE],
        )
        rows = cur.fetchall()
        if rows:
            for r in rows:
                print(f"  {asin} -> SKU: {r[0]}")
        else:
            print(f"  {asin} -> NOT in FR registry")

    # Broader search
    print("\n=== REGISTRY ANY MARKETPLACE ===")
    for asin in ["B08KH6GCMW", "B08KKTKHHG"]:
        cur.execute(
            "SELECT merchant_sku, marketplace_id FROM dbo.acc_amazon_listing_registry WITH (NOLOCK) WHERE asin = ?",
            [asin],
        )
        rows = cur.fetchall()
        for r in rows:
            print(f"  {asin} -> SKU: {r[0]}, MP: {r[1]}")
        if not rows:
            print(f"  {asin} -> NOT in any marketplace registry")

    # Children of these parents
    print("\n=== CHILDREN OF OLD PARENTS ===")
    for parent_asin in ["B08KH6GCMW", "B08KKTKHHG"]:
        cur.execute("""
            SELECT TOP 5 mlc.asin, ar.merchant_sku
            FROM dbo.marketplace_listing_child mlc WITH (NOLOCK)
            LEFT JOIN dbo.acc_amazon_listing_registry ar WITH (NOLOCK)
                ON ar.asin = mlc.asin AND ar.marketplace_id = ?
            WHERE mlc.marketplace = 'FR'
              AND mlc.current_parent_asin = ?
        """, [FR_MARKETPLACE, parent_asin])
        rows = cur.fetchall()
        print(f"\n  Parent {parent_asin} ({len(rows)} shown):")
        for r in rows:
            print(f"    Child ASIN: {r[0]}, SKU: {r[1] or 'N/A'}")
    
    conn.close()

    # 2. Catalog check
    fr_cat = CatalogClient(marketplace_id=FR_MARKETPLACE)
    print("\n=== CATALOG CHECK ===")
    for asin in ["B08KH6GCMW", "B08KKTKHHG"]:
        try:
            item = await fr_cat.get_item(asin, included_data="summaries,relationships")
            for s in item.get("summaries", []):
                print(f"  {asin}: {s.get('itemName', 'N/A')[:80]}")
            rels = item.get("relationships", [])
            for r in rels:
                for mr in r.get("relationships", [])[:3]:
                    print(f"    Type: {mr.get('type')}, childAsins: {len(mr.get('childAsins', []))}")
        except Exception as e:
            print(f"  {asin}: {e}")

    # 3. All issues on new parent
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
    
    print("\n--- 8801 issues (duplicate attrs) ---")
    for iss in issues:
        if iss.get("code") == "8801":
            print(f"  {iss.get('message', '')}")
    
    print("\n--- 8032 issues (multi-parent) first 5 ---")
    count = 0
    for iss in issues:
        if iss.get("code") == "8032" and count < 5:
            print(f"  {iss.get('message', '')}")
            count += 1

if __name__ == "__main__":
    asyncio.run(main())
