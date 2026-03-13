"""
1. Sprawdzenie stanu starego parenta B07YL989KJ (SKU: FO-EK0P-A0FI) na FR
2. Usunięcie go
3. Sprawdzenie jakie inne parenty mogą istnieć na FR dla family 1367
"""
import asyncio
import json
import sys
sys.path.insert(0, r'C:\ACC\apps\api')

from app.connectors.amazon_sp_api.listings import ListingsClient
from app.core.db_connection import connect_acc

FR_MARKETPLACE = "A13V1IB3VIYZZH"

async def main():
    fr_client = ListingsClient(marketplace_id=FR_MARKETPLACE)

    # 1. Check OLD parent FO-EK0P-A0FI
    old_sku = "FO-EK0P-A0FI"
    print(f"=== OLD PARENT: {old_sku} ===")
    try:
        listing = await fr_client.get_listings_item(
            fr_client.seller_id, old_sku,
            included_data="summaries,attributes",
        )
        for s in listing.get("summaries", []):
            print(f"  ASIN: {s.get('asin')}")
            print(f"  Status: {s.get('status')}")
        attrs = listing.get("attributes", {})
        print(f"  parentage_level: {json.dumps(attrs.get('parentage_level', []))}")
        print(f"  child_parent_sku_relationship: {json.dumps(attrs.get('child_parent_sku_relationship', []))}")
    except Exception as e:
        print(f"  ERROR: {e}")

    # 2. Check all parent ASINs from marketplace_listing_child for family 1367 on FR
    print(f"\n=== ALL PARENT ASINS for family 1367 on FR ===")
    conn = connect_acc(autocommit=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT current_parent_asin, COUNT(*) as child_count
        FROM dbo.marketplace_listing_child WITH (NOLOCK)
        WHERE marketplace = 'FR'
          AND asin IN (
              SELECT de_child_asin FROM dbo.global_family_child WITH (NOLOCK)
              WHERE global_family_id = 1367
          )
        GROUP BY current_parent_asin
        ORDER BY child_count DESC
    """)
    for row in cur.fetchall():
        print(f"  Parent ASIN: {row[0] or 'NULL (orphan)'} — {row[1]} children")
    conn.close()

    # 3. Delete old parent
    print(f"\n=== DELETING OLD PARENT: {old_sku} ===")
    try:
        result = await fr_client.delete_listings_item(
            fr_client.seller_id, old_sku,
        )
        print(f"  Result: {json.dumps(result, indent=2)}")
        if result.get("status") == "ACCEPTED":
            print("  ✅ Old parent deleted")
        else:
            print(f"  ⚠️  Status: {result.get('status')}")
    except Exception as e:
        print(f"  ERROR: {e}")

    # 4. Check new parent status
    new_sku = "FR-PARENT-1367-2A0A63DE"
    print(f"\n=== NEW PARENT: {new_sku} ===")
    try:
        listing2 = await fr_client.get_listings_item(
            fr_client.seller_id, new_sku,
            included_data="summaries,attributes,issues",
        )
        for s in listing2.get("summaries", []):
            print(f"  ASIN: {s.get('asin')}")
            print(f"  Status: {s.get('status')}")
        issues = listing2.get("issues", [])
        if issues:
            print(f"\n  --- ISSUES ({len(issues)}) ---")
            for i in issues[:10]:
                print(f"  [{i.get('severity')}] {i.get('code')}: {i.get('message', '')[:120]}")
    except Exception as e:
        print(f"  ERROR: {e}")

if __name__ == "__main__":
    asyncio.run(main())
