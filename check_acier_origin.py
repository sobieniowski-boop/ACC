"""
Deep investigation:
1. How did Acier ASINs end up in family 1367?
2. What does the DE parent for family 1367 contain?
3. Check FR listing attributes for ALL children - find ALL duplicate color+size pairs
4. Check what script wrote the color/size attrs - was it our targeted_repair or the restructure?
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

async def main():
    conn = connect_acc(autocommit=True)
    cur = conn.cursor()

    # 1. DE parent for family 1367
    print("=== DE PARENT FOR FAMILY 1367 ===")
    cur.execute("""
        SELECT de_parent_asin, de_parent_sku
        FROM dbo.global_family_child WITH (NOLOCK)
        WHERE global_family_id = 1367
    """)
    rows = cur.fetchall()
    de_parents = set()
    for r in rows:
        de_parents.add((r[0], r[1]))
    for p in de_parents:
        print(f"  DE parent ASIN: {p[0]}, SKU: {p[1]}")

    # 2. How many children does family 1367 have?
    print(f"\n  Total children in family 1367: {len(rows)}")

    # 3. Check which global_family_child entries correspond to Acier ASINs
    print("\n=== ACIER ASINs in global_family_child ===")
    acier_asins = ["B08JVL9DVD", "B08JVJSP2L", "B08JVKSN2M", "B08JVNDCFK",
                   "B08JVKH5B5", "B08JVKSH4G", "B08JVKZMCX", "B08JVMF8CD", "B08JVN8BT5"]
    for asin in acier_asins:
        cur.execute("""
            SELECT de_child_asin, de_parent_asin, de_parent_sku
            FROM dbo.global_family_child WITH (NOLOCK)
            WHERE de_child_asin = ? AND global_family_id = 1367
        """, [asin])
        rows = cur.fetchall()
        for r in rows:
            print(f"  {asin}: de_parent_asin={r[1]}, de_parent_sku={r[2]}")
        if not rows:
            print(f"  {asin}: NOT FOUND by de_child_asin")

    # 4. Are Acier children listed in marketplace_listing_child? What parent?
    print("\n=== Acier ASINs in marketplace_listing_child (FR) ===")
    for asin in acier_asins:
        cur.execute("""
            SELECT current_parent_asin
            FROM dbo.marketplace_listing_child WITH (NOLOCK)
            WHERE asin = ? AND marketplace = 'FR'
        """, [asin])
        rows = cur.fetchall()
        for r in rows:
            print(f"  {asin}: FR parent = {r[0]}")

    # 5. Check FR SKUs for acier ASINs via registry
    print("\n=== FR SKUs for Acier ASINs ===")
    for asin in acier_asins:
        cur.execute("""
            SELECT merchant_sku FROM dbo.acc_amazon_listing_registry WITH (NOLOCK)
            WHERE asin = ?
        """, [asin])
        rows = cur.fetchall()
        for r in rows:
            print(f"  {asin}: SKU = {r[0]}")
        if not rows:
            print(f"  {asin}: NO SKU in registry")

    # 6. Get ALL FR children for family 1367 with their color+size
    print("\n=== ALL FR CHILDREN: COLOR + SIZE ANALYSIS ===")
    fr = ListingsClient(marketplace_id=FR_MARKETPLACE)
    
    cur.execute("""
        SELECT DISTINCT mlc.asin
        FROM dbo.marketplace_listing_child mlc WITH (NOLOCK)
        WHERE mlc.marketplace = 'FR'
          AND mlc.asin IN (
              SELECT de_child_asin FROM dbo.global_family_child WITH (NOLOCK)
              WHERE global_family_id = 1367
          )
    """)
    all_fr_asins = [r[0] for r in cur.fetchall()]
    print(f"  Total FR ASINs in family 1367: {len(all_fr_asins)}")

    # Find all FR SKUs
    fr_sku_map = {}
    for asin in all_fr_asins:
        cur.execute("""
            SELECT merchant_sku FROM dbo.acc_amazon_listing_registry WHERE asin = ?
        """, [asin])
        rows = cur.fetchall()
        if rows:
            fr_sku_map[asin] = rows[0][0]

    # Check restructure log for how attrs were written
    print("\n=== RESTRUCTURE LOG - WHAT DID WE WRITE? ===")
    cur.execute("""
        SELECT TOP 1 steps_json
        FROM dbo.family_restructure_log WITH (NOLOCK)
        WHERE family_id = 1367 AND marketplace_id = ?
        ORDER BY executed_at DESC
    """, [FR_MARKETPLACE])
    r = cur.fetchone()
    if r and r[0]:
        steps = json.loads(r[0])
        # Look for steps involving acier ASINs
        acier_set = set(acier_asins)
        for step in steps:
            sku = step.get("sku", "")
            asin = step.get("asin", "")
            action = step.get("action", "")
            if asin in acier_set or any(a in str(step) for a in acier_asins[:2]):
                print(f"  [{action}] sku={sku} asin={asin}")
                if "patches" in step:
                    print(f"    patches: {json.dumps(step['patches'])[:300]}")
                if "result" in step:
                    print(f"    result: {str(step['result'])[:200]}")
    else:
        print("  No restructure log found")

    conn.close()

if __name__ == "__main__":
    asyncio.run(main())
