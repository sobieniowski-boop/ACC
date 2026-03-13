import asyncio, json, sys
sys.path.insert(0, r'C:\ACC\apps\api')
from app.connectors.amazon_sp_api.listings import ListingsClient
from app.core.db_connection import connect_acc

DE_MARKETPLACE = "A1PA6795UKMFR9"
FR_MARKETPLACE = "A13V1IB3VIYZZH"

ACIER_ASINS = ["B08JVL9DVD","B08JVJSP2L","B08JVKSN2M","B08JVNDCFK",
               "B08JVKH5B5","B08JVKSH4G","B08JVKZMCX","B08JVMF8CD","B08JVN8BT5"]

async def main():
    conn = connect_acc(autocommit=True)
    cur = conn.cursor()

    # 1. Acier children in global_family_child
    print("=== ACIER ASINs in global_family_child ===")
    for asin in ACIER_ASINS:
        cur.execute("SELECT global_family_id, sku_de, ean_de FROM dbo.global_family_child WITH (NOLOCK) WHERE de_child_asin = ?", [asin])
        rows = cur.fetchall()
        for r in rows:
            print(f"  {asin}: family={r[0]}, sku_de={r[1]}, ean={r[2]}")
        if not rows:
            print(f"  {asin}: NOT IN global_family_child")

    # 2. What are the DE SKUs for acier -> check listings on DE
    print("\n=== DE LISTINGS FOR ACIER (by sku_de) ===")
    de = ListingsClient(marketplace_id=DE_MARKETPLACE)
    acier_de_skus = set()
    for asin in ACIER_ASINS[:4]:
        cur.execute("SELECT sku_de FROM dbo.global_family_child WITH (NOLOCK) WHERE de_child_asin = ?", [asin])
        rows = cur.fetchall()
        if rows and rows[0][0]:
            de_sku = rows[0][0]
            acier_de_skus.add(de_sku)
            try:
                listing = await de.get_listings_item(de.seller_id, de_sku, included_data="summaries,attributes")
                attrs = listing.get("attributes", {})
                color = attrs.get("color", [])
                size = attrs.get("size", [])
                for s in listing.get("summaries", []):
                    print(f"\n  {de_sku} ({asin}): {s.get('itemName','')[:80]}")
                    print(f"    DE ASIN: {s.get('asin')}")
                print(f"    color: {json.dumps(color)}")
                print(f"    size: {json.dumps(size)}")
                parent_rel = attrs.get("child_parent_sku_relationship", [])
                if parent_rel:
                    print(f"    DE parent_sku: {parent_rel[0].get('parent_sku')}")
            except Exception as e:
                print(f"  {de_sku}: ERROR {e}")

    # 3. Also standard counterparts on DE
    STANDARD_ASINS = ["B07YC4RQ9N","B07YC4Z6QY","B07YC4PGQW","B07YC444C8"]
    print("\n=== DE LISTINGS FOR STANDARD (counterparts) ===")
    for asin in STANDARD_ASINS:
        cur.execute("SELECT sku_de FROM dbo.global_family_child WITH (NOLOCK) WHERE de_child_asin = ?", [asin])
        rows = cur.fetchall()
        if rows and rows[0][0]:
            de_sku = rows[0][0]
            try:
                listing = await de.get_listings_item(de.seller_id, de_sku, included_data="summaries,attributes")
                attrs = listing.get("attributes", {})
                color = attrs.get("color", [])
                size = attrs.get("size", [])
                for s in listing.get("summaries", []):
                    print(f"\n  {de_sku} ({asin}): {s.get('itemName','')[:80]}")
                    print(f"    DE ASIN: {s.get('asin')}")
                print(f"    color: {json.dumps(color)}")
                print(f"    size: {json.dumps(size)}")
                parent_rel = attrs.get("child_parent_sku_relationship", [])
                if parent_rel:
                    print(f"    DE parent_sku: {parent_rel[0].get('parent_sku')}")
            except Exception as e:
                print(f"  {de_sku}: ERROR {e}")
        else:
            print(f"  {asin}: no sku_de found")

    # 4. Check restructure log
    print("\n=== RESTRUCTURE LOG ===")
    cur.execute("""
        SELECT TOP 3 executed_at, steps_json
        FROM dbo.family_restructure_log WITH (NOLOCK)
        WHERE family_id = 1367 AND marketplace_id = ?
        ORDER BY executed_at DESC
    """, [FR_MARKETPLACE])
    for row in cur.fetchall():
        steps = json.loads(row[1]) if row[1] else []
        acier_steps = []
        for s in steps:
            asin_val = s.get("asin", "")
            sku_val = s.get("sku", "")
            if any(a in str(s) for a in ACIER_ASINS[:2]):
                acier_steps.append(s)
        print(f"\n  Log at {row[0]}: {len(steps)} steps, {len(acier_steps)} mention acier")
        for s in acier_steps[:3]:
            print(f"    [{s.get('action')}] sku={s.get('sku')} asin={s.get('asin')}")
            if s.get("patches"):
                print(f"      patches: {json.dumps(s['patches'])[:400]}")

    conn.close()

if __name__ == "__main__":
    asyncio.run(main())
