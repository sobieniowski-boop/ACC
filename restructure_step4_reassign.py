"""Step 4: Reassign all 98 children to the new parent FR-PARENT-1367-CEA8F738.

Sends PATCH child_parent_sku_relationship + parentage_level=child for each child.
Excludes FBA_5903699442920 (B0B97YPZMF) which returns 404.
"""
import asyncio, json, os, sys
sys.path.insert(0, r'C:\ACC\apps\api')
os.environ['PYTHONPATH'] = r'C:\ACC\apps\api'

from app.connectors.amazon_sp_api.listings import ListingsClient

FR_MP = "A13V1IB3VIYZZH"
PRODUCT_TYPE = "CONTAINER_LID"
NEW_PARENT_SKU = "FR-PARENT-1367-CEA8F738"

# All 98 children (excluding FBA_5903699442920 which 404s)
CHILDREN = [
    "FBA_5902730382126",  # B07YC4SWHK  Plastique 24cm
    "MAG_5903699477267",  # B0C2VQDX9Q  Acier lot
    "MAG_5903699477328",  # B0C2VP2HH2  Acier lot
    "MAG_5903699477380",  # B0C2VN3VLR  Acier lot
    "MAG_5903699477441",  # B0C2VQT7YQ  Acier lot
    "FBA_5903699409480",  # B08JZ25MNT  Beige 30cm
    "FBA_5903699409473",  # B08JYGM9BK  Beige 28cm
    "FBA_5903699409466",  # B08JY5QH4D  Beige 26cm
    "FBA_5903699409459",  # B08JYPKBMY  Beige 24cm
    "FBA_5903699409442",  # B08JYYCMHZ  Beige 22cm
    "FBA_5903699409435",  # B08JYYJFBM  Beige 20cm
    "FBA_5903699409428",  # B08JXHBDC5  Beige 18cm
    "FBA_5903699409404",  # B08JYVC8MH  Beige 14cm
    "FBA_5903699409411",  # B08JYHQRNS  Beige 16cm
    "MAG_5903699470473",  # B0BXP7XP5C  Brillant 14cm
    "MAG_5903699470480",  # B0BXP8JNCQ  Brillant 16cm
    "MAG_5903699470497",  # B0BXP651RD  Brillant 18cm
    "MAG_5903699470503",  # B0BXP71V7G  Brillant 20cm
    "MAG_5903699470510",  # B0BXP8KXLS  Brillant 22cm
    "MAG_5903699470527",  # B0BXP81PW1  Brillant 24cm
    "MAG_5903699470534",  # B0BXP5832H  Brillant 26cm
    "MAG_5903699470541",  # B0BXP7JKN3  Brillant 28cm
    "MAG_5903699470558",  # B0BXP7PVFF  Brillant 30cm
    "MAG_5903699477311",  # B0C2VPQ8FF  Brillant lot
    "MAG_5903699477373",  # B0C2VNYJDL  Brillant lot
    "MAG_5903699477434",  # B0C2VPDR32  Brillant lot
    "MAG_5903699477496",  # B0C2VP179D  Brillant lot
    "MAG_5903699470381",  # B0BXLT1KBG  Noir argent 14cm
    "MAG_5903699470398",  # B0BXLSWXBM  Noir argent 16cm
    "MAG_5903699470404",  # B0BXLTCW7F  Noir argent 18cm
    "MAG_5903699470411",  # B0BXLV3YFZ  Noir argent 20cm
    "MAG_5903699470428",  # B0BXLW5NMF  Noir argent 22cm
    "MAG_5903699470435",  # B0BXLVMGMT  Noir argent 24cm
    "MAG_5903699470442",  # B0BXLT9RF4  Noir argent 26cm
    "MAG_5903699470459",  # B0BXLTSG23  Noir argent 28cm
    "MAG_5903699470466",  # B0BXLVHZZL  Noir argent 30cm
    "MAG_5903699477304",  # B0C2VKFM7Z  Noir argent lot
    "MAG_5903699477366",  # B0C2VLWZJ1  Noir argent lot
    "MAG_5903699477427",  # B0C2VNSLZB  Noir argent lot
    "MAG_5903699477489",  # B0C2VL2FSK  Noir argent lot
    "FBA_5902730382119",  # B07YC5FSY1  Plastique 22cm
    "FBA_5902730382133",  # B07YC4T22B  Plastique 26cm
    "FBA_5902730382157",  # B07YC5B9BB  Plastique 28cm
    "FBA_5902730382140",  # B07YC5Z7R2  Plastique 30cm
    "MAG_5903699442845",  # B0B97W2CRS  Plastique lot 2x14
    "MAG_5903699436868",  # B0B5R673NB  Plastique lot 14+16
    "MAG_5903699442852",  # B0B97ZX3WM  Plastique lot 2x16
    "MAG_5903699436899",  # B0B5R7855X  Plastique lot 16+18
    "MAG_5903699436844",  # B0B5R55VH2  Plastique lot 16+20
    "MAG_5903699442869",  # B0B97W31BP  Plastique lot 2x18
    "MAG_5903699442876",  # B0B97XTT7B  Plastique lot 2x20
    "MAG_5903699436851",  # B0B5R8C9CS  Plastique lot 20+24
    "MAG_5903699436882",  # B0B5R77D55  Plastique lot 20+28
    "MAG_5903699442883",  # B0B97VCN6W  Plastique lot 2x22
    "MAG_5903699442890",  # B0B97YR12K  Plastique lot 2x24
    "MAG_5903699436837",  # B0B5R63FHF  Plastique lot 24+28
    "MAG_5903699442906",  # B0B97Y11YP  Plastique lot 2x26
    "MAG_5903699442913",  # B0B97ZVJWP  Plastique lot 2x28
    "MAG_5903699436905",  # B0B5R614GM  Plastique lot 28+30
    "MAG_5903699436875",  # B0B5R65HXT  Plastique lot 3
    "FBA_5903699409350",  # B08JVKH5B5  Acier 22cm
    "FBA_5903699409312",  # B08JVL9DVD  Acier 14cm
    "FBA_5903699409329",  # B08JVJSP2L  Acier 16cm
    "FBA_5903699409336",  # B08JVKSN2M  Acier 18cm
    "FBA_5903699409343",  # B08JVNDCFK  Acier 20cm
    "FBA_5903699409367",  # B08JVMF8CD  Acier 24cm
    "FBA_5903699409374",  # B08JVN8BT5  Acier 26cm
    "FBA_5903699409381",  # B08JVKZMCX  Acier 28cm
    "FBA_5903699409398",  # B08JVKSH4G  Acier 30cm
    "FBA_5902730382072",  # B07YC4RQ9N  Plastique 14cm
    "FBA_5902730382089",  # B07YC4Z6QY  Plastique 16cm
    "FBA_5902730382096",  # B07YC4PGQW  Plastique 18cm
    "FBA_5902730382102",  # B07YC444C8  Plastique 20cm
    "MAG_5903699470299",  # B0BXLPFNYR  noir mat 14cm
    "MAG_5903699470305",  # B0BXLNSRZW  noir mat 16cm
    "MAG_5903699470312",  # B0BXLP28DH  noir mat 18cm
    "MAG_5903699470329",  # B0BXLMTQ86  noir mat 20cm
    "MAG_5903699470336",  # B0BXLKRSNV  noir mat 22cm
    "MAG_5903699470343",  # B0BXLLTKJK  noir mat 24cm
    "MAG_5903699470350",  # B0BXLQ16SJ  noir mat 26cm
    "MAG_5903699470367",  # B0BXLPQ7YV  noir mat 28cm
    "MAG_5903699470374",  # B0BXLQ89JP  noir mat 30cm
    "MAG_5903699477298",  # B0C2VP4M8D  noir mat lot
    "MAG_5903699477359",  # B0C2VNKX9K  noir mat lot
    "MAG_5903699477410",  # B0C2VP6HLW  noir mat lot
    "MAG_5903699477472",  # B0C2VPDK2P  noir mat lot
    "MAG_5903699470206",  # B0BXL91LXY  argentée 14cm
    "MAG_5903699470213",  # B0BXLD9J1Y  argentée 16cm
    "MAG_5903699470220",  # B0BXLBZ11F  argentée 18cm
    "MAG_5903699470237",  # B0BXLCVXCY  argentée 20cm
    "MAG_5903699470244",  # B0BXL9H6MX  argentée 22cm
    "MAG_5903699470251",  # B0BXL9L3V4  argentée 24cm
    "MAG_5903699470268",  # B0BXLC66BT  argentée 26cm
    "MAG_5903699470275",  # B0BXL79B8L  argentée 28cm
    "MAG_5903699470282",  # B0BXLCX3HX  argentée 30cm
    "MAG_5903699477281",  # B0C2VPCG7T  argentée lot
    "MAG_5903699477342",  # B0C2VK5JFQ  argentée lot
    "MAG_5903699477403",  # B0C2VNVJJ7  argentée lot
    "MAG_5903699477465",  # B0C2VMVGZN  argentée lot
]

assert len(CHILDREN) == 99, f"Expected 99 children, got {len(CHILDREN)}"


def _reassign_patches(parent_sku: str) -> list[dict]:
    return [
        {
            "op": "replace",
            "path": "/attributes/child_parent_sku_relationship",
            "value": [{
                "child_relationship_type": "variation",
                "parent_sku": parent_sku,
                "marketplace_id": FR_MP,
            }],
        },
        {
            "op": "replace",
            "path": "/attributes/parentage_level",
            "value": [{
                "marketplace_id": FR_MP,
                "value": "child",
            }],
        },
    ]


async def main():
    client = ListingsClient(marketplace_id=FR_MP)
    seller_id = client.seller_id
    patches = _reassign_patches(NEW_PARENT_SKU)

    accepted = 0
    failed = 0
    failed_skus = []

    total = len(CHILDREN)
    for i, sku in enumerate(CHILDREN, 1):
        print(f"[{i:2d}/{total}] {sku}", end=" ")
        try:
            result = await client.patch_listings_item(seller_id, sku, patches, PRODUCT_TYPE)
            status = result.get("status", "UNKNOWN")
            if status == "ACCEPTED":
                accepted += 1
                print("✅")
            else:
                failed += 1
                failed_skus.append(sku)
                issues = result.get("issues", [])
                print(f"❌ {status}")
                for iss in issues[:2]:
                    print(f"     {iss.get('code')}: {iss.get('message','')[:120]}")
        except Exception as e:
            failed += 1
            failed_skus.append(sku)
            print(f"❌ ERROR: {e}")

        if i < len(CHILDREN):
            await asyncio.sleep(0.25)

    print(f"\n{'='*60}")
    print(f"ACCEPTED: {accepted}/{total}")
    print(f"FAILED:   {failed}/{total}")
    if failed_skus:
        print(f"Failed SKUs: {failed_skus}")


asyncio.run(main())
