"""
Get sizes for Silicone (Beige) children and B08JVKH5B5 (Acier no-size) from Catalog.
Also determine size for B07YC4SWHK (totally empty standard).
"""
import asyncio, json, sys
sys.path.insert(0, r'C:\ACC\apps\api')
from app.connectors.amazon_sp_api.catalog import CatalogClient

FR_MARKETPLACE = "A13V1IB3VIYZZH"

# Silicone children (Beige, no size) + 1 already has size
SILICONE = {
    "B08JZ25MNT": "5903699409480",  # no size
    "B08JYGM9BK": "5903699409473",
    "B08JY5QH4D": "5903699409466",
    "B08JYPKBMY": "5903699409459",
    "B08JYYCMHZ": "5903699409442",
    "B08JYYJFBM": "5903699409435",
    "B08JXHBDC5": "5903699409428",
    "B08JYVC8MH": "5903699409404",
    "B08JYHQRNS": "5903699409411",  # has size=16cm
}

# Acier no size
ACIER_NO_SIZE = {"B08JVKH5B5": "5903699409350"}

# Standard totally empty
STANDARD_EMPTY = {"B07YC4SWHK": "5902730382126"}

ALL = {**SILICONE, **ACIER_NO_SIZE, **STANDARD_EMPTY}

async def main():
    cat = CatalogClient(marketplace_id=FR_MARKETPLACE)
    
    for asin, ean in ALL.items():
        try:
            item = await cat.get_item(asin, included_data="summaries,dimensions,attributes")
            for s in item.get("summaries", []):
                title = s.get("itemName", "")
                # Extract size from title - look for (XX cm) pattern
                print(f"\n{asin} (EAN {ean}):")
                print(f"  Title: {title}")
                # Check dimensions
            dims = item.get("dimensions", [])
            if dims:
                for d in dims:
                    print(f"  Dimensions: {json.dumps(d, ensure_ascii=False)[:200]}")
            attrs = item.get("attributes", {})
            if attrs:
                for k in ["size", "color", "item_dimensions", "diameter"]:
                    if k in attrs:
                        print(f"  attr.{k}: {json.dumps(attrs[k], ensure_ascii=False)[:200]}")
        except Exception as e:
            print(f"\n{asin}: ERROR {e}")

if __name__ == "__main__":
    asyncio.run(main())
