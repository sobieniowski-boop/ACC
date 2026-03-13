import asyncio
import json

from app.connectors.amazon_sp_api.listings import ListingsClient


async def dump(mp_id: str, sku: str, label: str) -> None:
    client = ListingsClient(marketplace_id=mp_id)
    seller_id = client.seller_id
    data = await client.get_listings_item(seller_id, sku, included_data="attributes,summaries")
    product_type = (data.get("summaries") or [{}])[0].get("productType")
    attrs = data.get("attributes", {})
    keys = [k for k in attrs.keys() if "color" in k.lower() or "size" in k.lower() or "variation" in k.lower()]
    print(f"\n=== {label} ===")
    print("productType:", product_type)
    print("keys:", keys)
    print(json.dumps({k: attrs.get(k) for k in keys[:12]}, ensure_ascii=False, indent=2)[:4000])


async def main() -> None:
    sku = "FBA_5902730382133"
    await dump("A13V1IB3VIYZZH", sku, "FR")
    await dump("A1PA6795UKMFR9", sku, "DE")


if __name__ == "__main__":
    asyncio.run(main())
