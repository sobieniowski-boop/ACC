import asyncio
import json

from app.connectors.amazon_sp_api.listings import ListingsClient


async def main() -> None:
    sku = "FBA_5902730382133"
    client = ListingsClient(marketplace_id="A13V1IB3VIYZZH")
    seller_id = client.seller_id

    patches = [
        {
            "op": "replace",
            "path": "/attributes/color",
            "value": [
                {
                    "marketplace_id": "A13V1IB3VIYZZH",
                    "language_tag": "fr_FR",
                    "value": "Plastique",
                }
            ],
        },
        {
            "op": "replace",
            "path": "/attributes/size",
            "value": [
                {
                    "marketplace_id": "A13V1IB3VIYZZH",
                    "language_tag": "fr_FR",
                    "value": "26 cm",
                }
            ],
        },
    ]

    result = await client.patch_listings_item(
        seller_id,
        sku,
        patches,
        product_type="CONTAINER_LID",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
