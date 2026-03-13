"""SP-API Product Pricing v0 connector.

Fetches competitive pricing, BuyBox status, and listing offers.
Rate limits: 0.5 req/s burst for getCompetitivePricing, 10 items per call.

Reference: https://developer-docs.amazon.com/sp-api/docs/product-pricing-api-v0-reference
"""
from __future__ import annotations

import asyncio
from typing import Optional

import structlog

from app.connectors.amazon_sp_api.client import SPAPIClient

log = structlog.get_logger(__name__)


class PricingClient(SPAPIClient):
    """Fetch pricing and BuyBox data from Amazon SP-API."""

    async def get_competitive_pricing(
        self,
        asins: Optional[list[str]] = None,
        skus: Optional[list[str]] = None,
    ) -> list[dict]:
        """
        Get competitive pricing for up to 20 ASINs or SKUs per call.

        Returns list of pricing results with structure:
        [
            {
                "status": "Success",
                "ASIN": "B00...",
                "Product": {
                    "CompetitivePrices": [
                        {
                            "CompetitivePriceId": "1",  # 1 = New BuyBox
                            "Price": {
                                "ListingPrice": {"Amount": 19.99, "CurrencyCode": "EUR"},
                                "LandedPrice": {"Amount": 19.99, "CurrencyCode": "EUR"},
                                "Shipping": {"Amount": 0.0, "CurrencyCode": "EUR"}
                            },
                            "condition": "New",
                            "belongsToRequester": true
                        }
                    ],
                    "NumberOfOfferListings": [
                        {"condition": "New", "Count": 5},
                        {"condition": "Used", "Count": 2}
                    ],
                    "SalesRankings": [
                        {"ProductCategoryId": "kitchen_display_on_website", "Rank": 1234}
                    ]
                }
            }
        ]
        """
        if not asins and not skus:
            raise ValueError("Provide either asins or skus")

        params: dict = {"MarketplaceId": self.marketplace_id}

        if asins:
            params["Asins"] = ",".join(asins[:20])
            params["ItemType"] = "Asin"
        else:
            params["Skus"] = ",".join(skus[:20])
            params["ItemType"] = "Sku"

        data = await self.get("/products/pricing/v0/competitivePrice", params)
        return data.get("payload", [])

    async def get_pricing(
        self,
        asins: Optional[list[str]] = None,
        skus: Optional[list[str]] = None,
    ) -> list[dict]:
        """
        Get pricing information (listing price, buy box price) for up to 20 items.

        Returns list with structure:
        [
            {
                "status": "Success",
                "ASIN": "B00...",
                "Product": {
                    "Offers": [
                        {
                            "BuyingPrice": {"ListingPrice": {...}, "LandedPrice": {...}},
                            "RegularPrice": {...},
                            "FulfillmentChannel": "AMAZON",
                            "IsBuyBoxWinner": true,
                            "IsFeaturedMerchant": true,
                            "SellerSKU": "SKU123"
                        }
                    ]
                }
            }
        ]
        """
        if not asins and not skus:
            raise ValueError("Provide either asins or skus")

        params: dict = {"MarketplaceId": self.marketplace_id}

        if asins:
            params["Asins"] = ",".join(asins[:20])
            params["ItemType"] = "Asin"
        else:
            params["Skus"] = ",".join(skus[:20])
            params["ItemType"] = "Sku"

        data = await self.get("/products/pricing/v0/price", params)
        return data.get("payload", [])

    async def get_item_offers(
        self,
        asin: str,
        item_condition: str = "New",
    ) -> dict:
        """
        Get all active offers for a specific ASIN (including competitor offers).

        Returns:
        {
            "Identifier": {"ASIN": "B00...", "MarketplaceId": "..."},
            "Summary": {
                "LowestPrices": [...],
                "BuyBoxPrices": [...],
                "NumberOfOffers": [...],
                "BuyBoxEligibleOffers": [...]
            },
            "Offers": [
                {
                    "ListingPrice": {...},
                    "ShippingPrice": {...},
                    "IsBuyBoxWinner": true,
                    "IsFeaturedMerchant": true,
                    "IsFulfilledByAmazon": true,
                    "SellerFeedbackRating": {...}
                }
            ]
        }
        """
        params = {
            "MarketplaceId": self.marketplace_id,
            "ItemCondition": item_condition,
        }
        data = await self.get(f"/products/pricing/v0/items/{asin}/offers", params)
        return data.get("payload", {})

    async def get_competitive_pricing_batch(
        self,
        asins: list[str],
        batch_size: int = 20,
    ) -> list[dict]:
        """
        Get competitive pricing for a large list of ASINs, batched by 20.
        """
        all_results: list[dict] = []

        for i in range(0, len(asins), batch_size):
            batch = asins[i: i + batch_size]
            try:
                results = await self.get_competitive_pricing(asins=batch)
                all_results.extend(results)
                log.info("pricing.batch_ok", batch=i // batch_size + 1, count=len(results))
            except Exception as e:
                log.error("pricing.batch_error", batch=i // batch_size + 1, error=str(e))

            # Rate limit: 0.5 req/s → 2s between calls to be safe
            if i + batch_size < len(asins):
                await asyncio.sleep(2.0)

        return all_results

    async def get_fees_estimate(
        self,
        asin: str,
        price: float,
        currency: str = "EUR",
        is_fba: bool = True,
    ) -> dict:
        """
        Estimate Amazon fees for a product at a given price.

        Returns dict with:
          - TotalFeesEstimate: {Amount, CurrencyCode}
          - FeeDetailList: [{FeeType, FinalFee, ...}]
        """
        body = {
            "FeesEstimateRequest": {
                "MarketplaceId": self.marketplace_id,
                "IsAmazonFulfilled": is_fba,
                "PriceToEstimateFees": {
                    "ListingPrice": {
                        "CurrencyCode": currency,
                        "Amount": price,
                    }
                },
                "Identifier": asin,
                "IdType": "ASIN",
            }
        }
        data = await self.post(
            f"/products/fees/v0/items/{asin}/feesEstimate",
            body,
        )
        result = data.get("payload", {}).get("FeesEstimateResult", {})
        return result


def parse_competitive_pricing(item: dict) -> dict:
    """
    Parse competitive pricing response into a flat dict for acc_offer upsert.

    Returns:
        asin, buybox_price, buybox_currency, has_buybox, num_offers_new,
        bsr_rank, bsr_category
    """
    result: dict = {
        "asin": item.get("ASIN", ""),
        "has_buybox": False,
        "buybox_price": None,
        "num_offers_new": 0,
    }

    product = item.get("Product", {})

    # Competitive Prices — BuyBox
    for cp in product.get("CompetitivePrices", []):
        if cp.get("CompetitivePriceId") == "1":  # = New BuyBox price
            landed = cp.get("Price", {}).get("LandedPrice", {})
            result["buybox_price"] = landed.get("Amount")
            result["buybox_currency"] = landed.get("CurrencyCode")
            result["has_buybox"] = cp.get("belongsToRequester", False)

    # Number of offers
    for nol in product.get("NumberOfOfferListings", []):
        if nol.get("condition") == "New":
            result["num_offers_new"] = nol.get("Count", 0)

    # Sales Rankings — BSR
    rankings = product.get("SalesRankings", [])
    if rankings:
        result["bsr_rank"] = rankings[0].get("Rank")
        result["bsr_category"] = rankings[0].get("ProductCategoryId", "")

    return result


def parse_pricing_response(item: dict) -> dict:
    """
    Parse /products/pricing/v0/price response for offer data.

    Returns:
        asin, sku, price, currency, buybox_price, has_buybox, is_featured_merchant,
        fulfillment_channel
    """
    result: dict = {
        "asin": item.get("ASIN", ""),
        "has_buybox": False,
        "is_featured_merchant": False,
    }

    product = item.get("Product", {})
    offers = product.get("Offers", [])

    for offer in offers:
        sku = offer.get("SellerSKU")
        if sku:
            result["sku"] = sku

        buying_price = offer.get("BuyingPrice", {})
        listing_price = buying_price.get("ListingPrice", {})
        result["price"] = listing_price.get("Amount")
        result["currency"] = listing_price.get("CurrencyCode", "EUR")

        result["has_buybox"] = offer.get("IsBuyBoxWinner", False)
        result["is_featured_merchant"] = offer.get("IsFeaturedMerchant", False)

        fc = offer.get("FulfillmentChannel", "")
        result["fulfillment_channel"] = "FBA" if fc == "AMAZON" else "FBM"

        # BuyBox price (from regular price or buying price)
        if result["has_buybox"]:
            landed = buying_price.get("LandedPrice", {})
            result["buybox_price"] = landed.get("Amount") or result["price"]

    return result
