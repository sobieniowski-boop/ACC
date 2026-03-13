"""SP-API Orders connector."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

import structlog
from app.connectors.amazon_sp_api.client import SPAPIClient

log = structlog.get_logger(__name__)


class OrdersClient(SPAPIClient):
    def __init__(self, marketplace_id: Optional[str] = None, sync_profile: str | None = None):
        super().__init__(marketplace_id=marketplace_id, sync_profile=sync_profile or "core_sync")

    async def get_orders(
        self,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
        last_updated_after: Optional[datetime] = None,
        last_updated_before: Optional[datetime] = None,
        statuses: Optional[list[str]] = None,
        max_results: Optional[int] = None,
    ) -> list[dict]:
        """Fetch orders with automatic pagination."""
        if created_after is None and last_updated_after is None:
            created_after = datetime.now(timezone.utc) - timedelta(days=7)

        params = {
            "MarketplaceIds": self.marketplace_id,
            "MaxResultsPerPage": 100,
        }
        if created_after:
            params["CreatedAfter"] = created_after.strftime("%Y-%m-%dT%H:%M:%SZ")
        if created_before:
            params["CreatedBefore"] = created_before.strftime("%Y-%m-%dT%H:%M:%SZ")
        if last_updated_after:
            params["LastUpdatedAfter"] = last_updated_after.strftime("%Y-%m-%dT%H:%M:%SZ")
        if last_updated_before:
            params["LastUpdatedBefore"] = last_updated_before.strftime("%Y-%m-%dT%H:%M:%SZ")
        if statuses:
            params["OrderStatuses"] = ",".join(statuses)

        all_orders: list[dict] = []
        next_token: Optional[str] = None

        for _ in range(10_000):  # practically unlimited pagination
            if next_token:
                page = await self.get(
                    "/orders/v0/orders",
                    {"NextToken": next_token, "MarketplaceIds": self.marketplace_id},
                    endpoint_name="orders_v0.list_orders",
                )
            else:
                page = await self.get(
                    "/orders/v0/orders",
                    params,
                    endpoint_name="orders_v0.list_orders",
                )

            orders = page.get("payload", {}).get("Orders", [])
            all_orders.extend(orders)
            log.info("orders.fetched_page", count=len(orders), total=len(all_orders))

            next_token = page.get("payload", {}).get("NextToken")
            if not next_token or (max_results and len(all_orders) >= max_results):
                break
            # Small throttle to avoid 429 exhaustion on large pulls
            await asyncio.sleep(0.3)

        return all_orders[:max_results] if max_results else all_orders

    async def get_order_items(self, order_id: str) -> list[dict]:
        """Fetch line items for a single order."""
        result = []
        next_token = None
        for _ in range(20):
            params = {"MarketplaceIds": self.marketplace_id}
            if next_token:
                params["NextToken"] = next_token
            data = await self.get(
                f"/orders/v0/orders/{order_id}/orderItems",
                params,
                endpoint_name="orders_v0.list_order_items",
            )
            items = data.get("payload", {}).get("OrderItems", [])
            result.extend(items)
            next_token = data.get("payload", {}).get("NextToken")
            if not next_token:
                break
        return result
