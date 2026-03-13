"""SP-API Notifications v1 connector.

Manages destinations (SQS/EventBridge) and subscriptions for event-driven
data flows.  Builds on the shared SPAPIClient base (auth, backoff, telemetry).

SP-API Reference:
  https://developer-docs.amazon.com/sp-api/docs/notifications-api-v1-reference

Rate limits (EU):
  - createDestination     1 req/s burst
  - getDestinations       1 req/s burst
  - createSubscription    1 req/s burst
  - getSubscription       1 req/s burst
  - deleteSubscription    1 req/s burst
"""
from __future__ import annotations

from typing import Any, Optional

import structlog

from app.connectors.amazon_sp_api.client import SPAPIClient, _auth, _grantless_auth

log = structlog.get_logger(__name__)

NOTIFICATIONS_BASE = "/notifications/v1"

# ── Supported notification types for ACC ────────────────────────────────────
# See: https://developer-docs.amazon.com/sp-api/docs/notification-type-values

SUPPORTED_NOTIFICATION_TYPES: dict[str, str] = {
    "ANY_OFFER_CHANGED": "pricing",
    "LISTINGS_ITEM_STATUS_CHANGE": "listing",
    "LISTINGS_ITEM_ISSUES_CHANGE": "listing",
    "REPORT_PROCESSING_FINISHED": "report",
    "FBA_INVENTORY_AVAILABILITY_CHANGES": "inventory",
    "ORDER_STATUS_CHANGE": "order",
    "FEED_PROCESSING_FINISHED": "feed",
    "ITEM_PRODUCT_TYPE_CHANGE": "listing",
    "BRANDED_ITEM_CONTENT_CHANGE": "listing",
}


class NotificationsClient(SPAPIClient):
    """SP-API Notifications v1 — destinations & subscriptions."""

    def __init__(self) -> None:
        super().__init__(sync_profile="notifications", use_grantless=True)

    # ── Destinations ────────────────────────────────────────────────────────

    async def get_destinations(self) -> list[dict]:
        """List all registered notification destinations."""
        data = await self.get(
            f"{NOTIFICATIONS_BASE}/destinations",
            endpoint_name="notifications.getDestinations",
        )
        return data.get("payload", [])

    async def create_destination_sqs(
        self,
        name: str,
        sqs_arn: str,
    ) -> dict:
        """Register an SQS queue as a notification destination.

        The SQS queue must have a policy allowing Amazon to send messages.
        Returns the full destination object including ``destinationId``.
        """
        body = {
            "name": name,
            "resourceSpecification": {
                "sqs": {"arn": sqs_arn},
            },
        }
        data = await self.post(
            f"{NOTIFICATIONS_BASE}/destinations",
            body=body,
            endpoint_name="notifications.createDestination",
        )
        dest = data.get("payload", data)
        log.info(
            "notifications.destination_created",
            destination_id=dest.get("destinationId"),
            name=name,
        )
        return dest

    async def create_destination_eventbridge(
        self,
        name: str,
        account_id: str,
        region: str,
    ) -> dict:
        """Register an EventBridge partner event source as a destination."""
        body = {
            "name": name,
            "resourceSpecification": {
                "eventBridge": {
                    "accountId": account_id,
                    "region": region,
                },
            },
        }
        data = await self.post(
            f"{NOTIFICATIONS_BASE}/destinations",
            body=body,
            endpoint_name="notifications.createDestination",
        )
        dest = data.get("payload", data)
        log.info(
            "notifications.destination_created",
            destination_id=dest.get("destinationId"),
            name=name,
            type="eventbridge",
        )
        return dest

    async def get_destination(self, destination_id: str) -> dict:
        """Get a single destination by ID."""
        data = await self.get(
            f"{NOTIFICATIONS_BASE}/destinations/{destination_id}",
            endpoint_name="notifications.getDestination",
        )
        return data.get("payload", data)

    async def delete_destination(self, destination_id: str) -> None:
        """Delete a registered destination."""
        await self.delete(
            f"{NOTIFICATIONS_BASE}/destinations/{destination_id}",
            endpoint_name="notifications.deleteDestination",
        )
        log.info("notifications.destination_deleted", destination_id=destination_id)

    # ── Subscriptions ───────────────────────────────────────────────────────

    async def get_subscription(self, notification_type: str) -> Optional[dict]:
        """Get current subscription for a notification type.

        Returns None if no subscription exists (SP-API returns 404).
        Subscriptions use seller-authorized (refresh_token) auth.
        """
        saved = self._use_grantless
        self._use_grantless = False
        try:
            data = await self.get(
                f"{NOTIFICATIONS_BASE}/subscriptions/{notification_type}",
                endpoint_name="notifications.getSubscription",
            )
            return data.get("payload", data)
        except Exception as exc:
            if "404" in str(exc) or "not found" in str(exc).lower():
                return None
            raise
        finally:
            self._use_grantless = saved

    async def create_subscription(
        self,
        notification_type: str,
        destination_id: str,
        *,
        payload_version: str = "1.0",
    ) -> dict:
        """Create a subscription for a notification type routed to a destination.

        Only one subscription per notification type is allowed.
        Subscriptions use seller-authorized (refresh_token) auth.
        """
        body: dict[str, Any] = {
            "destinationId": destination_id,
            "payloadVersion": payload_version,
        }
        saved = self._use_grantless
        self._use_grantless = False
        try:
            data = await self.post(
                f"{NOTIFICATIONS_BASE}/subscriptions/{notification_type}",
                body=body,
                endpoint_name="notifications.createSubscription",
            )
        finally:
            self._use_grantless = saved
        sub = data.get("payload", data)
        log.info(
            "notifications.subscription_created",
            notification_type=notification_type,
            subscription_id=sub.get("subscriptionId"),
            destination_id=destination_id,
        )
        return sub

    async def delete_subscription(
        self,
        notification_type: str,
        subscription_id: str,
    ) -> None:
        """Delete a subscription."""
        saved = self._use_grantless
        self._use_grantless = False
        try:
            await self.delete(
                f"{NOTIFICATIONS_BASE}/subscriptions/{notification_type}/{subscription_id}",
                endpoint_name="notifications.deleteSubscription",
            )
        finally:
            self._use_grantless = saved
        log.info(
            "notifications.subscription_deleted",
            notification_type=notification_type,
            subscription_id=subscription_id,
        )

    async def create_subscription_with_filter(
        self,
        notification_type: str,
        destination_id: str,
        marketplace_ids: list[str],
        *,
        payload_version: str = "1.0",
    ) -> dict:
        """Create subscription with processingDirective/eventFilter (for listing types)."""
        body: dict[str, Any] = {
            "destinationId": destination_id,
            "payloadVersion": payload_version,
            "processingDirective": {
                "eventFilter": {
                    "eventFilterType": notification_type,
                    "marketplaceIds": marketplace_ids,
                }
            },
        }
        saved = self._use_grantless
        self._use_grantless = False
        try:
            data = await self.post(
                f"{NOTIFICATIONS_BASE}/subscriptions/{notification_type}",
                body=body,
                endpoint_name="notifications.createSubscription",
            )
        finally:
            self._use_grantless = saved
        sub = data.get("payload", data)
        log.info(
            "notifications.subscription_created",
            notification_type=notification_type,
            subscription_id=sub.get("subscriptionId"),
            destination_id=destination_id,
            has_filter=True,
        )
        return sub
