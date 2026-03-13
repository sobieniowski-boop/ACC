"""
SP-API Finances connector — v2024-06-19.

Uses the unified listTransactions endpoint with hierarchical breakdowns
for fee extraction.  See docs/SP_API_REFERENCE.md §5 for details.

Key constraints:
  - Max 180 days between postedAfter and postedBefore
  - 48 h data lag on recent orders
  - Up to 500 transactions per page
  - Rate limit: 0.5 req/s, burst 10
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import quote

import structlog
from app.connectors.amazon_sp_api.client import SPAPIClient

log = structlog.get_logger(__name__)

# Breakdown types we care about for order-level fee mapping
FBA_FEE_TYPES = frozenset({
    "FBAPerUnitFulfillmentFee",
    "FBAPerOrderFulfillmentFee",
    "FBAWeightBasedFee",
    "FBAPickAndPackFee",
})
REFERRAL_FEE_TYPES = frozenset({
    "Commission",
    "VariableClosingFee",
    "FixedClosingFee",
})
ALL_FEE_TYPES = FBA_FEE_TYPES | REFERRAL_FEE_TYPES


class FinancesClient(SPAPIClient):
    """SP-API Finances v2024-06-19 — listTransactions."""

    # ------------------------------------------------------------------ #
    # Core API call
    # ------------------------------------------------------------------ #
    async def list_transactions(
        self,
        posted_after: datetime,
        posted_before: Optional[datetime] = None,
        marketplace_id: Optional[str] = None,
        related_identifier_name: Optional[str] = None,
        related_identifier_value: Optional[str] = None,
        transaction_status: Optional[str] = None,
        max_pages: int = 500,
    ) -> list[dict]:
        """
        Fetch financial transactions in a date window.

        API constraint: max 180 days between postedAfter and postedBefore.
        Returns raw Transaction dicts from SP-API response.
        Rate limit: 0.5 req/s, burst 10.
        """
        latest_allowed = datetime.now(timezone.utc) - timedelta(minutes=2, seconds=5)
        if posted_before and posted_before > latest_allowed:
            posted_before = latest_allowed
        if posted_after > latest_allowed:
            posted_after = latest_allowed - timedelta(seconds=1)

        params: dict = {}
        if related_identifier_name and related_identifier_value:
            params["relatedIdentifierName"] = related_identifier_name
            params["relatedIdentifierValue"] = related_identifier_value
        else:
            params["postedAfter"] = posted_after.strftime("%Y-%m-%dT%H:%M:%SZ")
        if posted_before:
            params["postedBefore"] = posted_before.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        if marketplace_id:
            params["marketplaceId"] = marketplace_id
        if transaction_status:
            params["transactionStatus"] = transaction_status

        all_txns: list[dict] = []
        next_token: Optional[str] = None

        for page_num in range(max_pages):
            if next_token:
                data = await self.get(
                    "/finances/2024-06-19/transactions",
                    {"nextToken": next_token},
                )
            else:
                data = await self.get(
                    "/finances/2024-06-19/transactions", params
                )

            txns = data.get("transactions", [])
            all_txns.extend(txns)

            log.info(
                "finances.page",
                page=page_num + 1,
                page_count=len(txns),
                total=len(all_txns),
            )

            next_token = data.get("nextToken")
            if not next_token:
                break

            # Respect rate limit: 0.5 req/s → wait 2 s between pages
            await asyncio.sleep(2.0)

        return all_txns

    # ------------------------------------------------------------------ #
    # Parsing helpers (static — usable without client instance)
    # ------------------------------------------------------------------ #

    @staticmethod
    def extract_related_identifier(txn: dict, name: str) -> Optional[str]:
        for ri in txn.get("relatedIdentifiers", []):
            if ri.get("relatedIdentifierName") == name:
                return ri.get("relatedIdentifierValue")
        return None

    @staticmethod
    def extract_order_id(txn: dict) -> Optional[str]:
        """Extract ORDER_ID from transaction relatedIdentifiers."""
        return FinancesClient.extract_related_identifier(txn, "ORDER_ID")

    @staticmethod
    def extract_shipment_id(txn: dict) -> Optional[str]:
        """Extract SHIPMENT_ID from relatedIdentifiers."""
        return FinancesClient.extract_related_identifier(txn, "SHIPMENT_ID")

    @staticmethod
    def extract_settlement_id(txn: dict) -> Optional[str]:
        """Extract SETTLEMENT_ID from relatedIdentifiers."""
        return FinancesClient.extract_related_identifier(txn, "SETTLEMENT_ID")

    @staticmethod
    def extract_financial_event_group_id(txn: dict) -> Optional[str]:
        """Extract FINANCIAL_EVENT_GROUP_ID from relatedIdentifiers."""
        return FinancesClient.extract_related_identifier(txn, "FINANCIAL_EVENT_GROUP_ID")

    @staticmethod
    def flatten_breakdowns(breakdowns: list[dict]) -> list[dict]:
        """
        Recursively flatten nested breakdowns into leaf entries.

        Returns list of {"breakdownType": str, "amount": float, "currency": str}.
        Only *leaf* breakdowns (no sub-children) are emitted.
        """
        results: list[dict] = []
        for bd in breakdowns:
            children = bd.get("breakdowns", [])
            amt = bd.get("breakdownAmount", {})
            amount = float(amt.get("currencyAmount", 0))
            currency = amt.get("currencyCode", "")

            if children:
                # Parent node — recurse into children
                results.extend(FinancesClient.flatten_breakdowns(children))
            else:
                # Leaf node — emit if non-zero
                if abs(amount) > 0.001:
                    results.append({
                        "breakdownType": bd.get("breakdownType", "Unknown"),
                        "amount": amount,
                        "currency": currency,
                    })
        return results

    @staticmethod
    def parse_transaction_fees(txn: dict) -> list[dict]:
        """
        Parse a single v2024-06-19 Transaction dict into flat fee rows
        suitable for acc_finance_transaction inserts.

        Returns list of dicts:
          amazon_order_id, shipment_id, settlement_id, financial_event_group_id,
          sku, charge_type, amount, currency,
          posted_date, transaction_type, description
        """
        order_id = FinancesClient.extract_order_id(txn)
        shipment_id = FinancesClient.extract_shipment_id(txn)
        settlement_id = FinancesClient.extract_settlement_id(txn)
        financial_event_group_id = FinancesClient.extract_financial_event_group_id(txn)

        posted_date_str = txn.get("postedDate", "")
        try:
            posted_date = datetime.fromisoformat(
                posted_date_str.replace("Z", "+00:00")
            )
        except Exception:
            posted_date = datetime.now(timezone.utc)

        txn_type = txn.get("transactionType", "")
        description = txn.get("description", "")

        rows: list[dict] = []

        # --- Item-level breakdowns (preferred — carries SKU context) ---
        for item in txn.get("items", []):
            sku: Optional[str] = None
            for ctx in item.get("contexts", []):
                if ctx.get("sku"):
                    sku = ctx["sku"]
                    break

            breakdowns = FinancesClient.flatten_breakdowns(
                item.get("breakdowns", [])
            )
            for bd in breakdowns:
                rows.append({
                    "amazon_order_id": order_id,
                    "shipment_id": shipment_id,
                    "settlement_id": settlement_id,
                    "financial_event_group_id": financial_event_group_id,
                    "sku": sku,
                    "charge_type": bd["breakdownType"],
                    "amount": bd["amount"],
                    "currency": bd["currency"],
                    "posted_date": posted_date,
                    "transaction_type": txn_type,
                    "description": description,
                })

        # --- Transaction-level breakdowns (fallback when no items) ---
        if not txn.get("items") and txn.get("breakdowns"):
            breakdowns = FinancesClient.flatten_breakdowns(txn["breakdowns"])
            for bd in breakdowns:
                rows.append({
                    "amazon_order_id": order_id,
                    "shipment_id": shipment_id,
                    "settlement_id": settlement_id,
                    "financial_event_group_id": financial_event_group_id,
                    "sku": None,
                    "charge_type": bd["breakdownType"],
                    "amount": bd["amount"],
                    "currency": bd["currency"],
                    "posted_date": posted_date,
                    "transaction_type": txn_type,
                    "description": description,
                })

        return rows

    @staticmethod
    def parse_legacy_event_rows(
        event_type: str,
        event: dict,
        default_currency: str = "EUR",
        financial_event_group_id: str | None = None,
    ) -> list[dict]:
        """
        Parse a legacy v0 financial event into flat rows compatible with
        acc_finance_transaction inserts.
        """
        order_id = event.get("AmazonOrderId")
        posted_date_str = event.get("PostedDate", "")
        try:
            posted_date = datetime.fromisoformat(posted_date_str.replace("Z", "+00:00"))
        except Exception:
            posted_date = datetime.now(timezone.utc)

        rows: list[dict] = []

        def append_row(*, sku: str | None, charge_type: str, amount_obj: dict | None) -> None:
            amount_obj = amount_obj or {}
            amount = float(amount_obj.get("CurrencyAmount", amount_obj.get("Amount", 0)))
            if abs(amount) < 0.001:
                return
            group_id = financial_event_group_id or event.get("FinancialEventGroupId")
            rows.append(
                {
                    "amazon_order_id": order_id,
                    "shipment_id": None,
                    "settlement_id": group_id,
                    "financial_event_group_id": group_id,
                    "sku": sku,
                    "charge_type": charge_type,
                    "amount": amount,
                    "currency": amount_obj.get("CurrencyCode", default_currency),
                    "posted_date": posted_date,
                    "transaction_type": event_type,
                }
            )

        if event_type == "ShipmentEventList":
            for item in event.get("ShipmentItemList", []):
                sku = item.get("SellerSKU")
                for charge in item.get("ItemChargeList", []):
                    append_row(sku=sku, charge_type=charge.get("ChargeType", ""), amount_obj=charge.get("ChargeAmount"))
                for fee in item.get("ItemFeeList", []):
                    append_row(sku=sku, charge_type=fee.get("FeeType", ""), amount_obj=fee.get("FeeAmount"))
        elif event_type == "RefundEventList":
            for item in event.get("ShipmentItemAdjustmentList", []):
                sku = item.get("SellerSKU")
                for charge in item.get("ItemChargeAdjustmentList", []):
                    append_row(sku=sku, charge_type=charge.get("ChargeType", ""), amount_obj=charge.get("ChargeAmount"))
                for fee in item.get("ItemFeeAdjustmentList", []):
                    append_row(sku=sku, charge_type=fee.get("FeeType", ""), amount_obj=fee.get("FeeAmount"))
        elif event_type == "AdjustmentEventList":
            append_row(
                sku=None,
                charge_type=event.get("AdjustmentType", "Adjustment"),
                amount_obj=event.get("AdjustmentAmount"),
            )

        for charge in event.get("ChargeList", []):
            append_row(
                sku=None,
                charge_type=charge.get("ChargeType") or charge.get("FeeType", ""),
                amount_obj=charge.get("ChargeAmount") or charge.get("FeeAmount"),
            )
        for fee in event.get("FeeList", []):
            append_row(sku=None, charge_type=fee.get("FeeType", ""), amount_obj=fee.get("FeeAmount"))

        return rows

    async def list_financial_event_groups(
        self,
        started_after: datetime,
        started_before: Optional[datetime] = None,
        max_pages: int = 100,
    ) -> list[dict]:
        params: dict[str, object] = {
            "FinancialEventGroupStartedAfter": started_after.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        if started_before:
            params["FinancialEventGroupStartedBefore"] = started_before.strftime("%Y-%m-%dT%H:%M:%SZ")
        all_groups: list[dict] = []
        next_token: Optional[str] = None
        for _ in range(max_pages):
            if next_token:
                data = await self.get(
                    "/finances/v0/financialEventGroups",
                    {"NextToken": next_token, "MaxResultsPerPage": 100},
                )
            else:
                params["MaxResultsPerPage"] = 100
                data = await self.get("/finances/v0/financialEventGroups", params)
            payload = data.get("payload", {})
            groups = payload.get("FinancialEventGroupList", [])
            all_groups.extend(groups)
            next_token = payload.get("NextToken")
            if not next_token:
                break
            await asyncio.sleep(2.0)
        return all_groups

    async def list_financial_events_by_group_id(
        self,
        event_group_id: str,
        posted_after: Optional[datetime] = None,
        posted_before: Optional[datetime] = None,
        max_results: int = 500,
    ) -> dict[str, list]:
        params: dict[str, object] = {"MaxResultsPerPage": 100}
        if posted_after:
            params["PostedAfter"] = posted_after.strftime("%Y-%m-%dT%H:%M:%SZ")
        if posted_before:
            params["PostedBefore"] = posted_before.strftime("%Y-%m-%dT%H:%M:%SZ")
        all_events: dict[str, list] = {}
        next_token: Optional[str] = None
        for _ in range(50):
            if next_token:
                data = await self.get(
                    f"/finances/v0/financialEventGroups/{event_group_id}/financialEvents",
                    {"NextToken": next_token, "MaxResultsPerPage": 100},
                )
            else:
                data = await self.get(
                    f"/finances/v0/financialEventGroups/{event_group_id}/financialEvents",
                    params,
                )
            events = data.get("payload", {}).get("FinancialEvents", {})
            for event_type, event_list in events.items():
                all_events.setdefault(event_type, []).extend(event_list)
            next_token = data.get("payload", {}).get("NextToken")
            total = sum(len(v) for v in all_events.values())
            if not next_token or total >= max_results:
                break
            await asyncio.sleep(2.0)
        return all_events

    async def list_financial_events_by_order_id(
        self,
        amazon_order_id: str,
        max_results: int = 500,
    ) -> dict[str, list]:
        """
        Fetch legacy v0 financial events for a specific Amazon order.

        Primary path:
          /finances/v0/orders/{orderId}/financialEvents
        Fallback:
          /finances/v0/financialEvents?AmazonOrderId=...
        """
        order_id = str(amazon_order_id or "").strip()
        if not order_id:
            return {}

        order_path = f"/finances/v0/orders/{quote(order_id, safe='')}/financialEvents"
        all_events: dict[str, list] = {}
        next_token: Optional[str] = None
        use_query_fallback = False

        for _ in range(50):
            params: dict[str, object] = {"MaxResultsPerPage": 100}
            if next_token:
                params = {"NextToken": next_token, "MaxResultsPerPage": 100}
            elif use_query_fallback:
                params["AmazonOrderId"] = order_id

            if use_query_fallback:
                data = await self.get("/finances/v0/financialEvents", params)
            else:
                try:
                    data = await self.get(order_path, params)
                except Exception as exc:
                    # Some seller contexts reject the order-path variant.
                    if next_token is None:
                        log.warning(
                            "finances.by_order_path_failed_fallback_query",
                            order_id=order_id,
                            error=str(exc),
                        )
                        use_query_fallback = True
                        data = await self.get(
                            "/finances/v0/financialEvents",
                            {"AmazonOrderId": order_id, "MaxResultsPerPage": 100},
                        )
                    else:
                        raise

            events = data.get("payload", {}).get("FinancialEvents", {})
            for event_type, event_list in events.items():
                all_events.setdefault(event_type, []).extend(event_list)

            next_token = data.get("payload", {}).get("NextToken")
            total = sum(len(v) for v in all_events.values())
            if not next_token or total >= max_results:
                break
            await asyncio.sleep(2.0)

        return all_events

    # ------------------------------------------------------------------ #
    # Legacy v0 wrapper (kept for backward compat — do NOT use for new code)
    # ------------------------------------------------------------------ #
    async def list_financial_events(
        self,
        posted_after: Optional[datetime] = None,
        posted_before: Optional[datetime] = None,
        max_results: int = 500,
    ) -> dict[str, list]:
        """DEPRECATED — v0 endpoint.  Use list_transactions() instead."""
        log.warning("finances.v0_deprecated — migrate to list_transactions()")
        if posted_after is None:
            posted_after = datetime.now(timezone.utc) - timedelta(days=7)
        params: dict = {
            "PostedAfter": posted_after.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "MaxResultsPerPage": 100,
        }
        if posted_before:
            params["PostedBefore"] = posted_before.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        all_events: dict[str, list] = {}
        next_token = None
        for _ in range(50):
            if next_token:
                data = await self.get(
                    "/finances/v0/financialEvents",
                    {"NextToken": next_token, "MaxResultsPerPage": 100},
                )
            else:
                data = await self.get("/finances/v0/financialEvents", params)
            events = data.get("payload", {}).get("FinancialEvents", {})
            for event_type, event_list in events.items():
                all_events.setdefault(event_type, []).extend(event_list)
            next_token = data.get("payload", {}).get("NextToken")
            total = sum(len(v) for v in all_events.values())
            if not next_token or total >= max_results:
                break
        return all_events
