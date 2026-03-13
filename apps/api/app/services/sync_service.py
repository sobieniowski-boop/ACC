"""
Amazon SP-API ↔ MSSQL sync service.

Provides async functions to pull data from Amazon SP-API and upsert into
acc_* tables in NetfoxAnalityka MSSQL. Designed to work WITHOUT Celery/Redis —
can be called from:
  - FastAPI BackgroundTasks (via /api/v1/jobs/run endpoint)
  - Standalone sync_runner.py CLI (Windows Task Scheduler)

Each sync function:
  1. Accepts an optional JobRun ID to track progress
  2. Iterates over all active marketplaces (or a specific one)
  3. Calls the relevant SP-API connector
  4. Upserts data into SQL using merge/dedup logic
  5. Returns count of records processed

Rate-limit strategy:
  - SQL tables act as cache → never call API for data already in DB
  - Configurable days_back to limit API calls
  - Sleep between marketplace iterations
  - SP-API client handles 429 retries automatically
"""
from __future__ import annotations

import asyncio
import json
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

import structlog
from sqlalchemy import select, and_, func, or_, update, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects import mssql

from app.core.database import AsyncSessionLocal
from app.models.marketplace import Marketplace
from app.models.order import AccOrder, OrderLine
from app.models.product import Product
from app.models.offer import Offer
from app.models.inventory import InventorySnapshot
from app.models.finance import FinanceTransaction
from app.models.exchange_rate import ExchangeRate
from app.models.job import JobRun

from app.connectors.amazon_sp_api.orders import OrdersClient
from app.connectors.amazon_sp_api.inventory import InventoryClient
from app.connectors.amazon_sp_api.finances import FinancesClient
from app.connectors.amazon_sp_api.catalog import CatalogClient, parse_catalog_item
from app.connectors.amazon_sp_api.pricing_api import (
    PricingClient,
    parse_competitive_pricing,
    parse_pricing_response,
)
from app.connectors.amazon_sp_api.reports import ReportsClient, ReportType, parse_tsv_report
from app.connectors.nbp import fetch_nbp_rate, fetch_all_currencies
from app.core.config import settings
from app.core.db_connection import connect_acc, connect_netfox

log = structlog.get_logger(__name__)
SQL_IN_CHUNK = 1000
PRICING_BUYBOX_ASIN_LIMIT = 200

FEE_EXPECTED_SOURCE = "product_fees_v0"
FEE_ESTIMATE_FBA_TYPES = {
    "FBAFees",
    "FBAPerUnitFulfillmentFee",
    "FBAPerOrderFulfillmentFee",
    "FBAWeightBasedFee",
    "FBAPickAndPackFee",
}
FEE_ESTIMATE_REFERRAL_TYPES = {
    "ReferralFee",
    "Commission",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _get_active_marketplaces(db: AsyncSession) -> list[Marketplace]:
    """Get all active marketplace records."""
    result = await db.execute(
        select(Marketplace).where(Marketplace.is_active == True)  # noqa: E712
    )
    return list(result.scalars().all())


async def _update_job(db: AsyncSession, job_id: Optional[str], **kwargs):
    """Update JobRun record using raw SQL (old ODBC driver safe)."""
    if not job_id:
        return
    from sqlalchemy import text

    # Build SET clause dynamically — cast typed values for old ODBC driver
    set_parts = []
    params = {"job_id": str(job_id)}
    for k, v in kwargs.items():
        param_name = f"p_{k}"
        if isinstance(v, datetime):
            set_parts.append(f"{k} = CAST(:{param_name} AS DATETIME2)")
            params[param_name] = v.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(v, Decimal):
            set_parts.append(f"{k} = CAST(:{param_name} AS DECIMAL(10,2))")
            params[param_name] = float(v)
        elif isinstance(v, uuid.UUID):
            set_parts.append(f"{k} = CAST(:{param_name} AS UNIQUEIDENTIFIER)")
            params[param_name] = str(v)
        else:
            set_parts.append(f"{k} = :{param_name}")
            # Ensure strings are non-empty for old driver
            if isinstance(v, str) and v == "":
                v = "-"
            params[param_name] = v

    if not set_parts:
        return

    sql = f"UPDATE acc_job_run SET {', '.join(set_parts)} WHERE id = CAST(:job_id AS UNIQUEIDENTIFIER)"
    try:
        await db.execute(text(sql), params)
        await db.commit()
    except Exception as e:
        log.warning("_update_job.error", job_id=job_id, error=str(e))


def _apply_buybox_updates_sync(marketplace_id: str, parsed_rows: list[dict]) -> int:
    """Apply buybox/BSR updates via sync connector to avoid aioodbc hstmt contention."""
    if not parsed_rows:
        return 0
    conn = connect_acc(timeout=30, autocommit=False)
    cur = None
    try:
        cur = conn.cursor()
        updated = 0
        sql = """
            UPDATE dbo.acc_offer
            SET
                has_buybox = ?,
                buybox_price = COALESCE(?, buybox_price),
                bsr_rank = COALESCE(?, bsr_rank),
                bsr_category = COALESCE(?, bsr_category),
                updated_at = SYSUTCDATETIME()
            WHERE marketplace_id = ? AND asin = ?
        """
        for row in parsed_rows:
            asin = str(row.get("asin") or "").strip()
            if not asin:
                continue
            has_buybox = 1 if bool(row.get("has_buybox", False)) else 0
            buybox_price = row.get("buybox_price")
            if buybox_price is not None:
                buybox_price = float(Decimal(str(buybox_price)))
            bsr_rank = row.get("bsr_rank")
            bsr_rank = int(bsr_rank) if bsr_rank is not None else None
            bsr_category = row.get("bsr_category")
            cur.execute(
                sql,
                has_buybox,
                buybox_price,
                bsr_rank,
                bsr_category,
                marketplace_id,
                asin,
            )
            updated += 1
        conn.commit()
        return updated
    except Exception:
        conn.rollback()
        raise
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        conn.close()


def _chunked(values: list[str], size: int = SQL_IN_CHUNK):
    for start in range(0, len(values), size):
        yield values[start:start + size]


def _load_listing_registry_index(
    *,
    skus: list[str],
    asins: list[str],
) -> tuple[dict[str, str], dict[str, str]]:
    """
    Load lightweight listing registry lookup:
      - merchant_sku / alt -> internal_sku
      - asin -> internal_sku

    Keep this as a raw sync query to avoid ORM overhead for an unmodeled table.
    """
    sku_to_internal: dict[str, str] = {}
    asin_to_internal: dict[str, str] = {}
    if not skus and not asins:
        return sku_to_internal, asin_to_internal

    conn = connect_acc(autocommit=True, timeout=30)
    try:
        cur = conn.cursor()

        for sku_chunk in _chunked(sorted({v for v in skus if v})):
            placeholders = ",".join(["?"] * len(sku_chunk))
            cur.execute(
                f"""
                SELECT merchant_sku, merchant_sku_alt, internal_sku, asin
                FROM dbo.acc_amazon_listing_registry WITH (NOLOCK)
                WHERE merchant_sku IN ({placeholders})
                   OR merchant_sku_alt IN ({placeholders})
                """,
                sku_chunk + sku_chunk,
            )
            for merchant_sku, merchant_sku_alt, internal_sku, asin in cur.fetchall():
                internal = (str(internal_sku or "")).strip()
                if not internal:
                    continue
                if merchant_sku:
                    sku_to_internal[str(merchant_sku).strip()] = internal
                if merchant_sku_alt:
                    sku_to_internal[str(merchant_sku_alt).strip()] = internal
                if asin:
                    asin_to_internal[str(asin).strip()] = internal

        for asin_chunk in _chunked(sorted({v for v in asins if v})):
            placeholders = ",".join(["?"] * len(asin_chunk))
            cur.execute(
                f"""
                SELECT merchant_sku, merchant_sku_alt, internal_sku, asin
                FROM dbo.acc_amazon_listing_registry WITH (NOLOCK)
                WHERE asin IN ({placeholders})
                """,
                asin_chunk,
            )
            for merchant_sku, merchant_sku_alt, internal_sku, asin in cur.fetchall():
                internal = (str(internal_sku or "")).strip()
                if not internal:
                    continue
                if merchant_sku:
                    sku_to_internal.setdefault(str(merchant_sku).strip(), internal)
                if merchant_sku_alt:
                    sku_to_internal.setdefault(str(merchant_sku_alt).strip(), internal)
                if asin:
                    asin_to_internal[str(asin).strip()] = internal
    finally:
        conn.close()

    return sku_to_internal, asin_to_internal


def _money_amount(value: object) -> float:
    """Parse Amazon money node variants into float amount."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    if isinstance(value, dict):
        for key in ("Amount", "CurrencyAmount", "amount", "amountValue"):
            node = value.get(key)
            if node is not None and node != "":
                try:
                    return float(node)
                except Exception:
                    continue
        # Some payloads can be nested one level deeper.
        for key in ("FinalFee", "FeeAmount"):
            nested = value.get(key)
            amt = _money_amount(nested)
            if amt > 0:
                return amt
        return 0.0
    try:
        return float(str(value))
    except Exception:
        return 0.0


def _parse_fee_estimate_result(result: dict, *, offer_price: float) -> dict[str, float | str | list[dict]]:
    """
    Parse Product Fees v0 response into expected FBA/referral components.

    Returns amounts in the offer currency (the same currency used in request).
    """
    status = str(result.get("Status") or "UNKNOWN").upper()
    fees_node = result.get("FeesEstimate") or {}
    details = fees_node.get("FeeDetailList") or []

    fba_fee = 0.0
    referral_fee = 0.0
    other_fee = 0.0
    parsed_details: list[dict] = []

    for detail in details:
        fee_type = str(detail.get("FeeType") or "").strip()
        amount = _money_amount(detail.get("FinalFee") or detail.get("FeeAmount"))
        if amount <= 0:
            continue

        fee_type_upper = fee_type.upper()
        if fee_type in FEE_ESTIMATE_REFERRAL_TYPES or "REFERRAL" in fee_type_upper or "COMMISSION" in fee_type_upper:
            referral_fee += amount
        elif (
            fee_type in FEE_ESTIMATE_FBA_TYPES
            or "FBA" in fee_type_upper
            or "FULFILLMENT" in fee_type_upper
            or "PICKANDPACK" in fee_type_upper
        ):
            fba_fee += amount
        else:
            other_fee += amount

        parsed_details.append(
            {
                "fee_type": fee_type,
                "amount": round(amount, 4),
            }
        )

    total_fee = _money_amount(fees_node.get("TotalFeesEstimate"))
    if total_fee <= 0:
        total_fee = fba_fee + referral_fee + other_fee

    referral_rate = 0.0
    if offer_price > 0 and referral_fee > 0:
        referral_rate = referral_fee / offer_price

    return {
        "status": status,
        "fba_fee": round(fba_fee, 4),
        "referral_fee": round(referral_fee, 4),
        "other_fee": round(other_fee, 4),
        "total_fee": round(total_fee, 4),
        "referral_rate": round(referral_rate, 6),
        "details": parsed_details,
    }


async def _ensure_offer_fee_expected_schema(db: AsyncSession) -> None:
    """Create expected-fee cache table if missing (MSSQL safe, idempotent)."""
    await db.execute(
        text(
            """
            IF OBJECT_ID('dbo.acc_offer_fee_expected', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.acc_offer_fee_expected (
                    id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
                    marketplace_id NVARCHAR(64) NOT NULL,
                    sku NVARCHAR(120) NOT NULL,
                    asin NVARCHAR(32) NULL,
                    offer_price DECIMAL(18,4) NOT NULL,
                    currency NVARCHAR(8) NOT NULL,
                    fulfillment_channel NVARCHAR(20) NULL,
                    expected_fba_fee DECIMAL(18,4) NULL,
                    expected_referral_fee DECIMAL(18,4) NULL,
                    expected_total_fee DECIMAL(18,4) NULL,
                    expected_referral_rate DECIMAL(18,6) NULL,
                    fee_detail_json NVARCHAR(MAX) NULL,
                    source NVARCHAR(40) NOT NULL DEFAULT 'product_fees_v0',
                    status NVARCHAR(32) NOT NULL DEFAULT 'ok',
                    error_message NVARCHAR(1000) NULL,
                    synced_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
                );
            END;

            IF NOT EXISTS (
                SELECT 1
                FROM sys.indexes
                WHERE name = 'UX_acc_offer_fee_expected_key'
                  AND object_id = OBJECT_ID('dbo.acc_offer_fee_expected')
            )
            BEGIN
                CREATE UNIQUE INDEX UX_acc_offer_fee_expected_key
                    ON dbo.acc_offer_fee_expected(marketplace_id, sku, source);
            END;

            IF NOT EXISTS (
                SELECT 1
                FROM sys.indexes
                WHERE name = 'IX_acc_offer_fee_expected_lookup'
                  AND object_id = OBJECT_ID('dbo.acc_offer_fee_expected')
            )
            BEGIN
                CREATE INDEX IX_acc_offer_fee_expected_lookup
                    ON dbo.acc_offer_fee_expected(marketplace_id, sku, synced_at DESC);
            END;
            """
        )
    )
    await db.commit()


async def _upsert_offer_fee_expected(
    db: AsyncSession,
    *,
    marketplace_id: str,
    sku: str,
    asin: str | None,
    offer_price: float,
    currency: str,
    fulfillment_channel: str | None,
    expected_fba_fee: float | None,
    expected_referral_fee: float | None,
    expected_total_fee: float | None,
    expected_referral_rate: float | None,
    status: str,
    error_message: str | None,
    fee_details: list[dict] | None = None,
) -> None:
    """Upsert expected fee cache row (2-step upsert; avoids fragile MERGE races)."""
    await db.execute(
        text(
            """
            UPDATE dbo.acc_offer_fee_expected
            SET asin = :asin,
                offer_price = :offer_price,
                currency = :currency,
                fulfillment_channel = :fulfillment_channel,
                expected_fba_fee = :expected_fba_fee,
                expected_referral_fee = :expected_referral_fee,
                expected_total_fee = :expected_total_fee,
                expected_referral_rate = :expected_referral_rate,
                fee_detail_json = :fee_detail_json,
                status = :status,
                error_message = :error_message,
                synced_at = SYSUTCDATETIME()
            WHERE marketplace_id = :marketplace_id
              AND sku = :sku
              AND source = :source;

            IF @@ROWCOUNT = 0
            BEGIN
                INSERT INTO dbo.acc_offer_fee_expected (
                    marketplace_id, sku, asin, offer_price, currency, fulfillment_channel,
                    expected_fba_fee, expected_referral_fee, expected_total_fee,
                    expected_referral_rate, fee_detail_json, source, status, error_message, synced_at
                )
                VALUES (
                    :marketplace_id, :sku, :asin, :offer_price, :currency, :fulfillment_channel,
                    :expected_fba_fee, :expected_referral_fee, :expected_total_fee,
                    :expected_referral_rate, :fee_detail_json, :source, :status, :error_message,
                    SYSUTCDATETIME()
                );
            END
            """
        ),
        {
            "marketplace_id": marketplace_id,
            "sku": sku,
            "asin": asin,
            "offer_price": float(offer_price or 0),
            "currency": currency or "EUR",
            "fulfillment_channel": fulfillment_channel,
            "expected_fba_fee": expected_fba_fee,
            "expected_referral_fee": expected_referral_fee,
            "expected_total_fee": expected_total_fee,
            "expected_referral_rate": expected_referral_rate,
            "fee_detail_json": json.dumps(fee_details or [], ensure_ascii=False),
            "source": FEE_EXPECTED_SOURCE,
            "status": status,
            "error_message": (error_message or "")[:1000] or None,
        },
    )


async def _find_or_create_product(
    db: AsyncSession,
    sku: Optional[str] = None,
    asin: Optional[str] = None,
) -> Optional[Product]:
    """Find existing product by SKU or ASIN, or create a new one."""
    sku = (sku or "").strip() or None
    asin = (asin or "").strip() or None
    if not sku and not asin:
        return None

    # Try by SKU first
    if sku:
        result = await db.execute(select(Product).where(Product.sku == sku))
        product = result.scalar_one_or_none()
        if product:
            return product

    # Try by ASIN
    if asin:
        result = await db.execute(select(Product).where(Product.asin == asin))
        product = result.scalar_one_or_none()
        if product:
            # Update SKU if we have it now
            if sku and not product.sku:
                product.sku = sku
            return product

    # Create new product
    product = Product(id=uuid.uuid4(), sku=sku, asin=asin)
    db.add(product)
    try:
        await db.flush()
        return product
    except IntegrityError:
        await db.rollback()
        if sku:
            result = await db.execute(select(Product).where(Product.sku == sku))
            existing = result.scalar_one_or_none()
            if existing:
                return existing
        if asin:
            result = await db.execute(select(Product).where(Product.asin == asin))
            existing = result.scalar_one_or_none()
            if existing:
                return existing
        raise


# ---------------------------------------------------------------------------
# 1. SYNC ORDERS
# ---------------------------------------------------------------------------

async def sync_orders(
    marketplace_id: Optional[str] = None,
    days_back: int = 7,
    job_id: Optional[str] = None,
    sync_items: bool = True,
) -> int:
    """
    Sync orders from SP-API → acc_order + acc_order_line.

    For each marketplace:
      1. GET /orders/v0/orders (created in last N days)
      2. Upsert into acc_order (dedup by amazon_order_id)
      3. GET /orders/v0/orders/{id}/orderItems
      4. Upsert into acc_order_line (dedup by amazon_order_item_id)
      5. Link to acc_product by SKU/ASIN

    Returns total orders synced.
    """
    async with AsyncSessionLocal() as db:
        await _update_job(db, job_id, status="running",
                          started_at=datetime.now(timezone.utc))

        marketplaces = await _get_active_marketplaces(db)
        if marketplace_id:
            marketplaces = [m for m in marketplaces if m.id == marketplace_id]

        created_after = datetime.now(timezone.utc) - timedelta(days=days_back)
        total_orders = 0
        total_items = 0

        for mkt in marketplaces:
            try:
                client = OrdersClient(marketplace_id=mkt.id)
                raw_orders = await client.get_orders(
                    created_after=created_after,
                    statuses=["Shipped", "Unshipped", "PartiallyShipped"],
                    max_results=500,
                )

                for raw in raw_orders:
                    amazon_id = raw.get("AmazonOrderId", "")
                    if not amazon_id:
                        continue

                    # Upsert order
                    existing = await db.execute(
                        select(AccOrder).where(AccOrder.amazon_order_id == amazon_id)
                    )
                    order = existing.scalar_one_or_none()

                    if order is None:
                        order = AccOrder(
                            id=uuid.uuid4(),
                            amazon_order_id=amazon_id,
                            marketplace_id=mkt.id,
                        )
                        db.add(order)

                    # Map fields
                    order.status = raw.get("OrderStatus", "")
                    order.fulfillment_channel = raw.get("FulfillmentChannel", "FBA")
                    order.sales_channel = raw.get("SalesChannel")

                    purchase_date = raw.get("PurchaseDate", "")
                    if purchase_date:
                        order.purchase_date = datetime.fromisoformat(
                            purchase_date.replace("Z", "+00:00")
                        )

                    last_update = raw.get("LastUpdateDate", "")
                    if last_update:
                        order.last_update_date = datetime.fromisoformat(
                            last_update.replace("Z", "+00:00")
                        )

                    order_total = raw.get("OrderTotal", {})
                    if order_total:
                        order.order_total = Decimal(str(order_total.get("Amount", 0)))
                        order.currency = order_total.get("CurrencyCode", "EUR")

                    ship_addr = raw.get("ShippingAddress", {})
                    if ship_addr:
                        order.ship_country = ship_addr.get("CountryCode")
                        order.buyer_country = ship_addr.get("CountryCode")

                    order.synced_at = datetime.now(timezone.utc)
                    total_orders += 1

                    # Sync order items
                    if sync_items:
                        try:
                            raw_items = await client.get_order_items(amazon_id)
                            for raw_item in raw_items:
                                item_id = raw_item.get("OrderItemId", "")
                                if not item_id:
                                    continue

                                # Dedup by order + item ID
                                existing_item = await db.execute(
                                    select(OrderLine).where(
                                        OrderLine.order_id == order.id,
                                        OrderLine.amazon_order_item_id == item_id,
                                    )
                                )
                                line = existing_item.scalar_one_or_none()

                                if line is None:
                                    line = OrderLine(
                                        id=uuid.uuid4(),
                                        order_id=order.id,
                                        amazon_order_item_id=item_id,
                                    )
                                    db.add(line)

                                line.sku = raw_item.get("SellerSKU")
                                line.asin = raw_item.get("ASIN")
                                line.title = (raw_item.get("Title") or "")[:500]
                                line.quantity_ordered = raw_item.get("QuantityOrdered", 1)
                                line.quantity_shipped = raw_item.get("QuantityShipped", 0)

                                # Prices
                                item_price = raw_item.get("ItemPrice", {})
                                if item_price:
                                    line.item_price = Decimal(str(
                                        item_price.get("Amount", 0)
                                    ))
                                    line.currency = item_price.get("CurrencyCode", order.currency)

                                item_tax = raw_item.get("ItemTax", {})
                                if item_tax:
                                    line.item_tax = Decimal(str(
                                        item_tax.get("Amount", 0)
                                    ))

                                promo = raw_item.get("PromotionDiscount", {})
                                if promo:
                                    line.promotion_discount = Decimal(str(
                                        promo.get("Amount", 0)
                                    ))

                                # Link to product
                                product = await _find_or_create_product(
                                    db, sku=line.sku, asin=line.asin
                                )
                                if product:
                                    line.product_id = product.id

                                total_items += 1

                        except Exception as e:
                            log.warning("sync_orders.items_error",
                                        order=amazon_id, error=str(e))

                await db.flush()
                log.info("sync_orders.marketplace_done",
                         marketplace=mkt.code, orders=len(raw_orders))

            except Exception as e:
                log.error("sync_orders.marketplace_error",
                          marketplace=mkt.code, error=str(e))

        await db.commit()
        await _update_job(db, job_id,
                          status="success",
                          records_processed=total_orders,
                          progress_pct=100,
                          progress_message=f"{total_orders} orders, {total_items} items",
                          finished_at=datetime.now(timezone.utc))

        log.info("sync_orders.complete", orders=total_orders, items=total_items)
        return total_orders


# ---------------------------------------------------------------------------
# 2. SYNC INVENTORY
# ---------------------------------------------------------------------------

async def sync_inventory(
    marketplace_id: Optional[str] = None,
    job_id: Optional[str] = None,
) -> int:
    """
    Sync FBA inventory from SP-API → acc_inventory_snapshot.

    Creates daily snapshots. Deduplicates by (marketplace, sku, date).
    """
    async with AsyncSessionLocal() as db:
        await _update_job(db, job_id, status="running",
                          started_at=datetime.now(timezone.utc))

        marketplaces = await _get_active_marketplaces(db)
        if marketplace_id:
            marketplaces = [m for m in marketplaces if m.id == marketplace_id]

        today = date.today()
        total = 0

        for mkt in marketplaces:
            try:
                client = InventoryClient(marketplace_id=mkt.id)
                summaries = await client.get_inventory_summaries()

                for s in summaries:
                    sku = s.get("sellerSku", "")
                    if not sku:
                        continue

                    fnsku = s.get("fnSku")
                    asin = s.get("asin")
                    inv = s.get("inventoryDetails", {})

                    fulfillable = inv.get("fulfillableQuantity", 0)
                    reserved_obj = inv.get("reservedQuantity", {})
                    reserved = reserved_obj.get("totalReservedQuantity", 0) if isinstance(reserved_obj, dict) else 0
                    inbound = (
                        inv.get("inboundWorkingQuantity", 0)
                        + inv.get("inboundShippedQuantity", 0)
                        + inv.get("inboundReceivingQuantity", 0)
                    )
                    unfulfillable_obj = inv.get("unfulfillableQuantity", {})
                    unfulfillable = unfulfillable_obj.get("totalUnfulfillableQuantity", 0) if isinstance(unfulfillable_obj, dict) else 0

                    # Dedup: check if snapshot already exists for this sku+mkt+date
                    existing = await db.execute(
                        select(InventorySnapshot).where(
                            InventorySnapshot.marketplace_id == mkt.id,
                            InventorySnapshot.sku == sku,
                            InventorySnapshot.snapshot_date == today,
                        )
                    )
                    snap = existing.scalar_one_or_none()

                    if snap is None:
                        # Find/create product
                        product = await _find_or_create_product(db, sku=sku, asin=asin)

                        snap = InventorySnapshot(
                            id=uuid.uuid4(),
                            product_id=product.id if product else None,
                            marketplace_id=mkt.id,
                            snapshot_date=today,
                            sku=sku,
                            fnsku=fnsku,
                            asin=asin,
                        )
                        db.add(snap)

                    snap.qty_fulfillable = fulfillable
                    snap.qty_reserved = reserved
                    snap.qty_inbound = inbound
                    snap.qty_unfulfillable = unfulfillable
                    snap.synced_at = datetime.now(timezone.utc)

                    total += 1

                await db.flush()
                log.info("sync_inventory.marketplace_done",
                         marketplace=mkt.code, count=len(summaries))

            except Exception as e:
                log.error("sync_inventory.marketplace_error",
                          marketplace=mkt.code, error=str(e))

        await db.commit()
        await _update_job(db, job_id,
                          status="success",
                          records_processed=total,
                          progress_pct=100,
                          finished_at=datetime.now(timezone.utc))

        log.info("sync_inventory.complete", total=total)
        return total


# ---------------------------------------------------------------------------
# 3. SYNC PRICING / OFFERS
# ---------------------------------------------------------------------------

async def sync_pricing(
    marketplace_id: Optional[str] = None,
    job_id: Optional[str] = None,
) -> int:
    """
    Sync pricing & BuyBox data from SP-API → acc_offer.

    Uses GET_MERCHANT_LISTINGS_ALL_DATA report for base listing data,
    then competitive pricing API for BuyBox status.
    """
    async with AsyncSessionLocal() as db:
        await _update_job(db, job_id, status="running",
                          started_at=datetime.now(timezone.utc))

        marketplaces = await _get_active_marketplaces(db)
        if marketplace_id:
            marketplaces = [m for m in marketplaces if m.id == marketplace_id]

        total = 0
        failed_marketplaces: list[str] = []
        empty_marketplaces: list[str] = []

        for mkt in marketplaces:
            mkt_code = mkt.code
            try:
                # Step 1: Get all active listings via Reports API
                reports_client = ReportsClient(marketplace_id=mkt.id)
                try:
                    content = await reports_client.request_and_download(
                        report_type=ReportType.ACTIVE_LISTINGS,
                        marketplace_ids=[mkt.id],
                        poll_interval=15.0,
                    )
                    listings = parse_tsv_report(content)
                    log.info("sync_pricing.listings_report",
                             marketplace=mkt_code, count=len(listings))
                except Exception as e:
                    log.warning("sync_pricing.report_failed",
                                marketplace=mkt_code, error=str(e))
                    failed_marketplaces.append(mkt_code)
                    listings = []

                # Step 2: Upsert listings into acc_offer
                asins_for_pricing: list[str] = []
                marketplace_total = 0
                skipped_missing_identity = 0
                skipped_unmapped = 0
                limited_buybox_count = 0

                listing_skus = sorted(
                    {
                        (row.get("seller-sku") or row.get("sku") or "").strip()
                        for row in listings
                        if (row.get("seller-sku") or row.get("sku") or "").strip()
                    }
                )
                listing_asins = sorted(
                    {
                        (row.get("asin1") or row.get("asin") or "").strip()
                        for row in listings
                        if (row.get("asin1") or row.get("asin") or "").strip()
                    }
                )

                registry_internal_by_sku, registry_internal_by_asin = _load_listing_registry_index(
                    skus=listing_skus,
                    asins=listing_asins,
                )
                registry_internal_values = sorted(
                    {
                        v
                        for v in (
                            list(registry_internal_by_sku.values())
                            + list(registry_internal_by_asin.values())
                        )
                        if v
                    }
                )

                products_by_sku: dict[str, Product] = {}
                products_by_asin: dict[str, Product] = {}
                products_by_internal: dict[str, Product] = {}
                if listing_skus or listing_asins:
                    for sku_chunk in _chunked(listing_skus):
                        product_result = await db.execute(select(Product).where(Product.sku.in_(sku_chunk)))
                        for product in product_result.scalars().all():
                            if product.sku:
                                products_by_sku[product.sku] = product
                            if product.asin:
                                products_by_asin[product.asin] = product
                    for asin_chunk in _chunked(listing_asins):
                        product_result = await db.execute(select(Product).where(Product.asin.in_(asin_chunk)))
                        for product in product_result.scalars().all():
                            if product.sku:
                                products_by_sku[product.sku] = product
                            if product.asin:
                                products_by_asin[product.asin] = product
                    for internal_chunk in _chunked(registry_internal_values):
                        product_result = await db.execute(
                            select(Product).where(Product.internal_sku.in_(internal_chunk))
                        )
                        for product in product_result.scalars().all():
                            if product.internal_sku:
                                products_by_internal[product.internal_sku] = product
                            if product.sku:
                                products_by_sku.setdefault(product.sku, product)
                            if product.asin:
                                products_by_asin.setdefault(product.asin, product)

                offers_by_sku: dict[str, Offer] = {}
                if listing_skus:
                    for sku_chunk in _chunked(listing_skus):
                        offer_result = await db.execute(
                            select(Offer).where(
                                Offer.marketplace_id == mkt.id,
                                Offer.sku.in_(sku_chunk),
                            )
                        )
                        for offer in offer_result.scalars().all():
                            offers_by_sku[offer.sku] = offer

                for row in listings:
                    sku = (row.get("seller-sku") or row.get("sku") or "").strip()
                    asin = (row.get("asin1") or row.get("asin") or "").strip()
                    if not sku:
                        continue

                    product = products_by_sku.get(sku) or (products_by_asin.get(asin) if asin else None)
                    if product is None:
                        internal_sku = registry_internal_by_sku.get(sku)
                        if not internal_sku and asin:
                            internal_sku = registry_internal_by_asin.get(asin)
                        if internal_sku:
                            product = products_by_internal.get(internal_sku)

                    if product is None:
                        if not asin:
                            skipped_missing_identity += 1
                        else:
                            skipped_unmapped += 1
                        continue

                    # Upsert offer
                    offer = offers_by_sku.get(sku)

                    if offer is None:
                        offer = Offer(
                            id=uuid.uuid4(),
                            product_id=product.id,
                            marketplace_id=mkt.id,
                            sku=sku,
                        )
                        db.add(offer)
                        offers_by_sku[sku] = offer

                    offer.asin = asin
                    offer.fnsku = row.get("fulfillment-channel-sku") or row.get("fnsku")
                    offer.product_id = product.id

                    # Price from listing
                    price_str = row.get("price") or row.get("your-price") or "0"
                    try:
                        offer.price = Decimal(price_str)
                    except Exception:
                        pass

                    offer.currency = mkt.currency
                    offer.status = row.get("status") or row.get("item-condition") or "Active"

                    fc = row.get("fulfillment-channel") or ""
                    if "AMAZON" in fc.upper() or "AFN" in fc.upper():
                        offer.fulfillment_channel = "FBA"
                    elif fc:
                        offer.fulfillment_channel = "FBM"

                    offer.last_synced_at = datetime.now(timezone.utc)

                    if asin:
                        asins_for_pricing.append(asin)

                    total += 1
                    marketplace_total += 1

                if marketplace_total == 0 and mkt_code not in failed_marketplaces:
                    empty_marketplaces.append(mkt_code)

                await db.flush()
                await db.commit()

                # Step 3: Get competitive pricing (BuyBox) for ASINs in batches
                if asins_for_pricing:
                    unique_asins = list(set(asins_for_pricing))
                    if len(unique_asins) > PRICING_BUYBOX_ASIN_LIMIT:
                        limited_buybox_count = len(unique_asins) - PRICING_BUYBOX_ASIN_LIMIT
                        unique_asins = unique_asins[:PRICING_BUYBOX_ASIN_LIMIT]
                    pricing_client = PricingClient(marketplace_id=mkt.id)

                    try:
                        pricing_results = await pricing_client.get_competitive_pricing_batch(
                            unique_asins
                        )
                        parsed_rows = [parse_competitive_pricing(pr) for pr in pricing_results]
                        updated_rows = await asyncio.to_thread(
                            _apply_buybox_updates_sync,
                            str(mkt.id),
                            parsed_rows,
                        )
                        log.info("sync_pricing.buybox_done",
                                 marketplace=mkt_code, asins=len(unique_asins), updated_rows=updated_rows, skipped_buybox=limited_buybox_count)

                    except Exception as e:
                        log.warning("sync_pricing.buybox_error",
                                    marketplace=mkt_code, error=str(e))

                    # Step 3.5: Capture competitor offers for top ASINs
                    try:
                        from app.intelligence.buybox_radar import capture_competitor_offers
                        capture_result = await capture_competitor_offers(
                            str(mkt.id), asin_limit=min(50, len(unique_asins)),
                        )
                        log.info("sync_pricing.competitor_capture_done",
                                 marketplace=mkt_code,
                                 asins=capture_result.get("asins_sampled", 0),
                                 offers=capture_result.get("offers_recorded", 0))
                    except Exception as e:
                        log.warning("sync_pricing.competitor_capture_error",
                                    marketplace=mkt_code, error=str(e))

                log.info("sync_pricing.marketplace_done",
                         marketplace=mkt_code,
                         offers=marketplace_total,
                         skipped_missing_identity=skipped_missing_identity,
                         skipped_unmapped=skipped_unmapped,
                         skipped_buybox=limited_buybox_count)

            except Exception as e:
                log.error("sync_pricing.marketplace_error",
                          marketplace=mkt_code, error=str(e))
                failed_marketplaces.append(mkt_code)

        if total == 0:
            failure_detail = []
            if failed_marketplaces:
                failure_detail.append(f"report_failed={','.join(sorted(set(failed_marketplaces)))}")
            if empty_marketplaces:
                failure_detail.append(f"empty={','.join(sorted(set(empty_marketplaces)))}")
            detail = "; ".join(failure_detail) or "no_offers_imported"
            await _update_job(
                db,
                job_id,
                status="failure",
                records_processed=0,
                progress_pct=100,
                finished_at=datetime.now(timezone.utc),
                error_message=f"sync_pricing imported 0 offers ({detail})",
            )
            log.warning("sync_pricing.complete_zero", detail=detail)
            return 0

        progress_message = f"Pricing synced offers={total}"
        if empty_marketplaces:
            progress_message += f", empty={','.join(sorted(set(empty_marketplaces)))}"
        if failed_marketplaces:
            progress_message += f", failed={','.join(sorted(set(failed_marketplaces)))}"
        if total > 0:
            progress_message += f", buybox_cap={PRICING_BUYBOX_ASIN_LIMIT}"

        await _update_job(db, job_id,
                          status="success",
                          records_processed=total,
                          progress_pct=100,
                          progress_message=progress_message,
                          finished_at=datetime.now(timezone.utc))

        log.info("sync_pricing.complete", total=total)
        return total


# ---------------------------------------------------------------------------
# 3.1 SYNC EXPECTED FEES (Product Fees API v0)
# ---------------------------------------------------------------------------

async def sync_offer_fee_estimates(
    marketplace_id: Optional[str] = None,
    job_id: Optional[str] = None,
    max_offers: int = 600,
    only_missing: bool = True,
) -> dict[str, int]:
    """
    Sync expected FBA/referral fees from Product Fees API v0.

    Writes:
      - acc_offer.fba_fee / acc_offer.referral_fee_rate (snapshot used in what-if)
      - acc_offer_fee_expected (diagnostic cache for expected vs actual comparisons)
    """
    max_offers = max(1, int(max_offers or 1))

    async with AsyncSessionLocal() as db:
        await _update_job(
            db,
            job_id,
            status="running",
            started_at=datetime.now(timezone.utc),
            progress_pct=5,
            progress_message="Preparing expected fee sync",
        )
        await _ensure_offer_fee_expected_schema(db)

        marketplaces = await _get_active_marketplaces(db)
        if marketplace_id:
            marketplaces = [m for m in marketplaces if m.id == marketplace_id]

        if not marketplaces:
            await _update_job(
                db,
                job_id,
                progress_pct=100,
                progress_message="No active marketplaces for expected fee sync",
            )
            return {"processed": 0, "synced": 0, "errors": 0}

        per_market_limit = max(25, max_offers // max(1, len(marketplaces)))
        stats = {"processed": 0, "synced": 0, "errors": 0}

        for m_idx, mkt in enumerate(marketplaces):
            offers_stmt = (
                select(Offer)
                .where(Offer.marketplace_id == mkt.id)
                .where(Offer.asin.is_not(None))
                .where(Offer.price.is_not(None))
                .where(Offer.price > 0)
                .where(Offer.status.in_(("Active", "Incomplete")))
                .order_by(Offer.updated_at.desc())
                .limit(per_market_limit)
            )
            if only_missing:
                offers_stmt = offers_stmt.where(
                    or_(
                        Offer.fba_fee.is_(None),
                        Offer.fba_fee <= 0,
                        Offer.referral_fee_rate.is_(None),
                        Offer.referral_fee_rate <= 0,
                    )
                )

            offers = list((await db.execute(offers_stmt)).scalars().all())
            if not offers:
                continue

            client = PricingClient(marketplace_id=mkt.id)
            for o_idx, offer in enumerate(offers):
                if stats["processed"] >= max_offers:
                    break
                stats["processed"] += 1
                price = float(offer.price or 0)
                currency = (offer.currency or mkt.currency or "EUR").upper()
                sku = (offer.sku or "").strip()
                asin = (offer.asin or "").strip()
                if not sku or not asin or price <= 0:
                    continue

                try:
                    result = await client.get_fees_estimate(
                        asin=asin,
                        price=price,
                        currency=currency,
                        is_fba=(str(offer.fulfillment_channel or "").upper() == "FBA"),
                    )
                    parsed = _parse_fee_estimate_result(result, offer_price=price)

                    expected_fba = float(parsed["fba_fee"] or 0)
                    expected_referral_fee = float(parsed["referral_fee"] or 0)
                    expected_total = float(parsed["total_fee"] or 0)
                    expected_ref_rate = float(parsed["referral_rate"] or 0)

                    await _upsert_offer_fee_expected(
                        db,
                        marketplace_id=mkt.id,
                        sku=sku,
                        asin=asin,
                        offer_price=price,
                        currency=currency,
                        fulfillment_channel=offer.fulfillment_channel,
                        expected_fba_fee=expected_fba if expected_fba > 0 else None,
                        expected_referral_fee=expected_referral_fee if expected_referral_fee > 0 else None,
                        expected_total_fee=expected_total if expected_total > 0 else None,
                        expected_referral_rate=expected_ref_rate if expected_ref_rate > 0 else None,
                        status=str(parsed.get("status") or "UNKNOWN"),
                        error_message=None,
                        fee_details=list(parsed.get("details") or []),
                    )
                    if expected_fba > 0 or expected_ref_rate > 0:
                        await db.execute(
                            text(
                                """
                                UPDATE dbo.acc_offer
                                SET
                                    fba_fee = CASE
                                        WHEN :fba_fee IS NOT NULL THEN CAST(:fba_fee AS DECIMAL(18,4))
                                        ELSE fba_fee
                                    END,
                                    referral_fee_rate = CASE
                                        WHEN :ref_rate IS NOT NULL THEN CAST(:ref_rate AS DECIMAL(18,6))
                                        ELSE referral_fee_rate
                                    END,
                                    updated_at = SYSUTCDATETIME()
                                WHERE id = CAST(:offer_id AS UNIQUEIDENTIFIER)
                                """
                            ),
                            {
                                "fba_fee": expected_fba if expected_fba > 0 else None,
                                "ref_rate": expected_ref_rate if expected_ref_rate > 0 else None,
                                "offer_id": str(offer.id),
                            },
                        )
                    stats["synced"] += 1
                except Exception as exc:
                    stats["errors"] += 1
                    try:
                        await _upsert_offer_fee_expected(
                            db,
                            marketplace_id=mkt.id,
                            sku=sku,
                            asin=asin,
                            offer_price=price,
                            currency=currency,
                            fulfillment_channel=offer.fulfillment_channel,
                            expected_fba_fee=None,
                            expected_referral_fee=None,
                            expected_total_fee=None,
                            expected_referral_rate=None,
                            status="ERROR",
                            error_message=str(exc),
                            fee_details=None,
                        )
                    except Exception:
                        # Don't fail whole sync on diagnostic upsert problems.
                        pass

                if (o_idx + 1) % 20 == 0:
                    await db.commit()
                    progress = min(
                        95,
                        10 + int((stats["processed"] / max_offers) * 80),
                    )
                    await _update_job(
                        db,
                        job_id,
                        progress_pct=progress,
                        progress_message=(
                            f"expected fees processed={stats['processed']} "
                            f"synced={stats['synced']} errors={stats['errors']} "
                            f"mkt={mkt.code}"
                        ),
                        records_processed=stats["synced"],
                    )
                    await asyncio.sleep(0.6)

            await db.commit()
            if stats["processed"] >= max_offers:
                break

            progress = min(
                95,
                10 + int(((m_idx + 1) / max(1, len(marketplaces))) * 80),
            )
            await _update_job(
                db,
                job_id,
                progress_pct=progress,
                progress_message=(
                    f"expected fees synced={stats['synced']} errors={stats['errors']} "
                    f"last_mkt={mkt.code}"
                ),
                records_processed=stats["synced"],
            )

        await _update_job(
            db,
            job_id,
            progress_pct=99,
            progress_message=(
                f"Expected fee sync done synced={stats['synced']} "
                f"errors={stats['errors']}"
            ),
            records_processed=stats["synced"],
        )
        log.info("sync_offer_fee_estimates.complete", **stats)
        return stats


# ---------------------------------------------------------------------------
# 4. SYNC CATALOG (PRODUCTS)
# ---------------------------------------------------------------------------

async def sync_catalog(
    marketplace_id: Optional[str] = None,
    job_id: Optional[str] = None,
    max_items: int = 500,
) -> int:
    """
    Sync product details from SP-API Catalog Items → acc_product.

    Finds products missing details (no title/image) and enriches from API.
    """
    async with AsyncSessionLocal() as db:
        await _update_job(db, job_id, status="running",
                          started_at=datetime.now(timezone.utc))

        # Find products needing enrichment
        result = await db.execute(
            select(Product)
            .where(Product.asin.isnot(None))
            .where(
                (Product.title.is_(None))
                | (Product.title == "")
                | (Product.image_url.is_(None))
            )
            .limit(max_items)
        )
        products = list(result.scalars().all())

        if not products:
            log.info("sync_catalog.nothing_to_sync")
            await _update_job(db, job_id, status="success",
                              records_processed=0, progress_pct=100,
                              finished_at=datetime.now(timezone.utc))
            return 0

        mkt_id = marketplace_id
        if not mkt_id:
            # Use first active marketplace
            mkts = await _get_active_marketplaces(db)
            mkt_id = mkts[0].id if mkts else None

        if not mkt_id:
            return 0

        asins = [p.asin for p in products if p.asin]
        client = CatalogClient(marketplace_id=mkt_id)

        total = 0

        try:
            items = await client.get_items_batch(asins)

            # Index by ASIN for fast lookup
            items_by_asin = {}
            for item in items:
                a = item.get("asin")
                if a:
                    items_by_asin[a] = item

            for product in products:
                if product.asin not in items_by_asin:
                    continue

                parsed = parse_catalog_item(items_by_asin[product.asin], mkt_id)

                if parsed.get("title"):
                    product.title = parsed["title"][:500]
                if parsed.get("brand"):
                    product.brand = parsed["brand"][:100]
                if parsed.get("category"):
                    product.category = parsed["category"][:200]
                if parsed.get("image_url"):
                    product.image_url = parsed["image_url"][:500]

                total += 1

            await db.flush()

        except Exception as e:
            log.error("sync_catalog.error", error=str(e))

        await db.commit()
        await _update_job(db, job_id,
                          status="success",
                          records_processed=total,
                          progress_pct=100,
                          finished_at=datetime.now(timezone.utc))

        log.info("sync_catalog.complete", total=total)
        return total


# ---------------------------------------------------------------------------
# 4b. SYNC PRODUCT TITLES FROM PIM (Ergonode)
# ---------------------------------------------------------------------------

async def sync_product_titles_from_pim() -> int:
    """
    Update acc_product.title from Ergonode PIM attributes.

    Priority: tytul (pl_PL) → tytul_bl (pl_PL) → amazon_title (de_DE) → amazon_title (en_GB)

    Only updates products that already exist in acc_product (mapped via EAN/SKU).
    Returns number of products updated.
    """
    from app.connectors.ergonode import fetch_ergonode_title_lookup
    from app.core.db_connection import connect_acc

    log.info("sync_titles_pim.start")

    # Step 1: Load current products to get target SKUs
    conn = connect_acc()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, internal_sku, title
        FROM acc_product WITH (NOLOCK)
        WHERE internal_sku IS NOT NULL
    """)
    products = cur.fetchall()
    cur.close()
    conn.close()

    # Build internal_sku → (product_id, current_title)
    sku_to_product: dict[str, tuple[str, str | None]] = {}
    target_skus: set[str] = set()
    for row in products:
        isk = str(row[1]).strip()
        sku_to_product[isk] = (str(row[0]), row[2])
        target_skus.add(isk)

    log.info("sync_titles_pim.products_loaded", count=len(sku_to_product))

    # Step 2: Fetch titles from PIM (only for target SKUs)
    title_lookup = await fetch_ergonode_title_lookup(target_skus=target_skus)
    if not title_lookup:
        log.warning("sync_titles_pim.no_titles_found")
        return 0

    log.info("sync_titles_pim.pim_titles", count=len(title_lookup))

    # Step 3: Match and update
    conn = connect_acc()
    cur = conn.cursor()
    updated = 0
    skipped_same = 0
    skipped_no_match = 0

    for ergo_sku, title_data in title_lookup.items():
        ergo_sku_clean = str(ergo_sku).strip()
        if ergo_sku_clean not in sku_to_product:
            skipped_no_match += 1
            continue

        product_id, current_title = sku_to_product[ergo_sku_clean]
        new_title = str(title_data.get("title_best", "")).strip()[:500]

        if not new_title:
            continue

        # Skip if title is already set from PIM (exact match)
        if current_title and current_title.strip() == new_title:
            skipped_same += 1
            continue

        try:
            cur.execute(
                """
                UPDATE acc_product
                SET title = %s
                WHERE id = %s
                """,
                (new_title, product_id),
            )
            updated += 1
        except Exception as e:
            log.warning("sync_titles_pim.update_error",
                        product_id=product_id, error=str(e))

    conn.commit()
    cur.close()
    conn.close()

    log.info(
        "sync_titles_pim.complete",
        updated=updated,
        skipped_same=skipped_same,
        skipped_no_match=skipped_no_match,
        pim_total=len(title_lookup),
    )
    return updated


# ---------------------------------------------------------------------------
# 5. SYNC FINANCES
# ---------------------------------------------------------------------------

async def _run_step_sync_finances(
    marketplace_id: Optional[str] = None,
    days_back: int = 7,
) -> int:
    """Wrapper to call step_sync_finances from order_pipeline for full_sync."""
    from app.services.order_pipeline import step_sync_finances
    result = await step_sync_finances(
        days_back=days_back,
        marketplace_id=marketplace_id,
    )
    return result.get("fee_rows", 0)


async def sync_finances(
    marketplace_id: Optional[str] = None,
    days_back: int = 7,
    job_id: Optional[str] = None,
) -> int:
    """
    DEPRECATED — uses legacy v0 API without dedup. Causes duplicate rows.
    Use step_sync_finances() from order_pipeline instead.
    Kept only for backward compatibility — all callers have been rewired.
    """
    log.warning("sync_finances.deprecated — this function should not be called, use step_sync_finances()")
    from app.services.order_pipeline import step_sync_finances
    result = await step_sync_finances(days_back=days_back, marketplace_id=marketplace_id, job_id=job_id)
    return result.get("fee_rows", 0)


# ---------------------------------------------------------------------------
# 6. SYNC EXCHANGE RATES
# ---------------------------------------------------------------------------

async def sync_exchange_rates(
    days_back: int = 30,
    job_id: Optional[str] = None,
) -> int:
    """
    Sync PLN exchange rates from NBP API -> acc_exchange_rate.

    Uses bulk range fetch (1 HTTP request per currency instead of N).
    Handles AED/SAR via USD cross-rate (not in NBP Table A).
    Uses raw pyodbc INSERT to avoid HY104 with old ODBC driver.
    """
    import asyncio
    import pyodbc
    from app.core.config import settings
    from app.connectors.nbp import fetch_nbp_rates_range

    # NBP Table A currencies
    NBP_TABLE_A = {"EUR", "GBP", "SEK", "TRY", "USD", "CZK", "DKK", "NOK",
                   "CHF", "HUF", "JPY", "CAD", "AUD", "NZD", "BGN", "RON",
                   "CNY", "KRW", "MXN", "IDR", "INR", "MYR",
                   "PHP", "SGD", "THB", "ZAR", "ILS", "CLP", "ISK", "BRL"}
    # Currencies calculated from USD cross-rate (pegged to USD)
    USD_CROSS = {"AED": 3.6725, "SAR": 3.7500}

    async with AsyncSessionLocal() as db:
        await _update_job(db, job_id, status="running",
                          started_at=datetime.now(timezone.utc))

        # Determine currencies from active marketplaces
        marketplaces = await _get_active_marketplaces(db)
        mp_currencies = set(m.currency for m in marketplaces if m.currency != "PLN")
        log.info("sync_exchange_rates.currencies", currencies=sorted(mp_currencies))

        today = date.today()
        date_from = today - timedelta(days=days_back)

        # --- 1. Bulk-fetch rates from NBP for Table A currencies ---
        nbp_currencies = mp_currencies & NBP_TABLE_A
        cross_currencies = mp_currencies & set(USD_CROSS.keys())

        # Always fetch USD if we need cross-rates
        if cross_currencies:
            nbp_currencies.add("USD")

        all_rates: dict[str, list[tuple[date, float]]] = {}
        for cur in sorted(nbp_currencies):
            try:
                rates = await fetch_nbp_rates_range(cur, date_from, today)
                all_rates[cur] = rates
                log.info("sync_exchange_rates.fetched", currency=cur, count=len(rates))
            except Exception as e:
                log.error("sync_exchange_rates.fetch_error", currency=cur, error=str(e))

        # --- 2. Calculate cross-rate currencies from USD ---
        usd_rates = {d: r for d, r in all_rates.get("USD", [])}
        for cur in sorted(cross_currencies):
            divisor = USD_CROSS[cur]
            cross = [(d, round(r / divisor, 6)) for d, r in usd_rates.items()]
            all_rates[cur] = cross
            log.info("sync_exchange_rates.cross_rate", currency=cur, base="USD",
                     divisor=divisor, count=len(cross))

        # Remove USD if not in marketplace currencies
        if "USD" not in mp_currencies:
            all_rates.pop("USD", None)

        # --- 3. Insert into DB via connect_acc (supports Azure SQL + local) ---
        def _insert_rates_sync(rates_serialized: dict) -> int:
            conn = connect_acc(autocommit=False)
            total = 0
            try:
                # Get existing (rate_date, currency) pairs to skip duplicates
                cur_db = conn.cursor()
                cur_db.execute(
                    "SELECT CAST(rate_date AS VARCHAR(10)), currency "
                    "FROM acc_exchange_rate"
                )
                existing = set()
                for row in cur_db.fetchall():
                    existing.add((row[0].strip(), row[1]))
                cur_db.close()

                # Insert new rates
                for currency, rate_list in rates_serialized.items():
                    for rate_date_str, rate_val in rate_list:
                        key = (rate_date_str, currency)
                        if key in existing:
                            continue
                        source = "NBP" if currency in NBP_TABLE_A else "NBP-cross"
                        cur2 = conn.cursor()
                        cur2.execute(
                            "INSERT INTO acc_exchange_rate "
                            "(id, rate_date, currency, rate_to_pln, source) "
                            "VALUES (NEWID(), CAST(? AS DATE), ?, "
                            "CAST(? AS DECIMAL(10,6)), ?)",
                            [rate_date_str, currency,
                             str(round(rate_val, 6)), source]
                        )
                        cur2.close()
                        total += 1

                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
            return total

        # Serialize rates for thread (date -> string)
        rates_serialized = {
            cur: [(str(d), r) for d, r in rate_list]
            for cur, rate_list in all_rates.items()
        }

        try:
            total = await asyncio.to_thread(_insert_rates_sync, rates_serialized)
        except Exception as e:
            log.error("sync_exchange_rates.insert_error", error=str(e))
            await _update_job(db, job_id,
                              status="failure",
                              error_message=str(e)[:2000],
                              finished_at=datetime.now(timezone.utc))
            return 0

        await _update_job(db, job_id,
                          status="success",
                          records_processed=total,
                          progress_pct=100,
                          finished_at=datetime.now(timezone.utc))

        log.info("sync_exchange_rates.complete", total=total)
        return total


async def sync_ecb_exchange_rates(days_back: int = 90) -> int:
    """
    Backup: fetch EUR-based rates from ECB -> ecb_exchange_rate.

    Populates the ecb_exchange_rate table as a secondary source.
    Does NOT overwrite acc_exchange_rate (NBP remains primary).
    """
    from app.connectors.ecb import fetch_ecb_rates

    rates = await fetch_ecb_rates(days_back=days_back)
    if not rates:
        log.warning("sync_ecb.no_rates")
        return 0

    conn = connect_acc(autocommit=False)
    try:
        cur = conn.cursor()
        # Build set of existing keys to skip duplicates
        cur.execute(
            "SELECT CONVERT(VARCHAR(10), rate_date, 120), source_currency, "
            "target_currency FROM dbo.ecb_exchange_rate WITH (NOLOCK)"
        )
        existing = set()
        for row in cur.fetchall():
            existing.add((row[0].strip(), row[1], row[2]))

        inserted = 0
        for r in rates:
            key = (str(r["rate_date"]), r["source_currency"], r["target_currency"])
            if key in existing:
                continue
            cur.execute(
                "INSERT INTO dbo.ecb_exchange_rate "
                "(rate_date, source_currency, target_currency, rate) "
                "VALUES (CAST(%s AS DATE), %s, %s, CAST(%s AS DECIMAL(18,6)))",
                (str(r["rate_date"]), r["source_currency"],
                 r["target_currency"], str(r["rate"])),
            )
            inserted += 1

        conn.commit()
        log.info("sync_ecb.complete", inserted=inserted, total_fetched=len(rates))
        return inserted
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# 7. CALCULATE PROFIT
# ---------------------------------------------------------------------------

async def calc_profit(
    days_back: int = 1,
    marketplace_id: Optional[str] = None,
    job_id: Optional[str] = None,
) -> int:
    """
    Recalculate contribution margin for recent shipped orders.
    Uses V2 engine (mssql_store.recalc_profit_orders) — netto revenue, no ads in CM1.
    """
    from app.connectors.mssql.mssql_store import recalc_profit_orders

    async with AsyncSessionLocal() as db:
        await _update_job(db, job_id, status="running",
                          started_at=datetime.now(timezone.utc))

        date_to = date.today()
        date_from = date_to - timedelta(days=days_back)

        try:
            count = recalc_profit_orders(
                date_from=date_from, date_to=date_to,
            )
            await _update_job(db, job_id,
                              status="success",
                              records_processed=count,
                              progress_pct=100,
                              finished_at=datetime.now(timezone.utc))
            return count

        except Exception as e:
            log.error("calc_profit.error", error=str(e))
            await _update_job(db, job_id,
                              status="failure",
                              error_message=str(e),
                              finished_at=datetime.now(timezone.utc))
            return 0


# ---------------------------------------------------------------------------
# 7b. PURCHASE PRICES (Holding FIFO + XLSX fallback)
# ---------------------------------------------------------------------------

async def _read_xlsx_purchase_prices(skus: set[str]) -> dict[str, float]:
    """Read previously-synced XLSX prices from acc_purchase_price history.

    XLSX prices are pushed to acc_purchase_price (source='xlsx') by a
    lightweight script on the local machine that has N: drive access.
    This function simply reads back the latest valid prices.

    Returns ``{internal_sku: netto_price_pln}``.
    """
    import asyncio

    def _read_sync() -> dict[str, float]:
        try:
            conn = connect_acc(timeout=15)
            cursor = conn.cursor()
            sku_csv = ",".join(f"'{s}'" for s in skus)
            cursor.execute(
                f"SELECT internal_sku, netto_price_pln "
                f"FROM acc_purchase_price "
                f"WHERE internal_sku IN ({sku_csv}) "
                f"  AND source = 'xlsx' "
                f"  AND valid_to IS NULL "
                f"  AND netto_price_pln > 0"
            )
            prices: dict[str, float] = {}
            for row in cursor.fetchall():
                sku = str(row[0]).strip()
                price = round(float(row[1]), 4)
                if 0 < price < 10_000:
                    prices[sku] = price
            conn.close()
            log.info("xlsx_prices.from_history", found=len(prices), requested=len(skus))
            return prices
        except Exception as e:
            log.error("xlsx_prices.read_error", error=str(e))
            return {}

    return await asyncio.to_thread(_read_sync)


async def sync_purchase_prices(
    job_id: Optional[str] = None,
) -> int:
    """
    Sync netto purchase prices — 3-layer architecture:

    Layer 1: ``acc_purchase_price`` — price history with validity ranges
    Layer 2: ``acc_order_line.purchase_price_pln`` — per-line cost snapshot
    Layer 3: ``acc_product.netto_purchase_price_pln`` — current-price cache

    Sources (cascade):
      1. ITJK_BazaDanychSprzedazHolding — latest FIFO WZ price (primary)
      2. XLSX "Oficjalne ceny zakupu" — fallback for products not in Holding

    Uses raw pyodbc in a thread to avoid aioodbc Unicode column-name issues
    (Polish diacritics in ERP column names).
    """
    import asyncio

    async with AsyncSessionLocal() as db:
        await _update_job(
            db, job_id,
            status="running",
            started_at=datetime.now(timezone.utc),
        )

    # ── 1. Load mapped products ───────────────────────────────────
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Product).where(Product.internal_sku.isnot(None))
        )
        products = list(result.scalars().all())

    if not products:
        log.warning("sync_purchase_prices.no_mapped_products")
        async with AsyncSessionLocal() as db:
            await _update_job(
                db, job_id,
                status="success", records_processed=0,
                progress_pct=100,
                finished_at=datetime.now(timezone.utc),
            )
        return 0

    sku_map = {p.internal_sku: p for p in products}
    all_skus = set(sku_map.keys())
    log.info("sync_purchase_prices.start", products=len(all_skus))

    # ── 2. Holding FIFO — latest purchase price per SKU ───────────
    def _query_holding(skus: set[str]) -> dict[str, float]:
        # ERP table (ITJK_BazaDanychSprzedazHolding) — read-only
        conn = None
        cursor = None
        try:
            conn = connect_netfox()
            cursor = conn.cursor()

            sku_csv = ",".join(f"'{s}'" for s in skus)
            sql = (
                "SELECT sub.nr, sub.cena "
                "FROM ("
                "  SELECT [Numer artykułu] AS nr,"
                "         [Cena zakupu towaru] AS cena,"
                "         ROW_NUMBER() OVER ("
                "             PARTITION BY [Numer artykułu]"
                "             ORDER BY [Data] DESC"
                "         ) AS rn"
                "  FROM ITJK_BazaDanychSprzedazHolding WITH (NOLOCK)"
                f" WHERE [Numer artykułu] IN ({sku_csv})"
                "    AND [Cena zakupu towaru] > 0"
                ") sub "
                "WHERE sub.rn = 1"
            )
            cursor.execute(sql)

            prices: dict[str, float] = {}
            for row in cursor.fetchall():
                sku = str(row[0]).strip()
                price = round(float(row[1]), 4)
                if 0 < price < 10_000:
                    prices[sku] = price
            return prices
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

    try:
        holding_prices = await asyncio.to_thread(_query_holding, all_skus)
    except Exception as e:
        log.error("sync_purchase_prices.holding_error", error=str(e))
        holding_prices = {}

    log.info(
        "sync_purchase_prices.holding_done",
        found=len(holding_prices),
        total=len(all_skus),
    )
    async with AsyncSessionLocal() as db:
        await _update_job(
            db, job_id,
            progress_pct=30,
            progress_message=f"Holding: {len(holding_prices)}/{len(all_skus)}",
        )

    # ── 3. XLSX fallback for remaining ────────────────────────────
    remaining = all_skus - set(holding_prices.keys())
    xlsx_prices: dict[str, float] = {}
    if remaining:
        xlsx_prices = await _read_xlsx_purchase_prices(remaining)
        log.info(
            "sync_purchase_prices.xlsx_done",
            found=len(xlsx_prices),
            remaining=len(remaining),
        )

    # ── 4. Build merged price dict {sku: (price, source)} ─────────
    # Priority: xlsx_oficjalne > holding (xlsx is the official price list)
    merged: dict[str, tuple[float, str]] = {}
    for sku in all_skus:
        xlsx_price = xlsx_prices.get(sku)
        holding_price = holding_prices.get(sku)
        if xlsx_price is not None and xlsx_price <= 2000:
            merged[sku] = (xlsx_price, "xlsx")
        elif holding_price is not None:
            merged[sku] = (holding_price, "holding")

    # ── 5. Write all 3 layers via raw pyodbc ──────────────────────
    today_str = date.today().isoformat()

    def _write_all_layers(
        prices: dict[str, tuple[float, str]],
        sku_to_product_id: dict[str, str],
    ) -> tuple[int, int, int, dict[str, int]]:
        """
        Returns (product_updated, history_upserted, lines_stamped, stats).
        All in one pyodbc connection to avoid multiple connection overhead.
        """
        conn = connect_acc(autocommit=False)
        try:
            cur = conn.cursor()
            product_updated = 0
            history_upserted = 0
            stats = {"holding": 0, "xlsx": 0, "none": 0}

            for sku, (price, src) in prices.items():
                stats[src] += 1

                # --- Price cap: reject obvious garbage (xlsx sentinel values) ---
                if price > 2000:
                    log.warning("sync_pp.price_cap_exceeded",
                                sku=sku, price=price, source=src,
                                cap=2000)
                    continue

                # --- Layer 3: acc_product.netto_purchase_price_pln (cache) ---
                try:
                    cur.execute(
                        "UPDATE acc_product "
                        "SET netto_purchase_price_pln = ?, "
                        "    updated_at = GETUTCDATE() "
                        "WHERE internal_sku = ?",
                        price, sku,
                    )
                    product_updated += 1
                except Exception as e:
                    log.warning("sync_pp.product_update_err", sku=sku, error=str(e))

                # --- Layer 1: acc_purchase_price (history) ---
                # Only manage rows of the SAME source to avoid closing
                # rows from other sources (e.g. xlsx_oficjalne vs holding).
                try:
                    # Check if current open record exists for THIS source
                    cur.execute(
                        "SELECT id, netto_price_pln "
                        "FROM acc_purchase_price "
                        "WHERE internal_sku = ? AND source = ? "
                        "  AND valid_to IS NULL "
                        "ORDER BY valid_from DESC",
                        sku, src,
                    )
                    existing = cur.fetchone()

                    if existing:
                        existing_price = round(float(existing[1]), 4)
                        if existing_price == round(price, 4):
                            # Same price — just touch updated_at
                            cur.execute(
                                "UPDATE acc_purchase_price "
                                "SET updated_at = GETUTCDATE() "
                                "WHERE id = ?",
                                existing[0],
                            )
                            history_upserted += 1
                        else:
                            # Price changed — close old, insert new
                            cur.execute(
                                "UPDATE acc_purchase_price "
                                "SET valid_to = CAST(? AS DATE), "
                                "    updated_at = GETUTCDATE() "
                                "WHERE id = ?",
                                today_str, existing[0],
                            )
                            cur.execute(
                                "INSERT INTO acc_purchase_price "
                                "(internal_sku, netto_price_pln, valid_from, "
                                " valid_to, source, source_document) "
                                "VALUES (?, ?, CAST(? AS DATE), NULL, ?, ?)",
                                sku, price, today_str, src,
                                "ITJK_BazaDanychSprzedazHolding" if src == "holding"
                                else "purchase_prices.xlsx",
                            )
                            history_upserted += 1
                    else:
                        # No existing record — insert first entry
                        cur.execute(
                            "INSERT INTO acc_purchase_price "
                            "(internal_sku, netto_price_pln, valid_from, "
                            " valid_to, source, source_document) "
                            "VALUES (?, ?, CAST(? AS DATE), NULL, ?, ?)",
                            sku, price, today_str, src,
                            "ITJK_BazaDanychSprzedazHolding" if src == "holding"
                            else "purchase_prices.xlsx",
                        )
                        history_upserted += 1
                except Exception as e:
                    log.warning("sync_pp.history_err", sku=sku, error=str(e))

            # --- Layer 2: acc_order_line.purchase_price_pln (backfill) ---
            # Stamp all order lines that have a product_id with a known price
            # but no purchase_price_pln yet.
            lines_stamped = 0
            for sku, (price, src) in prices.items():
                pid = sku_to_product_id.get(sku)
                if not pid:
                    continue
                try:
                    cur.execute(
                        "UPDATE acc_order_line "
                        "SET purchase_price_pln = ?, "
                        "    cogs_pln = ? * ISNULL(quantity_ordered, 0), "
                        "    price_source = ? "
                        "WHERE product_id = CAST(? AS UNIQUEIDENTIFIER) "
                        "  AND purchase_price_pln IS NULL",
                        price, price, src, pid,
                    )
                    lines_stamped += cur.rowcount or 0
                except Exception as e:
                    log.warning("sync_pp.line_stamp_err", sku=sku, error=str(e))

            # Mark none-count
            stats["none"] = len(all_skus) - len(prices)

            conn.commit()
            return product_updated, history_upserted, lines_stamped, stats
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # Build sku→product_id mapping for layer 2
    sku_to_pid: dict[str, str] = {}
    for p in products:
        if p.internal_sku and p.id:
            sku_to_pid[p.internal_sku] = str(p.id)

    try:
        product_updated, history_upserted, lines_stamped, stats = (
            await asyncio.to_thread(_write_all_layers, merged, sku_to_pid)
        )
    except Exception as e:
        log.error("sync_purchase_prices.write_error", error=str(e))
        async with AsyncSessionLocal() as db:
            await _update_job(
                db, job_id,
                status="failure",
                error_message=str(e)[:2000],
                finished_at=datetime.now(timezone.utc),
            )
        return 0

    # ── 6. Fallback: resolve prices for unmapped products ─────────
    # Products without internal_sku that still have unstamped order lines.
    # Waterfall: EAN sibling → EAN→Holding direct → ASIN sibling → BL Kod
    async with AsyncSessionLocal() as db:
        await _update_job(
            db, job_id,
            progress_pct=90,
            progress_message="Fallback price resolution for unmapped products...",
        )

    def _resolve_fallback_prices() -> tuple[int, int, dict]:
        """
        Find purchase prices for products without internal_sku using
        alternative lookup methods.

        Waterfall per product:
          1. EAN sibling — extract EAN from Amazon SKU, find another
             acc_product with same EAN that has netto_purchase_price_pln
          2. EAN → Holding direct — try EAN as article number in
             ITJK_BazaDanychSprzedazHolding
          3. ASIN sibling — find another acc_product with same ASIN
             that has a price
          4. BL Kod lookup — ITJK_BL_OrdersSkuDetails SKU→Kod(EAN)
             → find product by EAN that has price

        Returns: (products_updated, lines_stamped, source_stats)
        """
        conn = None
        cur = None
        nfx = None
        nfx_cur = None
        try:
            # Azure SQL for acc_* tables (reads + writes)
            conn = connect_acc(autocommit=False)
            cur = conn.cursor()
            # Netfox ERP for ITJK_* tables (read-only lookups)
            nfx = connect_netfox()
            nfx_cur = nfx.cursor()

            # Get unmapped products that have unstamped order lines
            cur.execute("""
            SELECT DISTINCT
                CAST(p.id AS VARCHAR(36)) AS pid,
                p.sku,
                p.asin,
                p.ean
            FROM acc_product p
            INNER JOIN acc_order_line ol ON ol.product_id = p.id
            WHERE p.internal_sku IS NULL
              AND p.netto_purchase_price_pln IS NULL
              AND ol.purchase_price_pln IS NULL
        """)
            unmapped = cur.fetchall()

            if not unmapped:
                return 0, 0, {}

            products_updated = 0
            total_lines_stamped = 0
            fb_stats: dict[str, int] = {}

            for pid, sku, asin, ean_col in unmapped:
                price = None
                source = None

                # Try extract EAN from Amazon SKU (or use stored ean)
                ean = None
                if ean_col:
                    ean = str(ean_col).strip()
                if not ean and sku:
                    ean = _extract_ean(sku)

            # --- Fallback 1: EAN sibling ---
            if ean and not price:
                cur.execute(
                    "SELECT TOP 1 netto_purchase_price_pln "
                    "FROM acc_product "
                    "WHERE ean = ? "
                    "  AND netto_purchase_price_pln IS NOT NULL "
                    "  AND netto_purchase_price_pln > 0",
                    ean,
                )
                row = cur.fetchone()
                if row:
                    price = round(float(row[0]), 4)
                    source = "ean_sibling"

            # --- Fallback 2: EAN → Holding direct ---
            # Sometimes the EAN itself is used as [Numer artykułu]
            if ean and not price:
                nfx_cur.execute(
                    "SELECT TOP 1 [Cena zakupu towaru] "
                    "FROM ITJK_BazaDanychSprzedazHolding WITH (NOLOCK) "
                    "WHERE [Numer artykułu] = ? "
                    "  AND [Cena zakupu towaru] > 0 "
                    "ORDER BY [Data] DESC",
                    (ean,),
                )
                row = nfx_cur.fetchone()
                if row:
                    price = round(float(row[0]), 4)
                    source = "ean_holding"

            # --- Fallback 3: ASIN sibling ---
            if asin and not price:
                cur.execute(
                    "SELECT TOP 1 netto_purchase_price_pln "
                    "FROM acc_product "
                    "WHERE asin = ? "
                    "  AND netto_purchase_price_pln IS NOT NULL "
                    "  AND netto_purchase_price_pln > 0 "
                    "  AND CAST(id AS VARCHAR(36)) != ?",
                    asin, pid,
                )
                row = cur.fetchone()
                if row:
                    price = round(float(row[0]), 4)
                    source = "asin_sibling"

            # --- Fallback 4: BL Kod lookup ---
            # ITJK_BL_OrdersSkuDetails maps SKU → Kod (barcode/EAN)
            if sku and not price:
                nfx_cur.execute(
                    "SELECT TOP 1 Kod "
                    "FROM ITJK_BL_OrdersSkuDetails WITH (NOLOCK) "
                    "WHERE sku = ? AND Kod IS NOT NULL AND Kod != ''",
                    (sku,),
                )
                row = nfx_cur.fetchone()
                if row and row[0]:
                    bl_ean = str(row[0]).strip()
                    if bl_ean:
                        # Try matching this barcode to a product with price
                        cur.execute(
                            "SELECT TOP 1 netto_purchase_price_pln "
                            "FROM acc_product "
                            "WHERE ean = ? "
                            "  AND netto_purchase_price_pln IS NOT NULL "
                            "  AND netto_purchase_price_pln > 0",
                            bl_ean,
                        )
                        row2 = cur.fetchone()
                        if row2:
                            price = round(float(row2[0]), 4)
                            source = "bl_kod_sibling"

            # --- Apply price if found ---
                if price and 0 < price < 10_000:
                    try:
                    # Layer 3: Update product price cache
                        cur.execute(
                        "UPDATE acc_product "
                        "SET netto_purchase_price_pln = ?, "
                        "    updated_at = GETUTCDATE() "
                        "WHERE CAST(id AS VARCHAR(36)) = ?",
                        price, pid,
                    )
                        products_updated += 1
                        fb_stats[source] = fb_stats.get(source, 0) + 1

                        # Also store EAN on product if extracted and not set
                        if ean and not ean_col:
                            cur.execute(
                            "UPDATE acc_product SET ean = ? "
                            "WHERE CAST(id AS VARCHAR(36)) = ? AND ean IS NULL",
                            ean, pid,
                            )

                        # Layer 2: Stamp order lines directly
                        cur.execute(
                        "UPDATE acc_order_line "
                        "SET purchase_price_pln = ?, "
                        "    cogs_pln = ? * ISNULL(quantity_ordered, 1), "
                        "    price_source = ? "
                        "WHERE product_id = CAST(? AS UNIQUEIDENTIFIER) "
                        "  AND purchase_price_pln IS NULL",
                        price, price, source, pid,
                    )
                        total_lines_stamped += cur.rowcount or 0
                    except Exception as e:
                        log.warning(
                        "sync_pp.fallback_err", pid=pid, sku=sku,
                        source=source, error=str(e),
                        )

            conn.commit()
            return products_updated, total_lines_stamped, fb_stats
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()
            if nfx_cur is not None:
                nfx_cur.close()
            if nfx is not None:
                nfx.close()

    # Run fallback in thread
    fallback_products = 0
    fallback_lines = 0
    fallback_stats: dict[str, int] = {}
    try:
        fallback_products, fallback_lines, fallback_stats = (
            await asyncio.to_thread(_resolve_fallback_prices)
        )
        if fallback_products > 0:
            log.info(
                "sync_purchase_prices.fallback_done",
                products=fallback_products,
                lines=fallback_lines,
                sources=fallback_stats,
            )
    except Exception as e:
        log.error("sync_purchase_prices.fallback_error", error=str(e))

    summary = (
        f"holding={stats['holding']},xlsx={stats['xlsx']},"
        f"none={stats['none']},products={product_updated},"
        f"history={history_upserted},lines={lines_stamped},"
        f"fallback_products={fallback_products},"
        f"fallback_lines={fallback_lines}"
    )
    log.info(
        "sync_purchase_prices.done",
        **stats,
        products=product_updated,
        history=history_upserted,
        lines=lines_stamped,
        fallback_products=fallback_products,
        fallback_lines=fallback_lines,
    )

    async with AsyncSessionLocal() as db:
        await _update_job(
            db, job_id,
            status="success",
            records_processed=product_updated + fallback_products,
            progress_pct=100,
            result_summary=summary,
            finished_at=datetime.now(timezone.utc),
        )
    return product_updated + fallback_products


# ---------------------------------------------------------------------------

def _extract_ean(amazon_sku: str) -> str | None:
    """Extract EAN from Amazon SKU.

    Supported patterns:
      - FBA_<EAN>           (standard FBA)
      - FBM_<EAN>           (standard FBM / MFN)
      - MAG_<EAN>           (standard MAG)
      - FBA_<EAN>_SL        (with suffix)
      - amzn.gr.{PREFIX}_<EAN>-<hash>  (Amazon-generated)
      - ...found variants  (FBA_<EAN>found, <EAN>.found)
    """
    if not amazon_sku:
        return None
    s = amazon_sku.strip()

    # Pattern 1: amzn.gr.{PREFIX}_<EAN>-<randomhash>
    m = re.match(r"amzn\.gr\.(?:MAG|FBA|FBM)_(\d{8,})", s)
    if m:
        return m.group(1)

    # Pattern 2: standard FBA_/FBM_/MAG_ prefix (optionally with suffixes)
    for prefix in ("FBA_", "FBM_", "MAG_"):
        if s.startswith(prefix):
            rest = s[len(prefix):]
            # Split on _ or - to strip suffixes like _SL, -hash
            rest = re.split(r'[_\-]', rest)[0]
            # Strip .found / found suffix
            rest = re.sub(r'\.?found$', '', rest)
            if rest.isdigit() and len(rest) >= 8:
                return rest

    # Pattern 3: bare <EAN>.found (e.g. "5902730382126.found")
    m = re.match(r'(\d{8,})\.found$', s)
    if m:
        return m.group(1)

    return None


async def _fetch_gsheet_ean_lookup() -> dict[str, dict]:
    """
    Download Google Sheet CSV and build EAN lookup.
    Columns: D=B200 (name+K-number), E=EAN, F=Nr art. (internal SKU).
    Returns: {ean: {internal_sku, k_number, product_name}}
    """
    import httpx
    import csv
    import io
    
    url = settings.GSHEET_EAN_CSV_URL or settings.GSHEET_ALLEGRO_CSV_URL
    if not url:
        log.warning("gsheet.no_url_configured")
        return {}

    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
    except Exception as e:
        log.error("gsheet.fetch_error", error=str(e))
        return {}

    lookup: dict[str, dict] = {}
    reader = csv.reader(io.StringIO(resp.text))
    header = next(reader, None)  # skip header
    for row in reader:
        if len(row) < 6:
            continue
        b200 = row[3].strip()   # col D — name with K-number
        ean = row[4].strip()    # col E — EAN
        nr_art = row[5].strip() # col F — internal SKU
        if not ean or not ean.isdigit() or len(ean) < 8:
            continue

        # Extract K-number from B200 (e.g. "Siewnik na kolkach 12L K11297")
        k_match = re.search(r'\b(K\d{1,6})\b', b200)
        k_number = k_match.group(1) if k_match else ""

        lookup[ean] = {
            "internal_sku": nr_art,
            "k_number": k_number,
            "product_name": b200,
        }

    log.info("gsheet.ean_lookup_built", entries=len(lookup))
    return lookup


async def _fetch_baselinker_ean_lookup(eans: list[str]) -> dict[str, dict]:
    """
    Query ITJK_BL_OriginalOrders in MSSQL for unmatched EANs.
    Returns: {ean: {internal_sku, k_number, product_name}}

    Uses batch IN-clause queries (chunks of 500) instead of N+1 for performance.
    """
    if not eans:
        return {}

    def _query():
        conn = connect_netfox(timeout=30)
        cursor = conn.cursor()
        lookup: dict[str, dict] = {}

        # Process in batches of 500 (SQL Server IN clause limit is ~2100 params)
        batch_size = 500
        for i in range(0, len(eans), batch_size):
            batch = eans[i:i + batch_size]
            placeholders = ",".join(["?"] * len(batch))
            try:
                cursor.execute(
                    f"SELECT CAST([ean] AS VARCHAR(50)) AS ean_val, "
                    f"CAST([sku] AS VARCHAR(50)) AS sku_val, "
                    f"CAST([name] AS VARCHAR(200)) AS name_val, "
                    f"[date_add] "
                    f"FROM ITJK_BL_OriginalOrders WITH (NOLOCK) "
                    f"WHERE CAST([ean] AS VARCHAR(50)) IN ({placeholders})",
                    *batch,
                )
                rows = cursor.fetchall()
                # Group by EAN and take most recent (by date_add)
                best: dict[str, tuple] = {}
                for row in rows:
                    ean_val = str(row[0]).strip() if row[0] else ""
                    dt = row[3]
                    if ean_val and (ean_val not in best or (dt and dt > best[ean_val][3])):
                        best[ean_val] = row

                for ean_val, row in best.items():
                    bl_sku = str(row[1]).strip() if row[1] else ""
                    bl_name = str(row[2]).strip() if row[2] else ""
                    k_match = re.search(r'\b(K\d{1,6})\b', bl_name)
                    k_number = k_match.group(1) if k_match else ""
                    internal = bl_sku if bl_sku.isdigit() else ""
                    lookup[ean_val] = {
                        "internal_sku": internal,
                        "k_number": k_number,
                        "product_name": bl_name,
                    }
            except Exception as e:
                log.warning("baselinker.batch_error", batch_start=i, error=str(e))

        conn.close()
        log.info("baselinker.ean_lookup_built", entries=len(lookup),
                 total_queried=len(eans))
        return lookup

    import asyncio
    return await asyncio.to_thread(_query)


async def sync_product_mapping(
    only_unmapped: bool = True,
    job_id: str | None = None,
    skip_spapi: bool = False,
) -> int:
    """
    Map Amazon products to internal SKU/K-number using cascade:
      1. Ergonode PIM (primary — 18K+ products)
      2. Google Sheet EAN mapping CSV (fallback #1)
      3. MSSQL Baselinker orders (fallback #2)

    Args:
        only_unmapped: If True, only map products with internal_sku IS NULL.
        job_id: Optional JobRun ID for progress tracking.

    Returns: number of products successfully mapped.
    """
    from app.connectors.ergonode import fetch_ergonode_ean_lookup

    async with AsyncSessionLocal() as db:
        await _update_job(db, job_id, status="running",
                          started_at=datetime.now(timezone.utc),
                          progress_message="Fetching product data...")

    # --- Step 1: Load all acc_products ---
    async with AsyncSessionLocal() as db:
        if only_unmapped:
            result = await db.execute(
                select(Product).where(Product.internal_sku.is_(None))
            )
        else:
            result = await db.execute(select(Product))
        products = list(result.scalars().all())

    if not products:
        log.info("sync_mapping.nothing_to_map")
        async with AsyncSessionLocal() as db:
            await _update_job(db, job_id, status="success",
                              records_processed=0, progress_pct=100,
                              finished_at=datetime.now(timezone.utc))
        return 0

    log.info("sync_mapping.start", total_products=len(products), only_unmapped=only_unmapped)

    # --- Step 2: Build EAN lookups from all 3 sources ---
    async with AsyncSessionLocal() as db:
        await _update_job(db, job_id, progress_pct=10,
                          progress_message="Fetching Ergonode PIM data...")

    # Source 1: Ergonode PIM
    try:
        ergonode_lookup = await fetch_ergonode_ean_lookup()
    except Exception as e:
        log.error("sync_mapping.ergonode_error", error=str(e))
        ergonode_lookup = {}

    # Build ASIN->product lookup from Ergonode (for ASIN fallback)
    # Include both asin_child AND asin_parent for maximum coverage
    asin_lookup: dict[str, dict] = {}
    for ean_val, info in ergonode_lookup.items():
        entry = {
            "internal_sku": info["internal_sku"],
            "k_number": info["k_number"],
            "ergonode_id": info["ergonode_id"],
            "ean": ean_val,
        }
        ac = info.get("asin_child", "")
        if ac and len(ac) >= 5:
            asin_lookup[ac] = entry
        ap = info.get("asin_parent", "")
        if ap and len(ap) >= 5 and ap not in asin_lookup:
            asin_lookup[ap] = entry
    log.info("sync_mapping.asin_lookup_built", entries=len(asin_lookup))

    async with AsyncSessionLocal() as db:
        await _update_job(db, job_id, progress_pct=30,
                          progress_message="Fetching Google Sheet data...")

    # Source 2: Google Sheet
    try:
        gsheet_lookup = await _fetch_gsheet_ean_lookup()
    except Exception as e:
        log.error("sync_mapping.gsheet_error", error=str(e))
        gsheet_lookup = {}

    # --- Step 3: Match products via EAN cascade ---
    async with AsyncSessionLocal() as db:
        await _update_job(db, job_id, progress_pct=50,
                          progress_message="Matching products...")

    mapped_count = 0
    unmapped_eans: list[str] = []

    # First pass — Ergonode + GSheet
    updates: list[dict] = []
    for p in products:
        ean = _extract_ean(p.sku)
        if not ean:
            continue

        # Cascade: Ergonode → GSheet
        if ean in ergonode_lookup:
            info = ergonode_lookup[ean]
            updates.append({
                "product_id": str(p.id),
                "ean": ean,
                "internal_sku": info["internal_sku"],
                "k_number": info["k_number"],
                "ergonode_id": info["ergonode_id"],
                "mapping_source": "ergonode",
            })
        elif ean in gsheet_lookup:
            info = gsheet_lookup[ean]
            updates.append({
                "product_id": str(p.id),
                "ean": ean,
                "internal_sku": info["internal_sku"],
                "k_number": info["k_number"],
                "ergonode_id": "",
                "mapping_source": "gsheet",
            })
        else:
            unmapped_eans.append(ean)

    # Source 3: Baselinker fallback for remaining unmatched
    if unmapped_eans:
        async with AsyncSessionLocal() as db:
            await _update_job(db, job_id, progress_pct=60,
                              progress_message=f"Baselinker fallback for {len(unmapped_eans)} products...")
        try:
            bl_lookup = await _fetch_baselinker_ean_lookup(unmapped_eans)
        except Exception as e:
            log.error("sync_mapping.baselinker_error", error=str(e))
            bl_lookup = {}

        # Match remaining products
        for p in products:
            ean = _extract_ean(p.sku)
            if not ean or ean not in bl_lookup:
                continue
            # Skip if already matched
            if any(u["product_id"] == str(p.id) for u in updates):
                continue
            info = bl_lookup[ean]
            updates.append({
                "product_id": str(p.id),
                "ean": ean,
                "internal_sku": info["internal_sku"],
                "k_number": info["k_number"],
                "ergonode_id": "",
                "mapping_source": "baselinker",
            })

    # --- Step 3b: ASIN fallback for still-unmatched products ---
    matched_ids = {u["product_id"] for u in updates}
    asin_matched = 0
    for p in products:
        if str(p.id) in matched_ids:
            continue
        # Try ASIN match against Ergonode asin_child lookup
        if p.asin and p.asin in asin_lookup:
            info = asin_lookup[p.asin]
            updates.append({
                "product_id": str(p.id),
                "ean": info.get("ean", ""),
                "internal_sku": info["internal_sku"],
                "k_number": info["k_number"],
                "ergonode_id": info["ergonode_id"],
                "mapping_source": "ergonode_asin",
            })
            asin_matched += 1
    if asin_matched:
        log.info("sync_mapping.asin_fallback", matched=asin_matched)

    # --- Step 3c: SP-API ASIN→EAN for still-unmatched products ---
    matched_ids_after_3b = {u["product_id"] for u in updates}
    unmapped_with_asin = [
        p for p in products
        if str(p.id) not in matched_ids_after_3b and p.asin
    ]
    spapi_matched = 0
    if skip_spapi and unmapped_with_asin:
        log.info("sync_mapping.spapi_skipped",
                 unmapped=len(unmapped_with_asin),
                 reason="skip_spapi=True (backfill running)")
    elif unmapped_with_asin:
        async with AsyncSessionLocal() as db:
            await _update_job(db, job_id, progress_pct=70,
                              progress_message=f"SP-API ASIN→EAN for {len(unmapped_with_asin)} products...")
        try:
            from app.connectors.amazon_sp_api.catalog import CatalogClient

            catalog = CatalogClient()
            asin_list = [p.asin for p in unmapped_with_asin]
            items = await catalog.get_items_batch(
                asin_list, included_data="identifiers", batch_size=20,
            )

            # Build ASIN→EAN map from SP-API response
            spapi_asin_to_ean: dict[str, str] = {}
            for item in items:
                item_asin = item.get("asin", "")
                for id_group in item.get("identifiers", []):
                    for ident in id_group.get("identifiers", []):
                        id_type = ident.get("identifierType", "")
                        id_val = ident.get("identifier", "")
                        if id_type == "EAN" and id_val and id_val.isdigit() and len(id_val) >= 8:
                            spapi_asin_to_ean[item_asin] = id_val
                            break
                    if item_asin in spapi_asin_to_ean:
                        break

            log.info("sync_mapping.spapi_ean_found", count=len(spapi_asin_to_ean),
                     total_queried=len(asin_list))

            # Match SP-API EANs against Ergonode + GSheet lookups
            for p in unmapped_with_asin:
                ean = spapi_asin_to_ean.get(p.asin)
                if not ean:
                    continue

                if ean in ergonode_lookup:
                    info = ergonode_lookup[ean]
                    updates.append({
                        "product_id": str(p.id),
                        "ean": ean,
                        "internal_sku": info["internal_sku"],
                        "k_number": info["k_number"],
                        "ergonode_id": info["ergonode_id"],
                        "mapping_source": "spapi_ergonode",
                    })
                    spapi_matched += 1
                elif ean in gsheet_lookup:
                    info = gsheet_lookup[ean]
                    updates.append({
                        "product_id": str(p.id),
                        "ean": ean,
                        "internal_sku": info["internal_sku"],
                        "k_number": info["k_number"],
                        "ergonode_id": "",
                        "mapping_source": "spapi_gsheet",
                    })
                    spapi_matched += 1
                else:
                    # EAN from SP-API but not in any internal lookup —
                    # still store the EAN for reference
                    updates.append({
                        "product_id": str(p.id),
                        "ean": ean,
                        "internal_sku": None,
                        "k_number": None,
                        "ergonode_id": None,
                        "mapping_source": "spapi_ean_only",
                    })
                    spapi_matched += 1

            log.info("sync_mapping.spapi_matched", matched=spapi_matched)

        except Exception as e:
            log.error("sync_mapping.spapi_error", error=str(e))

    # --- Step 4: Write updates to DB ---
    async with AsyncSessionLocal() as db:
        await _update_job(db, job_id, progress_pct=80,
                          progress_message=f"Writing {len(updates)} mappings to DB...")

    if updates:

        def _write_mappings(upds):
            # Azure SQL for acc_* tables (reads + writes)
            conn = connect_acc(timeout=15)
            cursor = conn.cursor()

            # ── Cross-validation: fetch valid SKUs from Holding (Netfox) ──
            nfx = None
            nfx_cur = None
            try:
                nfx = connect_netfox()
                nfx_cur = nfx.cursor()
                nfx_cur.execute(
                    "SELECT DISTINCT LTRIM(RTRIM([Numer artykułu])) "
                    "FROM ITJK_BazaDanychSprzedazHolding WITH (NOLOCK) "
                    "WHERE [Numer artykułu] IS NOT NULL"
                )
                holding_skus = {str(r[0]) for r in nfx_cur.fetchall()}
            except Exception as e:
                log.error("sync_mapping.holding_xval_error", error=str(e))
                holding_skus = None  # None → skip validation
            finally:
                if nfx_cur is not None:
                    nfx_cur.close()
                if nfx is not None:
                    nfx.close()

            # Also check XLSX-sourced prices as secondary validation
            xlsx_skus: set[str] = set()
            if holding_skus is not None:
                try:
                    cursor.execute(
                        "SELECT DISTINCT internal_sku "
                        "FROM acc_purchase_price "
                        "WHERE source = 'xlsx' AND internal_sku IS NOT NULL"
                    )
                    xlsx_skus = {str(r[0]) for r in cursor.fetchall()}
                except Exception:
                    pass  # non-critical

            valid_skus = (holding_skus | xlsx_skus) if holding_skus is not None else None
            if valid_skus is not None:
                log.info("sync_mapping.xval_loaded",
                         holding=len(holding_skus or set()),
                         xlsx=len(xlsx_skus),
                         combined=len(valid_skus))

            count = 0
            skipped = 0
            for u in upds:
                sku = u["internal_sku"]
                # Cross-validate: reject if SKU doesn't exist in Holding or XLSX
                if sku and valid_skus is not None and sku not in valid_skus:
                    log.warning("sync_mapping.xval_rejected",
                                product_id=u["product_id"],
                                internal_sku=sku,
                                source=u["mapping_source"])
                    skipped += 1
                    continue

                try:
                    # --- Controlling: log mapping change ---
                    try:
                        from app.services.controlling import log_mapping_change
                        cursor.execute(
                            "SELECT internal_sku, mapping_source FROM acc_product "
                            "WHERE CAST(id AS VARCHAR(36)) = ?",
                            u["product_id"],
                        )
                        old_row = cursor.fetchone()
                        old_isk = str(old_row[0]) if old_row and old_row[0] else None
                        old_src = str(old_row[1]) if old_row and old_row[1] else None
                        if old_isk != (sku or None):
                            log_mapping_change(
                                conn,
                                product_id=u["product_id"],
                                old_internal_sku=old_isk,
                                new_internal_sku=sku or None,
                                old_source=old_src,
                                new_source=u["mapping_source"],
                                change_type="set" if not old_isk else "update",
                                reason=f"Ergonode sync ({u['mapping_source']})",
                            )
                    except Exception:
                        pass  # controlling is non-blocking

                    cursor.execute(
                        "UPDATE acc_product SET "
                        "ean = ?, internal_sku = ?, k_number = ?, "
                        "ergonode_id = ?, mapping_source = ?, "
                        "updated_at = GETUTCDATE() "
                        "WHERE CAST(id AS VARCHAR(36)) = ?",
                        u["ean"],
                        sku or None,
                        u["k_number"] or None,
                        u["ergonode_id"] or None,
                        u["mapping_source"],
                        u["product_id"],
                    )
                    count += 1
                except Exception as e:
                    log.warning("sync_mapping.update_error",
                                product_id=u["product_id"], error=str(e))

            if skipped:
                log.warning("sync_mapping.xval_summary",
                            skipped=skipped, written=count)
            conn.commit()
            conn.close()
            return count

        import asyncio
        mapped_count = await asyncio.to_thread(_write_mappings, updates)

    # --- Done ---
    sources = {}
    for u in updates:
        s = u["mapping_source"]
        sources[s] = sources.get(s, 0) + 1

    log.info("sync_mapping.complete",
             mapped=mapped_count, total=len(products),
             sources=sources)

    async with AsyncSessionLocal() as db:
        await _update_job(db, job_id,
                          status="success",
                          records_processed=mapped_count,
                          progress_pct=100,
                          progress_message=f"Mapped {mapped_count} products: {sources}",
                          result_summary=str(sources),
                          finished_at=datetime.now(timezone.utc))

    return mapped_count


# ---------------------------------------------------------------------------
# 9. FULL SYNC (orchestrator)
# ---------------------------------------------------------------------------

async def run_full_sync(
    marketplace_id: Optional[str] = None,
    days_back: int = 7,
    job_id: Optional[str] = None,
) -> dict:
    """
    Run all sync tasks in the correct order:
      1. Exchange rates (needed for profit calc)
      2. Orders + order items
      3. Inventory snapshots
      4. Pricing & BuyBox
      5. Product catalog enrichment
      6. Financial events
      7. Product mapping
      8. Purchase prices (Holding FIFO + XLSX)
      9. Profit calculation

    Returns dict with counts per sync type.
    """
    results = {}

    async with AsyncSessionLocal() as db:
        await _update_job(db, job_id, status="running",
                          started_at=datetime.now(timezone.utc),
                          progress_message="Starting full sync...")

    steps = [
        ("exchange_rates", lambda: sync_exchange_rates(days_back=days_back)),
        ("orders", lambda: sync_orders(marketplace_id=marketplace_id, days_back=days_back)),
        ("inventory", lambda: sync_inventory(marketplace_id=marketplace_id)),
        ("pricing", lambda: sync_pricing(marketplace_id=marketplace_id)),
        ("catalog", lambda: sync_catalog(marketplace_id=marketplace_id)),
        ("finances", lambda: _run_step_sync_finances(marketplace_id=marketplace_id, days_back=days_back)),
        ("product_mapping", lambda: sync_product_mapping(only_unmapped=True)),
        ("purchase_prices", lambda: sync_purchase_prices()),
        ("profit", lambda: calc_profit(days_back=days_back, marketplace_id=marketplace_id)),
    ]

    for i, (name, fn) in enumerate(steps, 1):
        try:
            async with AsyncSessionLocal() as db:
                await _update_job(db, job_id,
                                  progress_pct=int(i / len(steps) * 100),
                                  progress_message=f"Running {name}...")

            count = await fn()
            results[name] = {"status": "ok", "count": count}
            log.info(f"full_sync.step_done", step=name, count=count)

        except Exception as e:
            results[name] = {"status": "error", "error": str(e)}
            log.error(f"full_sync.step_error", step=name, error=str(e))

    async with AsyncSessionLocal() as db:
        await _update_job(db, job_id,
                          status="success",
                          progress_pct=100,
                          progress_message="Full sync complete",
                          result_summary=str(results),
                          finished_at=datetime.now(timezone.utc))

    log.info("full_sync.complete", results=results)
    return results
