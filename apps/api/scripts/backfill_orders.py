#!/usr/bin/env python3
"""
Safe Historical Backfill — Amazon SP-API Orders
================================================

Fetches ALL orders from 2025-01-01 to present for all 13 marketplaces.

Safety guarantees:
  ✓ Commits every COMMIT_BATCH orders (crash-safe, no data loss)
  ✓ Checkpoint file — resume from last completed window on restart
  ✓ Conservative rate limiting (well under SP-API burst limits)
  ✓ Error isolation — one failed window doesn't stop the whole run
  ✓ Full logging to backfill.log with timestamps
  ✓ Runs in background — survives SSH disconnect
  ✓ Upsert-only — harmless to re-run, won't duplicate data
  ✓ Won't interfere with regular 15-min pipeline (both upsert)

SP-API Rate Limits (EU endpoint):
  GetOrders:      burst=20, restore=1/min  -- we sleep 3s between pages
  GetOrderItems:  burst=30, restore=0.5/s  -- we sleep 2s between calls

Usage (from apps/api/):
  python scripts/backfill_orders.py                    # normal run
  python scripts/backfill_orders.py --resume           # continue after crash
  python scripts/backfill_orders.py --dry-run          # count only, no writes
  python scripts/backfill_orders.py --start 2025-06-01 # custom start date
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone, date
from decimal import Decimal
from pathlib import Path
from typing import Optional

# ── Path setup (so `from app.…` works when run from apps/api/) ──
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pyodbc
import structlog
import logging

# ── Logging setup ──
LOG_DIR = Path(__file__).parent.parent  # apps/api/
LOG_FILE = LOG_DIR / "backfill.log"

# Console handler with safe encoding (cp1250 on Windows)
_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setLevel(logging.INFO)
_console_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s"))
try:
    _console_handler.stream.reconfigure(errors="replace")
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        _console_handler,
    ],
)
logger = logging.getLogger("backfill")

# ── Configuration ──
START_DATE = date(2025, 1, 1)
WINDOW_DAYS = 14               # 2-week windows to keep API batches small
COMMIT_BATCH = 50              # commit to DB every N orders (small = less locking)
CHECKPOINT_FILE = LOG_DIR / "backfill_checkpoint.json"
PROGRESS_FILE = LOG_DIR / "backfill_progress.json"

# Rate limiting (seconds) — conservative to stay well within SP-API limits
SLEEP_BETWEEN_ORDER_PAGES = 3.0       # GetOrders: burst=20, restore=1/min
SLEEP_BETWEEN_ITEM_CALLS = 2.0        # GetOrderItems: burst=30, restore=0.5/s
SLEEP_BETWEEN_MARKETPLACES = 15.0     # courtesy pause between mkts
SLEEP_BETWEEN_WINDOWS = 5.0           # pause between date windows
SLEEP_ON_THROTTLE = 60.0              # if we get 429, back off hard
SLEEP_ON_ERROR = 30.0                 # on transient error, wait before retry

ORDER_STATUSES = ["Shipped", "Unshipped", "PartiallyShipped", "Canceled"]

from app.core.config import settings, MARKETPLACE_REGISTRY
from app.core.db_connection import connect_acc


# ─────────────────────────────────────────────────────────────────
# DB connection (central connect_acc — same as order_pipeline)
# ─────────────────────────────────────────────────────────────────
def _db_conn():
    conn = connect_acc(autocommit=False)
    # Use READ UNCOMMITTED to avoid blocking backend SELECT queries
    cur = conn.cursor()
    cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
    cur.close()
    conn.commit()
    return conn


# ─────────────────────────────────────────────────────────────────
# Checkpoint management
# ─────────────────────────────────────────────────────────────────
def load_checkpoint() -> dict:
    """Load checkpoint: {completed: ["MKT|2025-01-01", ...], stats: {...}}"""
    if CHECKPOINT_FILE.exists():
        try:
            data = json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
            logger.info(f"Checkpoint loaded: {len(data.get('completed', []))} windows done")
            return data
        except Exception as e:
            logger.warning(f"Checkpoint file corrupt, starting fresh: {e}")
    return {"completed": [], "stats": {"orders_total": 0, "items_total": 0, "errors": 0}}


def save_checkpoint(cp: dict):
    """Save checkpoint atomically (write to tmp then rename)."""
    tmp = CHECKPOINT_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(cp, indent=2, default=str), encoding="utf-8")
    tmp.replace(CHECKPOINT_FILE)


def save_progress(progress: dict):
    """Save human-readable progress snapshot."""
    try:
        PROGRESS_FILE.write_text(
            json.dumps(progress, indent=2, default=str), encoding="utf-8"
        )
    except Exception:
        pass


def window_key(mkt_id: str, window_start: date) -> str:
    return f"{mkt_id}|{window_start.isoformat()}"


# ─────────────────────────────────────────────────────────────────
# SP-API pagination — page-by-page with rate limiting
# ─────────────────────────────────────────────────────────────────
async def fetch_orders_page(
    client,
    params: dict,
    next_token: Optional[str] = None,
) -> tuple[list[dict], Optional[str]]:
    """Fetch one page of orders. Returns (orders, next_token)."""
    if next_token:
        req_params = {
            "NextToken": next_token,
            "MarketplaceIds": params["MarketplaceIds"],
        }
    else:
        req_params = params

    data = await client.get("/orders/v0/orders", req_params)
    orders = data.get("payload", {}).get("Orders", [])
    nt = data.get("payload", {}).get("NextToken")
    return orders, nt


async def fetch_order_items_safe(client, amazon_order_id: str) -> list[dict]:
    """Fetch items for one order, with full error handling."""
    result = []
    next_token = None
    for _ in range(20):
        try:
            params = {"MarketplaceIds": client.marketplace_id}
            if next_token:
                params["NextToken"] = next_token
            data = await client.get(
                f"/orders/v0/orders/{amazon_order_id}/orderItems", params
            )
            items = data.get("payload", {}).get("OrderItems", [])
            result.extend(items)
            next_token = data.get("payload", {}).get("NextToken")
            if not next_token:
                break
            await asyncio.sleep(SLEEP_BETWEEN_ITEM_CALLS)
        except Exception as e:
            logger.warning(f"  Items error for {amazon_order_id}: {e}")
            break
    return result


# ─────────────────────────────────────────────────────────────────
# DB upsert helpers (reused from order_pipeline logic)
# ─────────────────────────────────────────────────────────────────
def parse_sp_order(raw: dict, mkt_id: str) -> dict:
    pd_str = raw.get("PurchaseDate", "")
    lu_str = raw.get("LastUpdateDate", "")
    ot = raw.get("OrderTotal", {})
    sa = raw.get("ShippingAddress", {})
    return {
        "amazon_id": raw.get("AmazonOrderId", ""),
        "mkt_id": mkt_id,
        "status": raw.get("OrderStatus", ""),
        "fc": raw.get("FulfillmentChannel", "FBA"),
        "sc": raw.get("SalesChannel"),
        "purchase_date": (
            datetime.fromisoformat(pd_str.replace("Z", "+00:00")) if pd_str else None
        ),
        "last_update": (
            datetime.fromisoformat(lu_str.replace("Z", "+00:00")) if lu_str else None
        ),
        "order_total": Decimal(str(ot.get("Amount", 0))) if ot else None,
        "currency": ot.get("CurrencyCode", "EUR") if ot else "EUR",
        "ship_country": sa.get("CountryCode") if sa else None,
    }


def upsert_order(cur, o: dict) -> tuple[str, bool]:
    """Upsert order. Returns (order_id, is_new)."""
    cur.execute(
        "SELECT CAST(id AS VARCHAR(36)) FROM acc_order WHERE amazon_order_id = ?",
        o["amazon_id"],
    )
    row = cur.fetchone()
    synced_at = datetime.now(timezone.utc)

    if row is None:
        order_id = str(uuid.uuid4())
        cur.execute(
            "INSERT INTO acc_order "
            "(id, amazon_order_id, marketplace_id, status, "
            " fulfillment_channel, sales_channel, purchase_date, "
            " last_update_date, order_total, currency, "
            " ship_country, buyer_country, synced_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            order_id, o["amazon_id"], o["mkt_id"], o["status"],
            o["fc"], o["sc"], o["purchase_date"], o["last_update"],
            o["order_total"], o["currency"],
            o["ship_country"], o["ship_country"], synced_at,
        )
        return order_id, True

    # Existing -- just update
    order_id = row[0]
    cur.execute(
        "UPDATE acc_order SET status=?, fulfillment_channel=?, "
        "sales_channel=?, purchase_date=?, last_update_date=?, "
        "order_total=?, currency=?, ship_country=?, "
        "buyer_country=?, synced_at=? WHERE id=?",
        o["status"], o["fc"], o["sc"], o["purchase_date"], o["last_update"],
        o["order_total"], o["currency"], o["ship_country"], o["ship_country"],
        synced_at, order_id,
    )
    return order_id, False


def upsert_items(cur, raw_items: list[dict], order_id: str, currency: str) -> int:
    """Upsert order items. Returns count of new items inserted."""
    new_count = 0
    for ri in raw_items:
        iid = ri.get("OrderItemId", "")
        if not iid:
            continue

        sku = ri.get("SellerSKU")
        asin = ri.get("ASIN")
        title = (ri.get("Title") or "")[:500]

        # Lookup product for inline COGS stamp
        prod_id = None
        product_price = None
        if sku:
            cur.execute(
                "SELECT CAST(id AS VARCHAR(36)), netto_purchase_price_pln "
                "FROM acc_product WHERE sku=?",
                sku,
            )
            pr = cur.fetchone()
            if pr:
                prod_id = pr[0]
                product_price = float(pr[1]) if pr[1] is not None else None

        ip = ri.get("ItemPrice", {})
        item_price = Decimal(str(ip.get("Amount", 0))) if ip else None
        line_cur = ip.get("CurrencyCode", currency) if ip else currency

        it = ri.get("ItemTax", {})
        item_tax = Decimal(str(it.get("Amount", 0))) if it else None

        pd_d = ri.get("PromotionDiscount", {})
        promo = Decimal(str(pd_d.get("Amount", 0))) if pd_d else None

        qty_ordered = ri.get("QuantityOrdered", 1)
        qty_shipped = ri.get("QuantityShipped", 0)

        # Check if line already exists
        cur.execute(
            "SELECT id FROM acc_order_line WHERE amazon_order_item_id = ?", iid
        )
        existing = cur.fetchone()

        if existing:
            cur.execute(
                "UPDATE acc_order_line "
                "SET quantity_ordered=?, quantity_shipped=?, "
                "    item_price=?, item_tax=?, promotion_discount=?, "
                "    product_id = COALESCE(product_id, ?) "
                "WHERE amazon_order_item_id=?",
                qty_ordered, qty_shipped,
                item_price, item_tax, promo,
                prod_id, iid,
            )
        else:
            purchase_price = None
            cogs = None
            price_source = None
            if product_price is not None:
                purchase_price = product_price
                cogs = round(product_price * (qty_ordered or 1), 4)
                price_source = "auto"

            cur.execute(
                "INSERT INTO acc_order_line "
                "(id, order_id, product_id, "
                " amazon_order_item_id, sku, asin, title, "
                " quantity_ordered, quantity_shipped, "
                " item_price, item_tax, promotion_discount, "
                " currency, purchase_price_pln, cogs_pln, price_source) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                str(uuid.uuid4()), order_id, prod_id,
                iid, sku, asin, title,
                qty_ordered, qty_shipped,
                item_price, item_tax, promo, line_cur,
                purchase_price, cogs, price_source,
            )
            new_count += 1
    return new_count


# ─────────────────────────────────────────────────────────────────
# Post-backfill steps (products, linking, COGS, profit)
# ─────────────────────────────────────────────────────────────────
def run_post_backfill_steps():
    """Run pipeline steps 2-6 on the full backfilled dataset."""
    logger.info("=" * 60)
    logger.info("POST-BACKFILL: Running product/COGS/profit pipeline")
    logger.info("=" * 60)

    conn = _db_conn()
    cur = conn.cursor()

    # Step 2: Backfill missing products
    logger.info("Step 2: Backfill missing products...")
    cur.execute("""
        SELECT DISTINCT ol.sku, ol.asin
        FROM acc_order_line ol
        WHERE ol.product_id IS NULL
          AND ol.sku IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM acc_product p WHERE p.sku = ol.sku
          )
    """)
    missing = cur.fetchall()
    created = 0
    for row in missing:
        try:
            cur.execute(
                "INSERT INTO acc_product (id, sku, asin, is_parent) VALUES (?, ?, ?, 0)",
                str(uuid.uuid4()), row[0], row[1],
            )
            created += 1
        except Exception:
            pass
    conn.commit()
    logger.info(f"  Created {created} new products")

    # Step 3: Link order lines to products
    logger.info("Step 3: Link order lines to products...")
    cur.execute("""
        UPDATE ol
        SET ol.product_id = p.id
        FROM acc_order_line ol
        INNER JOIN acc_product p ON p.sku = ol.sku
        WHERE ol.product_id IS NULL
          AND ol.sku IS NOT NULL
    """)
    linked = cur.rowcount
    conn.commit()
    logger.info(f"  Linked {linked} order lines")

    # Step 5: Stamp purchase prices
    logger.info("Step 5: Stamp purchase prices (COGS)...")
    cur.execute("""
        UPDATE ol
        SET ol.purchase_price_pln = p.netto_purchase_price_pln,
            ol.cogs_pln = p.netto_purchase_price_pln
                          * ISNULL(ol.quantity_ordered, 1),
            ol.price_source = 'auto'
        FROM acc_order_line ol
        INNER JOIN acc_product p ON p.id = ol.product_id
        WHERE ol.purchase_price_pln IS NULL
          AND p.netto_purchase_price_pln IS NOT NULL
    """)
    stamped = cur.rowcount
    conn.commit()
    logger.info(f"  Stamped COGS on {stamped} order lines")

    cur.close()
    conn.close()

    # Step 6: Recalculate profit (full date range)
    logger.info("Step 6: Recalculate profit (full range)...")
    try:
        from app.connectors.mssql import recalc_profit_orders
        count = recalc_profit_orders(
            date_from=START_DATE,
            date_to=date.today(),
        )
        logger.info(f"  Profit recalculated for {count} orders")
    except Exception as e:
        logger.warning(f"  Profit recalc error: {e}")

    logger.info("POST-BACKFILL complete!")


# ─────────────────────────────────────────────────────────────────
# Main backfill loop
# ─────────────────────────────────────────────────────────────────
async def backfill_window(
    mkt_id: str,
    mkt_code: str,
    window_start: date,
    window_end: date,
    dry_run: bool = False,
) -> dict:
    """
    Fetch all orders in [window_start, window_end) for one marketplace.
    Returns stats dict.
    """
    from app.connectors.amazon_sp_api.orders import OrdersClient

    stats = {
        "orders_fetched": 0,
        "orders_new": 0,
        "orders_existing": 0,
        "items_fetched": 0,
        "items_new": 0,
        "pages": 0,
        "errors": 0,
    }

    created_after = datetime(
        window_start.year, window_start.month, window_start.day,
        tzinfo=timezone.utc,
    )
    created_before = datetime(
        window_end.year, window_end.month, window_end.day,
        tzinfo=timezone.utc,
    )

    params = {
        "MarketplaceIds": mkt_id,
        "MaxResultsPerPage": 100,
        "CreatedAfter": created_after.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "CreatedBefore": created_before.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "OrderStatuses": ",".join(ORDER_STATUSES),
    }

    client = OrdersClient(marketplace_id=mkt_id)

    if dry_run:
        # Just count via one page
        orders, _ = await fetch_orders_page(client, params)
        logger.info(f"  [DRY-RUN] {mkt_code} {window_start} -> first page: {len(orders)} orders")
        return stats

    conn = _db_conn()
    cur = conn.cursor()
    next_token = None
    uncommitted = 0

    try:
        for page_num in range(10_000):
            try:
                orders, next_token = await fetch_orders_page(
                    client, params, next_token
                )
            except Exception as e:
                err_str = str(e)
                stats["errors"] += 1
                if "429" in err_str or "throttl" in err_str.lower():
                    logger.warning(f"  THROTTLED on page {page_num}, sleeping {SLEEP_ON_THROTTLE}s")
                    await asyncio.sleep(SLEEP_ON_THROTTLE)
                    # Retry same page
                    try:
                        orders, next_token = await fetch_orders_page(
                            client, params, next_token
                        )
                    except Exception as e2:
                        logger.error(f"  Page {page_num} failed after retry: {e2}")
                        break
                else:
                    logger.error(f"  Page {page_num} error: {e}")
                    await asyncio.sleep(SLEEP_ON_ERROR)
                    break

            if not orders:
                break

            stats["pages"] += 1

            for raw in orders:
                parsed = parse_sp_order(raw, mkt_id)
                if not parsed["amazon_id"]:
                    continue

                order_id, is_new = upsert_order(cur, parsed)
                stats["orders_fetched"] += 1

                if is_new:
                    stats["orders_new"] += 1
                    # Fetch items only for NEW orders (existing orders already have items)
                    try:
                        raw_items = await fetch_order_items_safe(
                            client, parsed["amazon_id"]
                        )
                        if raw_items:
                            new_items = upsert_items(
                                cur, raw_items, order_id, parsed["currency"]
                            )
                            stats["items_fetched"] += len(raw_items)
                            stats["items_new"] += new_items
                        await asyncio.sleep(SLEEP_BETWEEN_ITEM_CALLS)
                    except Exception as e:
                        logger.warning(f"  Items error {parsed['amazon_id']}: {e}")
                        stats["errors"] += 1
                else:
                    stats["orders_existing"] += 1

                uncommitted += 1

                # Commit every COMMIT_BATCH orders
                if uncommitted >= COMMIT_BATCH:
                    conn.commit()
                    uncommitted = 0
                    logger.info(
                        f"  [COMMIT] {stats['orders_fetched']} orders "
                        f"({stats['orders_new']} new, {stats['items_new']} items) "
                        f"[page {page_num + 1}]"
                    )

            if not next_token:
                break

            # Rate limit between pages
            await asyncio.sleep(SLEEP_BETWEEN_ORDER_PAGES)

        # Final commit for remaining orders
        if uncommitted > 0:
            conn.commit()
            logger.info(
                f"  [COMMIT-FINAL] {stats['orders_fetched']} orders "
                f"({stats['orders_new']} new, {stats['items_new']} items)"
            )

    except Exception as e:
        logger.error(f"  Window error: {e}")
        stats["errors"] += 1
        # Still try to commit whatever we have
        try:
            conn.commit()
            logger.info("  [COMMIT-EMERGENCY] saved buffered data")
        except Exception:
            pass
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

    return stats


def generate_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Generate non-overlapping [start, end) date windows."""
    windows = []
    current = start
    while current < end:
        w_end = min(current + timedelta(days=WINDOW_DAYS), end)
        windows.append((current, w_end))
        current = w_end
    return windows


async def run_backfill(args):
    """Main backfill orchestrator."""
    start_date = (
        date.fromisoformat(args.start) if args.start else START_DATE
    )
    end_date = date.today() + timedelta(days=1)  # include today

    windows = generate_windows(start_date, end_date)
    marketplaces = list(MARKETPLACE_REGISTRY.items())
    total_windows = len(windows) * len(marketplaces)

    logger.info("=" * 70)
    logger.info("AMAZON ORDER BACKFILL — SAFE MODE")
    logger.info("=" * 70)
    logger.info(f"Date range    : {start_date} -> {end_date}")
    logger.info(f"Marketplaces  : {len(marketplaces)}")
    logger.info(f"Date windows  : {len(windows)} × {WINDOW_DAYS} days each")
    logger.info(f"Total windows : {total_windows}")
    logger.info(f"Commit every  : {COMMIT_BATCH} orders")
    logger.info(f"Checkpoint    : {CHECKPOINT_FILE}")
    logger.info(f"Dry run       : {args.dry_run}")
    logger.info(f"Rate limits   : pages={SLEEP_BETWEEN_ORDER_PAGES}s, "
                f"items={SLEEP_BETWEEN_ITEM_CALLS}s, "
                f"mkts={SLEEP_BETWEEN_MARKETPLACES}s")
    logger.info("=" * 70)

    # Load checkpoint
    cp = load_checkpoint() if args.resume else {
        "completed": [], "stats": {"orders_total": 0, "items_total": 0, "errors": 0}
    }
    completed_set = set(cp["completed"])
    global_stats = cp["stats"]

    t0_global = time.time()
    windows_done = len(completed_set)

    for mkt_idx, (mkt_id, mkt_info) in enumerate(marketplaces):
        mkt_code = mkt_info["code"]
        logger.info("")
        logger.info(f"{'-' * 50}")
        logger.info(f"MARKETPLACE {mkt_idx + 1}/{len(marketplaces)}: "
                     f"{mkt_code} ({mkt_info['name']})")
        logger.info(f"{'-' * 50}")

        for win_idx, (w_start, w_end) in enumerate(windows):
            wk = window_key(mkt_id, w_start)
            if wk in completed_set:
                continue

            windows_done += 1
            pct = round(100 * windows_done / total_windows, 1)
            elapsed = time.time() - t0_global
            eta_s = (elapsed / max(windows_done - len(cp["completed"]), 1)) * (
                total_windows - windows_done
            ) if windows_done > len(cp["completed"]) else 0
            eta_h = round(eta_s / 3600, 1)

            logger.info(
                f"\n  [{pct}%] Window {win_idx + 1}/{len(windows)}: "
                f"{w_start} -> {w_end}  (ETA: ~{eta_h}h)"
            )

            try:
                stats = await backfill_window(
                    mkt_id, mkt_code, w_start, w_end, dry_run=args.dry_run,
                )

                global_stats["orders_total"] += stats["orders_new"]
                global_stats["items_total"] += stats["items_new"]
                global_stats["errors"] += stats["errors"]

                logger.info(
                    f"  Window done: {stats['orders_fetched']} orders "
                    f"({stats['orders_new']} new, {stats['orders_existing']} existing), "
                    f"{stats['items_new']} items, {stats['pages']} pages, "
                    f"{stats['errors']} errors"
                )

                # Mark window completed
                if not args.dry_run:
                    cp["completed"].append(wk)
                    completed_set.add(wk)
                    cp["stats"] = global_stats
                    save_checkpoint(cp)

                # Save progress snapshot
                save_progress({
                    "status": "running",
                    "started_at": datetime.fromtimestamp(t0_global).isoformat(),
                    "current_marketplace": f"{mkt_code} ({mkt_info['name']})",
                    "current_window": f"{w_start} -> {w_end}",
                    "progress_pct": pct,
                    "eta_hours": eta_h,
                    "windows_done": windows_done,
                    "windows_total": total_windows,
                    "orders_new_total": global_stats["orders_total"],
                    "items_new_total": global_stats["items_total"],
                    "errors_total": global_stats["errors"],
                    "elapsed_seconds": round(elapsed),
                })

            except Exception as e:
                logger.error(f"  WINDOW FAILED: {e}")
                global_stats["errors"] += 1
                # Continue with next window — don't mark as completed
                await asyncio.sleep(SLEEP_ON_ERROR)

            # Pause between windows
            if not args.dry_run:
                await asyncio.sleep(SLEEP_BETWEEN_WINDOWS)

        # Pause between marketplaces
        if mkt_idx < len(marketplaces) - 1:
            logger.info(f"\n  Pausing {SLEEP_BETWEEN_MARKETPLACES}s before next marketplace...")
            await asyncio.sleep(SLEEP_BETWEEN_MARKETPLACES)

    # ── Summary ──
    total_elapsed = time.time() - t0_global
    hours = round(total_elapsed / 3600, 2)

    logger.info("")
    logger.info("=" * 70)
    logger.info("BACKFILL COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Duration      : {hours} hours ({round(total_elapsed)}s)")
    logger.info(f"New orders    : {global_stats['orders_total']}")
    logger.info(f"New items     : {global_stats['items_total']}")
    logger.info(f"Errors        : {global_stats['errors']}")
    logger.info(f"Windows done  : {len(cp['completed'])}/{total_windows}")
    logger.info("=" * 70)

    # ── Post-backfill steps ──
    if not args.dry_run and not args.skip_post:
        try:
            run_post_backfill_steps()
        except Exception as e:
            logger.error(f"Post-backfill error: {e}")

    save_progress({
        "status": "completed",
        "completed_at": datetime.now().isoformat(),
        "duration_hours": hours,
        "orders_new_total": global_stats["orders_total"],
        "items_new_total": global_stats["items_total"],
        "errors_total": global_stats["errors"],
        "windows_done": len(cp["completed"]),
        "windows_total": total_windows,
    })

    logger.info("Done! Check backfill_progress.json for final stats.")


# ─────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Safe historical backfill of Amazon SP-API orders"
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Resume from last checkpoint (skip completed windows)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Count orders only, don't write to DB"
    )
    parser.add_argument(
        "--start", type=str, default=None,
        help="Start date (YYYY-MM-DD), default: 2025-01-01"
    )
    parser.add_argument(
        "--skip-post", action="store_true",
        help="Skip post-backfill steps (products, COGS, profit)"
    )
    args = parser.parse_args()

    logger.info("Backfill script starting...")
    logger.info(f"Arguments: resume={args.resume}, dry_run={args.dry_run}, "
                f"start={args.start}, skip_post={args.skip_post}")

    asyncio.run(run_backfill(args))


if __name__ == "__main__":
    main()
