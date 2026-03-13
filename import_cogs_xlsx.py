"""
Import COGS from Excel files in 'cogs from sell/' folder.

Each XLSX has:
  Col B = internal_sku (Nr art. / SKU / Symbol)
  Col E = Cena netto PLN

Steps:
  1. Read all XLSX, collect internal_sku → price
  2. Upsert acc_purchase_price
  3. Update acc_product.netto_purchase_price_pln
  4. Stamp acc_order_line.purchase_price_pln + cogs_pln
  5. Recalc profit (CM1) via recalc_profit_orders()
"""
import sys, os, glob, time
from datetime import date, datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
os.chdir(os.path.join(os.path.dirname(__file__), "apps", "api"))

from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env", override=True)

import openpyxl
from app.core.db_connection import connect_acc

FOLDER = r"C:\ACC\cogs from sell"
SOURCE = "import_xlsx"
TODAY = date.today()
NOW = datetime.now(timezone.utc)


def read_all_xlsx() -> dict[str, tuple[float, str]]:
    """Read all XLSX files → {internal_sku: (price, source_file)}."""
    prices: dict[str, tuple[float, str]] = {}

    for fpath in sorted(glob.glob(os.path.join(FOLDER, "*.xlsx"))):
        fname = os.path.basename(fpath)
        # Skip inventory/GRN files (no price column)
        if fname.lower().startswith("stant"):
            print(f"  SKIP (inventory): {fname}")
            continue

        wb = openpyxl.load_workbook(fpath, read_only=True, data_only=True)
        for ws in wb.worksheets:
            for i, row in enumerate(ws.iter_rows(min_row=2, values_only=True)):
                if not row or len(row) < 5:
                    continue
                raw_sku = row[1]   # Col B
                raw_price = row[4] # Col E

                if raw_sku is None or raw_price is None:
                    continue

                sku = str(int(raw_sku)) if isinstance(raw_sku, float) else str(raw_sku).strip()
                if not sku or not sku.isdigit():
                    continue

                try:
                    price = float(raw_price)
                except (ValueError, TypeError):
                    continue

                if price <= 0:
                    continue

                # Keep last-seen price (newest file wins)
                prices[sku] = (round(price, 4), fname)
        wb.close()
        print(f"  {fname}: processed")

    return prices


def main():
    t0 = time.time()
    print("=" * 60)
    print("  IMPORT COGS from 'cogs from sell/' XLSX files")
    print("=" * 60)

    # ── 1. Read all files ──────────────────────────────────
    print("\n[1] Reading XLSX files...")
    prices = read_all_xlsx()
    print(f"\n  → {len(prices)} unique SKUs with price")
    for sku, (price, fname) in list(prices.items())[:5]:
        print(f"     {sku}: {price} PLN ({fname})")

    # ── 2. DB operations ──────────────────────────────────
    conn = connect_acc(autocommit=False)
    cur = conn.cursor()
    cur.execute("SET LOCK_TIMEOUT 30000")

    # Get existing state — per source so we only touch xlsx_oficjalne rows
    cur.execute("SELECT internal_sku, netto_price_pln, source FROM acc_purchase_price WITH (NOLOCK)")
    existing_pp = {}        # internal_sku → {source: price}
    for r in cur.fetchall():
        sku_key = str(r[0])
        src = str(r[2]) if r[2] else ''
        existing_pp.setdefault(sku_key, {})[src] = float(r[1])
    print(f"\n  Existing acc_purchase_price rows: {sum(len(v) for v in existing_pp.values())}")

    # SKUs that have manual prices — DO NOT overwrite these in product cache
    manual_skus = {
        sku for sku, srcs in existing_pp.items()
        if 'manual' in srcs
    }
    print(f"  SKUs with manual prices (protected): {len(manual_skus)}")

    cur.execute("SELECT internal_sku, CAST(id AS VARCHAR(36)), netto_purchase_price_pln FROM acc_product WITH (NOLOCK) WHERE internal_sku IS NOT NULL")
    product_map = {}  # internal_sku → (product_id, current_price)
    for r in cur.fetchall():
        product_map[str(r[0])] = (str(r[1]), float(r[2]) if r[2] else None)
    print(f"  Products with internal_sku: {len(product_map)}")

    # ── 3. Upsert purchase prices + update products ───────
    print("\n[2] Upserting prices...")
    pp_inserted = 0
    pp_updated = 0
    pp_skipped_holding = 0
    prod_updated = 0
    prod_skipped_holding = 0

    for sku, (price, fname) in prices.items():
        # acc_purchase_price — only touch the xlsx_oficjalne row
        sources_for_sku = existing_pp.get(sku, {})
        xlsx_price = sources_for_sku.get(SOURCE)
        if xlsx_price is not None:
            if abs(xlsx_price - price) > 0.001:
                cur.execute(
                    "UPDATE acc_purchase_price SET netto_price_pln = ?, "
                    "source_document = ?, updated_at = GETUTCDATE() "
                    "WHERE internal_sku = ? AND source = ?",
                    price, fname, sku, SOURCE
                )
                pp_updated += 1
        elif SOURCE not in sources_for_sku:
            # No xlsx_oficjalne row yet — insert one (doesn't touch other sources)
            cur.execute(
                "INSERT INTO acc_purchase_price "
                "(internal_sku, netto_price_pln, valid_from, source, source_document, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, GETUTCDATE(), GETUTCDATE())",
                sku, price, TODAY, SOURCE, fname
            )
            pp_inserted += 1

        # acc_product cache — SKIP if manual price exists (higher priority)
        if sku in manual_skus:
            pp_skipped_holding += 1
            continue

        if sku in product_map:
            pid, old_pp = product_map[sku]
            if old_pp is None or abs((old_pp or 0) - price) > 0.001:
                cur.execute(
                    "UPDATE acc_product SET netto_purchase_price_pln = ?, updated_at = GETUTCDATE() "
                    "WHERE internal_sku = ?",
                    price, sku
                )
                prod_updated += 1

    conn.commit()
    print(f"  acc_purchase_price: {pp_inserted} inserted, {pp_updated} updated")
    print(f"  acc_purchase_price: {pp_skipped_holding} skipped (manual exists)")
    print(f"  acc_product: {prod_updated} price-cache updated, {prod_skipped_holding} skipped (manual protected)")

    # ── 4. Stamp order lines ──────────────────────────────
    print("\n[3] Stamping order lines...")
    cur.execute("""
        UPDATE ol
        SET ol.purchase_price_pln = p.netto_purchase_price_pln,
            ol.cogs_pln = p.netto_purchase_price_pln * ISNULL(ol.quantity_ordered, 1),
            ol.price_source = CASE 
                WHEN ol.price_source IS NULL THEN 'auto'
                ELSE ol.price_source
            END
        FROM acc_order_line ol
        INNER JOIN acc_product p ON p.id = ol.product_id
        WHERE ol.purchase_price_pln IS NULL
          AND p.netto_purchase_price_pln IS NOT NULL
          AND p.netto_purchase_price_pln > 0
    """)
    lines_new = cur.rowcount
    conn.commit()
    print(f"  New lines stamped: {lines_new}")

    # Also OVERWRITE lines where price changed (re-stamp from updated product)
    # import_xlsx = wrzutki od zakupu = najświeższe ceny per dostawa
    # Override everything EXCEPT manual (priority 1)
    cur.execute("""
        UPDATE ol
        SET ol.purchase_price_pln = p.netto_purchase_price_pln,
            ol.cogs_pln = p.netto_purchase_price_pln * ISNULL(ol.quantity_ordered, 1),
            ol.price_source = 'import_xlsx'
        FROM acc_order_line ol
        INNER JOIN acc_product p ON p.id = ol.product_id
        WHERE p.netto_purchase_price_pln IS NOT NULL
          AND p.netto_purchase_price_pln > 0
          AND ol.purchase_price_pln IS NOT NULL
          AND ABS(ol.purchase_price_pln - p.netto_purchase_price_pln) > 0.001
          AND ISNULL(ol.price_source, '') NOT IN ('manual')
    """)
    lines_refreshed = cur.rowcount
    conn.commit()
    print(f"  Lines price-refreshed (changed): {lines_refreshed}")

    # ── 5. Coverage check ─────────────────────────────────
    cur.execute("""
        SELECT 
          COUNT(*) as total,
          SUM(CASE WHEN ol.purchase_price_pln > 0 THEN 1 ELSE 0 END) as has_pp,
          CAST(SUM(CASE WHEN ol.purchase_price_pln > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as pct
        FROM acc_order_line ol WITH (NOLOCK)
        JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        WHERE o.status NOT IN ('Cancelled','Canceled','Pending')
          AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
    """)
    r = cur.fetchone()
    print(f"\n  COGS coverage: {r[1]}/{r[0]} ({r[2]}%)")

    # Revenue-weighted
    cur.execute("""
        SELECT 
          CAST(SUM(CASE WHEN ol.purchase_price_pln > 0 THEN ISNULL(ol.item_price,0) ELSE 0 END) * 100.0 
            / NULLIF(SUM(ISNULL(ol.item_price,0)), 0) AS DECIMAL(5,2))
        FROM acc_order_line ol WITH (NOLOCK)
        JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        WHERE o.status NOT IN ('Cancelled','Canceled','Pending')
          AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
    """)
    print(f"  Revenue-weighted: {cur.fetchone()[0]}%")

    conn.close()

    # ── 6. Recalc profit ──────────────────────────────────
    print("\n[4] Recalculating profit (CM1)...")
    from app.connectors.mssql.mssql_store import recalc_profit_orders
    t_recalc = time.time()
    count = recalc_profit_orders(date_from=date(2024, 1, 1), date_to=date.today())
    print(f"  Recalculated {count} orders in {time.time()-t_recalc:.1f}s")

    # ── 7. Final stats ────────────────────────────────────
    conn2 = connect_acc()
    cur2 = conn2.cursor()
    cur2.execute("""
        SELECT 
          COUNT(*) as total,
          SUM(CASE WHEN ol.purchase_price_pln > 0 THEN 1 ELSE 0 END) as has_pp,
          CAST(SUM(CASE WHEN ol.purchase_price_pln > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as pct
        FROM acc_order_line ol WITH (NOLOCK)
        JOIN acc_order o WITH (NOLOCK) ON o.id = ol.order_id
        WHERE o.status NOT IN ('Cancelled','Canceled','Pending')
          AND ISNULL(o.sales_channel, 'Amazon.com') != 'Non-Amazon'
    """)
    r = cur2.fetchone()
    print(f"\n{'=' * 60}")
    print(f"  FINAL COGS COVERAGE: {r[1]}/{r[0]} ({r[2]}%)")

    cur2.execute("""
        SELECT 
          SUM(revenue_pln), SUM(cogs_pln), SUM(contribution_margin_pln),
          CAST(SUM(contribution_margin_pln) * 100.0 / NULLIF(SUM(revenue_pln), 0) AS DECIMAL(5,2))
        FROM acc_order WITH (NOLOCK)
        WHERE status NOT IN ('Cancelled','Canceled','Pending')
          AND ISNULL(sales_channel, 'Amazon.com') != 'Non-Amazon'
          AND revenue_pln > 0
    """)
    r = cur2.fetchone()
    print(f"  Revenue: {r[0]:,.2f} PLN")
    print(f"  COGS:    {r[1]:,.2f} PLN")
    print(f"  CM1:     {r[2]:,.2f} PLN ({r[3]}%)")
    print(f"{'=' * 60}")
    print(f"\n  Total time: {time.time()-t0:.1f}s")
    conn2.close()


if __name__ == "__main__":
    main()
