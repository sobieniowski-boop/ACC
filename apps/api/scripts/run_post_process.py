"""
Post-processing: link order lines to products + stamp COGS.
Safe to run WHILE backfill is still adding orders.
No SP-API calls — Azure SQL only.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".env"), override=True)

import uuid
from app.core.db_connection import connect_acc

def main():
    print("=" * 60)
    print("  POST-PROCESSING (safe, Azure SQL only)")
    print("=" * 60)

    conn = connect_acc(autocommit=False)
    cur = conn.cursor()

    # ── Step 2: Create missing products ──
    print("\n[Step 2] Create missing products...")
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
    print(f"  Created {created} new products")

    # ── Step 3: Link order lines to products (batch by product) ──
    print("\n[Step 3] Link order lines → products (batched)...")
    cur.execute("""
        SELECT DISTINCT ol.sku
        FROM acc_order_line ol
        WHERE ol.product_id IS NULL AND ol.sku IS NOT NULL
    """)
    unlinked_skus = [r[0] for r in cur.fetchall()]
    linked = 0
    for sku_val in unlinked_skus:
        try:
            cur.execute(
                "UPDATE acc_order_line "
                "SET product_id = (SELECT TOP 1 id FROM acc_product WHERE sku = ?) "
                "WHERE sku = ? AND product_id IS NULL",
                sku_val, sku_val,
            )
            linked += cur.rowcount or 0
            conn.commit()
        except Exception as e:
            conn.rollback()
            # Skip this SKU if locked
    print(f"  Linked {linked} order lines ({len(unlinked_skus)} SKUs processed)")

    # ── Step 5: Stamp COGS (batched per product to avoid locks) ──
    print("\n[Step 5] Stamp COGS (purchase_price_pln) batched...")
    cur.execute("""
        SELECT DISTINCT CAST(p.id AS VARCHAR(36)), p.netto_purchase_price_pln
        FROM acc_product p
        INNER JOIN acc_order_line ol ON ol.product_id = p.id
        WHERE ol.purchase_price_pln IS NULL
          AND p.netto_purchase_price_pln IS NOT NULL
    """)
    to_stamp = cur.fetchall()
    stamped = 0
    for pid, price in to_stamp:
        try:
            cur.execute(
                "UPDATE acc_order_line "
                "SET purchase_price_pln = ?, "
                "    cogs_pln = ? * ISNULL(quantity_ordered, 1), "
                "    price_source = 'auto' "
                "WHERE product_id = CAST(? AS UNIQUEIDENTIFIER) "
                "  AND purchase_price_pln IS NULL",
                float(price), float(price), pid,
            )
            stamped += cur.rowcount or 0
            conn.commit()
        except Exception:
            conn.rollback()
    print(f"  Stamped COGS on {stamped} order lines ({len(to_stamp)} products)")

    # ── Summary ──
    print("\n[Summary]")
    cur.execute("SELECT COUNT(*) FROM acc_product")
    print(f"  Total products:           {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM acc_order_line WHERE product_id IS NOT NULL")
    print(f"  Order lines with product: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM acc_order_line WHERE product_id IS NULL")
    print(f"  Order lines unlinked:     {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM acc_order_line WHERE purchase_price_pln IS NOT NULL")
    print(f"  Order lines with COGS:    {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM acc_order_line WHERE purchase_price_pln IS NULL")
    print(f"  Order lines no COGS:      {cur.fetchone()[0]}")

    conn.close()
    print("\n" + "=" * 60)
    print("  POST-PROCESSING COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
