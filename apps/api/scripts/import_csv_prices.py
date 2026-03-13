"""
Import products and purchase prices from IMPORT analiza ver.csv into ACC database.

Operations:
  A) Insert 13 missing prices for existing products (acc_purchase_price)
  B) Create ~412 new products (acc_product) + their prices (acc_purchase_price)
  C) Update netto_purchase_price_pln on acc_product for all imported prices

Source: "import_csv" / "IMPORT analiza ver.csv"
"""
import csv
import sys
import os
import uuid
from datetime import date, datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env", override=True)
from app.core.db_connection import connect_acc

CSV_PATH = r"C:\Users\msobieniowski\Downloads\IMPORT analiza ver.csv"
SOURCE = "import_csv"
SOURCE_DOC = "IMPORT analiza ver.csv"
DRY_RUN = "--dry-run" in sys.argv
TODAY = date.today()


def parse_price(val):
    if not val or not val.strip():
        return None
    v = (
        val.strip()
        .replace("zł", "")
        .replace("PLN", "")
        .replace("\u202f", "")
        .replace("\xa0", "")
        .replace(" ", "")
        .replace(",", ".")
        .strip()
    )
    try:
        f = float(v)
        return round(f, 4) if f > 0 else None
    except ValueError:
        return None


def load_csv():
    """Return dict sku -> {name, ean, k_number, price}."""
    products = {}
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=";")
        rows = list(reader)

    for row in rows[3:]:
        if len(row) < 28:
            continue
        sku = row[1].strip()
        if not sku:
            continue
        price = parse_price(row[27])
        if not price:
            continue

        name = row[2].strip() if row[2] else ""
        k_number = row[3].strip() if len(row) > 3 and row[3] else None
        ean_raw = row[12].strip() if len(row) > 12 and row[12] else None
        # EAN sanity: must be numeric, skip "pakiet" etc.
        ean = ean_raw if ean_raw and ean_raw.isdigit() and len(ean_raw) >= 8 else None

        products[sku] = {
            "name": name,
            "ean": ean,
            "k_number": k_number,
            "price": price,
        }
    return products


def main():
    mode = "DRY-RUN" if DRY_RUN else "LIVE"
    print(f"={'=' * 50}")
    print(f"  CSV -> ACC Import  [{mode}]")
    print(f"={'=' * 50}")

    csv_products = load_csv()
    print(f"\nCSV: {len(csv_products)} SKUs with price")

    conn = connect_acc(autocommit=False)
    cur = conn.cursor()
    cur.execute("SET LOCK_TIMEOUT 30000")

    # ─── Existing DB state ───
    cur.execute("SELECT id, internal_sku FROM acc_product WITH (NOLOCK)")
    db_products = {}
    for r in cur.fetchall():
        db_products[str(r[1])] = str(r[0])
    print(f"DB products: {len(db_products)}")

    cur.execute("SELECT DISTINCT internal_sku FROM acc_purchase_price WITH (NOLOCK)")
    db_has_price = {str(r[0]) for r in cur.fetchall()}
    print(f"DB SKUs with price: {len(db_has_price)}")

    # ─── Classify ───
    to_add_price_only = []   # existing product, no price yet
    to_create_product = []   # product not in DB at all

    for sku, info in csv_products.items():
        if sku in db_products and sku not in db_has_price:
            to_add_price_only.append((sku, info))
        elif sku not in db_products:
            to_create_product.append((sku, info))

    print(f"\n─── PLAN ───")
    print(f"  A) Add price for existing products: {len(to_add_price_only)}")
    print(f"  B) Create new products + prices:    {len(to_create_product)}")
    print(f"  Total price inserts:                {len(to_add_price_only) + len(to_create_product)}")

    if DRY_RUN:
        print(f"\n[DRY-RUN] No changes made.")
        conn.close()
        return

    # ═══════════════════════════════════════════════════
    # A) Insert prices for existing products (13)
    # ═══════════════════════════════════════════════════
    print(f"\n─── A) Inserting {len(to_add_price_only)} prices for existing products ───")
    a_ok = 0
    for sku, info in to_add_price_only:
        try:
            cur.execute(
                "INSERT INTO acc_purchase_price "
                "(internal_sku, netto_price_pln, valid_from, source, source_document, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                sku, info["price"], TODAY, SOURCE, SOURCE_DOC, datetime.utcnow(), datetime.utcnow(),
            )
            # Also update product cache
            product_id = db_products[sku]
            cur.execute(
                "UPDATE acc_product SET netto_purchase_price_pln = ?, updated_at = ? WHERE id = ?",
                info["price"], datetime.utcnow(), product_id,
            )
            a_ok += 1
        except Exception as e:
            print(f"  ERROR {sku}: {e}")
            conn.rollback()
            continue
    conn.commit()
    print(f"  ✓ {a_ok}/{len(to_add_price_only)} prices inserted")

    # ═══════════════════════════════════════════════════
    # B) Create new products + insert prices
    # ═══════════════════════════════════════════════════
    print(f"\n─── B) Creating {len(to_create_product)} new products ───")
    b_prod = 0
    b_price = 0
    batch_size = 50
    batch = []

    for sku, info in to_create_product:
        product_id = str(uuid.uuid4())
        # Extract brand from name (usually starts with "KADAX")
        brand = "KADAX" if info["name"].upper().startswith("KADAX") else None
        title = info["name"][:500] if info["name"] else sku

        batch.append((product_id, sku, info))

        if len(batch) >= batch_size:
            b_p, b_pr = _flush_batch(cur, conn, batch, brand_default="KADAX")
            b_prod += b_p
            b_price += b_pr
            batch = []

    if batch:
        b_p, b_pr = _flush_batch(cur, conn, batch, brand_default="KADAX")
        b_prod += b_p
        b_price += b_pr

    print(f"  ✓ {b_prod}/{len(to_create_product)} products created")
    print(f"  ✓ {b_price}/{len(to_create_product)} prices inserted")

    # ═══════════════════════════════════════════════════
    # Summary
    # ═══════════════════════════════════════════════════
    cur.execute("SELECT COUNT(*) FROM acc_product WITH (NOLOCK)")
    total_prod = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM acc_purchase_price WITH (NOLOCK)")
    total_prices = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM acc_product WITH (NOLOCK) "
        "WHERE netto_purchase_price_pln IS NOT NULL AND netto_purchase_price_pln > 0"
    )
    prod_with_price = cur.fetchone()[0]

    print(f"\n{'=' * 50}")
    print(f"  RESULTS")
    print(f"{'=' * 50}")
    print(f"  Total products:          {total_prod}")
    print(f"  Products with price:     {prod_with_price}")
    print(f"  acc_purchase_price rows: {total_prices}")
    print(f"  Source: {SOURCE}")

    conn.close()
    print(f"\nDone.")


def _flush_batch(cur, conn, batch, brand_default="KADAX"):
    """Insert products + prices one by one with per-pair commit."""
    prod_ok = 0
    price_ok = 0
    for product_id, sku, info in batch:
        try:
            brand = "KADAX" if info["name"].upper().startswith("KADAX") else brand_default
            title = info["name"][:500] if info["name"] else sku
            # Use placeholder ASIN (_CSV_{sku}) to avoid UNIQUE NULL constraint
            placeholder_asin = f"_CSV_{sku}"
            cur.execute(
                "INSERT INTO acc_product "
                "(id, internal_sku, sku, asin, ean, title, brand, k_number, "
                " netto_purchase_price_pln, mapping_source, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                product_id, sku, sku, placeholder_asin, info["ean"], title, brand, info["k_number"],
                info["price"], SOURCE, datetime.utcnow(), datetime.utcnow(),
            )
            prod_ok += 1
        except Exception as e:
            print(f"  PROD ERROR {sku}: {e}")
            conn.rollback()
            continue

        try:
            cur.execute(
                "INSERT INTO acc_purchase_price "
                "(internal_sku, netto_price_pln, valid_from, source, source_document, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                sku, info["price"], TODAY, SOURCE, SOURCE_DOC, datetime.utcnow(), datetime.utcnow(),
            )
            price_ok += 1
        except Exception as e:
            print(f"  PRICE ERROR {sku}: {e}")
            conn.rollback()
            continue

        # Commit each pair individually to avoid rollback cascading
        conn.commit()
    return prod_ok, price_ok


if __name__ == "__main__":
    main()
