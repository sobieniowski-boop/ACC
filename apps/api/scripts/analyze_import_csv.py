"""
Analyze IMPORT analiza ver.csv and cross-reference with ACC database.
Find products that can get purchase prices from this CSV.
"""
import csv
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env", override=True)
from app.core.db_connection import connect_acc

CSV_PATH = r"C:\Users\msobieniowski\Downloads\IMPORT analiza ver.csv"


def parse_price(val):
    if not val or not val.strip():
        return None
    v = (
        val.strip()
        .replace("zł", "")
        .replace("PLN", "")
        .replace("\u202f", "")  # narrow no-break space
        .replace("\xa0", "")    # no-break space
        .replace(" ", "")
        .replace(",", ".")
        .strip()
    )
    try:
        f = float(v)
        return f if f > 0 else None
    except ValueError:
        return None


def load_csv():
    """Load CSV and return dict of sku -> price (PLN)."""
    prices = {}
    all_skus = {}
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=";")
        rows = list(reader)

    # Header is row index 2, data starts at index 3
    for row in rows[3:]:
        if len(row) < 28:
            continue
        sku = row[1].strip()
        if not sku:
            continue
        name = row[2].strip()[:80] if row[2] else ""
        ean = row[12].strip() if len(row) > 12 and row[12] else ""
        price = parse_price(row[27])
        all_skus[sku] = {"name": name, "ean": ean, "price": price}
        if price:
            prices[sku] = price

    return all_skus, prices


def main():
    print("=" * 60)
    print("IMPORT analiza ver.csv  ⟷  ACC Database cross-reference")
    print("=" * 60)

    all_skus, csv_prices = load_csv()
    print(f"\nCSV: {len(all_skus)} SKUs total, {len(csv_prices)} with price")

    conn = connect_acc()
    cur = conn.cursor()

    # 1) All tables with 'price' in name
    cur.execute(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME"
    )
    all_tables = [r[0] for r in cur.fetchall()]
    price_tables = [t for t in all_tables if "price" in t.lower()]
    print(f"\nPrice-related tables: {price_tables}")

    # 2) All products
    cur.execute("SELECT id, internal_sku FROM acc_product WITH (NOLOCK)")
    db_products = {}
    for r in cur.fetchall():
        db_products[str(r[1])] = r[0]
    print(f"DB products: {len(db_products)}")

    # 3) Products that already have purchase price (acc_purchase_price has internal_sku, netto_price_pln)
    cur.execute(
        "SELECT DISTINCT internal_sku FROM acc_purchase_price WITH (NOLOCK)"
    )
    db_has_price = {str(r[0]) for r in cur.fetchall()}
    print(f"DB SKUs with price in acc_purchase_price: {len(db_has_price)}")

    # Also check acc_product.netto_purchase_price_pln
    cur.execute(
        "SELECT internal_sku FROM acc_product WITH (NOLOCK) "
        "WHERE netto_purchase_price_pln IS NOT NULL AND netto_purchase_price_pln > 0"
    )
    db_product_has_price = {str(r[0]) for r in cur.fetchall()}
    print(f"DB SKUs with netto_purchase_price_pln on product: {len(db_product_has_price)}")
    db_has_price = db_has_price | db_product_has_price

    db_no_price = set(db_products.keys()) - db_has_price

    # 4) Cross-reference
    csv_in_db = set(csv_prices.keys()) & set(db_products.keys())
    csv_can_fill = set(csv_prices.keys()) & db_no_price
    csv_already_have = set(csv_prices.keys()) & db_has_price
    csv_not_in_db = set(csv_prices.keys()) - set(db_products.keys())

    print(f"\n{'─' * 40}")
    print(f"CROSS-REFERENCE RESULTS:")
    print(f"{'─' * 40}")
    print(f"  CSV SKUs found in DB:        {len(csv_in_db)}")
    print(f"  CSV can FILL missing prices: {len(csv_can_fill)}")
    print(f"  CSV SKUs already priced:     {len(csv_already_have)}")
    print(f"  CSV SKUs NOT in DB:          {len(csv_not_in_db)}")
    print(f"  DB products without price:   {len(db_no_price)}")
    print(f"  Still without price after:   {len(db_no_price - set(csv_prices.keys()))}")

    print(f"\n{'─' * 40}")
    print(f"POTENTIAL GAIN: +{len(csv_can_fill)} products get purchase price")
    print(f"{'─' * 40}")

    if csv_can_fill:
        print(f"\nFill candidates (showing {min(30, len(csv_can_fill))}):")
        for sku in sorted(csv_can_fill)[:30]:
            info = all_skus.get(sku, {})
            name = info.get("name", "")[:50]
            print(f"  {sku:>8}: {csv_prices[sku]:>8.2f} PLN  {name}")

    if csv_not_in_db:
        print(f"\nCSV SKUs NOT in DB (showing {min(20, len(csv_not_in_db))}):")
        for sku in sorted(csv_not_in_db)[:20]:
            info = all_skus.get(sku, {})
            name = info.get("name", "")[:50]
            print(f"  {sku:>8}: {csv_prices[sku]:>8.2f} PLN  {name}")

    # 5) Price comparison for those that already have prices
    if csv_already_have:
        print(f"\nPrice comparison (CSV vs DB) for matched SKUs:")
        cur.execute(
            "SELECT internal_sku, netto_price_pln FROM acc_purchase_price WITH (NOLOCK)"
        )
        db_prices_map = {str(r[0]): float(r[1]) for r in cur.fetchall()}

        diffs = []
        for sku in sorted(csv_already_have)[:30]:
            csv_p = csv_prices[sku]
            db_p = db_prices_map.get(sku, 0)
            diff_pct = ((csv_p - db_p) / db_p * 100) if db_p else 0
            if abs(diff_pct) > 5:
                diffs.append((sku, csv_p, db_p, diff_pct))

        if diffs:
            print(f"  Significant differences (>5%): {len(diffs)}")
            for sku, csv_p, db_p, pct in diffs[:15]:
                print(f"    {sku:>8}: CSV={csv_p:.2f} DB={db_p:.2f} ({pct:+.1f}%)")
        else:
            print(f"  No significant price differences found")

    conn.close()
    print(f"\nDone.")


if __name__ == "__main__":
    main()
