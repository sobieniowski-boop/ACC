"""Check how many ASINs from our products appear in Brand Analytics search terms."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))

from app.core.db_connection import connect_acc

conn = connect_acc()
cur = conn.cursor()

# 1. How many unique ASINs in our products?
cur.execute("SELECT COUNT(DISTINCT asin) FROM acc_product WHERE asin IS NOT NULL AND asin != ''")
our_asins = cur.fetchone()[0]
print(f"Our product ASINs: {our_asins}")

# 2. How many unique ASINs in Brand Analytics?
cur.execute("SELECT COUNT(DISTINCT asin) FROM acc_search_term_monthly")
ba_asins = cur.fetchone()[0]
print(f"Brand Analytics unique ASINs: {ba_asins:,}")

# 3. How many match?
cur.execute("""
    SELECT COUNT(DISTINCT p.asin)
    FROM acc_product p
    JOIN acc_search_term_monthly stm ON stm.asin = p.asin
    WHERE p.asin IS NOT NULL AND p.asin != ''
""")
matched = cur.fetchone()[0]
print(f"Matched ASINs (our products in BA data): {matched}")

# 4. How many SKUs are in seasonality_index_cache?
cur.execute("SELECT COUNT(DISTINCT entity_id) FROM seasonality_index_cache WHERE entity_type = 'sku'")
si_skus = cur.fetchone()[0]
print(f"SKUs in seasonality_index_cache: {si_skus:,}")

# 5. How many SKUs have a product with ASIN?
cur.execute("""
    SELECT COUNT(DISTINCT ic.entity_id)
    FROM seasonality_index_cache ic
    JOIN acc_product p ON p.sku = ic.entity_id
    WHERE ic.entity_type = 'sku' AND p.asin IS NOT NULL AND p.asin != ''
""")
si_with_asin = cur.fetchone()[0]
print(f"SKUs in index_cache with ASIN: {si_with_asin}")

# 6. Month coverage in search terms
cur.execute("""
    SELECT marketplace_id, year, month, COUNT(DISTINCT asin) as asins, COUNT(*) as rows
    FROM acc_search_term_monthly
    GROUP BY marketplace_id, year, month
    ORDER BY marketplace_id, year, month
""")
print(f"\nBrand Analytics monthly coverage:")
for r in cur.fetchall():
    print(f"  {r[0]} | {r[1]}-{r[2]:02d} | ASINs: {r[3]:,} | Rows: {r[4]:,}")

conn.close()
