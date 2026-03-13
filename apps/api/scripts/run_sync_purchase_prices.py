"""
Safe runner for sync_purchase_prices.
Runs outside the API server to avoid any scheduler conflicts.
Prints detailed progress.
"""
import sys, os, asyncio

# Project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

# Load .env BEFORE any app imports
from dotenv import load_dotenv
env_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", ".env")
load_dotenv(env_path, override=True)

async def main():
    print("=" * 60)
    print("  sync_purchase_prices — SAFE RUNNER")
    print("=" * 60)

    # ── Pre-flight: verify connections ──
    print("\n[1/4] Verify Azure SQL connection...")
    from app.core.db_connection import connect_acc
    try:
        c = connect_acc()
        cur = c.cursor()
        cur.execute("SELECT COUNT(*) FROM acc_product WHERE internal_sku IS NOT NULL")
        mapped = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM acc_product")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM acc_order_line")
        lines = cur.fetchone()[0]
        c.close()
        print(f"  ✓ Azure SQL OK — {mapped} mapped / {total} total products, {lines} order lines")
    except Exception as e:
        print(f"  ✗ Azure SQL FAILED: {e}")
        return

    print("\n[2/4] Verify Netfox ERP connection...")
    from app.core.db_connection import connect_netfox
    try:
        c = connect_netfox()
        cur = c.cursor()
        cur.execute("SELECT TOP 1 [Numer artykułu] FROM ITJK_BazaDanychSprzedazHolding WITH (NOLOCK)")
        row = cur.fetchone()
        c.close()
        print(f"  ✓ Netfox OK — sample article: {row[0] if row else 'N/A'}")
    except Exception as e:
        print(f"  ✗ Netfox FAILED: {e}")
        print("  VPN aktywny? ping 192.168.230.120")
        return

    # ── Run the sync ──
    print("\n[3/4] Running sync_purchase_prices()...")
    print("  Main flow: 1 query to Netfox (Holding FIFO), writes to Azure SQL")
    print("  Fallback:  per-product lookups (EAN sibling, Holding, ASIN, BL Kod)")
    print("  This should take 10-60 seconds...\n")

    from app.services.sync_service import sync_purchase_prices
    try:
        result = await sync_purchase_prices(job_id=None)
        print(f"\n  ✓ DONE — {result} products updated with purchase prices")
    except Exception as e:
        print(f"\n  ✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return

    # ── Verify results ──
    print("\n[4/4] Verify results...")
    try:
        c = connect_acc()
        cur = c.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM acc_product
            WHERE netto_purchase_price_pln IS NOT NULL
              AND netto_purchase_price_pln > 0
        """)
        priced = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM acc_purchase_price")
        history = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM acc_order_line
            WHERE purchase_price_pln IS NOT NULL
        """)
        stamped = cur.fetchone()[0]

        cur.execute("""
            SELECT TOP 5 p.internal_sku, p.netto_purchase_price_pln, p.title
            FROM acc_product p
            WHERE p.netto_purchase_price_pln IS NOT NULL
            ORDER BY p.updated_at DESC
        """)
        samples = cur.fetchall()
        c.close()

        print(f"  Products with price:  {priced}")
        print(f"  Price history rows:   {history}")
        print(f"  Order lines stamped:  {stamped}")
        print(f"\n  Sample prices (latest 5):")
        for row in samples:
            sku, price, title = row[0], row[1], (row[2] or "")[:50]
            print(f"    {sku:20s}  {price:8.2f} PLN  {title}")
    except Exception as e:
        print(f"  Verification error: {e}")

    print("\n" + "=" * 60)
    print("  COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
