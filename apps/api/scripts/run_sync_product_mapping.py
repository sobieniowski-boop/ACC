"""
Safe runner for sync_product_mapping (skip SP-API, backfill-safe).
Maps Amazon products to internal SKU via Ergonode PIM + Google Sheet + Baselinker.
"""
import sys, os, asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".env"), override=True)


async def main():
    print("=" * 60)
    print("  sync_product_mapping — SAFE MODE (skip SP-API)")
    print("=" * 60)

    # ── Pre-flight ──
    print("\n[1/3] Pre-flight checks...")
    from app.core.db_connection import connect_acc, connect_netfox

    c = connect_acc()
    cur = c.cursor()
    cur.execute("SELECT COUNT(*) FROM acc_product WHERE internal_sku IS NULL")
    unmapped = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM acc_product WHERE internal_sku IS NOT NULL")
    mapped = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM acc_product")
    total = cur.fetchone()[0]
    c.close()
    print(f"  Products: {total} total, {mapped} mapped, {unmapped} unmapped")

    if unmapped == 0:
        print("  All products already mapped — nothing to do!")
        return

    # Netfox check
    try:
        nfx = connect_netfox()
        nfx_cur = nfx.cursor()
        nfx_cur.execute("SELECT COUNT(*) FROM ITJK_BL_OriginalOrders WITH (NOLOCK)")
        bl_cnt = nfx_cur.fetchone()[0]
        nfx.close()
        print(f"  Netfox OK — BL_OriginalOrders: {bl_cnt:,} rows")
    except Exception as e:
        print(f"  Netfox WARNING: {e}")
        print("  (will skip Baselinker fallback)")

    # Ergonode check
    from app.core.config import settings
    ergo_ok = bool(settings.ERGONODE_API_KEY)
    print(f"  Ergonode configured: {'Yes' if ergo_ok else 'No (will skip)'}")

    gsheet_ok = bool(settings.GSHEET_EAN_CSV_URL or settings.GSHEET_ALLEGRO_CSV_URL)
    print(f"  GSheet configured:  {'Yes' if gsheet_ok else 'No (will skip)'}")

    # ── Run ──
    print(f"\n[2/3] Running sync_product_mapping(skip_spapi=True)...")
    print(f"  Sources: Ergonode → GSheet → Baselinker → ASIN fallback")
    print(f"  SP-API step will be SKIPPED (backfill running)\n")

    from app.services.sync_service import sync_product_mapping
    try:
        result = await sync_product_mapping(
            only_unmapped=True,
            skip_spapi=True,
        )
        print(f"\n  ✓ DONE — {result} products mapped")
    except Exception as e:
        print(f"\n  ✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return

    # ── Verify ──
    print(f"\n[3/3] Verify results...")
    c = connect_acc()
    cur = c.cursor()
    cur.execute("SELECT COUNT(*) FROM acc_product WHERE internal_sku IS NOT NULL")
    new_mapped = cur.fetchone()[0]
    cur.execute("""
        SELECT mapping_source, COUNT(*) as cnt
        FROM acc_product
        WHERE mapping_source IS NOT NULL
        GROUP BY mapping_source
        ORDER BY cnt DESC
    """)
    print(f"  Products now mapped: {new_mapped} (was {mapped})")
    print(f"  Mapping sources:")
    for row in cur.fetchall():
        print(f"    {row[0]:25s} {row[1]:>5}")
    c.close()

    print("\n" + "=" * 60)
    print("  COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
