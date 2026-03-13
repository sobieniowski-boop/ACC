"""Run Brand Analytics search term sync — backfill monthly data.

Usage:  cd C:\\ACC && python _run_search_sync.py [months_back]
Default: 12 months for all brand_owner marketplaces.
"""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))

import asyncio
import logging
import structlog

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)

MONTHS_BACK = int(sys.argv[1]) if len(sys.argv) > 1 else 12

async def main():
    from app.core.config import MARKETPLACE_REGISTRY
    from app.services.search_term_sync import sync_search_terms

    brand_mkts = [
        (mid, info["code"])
        for mid, info in MARKETPLACE_REGISTRY.items()
        if info.get("brand_owner")
    ]
    print(f"\n=== Brand Analytics Backfill ({MONTHS_BACK} months) ===")
    print(f"Marketplaces: {', '.join(c for _, c in brand_mkts)}")

    t0 = time.time()
    result = await sync_search_terms(months_back=MONTHS_BACK)
    elapsed = time.time() - t0

    print(f"\n=== Done in {elapsed:.0f}s ===")
    for mkt, rows in result.get("per_marketplace", {}).items():
        status = f"{rows:>10,} rows" if rows >= 0 else "  FAILED"
        print(f"  {mkt:5}: {status}")
    print(f"  TOTAL: {result.get('total_monthly_rows', 0):>10,} rows")

    # Final DB count
    from app.core.db_connection import connect_acc
    conn = connect_acc()
    cur = conn.cursor()
    cur.execute("SELECT COUNT_BIG(*) FROM acc_search_term_monthly WITH (NOLOCK)")
    total = cur.fetchone()[0]
    conn.close()
    print(f"\n  DB total (acc_search_term_monthly): {total:,}")

if __name__ == "__main__":
    asyncio.run(main())
