"""Run monthly Brand Analytics sync — start with DE, then all marketplaces."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))

import asyncio
import structlog, logging

structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.INFO))
log = structlog.get_logger()

async def main():
    from app.services.search_term_sync import sync_search_terms
    from app.core.config import MARKETPLACE_REGISTRY
    from app.core.db_connection import connect_acc

    brand_mkts = [(mid, info["code"]) for mid, info in MARKETPLACE_REGISTRY.items() if info.get("brand_owner")]
    print(f"=== Brand Analytics Monthly Sync ===")
    print(f"Brand Owner marketplaces: {len(brand_mkts)}")

    # Phase 1: Just DE, 1 month (validate MERGE works)
    de_mid = "A1PA6795UKMFR9"
    print(f"\n--- Phase 1: DE only, 1 month ---")
    result1 = await sync_search_terms(months_back=1, marketplace_ids=[de_mid])
    print(f"Result: {result1}")

    # Check DB count
    conn = connect_acc()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM acc_search_term_monthly")
    count = cur.fetchone()[0]
    print(f"acc_search_term_monthly rows: {count:,}")
    conn.close()

    if result1.get("total_monthly_rows", 0) > 0:
        # Phase 2: DE, 12 months backfill for seasonality
        print(f"\n--- Phase 2: DE, 12 months backfill ---")
        result2 = await sync_search_terms(months_back=12, marketplace_ids=[de_mid])
        print(f"Result: {result2}")

        conn = connect_acc()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM acc_search_term_monthly")
        count = cur.fetchone()[0]
        cur.execute("SELECT DISTINCT year, month FROM acc_search_term_monthly WHERE marketplace_id = ? ORDER BY year, month", (de_mid,))
        months = cur.fetchall()
        conn.close()
        print(f"Total rows: {count:,}")
        print(f"Months available: {[(y, m) for y, m in months]}")

        # Phase 3: All marketplaces, 3 months
        print(f"\n--- Phase 3: All {len(brand_mkts)} marketplaces, 3 months ---")
        result3 = await sync_search_terms(months_back=3)
        print(f"Result: {result3}")

        conn = connect_acc()
        cur = conn.cursor()
        cur.execute("SELECT marketplace_id, COUNT(*) as cnt FROM acc_search_term_monthly GROUP BY marketplace_id")
        per_mkt = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM acc_search_term_monthly")
        total = cur.fetchone()[0]
        conn.close()
        print(f"\n=== Final Summary ===")
        print(f"Total rows: {total:,}")
        for mkt, cnt in per_mkt:
            code = {mid: info["code"] for mid, info in MARKETPLACE_REGISTRY.items()}.get(mkt, mkt)
            print(f"  {code}: {cnt:,}")
    else:
        print("Phase 1 returned 0 rows — cannot proceed.")

if __name__ == "__main__":
    asyncio.run(main())
