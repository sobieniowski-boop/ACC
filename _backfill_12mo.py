"""Backfill DE to 12 months, then expand other marketplaces to 12 months."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))

import asyncio
import structlog, logging
structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.INFO))

async def main():
    from app.services.search_term_sync import sync_search_terms
    from app.core.db_connection import connect_acc

    # Phase 1: DE 12 months
    print("=== DE 12-month backfill ===")
    de_mid = "A1PA6795UKMFR9"
    result = await sync_search_terms(months_back=12, marketplace_ids=[de_mid])
    print(f"DE result: {result}")

    # Phase 2: All other working marketplaces 12 months
    working_mkts = [
        "A1RKKUPIHCS9HS",  # ES
        "A13V1IB3VIYZZH",  # FR
        "A1805IZSGTT6HS",  # NL
        "APJ6JRA9NG5V4",   # IT
        "A2NODRKZP88ZB9",  # SE
    ]
    print(f"\n=== Remaining {len(working_mkts)} marketplaces 12-month backfill ===")
    result_all = await sync_search_terms(months_back=12, marketplace_ids=working_mkts)
    print(f"All result: {result_all}")

    # Summary
    conn = connect_acc()
    cur = conn.cursor()
    cur.execute('''SELECT marketplace_id, MIN(CAST(year AS VARCHAR)+'-'+RIGHT('0'+CAST(month AS VARCHAR),2)) as earliest,
                          MAX(CAST(year AS VARCHAR)+'-'+RIGHT('0'+CAST(month AS VARCHAR),2)) as latest,
                          COUNT(DISTINCT CAST(year AS VARCHAR)+RIGHT('0'+CAST(month AS VARCHAR),2)) as months,
                          COUNT(*) as rows
    FROM acc_search_term_monthly GROUP BY marketplace_id ORDER BY marketplace_id''')
    print(f"\n=== Final Coverage ===")
    total = 0
    for r in cur.fetchall():
        print(f"  {r[0]}: {r[1]} to {r[2]} ({r[3]} months, {r[4]:,} rows)")
        total += r[4]
    print(f"  TOTAL: {total:,} rows")
    conn.close()

if __name__ == "__main__":
    asyncio.run(main())
