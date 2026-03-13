"""
Fill missing logistics costs for Feb-Mar 2026 orders.

Strategy:
  1. Build cost model from Jan 2026 actual DHL/GLS billing data
     grouped by (ship_country, line_count_bucket)
  2. Query Feb-Mar 2026 orders that have NO logistics fact row
  3. For each order: lookup median cost from (country, bucket) model
     - Fallback: country-only median
     - Fallback: global median (~28 PLN)
  4. Upsert into acc_order_logistics_fact with calc_version='hist_country_v1'
  5. Commit in batches of 250
"""

import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))

from app.core.db_connection import connect_acc

CALC_VERSION = "hist_country_v1"
SOURCE_SYSTEM = "hist_ctry_v1"
BATCH_SIZE = 250
GLOBAL_FALLBACK_PLN = 28.00  # conservative global fallback

def build_cost_model(cur):
    """Build median cost model from Dec 2025 - Jan 2026 actual billing data."""
    sql = """
    WITH order_costs AS (
        SELECT
            o.amazon_order_id,
            o.ship_country,
            (SELECT COUNT(*) FROM dbo.acc_order_line ol WITH (NOLOCK)
             WHERE ol.amazon_order_id = o.amazon_order_id) AS line_count,
            f.total_logistics_pln
        FROM dbo.acc_order o WITH (NOLOCK)
        JOIN dbo.acc_order_logistics_fact f WITH (NOLOCK)
            ON f.amazon_order_id = o.amazon_order_id
        WHERE o.purchase_date >= '2025-12-01'
          AND o.purchase_date < '2026-02-01'
          AND f.calc_version IN ('dhl_v1', 'gls_v1')
          AND f.total_logistics_pln > 0
          AND o.ship_country IS NOT NULL
    ),
    bucketed AS (
        SELECT
            ship_country,
            CASE WHEN line_count >= 4 THEN 4 ELSE line_count END AS bucket,
            total_logistics_pln AS cost
        FROM order_costs
    )
    SELECT
        ship_country,
        bucket,
        COUNT(*) AS n,
        AVG(cost) AS avg_cost,
        -- median via PERCENTILE_CONT
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cost)
            OVER (PARTITION BY ship_country, bucket) AS median_cost
    FROM bucketed
    GROUP BY ship_country, bucket, cost
    """
    # The above with PERCENTILE_CONT is a window function; let me use a simpler approach
    # Actually let me use a cleaner query
    sql = """
    WITH order_costs AS (
        SELECT
            o.amazon_order_id,
            o.ship_country,
            (SELECT COUNT(*) FROM dbo.acc_order_line ol WITH (NOLOCK)
             WHERE ol.amazon_order_id = o.amazon_order_id) AS line_count,
            f.total_logistics_pln AS cost
        FROM dbo.acc_order o WITH (NOLOCK)
        JOIN dbo.acc_order_logistics_fact f WITH (NOLOCK)
            ON f.amazon_order_id = o.amazon_order_id
        WHERE o.purchase_date >= '2025-12-01'
          AND o.purchase_date < '2026-02-01'
          AND f.calc_version IN ('dhl_v1', 'gls_v1')
          AND f.total_logistics_pln > 0
          AND o.ship_country IS NOT NULL
    ),
    bucketed AS (
        SELECT
            ship_country,
            CASE WHEN line_count >= 4 THEN 4 ELSE line_count END AS bucket,
            cost
        FROM order_costs
    )
    SELECT
        b.ship_country,
        b.bucket,
        COUNT(*) AS n,
        AVG(b.cost) AS avg_cost,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b.cost) OVER (PARTITION BY b.ship_country, b.bucket) AS med_cost
    FROM bucketed b
    GROUP BY b.ship_country, b.bucket, b.cost
    """
    # PERCENTILE_CONT with OVER is tricky with GROUP BY, let me just fetch all rows and compute in Python

    sql = """
    SELECT
        o.ship_country,
        CASE WHEN lc.cnt >= 4 THEN 4 ELSE lc.cnt END AS bucket,
        f.total_logistics_pln AS cost
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_logistics_fact f WITH (NOLOCK)
        ON f.amazon_order_id = o.amazon_order_id
    CROSS APPLY (
        SELECT COUNT(*) AS cnt
        FROM dbo.acc_order_line ol WITH (NOLOCK)
        WHERE ol.order_id = o.id
    ) lc
    WHERE o.purchase_date >= '2025-12-01'
      AND o.purchase_date < '2026-02-01'
      AND f.calc_version IN ('dhl_v1', 'gls_v1')
      AND f.total_logistics_pln > 0
      AND o.ship_country IS NOT NULL
    """
    print("Building cost model from Dec 2025 - Jan 2026 actuals...")
    cur.execute(sql)
    rows = cur.fetchall()
    print(f"  Fetched {len(rows):,} cost records")

    # Group by (country, bucket)
    from collections import defaultdict
    import statistics
    groups = defaultdict(list)
    country_all = defaultdict(list)
    for country, bucket, cost in rows:
        cost = float(cost)
        groups[(country, bucket)].append(cost)
        country_all[country].append(cost)

    # Compute medians
    model = {}
    for key, costs in groups.items():
        model[key] = statistics.median(costs)

    country_fallback = {}
    for country, costs in country_all.items():
        country_fallback[country] = statistics.median(costs)

    print(f"  Model: {len(model)} (country, bucket) entries, {len(country_fallback)} country fallbacks")
    return model, country_fallback


def get_orders_to_fill(cur):
    """Get Feb-Mar 2026 orders missing logistics facts."""
    sql = """
    SELECT
        o.amazon_order_id,
        o.id AS acc_order_id,
        o.ship_country,
        CASE WHEN lc.cnt >= 4 THEN 4 ELSE lc.cnt END AS bucket
    FROM dbo.acc_order o WITH (NOLOCK)
    CROSS APPLY (
        SELECT COUNT(*) AS cnt
        FROM dbo.acc_order_line ol WITH (NOLOCK)
        WHERE ol.order_id = o.id
    ) lc
    WHERE o.purchase_date >= '2026-02-01'
      AND o.purchase_date < '2026-04-01'
      AND NOT EXISTS (
          SELECT 1 FROM dbo.acc_order_logistics_fact f WITH (NOLOCK)
          WHERE f.amazon_order_id = o.amazon_order_id
      )
      AND o.status NOT IN ('Cancelled', 'Canceled')
    """
    print("Querying Feb-Mar 2026 orders missing logistics facts...")
    cur.execute(sql)
    rows = cur.fetchall()
    print(f"  Found {len(rows):,} orders to fill")
    return rows


def upsert_fact(cur, amazon_order_id, acc_order_id, cost_pln, source_detail):
    """Insert or update logistics fact for an order."""
    # Check if row exists
    cur.execute(
        "SELECT 1 FROM dbo.acc_order_logistics_fact WITH (NOLOCK) "
        "WHERE amazon_order_id = ? AND calc_version = ?",
        (amazon_order_id, CALC_VERSION)
    )
    exists = cur.fetchone()

    if exists:
        cur.execute(
            "UPDATE dbo.acc_order_logistics_fact "
            "SET total_logistics_pln = ?, source_system = ?, calculated_at = SYSUTCDATETIME() "
            "WHERE amazon_order_id = ? AND calc_version = ?",
            (round(cost_pln, 4), source_detail, amazon_order_id, CALC_VERSION)
        )
    else:
        acc_id_str = str(acc_order_id) if acc_order_id else None
        cur.execute(
            "INSERT INTO dbo.acc_order_logistics_fact "
            "(amazon_order_id, acc_order_id, shipments_count, delivered_shipments_count, "
            "actual_shipments_count, estimated_shipments_count, total_logistics_pln, "
            "last_delivery_at, calc_version, source_system, calculated_at) "
            "VALUES (?, "
            "CASE WHEN ? IS NULL OR ? = '' THEN NULL ELSE CAST(? AS UNIQUEIDENTIFIER) END, "
            "?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME())",
            (
                amazon_order_id,
                acc_id_str, acc_id_str, acc_id_str,  # CASE WHEN for acc_order_id
                1,   # shipments_count
                0,   # delivered_shipments_count
                0,   # actual_shipments_count
                1,   # estimated_shipments_count
                round(cost_pln, 4),  # total_logistics_pln
                None,  # last_delivery_at
                CALC_VERSION,
                source_detail,
            )
        )


def main():
    t0 = time.time()
    conn = connect_acc(autocommit=False, timeout=60)
    try:
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 30000")

        # Step 1: Build cost model
        model, country_fallback = build_cost_model(cur)

        # Step 2: Get orders to fill
        orders = get_orders_to_fill(cur)
        if not orders:
            print("No orders to fill. Done.")
            return

        # Step 3: Fill logistics costs
        print(f"\nFilling logistics costs for {len(orders):,} orders...")
        stats = {"bucket_match": 0, "country_match": 0, "global_fallback": 0, "errors": 0}
        filled = 0

        for idx, (amazon_order_id, acc_order_id, ship_country, bucket) in enumerate(orders, 1):
            try:
                key = (ship_country, bucket)
                if key in model:
                    cost = model[key]
                    source = f"{SOURCE_SYSTEM}:bkt({ship_country},{bucket})"
                    stats["bucket_match"] += 1
                elif ship_country in country_fallback:
                    cost = country_fallback[ship_country]
                    source = f"{SOURCE_SYSTEM}:ctry({ship_country})"
                    stats["country_match"] += 1
                else:
                    cost = GLOBAL_FALLBACK_PLN
                    source = f"{SOURCE_SYSTEM}:global({ship_country})"
                    stats["global_fallback"] += 1

                upsert_fact(cur, amazon_order_id, acc_order_id, cost, source)
                filled += 1

                if idx % BATCH_SIZE == 0:
                    conn.commit()
                    elapsed = time.time() - t0
                    rate = idx / elapsed
                    print(f"  [{idx:,}/{len(orders):,}] committed ({rate:.0f} orders/sec)")

            except Exception as e:
                stats["errors"] += 1
                if stats["errors"] <= 5:
                    print(f"  ERROR on {amazon_order_id}: {e}")

        conn.commit()
        elapsed = time.time() - t0

        print(f"\n{'='*60}")
        print(f"DONE in {elapsed:.1f}s")
        print(f"  Filled:         {filled:,}")
        print(f"  Bucket match:   {stats['bucket_match']:,}")
        print(f"  Country match:  {stats['country_match']:,}")
        print(f"  Global fallback:{stats['global_fallback']:,}")
        print(f"  Errors:         {stats['errors']:,}")
        print(f"{'='*60}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
