"""
Fill missing logistics costs for Feb-Mar 2026 — FAST version.
Uses server-side INSERT...SELECT instead of per-row client-side inserts.
"""

import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))

from app.core.db_connection import connect_acc
from collections import defaultdict
import statistics

CALC_VERSION = "hist_country_v1"


def main():
    t0 = time.time()
    conn = connect_acc(autocommit=False, timeout=120)
    try:
        cur = conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 30000")

        # Step 0: Clean any partial previous run
        print("Cleaning previous hist_country_v1 rows...")
        cur.execute(
            "DELETE FROM dbo.acc_order_logistics_fact WHERE calc_version = ?",
            (CALC_VERSION,)
        )
        conn.commit()
        print("  Cleaned.")

        # Step 1: Build cost model from actuals
        print("Building cost model from Dec 2025 - Jan 2026 actuals...")
        sql_model = """
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
        cur.execute(sql_model)
        rows = cur.fetchall()
        print(f"  Fetched {len(rows):,} cost records")

        # Compute medians by (country, bucket) and by country
        groups = defaultdict(list)
        country_all = defaultdict(list)
        for country, bucket, cost in rows:
            c = float(cost)
            groups[(country, int(bucket))].append(c)
            country_all[country].append(c)

        model = {}
        for key, costs in groups.items():
            model[key] = round(statistics.median(costs), 4)
        country_fb = {}
        for country, costs in country_all.items():
            country_fb[country] = round(statistics.median(costs), 4)
        global_fb = 28.0
        print(f"  Model: {len(model)} bucket entries, {len(country_fb)} country fallbacks")

        # Step 2: Create temp table with cost model
        print("Creating temp cost model table...")
        cur.execute("""
            CREATE TABLE #cost_model (
                ship_country NVARCHAR(5),
                bucket INT,
                median_cost DECIMAL(18,4),
                source_tag NVARCHAR(32)
            )
        """)
        for (country, bucket), med in model.items():
            tag = f"hist_ctry_v1:bkt({country},{bucket})"
            if len(tag) > 32:
                tag = tag[:32]
            cur.execute(
                "INSERT INTO #cost_model VALUES (?, ?, ?, ?)",
                (country, bucket, med, tag)
            )

        # Country-only fallback table
        cur.execute("""
            CREATE TABLE #country_fb (
                ship_country NVARCHAR(5),
                median_cost DECIMAL(18,4),
                source_tag NVARCHAR(32)
            )
        """)
        for country, med in country_fb.items():
            tag = f"hist_ctry_v1:ctry({country})"
            if len(tag) > 32:
                tag = tag[:32]
            cur.execute(
                "INSERT INTO #country_fb VALUES (?, ?, ?)",
                (country, med, tag)
            )
        conn.commit()
        print(f"  Temp tables populated.")

        # Step 3: Count orders to fill
        cur.execute("""
            SELECT COUNT(*)
            FROM dbo.acc_order o WITH (NOLOCK)
            WHERE o.purchase_date >= '2026-02-01'
              AND o.purchase_date < '2026-04-01'
              AND NOT EXISTS (
                  SELECT 1 FROM dbo.acc_order_logistics_fact f WITH (NOLOCK)
                  WHERE f.amazon_order_id = o.amazon_order_id
              )
              AND o.status NOT IN ('Cancelled', 'Canceled')
        """)
        total = cur.fetchone()[0]
        print(f"  Orders to fill: {total:,}")

        # Step 4: Server-side approach via staging table
        print("Building staging table...")
        t1 = time.time()
        cur.execute(f"""
            SELECT
                src.amazon_order_id,
                src.acc_order_id,
                CAST(1 AS INT) AS shipments_count,
                CAST(0 AS INT) AS delivered_shipments_count,
                CAST(0 AS INT) AS actual_shipments_count,
                CAST(1 AS INT) AS estimated_shipments_count,
                src.cost AS total_logistics_pln,
                CAST(NULL AS DATETIME2) AS last_delivery_at,
                CAST('{CALC_VERSION}' AS NVARCHAR(32)) AS calc_version,
                src.source_tag AS source_system
            INTO #staging
            FROM (
                SELECT
                    o.amazon_order_id,
                    o.id AS acc_order_id,
                    COALESCE(cm.median_cost, cf.median_cost, {global_fb}) AS cost,
                    COALESCE(cm.source_tag, cf.source_tag, 'hist_ctry_v1:global') AS source_tag,
                    ROW_NUMBER() OVER (PARTITION BY o.amazon_order_id ORDER BY o.purchase_date DESC) AS rn
                FROM dbo.acc_order o WITH (NOLOCK)
                CROSS APPLY (
                    SELECT COUNT(*) AS cnt
                    FROM dbo.acc_order_line ol WITH (NOLOCK)
                    WHERE ol.order_id = o.id
                ) lc
                LEFT JOIN #cost_model cm
                    ON cm.ship_country = o.ship_country
                    AND cm.bucket = CASE WHEN lc.cnt >= 4 THEN 4 ELSE lc.cnt END
                LEFT JOIN #country_fb cf
                    ON cf.ship_country = o.ship_country
                WHERE o.purchase_date >= '2026-02-01'
                  AND o.purchase_date < '2026-04-01'
                  AND NOT EXISTS (
                      SELECT 1 FROM dbo.acc_order_logistics_fact f WITH (NOLOCK)
                      WHERE f.amazon_order_id = o.amazon_order_id
                  )
                  AND o.status NOT IN ('Cancelled', 'Canceled')
            ) src
            WHERE src.rn = 1
        """)
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM #staging")
        staged = cur.fetchone()[0]
        print(f"  Staged {staged:,} rows")

        # Check for any unexpected duplicates in staging
        cur.execute("""
            SELECT amazon_order_id, COUNT(*) AS cnt
            FROM #staging
            GROUP BY amazon_order_id
            HAVING COUNT(*) > 1
        """)
        dup_check = cur.fetchall()
        if dup_check:
            print(f"  WARNING: {len(dup_check)} duplicates in staging! Deduplicating...")
            cur.execute("""
                ;WITH cte AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY amazon_order_id ORDER BY total_logistics_pln) AS rn2
                    FROM #staging
                )
                DELETE FROM cte WHERE rn2 > 1
            """)
            conn.commit()
            cur.execute("SELECT COUNT(*) FROM #staging")
            staged = cur.fetchone()[0]
            print(f"  After dedup: {staged:,} rows")

        # Now do the INSERT from staging
        print("Inserting from staging into fact table...")
        cur.execute("""
            INSERT INTO dbo.acc_order_logistics_fact (
                amazon_order_id, acc_order_id,
                shipments_count, delivered_shipments_count,
                actual_shipments_count, estimated_shipments_count,
                total_logistics_pln, last_delivery_at,
                calc_version, source_system, calculated_at
            )
            SELECT
                amazon_order_id, acc_order_id,
                shipments_count, delivered_shipments_count,
                actual_shipments_count, estimated_shipments_count,
                total_logistics_pln, last_delivery_at,
                calc_version, source_system,
                SYSUTCDATETIME()
            FROM #staging
        """)
        inserted = cur.rowcount
        conn.commit()
        t2 = time.time()

        print(f"\n{'='*60}")
        print(f"DONE!")
        print(f"  Rows inserted: {inserted:,}")
        print(f"  INSERT time:   {t2-t1:.1f}s")
        print(f"  Total time:    {t2-t0:.1f}s")
        print(f"{'='*60}")

        # Step 5: Verify
        print("\nVerification:")
        cur.execute("""
            SELECT calc_version, COUNT(*), AVG(total_logistics_pln)
            FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
            WHERE calc_version = ?
            GROUP BY calc_version
        """, (CALC_VERSION,))
        row = cur.fetchone()
        if row:
            print(f"  {row[0]}: {row[1]:,} rows, avg={float(row[2]):.2f} PLN")

        # Feb-Mar coverage check
        cur.execute("""
            SELECT
                MONTH(o.purchase_date) AS m,
                COUNT(*) AS total_orders,
                SUM(CASE WHEN f.amazon_order_id IS NOT NULL THEN 1 ELSE 0 END) AS has_fact
            FROM dbo.acc_order o WITH (NOLOCK)
            LEFT JOIN (
                SELECT DISTINCT amazon_order_id
                FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
            ) f ON f.amazon_order_id = o.amazon_order_id
            WHERE o.purchase_date >= '2026-02-01'
              AND o.purchase_date < '2026-04-01'
              AND o.status NOT IN ('Cancelled', 'Canceled')
            GROUP BY MONTH(o.purchase_date)
            ORDER BY 1
        """)
        for month, total_o, has_f in cur.fetchall():
            pct = has_f / total_o * 100 if total_o else 0
            print(f"  Month {month}: {has_f:,}/{total_o:,} ({pct:.1f}%) have logistics fact")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
