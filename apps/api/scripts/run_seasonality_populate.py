"""Populate seasonality tables: widen columns, build metrics, indices, profiles, opportunities."""
import sys, os
from dotenv import load_dotenv
load_dotenv(r"C:\ACC\.env", override=True)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.core.db_connection import connect_acc

def widen_columns():
    conn = connect_acc()
    cur = conn.cursor()
    for tbl in [
        "seasonality_monthly_metrics",
        "seasonality_profile",
        "seasonality_index_cache",
        "seasonality_opportunity",
    ]:
        try:
            # Must drop unique index first if marketplace is part of it
            cur.execute(f"""
                SELECT i.name
                FROM sys.indexes i
                JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE OBJECT_NAME(i.object_id) = '{tbl}' AND c.name = 'marketplace'
                  AND i.is_primary_key = 0
            """)
            idxs = [r[0] for r in cur.fetchall()]
            for idx_name in idxs:
                try:
                    cur.execute(f"DROP INDEX [{idx_name}] ON [{tbl}]")
                    conn.commit()
                    print(f"  Dropped index {idx_name} on {tbl}")
                except Exception:
                    conn.rollback()

            cur.execute(f"ALTER TABLE [{tbl}] ALTER COLUMN marketplace NVARCHAR(20) NOT NULL")
            conn.commit()
            print(f"  {tbl}: widened marketplace to NVARCHAR(20)")

            # Recreate indexes
            if tbl == "seasonality_monthly_metrics":
                cur.execute("CREATE UNIQUE INDEX UX_season_monthly ON seasonality_monthly_metrics (marketplace, entity_type, entity_id, year, month)")
                cur.execute("CREATE INDEX IX_season_monthly_entity ON seasonality_monthly_metrics (entity_type, entity_id, marketplace)")
                conn.commit()
            elif tbl == "seasonality_profile":
                cur.execute("CREATE UNIQUE INDEX UX_season_profile ON seasonality_profile (marketplace, entity_type, entity_id)")
                conn.commit()
            elif tbl == "seasonality_index_cache":
                # PK is marketplace+entity_type+entity_id+month - skip, handled by PK
                pass
            elif tbl == "seasonality_opportunity":
                cur.execute("CREATE INDEX IX_season_opp_entity ON seasonality_opportunity (marketplace, entity_type, entity_id)")
                conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"  {tbl}: ERROR - {e}")
    conn.close()
    print("Column widening done.\n")


def populate():
    from app.services.seasonality_service import (
        build_monthly_metrics,
        recompute_indices,
        recompute_profiles,
    )
    from app.services.seasonality_opportunity_engine import detect_seasonality_opportunities

    print("Step 1/4: Building monthly metrics...")
    result = build_monthly_metrics(months_back=36)
    print(f"  => {result}")

    print("Step 2/4: Recomputing indices...")
    result2 = recompute_indices()
    print(f"  => {result2}")

    print("Step 3/4: Recomputing profiles...")
    result3 = recompute_profiles()
    print(f"  => {result3}")

    print("Step 4/4: Detecting opportunities...")
    result4 = detect_seasonality_opportunities()
    print(f"  => {result4}")

    print("\nAll done!")


if __name__ == "__main__":
    # widen_columns()  # Already done
    populate()
