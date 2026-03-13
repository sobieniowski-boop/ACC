from __future__ import annotations

import json
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.db_connection import connect_acc


RECOMMENDED_INDEXES = [
    (
        "dbo.acc_shipment_order_link",
        "IX_acc_shipment_order_link_primary_carrier",
        "CREATE INDEX IX_acc_shipment_order_link_primary_carrier ON dbo.acc_shipment_order_link(is_primary, shipment_id, amazon_order_id) INCLUDE (link_confidence, link_method, updated_at)",
    ),
    (
        "dbo.acc_order_logistics_fact",
        "IX_acc_order_logistics_fact_calc_version",
        "CREATE INDEX IX_acc_order_logistics_fact_calc_version ON dbo.acc_order_logistics_fact(calc_version, amazon_order_id) INCLUDE (shipments_count, total_logistics_pln, actual_shipments_count, estimated_shipments_count, calculated_at)",
    ),
    (
        "dbo.acc_order_logistics_shadow",
        "IX_acc_order_logistics_shadow_calc_version",
        "CREATE INDEX IX_acc_order_logistics_shadow_calc_version ON dbo.acc_order_logistics_shadow(calc_version, amazon_order_id) INCLUDE (comparison_status, delta_abs_pln, calculated_at)",
    ),
    (
        "dbo.acc_cache_bl_orders",
        "IX_acc_cache_bl_orders_external_order_id",
        "CREATE INDEX IX_acc_cache_bl_orders_external_order_id ON dbo.acc_cache_bl_orders(external_order_id) INCLUDE (order_id)",
    ),
    (
        "dbo.acc_cache_packages",
        "IX_acc_cache_packages_order_id",
        "CREATE INDEX IX_acc_cache_packages_order_id ON dbo.acc_cache_packages(order_id) INCLUDE (courier_package_nr, courier_inner_number, courier_code, courier_other_name)",
    ),
    (
        "dbo.acc_bl_distribution_order_cache",
        "IX_acc_bl_distribution_order_cache_external_order_id",
        "CREATE INDEX IX_acc_bl_distribution_order_cache_external_order_id ON dbo.acc_bl_distribution_order_cache(external_order_id) INCLUDE (order_id, delivery_package_nr, delivery_method, delivery_package_module)",
    ),
    (
        "dbo.acc_bl_distribution_package_cache",
        "IX_acc_bl_distribution_package_cache_order_id",
        "CREATE INDEX IX_acc_bl_distribution_package_cache_order_id ON dbo.acc_bl_distribution_package_cache(order_id) INCLUDE (courier_package_nr, courier_inner_number, courier_code, courier_other_name)",
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit courier SQL indexes and emit optional DDL.")
    parser.add_argument("--apply", action="store_true", help="Apply missing indexes (use only in low-traffic window).")
    args = parser.parse_args()

    conn = connect_acc(timeout=30)
    try:
        cur = conn.cursor()
        rows = []
        applied = []
        for table_name, index_name, ddl in RECOMMENDED_INDEXES:
            cur.execute(
                """
                SELECT CASE WHEN EXISTS (
                    SELECT 1
                    FROM sys.indexes i
                    WHERE i.object_id = OBJECT_ID(?)
                      AND i.name = ?
                ) THEN 1 ELSE 0 END
                """,
                (table_name, index_name),
            )
            exists = int(cur.fetchone()[0] or 0)
            if args.apply and not exists:
                cur.execute(ddl)
                applied.append(index_name)
            rows.append(
                {
                    "table": table_name,
                    "index": index_name,
                    "exists": bool(exists),
                    "ddl": ddl,
                }
            )
        if args.apply:
            conn.commit()
        print(json.dumps({"indexes": rows, "applied": applied, "apply_mode": bool(args.apply)}, ensure_ascii=False, indent=2))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
