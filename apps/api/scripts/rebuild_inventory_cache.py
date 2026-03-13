"""Rebuild the inventory item cache.

Run with:
    cd apps/api && python -m scripts.rebuild_inventory_cache
"""
from __future__ import annotations

import sys
import os

# ensure the app package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.manage_inventory import (
    ensure_manage_inventory_schema,
    _rebuild_inventory_item_cache,
)


def main() -> None:
    print("[1/2] Ensuring schema (creating missing tables if needed)...")
    ensure_manage_inventory_schema()
    print("      Schema OK.")

    print("[2/2] Rebuilding inventory item cache from snapshots...")
    result = _rebuild_inventory_item_cache()
    print(f"      Done! rows={result.get('rows', 0)}  snapshot_date={result.get('snapshot_date')}")
    print(f"      traffic_partial={result.get('traffic_partial', 0)}")


if __name__ == "__main__":
    main()
