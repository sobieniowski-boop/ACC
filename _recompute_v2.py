"""Run V2 recalc_profit_orders for full date range 2026-01-01 to 2026-03-09."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))

from datetime import date
from app.connectors.mssql.mssql_store import recalc_profit_orders

date_from = date(2026, 1, 1)
date_to = date(2026, 3, 9)

print(f"Running V2 recalc_profit_orders({date_from} to {date_to})...")
count = recalc_profit_orders(date_from=date_from, date_to=date_to)
print(f"Orders updated: {count}")
print("Done.")
