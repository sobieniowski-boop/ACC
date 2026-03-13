"""Migrate data from read-only NetfoxAnalityka to new Azure SQL database.

Reads all acc_* tables from old DB (Analityka user, read-only),
then inserts into new Azure SQL database.

Usage:
    python scripts/migrate_to_azure.py

Env vars (in .env or set before running):
    OLD_MSSQL_SERVER=192.168.230.120
    OLD_MSSQL_PORT=11901
    OLD_MSSQL_USER=Analityka
    OLD_MSSQL_PASSWORD=tE4rYuGmcU@@#$3
    OLD_MSSQL_DATABASE=NetfoxAnalityka

    MSSQL_SERVER=<your-azure-server>.database.windows.net
    MSSQL_PORT=1433
    MSSQL_USER=accadmin
    MSSQL_PASSWORD=<your-azure-password>
    MSSQL_DATABASE=acc-db

Safety:
    - Old DB: only SELECT (read-only user)
    - New DB: INSERT only, WITH (NOLOCK) on source reads
    - SET LOCK_TIMEOUT 30000 on new DB connection
    - Batch inserts (100 rows at a time) to avoid timeouts
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

# Add apps/api to path so we can import settings
sys.path.insert(0, str(Path(__file__).parent.parent / "apps" / "api"))

import pyodbc

# ---------------------------------------------------------------------------
# Connection strings
# ---------------------------------------------------------------------------

# Load .env manually (simple approach, no dependency)
_env_path = Path(__file__).parent.parent / ".env"
_env_vars: dict[str, str] = {}
if _env_path.exists():
    for line in _env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        _env_vars[key.strip()] = val.strip()


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, _env_vars.get(key, default))


def _detect_driver() -> str:
    """Pick best available ODBC driver."""
    available = pyodbc.drivers()
    for preferred in [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",
    ]:
        if preferred in available:
            return preferred
    return "SQL Server"


DRIVER = _detect_driver()
TRUST = "TrustServerCertificate=yes;" if "17" in DRIVER or "18" in DRIVER else ""

# Old DB (read-only source)
OLD_CONN = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={_env('OLD_MSSQL_SERVER', '192.168.230.120')},{_env('OLD_MSSQL_PORT', '11901')};"
    f"DATABASE={_env('OLD_MSSQL_DATABASE', 'NetfoxAnalityka')};"
    f"UID={_env('OLD_MSSQL_USER', 'Analityka')};"
    f"PWD={_env('OLD_MSSQL_PASSWORD', 'tE4rYuGmcU@@#$3')};"
    f"{TRUST}"
)

# New Azure SQL (target, full read+write)
NEW_CONN = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={_env('MSSQL_SERVER')},{_env('MSSQL_PORT', '1433')};"
    f"DATABASE={_env('MSSQL_DATABASE')};"
    f"UID={_env('MSSQL_USER')};"
    f"PWD={_env('MSSQL_PASSWORD')};"
    f"{TRUST}"
    f"Encrypt=yes;"
)

# Tables to migrate in dependency order (parents first)
# Only tables with acc_ prefix — we don't migrate Netfox internal tables
TABLES_ORDERED = [
    # 1. Independent tables (no FK dependencies)
    "acc_user",
    "acc_marketplace",
    # 2. Tables depending on acc_marketplace / acc_user
    "acc_product",
    "acc_exchange_rate",
    "acc_purchase_price",
    # 3. Orders
    "acc_order",
    "acc_order_line",
    # 4. Finance
    "acc_finance_transaction",
    # 5. Inventory & offers
    "acc_inventory_snapshot",
    "acc_offer",
    # 6. Ads
    "acc_ads_campaign",
    "acc_ads_campaign_day",
    # 7. Alerts
    "acc_alert_rule",
    "acc_alert",
    # 8. Plans
    "acc_plan_month",
    "acc_plan_line",
    # 9. Jobs
    "acc_job_run",
    # 10. AI
    "acc_ai_recommendation",
    # 11. Helper tables (acc_al_*)
    "acc_al_alert_rules",
    "acc_al_alerts",
    "acc_al_jobs",
    "acc_al_plans",
    "acc_al_plan_lines",
    "acc_al_profit_snapshot",
    "acc_audit_log",
    # 12. Family Mapper
    "global_family",
    "global_family_child",
    "marketplace_listing_child",
    "global_family_child_market_link",
    "global_family_market_link",
    "family_coverage_cache",
    "family_issues_cache",
    "family_fix_package",
    "family_fix_job",
]

BATCH_SIZE = 100


def _table_exists(cur: pyodbc.Cursor, table: str) -> bool:
    """Check if table exists in the database."""
    cur.execute(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
        [table],
    )
    return cur.fetchone()[0] > 0


def _count_rows(cur: pyodbc.Cursor, table: str) -> int:
    """Count rows in a table (with NOLOCK)."""
    cur.execute(f"SELECT COUNT(*) FROM [{table}] WITH (NOLOCK)")
    return cur.fetchone()[0]


def _has_identity(cur: pyodbc.Cursor, table: str) -> bool:
    """Check if table has IDENTITY column."""
    cur.execute(
        """
        SELECT COUNT(*)
        FROM sys.identity_columns ic
        JOIN sys.tables t ON ic.object_id = t.object_id
        WHERE t.name = ?
        """,
        [table],
    )
    return cur.fetchone()[0] > 0


def migrate_table(
    old_conn: pyodbc.Connection,
    new_conn: pyodbc.Connection,
    table: str,
) -> int:
    """Migrate one table from old DB to new DB. Returns row count."""
    old_cur = old_conn.cursor()
    new_cur = new_conn.cursor()

    # Check source exists
    if not _table_exists(old_cur, table):
        print(f"  ⏭  {table} — not found in source DB, skipping")
        old_cur.close()
        new_cur.close()
        return 0

    # Check target exists
    if not _table_exists(new_cur, table):
        print(f"  ⏭  {table} — not found in target DB (run CREATE TABLE first!), skipping")
        old_cur.close()
        new_cur.close()
        return 0

    # Count source
    src_count = _count_rows(old_cur, table)
    if src_count == 0:
        print(f"  ⏭  {table} — empty in source, skipping")
        old_cur.close()
        new_cur.close()
        return 0

    # Check if target already has data
    tgt_count = _count_rows(new_cur, table)
    if tgt_count > 0:
        print(f"  ⏭  {table} — target already has {tgt_count} rows, skipping (no duplicates)")
        old_cur.close()
        new_cur.close()
        return 0

    # Read all from source
    old_cur.execute(f"SELECT * FROM [{table}] WITH (NOLOCK)")
    cols = [desc[0] for desc in old_cur.description]
    rows = old_cur.fetchall()
    old_cur.close()

    if not rows:
        new_cur.close()
        return 0

    # Check for IDENTITY column in target
    has_ident = _has_identity(new_cur, table)

    # Build INSERT
    col_list = ", ".join(f"[{c}]" for c in cols)
    placeholders = ", ".join("?" for _ in cols)
    insert_sql = f"INSERT INTO [{table}] ({col_list}) VALUES ({placeholders})"

    # Insert in batches
    inserted = 0
    try:
        if has_ident:
            new_cur.execute(f"SET IDENTITY_INSERT [{table}] ON")

        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            for row in batch:
                # Convert row values — handle problematic types
                vals = []
                for v in row:
                    if isinstance(v, bytearray):
                        vals.append(bytes(v))
                    else:
                        vals.append(v)
                try:
                    new_cur.execute(insert_sql, vals)
                except pyodbc.Error as e:
                    print(f"    ⚠  Row insert error: {e}")
                    continue
                inserted += 1

            new_conn.commit()
            if (i + BATCH_SIZE) % 500 == 0 or i + BATCH_SIZE >= len(rows):
                print(f"    ... {min(inserted, len(rows))}/{len(rows)}")

        if has_ident:
            new_cur.execute(f"SET IDENTITY_INSERT [{table}] OFF")
            new_conn.commit()

    except Exception as exc:
        print(f"  ❌  {table} — error: {exc}")
        new_conn.rollback()
        new_cur.close()
        return inserted

    new_cur.close()
    return inserted


def main() -> None:
    print("=" * 60)
    print("ACC Data Migration: NetfoxAnalityka → Azure SQL")
    print("=" * 60)

    # Validate env
    if not _env("MSSQL_SERVER"):
        print("\n❌ ERROR: MSSQL_SERVER not set in .env!")
        print("   Set the new Azure SQL server address first.")
        print("   Example: MSSQL_SERVER=acc-kadax.database.windows.net")
        sys.exit(1)

    print(f"\nSource: {_env('OLD_MSSQL_SERVER', '192.168.230.120')}:{_env('OLD_MSSQL_PORT', '11901')}"
          f" / {_env('OLD_MSSQL_DATABASE', 'NetfoxAnalityka')}")
    print(f"Target: {_env('MSSQL_SERVER')}:{_env('MSSQL_PORT', '1433')}"
          f" / {_env('MSSQL_DATABASE')}")

    # Connect to old DB (read-only)
    print("\n🔌 Connecting to source (old DB)...")
    try:
        old_conn = pyodbc.connect(OLD_CONN, timeout=20, autocommit=True)
        print("   ✅ Source connected")
    except pyodbc.Error as e:
        print(f"   ❌ Source connection failed: {e}")
        sys.exit(1)

    # Connect to new DB
    print("🔌 Connecting to target (Azure SQL)...")
    try:
        new_conn = pyodbc.connect(NEW_CONN, timeout=30, autocommit=False)
        cur = new_conn.cursor()
        cur.execute("SET LOCK_TIMEOUT 30000")
        cur.close()
        new_conn.commit()
        print("   ✅ Target connected")
    except pyodbc.Error as e:
        print(f"   ❌ Target connection failed: {e}")
        print("   Check: MSSQL_SERVER, MSSQL_USER, MSSQL_PASSWORD, MSSQL_DATABASE in .env")
        print("   Make sure your IP is in Azure SQL firewall rules!")
        old_conn.close()
        sys.exit(1)

    # Migrate tables
    print(f"\n📦 Migrating {len(TABLES_ORDERED)} tables...\n")
    total_rows = 0
    t0 = time.time()

    for table in TABLES_ORDERED:
        print(f"  📋 {table}...")
        count = migrate_table(old_conn, new_conn, table)
        total_rows += count
        if count > 0:
            print(f"  ✅ {table} — {count} rows migrated")

    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"✅ Migration complete!")
    print(f"   Total rows: {total_rows}")
    print(f"   Time: {elapsed:.1f}s")
    print(f"{'=' * 60}")

    # Cleanup
    old_conn.close()
    new_conn.close()


if __name__ == "__main__":
    main()
