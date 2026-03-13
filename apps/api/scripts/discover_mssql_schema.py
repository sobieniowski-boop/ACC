"""
Utility: discover NetfoxAnalityka MSSQL schema.
Run once to print actual table/column names so you can update netfox.py.

Usage (inside Docker):
    docker-compose run --rm api python scripts/discover_mssql_schema.py

Or locally (with pyodbc installed):
    python scripts/discover_mssql_schema.py
"""
import os
import sys

import pyodbc

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={os.getenv('MSSQL_SERVER', '192.168.230.120')},{os.getenv('MSSQL_PORT', '11901')};"
    f"DATABASE={os.getenv('MSSQL_DATABASE', 'NetfoxAnalityka')};"
    f"UID={os.getenv('MSSQL_USER', 'Analityka')};"
    f"PWD={os.getenv('MSSQL_PASSWORD', '')};"
    "TrustServerCertificate=yes;"
)

TABLES_OF_INTEREST = [
    # Adjust these guesses based on what you find
    "Kartoteki", "Stany", "Zamowienia", "ZamowieniaPozycje",
    "Produkty", "Artykuly", "Towary", "StanyMagazynowe",
    "Cennik", "CennikPozycje", "KartoWartosci",
]


def main() -> None:
    print(f"Connecting to {CONN_STR[:60]}...\n")
    try:
        conn = pyodbc.connect(CONN_STR, timeout=10)
    except pyodbc.Error as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

    cursor = conn.cursor()

    # ---- List all user tables ------------------------------------------------
    print("=" * 70)
    print("ALL USER TABLES in database")
    print("=" * 70)
    cursor.execute(
        """
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
    )
    all_tables: list[tuple[str, str]] = cursor.fetchall()
    for schema, table in all_tables:
        print(f"  {schema}.{table}")

    # ---- Columns for tables of interest -------------------------------------
    found_tables = {t.lower() for _, t in all_tables}
    to_inspect = [t for t in TABLES_OF_INTEREST if t.lower() in found_tables]

    # Also add all tables found and print their columns
    to_inspect_all = [t for _, t in all_tables]

    print("\n" + "=" * 70)
    print("COLUMNS for tables of interest")
    print("=" * 70)
    for tbl in to_inspect_all:
        cursor.execute(
            """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            """,
            tbl,
        )
        cols = cursor.fetchall()
        if cols:
            print(f"\n--- {tbl} ---")
            for col_name, dtype, nullable, max_len in cols:
                len_str = f"({max_len})" if max_len else ""
                null_str = "NULL" if nullable == "YES" else "NOT NULL"
                print(f"    {col_name:<40} {dtype}{len_str} {null_str}")

    # ---- Row counts ---------------------------------------------------------
    print("\n" + "=" * 70)
    print("ROW COUNTS")
    print("=" * 70)
    for schema, tbl in all_tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{tbl}]")
            count = cursor.fetchone()[0]
            print(f"  {schema}.{tbl:<50} {count:>10} rows")
        except Exception as e:
            print(f"  {schema}.{tbl:<50} ERROR: {e}")

    conn.close()
    print("\nDone. Copy the column names above into apps/api/app/connectors/mssql/netfox.py")


if __name__ == "__main__":
    main()
