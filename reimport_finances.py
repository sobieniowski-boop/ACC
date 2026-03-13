"""
Full re-import of financial transactions using the canonical step_sync_finances.

This will DELETE existing rows per marketplace+time window and INSERT fresh data
from the Amazon SP-API Finances v2024-06-19 endpoint, eliminating all duplicates
that were caused by the old v0 import paths.
"""
import asyncio
import sys
import os
import time

# Ensure imports work
sys.path.insert(0, r"C:\ACC\apps\api")
os.chdir(r"C:\ACC\apps\api")

from dotenv import load_dotenv
load_dotenv("C:/ACC/.env")


async def main():
    from app.services.order_pipeline import step_sync_finances
    import pymssql

    # Before stats
    conn = pymssql.connect(
        server=os.getenv('MSSQL_SERVER'),
        user=os.getenv('MSSQL_USER'),
        password=os.getenv('MSSQL_PASSWORD'),
        database=os.getenv('MSSQL_DATABASE'),
        port=1433, tds_version='7.3'
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
    before = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*), SUM(amount) FROM acc_finance_transaction WHERE charge_type='FBAStorageFee'")
    fba_before = cur.fetchone()
    conn.close()

    print(f"BEFORE: {before:,} total rows")
    print(f"BEFORE FBAStorageFee: {fba_before[0]} rows, sum={fba_before[1]} EUR")
    print(f"\nRunning step_sync_finances(days_back=200)...")
    t0 = time.time()

    result = await step_sync_finances(days_back=200)
    elapsed = time.time() - t0

    print(f"\nResult: {result}")
    print(f"Elapsed: {elapsed:.1f}s")

    # After stats
    conn = pymssql.connect(
        server=os.getenv('MSSQL_SERVER'),
        user=os.getenv('MSSQL_USER'),
        password=os.getenv('MSSQL_PASSWORD'),
        database=os.getenv('MSSQL_DATABASE'),
        port=1433, tds_version='7.3'
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
    after = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*), SUM(amount) FROM acc_finance_transaction WHERE charge_type='FBAStorageFee'")
    fba_after = cur.fetchone()
    conn.close()

    summary = f"""
=== FINANCE RE-IMPORT COMPLETE ===
Before:  {before:,} total rows
After:   {after:,} total rows
Delta:   {after - before:,}
Elapsed: {elapsed:.1f}s

FBAStorageFee Before: {fba_before[0]} rows, sum={fba_before[1]} EUR
FBAStorageFee After:  {fba_after[0]} rows, sum={fba_after[1]} EUR

API Result: {result}
"""
    print(summary)
    with open('C:/ACC/reimport_result.txt', 'w') as f:
        f.write(summary)


if __name__ == "__main__":
    asyncio.run(main())
