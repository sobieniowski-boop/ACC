"""
Full clean reimport: TRUNCATE acc_finance_transaction → step_sync_finances(days_back=431)
Covers Jan 1 2025 → Mar 7 2026. Writes progress to C:/ACC/reimport_status.txt
"""
import asyncio, sys, os, time
sys.path.insert(0, r"C:\ACC\apps\api")
os.chdir(r"C:\ACC\apps\api")
from dotenv import load_dotenv
load_dotenv("C:/ACC/.env")

STATUS = "C:/ACC/reimport_status.txt"

def log(msg):
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(STATUS, "a") as f:
        f.write(line + "\n")

async def main():
    import pymssql
    from app.services.order_pipeline import step_sync_finances

    # Clear status file
    with open(STATUS, "w") as f:
        f.write("")

    # ── Step 1: TRUNCATE ──
    log("Connecting to DB...")
    conn = pymssql.connect(
        server=os.getenv('MSSQL_SERVER'), user=os.getenv('MSSQL_USER'),
        password=os.getenv('MSSQL_PASSWORD'), database=os.getenv('MSSQL_DATABASE'),
        port=1433, tds_version='7.3')
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
    before = cur.fetchone()[0]
    log(f"Before TRUNCATE: {before:,} rows")

    log("TRUNCATING acc_finance_transaction...")
    cur.execute("TRUNCATE TABLE acc_finance_transaction")
    conn.commit()
    log("TRUNCATE done.")
    conn.close()

    # ── Step 2: Reimport ──
    log("Starting step_sync_finances(days_back=431)...")
    t0 = time.time()
    result = await step_sync_finances(days_back=431)
    elapsed = time.time() - t0
    log(f"Reimport done in {elapsed:.0f}s. Result: {result}")

    # ── Step 3: Verify ──
    conn = pymssql.connect(
        server=os.getenv('MSSQL_SERVER'), user=os.getenv('MSSQL_USER'),
        password=os.getenv('MSSQL_PASSWORD'), database=os.getenv('MSSQL_DATABASE'),
        port=1433, tds_version='7.3')
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
    after = cur.fetchone()[0]

    cur.execute("SELECT MIN(posted_date), MAX(posted_date) FROM acc_finance_transaction")
    rng = cur.fetchone()

    cur.execute("SELECT COUNT(*), SUM(amount) FROM acc_finance_transaction WHERE charge_type='FBAStorageFee'")
    fba = cur.fetchone()

    cur.execute("""
        SELECT CONVERT(VARCHAR(7), posted_date, 120) m, COUNT(*) cnt, SUM(amount) s
        FROM acc_finance_transaction
        WHERE charge_type='FBAStorageFee'
        GROUP BY CONVERT(VARCHAR(7), posted_date, 120)
        ORDER BY m
    """)
    monthly = cur.fetchall()

    conn.close()

    log(f"\n=== VERIFICATION ===")
    log(f"Total rows: {after:,}")
    log(f"Date range: {rng[0]} to {rng[1]}")
    log(f"FBAStorageFee: {fba[0]} rows, sum={fba[1]} EUR")
    log(f"\nFBAStorageFee monthly:")
    for m in monthly:
        log(f"  {m[0]}: {m[1]} rows, sum={m[2]:.2f} EUR")
    log(f"\n=== COMPLETE ===")

if __name__ == "__main__":
    asyncio.run(main())
