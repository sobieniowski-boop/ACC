"""
Reimport v3: incremental fill (no truncate). 
Lock already cleared by _kill_lock.py.
"""
import asyncio, sys, os, time, traceback
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, r"C:\ACC\apps\api")
os.chdir(r"C:\ACC\apps\api")
from dotenv import load_dotenv; load_dotenv(r"C:\ACC\.env", override=True)

STATUS = r"C:\ACC\reimport_v3_status.txt"

def log(msg):
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(STATUS, "a") as f:
        f.write(line + "\n")

async def main():
    from app.services.order_pipeline import step_sync_finances
    from app.core.db_connection import connect_acc

    with open(STATUS, "w") as f:
        f.write("")

    conn = connect_acc(); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
    before = cur.fetchone()[0]
    conn.close()
    log(f"Starting incremental sync. Current rows: {before:,}")
    log("Syncing 431 days back (Jan 2025 -> Mar 2026)...")

    t0 = time.time()
    try:
        result = await step_sync_finances(days_back=431)
        elapsed = time.time() - t0
        log(f"Sync completed in {elapsed:.0f}s")
        log(f"Result: {result}")
    except Exception as e:
        elapsed = time.time() - t0
        log(f"Sync FAILED after {elapsed:.0f}s: {e}")
        traceback.print_exc()

    # Verification
    log("\n=== VERIFICATION ===")
    conn = connect_acc(); cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
    after = cur.fetchone()[0]
    log(f"Total rows: {after:,} (was {before:,}, delta: {after - before:+,})")

    cur.execute("SELECT MIN(posted_date), MAX(posted_date) FROM acc_finance_transaction")
    rng = cur.fetchone()
    log(f"Date range: {rng[0]} to {rng[1]}")

    cur.execute("""
        SELECT FORMAT(CAST(posted_date AS DATE), 'yyyy-MM') as m, COUNT(*) as cnt
        FROM acc_finance_transaction
        GROUP BY FORMAT(CAST(posted_date AS DATE), 'yyyy-MM')
        ORDER BY m
    """)
    log("\nMonthly distribution:")
    for r in cur.fetchall():
        log(f"  {r[0]}: {r[1]:6d} rows")

    cur.execute("""
        SELECT FORMAT(CAST(posted_date AS DATE), 'yyyy-MM') as m,
               COUNT(*) as cnt, SUM(amount) as eur, SUM(amount_pln) as pln
        FROM acc_finance_transaction
        WHERE charge_type = 'FBAStorageFee'
        GROUP BY FORMAT(CAST(posted_date AS DATE), 'yyyy-MM')
        ORDER BY m
    """)
    log("\nFBAStorageFee monthly:")
    for r in cur.fetchall():
        eur = float(r[2] or 0)
        pln = float(r[3] or 0)
        log(f"  {r[0]}: {r[1]:4d} rows, EUR={eur:>10.2f}, PLN={pln:>12.2f}")

    conn.close()
    log("\n=== DONE ===")

if __name__ == "__main__":
    asyncio.run(main())
