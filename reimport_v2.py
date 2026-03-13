"""
Release stuck finance sync lock, clear stale group sync data, 
then run full reimport WITHOUT truncating (incremental fill).
"""
import asyncio, sys, os, time, traceback
sys.path.insert(0, r"C:\ACC\apps\api")
os.chdir(r"C:\ACC\apps\api")
from dotenv import load_dotenv; load_dotenv(r"C:\ACC\.env", override=True)

STATUS = r"C:\ACC\reimport_v2_status.txt"

def log(msg):
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(STATUS, "a") as f:
        f.write(line + "\n")

async def main():
    import pymssql
    from app.services.order_pipeline import step_sync_finances

    with open(STATUS, "w") as f:
        f.write("")

    # ── Step 1: Kill stuck lock ──
    log("Connecting to DB to release stuck lock...")
    conn = pymssql.connect(
        server=os.getenv('MSSQL_SERVER'), user=os.getenv('MSSQL_USER'),
        password=os.getenv('MSSQL_PASSWORD'), database=os.getenv('MSSQL_DATABASE'),
        port=1433, tds_version='7.3')
    cur = conn.cursor()

    # Check current state
    cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
    before = cur.fetchone()[0]
    log(f"Current rows: {before:,}")
    
    # Check for stuck sessions holding the lock
    cur.execute("""
        SELECT request_session_id 
        FROM sys.dm_tran_locks 
        WHERE resource_type = 'APPLICATION'
          AND resource_description LIKE '%acc_finance_sync%'
    """)
    stuck_sessions = [r[0] for r in cur.fetchall()]
    
    if stuck_sessions:
        my_session = None
        cur.execute("SELECT @@SPID")
        my_session = cur.fetchone()[0]
        log(f"My session: {my_session}")
        
        for sid in stuck_sessions:
            if sid != my_session:
                log(f"Killing stuck session {sid}...")
                try:
                    cur.execute(f"KILL {sid}")
                    conn.commit()
                    log(f"  Session {sid} killed.")
                except Exception as e:
                    log(f"  Could not kill {sid}: {e}")
    else:
        log("No stuck locks found.")
    
    # Wait for lock release
    time.sleep(2)
    
    # Verify lock is gone
    cur.execute("""
        SELECT COUNT(*)
        FROM sys.dm_tran_locks 
        WHERE resource_type = 'APPLICATION'
          AND resource_description LIKE '%acc_finance_sync%'
    """)
    remaining = cur.fetchone()[0]
    log(f"Remaining application locks: {remaining}")
    
    # Clear stale group sync table if exists (so nothing is "skipped")
    cur.execute("""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = 'acc_finance_group_sync'
    """)
    if cur.fetchone()[0] > 0:
        cur.execute("DELETE FROM acc_finance_group_sync")
        conn.commit()
        log("Cleared acc_finance_group_sync to force full resync")
    
    conn.close()
    
    # ── Step 2: Run full sync ──
    # Don't truncate - just sync. The sync uses DELETE+INSERT per window/group
    # so it will fill gaps and update existing data 
    log("Starting step_sync_finances(days_back=431)...")
    t0 = time.time()
    
    try:
        result = await step_sync_finances(days_back=431)
        elapsed = time.time() - t0
        log(f"Sync completed in {elapsed:.0f}s. Result: {result}")
    except Exception as e:
        elapsed = time.time() - t0
        log(f"Sync FAILED after {elapsed:.0f}s: {e}")
        traceback.print_exc()
        # Continue to verification even if failed
    
    # ── Step 3: Verify ──
    log("\n=== VERIFICATION ===")
    conn2 = pymssql.connect(
        server=os.getenv('MSSQL_SERVER'), user=os.getenv('MSSQL_USER'),
        password=os.getenv('MSSQL_PASSWORD'), database=os.getenv('MSSQL_DATABASE'),
        port=1433, tds_version='7.3')
    cur2 = conn2.cursor()
    
    cur2.execute("SELECT COUNT(*) FROM acc_finance_transaction")
    after = cur2.fetchone()[0]
    
    cur2.execute("SELECT MIN(posted_date), MAX(posted_date) FROM acc_finance_transaction")
    rng = cur2.fetchone()
    
    cur2.execute("""
        SELECT FORMAT(CAST(posted_date AS DATE), 'yyyy-MM') as m,
               COUNT(*) as cnt
        FROM acc_finance_transaction
        GROUP BY FORMAT(CAST(posted_date AS DATE), 'yyyy-MM')
        ORDER BY m
    """)
    log(f"Total rows: {after:,} (was {before:,})")
    log(f"Date range: {rng[0]} to {rng[1]}")
    log("\nMonthly distribution:")
    for r in cur2.fetchall():
        log(f"  {r[0]}: {r[1]:6d} rows")
    
    # Storage fees
    cur2.execute("""
        SELECT COUNT(*), SUM(amount), SUM(amount_pln)
        FROM acc_finance_transaction 
        WHERE charge_type = 'FBAStorageFee'
    """)
    fba = cur2.fetchone()
    fba_cnt = fba[0] or 0
    fba_eur = float(fba[1] or 0)
    fba_pln = float(fba[2] or 0)
    log(f"\nFBAStorageFee: {fba_cnt} rows, EUR={fba_eur:.2f}, PLN={fba_pln:.2f}")
    
    cur2.execute("""
        SELECT FORMAT(CAST(posted_date AS DATE), 'yyyy-MM') as m,
               COUNT(*) as cnt, SUM(amount) as eur, SUM(amount_pln) as pln
        FROM acc_finance_transaction
        WHERE charge_type = 'FBAStorageFee'
        GROUP BY FORMAT(CAST(posted_date AS DATE), 'yyyy-MM')
        ORDER BY m
    """)
    log("\nFBAStorageFee monthly:")
    for r in cur2.fetchall():
        eur = float(r[2] or 0)
        pln = float(r[3] or 0)
        log(f"  {r[0]}: {r[1]:4d} rows, EUR={eur:>10.2f}, PLN={pln:>12.2f}")
    
    conn2.close()
    log("\n=== COMPLETE ===")

if __name__ == "__main__":
    asyncio.run(main())
