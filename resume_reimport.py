"""
Resume reimport (no TRUNCATE). Picks up where it left off via group watermarks.
Called automatically by watchdog_reimport.py if main process dies.
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
    print(line, flush=True)
    with open(STATUS, "a") as f:
        f.write(line + "\n")


async def main():
    import pymssql
    from app.services.order_pipeline import step_sync_finances

    log("RESUME: starting step_sync_finances(days_back=431)...")
    t0 = time.time()
    result = await step_sync_finances(days_back=431)
    elapsed = time.time() - t0
    log(f"RESUME: done in {elapsed:.0f}s. Result: {result}")

    # Verification
    conn = pymssql.connect(
        server=os.getenv("MSSQL_SERVER"), user=os.getenv("MSSQL_USER"),
        password=os.getenv("MSSQL_PASSWORD"), database=os.getenv("MSSQL_DATABASE"),
        port=1433, tds_version="7.3",
    )
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
    total = cur.fetchone()[0]

    cur.execute("""
        SELECT CONVERT(VARCHAR(7), posted_date, 120) m, COUNT(*) cnt, SUM(amount) s
        FROM acc_finance_transaction WHERE charge_type='FBAStorageFee'
        GROUP BY CONVERT(VARCHAR(7), posted_date, 120) ORDER BY m
    """)
    monthly = cur.fetchall()
    conn.close()

    log(f"\n=== VERIFICATION ===")
    log(f"Total rows: {total:,}")
    log(f"FBAStorageFee monthly:")
    for m in monthly:
        log(f"  {m[0]}: {m[1]} rows, sum={m[2]:.2f} EUR")
    log(f"\n=== COMPLETE ===")


if __name__ == "__main__":
    asyncio.run(main())
