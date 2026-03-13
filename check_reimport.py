"""Quick health check for the running reimport process."""
import pymssql, subprocess, sys, os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv("C:/ACC/.env")

def main():
    # 1) Is python still running the reimport?
    try:
        r = subprocess.run(
            ["powershell", "-c",
             "Get-Process python -ErrorAction SilentlyContinue | "
             "Select-Object Id, StartTime, CPU | Format-Table -AutoSize"],
            capture_output=True, text=True, timeout=10
        )
        procs = r.stdout.strip()
    except Exception as e:
        procs = f"(error checking processes: {e})"

    print("=" * 60)
    print(f"  REIMPORT HEALTH CHECK  —  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 60)

    if "python" in procs.lower() or procs.strip():
        print(f"\n✅ Python process(es) running:\n{procs}")
    else:
        print("\n❌ NO Python process found — reimport may have CRASHED or FINISHED!")

    # 2) Row count + last posted_date
    row_count = 0
    try:
        conn = pymssql.connect(
            server=os.getenv("MSSQL_SERVER", "acc-sql-kadax.database.windows.net"),
            user=os.getenv("MSSQL_USER", "accadmin"),
            password=os.getenv("MSSQL_PASSWORD"),
            database=os.getenv("MSSQL_DATABASE", "ACC"),
            port=int(os.getenv("MSSQL_PORT", "1433")),
            tds_version="7.3",
            login_timeout=15,
        )
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
        row_count = cur.fetchone()[0]

        cur.execute("SELECT MIN(posted_date), MAX(posted_date) FROM acc_finance_transaction")
        min_date, max_date = cur.fetchone()

        cur.execute("""
            SELECT TOP 5 marketplace_id, financial_event_group_id,
                   COUNT(*) as rows, MAX(posted_date) as last_date
            FROM acc_finance_transaction
            GROUP BY marketplace_id, financial_event_group_id
            ORDER BY MAX(posted_date) DESC
        """)
        recent = cur.fetchall()

        cur.execute("""
            SELECT charge_type, SUM(CAST(amount AS FLOAT)) as total
            FROM acc_finance_transaction
            WHERE charge_type = 'FBAStorageFee'
            GROUP BY charge_type
        """)
        fba = cur.fetchone()

        conn.close()

        print(f"\n📊 Table: acc_finance_transaction")
        print(f"   Total rows:  {row_count:,}")
        print(f"   Date range:  {min_date} → {max_date}")

        if fba:
            print(f"   FBAStorageFee total: {fba[1]:,.2f} EUR")

        if recent:
            print(f"\n   Last 5 groups imported:")
            for mkt, grp, cnt, dt in recent:
                print(f"     {mkt}  group={grp[:20]}...  rows={cnt}  last={dt}")

    except Exception as e:
        print(f"\n❌ DB error: {e}")

    # 3) Status file
    status_file = "C:/ACC/reimport_status.txt"
    if os.path.exists(status_file):
        with open(status_file, "r") as f:
            lines = f.readlines()
        print(f"\n📄 Status file ({len(lines)} lines):")
        for line in lines[-5:]:
            print(f"   {line.rstrip()}")
    else:
        print(f"\n⚠️  No status file found at {status_file}")

    # Verdict
    print("\n" + "=" * 60)
    if row_count == 0:
        print("⚠️  ZERO rows — reimport hasn't inserted anything yet or crashed early")
    elif row_count > 0 and ("python" not in procs.lower() and not procs.strip()):
        print("🏁 Process finished! Check if row count looks reasonable (~1.5M expected)")
    else:
        print(f"🔄 In progress... {row_count:,} rows so far")
    print("=" * 60)

if __name__ == "__main__":
    main()
