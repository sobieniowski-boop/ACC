"""
Watchdog for finance reimport. Checks every 60s.
- If reimport process died and rows < threshold → restarts step_sync_finances
- Logs to C:/ACC/watchdog_log.txt
- Run: C:\.venv\Scripts\python.exe C:\ACC\watchdog_reimport.py
"""
import subprocess, sys, os, time, pymssql
from dotenv import load_dotenv

load_dotenv("C:/ACC/.env")

LOG_FILE = "C:/ACC/watchdog_log.txt"
STATUS_FILE = "C:/ACC/reimport_status.txt"
EXPECTED_MIN_ROWS = 500_000  # below this = not done yet
CHECK_INTERVAL = 60  # seconds
PYTHON = r"C:\.venv\Scripts\python.exe"
RESUME_SCRIPT = "C:/ACC/resume_reimport.py"


def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def get_row_count():
    try:
        conn = pymssql.connect(
            server=os.getenv("MSSQL_SERVER"),
            user=os.getenv("MSSQL_USER"),
            password=os.getenv("MSSQL_PASSWORD"),
            database=os.getenv("MSSQL_DATABASE"),
            port=int(os.getenv("MSSQL_PORT", "1433")),
            tds_version="7.3",
            login_timeout=10,
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM acc_finance_transaction")
        count = cur.fetchone()[0]
        conn.close()
        return count
    except Exception as e:
        log(f"  DB error: {e}")
        return -1


def is_reimport_running():
    """Check if any python process is running reimport or resume script."""
    try:
        r = subprocess.run(
            ["powershell", "-c",
             "Get-WmiObject Win32_Process -Filter \"Name='python.exe'\" | "
             "Select-Object ProcessId, CommandLine | Format-List"],
            capture_output=True, text=True, timeout=15,
        )
        output = r.stdout.lower()
        return "reimport_clean" in output or "resume_reimport" in output
    except Exception as e:
        log(f"  Process check error: {e}")
        return True  # assume running if we can't check


def is_complete():
    """Check if status file says COMPLETE."""
    try:
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE, "r") as f:
                return "=== COMPLETE ===" in f.read()
    except Exception:
        pass
    return False


def spawn_resume():
    """Start the resume script in background."""
    log("  >>> Spawning resume_reimport.py <<<")
    subprocess.Popen(
        [PYTHON, RESUME_SCRIPT],
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
        stdout=open("C:/ACC/resume_stdout.txt", "a"),
        stderr=open("C:/ACC/resume_stderr.txt", "a"),
    )
    log("  Resume process started.")


def main():
    log("=" * 50)
    log("WATCHDOG STARTED")
    log(f"  Check interval: {CHECK_INTERVAL}s")
    log(f"  Min row threshold: {EXPECTED_MIN_ROWS:,}")
    log("=" * 50)

    prev_rows = 0
    stall_count = 0

    while True:
        time.sleep(CHECK_INTERVAL)

        if is_complete():
            rows = get_row_count()
            log(f"DONE! Status file says COMPLETE. Rows: {rows:,}. Watchdog exiting.")
            break

        rows = get_row_count()
        running = is_reimport_running()

        log(f"Check: rows={rows:,}  running={running}  prev={prev_rows:,}")

        if rows >= EXPECTED_MIN_ROWS and not running:
            log(f"Rows look good ({rows:,} >= {EXPECTED_MIN_ROWS:,}) and process finished. Watchdog exiting.")
            break

        if not running and rows < EXPECTED_MIN_ROWS and rows >= 0:
            log(f"ALERT: Process DEAD with only {rows:,} rows!")
            spawn_resume()
            stall_count = 0
            prev_rows = rows
            time.sleep(30)  # give it time to start
            continue

        # Detect stall (rows not growing for 5 checks = 5 min)
        if rows == prev_rows and rows >= 0 and running:
            stall_count += 1
            if stall_count >= 5:
                log(f"WARNING: Rows stuck at {rows:,} for {stall_count} checks.")
                stall_count = 0  # reset, just warn
        else:
            stall_count = 0

        prev_rows = rows

    log("Watchdog terminated.")


if __name__ == "__main__":
    main()
