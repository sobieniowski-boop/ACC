"""
Safe Finance API backfill with watchdog, monitoring, retry and natural resume.

How it works:
  - Calls step_sync_finances(days_back=N) which handles everything:
    Phase 1 tries direct v2024 API (usually empty for historical),
    Phase 2 falls back to legacy event groups API (where the data actually is).
  - Each event group is committed atomically to DB after processing.
  - acc_fin_event_group_sync tracks each group's status + payload signature.
  - On restart: terminal groups (Closed+Succeeded) are SKIPPED automatically
    by _should_resync_group(), giving natural resume behavior.

Features:
  - Watchdog thread: monitors heartbeat, logs progress, detects stalls
  - DB monitor: periodically queries acc_finance_transaction for real row count
  - Retry: up to MAX_RETRIES attempts with exponential backoff
  - Safe: NO TRUNCATE, uses DELETE+INSERT per group (idempotent)
  - Progress file: real-time status queryable with --status

Usage:
    cd C:\\ACC\\apps\\api
    python C:\\ACC\\_backfill_finances_safe.py              # run backfill
    python C:\\ACC\\_backfill_finances_safe.py --status      # show DB progress
    python C:\\ACC\\_backfill_finances_safe.py --dry-run     # show plan without executing

Covers: Jan 1 2025 → now (~435 days). Fills the Feb 2025 - Feb 2026 gap.
"""
import asyncio
import json
import os
import sys
import time
import threading
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Setup Python path ──
sys.path.insert(0, r"C:\ACC\apps\api")
os.chdir(r"C:\ACC\apps\api")
from dotenv import load_dotenv

load_dotenv("C:/ACC/.env")

# ── Paths ──
STATUS_FILE = Path("C:/ACC/backfill_status.json")
LOG_FILE = Path("C:/ACC/backfill_safe_log.txt")
HEARTBEAT_FILE = Path("C:/ACC/backfill_heartbeat.txt")

# ── Configuration ──
DAYS_BACK = 435  # Jan 1 2025 → now
MAX_RETRIES = 3
RETRY_BASE_WAIT_S = 60  # 60s, 120s, 240s
WATCHDOG_INTERVAL_S = 30
STUCK_THRESHOLD_S = 900  # 15 min without heartbeat → warning
MONITOR_INTERVAL_S = 60  # query DB for row count every 60s


# ═══════════════════════════════════════════════════════════════════
# Logging
# ═══════════════════════════════════════════════════════════════════
_log_lock = threading.Lock()


def log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with _log_lock:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")


# ═══════════════════════════════════════════════════════════════════
# Status file (queryable progress)
# ═══════════════════════════════════════════════════════════════════
def save_status(data: dict):
    data["updated_at"] = datetime.now(timezone.utc).isoformat()
    tmp = STATUS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    tmp.replace(STATUS_FILE)


# ═══════════════════════════════════════════════════════════════════
# Watchdog + DB monitor thread
# ═══════════════════════════════════════════════════════════════════
class Watchdog:
    def __init__(self):
        self._last_beat = time.time()
        self._running = True
        self._task = ""
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="watchdog")
        self._status = {
            "state": "starting",
            "attempt": 0,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "db_rows_start": 0,
            "db_rows_current": 0,
            "db_rows_new": 0,
            "db_monthly": {},
        }

    def start(self, initial_row_count: int):
        self._status["db_rows_start"] = initial_row_count
        self._status["db_rows_current"] = initial_row_count
        self._thread.start()
        log("Watchdog + DB monitor started")

    def stop(self):
        self._running = False
        self._status["state"] = "stopped"
        save_status(self._status)

    def heartbeat(self, task: str = ""):
        with self._lock:
            self._last_beat = time.time()
            self._task = task

    def set_state(self, state: str, attempt: int = 0):
        self._status["state"] = state
        self._status["attempt"] = attempt
        save_status(self._status)

    def _query_db_progress(self):
        """Query actual DB row counts — runs in watchdog thread."""
        try:
            from app.core.db_connection import connect_acc
            conn = connect_acc(autocommit=True, timeout=10)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM acc_finance_transaction WITH (NOLOCK)")
            total = cur.fetchone()[0]
            cur.execute("""
                SELECT CONVERT(VARCHAR(7), posted_date, 120) m, COUNT(*) c
                FROM acc_finance_transaction WITH (NOLOCK)
                GROUP BY CONVERT(VARCHAR(7), posted_date, 120)
                ORDER BY m
            """)
            monthly = {str(r[0]): int(r[1]) for r in cur.fetchall()}
            cur.close()
            conn.close()
            return total, monthly
        except Exception:
            return None, None

    def _loop(self):
        last_db_check = 0
        while self._running:
            time.sleep(WATCHDOG_INTERVAL_S)
            now = time.time()
            with self._lock:
                elapsed = now - self._last_beat
                task = self._task

            # Heartbeat file for external monitoring
            ts = time.strftime("%H:%M:%S")
            hb = f"[{ts}] heartbeat={elapsed:.0f}s ago | task={task}"
            try:
                HEARTBEAT_FILE.write_text(hb, encoding="utf-8")
            except OSError:
                pass

            if elapsed > STUCK_THRESHOLD_S:
                log(f"WATCHDOG WARNING: no heartbeat for {elapsed:.0f}s! task={task}")

            # Periodic DB progress check
            if now - last_db_check >= MONITOR_INTERVAL_S:
                last_db_check = now
                total, monthly = self._query_db_progress()
                if total is not None:
                    new_rows = total - self._status["db_rows_start"]
                    self._status["db_rows_current"] = total
                    self._status["db_rows_new"] = new_rows
                    self._status["db_monthly"] = monthly
                    save_status(self._status)
                    log(f"  DB MONITOR: {total:,} rows total (+{new_rows:,} new) | months with data: {len(monthly)}")


# ═══════════════════════════════════════════════════════════════════
# Backfill with retry
# ═══════════════════════════════════════════════════════════════════
async def run_backfill():
    from app.core.db_connection import connect_acc
    from app.services.order_pipeline import step_sync_finances

    # Get initial state
    conn = connect_acc(autocommit=True, timeout=15)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM acc_finance_transaction WITH (NOLOCK)")
    initial_rows = cur.fetchone()[0]
    cur.execute("""
        SELECT COUNT(*) FROM acc_fin_event_group_sync WITH (NOLOCK)
        WHERE processing_status = 'Closed'
          AND fund_transfer_status IN ('Succeeded', 'Transferred')
    """)
    terminal_groups = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM acc_fin_event_group_sync WITH (NOLOCK)")
    total_groups = cur.fetchone()[0]
    cur.close()
    conn.close()

    log("=" * 60)
    log("SAFE FINANCE API BACKFILL")
    log("=" * 60)
    log(f"days_back={DAYS_BACK} (~{DAYS_BACK // 30} months)")
    log(f"DB: {initial_rows:,} rows currently")
    log(f"Groups tracked: {total_groups} ({terminal_groups} terminal = will be skipped)")
    log(f"Max retries: {MAX_RETRIES}")
    log(f"Log: {LOG_FILE}")
    log(f"Status: {STATUS_FILE}")
    log("")

    watchdog = Watchdog()
    watchdog.start(initial_rows)

    for attempt in range(1, MAX_RETRIES + 1):
        log(f"--- Attempt {attempt}/{MAX_RETRIES} ---")
        watchdog.set_state("running", attempt)
        watchdog.heartbeat(f"step_sync_finances attempt {attempt}")

        try:
            t0 = time.time()
            result = await step_sync_finances(days_back=DAYS_BACK)
            elapsed = time.time() - t0

            log(f"step_sync_finances completed in {elapsed:.0f}s")
            log(f"Result: {json.dumps(result, default=str)}")
            watchdog.set_state("completed", attempt)
            watchdog.heartbeat("done")

            # Final verification
            conn = connect_acc(autocommit=True, timeout=15)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM acc_finance_transaction WITH (NOLOCK)")
            final_rows = cur.fetchone()[0]
            cur.execute("""
                SELECT CONVERT(VARCHAR(7), posted_date, 120) m, COUNT(*) c
                FROM acc_finance_transaction WITH (NOLOCK)
                GROUP BY CONVERT(VARCHAR(7), posted_date, 120)
                ORDER BY m
            """)
            monthly = cur.fetchall()
            cur.close()
            conn.close()

            log("")
            log("=== VERIFICATION ===")
            log(f"Total rows: {initial_rows:,} -> {final_rows:,} (+{final_rows - initial_rows:,})")
            log(f"Monthly breakdown:")
            for m in monthly:
                log(f"  {m[0]}: {m[1]:,} rows")
            log("=== BACKFILL COMPLETE ===")

            watchdog.stop()
            return result

        except Exception as e:
            elapsed = time.time() - t0 if 't0' in dir() else 0
            log(f"ATTEMPT {attempt} FAILED after {elapsed:.0f}s: {type(e).__name__}: {e}")
            log(traceback.format_exc())
            watchdog.set_state("retrying", attempt)

            if attempt < MAX_RETRIES:
                wait = RETRY_BASE_WAIT_S * (2 ** (attempt - 1))
                log(f"Waiting {wait}s before retry...")
                log("(already-synced groups will be skipped on restart)")
                await asyncio.sleep(wait)
            else:
                log(f"ALL {MAX_RETRIES} ATTEMPTS FAILED")
                watchdog.set_state("failed", attempt)

                # Show what we got so far
                try:
                    conn = connect_acc(autocommit=True, timeout=15)
                    cur = conn.cursor()
                    cur.execute("SELECT COUNT(*) FROM acc_finance_transaction WITH (NOLOCK)")
                    current = cur.fetchone()[0]
                    cur.close()
                    conn.close()
                    log(f"Rows saved despite failure: {initial_rows:,} -> {current:,} (+{current - initial_rows:,})")
                    log("Run with --resume to continue from where we left off")
                except Exception:
                    pass

    watchdog.stop()
    return None


# ═══════════════════════════════════════════════════════════════════
# Status display (--status)
# ═══════════════════════════════════════════════════════════════════
def show_status():
    from app.core.db_connection import connect_acc

    # Status file info
    if STATUS_FILE.exists():
        try:
            s = json.loads(STATUS_FILE.read_text(encoding="utf-8"))
            print(f"=== Backfill Status (from status file) ===")
            print(f"State:      {s.get('state', '?')}")
            print(f"Attempt:    {s.get('attempt', '?')}")
            print(f"Started:    {s.get('started_at', '?')}")
            print(f"Updated:    {s.get('updated_at', '?')}")
            print(f"Rows start: {s.get('db_rows_start', 0):,}")
            print(f"Rows now:   {s.get('db_rows_current', 0):,}")
            print(f"New rows:   {s.get('db_rows_new', 0):,}")
            print()
        except Exception:
            print("Status file unreadable")
    else:
        print("No status file found (backfill not started yet?)")

    # Live DB query
    print("=== Live DB State ===")
    conn = connect_acc(autocommit=True, timeout=15)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM acc_finance_transaction WITH (NOLOCK)")
    print(f"Total rows: {cur.fetchone()[0]:,}")

    cur.execute("""
        SELECT CONVERT(VARCHAR(7), posted_date, 120) m, COUNT(*) c
        FROM acc_finance_transaction WITH (NOLOCK)
        GROUP BY CONVERT(VARCHAR(7), posted_date, 120)
        ORDER BY m
    """)
    print("Monthly:")
    for r in cur.fetchall():
        print(f"  {r[0]}: {r[1]:,}")

    cur.execute("""
        SELECT COUNT(*) total,
               SUM(CASE WHEN processing_status='Closed'
                         AND fund_transfer_status IN ('Succeeded','Transferred') THEN 1 ELSE 0 END) terminal
        FROM acc_fin_event_group_sync WITH (NOLOCK)
    """)
    r = cur.fetchone()
    print(f"\nEvent groups: {r[0]} tracked, {r[1]} terminal (will skip on resume)")

    cur.close()
    conn.close()

    # Heartbeat
    if HEARTBEAT_FILE.exists():
        print(f"\nLast heartbeat: {HEARTBEAT_FILE.read_text()}")


# ═══════════════════════════════════════════════════════════════════
# Entrypoint
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    if "--status" in sys.argv:
        show_status()
    elif "--dry-run" in sys.argv:
        log(f"DRY RUN: would call step_sync_finances(days_back={DAYS_BACK})")
        log(f"  Covers ~{DAYS_BACK} days = {DAYS_BACK // 180 + 1} API windows of 180d")
        log(f"  Terminal groups in DB will be skipped (natural resume)")
        show_status()
    else:
        asyncio.run(run_backfill())
