"""Launch backfill_orders.py as a detached background process on Windows.

Run from apps/api/:
    python scripts/launch_backfill.py
    python scripts/launch_backfill.py --resume          # continue from checkpoint
    python scripts/launch_backfill.py --status           # show current progress

Then monitor:
    type backfill.log             # full log
    type backfill_progress.json   # machine-readable progress
"""
import subprocess
import sys
import os
import json
from pathlib import Path

BASE = Path(__file__).parent.parent  # apps/api/
PROGRESS = BASE / "backfill_progress.json"
CHECKPOINT = BASE / "backfill_checkpoint.json"


def show_status():
    if PROGRESS.exists():
        data = json.loads(PROGRESS.read_text(encoding="utf-8"))
        print("=== BACKFILL PROGRESS ===")
        for k, v in data.items():
            print(f"  {k:25s}: {v}")
    else:
        print("No progress file found. Backfill not yet started.")

    if CHECKPOINT.exists():
        data = json.loads(CHECKPOINT.read_text(encoding="utf-8"))
        done = len(data.get("completed", []))
        print(f"\n  Checkpoint: {done} windows completed")
        print(f"  Orders total: {data.get('stats', {}).get('orders_total', '?')}")

    # Check if process is running
    result = subprocess.run(
        ["tasklist", "/FI", "IMAGENAME eq python.exe", "/FO", "CSV"],
        capture_output=True, text=True
    )
    python_procs = [l for l in result.stdout.split("\n") if "python" in l.lower()]
    print(f"\n  Python processes running: {len(python_procs)}")


def launch(resume: bool = False):
    script = str(BASE / "scripts" / "backfill_orders.py")
    args = [sys.executable, script]
    if resume:
        args.append("--resume")

    log_out = open(BASE / "backfill_stdout.log", "w", encoding="utf-8")
    log_err = open(BASE / "backfill_stderr.log", "w", encoding="utf-8")

    # DETACHED_PROCESS = 0x00000008 — survives parent exit
    # CREATE_NO_WINDOW = 0x08000000 — no console window
    proc = subprocess.Popen(
        args,
        cwd=str(BASE),
        stdout=log_out,
        stderr=log_err,
        creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NO_WINDOW,
    )
    print(f"Backfill launched! PID={proc.pid}")
    print(f"  Resume mode: {resume}")
    print(f"  Log file:    {BASE / 'backfill.log'}")
    print(f"  Progress:    {PROGRESS}")
    print(f"  Checkpoint:  {CHECKPOINT}")
    print(f"\nMonitor with:")
    print(f"  python scripts/launch_backfill.py --status")
    print(f"  type backfill.log")
    print(f"  Get-Content backfill.log -Wait -Tail 20")


if __name__ == "__main__":
    if "--status" in sys.argv:
        show_status()
    else:
        resume = "--resume" in sys.argv
        launch(resume=resume)
