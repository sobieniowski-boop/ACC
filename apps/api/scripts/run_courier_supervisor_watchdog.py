from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _build_supervisor_cmd(args: argparse.Namespace) -> list[str]:
    script_path = Path(__file__).resolve().with_name("run_courier_order_universe_supervisor.py")
    cmd: list[str] = [
        sys.executable,
        str(script_path),
        "--months",
        *args.months,
        "--carriers",
        *args.carriers,
        "--limit-orders",
        str(args.limit_orders),
        "--created-to-buffer-days",
        str(args.created_to_buffer_days),
        "--stale-timeout-sec",
        str(args.stale_timeout_sec),
        "--hard-timeout-sec",
        str(args.hard_timeout_sec),
        "--transient-retries",
        str(args.transient_retries),
        "--checkpoint-file",
        args.checkpoint_file,
    ]
    if args.stop_on_failure:
        cmd.append("--stop-on-failure")
    return cmd


def _stream_process(proc: subprocess.Popen, log_file: Path) -> int:
    with log_file.open("a", encoding="utf-8", errors="replace") as fh:
        while True:
            line = proc.stdout.readline()
            if not line:
                if proc.poll() is not None:
                    break
                time.sleep(0.2)
                continue
            rendered = line.rstrip("\n")
            print(rendered, flush=True)
            fh.write(rendered + "\n")
            fh.flush()
    return int(proc.wait())


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Watchdog wrapper for courier order-universe supervisor with live logs and auto-restart."
    )
    parser.add_argument("--months", nargs="+", required=True)
    parser.add_argument("--carriers", nargs="+", default=["DHL", "GLS"])
    parser.add_argument("--limit-orders", type=int, default=3_000_000)
    parser.add_argument("--created-to-buffer-days", type=int, default=31)
    parser.add_argument("--stale-timeout-sec", type=int, default=900)
    parser.add_argument("--hard-timeout-sec", type=int, default=7200)
    parser.add_argument("--transient-retries", type=int, default=2)
    parser.add_argument(
        "--checkpoint-file",
        default="C:/ACC/apps/api/scripts/courier_order_universe_supervisor_checkpoint.json",
    )
    parser.add_argument(
        "--log-file",
        default="C:/ACC/apps/api/scripts/courier_supervisor_watchdog.log",
    )
    parser.add_argument("--max-restarts", type=int, default=5)
    parser.add_argument("--restart-delay-sec", type=int, default=20)
    parser.add_argument("--stop-on-failure", action="store_true")
    args = parser.parse_args()

    log_file = Path(args.log_file)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    attempts = 0
    max_attempts = max(1, int(args.max_restarts) + 1)
    cmd = _build_supervisor_cmd(args)

    while attempts < max_attempts:
        attempts += 1
        banner = (
            f"[{_ts()}] WATCHDOG_START attempt={attempts}/{max_attempts} "
            f"cmd={shlex.join(cmd)}"
        )
        print(banner, flush=True)
        with log_file.open("a", encoding="utf-8", errors="replace") as fh:
            fh.write(banner + "\n")

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            env=os.environ.copy(),
        )
        rc = _stream_process(proc, log_file)

        end_msg = f"[{_ts()}] WATCHDOG_END attempt={attempts}/{max_attempts} exit_code={rc}"
        print(end_msg, flush=True)
        with log_file.open("a", encoding="utf-8", errors="replace") as fh:
            fh.write(end_msg + "\n")

        if rc == 0:
            return 0
        if attempts >= max_attempts:
            break

        sleep_msg = (
            f"[{_ts()}] WATCHDOG_RESTART in {max(1, int(args.restart_delay_sec))}s "
            f"after non-zero exit"
        )
        print(sleep_msg, flush=True)
        with log_file.open("a", encoding="utf-8", errors="replace") as fh:
            fh.write(sleep_msg + "\n")
        time.sleep(max(1, int(args.restart_delay_sec)))

    fail_msg = f"[{_ts()}] WATCHDOG_GIVEUP attempts={attempts} max_restarts={args.max_restarts}"
    print(fail_msg, flush=True)
    with log_file.open("a", encoding="utf-8", errors="replace") as fh:
        fh.write(fail_msg + "\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
