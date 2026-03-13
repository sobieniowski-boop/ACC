from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path


def _default_log_paths(prefix: str) -> tuple[Path, Path]:
    ts = time.strftime("%Y%m%d_%H%M%S")
    log_dir = Path("C:/ACC/logs")
    return (
        log_dir / f"{prefix}_{ts}.log",
        log_dir / f"{prefix}_{ts}.err.log",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Launch DHL seed pipeline in background.")
    parser.add_argument("--log-prefix", default="dhl_seed_pipeline")
    parser.add_argument("pipeline_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    log_path, err_path = _default_log_paths(args.log_prefix)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    script_path = Path("C:/ACC/apps/api/scripts/run_dhl_seed_pipeline.py")
    python_exe = Path("C:/ACC/.venv/Scripts/python.exe")

    env = os.environ.copy()
    env["PYTHONPATH"] = "C:/ACC/apps/api"

    pipeline_args = list(args.pipeline_args)
    if pipeline_args and pipeline_args[0] == "--":
        pipeline_args = pipeline_args[1:]

    cmd = [str(python_exe), "-u", str(script_path)] + pipeline_args

    with log_path.open("w", encoding="utf-8") as stdout_handle, err_path.open("w", encoding="utf-8") as stderr_handle:
        creationflags = 0
        if os.name == "nt":
            creationflags = (
                getattr(subprocess, "DETACHED_PROCESS", 0)
                | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
                | getattr(subprocess, "CREATE_NO_WINDOW", 0)
            )
        proc = subprocess.Popen(
            cmd,
            cwd="C:/ACC/apps/api",
            env=env,
            stdout=stdout_handle,
            stderr=stderr_handle,
            creationflags=creationflags,
        )

    print(
        json.dumps(
            {
                "pid": proc.pid,
                "log": str(log_path),
                "err_log": str(err_path),
                "cmd": cmd,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
