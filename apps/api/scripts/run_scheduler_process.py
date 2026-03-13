from __future__ import annotations

import asyncio
import signal

from app.scheduler import start_scheduler, stop_scheduler


_STOP = False


def _handle_stop(signum, frame):  # type: ignore[no-untyped-def]
    del signum, frame
    global _STOP
    _STOP = True


async def _run_forever() -> None:
    while not _STOP:
        await asyncio.sleep(1.0)


async def _main_async() -> int:
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)
    start_scheduler()
    print("scheduler_process_started", flush=True)
    try:
        await _run_forever()
    finally:
        stop_scheduler()
    print("scheduler_process_stopped", flush=True)
    return 0


def main() -> int:
    return asyncio.run(_main_async())


if __name__ == "__main__":
    raise SystemExit(main())
