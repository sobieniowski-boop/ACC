from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.core.db_connection import connect_netfox


FORBIDDEN_SQL_TOKENS = (
    "INSERT ",
    "UPDATE ",
    "DELETE ",
    "MERGE ",
    "ALTER ",
    "DROP ",
    "TRUNCATE ",
    "CREATE ",
    "EXEC ",
    "EXECUTE ",
)


@dataclass
class ProbeResult:
    attempt: int
    ok: bool
    connect_ms: float | None
    query_ms: float | None
    error: str | None


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _run_readonly_query(
    *,
    sql: str,
    params: list[Any] | tuple[Any, ...] | None = None,
    timeout_sec: int = 8,
) -> list[tuple[Any, ...]]:
    sql_upper = sql.upper()
    for token in FORBIDDEN_SQL_TOKENS:
        if token in sql_upper:
            raise RuntimeError(f"Unsafe SQL token detected: {token.strip()}")
    conn = connect_netfox(autocommit=True, timeout=timeout_sec)
    try:
        cur = conn.cursor()
        cur.execute(sql, params or [])
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        conn.close()


def _probe(*, attempts: int, pause_sec: float, timeout_sec: int) -> dict[str, Any]:
    results: list[ProbeResult] = []
    for i in range(1, attempts + 1):
        t0 = time.perf_counter()
        connect_ms: float | None = None
        query_ms: float | None = None
        err: str | None = None
        ok = False
        try:
            conn = connect_netfox(autocommit=True, timeout=timeout_sec)
            connect_ms = round((time.perf_counter() - t0) * 1000, 1)
            cur = conn.cursor()
            t1 = time.perf_counter()
            cur.execute("SELECT 1")
            cur.fetchone()
            query_ms = round((time.perf_counter() - t1) * 1000, 1)
            cur.close()
            conn.close()
            ok = True
        except Exception as exc:  # pragma: no cover - runtime safety path
            err = f"{type(exc).__name__}: {exc}"
        results.append(
            ProbeResult(
                attempt=i,
                ok=ok,
                connect_ms=connect_ms,
                query_ms=query_ms,
                error=err,
            )
        )
        if i < attempts:
            time.sleep(pause_sec)
    ok_count = sum(1 for item in results if item.ok)
    return {
        "mode": "probe",
        "ts_utc": _utc_now(),
        "attempts": attempts,
        "ok": ok_count,
        "fail": attempts - ok_count,
        "results": [asdict(item) for item in results],
    }


def _load_tokens(path: Path, *, max_tokens: int) -> list[str]:
    raw = [line.strip().upper() for line in path.read_text(encoding="utf-8").splitlines()]
    values = [v for v in raw if v]
    dedup = sorted(set(values))
    return dedup[:max_tokens]


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _lookup_delivery_package_nr(
    *,
    tokens_file: Path,
    max_tokens: int,
    batch_size: int,
    sleep_ms: int,
    timeout_sec: int,
    max_timeouts: int,
) -> dict[str, Any]:
    tokens = _load_tokens(tokens_file, max_tokens=max_tokens)
    if not tokens:
        return {
            "mode": "lookup_delivery_package_nr",
            "ts_utc": _utc_now(),
            "tokens_loaded": 0,
            "batches_run": 0,
            "timeouts": 0,
            "matches": [],
        }

    sleep_sec = max(0, sleep_ms) / 1000.0
    timeouts = 0
    batches_run = 0
    matches: dict[str, int] = {}
    batch_logs: list[dict[str, Any]] = []

    for batch in _chunks(tokens, batch_size):
        batches_run += 1
        started = time.perf_counter()
        placeholders = ",".join("?" for _ in batch)
        sql = f"""
WITH ranked AS (
    SELECT
        UPPER(LTRIM(RTRIM(delivery_package_nr))) AS tracking_number,
        order_id,
        ROW_NUMBER() OVER (
            PARTITION BY UPPER(LTRIM(RTRIM(delivery_package_nr)))
            ORDER BY ISNULL(date_confirmed, 0) DESC, order_id DESC
        ) AS rn
    FROM dbo.ITJK_ZamowieniaBaselinkerAPI WITH (NOLOCK)
    WHERE delivery_package_nr IN ({placeholders})
)
SELECT tracking_number, order_id
FROM ranked
WHERE rn = 1
"""
        try:
            rows = _run_readonly_query(sql=sql, params=batch, timeout_sec=timeout_sec)
            for row in rows:
                if not row or row[0] is None or row[1] is None:
                    continue
                matches[str(row[0]).strip().upper()] = int(row[1])
            batch_logs.append(
                {
                    "batch_index": batches_run,
                    "batch_size": len(batch),
                    "ok": True,
                    "elapsed_ms": round((time.perf_counter() - started) * 1000, 1),
                    "matched_so_far": len(matches),
                }
            )
        except Exception as exc:  # pragma: no cover - runtime safety path
            timeouts += 1
            batch_logs.append(
                {
                    "batch_index": batches_run,
                    "batch_size": len(batch),
                    "ok": False,
                    "elapsed_ms": round((time.perf_counter() - started) * 1000, 1),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            if timeouts >= max_timeouts:
                break
        time.sleep(sleep_sec)

    return {
        "mode": "lookup_delivery_package_nr",
        "ts_utc": _utc_now(),
        "tokens_file": str(tokens_file),
        "tokens_loaded": len(tokens),
        "batch_size": batch_size,
        "batches_run": batches_run,
        "timeouts": timeouts,
        "max_timeouts": max_timeouts,
        "matched_count": len(matches),
        "matches": [{"tracking_number": k, "order_id": v} for k, v in sorted(matches.items())],
        "batch_logs": batch_logs,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Safe Netfox read-only runner with strict limits.")
    parser.add_argument("--mode", choices=["probe", "lookup_delivery_package_nr"], required=True)
    parser.add_argument("--out-json", type=Path, default=None)

    parser.add_argument("--attempts", type=int, default=6, help="Probe attempts.")
    parser.add_argument("--pause-sec", type=float, default=2.0, help="Pause between probe attempts.")

    parser.add_argument("--tokens-file", type=Path, default=None, help="One token per line.")
    parser.add_argument("--max-tokens", type=int, default=500)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--sleep-ms", type=int, default=150)
    parser.add_argument("--timeout-sec", type=int, default=8)
    parser.add_argument("--max-timeouts", type=int, default=2)
    args = parser.parse_args()

    if args.batch_size < 1 or args.batch_size > 100:
        raise SystemExit("--batch-size must be in range 1..100")
    if args.max_tokens < 1 or args.max_tokens > 5000:
        raise SystemExit("--max-tokens must be in range 1..5000")
    if args.max_timeouts < 1 or args.max_timeouts > 10:
        raise SystemExit("--max-timeouts must be in range 1..10")

    if args.mode == "probe":
        payload = _probe(attempts=args.attempts, pause_sec=args.pause_sec, timeout_sec=args.timeout_sec)
    else:
        if args.tokens_file is None:
            raise SystemExit("--tokens-file is required for lookup_delivery_package_nr mode")
        if not args.tokens_file.exists():
            raise SystemExit(f"tokens file not found: {args.tokens_file}")
        payload = _lookup_delivery_package_nr(
            tokens_file=args.tokens_file,
            max_tokens=args.max_tokens,
            batch_size=args.batch_size,
            sleep_ms=args.sleep_ms,
            timeout_sec=args.timeout_sec,
            max_timeouts=args.max_timeouts,
        )

    text = json.dumps(payload, ensure_ascii=True, indent=2)
    print(text)
    if args.out_json:
        args.out_json.write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
