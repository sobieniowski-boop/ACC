from __future__ import annotations

import argparse
import json
import statistics
import time
import urllib.error
import urllib.request


def _sample_once(url: str, timeout_sec: int) -> tuple[float, int, str]:
    started = time.perf_counter()
    req = urllib.request.Request(url=url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            body = resp.read(256)
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            return elapsed_ms, int(resp.getcode() or 0), body.decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as exc:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        return elapsed_ms, int(exc.code or 0), str(exc)
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        return elapsed_ms, 0, str(exc)


def _pctl(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((p / 100.0) * (len(ordered) - 1)))))
    return round(float(ordered[idx]), 2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure API p50/p95 latency for selected endpoints.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--samples", type=int, default=30)
    parser.add_argument("--interval-ms", type=int, default=300)
    parser.add_argument("--timeout-sec", type=int, default=10)
    parser.add_argument(
        "--paths",
        nargs="+",
        default=["/api/v1/health", "/docs", "/api/v1/jobs?page=1&page_size=20"],
    )
    args = parser.parse_args()

    report: dict[str, dict] = {}
    for path in args.paths:
        url = f"{args.base_url.rstrip('/')}{path}"
        latencies: list[float] = []
        statuses: dict[str, int] = {}
        failures = 0
        for _ in range(max(1, int(args.samples))):
            elapsed_ms, status_code, _payload = _sample_once(url, max(1, int(args.timeout_sec)))
            latencies.append(elapsed_ms)
            key = str(status_code)
            statuses[key] = int(statuses.get(key, 0)) + 1
            if status_code == 0 or status_code >= 500:
                failures += 1
            time.sleep(max(0, int(args.interval_ms)) / 1000.0)
        report[path] = {
            "samples": len(latencies),
            "p50_ms": _pctl(latencies, 50.0),
            "p95_ms": _pctl(latencies, 95.0),
            "avg_ms": round(statistics.mean(latencies), 2) if latencies else 0.0,
            "max_ms": round(max(latencies), 2) if latencies else 0.0,
            "statuses": statuses,
            "failures": failures,
        }

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
