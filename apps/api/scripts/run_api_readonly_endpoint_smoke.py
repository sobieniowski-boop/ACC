from __future__ import annotations

import asyncio
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.security import get_current_user
from app.main import app


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_json(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return {"raw": (resp.text or "")[:1000]}


async def _call(
    client: httpx.AsyncClient,
    method: str,
    path: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    resp = await client.request(method, path, params=params)
    elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
    payload = _safe_json(resp)
    status = "ok" if resp.status_code < 400 else "error"
    return {
        "endpoint": f"{method} {path}",
        "params": params or {},
        "http_status": resp.status_code,
        "status": status,
        "elapsed_ms": elapsed_ms,
        "payload_preview": payload,
    }


async def main() -> None:
    today = date.today()
    date_from = (today - timedelta(days=30)).isoformat()
    date_to = today.isoformat()

    app.dependency_overrides[get_current_user] = lambda: {
        "user_id": "smoke-readonly",
        "role": "admin",
        "allowed_marketplaces": [],
        "allowed_brands": [],
    }

    endpoints: list[tuple[str, str, dict[str, Any] | None]] = [
        ("GET", "/api/v1/health/order-sync", None),
        ("GET", "/api/v1/executive/overview", {"from": date_from, "to": date_to}),
        ("GET", "/api/v1/executive/products", {"from": date_from, "to": date_to, "page": 1, "page_size": 20}),
        ("GET", "/api/v1/executive/marketplaces", {"from": date_from, "to": date_to}),
        ("GET", "/api/v1/strategy/overview", None),
        ("GET", "/api/v1/strategy/opportunities", {"page": 1, "page_size": 20}),
        ("GET", "/api/v1/profitability/overview", {"from": date_from, "to": date_to}),
        ("GET", "/api/v1/profitability/orders", {"from": date_from, "to": date_to, "page": 1, "page_size": 20}),
        ("GET", "/api/v1/profitability/products", {"from": date_from, "to": date_to, "page": 1, "page_size": 20}),
        ("GET", "/api/v1/inventory/overview", None),
        ("GET", "/api/v1/inventory/all", {"risk_type": "all"}),
        ("GET", "/api/v1/inventory/families", {"limit": 100}),
        ("GET", "/api/v1/inventory/jobs", None),
        ("GET", "/api/v1/inventory/settings", None),
        ("GET", "/api/v1/finance/dashboard", {"from": date_from, "to": date_to}),
        ("GET", "/api/v1/finance/sync/diagnostics", {"limit": 12}),
        ("GET", "/api/v1/finance/sync/completeness", {"days_back": 30}),
        ("GET", "/api/v1/finance/sync/gap-diagnostics", {"days_back": 30}),
        ("GET", "/api/v1/fba/overview", None),
        ("GET", "/api/v1/fba/inventory", None),
        ("GET", "/api/v1/fba/inbound/shipments", None),
        ("GET", "/api/v1/fba/replenishment/suggestions", None),
        ("GET", "/api/v1/fba/aged", None),
        ("GET", "/api/v1/fba/stranded", None),
        ("GET", "/api/v1/courier/readiness", None),
    ]

    results: list[dict[str, Any]] = []
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://acc.local", timeout=900.0) as client:
        for method, path, params in endpoints:
            results.append(await _call(client, method, path, params))

    p1 = []
    p2 = []
    for row in results:
        if row["http_status"] >= 500:
            p1.append(
                {
                    "endpoint": row["endpoint"],
                    "http_status": row["http_status"],
                    "detail": row["payload_preview"],
                }
            )
            continue
        if row["http_status"] >= 400:
            p2.append(
                {
                    "endpoint": row["endpoint"],
                    "http_status": row["http_status"],
                    "detail": row["payload_preview"],
                }
            )
            continue
        if row["elapsed_ms"] > 60_000:
            p1.append(
                {
                    "endpoint": row["endpoint"],
                    "http_status": row["http_status"],
                    "elapsed_ms": row["elapsed_ms"],
                    "detail": "very_slow_gt_60s",
                }
            )
        elif row["elapsed_ms"] > 10_000:
            p2.append(
                {
                    "endpoint": row["endpoint"],
                    "http_status": row["http_status"],
                    "elapsed_ms": row["elapsed_ms"],
                    "detail": "slow_gt_10s",
                }
            )

    report = {
        "generated_at": _now_iso(),
        "totals": {
            "checked": len(results),
            "ok": sum(1 for r in results if r["http_status"] < 400),
            "errors": sum(1 for r in results if r["http_status"] >= 400),
        },
        "p1": p1,
        "p2": p2,
        "results": sorted(results, key=lambda x: x["elapsed_ms"], reverse=True),
    }

    out = Path(__file__).resolve().parent / f"api_readonly_smoke_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8")
    print(str(out))
    print(json.dumps({"p1": len(p1), "p2": len(p2), **report["totals"]}, ensure_ascii=True))


if __name__ == "__main__":
    asyncio.run(main())
