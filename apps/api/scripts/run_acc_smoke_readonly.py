from __future__ import annotations

import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.api.v1.routes_health import _check_order_sync
from app.services.courier_readiness import get_courier_readiness_snapshot
from app.services.executive_service import get_exec_marketplaces, get_exec_overview, get_exec_products
from app.services.fba_ops.inventory import get_overview as get_fba_overview
from app.services.finance_center.service import get_finance_dashboard
from app.services.manage_inventory import get_inventory_overview
from app.services.profitability_service import (
    get_profitability_orders,
    get_profitability_overview,
    get_profitability_products,
)
from app.services.strategy_service import get_opportunities_page, get_strategy_overview


def _iso(v):
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _run_check(name, fn):
    t0 = time.perf_counter()
    try:
        out = fn()
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
        return {
            "name": name,
            "status": "ok",
            "elapsed_ms": elapsed_ms,
            "meta": {
                "type": type(out).__name__,
                "has_kpi": bool(isinstance(out, dict) and out.get("kpi")),
                "items": len(out.get("items", [])) if isinstance(out, dict) and isinstance(out.get("items"), list) else None,
                "total": out.get("total") if isinstance(out, dict) else None,
            },
        }
    except Exception as exc:
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
        return {
            "name": name,
            "status": "error",
            "elapsed_ms": elapsed_ms,
            "error": str(exc),
        }


def main():
    date_from = date.today() - timedelta(days=30)
    date_to = date.today()

    checks = [
        ("health.order_sync", _check_order_sync),
        ("executive.overview", lambda: get_exec_overview(date_from, date_to, None)),
        ("executive.products", lambda: get_exec_products(date_from, date_to, None, None, "profit_pln", "desc", 1, 20)),
        ("executive.marketplaces", lambda: get_exec_marketplaces(date_from, date_to)),
        ("strategy.overview", get_strategy_overview),
        ("strategy.opportunities", lambda: get_opportunities_page(page=1, page_size=20)),
        ("profitability.overview", lambda: get_profitability_overview(date_from, date_to, None)),
        ("profitability.orders", lambda: get_profitability_orders(date_from, date_to, None, None, False, None, None, 1, 20)),
        ("profitability.products", lambda: get_profitability_products(date_from, date_to, None, None, "profit_pln", "desc", 1, 20)),
        ("inventory.overview", lambda: get_inventory_overview(marketplace_ids=None)),
        ("finance.dashboard", lambda: get_finance_dashboard(date_from=date_from, date_to=date_to)),
        ("fba.overview", get_fba_overview),
        ("courier.readiness", get_courier_readiness_snapshot),
    ]

    results = [_run_check(name, fn) for name, fn in checks]
    summary = {
        "ok": sum(1 for r in results if r["status"] == "ok"),
        "error": sum(1 for r in results if r["status"] != "ok"),
    }
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "results": results,
    }
    out = Path(__file__).resolve().parent / f"acc_smoke_readonly_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(report, ensure_ascii=True, indent=2, default=_iso), encoding="utf-8")
    print(str(out))
    print(json.dumps(summary, ensure_ascii=True))


if __name__ == "__main__":
    main()
