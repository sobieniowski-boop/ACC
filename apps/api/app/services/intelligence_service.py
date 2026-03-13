"""Intelligence Service — unified aggregator across all intelligence modules."""
from __future__ import annotations

import asyncio
from datetime import date, timedelta
from typing import Any

import structlog
from fastapi.concurrency import run_in_threadpool

log = structlog.get_logger(__name__)


async def _safe_call(name: str, fn, *args, **kwargs) -> tuple[str, Any]:
    """Run a blocking function in threadpool; return (name, result) or (name, error dict)."""
    try:
        result = await run_in_threadpool(fn, *args, **kwargs)
        return name, result
    except Exception as exc:
        log.warning("intelligence.module_error", module=name, error=str(exc))
        return name, {"error": str(exc)}


async def get_unified_dashboard(
    date_from: date | None = None,
    date_to: date | None = None,
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    """Aggregate all intelligence module dashboards into one response.

    Each module is called concurrently. If a module fails, its section
    returns ``{"error": "..."}`` instead of breaking the whole response.
    """
    if date_from is None:
        date_from = date.today() - timedelta(days=30)
    if date_to is None:
        date_to = date.today()

    # Lazy imports to avoid circular deps and keep startup fast
    from app.services.executive_service import get_exec_overview
    from app.services.strategy_service import get_strategy_overview
    from app.services.decision_intelligence_service import get_learning_dashboard
    from app.services.seasonality_service import get_overview as get_seasonality_overview
    from app.intelligence.inventory_risk import get_risk_dashboard
    from app.intelligence.refund_anomaly import get_anomaly_dashboard
    from app.intelligence.buybox_radar import get_buybox_dashboard
    from app.intelligence.content_optimization import get_score_distribution
    from app.intelligence.repricing_engine import get_repricing_dashboard

    tasks = [
        _safe_call("executive", get_exec_overview, date_from=date_from, date_to=date_to, marketplace_id=marketplace_id),
        _safe_call("strategy", get_strategy_overview),
        _safe_call("learning", get_learning_dashboard),
        _safe_call("seasonality", get_seasonality_overview, marketplace=marketplace_id),
        _safe_call("inventory_risk", get_risk_dashboard, marketplace_id, days=1),
        _safe_call("refund_anomaly", get_anomaly_dashboard),
        _safe_call("buybox", get_buybox_dashboard, marketplace_id, days=7),
        _safe_call("content", get_score_distribution, marketplace_id),
        _safe_call("repricing", get_repricing_dashboard, marketplace_id),
    ]

    results = await asyncio.gather(*tasks)

    data: dict[str, Any] = {}
    modules_ok = 0
    modules_err = 0
    for name, result in results:
        data[name] = result
        if isinstance(result, dict) and "error" in result and len(result) == 1:
            modules_err += 1
        else:
            modules_ok += 1

    data["_meta"] = {
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "marketplace_id": marketplace_id,
        "modules_ok": modules_ok,
        "modules_error": modules_err,
        "modules_total": modules_ok + modules_err,
    }

    return data


async def get_opportunity_funnel(
    marketplace_id: str | None = None,
) -> dict[str, Any]:
    """Return opportunity pipeline funnel: detected → accepted → completed → measured."""
    from app.services.strategy_service import get_strategy_overview
    from app.services.decision_intelligence_service import get_learning_dashboard

    overview_task = _safe_call("strategy", get_strategy_overview)
    learning_task = _safe_call("learning", get_learning_dashboard)

    results = dict(await asyncio.gather(overview_task, learning_task))

    strategy = results.get("strategy", {})
    learning = results.get("learning", {})

    kpi = strategy.get("kpi", {}) if isinstance(strategy, dict) else {}
    summary = learning.get("summary", {}) if isinstance(learning, dict) else {}

    return {
        "funnel": {
            "detected": kpi.get("total_opportunities", 0),
            "do_now": kpi.get("do_now_count", 0),
            "this_week": kpi.get("this_week_count", 0),
            "this_month": kpi.get("this_month_count", 0),
            "blocked": kpi.get("blocked_count", 0),
            "completed_30d": kpi.get("completed_30d", 0),
            "completed_impact_30d": kpi.get("completed_impact_30d", 0),
        },
        "model_quality": {
            "types_tracked": summary.get("types_tracked", 0),
            "avg_prediction_accuracy": summary.get("avg_prediction_accuracy"),
            "avg_win_rate": summary.get("avg_win_rate"),
            "total_evaluations": summary.get("total_evaluations", 0),
            "avg_roi": summary.get("avg_roi"),
        },
        "by_type": strategy.get("by_type", []) if isinstance(strategy, dict) else [],
    }


async def get_forecast_accuracy(
    opportunity_type: str | None = None,
) -> dict[str, Any]:
    """Compare forecast predictions against actual outcomes across opportunity types.

    Uses the decision_intelligence learning loop data:
    - Per-type accuracy, win rate, ROI
    - Overall model performance summary
    - Recent outcome evaluations
    """
    from app.services.decision_intelligence_service import get_learning_dashboard

    _, learning = await _safe_call("learning", get_learning_dashboard)

    if isinstance(learning, dict) and "error" in learning:
        return learning

    type_data = learning.get("learning", [])
    adjustments = learning.get("adjustments", [])
    summary = learning.get("summary", {})

    # Filter by opportunity type if specified
    if opportunity_type:
        type_data = [t for t in type_data if t.get("opportunity_type") == opportunity_type]
        adjustments = [a for a in adjustments if a.get("opportunity_type") == opportunity_type]

    # Classify types by accuracy tier
    tiers = {"high_accuracy": [], "medium_accuracy": [], "low_accuracy": [], "insufficient_data": []}
    for t in type_data:
        acc = t.get("prediction_accuracy")
        sample = t.get("sample_size", 0)
        if sample < 5:
            tiers["insufficient_data"].append(t.get("opportunity_type"))
        elif acc is not None and acc >= 0.8:
            tiers["high_accuracy"].append(t.get("opportunity_type"))
        elif acc is not None and acc >= 0.5:
            tiers["medium_accuracy"].append(t.get("opportunity_type"))
        else:
            tiers["low_accuracy"].append(t.get("opportunity_type"))

    return {
        "summary": summary,
        "by_type": type_data,
        "adjustments": adjustments,
        "accuracy_tiers": tiers,
    }
