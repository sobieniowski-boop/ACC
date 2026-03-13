"""Tests for scheduler dependency chain correctness.

Verifies:
  SCH-01: profitability chain function exists and has correct structure
  SCH-02: executive and strategy are NOT registered as separate cron jobs
  SCH-03: dependency abort logic present
  SCH-04: chain order is correct (ads→finance→rollup→executive→strategy)
  SCH-05: event-driven dependency gate registers handlers
"""
from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _load_source():
    """Load scheduler profit module source for static analysis."""
    sched_path = Path(__file__).resolve().parents[1] / "app" / "platform" / "scheduler" / "profit.py"
    pytest.source = sched_path.read_text(encoding="utf-8")


class TestSCH01_ChainFunctionExists:
    """_run_profitability_chain must contain the full chain."""

    def test_function_exists(self):
        assert "async def _run_profitability_chain" in pytest.source

    def test_safety_net_exists(self):
        assert "async def _recompute_profitability" in pytest.source

    def test_contains_ads_sync_step(self):
        assert "run_full_ads_sync" in pytest.source

    def test_contains_finance_sync_step(self):
        assert "step_sync_finances" in pytest.source

    def test_contains_rollup_step(self):
        assert "recompute_rollups" in pytest.source

    def test_contains_alerts_step(self):
        assert "evaluate_profitability_alerts" in pytest.source

    def test_contains_executive_step(self):
        assert "run_executive_pipeline" in pytest.source

    def test_contains_strategy_step(self):
        assert "run_strategy_detection" in pytest.source


class TestSCH02_NoOrphanRegistrations:
    """Executive and strategy must NOT be registered as separate scheduled jobs."""

    def test_no_executive_pipeline_daily_registration(self):
        assert 'id="executive-pipeline-daily"' not in pytest.source, (
            "executive-pipeline-daily must NOT be a separate cron job — "
            "it runs inside the profitability chain"
        )

    def test_no_strategy_detection_daily_registration(self):
        assert 'id="strategy-detection-daily"' not in pytest.source, (
            "strategy-detection-daily must NOT be a separate cron job — "
            "it runs inside the profitability chain"
        )

    def test_profitability_chain_registered(self):
        assert 'id="profitability-chain-daily"' in pytest.source, (
            "profitability-chain-daily must be registered as a scheduled job"
        )


class TestSCH03_DependencyAbort:
    """Chain must abort if dependency syncs fail."""

    def test_dep_failed_flag_exists(self):
        assert "dep_failed" in pytest.source

    def test_abort_on_dep_failure(self):
        # After dep_failed is set True, there must be a return before rollup
        fn_source = _extract_function(pytest.source, "_run_profitability_chain")
        assert fn_source is not None, "_run_profitability_chain not found"

        # Find the dep_failed check
        dep_check_idx = fn_source.find("if dep_failed:")
        assert dep_check_idx > 0, "dep_failed check not found in function"

        # After dep_failed check, there must be set_job_failure + return
        after_check = fn_source[dep_check_idx:dep_check_idx + 500]
        assert "set_job_failure" in after_check
        assert "return" in after_check

    def test_executive_failure_skips_strategy(self):
        fn_source = _extract_function(pytest.source, "_run_profitability_chain")
        assert fn_source is not None
        # executive failure block should contain strategy=SKIPPED
        assert "strategy=SKIPPED" in fn_source


class TestSCH04_ChainOrder:
    """Chain steps must execute in correct order.

    The chain is decomposed into helpers (_step_rollup_and_alerts,
    _step_executive, _step_strategy) called sequentially from
    _run_profitability_chain.  We verify:
      1. ads→finance ordering inside _run_profitability_chain
      2. rollup→executive→strategy step ordering inside _run_profitability_chain
      3. each helper contains the expected service call
    """

    def test_ads_before_finance(self):
        fn_source = _extract_function(pytest.source, "_run_profitability_chain")
        assert fn_source is not None
        ads_idx = fn_source.find("run_full_ads_sync")
        fin_idx = fn_source.find("step_sync_finances")
        assert ads_idx < fin_idx, "ads sync must run before finance sync"

    def test_rollup_then_executive_then_strategy_order(self):
        fn_source = _extract_function(pytest.source, "_run_profitability_chain")
        assert fn_source is not None
        roll_idx = fn_source.find("_step_rollup_and_alerts")
        exec_idx = fn_source.find("_step_executive")
        strat_idx = fn_source.find("_step_strategy")
        assert roll_idx > 0, "_step_rollup_and_alerts call not found"
        assert exec_idx > 0, "_step_executive call not found"
        assert strat_idx > 0, "_step_strategy call not found"
        assert roll_idx < exec_idx < strat_idx, \
            "chain must be rollup → executive → strategy"

    def test_rollup_helper_contains_recompute(self):
        fn_source = _extract_function(pytest.source, "_step_rollup_and_alerts")
        assert fn_source is not None
        assert "recompute_rollups" in fn_source

    def test_executive_helper_contains_pipeline(self):
        fn_source = _extract_function(pytest.source, "_step_executive")
        assert fn_source is not None
        assert "run_executive_pipeline" in fn_source

    def test_strategy_helper_contains_detection(self):
        fn_source = _extract_function(pytest.source, "_step_strategy")
        assert fn_source is not None
        assert "run_strategy_detection" in fn_source


class TestSCH05_EventHandlers:
    """Event-driven dependency gate must register handlers."""

    def test_register_event_handlers_function_exists(self):
        assert "def register_event_handlers" in pytest.source

    def test_ads_synced_handler_registered(self):
        assert '"ads", "synced"' in pytest.source or "'ads', 'synced'" in pytest.source

    def test_finance_synced_handler_registered(self):
        assert '"finance", "synced"' in pytest.source or "'finance', 'synced'" in pytest.source

    def test_event_handlers_called_from_register(self):
        fn_source = _extract_function(pytest.source, "register")
        assert fn_source is not None
        assert "register_event_handlers()" in fn_source


def _extract_function(source: str, name: str) -> str | None:
    """Extract the body of a function (sync or async) by name."""
    pattern = rf"((?:async )?def {name}\(.*?\n)(.*?)(?=\n(?:async )?def |\Z)"
    match = re.search(pattern, source, re.DOTALL)
    if match:
        return match.group(0)
    return None
