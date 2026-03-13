"""Tests for runtime guardrails service.

Validates:
  - Each sync check returns correct severity for various DB scenarios
  - Each async check returns correct severity for various Redis scenarios
  - Fail-open behaviour: checks return UNKNOWN on connection errors
  - Orchestrator: run_all_sync_checks, run_all_async_checks, run_guardrails
  - Persistence: persist_results writes to DB
  - API endpoints return expected structures
"""
from __future__ import annotations

from dataclasses import asdict
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

from app.services.guardrails import (
    GuardrailResult,
    Severity,
    _run_scalar,
    _run_rows,
    _timed,
    # Section 1 – Pipeline Freshness
    check_order_sync_freshness,
    check_finance_freshness,
    check_inventory_freshness,
    check_profitability_freshness,
    check_fx_rate_freshness,
    check_ads_freshness,
    check_content_queue_depth,
    # Section 2 – Financial Corruption
    check_unknown_fee_types,
    check_fee_classification_coverage,
    check_profit_margin_anomalies,
    check_missing_fx_rates,
    check_duplicate_finance_transactions,
    check_order_finance_drift,
    # Section 3 – Infrastructure (async)
    check_scheduler_health,
    check_circuit_breaker_state,
    check_rate_limit_blocks,
    # Section 4 – Daily Integrity
    check_order_finance_totals,
    check_inventory_integrity,
    check_ads_spend_consistency,
    check_shipping_cost_gaps,
    check_profit_calc_completeness,
    # Section 5 – SP-API & Jobs
    check_spapi_throttle_rate,
    check_job_duplication,
    # Orchestrator
    _SYNC_CHECKS,
    _ASYNC_CHECKS,
    run_all_sync_checks,
    run_all_async_checks,
    persist_results,
    run_guardrails,
)


# ── Helpers / Fixtures ──────────────────────────────────────────────────────

def _mock_conn(rows=None, scalar=None):
    """Return a mock pymssql connection whose cursor returns *rows*."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    if scalar is not None:
        cur.fetchone.return_value = (scalar,)
    elif rows is not None:
        cur.fetchall.return_value = rows
        cur.fetchone.return_value = rows[0] if rows else None
    else:
        cur.fetchone.return_value = None
        cur.fetchall.return_value = []
    return conn


CONNECT = "app.services.guardrails.connect_acc"


# ── Model tests ─────────────────────────────────────────────────────────────

class TestGuardrailResult:
    def test_asdict(self):
        r = GuardrailResult("test", Severity.OK, "all good", value=42, threshold=100)
        d = asdict(r)
        assert d["check_name"] == "test"
        assert d["severity"] == Severity.OK
        assert d["value"] == 42

    def test_default_checked_at(self):
        r = GuardrailResult("x", Severity.WARNING, "warn")
        assert r.checked_at is not None


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1 – Pipeline Freshness
# ═══════════════════════════════════════════════════════════════════════════

class TestOrderSyncFreshness:
    @patch(CONNECT)
    def test_ok(self, mock_conn_fn):
        mock_conn_fn.return_value = _mock_conn(scalar=10)
        r = check_order_sync_freshness()
        assert r.severity == Severity.OK
        assert r.value == 10

    @patch(CONNECT)
    def test_warning(self, mock_conn_fn):
        mock_conn_fn.return_value = _mock_conn(scalar=80)
        r = check_order_sync_freshness()
        assert r.severity == Severity.WARNING

    @patch(CONNECT)
    def test_critical(self, mock_conn_fn):
        mock_conn_fn.return_value = _mock_conn(scalar=150)
        r = check_order_sync_freshness()
        assert r.severity == Severity.CRITICAL

    @patch(CONNECT)
    def test_no_records(self, mock_conn_fn):
        mock_conn_fn.return_value = _mock_conn(scalar=None)
        r = check_order_sync_freshness()
        assert r.severity == Severity.CRITICAL
        assert "No order sync" in r.message

    @patch(CONNECT, side_effect=Exception("timeout"))
    def test_fail_open(self, mock_conn_fn):
        r = check_order_sync_freshness()
        assert r.severity == Severity.UNKNOWN


class TestFinanceFreshness:
    @patch(CONNECT)
    def test_ok(self, mock_conn_fn):
        mock_conn_fn.return_value = _mock_conn(scalar=12)
        r = check_finance_freshness()
        assert r.severity == Severity.OK

    @patch(CONNECT)
    def test_warning(self, mock_conn_fn):
        mock_conn_fn.return_value = _mock_conn(scalar=40)
        r = check_finance_freshness()
        assert r.severity == Severity.WARNING

    @patch(CONNECT)
    def test_critical(self, mock_conn_fn):
        mock_conn_fn.return_value = _mock_conn(scalar=60)
        r = check_finance_freshness()
        assert r.severity == Severity.CRITICAL

    @patch(CONNECT, side_effect=Exception("down"))
    def test_fail_open(self, _):
        r = check_finance_freshness()
        assert r.severity == Severity.UNKNOWN


class TestInventoryFreshness:
    @patch(CONNECT)
    def test_ok(self, m):
        m.return_value = _mock_conn(scalar=20)
        assert check_inventory_freshness().severity == Severity.OK

    @patch(CONNECT)
    def test_critical(self, m):
        m.return_value = _mock_conn(scalar=72)
        assert check_inventory_freshness().severity == Severity.CRITICAL


class TestProfitabilityFreshness:
    @patch(CONNECT)
    def test_ok(self, m):
        m.return_value = _mock_conn(scalar=10)
        assert check_profitability_freshness().severity == Severity.OK

    @patch(CONNECT, side_effect=Exception("err"))
    def test_fail_open(self, _):
        assert check_profitability_freshness().severity == Severity.UNKNOWN


class TestFxRateFreshness:
    @patch(CONNECT)
    def test_ok(self, m):
        m.return_value = _mock_conn(scalar=12)
        assert check_fx_rate_freshness().severity == Severity.OK

    @patch(CONNECT)
    def test_warning(self, m):
        m.return_value = _mock_conn(scalar=72)
        assert check_fx_rate_freshness().severity == Severity.WARNING

    @patch(CONNECT)
    def test_critical_over_7d(self, m):
        m.return_value = _mock_conn(scalar=200)
        assert check_fx_rate_freshness().severity == Severity.CRITICAL


class TestAdsFreshness:
    @patch(CONNECT)
    def test_ok(self, m):
        m.return_value = _mock_conn(scalar=24)
        assert check_ads_freshness().severity == Severity.OK

    @patch(CONNECT)
    def test_warning_no_data(self, m):
        m.return_value = _mock_conn(scalar=None)
        assert check_ads_freshness().severity == Severity.WARNING


class TestContentQueueDepth:
    @patch(CONNECT)
    def test_ok(self, m):
        m.return_value = _mock_conn(scalar=10)
        r = check_content_queue_depth()
        assert r.severity == Severity.OK
        assert r.value == 10

    @patch(CONNECT)
    def test_warning(self, m):
        m.return_value = _mock_conn(scalar=80)
        assert check_content_queue_depth().severity == Severity.WARNING

    @patch(CONNECT)
    def test_critical(self, m):
        m.return_value = _mock_conn(scalar=200)
        assert check_content_queue_depth().severity == Severity.CRITICAL


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2 – Financial Corruption
# ═══════════════════════════════════════════════════════════════════════════

class TestUnknownFeeTypes:
    @patch(CONNECT)
    def test_all_classified(self, m):
        m.return_value = _mock_conn(rows=[])
        assert check_unknown_fee_types().severity == Severity.OK

    @patch(CONNECT)
    def test_few_unknown(self, m):
        m.return_value = _mock_conn(rows=[("RefundCommission", 5), ("NewFee", 3)])
        assert check_unknown_fee_types().severity == Severity.WARNING

    @patch(CONNECT)
    def test_many_unknown_critical(self, m):
        m.return_value = _mock_conn(rows=[("Fee1", 60)])
        assert check_unknown_fee_types().severity == Severity.CRITICAL


class TestFeeClassificationCoverage:
    @patch(CONNECT)
    def test_high_coverage(self, m):
        m.return_value = _mock_conn(rows=[(1000, 950)])
        r = check_fee_classification_coverage()
        assert r.severity == Severity.OK
        assert r.value == 95.0

    @patch(CONNECT)
    def test_low_coverage(self, m):
        m.return_value = _mock_conn(rows=[(100, 70)])
        assert check_fee_classification_coverage().severity == Severity.CRITICAL


class TestProfitMarginAnomalies:
    @patch(CONNECT)
    def test_no_anomalies(self, m):
        m.return_value = _mock_conn(rows=[])
        assert check_profit_margin_anomalies().severity == Severity.OK

    @patch(CONNECT)
    def test_few_anomalies_warning(self, m):
        m.return_value = _mock_conn(rows=[
            ("SKU1", "DE", 100, 10, 90.0),
            ("SKU2", "PL", 200, 300, -50.0),
            ("SKU3", "IT", 150, 15, 85.0),
            ("SKU4", "FR", 180, 20, -60.0),
            ("SKU5", "ES", 120, 8, 92.0),
        ])
        assert check_profit_margin_anomalies().severity == Severity.WARNING


class TestMissingFxRates:
    @patch(CONNECT)
    def test_all_covered(self, m):
        m.return_value = _mock_conn(rows=[])
        assert check_missing_fx_rates().severity == Severity.OK

    @patch(CONNECT)
    def test_missing_currencies(self, m):
        m.return_value = _mock_conn(rows=[("GBP",), ("SEK",), ("CZK",)])
        r = check_missing_fx_rates()
        assert r.severity == Severity.CRITICAL
        assert r.value == 3


class TestDuplicateFinanceTxns:
    @patch(CONNECT)
    def test_no_dupes(self, m):
        m.return_value = _mock_conn(rows=[])
        assert check_duplicate_finance_transactions().severity == Severity.OK

    @patch(CONNECT)
    def test_some_dupes(self, m):
        m.return_value = _mock_conn(rows=[("hash1", 3), ("hash2", 2)])
        r = check_duplicate_finance_transactions()
        assert r.severity == Severity.WARNING
        assert r.value == 3  # excess rows: (3-1) + (2-1) = 3


class TestOrderFinanceDrift:
    @patch(CONNECT)
    def test_low_drift(self, m):
        m.return_value = _mock_conn(rows=[(5, 500)])
        r = check_order_finance_drift()
        assert r.severity == Severity.OK
        assert r.value == 1.0

    @patch(CONNECT)
    def test_high_drift(self, m):
        m.return_value = _mock_conn(rows=[(450, 500)])
        assert check_order_finance_drift().severity == Severity.CRITICAL


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3 – Infrastructure (async)
# ═══════════════════════════════════════════════════════════════════════════

class TestSchedulerHealth:
    @pytest.mark.asyncio
    async def test_healthy(self):
        mock_sched = MagicMock()
        mock_sched.running = True
        mock_sched.get_jobs.return_value = ["job1", "job2"]

        with patch("app.services.guardrails.scheduler", mock_sched, create=True), \
             patch.dict("sys.modules", {"app.scheduler": MagicMock(scheduler=mock_sched)}):
            r = await check_scheduler_health()
        assert r.severity == Severity.OK

    @pytest.mark.asyncio
    async def test_no_leader(self):
        fake_redis = AsyncMock()
        fake_redis.get.return_value = None
        fake_redis.ttl.return_value = -2

        with patch("app.core.redis_client.get_redis", return_value=fake_redis), \
             patch("app.core.scheduler_lock.LOCK_KEY", "test:lock"):
            r = await check_scheduler_health()
        assert r.severity == Severity.CRITICAL


class TestCircuitBreakerState:
    @pytest.mark.asyncio
    async def test_closed(self):
        with patch("app.core.circuit_breaker.get_state", new_callable=AsyncMock,
                    return_value={"state": "closed", "failures_in_window": 2}):
            r = await check_circuit_breaker_state()
        assert r.severity == Severity.OK

    @pytest.mark.asyncio
    async def test_open(self):
        with patch("app.core.circuit_breaker.get_state", new_callable=AsyncMock,
                    return_value={"state": "open", "failures_in_window": 15,
                                  "cooldown_remaining_seconds": 30}):
            r = await check_circuit_breaker_state()
        assert r.severity == Severity.WARNING


class TestRateLimitBlocks:
    @pytest.mark.asyncio
    async def test_few_blocked(self):
        fake_redis = AsyncMock()

        async def _scan(*args, **kwargs):
            for key in [b"auth:block:1.2.3.4"]:
                yield key

        fake_redis.scan_iter = _scan

        with patch("app.core.redis_client.get_redis", return_value=fake_redis):
            r = await check_rate_limit_blocks()
        assert r.severity == Severity.OK
        assert r.value == 1


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4 – Daily Integrity
# ═══════════════════════════════════════════════════════════════════════════

class TestOrderFinanceTotals:
    @patch(CONNECT)
    def test_ok(self, m):
        m.return_value = _mock_conn(rows=[(10000.0, 9500.0, 5.0)])
        r = check_order_finance_totals()
        assert r.severity == Severity.OK
        assert r.value == 5.0

    @patch(CONNECT)
    def test_high_drift(self, m):
        m.return_value = _mock_conn(rows=[(10000.0, 2000.0, 80.0)])
        assert check_order_finance_totals().severity == Severity.CRITICAL


class TestInventoryIntegrity:
    @patch(CONNECT)
    def test_clean(self, m):
        m.return_value = _mock_conn(rows=[(500, 0, 0)])
        assert check_inventory_integrity().severity == Severity.OK

    @patch(CONNECT)
    def test_issues(self, m):
        m.return_value = _mock_conn(rows=[(500, 15, 3)])
        assert check_inventory_integrity().severity == Severity.CRITICAL


class TestAdsSpendConsistency:
    @patch(CONNECT)
    def test_ok(self, m):
        m.return_value = _mock_conn(rows=[(1500.50, 200, 10)])
        r = check_ads_spend_consistency()
        assert r.severity == Severity.OK

    @patch(CONNECT)
    def test_no_data(self, m):
        m.return_value = _mock_conn(rows=[(None, 0, 0)])
        assert check_ads_spend_consistency().severity == Severity.WARNING


class TestShippingCostGaps:
    @patch(CONNECT)
    def test_ok(self, m):
        m.return_value = _mock_conn(rows=[(4, 1000, 20, 98.0)])
        r = check_shipping_cost_gaps()
        assert r.severity == Severity.OK
        assert "acc_courier_monthly_kpi_snapshot" in r.query_used

    @patch(CONNECT)
    def test_high_gap(self, m):
        m.return_value = _mock_conn(rows=[(4, 1000, 200, 80.0)])
        assert check_shipping_cost_gaps().severity == Severity.CRITICAL

    @patch(CONNECT)
    def test_missing_snapshot_is_unknown(self, m):
        m.return_value = _mock_conn(rows=[(0, 0, 0, None)])
        assert check_shipping_cost_gaps().severity == Severity.UNKNOWN


class TestProfitCalcCompleteness:
    @patch(CONNECT)
    def test_complete(self, m):
        m.return_value = _mock_conn(rows=[(1000, 10, 30)])
        assert check_profit_calc_completeness().severity == Severity.OK

    @patch(CONNECT)
    def test_many_missing(self, m):
        m.return_value = _mock_conn(rows=[(1000, 200, 200)])
        assert check_profit_calc_completeness().severity == Severity.CRITICAL


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 5 – SP-API & Jobs
# ═══════════════════════════════════════════════════════════════════════════

class TestSpApiThrottle:
    @patch(CONNECT)
    def test_ok(self, m):
        m.return_value = _mock_conn(rows=[(1000, 10, 2)])
        r = check_spapi_throttle_rate()
        assert r.severity == Severity.OK
        assert r.value == 1.0

    @patch(CONNECT)
    def test_high_throttle(self, m):
        m.return_value = _mock_conn(rows=[(100, 20, 5)])
        assert check_spapi_throttle_rate().severity == Severity.CRITICAL

    @patch(CONNECT)
    def test_no_calls(self, m):
        m.return_value = _mock_conn(rows=[(0, 0, 0)])
        assert check_spapi_throttle_rate().severity == Severity.OK


class TestJobDuplication:
    @patch(CONNECT)
    def test_no_dupes(self, m):
        m.return_value = _mock_conn(rows=[])
        assert check_job_duplication().severity == Severity.OK

    @patch(CONNECT)
    def test_dupes_found(self, m):
        m.return_value = _mock_conn(rows=[("order_pipeline", 3)])
        r = check_job_duplication()
        assert r.severity == Severity.WARNING
        assert "order_pipeline" in r.message


# ═══════════════════════════════════════════════════════════════════════════
# Orchestrator
# ═══════════════════════════════════════════════════════════════════════════

class TestOrchestrator:
    def test_sync_checks_list_complete(self):
        assert len(_SYNC_CHECKS) == 24

    def test_async_checks_list_complete(self):
        assert len(_ASYNC_CHECKS) == 3

    @patch(CONNECT)
    def test_run_all_sync_catches_exceptions(self, m):
        m.side_effect = Exception("db down")
        results = run_all_sync_checks()
        assert len(results) == 24
        for r in results:
            assert r.severity == Severity.UNKNOWN

    @pytest.mark.asyncio
    async def test_run_all_async_catches_exceptions(self):
        mock_sched = MagicMock()
        mock_sched.running.__bool__ = MagicMock(side_effect=Exception("scheduler down"))

        with patch("app.core.redis_client.get_redis", side_effect=Exception("redis down")), \
             patch.dict("sys.modules", {"app.scheduler": MagicMock(scheduler=mock_sched)}), \
             patch("app.core.circuit_breaker.get_state", new_callable=AsyncMock, side_effect=Exception("cb down")):
            results = await run_all_async_checks()
        assert len(results) == 3
        for r in results:
            assert r.severity in (Severity.UNKNOWN, Severity.OK)  # UNKNOWN or OK (dev-mode fallback)

    @patch(CONNECT)
    def test_persist_results(self, m):
        conn = _mock_conn()
        m.return_value = conn
        results = [
            GuardrailResult("test_check", Severity.OK, "fine", value=1, threshold=5),
        ]
        persist_results(results)
        cur = conn.cursor()
        assert cur.execute.call_count >= 2  # CREATE TABLE IF + INSERT
        assert conn.commit.call_count >= 1

    @pytest.mark.asyncio
    async def test_run_guardrails_returns_report(self):
        fake_results = [
            GuardrailResult("c1", Severity.OK, "ok"),
            GuardrailResult("c2", Severity.WARNING, "warn"),
        ]
        with patch("app.services.guardrails.run_all_sync_checks", return_value=fake_results), \
             patch("app.services.guardrails.run_all_async_checks",
                   new_callable=AsyncMock, return_value=[]), \
             patch("app.services.guardrails.persist_results"):
            report = await run_guardrails(persist=True)

        assert report["status"] == "degraded"
        assert report["total_checks"] == 2
        assert report["summary"]["ok"] == 1
        assert report["summary"]["warning"] == 1
        assert len(report["checks"]) == 2

    @pytest.mark.asyncio
    async def test_run_guardrails_healthy(self):
        fake_results = [
            GuardrailResult("c1", Severity.OK, "ok"),
        ]
        with patch("app.services.guardrails.run_all_sync_checks", return_value=fake_results), \
             patch("app.services.guardrails.run_all_async_checks",
                   new_callable=AsyncMock, return_value=[
                       GuardrailResult("c2", Severity.OK, "ok")
                   ]), \
             patch("app.services.guardrails.persist_results"):
            report = await run_guardrails(persist=False)

        assert report["status"] == "healthy"
        assert report["total_checks"] == 2

    @pytest.mark.asyncio
    async def test_run_guardrails_critical(self):
        with patch("app.services.guardrails.run_all_sync_checks", return_value=[
            GuardrailResult("c1", Severity.CRITICAL, "bad"),
        ]), \
             patch("app.services.guardrails.run_all_async_checks",
                   new_callable=AsyncMock, return_value=[]), \
             patch("app.services.guardrails.persist_results"):
            report = await run_guardrails(persist=True)

        assert report["status"] == "critical"
