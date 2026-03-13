from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.orm import aliased

from app.core.config import settings
from app.models.order import AccOrder
from app.models.shipment import AccOrderLogisticsFact
from app.services.order_logistics_source import (
    profit_logistics_join_sqla,
    profit_logistics_join_sql,
    profit_logistics_value_sql,
    profit_logistics_value_sqla,
    resolve_profit_logistics_pln,
)


class _FakeResult:
    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value


class _FakeAsyncSession:
    def __init__(self, value):
        self.value = value
        self.calls = 0

    async def execute(self, _stmt):
        self.calls += 1
        return _FakeResult(self.value)


def test_profit_logistics_sql_uses_legacy_when_flag_disabled(monkeypatch):
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", False, raising=False)

    assert profit_logistics_join_sql(order_alias="o", fact_alias="olf") == ""
    assert profit_logistics_value_sql(order_alias="o", fact_alias="olf") == "CAST(ISNULL(o.logistics_pln, 0) AS FLOAT)"


def test_profit_logistics_sql_prefers_fact_when_flag_enabled(monkeypatch):
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", True, raising=False)

    join_sql = profit_logistics_join_sql(order_alias="o", fact_alias="olf")
    value_sql = profit_logistics_value_sql(order_alias="o", fact_alias="olf")

    assert "acc_order_logistics_fact" in join_sql
    assert "olf.total_logistics_pln" in value_sql
    assert "o.logistics_pln" in value_sql


def test_profit_logistics_sql_uses_outer_apply_top1(monkeypatch):
    """F1 fix: join must use OUTER APPLY TOP 1 to prevent row multiplication."""
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", True, raising=False)

    join_sql = profit_logistics_join_sql(order_alias="o", fact_alias="olf")

    assert "OUTER APPLY" in join_sql
    assert "SELECT TOP 1" in join_sql
    assert "ORDER BY olf_inner.calculated_at DESC" in join_sql
    # Must NOT be a plain LEFT JOIN (the old bug)
    assert "LEFT JOIN" not in join_sql


def test_profit_logistics_sql_outer_apply_correlates_on_order_alias(monkeypatch):
    """OUTER APPLY must correlate on the correct order alias."""
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", True, raising=False)

    join_sql = profit_logistics_join_sql(order_alias="ord", fact_alias="lf")

    assert "ord.amazon_order_id" in join_sql
    assert ") lf" in join_sql


def test_profit_logistics_sqla_uses_legacy_when_flag_disabled(monkeypatch):
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", False, raising=False)

    expr = profit_logistics_value_sqla(order_model=AccOrder)
    sql = str(expr)

    assert "acc_order.logistics_pln" in sql
    assert "total_logistics_pln" not in sql


def test_profit_logistics_sqla_prefers_fact_when_flag_enabled(monkeypatch):
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", True, raising=False)

    expr = profit_logistics_value_sqla(order_model=AccOrder)
    sql = str(expr)

    assert "total_logistics_pln" in sql
    assert "acc_order.logistics_pln" in sql


def test_profit_logistics_sqla_value_uses_scalar_subquery(monkeypatch):
    """F1 fix: value expression must use a correlated scalar subquery, not a plain column."""
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", True, raising=False)

    expr = profit_logistics_value_sqla(order_model=AccOrder)
    sql = str(expr)

    # The scalar subquery should contain ORDER BY and LIMIT / TOP
    assert "ORDER BY" in sql.upper()
    assert "total_logistics_pln" in sql


def test_profit_logistics_sqla_join_deduplicates(monkeypatch):
    """F1 fix: join helper must produce a dedup subquery, not a plain outerjoin."""
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", True, raising=False)

    stmt = select(AccOrder.amazon_order_id).select_from(AccOrder)
    stmt = profit_logistics_join_sqla(stmt, order_model=AccOrder)
    sql = str(stmt)

    # Should contain a subquery with row_number dedup
    sql_upper = sql.upper()
    assert "ROW_NUMBER" in sql_upper or "LATERAL" in sql_upper or "_OLF_LATEST" in sql.lower()


@pytest.mark.asyncio
async def test_resolve_profit_logistics_prefers_fact_value(monkeypatch):
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", True, raising=False)
    session = _FakeAsyncSession(44.12)

    value = await resolve_profit_logistics_pln(
        session,
        amazon_order_id="302-1234567-1234567",
        legacy_logistics_pln=12.34,
    )

    assert value == 44.12
    assert session.calls == 1


@pytest.mark.asyncio
async def test_resolve_profit_logistics_falls_back_to_legacy(monkeypatch):
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", True, raising=False)
    session = _FakeAsyncSession(None)

    value = await resolve_profit_logistics_pln(
        session,
        amazon_order_id="302-1234567-1234567",
        legacy_logistics_pln=12.34,
    )

    assert value == 12.34
    assert session.calls == 1


@pytest.mark.asyncio
async def test_resolve_profit_logistics_skips_db_without_order_id(monkeypatch):
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", True, raising=False)
    session = _FakeAsyncSession(99.99)

    value = await resolve_profit_logistics_pln(
        session,
        amazon_order_id=None,
        legacy_logistics_pln=12.34,
    )

    assert value == 12.34
    assert session.calls == 0


# ── F1 fix: fact_model branch (SUM-safe, no correlated subquery) ──


def test_profit_logistics_sqla_value_with_fact_model_uses_literal_column(monkeypatch):
    """When fact_model is provided, the expression must reference the joined
    _olf_latest subquery via literal_column — NOT a correlated scalar subquery.
    This is critical for SQL Server which forbids subqueries inside SUM().
    """
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", True, raising=False)
    fact_alias = aliased(AccOrderLogisticsFact)

    expr = profit_logistics_value_sqla(order_model=AccOrder, fact_model=fact_alias)
    sql = str(expr).lower()

    # Must reference the joined subquery column, not a correlated subquery
    assert "_olf_latest.total_logistics_pln" in sql
    # Must NOT contain a nested SELECT (correlated subquery)
    assert "select" not in sql


def test_profit_logistics_sqla_value_without_fact_model_uses_subquery(monkeypatch):
    """Without fact_model the expression uses a correlated scalar subquery
    (safe for row-level, not inside aggregates).
    """
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", True, raising=False)

    expr = profit_logistics_value_sqla(order_model=AccOrder, fact_model=None)
    sql = str(expr).upper()

    # Must contain a correlated subquery with ORDER BY
    assert "SELECT" in sql
    assert "ORDER BY" in sql


def test_profit_logistics_sqla_join_with_fact_model_deduplicates(monkeypatch):
    """profit_logistics_join_sqla with fact_model must produce ROW_NUMBER dedup
    subquery _olf_latest joined via outerjoin — exactly 1 row per order.
    """
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", True, raising=False)
    fact_alias = aliased(AccOrderLogisticsFact)

    stmt = select(AccOrder.amazon_order_id).select_from(AccOrder)
    stmt = profit_logistics_join_sqla(stmt, order_model=AccOrder, fact_model=fact_alias)
    sql = str(stmt).lower()

    assert "row_number" in sql
    assert "_olf_latest" in sql
    assert "_olf_dedup" in sql
