from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from app.core.config import settings


def test_profitability_orders_use_canonical_logistics(monkeypatch):
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", True, raising=False)

    from app.services.profitability_service import get_profitability_orders

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_cur.fetchone.return_value = (0,)
    mock_cur.fetchall.return_value = []

    with patch("app.intelligence.profit.rollup.connect_acc", return_value=mock_conn):
        result = get_profitability_orders(date(2026, 1, 1), date(2026, 1, 7))

    sql = "\n".join(str(call.args[0]) for call in mock_cur.execute.call_args_list if call.args)

    assert result["total"] == 0
    assert "acc_order_logistics_fact" in sql
    assert "OUTER APPLY" in sql
    assert "olf.total_logistics_pln" in sql


def test_sync_profit_snapshot_uses_canonical_transport_and_shipping_revenue(monkeypatch):
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", True, raising=False)

    from app.connectors.mssql import mssql_store

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cur.rowcount = 7
    mock_conn.cursor.return_value = mock_cur

    with patch.object(mssql_store, "ensure_v2_schema"):
        with patch.object(mssql_store, "_connect", return_value=mock_conn):
            synced = mssql_store.sync_profit_snapshot(
                date_from=date(2026, 1, 1),
                date_to=date(2026, 1, 7),
            )

    sql = "\n".join(str(call.args[0]) for call in mock_cur.execute.call_args_list if call.args)

    assert synced == 7
    assert "acc_order_logistics_fact" in sql
    assert "AS transport" in sql
    assert "shipping_charge_net_pln" in sql
    assert "ShippingCharge', 'ShippingTax'" in sql


def test_load_finance_lookup_uses_net_shipping_revenue(monkeypatch):
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", True, raising=False)

    from app.services import profit_engine

    profit_engine._RESULT_CACHE.clear()
    cur = MagicMock()
    cur.fetchall.return_value = []

    profit_engine._load_finance_lookup(
        cur,
        date_from=date(2026, 1, 1),
        date_to=date(2026, 1, 7),
    )

    sql = str(cur.execute.call_args[0][0])

    assert "ShippingCharge" in sql
    assert "ShippingTax" in sql


@pytest.mark.asyncio
async def test_recalculate_profit_batch_delegates_to_v2_pipeline(monkeypatch):
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", True, raising=False)

    from app.services import profit_service

    with patch("app.connectors.mssql.mssql_store.recalc_profit_orders", return_value=11) as recalc:
        with patch("app.connectors.mssql.mssql_store.sync_profit_snapshot", return_value=7) as snapshot:
            with patch("app.connectors.mssql.mssql_store.evaluate_alert_rules", return_value=3) as alerts:
                count = await profit_service.recalculate_profit_batch(
                    MagicMock(),
                    date(2026, 1, 1),
                    date(2026, 1, 7),
                    marketplace_id="A1PA6795UKMFR9",
                )

    assert count == 11
    recalc.assert_called_once_with(
        date_from=date(2026, 1, 1),
        date_to=date(2026, 1, 7),
    )
    snapshot.assert_called_once_with(
        date_from=date(2026, 1, 1),
        date_to=date(2026, 1, 7),
    )
    alerts.assert_called_once_with(7)


@pytest.mark.asyncio
async def test_recalculate_profit_batch_tolerates_alert_refresh_failure(monkeypatch):
    monkeypatch.setattr(settings, "PROFIT_USE_LOGISTICS_FACT", True, raising=False)

    from app.services import profit_service

    with patch("app.connectors.mssql.mssql_store.recalc_profit_orders", return_value=5) as recalc:
        with patch("app.connectors.mssql.mssql_store.sync_profit_snapshot", return_value=4) as snapshot:
            with patch(
                "app.connectors.mssql.mssql_store.evaluate_alert_rules",
                side_effect=RuntimeError("alerts down"),
            ) as alerts:
                count = await profit_service.recalculate_profit_batch(
                    MagicMock(),
                    date(2026, 1, 1),
                    date(2026, 1, 2),
                )

    assert count == 5
    recalc.assert_called_once_with(
        date_from=date(2026, 1, 1),
        date_to=date(2026, 1, 2),
    )
    snapshot.assert_called_once_with(
        date_from=date(2026, 1, 1),
        date_to=date(2026, 1, 2),
    )
    alerts.assert_called_once_with(7)
