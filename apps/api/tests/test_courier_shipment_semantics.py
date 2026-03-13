from __future__ import annotations

from app.services.courier_shipment_semantics import ShipmentSemanticsInput, _classify_shipment_semantics, refresh_courier_shipment_outcomes


def test_classify_shipment_semantics_marks_replacement_delivery():
    result = _classify_shipment_semantics(
        ShipmentSemanticsInput(
            carrier="DHL",
            amazon_order_id="405-1234567-1234567",
            acc_order_id="00000000-0000-0000-0000-000000000111",
            bl_order_id=2002,
            primary_link_method="order_rel_repl_tracking_number_courier_package_nr",
            status_code="DELIVERED",
            status_label="Delivered",
            is_delivered=True,
            tracking_status="DELIVERED",
            package_type=None,
            package_is_return=False,
            event_text="delivered to recipient",
            relation_type="replacement_order",
            relation_confidence=0.98,
        )
    )

    assert result["outcome_code"] == "delivered"
    assert result["cost_reason"] == "replacement_shipment"


def test_classify_shipment_semantics_marks_return_cost_reason():
    result = _classify_shipment_semantics(
        ShipmentSemanticsInput(
            carrier="GLS",
            amazon_order_id=None,
            acc_order_id=None,
            bl_order_id=None,
            primary_link_method=None,
            status_code="RETURN",
            status_label="Returned to sender",
            is_delivered=False,
            tracking_status="RETURN",
            package_type="return",
            package_is_return=True,
            event_text="returned to sender",
            relation_type=None,
            relation_confidence=0.0,
        )
    )

    assert result["outcome_code"] == "return_to_sender"
    assert result["cost_reason"] == "failed_delivery_or_return"


class _FakeCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, object]] = []
        self.executemany_calls: list[tuple[str, list[list[object]]]] = []
        self.rowcount = 0
        self.fast_executemany = False

    def execute(self, sql: str, params=None):
        self.executed.append((sql, params))
        if "DELETE FROM dbo.acc_shipment_outcome_fact" in sql:
            self.rowcount = 2
        else:
            self.rowcount = 0
        return self

    def executemany(self, sql: str, params):
        materialized = list(params)
        self.executemany_calls.append((sql, materialized))
        return self


class _FakeConn:
    def __init__(self) -> None:
        self.cursor_obj = _FakeCursor()
        self.commit_count = 0
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self) -> None:
        self.commit_count += 1

    def close(self) -> None:
        self.closed = True


def test_refresh_courier_shipment_outcomes_writes_rows(monkeypatch):
    fake_conn = _FakeConn()
    monkeypatch.setattr("app.services.courier_shipment_semantics.ensure_dhl_schema", lambda: None)
    monkeypatch.setattr("app.services.courier_shipment_semantics.ensure_bl_distribution_cache_schema", lambda: None)
    monkeypatch.setattr("app.services.courier_shipment_semantics._connect", lambda: fake_conn)
    monkeypatch.setattr(
        "app.services.courier_shipment_semantics._load_shipment_rows",
        lambda *args, **kwargs: [
            (
                "00000000-0000-0000-0000-000000000001",
                "405-1234567-1234567",
                "00000000-0000-0000-0000-000000000111",
                2002,
                "primary_tracking",
                "DELIVERED",
                "Delivered",
                True,
                "DELIVERED",
                None,
                False,
                "delivered to recipient",
                "replacement_order",
                0.98,
            )
        ],
    )

    result = refresh_courier_shipment_outcomes(months=["2026-01"], carriers=["DHL"])

    assert result["rows_deleted"] == 2
    assert result["rows_written"] == 1
    assert result["matrix"]["2026-01"]["DHL"]["outcomes"]["delivered"] == 1
    assert result["matrix"]["2026-01"]["DHL"]["cost_reasons"]["replacement_shipment"] == 1
    assert fake_conn.commit_count == 1
    assert fake_conn.closed is True
    assert len(fake_conn.cursor_obj.executemany_calls) == 1
