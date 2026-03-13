from __future__ import annotations

from app.services.courier_readiness import _billing_period_coverage, _billing_period_variants


class _FakeCursor:
    def __init__(self) -> None:
        self.sql = ""
        self.params = []

    def execute(self, sql: str, params=None):
        self.sql = sql
        self.params = list(params or [])
        return self

    def fetchone(self):
        return (12, 9)


class _FakeConn:
    def __init__(self) -> None:
        self.cursor_obj = _FakeCursor()
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self) -> None:
        self.closed = True


def test_billing_period_variants_support_dash_and_dot():
    assert _billing_period_variants("2026-02") == ["2026-02", "2026.02"]
    assert _billing_period_variants("2026.02") == ["2026.02", "2026-02"]


def test_billing_period_coverage_queries_both_variants(monkeypatch):
    fake_conn = _FakeConn()
    monkeypatch.setattr("app.services.courier_readiness.connect_acc", lambda timeout=60: fake_conn)

    result = _billing_period_coverage(carrier="GLS", billing_period="2026-02")

    assert result["billed_shipments_total"] == 12
    assert result["billed_shipments_linked"] == 9
    assert result["link_coverage_pct"] == 75.0
    assert fake_conn.cursor_obj.params == ["GLS", "2026-02", "2026.02"]
    assert "c.billing_periodIN(?,?)" in fake_conn.cursor_obj.sql.replace(" ", "")
    assert fake_conn.closed is True
