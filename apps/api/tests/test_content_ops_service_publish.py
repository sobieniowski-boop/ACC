from __future__ import annotations

from app.services import content_ops
from app.services.content_ops import catalog as _catalog_mod
from app.services.content_ops import publish as _publish_mod


class _Cursor:
    def __init__(self, row=None, rowcount: int = 1):
        self._row = row
        self.rowcount = rowcount

    def execute(self, _sql, *_params):
        return self

    def fetchone(self):
        return self._row


class _Conn:
    def __init__(self, cursor: _Cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def commit(self):
        return None

    def close(self):
        return None


def test_load_required_attrs_state_missing_definition():
    cur = _Cursor(row=None)
    state, attrs = _catalog_mod._load_required_attrs_state_from_definition(  # noqa: SLF001
        cur,
        marketplace="DE",
        product_type="HOME",
    )
    assert state == "missing_definition"
    assert attrs == []


def test_load_required_attrs_state_empty_required_attrs():
    cur = _Cursor(row=("[]",))
    state, attrs = _catalog_mod._load_required_attrs_state_from_definition(  # noqa: SLF001
        cur,
        marketplace="DE",
        product_type="HOME",
    )
    assert state == "empty_required_attrs"
    assert attrs == []


def test_create_publish_push_confirm_is_queued(monkeypatch):
    monkeypatch.setattr(_publish_mod, "ensure_v2_schema", lambda: None)
    monkeypatch.setattr(_publish_mod, "_connect", lambda: _Conn(_Cursor()))
    monkeypatch.setattr(_publish_mod, "_get_publish_job", lambda _job_id: {"id": _job_id, "status": "queued"})

    calls = {"count": 0}

    def _unexpected_process(**_kwargs):
        calls["count"] += 1

    monkeypatch.setattr(_publish_mod, "_process_publish_push_job", _unexpected_process)

    out = content_ops.create_publish_push(
        payload={
            "marketplaces": ["DE"],
            "selection": "approved",
            "mode": "confirm",
        }
    )

    assert out["queued"] is True
    assert calls["count"] == 0


def test_process_queued_publish_jobs_success(monkeypatch):
    monkeypatch.setattr(_publish_mod, "ensure_v2_schema", lambda: None)
    conns = iter([_Conn(_Cursor()), _Conn(_Cursor())])
    monkeypatch.setattr(_publish_mod, "_connect", lambda: next(conns))
    monkeypatch.setattr(
        _publish_mod,
        "_fetchall_dict",
        lambda _cur: [
            {
                "id": "11111111-1111-1111-1111-111111111111",
                "marketplaces_json": '["DE"]',
                "selection_mode": "approved",
                "log_json": '{"sku_filter":["SKU-1"],"version_ids":[]}',
            }
        ],
    )
    processed = {"count": 0}

    def _process(**kwargs):
        processed["count"] += 1
        assert kwargs["mode"] == "confirm"
        assert kwargs["marketplaces"] == ["DE"]
        return {"status": "completed"}

    monkeypatch.setattr(_publish_mod, "_process_publish_push_job", _process)

    out = content_ops.process_queued_publish_jobs(limit=5)
    assert out == {"claimed": 1, "processed": 1, "failed": 0}
    assert processed["count"] == 1


def test_create_publish_push_confirm_idempotency_replay(monkeypatch):
    monkeypatch.setattr(_publish_mod, "ensure_v2_schema", lambda: None)
    monkeypatch.setattr(
        _publish_mod,
        "_find_publish_job_by_idempotency",
        lambda idempotency_key: {"id": "existing", "status": "running", "job_type": "publish_push"},
    )
    out = content_ops.create_publish_push(
        payload={
            "marketplaces": ["DE"],
            "selection": "approved",
            "mode": "confirm",
            "idempotency_key": "idem-123",
        }
    )
    assert out["job"]["id"] == "existing"
    assert out["queued"] is True


def test_process_queued_publish_jobs_failed_worker(monkeypatch):
    monkeypatch.setattr(_publish_mod, "ensure_v2_schema", lambda: None)
    conns = iter([_Conn(_Cursor()), _Conn(_Cursor()), _Conn(_Cursor())])
    monkeypatch.setattr(_publish_mod, "_connect", lambda: next(conns))
    monkeypatch.setattr(
        _publish_mod,
        "_fetchall_dict",
        lambda _cur: [
            {
                "id": "22222222-2222-2222-2222-222222222222",
                "marketplaces_json": '["FR"]',
                "selection_mode": "approved",
                "log_json": "{}",
            }
        ],
    )

    def _boom(**_kwargs):
        raise RuntimeError("worker_failed")

    monkeypatch.setattr(_publish_mod, "_process_publish_push_job", _boom)
    out = content_ops.process_queued_publish_jobs(limit=1)
    assert out == {"claimed": 1, "processed": 0, "failed": 1}


def test_retry_publish_job_uses_failed_skus(monkeypatch):
    monkeypatch.setattr(_publish_mod, "ensure_v2_schema", lambda: None)
    monkeypatch.setattr(
        _publish_mod,
        "_get_publish_job",
        lambda _job_id: {
            "id": _job_id,
            "job_type": "publish_push",
            "marketplaces": ["DE", "FR"],
            "selection_mode": "approved",
            "status": "failed",
            "log_json": {
                "per_marketplace": {
                    "DE": {"native_errors": [{"sku": "SKU-1"}, {"sku": "SKU-2"}]},
                    "FR": {"native_errors": [{"sku": "SKU-1"}]},
                }
            },
        },
    )
    monkeypatch.setattr(
        _publish_mod,
        "create_publish_push",
        lambda payload: {"job": {"id": "new-job", "status": "queued"}, "queued": True, "detail": f"payload={payload}"},
    )
    out = content_ops.retry_publish_job(job_id="old-job", payload={"failed_only": True})
    assert out["queued"] is True
    assert "source_job=old-job" in out["detail"]
