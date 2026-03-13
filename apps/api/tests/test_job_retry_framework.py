from __future__ import annotations

import pytest

from app.connectors.mssql import mssql_store


def test_retry_policy_defaults_respect_sensitive_jobs():
    assert mssql_store._normalize_retry_policy("sync_ads") == "standard"
    assert mssql_store._normalize_retry_policy("inventory_apply_draft") == "none"
    assert mssql_store._normalize_retry_policy(
        "inventory_apply_draft",
        {"retry_policy": "standard"},
    ) == "none"


def test_retry_backoff_standard_curve():
    assert mssql_store._retry_backoff_minutes(1) == 1
    assert mssql_store._retry_backoff_minutes(2) == 5
    assert mssql_store._retry_backoff_minutes(3) == 15
    assert mssql_store._retry_backoff_minutes(4) == 60
    assert mssql_store._retry_backoff_minutes(5) == 60


def test_classify_timeout_as_transient():
    kind, code = mssql_store._classify_job_error(TimeoutError("request timed out"))
    assert kind == "transient"
    assert code


def test_handle_job_failure_schedules_retry_for_retryable_job(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        mssql_store,
        "get_job",
        lambda job_id, include_params=False: {
            "id": job_id,
            "job_type": "sync_ads",
            "retry_count": 0,
            "max_retries": 4,
            "retry_policy": "standard",
        },
    )
    captured: dict[str, object] = {}

    def _schedule(job_id: str, **kwargs):
        captured["job_id"] = job_id
        captured.update(kwargs)
        return True

    monkeypatch.setattr(mssql_store, "schedule_job_retry", _schedule)
    monkeypatch.setattr(
        mssql_store,
        "_mark_job_failure_terminal",
        lambda *args, **kwargs: pytest.fail("terminal failure should not be used for transient retryable job"),
    )

    result = mssql_store.handle_job_failure("job-1", TimeoutError("SQL timed out"))

    assert result["status"] == "retrying"
    assert result["retry_count"] == 1
    assert captured["job_id"] == "job-1"
    assert captured["retry_count"] == 1
    assert captured["retry_policy"] == "standard"


def test_handle_job_failure_keeps_apply_jobs_manual(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        mssql_store,
        "get_job",
        lambda job_id, include_params=False: {
            "id": job_id,
            "job_type": "inventory_apply_draft",
            "retry_count": 0,
            "max_retries": 0,
            "retry_policy": "none",
        },
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        mssql_store,
        "schedule_job_retry",
        lambda *args, **kwargs: pytest.fail("apply job should not schedule auto-retry"),
    )

    def _terminal(job_id: str, error_message: str, **kwargs):
        captured["job_id"] = job_id
        captured["error_message"] = error_message
        captured.update(kwargs)
        return True

    monkeypatch.setattr(mssql_store, "_mark_job_failure_terminal", _terminal)

    result = mssql_store.handle_job_failure("job-2", TimeoutError("deadlock victim"))

    assert result["status"] == "failure"
    assert captured["job_id"] == "job-2"
    assert captured["error_kind"] == "transient"


def test_handle_job_failure_stops_after_max_retries(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        mssql_store,
        "get_job",
        lambda job_id, include_params=False: {
            "id": job_id,
            "job_type": "sync_ads",
            "retry_count": 4,
            "max_retries": 4,
            "retry_policy": "standard",
        },
    )
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        mssql_store,
        "schedule_job_retry",
        lambda *args, **kwargs: pytest.fail("job after max retries should fail terminally"),
    )

    def _terminal(job_id: str, error_message: str, **kwargs):
        captured["job_id"] = job_id
        captured["error_message"] = error_message
        captured.update(kwargs)
        return True

    monkeypatch.setattr(mssql_store, "_mark_job_failure_terminal", _terminal)

    result = mssql_store.handle_job_failure("job-3", TimeoutError("connection reset"))

    assert result["status"] == "failure"
    assert captured["job_id"] == "job-3"
    assert captured["error_kind"] == "transient"


def test_problematic_manual_jobs_are_single_flight():
    assert {
        "content_apply_publish_mapping_suggestions",
        "content_refresh_product_type_definition",
        "family_matching_pipeline",
        "import_products_upload",
        "inventory_apply_draft",
        "inventory_taxonomy_refresh",
        "planning_refresh_actuals",
        "profit_ai_match_run",
        "returns_backfill_fba",
        "returns_sync_fba",
        "sync_listings_to_products",
        "cogs_import",
    }.issubset(mssql_store._SINGLE_FLIGHT_JOB_TYPES)


def test_enqueue_job_coalesces_single_flight_jobs(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(mssql_store, "_acquire_job_single_flight_lock", lambda job_type: object())
    monkeypatch.setattr(mssql_store, "_release_job_single_flight_lock", lambda lock_conn, job_type: None)
    monkeypatch.setattr(
        mssql_store,
        "_find_active_job_by_type",
        lambda job_type: {"id": "active-1", "status": "running"} if job_type == "courier_order_universe_linking" else None,
    )
    monkeypatch.setattr(
        mssql_store,
        "create_job",
        lambda **kwargs: pytest.fail("create_job should not be called when single-flight guard coalesces active job"),
    )
    monkeypatch.setattr(
        mssql_store,
        "get_job",
        lambda job_id, include_params=False: {"id": job_id, "job_type": "courier_order_universe_linking", "status": "running"},
    )

    result = mssql_store.enqueue_job(job_type="courier_order_universe_linking", params={})

    assert result["id"] == "active-1"
    assert result["status"] == "running"


def test_enqueue_job_coalesces_retrying_single_flight_jobs(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(mssql_store, "_acquire_job_single_flight_lock", lambda job_type: object())
    monkeypatch.setattr(mssql_store, "_release_job_single_flight_lock", lambda lock_conn, job_type: None)
    monkeypatch.setattr(
        mssql_store,
        "_find_active_job_by_type",
        lambda job_type: {"id": "retry-1", "status": "retrying"} if job_type == "inventory_taxonomy_refresh" else None,
    )
    monkeypatch.setattr(
        mssql_store,
        "create_job",
        lambda **kwargs: pytest.fail("create_job should not be called when retrying single-flight job exists"),
    )
    monkeypatch.setattr(
        mssql_store,
        "get_job",
        lambda job_id, include_params=False: {"id": job_id, "job_type": "inventory_taxonomy_refresh", "status": "retrying"},
    )

    result = mssql_store.enqueue_job(job_type="inventory_taxonomy_refresh", params={})

    assert result["id"] == "retry-1"
    assert result["status"] == "retrying"


def test_enqueue_job_allows_other_job_types_with_active_unrelated(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(mssql_store, "_acquire_job_single_flight_lock", lambda job_type: object())
    monkeypatch.setattr(mssql_store, "_release_job_single_flight_lock", lambda lock_conn, job_type: None)
    monkeypatch.setattr(
        mssql_store,
        "_find_active_job_by_type",
        lambda job_type: {"id": "active-1", "status": "running"} if job_type == "courier_order_universe_linking" else None,
    )
    monkeypatch.setattr(
        mssql_store,
        "create_job",
        lambda **kwargs: {"id": "new-1", "job_type": kwargs["job_type"]},
    )
    monkeypatch.setattr(mssql_store, "_spawn_job_runner", lambda *args, **kwargs: None)
    monkeypatch.setattr(mssql_store, "get_job", lambda job_id, include_params=False: {"id": job_id, "job_type": "sync_ads"})

    result = mssql_store.enqueue_job(job_type="sync_ads", params={})

    assert result["job_type"] == "sync_ads"


def test_default_job_queue_mapping():
    assert mssql_store._default_job_queue("courier_order_universe_linking") == "courier.heavy"
    assert mssql_store._default_job_queue("sync_fba_inventory") == "fba.medium"
    assert mssql_store._default_job_queue("sync_orders") == "core.medium"
    assert mssql_store._default_job_queue("unknown_job_type") == "light.default"


def test_enqueue_job_dispatches_to_celery_in_worker_canary(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(mssql_store, "_acquire_job_single_flight_lock", lambda job_type: object())
    monkeypatch.setattr(mssql_store, "_release_job_single_flight_lock", lambda lock_conn, job_type: None)
    monkeypatch.setattr(mssql_store, "_find_active_job_by_type", lambda job_type: None)
    monkeypatch.setattr(mssql_store, "create_job", lambda **kwargs: {"id": "new-2", "job_type": kwargs["job_type"]})
    monkeypatch.setattr(mssql_store, "get_job", lambda job_id, include_params=False: {"id": job_id, "job_type": "sync_fba_inventory"})
    monkeypatch.setattr(mssql_store, "_should_dispatch_via_worker", lambda job_type: True)
    captured: dict[str, object] = {}

    def _enqueue(job_id: str, job_type: str):
        captured["job_id"] = job_id
        captured["job_type"] = job_type

    monkeypatch.setattr(mssql_store, "_enqueue_celery_job", _enqueue)
    monkeypatch.setattr(mssql_store, "_spawn_job_runner", lambda *args, **kwargs: pytest.fail("inline runner should not be used"))

    result = mssql_store.enqueue_job(job_type="sync_fba_inventory", params={})

    assert result["job_type"] == "sync_fba_inventory"
    assert captured["job_id"] == "new-2"
    assert captured["job_type"] == "sync_fba_inventory"
