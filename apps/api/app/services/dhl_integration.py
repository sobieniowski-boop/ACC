from __future__ import annotations

from app.core.db_connection import connect_acc


def _connect():
    return connect_acc(autocommit=False, timeout=20)


def ensure_dhl_schema() -> None:
    """No-op: schema managed by Alembic migration eb011."""
    pass
