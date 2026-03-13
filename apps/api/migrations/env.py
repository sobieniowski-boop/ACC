"""Alembic env.py — MSSQL sync via pyodbc.

Używamy synchronicznego silnika (pyodbc) zamiast async (aioodbc) bo Alembic
nie potrzebuje async, a sync jest stabilniejszy dla operacji DDL na MSSQL.
"""
from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool

# Load app config
from app.core.config import settings
from app.core.database import Base

# Import all models so Alembic can detect them in autogenerate
import app.models  # noqa: F401

config = context.config

# We use a pyodbc creator function — no need to set sqlalchemy.url here.
# (Setting it would fail because % chars in the URL break configparser interpolation)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Tryb offline — generuje SQL bez połączenia z bazą."""
    context.configure(
        url=settings.DATABASE_URL_SYNC,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        render_as_batch=True,  # MSSQL wymaga batch mode dla ALTER TABLE
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Tryb online — łączy się z MSSQL i wykonuje migracje."""
    import pyodbc

    def _creator():
        return pyodbc.connect(settings.mssql_connection_string)

    connectable = create_engine(
        "mssql+pyodbc://",
        creator=_creator,
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            render_as_batch=True,  # MSSQL wymaga batch mode dla ALTER TABLE
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
