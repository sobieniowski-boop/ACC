"""SQLAlchemy async engine + session factory."""
from __future__ import annotations

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from app.core.config import settings

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=600,       # recycle connections after 10 min (Azure SQL idle timeout)
    pool_pre_ping=True,     # check connection liveness before use
)

# ---------------------------------------------------------------------------
# Force READ UNCOMMITTED on every new raw DBAPI connection so that SELECT
# queries are never blocked by concurrent writers (e.g. backfill scripts).
# This is equivalent to adding WITH (NOLOCK) to every query.
# ---------------------------------------------------------------------------
@event.listens_for(engine.sync_engine, "connect")
def _set_read_uncommitted(dbapi_conn, connection_record):
    cursor = dbapi_conn.cursor()
    cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
    cursor.close()


AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


def get_pool_status() -> dict:
    """Zwraca metryki connection pool SQLAlchemy.

    Dostępne pola:
      - pool_size: ustawiony rozmiar puli
      - checked_in: połączenia dostępne (idle)
      - checked_out: połączenia aktualnie używane
      - overflow: dodatkowe połączenia powyżej pool_size
      - pool_max_overflow: konfiguracja max_overflow
    """
    pool = engine.sync_engine.pool
    return {
        "pool_size": pool.size(),
        "checked_in": pool.checkedin(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
        "pool_max_overflow": pool._max_overflow,
    }
