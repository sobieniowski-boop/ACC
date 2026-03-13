"""Skrypt seedujący dane deweloperskie — bezpieczny do uruchomienia wielokrotnie.

Tworzy:
  - Użytkownika admin dev@acc.local (jeśli nie istnieje)
  - Wpisy marketplace DE, PL, FR (jeśli tabela istnieje i brak wpisów)

Uruchomienie:
    cd apps/api
    python -m scripts.seed_dev_data
"""
from __future__ import annotations

import sys
import os

# Dodaj ścieżkę apps/api do sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import bcrypt
import structlog

from app.core.db_connection import connect_acc

log = structlog.get_logger("seed_dev_data")

DEV_USER_EMAIL = "dev@acc.local"
DEV_USER_PASSWORD = "DevAdmin123!"
DEV_USER_ROLE = "admin"


def _hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _seed_admin_user(conn) -> None:
    """Tworzy użytkownika admin jeśli nie istnieje."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM acc_user WITH (NOLOCK) WHERE email = %s",
        (DEV_USER_EMAIL,),
    )
    count = cursor.fetchone()[0]
    if count > 0:
        log.info("seed.admin_user_exists", email=DEV_USER_EMAIL)
        return

    import uuid

    user_id = str(uuid.uuid4())
    hashed = _hash_password(DEV_USER_PASSWORD)
    cursor.execute(
        """
        INSERT INTO acc_user (id, email, hashed_password, role, full_name, is_active)
        VALUES (%s, %s, %s, %s, %s, 1)
        """,
        (user_id, DEV_USER_EMAIL, hashed, DEV_USER_ROLE, "Dev Admin"),
    )
    conn.commit()
    log.info("seed.admin_user_created", email=DEV_USER_EMAIL, role=DEV_USER_ROLE)


def _table_exists(cursor, table_name: str) -> bool:
    cursor.execute(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WITH (NOLOCK) "
        "WHERE TABLE_NAME = %s",
        (table_name,),
    )
    return cursor.fetchone()[0] > 0


def _seed_marketplaces(conn) -> None:
    """Tworzy przykładowe rekordy marketplace jeśli tabela istnieje."""
    cursor = conn.cursor()
    if not _table_exists(cursor, "acc_marketplace"):
        log.info("seed.marketplace_table_missing — pomijam")
        return

    marketplaces = [
        ("A1PA6795UKMFR9", "DE", "Amazon.de", "EUR", "Europe/Berlin"),
        ("A1C3SOZRARQ6R3", "PL", "Amazon.pl", "PLN", "Europe/Warsaw"),
        ("A13V1IB3VIYZZH", "FR", "Amazon.fr", "EUR", "Europe/Paris"),
    ]
    for mp_id, code, name, currency, tz in marketplaces:
        cursor.execute(
            "SELECT COUNT(*) FROM acc_marketplace WITH (NOLOCK) WHERE code = %s",
            (code,),
        )
        if cursor.fetchone()[0] > 0:
            log.info("seed.marketplace_exists", code=code)
            continue
        cursor.execute(
            """
            INSERT INTO acc_marketplace (id, code, name, currency, timezone, is_active)
            VALUES (%s, %s, %s, %s, %s, 1)
            """,
            (mp_id, code, name, currency, tz),
        )
        log.info("seed.marketplace_created", code=code)

    conn.commit()


def main() -> None:
    log.info("seed.start")
    conn = connect_acc()
    try:
        _seed_admin_user(conn)
        _seed_marketplaces(conn)
    finally:
        conn.close()
    log.info("seed.done")


if __name__ == "__main__":
    main()
