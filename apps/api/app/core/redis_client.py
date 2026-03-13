"""Redis async client."""
from __future__ import annotations

import asyncio

import redis.asyncio as aioredis

from app.core.config import settings

_redis: aioredis.Redis | None = None
_redis_loop: asyncio.AbstractEventLoop | None = None


def _create_redis_client() -> aioredis.Redis:
    url = settings.REDIS_URL
    use_ssl = url.startswith("rediss://")
    return aioredis.from_url(
        url,
        encoding="utf-8",
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
        socket_keepalive=True,
        retry_on_timeout=True,
        health_check_interval=30,
        **({"ssl_cert_reqs": "none"} if use_ssl else {}),
    )


async def get_redis() -> aioredis.Redis:
    global _redis, _redis_loop

    current_loop = asyncio.get_running_loop()
    if _redis is None or _redis_loop is None or _redis_loop.is_closed() or _redis_loop is not current_loop:
        if _redis is not None:
            try:
                await _redis.aclose()
            except Exception:
                pass
        _redis = _create_redis_client()
        _redis_loop = current_loop
    return _redis


async def close_redis() -> None:
    global _redis, _redis_loop
    if _redis:
        await _redis.aclose()
        _redis = None
    _redis_loop = None
