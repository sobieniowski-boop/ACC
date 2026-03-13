"""Generic TTL-based in-memory cache.

Replaces ad-hoc caching patterns scattered across services with a single,
thread-safe implementation that works identically in sync and async contexts.

Usage:
    from app.platform.shared.cache import ttl_cache

    _mkt_cache = ttl_cache(ttl_seconds=3600)

    def get_marketplace_data(mkt_id: str) -> dict:
        cached = _mkt_cache.get(mkt_id)
        if cached is not None:
            return cached
        data = _expensive_query(mkt_id)
        _mkt_cache.set(mkt_id, data)
        return data
"""
from __future__ import annotations

import threading
import time
from typing import Any


class TTLCache:
    """Thread-safe TTL cache with optional max-size eviction."""

    __slots__ = ("_data", "_lock", "_ttl", "_max_size")

    def __init__(self, ttl_seconds: int = 300, max_size: int = 1024) -> None:
        self._data: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()
        self._ttl = ttl_seconds
        self._max_size = max_size

    def get(self, key: str) -> Any | None:
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            expires_at, value = entry
            if time.monotonic() > expires_at:
                del self._data[key]
                return None
            return value

    def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        ttl = ttl_seconds if ttl_seconds is not None else self._ttl
        with self._lock:
            if len(self._data) >= self._max_size and key not in self._data:
                self._evict_expired()
                if len(self._data) >= self._max_size:
                    oldest_key = min(self._data, key=lambda k: self._data[k][0])
                    del self._data[oldest_key]
            self._data[key] = (time.monotonic() + ttl, value)

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def _evict_expired(self) -> None:
        now = time.monotonic()
        expired = [k for k, (exp, _) in self._data.items() if now > exp]
        for k in expired:
            del self._data[k]

    def __len__(self) -> int:
        with self._lock:
            self._evict_expired()
            return len(self._data)


def ttl_cache(ttl_seconds: int = 300, max_size: int = 1024) -> TTLCache:
    """Factory for creating a TTL cache instance."""
    return TTLCache(ttl_seconds=ttl_seconds, max_size=max_size)
