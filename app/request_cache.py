"""Simple thread-safe single-value TTL cache for rarely-changing DB data."""
from __future__ import annotations

import threading
import time
from typing import Any, Callable

_DEFAULT_TTL = 60.0  # seconds


class _Missing:
    __slots__ = ()


_MISSING = _Missing()


class TTLCache:
    """Thread-safe in-process cache for a single value with TTL expiry.

    Usage::

        _cache = TTLCache()

        def load_something():
            return _cache.get_or_load(_load_something_from_db)

        def _load_something_from_db():
            ...  # actual DB query

        def invalidate_something():
            _cache.invalidate()
    """

    __slots__ = ("_ttl", "_value", "_expires", "_lock")

    def __init__(self, ttl: float = _DEFAULT_TTL) -> None:
        self._ttl = ttl
        self._value: Any = _MISSING
        self._expires: float = 0.0
        self._lock = threading.Lock()

    def get_or_load(self, loader: Callable[[], Any]) -> Any:
        """Return cached value if still fresh; otherwise call loader() and cache result.

        DB I/O runs outside the lock so other threads are not blocked during reload.
        Two threads may both call loader() at expiry — last write wins, harmless.
        """
        now = time.monotonic()
        with self._lock:
            if self._value is not _MISSING and now < self._expires:
                return self._value
        value = loader()
        with self._lock:
            self._value = value
            self._expires = time.monotonic() + self._ttl
        return value

    def invalidate(self) -> None:
        """Force next get_or_load() to reload from DB."""
        with self._lock:
            self._value = _MISSING
            self._expires = 0.0
