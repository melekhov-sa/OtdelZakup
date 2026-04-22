"""Process-wide in-memory snapshot of active catalog items.

The matcher, /api endpoints and batch /transform each need the full list of
active `InternalItem` rows + a `{id → item}` lookup.  Without a cache every
request re-loads all ~220k rows into a fresh session — at ~3 KB per ORM
object that is hundreds of megabytes of transient allocation per request.
Under Windows the process-level RSS grows steadily because the allocator
rarely returns freed pages to the OS.

This module keeps a single snapshot in module globals, tagged with the
catalog_version at load time.  Callers get back the **same** list/dict on
every call while the version is unchanged, so RAM is allocated once per
process and per version bump — not once per request.

Thread-safety: the snapshot is rebuilt under a Lock so concurrent first
callers don't start two parallel DB loads.  Readers see the previous
snapshot until the new one fully replaces it (atomic tuple assignment).

Detachment: after loading we call `session.expunge_all()` and close the
session.  The returned ORM objects are detached but still expose all their
column values via normal attribute access — which is what every caller in
this codebase needs (no lazy-loaded relationships are touched).
"""
from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.models import InternalItem

logger = logging.getLogger(__name__)


_lock = threading.Lock()
_snapshot: tuple[list, dict] | None = None
_snapshot_version: int = -1


def get_snapshot() -> tuple[list["InternalItem"], dict[int, "InternalItem"]]:
    """Return (all_items, item_by_id) for the current catalog version.

    Cached across calls. A catalog_version bump (via
    `app.database.increment_catalog_version()` or explicit `invalidate()`)
    triggers a reload on the next call.
    """
    global _snapshot, _snapshot_version

    from app.database import get_catalog_version

    current_ver = get_catalog_version()
    snap = _snapshot
    if snap is not None and _snapshot_version == current_ver:
        return snap

    with _lock:
        # Re-check after acquiring lock (another thread may have just reloaded)
        snap = _snapshot
        if snap is not None and _snapshot_version == current_ver:
            return snap

        from app.database import get_db_session
        from app.models import InternalItem

        session = get_db_session()
        try:
            items = session.query(InternalItem).filter_by(is_active=True).all()
            session.expunge_all()
        finally:
            session.close()

        by_id = {it.id: it for it in items}
        new_snap = (items, by_id)
        _snapshot = new_snap
        _snapshot_version = current_ver
        logger.info(
            "Catalog snapshot loaded: %d items, version=%d", len(items), current_ver,
        )
        return new_snap


def invalidate() -> None:
    """Drop the cached snapshot so the next get_snapshot() reloads from DB."""
    global _snapshot, _snapshot_version
    with _lock:
        _snapshot = None
        _snapshot_version = -1
