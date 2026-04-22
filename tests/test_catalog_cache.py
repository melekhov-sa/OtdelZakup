"""Tests for the process-wide catalog snapshot cache (app.catalog_cache).

Covers:
1. First call loads from DB; subsequent calls return the same list/dict object
2. A catalog_version bump causes a reload on the next get_snapshot()
3. invalidate() forces a reload on the next get_snapshot()
4. Cache returns an empty snapshot when the table is empty
5. _bump_catalog_version() (CRUD helper) invalidates the cache end-to-end
"""

import pytest

from app import catalog_cache
from app.database import get_db_session, increment_catalog_version
from app.models import InternalItem
from app.seed import seed_catalog_version


@pytest.fixture(autouse=True)
def _set_dirs(tmp_path, monkeypatch):
    upload_dir = tmp_path / "uploads"
    cache_dir = tmp_path / "cache"
    monkeypatch.setenv("OTDELZAKUP_UPLOAD_DIR", str(upload_dir))
    monkeypatch.setenv("OTDELZAKUP_CACHE_DIR", str(cache_dir))
    import app.cache as cache_mod
    cache_mod.UPLOAD_DIR = upload_dir
    cache_mod.CACHE_DIR = cache_dir

    db_path = tmp_path / "test.db"
    monkeypatch.setenv("OTDELZAKUP_DB_PATH", str(db_path))
    import app.database as db_mod
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    db_mod.DB_PATH = db_path
    db_mod.engine = create_engine(
        f"sqlite:///{db_path}", connect_args={"check_same_thread": False}
    )
    db_mod.SessionLocal = sessionmaker(
        bind=db_mod.engine, autoflush=False, expire_on_commit=False
    )
    db_mod.init_db()

    # Always start each test with a clean in-process snapshot
    catalog_cache.invalidate()


def _add_item(name: str, item_type: str = "болт", size: str = "M12") -> int:
    session = get_db_session()
    try:
        it = InternalItem(
            name=name,
            item_type=item_type,
            size=size,
            is_active=True,
        )
        session.add(it)
        session.commit()
        return it.id
    finally:
        session.close()


class TestSnapshotCaching:
    def test_first_call_loads_second_call_returns_same_object(self):
        seed_catalog_version()
        _add_item("Болт M12")
        _add_item("Гайка M12")

        items_1, by_id_1 = catalog_cache.get_snapshot()
        items_2, by_id_2 = catalog_cache.get_snapshot()

        # Same list and dict instance => no reload
        assert items_1 is items_2
        assert by_id_1 is by_id_2
        assert len(items_1) == 2

    def test_empty_catalog_returns_empty_snapshot(self):
        seed_catalog_version()
        items, by_id = catalog_cache.get_snapshot()
        assert items == []
        assert by_id == {}


class TestInvalidation:
    def test_explicit_invalidate_triggers_reload(self):
        seed_catalog_version()
        _add_item("Болт M12")
        items_1, _ = catalog_cache.get_snapshot()

        catalog_cache.invalidate()
        _add_item("Гайка M12")
        items_2, by_id_2 = catalog_cache.get_snapshot()

        assert items_1 is not items_2
        assert len(items_2) == 2
        assert any(it.name == "Гайка M12" for it in items_2)

    def test_catalog_version_bump_triggers_reload(self):
        seed_catalog_version()
        _add_item("Болт M12")
        items_1, _ = catalog_cache.get_snapshot()

        # Version bump (no invalidate() call) — next get_snapshot must reload
        increment_catalog_version()
        _add_item("Шайба 12")
        items_2, _ = catalog_cache.get_snapshot()

        assert items_1 is not items_2
        assert len(items_2) == 2

    def test_bump_helper_invalidates_cache_end_to_end(self):
        """Simulates the CRUD path: _bump_catalog_version must propagate to the snapshot."""
        from app.internal_item_routes import _bump_catalog_version

        seed_catalog_version()
        _add_item("Болт M12")
        items_1, _ = catalog_cache.get_snapshot()

        _bump_catalog_version()  # what every CRUD/sync call uses
        _add_item("Гайка M12")
        items_2, _ = catalog_cache.get_snapshot()

        assert items_1 is not items_2
        assert len(items_2) == 2


class TestDetachedObjects:
    def test_objects_are_usable_after_session_closed(self):
        seed_catalog_version()
        _add_item("Болт M12", item_type="болт", size="M12")

        items, by_id = catalog_cache.get_snapshot()
        # The loading session is closed inside get_snapshot();
        # column values must still be accessible
        assert items[0].name == "Болт M12"
        assert items[0].item_type == "болт"
        assert items[0].size == "M12"
        assert items[0].is_active is True
        assert by_id[items[0].id] is items[0]
