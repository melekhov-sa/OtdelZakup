"""Global test fixtures.

Resets module-level caches between tests so snapshots loaded from one test's
tmp_path DB do not leak into the next test with a different DB.
"""

import pytest


@pytest.fixture(autouse=True)
def _reset_module_caches():
    """Invalidate process-wide caches before AND after every test.

    Module-level state (catalog snapshot, in-memory MinHash index) is normally
    a feature — it avoids reloading 220k ORM rows on every request.  In the
    test suite, though, each test gets its own tmp_path database via the
    per-file ``_set_dirs`` fixture, so any cached snapshot from the previous
    test's DB would be stale by the time this test queries it.
    """
    from app import catalog_cache
    catalog_cache.invalidate()
    yield
    catalog_cache.invalidate()
