"""Tests for on-disk MinHash cache behaviour and related startup seeds.

Covers:
1. save() persists state to a readable file and load() recovers it
2. save() writes atomically (no half-written pkl visible during crash)
3. save() swallows exceptions without raising
4. seed_catalog_version() creates the row exactly once (idempotent)
5. _cleanup_stale_file_caches removes old subdirs but preserves recent ones
   and the top-level minhash_index.pkl/.fp files
"""

import os
import pickle
import time
from pathlib import Path

import pytest

from app.matching import minhash_cache


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


# ── 1. Round-trip ─────────────────────────────────────────────────────────────


class TestSaveLoadRoundTrip:
    def test_save_then_load_same_fp_returns_state(self, tmp_path):
        cache_dir = tmp_path / "cache"
        fp = "abc123"
        state = {"minhashes": {1: "mh1", 2: "mh2"}, "lsh_all": None,
                 "lsh_by_type": {}, "item_types": {1: "болт"},
                 "num_perm": 128, "ngram_n": 4, "threshold": 0.3}

        minhash_cache.save(cache_dir, fp, state)

        assert (cache_dir / "minhash_index.pkl").exists()
        assert (cache_dir / "minhash_index.fp").exists()
        assert (cache_dir / "minhash_index.fp").read_text(encoding="utf-8") == fp

        loaded = minhash_cache.load(cache_dir, fp)
        assert loaded is not None
        assert loaded["minhashes"] == state["minhashes"]
        assert loaded["item_types"] == state["item_types"]

    def test_load_different_fp_returns_none(self, tmp_path):
        cache_dir = tmp_path / "cache"
        minhash_cache.save(cache_dir, "fp1", {"minhashes": {}})
        assert minhash_cache.load(cache_dir, "fp2") is None

    def test_load_missing_files_returns_none(self, tmp_path):
        assert minhash_cache.load(tmp_path / "empty", "fp") is None

    def test_invalidate_removes_fp_not_pkl(self, tmp_path):
        cache_dir = tmp_path / "cache"
        minhash_cache.save(cache_dir, "fp1", {"minhashes": {}})
        minhash_cache.invalidate(cache_dir)
        assert not (cache_dir / "minhash_index.fp").exists()
        assert (cache_dir / "minhash_index.pkl").exists()
        # subsequent load with any fp returns None because fp file is gone
        assert minhash_cache.load(cache_dir, "fp1") is None


# ── 2. Atomic write ───────────────────────────────────────────────────────────


class TestAtomicWrite:
    def test_no_tmp_leftover_after_success(self, tmp_path):
        cache_dir = tmp_path / "cache"
        minhash_cache.save(cache_dir, "fp", {"minhashes": {1: "x"}})
        # .tmp files must not remain after success
        leftovers = list(cache_dir.glob("*.tmp"))
        assert leftovers == []

    def test_existing_file_replaced_atomically(self, tmp_path):
        cache_dir = tmp_path / "cache"
        minhash_cache.save(cache_dir, "fp_v1", {"minhashes": {1: "v1"}})
        minhash_cache.save(cache_dir, "fp_v2", {"minhashes": {1: "v2"}})
        loaded = minhash_cache.load(cache_dir, "fp_v2")
        assert loaded["minhashes"][1] == "v2"
        assert list(cache_dir.glob("*.tmp")) == []


# ── 3. Error handling ─────────────────────────────────────────────────────────


class TestSaveSwallowsErrors:
    def test_unpicklable_state_does_not_raise(self, tmp_path, caplog):
        cache_dir = tmp_path / "cache"
        # lambdas are not picklable
        state = {"minhashes": {1: lambda: None}}
        minhash_cache.save(cache_dir, "fp", state)  # must not raise
        # Nothing was committed
        assert not (cache_dir / "minhash_index.pkl").exists()
        assert not (cache_dir / "minhash_index.fp").exists()
        # .tmp files cleaned up on failure
        assert list(cache_dir.glob("*.tmp")) == []


# ── 4. seed_catalog_version ───────────────────────────────────────────────────


class TestSeedCatalogVersion:
    def test_seeds_initial_value_when_missing(self):
        from app.database import get_catalog_version
        from app.seed import seed_catalog_version

        assert get_catalog_version() == 0  # no row yet
        seed_catalog_version()
        assert get_catalog_version() == 1

    def test_idempotent_when_already_set(self):
        from app.database import (
            get_catalog_version,
            increment_catalog_version,
        )
        from app.seed import seed_catalog_version

        seed_catalog_version()
        increment_catalog_version()
        increment_catalog_version()  # → 3
        assert get_catalog_version() == 3

        seed_catalog_version()  # must NOT reset
        assert get_catalog_version() == 3


# ── 5. _cleanup_stale_file_caches ─────────────────────────────────────────────


class TestCleanupStaleFileCaches:
    def _touch(self, path: Path, age_days: float) -> None:
        path.mkdir(parents=True, exist_ok=True)
        (path / "dummy.txt").write_text("x", encoding="utf-8")
        old = time.time() - age_days * 86400
        os.utime(path, (old, old))

    def test_removes_old_keeps_recent(self, tmp_path, monkeypatch):
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()

        import app.cache as cache_mod
        cache_mod.CACHE_DIR = cache_dir

        old_dir = cache_dir / "aaaaaaaaaaaaaaaa"
        fresh_dir = cache_dir / "bbbbbbbbbbbbbbbb"
        self._touch(old_dir, age_days=60)
        self._touch(fresh_dir, age_days=1)

        # Also create the protected top-level files
        (cache_dir / "minhash_index.pkl").write_bytes(b"data")
        (cache_dir / "minhash_index.fp").write_text("fp", encoding="utf-8")
        os.utime(cache_dir / "minhash_index.pkl",
                 (time.time() - 60 * 86400, time.time() - 60 * 86400))

        from app.main import _cleanup_stale_file_caches
        _cleanup_stale_file_caches(ttl_days=30)

        assert not old_dir.exists()
        assert fresh_dir.exists()
        # files at top level are NOT dirs and therefore NOT touched
        assert (cache_dir / "minhash_index.pkl").exists()
        assert (cache_dir / "minhash_index.fp").exists()

    def test_noop_when_cache_dir_missing(self, tmp_path, monkeypatch):
        import app.cache as cache_mod
        cache_mod.CACHE_DIR = tmp_path / "does_not_exist"
        from app.main import _cleanup_stale_file_caches
        _cleanup_stale_file_caches(ttl_days=30)  # must not raise
