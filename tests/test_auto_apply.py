"""Tests for MinHash-based auto-apply logic.

Covers:
1. auto_apply_enabled=True + best_J >= threshold  => AUTO_MINHASH
2. auto_apply_enabled=True + best_J <  threshold  => SUGGESTED
3. auto_apply_enabled=False                        => SUGGESTED or NONE (never AUTO)
4. No candidates                                   => NONE
5. Deduplication of minhash_raw by item_id
6. Settings persistence (auto_apply_enabled, auto_apply_jaccard_threshold)
7. always_require_confirmation overrides AUTO_MINHASH → SUGGESTED
"""

import pytest

from app.matcher import (
    MATCH_MODE_AUTO_EXACT,
    MATCH_MODE_AUTO_MINHASH,
    MATCH_MODE_NONE,
    MATCH_MODE_SUGGESTED,
    _dedup_minhash_raw,
    add_internal_matches,
)
from app.match_settings import MatchSettings, load_match_settings, save_match_settings
from app.models import InternalItem
from app.matching.minhash_index import rebuild_index


# ── Test isolation fixture ─────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _set_dirs(tmp_path, monkeypatch):
    upload_dir = tmp_path / "uploads"
    cache_dir  = tmp_path / "cache"
    monkeypatch.setenv("OTDELZAKUP_UPLOAD_DIR", str(upload_dir))
    monkeypatch.setenv("OTDELZAKUP_CACHE_DIR",  str(cache_dir))
    import app.cache as cache_mod
    cache_mod.UPLOAD_DIR = upload_dir
    cache_mod.CACHE_DIR  = cache_dir

    db_path = tmp_path / "test.db"
    monkeypatch.setenv("OTDELZAKUP_DB_PATH", str(db_path))
    import app.database as db_mod
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    db_mod.DB_PATH = db_path
    db_mod.engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    db_mod.SessionLocal = sessionmaker(bind=db_mod.engine, autoflush=False, expire_on_commit=False)
    db_mod.init_db()


# ── Helper ─────────────────────────────────────────────────────────────────────

def _make_settings(**kw) -> MatchSettings:
    """Build a MatchSettings with MinHash enabled and defaults, overriding with kw."""
    defaults = dict(
        enable_minhash=True,
        lsh_threshold=0.1,          # low threshold to find candidates easily
        num_perm=64,
        minhash_top_k=20,
        ngram_n=4,
        use_type_buckets=True,
        min_candidates_before_fallback=3,
        auto_apply_enabled=True,
        auto_apply_jaccard_threshold=0.55,
        always_require_confirmation=False,
    )
    defaults.update(kw)
    return MatchSettings(**defaults)


def _seed_catalog(session, items_data: list[dict]) -> list[InternalItem]:
    """Insert InternalItem rows into the test DB."""
    from datetime import datetime, timezone
    items = []
    for d in items_data:
        item = InternalItem(
            name=d["name"],
            item_type=d.get("item_type", ""),
            size=d.get("size", ""),
            standard_text=d.get("standard_text", ""),
            is_active=True,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        session.add(item)
    session.flush()
    session.commit()
    for item in session.query(InternalItem).all():
        items.append(item)
    return items


def _transform_df(rows: list[dict]):
    """Build a minimal transformed DataFrame for add_internal_matches()."""
    import pandas as pd
    return pd.DataFrame(rows)


# ── 1. auto_apply_enabled=True + best_J >= threshold → AUTO_MINHASH ──────────

class TestAutoApplyEnabled:
    def test_auto_applied_when_j_above_threshold(self, tmp_path):
        """High-J MinHash hit → AUTO_MINHASH mode."""
        from app.database import get_db_session
        session = get_db_session()
        items = _seed_catalog(session, [
            {"name": "Саморез 4.8x35 по дереву", "item_type": "саморез", "size": "4.8x35"},
        ])
        session.close()

        # Build index with very low threshold so the item is found
        rebuild_index(items, num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=True)

        # Manually craft a settings with threshold=0.0 to guarantee J >= threshold
        settings = _make_settings(auto_apply_enabled=True, auto_apply_jaccard_threshold=0.0)

        df = _transform_df([{
            "name": "Саморез 4.8x35", "name_raw": "Саморез 4.8x35",
            "item_type": "саморез", "size": "4.8x35",
            "gost": "", "iso": "", "din": "", "diameter": "", "length": "",
            "strength": "", "coating": "",
        }])
        _, results = add_internal_matches(df, settings=settings)
        assert len(results) == 1
        r = results[0]
        assert r["mode"] in (MATCH_MODE_AUTO_MINHASH, MATCH_MODE_AUTO_EXACT), f"Expected AUTO, got {r['mode']}"
        assert r["internal_item_id"] == items[0].id
        assert r["source"] in ("minhash", "exact")

    def test_auto_not_applied_when_j_below_threshold(self, tmp_path):
        """Low-J hit below threshold → SUGGESTED, not AUTO."""
        from app.database import get_db_session
        session = get_db_session()
        items = _seed_catalog(session, [
            {"name": "Болт М12x60 ГОСТ 7798", "item_type": "болт", "size": "M12x60"},
        ])
        session.close()
        rebuild_index(items, num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=True)

        # Set a very high threshold so J will be < threshold
        settings = _make_settings(auto_apply_enabled=True, auto_apply_jaccard_threshold=0.99)

        df = _transform_df([{
            "name": "Болт М12x60", "name_raw": "Болт М12x60",
            "item_type": "болт", "size": "M12x60",
            "gost": "", "iso": "", "din": "", "diameter": "", "length": "",
            "strength": "", "coating": "",
        }])
        _, results = add_internal_matches(df, settings=settings)
        assert len(results) == 1
        r = results[0]
        # Either SUGGESTED, NONE, or AUTO_EXACT (exact SQL bypasses jaccard threshold)
        assert r["mode"] in (MATCH_MODE_SUGGESTED, MATCH_MODE_NONE, MATCH_MODE_AUTO_EXACT), (
            f"Expected SUGGESTED, NONE or AUTO_EXACT, got {r['mode']}"
        )
        assert r["mode"] != MATCH_MODE_AUTO_MINHASH


# ── 2. auto_apply_enabled=False → never AUTO_MINHASH ─────────────────────────

class TestAutoApplyDisabled:
    def test_disabled_never_auto(self, tmp_path):
        """With auto_apply_enabled=False, result must never be AUTO_MINHASH."""
        from app.database import get_db_session
        session = get_db_session()
        items = _seed_catalog(session, [
            {"name": "Гайка М10 DIN 934", "item_type": "гайка", "size": "M10"},
        ])
        session.close()
        rebuild_index(items, num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=True)

        # Even with threshold=0.0, if disabled, no auto
        settings = _make_settings(auto_apply_enabled=False, auto_apply_jaccard_threshold=0.0)

        df = _transform_df([{
            "name": "Гайка М10", "name_raw": "Гайка М10",
            "item_type": "гайка", "size": "M10",
            "gost": "", "iso": "", "din": "", "diameter": "", "length": "",
            "strength": "", "coating": "",
        }])
        _, results = add_internal_matches(df, settings=settings)
        assert len(results) == 1
        r = results[0]
        assert r["mode"] != MATCH_MODE_AUTO_MINHASH, (
            f"Expected not AUTO_MINHASH when disabled, got {r['mode']}"
        )


# ── 3. No candidates → NONE ───────────────────────────────────────────────────

class TestNoCandidates:
    def test_empty_catalog_returns_none(self, tmp_path):
        """Empty catalog → NONE mode."""
        rebuild_index([], num_perm=64, threshold=0.3, ngram_n=4)
        settings = _make_settings(auto_apply_enabled=True, auto_apply_jaccard_threshold=0.55)

        df = _transform_df([{
            "name": "Винт М6x20", "name_raw": "Винт М6x20",
            "item_type": "винт", "size": "M6x20",
            "gost": "", "iso": "", "din": "", "diameter": "", "length": "",
            "strength": "", "coating": "",
        }])
        _, results = add_internal_matches(df, settings=settings)
        assert len(results) == 1
        assert results[0]["mode"] == MATCH_MODE_NONE


# ── 4. always_require_confirmation overrides AUTO_MINHASH → SUGGESTED ────────

class TestAlwaysRequireConfirmation:
    def test_confirmation_flag_overrides_auto(self, tmp_path):
        """always_require_confirmation=True forces AUTO_MINHASH → SUGGESTED."""
        from app.database import get_db_session
        session = get_db_session()
        items = _seed_catalog(session, [
            {"name": "Анкер М16x200", "item_type": "анкер", "size": "M16x200"},
        ])
        session.close()
        rebuild_index(items, num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=True)

        settings = _make_settings(
            auto_apply_enabled=True,
            auto_apply_jaccard_threshold=0.0,   # would normally trigger AUTO
            always_require_confirmation=True,
        )
        df = _transform_df([{
            "name": "Анкер М16x200", "name_raw": "Анкер М16x200",
            "item_type": "анкер", "size": "M16x200",
            "gost": "", "iso": "", "din": "", "diameter": "", "length": "",
            "strength": "", "coating": "",
        }])
        _, results = add_internal_matches(df, settings=settings)
        assert len(results) == 1
        r = results[0]
        assert r["mode"] == MATCH_MODE_SUGGESTED, (
            f"Expected SUGGESTED due to always_require_confirmation, got {r['mode']}"
        )


# ── 5. match_debug contains MinHash diagnostics ───────────────────────────────

class TestMatchDebug:
    def test_debug_has_jaccard_fields(self, tmp_path):
        """match_debug should contain best_jaccard, applied_mode, threshold_used."""
        from app.database import get_db_session
        session = get_db_session()
        items = _seed_catalog(session, [
            {"name": "Дюбель 10x80", "item_type": "дюбель", "size": "10x80"},
        ])
        session.close()
        rebuild_index(items, num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=True)

        settings = _make_settings(auto_apply_jaccard_threshold=0.55)
        df = _transform_df([{
            "name": "Дюбель 10x80", "name_raw": "Дюбель 10x80",
            "item_type": "дюбель", "size": "10x80",
            "gost": "", "iso": "", "din": "", "diameter": "", "length": "",
            "strength": "", "coating": "",
        }])
        _, results = add_internal_matches(df, settings=settings)
        dbg = results[0].get("match_debug", {})
        assert "best_jaccard" in dbg
        assert "applied_mode" in dbg
        assert "threshold_used" in dbg
        assert "top_minhash_candidates" in dbg
        assert dbg["threshold_used"] in (0.55, 90)  # jaccard threshold or auto_match_threshold
        assert dbg["applied_mode"] in ("AUTO", "SUGGEST", "NONE", "EXACT")

    def test_top_minhash_candidates_max_5(self, tmp_path):
        """top_minhash_candidates should have at most 5 entries."""
        from app.database import get_db_session
        session = get_db_session()
        items_data = [
            {"name": f"Болт М12x{40 + i * 10}", "item_type": "болт"}
            for i in range(8)
        ]
        items = _seed_catalog(session, items_data)
        session.close()
        rebuild_index(items, num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=True)

        settings = _make_settings(auto_apply_jaccard_threshold=0.99)
        df = _transform_df([{
            "name": "Болт М12x40", "name_raw": "Болт М12x40",
            "item_type": "болт", "size": "M12x40",
            "gost": "", "iso": "", "din": "", "diameter": "", "length": "",
            "strength": "", "coating": "",
        }])
        _, results = add_internal_matches(df, settings=settings)
        dbg = results[0].get("match_debug", {})
        assert len(dbg.get("top_minhash_candidates", [])) <= 5


# ── 6. Deduplication of minhash_raw ──────────────────────────────────────────

class TestDeduplication:
    def test_dedup_keeps_highest_jaccard(self):
        """_dedup_minhash_raw should collapse duplicate item_ids, keeping max J."""
        raw = [
            {"item_id": 1, "name": "Болт", "jaccard": 0.42},
            {"item_id": 2, "name": "Гайка", "jaccard": 0.61},
            {"item_id": 1, "name": "Болт", "jaccard": 0.55},  # dup of id=1
        ]
        deduped = _dedup_minhash_raw(raw)
        ids = [r["item_id"] for r in deduped]
        assert ids.count(1) == 1, "item_id=1 should appear exactly once"
        assert ids.count(2) == 1, "item_id=2 should appear exactly once"
        # item_id=1 should have max(0.42, 0.55) = 0.55
        item1 = next(r for r in deduped if r["item_id"] == 1)
        assert item1["jaccard"] == 0.55

    def test_dedup_sorted_by_jaccard_desc(self):
        """_dedup_minhash_raw output should be sorted by jaccard descending."""
        raw = [
            {"item_id": 3, "name": "C", "jaccard": 0.3},
            {"item_id": 1, "name": "A", "jaccard": 0.7},
            {"item_id": 2, "name": "B", "jaccard": 0.5},
        ]
        deduped = _dedup_minhash_raw(raw)
        jaccards = [r["jaccard"] for r in deduped]
        assert jaccards == sorted(jaccards, reverse=True)

    def test_dedup_empty(self):
        assert _dedup_minhash_raw([]) == []

    def test_dedup_no_duplicates(self):
        """No duplicates → same order, sorted by jaccard desc."""
        raw = [
            {"item_id": 1, "name": "A", "jaccard": 0.8},
            {"item_id": 2, "name": "B", "jaccard": 0.6},
        ]
        deduped = _dedup_minhash_raw(raw)
        assert [r["item_id"] for r in deduped] == [1, 2]


# ── 7. Settings persistence ───────────────────────────────────────────────────

class TestAutoApplySettings:
    def test_settings_roundtrip(self):
        """auto_apply_enabled and auto_apply_jaccard_threshold persist to DB."""
        s = MatchSettings(
            auto_apply_enabled=False,
            auto_apply_jaccard_threshold=0.72,
        )
        save_match_settings(s)
        loaded = load_match_settings()
        assert loaded.auto_apply_enabled is False
        assert abs(loaded.auto_apply_jaccard_threshold - 0.72) < 1e-9

    def test_settings_default_values(self):
        """Default MatchSettings values match expected defaults."""
        s = MatchSettings()
        assert s.auto_apply_enabled is True
        assert s.auto_apply_jaccard_threshold == 0.40
