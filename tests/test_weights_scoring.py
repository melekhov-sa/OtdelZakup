"""Tests for configurable weighted scoring, gating, and delta logic.

Covers:
1. Bolt M12x45 GOST 7798-70 — three candidates ranked correctly
2. Nut M20 vs bolt kit — gated by type + kit penalty
3. Normalization: Cyrillic M vs Latin M produce same scores
4. Diameter mismatch gate: M8 vs M12 gets heavy penalty
5. Delta logic: auto-match only when gap is large enough
6. Custom weights: changing w_type dominance
7. Settings persistence: save and load round-trip
"""

import pytest

from app.match_settings import MatchSettings, load_match_settings, save_match_settings
from app.matching.scorer import _is_kit, score_match
from app.models import InternalItem


def _item(**kwargs):
    defaults = dict(is_active=True, name="Test")
    defaults.update(kwargs)
    return InternalItem(**defaults)


def _default_settings():
    return MatchSettings()


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
    db_mod.DB_PATH      = db_path
    db_mod.engine       = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    db_mod.SessionLocal = sessionmaker(bind=db_mod.engine, autoflush=False, expire_on_commit=False)
    db_mod.init_db()


# ── 1. Three candidates ranked correctly ──────────────────────────────────────

class TestBoltM12x45ThreeCandidates:
    """Row: Bolt M12x45 GOST 7798-70.
    Candidates:
      A: Bolt M8x50 GOST 7798-70  — wrong diameter
      B: Bolt M12x45 GOST 7798-70 — exact match
      C: Bolt M12x55 GOST 7798-70 — same diam, different length
    Expected: B >> C >> A
    """

    def _row(self):
        return {
            "item_type": "болт", "size": "M12x45",
            "gost": "ГОСТ 7798-70", "iso": "", "din": "",
            "diameter": "", "length": "",
            "strength": "", "coating": "",
        }

    def _candidates(self):
        a = _item(name="Болт M8x50 ГОСТ 7798-70", item_type="болт", size="M8x50",
                  standard_key="GOST-7798-70", standard_text="ГОСТ 7798-70")
        b = _item(name="Болт M12x45 ГОСТ 7798-70", item_type="болт", size="M12x45",
                  standard_key="GOST-7798-70", standard_text="ГОСТ 7798-70")
        c = _item(name="Болт M12x55 ГОСТ 7798-70", item_type="болт", size="M12x55",
                  standard_key="GOST-7798-70", standard_text="ГОСТ 7798-70")
        return a, b, c

    def test_exact_match_highest(self):
        row = self._row()
        settings = _default_settings()
        a, b, c = self._candidates()
        sa = score_match(row, a, settings)["score"]
        sb = score_match(row, b, settings)["score"]
        sc = score_match(row, c, settings)["score"]
        assert sb >= 90, f"Exact match B should be >= 90, got {sb}"
        assert sb > sc, f"B ({sb}) should rank above C ({sc})"
        assert sc > sa, f"C ({sc}) should rank above A ({sa})"

    def test_wrong_diameter_penalized_heavily(self):
        """M8 vs M12 should get diameter mismatch penalty."""
        row = self._row()
        settings = _default_settings()
        a, _, _ = self._candidates()
        result = score_match(row, a, settings)
        assert result["score"] <= 15, (
            f"Wrong diameter should be <= 15, got {result['score']}; "
            f"breakdown={result['breakdown']}"
        )

    def test_same_diam_diff_length_below_exact(self):
        """M12x55 vs M12x45 — same diam, different length → below exact match."""
        row = self._row()
        settings = _default_settings()
        _, b, c = self._candidates()
        rb = score_match(row, b, settings)["score"]
        rc = score_match(row, c, settings)["score"]
        assert rc < rb, f"Diff length ({rc}) should be below exact ({rb})"
        assert rc >= 40, f"Diff length should be >= 40, got {rc}"


# ── 2. Nut M20 vs bolt kit ───────────────────────────────────────────────────

class TestNutVsBoltKit:
    def test_nut_vs_bolt_kit_low_score(self):
        """Гайка M20 vs Болт M20 комплект: гайка+шайба → very low score."""
        item = _item(
            name="Болт M20 комплект: гайка + шайба",
            item_type="болт", size="M20",
        )
        row = {
            "item_type": "гайка", "size": "M20",
            "gost": "", "iso": "", "din": "",
            "diameter": "", "length": "",
            "strength": "", "coating": "",
        }
        settings = _default_settings()
        result = score_match(row, item, settings)
        assert result["score"] <= 20, (
            f"Kit+type mismatch should be <= 20, got {result['score']}; "
            f"breakdown={result['breakdown']}"
        )

    def test_nut_vs_bolt_no_kit_still_penalized(self):
        """Гайка M20 vs Болт M20 (no kit) → type penalty applied."""
        item = _item(name="Болт M20 DIN 934", item_type="болт", size="M20")
        row = {
            "item_type": "гайка", "size": "M20",
            "gost": "", "iso": "", "din": "",
            "diameter": "", "length": "",
            "strength": "", "coating": "",
        }
        settings = _default_settings()
        result = score_match(row, item, settings)
        assert result["score"] <= 40, (
            f"Type mismatch should be <= 40, got {result['score']}"
        )


# ── 3. Normalization: Cyrillic M vs Latin M ───────────────────────────────────

class TestNormalization:
    def test_cyrillic_vs_latin_same_score(self):
        """'М12x60' (Cyrillic M) vs 'M12x60' (Latin M) → same item should match."""
        item = _item(name="Болт M12x60", item_type="болт", size="M12x60")
        row_cyr = {
            "item_type": "болт", "size": "М12x60",
            "gost": "", "iso": "", "din": "",
            "diameter": "", "length": "",
            "strength": "", "coating": "",
        }
        row_lat = {
            "item_type": "болт", "size": "M12x60",
            "gost": "", "iso": "", "din": "",
            "diameter": "", "length": "",
            "strength": "", "coating": "",
        }
        settings = _default_settings()
        s_cyr = score_match(row_cyr, item, settings)["score"]
        s_lat = score_match(row_lat, item, settings)["score"]
        assert s_cyr == s_lat, f"Cyrillic ({s_cyr}) and Latin ({s_lat}) sizes should score the same"
        assert s_cyr >= 85, f"Both should be >= 85, got {s_cyr}"


# ── 4. Diameter mismatch gate ─────────────────────────────────────────────────

class TestDiameterGate:
    def test_m8_vs_m12_gated(self):
        """M8 bolt vs M12 bolt — diameter mismatch penalty gates the candidate."""
        item = _item(name="Болт M12x80", item_type="болт", size="M12x80",
                     standard_key="GOST-7798-70")
        row = {
            "item_type": "болт", "size": "M8x50",
            "gost": "ГОСТ 7798-70", "iso": "", "din": "",
            "diameter": "", "length": "",
            "strength": "", "coating": "",
        }
        settings = _default_settings()
        result = score_match(row, item, settings)
        assert result["score"] <= 10, (
            f"Diameter mismatch should gate to <= 10, got {result['score']}"
        )


# ── 5. Custom weights ────────────────────────────────────────────────────────

class TestCustomWeights:
    def test_type_heavy_weight(self):
        """With w_type=80, type match dominates even without size match."""
        item = _item(name="Болт M20x100", item_type="болт", size="M20x100")
        row = {
            "item_type": "болт", "size": "M8x30",
            "gost": "", "iso": "", "din": "",
            "diameter": "", "length": "",
            "strength": "", "coating": "",
        }
        # Default: type has low weight, diameter mismatch kills score
        default_s = _default_settings()
        r_default = score_match(row, item, default_s)

        # Custom: type weight=80, reduce diameter penalty
        custom = MatchSettings(w_type=80, w_size=10, w_standard=5, w_text=5,
                               p_diameter_mismatch=10)
        r_custom = score_match(row, item, custom)
        assert r_custom["score"] > r_default["score"], (
            f"Custom weights (heavy type) should give higher score; "
            f"custom={r_custom['score']}, default={r_default['score']}"
        )


# ── 6. Settings persistence ──────────────────────────────────────────────────

class TestSettingsPersistence:
    def test_save_and_load_roundtrip(self):
        settings = MatchSettings(
            w_type=50, w_size=30, w_standard=15, w_text=5,
            p_type_mismatch=70, auto_match_delta=20,
        )
        save_match_settings(settings)
        loaded = load_match_settings()
        assert loaded.w_type == 50
        assert loaded.w_size == 30
        assert loaded.w_standard == 15
        assert loaded.w_text == 5
        assert loaded.p_type_mismatch == 70
        assert loaded.auto_match_delta == 20


# ── 7. Exact match score >= 90 ───────────────────────────────────────────────

class TestExactMatch:
    def test_bolt_exact_match_ge_90(self):
        """Bolt M16x50 GOST 7798-70 exact match → score >= 90."""
        item = _item(
            name="Болт ГОСТ 7798-70 M16x50",
            item_type="болт", size="M16x50",
            standard_key="GOST-7798-70", standard_text="ГОСТ 7798-70",
        )
        row = {
            "item_type": "болт", "size": "M16x50",
            "gost": "ГОСТ 7798-70", "iso": "", "din": "",
            "diameter": "", "length": "",
            "strength": "", "coating": "",
        }
        settings = _default_settings()
        result = score_match(row, item, settings)
        assert result["score"] >= 90, (
            f"Exact match should be >= 90, got {result['score']}; "
            f"breakdown={result['breakdown']}"
        )

    def test_no_penalty_in_breakdown(self):
        """Exact match should have no penalty in breakdown."""
        item = _item(
            name="Гайка M20 DIN 934",
            item_type="гайка", size="M20",
            standard_key="DIN-934",
        )
        row = {
            "item_type": "гайка", "size": "M20",
            "gost": "", "iso": "", "din": "DIN 934",
            "diameter": "", "length": "",
            "strength": "", "coating": "",
        }
        settings = _default_settings()
        result = score_match(row, item, settings)
        assert result["score"] >= 90
        assert "penalty" not in result["breakdown"], (
            f"No penalty expected; breakdown={result['breakdown']}"
        )


# ── 8. Standard conflict ─────────────────────────────────────────────────────

class TestStandardConflict:
    def test_different_standards_penalized(self):
        """Same type+size but different standard → heavily penalized."""
        item = _item(
            name="Болт M12x60 DIN 933",
            item_type="болт", size="M12x60",
            standard_key="DIN-933",
        )
        row = {
            "item_type": "болт", "size": "M12x60",
            "gost": "ГОСТ 7798-70", "iso": "", "din": "",
            "diameter": "", "length": "",
            "strength": "", "coating": "",
        }
        settings = _default_settings()
        result = score_match(row, item, settings)
        assert result["score"] <= 70, (
            f"Standard conflict should reduce score, got {result['score']}"
        )


# ── 9. Disk close-size match ─────────────────────────────────────────────────

class TestDiskCloseSize:
    def test_disk_close_size_ge_80(self):
        """Disk 125x22.2x1.6 vs 125x1,6x22mm → close-size match >= 80."""
        item = _item(
            name="Диск отрезной 125x1,6x22мм по металлу",
            item_type="диск отрезной",
            size="125x1,6x22мм",
        )
        row = {
            "item_type": "диск отрезной", "size": "125x22.2x1.6",
            "gost": "", "iso": "", "din": "",
            "diameter": "", "length": "",
            "strength": "", "coating": "",
        }
        settings = _default_settings()
        result = score_match(row, item, settings)
        assert result["score"] >= 80, (
            f"Close-size disk match should be >= 80, got {result['score']}"
        )
