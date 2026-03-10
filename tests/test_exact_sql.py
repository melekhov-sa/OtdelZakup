"""Tests for Stage 1 exact SQL matching (type + size_norm).

Covers:
1. Exact match found when type + size match → AUTO_EXACT
2. No exact match when type is missing → falls through to MinHash
3. No exact match when size is missing → falls through to MinHash
4. Standard match → score 100; standard mismatch → deduction (-20)
5. Standard analog → smaller deduction (-5)
6. Strength mismatch → deduction (-10)
7. Coating mismatch → deduction (-5)
8. auto_match_threshold controls AUTO_EXACT vs SUGGESTED
9. always_require_confirmation forces SUGGESTED even on exact hit
10. Exact SQL preferred over MinHash when both could match
"""

import pytest

from app.matcher import (
    MATCH_MODE_AUTO_EXACT,
    MATCH_MODE_AUTO_MINHASH,
    MATCH_MODE_NONE,
    MATCH_MODE_SUGGESTED,
    add_internal_matches,
)
from app.match_settings import MatchSettings
from app.models import InternalItem
from app.matching.minhash_index import rebuild_index


# ── Test isolation fixture ───────────────────────────────────────────────────

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


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_settings(**kw) -> MatchSettings:
    defaults = dict(
        enable_minhash=True,
        lsh_threshold=0.05,
        num_perm=64,
        minhash_top_k=20,
        ngram_n=4,
        use_type_buckets=False,
        min_candidates_before_fallback=1,
        auto_apply_enabled=True,
        auto_apply_jaccard_threshold=0.0,
        always_require_confirmation=False,
        auto_match_threshold=90,
        min_display_score=0,
    )
    defaults.update(kw)
    return MatchSettings(**defaults)


def _seed(session, name, **kwargs):
    from datetime import datetime, timezone
    item = InternalItem(
        name=name, is_active=True,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        **kwargs,
    )
    session.add(item)
    session.commit()
    return item


def _df(rows):
    import pandas as pd
    return pd.DataFrame(rows)


def _make_row(*, item_type="болт", size="M12x60", gost="", din="", iso="",
              strength="", coating="", name_raw=None):
    if name_raw is None:
        name_raw = f"{item_type} {size} {gost} {din} {iso}".strip()
    return {
        "name": name_raw.lower(),
        "name_raw": name_raw,
        "item_type": item_type,
        "size": size,
        "gost": gost,
        "iso": iso,
        "din": din,
        "diameter": "",
        "length": "",
        "strength": strength,
        "coating": coating,
    }


def _run_match(items, rows, settings=None):
    rebuild_index(items, num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=False)
    if settings is None:
        settings = _make_settings()
    df = _df(rows)
    _, results = add_internal_matches(df, settings=settings)
    return results


# ── 1. Exact match: type + size → AUTO_EXACT ────────────────────────────────

def test_exact_match_type_size_auto_exact():
    """Item with same type + size is found via exact SQL → AUTO_EXACT."""
    from app.database import get_db_session
    session = get_db_session()
    item = _seed(session, "Болт М12x60 ГОСТ 7798-70",
                 item_type="болт", size="M12x60",
                 standard_text="ГОСТ 7798-70", standard_key="GOST-7798-70")
    items = session.query(InternalItem).all()
    session.close()

    results = _run_match(items, [
        _make_row(item_type="болт", size="M12x60", gost="ГОСТ 7798-70"),
    ])
    r = results[0]
    assert r["mode"] == MATCH_MODE_AUTO_EXACT
    assert r["internal_item_id"] == item.id
    assert r["source"] == "exact"


# ── 2. No type → exact SQL skipped, falls to MinHash ────────────────────────

def test_no_type_skips_exact():
    """Without item_type, exact SQL stage is skipped."""
    from app.database import get_db_session
    session = get_db_session()
    item = _seed(session, "Болт М12x60 ГОСТ 7798",
                 item_type="болт", size="M12x60")
    items = session.query(InternalItem).all()
    session.close()

    results = _run_match(items, [
        _make_row(item_type="", size="M12x60",
                  name_raw="Болт М12x60 ГОСТ 7798"),
    ])
    r = results[0]
    # Should NOT be AUTO_EXACT (no type → exact SQL skipped)
    # Might be AUTO_MINHASH or SUGGESTED via MinHash fallback
    assert r["mode"] != MATCH_MODE_AUTO_EXACT or r["mode"] == MATCH_MODE_NONE


# ── 3. No size → exact SQL skipped ──────────────────────────────────────────

def test_no_size_skips_exact():
    """Without size, exact SQL stage is skipped."""
    from app.database import get_db_session
    session = get_db_session()
    _seed(session, "Болт М12x60", item_type="болт", size="M12x60")
    items = session.query(InternalItem).all()
    session.close()

    results = _run_match(items, [
        _make_row(item_type="болт", size="",
                  name_raw="Болт"),
    ])
    r = results[0]
    assert r["mode"] != MATCH_MODE_AUTO_EXACT


# ── 4. Standard match vs mismatch scoring ────────────────────────────────────

def test_exact_standard_match_score_100():
    """Exact match with matching standard → score 100."""
    from app.database import get_db_session
    session = get_db_session()
    _seed(session, "Болт М12x60 ГОСТ 7798-70",
          item_type="болт", size="M12x60",
          standard_key="GOST-7798-70")
    items = session.query(InternalItem).all()
    session.close()

    results = _run_match(items, [
        _make_row(item_type="болт", size="M12x60", gost="ГОСТ 7798-70"),
    ])
    r = results[0]
    assert r["mode"] == MATCH_MODE_AUTO_EXACT
    assert r["score"] == 100


def test_exact_standard_mismatch_excluded_without_analogs():
    """Without analogs, mismatched standard item is excluded from exact SQL results."""
    from app.database import get_db_session
    session = get_db_session()
    _seed(session, "Болт М12x60 DIN 931",
          item_type="болт", size="M12x60",
          standard_key="DIN-931")
    items = session.query(InternalItem).all()
    session.close()

    results = _run_match(items, [
        _make_row(item_type="болт", size="M12x60", gost="ГОСТ 7798-70"),
    ])
    r = results[0]
    # Without analogs, DIN item is filtered out from exact SQL
    assert r["mode"] != MATCH_MODE_AUTO_EXACT


def test_exact_standard_mismatch_with_analogs_score_deducted():
    """With analogs enabled, mismatched (non-analog) standard → score 80."""
    from app.database import get_db_session
    session = get_db_session()
    # DIN 931 is NOT an analog of ГОСТ 15589 (no entry in standard_equivalents)
    _seed(session, "Болт М12x60 ГОСТ 15589-70",
          item_type="болт", size="M12x60",
          standard_key="GOST-15589-70")
    items = session.query(InternalItem).all()
    session.close()

    settings = _make_settings(use_standard_analogs_in_main_match=True)
    results = _run_match(items, [
        _make_row(item_type="болт", size="M12x60", gost="ГОСТ 7798-70"),
    ], settings=settings)
    r = results[0]
    assert r["score"] == 80


# ── 5. Standard analog → smaller deduction ──────────────────────────────────

def test_exact_standard_analog_score():
    """Exact match with analog standard → score 95 (100 - 5)."""
    from app.database import get_db_session
    from app.models import StandardEquivalent
    session = get_db_session()
    _seed(session, "Болт М12x60 DIN 933",
          item_type="болт", size="M12x60",
          standard_key="DIN-933")
    session.add(StandardEquivalent(
        src_canonical="GOST-7798-70", dst_canonical="DIN-933", is_active=True,
    ))
    session.commit()
    items = session.query(InternalItem).all()
    session.close()

    settings = _make_settings(use_standard_analogs_in_main_match=True)
    results = _run_match(items, [
        _make_row(item_type="болт", size="M12x60", gost="ГОСТ 7798-70"),
    ], settings=settings)
    r = results[0]
    assert r["score"] == 95
    assert r["mode"] == MATCH_MODE_AUTO_EXACT


# ── 6. Strength mismatch → deduction ────────────────────────────────────────

def test_exact_strength_mismatch():
    """Strength mismatch deducts 10 points: 100 - 10 = 90."""
    from app.database import get_db_session
    session = get_db_session()
    _seed(session, "Болт М12x60 8.8",
          item_type="болт", size="M12x60",
          strength_class="10.9")
    items = session.query(InternalItem).all()
    session.close()

    results = _run_match(items, [
        _make_row(item_type="болт", size="M12x60", strength="8.8"),
    ])
    r = results[0]
    assert r["score"] == 90


# ── 7. Coating mismatch → deduction ─────────────────────────────────────────

def test_exact_coating_mismatch():
    """Coating mismatch deducts 5 points: 100 - 5 = 95."""
    from app.database import get_db_session
    session = get_db_session()
    _seed(session, "Болт М12x60 цинк",
          item_type="болт", size="M12x60",
          material_coating="горячий цинк")
    items = session.query(InternalItem).all()
    session.close()

    results = _run_match(items, [
        _make_row(item_type="болт", size="M12x60", coating="цинк"),
    ])
    r = results[0]
    assert r["score"] == 95


# ── 8. auto_match_threshold controls AUTO_EXACT vs SUGGESTED ────────────────

def test_exact_below_threshold_gives_suggested():
    """Score below auto_match_threshold → SUGGESTED, not AUTO_EXACT."""
    from app.database import get_db_session
    session = get_db_session()
    # Use same standard (ГОСТ 7798-70) but mismatched strength → score=90
    # Then add coating mismatch → score=85
    _seed(session, "Болт М12x60 ГОСТ 7798-70",
          item_type="болт", size="M12x60",
          standard_key="GOST-7798-70", strength_class="10.9",
          material_coating="горячий цинк")
    items = session.query(InternalItem).all()
    session.close()

    # strength mismatch (-10), coating mismatch (-5) → score=85
    # Set threshold to 95 so 85 < 95 → SUGGESTED
    settings = _make_settings(auto_match_threshold=95)
    results = _run_match(items, [
        _make_row(item_type="болт", size="M12x60", gost="ГОСТ 7798-70",
                  strength="8.8", coating="цинк"),
    ], settings=settings)
    r = results[0]
    assert r["score"] == 85
    assert r["mode"] == MATCH_MODE_SUGGESTED


# ── 9. always_require_confirmation → SUGGESTED on exact hit ──────────────────

def test_exact_always_require_confirmation():
    """always_require_confirmation=True forces SUGGESTED even on perfect exact match."""
    from app.database import get_db_session
    session = get_db_session()
    _seed(session, "Болт М12x60 ГОСТ 7798-70",
          item_type="болт", size="M12x60",
          standard_key="GOST-7798-70")
    items = session.query(InternalItem).all()
    session.close()

    settings = _make_settings(always_require_confirmation=True)
    results = _run_match(items, [
        _make_row(item_type="болт", size="M12x60", gost="ГОСТ 7798-70"),
    ], settings=settings)
    r = results[0]
    assert r["mode"] == MATCH_MODE_SUGGESTED
    assert r["score"] == 100


# ── 10. Exact SQL preferred over MinHash ─────────────────────────────────────

def test_exact_preferred_over_minhash():
    """When both exact and MinHash can match, exact wins (source='exact')."""
    from app.database import get_db_session
    session = get_db_session()
    item = _seed(session, "Гайка М10 DIN 934",
                 item_type="гайка", size="M10",
                 standard_text="DIN 934", standard_key="DIN-934")
    items = session.query(InternalItem).all()
    session.close()

    results = _run_match(items, [
        _make_row(item_type="гайка", size="M10", din="DIN 934",
                  name_raw="Гайка М10 DIN 934"),
    ])
    r = results[0]
    assert r["source"] == "exact"
    assert r["internal_item_id"] == item.id


# ── 11. Multiple exact matches sorted by score ──────────────────────────────

def test_exact_multiple_items_best_score_wins():
    """When multiple items match type+size, the one with highest score is chosen."""
    from app.database import get_db_session
    session = get_db_session()
    item_good = _seed(session, "Болт М16x80 ГОСТ 7798-70",
                      item_type="болт", size="M16x80",
                      standard_key="GOST-7798-70")
    _seed(session, "Болт М16x80 DIN 931",
          item_type="болт", size="M16x80",
          standard_key="DIN-931")
    items = session.query(InternalItem).all()
    session.close()

    results = _run_match(items, [
        _make_row(item_type="болт", size="M16x80", gost="ГОСТ 7798-70"),
    ])
    r = results[0]
    # item_good has matching standard (score=100), item_bad has mismatched (score=80)
    assert r["internal_item_id"] == item_good.id
    assert r["score"] == 100


# ── 12. match_debug contains exact stage info ────────────────────────────────

def test_exact_match_debug_fields():
    """match_debug should contain exact match stage diagnostics."""
    from app.database import get_db_session
    session = get_db_session()
    _seed(session, "Шпилька М20x200 ГОСТ 9066-75",
          item_type="шпилька", size="M20x200",
          standard_key="GOST-9066-75")
    items = session.query(InternalItem).all()
    session.close()

    results = _run_match(items, [
        _make_row(item_type="шпилька", size="M20x200", gost="ГОСТ 9066-75"),
    ])
    r = results[0]
    assert r["mode"] == MATCH_MODE_AUTO_EXACT
    dbg = r.get("match_debug", {})
    assert "applied_mode" in dbg
    assert dbg["applied_mode"] == "EXACT"
