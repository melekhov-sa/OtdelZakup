"""Tests for standard analog normalization and lookup.

Test cases:
1. normalize_standard — ГОСТ/DIN/ISO/ГОСТ Р variants
2. normalize_standard — unknown prefix returns None
3. get_standard_analogs — direct lookup (src→dst and dst→src)
4. canonical_to_display — round-trip display form
5. MinHash matching finds DIN item when row has equivalent ГОСТ standard
"""

import pytest


# ── Test isolation fixture ────────────────────────────────────────────────────

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


# ── Test 1: normalize_standard — standard prefixes ────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("ГОСТ 7798-70",   "GOST-7798-70"),
    ("гост 7798-70",   "GOST-7798-70"),
    ("ГОСТ Р 52627",   "GOST-52627"),
    ("DIN 933",        "DIN-933"),
    ("DIN  931-A",     "DIN-931-A"),
    ("ISO 4017",       "ISO-4017"),
    ("ИСО 4017",       "ISO-4017"),
    ("iso4017",        "ISO-4017"),    # no space is fine: prefix stripped, code = "4017"
    ("ГОСТ7798",       "GOST-7798"),   # no space after prefix — still recognized
    ("", None),
    ("   ", None),
])
def test_normalize_standard_variants(raw, expected):
    from app.matching.standard_analogs import normalize_standard
    assert normalize_standard(raw) == expected


# ── Test 2: normalize_standard — unknown prefix ───────────────────────────────

def test_normalize_standard_unknown_prefix():
    from app.matching.standard_analogs import normalize_standard
    assert normalize_standard("TUV 1234") is None
    assert normalize_standard("EN 933") is None


# ── Test 3: get_standard_analogs — bidirectional lookup ──────────────────────

def _seed_equiv(src, dst, confidence=95):
    from app.database import get_db_session
    from app.models import StandardEquivalent
    session = get_db_session()
    try:
        session.add(StandardEquivalent(
            src_canonical=src, dst_canonical=dst,
            confidence=confidence, is_active=True,
        ))
        session.commit()
    finally:
        session.close()


def test_get_standard_analogs_src_to_dst():
    """Seeded GOST→DIN: lookup from GOST returns [DIN]."""
    from app.matching.standard_analogs import get_standard_analogs
    _seed_equiv("GOST-7798-70", "DIN-933")
    result = get_standard_analogs("GOST-7798-70")
    assert "DIN-933" in result


def test_get_standard_analogs_dst_to_src():
    """Seeded GOST→DIN: lookup from DIN also returns [GOST] (bidirectional)."""
    from app.matching.standard_analogs import get_standard_analogs
    _seed_equiv("GOST-7798-70", "DIN-933")
    result = get_standard_analogs("DIN-933")
    assert "GOST-7798-70" in result


def test_get_standard_analogs_inactive_excluded():
    """Inactive equivalents must not be returned."""
    from app.database import get_db_session
    from app.models import StandardEquivalent
    from app.matching.standard_analogs import get_standard_analogs
    session = get_db_session()
    try:
        session.add(StandardEquivalent(
            src_canonical="DIN-125", dst_canonical="ISO-7089",
            confidence=90, is_active=False,
        ))
        session.commit()
    finally:
        session.close()
    result = get_standard_analogs("DIN-125")
    assert "ISO-7089" not in result


def test_get_standard_analogs_empty_when_none():
    """No seeded data → empty list (no crash)."""
    from app.matching.standard_analogs import get_standard_analogs
    assert get_standard_analogs("GOST-9999-99") == []


# ── Test 4: canonical_to_display round-trip ───────────────────────────────────

@pytest.mark.parametrize("canonical,expected_display", [
    ("GOST-7798-70", "ГОСТ 7798 70"),
    ("DIN-933",      "DIN 933"),
    ("ISO-4017",     "ISO 4017"),
])
def test_canonical_to_display(canonical, expected_display):
    from app.matching.standard_analogs import canonical_to_display
    assert canonical_to_display(canonical) == expected_display


# ── Test 5: MinHash finds DIN item when row has equivalent ГОСТ ───────────────

def _seed_item(name, **kwargs):
    from app.database import get_db_session
    from app.models import InternalItem
    session = get_db_session()
    try:
        item = InternalItem(is_active=True, name=name, **kwargs)
        session.add(item)
        session.commit()
        return item.id
    finally:
        session.close()


def test_analog_match_finds_din_item_via_gost_row():
    """Row specifies ГОСТ 7798-70 → analog DIN-933 → finds DIN-indexed catalog item."""
    from app.matcher import add_internal_matches
    from app.matching.minhash_index import rebuild_index
    from app.match_settings import MatchSettings
    from app.database import get_db_session
    from app.models import InternalItem
    import pandas as pd

    # Seed catalog: bolt DIN 933 (no ГОСТ in name)
    _seed_item("Болт DIN 933 M12x60", item_type="болт", size="M12x60",
               standard_text="DIN 933", standard_key="DIN-933")
    _seed_item("Гайка DIN 934 M12", item_type="гайка", size="M12",
               standard_text="DIN 934", standard_key="DIN-934")

    # Seed analog mapping: ГОСТ 7798-70 ↔ DIN 933
    _seed_equiv("GOST-7798-70", "DIN-933", confidence=95)

    session = get_db_session()
    try:
        all_items = session.query(InternalItem).filter_by(is_active=True).all()
    finally:
        session.close()
    rebuild_index(all_items, num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=False)

    settings = MatchSettings(
        enable_minhash=True, lsh_threshold=0.05, num_perm=64, ngram_n=4,
        use_type_buckets=False, min_candidates_before_fallback=1,
        auto_apply_enabled=False,
        use_standard_analogs_in_main_match=True,
    )
    df = pd.DataFrame([{
        "item_type": "болт", "size": "M12x60",
        "gost": "ГОСТ 7798-70",
        "iso": "", "din": "",
        "diameter": "", "length": "",
        "strength": "", "coating": "",
        "name_raw": "Болт M12x60 ГОСТ 7798-70",
        "name":     "болт m12x60 гост 7798 70",
    }])

    df_out, results = add_internal_matches(df, settings=settings)
    mr = results[0]

    candidate_names = [c["name"] for c in mr.get("candidates", [])]
    assert any("DIN 933" in n or "din 933" in n.lower() for n in candidate_names), (
        f"Expected DIN 933 bolt in candidates via GOST analog; got: {candidate_names}"
    )


def test_analog_disabled_does_not_add_analog_candidates():
    """When use_standard_analogs_in_main_match=False, analog is not searched."""
    from app.matcher import add_internal_matches
    from app.matching.minhash_index import rebuild_index
    from app.match_settings import MatchSettings
    from app.database import get_db_session
    from app.models import InternalItem
    import pandas as pd

    # Only DIN item in catalog, row uses ГОСТ
    _seed_item("Болт DIN 933 M12x60 x", item_type="болт", size="M12x60",
               standard_text="DIN 933", standard_key="DIN-933")
    _seed_equiv("GOST-7798-70", "DIN-933", confidence=95)

    session = get_db_session()
    try:
        all_items = session.query(InternalItem).filter_by(is_active=True).all()
    finally:
        session.close()
    rebuild_index(all_items, num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=False)

    settings = MatchSettings(
        enable_minhash=True, lsh_threshold=0.05, num_perm=64, ngram_n=4,
        use_type_buckets=False, min_candidates_before_fallback=1,
        auto_apply_enabled=False,
        use_standard_analogs_in_main_match=False,  # disabled
    )
    df = pd.DataFrame([{
        "item_type": "болт", "size": "M12x60",
        "gost": "ГОСТ 7798-70",
        "iso": "", "din": "",
        "diameter": "", "length": "",
        "strength": "", "coating": "",
        "name_raw": "Болт M12x60 ГОСТ 7798-70",
        "name":     "болт m12x60 гост 7798 70",
    }])

    df_out, results = add_internal_matches(df, settings=settings)
    mr = results[0]

    # Without analog search, DIN item with unrelated text may still appear
    # (MinHash based on text similarity). We just check no via_analog flag.
    candidates = mr.get("candidates", [])
    via_analog_candidates = [c for c in candidates if c.get("via_analog")]
    assert via_analog_candidates == [], (
        f"Expected no via_analog candidates when disabled; got: {via_analog_candidates}"
    )
