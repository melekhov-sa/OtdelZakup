"""Tests for diagnostic/debug features and zero-candidate fixes.

Test cases:
1. clean_excel_escapes strips _x0002_ sequences and control chars
2. Matching works without item_type — candidates found via keyword/size signals
3. Matching works without size — match found via type + keyword signals
4. match_debug dict present with all required diagnostic fields
5. Samorez "4.2 x 70" (spaces around x) normalizes and matches catalog item
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


def _seed(name, **kwargs):
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


# ── Test 1: clean_excel_escapes ───────────────────────────────────────────────

def test_cleanup_x0002_sequences():
    """_x0002_ escape sequences are removed; remaining text is parseable."""
    from app.matching.normalizer import clean_excel_escapes, normalize_size, parse_size_tokens

    # Typical openpyxl artefact: non-printable byte encoded as _xNNNN_
    dirty = "пресс_x0002_шайба 4.2x16"
    clean = clean_excel_escapes(dirty)
    assert "_x0002_" not in clean
    assert "шайба" in clean

    # Control chars (0x01–0x08 range) removed
    with_ctrl = "болт\x02 4.2x16"
    assert "\x02" not in clean_excel_escapes(with_ctrl)

    # normalize_size calls clean internally — size tokens must survive
    size_with_escape = "4.2_x0009_x16"
    normalized = normalize_size(size_with_escape)
    tokens = parse_size_tokens(normalized)
    assert 4.2 in tokens
    assert 16.0 in tokens


# ── Test 2: matching without item_type ───────────────────────────────────────

def test_match_runs_without_type():
    """Row with no item_type still gets candidates via MinHash text similarity."""
    from app.matcher import add_internal_matches
    from app.matching.minhash_index import rebuild_index
    from app.match_settings import MatchSettings
    from app.database import get_db_session
    from app.models import InternalItem
    import pandas as pd

    _seed("Болт M12x80 ГОСТ 7798", item_type="болт", size="M12x80")
    _seed("Гайка M12 DIN 934", item_type="гайка", size="M12")

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
    )
    df = pd.DataFrame([{
        "item_type": "",           # supplier did not provide type
        "size": "M12x80",
        "diameter": "", "length": "", "gost": "", "iso": "", "din": "",
        "strength": "", "coating": "",
        "name_raw": "Болт M12x80 ГОСТ 7798",
        "name":     "болт m12x80 гост 7798",
    }])

    df_out, results = add_internal_matches(df, settings=settings)
    mr = results[0]
    dbg = mr.get("match_debug", {})

    assert dbg.get("total_scanned") == 2
    assert len(mr.get("candidates", [])) > 0, (
        f"Expected ≥1 candidate without item_type; debug={dbg}"
    )


# ── Test 3: matching without size ────────────────────────────────────────────

def test_match_runs_without_size():
    """Row with no size still gets candidates via MinHash text similarity."""
    from app.matcher import add_internal_matches
    from app.matching.minhash_index import rebuild_index
    from app.match_settings import MatchSettings
    from app.database import get_db_session
    from app.models import InternalItem
    import pandas as pd

    _seed("Герметик силиконовый белый 310 мл", item_type="герметик", size="")

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
    )
    df = pd.DataFrame([{
        "item_type": "герметик",
        "size": "",                # no size info
        "diameter": "", "length": "", "gost": "", "iso": "", "din": "",
        "strength": "", "coating": "",
        "name_raw": "Герметик силиконовый белый 310 мл",
        "name":     "герметик силиконовый белый",
    }])

    df_out, results = add_internal_matches(df, settings=settings)
    mr = results[0]
    dbg = mr.get("match_debug", {})

    # Candidates must be returned
    assert len(mr.get("candidates", [])) > 0, (
        f"Expected ≥1 candidate without size; debug={dbg}"
    )


# ── Test 4: match_debug has all required diagnostic fields ───────────────────

def test_debug_contains_stage_counts():
    """match_debug is present on every result with the documented fields."""
    from app.matcher import add_internal_matches
    from app.matching.minhash_index import rebuild_index
    from app.match_settings import MatchSettings
    from app.database import get_db_session
    from app.models import InternalItem
    import pandas as pd

    _seed("Саморез СГД 4.2x70", item_type="саморез", size="4.2x70")
    _seed("Болт М10 DIN 931",    item_type="болт",    size="M10")

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
    )
    df = pd.DataFrame([{
        "item_type": "саморез", "size": "4.2x70",
        "diameter": "", "length": "", "gost": "", "iso": "", "din": "",
        "strength": "", "coating": "",
        "name_raw": "Саморез 4.2x70",
        "name":     "саморез 4.2x70",
    }])

    df_out, results = add_internal_matches(df, settings=settings)
    dbg = results[0].get("match_debug")

    assert dbg is not None, "match_debug must be present on every result"
    for key in ("total_scanned", "nonzero_scored", "top5_count", "best_score", "extracted"):
        assert key in dbg, f"match_debug missing key '{key}'"
    ex = dbg["extracted"]
    for key in ("item_type", "size_tokens", "keywords"):
        assert key in ex, f"extracted missing key '{key}'"

    assert dbg["total_scanned"] == 2


# ── Test 5: size with spaces around x matches catalog item ───────────────────

def test_samorez_space_variant_in_top5():
    """Size '4.2 x 70' (spaces around ×) normalises to '4.2x70' and matches catalog."""
    from app.matcher import add_internal_matches
    from app.matching.minhash_index import rebuild_index
    from app.match_settings import MatchSettings
    from app.database import get_db_session
    from app.models import InternalItem
    import pandas as pd

    _seed("Саморез СГД 4.2x70", item_type="саморез", size="4.2x70")

    # Build MinHash index so the new J-based decision logic can find the item
    session = get_db_session()
    try:
        all_items = session.query(InternalItem).filter_by(is_active=True).all()
    finally:
        session.close()
    rebuild_index(all_items, num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=True)

    df = pd.DataFrame([{
        "item_type": "саморез",
        "size": "4.2 x 70",   # spaces — common in supplier exports
        "diameter": "", "length": "", "gost": "", "iso": "", "din": "",
        "strength": "", "coating": "",
        "name_raw": "Саморез 4.2 x 70",
        "name":     "саморез 4.2x70",
    }])

    settings = MatchSettings(
        enable_minhash=True, lsh_threshold=0.05, num_perm=64, ngram_n=4,
        use_type_buckets=True, min_candidates_before_fallback=3,
        auto_apply_enabled=True, auto_apply_jaccard_threshold=0.0,
        min_display_score=0,  # disable display threshold for this low-J test
    )
    df_out, results = add_internal_matches(df, settings=settings)
    mr = results[0]

    candidate_names = [c["name"] for c in mr.get("candidates", [])]
    assert any("4.2" in n for n in candidate_names), (
        f"Expected саморез 4.2x70 in candidates; got {candidate_names}"
    )
    assert mr["mode"] in ("AUTO_SCORE", "SUGGESTED", "AUTO_MEMORY", "AUTO_MINHASH", "AUTO_EXACT"), (
        f"Expected AUTO/SUGGESTED after size normalisation; "
        f"mode={mr['mode']}, score={mr.get('score')}, candidates={candidate_names}"
    )
