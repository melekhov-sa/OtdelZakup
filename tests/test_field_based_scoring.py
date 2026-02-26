"""Tests for field-based scoring: on-the-fly item extraction, size/standard normalization,
high-confidence score for bolt with ГОСТ, keyword fallback when size is missing.

Test cases:
1. parse_internal_item_name extracts item_type, size, standard_text from name
2. normalize_size handles м/Cyrillic-x/comma/spaces variants correctly
3. standard_key_from_text normalises ГОСТ/DIN/ISO variants to canonical keys
4. score_match ≥ 90 for M8x50 ГОСТ bolt (catalog item has empty structural fields)
5. Keyword fallback: type + keyword match returns positive score when size absent
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


# ── Test 1: parse_internal_item_name extracts structural fields ───────────────

def test_parse_our_catalog_fields_from_name():
    """parse_internal_item_name returns item_type, size, standard_text for a bolt name."""
    from app.item_parser import parse_internal_item_name

    result = parse_internal_item_name("Болт M8x50 ГОСТ 7805-70")

    assert result["item_type"].lower() == "болт", (
        f"Expected item_type='болт', got {result['item_type']!r}"
    )
    # Size must include the key numeric components
    size_norm = result["size"].lower().replace(" ", "")
    assert "8" in size_norm and "50" in size_norm, (
        f"Expected size to contain 8 and 50, got {result['size']!r}"
    )
    assert result["standard_text"], "standard_text must not be empty"
    assert "7805" in result["standard_text"] or "гост" in result["standard_text"].lower(), (
        f"standard_text should reference ГОСТ 7805, got {result['standard_text']!r}"
    )
    assert result["parse_status"] in ("ok", "review"), (
        f"Expected parse_status=ok/review, got {result['parse_status']!r}"
    )


# ── Test 2: normalize_size handles all common variants ───────────────────────

def test_normalize_size_mxl_variants():
    """normalize_size collapses М/×/spaces/comma variants to canonical form."""
    from app.matching.normalizer import normalize_size, parse_size_tokens

    cases = [
        ("M8x50",       [8.0, 50.0]),
        ("М8х50",       [8.0, 50.0]),   # Cyrillic М and х
        ("M8 x 50",     [8.0, 50.0]),   # spaces around x
        ("8×50",        [8.0, 50.0]),   # Unicode ×
        ("4,2x70",      [4.2, 70.0]),   # decimal comma
        ("4.2 x 70 мм", [4.2, 70.0]),   # trailing мм + spaces
        ("125x1.6x22",  [125.0, 1.6, 22.0]),
    ]
    for raw, expected_tokens in cases:
        norm = normalize_size(raw)
        tokens = parse_size_tokens(norm)
        assert sorted(tokens) == sorted(expected_tokens), (
            f"normalize_size({raw!r}) → tokens {tokens}, expected {expected_tokens}"
        )


# ── Test 3: standard_key_from_text normalises ГОСТ/DIN/ISO variants ──────────

def test_normalize_standard_variants():
    """standard_key_from_text returns canonical key for common standard strings."""
    from app.standard_normalizer import standard_key_from_text

    cases = [
        "ГОСТ 7805-70",
        "гост7805-70",
        "GOST 7805-70",
    ]
    keys = {standard_key_from_text(s) for s in cases}
    assert None not in keys, f"Some cases returned None: {dict(zip(cases, [standard_key_from_text(s) for s in cases]))}"
    # All variants of the same standard should produce the same key
    assert len(keys) == 1, f"Expected all variants to map to the same key, got {keys}"

    # DIN variant
    din_key = standard_key_from_text("DIN 934")
    assert din_key is not None, "DIN 934 must produce a key"
    assert "934" in din_key, f"Key should contain '934', got {din_key!r}"


# ── Test 4: score ≥ 90 for bolt M8x50 ГОСТ when catalog item has no stored fields ──

def test_match_bolt_m8x50_gost_high_score():
    """On-the-fly extraction makes catalog item score ≥ 90 even if stored fields are empty."""
    from app.database import get_db_session
    from app.models import InternalItem
    from app.matching.scorer import score_match
    from app.standard_normalizer import standard_key_from_text

    # Create item with ONLY name set — no item_type, size, standard_key
    session = get_db_session()
    try:
        item = InternalItem(
            is_active=True,
            name="Болт M8x50 ГОСТ 7805-70",
            name_full="Болт M8x50 ГОСТ 7805-70",
            item_type=None,
            size=None,
            standard_key=None,
            standard_text=None,
        )
        session.add(item)
        session.commit()
        session.refresh(item)

        row_dict = {
            "item_type": "болт",
            "size": "M8x50",
            "gost": "ГОСТ 7805-70",
            "iso": "", "din": "",
            "strength": "", "coating": "",
            "name_raw": "Болт M8x50 ГОСТ 7805-70",
        }

        result = score_match(row_dict, item)
        assert result["score"] >= 90, (
            f"Expected score ≥ 90 for bolt M8x50 ГОСТ via on-the-fly extraction; "
            f"got {result['score']}; reasons={result['reasons']}; warns={result['warn_reasons']}"
        )
        # Must have fired at least type and size signals
        reasons_text = " ".join(result["reasons"])
        assert any("тип" in r or "размер" in r or "стандарт" in r for r in result["reasons"]), (
            f"Expected type/size/standard reason; got {result['reasons']}"
        )
    finally:
        session.close()


# ── Test 5: keyword fallback when size is missing ─────────────────────────────

def test_match_fallback_keywords_when_size_missing():
    """When row has no size, type + keyword signals produce positive score > 0."""
    from app.database import get_db_session
    from app.models import InternalItem
    from app.matching.scorer import score_match

    session = get_db_session()
    try:
        # Catalog item has both type and name keywords
        item = InternalItem(
            is_active=True,
            name="Герметик силиконовый прозрачный 310 мл",
            item_type="герметик",
            size=None,
        )
        session.add(item)
        session.commit()
        session.refresh(item)

        row_dict = {
            "item_type": "герметик",
            "size": "",      # no size
            "gost": "", "iso": "", "din": "",
            "strength": "", "coating": "",
            "name_raw": "Герметик силиконовый прозрачный 310 мл",
        }

        result = score_match(row_dict, item)
        assert result["score"] > 0, (
            f"Expected positive score via type+keywords fallback; "
            f"got score={result['score']}; reasons={result['reasons']}"
        )
        # Breakdown must be returned with at least one component
        assert result["breakdown"], f"Expected non-empty breakdown; got {result['breakdown']}"
        # Type must have matched
        assert any("тип" in r for r in result["reasons"]), (
            f"Expected type match reason; got {result['reasons']}"
        )
    finally:
        session.close()
