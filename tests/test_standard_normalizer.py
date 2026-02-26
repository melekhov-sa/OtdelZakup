"""Tests for standard normalization and key-based matching.

- test_normalize_standard_token_din438_equals_din_438
- test_extract_standards_multiple_tokens
- test_internal_matching_works_with_din438_input_when_internal_has_din_438
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
    db_mod.DB_PATH       = db_path
    db_mod.engine        = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    db_mod.SessionLocal  = sessionmaker(bind=db_mod.engine, autoflush=False, expire_on_commit=False)
    db_mod.init_db()


# ── Helper ────────────────────────────────────────────────────────────────────

def _seed_internal_item(**kwargs):
    from app.database import get_db_session
    from app.models import InternalItem
    session = get_db_session()
    try:
        item = InternalItem(is_active=True, **kwargs)
        session.add(item)
        session.commit()
        return item.id
    finally:
        session.close()


# ── Test 1: "DIN438" and "DIN 438" normalize to the same key ─────────────────

def test_normalize_standard_token_din438_equals_din_438():
    from app.standard_normalizer import normalize_standard_token

    t1 = normalize_standard_token("DIN438")
    t2 = normalize_standard_token("DIN 438")

    assert t1 is not None, "DIN438 should produce a token"
    assert t2 is not None, "DIN 438 should produce a token"
    assert t1.key == t2.key == "DIN-438"
    assert t1.system == t2.system == "DIN"
    assert t1.number == t2.number == "438"
    assert t1.display == "DIN 438"
    assert t2.display == "DIN 438"


# ── Test 2: extract_standards finds multiple tokens ──────────────────────────

def test_extract_standards_multiple_tokens():
    from app.standard_normalizer import extract_standards

    tokens = extract_standards("DIN438/ ГОСТ1479 M10x20")

    keys = [t.key for t in tokens]
    assert "DIN-438" in keys
    assert "GOST-1479" in keys
    assert len(tokens) == 2

    # ISO should NOT appear here
    systems = {t.system for t in tokens}
    assert "ISO" not in systems


# ── Test 3: key-based matching works across format variants ──────────────────

def test_internal_matching_works_with_din438_input_when_internal_has_din_438():
    """Supplier row has din='DIN438', internal item has standard_key='DIN-438'.

    The score_candidate must award +30 via key-based match despite the
    different raw spelling (no space vs space).
    """
    from app.matcher import score_candidate
    from app.models import InternalItem

    # Internal item as if stored with standard_key computed from "DIN 438"
    item = InternalItem(
        name="Гайка М10 DIN 438",
        item_type="гайка",
        size="M10",
        standard_text="DIN 438",
        standard_key="DIN-438",   # pre-computed canonical key
        is_active=True,
    )

    # Row dict: din extractor would produce "DIN438" (no space variant)
    row = {
        "item_type": "гайка",
        "size": "M10",
        "diameter": "",
        "length": "",
        "gost": "",
        "iso": "",
        "din": "DIN438",   # no space — format differs from stored standard_text
        "strength": "",
        "coating": "",
    }

    s = score_candidate(row, item)

    # size(50) + item_type(20) + standard_key_match(30) = 100
    assert s >= 100, f"Expected score >= 100, got {s}"
    # Specifically verify standard match contributed
    # Row without din should score 70 (size+item_type only)
    row_no_std = dict(row)
    row_no_std["din"] = ""
    s_no_std = score_candidate(row_no_std, item)
    assert s - s_no_std == 30, (
        f"Standard key match should add exactly 30 points, "
        f"but score diff is {s - s_no_std} (with={s}, without={s_no_std})"
    )
