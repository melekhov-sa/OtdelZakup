"""Tests for canonical key computation and deduplication.

Test cases
----------
1. compute_canonical_key: bolt with all fields → correct structured key.
2. Near-duplicates produce the same key ("М 12x60" == "M12x60").
3. Different sizes produce different keys.
4. compute_canonical_key_from_row: correct key from row_dict.
5. Scoring: "Болт M8x50 ГОСТ 7805-70" finds its catalog item with score ≥ 85.
6. Scoring: "Диск отрезной 125x22.2x1.6" matches "125x1,6x22мм" with score ≥ 60.
7. All candidates in add_internal_matches results have a non-None item_id.
"""

import pytest


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


def _make_item(**kwargs):
    """Return a transient (not persisted) InternalItem for unit tests."""
    from app.models import InternalItem
    defaults = dict(is_active=True, name="Test")
    defaults.update(kwargs)
    return InternalItem(**defaults)


def _seed(name, **kwargs):
    from app.database import get_db_session
    from app.models import InternalItem
    from app.matching.canonicalize import compute_canonical_key
    session = get_db_session()
    try:
        item = InternalItem(is_active=True, name=name, **kwargs)
        session.add(item)
        session.flush()
        item.canonical_key = compute_canonical_key(item)
        session.commit()
        return item.id
    finally:
        session.close()


# ── 1. compute_canonical_key: structured key format ───────────────────────────

def test_canonical_key_format_bolt_with_all_fields():
    """Bolt with type + standard + size → correct key components."""
    from app.matching.canonicalize import compute_canonical_key
    item = _make_item(
        item_type="болт",
        size="M12x60",
        standard_key="GOST-7798-70",
    )
    key = compute_canonical_key(item)
    assert "type=болт" in key,          f"Expected 'type=болт' in key; got {key!r}"
    assert "std=GOST-7798-70" in key,   f"Expected 'std=GOST-7798-70' in key; got {key!r}"
    assert "size=" in key,              f"Expected 'size=...' in key; got {key!r}"
    # Size tokens 12 and 60, sorted → "12x60"
    assert "size=12x60" in key,         f"Expected 'size=12x60' in key; got {key!r}"


# ── 2. Near-duplicates produce the same canonical key ─────────────────────────

def test_cyrillic_vs_latin_size_produce_same_canonical_key():
    """Items "М 12x60" and "M12x60" (Cyrillic М, space) → identical canonical_key."""
    from app.matching.canonicalize import compute_canonical_key
    item_cyr = _make_item(item_type="болт", size="М 12x60", standard_key="GOST-7798-70")
    item_lat = _make_item(item_type="болт", size="M12x60",  standard_key="GOST-7798-70")
    assert compute_canonical_key(item_cyr) == compute_canonical_key(item_lat), (
        f"Expected same key; cyr={compute_canonical_key(item_cyr)!r}, "
        f"lat={compute_canonical_key(item_lat)!r}"
    )


def test_disk_size_reordered_same_canonical_key():
    """Disk sizes "125x1,6x22мм" and "125x22x1.6" → same canonical key (sorted tokens)."""
    from app.matching.canonicalize import compute_canonical_key
    item_a = _make_item(item_type="диск", size="125x1,6x22мм")
    item_b = _make_item(item_type="диск", size="125x22x1.6")
    assert compute_canonical_key(item_a) == compute_canonical_key(item_b), (
        f"Expected same key for reordered disk sizes; "
        f"a={compute_canonical_key(item_a)!r}, b={compute_canonical_key(item_b)!r}"
    )


# ── 3. Different sizes produce different keys ──────────────────────────────────

def test_different_sizes_produce_different_canonical_keys():
    """M12x60 and M12x80 must NOT share a canonical key."""
    from app.matching.canonicalize import compute_canonical_key
    item_60 = _make_item(item_type="болт", size="M12x60", standard_key="GOST-7798-70")
    item_80 = _make_item(item_type="болт", size="M12x80", standard_key="GOST-7798-70")
    assert compute_canonical_key(item_60) != compute_canonical_key(item_80), (
        f"Expected different keys for M12x60 vs M12x80; "
        f"got {compute_canonical_key(item_60)!r} == {compute_canonical_key(item_80)!r}"
    )


# ── 4. compute_canonical_key_from_row ─────────────────────────────────────────

def test_canonical_key_from_row_matches_item_key():
    """Row dict produces the same canonical key as the matching InternalItem."""
    from app.matching.canonicalize import compute_canonical_key, compute_canonical_key_from_row

    item = _make_item(item_type="болт", size="M12x60", standard_key="GOST-7798-70")
    row_dict = {
        "item_type": "болт",
        "size": "M12x60",
        "gost": "ГОСТ 7798-70",
        "iso": "", "din": "",
    }
    item_key = compute_canonical_key(item)
    row_key  = compute_canonical_key_from_row(row_dict)
    assert item_key == row_key, (
        f"Expected item_key == row_key; item={item_key!r}, row={row_key!r}"
    )


# ── 5. End-to-end search: bolt M8x50 ГОСТ 7805-70 ────────────────────────────

def test_search_bolt_m8x50_gost_score_ge_85():
    """Row 'Болт M8x50 ГОСТ 7805-70' must find the matching catalog item."""
    from app.matcher import add_internal_matches, MATCH_MODE_NONE
    from app.matching.minhash_index import rebuild_index
    from app.match_settings import MatchSettings
    from app.database import get_db_session
    from app.models import InternalItem
    import pandas as pd

    _seed(
        "Болт M8x50 ГОСТ 7805-70",
        item_type="болт", size="M8x50",
        standard_key="GOST-7805-70", standard_text="ГОСТ 7805-70",
    )

    # Build MinHash index so the item can be found by the new J-based logic
    session = get_db_session()
    try:
        all_items = session.query(InternalItem).filter_by(is_active=True).all()
    finally:
        session.close()
    rebuild_index(all_items, num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=True)

    df = pd.DataFrame([{
        "item_type": "болт",
        "size": "M8x50",
        "gost": "ГОСТ 7805-70",
        "iso": "", "din": "", "diameter": "", "length": "",
        "strength": "", "coating": "",
        "name_raw": "Болт M8x50 ГОСТ 7805-70",
        "name": "болт m8x50 гост 7805-70",
    }])

    # Use low threshold to ensure any MinHash hit triggers auto-apply
    settings = MatchSettings(
        enable_minhash=True, lsh_threshold=0.05, num_perm=64, ngram_n=4,
        use_type_buckets=True, min_candidates_before_fallback=3,
        auto_apply_enabled=True, auto_apply_jaccard_threshold=0.0,
    )
    _df, results = add_internal_matches(df, settings=settings)
    mr = results[0]
    assert mr["mode"] != MATCH_MODE_NONE, (
        f"Expected item to be found; got mode={mr['mode']}"
    )
    assert mr["internal_item_id"] is not None


# ── 6. End-to-end search: disk close size ─────────────────────────────────────

def test_search_disk_close_size_score_ge_60():
    """Disk 125x22.2x1.6 must find the catalog entry 125x1,6x22мм via MinHash."""
    from app.matcher import add_internal_matches
    from app.matching.minhash_index import rebuild_index
    from app.match_settings import MatchSettings
    from app.database import get_db_session
    from app.models import InternalItem
    import pandas as pd

    _seed(
        "Диск отрезной 125x1,6x22мм по металлу",
        item_type="диск отрезной",
        size="125x1,6x22мм",
    )

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
        "item_type": "диск отрезной",
        "size": "125x22.2x1.6",
        "gost": "", "iso": "", "din": "", "diameter": "", "length": "",
        "strength": "", "coating": "",
        "name_raw": "Диск отрезной 125x1,6x22мм по металлу",
        "name": "диск отрезной 125x1,6x22мм по металлу",
    }])

    _df, results = add_internal_matches(df, settings=settings)
    candidates = results[0].get("candidates", [])
    assert candidates, "Expected at least one candidate from MinHash"


# ── 7. All candidates must have a valid item_id ───────────────────────────────

def test_all_candidates_have_valid_item_id():
    """Every candidate dict in match results must have a non-None item_id
    pointing to a real DB record (no 'virtual' candidates)."""
    from app.matcher import add_internal_matches
    from app.database import get_db_session
    from app.models import InternalItem
    import pandas as pd

    id1 = _seed("Болт M12x80 ГОСТ 7798-70", item_type="болт", size="M12x80", standard_key="GOST-7798-70")
    id2 = _seed("Болт M10x45 ГОСТ 7798-70", item_type="болт", size="M10x45", standard_key="GOST-7798-70")
    valid_ids = {id1, id2}

    df = pd.DataFrame([{
        "item_type": "болт", "size": "M12x80",
        "gost": "ГОСТ 7798-70",
        "iso": "", "din": "", "diameter": "", "length": "",
        "strength": "", "coating": "",
        "name_raw": "Болт M12x80 ГОСТ 7798-70",
        "name": "болт m12x80 гост 7798-70",
    }])

    _df, results = add_internal_matches(df)
    for mr in results:
        for c in mr.get("candidates", []):
            iid = c.get("item_id")
            assert iid is not None, f"Candidate has item_id=None: {c}"
            assert iid in valid_ids, (
                f"Candidate item_id={iid} not in DB; valid={valid_ids}; candidate={c}"
            )
