"""Tests for size normalization (Cyrillic→Latin), deduplication of near-identical
catalog items, and scoring correctness on the key problem cases.

Test cases
----------
1. normalize_size: Cyrillic М/Х variants all produce the same string as Latin.
2. parse_size_tokens: variant inputs produce the same token list.
3. standard_key: ГОСТ 15589-70 variants all map to the same canonical key.
4. Scoring rank: Болт M12x60 ГОСТ 15589-70 — M12x60 ranks above M10x45 with a big gap.
5. Scoring disk: 125×22.2×1.6 vs 125×1,6×22мм — close-size match gives score ≥ 60.
6. Dedup: two DB items "М 12x60" and "M12x60" collapse to one candidate in top-10.
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


# ── 1. normalize_size: Cyrillic variants → same canonical string ───────────────

def test_normalize_size_cyrillic_m_equals_latin_m():
    """М (Cyrillic) and M (Latin) must produce identical normalized strings."""
    from app.matching.normalizer import normalize_size

    variants = [
        "М12x60",    # Cyrillic М
        "M12x60",    # Latin M
        "М 12x60",   # Cyrillic М + space before digit
        "M 12x60",   # Latin M + space
        "М12х60",    # Cyrillic М and х
        "М12×60",    # Cyrillic М and Unicode ×
    ]
    results = [normalize_size(v) for v in variants]
    expected = "m12x60"
    for raw, norm in zip(variants, results):
        assert norm == expected, (
            f"normalize_size({raw!r}) → {norm!r}, expected {expected!r}"
        )


def test_normalize_size_decimal_comma():
    """Decimal comma and period produce the same normalized string."""
    from app.matching.normalizer import normalize_size
    assert normalize_size("4,2x70") == normalize_size("4.2x70"), (
        "Comma and period variants must normalize identically"
    )
    assert normalize_size("125x1,6x22мм") == normalize_size("125x1.6x22"), (
        "Trailing мм + comma must normalize correctly"
    )


# ── 2. parse_size_tokens: variant inputs → same token list ────────────────────

def test_parse_size_tokens_cyrillic_variants():
    """Cyrillic/Latin size variants produce identical token lists."""
    from app.matching.normalizer import normalize_size, parse_size_tokens

    pairs = [
        ("М 12x60",  "M12x60"),
        ("М12х80",   "M12x80"),
        ("125x1,6x22мм", "125x1.6x22"),
    ]
    for cyr, lat in pairs:
        t_cyr = parse_size_tokens(normalize_size(cyr))
        t_lat = parse_size_tokens(normalize_size(lat))
        assert sorted(t_cyr) == sorted(t_lat), (
            f"Tokens differ: {cyr!r} → {t_cyr}  vs  {lat!r} → {t_lat}"
        )


# ── 3. standard_key: ГОСТ variants → same canonical key ───────────────────────

def test_standard_key_gost_variants():
    """All common spellings of ГОСТ 15589-70 map to the same canonical key."""
    from app.standard_normalizer import standard_key_from_text

    variants = [
        "ГОСТ 15589-70",
        "GOST 15589-70",
        "гост15589-70",
        "ГОСТ15589-70",
    ]
    keys = [standard_key_from_text(v) for v in variants]
    assert all(k is not None for k in keys), (
        f"Some variants returned None: {dict(zip(variants, keys))}"
    )
    assert len(set(keys)) == 1, (
        f"Expected all variants to map to the same key; got {set(keys)}"
    )


# ── 4. Scoring rank: M12x60 >> M10x45 for same ГОСТ ──────────────────────────

def test_scoring_rank_m12x60_beats_m10x45_same_gost():
    """For query Болт M12x60 ГОСТ 15589-70 the M12x60 item must rank higher
    than M10x45 (same ГОСТ) by a substantial margin (≥ 40 pts gap)."""
    from app.matching.scorer import score_match
    from app.models import InternalItem

    def _item(name, size, std_key):
        return InternalItem(
            is_active=True, name=name,
            item_type="болт", size=size,
            standard_key=std_key, standard_text="ГОСТ 15589-70",
        )

    item_m12 = _item("Болт ГОСТ 15589-70 M12x60", "M12x60", "GOST-15589-70")
    item_m10 = _item("Болт ГОСТ 15589-70 M10x45", "M10x45", "GOST-15589-70")

    row = {
        "item_type": "болт",
        "size": "M12x60",
        "gost": "ГОСТ 15589-70",
        "iso": "", "din": "", "diameter": "", "length": "",
        "strength": "", "coating": "",
    }

    s_m12 = score_match(row, item_m12)["score"]
    s_m10 = score_match(row, item_m10)["score"]

    assert s_m12 >= 90, f"M12x60 item should score ≥ 90; got {s_m12}"
    assert s_m10 <= 50, f"M10x45 item should score ≤ 50 (size mismatch); got {s_m10}"
    assert s_m12 - s_m10 >= 40, (
        f"Gap must be ≥ 40; M12x60={s_m12}, M10x45={s_m10}"
    )


# ── 5. Scoring disk: close size (1.6 vs 1,6 reordered dimensions) ─────────────

def test_scoring_disk_close_size_reordered_dimensions():
    """Disk 125x22.2x1.6 vs catalog 125x1,6x22мм — dimensions reordered and
    slightly different (22.2 vs 22) — must give score ≥ 60 (close-size match)."""
    from app.matching.scorer import score_match
    from app.models import InternalItem

    item = InternalItem(
        is_active=True,
        name="Диск отрезной 125x1,6x22мм по металлу",
        item_type="диск отрезной",
        size="125x1,6x22мм",   # raw as stored — scorer normalises it
    )
    row = {
        "item_type": "диск отрезной",
        "size": "125x22.2x1.6",
        "gost": "", "iso": "", "din": "",
        "diameter": "", "length": "",
        "strength": "", "coating": "",
    }
    result = score_match(row, item)
    assert result["score"] >= 60, (
        f"Expected score ≥ 60 for close-size disk match; "
        f"got {result['score']}; reasons={result['reasons']}; warns={result['warn_reasons']}"
    )


# ── 6. Dedup: "М 12x60" and "M12x60" collapse to one candidate ────────────────

def test_dedup_cyrillic_vs_latin_size_collapsed_to_one_candidate():
    """Two DB items that differ only in Cyrillic/Latin М and spacing must appear
    as a single candidate in the top-10 list (no confusing duplicate entries)."""
    from app.matcher import add_internal_matches
    from app.matching.minhash_index import rebuild_index
    from app.match_settings import MatchSettings
    from app.database import get_db_session
    from app.models import InternalItem
    import pandas as pd

    _seed(
        "Болт М 12x60 ГОСТ 7798-70",
        item_type="болт", size="М 12x60",
        standard_key="GOST-7798-70", standard_text="ГОСТ 7798-70",
    )
    _seed(
        "Болт M12x60 ГОСТ 7798-70",
        item_type="болт", size="M12x60",
        standard_key="GOST-7798-70", standard_text="ГОСТ 7798-70",
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
        "item_type": "болт",
        "size": "M12x60",
        "gost": "ГОСТ 7798-70",
        "iso": "", "din": "", "diameter": "", "length": "",
        "strength": "", "coating": "",
        "name_raw": "Болт M12x60 ГОСТ 7798-70",
        "name": "болт m12x60 гост 7798-70",
    }])

    _df_out, results = add_internal_matches(df, settings=settings)
    candidates = results[0].get("candidates", [])

    assert len(candidates) == 1, (
        f"Expected exactly 1 candidate after dedup (two near-duplicate items); "
        f"got {len(candidates)}: {[c['name'] for c in candidates]}"
    )
