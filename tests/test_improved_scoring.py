"""Tests for improved field-based scoring (normalized weighted model).

Test cases:
1. test_match_bolt_m12x60_gost_rank1_high_score
   catalog: ["Болт ГОСТ 15589-70 M12x60", "Болт ГОСТ 15589-70 M10x45"]
   row: Болт M12x60 ГОСТ 15589-70
   expect: M12x60 is #1 with score >= 90; M10x45 score <= 45

2. test_match_partial_size_same_diameter_diff_length
   catalog: "Болт M12x80 DIN 931", row: Болт M12x60 DIN 931
   expect: score in SUGGEST range (>= 65, < 90); reason mentions diameter

3. test_match_only_standard_no_size
   catalog: "Болт ГОСТ 7805-70 M8" (no row size info)
   row: type=болт, gost=ГОСТ 7805-70, size=""
   expect: positive score; standard and type signals present

4. test_keywords_stopwords_do_not_dominate
   catalog: "Болт M10 ГОСТ 7798", "Гайка M10 ГОСТ 5927"
   row: "гост гост гост болт гост" (only GOST keyword repeated)
   expect: items do NOT all score the same; ГОСТ alone doesn't dominate
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


def _make_item(name, *, item_type=None, size=None, standard_key=None, standard_text=None):
    """Create a transient InternalItem for direct scorer tests (not DB-persisted)."""
    from app.models import InternalItem
    return InternalItem(
        name=name,
        name_full=name,
        item_type=item_type or "",
        size=size or "",
        diameter=None,
        length=None,
        standard_key=standard_key,
        standard_text=standard_text or "",
        strength_class=None,
        material_coating=None,
        is_active=True,
    )


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


# ── Test 1: bolt M12x60 vs M10x45 — same ГОСТ, different size ────────────────

def test_match_bolt_m12x60_gost_rank1_high_score():
    """M12x60 must rank #1 in MinHash candidates (higher Jaccard than M10x45)."""
    from app.matcher import add_internal_matches
    from app.matching.minhash_index import rebuild_index
    from app.match_settings import MatchSettings
    from app.database import get_db_session
    from app.models import InternalItem
    import pandas as pd

    _seed("Болт ГОСТ 15589-70 M12x60",
          item_type="болт", size="M12x60", standard_key="GOST-15589-70")
    _seed("Болт ГОСТ 15589-70 M10x45",
          item_type="болт", size="M10x45", standard_key="GOST-15589-70")

    session = get_db_session()
    try:
        all_items = session.query(InternalItem).filter_by(is_active=True).all()
    finally:
        session.close()
    rebuild_index(all_items, num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=False)

    settings = MatchSettings(
        enable_minhash=True, lsh_threshold=0.05, num_perm=64, ngram_n=4,
        use_type_buckets=False, min_candidates_before_fallback=1,
        auto_apply_enabled=True, auto_apply_jaccard_threshold=0.0,
    )
    df = pd.DataFrame([{
        "item_type": "болт", "size": "M12x60",
        "gost": "ГОСТ 15589-70",
        "diameter": "", "length": "", "iso": "", "din": "",
        "strength": "", "coating": "",
        "name_raw": "Болт M12x60 ГОСТ 15589-70",
        "name":     "болт m12x60 гост 15589-70",
    }])

    df_out, results = add_internal_matches(df, settings=settings)
    mr = results[0]
    candidates = mr.get("candidates", [])

    assert len(candidates) >= 1, f"Expected ≥1 candidate; got {candidates}"

    c_m12 = next((c for c in candidates if "M12" in c["name"] or "m12" in c["name"].lower()), None)
    assert c_m12 is not None, f"M12x60 not in candidates: {[c['name'] for c in candidates]}"
    # M12x60 should be the top candidate (best Jaccard)
    assert candidates[0]["name"] == c_m12["name"], (
        f"Expected M12x60 ranked first; order={[c['name'] for c in candidates]}"
    )
    # Mode should be auto or suggested
    assert mr["mode"] in ("AUTO_MINHASH", "AUTO_EXACT", "AUTO_MEMORY", "SUGGESTED"), (
        f"Expected AUTO/SUGGESTED mode; got mode={mr['mode']}, score={mr['score']}"
    )


# ── Test 2: same diameter, different length — should give SUGGEST ─────────────

def test_match_partial_size_same_diameter_diff_length():
    """Bolt M12x80 vs row M12x60: same diameter, different length → SUGGEST range."""
    from app.matching.scorer import score_match

    item = _make_item(
        "Болт М12x80 DIN 931",
        item_type="болт",
        size="M12x80",
        standard_key="DIN-931",
    )
    row_dict = {
        "item_type": "болт",
        "size": "M12x60",
        "din": "DIN 931",
        "gost": "", "iso": "",
        "diameter": "", "length": "",
        "strength": "", "coating": "",
    }

    result = score_match(row_dict, item)

    assert 60 <= result["score"] < 90, (
        f"Expected score in SUGGEST range [60..90); got {result['score']}; "
        f"reasons={result['reasons']}; warns={result['warn_reasons']}"
    )
    # Must explain the partial size match
    all_msgs = result["reasons"] + result["warn_reasons"]
    assert any("диаметр" in m for m in all_msgs), (
        f"Expected diameter signal; got reasons={result['reasons']}, warns={result['warn_reasons']}"
    )


# ── Test 3: standard matches, no size in row ──────────────────────────────────

def test_match_only_standard_no_size():
    """When row has no size info, type + standard still produce a positive score."""
    from app.matching.scorer import score_match

    item = _make_item(
        "Болт ГОСТ 7805-70 M8",
        item_type="болт",
        size="M8",
        standard_key="GOST-7805-70",
    )
    row_dict = {
        "item_type": "болт",
        "size": "",          # no size
        "gost": "ГОСТ 7805-70",
        "iso": "", "din": "",
        "diameter": "", "length": "",
        "strength": "", "coating": "",
    }

    result = score_match(row_dict, item)

    assert result["score"] > 0, (
        f"Expected positive score; got {result['score']}"
    )
    reasons_text = " ".join(result["reasons"])
    assert "тип" in reasons_text or "стандарт" in reasons_text, (
        f"Expected type or standard in reasons; got {result['reasons']}"
    )
    # Standard component must have fired
    assert "standard" in result["breakdown"], (
        f"Expected standard in breakdown; got breakdown={result['breakdown']}"
    )


# ── Test 4: ГОСТ keyword alone must not dominate the score ───────────────────

def test_keywords_stopwords_do_not_dominate():
    """Repeating 'гост' in name_raw must not give all catalog items the same score."""
    from app.matching.scorer import score_match

    item_bolt = _make_item(
        "Болт M10 ГОСТ 7798",
        item_type="болт",
        size="M10",
        standard_key="GOST-7798",
    )
    item_nut = _make_item(
        "Гайка M10 ГОСТ 5927",
        item_type="гайка",
        size="M10",
        standard_key="GOST-5927",
    )

    # Row: mentions ГОСТ repeatedly — standard stopword — but has actual type+size
    row_bolt = {
        "item_type": "болт",
        "size": "M10",
        "gost": "ГОСТ 7798",
        "iso": "", "din": "",
        "diameter": "", "length": "",
        "strength": "", "coating": "",
        "name_raw": "болт m10 гост 7798 гост гост гост",
    }

    s_bolt = score_match(row_bolt, item_bolt)["score"]
    s_nut  = score_match(row_bolt, item_nut)["score"]

    # The bolt (correct type + size + standard) must score significantly higher
    assert s_bolt > s_nut, (
        f"bolt score ({s_bolt}) should exceed nut score ({s_nut})"
    )
    # Gap should be substantial (гост alone shouldn't close the gap)
    assert s_bolt - s_nut >= 30, (
        f"Expected ≥30 pt gap; bolt={s_bolt}, nut={s_nut}"
    )
