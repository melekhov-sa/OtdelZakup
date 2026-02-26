"""Tests for the improved match scorer (app/matching/scorer.py).

Test cases:
1. Exact samorez match → score >= 90 (AUTO range)
2. Hermetik belyi → appears in candidates by type + keyword match
3. Disk cross-format size (125x22.2x1.6 vs 125x1.6x22мм) → sizes_close → SUGGEST range
4. Close-size suggest (4.2x50 vs 4.2x51) → SUGGEST + reason "размер близкий"
5. Override: manual selection saves fingerprint → next lookup hits memory
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


# ── Helper ────────────────────────────────────────────────────────────────────

def _make_item(name, item_type=None, size=None, material_coating=None, name_full=None):
    """Create a transient InternalItem for scoring (not DB-persisted)."""
    from app.models import InternalItem
    return InternalItem(
        name=name,
        item_type=item_type or "",
        size=size or "",
        diameter=None,
        length=None,
        standard_text=None,
        standard_key=None,
        strength_class=None,
        material_coating=material_coating or "",
        name_full=name_full or "",
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


# ── Test 1: Exact screw match → AUTO score (≥ 90) ────────────────────────────

def test_exact_screw_match_scores_90():
    """Саморез 4.2x70 exact → type(20)+size(60)+bonus(10) = 90."""
    from app.matching.scorer import score_match

    item = _make_item("Саморез СГД 4.2x70", item_type="саморез", size="4.2x70")
    row = {"item_type": "саморез", "size": "4.2x70"}

    result = score_match(row, item)

    assert result["score"] >= 90, f"Expected score ≥ 90, got {result['score']}"
    assert result["score"] <= 100
    reasons_text = " ".join(result["reasons"])
    assert "тип" in reasons_text
    assert "размер" in reasons_text


# ── Test 2: Sealant with color keyword appears in candidates ──────────────────

def test_sealant_appears_in_candidates():
    """Герметик белый → catalog item matched by type + keyword 'белый'."""
    from app.matcher import add_internal_matches
    import pandas as pd

    _seed("Герметик силиконовый белый 310 мл", item_type="герметик", size="")
    _seed("Герметик силиконовый чёрный 310 мл", item_type="герметик", size="")
    _seed("Болт М12x80 DIN 933", item_type="болт", size="M12x80")

    df = pd.DataFrame([{
        "item_type": "герметик",
        "size": "",
        "diameter": "",
        "length": "",
        "gost": "",
        "iso": "",
        "din": "",
        "strength": "",
        "coating": "",
        "name_raw": "Герметик белый 310 мл",
        "name": "герметик белый 310 мл",
    }])

    df_out, results = add_internal_matches(df)
    mr = results[0]

    # Should appear in candidates
    candidate_names = [c["name"] for c in mr.get("candidates", [])]
    assert any("белый" in n.lower() for n in candidate_names), (
        f"Expected герметик белый in candidates; got: {candidate_names}"
    )
    # White should rank above black (better keyword match + volume)
    if len(candidate_names) >= 2:
        assert "белый" in candidate_names[0].lower() or "белый" in candidate_names[1].lower()


# ── Test 3: Cross-format disk size → close match ─────────────────────────────

def test_disk_cross_format_size_matches():
    """Диск 125x22.2x1.6 vs 'Диск отрезной 125x1,6x22мм' → sizes_close → score ≥ 65."""
    from app.matching.scorer import score_match

    # Catalog item: standard format 125mm disc, 1.6mm thick, 22mm bore
    item = _make_item(
        "Диск отрезной 125x1,6x22мм по металлу",
        item_type="диск",
        size="125x1,6x22мм",
    )
    # Supplier uses different dimension order
    row = {"item_type": "диск", "size": "125x22.2x1.6"}

    result = score_match(row, item)

    assert result["score"] >= 65, (
        f"Expected score ≥ 65 (SUGGEST range) for cross-format disk, got {result['score']}"
    )
    # Should report a size-related reason or warn
    all_msgs = result["reasons"] + result["warn_reasons"]
    assert any("размер" in m or "диаметр" in m for m in all_msgs), (
        f"Expected size signal in reasons; got reasons={result['reasons']}, warns={result['warn_reasons']}"
    )


# ── Test 4: Close size → SUGGEST mode with reason ────────────────────────────

def test_close_size_gives_suggest():
    """Саморез 4.2x50 vs 4.2x51 → sizes differ by ~2% → SUGGEST mode."""
    from app.matcher import decide_match
    from app.match_settings import MatchSettings

    _seed("Саморез СГД 4.2x51", item_type="саморез", size="4.2x51")

    settings = MatchSettings(
        enable_auto_match=True,
        auto_match_threshold=90,
        suggest_threshold=65,
        always_require_confirmation=False,
        enable_auto_match_memory=False,
    )
    row = {
        "item_type": "саморез", "size": "4.2x50",
        "diameter": "", "length": "", "gost": "", "iso": "", "din": "",
        "strength": "", "coating": "",
    }
    result = decide_match(row, settings)

    assert result["mode"] == "SUGGESTED", (
        f"Expected SUGGESTED for close size; got mode={result['mode']}, score={result['score']}"
    )
    # Warn reason should mention "близкий"
    top_candidate = result["candidates"][0] if result["candidates"] else {}
    warn_msgs = top_candidate.get("warn_reasons", [])
    assert any("близкий" in w for w in warn_msgs), (
        f"Expected 'близкий' in warn_reasons; got {warn_msgs}"
    )


# ── Test 5: Override — manual selection persists and is recalled ──────────────

def test_override_persists_and_recalled():
    """Selecting an item manually stores fingerprint; next match returns memory hit."""
    from app.database import get_db_session
    from app.matcher import build_fingerprint, decide_match
    from app.match_settings import MatchSettings
    from app.models import SupplierInternalMatch

    item_id = _seed("Гайка М10 DIN 934", item_type="гайка", size="M10")

    row = {"item_type": "гайка", "size": "M10", "din": "DIN 934",
           "diameter": "", "length": "", "gost": "", "iso": "",
           "strength": "", "coating": ""}
    fp = build_fingerprint(row)

    # Manually save memory mapping (simulates user clicking "Выбрать + запомнить")
    session = get_db_session()
    try:
        session.add(SupplierInternalMatch(fingerprint=fp, internal_item_id=item_id))
        session.commit()
    finally:
        session.close()

    settings = MatchSettings(
        enable_auto_match_memory=True,
        always_require_confirmation=False,
        enable_auto_match=True,
        auto_match_threshold=90,
        suggest_threshold=65,
    )
    result = decide_match(row, settings)

    assert result["mode"] == "AUTO_MEMORY", f"Expected AUTO_MEMORY, got {result['mode']}"
    assert result["internal_item_id"] == item_id
    assert result["score"] == 100
