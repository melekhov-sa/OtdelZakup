"""Tests for the threshold-based auto-match decision engine (decide_match).

- test_auto_memory_applies_when_fingerprint_exists
- test_auto_score_applies_above_threshold
- test_suggested_between_thresholds
- test_none_below_suggest_threshold
- test_always_require_confirmation_turns_auto_into_suggested
- test_confirm_suggested_saves_memory_mapping_when_enabled
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


# ── Test 1: Memory hit → AUTO_MEMORY ─────────────────────────────────────────

def test_auto_memory_applies_when_fingerprint_exists():
    from app.database import get_db_session
    from app.matcher import MATCH_MODE_AUTO_MEMORY, build_fingerprint, decide_match
    from app.match_settings import MatchSettings
    from app.models import SupplierInternalMatch

    item_id = _seed_internal_item(name="Болт М12x80", item_type="болт", size="M12x80")

    row = {"item_type": "болт", "size": "M12x80"}
    fp  = build_fingerprint(row)

    session = get_db_session()
    try:
        session.add(SupplierInternalMatch(fingerprint=fp, internal_item_id=item_id))
        session.commit()
    finally:
        session.close()

    settings = MatchSettings(
        enable_auto_match_memory=True, always_require_confirmation=False,
        enable_auto_match=True, auto_match_threshold=90, suggest_threshold=70,
    )
    result = decide_match(row, settings)

    assert result["mode"] == MATCH_MODE_AUTO_MEMORY
    assert result["internal_item_id"] == item_id
    assert result["score"] == 100


# ── Test 2: MinHash finds item → AUTO_MINHASH ─────────────────────────────────

def test_auto_score_applies_above_threshold():
    """MinHash finds the seeded item with J ≥ threshold → AUTO_MINHASH."""
    from app.matcher import MATCH_MODE_AUTO_MINHASH, decide_match
    from app.matching.minhash_index import rebuild_index
    from app.match_settings import MatchSettings
    from app.database import get_db_session
    from app.models import InternalItem

    _seed_internal_item(
        name="Болт М12x80 ГОСТ 7798-70",
        item_type="болт", size="M12x80", diameter="M12",
        standard_text="ГОСТ 7798-70",
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
        auto_apply_enabled=True, auto_apply_jaccard_threshold=0.0,
        always_require_confirmation=False, enable_auto_match_memory=False,
    )
    row = {"item_type": "болт", "size": "M12x80", "diameter": "M12",
           "gost": "ГОСТ 7798-70", "length": "", "iso": "", "din": "",
           "strength": "", "coating": "",
           "name_raw": "Болт М12x80 ГОСТ 7798-70", "name": "болт м12x80 гост 7798-70"}
    result = decide_match(row, settings)

    assert result["mode"] == MATCH_MODE_AUTO_MINHASH, f"Got mode={result['mode']}, score={result['score']}"
    assert result["internal_item_id"] is not None


# ── Test 3: MinHash below auto threshold → SUGGESTED ─────────────────────────

def test_suggested_between_thresholds():
    """MinHash finds item but J < auto threshold → SUGGESTED."""
    from app.matcher import MATCH_MODE_SUGGESTED, decide_match
    from app.matching.minhash_index import rebuild_index
    from app.match_settings import MatchSettings
    from app.database import get_db_session
    from app.models import InternalItem

    _seed_internal_item(
        name="Болт М12x80 DIN 933",
        item_type="болт", size="M12x80", standard_text="DIN 933",
    )

    session = get_db_session()
    try:
        all_items = session.query(InternalItem).filter_by(is_active=True).all()
    finally:
        session.close()
    rebuild_index(all_items, num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=False)

    # auto threshold set unreachably high so the item stays in SUGGESTED
    settings = MatchSettings(
        enable_minhash=True, lsh_threshold=0.05, num_perm=64, ngram_n=4,
        use_type_buckets=False, min_candidates_before_fallback=1,
        auto_apply_enabled=True, auto_apply_jaccard_threshold=0.99,
        always_require_confirmation=False, enable_auto_match_memory=False,
    )
    row = {"item_type": "болт", "size": "M12x60", "diameter": "", "length": "",
           "gost": "", "iso": "", "din": "DIN 933", "strength": "", "coating": "",
           "name_raw": "Болт М12x60 DIN 933", "name": "болт м12x60 din 933"}
    result = decide_match(row, settings)

    assert result["mode"] == MATCH_MODE_SUGGESTED, f"Got mode={result['mode']}, score={result['score']}"
    assert result["internal_item_id"] is not None


# ── Test 4: Low score → NONE ─────────────────────────────────────────────────

def test_none_below_suggest_threshold():
    """Row with no extractable fields scores 0 → NONE."""
    from app.matcher import MATCH_MODE_NONE, decide_match
    from app.match_settings import MatchSettings

    _seed_internal_item(
        name="Болт М12x80 DIN 933 8.8",
        item_type="болт", size="M12x80", standard_text="DIN 933",
    )

    settings = MatchSettings(
        enable_auto_match=True, auto_match_threshold=90, suggest_threshold=70,
        always_require_confirmation=False, enable_auto_match_memory=False,
    )
    row = {"item_type": "", "size": "", "diameter": "", "length": "",
           "gost": "", "iso": "", "din": "", "strength": "", "coating": ""}
    result = decide_match(row, settings)

    assert result["mode"] == MATCH_MODE_NONE
    assert result["internal_item_id"] is None


# ── Test 5: always_require_confirmation → AUTO becomes SUGGESTED ──────────────

def test_always_require_confirmation_turns_auto_into_suggested():
    """MinHash auto-match + always_require_confirmation=True → SUGGESTED (not AUTO_MINHASH)."""
    from app.matcher import MATCH_MODE_SUGGESTED, decide_match
    from app.matching.minhash_index import rebuild_index
    from app.match_settings import MatchSettings
    from app.database import get_db_session
    from app.models import InternalItem

    _seed_internal_item(
        name="Болт М12x80 ГОСТ 7798-70",
        item_type="болт", size="M12x80", diameter="M12",
        standard_text="ГОСТ 7798-70",
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
        auto_apply_enabled=True, auto_apply_jaccard_threshold=0.0,
        always_require_confirmation=True,   # <-- force confirmation
        enable_auto_match_memory=False,
    )
    row = {"item_type": "болт", "size": "M12x80", "diameter": "M12",
           "gost": "ГОСТ 7798-70", "length": "", "iso": "", "din": "",
           "strength": "", "coating": "",
           "name_raw": "Болт М12x80 ГОСТ 7798-70", "name": "болт м12x80 гост 7798-70"}
    result = decide_match(row, settings)

    assert result["mode"] == MATCH_MODE_SUGGESTED, f"Expected SUGGESTED, got {result['mode']}"
    assert result["internal_item_id"] is not None


# ── Test 6: confirm-match endpoint saves memory mapping ──────────────────────

def test_confirm_suggested_saves_memory_mapping_when_enabled():
    """POST /files/{fid}/rows/1/confirm-match should write SupplierInternalMatch."""
    from fastapi.testclient import TestClient
    from app.main import app
    from app.database import get_db_session
    from app.models import SupplierInternalMatch
    from app.trace import save_traces

    client = TestClient(app)

    item_id = _seed_internal_item(name="Гайка М12 DIN 934", item_type="гайка", size="M12")

    file_id = "test_confirm_001"
    traces = [{
        "row_number": 1,
        "raw_inputs": {"raw_name": "Гайка М12"},
        "extracted_fields": {}, "enrichment": {}, "inference": {},
        "readiness": {}, "validation": {}, "final": {},
        "matching": {
            "mode": "SUGGESTED",
            "internal_item_id": item_id,
            "name": "Гайка М12 DIN 934",
            "score": 80,
            "fingerprint": "confirm_test_fp_0001",
            "candidates": [],
        },
    }]
    save_traces(file_id, traces)

    response = client.post(
        f"/files/{file_id}/rows/1/confirm-match",
        data={"remember": "true"},
    )
    assert response.status_code == 200, f"HTTP {response.status_code}: {response.text}"
    data = response.json()
    assert data["ok"] is True
    assert data["mode"] == "CONFIRMED"
    assert data["name"] == "Гайка М12 DIN 934"

    session = get_db_session()
    try:
        mem = session.query(SupplierInternalMatch).filter_by(
            fingerprint="confirm_test_fp_0001"
        ).first()
        assert mem is not None, "SupplierInternalMatch record should exist"
        assert mem.internal_item_id == item_id
    finally:
        session.close()
