"""Tests for internal catalog matching.

- test_fingerprint_is_deterministic
- test_match_from_memory_by_fingerprint
- test_matching_scores_size_and_standard
- test_no_match_when_low_score
- test_select_internal_item_creates_memory_mapping
"""

import pytest


# ── Test isolation fixture ────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _set_dirs(tmp_path, monkeypatch):
    upload_dir = tmp_path / "uploads"
    cache_dir = tmp_path / "cache"
    monkeypatch.setenv("OTDELZAKUP_UPLOAD_DIR", str(upload_dir))
    monkeypatch.setenv("OTDELZAKUP_CACHE_DIR", str(cache_dir))
    import app.cache as cache_mod

    cache_mod.UPLOAD_DIR = upload_dir
    cache_mod.CACHE_DIR = cache_dir

    db_path = tmp_path / "test.db"
    monkeypatch.setenv("OTDELZAKUP_DB_PATH", str(db_path))
    import app.database as db_mod
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    db_mod.DB_PATH = db_path
    db_mod.engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    db_mod.SessionLocal = sessionmaker(bind=db_mod.engine, autoflush=False, expire_on_commit=False)
    db_mod.init_db()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _seed_internal_item(**kwargs):
    """Insert an InternalItem and return its id."""
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


# ── Test 1: Fingerprint is deterministic ─────────────────────────────────────

def test_fingerprint_is_deterministic():
    """Same row_dict should always produce the same fingerprint."""
    from app.matcher import build_fingerprint

    row = {"item_type": "болт", "size": "M12x80", "gost": "7798-70", "strength": "8.8"}
    fp1 = build_fingerprint(row)
    fp2 = build_fingerprint(row)

    assert fp1 == fp2, "Fingerprint must be deterministic"
    assert len(fp1) == 16, "Fingerprint should be 16 hex chars"

    # Different row → different fingerprint
    row2 = {"item_type": "гайка", "size": "M12", "din": "934"}
    assert build_fingerprint(row2) != fp1


# ── Test 2: Memory match from fingerprint ────────────────────────────────────

def test_match_from_memory_by_fingerprint():
    """If fingerprint is in SupplierInternalMatch, it should be returned immediately."""
    from app.database import get_db_session
    from app.matcher import build_fingerprint, find_match
    from app.models import SupplierInternalMatch

    item_id = _seed_internal_item(name="Болт М12x80 DIN 933 8.8", item_type="болт", size="M12x80")

    row = {"item_type": "болт", "size": "M12x80", "din": "933", "strength": "8.8"}
    fp = build_fingerprint(row)

    # Manually add to memory
    session = get_db_session()
    try:
        session.add(SupplierInternalMatch(fingerprint=fp, internal_item_id=item_id))
        session.commit()
    finally:
        session.close()

    result = find_match(row)

    assert result["source"] == "memory", f"Expected 'memory', got '{result['source']}'"
    assert result["best"] is not None
    assert result["best"].name == "Болт М12x80 DIN 933 8.8"


# ── Test 3: Scoring — size and standard contribute most ──────────────────────

def test_matching_scores_size_and_standard():
    """An item matching size + standard should score >= threshold and be selected."""
    from app.matcher import find_match, _MATCH_THRESHOLD

    # Item matching size + standard
    _seed_internal_item(name="Болт М12x80 ГОСТ 7798-70 8.8", item_type="болт",
                        size="M12x80", diameter="M12", length="80",
                        standard_text="ГОСТ 7798-70", strength_class="8.8")
    # Decoy item — different size
    _seed_internal_item(name="Болт М8x40 ГОСТ 7798-70 8.8", item_type="болт",
                        size="M8x40", diameter="M8", length="40",
                        standard_text="ГОСТ 7798-70", strength_class="8.8")

    row = {"item_type": "болт", "size": "M12x80", "gost": "7798-70", "strength": "8.8"}
    result = find_match(row)

    assert result["source"] == "scored", f"Expected 'scored', got '{result['source']}'"
    assert result["best"] is not None, "Expected a best match"
    assert "М12x80" in result["best"].name, f"Wrong item matched: {result['best'].name}"
    assert result["score"] >= _MATCH_THRESHOLD


# ── Test 4: No match when signals too weak ────────────────────────────────────

def test_no_match_when_low_score():
    """Rows with no extractable fields should not match any item."""
    from app.matcher import find_match

    # Seed catalog items
    _seed_internal_item(name="Болт М12x80 DIN 933 8.8", item_type="болт",
                        size="M12x80", diameter="M12", length="80",
                        standard_text="DIN 933", strength_class="8.8")

    # Row with no matching fields at all
    row = {"item_type": "", "size": "", "diameter": "", "length": "",
           "gost": "", "iso": "", "din": "", "strength": "", "coating": ""}
    result = find_match(row)

    assert result["source"] == "none", f"Expected 'none', got '{result['source']}'"
    assert result["best"] is None


# ── Test 5: Select-internal-item endpoint creates memory mapping ──────────────

def test_select_internal_item_creates_memory_mapping():
    """POST /files/{file_id}/rows/{row_num}/select-internal-item with remember=True
    should create a SupplierInternalMatch record."""
    import json
    from fastapi.testclient import TestClient
    from app.main import app
    from app.database import get_db_session
    from app.models import SupplierInternalMatch
    from app.trace import save_traces

    client = TestClient(app)

    item_id = _seed_internal_item(name="Гайка М16 DIN 934", item_type="гайка", size="M16")

    # Prepare a fake trace with fingerprint info
    file_id = "test_file_001"
    matching_data = {
        "source": "none",
        "fingerprint": "abcd1234ef567890",
        "score": 0,
        "candidates": [],
    }
    traces = [{
        "row_number": 1,
        "raw_inputs": {"raw_name": "Гайка М16"},
        "extracted_fields": {},
        "enrichment": {},
        "inference": {},
        "readiness": {},
        "validation": {},
        "final": {},
        "matching": matching_data,
    }]
    save_traces(file_id, traces)

    response = client.post(
        f"/files/{file_id}/rows/1/select-internal-item",
        data={"internal_item_id": item_id, "remember": "true"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert data["name"] == "Гайка М16 DIN 934"

    # Check memory record was created
    session = get_db_session()
    try:
        mem = session.query(SupplierInternalMatch).filter_by(
            fingerprint="abcd1234ef567890"
        ).first()
        assert mem is not None, "SupplierInternalMatch record should have been created"
        assert mem.internal_item_id == item_id
    finally:
        session.close()
