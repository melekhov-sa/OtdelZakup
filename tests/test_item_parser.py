"""Tests for smart internal item parsing (item_parser + bulk-import endpoint).

- test_parse_internal_item_name_extracts_standard_and_size
- test_bulk_preview_returns_statuses
- test_bulk_import_creates_items
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


# ── Test 1: parse_internal_item_name extracts key fields ─────────────────────

def test_parse_internal_item_name_extracts_standard_and_size():
    """'Болт М12x80 ГОСТ 7798-70 8.8' should extract item_type, size, standard, strength."""
    from app.item_parser import parse_internal_item_name

    result = parse_internal_item_name("Болт М12x80 ГОСТ 7798-70 8.8")

    assert result["item_type"] == "болт", f"Expected 'болт', got {result['item_type']!r}"
    assert result["size"] == "M12x80", f"Expected 'M12x80', got {result['size']!r}"
    assert "7798" in result["standard_text"], f"standard_text missing ГОСТ: {result['standard_text']!r}"
    assert result["parse_status"] == "ok", f"Expected 'ok', got {result['parse_status']!r}"
    assert result["parse_reason"] == "", f"Expected empty reason, got {result['parse_reason']!r}"

    # Verify all expected keys are present
    expected_keys = {"item_type", "size", "diameter", "length", "standard_text",
                     "strength_class", "material_coating", "parse_status", "parse_reason"}
    assert expected_keys <= set(result.keys()), f"Missing keys: {expected_keys - set(result.keys())}"


# ── Test 2: bulk_parse returns correct statuses ───────────────────────────────

def test_bulk_preview_returns_statuses():
    """bulk_parse should assign parse_status per name and include name_full."""
    from app.item_parser import bulk_parse

    names = [
        "Болт М12x80 ГОСТ 7798-70 8.8",   # ok — item_type + standard
        "Непонятный крепёж 123",           # manual — no item_type
        "",                                 # skipped by skip_empty=True
    ]
    results = bulk_parse(names, skip_empty=True, dedup=False)

    assert len(results) == 2, f"Expected 2 results (empty filtered), got {len(results)}"

    assert results[0]["parse_status"] == "ok", f"Got {results[0]['parse_status']!r}"
    assert results[1]["parse_status"] == "manual", f"Got {results[1]['parse_status']!r}"

    assert "name_full" in results[0], "name_full key missing from result"
    assert results[0]["name_full"] == "Болт М12x80 ГОСТ 7798-70 8.8"


# ── Test 3: bulk-import endpoint creates items ────────────────────────────────

def test_bulk_import_creates_items():
    """POST /internal-items/bulk-import should create InternalItem records."""
    from fastapi.testclient import TestClient
    from app.main import app
    from app.database import get_db_session
    from app.models import InternalItem

    client = TestClient(app)

    names_text = (
        "Болт М12x80 ГОСТ 7798-70 8.8\n"
        "Гайка М12 DIN 934\n"
        "Шайба 12 ГОСТ 11371-78"
    )
    response = client.post(
        "/internal-items/bulk-import",
        data={"names_text": names_text},
    )

    assert response.status_code == 200, f"HTTP {response.status_code}: {response.text}"
    data = response.json()
    assert data["ok"] is True, f"Expected ok=True, got {data}"
    assert data["created"] == 3, f"Expected 3 created, got {data['created']}"

    session = get_db_session()
    try:
        items = session.query(InternalItem).all()
        assert len(items) == 3, f"Expected 3 items in DB, got {len(items)}"

        names = [it.name for it in items]
        assert any("Болт" in n for n in names), f"Болт not found: {names}"
        assert any("Гайка" in n for n in names), f"Гайка not found: {names}"
        assert any("Шайба" in n for n in names), f"Шайба not found: {names}"

        # parse_status should be set for all items
        for it in items:
            assert it.parse_status in ("ok", "review", "manual"), \
                f"Unexpected parse_status {it.parse_status!r} for {it.name!r}"
    finally:
        session.close()
