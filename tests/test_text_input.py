"""Tests for text input: parser unit tests + integration POST /text-input."""

import pytest
from fastapi.testclient import TestClient

from app.text_input.parser import parse_text_to_rows


# ── Fixtures (shared with other tests) ───────────────────────


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
    db_mod.engine = create_engine(
        f"sqlite:///{db_path}", connect_args={"check_same_thread": False}
    )
    db_mod.SessionLocal = sessionmaker(
        bind=db_mod.engine, autoflush=False, expire_on_commit=False
    )
    db_mod.init_db()


@pytest.fixture()
def client():
    from app.main import app

    return TestClient(app)


# ── Unit tests for parse_text_to_rows ─────────────────────────


def test_parse_numbered_list_with_qty_uom():
    """Numbered item with dash qty: '1. Винт М2,5x20 DIN85 - 645 шт' → 1 row, qty=645, uom='шт'."""
    rows = parse_text_to_rows("1. Винт М2,5x20 DIN85 - 645 шт")
    assert len(rows) == 1
    row = rows[0]
    assert row["name"] == "Винт М2,5x20 DIN85"
    assert row["qty"] == 645
    assert row["uom"] == "шт"
    assert row["row_number"] == 1


def test_parse_two_line_quantity():
    """Two-line format: 'Наименование\\nКоличество: 10 000 шт' → qty=10000 (thousands)."""
    text = "Болт М10х50\nКоличество: 10 000 шт"
    rows = parse_text_to_rows(text)
    assert len(rows) == 1
    assert rows[0]["qty"] == 10000
    assert rows[0]["uom"] == "шт"
    assert rows[0]["name"] == "Болт М10х50"


def test_parse_dash_quantity_requires_uom():
    """Dash without uom: 'Винт М2,5x20 - 645' → qty=None, uom=None (strict)."""
    rows = parse_text_to_rows("Винт М2,5x20 - 645")
    assert len(rows) == 1
    assert rows[0]["qty"] is None
    assert rows[0]["uom"] is None
    # Name stays as the full original line (no dash split when uom missing)
    assert "Винт" in rows[0]["name"]


def test_notes_applied_to_all_rows():
    """Service line ('все в цинке') is collected as note_raw on all rows."""
    text = "1. Болт М12 - 10 шт\n2. Гайка М12 - 20 шт\nвсе в цинке"
    rows = parse_text_to_rows(text)
    assert len(rows) == 2
    for row in rows:
        assert "все в цинке" in row["note_raw"]


# ── Integration test ──────────────────────────────────────────


def test_text_input_post_returns_view_raw(client):
    """POST /text-input with valid text → 200 and positions visible in HTML."""
    text = "1. Болт М12х80 — 100 шт\n2. Гайка М12 — 50 шт"
    resp = client.post("/text-input", data={"text": text})
    assert resp.status_code == 200
    html = resp.text
    assert "Болт М12х80" in html
    assert "Гайка М12" in html
    # file_id should be present for subsequent transform
    assert "file_id" in html
