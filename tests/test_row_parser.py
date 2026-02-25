"""Tests for app.parsing.row_parser.parse_row."""
import io

import pandas as pd
import pytest


# ── DB / dir isolation (same pattern as other test modules) ──────────────────


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

    from fastapi.testclient import TestClient
    return TestClient(app)


def _make_xlsx(rows: list[dict]) -> io.BytesIO:
    buf = io.BytesIO()
    pd.DataFrame(rows).to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    return buf


# ── 1. Combined qty column ("1500 шт") ───────────────────────────────────────


def test_rowparser_qty_uom_from_qty_column_combined():
    """qty_is_combined=True, cell '1500 шт' → qty=1500, uom='шт'."""
    from app.parsing.row_parser import parse_row

    cells = {"Наименование": "Болт М12", "Заказ": "1500 шт"}
    mapping = {"name_col": "Наименование", "qty_col": "Заказ", "qty_is_combined": True}
    result = parse_row(cells, mapping)

    assert result["qty"] == 1500
    assert result["uom"] == "шт"
    assert result["qty_uom_source"] == "из колонки количества"
    assert result["name"] == "Болт М12"


# ── 2. qty+uom extracted from name tail ──────────────────────────────────────


def test_rowparser_qty_uom_from_name_tail():
    """No qty column; name ends in ' 1500 шт' → extracted, name cleaned."""
    from app.parsing.row_parser import parse_row

    cells = {"Наименование": "Болт М10х50 1500 шт"}
    mapping = {"name_col": "Наименование"}
    result = parse_row(cells, mapping)

    assert result["qty"] == 1500
    assert result["uom"] == "шт"
    assert "1500" not in result["name"]
    assert "Болт" in result["name"]
    assert result["qty_uom_source"] == "из наименования"


# ── 3. One column, all in one ("Гайка М10 DIN 934 200 шт") ──────────────────


def test_rowparser_one_column_all_in_one():
    """Single name column with embedded qty+uom → both extracted, name cleaned."""
    from app.parsing.row_parser import parse_row

    cells = {"Наименование": "Гайка М10 DIN 934 200 шт"}
    mapping = {"name_col": "Наименование"}
    result = parse_row(cells, mapping)

    assert result["qty"] == 200
    assert result["uom"] == "шт"
    assert "200" not in result["name"]
    assert "Гайка" in result["name"]


# ── 4. No unit → strict both-or-none policy ──────────────────────────────────


def test_rowparser_no_defaults_if_missing_uom():
    """Plain numeric qty column '645', no uom anywhere → qty=None, uom=None."""
    from app.parsing.row_parser import parse_row

    cells = {"Наименование": "Винт М2,5x20", "Заказ": "645"}
    mapping = {"name_col": "Наименование", "qty_col": "Заказ"}
    result = parse_row(cells, mapping)

    assert result["qty"] is None
    assert result["uom"] is None
    assert result["qty_uom_source"] == "не найдено"


# ── 5. Integration: preview shows split qty / uom ────────────────────────────


def test_preview_shows_split_qty_uom(client):
    """Upload xlsx with combined qty column → preview HTML has separate qty + uom."""
    import re

    rows = [
        {"Код": "001", "Номенклатура": "Болт М12х80", "Количество": "10 шт"},
        {"Код": "002", "Номенклатура": "Гайка М12", "Количество": "20 шт"},
    ]
    xlsx = _make_xlsx(rows)
    resp = client.post(
        "/upload",
        files={"file": ("test.xlsx", xlsx, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    )
    assert resp.status_code == 200
    html = resp.text

    # Preview table should have separate qty and uom columns (Russian labels)
    assert "Количество" in html
    assert "Ед." in html
    # Actual values should appear separated
    assert "10" in html
    assert "шт" in html
    # raw_text / qty_uom_source should NOT appear as table column headers in the preview
    assert "raw_text" not in html
    assert "qty_uom_source" not in html
