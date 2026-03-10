"""Tests for text input: parser unit tests + integration POST /text-input."""

import pytest
from fastapi.testclient import TestClient

from app.text_input.parser import parse_text_line, parse_text_to_rows


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


# ── New tests: tail extraction via textarea ────────────────────


def test_textarea_multiple_lines_parsed_as_multiple_rows():
    """4 plain lines → 4 rows (each is a separate position)."""
    text = "Болт М10\nГайка М10\nШайба 10\nВинт М6"
    rows = parse_text_to_rows(text)
    assert len(rows) == 4
    names = [r["name"] for r in rows]
    assert "Болт М10" in names
    assert "Гайка М10" in names


def test_textarea_tail_qty_uom_extracted():
    """Plain line 'Шайба 12 ГОСТ 11371-78 6 шт' → qty=6, uom='шт' from tail."""
    rows = parse_text_to_rows("Шайба 12 ГОСТ 11371-78 6 шт")
    assert len(rows) == 1
    row = rows[0]
    assert row["qty"] == 6
    assert row["uom"] == "шт"
    assert row["qty_uom_source"] == "из наименования"
    # '6 шт' stripped from cleaned name
    assert "6 шт" not in row["name"]
    assert "Шайба" in row["name"]


def test_textarea_tail_qty_thousands():
    """'Гайка М6 10 тыс. шт.' → qty=10000, uom='шт', multiplier=1000."""
    rows = parse_text_to_rows("Гайка М6 10 тыс. шт.")
    assert len(rows) == 1
    row = rows[0]
    assert row["qty"] == 10000
    assert row["uom"] == "шт"
    assert row["qty_multiplier"] == 1000


def test_columns_qty_uom_override_tail():
    """When qty/uom come from a dedicated column, tail '100 шт' in name is NOT stripped."""
    from app.parsing.preprocess import preprocess_row_text

    pp = preprocess_row_text(
        "Гайка М10 100 шт",
        qty_cell_text="92",
        uom_cell_text="шт",
    )
    assert pp["qty"] == 92
    assert pp["uom"] == "шт"
    assert pp["source"] == "из отдельных колонок"
    # Name is unchanged — qty came from the column, not from the tail
    assert "100 шт" in pp["cleaned_name"]


# ── Tests for parse_text_line ─────────────────────────────────


class TestParseTextLine:
    """parse_text_line extracts qty/unit from various free-text formats."""

    def test_unit_dot_then_qty(self):
        """'Гайка М16 шт. 10' → qty=10, unit='шт'."""
        r = parse_text_line("Гайка М16 шт. 10")
        assert r["qty"] == 10
        assert r["unit"] == "шт"
        assert r["name"] == "Гайка М16"

    def test_qty_then_unit(self):
        """'Болт М16х50 20 шт' → qty=20, unit='шт'."""
        r = parse_text_line("Болт М16х50 20 шт")
        assert r["qty"] == 20
        assert r["unit"] == "шт"
        assert r["name"] == "Болт М16х50"

    def test_no_unit_no_qty(self):
        """'Шайба DIN125 50' — no unit → qty=None, unit=None."""
        r = parse_text_line("Шайба DIN125 50")
        assert r["qty"] is None
        assert r["unit"] is None
        assert "Шайба DIN125" in r["name"]

    def test_pcs_unit(self):
        """'Винт M12x35 pcs 30' → qty=30, unit='pcs'."""
        r = parse_text_line("Винт M12x35 pcs 30")
        assert r["qty"] == 30
        assert r["unit"] == "pcs"
        assert r["name"] == "Винт M12x35"

    def test_no_qty_at_all(self):
        """'Анкер М10' — no number near unit → qty=None."""
        r = parse_text_line("Анкер М10")
        assert r["qty"] is None
        assert r["unit"] is None

    def test_washer_nordlock(self):
        """Real-world: 'Шайба Nord-Lock 16/WASHER NORD-LOCK NL16 шт. 8' → qty=8."""
        r = parse_text_line("Шайба Nord-Lock 16/WASHER NORD-LOCK NL16 шт. 8")
        assert r["qty"] == 8
        assert r["unit"] == "шт"
        assert "Шайба Nord-Lock 16/WASHER NORD-LOCK NL16" == r["name"]

    def test_nut_hm(self):
        """'Гайка Hm, M16-05/NUT M16-05 шт. 1' → qty=1."""
        r = parse_text_line("Гайка Hm, M16-05/NUT M16-05 шт. 1")
        assert r["qty"] == 1
        assert r["unit"] == "шт"

    def test_screw_chc(self):
        """'Винт CHC, M12-35/35 8.8/SCREW шт. 24' → qty=24."""
        r = parse_text_line("Винт CHC, M12-35/35 8.8/SCREW шт. 24")
        assert r["qty"] == 24
        assert r["unit"] == "шт"
        assert "Винт CHC, M12-35/35 8.8/SCREW" == r["name"]

    def test_bolt_iso(self):
        """'Болт с шестигранной головкой ISO 4017 – M16x60 – 8.8 – оцинковка шт. 9'."""
        r = parse_text_line("Болт с шестигранной головкой ISO 4017 – M16x60 – 8.8 – оцинковка шт. 9")
        assert r["qty"] == 9
        assert r["unit"] == "шт"
        assert "ISO 4017" in r["name"]
        assert "M16x60" in r["name"]

    def test_tabs_and_double_spaces(self):
        """Tabs and double spaces are normalized."""
        r = parse_text_line("Гайка  М16\tшт.  10")
        assert r["qty"] == 10
        assert r["unit"] == "шт"

    def test_unit_colon_qty(self):
        """'Болт М12 шт: 5' → qty=5."""
        r = parse_text_line("Болт М12 шт: 5")
        assert r["qty"] == 5
        assert r["unit"] == "шт"

    def test_float_qty(self):
        """'Труба 50мм кг 2,5' → qty=2.5, unit='кг'."""
        r = parse_text_line("Труба 50мм кг 2,5")
        assert r["qty"] == 2.5
        assert r["unit"] == "кг"


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
