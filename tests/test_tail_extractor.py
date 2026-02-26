"""Tests for app.parsing.tail_extractor.

- test_extract_qty_uom_tail_simple
- test_extract_qty_uom_tail_no_space
- test_extract_qty_uom_tail_decimal_comma
- test_extract_qty_uom_tail_thousands
- test_extract_qty_uom_tail_thousands_decimal
- test_tail_phrase_cut
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


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_extract_qty_uom_tail_simple():
    """Basic space-separated qty + UOM at end of name."""
    from app.parsing.tail_extractor import extract_qty_uom_from_tail

    clean, qty, uom, mult, expr, reason = extract_qty_uom_from_tail("Гайка М10 DIN 934 6 шт")

    assert qty == 6
    assert uom == "шт"
    assert mult == 1
    assert reason is None
    assert "6" in expr
    assert clean == "Гайка М10 DIN 934"


def test_extract_qty_uom_tail_no_space():
    """Qty glued to UOM without space: '6шт'."""
    from app.parsing.tail_extractor import extract_qty_uom_from_tail

    clean, qty, uom, mult, expr, reason = extract_qty_uom_from_tail("Болт М8x40 ГОСТ 7798 6шт")

    assert qty == 6
    assert uom == "шт"
    assert reason is None
    assert "Болт М8x40 ГОСТ 7798" in clean


def test_extract_qty_uom_tail_decimal_comma():
    """Decimal number with comma separator: '0,5 кг'."""
    from app.parsing.tail_extractor import extract_qty_uom_from_tail

    clean, qty, uom, mult, expr, reason = extract_qty_uom_from_tail("Смазка техническая 0,5 кг")

    assert qty == 0.5
    assert uom == "кг"
    assert mult == 1
    assert reason is None
    assert clean == "Смазка техническая"


def test_extract_qty_uom_tail_thousands():
    """Thousands multiplier: '10 тыс. шт.' → qty=10000, mult=1000."""
    from app.parsing.tail_extractor import extract_qty_uom_from_tail

    clean, qty, uom, mult, expr, reason = extract_qty_uom_from_tail("Гайка М6 10 тыс. шт.")

    assert qty == 10000
    assert uom == "шт"
    assert mult == 1000
    assert reason is None
    assert clean == "Гайка М6"


def test_extract_qty_uom_tail_thousands_decimal():
    """Thousands multiplier with decimal: '1,2 тыс шт' → qty=1200, mult=1000."""
    from app.parsing.tail_extractor import extract_qty_uom_from_tail

    clean, qty, uom, mult, expr, reason = extract_qty_uom_from_tail("Шайба 12 ГОСТ 11371 1,2 тыс шт")

    assert qty == 1200
    assert uom == "шт"
    assert mult == 1000
    assert reason is None
    assert clean == "Шайба 12 ГОСТ 11371"


def test_tail_phrase_cut():
    """strip_tail_phrase removes known stop-phrase from the end of name."""
    from app.parsing.tail_extractor import strip_tail_phrase

    phrases = ["Ремонт машин и оборудования", "Капитальный ремонт"]

    clean, cut = strip_tail_phrase(
        "Болт М12x80 Ремонт машин и оборудования",
        phrases,
    )
    assert cut == "Ремонт машин и оборудования"
    assert clean == "Болт М12x80"

    # Phrase not present — no change
    clean2, cut2 = strip_tail_phrase("Болт М12x80", phrases)
    assert cut2 is None
    assert clean2 == "Болт М12x80"
