"""Unit tests for the deterministic column scorer."""

from app.column_scorer import run_column_scorer


def test_autodetect_name_and_qty_combined():
    """Two-column sheet: hardware text + qty+uom → name_idx=0, qty_idx=1."""
    values_2d = [
        ["Болт М12x80 ГОСТ 7798-70", "200 шт"],
        ["Гайка М12 DIN 934", "50 шт"],
        ["Шайба 12 ГОСТ 11371-78", "100 шт"],
    ]
    r = run_column_scorer(values_2d, data_start=0)
    assert r.name_idx == 0
    assert r.qty_idx == 1


def test_autodetect_one_column_all_in_one():
    """Single-column sheet: everything in one cell → name_idx=0, qty_idx=None."""
    values_2d = [
        ["Гайка М10 DIN 934 200 шт"],
        ["Болт М8x40 ГОСТ 7798 100 шт"],
    ]
    r = run_column_scorer(values_2d, data_start=0)
    assert r.name_idx == 0
    assert r.qty_idx is None  # one-column → no separate qty column


def test_autodetect_header_row_not_first():
    """Header row is at index 2 (junk rows above) — scorer should detect it."""
    values_2d = [
        ["ООО ТоргМеталл", None, None],
        [None, None, None],
        ["Код", "Наименование", "Кол-во"],
        ["001", "Болт М12x80", "10 шт"],
        ["002", "Гайка М12", "20 шт"],
    ]
    r = run_column_scorer(values_2d)
    assert r.header_row == 2


def test_low_confidence_when_signals_weak():
    """Pure numeric data provides no usable signals → low_confidence=True."""
    values_2d = [
        ["42", "37", "99"],
        ["11", "56", "78"],
    ]
    r = run_column_scorer(values_2d, data_start=0)
    assert r.low_confidence is True


def test_autodetect_uom_col():
    """Column with uniform UOM strings is detected as uom_col (separate from qty_col)."""
    values_2d = [
        ["Болт М12", "100", "шт"],
        ["Гайка М10", "50", "шт"],
        ["Шайба 12", "200", "кг"],
    ]
    r = run_column_scorer(values_2d, data_start=0)
    assert r.uom_idx == 2
