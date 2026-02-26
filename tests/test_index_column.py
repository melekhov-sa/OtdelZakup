"""Tests for index column detection in column_scorer."""

from app.column_scorer import detect_index_column, run_column_scorer


def test_detect_single_index_column():
    """A column with cleanly ascending integers is detected as an index column."""
    col = ["1", "2", "3", "4", "5"]
    assert detect_index_column(col) is True


def test_detect_two_index_columns():
    """Two adjacent sequential-integer columns → both appear in index_cols."""
    values_2d = [
        ["1", "1", "Болт М12x80", "10 шт"],
        ["2", "2", "Гайка М12", "5 шт"],
        ["3", "3", "Шайба 12", "20 шт"],
        ["4", "4", "Винт М6x20", "50 шт"],
        ["5", "5", "Саморез 4.2x16", "100 шт"],
    ]
    r = run_column_scorer(values_2d, data_start=0)
    assert 0 in r.index_cols
    assert 1 in r.index_cols


def test_do_not_treat_index_as_name():
    """An index column must be excluded from name_col assignment."""
    values_2d = [
        ["1", "Болт М12x80"],
        ["2", "Гайка М12"],
        ["3", "Шайба 12"],
        ["4", "Винт М6x20"],
        ["5", "Саморез 4.2x16"],
    ]
    r = run_column_scorer(values_2d, data_start=0)
    assert 0 in r.index_cols   # col 0 is an index column
    assert r.name_idx == 1     # name assigned to col 1, not the index col


def test_column_with_letters_not_detected_as_index():
    """A column containing letter characters must not be treated as an index."""
    col = ["A1", "A2", "A3", "A4", "A5"]
    assert detect_index_column(col) is False


def test_non_monotonic_column_not_index():
    """A column with many order violations must not be detected as index."""
    col = ["5", "3", "1", "4", "2"]
    assert detect_index_column(col) is False


def test_decimal_values_not_index():
    """A column containing decimal numbers must not be detected as index."""
    col = ["1.5", "2.5", "3.5", "4.5"]
    assert detect_index_column(col) is False
