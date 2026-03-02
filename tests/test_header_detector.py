"""Unit tests for app.parsing.header_detector.

Spec cases:
1) Clear header row ["Наименование", "Кол-во", "Ед."] → has_header=True
2) First row is product data "Болт M12x45..." → has_header=False
3) False header: first row has ГОСТ numbers → has_header=False
4) Dimension in first row (12x50) → has_header=False
5) Strength class 8.8 in first row → has_header=False
6) Only numeric first row → has_header=False
7) Header with № symbol → has_header=True
8) Short ambiguous row with DocAI flag → depends on tokens
9) Large table: header bonus from data-row comparison → True
10) Empty/single-row tables → safe defaults
"""
from __future__ import annotations

import pytest

from app.parsing.header_detector import HeaderDecision, detect_header_row


# ── helpers ───────────────────────────────────────────────────────────────────

def _rows(header: list[str], *data: list[str]) -> list[list[str]]:
    """Build rows list with header first, then data rows."""
    return [header] + list(data)


# ── 1) Clear header ────────────────────────────────────────────────────────────

class TestClearHeader:

    def test_standard_three_col_header(self):
        rows = _rows(
            ["Наименование", "Кол-во", "Ед."],
            ["Болт М10x45 ГОСТ 7798", "250", "шт"],
            ["Гайка М10 ГОСТ 5915",   "500", "шт"],
        )
        d = detect_header_row(rows)
        assert d.has_header is True
        assert d.header_row_index == 0
        assert d.confidence >= 0.65

    def test_four_col_header(self):
        rows = _rows(
            ["№", "Наименование", "Кол-во", "Ед.изм"],
            ["1", "Болт М12", "100", "шт"],
            ["2", "Гайка М12", "200", "шт"],
        )
        d = detect_header_row(rows)
        assert d.has_header is True

    def test_header_tokens_found_in_reasons(self):
        rows = _rows(
            ["Наименование", "Количество"],
            ["Болт", "100"],
        )
        d = detect_header_row(rows)
        assert any("header_tokens" in r for r in d.reasons)

    def test_docai_confirmed_flag(self):
        """DocAI had header + clear tokens → header_was_guessed=False."""
        rows = _rows(
            ["Наименование", "Кол-во"],
            ["Болт М12", "100"],
        )
        d = detect_header_row(rows, docai_had_header=True)
        assert d.has_header is True
        assert d.header_was_guessed is False

    def test_english_header_tokens(self):
        rows = _rows(
            ["Name", "Qty", "UOM"],
            ["Bolt M12", "100", "pcs"],
        )
        d = detect_header_row(rows)
        assert d.has_header is True

    def test_partial_header_match(self):
        """'Наим.' contains 'наимен' → recognized as header partial match."""
        rows = _rows(
            ["Наим.", "Количество"],
            ["Болт М10", "50"],
            ["Гайка М10", "100"],
        )
        d = detect_header_row(rows)
        assert d.has_header is True


# ── 2) First row is product data ──────────────────────────────────────────────

class TestDataFirstRow:

    def test_bolt_with_gost(self):
        """'Болт M12x45 ГОСТ 7798-70' is NOT a header."""
        rows = _rows(
            ["Болт M12x45 ГОСТ 7798-70", "250", "шт"],
            ["Гайка M10 ГОСТ 5915-70",   "500", "шт"],
        )
        d = detect_header_row(rows)
        assert d.has_header is False

    def test_dimension_in_first_row(self):
        """First row with 12x50 dimension → data, not header."""
        rows = _rows(
            ["Анкер 12x50 нержавейка", "100", "шт"],
            ["Дюбель 10x40",           "200", "шт"],
        )
        d = detect_header_row(rows)
        assert d.has_header is False
        assert any("item_pattern" in r for r in d.reasons)

    def test_gost_number_in_first_row(self):
        """'ГОСТ 7798' signals data → has_header=False."""
        rows = _rows(
            ["Болт ГОСТ 7798", "100", "шт"],
            ["Гайка ГОСТ 5915", "200", "шт"],
        )
        d = detect_header_row(rows)
        assert d.has_header is False

    def test_din_number_in_first_row(self):
        """DIN 933 signals data → has_header=False."""
        rows = _rows(
            ["Болт DIN 933 M12", "50", "шт"],
            ["Болт DIN 933 M10", "100", "шт"],
        )
        d = detect_header_row(rows)
        assert d.has_header is False

    def test_strength_class_8_8(self):
        """Strength class 8.8 in first row → data."""
        rows = _rows(
            ["Болт 8.8 M12x60", "50", "шт"],
            ["Болт 10.9 M10x50", "100", "шт"],
        )
        d = detect_header_row(rows)
        assert d.has_header is False

    def test_bolt_size_latin_m(self):
        """Latin M12 pattern triggers item detection."""
        rows = _rows(
            ["M12x50 hex bolt", "100", "pcs"],
            ["M10x40 hex bolt", "200", "pcs"],
        )
        d = detect_header_row(rows)
        assert d.has_header is False

    def test_cyrillic_m_with_dimension(self):
        """Cyrillic М + dimension → item pattern."""
        rows = _rows(
            ["М12x50 оцинкованный", "250", "шт"],
            ["М10x40 оцинкованный", "150", "шт"],
        )
        d = detect_header_row(rows)
        assert d.has_header is False


# ── 3) Numeric-heavy first row ─────────────────────────────────────────────────

class TestNumericFirstRow:

    def test_mostly_numeric_first_row(self):
        """First row with many numbers → data."""
        rows = _rows(
            ["1", "100", "50", "шт"],
            ["2", "200", "75", "шт"],
        )
        d = detect_header_row(rows)
        assert d.has_header is False

    def test_all_numeric_first_row(self):
        rows = _rows(
            ["100", "200", "300"],
            ["400", "500", "600"],
        )
        d = detect_header_row(rows)
        assert d.has_header is False
        assert any("high_numeric" in r for r in d.reasons)


# ── 4) Edge cases ──────────────────────────────────────────────────────────────

class TestEdgeCases:

    def test_empty_rows_list(self):
        d = detect_header_row([])
        assert d.has_header is False
        assert d.confidence == 0.0
        assert "no_rows" in d.reasons

    def test_empty_first_row(self):
        d = detect_header_row([["", "  ", ""]])
        assert d.has_header is False
        assert "empty_first_row" in d.reasons

    def test_single_row_no_header_tokens(self):
        """Single row without header tokens → data."""
        d = detect_header_row([["Болт М12", "100", "шт"]])
        assert d.has_header is False

    def test_single_row_with_header_tokens(self):
        """Single row that is all header tokens (no data to compare) → still header."""
        d = detect_header_row([["Наименование", "Кол-во", "Ед."]])
        assert d.has_header is True

    def test_docai_had_header_without_tokens_no_false_positive(self):
        """DocAI marking alone (without tokens) must NOT produce a false positive."""
        # "Гайка", "М8" — no header tokens, no item pattern, short cells
        d = detect_header_row([["Гайка", "М8"], ["Болт", "М10"]], docai_had_header=True)
        assert d.has_header is False

    def test_reasons_always_list(self):
        d = detect_header_row([["Наименование", "Кол-во"]])
        assert isinstance(d.reasons, list)

    def test_confidence_clamped_to_0_1(self):
        """Even with all negative signals, confidence stays in [0, 1]."""
        rows = _rows(
            ["ГОСТ 7798", "DIN 933", "100", "8.8"],
            ["Болт", "50", "шт", "оцинк"],
        )
        d = detect_header_row(rows)
        assert 0.0 <= d.confidence <= 1.0

    def test_header_was_guessed_true_when_no_docai(self):
        rows = _rows(
            ["Наименование", "Кол-во"],
            ["Болт", "100"],
        )
        d = detect_header_row(rows, docai_had_header=False)
        assert d.has_header is True
        assert d.header_was_guessed is True


# ── 5) Data-row comparison bonus (part E) ─────────────────────────────────────

class TestDataRowComparisonBonus:

    def test_numeric_data_columns_boost_header_detection(self):
        """When data rows have clearly numeric columns not present in header → boost."""
        rows = [
            ["Наименование", "Кол-во"],    # header row
            ["Болт М12x50",  "100"],        # data row
            ["Гайка М12",    "200"],        # data row
            ["Шайба 12",     "300"],        # data row
        ]
        d = detect_header_row(rows)
        assert d.has_header is True
        # Bonus from data comparison should appear
        assert any("data_numeric_cols" in r for r in d.reasons)

    def test_no_bonus_when_too_few_rows(self):
        """Part E needs more than 2 rows; with exactly 2 → no bonus."""
        rows = [
            ["Наименование", "Кол-во"],
            ["Болт М12", "100"],
        ]
        d = detect_header_row(rows)
        # Still detects header via tokens, but no "data_numeric_cols" bonus
        assert d.has_header is True
        assert not any("data_numeric_cols" in r for r in d.reasons)
