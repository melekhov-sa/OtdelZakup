"""Unit tests for app.parsing.structured_list_parser.

Covers:
- parse_structured_list: numbered prefixes, тыс. multiplier, no-qty skip,
  multiple formats, dash separator, free-text fallback lines.
- parse_free_text: any line with qty+uom, skip no-qty, decimal comma, тыс.
- parsed_rows_to_df_data: round-trip to dict list.
"""
from __future__ import annotations

import pytest

from app.parsing.structured_list_parser import (
    ParsedRow,
    parse_free_text,
    parse_structured_list,
    parsed_rows_to_df_data,
)


# ── parse_structured_list ──────────────────────────────────────────────────────

class TestParseStructuredList:

    def test_simple_numbered_line(self):
        """'1) Болт М12x50 — 296 шт.' → name='Болт М12x50', qty=296, uom='шт'."""
        rows = parse_structured_list("1) Болт М12x50 — 296 шт.")
        assert len(rows) == 1
        r = rows[0]
        assert "Болт М12x50" in r.name_raw
        assert r.qty == 296
        assert r.uom == "шт"

    def test_hierarchical_prefix(self):
        """'1.1) M12x50 - 50 шт' — hierarchical numbering stripped."""
        rows = parse_structured_list("1.1) M12x50 - 50 шт")
        assert len(rows) == 1
        assert "M12x50" in rows[0].name_raw
        assert rows[0].qty == 50

    def test_thous_multiplier(self):
        """'2) Дюбель 6x40 — 2,5 тыс. шт.' → qty=2500."""
        rows = parse_structured_list("2) Дюбель 6x40 — 2,5 тыс. шт.")
        assert len(rows) == 1
        assert rows[0].qty == 2500
        assert rows[0].uom == "шт"
        assert rows[0].qty_multiplier == 1000

    def test_skip_no_qty(self):
        """Lines without qty+uom are silently skipped."""
        text = "1) Болт М12 — 100 шт.\n2) Примечание без количества\n3) Гайка М12 — 50 шт."
        rows = parse_structured_list(text)
        assert len(rows) == 2

    def test_multiple_lines(self):
        """Multiple numbered lines in one text block."""
        text = (
            "1) Болт М10x45 - 250 шт\n"
            "2) Гайка М10 - 500 шт\n"
            "3) Шайба 10 - 1000 шт\n"
        )
        rows = parse_structured_list(text)
        assert len(rows) == 3
        assert rows[0].qty == 250
        assert rows[1].qty == 500
        assert rows[2].qty == 1000

    def test_dash_separator(self):
        """Trailing dash/em-dash separator is stripped from name_raw."""
        rows = parse_structured_list("1) Болт М12x50 — 100 кг")
        assert len(rows) == 1
        name = rows[0].name_raw
        assert not name.endswith("—")
        assert not name.endswith("-")

    def test_colon_separator(self):
        """Trailing colon is stripped from name_raw."""
        rows = parse_structured_list("1) Метиз: 50 шт")
        assert len(rows) == 1
        assert not rows[0].name_raw.endswith(":")

    def test_decimal_comma(self):
        """Decimal comma in quantity: '1,5 кг' → qty=1.5."""
        rows = parse_structured_list("1) Материал А — 1,5 кг")
        assert len(rows) == 1
        assert rows[0].qty == pytest.approx(1.5)
        assert rows[0].uom == "кг"

    def test_dot_prefix(self):
        """'3. Болт М8 - 200 шт' — dot separator after number."""
        rows = parse_structured_list("3. Болт М8 - 200 шт")
        assert len(rows) == 1
        assert rows[0].qty == 200

    def test_decimal_number_not_stripped_as_prefix(self):
        """'3.14 не должно терять первую часть числа' — decimal not treated as prefix."""
        # "3.14 кг" → the "3.14" is NOT a numbering prefix (no letter after the dot)
        # so entire line is treated as name+qty tail; qty should come from tail
        text = "3.14 кг"
        rows = parse_structured_list(text)
        # "3.14 кг" has no name before the number, so name_raw will be empty/blank
        # but qty=3.14, uom='кг' should be extracted
        assert len(rows) == 1
        assert rows[0].qty == pytest.approx(3.14)
        assert rows[0].uom == "кг"

    def test_empty_text(self):
        """Empty string → empty list."""
        assert parse_structured_list("") == []

    def test_no_numbered_lines_with_qty(self):
        """Lines without numbers still parsed if they have qty (prefix strip is attempted)."""
        rows = parse_structured_list("Болт М12 — 100 шт.")
        # No numbering prefix to strip, but qty still found
        assert len(rows) == 1
        assert rows[0].qty == 100

    def test_thous_kg(self):
        """тыс. with кг unit."""
        rows = parse_structured_list("1) Проволока — 3 тыс. кг")
        assert len(rows) == 1
        assert rows[0].qty == 3000
        assert rows[0].uom == "кг"

    def test_raw_line_preserved(self):
        """raw_line must equal original line text."""
        original = "2) Болт М10x45 — 250 шт"
        rows = parse_structured_list(original)
        assert len(rows) == 1
        assert rows[0].raw_line == original

    def test_unit_кг(self):
        """кг unit correctly extracted."""
        rows = parse_structured_list("1) Материал Х — 10 кг")
        assert len(rows) == 1
        assert rows[0].uom == "кг"

    def test_unit_м(self):
        """м (metre) unit extracted."""
        rows = parse_structured_list("1) Труба d50 — 25 м")
        assert len(rows) == 1
        assert rows[0].uom == "м"

    def test_whitespace_only_lines_skipped(self):
        """Blank lines produce no rows."""
        text = "\n\n1) Болт М12 — 100 шт\n\n"
        rows = parse_structured_list(text)
        assert len(rows) == 1


# ── parse_free_text ────────────────────────────────────────────────────────────

class TestParseFreeText:

    def test_lines_with_qty_included(self):
        """Lines containing qty+uom are included; others skipped."""
        text = (
            "Счёт-фактура № 123\n"
            "Болт М12x50 - 100 шт\n"
            "Покупатель: ООО Ромашка\n"
            "Гайка М12 - 50 шт\n"
            "Итого: 150\n"
        )
        rows = parse_free_text(text)
        assert len(rows) == 2
        assert rows[0].qty == 100
        assert rows[1].qty == 50

    def test_no_qty_lines_skipped(self):
        """Text with no qty lines → empty list."""
        rows = parse_free_text("Заголовок документа\nОтдел закупок\nДата: 01.01.2025")
        assert len(rows) == 0

    def test_decimal_comma(self):
        """Decimal comma: '1,5 кг' → qty=1.5."""
        rows = parse_free_text("Материал А — 1,5 кг")
        assert len(rows) == 1
        assert rows[0].qty == pytest.approx(1.5)
        assert rows[0].uom == "кг"

    def test_thous_multiplier(self):
        """тыс. multiplier in free text."""
        rows = parse_free_text("Дюбель 6x40 — 10 тыс. шт.")
        assert len(rows) == 1
        assert rows[0].qty == 10000
        assert rows[0].uom == "шт"
        assert rows[0].qty_multiplier == 1000

    def test_no_prefix_strip(self):
        """In free_text mode, numbering is NOT stripped — it stays in name_raw."""
        rows = parse_free_text("1) Болт М12 — 100 шт.")
        assert len(rows) == 1
        # "1)" should NOT be stripped in free_text mode
        assert "1)" in rows[0].name_raw or rows[0].name_raw.startswith("1")

    def test_raw_line_preserved(self):
        """raw_line must equal original line text."""
        original = "Болт М10x45 250 шт"
        rows = parse_free_text(original)
        assert len(rows) == 1
        assert rows[0].raw_line == original

    def test_empty_text(self):
        """Empty string → empty list."""
        assert parse_free_text("") == []

    def test_multiline_mixed(self):
        """Mixed: header lines + data lines."""
        text = (
            "ПЕРЕЧЕНЬ МАТЕРИАЛОВ\n"
            "Болт М10x45 ГОСТ 7798-70 250 шт\n"
            "Гайка М10 ГОСТ 5915-70 500 шт\n"
            "Шайба 10 ГОСТ 11371-78 1000 шт\n"
            "Примечание: по согласованию\n"
        )
        rows = parse_free_text(text)
        assert len(rows) == 3


# ── parsed_rows_to_df_data ─────────────────────────────────────────────────────

class TestParsedRowsToDfData:

    def test_basic_conversion(self):
        """ParsedRow list → list of dicts with expected keys."""
        row = ParsedRow(raw_line="Болт — 100 шт", name_raw="Болт", qty=100, uom="шт")
        result = parsed_rows_to_df_data([row])
        assert len(result) == 1
        d = result[0]
        assert d["name"] == "Болт"
        assert d["qty"] == 100
        assert d["uom"] == "шт"
        assert "qty_uom_source" in d

    def test_empty_list(self):
        """Empty input → empty output."""
        assert parsed_rows_to_df_data([]) == []

    def test_multiple_rows(self):
        """Multiple rows converted correctly."""
        rows = [
            ParsedRow(raw_line="A — 10 шт", name_raw="A", qty=10, uom="шт"),
            ParsedRow(raw_line="B — 20 кг", name_raw="B", qty=20, uom="кг"),
        ]
        result = parsed_rows_to_df_data(rows)
        assert len(result) == 2
        assert result[0]["name"] == "A"
        assert result[1]["uom"] == "кг"
