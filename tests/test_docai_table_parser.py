"""Unit tests for app.parsing.docai_table_parser.

All cases from the spec + edge cases:
- parse_qty_uom: UOM before qty, OCR junk, тыс. multiplier, decimal comma,
  split combined cell, header hint, direct, not_found.
- detect_columns: name / qty / uom auto-detection from header and data.
- build_canonical_df: end-to-end DataFrame construction.
"""
from __future__ import annotations

import pandas as pd
import pytest

from app.parsing.docai_table_parser import (
    EXTRA_COL_PREFIX,
    build_canonical_df,
    detect_columns,
    parse_qty_uom,
)


# ── parse_qty_uom ─────────────────────────────────────────────────────────────

class TestParseQtyUom:
    # ── Spec cases ────────────────────────────────────────────────────────────

    def test_uom_before_qty_kg4(self):
        """'КГ 4' in uom cell → qty=4, uom='кг'."""
        qty, uom, src = parse_qty_uom(None, "КГ 4")
        assert qty == 4
        assert uom == "кг"
        assert src == "uom_before_qty"

    def test_uom_before_qty_3kg(self):
        """'3 КГ' in uom cell → qty=3, uom='кг'."""
        qty, uom, src = parse_qty_uom(None, "3 КГ")
        assert qty == 3
        assert uom == "кг"
        assert src == "num_uom_in_uom_col"

    def test_ocr_junk_n_kg(self):
        """'N КГ' in uom cell + '250' in qty cell → qty=250, uom='кг'."""
        qty, uom, src = parse_qty_uom("250", "N КГ")
        assert qty == 250
        assert uom == "кг"
        assert src == "ocr_junk_uom"

    def test_thous_multiplier_2_5(self):
        """'тыс. шт' in uom cell + '2,5' in qty cell → qty=2500, uom='шт'."""
        qty, uom, src = parse_qty_uom("2,5", "тыс. шт")
        assert qty == 2500
        assert uom == "шт"
        assert src == "thous_mult"

    def test_direct_qty_uom(self):
        """qty_col='200', uom_col='ШТ' → direct match."""
        qty, uom, src = parse_qty_uom("200", "ШТ")
        assert qty == 200
        assert uom == "шт"
        assert src == "direct"

    def test_split_qty_col(self):
        """qty_col='200 шт', uom_col=None → split from qty cell."""
        qty, uom, src = parse_qty_uom("200 шт", None)
        assert qty == 200
        assert uom == "шт"
        assert src == "split_qty_col"

    # ── Additional cases ──────────────────────────────────────────────────────

    def test_decimal_comma(self):
        """Decimal comma: '1,5' → 1.5."""
        qty, uom, src = parse_qty_uom("1,5", "кг")
        assert qty == pytest.approx(1.5)
        assert uom == "кг"
        assert src == "direct"

    def test_thous_in_uom_cell_self_contained(self):
        """'2,5 тыс. шт' fully in the uom cell."""
        qty, uom, src = parse_qty_uom(None, "2,5 тыс. шт")
        assert qty == 2500
        assert uom == "шт"
        assert src == "thous_in_uom"

    def test_thous_in_qty_cell(self):
        """'10 тыс. кг' fully in the qty cell."""
        qty, uom, src = parse_qty_uom("10 тыс. кг", None)
        assert qty == 10000
        assert uom == "кг"
        assert src == "thous_in_qty"

    def test_header_hint(self):
        """qty_col='5', no uom_col, but header says 'Кол-во, шт'."""
        qty, uom, src = parse_qty_uom("5", None, header_hint="Кол-во, шт")
        assert qty == 5
        assert uom == "шт"
        assert src == "header_hint"

    def test_uom_before_qty_sht250(self):
        """'ШТ 250' in uom cell → qty=250, uom='шт'."""
        qty, uom, src = parse_qty_uom(None, "ШТ 250")
        assert qty == 250
        assert uom == "шт"
        assert src == "uom_before_qty"

    def test_not_found_when_both_empty(self):
        """Both cells empty → not_found, qty=None, uom=None."""
        qty, uom, src = parse_qty_uom(None, None)
        assert qty is None
        assert uom is None
        assert src == "not_found"

    def test_not_found_no_uom(self):
        """Only a plain number, no UOM anywhere → not_found (strict policy)."""
        qty, uom, src = parse_qty_uom("100", None)
        assert qty is None
        assert uom is None
        assert src == "not_found"

    def test_num_uom_in_uom_col_tons(self):
        """'3 т' in uom cell, qty cell empty → qty=3, uom='т'."""
        qty, uom, src = parse_qty_uom(None, "3 т")
        assert qty == 3
        assert uom == "т"
        assert src == "num_uom_in_uom_col"

    def test_uom_before_qty_lowercase(self):
        """'кг 4' (lowercase) in uom cell."""
        qty, uom, src = parse_qty_uom(None, "кг 4")
        assert qty == 4
        assert uom == "кг"

    def test_integer_qty_value(self):
        """Whole-number quantity: value is correct (int coercion lives in build_canonical_df)."""
        qty, uom, src = parse_qty_uom("100", "шт")
        assert qty == 100
        assert uom == "шт"

    def test_float_qty_stays_float(self):
        """Non-whole quantity stays as float."""
        qty, uom, src = parse_qty_uom("1.5", "кг")
        assert isinstance(qty, float)
        assert qty == pytest.approx(1.5)

    def test_uom_dot_suffix(self):
        """UOM with trailing dot: 'шт.' → 'шт'."""
        qty, uom, src = parse_qty_uom("10", "шт.")
        assert qty == 10
        assert uom == "шт"
        assert src == "direct"

    def test_thous_mult_kg(self):
        """тыс. keyword in uom cell with kg token."""
        qty, uom, src = parse_qty_uom("3", "тыс. кг")
        assert qty == 3000
        assert uom == "кг"

    def test_ocr_junk_dash_prefix(self):
        """'- кг' in uom cell + qty cell has number."""
        qty, uom, src = parse_qty_uom("50", "- кг")
        assert qty == 50
        assert uom == "кг"
        assert src == "ocr_junk_uom"

    def test_split_qty_col_with_space(self):
        """'100 уп' in qty cell (space before UOM)."""
        qty, uom, src = parse_qty_uom("100 уп", None)
        assert qty == 100
        assert uom == "уп"
        assert src == "split_qty_col"


# ── detect_columns ────────────────────────────────────────────────────────────

class TestDetectColumns:

    def test_typical_four_col_table(self):
        """Standard № / Name / Qty / UOM layout."""
        headers = ["№", "Наименование", "Кол-во", "Ед."]
        rows = [
            ["1", "Болт М10x45 ГОСТ 7798", "250", "шт"],
            ["2", "Гайка М10 ГОСТ 5915", "500", "шт"],
            ["3", "Шайба 10 ГОСТ 11371", "1000", "шт"],
        ]
        result = detect_columns(headers, rows)
        assert result["name_idx"] == 1   # longest text col
        assert result["qty_idx"] == 2
        assert result["uom_idx"] == 3

    def test_no_header_two_cols_name_qty(self):
        """Two columns: name (text) and qty (numeric) — no UOM."""
        headers = []
        rows = [
            ["Болт М12x60 ГОСТ 7798-70", "100"],
            ["Гайка М12 ГОСТ 5915-70", "200"],
        ]
        result = detect_columns(headers, rows)
        assert result["name_idx"] == 0
        assert result["qty_idx"] == 1
        assert result["uom_idx"] is None

    def test_header_keyword_bonus(self):
        """'Кол-во' header boosts qty detection even for mixed column."""
        headers = ["Наименование", "Кол-во, шт", "Покрытие"]
        rows = [
            ["Болт М10", "50", "цинк"],
            ["Болт М12", "75", "нержавейка"],
        ]
        result = detect_columns(headers, rows)
        assert result["qty_idx"] == 1
        # header_hints should carry the UOM extracted from "Кол-во, шт"
        assert result["header_hints"].get("1") == "шт"

    def test_single_column(self):
        """Single-column table → name_idx=0, qty/uom=None."""
        result = detect_columns([], [["Болт М10x45"], ["Гайка М10"]])
        assert result["name_idx"] == 0
        assert result["qty_idx"] is None
        assert result["uom_idx"] is None

    def test_empty_rows(self):
        """Empty row list → safe defaults."""
        result = detect_columns([], [])
        assert result["name_idx"] == 0
        assert result["qty_idx"] is None
        assert result["uom_idx"] is None

    def test_uom_header_keyword(self):
        """'Ед.изм' header boosts uom detection."""
        headers = ["Наименование", "Количество", "Ед.изм"]
        rows = [
            ["Болт М10", "100", "шт"],
            ["Болт М12", "200", "кг"],
        ]
        result = detect_columns(headers, rows)
        assert result["uom_idx"] == 2

    def test_ocr_junk_in_uom_column(self):
        """UOM column with OCR junk ('N КГ') still detected as UOM."""
        headers = []
        rows = [
            ["Болт М10 ГОСТ 7798", "250", "N КГ"],
            ["Гайка М10 ГОСТ 5915", "500", "ШТ"],
        ]
        result = detect_columns(headers, rows)
        assert result["uom_idx"] == 2


# ── build_canonical_df ────────────────────────────────────────────────────────

class TestBuildCanonicalDf:

    def _make_col_df(self, rows: list[list[str]]) -> pd.DataFrame:
        n_cols = max(len(r) for r in rows)
        col_names = [f"col_{i}" for i in range(n_cols)]
        padded = [r + [""] * (n_cols - len(r)) for r in rows]
        return pd.DataFrame(padded, columns=col_names)

    def test_basic_four_col(self):
        rows = [
            ["Болт М10x45", "250", "шт", "термодиффузия"],
            ["Гайка М10",   "500", "кг", "цинк"],
        ]
        df = self._make_col_df(rows)
        col_map = {"name_idx": 0, "qty_idx": 1, "uom_idx": 2, "header_hints": {}}
        result = build_canonical_df(df, ["Наим.", "Кол-во", "Ед.", "Покрытие"], col_map)

        assert list(result["name"]) == ["Болт М10x45", "Гайка М10"]
        assert list(result["qty"]) == [250, 500]
        assert list(result["uom"]) == ["шт", "кг"]
        # Extra col 3 should appear as _docai_extra_3
        assert f"{EXTRA_COL_PREFIX}3" in result.columns

    def test_ocr_junk_n_kg(self):
        """End-to-end: uom cell 'N КГ' → qty from qty col, uom='кг'."""
        rows = [["Болт М12", "100", "N КГ"]]
        df = self._make_col_df(rows)
        col_map = {"name_idx": 0, "qty_idx": 1, "uom_idx": 2, "header_hints": {}}
        result = build_canonical_df(df, [], col_map)

        assert result.iloc[0]["qty"] == 100
        assert result.iloc[0]["uom"] == "кг"

    def test_thous_multiplier(self):
        rows = [["Дюбель 6x40", "2,5", "тыс. шт"]]
        df = self._make_col_df(rows)
        col_map = {"name_idx": 0, "qty_idx": 1, "uom_idx": 2, "header_hints": {}}
        result = build_canonical_df(df, [], col_map)

        assert result.iloc[0]["qty"] == 2500
        assert result.iloc[0]["uom"] == "шт"

    def test_name_only_no_qty_uom(self):
        """No qty/uom columns → name extracted, qty/uom=None."""
        rows = [["Болт М10x45 ГОСТ 7798-70"]]
        df = self._make_col_df(rows)
        col_map = {"name_idx": 0, "qty_idx": None, "uom_idx": None, "header_hints": {}}
        result = build_canonical_df(df, [], col_map)

        assert result.iloc[0]["name"] == "Болт М10x45 ГОСТ 7798-70"
        assert result.iloc[0]["qty"] is None
        assert result.iloc[0]["uom"] is None

    def test_name_column_clean(self):
        """Name column must NOT contain qty/uom cell values."""
        rows = [["Болт М10x45", "250", "шт"]]
        df = self._make_col_df(rows)
        col_map = {"name_idx": 0, "qty_idx": 1, "uom_idx": 2, "header_hints": {}}
        result = build_canonical_df(df, [], col_map)

        name = result.iloc[0]["name"]
        assert "250" not in name
        assert "шт" not in name

    def test_qty_source_recorded(self):
        """qty_uom_source column must be present."""
        rows = [["Болт", "100", "шт"]]
        df = self._make_col_df(rows)
        col_map = {"name_idx": 0, "qty_idx": 1, "uom_idx": 2, "header_hints": {}}
        result = build_canonical_df(df, [], col_map)

        assert "qty_uom_source" in result.columns
        assert result.iloc[0]["qty_uom_source"] == "direct"

    def test_empty_dataframe(self):
        """Empty input → empty output with correct columns."""
        df = pd.DataFrame(columns=["col_0"])
        col_map = {"name_idx": 0, "qty_idx": None, "uom_idx": None, "header_hints": {}}
        result = build_canonical_df(df, [], col_map)
        assert "name" in result.columns
        assert len(result) == 0
