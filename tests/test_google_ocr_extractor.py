"""Unit tests for app.services.google_ocr_extractor.

All tests use plain dict mocks — no network calls, no Google SDK required.
The document dict structure mirrors what MessageToDict(document._pb) returns.
"""
import pytest

from app.services.google_ocr_extractor import (
    ExtractResult,
    _avg_confidence,
    _cell_text,
    _detect_column_role,
    _extract_best_table,
    _extract_lines,
    _extract_paragraphs,
    _filter_product_lines,
    _find_product_table,
    _header_contains,
    _is_product_table_header,
    _is_summary_row,
    _layout_text,
    _map_columns,
    _parse_float,
    _table_score,
    _text_for_segment,
    extract_rows,
)


# ── Helpers for building mock document dicts ──────────────────────────────────

def _segment(start: int, end: int) -> dict:
    return {"startIndex": str(start), "endIndex": str(end)}


def _layout(start: int, end: int, confidence: float | None = None) -> dict:
    layout = {"textAnchor": {"textSegments": [_segment(start, end)]}}
    if confidence is not None:
        layout["confidence"] = confidence
    return layout


def _cell(start: int, end: int) -> dict:
    return {"layout": _layout(start, end)}


def _row(cells: list[dict]) -> dict:
    return {"cells": cells}


def _table(header_rows: list, body_rows: list) -> dict:
    return {"headerRows": header_rows, "bodyRows": body_rows}


def _paragraph(start: int, end: int) -> dict:
    return {"layout": _layout(start, end)}


def _line(start: int, end: int) -> dict:
    return {"layout": _layout(start, end)}


def _block(start: int, end: int, confidence: float) -> dict:
    return {"layout": _layout(start, end, confidence)}


def _page(
    tables: list | None = None,
    paragraphs: list | None = None,
    lines: list | None = None,
    blocks: list | None = None,
) -> dict:
    p: dict = {}
    if tables     is not None: p["tables"]     = tables
    if paragraphs is not None: p["paragraphs"] = paragraphs
    if lines      is not None: p["lines"]      = lines
    if blocks     is not None: p["blocks"]     = blocks
    return p


def _doc(text: str, pages: list) -> dict:
    return {"text": text, "pages": pages}


# ── Helpers for building text-offset tables ──────────────────────────────────

def _make_text_table(headers: list[str], data_rows: list[list[str]]):
    """Build a (text, table_dict, page) from header strings and row strings.

    Returns (full_text, table_dict, page_dict) where table_dict uses the
    correct text offsets.
    """
    all_cells: list[str] = list(headers)
    for row in data_rows:
        all_cells.extend(row)

    # Build text with | separators for readability
    full_text = "|".join(all_cells)

    # Build offset map
    offsets: list[tuple[int, int]] = []
    pos = 0
    for cell_text in all_cells:
        start = pos
        end = pos + len(cell_text)
        offsets.append((start, end))
        pos = end + 1  # +1 for the "|" separator

    ncols = len(headers)
    # Build header row
    h_cells = [_cell(offsets[i][0], offsets[i][1]) for i in range(ncols)]
    h_row = _row(h_cells)

    # Build body rows
    b_rows = []
    idx = ncols
    for row in data_rows:
        r_cells = [_cell(offsets[idx + j][0], offsets[idx + j][1]) for j in range(len(row))]
        b_rows.append(_row(r_cells))
        idx += len(row)

    tbl = _table(header_rows=[h_row], body_rows=b_rows)
    page = _page(tables=[tbl])
    return full_text, tbl, page


# ── _text_for_segment ─────────────────────────────────────────────────────────

def test_text_for_segment_basic():
    assert _text_for_segment("Hello World", {"startIndex": "6", "endIndex": "11"}) == "World"


def test_text_for_segment_missing_index():
    assert _text_for_segment("ABC", {}) == ""


# ── _layout_text ──────────────────────────────────────────────────────────────

def test_layout_text_joins_segments():
    text = "ABCDEF"
    layout = {
        "textAnchor": {
            "textSegments": [_segment(0, 3), _segment(3, 6)]
        }
    }
    assert _layout_text(text, layout) == "ABCDEF"


def test_layout_text_empty_layout():
    assert _layout_text("hello", {}) == ""


# ── _table_score ──────────────────────────────────────────────────────────────

def test_table_score_all_filled_with_numbers():
    text = "Болт М12x60 DIN 931 100 шт"
    t = _table(
        header_rows=[],
        body_rows=[
            _row([_cell(0, 4), _cell(5, 9), _cell(10, 14)]),
            _row([_cell(15, 18), _cell(19, 22), _cell(23, 26)]),
        ],
    )
    score = _table_score(t, text)
    assert score == pytest.approx(2.2, abs=0.01)


def test_table_score_empty_table():
    score = _table_score({"headerRows": [], "bodyRows": []}, "")
    assert score == 0.0


def test_table_score_partially_filled_no_numbers():
    text = "ABCDE"
    t = _table(
        header_rows=[_row([_cell(0, 3), _cell(0, 0)])],
        body_rows=[],
    )
    score = _table_score(t, text)
    assert score == pytest.approx(0.5, abs=0.01)


# ── Product table detection ──────────────────────────────────────────────────

class TestProductTableDetection:

    def test_header_contains_name_keyword(self):
        assert _header_contains(["Артикул", "Товары (работы, услуги)", "Кол-во"], _is_product_table_header.__code__.co_consts[0] if False else {"товар"})
        # Direct test via _is_product_table_header
        assert _is_product_table_header(["Артикул", "Товары (работы, услуги)", "Кол-во", "Цена", "Сумма"])

    def test_invoice_table_detected(self):
        """Table from the user's example invoice should be detected as product table."""
        headers = ["№", "Артикул", "Товары (работы, услуги)", "Кол-во", "Ед", "Цена", "Кол-во шт", "Цена за шт", "Сумма"]
        assert _is_product_table_header(headers)

    def test_debt_table_not_detected(self):
        """Debt/balance table should NOT be detected as product table."""
        headers = ["Тип задолженности", "Дата планового погашения", "Долг клиента", "Счет на оплату"]
        assert not _is_product_table_header(headers)

    def test_simple_product_table(self):
        headers = ["Наименование", "Кол-во", "Цена", "Сумма"]
        assert _is_product_table_header(headers)

    def test_english_product_table(self):
        headers = ["Item", "Qty", "Price", "Total"]
        assert _is_product_table_header(headers)

    def test_no_name_column_not_product(self):
        headers = ["Кол-во", "Цена", "Сумма"]
        assert not _is_product_table_header(headers)

    def test_no_numeric_column_not_product(self):
        headers = ["Наименование", "Описание", "Примечание"]
        assert not _is_product_table_header(headers)


class TestColumnMapping:

    def test_full_invoice_mapping(self):
        headers = ["№", "Артикул", "Товары (работы, услуги)", "Кол-во", "Ед", "Цена", "Кол-во шт", "Цена за шт", "Сумма"]
        mapping = _map_columns(headers)
        assert mapping["name"] == 2  # "Товары (работы, услуги)"
        assert mapping["qty"] == 3   # "Кол-во"
        assert mapping["unit"] == 4  # "Ед"
        assert "price_total" in mapping  # "Сумма"

    def test_simple_mapping(self):
        headers = ["Наименование", "Количество", "Ед.", "Цена", "Сумма"]
        mapping = _map_columns(headers)
        assert mapping["name"] == 0
        assert mapping["qty"] == 1
        assert mapping["unit"] == 2
        assert mapping["price_unit"] == 3
        assert mapping["price_total"] == 4

    def test_detect_column_role_name(self):
        assert _detect_column_role("Товары (работы, услуги)") == "name"
        assert _detect_column_role("Наименование") == "name"

    def test_detect_column_role_qty(self):
        assert _detect_column_role("Кол-во") == "qty"
        assert _detect_column_role("Количество") == "qty"
        assert _detect_column_role("Qty") == "qty"

    def test_detect_column_role_unit(self):
        assert _detect_column_role("Ед") == "unit"
        assert _detect_column_role("Ед.") == "unit"

    def test_detect_column_role_price(self):
        assert _detect_column_role("Цена") == "price_unit"
        assert _detect_column_role("Цена за шт") == "price_unit"

    def test_detect_column_role_total(self):
        assert _detect_column_role("Сумма") == "price_total"
        assert _detect_column_role("Стоимость") == "price_total"

    def test_detect_column_role_unknown(self):
        assert _detect_column_role("Артикул") is None
        assert _detect_column_role("№") is None


class TestSummaryRowFilter:

    def test_itogo_filtered(self):
        assert _is_summary_row(["", "Итого", "", "1000.00"])

    def test_nds_filtered(self):
        assert _is_summary_row(["", "В т.ч. НДС", "", "180.00"])

    def test_vsego_filtered(self):
        assert _is_summary_row(["Всего к оплате:", "5000"])

    def test_product_row_not_filtered(self):
        assert not _is_summary_row(["Болт М12x60", "100", "шт", "50.00", "5000.00"])

    def test_empty_row_not_filtered(self):
        assert not _is_summary_row(["", "", ""])


class TestParseFloat:

    def test_normal(self):
        assert _parse_float("100.50") == 100.5

    def test_comma(self):
        assert _parse_float("1 234,56") == 1234.56

    def test_empty(self):
        assert _parse_float("") is None
        assert _parse_float(None) is None

    def test_non_numeric(self):
        assert _parse_float("abc") is None


class TestFindProductTable:

    def test_finds_product_table_among_multiple(self):
        """Product table is found even when another table (debt) has more rows."""
        # Build a product table
        product_text, product_tbl, _ = _make_text_table(
            ["Наименование", "Кол-во", "Цена", "Сумма"],
            [["Болт М12x60 DIN 933", "100", "50.00", "5000.00"]],
        )
        # Build a debt table (more rows)
        debt_headers = ["Тип задолженности", "Долг клиента"]
        debt_data = [
            ["Текущая", "5300896.15"],
            ["Просроченная", "120000.00"],
            ["Баланс", "5420896.15"],
        ]
        all_cells = debt_headers[:]
        for r in debt_data:
            all_cells.extend(r)
        debt_text = "|".join(all_cells)

        full_text = product_text + "||" + debt_text
        offset = len(product_text) + 2

        # Rebuild debt table with offsets
        debt_offsets = []
        pos = offset
        for cell in all_cells:
            debt_offsets.append((pos, pos + len(cell)))
            pos += len(cell) + 1

        ncols = len(debt_headers)
        dh_cells = [_cell(debt_offsets[i][0], debt_offsets[i][1]) for i in range(ncols)]
        db_rows = []
        idx = ncols
        for r in debt_data:
            rc = [_cell(debt_offsets[idx + j][0], debt_offsets[idx + j][1]) for j in range(len(r))]
            db_rows.append(_row(rc))
            idx += len(r)
        debt_tbl = _table(header_rows=[_row(dh_cells)], body_rows=db_rows)

        page = _page(tables=[product_tbl, debt_tbl])
        result = _find_product_table([page], full_text)

        assert result is not None
        data_rows, header, total_tables = result
        assert total_tables == 2
        assert len(data_rows) == 1  # single product row
        assert "Наименование" in header[0]

    def test_returns_none_for_no_product_table(self):
        text, tbl, page = _make_text_table(
            ["Тип задолженности", "Долг"],
            [["Текущая", "5000"]],
        )
        assert _find_product_table([page], text) is None

    def test_single_row_product_table(self):
        """A product table with just 1 data row is valid."""
        text, tbl, page = _make_text_table(
            ["Товар", "Кол-во", "Цена"],
            [["Шайба М16 DIN 125", "200", "15.00"]],
        )
        result = _find_product_table([page], text)
        assert result is not None
        data_rows, header, _ = result
        assert len(data_rows) == 1


class TestExtractStructuredRows:

    def test_full_extraction(self):
        """Full pipeline: product table → structured rows with all fields."""
        headers = ["№", "Наименование", "Кол-во", "Ед.", "Цена", "Сумма"]
        data = [
            ["1", "Болт М12x60 DIN 933", "100", "шт", "50.00", "5000.00"],
            ["2", "Гайка М12 DIN 934", "100", "шт", "10.00", "1000.00"],
            ["", "Итого", "", "", "", "6000.00"],
        ]
        text, tbl, page = _make_text_table(headers, [cell for row in data for cell in [row]])

        # Need to rebuild properly — _make_text_table expects flat rows
        # Let's build manually
        all_cells = headers[:]
        for row in data:
            all_cells.extend(row)
        full_text = "|".join(all_cells)

        offsets = []
        pos = 0
        for c in all_cells:
            offsets.append((pos, pos + len(c)))
            pos += len(c) + 1

        ncols = len(headers)
        h_cells = [_cell(offsets[i][0], offsets[i][1]) for i in range(ncols)]
        b_rows = []
        idx = ncols
        for row in data:
            rc = [_cell(offsets[idx + j][0], offsets[idx + j][1]) for j in range(len(row))]
            b_rows.append(_row(rc))
            idx += len(row)

        tbl = _table(header_rows=[_row(h_cells)], body_rows=b_rows)
        page = _page(tables=[tbl])
        doc = _doc(full_text, [page])

        result = extract_rows(doc)
        assert result.mode == "product_table"
        assert len(result.structured_rows) == 2  # "Итого" filtered out
        assert result.structured_rows[0]["name"] == "Болт М12x60 DIN 933"
        assert result.structured_rows[0]["qty"] == 100.0
        assert result.structured_rows[0]["unit"] == "шт"
        assert result.structured_rows[0]["price_unit"] == 50.0
        assert result.structured_rows[0]["price_total"] == 5000.0

    def test_invoice_example_from_spec(self):
        """The specific invoice format from the user's problem description."""
        headers = ["№", "Артикул", "Товары (работы, услуги)", "Кол-во", "Ед", "Цена", "Кол-во шт", "Цена за шт", "Сумма"]
        data = [
            ["1", "ART-001", "Болт М16x80 ГОСТ 7798-70 кл.пр. 8.8", "1", "кг", "350.00", "50", "7.00", "350.00"],
        ]

        all_cells = headers[:]
        for row in data:
            all_cells.extend(row)
        full_text = "|".join(all_cells)

        offsets = []
        pos = 0
        for c in all_cells:
            offsets.append((pos, pos + len(c)))
            pos += len(c) + 1

        ncols = len(headers)
        h_cells = [_cell(offsets[i][0], offsets[i][1]) for i in range(ncols)]
        b_rows = []
        idx = ncols
        for row in data:
            rc = [_cell(offsets[idx + j][0], offsets[idx + j][1]) for j in range(len(row))]
            b_rows.append(_row(rc))
            idx += len(row)

        tbl = _table(header_rows=[_row(h_cells)], body_rows=b_rows)
        page = _page(tables=[tbl])
        doc = _doc(full_text, [page])

        result = extract_rows(doc)
        assert result.mode == "product_table"
        assert len(result.structured_rows) == 1
        sr = result.structured_rows[0]
        assert "Болт М16x80" in sr["name"]
        assert sr["qty"] == 1.0
        assert sr["unit"] == "кг"
        assert sr["price_total"] == 350.0

    def test_summary_rows_filtered(self):
        """Rows with 'Итого', 'НДС', 'Всего' are filtered out."""
        headers = ["Наименование", "Кол-во", "Сумма"]
        data = [
            ["Гайка М8", "50", "500"],
            ["Итого", "", "500"],
            ["В т.ч. НДС", "", "83.33"],
        ]

        all_cells = headers[:]
        for row in data:
            all_cells.extend(row)
        full_text = "|".join(all_cells)

        offsets = []
        pos = 0
        for c in all_cells:
            offsets.append((pos, pos + len(c)))
            pos += len(c) + 1

        ncols = len(headers)
        h_cells = [_cell(offsets[i][0], offsets[i][1]) for i in range(ncols)]
        b_rows = []
        idx = ncols
        for row in data:
            rc = [_cell(offsets[idx + j][0], offsets[idx + j][1]) for j in range(len(row))]
            b_rows.append(_row(rc))
            idx += len(row)

        tbl = _table(header_rows=[_row(h_cells)], body_rows=b_rows)
        doc = _doc(full_text, [_page(tables=[tbl])])

        result = extract_rows(doc)
        assert result.mode == "product_table"
        assert len(result.structured_rows) == 1
        assert result.structured_rows[0]["name"] == "Гайка М8"
        assert result.debug["rows_filtered"] == 2


class TestDebugInfo:

    def test_debug_contains_required_fields(self):
        headers = ["Товар", "Кол-во", "Сумма"]
        data = [["Болт М10", "10", "100"]]

        all_cells = headers + data[0]
        full_text = "|".join(all_cells)
        offsets = []
        pos = 0
        for c in all_cells:
            offsets.append((pos, pos + len(c)))
            pos += len(c) + 1

        ncols = len(headers)
        h_cells = [_cell(offsets[i][0], offsets[i][1]) for i in range(ncols)]
        rc = [_cell(offsets[ncols + j][0], offsets[ncols + j][1]) for j in range(len(data[0]))]

        tbl = _table(header_rows=[_row(h_cells)], body_rows=[_row(rc)])
        doc = _doc(full_text, [_page(tables=[tbl])])

        result = extract_rows(doc)
        assert result.debug["detected_table_type"] == "product"
        assert "columns_detected" in result.debug
        assert result.debug["rows_extracted"] == 1
        assert result.debug["rows_filtered"] == 0


# ── Fallback modes ───────────────────────────────────────────────────────────

class TestFallbackModes:

    def test_generic_table_fallback(self):
        """When no product table header, falls back to generic best-table."""
        text = "Текущая|5300|Просроченная|120"
        t = _table(
            header_rows=[],
            body_rows=[
                _row([_cell(0, 7), _cell(8, 12)]),
                _row([_cell(13, 26), _cell(27, 30)]),
            ],
        )
        doc = _doc(text, [_page(tables=[t])])
        result = extract_rows(doc)
        assert result.mode == "table"
        assert result.structured_rows == []

    def test_paragraph_fallback(self):
        text = "Строка первая\nСтрока вторая\n"
        pages = [_page(paragraphs=[_paragraph(0, 13), _paragraph(14, 27)])]
        doc = _doc(text, pages)
        result = extract_rows(doc)
        assert result.mode == "paragraph"

    def test_line_fallback(self):
        text = "Шайба М6\n"
        pages = [_page(lines=[_line(0, 8)])]
        doc = _doc(text, pages)
        result = extract_rows(doc)
        assert result.mode == "line"
        assert result.rows == [["Шайба М6"]]

    def test_empty_document(self):
        doc = _doc("", [])
        result = extract_rows(doc)
        assert result.mode == "line"
        assert result.rows == []

    def test_product_line_filter_in_paragraphs(self):
        """Paragraph mode with product patterns filters non-product lines."""
        text = "Болт М12x60 100 шт\nРеквизиты компании\n"
        pages = [_page(paragraphs=[_paragraph(0, 18), _paragraph(19, 39)])]
        doc = _doc(text, pages)
        result = extract_rows(doc)
        assert result.mode == "paragraph"
        # Only the product line should remain
        assert len(result.rows) == 1
        assert "Болт" in result.rows[0][0]


class TestFilterProductLines:

    def test_keeps_product_lines(self):
        rows = [["Болт М12x60 100 шт"], ["Просто текст"], ["Гайка М8 DIN 934 50 кг"]]
        filtered = _filter_product_lines(rows)
        assert len(filtered) == 2

    def test_empty_input(self):
        assert _filter_product_lines([]) == []

    def test_no_product_lines(self):
        rows = [["Реквизиты"], ["Адрес компании"]]
        assert _filter_product_lines(rows) == []


# ── Legacy helpers ───────────────────────────────────────────────────────────

def test_extract_best_table_picks_highest_score():
    text = "Болт М12x60 100 шт"
    small = _table(
        header_rows=[],
        body_rows=[_row([_cell(0, 4)])],
    )
    large = _table(
        header_rows=[],
        body_rows=[
            _row([_cell(5, 11), _cell(12, 15)]),
            _row([_cell(5, 11), _cell(12, 15)]),
            _row([_cell(5, 11), _cell(12, 15)]),
        ],
    )
    pages = [_page(tables=[small, large])]
    result = _extract_best_table(pages, text)
    assert result is not None
    all_rows_raw, docai_had_header, total_tables, shape = result
    assert total_tables == 2
    assert shape[0] == 3
    assert shape[1] == 2
    assert docai_had_header is False


def test_extract_best_table_none_when_no_tables():
    pages = [_page(paragraphs=[_paragraph(0, 5)])]
    assert _extract_best_table(pages, "Hello") is None


def test_extract_paragraphs_basic():
    text = "Строка первая\nСтрока вторая\n"
    pages = [_page(paragraphs=[_paragraph(0, 13), _paragraph(14, 27)])]
    rows = _extract_paragraphs(pages, text)
    assert len(rows) == 2
    assert rows[0] == ["Строка первая"]


def test_extract_lines_basic():
    text = "Линия 1\nЛиния 2\n"
    pages = [_page(lines=[_line(0, 7), _line(8, 15)])]
    rows = _extract_lines(pages, text)
    assert len(rows) == 2


def test_avg_confidence_basic():
    pages = [
        _page(blocks=[_block(0, 1, 0.9), _block(1, 2, 0.7)]),
        _page(blocks=[_block(2, 3, 0.8)]),
    ]
    avg = _avg_confidence(pages)
    assert avg == pytest.approx(80.0, abs=0.1)


def test_avg_confidence_no_blocks():
    assert _avg_confidence([_page()]) is None


def test_extract_rows_confidence_included():
    text = "Болт М12"
    pages = [_page(lines=[_line(0, 8)], blocks=[_block(0, 8, 0.95)])]
    doc = _doc(text, pages)
    result = extract_rows(doc)
    assert result.confidence_avg == pytest.approx(95.0, abs=0.1)
