"""Unit tests for app.services.google_ocr_extractor.

All tests use plain dict mocks — no network calls, no Google SDK required.
The document dict structure mirrors what MessageToDict(document._pb) returns.
"""
import pytest

from app.services.google_ocr_extractor import (
    ExtractResult,
    _avg_confidence,
    _cell_text,
    _extract_best_table,
    _extract_lines,
    _extract_paragraphs,
    _layout_text,
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


# ── _text_for_segment ─────────────────────────────────────────────────────────

def test_text_for_segment_basic():
    assert _text_for_segment("Hello World", {"startIndex": "6", "endIndex": "11"}) == "World"


def test_text_for_segment_missing_index():
    # missing keys → defaults to 0, returns empty string
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
    #      0123456789...
    # Two body rows, each with 3 cells (all filled, all have digits)
    t = _table(
        header_rows=[],
        body_rows=[
            _row([_cell(0, 4), _cell(5, 9), _cell(10, 14)]),
            _row([_cell(15, 18), _cell(19, 22), _cell(23, 26)]),
        ],
    )
    score = _table_score(t, text)
    # 2 rows × 1.0 fill_ratio + 0.2 (has numbers) = 2.2
    assert score == pytest.approx(2.2, abs=0.01)


def test_table_score_empty_table():
    score = _table_score({"headerRows": [], "bodyRows": []}, "")
    assert score == 0.0


def test_table_score_partially_filled_no_numbers():
    text = "ABCDE"
    # 1 row, 2 cells, only first filled
    t = _table(
        header_rows=[_row([_cell(0, 3), _cell(0, 0)])],
        body_rows=[],
    )
    score = _table_score(t, text)
    # 1 row × 0.5 fill + 0.0 = 0.5
    assert score == pytest.approx(0.5, abs=0.01)


# ── _extract_best_table ────────────────────────────────────────────────────────

def test_extract_best_table_picks_highest_score():
    # text:  0123456789012345
    text = "Болт М12x60 100 шт"
    # Small table: 1 body row, 1 col (text but no digits in range 0..4 "Болт")
    small = _table(
        header_rows=[],
        body_rows=[_row([_cell(0, 4)])],
    )
    # Large table: 3 body rows, 2 cols each (all have text with digits)
    large = _table(
        header_rows=[],
        body_rows=[
            _row([_cell(5, 11), _cell(12, 15)]),   # "М12x60" "100"
            _row([_cell(5, 11), _cell(12, 15)]),
            _row([_cell(5, 11), _cell(12, 15)]),
        ],
    )
    pages = [_page(tables=[small, large])]
    result = _extract_best_table(pages, text)
    assert result is not None
    all_rows_raw, docai_had_header, total_tables, shape = result
    assert total_tables == 2
    assert shape[0] == 3  # 3 rows in all_rows_raw (no DocAI header → all body)
    assert shape[1] == 2  # 2 cols
    assert docai_had_header is False


def test_extract_best_table_none_when_no_tables():
    pages = [_page(paragraphs=[_paragraph(0, 5)])]
    assert _extract_best_table(pages, "Hello") is None


def test_extract_best_table_single_table():
    text = "Гайка М8 DIN 934 50 шт"
    t = _table(
        header_rows=[_row([_cell(0, 5), _cell(6, 8)])],
        body_rows=[_row([_cell(0, 5), _cell(6, 8)])],
    )
    result = _extract_best_table([_page(tables=[t])], text)
    assert result is not None
    all_rows_raw, docai_had_header, total_tables, shape = result
    assert total_tables == 1
    assert len(all_rows_raw) == 2  # DocAI header row + 1 body row = 2 total
    assert docai_had_header is True
    assert shape == (2, 2)  # all 2 rows × 2 cols


# ── _extract_paragraphs ───────────────────────────────────────────────────────

def test_extract_paragraphs_basic():
    text = "Строка первая\nСтрока вторая\n"
    pages = [
        _page(paragraphs=[_paragraph(0, 13), _paragraph(14, 27)]),
    ]
    rows = _extract_paragraphs(pages, text)
    assert len(rows) == 2
    assert rows[0] == ["Строка первая"]
    assert rows[1] == ["Строка вторая"]


def test_extract_paragraphs_skips_empty():
    text = "Текст\n\n"
    pages = [_page(paragraphs=[_paragraph(0, 5), _paragraph(6, 7)])]
    rows = _extract_paragraphs(pages, text)
    # second paragraph is "\n" → stripped → empty → skipped
    assert len(rows) == 1
    assert rows[0] == ["Текст"]


# ── _extract_lines ────────────────────────────────────────────────────────────

def test_extract_lines_basic():
    text = "Линия 1\nЛиния 2\n"
    pages = [_page(lines=[_line(0, 7), _line(8, 15)])]
    rows = _extract_lines(pages, text)
    assert len(rows) == 2
    assert rows[0] == ["Линия 1"]
    assert rows[1] == ["Линия 2"]


# ── _avg_confidence ───────────────────────────────────────────────────────────

def test_avg_confidence_basic():
    text = "ABC"
    pages = [
        _page(blocks=[_block(0, 1, 0.9), _block(1, 2, 0.7)]),
        _page(blocks=[_block(2, 3, 0.8)]),
    ]
    avg = _avg_confidence(pages)
    assert avg == pytest.approx(80.0, abs=0.1)  # (0.9+0.7+0.8)/3 * 100


def test_avg_confidence_no_blocks():
    pages = [_page()]
    assert _avg_confidence(pages) is None


# ── extract_rows — integration ─────────────────────────────────────────────────

def test_extract_rows_table_mode():
    text = "Болт М12x60 100"
    t = _table(
        header_rows=[],
        body_rows=[
            _row([_cell(0, 4), _cell(5, 11)]),
            _row([_cell(5, 11), _cell(12, 15)]),
        ],
    )
    doc = _doc(text, [_page(tables=[t], paragraphs=[_paragraph(0, 4)])])
    result = extract_rows(doc)
    assert result.mode == "table"
    assert result.tables_count == 1
    assert result.pages_count == 1
    assert len(result.rows) == 2
    assert result.selected_table_shape == (2, 2)


def test_extract_rows_paragraph_mode_no_tables():
    text = "Гайка М10\nДИН 934\n"
    pages = [_page(paragraphs=[_paragraph(0, 9), _paragraph(10, 18)])]
    doc = _doc(text, pages)
    result = extract_rows(doc)
    assert result.mode == "paragraph"
    assert result.tables_count == 0
    assert len(result.rows) == 2
    assert result.selected_table_shape is None


def test_extract_rows_line_mode_fallback():
    text = "Шайба М6\n"
    pages = [_page(lines=[_line(0, 8)])]
    doc = _doc(text, pages)
    result = extract_rows(doc)
    assert result.mode == "line"
    assert result.rows == [["Шайба М6"]]


def test_extract_rows_empty_document():
    doc = _doc("", [])
    result = extract_rows(doc)
    assert result.mode == "line"
    assert result.rows == []
    assert result.pages_count == 0
    assert result.tables_count == 0
    assert result.confidence_avg is None


def test_extract_rows_confidence_included():
    text = "Болт М12"
    pages = [
        _page(
            lines=[_line(0, 8)],
            blocks=[_block(0, 8, 0.95)],
        )
    ]
    doc = _doc(text, pages)
    result = extract_rows(doc)
    assert result.confidence_avg == pytest.approx(95.0, abs=0.1)


def test_extract_rows_prefers_table_over_paragraphs():
    """When both tables and paragraphs exist, table mode wins."""
    text = "Болт М12 100"
    t = _table(
        header_rows=[],
        body_rows=[_row([_cell(0, 4), _cell(5, 8)])],
    )
    pages = [
        _page(
            tables=[t],
            paragraphs=[_paragraph(0, 4), _paragraph(5, 8)],
        )
    ]
    doc = _doc(text, pages)
    result = extract_rows(doc)
    assert result.mode == "table"
