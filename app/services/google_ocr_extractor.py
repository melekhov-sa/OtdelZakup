"""Extract structured rows from a Google Document AI response dict.

Priority:
  1. Tables   — pick the best table by score (rows × fill_ratio + number_bonus)
  2. Paragraphs — joined paragraph text, one per row
  3. Raw lines  — fallback when no paragraphs exist

No network calls; pure transformation of the already-fetched document dict.
"""
from __future__ import annotations

from dataclasses import dataclass, field


# ── Data model ─────────────────────────────────────────────────────────────────

@dataclass
class ExtractResult:
    rows: list[list[str]]
    mode: str                              # "table" | "paragraph" | "line"
    pages_count: int
    tables_count: int
    selected_table_shape: tuple[int, int] | None   # (nrows, ncols)
    confidence_avg: float | None
    header_row: list[str] = field(default_factory=list)  # Table header cells (empty if none)


# ── Internal helpers ───────────────────────────────────────────────────────────

def _text_for_segment(document_text: str, segment: dict) -> str:
    """Extract text slice from document.text using a textSegment dict."""
    start = int(segment.get("startIndex", 0))
    end   = int(segment.get("endIndex", 0))
    return document_text[start:end]


def _layout_text(document_text: str, layout: dict) -> str:
    """Reconstruct text for any layout object by joining all textSegments."""
    anchor = layout.get("textAnchor") or {}
    segments = anchor.get("textSegments") or []
    parts = [_text_for_segment(document_text, s) for s in segments]
    return "".join(parts).strip()


def _cell_text(document_text: str, cell: dict) -> str:
    return _layout_text(document_text, cell.get("layout") or {})


def _table_score(table: dict, document_text: str) -> float:
    """Heuristic score for a table: rows × fill_ratio + 0.2 if contains digits."""
    header_rows = table.get("headerRows") or []
    body_rows   = table.get("bodyRows") or []
    all_rows    = header_rows + body_rows
    n_rows      = len(all_rows)
    if n_rows == 0:
        return 0.0

    total_cells  = 0
    filled_cells = 0
    has_numbers  = False
    for row in all_rows:
        for cell in (row.get("cells") or []):
            total_cells += 1
            txt = _cell_text(document_text, cell)
            if txt:
                filled_cells += 1
                if any(ch.isdigit() for ch in txt):
                    has_numbers = True

    fill_ratio = filled_cells / total_cells if total_cells else 0.0
    return n_rows * fill_ratio + (0.2 if has_numbers else 0.0)


def _extract_best_table(
    pages: list, document_text: str
) -> tuple[list[str], list[list[str]], int, tuple[int, int]] | None:
    """Find the highest-scoring table across all pages.

    Returns ``(header_row, body_rows, total_tables_found, shape)`` or ``None``.

    ``header_row`` contains the text of the first headerRow (if any); it is
    separated from the body rows so the caller can use it as column headers
    for column-detection heuristics without treating it as data.
    """
    best_score   = -1.0
    best_headers: list[str]       = []
    best_body:    list[list[str]] = []
    best_shape:   tuple[int, int] = (0, 0)
    total_tables = 0

    for page in pages:
        for table in (page.get("tables") or []):
            total_tables += 1
            score = _table_score(table, document_text)
            if score > best_score:
                best_score = score

                header_rows_raw = table.get("headerRows") or []
                body_rows_raw   = table.get("bodyRows") or []

                # First header row → used as column labels
                if header_rows_raw:
                    best_headers = [
                        _cell_text(document_text, cell)
                        for cell in (header_rows_raw[0].get("cells") or [])
                    ]
                else:
                    best_headers = []

                # Body rows (+ remaining header rows as data if multiple headers)
                body: list[list[str]] = []
                for row in header_rows_raw[1:] + body_rows_raw:
                    cells_text = [
                        _cell_text(document_text, cell)
                        for cell in (row.get("cells") or [])
                    ]
                    body.append(cells_text)

                best_body = body
                n_cols = max(
                    (len(r) for r in ([best_headers] if best_headers else []) + body),
                    default=0,
                )
                best_shape = (len(body), n_cols)

    if not best_body and not best_headers:
        return None
    return best_headers, best_body, total_tables, best_shape


def _extract_paragraphs(pages: list, document_text: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for page in pages:
        for para in (page.get("paragraphs") or []):
            txt = _layout_text(document_text, para.get("layout") or {})
            if txt:
                rows.append([txt])
    return rows


def _extract_lines(pages: list, document_text: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for page in pages:
        for line in (page.get("lines") or []):
            txt = _layout_text(document_text, line.get("layout") or {})
            if txt:
                rows.append([txt])
    return rows


def _avg_confidence(pages: list) -> float | None:
    """Average layout confidence across all blocks on all pages, as a percentage."""
    values: list[float] = []
    for page in pages:
        for block in (page.get("blocks") or []):
            conf = (block.get("layout") or {}).get("confidence")
            if conf is not None:
                values.append(float(conf))
    if not values:
        return None
    return round(sum(values) / len(values) * 100, 1)


# ── Public API ─────────────────────────────────────────────────────────────────

def extract_rows(document: dict) -> ExtractResult:
    """Extract structured rows from a Google Document AI document dict.

    Falls back through table → paragraph → line extraction.
    """
    document_text: str = document.get("text") or ""
    pages: list        = document.get("pages") or []
    pages_count        = len(pages)
    confidence_avg     = _avg_confidence(pages)

    # 1. Tables
    table_result = _extract_best_table(pages, document_text)
    if table_result is not None:
        header_row, rows, tables_count, shape = table_result
        return ExtractResult(
            rows=rows,
            mode="table",
            pages_count=pages_count,
            tables_count=tables_count,
            selected_table_shape=shape,
            confidence_avg=confidence_avg,
            header_row=header_row,
        )

    # 2. Paragraphs
    para_rows = _extract_paragraphs(pages, document_text)
    if para_rows:
        return ExtractResult(
            rows=para_rows,
            mode="paragraph",
            pages_count=pages_count,
            tables_count=0,
            selected_table_shape=None,
            confidence_avg=confidence_avg,
        )

    # 3. Raw lines (fallback)
    line_rows = _extract_lines(pages, document_text)
    return ExtractResult(
        rows=line_rows,
        mode="line",
        pages_count=pages_count,
        tables_count=0,
        selected_table_shape=None,
        confidence_avg=confidence_avg,
    )
