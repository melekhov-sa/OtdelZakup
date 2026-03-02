"""Extract structured rows from a Google Document AI response dict.

Priority:
  1. Tables   — pick the best table by score (rows × fill_ratio + number_bonus)
  2. Paragraphs — joined paragraph text, one per row
  3. Raw lines  — fallback when no paragraphs exist

No network calls; pure transformation of the already-fetched document dict.

Header detection (table mode):
  _extract_best_table returns ALL table rows (DocAI headerRows + bodyRows) and
  a flag indicating whether DocAI had an explicit header.  extract_rows then
  calls detect_header_row() to decide, via heuristic, whether the first row is
  truly a header or a product data row, and splits accordingly.
  This prevents silent row loss when DocAI misidentifies the first data row as
  a column header.
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
    selected_table_shape: tuple[int, int] | None   # (data_rows, ncols)
    confidence_avg: float | None
    header_row: list[str] = field(default_factory=list)  # Column labels (empty if no header)
    all_rows_raw: list[list[str]] = field(default_factory=list)  # ALL rows before header split
    header_decision: object | None = None            # HeaderDecision or None


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
) -> tuple[list[list[str]], bool, int, tuple[int, int]] | None:
    """Find the highest-scoring table across all pages.

    Returns ``(all_rows_raw, docai_had_header, total_tables_found, shape)``
    or ``None`` if no table was found.

    ``all_rows_raw``       — ALL rows in order: DocAI headerRows first, then
                             bodyRows.  No rows are discarded here.
    ``docai_had_header``   — True if DocAI explicitly marked at least one
                             headerRow for the winning table.
    ``shape``              — (total_rows_count, n_cols) of all_rows_raw.

    The caller (extract_rows) is responsible for running detect_header_row()
    on all_rows_raw and splitting into header labels + data rows.
    """
    best_score: float = -1.0
    best_all_rows: list[list[str]] = []
    best_docai_header: bool = False
    best_shape: tuple[int, int] = (0, 0)
    total_tables = 0

    for page in pages:
        for table in (page.get("tables") or []):
            total_tables += 1
            score = _table_score(table, document_text)
            if score > best_score:
                best_score = score

                header_rows_raw = table.get("headerRows") or []
                body_rows_raw   = table.get("bodyRows") or []

                # Collect ALL rows: DocAI headerRows first, then bodyRows
                all_rows: list[list[str]] = []
                for row in header_rows_raw + body_rows_raw:
                    cells_text = [
                        _cell_text(document_text, cell)
                        for cell in (row.get("cells") or [])
                    ]
                    all_rows.append(cells_text)

                best_all_rows = all_rows
                best_docai_header = bool(header_rows_raw)
                n_cols = max((len(r) for r in all_rows), default=0)
                best_shape = (len(all_rows), n_cols)

    if not best_all_rows:
        return None
    return best_all_rows, best_docai_header, total_tables, best_shape


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

    For table mode, runs detect_header_row() heuristic to decide whether
    the first row is a true column header or a product data row.
    """
    from app.parsing.header_detector import detect_header_row  # noqa: PLC0415

    document_text: str = document.get("text") or ""
    pages: list        = document.get("pages") or []
    pages_count        = len(pages)
    confidence_avg     = _avg_confidence(pages)

    # 1. Tables
    table_result = _extract_best_table(pages, document_text)
    if table_result is not None:
        all_rows_raw, docai_had_header, tables_count, _raw_shape = table_result

        # Smart header detection: never silently drop rows
        decision = detect_header_row(all_rows_raw, docai_had_header)

        if decision.has_header and len(all_rows_raw) > 1:
            header_row = all_rows_raw[0]
            data_rows  = all_rows_raw[1:]
        else:
            header_row = []
            data_rows  = all_rows_raw

        n_cols = max((len(r) for r in data_rows), default=0)
        shape  = (len(data_rows), n_cols)

        return ExtractResult(
            rows=data_rows,
            mode="table",
            pages_count=pages_count,
            tables_count=tables_count,
            selected_table_shape=shape,
            confidence_avg=confidence_avg,
            header_row=header_row,
            all_rows_raw=all_rows_raw,
            header_decision=decision,
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
