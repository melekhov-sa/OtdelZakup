r"""Extract structured rows from a Google Document AI response dict.

Priority:
  1. Product table — detect by column keywords (товар/наименование + кол-во/цена/сумма)
  2. Best table   — highest-scoring table (rows x fill_ratio + number_bonus)
  3. Paragraphs   — joined paragraph text, one per row
  4. Raw lines    — fallback: lines matching product patterns (M\d+, DIN, ГОСТ, etc.)

Product table detection (step 1):
  A table is "product" if its header contains at least one name keyword
  (товар, наименование, товары, product, item, услуги, работы) AND at least
  one numeric keyword (кол-во, количество, qty, ед, шт, кг, цена, сумма).

Field mapping is dynamic — column roles (name, qty, unit, price_unit,
price_total) are assigned by matching header text against keyword sets.

Rows containing "итого", "ндс", "всего", "к оплате", "сумма по счету"
are filtered out as summary rows.

No network calls; pure transformation of the already-fetched document dict.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


# ── Data model ─────────────────────────────────────────────────────────────────

@dataclass
class ExtractResult:
    rows: list[list[str]]
    mode: str                              # "product_table" | "table" | "paragraph" | "line"
    pages_count: int
    tables_count: int
    selected_table_shape: tuple[int, int] | None   # (data_rows, ncols)
    confidence_avg: float | None
    header_row: list[str] = field(default_factory=list)
    all_rows_raw: list[list[str]] = field(default_factory=list)
    header_decision: object | None = None
    # Structured fields extracted from product tables
    structured_rows: list[dict] = field(default_factory=list)
    # Debug info
    debug: dict = field(default_factory=dict)


# ── Product table keywords ────────────────────────────────────────────────────

_NAME_KEYWORDS = {"товар", "наименование", "товары", "product", "item", "услуги", "работы"}

_NUMERIC_KEYWORDS = {"кол-во", "количество", "qty", "ед", "шт", "кг", "цена", "сумма"}

_SUMMARY_RE = re.compile(
    r"\b(?:итого|ндс|всего|к оплате|сумма по счету|в т\.?ч\.?\s*ндс|без ндс)\b",
    re.IGNORECASE,
)

# Column role detection keywords
_COL_NAME_KW = {"товар", "наименование", "товары", "product", "item", "услуги", "работы", "номенклатура"}
_COL_QTY_KW = {"кол-во", "количество", "qty", "кол-во шт", "кол."}
_COL_UNIT_KW = {"ед", "ед.", "ед.изм", "unit", "единица"}
_COL_PRICE_UNIT_KW = {"цена", "цена за шт", "price"}
_COL_PRICE_TOTAL_KW = {"сумма", "стоимость", "total", "amount"}

# Fallback: product line patterns (paragraph/line mode)
_PRODUCT_LINE_RE = re.compile(
    r"(?:"
    r"[МмMm]\d+"            # M12, М16
    r"|DIN\s*\d+"           # DIN 933
    r"|ГОСТ\s*[\dР]"       # ГОСТ 7798
    r"|(?:болт|гайка|шайба|винт|саморез|шпилька|анкер|дюбель|заклепка|гвоздь|электрод|сверло|диск|круг|хомут|фланец|муфта|труба)\b"
    r")",
    re.IGNORECASE,
)
_QTY_UNIT_NEARBY_RE = re.compile(
    r"\d+(?:[.,]\d+)?\s*(?:шт|кг|м|уп|компл|pcs|л)\b",
    re.IGNORECASE,
)


# ── Internal helpers ───────────────────────────────────────────────────────────

def _text_for_segment(document_text: str, segment: dict) -> str:
    start = int(segment.get("startIndex", 0))
    end   = int(segment.get("endIndex", 0))
    return document_text[start:end]


def _layout_text(document_text: str, layout: dict) -> str:
    anchor = layout.get("textAnchor") or {}
    segments = anchor.get("textSegments") or []
    parts = [_text_for_segment(document_text, s) for s in segments]
    return "".join(parts).strip()


def _cell_text(document_text: str, cell: dict) -> str:
    return _layout_text(document_text, cell.get("layout") or {})


def _table_rows(table: dict, document_text: str) -> list[list[str]]:
    """Extract all rows (header + body) from a table as list of cell strings."""
    header_rows = table.get("headerRows") or []
    body_rows   = table.get("bodyRows") or []
    result: list[list[str]] = []
    for row in header_rows + body_rows:
        cells = [_cell_text(document_text, c) for c in (row.get("cells") or [])]
        result.append(cells)
    return result


def _table_score(table: dict, document_text: str) -> float:
    """Heuristic score for a table: rows x fill_ratio + 0.2 if contains digits."""
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


def _header_contains(header_cells: list[str], keywords: set[str]) -> bool:
    """Check if any header cell contains any of the keywords (case-insensitive)."""
    for cell in header_cells:
        cell_lower = cell.lower().strip()
        for kw in keywords:
            if kw in cell_lower:
                return True
    return False


def _is_product_table_header(header_cells: list[str]) -> bool:
    """Return True if header matches product-table signature."""
    has_name = _header_contains(header_cells, _NAME_KEYWORDS)
    has_numeric = _header_contains(header_cells, _NUMERIC_KEYWORDS)
    return has_name and has_numeric


def _detect_column_role(header_text: str) -> str | None:
    """Determine column role from header text. Returns role string or None."""
    h = header_text.lower().strip()
    # Order matters: check price_total before price_unit (both may contain "цена")
    for kw in _COL_PRICE_TOTAL_KW:
        if kw in h:
            # "цена за шт" should NOT match price_total — exclude if "цена" alone
            if kw == "сумма" or kw == "стоимость" or kw == "total" or kw == "amount":
                return "price_total"
    for kw in _COL_QTY_KW:
        if kw in h:
            return "qty"
    for kw in _COL_UNIT_KW:
        if kw in h:
            return "unit"
    for kw in _COL_PRICE_UNIT_KW:
        if kw in h:
            return "price_unit"
    for kw in _COL_NAME_KW:
        if kw in h:
            return "name"
    return None


def _map_columns(header_cells: list[str]) -> dict[str, int]:
    """Map column roles to indices. Returns {role: col_index}."""
    mapping: dict[str, int] = {}
    for i, cell in enumerate(header_cells):
        role = _detect_column_role(cell)
        if role and role not in mapping:
            mapping[role] = i
    return mapping


def _is_summary_row(cells: list[str]) -> bool:
    """Return True if any cell in the row matches summary patterns."""
    joined = " ".join(cells)
    return bool(_SUMMARY_RE.search(joined))


def _parse_float(s: str) -> float | None:
    """Try to parse a float from a cell string."""
    if not s or not s.strip():
        return None
    cleaned = s.strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def _extract_structured_rows(
    all_rows: list[list[str]],
    header_row: list[str],
) -> tuple[list[dict], dict]:
    """Extract structured product rows from table data.

    Returns (structured_rows, debug_info).
    """
    col_map = _map_columns(header_row)

    debug = {
        "detected_table_type": "product",
        "columns_detected": {role: header_row[idx] if idx < len(header_row) else "?"
                             for role, idx in col_map.items()},
        "rows_extracted": 0,
        "rows_filtered": 0,
    }

    structured: list[dict] = []
    for row_cells in all_rows:
        if _is_summary_row(row_cells):
            debug["rows_filtered"] += 1
            continue

        name = ""
        if "name" in col_map:
            idx = col_map["name"]
            name = row_cells[idx].strip() if idx < len(row_cells) else ""

        if not name:
            # Skip rows with no name
            debug["rows_filtered"] += 1
            continue

        qty = None
        if "qty" in col_map:
            idx = col_map["qty"]
            raw = row_cells[idx] if idx < len(row_cells) else ""
            qty = _parse_float(raw)

        unit = None
        if "unit" in col_map:
            idx = col_map["unit"]
            unit = row_cells[idx].strip() if idx < len(row_cells) else None

        price_unit = None
        if "price_unit" in col_map:
            idx = col_map["price_unit"]
            raw = row_cells[idx] if idx < len(row_cells) else ""
            price_unit = _parse_float(raw)

        price_total = None
        if "price_total" in col_map:
            idx = col_map["price_total"]
            raw = row_cells[idx] if idx < len(row_cells) else ""
            price_total = _parse_float(raw)

        debug["rows_extracted"] += 1
        structured.append({
            "name": name,
            "qty": qty,
            "unit": unit or None,
            "price_unit": price_unit,
            "price_total": price_total,
        })

    return structured, debug


# ── Product table finder ──────────────────────────────────────────────────────

def _find_product_table(
    pages: list, document_text: str,
) -> tuple[list[list[str]], list[str], int] | None:
    """Find the product table across all pages.

    Returns (data_rows, header_row, total_tables) or None.
    Checks ALL tables — picks the one whose header matches product-table signature.
    If multiple match, picks the one with most data rows.
    """
    best_header: list[str] = []
    best_data: list[list[str]] = []
    total_tables = 0

    for page in pages:
        for table in (page.get("tables") or []):
            total_tables += 1
            rows = _table_rows(table, document_text)
            if not rows:
                continue

            # First row is potential header
            header_candidate = rows[0]
            if _is_product_table_header(header_candidate):
                data = rows[1:]
                if len(data) > len(best_data):
                    best_header = header_candidate
                    best_data = data

    if not best_header:
        return None
    return best_data, best_header, total_tables


# ── Legacy best-table (fallback when no product table found) ──────────────────

def _extract_best_table(
    pages: list, document_text: str,
) -> tuple[list[list[str]], bool, int, tuple[int, int]] | None:
    """Find the highest-scoring table across all pages (legacy fallback).

    Returns (all_rows_raw, docai_had_header, total_tables_found, shape) or None.
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


def _filter_product_lines(rows: list[list[str]]) -> list[list[str]]:
    """From paragraph/line rows, keep only those matching product patterns."""
    result: list[list[str]] = []
    for row in rows:
        text = " ".join(row)
        if _PRODUCT_LINE_RE.search(text) and _QTY_UNIT_NEARBY_RE.search(text):
            result.append(row)
    return result


def _avg_confidence(pages: list) -> float | None:
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

    Falls back through: product_table -> table -> paragraph -> line.

    For product tables, returns structured_rows with dynamic column mapping.
    For other modes, structured_rows is empty (caller joins cells into name).
    """
    from app.parsing.header_detector import detect_header_row  # noqa: PLC0415

    document_text: str = document.get("text") or ""
    pages: list        = document.get("pages") or []
    pages_count        = len(pages)
    confidence_avg     = _avg_confidence(pages)

    # ── 1. Product table (keyword-based detection) ────────────────────────────
    product_result = _find_product_table(pages, document_text)
    if product_result is not None:
        data_rows, header_row, tables_count = product_result
        structured, debug = _extract_structured_rows(data_rows, header_row)

        if structured:  # at least one valid product row
            n_cols = max((len(r) for r in data_rows), default=0)
            shape = (len(data_rows), n_cols)

            logger.info(
                "Product table detected: %d cols mapped, %d rows extracted, %d filtered",
                len(debug.get("columns_detected", {})),
                debug.get("rows_extracted", 0),
                debug.get("rows_filtered", 0),
            )

            return ExtractResult(
                rows=data_rows,
                mode="product_table",
                pages_count=pages_count,
                tables_count=tables_count,
                selected_table_shape=shape,
                confidence_avg=confidence_avg,
                header_row=header_row,
                all_rows_raw=[header_row] + data_rows,
                structured_rows=structured,
                debug=debug,
            )

    # ── 2. Generic best table (legacy fallback) ──────────────────────────────
    table_result = _extract_best_table(pages, document_text)
    if table_result is not None:
        all_rows_raw, docai_had_header, tables_count, _raw_shape = table_result

        decision = detect_header_row(all_rows_raw, docai_had_header)

        if decision.has_header and len(all_rows_raw) > 1:
            header_row = all_rows_raw[0]
            data_rows  = all_rows_raw[1:]
        else:
            header_row = []
            data_rows  = all_rows_raw

        n_cols = max((len(r) for r in data_rows), default=0)
        shape  = (len(data_rows), n_cols)

        debug = {"detected_table_type": "generic", "rows_extracted": len(data_rows), "rows_filtered": 0}

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
            debug=debug,
        )

    # ── 3. Paragraphs ────────────────────────────────────────────────────────
    para_rows = _extract_paragraphs(pages, document_text)
    if para_rows:
        # Try filtering for product lines
        product_lines = _filter_product_lines(para_rows)
        if product_lines:
            debug = {"detected_table_type": "paragraph_filtered", "rows_extracted": len(product_lines), "rows_filtered": len(para_rows) - len(product_lines)}
            return ExtractResult(
                rows=product_lines,
                mode="paragraph",
                pages_count=pages_count,
                tables_count=0,
                selected_table_shape=None,
                confidence_avg=confidence_avg,
                debug=debug,
            )
        debug = {"detected_table_type": "paragraph", "rows_extracted": len(para_rows), "rows_filtered": 0}
        return ExtractResult(
            rows=para_rows,
            mode="paragraph",
            pages_count=pages_count,
            tables_count=0,
            selected_table_shape=None,
            confidence_avg=confidence_avg,
            debug=debug,
        )

    # ── 4. Raw lines (fallback) ──────────────────────────────────────────────
    line_rows = _extract_lines(pages, document_text)
    product_lines = _filter_product_lines(line_rows)
    final_rows = product_lines if product_lines else line_rows
    debug = {
        "detected_table_type": "line_filtered" if product_lines else "line",
        "rows_extracted": len(final_rows),
        "rows_filtered": len(line_rows) - len(final_rows) if product_lines else 0,
    }
    return ExtractResult(
        rows=final_rows,
        mode="line",
        pages_count=pages_count,
        tables_count=0,
        selected_table_shape=None,
        confidence_avg=confidence_avg,
        debug=debug,
    )
