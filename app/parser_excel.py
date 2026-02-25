import io
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

try:
    from rapidfuzz import fuzz as _fuzz
except ImportError:  # pragma: no cover – graceful degradation
    _fuzz = None


class ParseError(Exception):
    """Raised when the Excel file structure cannot be parsed."""


# ── Result data types ────────────────────────────────────────


@dataclass
class DetectedColumns:
    """Which columns were detected and how."""
    name_idx: Optional[int] = None
    qty_idx: Optional[int] = None
    code_idx: Optional[int] = None
    score: int = 0
    header_row: Optional[int] = None
    method: str = "auto"  # "auto", "fuzzy", "heuristic", "manual"
    fuzzy_scores: dict = field(default_factory=dict)


@dataclass
class ParseResult:
    """Result of parsing an Excel file."""
    df: Optional[pd.DataFrame] = None
    raw_values: Optional[list] = None
    raw_headers: Optional[list] = None
    detected: DetectedColumns = field(default_factory=DetectedColumns)
    needs_manual_selection: bool = False


# ── Synonym lists (lowercase) ────────────────────────────────

_CODE_SYNONYMS = ["код", "артикул", "№", "номер"]
_NAME_SYNONYMS = ["номенклатура", "наименование", "товар", "позиция"]
# Note: "зaказ" with latin 'a' included intentionally for OCR/copy-paste issues
_QTY_SYNONYMS = [
    "заказ", "зaказ", "кол", "кол-во", "количество",
    "кол во", "колво", "потребность", "к заказу",
]

_SYNONYMS_MAP = {
    "code": _CODE_SYNONYMS,
    "name": _NAME_SYNONYMS,
    "qty": _QTY_SYNONYMS,
}

# Sub-header tokens that indicate a multiline header
_SUBHEADER_TOKENS = ["шт", "ед.изм", "ед. изм", "единица", "штук"]


# ── Low-level helpers ─────────────────────────────────────────

def _cell_value(ws, row: int, col: int):
    """Get cell value, resolving merged cells to the top-left value."""
    cell = ws.cell(row=row, column=col)
    if cell.value is not None:
        return cell.value
    for mr in ws.merged_cells.ranges:
        if cell.coordinate in mr:
            return ws.cell(row=mr.min_row, column=mr.min_col).value
    return None


def _read_sheet_values(ws, max_rows: int | None = None) -> list[list]:
    """Read worksheet into a 2D list, resolving merged cells."""
    last_row = ws.max_row or 0
    if max_rows is not None:
        last_row = min(last_row, max_rows)
    max_col = ws.max_column or 0
    rows = []
    for r in range(1, last_row + 1):
        row = [_cell_value(ws, r, c) for c in range(1, max_col + 1)]
        rows.append(row)
    return rows


def _norm(val) -> str:
    """Normalize a cell value to lowercase stripped string with NFKC."""
    if val is None:
        return ""
    s = str(val).strip().lower()
    return unicodedata.normalize("NFKC", s)


def _matches(header: str, synonyms: list[str]) -> bool:
    """Check if a normalized header contains any synonym."""
    for syn in synonyms:
        if syn in header:
            return True
    return False


# ── Fuzzy matching ────────────────────────────────────────────

def _fuzzy_match(header: str, synonyms: list[str], threshold: int = 70) -> tuple[bool, int]:
    """Check fuzzy similarity of header against synonyms.

    Returns (matched, best_score). Falls back to substring if rapidfuzz unavailable.
    """
    if not header:
        return False, 0

    # Fast path: exact substring match
    for syn in synonyms:
        if syn in header:
            return True, 100

    if _fuzz is None:
        return False, 0

    best = 0
    for syn in synonyms:
        score = _fuzz.partial_ratio(header, syn)
        if score > best:
            best = score
    return best >= threshold, int(best)


def _fuzzy_find_col(headers: list[str], synonyms: list[str], threshold: int = 70) -> tuple[int | None, int]:
    """Find the column with best fuzzy match above threshold.

    Returns (col_index, best_score).
    """
    best_idx = None
    best_score = 0
    for i, h in enumerate(headers):
        if not h:
            continue
        matched, score = _fuzzy_match(h, synonyms, threshold)
        if matched and score > best_score:
            best_score = score
            best_idx = i
    return best_idx, best_score


# ── Header / column detection ─────────────────────────────────

def _table_score(row_values: list) -> int:
    """Calculate table score for a row: +1 per matched column category."""
    headers = [_norm(v) for v in row_values]
    score = 0
    for _category, synonyms in _SYNONYMS_MAP.items():
        for h in headers:
            if h:
                matched, _ = _fuzzy_match(h, synonyms, threshold=70)
                if matched:
                    score += 1
                    break
    return score


def _find_header_row_scored(values_2d: list[list], max_scan: int = 80) -> tuple[int | None, int]:
    """Find the best header row by table score.

    Returns (row_index, score). Only considers rows with >=2 non-empty cells.
    """
    best_idx = None
    best_score = 0
    for i, row in enumerate(values_2d[:max_scan]):
        non_empty = sum(1 for v in row if v is not None and str(v).strip())
        if non_empty < 2:
            continue
        score = _table_score(row)
        if score > best_score:
            best_score = score
            best_idx = i
    return best_idx, best_score


def _check_multiline_header(values_2d: list[list], header_idx: int) -> tuple[list[str], bool]:
    """Check for multiline header and concatenate if needed.

    Returns (final_headers, consumed_next_row).
    """
    headers = [_norm(v) for v in values_2d[header_idx]]
    next_idx = header_idx + 1

    if next_idx >= len(values_2d):
        return headers, False

    next_row = [_norm(v) for v in values_2d[next_idx]]

    # Check if the next row contains sub-header tokens
    has_subheader = False
    for cell in next_row:
        if cell:
            for token in _SUBHEADER_TOKENS:
                if token in cell:
                    has_subheader = True
                    break
            # Also check if it looks like a continuation (has qty-like words not in main header)
            if not has_subheader:
                for syn in _QTY_SYNONYMS:
                    if syn in cell and not any(syn in h for h in headers if h):
                        has_subheader = True
                        break
        if has_subheader:
            break

    if not has_subheader:
        return headers, False

    # Concatenate: header[col] + " " + next_row[col]
    merged = []
    for i in range(len(headers)):
        parts = []
        if headers[i]:
            parts.append(headers[i])
        if i < len(next_row) and next_row[i]:
            parts.append(next_row[i])
        merged.append(" ".join(parts))
    return merged, True


def _find_col(headers: list[str], synonyms: list[str]) -> int | None:
    """Find column by substring match (legacy fast path)."""
    for i, h in enumerate(headers):
        if h and _matches(h, synonyms):
            return i
    return None


# ── Content heuristics ────────────────────────────────────────

def _find_qty_heuristic(
    values_2d: list[list], data_start: int, num_cols: int, exclude_cols: set[int] | None = None
) -> int | None:
    """Fallback: find a column where >=60% of values are numeric and mean > 0."""
    exclude = exclude_cols or set()
    total = len(values_2d) - data_start
    if total <= 0:
        return None
    best_idx, best_count = None, 0
    for col_idx in range(num_cols):
        if col_idx in exclude:
            continue
        count = 0
        value_sum = 0.0
        for row_idx in range(data_start, len(values_2d)):
            val = values_2d[row_idx][col_idx] if col_idx < len(values_2d[row_idx]) else None
            if val is not None:
                try:
                    num = float(val)
                    count += 1
                    value_sum += num
                except (ValueError, TypeError):
                    pass
        if count > best_count and count >= total * 0.6:
            mean = value_sum / count if count > 0 else 0
            if mean > 0:
                best_count = count
                best_idx = col_idx
    return best_idx


def _find_name_heuristic(
    values_2d: list[list], data_start: int, num_cols: int, exclude_cols: set[int] | None = None
) -> int | None:
    """Fallback: find column with longest average string length that is mostly text."""
    exclude = exclude_cols or set()
    total = len(values_2d) - data_start
    if total <= 0:
        return None
    best_idx = None
    best_avg_len = 0.0
    for col_idx in range(num_cols):
        if col_idx in exclude:
            continue
        text_count = 0
        total_len = 0
        for row_idx in range(data_start, len(values_2d)):
            val = values_2d[row_idx][col_idx] if col_idx < len(values_2d[row_idx]) else None
            if val is not None:
                s = str(val).strip()
                if s:
                    # Check if it's not purely numeric
                    try:
                        float(s)
                    except (ValueError, TypeError):
                        text_count += 1
                        total_len += len(s)
        if text_count > total * 0.5 and text_count > 0:
            avg_len = total_len / text_count
            if avg_len > best_avg_len:
                best_avg_len = avg_len
                best_idx = col_idx
    return best_idx


# ── DataFrame builder ─────────────────────────────────────────

def build_dataframe_from_columns(
    values_2d: list[list],
    header_idx: int,
    name_idx: int,
    qty_idx: int | None,
    code_idx: int | None,
) -> pd.DataFrame:
    """Build canonical DataFrame from raw 2D values and explicit column indices.

    Skips rows where name is empty. Returns DataFrame with columns: code, name, qty, uom.
    Raises ParseError if no data rows found.
    """
    data_start = header_idx + 1
    result_rows = []

    for row_idx in range(data_start, len(values_2d)):
        row = values_2d[row_idx]

        def _get(idx):
            if idx is not None and idx < len(row):
                return row[idx]
            return None

        name_val = _get(name_idx)
        if name_val is None or str(name_val).strip() == "":
            continue

        code_val = _get(code_idx)
        qty_val = _get(qty_idx)

        code_str = str(code_val).strip() if code_val is not None else ""
        name_str = str(name_val).strip()

        qty_num = None
        if qty_val is not None:
            try:
                qty_num = int(float(qty_val))
            except (ValueError, TypeError):
                pass

        result_rows.append({
            "code": code_str,
            "name": name_str,
            "qty": qty_num,
            "uom": "шт",
        })

    if not result_rows:
        raise ParseError("Табличная часть найдена, но данные отсутствуют.")

    return pd.DataFrame(result_rows)


# ── Main parser (new entry point) ─────────────────────────────

_PARSE_ERR = (
    "Не найдена табличная часть "
    "(ожидаются колонки: Код, Номенклатура, Кол-во/Заказ)."
)


def parse_excel(file_path: str | Path) -> ParseResult:
    """Parse .xlsx with smart auto-detect. Returns ParseResult (never raises on detection failure)."""
    wb = load_workbook(str(file_path), data_only=True)
    ws = wb.active
    values_2d = _read_sheet_values(ws)
    wb.close()

    if not values_2d:
        raise ParseError("Файл пуст.")

    num_cols = max(len(row) for row in values_2d) if values_2d else 0

    # Step 1: find best header row by score
    header_idx, score = _find_header_row_scored(values_2d)

    detected = DetectedColumns(score=score, header_row=header_idx)

    if header_idx is None or score < 2:
        # Cannot find a confident header row → fallback
        # Try to salvage partial info
        raw_headers = [str(v) if v is not None else "" for v in values_2d[0]] if values_2d else []
        return ParseResult(
            raw_values=values_2d,
            raw_headers=raw_headers,
            detected=detected,
            needs_manual_selection=True,
        )

    # Step 2: check for multiline header
    headers, consumed_next = _check_multiline_header(values_2d, header_idx)
    data_start = header_idx + (2 if consumed_next else 1)
    detected.header_row = header_idx

    # Step 3: fuzzy-match columns
    name_idx, name_score = _fuzzy_find_col(headers, _NAME_SYNONYMS, threshold=70)
    qty_idx, qty_score = _fuzzy_find_col(headers, _QTY_SYNONYMS, threshold=70)
    code_idx, code_score = _fuzzy_find_col(headers, _CODE_SYNONYMS, threshold=70)

    detected.fuzzy_scores = {"name": name_score, "qty": qty_score, "code": code_score}
    detected.method = "fuzzy"

    # Step 4: content heuristic fallbacks
    assigned = {i for i in (name_idx, qty_idx, code_idx) if i is not None}

    if qty_idx is None:
        qty_idx = _find_qty_heuristic(values_2d, data_start, num_cols, exclude_cols=assigned)
        if qty_idx is not None:
            assigned.add(qty_idx)
            detected.method = "heuristic"

    if name_idx is None:
        name_idx = _find_name_heuristic(values_2d, data_start, num_cols, exclude_cols=assigned)
        if name_idx is not None:
            assigned.add(name_idx)
            detected.method = "heuristic"

    detected.name_idx = name_idx
    detected.qty_idx = qty_idx
    detected.code_idx = code_idx

    # Step 5: decide if we have enough
    if name_idx is None:
        raw_headers = [str(v) if v is not None else "" for v in values_2d[header_idx]]
        return ParseResult(
            raw_values=values_2d,
            raw_headers=raw_headers,
            detected=detected,
            needs_manual_selection=True,
        )

    # NAME is found — build DataFrame (QTY may be None, that's ok)
    try:
        df = build_dataframe_from_columns(values_2d, header_idx if not consumed_next else header_idx + 1, name_idx, qty_idx, code_idx)
    except ParseError:
        raw_headers = [str(v) if v is not None else "" for v in values_2d[header_idx]]
        return ParseResult(
            raw_values=values_2d,
            raw_headers=raw_headers,
            detected=detected,
            needs_manual_selection=True,
        )

    return ParseResult(
        df=df,
        detected=detected,
        needs_manual_selection=False,
    )


def load_excel(file_path: str | Path) -> pd.DataFrame:
    """Backward-compatible wrapper. Raises ParseError on failure."""
    result = parse_excel(file_path)
    if result.needs_manual_selection or result.df is None:
        raise ParseError(_PARSE_ERR)
    return result.df


# ── Display helpers (unchanged) ───────────────────────────────


def dataframe_preview(df: pd.DataFrame, limit: int = 200) -> pd.DataFrame:
    """Return the first `limit` rows of the DataFrame."""
    return df.head(limit)


def dataframe_to_html(df: pd.DataFrame) -> str:
    """Convert DataFrame to an HTML table string."""
    return df.to_html(
        index=False,
        classes="table",
        border=0,
        na_rep="",
    )


def dataframe_to_xlsx_bytes(df: pd.DataFrame) -> bytes:
    """Export DataFrame to .xlsx bytes with bold header and reasonable column widths."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Результат")
        ws = writer.sheets["Результат"]

        bold = Font(bold=True)
        for cell in ws[1]:
            cell.font = bold

        for col_idx, col_name in enumerate(df.columns, start=1):
            width = min(max(len(str(col_name)) + 2, 12), 40)
            ws.column_dimensions[get_column_letter(col_idx)].width = width

    return buf.getvalue()
