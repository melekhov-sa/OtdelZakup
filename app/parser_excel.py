import io
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter


class ParseError(Exception):
    """Raised when the Excel file structure cannot be parsed."""


# ── Synonym lists (lowercase) ────────────────────────────────

_CODE_SYNONYMS = ["код", "артикул"]
_NAME_SYNONYMS = ["номенклатура", "наименование", "товар"]
# Note: "зaказ" with latin 'a' included intentionally for OCR/copy-paste issues
_QTY_SYNONYMS = ["заказ", "зaказ", "кол", "кол-во", "количество", "кол во", "колво"]


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
    """Normalize a cell value to lowercase stripped string."""
    if val is None:
        return ""
    return str(val).strip().lower()


def _matches(header: str, synonyms: list[str]) -> bool:
    """Check if a normalized header contains any synonym."""
    for syn in synonyms:
        if syn in header:
            return True
    return False


# ── Header / column detection ─────────────────────────────────

def _find_header_row(values_2d: list[list], max_scan: int = 60) -> int | None:
    """Return 0-based index of the header row, or None."""
    for i, row in enumerate(values_2d[:max_scan]):
        headers = [_norm(v) for v in row]
        has_code = any(_matches(h, _CODE_SYNONYMS) for h in headers if h)
        has_name = any(_matches(h, _NAME_SYNONYMS) for h in headers if h)
        if has_code and has_name:
            return i
    return None


def _find_col(headers: list[str], synonyms: list[str]) -> int | None:
    for i, h in enumerate(headers):
        if h and _matches(h, synonyms):
            return i
    return None


def _find_qty_heuristic(values_2d: list[list], headers: list[str], data_start: int) -> int | None:
    """Fallback: find a column where >50% of values are numeric."""
    total = len(values_2d) - data_start
    if total <= 0:
        return None
    best_idx, best_count = None, 0
    for col_idx in range(len(headers)):
        if not headers[col_idx]:
            continue
        count = 0
        for row_idx in range(data_start, len(values_2d)):
            val = values_2d[row_idx][col_idx] if col_idx < len(values_2d[row_idx]) else None
            if val is not None:
                try:
                    float(val)
                    count += 1
                except (ValueError, TypeError):
                    pass
        if count > best_count and count > total * 0.5:
            best_count = count
            best_idx = col_idx
    return best_idx


# ── Main parser ───────────────────────────────────────────────

_PARSE_ERR = (
    "Не найдена табличная часть "
    "(ожидаются колонки: Код, Номенклатура, Кол-во/Заказ)."
)


def load_excel(file_path: str | Path) -> pd.DataFrame:
    """Read .xlsx with smart header detection. Returns DataFrame with canonical columns."""
    wb = load_workbook(str(file_path), data_only=True)
    ws = wb.active
    values_2d = _read_sheet_values(ws)
    wb.close()

    if not values_2d:
        raise ParseError("Файл пуст.")

    header_idx = _find_header_row(values_2d)
    if header_idx is None:
        raise ParseError(_PARSE_ERR)

    headers = [_norm(v) for v in values_2d[header_idx]]

    code_idx = _find_col(headers, _CODE_SYNONYMS)
    name_idx = _find_col(headers, _NAME_SYNONYMS)
    qty_idx = _find_col(headers, _QTY_SYNONYMS)

    if code_idx is None or name_idx is None:
        raise ParseError(_PARSE_ERR)

    data_start = header_idx + 1
    if qty_idx is None:
        qty_idx = _find_qty_heuristic(values_2d, headers, data_start)

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
