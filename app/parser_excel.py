import io
from pathlib import Path

import pandas as pd
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter


def load_excel(file_path: str | Path) -> pd.DataFrame:
    """Read an .xlsx file and return a DataFrame."""
    return pd.read_excel(file_path, engine="openpyxl")


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
