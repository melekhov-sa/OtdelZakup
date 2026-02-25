"""Route for text-based input: paste text → parse → show view_raw for transform."""

import hashlib
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.cache import UPLOAD_DIR, save_cache
from app.extractors import DEFAULT_FIELD_KEYS, EXTRACTORS
from app.name_builder import load_active_template
from app.parser_excel import dataframe_preview, dataframe_to_html
from app.text_input.parser import parse_text_to_rows

text_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def _rows_to_dataframe(rows: list[dict]) -> pd.DataFrame:
    """Convert parsed text rows to the canonical DataFrame format.

    uom stays None when not explicitly found — no default is applied.
    """
    result = []
    for row in rows:
        result.append(
            {
                "code": "",
                "name": str(row.get("name", "")).strip(),
                "qty": row.get("qty"),       # None if not found
                "uom": row.get("uom"),       # None if not found
                "standard_raw": "",
                "strength_raw": "",
                "note_raw": str(row.get("note_raw", "")),
            }
        )
    return pd.DataFrame(result)


@text_router.post("/text-input", response_class=HTMLResponse)
async def text_input(
    request: Request,
    text: str = Form(...),
):
    """Parse pasted text and show the raw table for transformation."""
    if not text.strip():
        return templates.TemplateResponse(
            "upload.html",
            {"request": request, "error": "Введите текст для анализа."},
            status_code=400,
        )

    rows = parse_text_to_rows(text.strip())
    if not rows:
        return templates.TemplateResponse(
            "upload.html",
            {
                "request": request,
                "error": "Не удалось распознать ни одной позиции в тексте.",
            },
            status_code=400,
        )

    df = _rows_to_dataframe(rows)

    # Generate a stable file_id from the text content
    fid = "txt_" + hashlib.sha256(text.encode()).hexdigest()[:12]

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    save_cache(fid, "текстовый_ввод.txt", df, detected_columns={"source": "text"})

    total_rows = len(df)
    preview = dataframe_preview(df, limit=200)
    table_html = dataframe_to_html(preview)

    return templates.TemplateResponse(
        "view_raw.html",
        {
            "request": request,
            "filename": "Текстовый ввод",
            "file_id": fid,
            "total_rows": total_rows,
            "table_html": table_html,
            "extractors": EXTRACTORS,
            "field_keys": DEFAULT_FIELD_KEYS,
            "has_active_template": load_active_template() is not None,
        },
    )
