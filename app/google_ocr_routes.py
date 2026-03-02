"""Routes for Google Document AI import (/upload-google-ocr and /google-ocr-wizard/{fid})."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.cache import CACHE_DIR, UPLOAD_DIR, save_cache
from app.database import get_db_session
from app.extractors import DEFAULT_FIELD_KEYS, EXTRACTORS
from app.models import ImportAttachment, ImportParseAttempt
from app.name_builder import load_active_template

google_ocr_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

_ALLOWED_EXT = {".pdf", ".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".webp"}
_EXT_TO_MIME = {
    ".pdf":  "application/pdf",
    ".png":  "image/png",
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".tif":  "image/tiff",
    ".tiff": "image/tiff",
    ".bmp":  "image/bmp",
    ".webp": "image/webp",
}
# Labels for DocAI structural modes (returned by extract_rows)
_DOCAI_MODE_LABELS = {
    "table":     "Таблица (Document AI)",
    "paragraph": "Параграфы (Document AI)",
    "line":      "Строки (Document AI)",
}
# Labels for user-selected processing modes
_USER_MODE_LABELS = {
    "table":           "Таблица",
    "structured_list": "Структурированный список",
    "free_text":       "Свободный текст",
}

# Preview length for raw text in list/free-text wizard
_RAW_TEXT_PREVIEW_CHARS = 3000


def _file_id(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]


# ── DataFrame builders ────────────────────────────────────────────────────────

def _table_to_multicolumn_df(rows: list[list[str]]) -> pd.DataFrame:
    """Convert table rows (2D list) to a multi-column DataFrame col_0..col_N-1.

    Empty rows are skipped.  The column count equals the width of the widest row.
    """
    if not rows:
        return pd.DataFrame(columns=["col_0"])

    n_cols = max(len(r) for r in rows)
    if n_cols == 0:
        return pd.DataFrame(columns=["col_0"])

    col_names = [f"col_{i}" for i in range(n_cols)]
    padded = [r + [""] * (n_cols - len(r)) for r in rows]
    # Skip completely empty rows
    padded = [r for r in padded if any(c.strip() for c in r)]
    return pd.DataFrame(padded, columns=col_names)


def _text_rows_to_df(rows: list[list[str]]) -> pd.DataFrame:
    """Convert paragraph/line rows to a single-column 'name' DataFrame."""
    names = [row[0].strip() for row in rows if row and row[0].strip()]
    return pd.DataFrame({"name": names}) if names else pd.DataFrame(columns=["name"])


def _parsed_rows_to_df(row_dicts: list[dict]) -> pd.DataFrame:
    """Convert list-of-dicts from parsed_rows_to_df_data to a DataFrame."""
    if not row_dicts:
        return pd.DataFrame(columns=["name", "qty", "uom", "qty_uom_source"])
    df = pd.DataFrame(row_dicts)
    # Keep only the columns that the transform pipeline understands
    keep = ["name", "qty", "uom", "qty_uom_source"]
    for col in keep:
        if col not in df.columns:
            df[col] = None
    return df[keep]


# ── DB helpers ────────────────────────────────────────────────────────────────

def _save_attachment(fid: str, filename: str, path: Path, session) -> ImportAttachment:
    att = ImportAttachment(
        file_id=fid,
        filename=filename,
        mime_type="",
        storage_path=str(path),
        kind="google_document_ai",
        created_at=datetime.now(timezone.utc),
    )
    session.add(att)
    session.flush()
    return att


def _save_attempt(
    fid: str,
    att_id: int,
    mode: str,
    rows_count: int,
    metrics: dict,
    session,
) -> ImportParseAttempt:
    attempt = ImportParseAttempt(
        file_id=fid,
        attachment_id=att_id,
        method="google_document_ai",
        status="ok",
        rows_found=rows_count,
        metrics_json="{}",
        error_text=None,
        created_at=datetime.now(timezone.utc),
    )
    attempt.metrics = metrics
    session.add(attempt)
    session.flush()
    return attempt


def _maybe_save_raw_response(fid: str, doc: dict) -> None:
    """Save raw Document AI JSON to disk if the system setting is enabled (default OFF)."""
    from app.models import SystemSetting  # noqa: PLC0415

    session = get_db_session()
    try:
        setting = session.query(SystemSetting).filter_by(key="google_ocr_save_raw").first()
        save_raw = setting is not None and setting.value == "true"
    finally:
        session.close()

    if save_raw:
        path = CACHE_DIR / fid
        path.mkdir(parents=True, exist_ok=True)
        (path / "raw_response.json").write_text(
            json.dumps(doc, ensure_ascii=False), encoding="utf-8"
        )


# ── Column detection helper ───────────────────────────────────────────────────

def _auto_detect(result) -> dict:
    """Run detect_columns on the ExtractResult; returns col_map dict."""
    from app.parsing.docai_table_parser import detect_columns  # noqa: PLC0415

    if result.mode != "table":
        return {}
    return detect_columns(result.header_row or [], result.rows)


# ── Routes ────────────────────────────────────────────────────────────────────

@google_ocr_router.get("/upload-google-ocr", response_class=HTMLResponse)
async def upload_google_ocr_form(request: Request):
    from app.integrations.google_document_ai import is_configured  # noqa: PLC0415

    return templates.TemplateResponse(
        "upload_google_ocr.html",
        {"request": request, "configured": is_configured()},
    )


@google_ocr_router.post("/upload-google-ocr", response_class=HTMLResponse)
async def upload_google_ocr(
    request: Request,
    file: UploadFile = File(...),
    ocr_mode: str = Form(default="table"),
):
    from app.integrations.google_document_ai import (  # noqa: PLC0415
        GoogleDocAIError,
        is_configured,
        process_document,
    )
    from app.services.google_ocr_extractor import extract_rows  # noqa: PLC0415

    if not is_configured():
        return templates.TemplateResponse(
            "upload_google_ocr.html",
            {
                "request": request,
                "configured": False,
                "error": (
                    "Google Document AI не настроен. "
                    "Задайте переменные окружения GOOGLE_PROJECT_ID, "
                    "GOOGLE_LOCATION и GOOGLE_PROCESSOR_ID."
                ),
            },
            status_code=400,
        )

    fname = file.filename or "document"
    ext = Path(fname).suffix.lower()

    if ext not in _ALLOWED_EXT:
        return templates.TemplateResponse(
            "upload_google_ocr.html",
            {
                "request": request,
                "configured": True,
                "error": (
                    f"Формат '{ext}' не поддерживается. "
                    "Допустимые: PDF, PNG, JPG, TIFF, BMP, WEBP."
                ),
            },
            status_code=400,
        )

    # Normalise mode value
    if ocr_mode not in ("table", "structured_list", "free_text"):
        ocr_mode = "table"

    file_bytes = await file.read()
    fid = _file_id(file_bytes)
    mime_type = _EXT_TO_MIME.get(ext, "application/octet-stream")

    # Save raw file to uploads dir
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = UPLOAD_DIR / fname
    dest.write_bytes(file_bytes)

    # Call Google Document AI
    try:
        doc = process_document(file_bytes, mime_type)
    except GoogleDocAIError as exc:
        return templates.TemplateResponse(
            "upload_google_ocr.html",
            {
                "request": request,
                "configured": True,
                "error": str(exc),
            },
            status_code=422,
        )

    # Optionally persist raw response JSON
    _maybe_save_raw_response(fid, doc)

    # ── Table mode — existing pipeline ─────────────────────────────────────────
    if ocr_mode == "table":
        result = extract_rows(doc)

        metrics = {
            "mode":                 result.mode,
            "pages_count":          result.pages_count,
            "tables_count":         result.tables_count,
            "confidence_avg":       result.confidence_avg,
            "selected_table_shape": (
                list(result.selected_table_shape)
                if result.selected_table_shape else None
            ),
        }
        session = get_db_session()
        try:
            att      = _save_attachment(fid, fname, dest, session)
            _attempt = _save_attempt(fid, att.id, result.mode, len(result.rows), metrics, session)
            session.commit()
        finally:
            session.close()

        if result.mode == "table" and result.rows:
            df = _table_to_multicolumn_df(result.rows)
            col_map = _auto_detect(result)
            headers = result.header_row or []
            save_cache(
                fid, fname, df,
                detected_columns=col_map,
                source_kind="docai_table",
                docai_headers=headers,
            )
        else:
            df = _text_rows_to_df(result.rows)
            save_cache(fid, fname, df, source_kind="docai_text")
            col_map = {}
            headers = []

        n_cols = len(df.columns)
        col_examples: list[str] = []
        if result.mode == "table" and not df.empty:
            first_row = df.iloc[0]
            for ci in range(n_cols):
                col_examples.append(str(first_row.get(f"col_{ci}", ""))[:60])

        preview_rows = result.rows[:20]
        return templates.TemplateResponse(
            "google_ocr_wizard.html",
            {
                "request":             request,
                "filename":            fname,
                "file_id":             fid,
                "mode":                result.mode,
                "mode_label":          _DOCAI_MODE_LABELS.get(result.mode, result.mode),
                "metrics":             metrics,
                "total_rows":          len(result.rows),
                "preview_rows":        preview_rows,
                "extractors":          EXTRACTORS,
                "field_keys":          DEFAULT_FIELD_KEYS,
                "has_active_template": load_active_template() is not None,
                "n_cols":              n_cols if result.mode == "table" else 0,
                "col_headers":         headers,
                "col_examples":        col_examples,
                "detected":            col_map,
            },
        )

    # ── Structured list / free text modes ─────────────────────────────────────
    from app.parsing.structured_list_parser import (  # noqa: PLC0415
        parse_free_text,
        parse_structured_list,
        parsed_rows_to_df_data,
    )

    raw_text: str = doc.get("text") or ""

    if ocr_mode == "structured_list":
        parsed_rows = parse_structured_list(raw_text)
    else:
        parsed_rows = parse_free_text(raw_text)

    total_lines = len([l for l in raw_text.splitlines() if l.strip()])
    skipped = total_lines - len(parsed_rows)

    row_dicts = parsed_rows_to_df_data(parsed_rows)
    df = _parsed_rows_to_df(row_dicts)

    source_kind = f"docai_{ocr_mode}"  # "docai_structured_list" or "docai_free_text"
    save_cache(fid, fname, df, source_kind=source_kind)

    metrics = {
        "mode":          ocr_mode,
        "pages_count":   len(doc.get("pages") or []),
        "tables_count":  0,
        "confidence_avg": None,
        "selected_table_shape": None,
        "total_lines":   total_lines,
        "parsed_rows":   len(parsed_rows),
        "skipped_rows":  max(0, skipped),
    }
    session = get_db_session()
    try:
        att      = _save_attachment(fid, fname, dest, session)
        _attempt = _save_attempt(fid, att.id, ocr_mode, len(parsed_rows), metrics, session)
        session.commit()
    finally:
        session.close()

    return templates.TemplateResponse(
        "google_ocr_list_wizard.html",
        {
            "request":             request,
            "filename":            fname,
            "file_id":             fid,
            "ocr_mode":            ocr_mode,
            "mode_label":          _USER_MODE_LABELS.get(ocr_mode, ocr_mode),
            "metrics":             metrics,
            "raw_text_preview":    raw_text[:_RAW_TEXT_PREVIEW_CHARS],
            "raw_text_truncated":  len(raw_text) > _RAW_TEXT_PREVIEW_CHARS,
            "parsed_rows":         parsed_rows[:50],    # preview max 50
            "total_parsed":        len(parsed_rows),
            "skipped_rows":        max(0, skipped),
            "extractors":          EXTRACTORS,
            "field_keys":          DEFAULT_FIELD_KEYS,
            "has_active_template": load_active_template() is not None,
        },
    )
