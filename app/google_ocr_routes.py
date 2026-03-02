"""Routes for Google Document AI import (/upload-google-ocr and /google-ocr-wizard/{fid})."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, File, Request, UploadFile
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
_MODE_LABELS = {
    "table":     "Таблица (Document AI)",
    "paragraph": "Параграфы (Document AI)",
    "line":      "Строки (Document AI)",
}


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
async def upload_google_ocr(request: Request, file: UploadFile = File(...)):
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

    # Extract rows from the document
    result = extract_rows(doc)

    # Optionally persist raw response JSON
    _maybe_save_raw_response(fid, doc)

    # Persist attachment + attempt metadata
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

    # ── Build DataFrame and save to cache ────────────────────────────────────
    if result.mode == "table" and result.rows:
        # Multi-column df: col_0 .. col_N-1
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
        # Paragraph / line mode → single "name" column (text-mode pipeline)
        df = _text_rows_to_df(result.rows)
        save_cache(fid, fname, df, source_kind="docai_text")
        col_map = {}
        headers = []

    # ── Compute column examples for the wizard UI ─────────────────────────────
    n_cols = len(df.columns)
    col_examples: list[str] = []
    if result.mode == "table" and not df.empty:
        first_row = df.iloc[0]
        for ci in range(n_cols):
            col_examples.append(str(first_row.get(f"col_{ci}", ""))[:60])
    else:
        col_examples = []

    # Render wizard
    preview_rows = result.rows[:20]
    return templates.TemplateResponse(
        "google_ocr_wizard.html",
        {
            "request":            request,
            "filename":           fname,
            "file_id":            fid,
            "mode":               result.mode,
            "mode_label":         _MODE_LABELS.get(result.mode, result.mode),
            "metrics":            metrics,
            "total_rows":         len(result.rows),
            "preview_rows":       preview_rows,
            "extractors":         EXTRACTORS,
            "field_keys":         DEFAULT_FIELD_KEYS,
            "has_active_template": load_active_template() is not None,
            # Column mapping (table mode only)
            "n_cols":             n_cols if result.mode == "table" else 0,
            "col_headers":        headers,
            "col_examples":       col_examples,
            "detected":           col_map,   # name_idx / qty_idx / uom_idx
        },
    )
