"""Routes for PDF and image import (/upload-pdf and /pdf-wizard/{fid})."""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.cache import UPLOAD_DIR, save_cache
from app.database import get_db_session
from app.extractors import DEFAULT_FIELD_KEYS, EXTRACTORS
from app.models import ImportAttachment, ImportParseAttempt
from app.name_builder import load_active_template
from app.pdf_parser import ParseResult, detect_pdf_kind, parse_pdf

pdf_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

_ALLOWED_MIME = {
    "application/pdf",
    "image/png", "image/jpeg", "image/tiff",
    "image/bmp", "image/webp",
}
_ALLOWED_EXT = {".pdf", ".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".webp"}

_KIND_LABELS = {
    "TEXT_PDF": "PDF с текстом",
    "SCAN_PDF": "PDF-скан",
    "IMAGE":    "Изображение",
    "UNKNOWN":  "Неизвестный тип",
}
_METHOD_LABELS = {
    "pdfplumber_table": "Таблицы pdfplumber",
    "pdfplumber_words": "Слова pdfplumber (без таблиц)",
    "ocr_tesseract":    "OCR Tesseract",
}


def _file_id(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]


def _rows_to_dataframe(rows: list[list[str]]) -> pd.DataFrame:
    """Convert raw token rows to a one-column DataFrame for /transform."""
    names = []
    for row in rows:
        joined = " ".join(t for t in row if t.strip())
        if joined.strip():
            names.append(joined.strip())
    if not names:
        return pd.DataFrame(columns=["name"])
    return pd.DataFrame({"name": names})


def _save_attachment(fid: str, filename: str, path: Path, kind: str, session) -> ImportAttachment:
    att = ImportAttachment(
        file_id=fid,
        filename=filename,
        mime_type="",
        storage_path=str(path),
        kind=kind,
        created_at=datetime.now(timezone.utc),
    )
    session.add(att)
    session.flush()
    return att


def _save_attempt(
    fid: str,
    att_id: int,
    result: ParseResult,
    session,
) -> ImportParseAttempt:
    attempt = ImportParseAttempt(
        file_id=fid,
        attachment_id=att_id,
        method=result.method,
        status=result.status,
        rows_found=len(result.rows),
        metrics_json="{}",
        error_text=result.error,
        created_at=datetime.now(timezone.utc),
    )
    attempt.metrics = result.metrics
    session.add(attempt)
    session.flush()
    return attempt


@pdf_router.get("/upload-pdf", response_class=HTMLResponse)
async def upload_pdf_form(request: Request):
    return templates.TemplateResponse("upload_pdf.html", {"request": request})


@pdf_router.post("/upload-pdf", response_class=HTMLResponse)
async def upload_pdf(request: Request, file: UploadFile = File(...)):
    fname = file.filename or "document"
    ext = Path(fname).suffix.lower()

    if ext not in _ALLOWED_EXT:
        return templates.TemplateResponse(
            "upload_pdf.html",
            {
                "request": request,
                "error": f"Формат '{ext}' не поддерживается. Допустимые: PDF, PNG, JPG, TIFF, BMP, WEBP.",
            },
            status_code=400,
        )

    file_bytes = await file.read()
    fid = _file_id(file_bytes)

    # Save raw file
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = UPLOAD_DIR / fname
    dest.write_bytes(file_bytes)

    # Detect kind
    kind = detect_pdf_kind(dest)

    # Parse
    result = parse_pdf(dest)

    # Persist metadata
    session = get_db_session()
    try:
        att = _save_attachment(fid, fname, dest, kind, session)
        attempt = _save_attempt(fid, att.id, result, session)
        session.commit()
        att_id = att.id
        attempt_id = attempt.id
    finally:
        session.close()

    if result.status == "error":
        return templates.TemplateResponse(
            "upload_pdf.html",
            {
                "request": request,
                "error": f"Ошибка разбора: {result.error}",
            },
            status_code=422,
        )

    # Build DataFrame and save to cache
    df = _rows_to_dataframe(result.rows)
    save_cache(fid, fname, df)

    # Render wizard
    preview_rows = result.rows[:20]
    return templates.TemplateResponse(
        "pdf_wizard.html",
        {
            "request": request,
            "filename": fname,
            "file_id": fid,
            "kind": kind,
            "kind_label": _KIND_LABELS.get(kind, kind),
            "method": result.method,
            "method_label": _METHOD_LABELS.get(result.method, result.method),
            "metrics": result.metrics,
            "total_rows": len(result.rows),
            "preview_rows": preview_rows,
            "extractors": EXTRACTORS,
            "field_keys": DEFAULT_FIELD_KEYS,
            "has_active_template": load_active_template() is not None,
        },
    )
