"""Routes for the Quote OCR (raw table extraction) page."""
from __future__ import annotations

import csv
import io
from pathlib import Path

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from app.database import get_db_session

quote_ocr_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

_MIME_MAP = {
    "pdf": "application/pdf",
    "png": "image/png",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "tiff": "image/tiff",
    "bmp": "image/bmp",
}


@quote_ocr_router.get("/quote-ocr", response_class=HTMLResponse)
def quote_ocr_page(request: Request, job_id: int = 0, table_idx: int = 0):
    from app.order_models import QuoteOcrJob, QuoteOcrTable

    ocr_available = False
    try:
        from app.integrations.google_document_ai import is_configured
        ocr_available = is_configured()
    except Exception:
        pass

    job = None
    tables_meta = []
    selected_table = None
    selected_rows = []

    if job_id:
        session = get_db_session()
        try:
            job = session.get(QuoteOcrJob, job_id)
            if job:
                ocr_tables = (
                    session.query(QuoteOcrTable)
                    .filter_by(job_id=job_id)
                    .order_by(QuoteOcrTable.table_index)
                    .all()
                )
                tables_meta = [{
                    "id": t.id,
                    "index": t.table_index,
                    "page_no": t.page_no,
                    "n_rows": t.n_rows,
                    "n_cols": t.n_cols,
                    "confidence_avg": t.confidence_avg,
                } for t in ocr_tables]

                if ocr_tables:
                    # Select by index
                    sel = ocr_tables[table_idx] if table_idx < len(ocr_tables) else ocr_tables[0]
                    selected_table = {
                        "id": sel.id,
                        "index": sel.table_index,
                        "page_no": sel.page_no,
                        "n_rows": sel.n_rows,
                        "n_cols": sel.n_cols,
                        "confidence_avg": sel.confidence_avg,
                    }
                    selected_rows = sel.rows
        finally:
            session.close()

    return templates.TemplateResponse("quote_ocr.html", {
        "request": request,
        "ocr_available": ocr_available,
        "job": job,
        "tables_meta": tables_meta,
        "selected_table": selected_table,
        "selected_rows": selected_rows,
        "table_idx": table_idx,
    })


@quote_ocr_router.post("/quote-ocr", response_class=HTMLResponse)
def quote_ocr_upload(
    request: Request,
    file: UploadFile = File(...),
):
    from app.services.quote_ocr import run_quote_ocr

    file_bytes = file.file.read()
    ext = (file.filename.rsplit(".", 1)[-1] if file.filename and "." in file.filename else "").lower()
    content_type = _MIME_MAP.get(ext, "application/pdf")

    session = get_db_session()
    try:
        job_id = run_quote_ocr(file_bytes, file.filename or "upload", content_type, session)
    finally:
        session.close()

    return RedirectResponse(f"/quote-ocr?job_id={job_id}", status_code=303)


@quote_ocr_router.get("/quote-ocr/{job_id}/table/{table_id}/csv")
def quote_ocr_csv(job_id: int, table_id: int):
    from app.order_models import QuoteOcrTable

    session = get_db_session()
    try:
        tbl = session.get(QuoteOcrTable, table_id)
        if not tbl or tbl.job_id != job_id:
            return Response("Table not found", status_code=404)
        rows = tbl.rows
    finally:
        session.close()

    output = io.StringIO()
    writer = csv.writer(output)
    for row in rows:
        writer.writerow(row)

    csv_bytes = output.getvalue().encode("utf-8-sig")
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="table_{table_id}.csv"'},
    )
