from pathlib import Path
from typing import List

import pandas as pd
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates

from app.cache import (
    UPLOAD_DIR,
    file_id_from_bytes,
    load_dataframe,
    load_meta,
    load_raw_values,
    load_result,
    make_download_token,
    save_cache,
    save_raw_cache,
    save_result,
    update_cache_with_columns,
)
from app.extractors import ALL_FIELD_KEYS, EXTRACTORS, compute_status, transform_dataframe
from app.parser_excel import (
    ParseError,
    build_dataframe_from_columns,
    dataframe_preview,
    dataframe_to_html,
    dataframe_to_xlsx_bytes,
    parse_excel,
)

app = FastAPI(title="Отдел закупок — MVP")

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def _detected_to_dict(detected) -> dict:
    """Convert DetectedColumns to a plain dict for cache storage."""
    return {
        "name_idx": detected.name_idx,
        "qty_idx": detected.qty_idx,
        "code_idx": detected.code_idx,
        "header_row": detected.header_row,
        "method": detected.method,
        "score": detected.score,
    }


def _save_and_parse(file_bytes: bytes, filename: str):
    """Save uploaded file to disk, parse it. Returns (file_id, ParseResult)."""
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = UPLOAD_DIR / filename
    dest.write_bytes(file_bytes)

    result = parse_excel(dest)
    fid = file_id_from_bytes(file_bytes)

    if result.df is not None and not result.needs_manual_selection:
        save_cache(fid, filename, result.df, detected_columns=_detected_to_dict(result.detected))
    else:
        save_raw_cache(fid, filename, result.raw_values or [], _detected_to_dict(result.detected))

    return fid, result


# ── Web routes ───────────────────────────────────────────────


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("upload.html", {"request": request})


@app.post("/upload", response_class=HTMLResponse)
async def upload(request: Request, file: UploadFile = File(...)):
    fname = (file.filename or "").lower()
    if fname.endswith(".xls") and not fname.endswith(".xlsx"):
        return templates.TemplateResponse(
            "upload.html",
            {"request": request, "error": "Формат .xls пока не поддерживается, сохраните как .xlsx."},
            status_code=400,
        )
    if not fname.endswith(".xlsx"):
        return templates.TemplateResponse(
            "upload.html",
            {"request": request, "error": "Только файлы .xlsx допускаются для загрузки."},
            status_code=400,
        )

    file_bytes = await file.read()

    try:
        fid, result = _save_and_parse(file_bytes, file.filename)
    except ParseError as exc:
        return templates.TemplateResponse(
            "upload.html",
            {"request": request, "error": str(exc)},
            status_code=400,
        )
    except Exception:
        return templates.TemplateResponse(
            "upload.html",
            {"request": request, "error": "Не удалось прочитать файл. Убедитесь, что это корректный .xlsx."},
            status_code=400,
        )

    if result.needs_manual_selection:
        preview_rows = (result.raw_values or [])[:21]
        num_columns = max(len(r) for r in preview_rows) if preview_rows else 0
        header_row_idx = result.detected.header_row if result.detected.header_row is not None else 0
        if header_row_idx < len(result.raw_values or []):
            col_headers = [str(v) if v is not None else "" for v in (result.raw_values or [])[header_row_idx]]
        else:
            col_headers = [""] * num_columns
        col_headers.extend([""] * (num_columns - len(col_headers)))

        return templates.TemplateResponse(
            "select_columns.html",
            {
                "request": request,
                "filename": file.filename,
                "file_id": fid,
                "preview_rows": preview_rows,
                "num_columns": num_columns,
                "col_headers": col_headers,
                "detected": result.detected,
            },
        )

    df = result.df
    total_rows = len(df)
    preview = dataframe_preview(df, limit=200)
    table_html = dataframe_to_html(preview)

    return templates.TemplateResponse(
        "view_raw.html",
        {
            "request": request,
            "filename": file.filename,
            "file_id": fid,
            "total_rows": total_rows,
            "table_html": table_html,
            "extractors": EXTRACTORS,
            "field_keys": ALL_FIELD_KEYS,
        },
    )


@app.post("/apply-columns", response_class=HTMLResponse)
async def apply_columns(
    request: Request,
    file_id: str = Form(...),
    name_col: int = Form(...),
    qty_col: int = Form(...),
    code_col: int = Form(default=-1),
    header_row: int = Form(...),
):
    """Accept manual column selection, build DataFrame, cache it, show view_raw."""
    raw_values = load_raw_values(file_id)
    meta = load_meta(file_id)
    if raw_values is None or meta is None:
        return templates.TemplateResponse(
            "upload.html",
            {"request": request, "error": "Файл не найден. Загрузите файл заново."},
            status_code=400,
        )

    code_idx = code_col if code_col >= 0 else None
    qty_idx = qty_col if qty_col >= 0 else None

    try:
        df = build_dataframe_from_columns(raw_values, header_row, name_col, qty_idx, code_idx)
    except ParseError as exc:
        return templates.TemplateResponse(
            "upload.html",
            {"request": request, "error": str(exc)},
            status_code=400,
        )
    except Exception:
        return templates.TemplateResponse(
            "upload.html",
            {"request": request, "error": "Не удалось построить таблицу с указанными колонками."},
            status_code=400,
        )

    detected_dict = {
        "name_idx": name_col,
        "qty_idx": qty_idx,
        "code_idx": code_idx,
        "header_row": header_row,
        "method": "manual",
        "score": 0,
    }
    update_cache_with_columns(file_id, df, detected_columns=detected_dict, manual_override=True)

    total_rows = len(df)
    preview = dataframe_preview(df, limit=200)
    table_html = dataframe_to_html(preview)

    return templates.TemplateResponse(
        "view_raw.html",
        {
            "request": request,
            "filename": meta["filename"],
            "file_id": file_id,
            "total_rows": total_rows,
            "table_html": table_html,
            "extractors": EXTRACTORS,
            "field_keys": ALL_FIELD_KEYS,
        },
    )


def _compute_stats(transformed: "pd.DataFrame") -> dict:
    """Compute OK / warning / error counts from transformed DataFrame."""
    if "status" not in transformed.columns:
        return {"ok": 0, "warning": 0, "error": 0, "total": len(transformed), "ok_pct": 0}
    counts = transformed["status"].value_counts()
    ok = int(counts.get("ok", 0))
    warn = int(counts.get("warning", 0))
    err = int(counts.get("error", 0))
    total = len(transformed)
    pct = round(ok / total * 100) if total else 0
    return {"ok": ok, "warning": warn, "error": err, "total": total, "ok_pct": pct}


def _result_table_html(df: "pd.DataFrame") -> str:
    """Render transformed DataFrame as HTML table with data-status on each row."""
    cols = [c for c in df.columns if c not in ("confidence", "status")]
    header = "".join(f"<th>{c}</th>" for c in cols)
    rows_html = []
    for _, row in df.iterrows():
        status = row.get("status", "")
        cells = "".join(
            f"<td>{'' if pd.isna(row[c]) else row[c]}</td>" for c in cols
        )
        rows_html.append(f'<tr data-status="{status}">{cells}</tr>')
    return (
        '<table class="table" id="result-table">'
        f"<thead><tr>{header}</tr></thead>"
        f'<tbody>{"".join(rows_html)}</tbody></table>'
    )


@app.post("/transform", response_class=HTMLResponse)
async def transform(
    request: Request,
    file_id: str = Form(...),
    fields: List[str] = Form(default=[]),
):
    df = load_dataframe(file_id)
    meta = load_meta(file_id)
    if df is None or meta is None:
        return templates.TemplateResponse(
            "upload.html",
            {"request": request, "error": "Файл не найден. Загрузите файл заново."},
            status_code=400,
        )

    total_rows = len(df)
    valid_fields = [f for f in fields if f in EXTRACTORS]

    transformed = transform_dataframe(df, valid_fields)
    processed_rows = len(transformed)

    # Save full result for download (all rows, not limited)
    token = make_download_token(file_id, valid_fields)
    save_result(token, file_id, transformed)

    # Save OK-only result for separate download
    ok_df = transformed[transformed["status"] == "ok"]
    token_ok = make_download_token(file_id, valid_fields + ["__ok_only__"])
    save_result(token_ok, file_id, ok_df)

    stats = _compute_stats(transformed)

    raw_preview = dataframe_preview(df, limit=200)
    transformed_preview = dataframe_preview(transformed, limit=200)

    return templates.TemplateResponse(
        "view_result.html",
        {
            "request": request,
            "filename": meta["filename"],
            "total_rows": total_rows,
            "processed_rows": processed_rows,
            "raw_table": dataframe_to_html(raw_preview),
            "result_table": _result_table_html(transformed_preview),
            "download_token": token,
            "download_token_ok": token_ok,
            "file_id": file_id,
            "stats": stats,
        },
    )


@app.get("/download/{file_id}/{token}")
async def download(file_id: str, token: str):
    meta = load_meta(file_id)
    if meta is None:
        return Response("Файл не найден", status_code=404)

    result_df = load_result(token, file_id)
    if result_df is None:
        return Response("Результат не найден. Выполните преобразование заново.", status_code=404)

    xlsx_bytes = dataframe_to_xlsx_bytes(result_df)
    base = meta["filename"].replace(".xlsx", "")
    safe_name = f"{base}_result.xlsx"
    utf8_name = f"{base}_результат.xlsx"

    from urllib.parse import quote
    disposition = (
        f"attachment; filename=\"{safe_name}\"; "
        f"filename*=UTF-8''{quote(utf8_name)}"
    )

    return Response(
        content=xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": disposition},
    )


# ── API routes ───────────────────────────────────────────────

from app.api import router as api_router  # noqa: E402

app.include_router(api_router)
