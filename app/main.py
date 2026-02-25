from pathlib import Path
from typing import List

import pandas as pd
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, Response
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
from app.database import get_db_session, init_db
from app.display_labels import display_label
from app.extractors import DEFAULT_FIELD_KEYS, EXTRACTORS, compute_status, transform_dataframe
from app.models import NameTemplate, ReadinessRule, StandardRef, ValidationRule
from app.name_builder import apply_normalized_names, load_active_template
from app.parser_excel import (
    ParseError,
    build_dataframe_from_columns,
    dataframe_preview,
    dataframe_to_html,
    dataframe_to_xlsx_bytes,
    parse_excel,
)
from app.readiness import apply_readiness, load_active_rules, load_active_standards
from app.trace import build_traces, load_traces, save_traces
from app.seed import seed_default_rules, seed_default_standards, seed_default_template

app = FastAPI(title="Отдел закупок — MVP")


@app.on_event("startup")
def on_startup():
    init_db()
    seed_default_rules()
    seed_default_standards()
    seed_default_template()

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def _detected_to_dict(detected) -> dict:
    """Convert DetectedColumns to a plain dict for cache storage."""
    return {
        "name_idx": detected.name_idx,
        "qty_idx": detected.qty_idx,
        "code_idx": detected.code_idx,
        "standard_idx": detected.standard_idx,
        "strength_col_idx": detected.strength_col_idx,
        "note_idx": detected.note_idx,
        "header_row": detected.header_row,
        "method": detected.method,
        "score": detected.score,
        "qty_uom_combined": getattr(detected, "qty_uom_combined", False),
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


_AVAILABLE_FIELDS_DICT = {
    "size": "Размер", "qty": "Количество", "name": "Наименование",
    "code": "Код", "item_type": "Тип изделия", "length": "Длина",
    "strength": "Класс прочности", "coating": "Покрытие",
    "gost": "ГОСТ", "iso": "ISO", "din": "DIN",
}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    session = get_db_session()
    try:
        rules = (
            session.query(ReadinessRule)
            .order_by(ReadinessRule.priority.asc(), ReadinessRule.id)
            .all()
        )
        standards = (
            session.query(StandardRef)
            .order_by(StandardRef.standard_kind, StandardRef.standard_code)
            .all()
        )
        validation_rules = (
            session.query(ValidationRule)
            .order_by(ValidationRule.priority.asc(), ValidationRule.id)
            .all()
        )
        name_templates = (
            session.query(NameTemplate)
            .order_by(NameTemplate.priority.asc(), NameTemplate.id)
            .all()
        )
        session.expunge_all()
    finally:
        session.close()

    return templates.TemplateResponse(
        "upload.html",
        {
            "request": request,
            "rules": rules,
            "standards": standards,
            "validation_rules": validation_rules,
            "name_templates": name_templates,
            "available_fields": _AVAILABLE_FIELDS_DICT,
        },
    )


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
            "field_keys": DEFAULT_FIELD_KEYS,
            "has_active_template": load_active_template() is not None,
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
            "field_keys": DEFAULT_FIELD_KEYS,
            "has_active_template": load_active_template() is not None,
        },
    )


def _compute_stats(transformed: "pd.DataFrame") -> dict:
    """Compute ok / review / manual counts from transformed DataFrame."""
    if "status" not in transformed.columns:
        return {"ok": 0, "review": 0, "manual": 0, "total": len(transformed), "ok_pct": 0}
    counts = transformed["status"].value_counts()
    ok = int(counts.get("ok", 0))
    review = int(counts.get("review", 0))
    manual = int(counts.get("manual", 0))
    total = len(transformed)
    pct = round(ok / total * 100) if total else 0
    return {"ok": ok, "review": review, "manual": manual, "total": total, "ok_pct": pct}


def _result_table_html(df: "pd.DataFrame", file_id: str = "") -> str:
    """Render transformed DataFrame as HTML table with data-status on each row.

    Adds a leading '№' column with a row number and an analysis link (🔍) when
    file_id is provided.
    """
    cols = [c for c in df.columns if c not in ("confidence", "status")]
    header = "<th>№</th>" + "".join(f"<th>{display_label(c)}</th>" for c in cols)
    rows_html = []
    for row_num, (_, row) in enumerate(df.iterrows(), start=1):
        status = row["status"] if "status" in row.index else ""
        if file_id:
            num_cell = (
                f'<td style="white-space:nowrap">{row_num}'
                f' <a href="/files/{file_id}/rows/{row_num}/analysis"'
                f' class="analysis-link" title="Анализ строки">🔍</a></td>'
            )
        else:
            num_cell = f"<td>{row_num}</td>"
        cells = num_cell + "".join(
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
    include_normalized = "normalized_name" in fields
    valid_fields = [f for f in fields if f in EXTRACTORS]

    # Load rules and standards once — reused by apply_readiness and build_traces
    rules = load_active_rules()
    standards_cache = load_active_standards()

    transformed = transform_dataframe(df, valid_fields)
    transformed = apply_readiness(df, transformed, rules=rules, standards_cache=standards_cache)

    if include_normalized:
        active_tpl = load_active_template()
        if active_tpl:
            transformed = apply_normalized_names(df, transformed, active_tpl.template_string)

    # Build and persist per-row trace data (for the analysis endpoint)
    traces = build_traces(df, transformed, rules=rules, standards_cache=standards_cache)
    save_traces(file_id, traces)

    processed_rows = len(transformed)

    # Save full result for download (all rows, not limited)
    token_fields = valid_fields + (["normalized_name"] if include_normalized else [])
    token = make_download_token(file_id, token_fields)
    save_result(token, file_id, transformed)

    # Save OK-only result for separate download
    ok_df = transformed[transformed["status"] == "ok"]
    token_ok = make_download_token(file_id, token_fields + ["__ok_only__"])
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
            "result_table": _result_table_html(transformed_preview, file_id),
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


@app.get("/files/{file_id}/rows/{row_number}/analysis")
async def row_analysis(file_id: str, row_number: int):
    """Return trace JSON for a single result row (1-indexed)."""
    traces = load_traces(file_id)
    if traces is None:
        return JSONResponse(
            {"error": "Сначала выполните преобразование файла"},
            status_code=404,
        )
    if row_number < 1 or row_number > len(traces):
        return JSONResponse(
            {"error": f"Строка {row_number} не найдена (всего {len(traces)} строк)"},
            status_code=404,
        )
    return JSONResponse(traces[row_number - 1])


# ── API routes ───────────────────────────────────────────────

from app.api import router as api_router  # noqa: E402
from app.name_template_routes import name_template_router  # noqa: E402
from app.readiness_routes import readiness_router  # noqa: E402
from app.standard_routes import standard_router  # noqa: E402
from app.validation_routes import rules_router  # noqa: E402

app.include_router(api_router)
app.include_router(readiness_router)
app.include_router(standard_router)
app.include_router(rules_router)
app.include_router(name_template_router)
