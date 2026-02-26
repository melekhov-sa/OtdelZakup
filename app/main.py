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
from app.display_labels import display_label, format_qty
from app.extractors import DEFAULT_FIELD_KEYS, EXTRACTORS, compute_status, transform_dataframe
from app.models import InferenceRule, NameTemplate, ReadinessRule, StandardRef, ValidationRule
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
from app.seed import (
    seed_default_rules,
    seed_default_standards,
    seed_default_template,
    seed_default_product_types,
)
from app.inference_engine import load_active_inference_rules

app = FastAPI(title="Отдел закупок — MVP")


@app.on_event("startup")
def on_startup():
    init_db()
    seed_default_rules()
    seed_default_standards()
    seed_default_template()
    seed_default_product_types()

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def _detected_to_dict(detected) -> dict:
    """Convert DetectedColumns to a plain dict for cache storage."""
    return {
        "name_idx": detected.name_idx,
        "qty_idx": detected.qty_idx,
        "uom_idx": getattr(detected, "uom_idx", None),
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
        inference_rules_list = (
            session.query(InferenceRule)
            .order_by(InferenceRule.priority.asc(), InferenceRule.id)
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
            "inference_rules": inference_rules_list,
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

        # First data row — for example values shown in column select options
        raw = result.raw_values or []
        data_row_idx = header_row_idx + 1
        ex_row = raw[data_row_idx] if data_row_idx < len(raw) else []
        examples = [str(v) if v is not None else "" for v in ex_row]
        examples += [""] * max(0, num_columns - len(examples))

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
                "examples": examples,
            },
        )

    df = result.df
    total_rows = len(df)
    preview = dataframe_preview(df, limit=200)
    table_html = dataframe_to_html(_drop_internal(preview))

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
    qty_col: int = Form(default=-1),
    uom_col: int = Form(default=-1),
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
    uom_idx = uom_col if uom_col >= 0 else None

    try:
        from app.parsing.tail_extractor import load_active_tail_phrases
        _tail_phrases = load_active_tail_phrases()
        df = build_dataframe_from_columns(
            raw_values, header_row, name_col, qty_idx, code_idx, uom_idx=uom_idx,
            tail_phrases=_tail_phrases,
        )
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
        "uom_idx": uom_idx,
        "code_idx": code_idx,
        "header_row": header_row,
        "method": "manual",
        "score": 0,
        "qty_uom_combined": False,
    }
    update_cache_with_columns(file_id, df, detected_columns=detected_dict, manual_override=True)

    total_rows = len(df)
    preview = dataframe_preview(df, limit=200)
    table_html = dataframe_to_html(_drop_internal(preview))

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


_INTERNAL_COLS = frozenset({
    "raw_text", "qty_uom_source",
    "tail_phrase_cut", "tail_qty_expr", "qty_multiplier", "qty_fail_reason",
})

# Columns hidden from the HTML result table but exported to xlsx
_RESULT_TABLE_EXTRA_HIDE = frozenset({"Режим подбора", "Score"})

# Human-readable mode labels for export
_EXPORT_MODE_LABELS = {
    "AUTO_MEMORY": "Авто (память)",
    "AUTO_SCORE":  "Авто",
    "SUGGESTED":   "Предложено",
    "NONE":        "Нет",
    "MANUAL_SELECTED": "Вручную",
    "CONFIRMED":   "Подтверждено",
}


def _drop_internal(df: "pd.DataFrame") -> "pd.DataFrame":
    """Return df without internal RowParser columns (for display/export)."""
    drop = [c for c in _INTERNAL_COLS if c in df.columns]
    return df.drop(columns=drop) if drop else df


def _prepare_export_df(df: "pd.DataFrame", match_results: list) -> "pd.DataFrame":
    """Build an export-ready DataFrame with readable match columns."""
    out = _drop_internal(df).copy()
    if "internal_match" in out.columns:
        out.rename(columns={"internal_match": "Наша номенклатура"}, inplace=True)
    if match_results:
        out["Режим подбора"] = [
            _EXPORT_MODE_LABELS.get(r.get("mode", ""), "") for r in match_results
        ]
        out["Score"] = [
            r.get("score", "") if r.get("mode", "NONE") != "NONE" else ""
            for r in match_results
        ]
    return out


def _esc(s: str) -> str:
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


_RESULT_TABLE_HIDE = {"confidence", "status"} | _INTERNAL_COLS | _RESULT_TABLE_EXTRA_HIDE


def _result_table_html(
    df: "pd.DataFrame",
    file_id: str = "",
    match_results: list | None = None,
) -> str:
    """Render transformed DataFrame as HTML table with data-status on each row.

    Adds a leading '№' column with a row number and an analysis link (🔍).
    The internal_match cell is rendered differently based on match_mode.
    """
    mr_by_row: dict = {i + 1: r for i, r in enumerate(match_results or [])}

    cols = [c for c in df.columns if c not in _RESULT_TABLE_HIDE]
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

        mr = mr_by_row.get(row_num, {})

        def _cell(c):
            val = row[c]
            raw = format_qty(val) if c == "qty" else ("" if pd.isna(val) else str(val) if val is not None else "")
            if c == "internal_match" and file_id:
                return _render_match_cell(raw, row_num, mr, file_id)
            return f"<td>{_esc(raw)}</td>"

        cells = num_cell + "".join(_cell(c) for c in cols)
        rows_html.append(f'<tr data-status="{status}">{cells}</tr>')

    return (
        '<table class="table" id="result-table">'
        f"<thead><tr>{header}</tr></thead>"
        f'<tbody>{"".join(rows_html)}</tbody></table>'
    )


def _render_match_cell(name: str, row_num: int, mr: dict, file_id: str) -> str:
    """Render the 'Наша номенклатура' cell based on match mode."""
    import json
    mode = mr.get("mode", "NONE")
    iid  = mr.get("internal_item_id") or 0
    select_url = f"/files/{file_id}/rows/{row_num}/select-internal"
    safe_name  = _esc(name)
    safe_fid   = _esc(file_id)

    if mode in ("AUTO_MEMORY", "AUTO_SCORE"):
        badge = "память" if mode == "AUTO_MEMORY" else "авто"
        return (
            f'<td style="white-space:nowrap">'
            f'<span style="color:#2e7d32">&#10003;</span> {safe_name} '
            f'<span style="background:#e8f5e9;color:#2e7d32;font-size:10px;'
            f'padding:1px 5px;border-radius:8px">{badge}</span>'
            f' <a href="{select_url}" style="font-size:10px;color:#aaa">Изм.</a>'
            f'</td>'
        )

    if mode == "SUGGESTED":
        fid_js = json.dumps(file_id)
        return (
            f'<td style="white-space:nowrap" data-confirm-row="{row_num}">'
            f'<span style="color:#f57f17">?</span> {safe_name} '
            f'<button onclick="confirmMatch({fid_js},{row_num},{iid})" '
            f'style="font-size:11px;padding:2px 7px;background:#f57f17;color:#fff;'
            f'border:none;border-radius:3px;cursor:pointer">Подтвердить</button>'
            f' <a href="{select_url}" style="font-size:10px;color:#aaa">Изм.</a>'
            f'</td>'
        )

    if mode in ("MANUAL_SELECTED", "CONFIRMED"):
        label = "подтверждено" if mode == "CONFIRMED" else "вручную"
        return (
            f'<td style="white-space:nowrap">'
            f'<span style="color:#2e7d32">&#10003;</span> {safe_name} '
            f'<span style="font-size:10px;color:#888">{label}</span>'
            f' <a href="{select_url}" style="font-size:10px;color:#aaa">Изм.</a>'
            f'</td>'
        )

    # NONE — show candidate count hint
    n_cand = len(mr.get("candidates", []))
    debug = mr.get("match_debug") or {}
    if n_cand > 0:
        hint = (
            f'<span style="font-size:10px;color:#aaa">({n_cand}\u00a0канд.)</span> '
        )
    else:
        zero_r = debug.get("zero_reason") or ""
        hint_txt = "0\u00a0канд." + (f"\u00a0\u2014\u00a0{zero_r}" if zero_r else "")
        hint = f'<span style="font-size:10px;color:#c62828">({hint_txt})</span> '
    return (
        f'<td style="white-space:nowrap">'
        f'{hint}<a href="{select_url}" style="font-size:11px;color:#888">Выбрать...</a>'
        f'</td>'
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
    inference_rules = load_active_inference_rules()

    transformed = transform_dataframe(df, valid_fields)
    transformed = apply_readiness(
        df, transformed, rules=rules, standards_cache=standards_cache,
        inference_rules=inference_rules,
    )

    if include_normalized:
        active_tpl = load_active_template()
        if active_tpl:
            transformed = apply_normalized_names(df, transformed, active_tpl.template_string)

    # Internal catalog matching
    from app.matcher import add_internal_matches
    transformed, match_results = add_internal_matches(transformed)

    # Build and persist per-row trace data (for the analysis endpoint)
    traces = build_traces(
        df, transformed, rules=rules, standards_cache=standards_cache,
        inference_rules=inference_rules, match_results=match_results,
    )
    save_traces(file_id, traces)

    processed_rows = len(transformed)

    # Save full result for download (all rows) — with match mode + score columns
    token_fields = valid_fields + (["normalized_name"] if include_normalized else [])
    token = make_download_token(file_id, token_fields)
    save_result(token, file_id, _prepare_export_df(transformed, match_results))

    # Save OK-only result for separate download
    ok_mask = transformed["status"] == "ok"
    ok_df = transformed[ok_mask]
    ok_match_results = [r for r, ok in zip(match_results, ok_mask) if ok]
    token_ok = make_download_token(file_id, token_fields + ["__ok_only__"])
    save_result(token_ok, file_id, _prepare_export_df(ok_df, ok_match_results))

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
            "raw_table": dataframe_to_html(_drop_internal(raw_preview)),
            "result_table": _result_table_html(transformed_preview, file_id, match_results),
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
from app.inference_routes import inference_router  # noqa: E402
from app.standard_routes import standard_router  # noqa: E402
from app.text_routes import text_router  # noqa: E402
from app.validation_routes import rules_router  # noqa: E402
from app.sandbox_routes import sandbox_router  # noqa: E402
from app.internal_item_routes import internal_item_router  # noqa: E402
from app.settings_routes import settings_router  # noqa: E402
from app.tail_phrase_routes import tail_phrase_router  # noqa: E402
from app.product_type_routes import product_type_router  # noqa: E402

app.include_router(api_router)
app.include_router(readiness_router)
app.include_router(standard_router)
app.include_router(rules_router)
app.include_router(name_template_router)
app.include_router(text_router)
app.include_router(inference_router)
app.include_router(sandbox_router)
app.include_router(internal_item_router)
app.include_router(settings_router)
app.include_router(tail_phrase_router)
app.include_router(product_type_router)
