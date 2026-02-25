"""JSON API v1 — endpoints for 1C integration."""

from typing import List, Optional

from fastapi import APIRouter, File, Query, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.cache import (
    UPLOAD_DIR,
    file_id_from_bytes,
    load_dataframe,
    load_meta,
    load_raw_values,
    save_cache,
    save_raw_cache,
    update_cache_with_columns,
)
from app.extractors import EXTRACTORS, transform_dataframe
from app.parser_excel import ParseError, build_dataframe_from_columns, parse_excel
from app.readiness import apply_readiness

router = APIRouter(prefix="/api/v1")


def _error(status: int, msg: str) -> JSONResponse:
    return JSONResponse({"error": msg}, status_code=status)


def _df_to_rows(df) -> list[list]:
    """Convert DataFrame to list-of-lists (NaN → None)."""
    return df.where(df.notna(), None).values.tolist()


def _detected_to_dict(detected) -> dict:
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
    }


# ── POST /api/v1/upload ─────────────────────────────────────


@router.post("/upload")
async def api_upload(file: UploadFile = File(...)):
    fname = (file.filename or "").lower()
    if fname.endswith(".xls") and not fname.endswith(".xlsx"):
        return _error(400, "Формат .xls пока не поддерживается, сохраните как .xlsx.")
    if not fname.endswith(".xlsx"):
        return _error(400, "Только файлы .xlsx допускаются для загрузки.")

    file_bytes = await file.read()

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = UPLOAD_DIR / file.filename
    dest.write_bytes(file_bytes)

    try:
        result = parse_excel(dest)
    except ParseError as exc:
        return _error(400, str(exc))
    except Exception:
        return _error(400, "Не удалось прочитать файл. Убедитесь, что это корректный .xlsx.")

    fid = file_id_from_bytes(file_bytes)

    if result.needs_manual_selection:
        save_raw_cache(fid, file.filename, result.raw_values or [], _detected_to_dict(result.detected))
        preview_rows = (result.raw_values or [])[:21]
        return {
            "file_id": fid,
            "filename": file.filename,
            "needs_column_selection": True,
            "preview_rows": preview_rows,
            "num_columns": max(len(r) for r in preview_rows) if preview_rows else 0,
            "detected": _detected_to_dict(result.detected),
        }

    save_cache(fid, file.filename, result.df, detected_columns=_detected_to_dict(result.detected))
    return {
        "file_id": fid,
        "filename": file.filename,
        "rows_total": len(result.df),
        "columns": list(result.df.columns),
        "needs_column_selection": False,
    }


# ── POST /api/v1/apply-columns ──────────────────────────────


class ApplyColumnsRequest(BaseModel):
    file_id: str
    name_col: int
    qty_col: int = -1
    code_col: int = -1
    header_row: int


@router.post("/apply-columns")
async def api_apply_columns(body: ApplyColumnsRequest):
    raw_values = load_raw_values(body.file_id)
    if raw_values is None:
        return _error(404, "not found")

    code_idx = body.code_col if body.code_col >= 0 else None
    qty_idx = body.qty_col if body.qty_col >= 0 else None

    try:
        df = build_dataframe_from_columns(
            raw_values, body.header_row, body.name_col, qty_idx, code_idx
        )
    except ParseError as exc:
        return _error(400, str(exc))
    except Exception:
        return _error(400, "Не удалось построить таблицу с указанными колонками.")

    detected_dict = {
        "name_idx": body.name_col,
        "qty_idx": qty_idx,
        "code_idx": code_idx,
        "header_row": body.header_row,
        "method": "manual",
        "score": 0,
    }
    update_cache_with_columns(body.file_id, df, detected_columns=detected_dict, manual_override=True)
    meta = load_meta(body.file_id)

    return {
        "file_id": body.file_id,
        "filename": meta["filename"],
        "rows_total": len(df),
        "columns": list(df.columns),
    }


# ── GET /api/v1/preview/{file_id} ───────────────────────────


@router.get("/preview/{file_id}")
async def api_preview(file_id: str, limit: int = Query(default=200, ge=1)):
    meta = load_meta(file_id)
    if meta is None:
        return _error(404, "not found")

    df = load_dataframe(file_id)
    if df is None:
        return _error(404, "not found")

    preview = df.head(limit)

    return {
        "file_id": file_id,
        "rows_total": meta["rows_total"],
        "limit": limit,
        "columns": list(preview.columns),
        "rows": _df_to_rows(preview),
    }


# ── POST /api/v1/transform ──────────────────────────────────


class TransformRequest(BaseModel):
    file_id: str
    fields: List[str] = []
    limit: int = 200


@router.post("/transform")
async def api_transform(body: TransformRequest):
    df = load_dataframe(body.file_id)
    if df is None:
        return _error(404, "not found")

    meta = load_meta(body.file_id)

    valid_fields = [f for f in body.fields if f in EXTRACTORS]
    transformed = transform_dataframe(df, valid_fields)
    transformed = apply_readiness(df, transformed)
    preview = transformed.head(body.limit)

    return {
        "file_id": body.file_id,
        "rows_total": meta["rows_total"],
        "fields": valid_fields,
        "columns": list(preview.columns),
        "rows": _df_to_rows(preview),
    }
