"""JSON API v1 — endpoints for 1C integration."""

from typing import List

from fastapi import APIRouter, File, Query, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.cache import file_id_from_bytes, load_dataframe, load_meta, save_cache
from app.extractors import EXTRACTORS, transform_dataframe
from app.main import UPLOAD_DIR
from app.parser_excel import load_excel

router = APIRouter(prefix="/api/v1")


def _error(status: int, msg: str) -> JSONResponse:
    return JSONResponse({"error": msg}, status_code=status)


def _df_to_rows(df) -> list[list]:
    """Convert DataFrame to list-of-lists (NaN → None)."""
    return df.where(df.notna(), None).values.tolist()


# ── POST /api/v1/upload ─────────────────────────────────────


@router.post("/upload")
async def api_upload(file: UploadFile = File(...)):
    if not file.filename or not file.filename.lower().endswith(".xlsx"):
        return _error(400, "Только файлы .xlsx допускаются для загрузки.")

    file_bytes = await file.read()

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = UPLOAD_DIR / file.filename
    dest.write_bytes(file_bytes)

    try:
        df = load_excel(dest)
    except Exception:
        return _error(400, "Не удалось прочитать файл. Убедитесь, что это корректный .xlsx.")

    fid = file_id_from_bytes(file_bytes)
    save_cache(fid, file.filename, df)

    return {
        "file_id": fid,
        "filename": file.filename,
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


@router.post("/transform")
async def api_transform(body: TransformRequest):
    df = load_dataframe(body.file_id)
    if df is None:
        return _error(404, "not found")

    meta = load_meta(body.file_id)

    valid_fields = [f for f in body.fields if f in EXTRACTORS]
    transformed = transform_dataframe(df, valid_fields)
    preview = transformed.head(200)

    return {
        "file_id": body.file_id,
        "rows_total": meta["rows_total"],
        "fields": valid_fields,
        "columns": list(preview.columns),
        "rows": _df_to_rows(preview),
    }
