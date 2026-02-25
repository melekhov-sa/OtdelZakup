import os
import shutil
from pathlib import Path
from typing import List

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.cache import CACHE_DIR, file_id_from_bytes, load_dataframe, load_meta, save_cache
from app.extractors import ALL_FIELD_KEYS, EXTRACTORS, transform_dataframe
from app.parser_excel import dataframe_preview, dataframe_to_html, load_excel

UPLOAD_DIR = Path(os.environ.get("OTDELZAKUP_UPLOAD_DIR", "./data/uploads"))

app = FastAPI(title="Отдел закупок — MVP")

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def _save_and_cache(file_bytes: bytes, filename: str) -> tuple[str, "pd.DataFrame"]:
    """Save uploaded file to disk, parse it, cache the DataFrame. Returns (file_id, df)."""
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = UPLOAD_DIR / filename
    dest.write_bytes(file_bytes)

    df = load_excel(dest)
    fid = file_id_from_bytes(file_bytes)
    save_cache(fid, filename, df)
    return fid, df


# ── Web routes ───────────────────────────────────────────────


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("upload.html", {"request": request})


@app.post("/upload", response_class=HTMLResponse)
async def upload(request: Request, file: UploadFile = File(...)):
    if not file.filename or not file.filename.lower().endswith(".xlsx"):
        return templates.TemplateResponse(
            "upload.html",
            {
                "request": request,
                "error": "Только файлы .xlsx допускаются для загрузки.",
            },
            status_code=400,
        )

    file_bytes = await file.read()

    try:
        fid, df = _save_and_cache(file_bytes, file.filename)
    except Exception:
        return templates.TemplateResponse(
            "upload.html",
            {
                "request": request,
                "error": "Не удалось прочитать файл. Убедитесь, что это корректный .xlsx.",
            },
            status_code=400,
        )

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
            "result_table": dataframe_to_html(transformed_preview),
        },
    )


# ── API routes ───────────────────────────────────────────────

from app.api import router as api_router  # noqa: E402

app.include_router(api_router)
