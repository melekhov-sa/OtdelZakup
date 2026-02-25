import hashlib
import os
import shutil
from pathlib import Path
from typing import List

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.extractors import ALL_FIELD_KEYS, EXTRACTORS, transform_dataframe
from app.parser_excel import dataframe_preview, dataframe_to_html, load_excel

UPLOAD_DIR = Path(os.environ.get("OTDELZAKUP_UPLOAD_DIR", "./data/uploads"))

app = FastAPI(title="Отдел закупок — MVP")

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def _file_id(filename: str) -> str:
    """Deterministic hash-based id for a given filename."""
    return hashlib.sha256(filename.encode()).hexdigest()[:16]


def _find_file_by_id(file_id: str) -> Path | None:
    """Locate a previously uploaded file by its id."""
    if not UPLOAD_DIR.exists():
        return None
    for p in UPLOAD_DIR.iterdir():
        if _file_id(p.name) == file_id:
            return p
    return None


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

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = UPLOAD_DIR / file.filename
    with open(dest, "wb") as buf:
        shutil.copyfileobj(file.file, buf)

    try:
        df = load_excel(dest)
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
    fid = _file_id(file.filename)

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
    path = _find_file_by_id(file_id)
    if path is None:
        return templates.TemplateResponse(
            "upload.html",
            {"request": request, "error": "Файл не найден. Загрузите файл заново."},
            status_code=400,
        )

    df = load_excel(path)
    total_rows = len(df)

    # Filter to only valid field keys
    valid_fields = [f for f in fields if f in EXTRACTORS]

    transformed = transform_dataframe(df, valid_fields)
    processed_rows = len(transformed)

    raw_preview = dataframe_preview(df, limit=200)
    transformed_preview = dataframe_preview(transformed, limit=200)

    return templates.TemplateResponse(
        "view_result.html",
        {
            "request": request,
            "filename": path.name,
            "total_rows": total_rows,
            "processed_rows": processed_rows,
            "raw_table": dataframe_to_html(raw_preview),
            "result_table": dataframe_to_html(transformed_preview),
        },
    )
