"""Routes for the settings snapshot: page, download, upload."""
import json
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, File, Request, UploadFile
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates

from app.cache import CACHE_DIR
from app.database import get_db_session
from app.settings_backup import (
    SNAPSHOT_MODELS,
    SnapshotError,
    build_snapshot,
    dump_snapshot,
    restore_snapshot,
)

settings_backup_router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

# Human-readable names for the report; the key is the table name.
TABLE_TITLES = {
    "standard_equivalents": "Аналоги стандартов",
    "standard_ref": "Справочник стандартов",
    "product_type": "Типы продукции",
    "system_tail_phrase": "Хвостовые фразы",
    "readiness_rule": "Правила готовности",
    "base_validation_rule": "Базовые правила проверки",
    "validation_rule": "Правила проверки",
    "validation_rule_exception": "Исключения правил проверки",
    "coating_rule": "Правила покрытий",
    "strength_rule": "Правила прочности",
    "size_rule": "Правила размеров",
    "normalization_rules": "Правила нормализации",
    "inference_rule": "Правила логического вывода",
    "name_template": "Шаблоны наименования",
    "system_setting": "Системные настройки",
    "master_items": "Группы объединения",
    "master_item_members": "Состав групп",
}


def _backup_dir() -> Path:
    """Where automatic backups go — next to the cache, under data/."""
    return CACHE_DIR.parent / "backups"


def _current_counts() -> list:
    session = get_db_session()
    try:
        return [
            (m.__tablename__, TABLE_TITLES.get(m.__tablename__, m.__tablename__),
             session.query(m).count())
            for m in SNAPSHOT_MODELS
        ]
    finally:
        session.close()


@settings_backup_router.get("/settings/backup", response_class=HTMLResponse)
def backup_page(request: Request):
    return templates.TemplateResponse(
        "settings_backup.html",
        {"request": request, "counts": _current_counts(),
         "report": None, "error": None, "titles": TABLE_TITLES},
    )


@settings_backup_router.get("/settings/backup/export")
def backup_export():
    body = dump_snapshot(build_snapshot())
    name = f"otdelzakup-settings-{datetime.now():%Y-%m-%d-%H%M}.json"
    return Response(
        content=body.encode("utf-8"),
        media_type="application/json; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{name}"'},
    )


@settings_backup_router.post("/settings/backup/import", response_class=HTMLResponse)
async def backup_import(request: Request, file: UploadFile = File(...)):
    error = None
    report = None
    raw = await file.read()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        error = "Файл не читается как JSON."
        payload = None

    if payload is not None:
        try:
            report = restore_snapshot(payload, backup_dir=_backup_dir())
        except SnapshotError as exc:
            error = str(exc)

    return templates.TemplateResponse(
        "settings_backup.html",
        {"request": request, "counts": _current_counts(),
         "report": report, "error": error, "titles": TABLE_TITLES},
    )
