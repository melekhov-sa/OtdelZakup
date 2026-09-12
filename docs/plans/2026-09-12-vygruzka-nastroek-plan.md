# Выгрузка и загрузка настроек — план реализации

> **Для агентов:** реализовывать по задачам, каждая — цикл «тест → провал → код
> → зелено → коммит». Шаги отмечены чекбоксами.

**Спецификация:** `docs/specs/2026-09-12-vygruzka-nastroek.md` — читать целиком до начала.

**Цель:** снимок 17 таблиц справочников, правил и настроек в JSON-файл, с
загрузкой обратно через полную замену — чтобы переносить настройки между
серверами и не терять их при переустановке.

**Архитектура:** вся работа со снимком живёт в одном модуле без веба —
`app/settings_backup.py`. Веб-маршруты и консольный скрипт зовут его функции и
ничего своего не считают, поэтому файл из браузера и файл из планировщика
идентичны. Состав снимка задан одним списком моделей `SNAPSHOT_MODELS`: добавить
таблицу — одна строка.

**Стек:** Python 3, FastAPI, SQLAlchemy, Jinja2, pytest.

## Общие требования ко всем задачам

- Прогон тестов: `./venv/Scripts/python.exe -m pytest <путь> -q` из корня проекта.
- Комментарии и docstring — на английском, как во всём проекте.
- Текст на страницах и сообщения пользователю — на русском.
- `FORMAT_VERSION = 1`.
- Каталог (`internal_item`, `nomenclature_folder`), память подбора
  (`supplier_internal_match`), заказы, КП и песочница в снимок **не входят**.
- Таблица, отсутствующая в файле, не трогается. Таблица с пустым списком очищается.
- Изолирующая фикстура тестов копируется из `tests/test_use_analogs.py:30-49`.

## Структура файлов

| Файл | Ответственность |
|---|---|
| `app/settings_backup.py` | Создаётся. Сбор снимка, восстановление, сброс кэшей, автобэкап. Без веба |
| `app/settings_backup_routes.py` | Создаётся. Страница, скачивание файла, приём файла |
| `app/templates/settings_backup.html` | Создаётся. Страница с двумя кнопками и отчётом |
| `scripts/export_settings.py` | Создаётся. Снимок в файл из консоли |
| `app/main.py` | Изменяется. Регистрация роутера |
| `app/templates/match_settings.html` | Изменяется. Ссылка на страницу |
| `DEPLOY.md` | Изменяется. Шаг выгрузки в флоу обновления |
| `tests/test_settings_backup.py` | Создаётся. Ядро: снимок, восстановление, кэши, бэкап |
| `tests/test_settings_backup_routes.py` | Создаётся. Маршруты |

---

## Задача 1. Сбор снимка

**Файлы:**
- Создать: `app/settings_backup.py`
- Создать: `tests/test_settings_backup.py`

**Интерфейсы:**
- Отдаёт: `FORMAT_VERSION: int`, `SNAPSHOT_MODELS: list`, `SnapshotError`,
  `build_snapshot() -> dict`, `dump_snapshot(snapshot: dict) -> str`.

- [ ] **Шаг 1: Написать падающий тест**

Создать `tests/test_settings_backup.py`:

```python
"""Settings snapshot: collect, restore, invalidate caches, back up."""
import json

import pytest

from app.models import ReadinessRule, StandardEquivalent


@pytest.fixture(autouse=True)
def _set_dirs(tmp_path, monkeypatch):
    upload_dir = tmp_path / "uploads"
    cache_dir = tmp_path / "cache"
    monkeypatch.setenv("OTDELZAKUP_UPLOAD_DIR", str(upload_dir))
    monkeypatch.setenv("OTDELZAKUP_CACHE_DIR", str(cache_dir))
    import app.cache as cache_mod
    cache_mod.UPLOAD_DIR = upload_dir
    cache_mod.CACHE_DIR = cache_dir

    db_path = tmp_path / "test.db"
    monkeypatch.setenv("OTDELZAKUP_DB_PATH", str(db_path))
    import app.database as db_mod
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    db_mod.DB_PATH = db_path
    db_mod.engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    db_mod.SessionLocal = sessionmaker(bind=db_mod.engine, autoflush=False, expire_on_commit=False)
    db_mod.init_db()


def _add_equiv(src="GOST-7798-70", dst="DIN-933"):
    from app.database import get_db_session
    session = get_db_session()
    session.add(StandardEquivalent(src_canonical=src, dst_canonical=dst, is_active=True))
    session.commit()
    session.close()


def test_snapshot_has_header():
    from app.settings_backup import FORMAT_VERSION, build_snapshot
    snap = build_snapshot()
    assert snap["format_version"] == FORMAT_VERSION
    assert snap["exported_at"]
    assert "source_host" in snap
    assert isinstance(snap["tables"], dict)


def test_snapshot_covers_every_model():
    from app.settings_backup import SNAPSHOT_MODELS, build_snapshot
    snap = build_snapshot()
    assert len(SNAPSHOT_MODELS) == 17
    for model in SNAPSHOT_MODELS:
        assert model.__tablename__ in snap["tables"]


def test_snapshot_excludes_catalog_and_match_memory():
    from app.settings_backup import build_snapshot
    tables = build_snapshot()["tables"]
    for forbidden in ("internal_item", "nomenclature_folder", "supplier_internal_match",
                      "orders", "quotes"):
        assert forbidden not in tables


def test_snapshot_keeps_rows_with_their_ids():
    _add_equiv()
    from app.settings_backup import build_snapshot
    rows = build_snapshot()["tables"]["standard_equivalents"]
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["src_canonical"] == "GOST-7798-70"
    assert rows[0]["dst_canonical"] == "DIN-933"


def test_dates_are_serialized_as_text():
    from app.database import get_db_session
    from app.settings_backup import build_snapshot, dump_snapshot
    session = get_db_session()
    session.add(ReadinessRule(name="Проверка", item_type="болт", priority=1, is_active=True))
    session.commit()
    session.close()

    snap = build_snapshot()
    # Must survive json.dumps — a raw datetime would raise here.
    text = dump_snapshot(snap)
    assert json.loads(text)["tables"]["readiness_rule"][0]["name"] == "Проверка"


def test_dump_is_readable_utf8():
    _add_equiv()
    from app.settings_backup import build_snapshot, dump_snapshot
    text = dump_snapshot(build_snapshot())
    assert "GOST-7798-70" in text
    assert "\\u0413" not in text  # Cyrillic must not be escaped
```

- [ ] **Шаг 2: Убедиться, что тест падает**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_settings_backup.py -q`
Ожидаемо: `ModuleNotFoundError: No module named 'app.settings_backup'`.

- [ ] **Шаг 3: Реализовать**

Создать `app/settings_backup.py`:

```python
"""Snapshot of the user-editable reference data: rules, directories, settings.

The catalog comes from 1C and is restored by syncing again; the match memory is
keyed by local item ids that differ on another server.  Neither belongs in a
snapshot — see docs/specs/2026-09-12-vygruzka-nastroek.md.
"""
from __future__ import annotations

import json
import socket
import subprocess
from datetime import datetime
from pathlib import Path

from app.database import get_db_session
from app.models import (
    BaseValidationRule,
    CoatingRule,
    InferenceRule,
    MasterItem,
    MasterItemMember,
    NameTemplate,
    NormalizationRule,
    ProductType,
    ReadinessRule,
    SizeRule,
    StandardEquivalent,
    StandardRef,
    StrengthRule,
    SystemSetting,
    TailPhrase,
    ValidationRule,
    ValidationRuleException,
)

FORMAT_VERSION = 1

# Parents before children: restore inserts in this order and deletes in the
# reverse one, so a row never points at something that is not there yet.
SNAPSHOT_MODELS = [
    StandardEquivalent,
    StandardRef,
    ProductType,
    TailPhrase,
    ReadinessRule,
    BaseValidationRule,
    ValidationRule,
    ValidationRuleException,
    CoatingRule,
    StrengthRule,
    SizeRule,
    NormalizationRule,
    InferenceRule,
    NameTemplate,
    SystemSetting,
    MasterItem,
    MasterItemMember,
]


class SnapshotError(Exception):
    """The file cannot be used as a settings snapshot."""


def _row_to_dict(row) -> dict:
    out: dict = {}
    for col in row.__table__.columns:
        value = getattr(row, col.name)
        if isinstance(value, datetime):
            value = value.isoformat()
        out[col.name] = value
    return out


def _current_commit() -> str:
    """Short commit of the running code, or "" when git is not available."""
    try:
        done = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5,
            cwd=str(Path(__file__).resolve().parent.parent),
        )
        return done.stdout.strip() if done.returncode == 0 else ""
    except Exception:
        return ""


def build_snapshot() -> dict:
    """Collect every snapshot table into a plain dict ready for JSON."""
    session = get_db_session()
    try:
        tables = {
            model.__tablename__: [_row_to_dict(r) for r in session.query(model).all()]
            for model in SNAPSHOT_MODELS
        }
    finally:
        session.close()

    return {
        "format_version": FORMAT_VERSION,
        "exported_at": datetime.now().astimezone().isoformat(),
        "app_commit": _current_commit(),
        "source_host": socket.gethostname(),
        "tables": tables,
    }


def dump_snapshot(snapshot: dict) -> str:
    """Render a snapshot as the text that goes into the file."""
    return json.dumps(snapshot, ensure_ascii=False, indent=2)
```

- [ ] **Шаг 4: Прогнать тесты**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_settings_backup.py -q`
Ожидаемо: `6 passed`.

- [ ] **Шаг 5: Коммит**

```bash
git add app/settings_backup.py tests/test_settings_backup.py
git commit -m "feat(settings): сбор снимка справочников, правил и настроек"
```

---

## Задача 2. Восстановление и сброс кэшей

**Файлы:**
- Изменить: `app/settings_backup.py` — дописать в конец
- Изменить: `tests/test_settings_backup.py` — дописать в конец

**Интерфейсы:**
- Потребляет: `SNAPSHOT_MODELS`, `FORMAT_VERSION`, `SnapshotError`, `build_snapshot()`.
- Отдаёт: `restore_snapshot(payload: dict, backup_dir: Path | None = None) -> dict`
  — возвращает `{"tables": {имя: {"before": int, "after": int}}, "backup_path": str | None}`;
  `invalidate_all_caches() -> None`.

- [ ] **Шаг 1: Написать падающий тест**

Дописать в конец `tests/test_settings_backup.py`:

```python
def test_restore_fills_empty_database():
    _add_equiv()
    from app.settings_backup import build_snapshot, restore_snapshot
    snap = build_snapshot()

    from app.database import get_db_session
    session = get_db_session()
    session.query(StandardEquivalent).delete()
    session.commit()
    assert session.query(StandardEquivalent).count() == 0
    session.close()

    report = restore_snapshot(snap)

    session = get_db_session()
    assert session.query(StandardEquivalent).count() == 1
    session.close()
    assert report["tables"]["standard_equivalents"]["after"] == 1


def test_restore_replaces_existing_rows():
    _add_equiv("GOST-5927-70", "DIN-934")
    from app.settings_backup import build_snapshot, restore_snapshot
    snap = build_snapshot()

    _add_equiv("GOST-7798-70", "DIN-933")  # local addition, must not survive

    restore_snapshot(snap)

    from app.database import get_db_session
    session = get_db_session()
    rows = session.query(StandardEquivalent).all()
    pairs = {(r.src_canonical, r.dst_canonical) for r in rows}
    session.close()
    assert pairs == {("GOST-5927-70", "DIN-934")}


def test_absent_table_is_left_alone():
    _add_equiv()
    from app.settings_backup import restore_snapshot
    payload = {"format_version": 1, "tables": {"product_type": []}}

    restore_snapshot(payload)

    from app.database import get_db_session
    session = get_db_session()
    assert session.query(StandardEquivalent).count() == 1
    session.close()


def test_empty_list_clears_the_table():
    _add_equiv()
    from app.settings_backup import restore_snapshot
    restore_snapshot({"format_version": 1, "tables": {"standard_equivalents": []}})

    from app.database import get_db_session
    session = get_db_session()
    assert session.query(StandardEquivalent).count() == 0
    session.close()


def test_unknown_format_version_is_refused():
    _add_equiv()
    from app.settings_backup import SnapshotError, restore_snapshot
    with pytest.raises(SnapshotError):
        restore_snapshot({"format_version": 999, "tables": {"standard_equivalents": []}})

    from app.database import get_db_session
    session = get_db_session()
    assert session.query(StandardEquivalent).count() == 1  # untouched
    session.close()


def test_payload_without_tables_is_refused():
    from app.settings_backup import SnapshotError, restore_snapshot
    with pytest.raises(SnapshotError):
        restore_snapshot({"format_version": 1})


def test_restore_reports_before_and_after():
    _add_equiv("GOST-5927-70", "DIN-934")
    from app.settings_backup import build_snapshot, restore_snapshot
    snap = build_snapshot()
    _add_equiv("GOST-7798-70", "DIN-933")

    report = restore_snapshot(snap)
    assert report["tables"]["standard_equivalents"] == {"before": 2, "after": 1}


def test_restore_leaves_the_catalog_alone():
    """The catalog comes from 1C — a settings snapshot must not touch it."""
    from datetime import datetime, timezone
    from app.database import get_db_session
    from app.models import InternalItem

    session = get_db_session()
    session.add(InternalItem(
        name="Болт М12x60 DIN 933", item_type="болт", size="M12x60",
        standard_text="DIN 933", standard_key="DIN-933", is_active=True,
        created_at=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc),
    ))
    session.commit()
    session.close()

    from app.settings_backup import restore_snapshot
    restore_snapshot({"format_version": 1, "tables": {"standard_equivalents": []}})

    session = get_db_session()
    assert session.query(InternalItem).count() == 1
    session.close()


def test_restore_drops_the_analog_cache():
    """A stale cache would keep serving the pairs that were just replaced."""
    from app.matching.standard_analogs import get_standard_analogs
    _add_equiv("GOST-7798-70", "DIN-933")
    from app.settings_backup import build_snapshot, restore_snapshot
    snap = build_snapshot()

    assert get_standard_analogs("GOST-7798-70") == ["DIN-933"]  # warms the cache

    snap["tables"]["standard_equivalents"] = [
        {"id": 1, "src_canonical": "GOST-5927-70", "dst_canonical": "DIN-934",
         "confidence": 100, "is_active": True}
    ]
    restore_snapshot(snap)

    assert get_standard_analogs("GOST-7798-70") == []
    assert get_standard_analogs("GOST-5927-70") == ["DIN-934"]
```

- [ ] **Шаг 2: Убедиться, что тест падает**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_settings_backup.py -q -k restore`
Ожидаемо: `ImportError: cannot import name 'restore_snapshot'`.

- [ ] **Шаг 3: Реализовать восстановление**

Дописать в конец `app/settings_backup.py`:

```python
def _dict_to_kwargs(model, data: dict) -> dict:
    """Turn one exported row back into constructor arguments for *model*."""
    from sqlalchemy import DateTime  # noqa: PLC0415

    kwargs: dict = {}
    for col in model.__table__.columns:
        if col.name not in data:
            continue
        value = data[col.name]
        if isinstance(value, str) and isinstance(col.type, DateTime):
            value = datetime.fromisoformat(value)
        kwargs[col.name] = value
    return kwargs


def restore_snapshot(payload: dict, backup_dir: Path | None = None) -> dict:
    """Replace the snapshot tables with the contents of *payload*.

    A table absent from the payload is left alone: an older snapshot must not
    wipe data it never knew about.  A table present as an empty list is
    emptied — that is a deliberate "nothing here".

    Everything happens in one transaction: a failure half-way leaves the
    database exactly as it was.
    """
    if not isinstance(payload, dict):
        raise SnapshotError("Файл не является снимком настроек.")

    version = payload.get("format_version")
    if version != FORMAT_VERSION:
        raise SnapshotError(
            f"Формат файла ({version}) не поддерживается, нужен {FORMAT_VERSION}."
        )

    tables = payload.get("tables")
    if not isinstance(tables, dict):
        raise SnapshotError("В файле нет раздела tables.")

    backup_path = write_backup_file(backup_dir) if backup_dir is not None else None

    session = get_db_session()
    report: dict = {}
    try:
        for model in SNAPSHOT_MODELS:
            report[model.__tablename__] = {
                "before": session.query(model).count(),
                "after": 0,
            }

        for model in reversed(SNAPSHOT_MODELS):
            if model.__tablename__ in tables:
                session.query(model).delete()

        for model in SNAPSHOT_MODELS:
            name = model.__tablename__
            rows = tables.get(name)
            if rows is None:
                report[name]["after"] = report[name]["before"]
                continue
            for row in rows:
                session.add(model(**_dict_to_kwargs(model, row)))
            report[name]["after"] = len(rows)

        session.commit()
    except SnapshotError:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise SnapshotError(f"Загрузка не удалась: {exc}") from exc
    finally:
        session.close()

    invalidate_all_caches()

    return {
        "tables": report,
        "backup_path": str(backup_path) if backup_path else None,
    }
```

- [ ] **Шаг 4: Реализовать сброс кэшей**

Дописать в конец `app/settings_backup.py`:

```python
def invalidate_all_caches() -> None:
    """Drop every in-process cache that holds snapshot data.

    Without this the file lands in the database and the service keeps serving
    the previous rules until each TTL runs out.
    """
    from app.category_validator import invalidate_category_validator_cache  # noqa: PLC0415
    from app.inference_engine import invalidate_inference_cache  # noqa: PLC0415
    from app.match_settings import invalidate_settings_cache  # noqa: PLC0415
    from app.matcher import (  # noqa: PLC0415
        invalidate_master_guid_cache,
        invalidate_match_memory_cache,
    )
    from app.matching.standard_analogs import invalidate_standard_analogs_cache  # noqa: PLC0415
    from app.parsing.tail_extractor import invalidate_tail_phrases_cache  # noqa: PLC0415
    from app.product_type_matcher import invalidate_product_types_cache  # noqa: PLC0415
    from app.readiness import invalidate_readiness_caches  # noqa: PLC0415
    from app.services.coating_detector import invalidate_coating_cache  # noqa: PLC0415
    from app.services.normalization_service import invalidate_normalization_cache  # noqa: PLC0415
    from app.services.size_detector import invalidate_size_cache  # noqa: PLC0415
    from app.services.strength_detector import invalidate_strength_cache  # noqa: PLC0415

    for drop in (
        invalidate_standard_analogs_cache,
        invalidate_readiness_caches,
        invalidate_category_validator_cache,
        invalidate_inference_cache,
        invalidate_settings_cache,
        invalidate_product_types_cache,
        invalidate_tail_phrases_cache,
        invalidate_coating_cache,
        invalidate_strength_cache,
        invalidate_size_cache,
        invalidate_normalization_cache,
        invalidate_match_memory_cache,
        invalidate_master_guid_cache,
    ):
        # One cache module refusing to load must not leave the rest stale.
        try:
            drop()
        except Exception:
            pass
```

- [ ] **Шаг 5: Добавить заглушку автобэкапа**

`restore_snapshot()` зовёт `write_backup_file()`, которая появится в задаче 3.
Чтобы задача 2 была самостоятельной, добавить её сейчас в минимальном виде —
в задаче 3 она обрастёт тестами:

```python
def write_backup_file(directory: Path) -> Path:
    """Save the current state as a snapshot file and return its path."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"settings-{datetime.now():%Y-%m-%d-%H%M%S}.json"
    path.write_text(dump_snapshot(build_snapshot()), encoding="utf-8")
    return path
```

- [ ] **Шаг 6: Прогнать тесты**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_settings_backup.py -q`
Ожидаемо: `15 passed`.

- [ ] **Шаг 7: Коммит**

```bash
git add app/settings_backup.py tests/test_settings_backup.py
git commit -m "feat(settings): загрузка снимка с полной заменой и сбросом кэшей"
```

---

## Задача 3. Автобэкап перед заливкой

**Файлы:**
- Изменить: `tests/test_settings_backup.py` — дописать в конец

**Интерфейсы:**
- Потребляет: `write_backup_file(directory: Path) -> Path`, `restore_snapshot(payload, backup_dir)`.

- [ ] **Шаг 1: Написать падающий тест**

Дописать в конец `tests/test_settings_backup.py`:

```python
def test_backup_file_is_written_before_replacing(tmp_path):
    _add_equiv("GOST-7798-70", "DIN-933")
    from app.settings_backup import restore_snapshot

    backups = tmp_path / "backups"
    report = restore_snapshot(
        {"format_version": 1, "tables": {"standard_equivalents": []}},
        backup_dir=backups,
    )

    saved = list(backups.glob("settings-*.json"))
    assert len(saved) == 1
    assert report["backup_path"] == str(saved[0])

    # The backup holds what the database looked like BEFORE the replace.
    rescued = json.loads(saved[0].read_text(encoding="utf-8"))
    rows = rescued["tables"]["standard_equivalents"]
    assert [r["src_canonical"] for r in rows] == ["GOST-7798-70"]


def test_backup_can_be_restored_back(tmp_path):
    _add_equiv("GOST-7798-70", "DIN-933")
    from app.settings_backup import restore_snapshot

    backups = tmp_path / "backups"
    restore_snapshot(
        {"format_version": 1, "tables": {"standard_equivalents": []}},
        backup_dir=backups,
    )
    saved = list(backups.glob("settings-*.json"))[0]

    restore_snapshot(json.loads(saved.read_text(encoding="utf-8")))

    from app.database import get_db_session
    session = get_db_session()
    assert session.query(StandardEquivalent).count() == 1
    session.close()


def test_no_backup_dir_means_no_backup():
    _add_equiv()
    from app.settings_backup import restore_snapshot
    report = restore_snapshot({"format_version": 1, "tables": {}})
    assert report["backup_path"] is None
```

- [ ] **Шаг 2: Прогнать тесты**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_settings_backup.py -q`
Ожидаемо: `18 passed`. Функция `write_backup_file()` уже написана в задаче 2 —
эти тесты закрепляют её поведение и порядок «сначала бэкап, потом замена».

Если тест `test_backup_file_is_written_before_replacing` падает с пустым списком
пар внутри бэкапа — значит `write_backup_file()` вызывается после очистки
таблиц. Перенести вызов выше, до открытия сессии замены.

- [ ] **Шаг 3: Коммит**

```bash
git add tests/test_settings_backup.py
git commit -m "test(settings): автобэкап снимается до замены и читается обратно"
```

---

## Задача 4. Страница, выгрузка и загрузка

**Файлы:**
- Создать: `app/settings_backup_routes.py`
- Создать: `app/templates/settings_backup.html`
- Создать: `tests/test_settings_backup_routes.py`
- Изменить: `app/main.py` — импорт роутера рядом со строкой 1082, регистрация рядом со строкой 1109

**Интерфейсы:**
- Потребляет: `build_snapshot()`, `dump_snapshot()`, `restore_snapshot()`, `SnapshotError`, `SNAPSHOT_MODELS`.
- Отдаёт: маршруты `GET /settings/backup`, `GET /settings/backup/export`, `POST /settings/backup/import`.

- [ ] **Шаг 1: Написать падающий тест**

Создать `tests/test_settings_backup_routes.py`:

```python
"""Routes of the settings snapshot page."""
import json

import pytest
from fastapi.testclient import TestClient

from app.models import StandardEquivalent


@pytest.fixture(autouse=True)
def _set_dirs(tmp_path, monkeypatch):
    upload_dir = tmp_path / "uploads"
    cache_dir = tmp_path / "cache"
    monkeypatch.setenv("OTDELZAKUP_UPLOAD_DIR", str(upload_dir))
    monkeypatch.setenv("OTDELZAKUP_CACHE_DIR", str(cache_dir))
    import app.cache as cache_mod
    cache_mod.UPLOAD_DIR = upload_dir
    cache_mod.CACHE_DIR = cache_dir

    db_path = tmp_path / "test.db"
    monkeypatch.setenv("OTDELZAKUP_DB_PATH", str(db_path))
    import app.database as db_mod
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    db_mod.DB_PATH = db_path
    db_mod.engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    db_mod.SessionLocal = sessionmaker(bind=db_mod.engine, autoflush=False, expire_on_commit=False)
    db_mod.init_db()


@pytest.fixture
def client():
    from fastapi import FastAPI
    from app.settings_backup_routes import settings_backup_router
    app = FastAPI()
    app.include_router(settings_backup_router)
    return TestClient(app)


def _add_equiv(src="GOST-7798-70", dst="DIN-933"):
    from app.database import get_db_session
    session = get_db_session()
    session.add(StandardEquivalent(src_canonical=src, dst_canonical=dst, is_active=True))
    session.commit()
    session.close()


def test_page_opens(client):
    response = client.get("/settings/backup")
    assert response.status_code == 200
    assert "Выгрузить" in response.text
    assert "Загрузить" in response.text


def test_export_returns_a_named_file(client):
    _add_equiv()
    response = client.get("/settings/backup/export")
    assert response.status_code == 200
    assert "attachment" in response.headers["content-disposition"]
    assert "otdelzakup-settings-" in response.headers["content-disposition"]
    payload = json.loads(response.text)
    assert payload["tables"]["standard_equivalents"][0]["src_canonical"] == "GOST-7798-70"


def test_import_replaces_data(client):
    _add_equiv("GOST-5927-70", "DIN-934")
    snapshot = json.loads(client.get("/settings/backup/export").text)

    _add_equiv("GOST-7798-70", "DIN-933")

    files = {"file": ("snap.json", json.dumps(snapshot).encode("utf-8"), "application/json")}
    response = client.post("/settings/backup/import", files=files)
    assert response.status_code == 200

    from app.database import get_db_session
    session = get_db_session()
    pairs = {(r.src_canonical, r.dst_canonical) for r in session.query(StandardEquivalent).all()}
    session.close()
    assert pairs == {("GOST-5927-70", "DIN-934")}


def test_import_of_broken_json_reports_an_error(client):
    _add_equiv()
    files = {"file": ("snap.json", b"{not json", "application/json")}
    response = client.post("/settings/backup/import", files=files)
    assert response.status_code == 200
    assert "не читается" in response.text

    from app.database import get_db_session
    session = get_db_session()
    assert session.query(StandardEquivalent).count() == 1
    session.close()


def test_import_of_future_format_reports_an_error(client):
    _add_equiv()
    payload = {"format_version": 999, "tables": {}}
    files = {"file": ("snap.json", json.dumps(payload).encode("utf-8"), "application/json")}
    response = client.post("/settings/backup/import", files=files)
    assert response.status_code == 200
    assert "не поддерживается" in response.text
```

- [ ] **Шаг 2: Убедиться, что тест падает**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_settings_backup_routes.py -q`
Ожидаемо: `ModuleNotFoundError: No module named 'app.settings_backup_routes'`.

- [ ] **Шаг 3: Реализовать маршруты**

Создать `app/settings_backup_routes.py`:

```python
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
```

- [ ] **Шаг 4: Создать шаблон**

Создать `app/templates/settings_backup.html`:

```html
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Выгрузка и загрузка настроек</title>
    <style>
        body { font-family: sans-serif; margin: 40px auto; padding: 0 20px; max-width: 960px; }
        h1 { margin-bottom: 8px; }
        a { color: #1a73e8; }
        .nav { margin-bottom: 20px; font-size: 14px; }
        .desc { color: #555; margin-bottom: 24px; font-size: 14px; max-width: 720px; line-height: 1.5; }
        .card { background: #f8f9fa; border: 1px solid #ddd; border-radius: 6px; padding: 16px 20px; margin-bottom: 24px; }
        .card h3 { margin: 0 0 12px; font-size: 15px; }
        .btn { display: inline-block; padding: 8px 16px; border: none; border-radius: 4px; font-size: 14px; cursor: pointer; text-decoration: none; }
        .btn-primary { background: #1a73e8; color: #fff; }
        .btn-danger { background: #e53935; color: #fff; }
        .error { background: #ffebee; border: 1px solid #ef9a9a; border-radius: 4px; padding: 10px 14px; margin-bottom: 16px; color: #c62828; font-size: 14px; }
        .ok { background: #e8f5e9; border: 1px solid #a5d6a7; border-radius: 4px; padding: 10px 14px; margin-bottom: 16px; color: #2e7d32; font-size: 14px; }
        .warn { background: #fff8e1; border: 1px solid #ffe082; border-radius: 4px; padding: 10px 14px; margin-bottom: 16px; color: #8d6e00; font-size: 14px; }
        table { border-collapse: collapse; width: 100%; font-size: 14px; }
        th, td { border: 1px solid #ddd; padding: 7px 10px; text-align: left; }
        th { background: #f5f5f5; }
        td.num { text-align: right; width: 90px; }
        .changed { font-weight: 600; }
        #file-info { margin-top: 10px; font-size: 13px; color: #555; }
    </style>
</head>
<body>
    <h1>Выгрузка и загрузка настроек</h1>
    <p class="nav"><a href="/">&larr; Главная</a> &middot; <a href="/settings/match">Настройки подбора</a></p>

    <p class="desc">
        Снимок содержит справочники, правила и настройки — всё, что ведут закупщики.
        Каталог номенклатуры в снимок не входит: он приезжает из 1С синхронизацией.
        Память подбора тоже не переносится — она привязана к позициям конкретного сервера.
    </p>

    {% if error %}<div class="error">{{ error }}</div>{% endif %}

    {% if report %}
        <div class="ok">Загрузка выполнена.
        {% if report.backup_path %}Предыдущее состояние сохранено: <code>{{ report.backup_path }}</code>{% endif %}
        </div>
        <table>
            <tr><th>Что</th><th class="num">Было</th><th class="num">Стало</th></tr>
            {% for name, row in report.tables.items() %}
            <tr class="{% if row.before != row.after %}changed{% endif %}">
                <td>{{ titles.get(name, name) }}</td>
                <td class="num">{{ row.before }}</td>
                <td class="num">{{ row.after }}</td>
            </tr>
            {% endfor %}
        </table>
    {% else %}
        <div class="card">
            <h3>Выгрузить</h3>
            <p class="desc">Скачивает файл со всеми настройками этого сервера.</p>
            <a class="btn btn-primary" href="/settings/backup/export">Выгрузить настройки</a>
        </div>

        <div class="card">
            <h3>Загрузить</h3>
            <div class="warn">
                Загрузка <strong>полностью заменяет</strong> справочники, правила и настройки
                содержимым файла. Всё, что заведено на этом сервере и чего нет в файле, будет
                удалено. Перед заливкой автоматически сохраняется копия текущего состояния.
            </div>
            <form method="post" action="/settings/backup/import" enctype="multipart/form-data">
                <input type="file" name="file" accept=".json,application/json" required
                       onchange="showFileInfo(this)">
                <button type="submit" class="btn btn-danger"
                        onclick="return confirm('Заменить настройки содержимым файла?')">Загрузить</button>
                <div id="file-info"></div>
            </form>
        </div>

        <h3>Сейчас в базе</h3>
        <table>
            <tr><th>Что</th><th class="num">Записей</th></tr>
            {% for name, title, count in counts %}
            <tr><td>{{ title }}</td><td class="num">{{ count }}</td></tr>
            {% endfor %}
        </table>
    {% endif %}

    <script>
        function showFileInfo(input) {
            var box = document.getElementById('file-info');
            box.textContent = '';
            if (!input.files || !input.files[0]) { return; }
            var reader = new FileReader();
            reader.onload = function (e) {
                try {
                    var data = JSON.parse(e.target.result);
                    box.textContent = 'Снимок с ' + (data.source_host || 'неизвестного сервера') +
                        ', снят ' + (data.exported_at || 'без даты') +
                        ', версия формата ' + data.format_version;
                } catch (err) {
                    box.textContent = 'Файл не читается как JSON.';
                }
            };
            reader.readAsText(input.files[0]);
        }
    </script>
</body>
</html>
```

- [ ] **Шаг 5: Зарегистрировать роутер**

В `app/main.py`, рядом с импортом на строке 1082:

```python
from app.settings_backup_routes import settings_backup_router  # noqa: E402
```

И рядом с регистрацией на строке 1109:

```python
app.include_router(settings_backup_router)
```

- [ ] **Шаг 6: Прогнать тесты**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_settings_backup_routes.py -q`
Ожидаемо: `5 passed`.

- [ ] **Шаг 7: Коммит**

```bash
git add app/settings_backup_routes.py app/templates/settings_backup.html app/main.py tests/test_settings_backup_routes.py
git commit -m "feat(settings): страница выгрузки и загрузки настроек"
```

---

## Задача 5. Консольная выгрузка

**Файлы:**
- Создать: `scripts/export_settings.py`

**Интерфейсы:**
- Потребляет: `build_snapshot()`, `dump_snapshot()`.

- [ ] **Шаг 1: Написать скрипт**

Создать `scripts/export_settings.py`:

```python
"""Save a settings snapshot to a file.

Usage:
    python scripts/export_settings.py D:\\OtdelZakup\\backups\\settings.json

Meant for the Windows task scheduler: the file it writes is the same one the
"Выгрузить настройки" button produces.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.settings_backup import build_snapshot, dump_snapshot  # noqa: E402


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python scripts/export_settings.py <path-to-file.json>")
        return 2

    path = Path(sys.argv[1])
    path.parent.mkdir(parents=True, exist_ok=True)
    snapshot = build_snapshot()
    path.write_text(dump_snapshot(snapshot), encoding="utf-8")

    rows = sum(len(v) for v in snapshot["tables"].values())
    print(f"Снимок сохранён: {path} ({rows} записей)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Шаг 2: Проверить вручную**

Запустить: `./venv/Scripts/python.exe scripts/export_settings.py "$TMP/snap.json"`
Ожидаемо: строка `Снимок сохранён: ... (N записей)`, файл на месте.

Запустить без аргумента: `./venv/Scripts/python.exe scripts/export_settings.py`
Ожидаемо: строка `Usage: ...`, код возврата 2.

- [ ] **Шаг 3: Коммит**

```bash
git add scripts/export_settings.py
git commit -m "tools: консольная выгрузка настроек для планировщика"
```

---

## Задача 6. Ссылки и документация

**Файлы:**
- Изменить: `app/templates/match_settings.html:43`
- Изменить: `DEPLOY.md` — раздел «Типовой флоу обновления»

- [ ] **Шаг 1: Ссылка со страницы настроек подбора**

В `app/templates/match_settings.html`, рядом со строкой 43, где уже стоит ссылка
на аналоги, добавить вторую:

```html
        <a href="/settings/backup">Выгрузка и загрузка настроек</a>
```

- [ ] **Шаг 2: Шаг выгрузки в флоу обновления**

В `DEPLOY.md`, в разделе «Типовой флоу обновления», перед `git pull` добавить:

```powershell
# Снимок настроек на случай отката — справочники и правила живут только в БД
.\venv\Scripts\python.exe scripts\export_settings.py "D:\OtdelZakup\backups\settings-before-update.json"
```

И абзац после блока:

```markdown
Справочники, правила и настройки хранятся в `readiness.db` и с кодом не едут.
Перенести их на другой сервер или вернуть после переустановки — через страницу
`/settings/backup`: «Выгрузить настройки» на источнике, «Загрузить» на приёмнике.
Загрузка полностью заменяет справочники содержимым файла.
```

- [ ] **Шаг 3: Полный прогон**

Запустить: `./venv/Scripts/python.exe -m pytest -q`
Ожидаемо: падений не больше, чем было до начала работы. На `main` на 2026-09-12
стабильно падали 6 тестов, не связанных с этой задачей:
`test_minhash_cache_disk.py::TestSaveSwallowsErrors::test_unpicklable_state_does_not_raise`,
`test_order_module.py::TestDBConstraints::test_cascade_delete_order`,
`test_row_parser.py::test_rowparser_no_defaults_if_missing_uom`,
`test_standard_match.py` (два теста), `test_standards.py::test_standard_mismatch_sets_review`.
Любое седьмое падение — регресс, разбирать до конца.

- [ ] **Шаг 4: Коммит**

```bash
git add app/templates/match_settings.html DEPLOY.md
git commit -m "docs: выгрузка настроек в флоу обновления и ссылка со страницы настроек"
```

---

## Что проверить на проде после выката

1. Открыть `/settings/backup` — таблица «Сейчас в базе» показывает реальные числа.
2. Нажать «Выгрузить настройки», открыть файл, убедиться, что в нём есть правила
   и стандарты.
3. Завести пары аналогов, выгрузить снова, сравнить.
4. Проверить, что `data/backups/` наполняется при каждой загрузке.
