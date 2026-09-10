# Поиск по DIN — план реализации

> **Для агентов:** реализовывать по задачам, каждая задача — отдельный цикл
> «тест → провал → код → зелено → коммит». Шаги отмечены чекбоксами.

**Спецификация:** `docs/specs/2026-09-10-poisk-po-din.md` — читать до начала работы.

**Цель:** добавить четвёртый режим подбора `din`, при котором позиции по не-DIN
стандартам ищутся во внутреннем каталоге через свой DIN-аналог, а позиции по DIN
остаются на своём стандарте.

**Архитектура:** режим включается флагом `din_only` в `MatchSettings`. Для каждой
строки функция `din_targets_for_row()` возвращает список целевых DIN-ключей;
пустой список означает «вести строку как в режиме `off`». Целевые ключи
вычисляются **внутри** трёх функций подбора, которые уже получают `row_dict` и
`settings`, — поэтому места вызова этих функций (их семь) не меняются вовсе.

**Стек:** Python 3, FastAPI, SQLAlchemy, pandas, datasketch (MinHash), Jinja2, pytest.

## Общие требования ко всем задачам

- Прогон тестов: `./venv/Scripts/python.exe -m pytest <путь> -q` из корня проекта.
- Полный прогон долгий (десятки секунд на файл) — в шагах указан точный файл.
- Комментарии в коде и docstring — на английском, как во всём проекте.
- Текст в UI и сообщения пользователю — на русском.
- Значения режима: `off`, `with`, `only`, `din`. Неизвестное значение трактуется
  как «не передан», ошибку не возвращаем.
- При `din_only=False` все ветки обязаны исполняться ровно как сегодня.
- Изолирующая фикстура тестов копируется из `tests/test_use_analogs.py:30-49`
  без изменений — она подменяет пути и БД на временные.

## Отступление от спецификации

Разделы 4.4 и 4.5 спецификации описывали передачу `din_targets` параметром из
мест вызова. В плане целевые DIN вычисляются внутри `_build_exact_candidates()`
и `post_filter_candidates()` из `settings` и `row_dict`, которые туда уже
приходят. Причина: у `_build_exact_candidates()` четыре места вызова, у
`post_filter_candidates()` — три, и правка каждого — лишний шанс пропустить одно
и получить режим, работающий на одном экране и молчащий на другом.

## Структура файлов

| Файл | Что делает |
|---|---|
| `app/matching/standard_analogs.py` | Изменяется: поиск аналогов без учёта года, `din_targets_for_row()`, фильтр в `build_analog_queries()` |
| `app/match_settings.py` | Изменяется: поле `din_only` |
| `app/matcher.py` | Изменяется: `_query_minhash()`, `_build_exact_candidates()` |
| `app/matching/post_filter.py` | Изменяется: `compute_candidate_badges()`, `post_filter_candidates()` |
| `app/main.py` | Изменяется: `analog_mode` принимает `din` |
| `app/api.py` | Изменяется: параметр `analog_mode` в трёх маршрутах |
| `app/templates/view_raw.html` и ещё три визарда | Изменяются: пункт списка «Поиск по DIN» |
| `tests/test_standard_analogs_year.py` | Создаётся: аналоги у стандартов без года |
| `tests/test_din_targets.py` | Создаётся: `din_targets_for_row()` и фильтр запросов |
| `tests/test_din_search.py` | Создаётся: сквозной подбор в режиме `din` |
| `tests/test_din_api.py` | Создаётся: параметр `analog_mode` в API |

---

## Задача 1. Аналоги находятся у стандартов без года

Строка «ГОСТ 7798» нормализуется в `GOST-7798`, в справочнике лежит
`GOST-7798-70` — сейчас аналог не находится. Правка нужна раньше остальных:
без неё режим `din` будет молча пропускать половину строк.

**Файлы:**
- Создать: `tests/test_standard_analogs_year.py`
- Изменить: `app/matching/standard_analogs.py` — `_load_analogs_from_db()` (строки 20-31), `get_standard_analogs()` (строки 159-170)

**Интерфейсы:**
- Отдаёт: `get_standard_analogs(key)` возвращает аналоги и когда `key` записан без года издания.

- [ ] **Шаг 1: Написать падающий тест**

Создать `tests/test_standard_analogs_year.py`:

```python
"""Analog lookup must ignore the edition year of a standard key."""
import pytest

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


def _seed(src, dst):
    from app.database import get_db_session
    from app.matching.standard_analogs import invalidate_standard_analogs_cache
    session = get_db_session()
    session.add(StandardEquivalent(src_canonical=src, dst_canonical=dst, is_active=True))
    session.commit()
    session.close()
    invalidate_standard_analogs_cache()


def test_exact_key_still_works():
    _seed("GOST-7798-70", "DIN-933")
    from app.matching.standard_analogs import get_standard_analogs
    assert get_standard_analogs("GOST-7798-70") == ["DIN-933"]


def test_key_without_year_finds_analog():
    _seed("GOST-7798-70", "DIN-933")
    from app.matching.standard_analogs import get_standard_analogs
    assert get_standard_analogs("GOST-7798") == ["DIN-933"]


def test_reverse_direction_without_year():
    _seed("GOST-7798-70", "DIN-933")
    from app.matching.standard_analogs import get_standard_analogs
    assert get_standard_analogs("DIN-933") == ["GOST-7798-70"]


def test_unknown_key_returns_empty():
    _seed("GOST-7798-70", "DIN-933")
    from app.matching.standard_analogs import get_standard_analogs
    assert get_standard_analogs("GOST-9999") == []


def test_result_is_a_copy():
    """Caller must not be able to corrupt the shared cache."""
    _seed("GOST-7798-70", "DIN-933")
    from app.matching.standard_analogs import get_standard_analogs
    got = get_standard_analogs("GOST-7798-70")
    got.append("JUNK")
    assert get_standard_analogs("GOST-7798-70") == ["DIN-933"]
```

- [ ] **Шаг 2: Убедиться, что тест падает**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_standard_analogs_year.py -q`

Ожидаемо: `test_key_without_year_finds_analog` падает — `assert [] == ['DIN-933']`.
Остальные проходят.

- [ ] **Шаг 3: Реализовать**

В `app/matching/standard_analogs.py` заменить тело `_load_analogs_from_db()`:

```python
def _load_analogs_from_db() -> dict[str, list[str]]:
    from app.database import get_db_session
    from app.models import StandardEquivalent
    session = get_db_session()
    try:
        rows = session.query(StandardEquivalent).filter_by(is_active=True).all()
        result: dict[str, list[str]] = {}

        def _add(key: str, value: str) -> None:
            # Index under both the written key and its year-stripped form, so a
            # row typed as "ГОСТ 7798" still finds the pair stored as
            # "GOST-7798-70".  Which edition was written down does not change
            # which standard is meant.
            for k in {key, strip_edition_year(key)}:
                if not k:
                    continue
                bucket = result.setdefault(k, [])
                if value not in bucket:
                    bucket.append(value)

        for row in rows:
            _add(row.src_canonical, row.dst_canonical)
            _add(row.dst_canonical, row.src_canonical)
        return result
    finally:
        session.close()
```

`strip_edition_year()` объявлена ниже по файлу — на момент вызова функции она
уже определена, переносить ничего не нужно.

Заменить тело `get_standard_analogs()`:

```python
def get_standard_analogs(standard_norm: str, max_depth: int = 1) -> list[str]:
    """Return list of analogue canonical keys for the given canonical standard.

    Uses an in-process cache of the full standard_equivalents table so that
    repeated calls within a request are O(1) dict lookups instead of DB queries.
    Falls back to the year-stripped key, so "GOST-7798" finds the pair stored
    as "GOST-7798-70".
    """
    if not standard_norm:
        return []
    try:
        data = _analogs_cache.get_or_load(_load_analogs_from_db)
    except Exception:
        return []
    hit = data.get(standard_norm)
    if hit is None:
        hit = data.get(strip_edition_year(standard_norm))
    return list(hit or [])
```

- [ ] **Шаг 4: Убедиться, что тесты проходят**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_standard_analogs_year.py -q`
Ожидаемо: `5 passed`.

- [ ] **Шаг 5: Проверить, что старые режимы не сломались**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_standard_analogs.py tests/test_use_analogs.py tests/test_standard_match.py -q`
Ожидаемо: все проходят. Если падает — правка меняет поведение режимов `with`
и `only` сильнее, чем задумано; разобраться до перехода к задаче 2.

- [ ] **Шаг 6: Коммит**

```bash
git add tests/test_standard_analogs_year.py app/matching/standard_analogs.py
git commit -m "fix(matching): аналог стандарта находится и когда год издания не указан"
```

---

## Задача 2. Функция `din_targets_for_row()`

**Файлы:**
- Создать: `tests/test_din_targets.py`
- Изменить: `app/matching/standard_analogs.py` — добавить в конец файла

**Интерфейсы:**
- Потребляет: `get_standard_analogs()`, `normalize_standard()`, `_STD_PATTERNS` из того же модуля.
- Отдаёт: `din_targets_for_row(row_dict: dict) -> list[str]` — список канонических
  DIN-ключей (`["DIN-933"]`). Пустой список означает «искать как в режиме `off`».

- [ ] **Шаг 1: Написать падающий тест**

Создать `tests/test_din_targets.py`:

```python
"""din_targets_for_row: which DIN keys a row must be searched by."""
import pytest

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
def seeded():
    """ГОСТ 7798-70 ↔ DIN 933, DIN 933 ↔ ISO 4017, ISO 4014 ↔ DIN 931."""
    from app.database import get_db_session
    from app.matching.standard_analogs import invalidate_standard_analogs_cache
    session = get_db_session()
    for src, dst in [
        ("GOST-7798-70", "DIN-933"),
        ("DIN-933", "ISO-4017"),
        ("ISO-4014", "DIN-931"),
    ]:
        session.add(StandardEquivalent(src_canonical=src, dst_canonical=dst, is_active=True))
    session.commit()
    session.close()
    invalidate_standard_analogs_cache()


def _row(**kw):
    row = {"name": "", "name_raw": "", "gost": "", "iso": "", "din": ""}
    row.update(kw)
    return row


@pytest.mark.parametrize("row,expected", [
    (_row(din="DIN 933"),                        []),
    (_row(gost="ГОСТ 7798-70"),                  ["DIN-933"]),
    (_row(gost="ГОСТ 7798"),                     ["DIN-933"]),
    (_row(iso="ISO 4017"),                       ["DIN-933"]),
    (_row(gost="ГОСТ Р ИСО 4014"),               ["DIN-931"]),
    (_row(gost="ГОСТ 9999"),                     []),
    (_row(),                                     []),
    (_row(name_raw="Болт М12х60 без стандарта"), []),
    (_row(name_raw="Болт ГОСТ 7798-70 М12х60"),  ["DIN-933"]),
    (_row(name_raw="Болт DIN 933 М12х60"),       []),
])
def test_din_targets(seeded, row, expected):
    from app.matching.standard_analogs import din_targets_for_row
    assert din_targets_for_row(row) == expected


def test_din_wins_over_gost_in_the_same_text(seeded):
    """A row that already names a DIN stays on it, even if a ГОСТ is written too."""
    from app.matching.standard_analogs import din_targets_for_row
    assert din_targets_for_row(_row(name_raw="Болт DIN 933 (аналог ГОСТ 7798-70)")) == []


def test_only_din_analogs_are_returned(seeded):
    """ISO 4017 has both a DIN and a GOST analog; only DIN comes back."""
    from app.database import get_db_session
    from app.matching.standard_analogs import (
        din_targets_for_row,
        invalidate_standard_analogs_cache,
    )
    session = get_db_session()
    session.add(StandardEquivalent(src_canonical="ISO-4017", dst_canonical="GOST-7805-70", is_active=True))
    session.commit()
    session.close()
    invalidate_standard_analogs_cache()
    assert din_targets_for_row(_row(iso="ISO 4017")) == ["DIN-933"]
```

- [ ] **Шаг 2: Убедиться, что тест падает**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_din_targets.py -q`
Ожидаемо: `ImportError: cannot import name 'din_targets_for_row'`.

- [ ] **Шаг 3: Реализовать**

Добавить в конец `app/matching/standard_analogs.py`:

```python
# ── DIN search mode ───────────────────────────────────────────────────────────

# Order matters: a row that already names a DIN stays on that DIN, whatever
# else is written next to it.  Mirrors the field order din → gost → iso used
# elsewhere in the matcher.
_DIN_FIRST_PATTERNS = [_STD_PATTERNS[2], _STD_PATTERNS[0], _STD_PATTERNS[1], _STD_PATTERNS[3]]


def row_standard_canonical(row_dict: dict) -> str | None:
    """The canonical standard key of a row: its columns first, then its text."""
    for key in ("din", "gost", "iso"):
        value = str(row_dict.get(key) or "").strip()
        if value:
            canonical = normalize_standard(value)
            if canonical:
                return canonical

    text = str(row_dict.get("name_raw") or row_dict.get("name") or "").strip()
    if not text:
        return None
    for pattern in _DIN_FIRST_PATTERNS:
        m = pattern.search(text)
        if m:
            canonical = normalize_standard(m.group(0).strip())
            if canonical:
                return canonical
    return None


def din_targets_for_row(row_dict: dict) -> list[str]:
    """DIN keys this row must be searched by in the "Поиск по DIN" mode.

    An empty list means "search this row the ordinary way": either the row has
    no recognizable standard, or it is already a DIN, or the reference book
    holds no DIN counterpart for it.
    """
    canonical = row_standard_canonical(row_dict)
    if not canonical or canonical.startswith("DIN-"):
        return []
    return [a for a in get_standard_analogs(canonical) if a.startswith("DIN-")]
```

- [ ] **Шаг 4: Убедиться, что тесты проходят**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_din_targets.py -q`
Ожидаемо: `12 passed`.

- [ ] **Шаг 5: Коммит**

```bash
git add tests/test_din_targets.py app/matching/standard_analogs.py
git commit -m "feat(matching): din_targets_for_row - целевые DIN-стандарты строки"
```

---

## Задача 3. Фильтр аналогов в `build_analog_queries()`

**Файлы:**
- Изменить: `app/matching/standard_analogs.py` — `build_analog_queries()` (строки 206-256)
- Изменить: `tests/test_din_targets.py` — дописать в конец

**Интерфейсы:**
- Отдаёт: `build_analog_queries(raw_text, row_dict=None, allowed_analogs: set[str] | None = None)`.
  Когда `allowed_analogs` задан, переписывание идёт только на эти ключи.

- [ ] **Шаг 1: Написать падающий тест**

Дописать в конец `tests/test_din_targets.py`:

```python
def test_analog_queries_unfiltered(seeded):
    """Without a filter, ГОСТ 7798-70 is rewritten to its DIN analog."""
    from app.matching.standard_analogs import build_analog_queries
    queries = build_analog_queries("Болт ГОСТ 7798-70 М12х60")
    assert [q.analog_canonical for q in queries] == ["DIN-933"]
    assert queries[0].rewritten_text == "Болт DIN 933 М12х60"


def test_analog_queries_filtered_to_din(seeded):
    """DIN 933 has both an ISO and a GOST analog; the filter keeps neither."""
    from app.matching.standard_analogs import build_analog_queries
    queries = build_analog_queries("Болт DIN 933 М12х60", allowed_analogs={"DIN-931"})
    assert queries == []


def test_analog_queries_filter_keeps_allowed(seeded):
    from app.matching.standard_analogs import build_analog_queries
    queries = build_analog_queries("Болт ГОСТ 7798-70 М12х60", allowed_analogs={"DIN-933"})
    assert [q.analog_canonical for q in queries] == ["DIN-933"]
```

- [ ] **Шаг 2: Убедиться, что тест падает**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_din_targets.py -q -k analog_queries`
Ожидаемо: `TypeError: build_analog_queries() got an unexpected keyword argument 'allowed_analogs'`.

- [ ] **Шаг 3: Реализовать**

В `app/matching/standard_analogs.py` изменить сигнатуру и цикл:

```python
def build_analog_queries(
    raw_text: str,
    row_dict: dict | None = None,
    allowed_analogs: set[str] | None = None,
) -> list[AnalogQuery]:
```

В docstring дописать абзац:

```
    When *allowed_analogs* is given, only those analog keys are used — this is
    how the DIN search mode rewrites a row onto its DIN counterpart and nothing
    else.
```

Внутри цикла по аналогам, первой строкой тела `for analog_key in analogs:`:

```python
        for analog_key in analogs:
            if allowed_analogs is not None and analog_key not in allowed_analogs:
                continue
```

- [ ] **Шаг 4: Убедиться, что тесты проходят**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_din_targets.py -q`
Ожидаемо: `15 passed`.

- [ ] **Шаг 5: Коммит**

```bash
git add tests/test_din_targets.py app/matching/standard_analogs.py
git commit -m "feat(matching): build_analog_queries умеет ограничивать набор аналогов"
```

---

## Задача 4. Флаг `din_only` и генерация запросов MinHash

**Файлы:**
- Изменить: `app/match_settings.py` — блок «Standard analogs» (строки 71-73)
- Изменить: `app/matcher.py` — `_query_minhash()` (строки 163-214)
- Создать: `tests/test_din_search.py`

**Интерфейсы:**
- Потребляет: `din_targets_for_row()`, `build_analog_queries(allowed_analogs=...)`, `canonical_to_display()`.
- Отдаёт: `MatchSettings.din_only: bool`; `_query_minhash()` в режиме `din`
  не делает прямой запрос для строк с целевыми DIN.

- [ ] **Шаг 1: Написать падающий тест**

Создать `tests/test_din_search.py`:

```python
"""End-to-end matching in the "Поиск по DIN" mode."""
import pytest

from app.matcher import add_internal_matches
from app.match_settings import MatchSettings
from app.matching.minhash_index import rebuild_index
from app.models import InternalItem, StandardEquivalent


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


def _settings(**kw):
    defaults = dict(
        enable_minhash=True,
        lsh_threshold=0.05,
        num_perm=64,
        minhash_top_k=20,
        ngram_n=4,
        use_type_buckets=False,
        min_candidates_before_fallback=1,
        auto_apply_enabled=True,
        auto_apply_jaccard_threshold=0.0,
        always_require_confirmation=False,
        use_standard_analogs_in_main_match=False,
        analogs_only=False,
        din_only=False,
        min_display_score=0,
    )
    defaults.update(kw)
    return MatchSettings(**defaults)


def _seed_catalog(items_data):
    from datetime import datetime, timezone
    from app.database import get_db_session
    session = get_db_session()
    for d in items_data:
        session.add(InternalItem(
            name=d["name"],
            item_type=d.get("item_type", "болт"),
            size=d.get("size", "M12x60"),
            standard_text=d.get("standard_text", ""),
            standard_key=d.get("standard_key"),
            is_active=True,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        ))
    session.commit()
    items = session.query(InternalItem).all()
    session.close()
    rebuild_index(items, num_perm=64, threshold=0.05, ngram_n=4, use_type_buckets=False)
    return items


def _seed_equiv(pairs):
    from app.database import get_db_session
    from app.matching.standard_analogs import invalidate_standard_analogs_cache
    session = get_db_session()
    for src, dst in pairs:
        session.add(StandardEquivalent(src_canonical=src, dst_canonical=dst, is_active=True))
    session.commit()
    session.close()
    invalidate_standard_analogs_cache()


def _df(rows):
    import pandas as pd
    return pd.DataFrame(rows)


def _row(name, gost="", din="", iso="", size="M12x60", item_type="болт"):
    return {
        "name": name, "name_raw": name, "item_type": item_type, "size": size,
        "gost": gost, "iso": iso, "din": din,
        "diameter": "", "length": "", "strength": "", "coating": "",
    }


DIN_ITEM = {"name": "Болт М12x60 DIN 933", "standard_text": "DIN 933", "standard_key": "DIN-933"}
GOST_ITEM = {"name": "Болт М12x60 ГОСТ 7798-70", "standard_text": "ГОСТ 7798-70", "standard_key": "GOST-7798-70"}


def test_gost_row_finds_din_item():
    """A ГОСТ row is matched to the DIN item through the analog."""
    _seed_catalog([DIN_ITEM])
    _seed_equiv([("GOST-7798-70", "DIN-933")])

    df = _df([_row("Болт М12x60 ГОСТ 7798-70", gost="ГОСТ 7798-70")])
    _, results = add_internal_matches(df, settings=_settings(din_only=True))

    assert results[0]["internal_item_id"] is not None
    assert "DIN 933" in results[0]["name"]


def test_gost_row_standard_only_in_column():
    """The standard lives in a column, not in the name — the DIN search still works."""
    _seed_catalog([DIN_ITEM])
    _seed_equiv([("GOST-7798-70", "DIN-933")])

    df = _df([_row("Болт М12x60", gost="ГОСТ 7798-70")])
    _, results = add_internal_matches(df, settings=_settings(din_only=True))

    assert results[0]["internal_item_id"] is not None
```

- [ ] **Шаг 2: Убедиться, что тест падает**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_din_search.py -q`
Ожидаемо: `TypeError: MatchSettings.__init__() got an unexpected keyword argument 'din_only'`.

- [ ] **Шаг 3: Добавить флаг в настройки**

В `app/match_settings.py`, в блоке «Standard analogs», после `analogs_only`:

```python
    # ── Standard analogs ──────────────────────────────────────────────────────
    use_standard_analogs_in_main_match: bool = False  # augment MinHash with analog standards
    analogs_only: bool = False  # search ONLY via analog standards (skip direct query)
    din_only: bool = False  # "Поиск по DIN": route non-DIN rows onto their DIN analog
```

Поле намеренно не добавляется в `_SETTING_KEYS`: это переопределение на один
вызов, в БД оно не хранится.

- [ ] **Шаг 4: Реализовать генерацию запросов**

В `app/matcher.py`, в `_query_minhash()`, заменить блок от `raw = []` до конца
аналоговой ветки:

```python
    raw = []
    analogs_only = getattr(settings, "analogs_only", False)

    # ── DIN search mode ───────────────────────────────────────────────────────
    # Targets are the DIN keys this row must be searched by.  Empty means the
    # row is already a DIN, has no standard, or has no DIN counterpart — such a
    # row is searched the ordinary way.
    din_targets: list[str] = []
    if getattr(settings, "din_only", False):
        from app.matching.standard_analogs import din_targets_for_row  # noqa: PLC0415
        din_targets = din_targets_for_row(row_dict)

    skip_direct = analogs_only or bool(din_targets)

    # ── Direct (non-analog) query ─────────────────────────────────────────────
    if not skip_direct:
        mh_results = query_index_with_scores(
            r_text, item_type=r_type, size=r_size, standard_text=r_std,
            top_k=settings.minhash_top_k,
            use_type_buckets=settings.use_type_buckets,
            min_candidates_before_fallback=settings.min_candidates_before_fallback,
        )
        for r in mh_results:
            iid = r["item_id"]
            it  = item_by_id.get(iid)
            if it:
                raw.append({"item_id": iid, "name": it.name, "jaccard": r["jaccard"], "via_analog": None})

    # ── Analog standard augmentation ──────────────────────────────────────────
    if (settings.use_standard_analogs_in_main_match or analogs_only or din_targets) and r_text:
        from app.matching.standard_analogs import (  # noqa: PLC0415
            build_analog_queries,
            canonical_to_display,
        )

        allowed = set(din_targets) if din_targets else None
        analog_queries = build_analog_queries(r_text, allowed_analogs=allowed)

        if din_targets and not analog_queries:
            # The standard sits in a column, not in the name text, so there is
            # nothing to rewrite.  Append the DIN instead — dropping the direct
            # query and building nothing would leave the row with no candidates
            # at all, which is worse than the mode being off.
            from app.matching.standard_analogs import (  # noqa: PLC0415
                AnalogQuery,
                row_standard_canonical,
            )
            row_canonical = row_standard_canonical(row_dict) or ""
            analog_queries = [
                AnalogQuery(
                    rewritten_text=f"{r_text} {canonical_to_display(t)}".strip(),
                    original_canonical=row_canonical,
                    analog_canonical=t,
                    analog_display=canonical_to_display(t),
                )
                for t in din_targets
            ]

        for aq in analog_queries:
            aq_results = query_index_with_scores(
                aq.rewritten_text, item_type=r_type, size=r_size,
                standard_text="",  # std already embedded in rewritten text
                top_k=settings.minhash_top_k,
                use_type_buckets=settings.use_type_buckets,
                min_candidates_before_fallback=settings.min_candidates_before_fallback,
            )
            for r in aq_results:
                iid = r["item_id"]
                it  = item_by_id.get(iid)
                if it:
                    raw.append({
                        "item_id": iid, "name": it.name,
                        "jaccard": r["jaccard"],
                        "via_analog": aq.analog_canonical,
                    })

    return _dedup_minhash_raw(raw)
```

- [ ] **Шаг 5: Прогнать тест**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_din_search.py -q`
Ожидаемо: `2 passed`. Если падает — смотреть, отработала ли первая стадия
(точное совпадение type+size) раньше MinHash: она чинится в задаче 5.

- [ ] **Шаг 6: Проверить регресс**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_use_analogs.py tests/test_minhash_matching.py -q`
Ожидаемо: все проходят — при `din_only=False` ветки прежние.

- [ ] **Шаг 7: Коммит**

```bash
git add app/match_settings.py app/matcher.py tests/test_din_search.py
git commit -m "feat(matching): режим din - запросы MinHash идут по DIN-аналогу строки"
```

---

## Задача 5. Кандидаты первой стадии, пост-фильтр и бейджи

Первая стадия (точное совпадение типа и размера) отрабатывает раньше MinHash и
возвращает результат сразу — без правки она в режиме `din` подставит ГОСТ-позицию.

Первая стадия и пост-фильтр правятся одной задачей: первая стадия передаёт
`din_targets` в `compute_candidate_badges()`, а параметр появляется в
пост-фильтре. По отдельности они не собираются в рабочее состояние, поэтому
коммит один.

**Файлы:**
- Изменить: `app/matcher.py` — `_build_exact_candidates()` (строки 359-492)
- Изменить: `app/matching/post_filter.py` — `compute_candidate_badges()` (строки 79-148), `post_filter_candidates()` (строки 191-270)
- Изменить: `tests/test_din_search.py` — дописать в конец

**Интерфейсы:**
- Потребляет: `din_targets_for_row()`, `same_standard()`, `strip_edition_year()`.
- Отдаёт: в режиме `din` кандидат первой стадии проходит, только если его
  `standard_key` — один из целевых DIN; `match_standard_mode` у него `"analog"`,
  `via_analog` — целевой DIN-ключ.
- Отдаёт: `compute_candidate_badges(..., din_targets: list[str] | None = None)`;
  в режиме `din` бейдж стандарта — `True` только у целевых DIN, и включается
  строгий фильтр (кандидаты без стандарта отбрасываются).

- [ ] **Шаг 1: Написать падающий тест**

Дописать в конец `tests/test_din_search.py`:

```python
def test_gost_item_not_substituted_for_gost_row():
    """Следствие 1: DIN-аналог есть, но по нему в каталоге пусто — подбора нет."""
    _seed_catalog([GOST_ITEM])
    _seed_equiv([("GOST-7798-70", "DIN-933")])

    df = _df([_row("Болт М12x60 ГОСТ 7798-70", gost="ГОСТ 7798-70")])
    _, results = add_internal_matches(df, settings=_settings(din_only=True))

    assert results[0]["internal_item_id"] is None


def test_din_row_stays_on_din():
    """A DIN row keeps its own standard and is not led onto the ГОСТ item."""
    _seed_catalog([DIN_ITEM, GOST_ITEM])
    _seed_equiv([("GOST-7798-70", "DIN-933")])

    df = _df([_row("Болт М12x60 DIN 933", din="DIN 933")])
    _, results = add_internal_matches(df, settings=_settings(din_only=True))

    assert "DIN 933" in results[0]["name"]


def test_row_without_standard_matches_as_usual():
    """No standard in the row — the mode must not change anything for it."""
    _seed_catalog([DIN_ITEM])
    _seed_equiv([("GOST-7798-70", "DIN-933")])

    df = _df([_row("Болт М12x60")])
    off = add_internal_matches(_df([_row("Болт М12x60")]), settings=_settings())[1]
    din = add_internal_matches(df, settings=_settings(din_only=True))[1]

    assert din[0]["internal_item_id"] == off[0]["internal_item_id"]


def test_row_without_din_analog_falls_back_to_own_standard():
    """No DIN counterpart in the reference book — search by the row's own standard."""
    _seed_catalog([GOST_ITEM])
    _seed_equiv([("GOST-5927-70", "DIN-934")])  # unrelated pair

    df = _df([_row("Болт М12x60 ГОСТ 7798-70", gost="ГОСТ 7798-70")])
    _, results = add_internal_matches(df, settings=_settings(din_only=True))

    assert results[0]["internal_item_id"] is not None
    assert "ГОСТ 7798-70" in results[0]["name"]
```

- [ ] **Шаг 2: Убедиться, что тест падает**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_din_search.py -q -k "not_substituted or stays_on_din"`
Ожидаемо: `test_gost_item_not_substituted_for_gost_row` падает — первая стадия
подставила ГОСТ-позицию, `internal_item_id` не `None`.

- [ ] **Шаг 3: Реализовать**

В `app/matcher.py`, в `_build_exact_candidates()`, сразу после строки
`r_paint = _norm(row_dict.get("paint_color"))` добавить:

```python
    # DIN search mode: this row must be matched to one of these DIN standards
    # and to nothing else.  Empty list → ordinary behaviour for this row.
    din_targets: list[str] = []
    if getattr(settings, "din_only", False):
        from app.matching.standard_analogs import din_targets_for_row  # noqa: PLC0415
        din_targets = din_targets_for_row(row_dict)
```

Заменить блок «Standard check» и следующий за ним фильтр:

```python
        # Standard check
        item_std_key = (item.standard_key or "").strip()
        _std_direct = False
        _std_analog = False
        if din_targets:
            from app.matching.standard_analogs import same_standard  # noqa: PLC0415
            _std_analog = bool(item_std_key) and any(
                same_standard(item_std_key, t) for t in din_targets
            )
            if not _std_analog:
                # In DIN mode an item that is not the DIN counterpart is not a
                # candidate — including items with no standard at all, which
                # would otherwise slip through as "no verdict".
                continue
            score -= 5
            reasons.append("стандарт (аналог)")
        elif r_std_keys and item_std_key:
            if item_std_key in r_std_keys:
                _std_direct = True
                reasons.append("стандарт ✓")
            else:
                if use_analogs or analogs_only or settings.use_standard_analogs_in_main_match:
                    from app.matching.standard_analogs import get_standard_analogs  # noqa: PLC0415
                    _std_analog = any(
                        item_std_key in get_standard_analogs(k) for k in r_std_keys
                    )
                if _std_analog:
                    score -= 5
                    reasons.append("стандарт (аналог)")
                else:
                    score -= 20
                    reasons.append("стандарт ✗")
        elif r_std_keys and not item_std_key:
            score -= 3

        # Filter by analog mode when row has a standard
        if r_std_keys and not din_targets:
            if analogs_only:
                # "Только аналоги" — require analog standard match; skip all else
                if not _std_analog:
                    continue
            elif item_std_key and not use_analogs and not _std_direct:
                continue  # "Без аналогов" — skip analog and mismatched
```

Заменить вычисление `std_mode` (было `std_mode = "exact" if ... else "none"`):

```python
        if din_targets:
            std_mode = "analog"
        elif item_std_key and item_std_key in r_std_keys:
            std_mode = "exact"
        else:
            std_mode = "none"
```

В том же словаре `cand` заменить две строки диагностики — по ним UI рисует
бейдж «через аналог», и без них не видно, по какому DIN шёл поиск:

```python
            "via_analog": din_targets[0] if din_targets else None,
            "via_analog_display": canonical_to_display(din_targets[0]) if din_targets else "",
```

Импорт `canonical_to_display` добавить к уже существующему импорту в начале
функции:

```python
    from app.matching.standard_analogs import canonical_to_display  # noqa: PLC0415
    from app.matching.standard_analogs import normalize_standard as _ns_std  # noqa: PLC0415
```

Заменить вызов расчёта бейджей, добавив последним аргументом целевые DIN:

```python
        cand["field_badges"] = compute_candidate_badges(
            cand, row_dict, item_by_id, row_size_norm, row_std_canon,
            use_analogs or analogs_only,
            din_targets=din_targets,
        )
```

- [ ] **Шаг 4: Дописать тесты пост-фильтра**

Дописать в конец `tests/test_din_search.py`:

```python
def test_item_without_standard_is_dropped_in_din_mode():
    """An item with no standard must not pass as "no verdict" in DIN mode."""
    _seed_catalog([
        DIN_ITEM,
        {"name": "Болт М12x60 без стандарта", "standard_text": "", "standard_key": None},
    ])
    _seed_equiv([("GOST-7798-70", "DIN-933")])

    df = _df([_row("Болт М12x60 ГОСТ 7798-70", gost="ГОСТ 7798-70")])
    _, results = add_internal_matches(df, settings=_settings(din_only=True))

    names = [c["name"] for c in results[0]["candidates"]]
    assert all("без стандарта" not in n for n in names)


def test_standard_badge_true_for_din_counterpart():
    _seed_catalog([DIN_ITEM])
    _seed_equiv([("GOST-7798-70", "DIN-933")])

    df = _df([_row("Болт М12x60 ГОСТ 7798-70", gost="ГОСТ 7798-70")])
    _, results = add_internal_matches(df, settings=_settings(din_only=True))

    best = results[0]["candidates"][0]
    assert best["field_badges"]["standard"]["match"] is True
```

- [ ] **Шаг 5: Убедиться, что тест падает**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_din_search.py -q`
Ожидаемо: `TypeError: compute_candidate_badges() got an unexpected keyword
argument 'din_targets'` — параметр добавляется следующим шагом.

- [ ] **Шаг 6: Реализовать бейджи**

В `app/matching/post_filter.py` изменить сигнатуру `compute_candidate_badges()`:

```python
def compute_candidate_badges(
    candidate: dict,
    row_dict: dict,
    item_by_id: dict,
    row_size_norm: str,
    row_std_canon: str | None,
    use_analogs: bool,
    din_targets: list[str] | None = None,
) -> dict:
```

В docstring дописать:

```
    ``din_targets`` — DIN search mode: the row is judged against these DIN keys
    instead of its own standard and its full analog group.
```

Заменить блок «Standard»:

```python
    # ── Standard ──────────────────────────────────────────────────────────────
    item_std_canon = _item_std_canonical(item)
    item_std_display = str(getattr(item, "standard_text", "") or "").strip() or "—"
    if din_targets:
        from app.matching.standard_analogs import strip_edition_year  # noqa: PLC0415
        # No standard on the item means no verdict in the ordinary modes, but in
        # DIN mode it is a definite miss: we are looking for one exact set of
        # DIN keys and nothing else qualifies.
        if not item_std_canon:
            badges["standard"] = {"match": False, "label": item_std_display}
        else:
            targets = {strip_edition_year(t) for t in din_targets}
            badges["standard"] = {
                "match": strip_edition_year(item_std_canon) in targets,
                "label": item_std_display,
            }
    elif not row_std_canon or not item_std_canon:
        badges["standard"] = {"match": None, "label": item_std_display}
    else:
        r_group = _std_group(row_std_canon, use_analogs)
        i_group = _std_group(item_std_canon, use_analogs)
        badges["standard"] = {"match": bool(r_group & i_group), "label": item_std_display}
```

- [ ] **Шаг 7: Реализовать строгий фильтр**

В `post_filter_candidates()`, после строки `row_type = str(row_dict.get("item_type") or "").strip().lower()`:

```python
    din_targets: list[str] = []
    if getattr(settings, "din_only", False):
        from app.matching.standard_analogs import din_targets_for_row  # noqa: PLC0415
        din_targets = din_targets_for_row(row_dict)
```

Заменить аннотацию кандидатов бейджами:

```python
    for c in all_candidates:
        c["field_badges"] = compute_candidate_badges(
            c, row_dict, item_by_id, row_size_norm, row_std_canon, use_analogs,
            din_targets=din_targets,
        )
```

Заменить блок строгого режима:

```python
    # In analogs_only and DIN modes: require standard match=True (not just
    # not-False).  Items with no standard badge (match=None) are excluded.
    _strict_mode = analogs_only or bool(din_targets)
    if _strict_mode and (has_std or din_targets):
        has_std = True  # force standard filter on
    _std_strict = _strict_mode and has_std  # require match=True, not just !=False
```

- [ ] **Шаг 8: Прогнать тесты**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_din_search.py -q`
Ожидаемо: `8 passed`.

- [ ] **Шаг 9: Проверить регресс ядра подбора**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_use_analogs.py tests/test_matcher.py tests/test_match_decide.py tests/test_minhash_matching.py tests/test_standard_match.py -q`
Ожидаемо: все проходят.

- [ ] **Шаг 10: Коммит**

```bash
git add app/matcher.py app/matching/post_filter.py tests/test_din_search.py
git commit -m "feat(matching): режим din в первой стадии и пост-фильтре"
```

---

## Задача 6. Режим в визардах загрузки

**Файлы:**
- Изменить: `app/main.py` — `transform()` (строки 888-970)
- Изменить: `app/templates/view_raw.html:53`, `app/templates/pdf_wizard.html:150`, `app/templates/google_ocr_wizard.html:268`, `app/templates/google_ocr_list_wizard.html:165`

**Интерфейсы:**
- Потребляет: `MatchSettings.din_only`.
- Отдаёт: `POST /transform` принимает `analog_mode=din`; значение пишется в `meta.json`.

- [ ] **Шаг 1: Расширить проверку значения**

В `app/main.py`, в `transform()`, заменить:

```python
    # analog_mode: "off" | "with" | "only" | "din"
    if analog_mode not in ("off", "with", "only", "din"):
        analog_mode = "off"
```

- [ ] **Шаг 2: Добавить ветку режима**

Там же, заменить блок выбора настроек:

```python
    _ms = _load_ms()
    if analog_mode == "with":
        _ms = _dataclasses.replace(_ms, use_standard_analogs_in_main_match=True, analogs_only=False, din_only=False)
    elif analog_mode == "only":
        _ms = _dataclasses.replace(_ms, use_standard_analogs_in_main_match=False, analogs_only=True, din_only=False)
    elif analog_mode == "din":
        _ms = _dataclasses.replace(_ms, use_standard_analogs_in_main_match=False, analogs_only=False, din_only=True)
    else:
        _ms = _dataclasses.replace(_ms, use_standard_analogs_in_main_match=False, analogs_only=False, din_only=False)
```

- [ ] **Шаг 3: Добавить пункт в четыре шаблона**

В каждом из четырёх файлов строка

```html
                    <option value="only">Только аналоги</option>
```

дополняется следующей строкой:

```html
                    <option value="din">Поиск по DIN</option>
```

Файлы и строки: `view_raw.html:53`, `pdf_wizard.html:150`,
`google_ocr_wizard.html:268`, `google_ocr_list_wizard.html:165`.

- [ ] **Шаг 4: Проверить руками**

Запустить: `./venv/Scripts/python.exe -m uvicorn app.main:app --port 8001`

Открыть http://localhost:8001, загрузить любой xlsx с позициями по ГОСТу,
в списке режимов выбрать «Поиск по DIN», нажать «Обработать».

Ожидаемо: страница результата открывается без ошибки; в `data/cache/<file_id>/meta.json`
поле `"analog_mode": "din"`.

- [ ] **Шаг 5: Коммит**

```bash
git add app/main.py app/templates/view_raw.html app/templates/pdf_wizard.html app/templates/google_ocr_wizard.html app/templates/google_ocr_list_wizard.html
git commit -m "feat(ui): режим Поиск по DIN в визардах загрузки"
```

---

## Задача 7. Параметр `analog_mode` в API для 1С

**Файлы:**
- Изменить: `app/api.py` — `MatchRequestBody` (строки 224-227), `_apply_analog_override()` (строки 248-260), вызов на строке 355, `ParseRequestBase64Body` (строки 899-904), вызов на строке 930, форма `/parse-request` (строки 980-986), вызов на строке 1054
- Создать: `tests/test_din_api.py`

**Интерфейсы:**
- Отдаёт: `analog_mode` со значениями `off` / `with` / `only` / `din` в трёх
  маршрутах; старый `use_analogs` продолжает работать.

- [ ] **Шаг 1: Написать падающий тест**

Создать `tests/test_din_api.py`:

```python
"""analog_mode parameter of the 1C API."""
import pytest

from app.match_settings import MatchSettings


def _base():
    return MatchSettings()


def test_no_params_keeps_settings():
    from app.api import _resolve_analog_mode
    s = _base()
    assert _resolve_analog_mode(s, None, None) is s


def test_din_mode():
    from app.api import _resolve_analog_mode
    got = _resolve_analog_mode(_base(), "din", None)
    assert got.din_only is True
    assert got.analogs_only is False
    assert got.use_standard_analogs_in_main_match is False


def test_only_mode():
    from app.api import _resolve_analog_mode
    got = _resolve_analog_mode(_base(), "only", None)
    assert got.analogs_only is True
    assert got.din_only is False


def test_legacy_use_analogs_true():
    from app.api import _resolve_analog_mode
    got = _resolve_analog_mode(_base(), None, True)
    assert got.use_standard_analogs_in_main_match is True
    assert got.din_only is False


def test_legacy_use_analogs_false():
    from app.api import _resolve_analog_mode
    got = _resolve_analog_mode(_base(), None, False)
    assert got.use_standard_analogs_in_main_match is False
    assert got.din_only is False


def test_analog_mode_wins_over_legacy():
    from app.api import _resolve_analog_mode
    got = _resolve_analog_mode(_base(), "din", True)
    assert got.din_only is True
    assert got.use_standard_analogs_in_main_match is False


def test_unknown_value_is_ignored():
    from app.api import _resolve_analog_mode
    s = _base()
    assert _resolve_analog_mode(s, "DIN-931", None) is s


@pytest.mark.parametrize("raw", ["DIN", " din ", "Din"])
def test_value_is_case_insensitive(raw):
    from app.api import _resolve_analog_mode
    assert _resolve_analog_mode(_base(), raw, None).din_only is True
```

- [ ] **Шаг 2: Убедиться, что тест падает**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_din_api.py -q`
Ожидаемо: `ImportError: cannot import name '_resolve_analog_mode'`.

- [ ] **Шаг 3: Заменить `_apply_analog_override()`**

В `app/api.py` заменить функцию целиком:

```python
_ANALOG_MODES = ("off", "with", "only", "din")


def _resolve_analog_mode(settings, analog_mode=None, use_analogs=None):
    """Let one request choose how standards are matched.

    ``analog_mode`` is the current parameter: "off", "with", "only" or "din".
    ``use_analogs`` is the older boolean and still works — True means "with",
    False means "off".  When both arrive, the mode wins.  When neither says
    anything, whatever the global setting holds is kept: substitutes are
    welcome on some заявки and unacceptable on others, so the caller decides.
    """
    mode = str(analog_mode or "").strip().lower()
    if mode not in _ANALOG_MODES:
        mode = ""
    if not mode:
        if use_analogs is None:
            return settings
        mode = "with" if use_analogs else "off"

    import dataclasses  # noqa: PLC0415

    return dataclasses.replace(
        settings,
        use_standard_analogs_in_main_match=(mode == "with"),
        analogs_only=(mode == "only"),
        din_only=(mode == "din"),
    )
```

- [ ] **Шаг 4: Прокинуть параметр в три маршрута**

`MatchRequestBody`:

```python
class MatchRequestBody(BaseModel):
    rows: List[RequestRow]
    use_analogs: Optional[bool] = None   # устаревший: True → "with", False → "off"
    analog_mode: Optional[str] = None    # "off" | "with" | "only" | "din"
```

Строка 355 — заменить вызов:

```python
    settings = _resolve_analog_mode(load_match_settings(), body.analog_mode, body.use_analogs)
```

`ParseRequestBase64Body`:

```python
class ParseRequestBase64Body(BaseModel):
    file_base64: str
    filename: str = "upload.xlsx"
    hint: str = ""
    use_analogs: Optional[bool] = None
    analog_mode: Optional[str] = None
```

Строка 930 — заменить проброс:

```python
    return api_parse_request(
        file=fake_file, text="", hint=body.hint,
        use_analogs=body.use_analogs, analog_mode=body.analog_mode,
    )
```

Форма `/parse-request` — добавить поле:

```python
@router.post("/parse-request")
def api_parse_request(
    text: str = Form(default=""),
    file: Optional[UploadFile] = File(default=None),
    hint: str = Form(default=""),
    use_analogs: Optional[bool] = Form(default=None),
    analog_mode: Optional[str] = Form(default=None),
):
```

Строка 1054 — заменить вызов:

```python
    settings = _resolve_analog_mode(load_match_settings(), analog_mode, use_analogs)
```

- [ ] **Шаг 5: Прогнать тесты**

Запустить: `./venv/Scripts/python.exe -m pytest tests/test_din_api.py -q`
Ожидаемо: `10 passed`.

- [ ] **Шаг 6: Проверить, что старые вызовы API живы**

Запустить: `./venv/Scripts/python.exe -m pytest tests/ -q -k "api or quote"`
Ожидаемо: все проходят.

- [ ] **Шаг 7: Коммит**

```bash
git add app/api.py tests/test_din_api.py
git commit -m "feat(api): параметр analog_mode с режимом din для 1С"
```

---

## Задача 8. Полный прогон и документация

**Файлы:**
- Изменить: `docs/specs/2026-09-10-poisk-po-din.md` — разделы 4.4 и 4.5

- [ ] **Шаг 1: Полный прогон тестов**

Запустить: `./venv/Scripts/python.exe -m pytest -q`
Ожидаемо: падений нет. Каждое падение разобрать до конца — режим `din`
затрагивает ядро подбора, и красный тест здесь означает регресс, а не шум.

- [ ] **Шаг 2: Привести спецификацию в соответствие с кодом**

В `docs/specs/2026-09-10-poisk-po-din.md`, разделы 4.4 и 4.5, заменить
формулировку «функция получает параметр `din_targets`» на то, как сделано:
целевые DIN вычисляются внутри `_build_exact_candidates()` и
`post_filter_candidates()` из `settings` и `row_dict`; наружу параметр
`din_targets` торчит только у `compute_candidate_badges()`.

- [ ] **Шаг 3: Коммит**

```bash
git add docs/specs/2026-09-10-poisk-po-din.md
git commit -m "docs: спецификация поиска по DIN приведена в соответствие с кодом"
```

---

## Что проверить на проде после деплоя

Не часть реализации — шаги для выката, выполняются вместе с заказчиком.

1. Справочник соответствий: `SELECT COUNT(*) FROM standard_equivalents WHERE is_active=1`
   и сколько из них имеют сторону `DIN-%`. Если пар мало, режим повлияет только
   на те позиции, что в них попали.
2. Заявка с ГОСТами через визард в режиме «Поиск по DIN» — сравнить долю
   подобранных позиций с режимом «Без аналогов».
3. Вызов из 1С с `analog_mode: "din"` — убедиться, что подбор отличается от
   вызова без параметра.
4. Флоу обновления прода: `nssm stop OtdelZakup` → `git pull` →
   `pip install -r requirements.txt` → `alembic upgrade head` → `nssm start OtdelZakup`.
   Миграций эта задача не добавляет: `din_only` в БД не хранится.
