# OtdelZakup

Веб-приложение + JSON API для загрузки, просмотра и преобразования Excel-файлов (.xlsx) отдела закупок.

## Запуск локально

```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

Приложение доступно на http://localhost:8000

## Запуск через Docker Compose

```bash
docker compose up --build
```

## Тесты

```bash
pytest -q
```

## Переменные окружения

| Переменная | По умолчанию | Описание |
|---|---|---|
| `OTDELZAKUP_UPLOAD_DIR` | `./data/uploads` | Директория для сохранения загруженных .xlsx файлов |
| `OTDELZAKUP_CACHE_DIR` | `./data/cache` | Директория файлового кэша (parquet + meta.json) |

## Веб-интерфейс

- `GET /` — страница загрузки файла
- `POST /upload` — загрузка .xlsx, отображение таблицы в браузере
- `POST /transform` — преобразование с выбранными полями, две таблицы рядом

## JSON API v1 (для интеграции с 1С)

### POST /api/v1/upload

Загрузка файла. `multipart/form-data`, поле `file` (.xlsx).

Ответ:
```json
{"file_id": "abc123...", "filename": "data.xlsx", "rows_total": 150, "columns": ["name", "qty"]}
```

Ошибка: `400 {"error": "..."}`.

### GET /api/v1/preview/{file_id}?limit=200

Предпросмотр исходных данных. `limit` — опционально (по умолчанию 200).

Ответ:
```json
{"file_id": "...", "rows_total": 150, "limit": 200, "columns": ["name", "qty"], "rows": [["Болт M12", 100], ...]}
```

`rows` — массивы (не объекты). Если `file_id` не найден: `404 {"error": "not found"}`.

### POST /api/v1/transform

Извлечение полей. `Content-Type: application/json`.

Тело запроса:
```json
{"file_id": "...", "fields": ["diameter", "strength"], "limit": 200}
```

Доступные поля: `diameter`, `length`, `size`, `strength`, `coating`, `gost`, `din`, `tail_code`. `limit` — опционально (по умолчанию 200).

Ответ:
```json
{"file_id": "...", "rows_total": 150, "fields": ["diameter", "strength"], "columns": ["name", "qty", "Диаметр", "Класс прочности"], "rows": [["Болт M12", 100, "M12", "8.8"], ...]}
```

Если `file_id` не найден: `404 {"error": "not found"}`.
