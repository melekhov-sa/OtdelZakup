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
- `GET /download/{file_id}/{token}` — скачать результат преобразования в .xlsx

### Сценарий демо

1. Открыть http://localhost:8000
2. Загрузить .xlsx файл с метизами
3. Отметить нужные поля (диаметр, длина, класс прочности, покрытие, ГОСТ и т.д.)
4. Нажать "Преобразить" — увидеть исходник слева, результат справа
5. Нажать "Скачать результат .xlsx" — получить файл со всеми строками и извлечёнными полями

### Доступные поля для извлечения

| Ключ | Название | Пример |
|---|---|---|
| `diameter` | Диаметр | M12 |
| `length` | Длина | 150 |
| `size` | Размер MxL | M12x150 |
| `strength` | Класс прочности | 8.8, 10.9, 12.9 |
| `coating` | Покрытие | цинк |
| `gost` | ГОСТ | ГОСТ 7798-70 |
| `iso` | ISO | ISO 4017 |
| `din` | DIN | DIN 931 |
| `tail_code` | Хвост-код | .88.016 |

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

Доступные поля: `diameter`, `length`, `size`, `strength`, `coating`, `gost`, `iso`, `din`, `tail_code`. `limit` — опционально (по умолчанию 200).

Ответ:
```json
{"file_id": "...", "rows_total": 150, "fields": ["diameter", "strength"], "columns": ["name", "qty", "Диаметр", "Класс прочности"], "rows": [["Болт M12", 100, "M12", "8.8"], ...]}
```

Если `file_id` не найден: `404 {"error": "not found"}`.
