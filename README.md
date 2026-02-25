# OtdelZakup — MVP (Итерация 0-1)

Веб-приложение для загрузки и просмотра Excel-файлов (.xlsx) отдела закупок.

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

Приложение доступно на http://localhost:8000

## Тесты

```bash
pytest -q
```

## Структура

- `GET /` — страница загрузки файла
- `POST /upload` — загрузка .xlsx, отображение таблицы в браузере
