# Запуск OtdelZakup

## Первый запуск (новая машина / новый venv)

```powershell
# 1. Создать виртуальное окружение
python -m venv venv

# 2. Активировать
.\venv\Scripts\Activate.ps1
# Если PowerShell ругается на политику — один раз:
# Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned

# 3. Установить зависимости
pip install -r requirements.txt

# 4. Применить миграции БД
python -m alembic upgrade head

# 5. Запустить сервер
python -m uvicorn app.main:app --reload --port 8000
```

Приложение: http://localhost:8000

> **Первый старт строит MinHash-индекс (~1-2 минуты).**
> Дождись строки `MinHash cache saved: ...` в логах — после этого
> следующие запуски будут мгновенными.

---

## Обычный запуск (venv уже есть)

```powershell
.\venv\Scripts\Activate.ps1
python -m uvicorn app.main:app --reload --port 8000
```

---

## Запуск после обновления кода (git pull)

```powershell
# 1. Получить изменения
git pull origin main

# 2. Активировать окружение
.\venv\Scripts\Activate.ps1

# 3. Обновить зависимости (если менялся requirements.txt)
pip install -r requirements.txt

# 4. Применить новые миграции БД (безопасно запускать всегда)
python -m alembic upgrade head

# 5. Запустить сервер
python -m uvicorn app.main:app --reload --port 8000
```

---

## Продакшн-сервер (Srv-sq-b1, Z:\ProjectZ)

Путь: `Z:\ProjectZ` → `\\Srv-sq-b1\ВыгрузкаРазработки\BB-8\ProjectZ`

```powershell
# Если работает как служба NSSM:
nssm stop OtdelZakup
git pull origin main
.\venv\Scripts\pip.exe install -r requirements.txt
.\venv\Scripts\python.exe -m alembic upgrade head
nssm start OtdelZakup

# Если запускается вручную (без службы):
git pull origin main
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m alembic upgrade head
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

> На продакшне убери флаг `--reload` (он лишний и замедляет старт).

---

## Частые проблемы

| Проблема | Решение |
|---|---|
| `No module named uvicorn` | Не активирован venv или не установлены зависимости |
| `.\venv\Scripts\python.exe` не найден | Используй просто `python` после активации venv |
| MinHash строится при каждом перезапуске | Дай серверу закончить сохранение — дождись `MinHash cache saved` в логах |
| Ошибка миграции | Проверь путь к БД (`OTDELZAKUP_DB_PATH`), по умолчанию `./data/readiness.db` |
