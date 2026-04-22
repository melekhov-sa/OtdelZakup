# Запуск на рабочем сервере `Srv-sq-b1`

Windows, PowerShell от имени администратора. Рабочая директория — `Z:\ProjectZ` (смапирована на `\\Srv-sq-b1\ВыгрузкаРазработки\BB-8\ProjectZ`).

## 1. Подтянуть последнюю версию

```powershell
cd Z:\ProjectZ
git pull origin main
```

## 2. Venv и зависимости (только если venv нет или requirements менялись)

```powershell
# Создать venv один раз:
python -m venv venv

# Активировать (если политика ругается — см. ниже):
.\venv\Scripts\Activate.ps1

# Обновить зависимости:
pip install -r requirements.txt
```

Если `Activate.ps1` блокируется — однократно разрешить для пользователя:

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

## 3. Применить миграции БД

```powershell
.\venv\Scripts\python.exe -m alembic upgrade head
```

Миграции идемпотентны — на существующую БД ничего лишнего не сломают.

## 4. Запустить сервер

Слушаем на всех интерфейсах, без `--reload` (прод):

```powershell
.\venv\Scripts\python.exe -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Проверить: `http://localhost:8000` с самого сервера, `http://Srv-sq-b1:8000` с других машин (когда откроешь порт).

## 5. Брандмауэр (один раз)

Если хочешь доступ с других машин в сети:

```powershell
New-NetFirewallRule -DisplayName "OtdelZakup 8000" -Direction Inbound -LocalPort 8000 -Protocol TCP -Action Allow
```

## 6. Переменные окружения (опционально)

Если хочешь, чтобы `data/` лежала не рядом с проектом — выставь в той же сессии:

```powershell
$env:OTDELZAKUP_UPLOAD_DIR = "D:\OtdelZakup\uploads"
$env:OTDELZAKUP_CACHE_DIR  = "D:\OtdelZakup\cache"
$env:OTDELZAKUP_DB_PATH    = "D:\OtdelZakup\readiness.db"
```

Google OCR — через `.env` или постоянные системные переменные:

- `GOOGLE_PROJECT_ID`
- `GOOGLE_LOCATION` (`eu` / `us`)
- `GOOGLE_PROCESSOR_ID`
- `GOOGLE_APPLICATION_CREDENTIALS` — путь к JSON сервисного аккаунта

## 7. Автозапуск как служба Windows (когда будешь готов)

Сейчас процесс живёт только пока открыт терминал. Чтобы сделать постоянную службу — через NSSM:

```powershell
# установить NSSM один раз, затем:
nssm install OtdelZakup "Z:\ProjectZ\venv\Scripts\python.exe" "-m uvicorn app.main:app --host 0.0.0.0 --port 8000"
nssm set OtdelZakup AppDirectory "Z:\ProjectZ"
nssm set OtdelZakup AppStdout "Z:\ProjectZ\logs\stdout.log"
nssm set OtdelZakup AppStderr "Z:\ProjectZ\logs\stderr.log"
nssm start OtdelZakup
```

## 8. Типовой флоу обновления

```powershell
cd Z:\ProjectZ

# если крутится как служба:
nssm stop OtdelZakup

git pull origin main
.\venv\Scripts\pip.exe install -r requirements.txt
.\venv\Scripts\python.exe -m alembic upgrade head

nssm start OtdelZakup
# либо запустить вручную через uvicorn, если служба не настроена
```

## Важно про MinHash-кэш

- Первый запуск после `git pull` будет долгим — один раз соберёт индекс по ~220k позиций (1–2 минуты) и сохранит `data/cache/minhash_index.pkl` (~350 МБ при `num_perm=128`).
- Все последующие старты — мгновенные, кэш подхватится с диска.
- Если начинает долго стартовать повторно — значит `save` упал: проверь логи `logger.exception` от `app.matching.minhash_cache` и свободное место на диске в `data/cache/`.
- Если в БД был `num_perm=256` — разово выстави 128 через `/settings/match` в UI (pickle уменьшится с ~1.3 ГБ до ~350 МБ при том же recall).

## Проверка после запуска

1. `http://localhost:8000` открывается — главная страница с формой загрузки.
2. `http://localhost:8000/internal-items` — видно ~220k позиций (с пагинацией).
3. `http://localhost:8000/settings/match` — настройки auto-match открываются.
4. В логах при старте должно быть:
   - `MinHash cache hit: NNNNNN items, fp=XXXXXXXX` (если индекс уже был)
   - или `MinHash rebuild: NNNNNN items...` → `MinHash cache saved: ...` (первый запуск).
