# Legend Trading: Full Project

Полный проект в одном репозитории:
- Telegram бот (`bot.py`)
- Единый сервер (`legend_server.py`) для WebApp + запуска бота
- WebApp в стиле `Legend Trading` (`legend_web/`)
- Совместимость БД: `SQLite` и `PostgreSQL` через `DATABASE_URL` (`db_compat.py`)

## Запуск локально

```powershell
cd "c:\Users\Ryzen PC Admin\Desktop\ТрейдингБот\TradingBot+WebApp"
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn legend_server:app --reload --port 8080
```

Открыть: `http://127.0.0.1:8080`

## Railway

Переменные окружения:
- `BOT_TOKEN`
- `ADMIN_IDS`
- `DATABASE_URL` (PostgreSQL)
- `RUN_BOT=1`
- `WEBAPP_DEFAULT_TG_ID=<ваш tg_id>`

Start command:

```bash
uvicorn legend_server:app --host 0.0.0.0 --port $PORT
```
