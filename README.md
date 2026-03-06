# Legend Trading: Full Project

Полный проект в одном репозитории:
- Telegram бот (`bot.py`)
- Единый сервер (`legend_server.py`) для WebApp + запуска бота
- WebApp в стиле `Legend Trading` (`legend_web/`)
- Совместимость БД: `SQLite` и `PostgreSQL` через `DATABASE_URL` (`db_compat.py`)
- Автодеплой для Railway через `railway.toml`

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
- `BOT_MODE=webhook` (рекомендуется для Railway)
- `WEBAPP_DEFAULT_TG_ID=<ваш tg_id>`
- `WEBHOOK_BASE_URL=https://<ваш-railway-домен>`
- `WEBHOOK_SECRET=<случайная_строка>`
- `WEBAPP_URL=https://<ваш-railway-домен>`
- `CARD_PAY_URL=<ссылка_на_оплату_картой>` (опционально)
- `CARD_REQUISITES=<реквизиты_карты>` (опционально)
- `SESSION_SECRET=<секрет_сессии>`
- `ADMIN_WEB_USERNAME=admin`
- `ADMIN_WEB_PASSWORD=<сильный_пароль>`
- опционально: `LOG_CHAT_ID`, `CRYPTO_BOT_URL`, `TRC20_ADDRESS`, `SUPPORT_URL`

### Быстрый деплой (без ручной настройки команды запуска)

1. `New Project` -> `Deploy from GitHub repo`.
2. Выберите этот репозиторий.
3. Railway подхватит `railway.toml` и сам возьмет `startCommand`.
4. Добавьте переменные окружения из `.env.example`.
5. Нажмите `Deploy`.

Проверка после деплоя:
- `https://<ваш-домен>/api/overview` должен отдавать JSON.
- `https://<ваш-домен>/admin/login` доступ в веб-админку.
