# TradingBot WebApp

Веб-панель для мониторинга Telegram Trading Bot на базе `bot.db`.

## Что есть

- Дашборд с ключевыми метриками.
- Таблицы пользователей, сделок и выводов.
- API `GET /api/overview` для live-обновления метрик.

## Запуск

```powershell
cd "c:\Users\Ryzen PC Admin\Desktop\ТрейдингБот\TradingBot+WebApp"
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn main:app --reload --port 8080
```

Открыть в браузере: `http://127.0.0.1:8080`.
