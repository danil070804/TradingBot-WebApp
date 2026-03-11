import asyncio
import os
import random
import contextlib
import json
import hmac
import hashlib
import time
import secrets
from collections import deque
from urllib.parse import parse_qsl, urlencode
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from aiogram.types import Update
from starlette.middleware.sessions import SessionMiddleware

import bot
import db_compat as db
from binance_market import (
    BinanceMarketService,
    asset_to_binance_ticker,
    calculate_trade_profit,
    ticker_to_symbol,
)

try:
    import asyncpg
except Exception:
    asyncpg = None


BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "legend_web"
DEFAULT_TG_ID = int(os.getenv("WEBAPP_DEFAULT_TG_ID", "0"))
ALLOW_DEFAULT_TG_FALLBACK = os.getenv("ALLOW_DEFAULT_TG_FALLBACK", "0") == "1"
RUN_BOT = os.getenv("RUN_BOT", "1") == "1"
BOT_MODE = os.getenv("BOT_MODE", "webhook").strip().lower()
MARKET_DEV_FALLBACK = os.getenv("MARKET_DEV_FALLBACK", "0") == "1"
POLLING_LOCK_KEY = int(os.getenv("BOT_POLLING_LOCK_KEY", "8598101146"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/telegram/webhook")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
SESSION_SECRET = os.getenv("SESSION_SECRET", WEBHOOK_SECRET or "legend_trading_session_secret")
ADMIN_WEB_USERNAME = os.getenv("ADMIN_WEB_USERNAME", "admin")
ADMIN_WEB_PASSWORD = os.getenv("ADMIN_WEB_PASSWORD", "")
USER_SESSION_KEY = "tg_user_id"
LANG_SESSION_KEY = "web_lang"


polling_task: asyncio.Task | None = None
polling_lock_conn = None
market_feed_task: asyncio.Task | None = None
market_price_task: asyncio.Task | None = None
MARKET_SERVICE = BinanceMarketService()
ASSET_TICKER_MAP: dict[str, str] = {}
MARKET_TAPE = deque(maxlen=120)
MARKET_TAPE_NEXT_TS = 0.0
MARKET_DEPTH_REFRESH_AT: dict[str, float] = {}
ACTIVE_WEB_TRADES: dict[str, dict] = {}
MARKET_BOOK_STATE: dict[str, dict] = {}
MARKET_HISTORY_MAXLEN = 12000
ASSET_SPECS = {
    "BTC": {"name": "Bitcoin", "start": 64000.0, "vol": 0.004, "qty_min": 0.01, "qty_max": 2.4},
    "ETH": {"name": "Ethereum", "start": 3400.0, "vol": 0.005, "qty_min": 0.03, "qty_max": 12.0},
    "SOL": {"name": "Solana", "start": 145.0, "vol": 0.009, "qty_min": 0.1, "qty_max": 110.0},
    "XRP": {"name": "Ripple", "start": 0.62, "vol": 0.012, "qty_min": 20.0, "qty_max": 8000.0},
    "DOGE": {"name": "Dogecoin", "start": 0.18, "vol": 0.015, "qty_min": 120.0, "qty_max": 24000.0},
    "TON": {"name": "Toncoin", "start": 6.4, "vol": 0.01, "qty_min": 6.0, "qty_max": 1800.0},
    "ADA": {"name": "Cardano", "start": 0.72, "vol": 0.012, "qty_min": 40.0, "qty_max": 13000.0},
    "AVAX": {"name": "Avalanche", "start": 35.0, "vol": 0.011, "qty_min": 1.2, "qty_max": 1600.0},
    "LINK": {"name": "Chainlink", "start": 21.0, "vol": 0.009, "qty_min": 1.0, "qty_max": 1200.0},
    "TRX": {"name": "TRON", "start": 0.13, "vol": 0.01, "qty_min": 150.0, "qty_max": 42000.0},
    "MATIC": {"name": "Polygon", "start": 0.94, "vol": 0.013, "qty_min": 20.0, "qty_max": 9000.0},
    "DOT": {"name": "Polkadot", "start": 8.4, "vol": 0.01, "qty_min": 4.0, "qty_max": 3000.0},
    "LTC": {"name": "Litecoin", "start": 86.0, "vol": 0.008, "qty_min": 0.2, "qty_max": 240.0},
}
MARKET_PRICE_STATE = {k: v["start"] for k, v in ASSET_SPECS.items()}
_now_ts = int(time.time())
MARKET_DAY_STATS = {k: {"open": v["start"], "high": v["start"], "low": v["start"], "ts": _now_ts} for k, v in ASSET_SPECS.items()}
MARKET_MOMENTUM_STATE = {k: 0.0 for k in ASSET_SPECS.keys()}
MARKET_REGIME_STATE = {k: {"drift": 0.0, "until": 0} for k in ASSET_SPECS.keys()}
MARKET_PRICE_HISTORY = {k: deque(maxlen=MARKET_HISTORY_MAXLEN) for k in ASSET_SPECS.keys()}

WEB_I18N = {
    "ru": {
        "nav_home": "Главная",
        "nav_markets": "Рынки",
        "nav_trade": "Торговля",
        "nav_deposit": "Пополнить",
        "nav_deals": "Сделки",
        "nav_profile": "Профиль",
        "profile_title": "Профиль аккаунта",
        "quick_trade": "Открыть сделку",
        "quick_deposit": "Пополнить",
        "quick_history": "История сделок",
        "quick_profile": "Полный профиль",
        "quick_worker": "Панель воркера",
        "live_tape": "Лента рынка (live)",
        "home_balance": "Общий баланс",
        "home_success": "Успешные",
        "home_fail": "Неуспешные",
        "home_pairs": "Популярные пары",
        "home_recent": "Последние сделки",
        "home_no_deals": "Сделок пока нет.",
        "markets_title": "Рынки",
        "trade_title": "Futures Terminal",
        "trade_balance": "Баланс",
        "trade_pair": "Пара",
        "trade_amount": "Сумма",
        "trade_leverage": "Плечо",
        "trade_expiration": "Экспирация",
        "trade_long": "ЛОНГ",
        "trade_short": "ШОРТ",
        "trade_create": "Создать сделку",
        "trade_order_book": "Order Book",
        "trade_countdown": "До завершения",
        "trade_status_open": "Сделка открыта",
        "trade_status_closed": "Сделка закрыта",
        "trade_open_positions": "Открытые позиции",
        "trade_close_now": "Закрыть сейчас",
        "deals_title": "История сделок",
        "deals_empty": "История пока пустая.",
        "deposit_title": "Пополнение баланса",
        "deposit_method": "Метод оплаты",
        "deposit_amount": "Сумма",
        "deposit_way": "Способ",
        "pay_card": "Банковская карта",
        "deposit_continue": "Продолжить",
        "deposit_crypto_hint": "Оплата по ссылке, затем заявка верификации админу.",
        "deposit_open_crypto": "Открыть Crypto Bot",
        "deposit_trc20_hint": "Адрес для перевода:",
        "deposit_not_set": "Не задан админом",
        "deposit_card_hint": "Для карты подтверждение только через поддержку.",
        "deposit_open_card": "Открыть оплату картой",
        "exchange_title": "Обмен валют",
        "exchange_from": "Отдаю",
        "exchange_to": "Получаю",
        "exchange_amount": "Сумма",
        "exchange_action": "Обменять",
        "profile_not_init_title": "Профиль не инициализирован",
        "profile_not_init_desc": "Откройте WebApp напрямую из Telegram-кнопки бота, чтобы загрузить ваш профиль.",
        "profile_user_data": "Данные пользователя",
        "profile_name": "Имя",
        "profile_username": "Username",
        "profile_lang": "Язык",
        "profile_currency": "Валюта",
        "profile_pending": "На выводе",
        "profile_total_deals": "Сделок",
        "profile_wins": "Успешные",
        "profile_recent_deposits": "Последние пополнения",
        "profile_no_deposits": "Пополнений пока нет.",
        "profile_recent_withdraws": "Последние выводы",
        "profile_no_withdraws": "Выводов пока нет.",
        "js_trade_opening": "Открываем сделку...",
        "js_trade_error": "не удалось открыть сделку",
        "js_trade_done": "Сделка завершена",
        "js_trade_started": "Сделка открыта, идет отсчет",
        "js_trade_waiting": "До завершения",
        "js_trade_balance": "Новый баланс",
        "js_trade_rate": "Курс",
        "js_reason_time": "По времени",
        "js_reason_tp": "Take Profit",
        "js_reason_sl": "Stop Loss",
        "js_reason_manual": "Ручное закрытие",
        "js_network_error": "Сетевая ошибка",
        "js_exchange_processing": "Обрабатываем обмен...",
        "js_exchange_error": "не удалось обменять",
        "js_exchange_rate": "Курс",
        "js_exchange_received": "Получено",
        "js_deposit_processing": "Обрабатываем...",
        "js_deposit_error": "не удалось отправить",
        "js_deposit_sent": "Заявка #{id} отправлена админу",
        "js_card_support_msg": "Для оплаты картой напишите в поддержку и приложите чек.",
        "js_support_btn": "Перейти в поддержку",
        "js_loading": "Загрузка...",
        "js_side_buy": "ПОКУПКА",
        "js_side_sell": "ПРОДАЖА",
        "worker_title": "Панель воркера",
        "worker_hint": "Управление вашими рефералами в реальном времени",
        "worker_empty": "У вас пока нет рефералов.",
    },
    "en": {
        "nav_home": "Home",
        "nav_markets": "Markets",
        "nav_trade": "Trade",
        "nav_deposit": "Deposit",
        "nav_deals": "Deals",
        "nav_profile": "Profile",
        "profile_title": "Account Profile",
        "quick_trade": "Open Trade",
        "quick_deposit": "Deposit",
        "quick_history": "Trade History",
        "quick_profile": "Full Profile",
        "quick_worker": "Worker Panel",
        "live_tape": "Market Tape (live)",
        "home_balance": "Total Balance",
        "home_success": "Wins",
        "home_fail": "Losses",
        "home_pairs": "Popular Pairs",
        "home_recent": "Recent Deals",
        "home_no_deals": "No deals yet.",
        "markets_title": "Markets",
        "trade_title": "Futures Terminal",
        "trade_balance": "Balance",
        "trade_pair": "Pair",
        "trade_amount": "Amount",
        "trade_leverage": "Leverage",
        "trade_expiration": "Expiration",
        "trade_long": "LONG",
        "trade_short": "SHORT",
        "trade_create": "Create Deal",
        "trade_order_book": "Order Book",
        "trade_countdown": "Time left",
        "trade_status_open": "Trade opened",
        "trade_status_closed": "Trade closed",
        "trade_open_positions": "Open Positions",
        "trade_close_now": "Close Now",
        "deals_title": "Deals History",
        "deals_empty": "No history yet.",
        "deposit_title": "Balance Top Up",
        "deposit_method": "Payment Method",
        "deposit_amount": "Amount",
        "deposit_way": "Method",
        "pay_card": "Bank Card",
        "deposit_continue": "Continue",
        "deposit_crypto_hint": "Pay by link, then send verification request to admin.",
        "deposit_open_crypto": "Open Crypto Bot",
        "deposit_trc20_hint": "Transfer address:",
        "deposit_not_set": "Not set by admin",
        "deposit_card_hint": "Card payments are confirmed via support only.",
        "deposit_open_card": "Open Card Payment",
        "exchange_title": "Currency Exchange",
        "exchange_from": "From",
        "exchange_to": "To",
        "exchange_amount": "Amount",
        "exchange_action": "Exchange",
        "profile_not_init_title": "Profile is not initialized",
        "profile_not_init_desc": "Open this WebApp directly from bot button in Telegram to load your profile.",
        "profile_user_data": "User Data",
        "profile_name": "Name",
        "profile_username": "Username",
        "profile_lang": "Language",
        "profile_currency": "Currency",
        "profile_pending": "Pending withdraw",
        "profile_total_deals": "Deals",
        "profile_wins": "Wins",
        "profile_recent_deposits": "Recent Deposits",
        "profile_no_deposits": "No deposits yet.",
        "profile_recent_withdraws": "Recent Withdrawals",
        "profile_no_withdraws": "No withdrawals yet.",
        "js_trade_opening": "Opening trade...",
        "js_trade_error": "failed to open trade",
        "js_trade_done": "Deal completed",
        "js_trade_started": "Trade opened, countdown started",
        "js_trade_waiting": "Time left",
        "js_trade_balance": "New balance",
        "js_trade_rate": "Rate",
        "js_reason_time": "By timer",
        "js_reason_tp": "Take Profit",
        "js_reason_sl": "Stop Loss",
        "js_reason_manual": "Manual close",
        "js_network_error": "Network error",
        "js_exchange_processing": "Processing exchange...",
        "js_exchange_error": "failed to exchange",
        "js_exchange_rate": "Rate",
        "js_exchange_received": "Received",
        "js_deposit_processing": "Processing...",
        "js_deposit_error": "failed to send",
        "js_deposit_sent": "Request #{id} sent to admin",
        "js_card_support_msg": "For bank card payment, contact support and attach payment proof.",
        "js_support_btn": "Open Support",
        "js_loading": "Loading...",
        "js_side_buy": "BUY",
        "js_side_sell": "SELL",
        "worker_title": "Worker Panel",
        "worker_hint": "Manage your referrals in real time",
        "worker_empty": "No referrals yet.",
    },
    "uk": {
        "nav_home": "Головна",
        "nav_markets": "Ринки",
        "nav_trade": "Торгівля",
        "nav_deposit": "Поповнити",
        "nav_deals": "Угоди",
        "nav_profile": "Профіль",
        "profile_title": "Профіль акаунта",
        "quick_trade": "Відкрити угоду",
        "quick_deposit": "Поповнити",
        "quick_history": "Історія угод",
        "quick_profile": "Повний профіль",
        "quick_worker": "Панель воркера",
        "live_tape": "Стрічка ринку (live)",
        "home_balance": "Загальний баланс",
        "home_success": "Успішні",
        "home_fail": "Неуспішні",
        "home_pairs": "Популярні пари",
        "home_recent": "Останні угоди",
        "home_no_deals": "Угод поки немає.",
        "markets_title": "Ринки",
        "trade_title": "Futures Terminal",
        "trade_balance": "Баланс",
        "trade_pair": "Пара",
        "trade_amount": "Сума",
        "trade_leverage": "Плече",
        "trade_expiration": "Експірація",
        "trade_long": "ЛОНГ",
        "trade_short": "ШОРТ",
        "trade_create": "Створити угоду",
        "trade_order_book": "Order Book",
        "trade_countdown": "До завершення",
        "trade_status_open": "Угода відкрита",
        "trade_status_closed": "Угода закрита",
        "trade_open_positions": "Відкриті позиції",
        "trade_close_now": "Закрити зараз",
        "deals_title": "Історія угод",
        "deals_empty": "Історія поки порожня.",
        "deposit_title": "Поповнення балансу",
        "deposit_method": "Метод оплати",
        "deposit_amount": "Сума",
        "deposit_way": "Спосіб",
        "pay_card": "Банківська картка",
        "deposit_continue": "Продовжити",
        "deposit_crypto_hint": "Оплата за посиланням, далі заявка на верифікацію адміну.",
        "deposit_open_crypto": "Відкрити Crypto Bot",
        "deposit_trc20_hint": "Адреса для переказу:",
        "deposit_not_set": "Не задано адміністратором",
        "deposit_card_hint": "Для картки підтвердження тільки через підтримку.",
        "deposit_open_card": "Відкрити оплату карткою",
        "exchange_title": "Обмін валют",
        "exchange_from": "Віддаю",
        "exchange_to": "Отримую",
        "exchange_amount": "Сума",
        "exchange_action": "Обміняти",
        "profile_not_init_title": "Профіль не ініціалізовано",
        "profile_not_init_desc": "Відкрийте цей WebApp напряму з кнопки бота в Telegram, щоб завантажити профіль.",
        "profile_user_data": "Дані користувача",
        "profile_name": "Ім'я",
        "profile_username": "Username",
        "profile_lang": "Мова",
        "profile_currency": "Валюта",
        "profile_pending": "На виведенні",
        "profile_total_deals": "Угод",
        "profile_wins": "Успішні",
        "profile_recent_deposits": "Останні поповнення",
        "profile_no_deposits": "Поповнень поки немає.",
        "profile_recent_withdraws": "Останні виводи",
        "profile_no_withdraws": "Виводів поки немає.",
        "js_trade_opening": "Відкриваємо угоду...",
        "js_trade_error": "не вдалося відкрити угоду",
        "js_trade_done": "Угода завершена",
        "js_trade_started": "Угода відкрита, іде відлік",
        "js_trade_waiting": "До завершення",
        "js_trade_balance": "Новий баланс",
        "js_trade_rate": "Курс",
        "js_reason_time": "За часом",
        "js_reason_tp": "Take Profit",
        "js_reason_sl": "Stop Loss",
        "js_reason_manual": "Ручне закриття",
        "js_network_error": "Мережева помилка",
        "js_exchange_processing": "Обробляємо обмін...",
        "js_exchange_error": "не вдалося обміняти",
        "js_exchange_rate": "Курс",
        "js_exchange_received": "Отримано",
        "js_deposit_processing": "Обробляємо...",
        "js_deposit_error": "не вдалося відправити",
        "js_deposit_sent": "Заявка #{id} відправлена адміну",
        "js_card_support_msg": "Для оплати карткою напишіть у підтримку та додайте чек.",
        "js_support_btn": "Перейти в підтримку",
        "js_loading": "Завантаження...",
        "js_side_buy": "КУПІВЛЯ",
        "js_side_sell": "ПРОДАЖ",
        "worker_title": "Панель воркера",
        "worker_hint": "Керуйте вашими рефералами у реальному часі",
        "worker_empty": "У вас поки немає рефералів.",
    },
}


async def acquire_polling_lock() -> bool:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url.startswith(("postgres://", "postgresql://")) or asyncpg is None:
        return True

    global polling_lock_conn
    polling_lock_conn = await asyncpg.connect(database_url)
    locked = await polling_lock_conn.fetchval("SELECT pg_try_advisory_lock($1)", POLLING_LOCK_KEY)
    return bool(locked)


async def release_polling_lock():
    global polling_lock_conn
    if polling_lock_conn is None:
        return
    try:
        await polling_lock_conn.execute("SELECT pg_advisory_unlock($1)", POLLING_LOCK_KEY)
    except Exception:
        pass
    await polling_lock_conn.close()
    polling_lock_conn = None


def resolve_public_base_url() -> str:
    explicit = os.getenv("WEBHOOK_BASE_URL", "").strip().rstrip("/")
    if explicit:
        return explicit

    static_url = os.getenv("RAILWAY_STATIC_URL", "").strip().rstrip("/")
    if static_url:
        if static_url.startswith("http://") or static_url.startswith("https://"):
            return static_url
        return f"https://{static_url}"

    public_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN", "").strip().rstrip("/")
    if public_domain:
        return f"https://{public_domain}"

    return ""


async def fetch_all(query: str, params: tuple = ()):
    async with db.connect(bot.DB_PATH) as conn:
        conn.row_factory = db.Row
        cur = await conn.execute(query, params)
        return await cur.fetchall()


async def fetch_one(query: str, params: tuple = ()):
    async with db.connect(bot.DB_PATH) as conn:
        conn.row_factory = db.Row
        cur = await conn.execute(query, params)
        return await cur.fetchone()


async def execute_query(query: str, params: tuple = ()):
    async with db.connect(bot.DB_PATH) as conn:
        await conn.execute(query, params)
        await conn.commit()


async def refresh_asset_market_map() -> dict[str, str]:
    rows = await fetch_all("SELECT name FROM ecn_assets ORDER BY id ASC")
    names = [str(r["name"]) for r in rows if r and r["name"]]
    mapping = {name: asset_to_binance_ticker(name) for name in names}
    ASSET_TICKER_MAP.clear()
    ASSET_TICKER_MAP.update(mapping)
    await MARKET_SERVICE.configure_assets(names)
    return mapping


async def ensure_webapp_user(user_data: dict):
    tg_id = int(user_data["id"])
    first_name = user_data.get("first_name")
    username = user_data.get("username")
    async with db.connect(bot.DB_PATH) as conn:
        await conn.execute(
            "INSERT OR IGNORE INTO users(tg_id, first_name, username) VALUES (?, ?, ?)",
            (tg_id, first_name, username),
        )
        await conn.execute(
            "UPDATE users SET first_name = ?, username = ? WHERE tg_id = ?",
            (first_name, username, tg_id),
        )
        await conn.commit()


async def get_settings_map() -> dict:
    rows = await fetch_all("SELECT key, value FROM settings")
    data = {row["key"]: row["value"] for row in rows}
    data.setdefault("crypto_bot_url", bot.config.crypto_bot_url or "")
    data.setdefault("trc20_address", bot.config.trc20_address or "")
    data.setdefault("card_pay_url", bot.config.card_pay_url or "")
    data.setdefault("card_requisites", bot.config.card_requisites or "")
    data.setdefault("support_url", bot.config.support_url or "")
    data.setdefault("webapp_url", bot.config.webapp_url or "")
    return data


def is_admin_session(request: Request) -> bool:
    return bool(request.session.get("is_admin"))


async def get_or_pick_user_id() -> int:
    if ALLOW_DEFAULT_TG_FALLBACK and DEFAULT_TG_ID:
        return DEFAULT_TG_ID
    return 0


async def get_current_user_id(request: Request) -> int:
    from_session = request.session.get(USER_SESSION_KEY)
    if from_session:
        return int(from_session)
    return await get_or_pick_user_id()


def deposit_method_label(method: str) -> str:
    return {
        "crypto": "Crypto bot",
        "trc20": "TRC20 USDT",
        "card": "Банковская карта",
    }.get((method or "").strip().lower(), method or "Неизвестно")


async def notify_worker_deposit_event(
    client_tg_id: int,
    first_name: str | None,
    username: str | None,
    amount: float | None,
    currency: str | None,
    method: str | None,
    stage: str,
    deposit_id: int | None = None,
):
    worker_id = await bot.get_worker_for_client(client_tg_id)
    if not worker_id:
        return
    stage_text = {
        "request_created": "создал заявку на пополнение",
        "support_opened": "перешёл в техподдержку по пополнению",
    }.get(stage, stage)
    amount_text = f"{float(amount):.2f} {(currency or 'USD')}" if amount is not None else "не указана"
    name = (first_name or "").strip() or "Пользователь"
    username_line = f"@{username}" if username else "без username"
    deposit_line = f"\nЗаявка: <b>#{deposit_id}</b>" if deposit_id else ""
    text = (
        "🔔 <b>Действие реферала по пополнению</b>\n\n"
        f"Реферал: <b>{name}</b>\n"
        f"User ID: <code>{client_tg_id}</code>\n"
        f"Username: {username_line}\n"
        f"Сумма: <b>{amount_text}</b>\n"
        f"Метод: <b>{deposit_method_label(method or 'Не указан')}</b>\n"
        f"Статус: <b>{stage_text}</b>"
        f"{deposit_line}"
    )
    await bot.create_activity_event(
        worker_tg_id=worker_id,
        client_tg_id=client_tg_id,
        actor_tg_id=client_tg_id,
        actor_source="web",
        event_type=f"deposit_{stage}",
        title="Пополнение",
        details=f"{stage_text}. Метод: {deposit_method_label(method or 'Не указан')}.",
        amount=amount,
        currency=currency or "USD",
        meta={"stage": stage, "deposit_id": deposit_id, "method": method},
    )
    try:
        await bot.bot.send_message(worker_id, text)
    except Exception:
        pass


def build_support_redirect_url(amount: float | None = None, method: str | None = None, deposit_id: int | None = None) -> str:
    params: dict[str, str] = {}
    if amount is not None:
        params["amount"] = f"{float(amount):.2f}"
    if method:
        params["method"] = method
    if deposit_id is not None:
        params["deposit_id"] = str(int(deposit_id))
    return f"/deposit/support?{urlencode(params)}" if params else "/deposit/support"


def build_worker_ref_link(worker_tg_id: int) -> str:
    username = (bot.BOT_USERNAME or "").strip()
    if not username:
        return ""
    return f"https://t.me/{username}?start=ref{worker_tg_id}"


async def log_web_activity_for_worker(
    client_tg_id: int,
    actor_tg_id: int | None,
    event_type: str,
    title: str,
    details: str = "",
    amount: float | None = None,
    currency: str | None = None,
    meta: dict | None = None,
):
    worker_id = await bot.get_worker_for_client(client_tg_id)
    if not worker_id:
        return
    await bot.create_activity_event(
        worker_tg_id=worker_id,
        client_tg_id=client_tg_id,
        actor_tg_id=actor_tg_id,
        actor_source="web",
        event_type=event_type,
        title=title,
        details=details,
        amount=amount,
        currency=currency,
        meta=meta,
    )


async def log_admin_user_action(
    target_tg_id: int,
    actor_tg_id: int | None,
    event_type: str,
    title: str,
    details: str = "",
    amount: float | None = None,
    currency: str | None = None,
    meta: dict | None = None,
):
    worker_id = await bot.get_worker_for_client(target_tg_id)
    await bot.create_activity_event(
        worker_tg_id=worker_id,
        client_tg_id=target_tg_id,
        actor_tg_id=actor_tg_id,
        actor_source="admin_web",
        event_type=event_type,
        title=title,
        details=details,
        amount=amount,
        currency=currency,
        meta=meta,
    )


def normalize_lang_code(lang: str | None) -> str:
    code = (lang or "ru").strip().lower()
    if code.startswith("en"):
        return "en"
    if code.startswith("uk"):
        return "uk"
    return "ru"


async def get_lang_and_labels(tg_id: int) -> tuple[str, dict]:
    # session override is handled in route via request.session
    if not tg_id:
        return "ru", WEB_I18N["ru"]
    row = await fetch_one("SELECT language FROM users WHERE tg_id = ?", (tg_id,))
    lang = normalize_lang_code(row["language"] if row else "ru")
    return lang, WEB_I18N.get(lang, WEB_I18N["ru"])


def labels_for_lang(lang: str) -> dict:
    return WEB_I18N.get(normalize_lang_code(lang), WEB_I18N["ru"])


async def get_request_lang_labels(request: Request, tg_id: int) -> tuple[str, dict]:
    forced = request.session.get(LANG_SESSION_KEY)
    if forced:
        lang = normalize_lang_code(forced)
        return lang, labels_for_lang(lang)
    return await get_lang_and_labels(tg_id)


def validate_telegram_init_data(init_data: str, bot_token: str) -> dict | None:
    if not init_data or not bot_token:
        return None
    try:
        pairs = dict(parse_qsl(init_data, strict_parsing=False))
    except Exception:
        return None
    incoming_hash = pairs.pop("hash", None)
    if not incoming_hash:
        return None
    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
    secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    calculated = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    if calculated != incoming_hash:
        return None
    user_raw = pairs.get("user")
    if not user_raw:
        return None
    return json.loads(user_raw)


def symbol_from_asset_name(name: str) -> str:
    n = (name or "").strip().lower()
    aliases = {
        "bitcoin": "BTC",
        "ethereum": "ETH",
        "solana": "SOL",
        "dogecoin": "DOGE",
        "litecoin": "LTC",
        "xrp": "XRP",
        "cardano": "ADA",
        "avalanche": "AVAX",
        "polkadot": "DOT",
        "chainlink": "LINK",
        "toncoin": "TON",
        "tron": "TRX",
        "polygon": "MATIC",
    }
    if n in aliases:
        return aliases[n]
    return (name[:5] if name else "UNKN").upper()


def ticker_from_asset_name(name: str) -> str:
    if name in ASSET_TICKER_MAP:
        return ASSET_TICKER_MAP[name]
    return asset_to_binance_ticker(name)


def ticker_from_symbol_input(symbol_or_asset: str) -> str:
    return MARKET_SERVICE.resolve_ticker(symbol_or_asset)


def _format_price(price: float) -> float:
    return round(float(price), 5 if float(price) < 1 else 2)


def legacy_symbol_from_symbol_or_ticker(symbol_or_asset: str) -> str:
    raw = (symbol_or_asset or "").strip()
    up = raw.upper()
    if up.endswith("USDT"):
        return ticker_to_symbol(up)
    return symbol_from_asset_name(raw)


def schedule_depth_refresh(ticker: str, levels: int = 10, min_interval: float = 2.0):
    now = time.time()
    due = float(MARKET_DEPTH_REFRESH_AT.get(ticker, 0.0))
    if now < due:
        return
    MARKET_DEPTH_REFRESH_AT[ticker] = now + max(0.2, float(min_interval))

    async def _runner():
        with contextlib.suppress(Exception):
            await MARKET_SERVICE.ensure_depth(ticker, levels=levels, max_age=min_interval)

    with contextlib.suppress(RuntimeError):
        asyncio.create_task(_runner())


async def refresh_depth_quick(ticker: str, levels: int = 10, min_interval: float = 2.0, wait_sec: float = 0.85):
    now = time.time()
    due = float(MARKET_DEPTH_REFRESH_AT.get(ticker, 0.0))
    if now < due:
        return
    MARKET_DEPTH_REFRESH_AT[ticker] = now + max(0.2, float(min_interval))
    with contextlib.suppress(Exception):
        await asyncio.wait_for(
            MARKET_SERVICE.ensure_depth(ticker, levels=levels, max_age=min_interval),
            timeout=max(0.2, float(wait_sec)),
        )


async def fetch_live_mark_or_none(symbol_or_asset: str) -> float | None:
    ticker = ticker_from_symbol_input(symbol_or_asset)
    quote = MARKET_SERVICE.get_quote(ticker)
    if quote and float(quote.get("mark") or 0) > 0:
        return float(quote["mark"])
    await refresh_depth_quick(ticker, levels=10, min_interval=0.8, wait_sec=1.1)
    quote = MARKET_SERVICE.get_quote(ticker)
    if quote and float(quote.get("mark") or 0) > 0:
        return float(quote["mark"])
    legacy_symbol = legacy_symbol_from_symbol_or_ticker(symbol_or_asset)
    return float(next_symbol_price(legacy_symbol))


def current_tape_items(limit: int = 25) -> list[dict]:
    if MARKET_SERVICE.tape:
        return list(MARKET_SERVICE.tape)[:limit]
    if MARKET_SERVICE.quote_cache:
        out = []
        now_ts = int(time.time())
        for ticker, quote in list(MARKET_SERVICE.quote_cache.items())[:limit]:
            mark = float(quote.get("mark") or 0.0)
            if mark <= 0:
                continue
            spread = float(quote.get("spread") or 0.0)
            out.append(
                {
                    "symbol": ticker_to_symbol(ticker),
                    "price": _format_price(mark),
                    "qty": round(max(0.001, spread * 10.0), 4),
                    "side": "buy" if float(quote.get("day_change") or 0.0) >= 0 else "sell",
                    "ts": now_ts,
                }
            )
        if out:
            return out[:limit]
    global MARKET_TAPE_NEXT_TS
    if not MARKET_TAPE:
        for _ in range(max(8, min(20, limit))):
            MARKET_TAPE.appendleft(generate_tape_tick())
        MARKET_TAPE_NEXT_TS = time.time() + random.choice([10, 15, 20])
    elif time.time() >= MARKET_TAPE_NEXT_TS:
        batch = 1 if random.random() < 0.72 else 2
        for _ in range(batch):
            MARKET_TAPE.appendleft(generate_tape_tick())
        MARKET_TAPE_NEXT_TS = time.time() + random.choice([10, 15, 20])
    return list(MARKET_TAPE)[:limit]


def next_symbol_price(symbol: str, ts: int | None = None) -> float:
    spec = ASSET_SPECS.get(symbol)
    if not spec:
        base = MARKET_PRICE_STATE.get(symbol, random.uniform(0.3, 800.0))
        vol = 0.006
        anchor = base
    else:
        base = MARKET_PRICE_STATE.get(symbol, spec["start"])
        vol = spec["vol"]
        anchor = spec["start"]

    tick_ts = int(ts or time.time())
    regime = MARKET_REGIME_STATE.setdefault(symbol, {"drift": 0.0, "until": 0})
    if tick_ts >= int(regime.get("until", 0)):
        regime["drift"] = random.uniform(-vol * 0.08, vol * 0.08)
        regime["until"] = tick_ts + random.randint(40, 140)

    momentum = MARKET_MOMENTUM_STATE.get(symbol, 0.0)
    regime_drift = float(regime.get("drift", 0.0))
    noise = random.uniform(-vol * 0.08, vol * 0.08)
    mean_reversion = ((anchor - base) / max(anchor, 0.00001)) * 0.006
    momentum = momentum * 0.86 + regime_drift + noise + mean_reversion
    max_step = max(0.00002, vol * 0.14)
    momentum = max(-max_step, min(max_step, momentum))

    updated = max(0.00001, base * (1 + momentum))
    min_price = max(0.00001, anchor * 0.55)
    max_price = max(min_price * 1.02, anchor * 1.65)
    if updated < min_price:
        updated = min_price * (1 + random.uniform(0.0004, 0.0025))
        momentum = abs(momentum) * 0.45
    elif updated > max_price:
        updated = max_price * (1 - random.uniform(0.0004, 0.0025))
        momentum = -abs(momentum) * 0.45
    MARKET_MOMENTUM_STATE[symbol] = momentum
    MARKET_PRICE_STATE[symbol] = updated
    stats = MARKET_DAY_STATS.setdefault(symbol, {"open": updated, "high": updated, "low": updated, "ts": tick_ts})
    if tick_ts - int(stats.get("ts", tick_ts)) > 86400:
        stats["open"] = updated
        stats["high"] = updated
        stats["low"] = updated
    stats["high"] = max(stats["high"], updated)
    stats["low"] = min(stats["low"], updated)
    stats["ts"] = tick_ts
    hist = MARKET_PRICE_HISTORY.setdefault(symbol, deque(maxlen=MARKET_HISTORY_MAXLEN))
    if hist and hist[-1]["t"] == tick_ts:
        hist[-1]["p"] = updated
    elif not hist or tick_ts > hist[-1]["t"]:
        hist.append({"t": tick_ts, "p": updated})
    return updated


async def generate_market_rows(assets) -> list[dict]:
    rows = []
    prewarm_left = 5
    for asset in assets:
        ticker = ticker_from_asset_name(asset["name"])
        quote = MARKET_SERVICE.get_quote(ticker) or {}
        if not quote and prewarm_left > 0:
            schedule_depth_refresh(ticker, levels=8, min_interval=1.5)
            prewarm_left -= 1
        price = float(quote.get("mark") or 0)
        day_change = float(quote.get("day_change") or 0.0)
        if price <= 0:
            legacy_symbol = symbol_from_asset_name(asset["name"])
            price = next_symbol_price(legacy_symbol)
            stats = MARKET_DAY_STATS.get(legacy_symbol) or {"open": price}
            day_open = float(stats.get("open", price) or price)
            day_change = round(((price - day_open) / day_open) * 100, 2) if day_open else 0.0
        symbol = ticker_to_symbol(ticker)
        rows.append(
            {
                "id": asset["id"],
                "name": asset["name"],
                "symbol": symbol,
                "price": _format_price(price),
                "day_change": round(day_change, 2),
            }
        )
    return rows


def generate_tape_tick() -> dict:
    symbol = random.choice(list(ASSET_SPECS.keys()))
    spec = ASSET_SPECS[symbol]
    price = next_symbol_price(symbol)
    qty = round(random.uniform(spec["qty_min"], spec["qty_max"]), 4)
    side = "buy" if random.random() > 0.48 else "sell"
    return {
        "symbol": symbol,
        "price": round(price, 2 if price >= 1 else 5),
        "qty": qty,
        "side": side,
        "ts": int(time.time()),
    }


def build_orderbook(symbol: str, levels: int = 10) -> tuple[list[dict], list[dict], float]:
    mark = next_symbol_price(symbol)
    state = MARKET_BOOK_STATE.get(symbol)
    if not state or len(state.get("ask_qty", [])) != levels:
        state = {
            "ask_qty": [random.uniform(0.2, 8.5 + i * 0.25) for i in range(levels)],
            "bid_qty": [random.uniform(0.2, 8.5 + i * 0.25) for i in range(levels)],
        }
        MARKET_BOOK_STATE[symbol] = state

    asks = []
    bids = []
    step = max(mark * 0.0003, 0.00001)
    for i in range(levels):
        spread = (i + 1) * step
        ask_price = mark + spread
        bid_price = max(0.00001, mark - spread)
        # Keep quantities moving slowly to avoid book flashing.
        state["ask_qty"][i] = max(0.03, state["ask_qty"][i] * (1 + random.uniform(-0.035, 0.035)))
        state["bid_qty"][i] = max(0.03, state["bid_qty"][i] * (1 + random.uniform(-0.035, 0.035)))
        asks.append(
            {
                "price": round(ask_price, 2 if ask_price >= 1 else 5),
                "qty": round(state["ask_qty"][i], 3),
            }
        )
        bids.append(
            {
                "price": round(bid_price, 2 if bid_price >= 1 else 5),
                "qty": round(state["bid_qty"][i], 3),
            }
        )
    return asks, bids, mark


def seed_symbol_history(symbol: str, points: int = 5800, step_sec: int = 5, force: bool = False):
    spec = ASSET_SPECS.get(symbol, {"start": MARKET_PRICE_STATE.get(symbol, 100.0), "vol": 0.006})
    start = float(MARKET_PRICE_STATE.get(symbol, spec["start"]))
    vol = float(spec.get("vol", 0.006))
    hist = MARKET_PRICE_HISTORY.setdefault(symbol, deque(maxlen=MARKET_HISTORY_MAXLEN))
    if hist and not force:
        return
    if force:
        hist.clear()
    points = max(300, min(MARKET_HISTORY_MAXLEN, int(points)))
    step_sec = max(2, int(step_sec))
    now = int(time.time())
    ts = now - points * step_sec
    price = start
    trend = random.uniform(-vol * 0.04, vol * 0.04)
    momentum = 0.0
    for _ in range(points):
        ts += step_sec
        trend = max(-vol * 0.08, min(vol * 0.08, trend + random.uniform(-vol * 0.01, vol * 0.01)))
        noise = random.uniform(-vol * 0.06, vol * 0.06)
        mean_reversion = ((start - price) / max(start, 0.00001)) * 0.018
        momentum = momentum * 0.9 + trend + noise + mean_reversion
        step = max(-vol * 0.12, min(vol * 0.12, momentum))
        price = max(0.00001, price * (1 + step))
        lower = max(0.00001, start * 0.55)
        upper = max(lower * 1.02, start * 1.65)
        price = max(lower, min(upper, price))
        hist.append({"t": ts, "p": price})
    MARKET_PRICE_STATE[symbol] = price
    stats = MARKET_DAY_STATS.setdefault(symbol, {"open": price, "high": price, "low": price, "ts": now})
    stats["open"] = price
    stats["high"] = price
    stats["low"] = price
    stats["ts"] = now


def build_candles_from_history(symbol: str, tf_sec: int, limit: int = 80) -> list[dict]:
    tf = max(5, int(tf_sec))
    n = max(20, min(300, int(limit)))
    history = MARKET_PRICE_HISTORY.get(symbol)
    required_span = tf * (n + 6)
    min_points = max(1200, min(MARKET_HISTORY_MAXLEN, (required_span // 5) + 240))
    if not history or len(history) < min_points:
        seed_symbol_history(symbol, points=min_points, step_sec=5, force=True)
        history = MARKET_PRICE_HISTORY.get(symbol)
    if not history:
        return []

    recent = list(history)

    buckets: dict[int, dict] = {}
    for item in recent:
        t = int(item["t"])
        p = float(item["p"])
        b = (t // tf) * tf
        c = buckets.get(b)
        if c is None:
            buckets[b] = {"t": b, "o": p, "h": p, "l": p, "c": p, "v": 0.0}
        else:
            c["h"] = max(c["h"], p)
            c["l"] = min(c["l"], p)
            c["v"] += abs(p - c["c"])
            c["c"] = p

    ordered = [buckets[k] for k in sorted(buckets.keys())]
    if not ordered:
        return []

    if len(ordered) < n:
        missing = n - len(ordered)
        spec = ASSET_SPECS.get(symbol, {"vol": 0.006})
        vol = max(0.001, float(spec.get("vol", 0.006)))
        first = ordered[0]
        price = float(first["o"])
        prefix = []
        for idx in range(missing, 0, -1):
            ts = int(first["t"]) - tf * idx
            close = max(0.00001, price * (1 + random.uniform(-vol * 1.6, vol * 1.6)))
            high = max(price, close) * (1 + abs(random.uniform(0.0, vol * 0.9)))
            low = max(0.00001, min(price, close) * (1 - abs(random.uniform(0.0, vol * 0.9))))
            prefix.append({"t": ts, "o": price, "h": high, "l": low, "c": close, "v": abs(close - price) * 900.0})
            price = close
        ordered = prefix + ordered

    out = ordered[-n:]
    spec = ASSET_SPECS.get(symbol, {"vol": 0.006})
    wick_ratio = max(0.0005, float(spec.get("vol", 0.006)) * 0.9)
    result = []
    for c in out:
        o = float(c["o"])
        close = float(c["c"])
        hi = max(float(c["h"]), o, close)
        lo = min(float(c["l"]), o, close)
        body = max(abs(close - o), max(abs(o), abs(close), 0.00001) * wick_ratio * 0.35, 0.00001)
        hi = max(hi, max(o, close) + body * random.uniform(0.08, 0.45))
        lo = min(lo, min(o, close) - body * random.uniform(0.08, 0.45))
        result.append(
            {
                "t": c["t"],
                "o": round(o, 6),
                "h": round(max(hi, o, close), 6),
                "l": round(max(0.00001, min(lo, o, close)), 6),
                "c": round(close, 6),
                "v": round(max(1.0, float(c.get("v", 0.0)) * 700.0 + body * 1800.0), 2),
            }
        )
    return result


async def market_feed_loop():
    while True:
        batch = random.choice([1, 1, 2])  # 1-2 trades per cycle
        for idx in range(batch):
            MARKET_TAPE.appendleft(generate_tape_tick())
            if idx < batch - 1:
                await asyncio.sleep(0.8)
        await asyncio.sleep(random.choice([10, 15, 20]))


async def market_price_loop():
    while True:
        ts = int(time.time())
        for symbol in ASSET_SPECS.keys():
            next_symbol_price(symbol, ts=ts)
        await asyncio.sleep(2.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global polling_task, market_feed_task, market_price_task
    await bot.init_db()
    await refresh_asset_market_map()
    await MARKET_SERVICE.start()
    if MARKET_DEV_FALLBACK:
        for symbol in ASSET_SPECS.keys():
            seed_symbol_history(symbol)
        if not MARKET_TAPE:
            for _ in range(30):
                MARKET_TAPE.appendleft(generate_tape_tick())
        market_feed_task = asyncio.create_task(market_feed_loop())
        market_price_task = asyncio.create_task(market_price_loop())
    if RUN_BOT:
        me = await bot.bot.get_me()
        bot.BOT_USERNAME = me.username

        if BOT_MODE == "webhook":
            base_url = resolve_public_base_url()
            if not base_url:
                raise RuntimeError("Webhook mode enabled, but WEBHOOK_BASE_URL/RAILWAY_STATIC_URL is not set")
            webhook_url = f"{base_url}{WEBHOOK_PATH}"
            await bot.bot.set_webhook(
                url=webhook_url,
                secret_token=WEBHOOK_SECRET if WEBHOOK_SECRET else None,
                drop_pending_updates=True,
            )
            print(f"Bot webhook set: {webhook_url}")
        else:
            can_start_polling = await acquire_polling_lock()
            if can_start_polling:
                await bot.bot.delete_webhook(drop_pending_updates=True)
                print(f"Bot polling started as @{bot.BOT_USERNAME}")
                polling_task = asyncio.create_task(bot.dp.start_polling(bot.bot))
            else:
                print("Polling lock is busy: another instance already runs bot polling. Starting web only.")
    yield
    if polling_task:
        polling_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await polling_task
    if market_feed_task:
        market_feed_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await market_feed_task
        market_feed_task = None
    if market_price_task:
        market_price_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await market_price_task
        market_price_task = None
    await MARKET_SERVICE.stop()
    await release_polling_lock()
    await bot.bot.session.close()


app = FastAPI(title="Legend Trading", lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET, same_site="lax", https_only=True)
templates = Jinja2Templates(directory=str(WEB_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(WEB_DIR / "static")), name="static")


@app.post(WEBHOOK_PATH, response_class=JSONResponse)
async def telegram_webhook(request: Request):
    if WEBHOOK_SECRET:
        incoming_secret = request.headers.get("x-telegram-bot-api-secret-token", "")
        if incoming_secret != WEBHOOK_SECRET:
            return JSONResponse({"ok": False, "error": "invalid secret"}, status_code=403)
    update_data = await request.json()
    update = Update.model_validate(update_data)
    await bot.dp.feed_update(bot.bot, update)
    return JSONResponse({"ok": True})


class TelegramAuthPayload(BaseModel):
    init_data: str


@app.post("/api/auth/telegram", response_class=JSONResponse)
async def telegram_auth(payload: TelegramAuthPayload, request: Request):
    user = validate_telegram_init_data(payload.init_data, bot.config.bot_token)
    if not user:
        return JSONResponse({"ok": False, "error": "Invalid initData"}, status_code=401)
    await ensure_webapp_user(user)
    request.session[USER_SESSION_KEY] = int(user["id"])
    return JSONResponse({"ok": True, "tg_id": int(user["id"])})


class SetLangPayload(BaseModel):
    lang: str


@app.post("/api/lang", response_class=JSONResponse)
async def api_set_lang(payload: SetLangPayload, request: Request):
    lang = normalize_lang_code(payload.lang)
    request.session[LANG_SESSION_KEY] = lang
    return JSONResponse({"ok": True, "lang": lang})


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    tg_id = await get_current_user_id(request)
    lang, labels = await get_request_lang_labels(request, tg_id)
    user = await fetch_one("SELECT * FROM users WHERE tg_id = ?", (tg_id,))
    deals = await fetch_all(
        "SELECT id, asset_name, direction, amount, currency, is_win, profit, created_at FROM deals WHERE user_tg_id = ? ORDER BY id DESC LIMIT 5",
        (tg_id,),
    )
    stats = await bot.get_user_deal_stats(tg_id) if tg_id else {"wins": 0, "losses": 0, "total": 0, "total_profit": 0.0}
    assets = await fetch_all("SELECT id, name FROM ecn_assets ORDER BY id ASC")
    markets = await generate_market_rows(assets)
    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "page": "home",
            "title": "Legend Trading",
            "tg_id": tg_id,
            "user": user,
            "lang": lang,
            "labels": labels,
            "stats": stats,
            "markets": markets[:10],
            "deals": deals,
            "tape": current_tape_items(20),
        },
    )


@app.get("/markets", response_class=HTMLResponse)
async def markets(request: Request):
    tg_id = await get_current_user_id(request)
    lang, labels = await get_request_lang_labels(request, tg_id)
    assets = await fetch_all("SELECT id, name FROM ecn_assets ORDER BY id ASC")
    return templates.TemplateResponse(
        "markets.html",
        {"request": request, "page": "markets", "title": "Legend Trading", "markets": await generate_market_rows(assets), "lang": lang, "labels": labels},
    )


@app.get("/api/markets/live", response_class=JSONResponse)
async def api_markets_live():
    assets = await fetch_all("SELECT id, name FROM ecn_assets ORDER BY id ASC LIMIT 200")
    rows = await generate_market_rows(assets)
    return JSONResponse({"ok": True, "items": rows})


@app.get("/trade", response_class=HTMLResponse)
async def trade(request: Request):
    tg_id = await get_current_user_id(request)
    lang, labels = await get_request_lang_labels(request, tg_id)
    raw_assets = await fetch_all("SELECT id, name FROM ecn_assets ORDER BY id ASC")
    assets = [{"id": a["id"], "name": a["name"], "ticker": ticker_from_asset_name(a["name"])} for a in raw_assets]
    user = await fetch_one("SELECT balance, currency FROM users WHERE tg_id = ?", (tg_id,))
    return templates.TemplateResponse(
        "trade.html",
        {
            "request": request,
            "page": "trade",
            "title": "Legend Trading",
            "tg_id": tg_id,
            "assets": assets,
            "user": user,
            "lang": lang,
            "labels": labels,
        },
    )


@app.get("/trade/chart", response_class=HTMLResponse)
async def trade_chart(request: Request):
    tg_id = await get_current_user_id(request)
    lang, labels = await get_request_lang_labels(request, tg_id)
    raw_assets = await fetch_all("SELECT id, name FROM ecn_assets ORDER BY id ASC")
    assets = [{"id": a["id"], "name": a["name"], "ticker": ticker_from_asset_name(a["name"])} for a in raw_assets]
    selected = (request.query_params.get("symbol") or "").strip()
    if not selected and assets:
        selected = assets[0]["name"]
    return templates.TemplateResponse(
        "trade_chart.html",
        {
            "request": request,
            "page": "trade",
            "title": "Legend Trading Chart",
            "tg_id": tg_id,
            "assets": assets,
            "selected_symbol": selected,
            "lang": lang,
            "labels": labels,
        },
    )


@app.get("/exchange", response_class=HTMLResponse)
async def exchange(request: Request):
    tg_id = await get_current_user_id(request)
    lang, labels = await get_request_lang_labels(request, tg_id)
    user = await fetch_one("SELECT balance, currency FROM users WHERE tg_id = ?", (tg_id,))
    return templates.TemplateResponse(
        "exchange.html",
        {"request": request, "page": "exchange", "title": "Legend Trading", "tg_id": tg_id, "user": user, "lang": lang, "labels": labels},
    )


@app.get("/deposit", response_class=HTMLResponse)
async def deposit_page(request: Request):
    tg_id = await get_current_user_id(request)
    lang, labels = await get_request_lang_labels(request, tg_id)
    user = await fetch_one("SELECT balance, currency FROM users WHERE tg_id = ?", (tg_id,))
    return templates.TemplateResponse(
        "deposit.html",
        {
            "request": request,
            "page": "deposit",
            "title": "Legend Trading",
            "tg_id": tg_id,
            "user": user,
            "crypto_url": bot.config.crypto_bot_url,
            "trc20_address": bot.config.trc20_address,
            "card_pay_url": bot.config.card_pay_url,
            "card_requisites": bot.config.card_requisites,
            "support_url": bot.config.support_url,
            "support_entry_url": build_support_redirect_url(),
            "support_contact": bot.support_contact_text(),
            "lang": lang,
            "labels": labels,
        },
    )


@app.get("/deposit/support")
async def deposit_support_redirect(
    request: Request,
    amount: float | None = None,
    method: str | None = None,
    deposit_id: int | None = None,
):
    tg_id = await get_current_user_id(request)
    if tg_id:
        user = await fetch_one("SELECT first_name, username, currency FROM users WHERE tg_id = ?", (tg_id,))
        await notify_worker_deposit_event(
            client_tg_id=tg_id,
            first_name=user["first_name"] if user else None,
            username=user["username"] if user else None,
            amount=amount,
            currency=(user["currency"] if user and user["currency"] else "USD"),
            method=method,
            stage="support_opened",
            deposit_id=deposit_id,
        )
    target = (bot.config.support_url or "").strip() or "/deposit"
    return RedirectResponse(url=target, status_code=302)


@app.get("/deals", response_class=HTMLResponse)
async def deals(request: Request):
    tg_id = await get_current_user_id(request)
    lang, labels = await get_request_lang_labels(request, tg_id)
    rows = await fetch_all(
        "SELECT id, asset_name, direction, amount, currency, is_win, profit, created_at FROM deals WHERE user_tg_id = ? ORDER BY id DESC LIMIT 200",
        (tg_id,),
    )
    return templates.TemplateResponse(
        "deals.html",
        {"request": request, "page": "deals", "title": "Legend Trading", "deals": rows, "tg_id": tg_id, "lang": lang, "labels": labels, "tape": current_tape_items(25)},
    )


@app.get("/profile", response_class=HTMLResponse)
async def profile_page(request: Request):
    tg_id = await get_current_user_id(request)
    lang, labels = await get_request_lang_labels(request, tg_id)
    if not tg_id:
        return templates.TemplateResponse(
            "profile.html",
            {"request": request, "page": "profile", "title": "Legend Trading", "user": None, "stats": {"total": 0, "wins": 0, "losses": 0, "total_profit": 0.0}, "pending": 0.0, "tg_id": 0, "withdrawals": [], "deposits": [], "lang": lang, "labels": labels},
        )
    user = await fetch_one(
        "SELECT tg_id, first_name, username, language, currency, balance, is_worker, created_at FROM users WHERE tg_id = ?",
        (tg_id,),
    )
    stats = await bot.get_user_deal_stats(tg_id) if tg_id else {"wins": 0, "losses": 0, "total": 0, "total_profit": 0.0}
    pending = await bot.get_user_pending_withdraw_sum(tg_id) if tg_id else 0.0
    withdrawals = await fetch_all(
        "SELECT amount, currency, method, status, created_at FROM withdrawals WHERE user_tg_id = ? ORDER BY id DESC LIMIT 8",
        (tg_id,),
    )
    deposits = await fetch_all(
        "SELECT amount, currency, method, status, created_at FROM deposit_requests WHERE user_tg_id = ? ORDER BY id DESC LIMIT 8",
        (tg_id,),
    )
    return templates.TemplateResponse(
        "profile.html",
        {
            "request": request,
            "page": "profile",
            "title": "Legend Trading",
            "user": user,
            "stats": stats,
            "pending": pending,
            "tg_id": tg_id,
            "withdrawals": withdrawals,
            "deposits": deposits,
            "lang": lang,
            "labels": labels,
        },
    )


@app.get("/worker", response_class=HTMLResponse)
async def worker_page(request: Request):
    tg_id = await get_current_user_id(request)
    if not tg_id:
        return RedirectResponse(url="/", status_code=302)

    user = await fetch_one("SELECT is_worker FROM users WHERE tg_id = ?", (tg_id,))
    if not user or not bool(user["is_worker"]):
        return RedirectResponse(url="/profile", status_code=302)

    lang, labels = await get_request_lang_labels(request, tg_id)
    rows = await fetch_all(
        """
        SELECT wc.id, wc.client_tg_id, wc.min_deposit, wc.min_withdraw, wc.verified, wc.withdraw_enabled,
               wc.trading_enabled, wc.favorite, wc.blocked, u.first_name, u.username, u.balance, u.currency
        FROM worker_clients wc
        LEFT JOIN users u ON u.tg_id = wc.client_tg_id
        WHERE wc.worker_tg_id = ?
        ORDER BY wc.id DESC
        LIMIT 200
        """,
        (tg_id,),
    )
    activity = await bot.get_worker_activity_events(tg_id, 24)
    return templates.TemplateResponse(
        "worker.html",
        {
            "request": request,
            "page": "worker",
            "title": "Панель воркера | Legend Trading",
            "lang": lang,
            "labels": labels,
            "worker_id": tg_id,
            "worker_ref_link": build_worker_ref_link(tg_id),
            "clients": rows,
            "activity": activity,
        },
    )


@app.get("/worker/client/{wc_id}", response_class=HTMLResponse)
async def worker_client_page(request: Request, wc_id: int):
    tg_id = await get_current_user_id(request)
    if not tg_id:
        return RedirectResponse(url="/", status_code=302)

    user = await fetch_one("SELECT is_worker FROM users WHERE tg_id = ?", (tg_id,))
    if not user or not bool(user["is_worker"]):
        return RedirectResponse(url="/profile", status_code=302)

    lang, labels = await get_request_lang_labels(request, tg_id)
    client = await fetch_one(
        """
        SELECT wc.id, wc.worker_tg_id, wc.client_tg_id, wc.min_deposit, wc.min_withdraw, wc.verified, wc.withdraw_enabled,
               wc.trading_enabled, wc.favorite, wc.blocked, wc.created_at,
               u.first_name, u.username, u.language, u.currency, u.balance, u.created_at AS user_created_at
        FROM worker_clients wc
        LEFT JOIN users u ON u.tg_id = wc.client_tg_id
        WHERE wc.id = ? AND wc.worker_tg_id = ?
        LIMIT 1
        """,
        (wc_id, tg_id),
    )
    if not client:
        return RedirectResponse(url="/worker", status_code=302)

    client_tg_id = int(client["client_tg_id"])
    stats = await bot.get_user_deal_stats(client_tg_id)
    pending = await bot.get_user_pending_withdraw_sum(client_tg_id)
    luck = await bot.get_luck_for_worker_client(tg_id, client_tg_id)
    deposits = await fetch_all(
        "SELECT id, amount, currency, method, status, created_at FROM deposit_requests WHERE user_tg_id = ? ORDER BY id DESC LIMIT 20",
        (client_tg_id,),
    )
    withdrawals = await fetch_all(
        "SELECT id, amount, currency, method, status, created_at FROM withdrawals WHERE user_tg_id = ? ORDER BY id DESC LIMIT 20",
        (client_tg_id,),
    )
    deals = await fetch_all(
        "SELECT id, asset_name, direction, amount, currency, profit, is_win, created_at FROM deals WHERE user_tg_id = ? ORDER BY id DESC LIMIT 20",
        (client_tg_id,),
    )
    activity = await fetch_all(
        """
        SELECT id, title, details, amount, currency, created_at, actor_source, event_type
        FROM activity_log
        WHERE worker_tg_id = ? AND client_tg_id = ?
        ORDER BY id DESC
        LIMIT 40
        """,
        (tg_id, client_tg_id),
    )
    return templates.TemplateResponse(
        "worker_client.html",
        {
            "request": request,
            "page": "worker",
            "title": "Карточка реферала | Legend Trading",
            "lang": lang,
            "labels": labels,
            "worker_id": tg_id,
            "client": client,
            "stats": stats,
            "pending": pending,
            "luck": luck,
            "deposits": deposits,
            "withdrawals": withdrawals,
            "deals": deals,
            "activity": activity,
        },
    )


@app.get("/api/worker/clients", response_class=JSONResponse)
async def api_worker_clients(request: Request):
    tg_id = await get_current_user_id(request)
    user = await fetch_one("SELECT is_worker FROM users WHERE tg_id = ?", (tg_id,))
    if not user or not bool(user["is_worker"]):
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)
    rows = await fetch_all(
        """
        SELECT wc.id, wc.client_tg_id, wc.min_deposit, wc.min_withdraw, wc.verified, wc.withdraw_enabled,
               wc.trading_enabled, wc.favorite, wc.blocked, u.first_name, u.username, u.balance, u.currency
        FROM worker_clients wc
        LEFT JOIN users u ON u.tg_id = wc.client_tg_id
        WHERE wc.worker_tg_id = ?
        ORDER BY wc.id DESC
        LIMIT 200
        """,
        (tg_id,),
    )
    items = [dict(r) for r in rows]
    return JSONResponse({"ok": True, "items": items})


class WorkerClientUpdatePayload(BaseModel):
    wc_id: int
    action: str
    value: float | int | None = None


@app.post("/api/worker/client/update", response_class=JSONResponse)
async def api_worker_client_update(payload: WorkerClientUpdatePayload, request: Request):
    tg_id = await get_current_user_id(request)
    user = await fetch_one("SELECT is_worker FROM users WHERE tg_id = ?", (tg_id,))
    if not user or not bool(user["is_worker"]):
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)

    wc = await fetch_one(
        "SELECT id, client_tg_id, worker_tg_id, verified, withdraw_enabled, trading_enabled, favorite, blocked "
        "FROM worker_clients WHERE id = ?",
        (payload.wc_id,),
    )
    if not wc or int(wc["worker_tg_id"]) != int(tg_id):
        return JSONResponse({"ok": False, "error": "Client not found"}, status_code=404)

    action = payload.action.strip().lower()
    activity_title = "Изменение реферала"
    activity_details = ""
    activity_amount = None
    if action == "toggle_verified":
        new_val = 0 if wc["verified"] else 1
        await bot.update_worker_client_field(payload.wc_id, "verified", new_val)
        activity_title = "KYC реферала"
        activity_details = "Верификация включена" if new_val else "Верификация отключена"
    elif action == "toggle_withdraw":
        new_val = 0 if wc["withdraw_enabled"] else 1
        await bot.update_worker_client_field(payload.wc_id, "withdraw_enabled", new_val)
        activity_title = "Вывод реферала"
        activity_details = "Вывод разрешён" if new_val else "Вывод отключён"
    elif action == "toggle_trade":
        new_val = 0 if wc["trading_enabled"] else 1
        await bot.update_worker_client_field(payload.wc_id, "trading_enabled", new_val)
        activity_title = "Торговля реферала"
        activity_details = "Торговля разрешена" if new_val else "Торговля отключена"
    elif action == "toggle_favorite":
        new_val = 0 if wc["favorite"] else 1
        await bot.update_worker_client_field(payload.wc_id, "favorite", new_val)
        activity_title = "Избранное"
        activity_details = "Реферал добавлен в избранное" if new_val else "Реферал убран из избранного"
    elif action == "toggle_block":
        new_val = 0 if wc["blocked"] else 1
        await bot.update_worker_client_field(payload.wc_id, "blocked", new_val)
        activity_title = "Блокировка реферала"
        activity_details = "Реферал заблокирован" if new_val else "Реферал разблокирован"
    elif action == "set_min_deposit":
        val = float(payload.value or 0)
        if val < 0:
            return JSONResponse({"ok": False, "error": "Value must be >= 0"}, status_code=400)
        await bot.update_worker_client_field(payload.wc_id, "min_deposit", val)
        activity_title = "Мин. депозит"
        activity_details = f"Установлен минимальный депозит {val:.2f}"
        activity_amount = val
    elif action == "set_min_withdraw":
        val = float(payload.value or 0)
        if val < 0:
            return JSONResponse({"ok": False, "error": "Value must be >= 0"}, status_code=400)
        await bot.update_worker_client_field(payload.wc_id, "min_withdraw", val)
        activity_title = "Мин. вывод"
        activity_details = f"Установлен минимальный вывод {val:.2f}"
        activity_amount = val
    elif action == "set_luck":
        val = float(payload.value or 0)
        if val < 0 or val > 100:
            return JSONResponse({"ok": False, "error": "Luck must be in 0..100"}, status_code=400)
        await bot.set_client_luck(tg_id, int(wc["client_tg_id"]), val)
        activity_title = "Удача реферала"
        activity_details = f"Значение удачи изменено на {val:.2f}%"
    elif action == "add_balance":
        val = float(payload.value or 0)
        if val <= 0:
            return JSONResponse({"ok": False, "error": "Amount must be > 0"}, status_code=400)
        await bot.change_balance(int(wc["client_tg_id"]), val)
        activity_title = "Пополнение баланса"
        activity_details = f"Воркер добавил баланс {val:.2f}"
        activity_amount = val
    else:
        return JSONResponse({"ok": False, "error": "Unsupported action"}, status_code=400)

    await log_web_activity_for_worker(
        client_tg_id=int(wc["client_tg_id"]),
        actor_tg_id=tg_id,
        event_type=f"worker_{action}",
        title=activity_title,
        details=activity_details,
        amount=activity_amount,
        meta={"action": action, "wc_id": int(payload.wc_id)},
    )

    return JSONResponse({"ok": True})


@app.get("/admin/login", response_class=HTMLResponse)
async def admin_login_page(request: Request):
    if is_admin_session(request):
        return RedirectResponse(url="/admin", status_code=302)
    return templates.TemplateResponse("admin_login.html", {"request": request, "title": "Legend Trading Admin"})


class AdminLoginPayload(BaseModel):
    username: str
    password: str


@app.post("/admin/login", response_class=JSONResponse)
async def admin_login(payload: AdminLoginPayload, request: Request):
    if payload.username == ADMIN_WEB_USERNAME and payload.password == ADMIN_WEB_PASSWORD and ADMIN_WEB_PASSWORD:
        request.session["is_admin"] = True
        return JSONResponse({"ok": True})
    return JSONResponse({"ok": False, "error": "Invalid credentials"}, status_code=401)


@app.post("/admin/logout", response_class=JSONResponse)
async def admin_logout(request: Request):
    request.session.clear()
    return JSONResponse({"ok": True})


@app.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    if not is_admin_session(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    users = await fetch_one("SELECT COUNT(*) AS c FROM users")
    workers = await fetch_one("SELECT COUNT(*) AS c FROM users WHERE is_worker = 1")
    deals = await fetch_one("SELECT COUNT(*) AS c FROM deals")
    withdrawals_pending = await fetch_one("SELECT COUNT(*) AS c FROM withdrawals WHERE status = 'pending'")
    settings_map = await get_settings_map()
    assets = await fetch_all("SELECT id, name FROM ecn_assets ORDER BY id ASC LIMIT 200")
    users_rows = await fetch_all(
        """
        SELECT tg_id, first_name, username, language, currency, balance, is_admin, is_worker, created_at
        FROM users
        ORDER BY id DESC
        LIMIT 300
        """
    )
    return templates.TemplateResponse(
        "admin_dashboard.html",
        {
            "request": request,
            "title": "Legend Trading Admin",
            "users_count": int(users["c"] if users else 0),
            "workers_count": int(workers["c"] if workers else 0),
            "deals_count": int(deals["c"] if deals else 0),
            "pending_withdrawals": int(withdrawals_pending["c"] if withdrawals_pending else 0),
            "settings_map": settings_map,
            "assets": assets,
            "users_rows": users_rows,
        },
    )


@app.get("/admin/user/{tg_id}", response_class=HTMLResponse)
async def admin_user_page(request: Request, tg_id: int):
    if not is_admin_session(request):
        return RedirectResponse(url="/admin/login", status_code=302)
    user = await fetch_one(
        "SELECT tg_id, first_name, username, language, currency, balance, is_admin, is_worker, created_at FROM users WHERE tg_id = ?",
        (tg_id,),
    )
    if not user:
        return RedirectResponse(url="/admin", status_code=302)
    stats = await bot.get_user_deal_stats(tg_id)
    pending = await bot.get_user_pending_withdraw_sum(tg_id)
    worker_id = await bot.get_worker_for_client(tg_id)
    deposits = await fetch_all(
        "SELECT id, amount, currency, method, status, created_at FROM deposit_requests WHERE user_tg_id = ? ORDER BY id DESC LIMIT 20",
        (tg_id,),
    )
    withdrawals = await fetch_all(
        "SELECT id, amount, currency, method, status, created_at FROM withdrawals WHERE user_tg_id = ? ORDER BY id DESC LIMIT 20",
        (tg_id,),
    )
    deals = await fetch_all(
        "SELECT id, asset_name, direction, amount, currency, profit, is_win, created_at FROM deals WHERE user_tg_id = ? ORDER BY id DESC LIMIT 20",
        (tg_id,),
    )
    activity = await fetch_all(
        """
        SELECT id, title, details, amount, currency, created_at, actor_source, event_type
        FROM activity_log
        WHERE client_tg_id = ?
        ORDER BY id DESC
        LIMIT 40
        """,
        (tg_id,),
    )
    return templates.TemplateResponse(
        "admin_user.html",
        {
            "request": request,
            "title": "Карточка пользователя | Legend Trading Admin",
            "user": user,
            "stats": stats,
            "pending": pending,
            "worker_id": worker_id,
            "deposits": deposits,
            "withdrawals": withdrawals,
            "deals": deals,
            "activity": activity,
        },
    )


class AdminSettingPayload(BaseModel):
    key: str
    value: str


@app.post("/admin/api/settings", response_class=JSONResponse)
async def admin_update_setting(payload: AdminSettingPayload, request: Request):
    if not is_admin_session(request):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    allowed_keys = {
        "crypto_bot_url",
        "trc20_address",
        "card_pay_url",
        "card_requisites",
        "support_url",
        "webapp_url",
    }
    key = payload.key.strip()
    if key not in allowed_keys:
        return JSONResponse({"ok": False, "error": "Unsupported key"}, status_code=400)
    await bot.set_setting(key, payload.value.strip())
    return JSONResponse({"ok": True})


class AdminAssetPayload(BaseModel):
    name: str


@app.post("/admin/api/assets", response_class=JSONResponse)
async def admin_add_asset(payload: AdminAssetPayload, request: Request):
    if not is_admin_session(request):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    name = payload.name.strip()
    if len(name) < 2:
        return JSONResponse({"ok": False, "error": "Asset name too short"}, status_code=400)
    await bot.add_ecn_asset(name)
    await refresh_asset_market_map()
    return JSONResponse({"ok": True, "name": name})


class AdminUserActionPayload(BaseModel):
    tg_id: int
    action: str
    value: float | int | None = None


@app.post("/admin/api/user/action", response_class=JSONResponse)
async def admin_user_action(payload: AdminUserActionPayload, request: Request):
    if not is_admin_session(request):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    target = await fetch_one(
        "SELECT tg_id, first_name, username, currency, balance, is_admin, is_worker FROM users WHERE tg_id = ?",
        (payload.tg_id,),
    )
    if not target:
        return JSONResponse({"ok": False, "error": "User not found"}, status_code=404)

    action = payload.action.strip().lower()
    details = ""
    amount = None
    currency = target["currency"] or "USD"
    if action == "toggle_worker":
        new_val = 0 if target["is_worker"] else 1
        await bot.set_worker_flag(int(payload.tg_id), bool(new_val))
        details = "Права воркера включены" if new_val else "Права воркера отключены"
    elif action == "toggle_admin":
        new_val = 0 if target["is_admin"] else 1
        await execute_query("UPDATE users SET is_admin = ? WHERE tg_id = ?", (new_val, payload.tg_id))
        details = "Права админа включены" if new_val else "Права админа отключены"
    elif action == "add_balance":
        amount = float(payload.value or 0)
        if amount == 0:
            return JSONResponse({"ok": False, "error": "Amount must not be 0"}, status_code=400)
        await bot.change_balance(int(payload.tg_id), amount)
        details = f"Баланс изменён на {amount:+.2f} {currency}"
        try:
            await bot.bot.send_message(int(payload.tg_id), f"💰 Администратор изменил ваш баланс на {amount:+.2f} {currency}")
        except Exception:
            pass
    elif action == "set_balance":
        amount = float(payload.value or 0)
        current_balance = float(target["balance"] or 0)
        delta = amount - current_balance
        await bot.change_balance(int(payload.tg_id), delta)
        details = f"Баланс установлен на {amount:.2f} {currency}"
        try:
            await bot.bot.send_message(int(payload.tg_id), f"💰 Администратор установил ваш баланс: {amount:.2f} {currency}")
        except Exception:
            pass
    else:
        return JSONResponse({"ok": False, "error": "Unsupported action"}, status_code=400)

    await log_admin_user_action(
        target_tg_id=int(payload.tg_id),
        actor_tg_id=None,
        event_type=f"admin_{action}",
        title="Действие администратора",
        details=details,
        amount=amount,
        currency=currency,
        meta={"action": action},
    )
    return JSONResponse({"ok": True, "details": details})


class TradeOpenPayload(BaseModel):
    tg_id: int
    asset_name: str
    direction: str
    amount: float
    seconds: int
    leverage: int = 10
    risk_percent: float | None = None
    tp_percent: float | None = None
    sl_percent: float | None = None


@app.post("/api/trade/open", response_class=JSONResponse)
async def api_trade_open(
    payload: TradeOpenPayload,
):
    tg_id = payload.tg_id
    asset_name = payload.asset_name
    direction = payload.direction
    amount = payload.amount
    seconds = payload.seconds
    leverage = payload.leverage

    if direction not in {"up", "down"}:
        return JSONResponse({"ok": False, "error": "Некорректное направление сделки"}, status_code=400)
    if seconds not in {10, 30, 60, 300}:
        return JSONResponse({"ok": False, "error": "Некорректная экспирация"}, status_code=400)
    if leverage < 1 or leverage > 50:
        return JSONResponse({"ok": False, "error": "Плечо должно быть от 1 до 50"}, status_code=400)

    user = await fetch_one("SELECT balance, currency FROM users WHERE tg_id = ?", (tg_id,))
    if not user:
        return JSONResponse({"ok": False, "error": "Пользователь не найден"}, status_code=404)
    balance = float(user["balance"] or 0.0)
    currency = user["currency"] or "USD"

    risk_percent = float(payload.risk_percent or 0.0)
    if risk_percent > 0:
        amount = round(balance * (risk_percent / 100.0), 2)

    if amount < 100:
        return JSONResponse({"ok": False, "error": "Минимальная сумма сделки: 100"}, status_code=400)
    if amount > balance:
        return JSONResponse({"ok": False, "error": "Недостаточно средств"}, status_code=400)

    start_mark = await fetch_live_mark_or_none(asset_name)
    if start_mark is None or start_mark <= 0:
        return JSONResponse({"ok": False, "error": "Рынок временно недоступен"}, status_code=503)

    await bot.change_balance(tg_id, -amount)
    ticker = ticker_from_asset_name(asset_name)
    start_price = _format_price(start_mark)
    tp_percent = max(0.0, float(payload.tp_percent or 0.0))
    sl_percent = max(0.0, float(payload.sl_percent or 0.0))
    tp_price = None
    sl_price = None
    if tp_percent > 0:
        if direction == "up":
            tp_price = start_price * (1 + tp_percent / 100.0)
        else:
            tp_price = start_price * (1 - tp_percent / 100.0)
    if sl_percent > 0:
        if direction == "up":
            sl_price = start_price * (1 - sl_percent / 100.0)
        else:
            sl_price = start_price * (1 + sl_percent / 100.0)
    now_ts = time.time()
    trade_id = secrets.token_hex(8)
    ACTIVE_WEB_TRADES[trade_id] = {
        "id": trade_id,
        "status": "open",
        "tg_id": tg_id,
        "asset_name": asset_name,
        "ticker": ticker,
        "direction": direction,
        "amount": amount,
        "seconds": int(seconds),
        "leverage": int(leverage),
        "currency": currency,
        "start_price": start_price,
        "opened_ts": now_ts,
        "close_ts": now_ts + seconds,
        "tp_price": tp_price,
        "sl_price": sl_price,
        "risk_percent": risk_percent,
        "close_reason": "time",
    }
    await log_web_activity_for_worker(
        client_tg_id=tg_id,
        actor_tg_id=tg_id,
        event_type="trade_opened",
        title="Открыта сделка",
        details=f"{'ЛОНГ' if direction == 'up' else 'ШОРТ'} по {asset_name}, плечо {int(leverage)}x, экспирация {int(seconds)}с",
        amount=amount,
        currency=currency,
        meta={"asset_name": asset_name, "direction": direction, "leverage": int(leverage), "seconds": int(seconds), "trade_id": trade_id},
    )
    return JSONResponse(
        {
            "ok": True,
            "trade_id": trade_id,
            "status": "open",
            "start_price": start_price,
            "close_at": int(now_ts + seconds),
            "seconds": int(seconds),
            "amount": amount,
        }
    )


@app.get("/api/trade/status", response_class=JSONResponse)
async def api_trade_status(trade_id: str, tg_id: int):
    trade = ACTIVE_WEB_TRADES.get(trade_id)
    if not trade:
        return JSONResponse({"ok": False, "error": "Trade not found"}, status_code=404)
    if int(trade["tg_id"]) != int(tg_id):
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)

    trade = await settle_web_trade(trade_id)
    if not trade:
        return JSONResponse({"ok": False, "error": "Trade not found"}, status_code=404)

    remaining = max(0, int(trade["close_ts"] - time.time()))
    payload = {
        "ok": True,
        "trade_id": trade_id,
        "status": trade["status"],
        "remaining": remaining,
        "start_price": trade["start_price"],
    }
    if trade["status"] == "closed":
        payload.update(
            {
                "is_win": bool(trade["is_win"]),
                "profit": float(trade["profit"]),
                "balance": float(trade["balance"]),
                "end_price": float(trade["end_price"]),
                "change_percent": float(trade["change_percent"]),
                "close_reason": trade.get("close_reason", "time"),
            }
        )
    return JSONResponse(payload)


class ExchangePayload(BaseModel):
    tg_id: int
    from_currency: str
    to_currency: str
    amount: float


@app.post("/api/exchange", response_class=JSONResponse)
async def api_exchange(
    payload: ExchangePayload,
):
    tg_id = payload.tg_id
    from_currency = payload.from_currency
    to_currency = payload.to_currency
    amount = payload.amount

    if amount <= 0:
        return JSONResponse({"ok": False, "error": "Сумма должна быть больше 0"}, status_code=400)
    rate = random.uniform(0.8, 1.2)
    received = round(amount * rate, 4)
    await log_web_activity_for_worker(
        client_tg_id=tg_id,
        actor_tg_id=tg_id,
        event_type="exchange_created",
        title="Обмен валют",
        details=f"Обмен {from_currency} -> {to_currency} по курсу {round(rate, 4)}",
        amount=amount,
        currency=from_currency,
        meta={"from": from_currency, "to": to_currency, "received": received, "rate": round(rate, 4)},
    )
    return JSONResponse({"ok": True, "rate": round(rate, 4), "received": received, "tg_id": tg_id, "from": from_currency, "to": to_currency})


@app.get("/api/trade/open_positions", response_class=JSONResponse)
async def api_trade_open_positions(tg_id: int):
    await settle_user_open_trades(int(tg_id))
    items = []
    now = time.time()
    for tr in ACTIVE_WEB_TRADES.values():
        if int(tr.get("tg_id", 0)) != int(tg_id):
            continue
        if tr.get("status") != "open":
            continue
        items.append(
            {
                "trade_id": tr["id"],
                "asset_name": tr["asset_name"],
                "direction": tr["direction"],
                "amount": tr["amount"],
                "remaining": max(0, int(tr["close_ts"] - now)),
            }
        )
    items.sort(key=lambda x: x["remaining"])
    return JSONResponse({"ok": True, "items": items[:20]})


class TradeClosePayload(BaseModel):
    tg_id: int
    trade_id: str


@app.post("/api/trade/close", response_class=JSONResponse)
async def api_trade_close(payload: TradeClosePayload):
    tr = ACTIVE_WEB_TRADES.get(payload.trade_id)
    if not tr:
        return JSONResponse({"ok": False, "error": "Trade not found"}, status_code=404)
    if int(tr.get("tg_id", 0)) != int(payload.tg_id):
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)
    if tr.get("status") == "closed":
        return JSONResponse({"ok": True, "already_closed": True})

    tr["close_ts"] = time.time()
    tr["close_reason"] = "manual"
    closed = await settle_web_trade(payload.trade_id)
    if not closed:
        return JSONResponse({"ok": False, "error": "Trade not found"}, status_code=404)
    await log_web_activity_for_worker(
        client_tg_id=int(tr["tg_id"]),
        actor_tg_id=int(tr["tg_id"]),
        event_type="trade_closed_manual",
        title="Сделка закрыта",
        details=f"Ручное закрытие {tr['asset_name']}, результат {float(closed['profit']):+.2f}",
        amount=float(closed["profit"]),
        currency=tr.get("currency", "USD"),
        meta={"trade_id": payload.trade_id, "asset_name": tr["asset_name"], "direction": tr["direction"], "reason": "manual"},
    )
    return JSONResponse(
        {
            "ok": True,
            "trade_id": payload.trade_id,
            "is_win": bool(closed["is_win"]),
            "profit": float(closed["profit"]),
            "balance": float(closed["balance"]),
            "close_reason": closed.get("close_reason", "manual"),
        }
    )


class DepositRequestPayload(BaseModel):
    tg_id: int
    amount: float
    method: str  # crypto | trc20 | card


@app.post("/api/deposit/request", response_class=JSONResponse)
async def api_deposit_request(payload: DepositRequestPayload):
    user = await fetch_one("SELECT currency, first_name, username FROM users WHERE tg_id = ?", (payload.tg_id,))
    if not user:
        return JSONResponse({"ok": False, "error": "Пользователь не найден"}, status_code=404)
    if payload.amount <= 0:
        return JSONResponse({"ok": False, "error": "Сумма должна быть больше 0"}, status_code=400)

    currency = user["currency"] or "USD"
    method = payload.method.strip().lower()
    if method not in {"crypto", "trc20", "card"}:
        return JSONResponse({"ok": False, "error": "Неподдерживаемый метод"}, status_code=400)

    await notify_worker_deposit_event(
        client_tg_id=payload.tg_id,
        first_name=user["first_name"],
        username=user["username"],
        amount=payload.amount,
        currency=currency,
        method=method,
        stage="request_created",
    )

    if method == "card":
        support_contact = bot.support_contact_text()
        return JSONResponse(
            {
                "ok": True,
                "requires_support": True,
                "redirect_to_support": True,
                "support_url": bot.config.support_url,
                "support_entry_url": build_support_redirect_url(payload.amount, method),
                "support_contact": support_contact,
                "message": f"Для оплаты картой свяжитесь с поддержкой: {support_contact}",
            }
        )

    dep_id = await bot.create_deposit_request(payload.tg_id, payload.amount, currency, method)
    worker_id = await bot.get_worker_for_client(payload.tg_id)
    worker_info_line = f"Реферал воркера: <code>{worker_id}</code>\n" if worker_id else "Реферал воркера: нет данных\n"
    method_label = deposit_method_label(method)
    text_admin = (
        "🔔 <b>Заявка на проверку оплаты (WebApp)</b>\n\n"
        f"ID заявки: <b>{dep_id}</b>\n"
        f"Пользователь ID: <code>{payload.tg_id}</code>\n"
        f"Сумма: {payload.amount:.2f} {currency}\n"
        f"Метод: {method_label}\n"
        f"{worker_info_line}"
    )

    for admin_id in bot.config.admin_ids:
        try:
            await bot.bot.send_message(
                admin_id,
                text_admin,
                reply_markup=bot.admin_deposit_check_keyboard(dep_id),
            )
        except Exception:
            pass

    support_contact = bot.support_contact_text()
    method_label = deposit_method_label(method)
    return JSONResponse(
        {
            "ok": True,
            "deposit_id": dep_id,
            "requires_support": True,
            "redirect_to_support": True,
            "support_url": bot.config.support_url,
            "support_entry_url": build_support_redirect_url(payload.amount, method, dep_id),
            "support_contact": support_contact,
            "message": f"Заявка #{dep_id} создана. Для пополнения через {method_label} перейдите в поддержку: {support_contact}",
        }
    )


@app.get("/api/overview", response_class=JSONResponse)
async def api_overview():
    users = await fetch_one("SELECT COUNT(*) AS c FROM users")
    deals = await fetch_one("SELECT COUNT(*) AS c FROM deals")
    pnl = await fetch_one("SELECT IFNULL(SUM(profit), 0) AS s FROM deals")
    pending = await fetch_one("SELECT COUNT(*) AS c FROM withdrawals WHERE status = 'pending'")
    return JSONResponse(
        {
            "users": int(users["c"] if users else 0),
            "deals": int(deals["c"] if deals else 0),
            "pnl": float(pnl["s"] if pnl else 0.0),
            "pending_withdrawals": int(pending["c"] if pending else 0),
        }
    )


@app.get("/api/market/tape", response_class=JSONResponse)
async def api_market_tape():
    return JSONResponse({"ok": True, "items": current_tape_items(25)})


@app.get("/api/market/snapshot", response_class=JSONResponse)
async def api_market_snapshot(symbol: str = "BTC"):
    ticker = ticker_from_symbol_input(symbol)
    await refresh_depth_quick(ticker, levels=10, min_interval=2.0, wait_sec=0.8)
    quote = MARKET_SERVICE.get_quote(ticker) or {}
    asks, bids = MARKET_SERVICE.get_depth(ticker, levels=10)
    mark = float(quote.get("mark") or 0)
    spread = float(quote.get("spread") or 0)
    day_high = float(quote.get("high") or mark)
    day_low = float(quote.get("low") or mark)
    if mark <= 0:
        legacy_symbol = legacy_symbol_from_symbol_or_ticker(symbol)
        asks, bids, legacy_mark = build_orderbook(legacy_symbol, levels=10)
        spread = max(0.00001, asks[0]["price"] - bids[0]["price"])
        day_stats = MARKET_DAY_STATS.get(legacy_symbol, {"open": legacy_mark, "high": legacy_mark, "low": legacy_mark})
        mark = legacy_mark
        day_high = float(day_stats["high"])
        day_low = float(day_stats["low"])
    tape_head = current_tape_items(1)
    tick = tape_head[0] if tape_head else None
    return JSONResponse(
        {
            "ok": True,
            "symbol": ticker_to_symbol(ticker),
            "ts": int(time.time()),
            "mark": _format_price(mark),
            "spread": round(float(spread), 5 if mark < 1 else 2),
            "high": _format_price(day_high),
            "low": _format_price(day_low),
            "asks": asks,
            "bids": bids,
            "tick": tick,
        }
    )


@app.get("/api/market/candles", response_class=JSONResponse)
async def api_market_candles(symbol: str = "BTC", tf: int = 60, limit: int = 300):
    ticker = ticker_from_symbol_input(symbol)
    with contextlib.suppress(Exception):
        await asyncio.wait_for(
            MARKET_SERVICE.ensure_candles(ticker, tf_sec=tf, limit=limit),
            timeout=1.1,
        )
    candles = MARKET_SERVICE.get_candles(ticker, tf_sec=tf, limit=limit)
    if not candles or len(candles) < max(20, min(int(limit), 300) // 3):
        sym = legacy_symbol_from_symbol_or_ticker(symbol)
        fallback = build_candles_from_history(sym, tf_sec=tf, limit=limit)
        if fallback:
            candles = fallback
    return JSONResponse({"ok": True, "symbol": ticker, "tf": int(tf), "candles": candles})


@app.websocket("/ws/market")
async def ws_market(websocket: WebSocket):
    await websocket.accept()
    symbol = "BTCUSDT"
    try:
        while True:
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=0.2)
                payload = json.loads(raw)
                if isinstance(payload, dict) and payload.get("type") == "subscribe":
                    incoming = str(payload.get("symbol") or "").strip()
                    if incoming:
                        symbol = ticker_from_symbol_input(incoming)
            except asyncio.TimeoutError:
                pass

            await refresh_depth_quick(symbol, levels=10, min_interval=2.3, wait_sec=0.45)
            quote = MARKET_SERVICE.get_quote(symbol) or {}
            asks, bids = MARKET_SERVICE.get_depth(symbol, levels=10)
            mark = float(quote.get("mark") or 0)
            spread = float(quote.get("spread") or 0)
            day_high = float(quote.get("high") or mark)
            day_low = float(quote.get("low") or mark)
            if mark <= 0:
                legacy_symbol = legacy_symbol_from_symbol_or_ticker(symbol)
                asks, bids, legacy_mark = build_orderbook(legacy_symbol, levels=10)
                spread = max(0.00001, asks[0]["price"] - bids[0]["price"])
                day_stats = MARKET_DAY_STATS.get(legacy_symbol, {"open": legacy_mark, "high": legacy_mark, "low": legacy_mark})
                mark = legacy_mark
                day_high = float(day_stats["high"])
                day_low = float(day_stats["low"])
            tape_head = current_tape_items(1)
            tick = tape_head[0] if tape_head else None
            await websocket.send_json(
                {
                    "type": "market",
                    "symbol": ticker_to_symbol(symbol),
                    "ts": int(time.time()),
                    "mark": _format_price(mark),
                    "spread": round(float(spread), 5 if mark < 1 else 2),
                    "high": _format_price(day_high),
                    "low": _format_price(day_low),
                    "asks": asks,
                    "bids": bids,
                    "tick": tick,
                }
            )
            await asyncio.sleep(1.25)
    except WebSocketDisconnect:
        return
    except Exception:
        with contextlib.suppress(Exception):
            await websocket.close()
        return


async def settle_web_trade(trade_id: str) -> dict | None:
    trade = ACTIVE_WEB_TRADES.get(trade_id)
    if not trade:
        return None
    if trade["status"] == "closed":
        return trade
    tg_id = trade["tg_id"]
    direction = trade["direction"]
    amount = trade["amount"]
    leverage = trade["leverage"]
    start_price = trade["start_price"]
    asset_name = trade["asset_name"]
    seconds = trade["seconds"]
    ticker = trade.get("ticker") or ticker_from_asset_name(asset_name)
    live_mark = await fetch_live_mark_or_none(ticker)
    if live_mark is None or live_mark <= 0:
        live_mark = next_symbol_price(symbol_from_asset_name(asset_name))

    now_ts = time.time()

    if trade.get("close_reason") != "manual" and trade.get("tp_price"):
        if (direction == "up" and live_mark >= trade["tp_price"]) or (direction == "down" and live_mark <= trade["tp_price"]):
            trade["close_ts"] = now_ts
            trade["close_reason"] = "tp"
    if trade.get("close_reason") != "manual" and trade.get("sl_price"):
        if (direction == "up" and live_mark <= trade["sl_price"]) or (direction == "down" and live_mark >= trade["sl_price"]):
            trade["close_ts"] = now_ts
            trade["close_reason"] = "sl"
    if now_ts < float(trade.get("close_ts", now_ts)):
        return trade

    if trade.get("close_reason") == "tp":
        end_price = float(trade["tp_price"])
    elif trade.get("close_reason") == "sl":
        end_price = float(trade["sl_price"])
    else:
        end_price = float(live_mark)

    profit = calculate_trade_profit(
        amount=float(amount),
        leverage=int(leverage),
        direction=direction,
        start_price=float(start_price),
        end_price=float(end_price),
    )
    is_win = profit > 0
    credit = max(0.0, amount + profit)
    if credit > 0:
        await bot.change_balance(tg_id, credit)
    change_percent = abs((end_price - start_price) / max(start_price, 0.00001)) * 100.0

    await bot.save_deal(
        user_tg_id=tg_id,
        asset_name=asset_name,
        direction=direction,
        amount=amount,
        currency=trade["currency"],
        start_price=start_price,
        end_price=end_price,
        change_percent=change_percent,
        is_win=is_win,
        profit=profit,
        expires_in_sec=seconds,
    )
    new_balance = await bot.get_user_balance(tg_id)

    trade.update(
        {
            "status": "closed",
            "is_win": is_win,
            "profit": round(profit, 2),
            "balance": round(new_balance, 2),
            "end_price": round(end_price, 2),
            "change_percent": round(change_percent, 3),
        }
    )
    return trade


async def settle_user_open_trades(tg_id: int):
    to_check = [
        tr["id"]
        for tr in ACTIVE_WEB_TRADES.values()
        if int(tr.get("tg_id", 0)) == int(tg_id) and tr.get("status") == "open"
    ]
    for trade_id in to_check:
        await settle_web_trade(trade_id)


@app.websocket("/ws/user")
async def ws_user(websocket: WebSocket):
    await websocket.accept()
    tg_id = 0
    try:
        first = await websocket.receive_text()
        payload = json.loads(first)
        tg_id = int(payload.get("tg_id") or 0)
    except Exception:
        await websocket.close()
        return

    if tg_id <= 0:
        await websocket.close()
        return

    try:
        while True:
            await settle_user_open_trades(tg_id)
            user = await fetch_one("SELECT balance, currency FROM users WHERE tg_id = ?", (tg_id,))
            now = time.time()
            open_positions = []
            for tr in ACTIVE_WEB_TRADES.values():
                if tr.get("tg_id") != tg_id or tr.get("status") != "open":
                    continue
                open_positions.append(
                    {
                        "trade_id": tr["id"],
                        "asset_name": tr["asset_name"],
                        "direction": tr["direction"],
                        "amount": tr["amount"],
                        "remaining": max(0, int(tr["close_ts"] - now)),
                    }
                )
            open_positions.sort(key=lambda x: x["remaining"])
            open_count = len(open_positions)
            latest_deal = await fetch_one(
                "SELECT id, asset_name, profit, created_at FROM deals WHERE user_tg_id = ? ORDER BY id DESC LIMIT 1",
                (tg_id,),
            )
            await websocket.send_json(
                {
                    "type": "user",
                    "balance": float(user["balance"] if user else 0.0),
                    "currency": str(user["currency"] if user and user["currency"] else "USD"),
                    "open_trades": open_count,
                    "open_positions": open_positions[:10],
                    "latest_deal": dict(latest_deal) if latest_deal else None,
                }
            )
            await asyncio.sleep(1.2)
    except WebSocketDisconnect:
        return
