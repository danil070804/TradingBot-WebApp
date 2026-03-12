import asyncio
import os
import random
import contextlib
import json
import hmac
import hashlib
import time
import secrets
from datetime import datetime, timedelta, timezone
from decimal import Decimal
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
from identity_guard import resolve_session_user_id
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
        "nav_withdraw": "Вывести",
        "nav_deals": "Сделки",
        "nav_profile": "Профиль",
        "nav_admin": "Админка",
        "profile_title": "Профиль аккаунта",
        "quick_trade": "Открыть сделку",
        "quick_deposit": "Пополнить",
        "quick_history": "История сделок",
        "quick_profile": "Полный профиль",
        "quick_verify": "Пройти верификацию",
        "quick_worker": "Панель воркера",
        "quick_admin": "Админ-панель",
        "live_tape": "Лента рынка (live)",
        "home_balance": "Общий баланс",
        "home_success": "Успешные",
        "home_fail": "Неуспешные",
        "home_pairs": "Популярные пары",
        "home_recent": "Последние сделки",
        "home_no_deals": "Сделок пока нет.",
        "home_hero_title": "Точка входа в рынок, сделки и контроль счета",
        "home_hero_text": "Смотри баланс, риски, последние сделки и market pulse в одном командном экране.",
        "home_go_trade": "К торговле",
        "home_market_pulse": "Пульс рынка",
        "home_winrate": "Винрейт",
        "home_exposure": "Экспозиция",
        "home_risk_load": "Риск нагрузки",
        "markets_title": "Рынки",
        "markets_hero_title": "Сканируй движение рынка и открывай нужный актив без лишних шагов",
        "markets_hero_text": "Лидеры роста, watchlist, живые цены и быстрый переход в сделку или график в одном списке.",
        "markets_open_terminal": "Открыть терминал",
        "markets_scan_mode": "Режим обзора",
        "markets_live_scanner": "Живой сканер",
        "markets_scan_text": "Отслеживай рынок по фильтрам, росту, падению и своей watchlist.",
        "markets_next_step": "Следующий шаг",
        "markets_next_step_title": "Выбери актив и открой trade desk",
        "markets_next_step_text": "Каждая строка уже ведет в инструмент, график и быструю сделку.",
        "markets_working_list": "Рабочий список инструментов",
        "markets_filter_all": "Все",
        "markets_filter_watch": "Избранное",
        "markets_filter_gainers": "Рост",
        "markets_filter_losers": "Падение",
        "markets_filter_volatile": "Волатильные",
        "markets_search_placeholder": "Поиск: BTC, Bitcoin, ETH...",
        "markets_sort_gainers": "Лидеры роста",
        "markets_sort_losers": "Лидеры падения",
        "markets_sort_price_high": "Цена: выше",
        "markets_sort_price_low": "Цена: ниже",
        "markets_sort_alpha": "По алфавиту",
        "markets_insights": "Инсайты рынка",
        "markets_instrument_drawer": "Панель инструмента",
        "trade_title": "Futures Terminal",
        "trade_hero_title": "Открой сделку быстро и контролируй риск в одном окне",
        "trade_hero_text": "Живой рынок, быстрый вход, контроль экспозиции и результат сделки без прыжков между разделами.",
        "trade_full_chart": "Полный график",
        "trade_flow_rate": "0 сделок/мин",
        "trade_symbols_active": "0 активных символов",
        "trade_active_mode": "Активный режим",
        "trade_turbo_futures": "Турбо-фьючерсы",
        "trade_turbo_text": "Быстрый вход с таймером и live-панелью",
        "trade_market_label": "Рынок",
        "trade_market_mark": "Текущий mark в столе",
        "trade_market_sync": "Книга цен, лента и chart preview синхронизированы",
        "trade_market_mode": "Режим рынка",
        "trade_awaiting_feed": "Ожидание потока",
        "trade_zero_positions": "0 позиций",
        "trade_execution_desk": "Торговый стол",
        "trade_build_setup": "Собери параметры сделки",
        "trade_ready": "готово",
        "trade_position_base": "База позиции",
        "trade_risk_percent": "Риск %",
        "trade_leverage_timing": "Плечо и время",
        "trade_exit_control": "Управление выходом",
        "trade_direction_title": "Направление сделки",
        "trade_preview_note": "Перед открытием ты увидишь финальное подтверждение с оценкой сценария.",
        "trade_direction": "Направление",
        "trade_potential_tp": "Потенциал TP",
        "trade_potential_sl": "Потенциал SL",
        "trade_confirm_title": "Подтверждение сделки",
        "trade_asset": "Актив",
        "trade_side": "Направление",
        "trade_risk_load": "Риск нагрузки",
        "trade_scenario_title": "Сценарий",
        "trade_entry": "Вход",
        "trade_tp_zone": "TP зона",
        "trade_sl_zone": "SL зона",
        "trade_confirm_note": "Оценка, не гарантия. Финальный результат зависит от движения рынка.",
        "trade_cancel": "Отмена",
        "trade_confirm_open": "Подтвердить и открыть",
        "trade_balance": "Баланс",
        "trade_pair": "Пара",
        "trade_amount": "Сумма",
        "trade_leverage": "Плечо",
        "trade_expiration": "Экспирация",
        "trade_long": "ЛОНГ",
        "trade_short": "ШОРТ",
        "trade_create": "Создать сделку",
        "trade_order_book": "Стакан заявок",
        "trade_open_trades": "Открытых сделок",
        "trade_open_chart": "Открыть график",
        "trade_chart_title": "Биржевой график",
        "trade_chart_back": "Назад к торговле",
        "trade_chart_live": "Живой график",
        "trade_chart_asset": "Актив",
        "trade_chart_timeframe": "Таймфрейм",
        "chart_reset": "Сброс",
        "chart_fit": "Фокус",
        "chart_crosshair": "Курсор",
        "chart_live_lock": "Live-режим",
        "trade_feed_booting": "Рынок: подключение",
        "trade_feed_live": "Рынок: live",
        "trade_feed_reconnect": "Рынок: переподключение",
        "trade_feed_polling": "Рынок: polling",
        "trade_countdown": "До завершения",
        "trade_status_open": "Сделка открыта",
        "trade_status_closed": "Сделка закрыта",
        "trade_open_positions": "Открытые позиции",
        "trade_close_now": "Закрыть сейчас",
        "deals_title": "История сделок",
        "deals_empty": "История пока пустая.",
        "deposit_title": "Пополнение баланса",
        "deposit_hero_title": "Пополняй счет быстро и выбирай удобный маршрут оплаты",
        "deposit_hero_text": "Выбери метод, отправь заявку и получи актуальные реквизиты или ссылку на оплату.",
        "deposit_to_profile": "К профилю",
        "deposit_strategy": "Стратегия пополнения",
        "deposit_strategy_title": "Выбери скорость, маршрут и удобство",
        "deposit_strategy_text": "Crypto bot для быстрого старта, TRC20 для прямого перевода, карта через поддержку.",
        "deposit_choose_route": "Выбери маршрут пополнения",
        "deposit_lead": "Выбери метод оплаты. После заявки поддержка пришлет актуальные реквизиты или ссылку на оплату.",
        "deposit_method": "Метод оплаты",
        "deposit_amount": "Сумма",
        "deposit_way": "Способ",
        "deposit_go_trade": "К торговле",
        "deposit_open_markets": "Открыть рынки",
        "deposit_crypto_fast": "Быстрое подтверждение через менеджера",
        "deposit_eta": "Время: 1-5 мин",
        "deposit_trc20_text": "Прямой перевод на кошелек",
        "deposit_network_fee": "Комиссия сети",
        "deposit_card_text": "Оплата картой через поддержку",
        "deposit_manual_approval": "Подтверждение оператором",
        "deposit_speed_instant": "Скорость: мгновенно",
        "deposit_fee_low": "Комиссия: минимальная",
        "deposit_recommended_fast": "Рекомендация: для быстрого старта",
        "deposit_help": "Нужна помощь с пополнением?",
        "deposit_support_title": "Пополнение через поддержку",
        "deposit_support_text": "Менеджер пришлет реквизиты или ссылку на оплату после выбора метода.",
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
        "worker_page_title": "Панель воркера",
        "worker_hero_title": "Управляй клиентами, воронкой и поддержкой из одного рабочего центра",
        "worker_hint": "Управление вашими рефералами в реальном времени",
        "worker_ref_link": "Реферальная ссылка",
        "worker_funnel_title": "Реферальная воронка",
        "worker_manage_lead": "Управляйте клиентами, лимитами и статусами в одном окне. Нажмите на карточку клиента, чтобы открыть профиль.",
        "worker_client_page_title": "Карточка клиента",
        "worker_client_hero_title": "Полный контроль по клиенту, статусам и CRM-истории",
        "worker_client_hero_text": "Следи за балансом, ограничениями, этапом воронки и действиями по клиенту в одном окне.",
        "worker_stat_referrals": "РЕФЕРАЛЫ",
        "worker_stat_active_day": "АКТИВНЫЕ 24Ч",
        "worker_stat_with_deposit": "С ПОПОЛНЕНИЕМ",
        "worker_stat_vip_trade": "VIP / ТОРГОВЛЯ",
        "worker_search_placeholder": "Поиск по имени, ID или username",
        "worker_filter_all": "Все",
        "worker_filter_favorite": "Избранные",
        "worker_filter_blocked": "Заблокированные",
        "worker_filter_verified": "KYC",
        "worker_filter_trade_off": "Торговля выкл.",
        "worker_filter_withdraw_off": "Вывод выкл.",
        "worker_filter_support": "Поддержка",
        "worker_transfer_to": "Передать воркеру",
        "worker_badge_blocked": "Заблокирован",
        "worker_badge_favorite": "Избранный",
        "worker_badge_active": "Активен",
        "worker_unblock_btn": "Разблокировать",
        "worker_block_btn": "Блокировать",
        "worker_transfer_btn": "Передать",
        "worker_card_btn": "Карточка",
        "worker_recent_events": "Последние события по рефералам",
        "worker_stage": "Этап",
        "worker_back_to_panel": "Назад в панель",
        "worker_stat_deals": "СДЕЛОК",
        "worker_luck": "УДАЧА",
        "worker_client_path": "Путь клиента",
        "worker_status_limits": "Статусы и лимиты",
        "worker_min_deposit": "Мин. пополнение",
        "worker_min_withdraw": "Мин. вывод",
        "worker_min_trade": "Мин. сделка",
        "worker_coeff": "Коэфф.",
        "worker_manage_client": "Управление клиентом",
        "worker_event_history": "История событий",
        "worker_events_empty": "Событий пока нет.",
        "worker_tags": "Теги",
        "worker_no_tags": "Тегов нет",
        "worker_note": "Заметка",
        "worker_no_notes": "Заметок пока нет.",
        "worker_funnel_stages": "Этапы воронки",
        "worker_support_center": "Центр поддержки",
        "js_trade_opening": "Открываем сделку...",
        "js_trade_error": "не удалось открыть сделку",
        "js_trade_done": "Сделка завершена",
        "js_trade_started": "Сделка открыта, идет отсчет",
        "js_trade_waiting": "До завершения",
        "js_trade_balance": "Новый баланс",
        "js_trade_rate": "Курс",
        "js_reason_time": "По времени",
        "js_reason_tp": "Тейк-профит",
        "js_reason_sl": "Стоп-лосс",
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
        "worker_page_title": "Панель воркера",
        "worker_ref_link": "Реферальная ссылка",
        "worker_stat_referrals": "РЕФЕРАЛЫ",
        "worker_stat_active_day": "АКТИВНЫЕ 24Ч",
        "worker_stat_with_deposit": "С ПОПОЛНЕНИЕМ",
        "worker_stat_vip_trade": "VIP / ТОРГОВЛЯ",
        "worker_funnel_title": "Воронка реферала",
        "worker_manage_lead": "Управляйте клиентами, лимитами и статусами в одном окне. Нажмите карточку клиента для подробностей.",
        "worker_search_placeholder": "Поиск по имени, ID или username",
        "worker_filter_all": "Все",
        "worker_filter_favorite": "Избранные",
        "worker_filter_blocked": "Заблокированные",
        "worker_filter_verified": "KYC",
        "worker_filter_trade_off": "Торговля выкл",
        "worker_filter_withdraw_off": "Вывод выкл",
        "worker_filter_support": "Поддержка",
        "worker_transfer_to": "Передать воркеру",
        "worker_badge_blocked": "Заблокирован",
        "worker_badge_favorite": "Избранный",
        "worker_badge_active": "Активный",
        "worker_unblock_btn": "Разблок",
        "worker_block_btn": "Блок",
        "worker_transfer_btn": "Передать",
        "worker_card_btn": "Карточка",
        "worker_recent_events": "Последние события рефералов",
        "deal_profit": "Прибыль",
        "deal_loss": "Убыток",
        "deal_up": "ВВЕРХ",
        "deal_down": "ВНИЗ",
        "worker_feed_online": "CRM-лента: онлайн",
        "worker_feed_live": "CRM-лента: live",
        "worker_feed_reconnect": "CRM-лента: переподключение",
        "worker_feed_polling": "CRM-лента: polling",
        "client_feed_online": "Лента клиента: онлайн",
        "client_feed_live": "Лента клиента: live",
        "client_feed_reconnect": "Лента клиента: переподключение",
        "client_feed_polling": "Лента клиента: polling",
        "market_mode_live": "Реальный",
        "market_mode_synthetic": "Синтетический",
        "market_mark": "Марка",
        "market_spread": "Спред",
        "market_high": "Макс. 24ч",
        "market_low": "Мин. 24ч",
        "market_latest_tape": "Последний тик",
        "market_open_from_card": "Откройте график или сделку прямо из карточки рынка.",
    },
    "en": {
        "nav_home": "Home",
        "nav_markets": "Markets",
        "nav_trade": "Trade",
        "nav_deposit": "Deposit",
        "nav_withdraw": "Withdraw",
        "nav_deals": "Deals",
        "nav_profile": "Profile",
        "nav_admin": "Admin",
        "profile_title": "Account Profile",
        "quick_trade": "Open Trade",
        "quick_deposit": "Deposit",
        "quick_history": "Trade History",
        "quick_profile": "Full Profile",
        "quick_verify": "Verify Account",
        "quick_worker": "Worker Panel",
        "quick_admin": "Admin Panel",
        "live_tape": "Market Tape (live)",
        "home_balance": "Total Balance",
        "home_success": "Wins",
        "home_fail": "Losses",
        "home_pairs": "Popular Pairs",
        "home_recent": "Recent Deals",
        "home_no_deals": "No deals yet.",
        "home_hero_title": "Your launch point for markets, trades and account control",
        "home_hero_text": "See balance, risk, recent deals and market pulse in one command screen.",
        "home_go_trade": "Go To Trade",
        "home_market_pulse": "Market Pulse",
        "home_winrate": "Winrate",
        "home_exposure": "Exposure",
        "home_risk_load": "Risk load",
        "markets_title": "Markets",
        "markets_hero_title": "Scan the market and jump into the right asset without extra steps",
        "markets_hero_text": "Gainers, watchlist, live prices and direct jumps to trade or chart from one list.",
        "markets_open_terminal": "Open Terminal",
        "markets_scan_mode": "Scan mode",
        "markets_live_scanner": "Live Scanner",
        "markets_scan_text": "Track the market by filters, movers and your personal watchlist.",
        "markets_next_step": "Next step",
        "markets_next_step_title": "Pick an asset and open the trade desk",
        "markets_next_step_text": "Each row already leads into the instrument, chart and quick trade flow.",
        "markets_working_list": "Working list of instruments",
        "markets_filter_all": "All",
        "markets_filter_watch": "Watchlist",
        "markets_filter_gainers": "Gainers",
        "markets_filter_losers": "Losers",
        "markets_filter_volatile": "Volatile",
        "markets_search_placeholder": "Search: BTC, Bitcoin, ETH...",
        "markets_sort_gainers": "Top gainers",
        "markets_sort_losers": "Top losers",
        "markets_sort_price_high": "Price: high",
        "markets_sort_price_low": "Price: low",
        "markets_sort_alpha": "Alphabetical",
        "markets_insights": "Market Insights",
        "markets_instrument_drawer": "Instrument Drawer",
        "trade_title": "Futures Terminal",
        "trade_hero_title": "Open a position fast and control risk in one screen",
        "trade_hero_text": "Live market, fast execution, exposure control and result tracking without jumping between screens.",
        "trade_full_chart": "Full Chart",
        "trade_flow_rate": "0 trades/min",
        "trade_symbols_active": "0 symbols active",
        "trade_active_mode": "Active mode",
        "trade_turbo_futures": "Turbo Futures",
        "trade_turbo_text": "Fast execution with timer and live panel",
        "trade_market_label": "Market",
        "trade_market_mark": "Live mark via desk",
        "trade_market_sync": "Order book, tape and chart preview stay in sync",
        "trade_market_mode": "Market mode",
        "trade_awaiting_feed": "Awaiting feed",
        "trade_zero_positions": "0 positions",
        "trade_execution_desk": "Execution desk",
        "trade_build_setup": "Build your trade setup",
        "trade_ready": "ready",
        "trade_position_base": "Position base",
        "trade_risk_percent": "Risk %",
        "trade_leverage_timing": "Leverage and timing",
        "trade_exit_control": "Exit control",
        "trade_direction_title": "Trade direction",
        "trade_preview_note": "Before execution you will see a final confirmation with a scenario preview.",
        "trade_direction": "Direction",
        "trade_potential_tp": "Potential TP",
        "trade_potential_sl": "Potential SL",
        "trade_confirm_title": "Confirm trade",
        "trade_asset": "Asset",
        "trade_side": "Side",
        "trade_risk_load": "Risk load",
        "trade_scenario_title": "Scenario Preview",
        "trade_entry": "Entry",
        "trade_tp_zone": "TP zone",
        "trade_sl_zone": "SL zone",
        "trade_confirm_note": "This estimate is not guaranteed and depends on live market conditions.",
        "trade_cancel": "Cancel",
        "trade_confirm_open": "Confirm & Open",
        "trade_balance": "Balance",
        "trade_pair": "Pair",
        "trade_amount": "Amount",
        "trade_leverage": "Leverage",
        "trade_expiration": "Expiration",
        "trade_long": "LONG",
        "trade_short": "SHORT",
        "trade_create": "Create Deal",
        "trade_order_book": "Order Book",
        "trade_open_trades": "Open trades",
        "trade_open_chart": "Open Chart",
        "trade_chart_title": "Exchange Chart",
        "trade_chart_back": "Back to Trade",
        "trade_chart_live": "Live chart",
        "trade_chart_asset": "Asset",
        "trade_chart_timeframe": "Timeframe",
        "chart_reset": "Reset",
        "chart_fit": "Fit",
        "chart_crosshair": "Crosshair",
        "chart_live_lock": "Live Lock",
        "trade_feed_booting": "Market: booting",
        "trade_feed_live": "Market: live",
        "trade_feed_reconnect": "Market: reconnecting",
        "trade_feed_polling": "Market: polling",
        "trade_countdown": "Time left",
        "trade_status_open": "Trade opened",
        "trade_status_closed": "Trade closed",
        "trade_open_positions": "Open Positions",
        "trade_close_now": "Close Now",
        "deals_title": "Deals History",
        "deals_empty": "No history yet.",
        "deposit_title": "Balance Top Up",
        "deposit_hero_title": "Top up fast and choose the payment route that fits you",
        "deposit_hero_text": "Pick a method, submit the request and get the latest payment details or payment link.",
        "deposit_to_profile": "Go To Profile",
        "deposit_strategy": "Funding strategy",
        "deposit_strategy_title": "Choose speed, route and convenience",
        "deposit_strategy_text": "Use crypto bot for the fastest start, TRC20 for direct transfer, or card via support.",
        "deposit_choose_route": "Choose your funding route",
        "deposit_lead": "Choose a payment method. After you submit a request, support will provide the latest payment instructions.",
        "deposit_method": "Payment Method",
        "deposit_amount": "Amount",
        "deposit_way": "Method",
        "deposit_go_trade": "Go To Trade",
        "deposit_open_markets": "Open Markets",
        "deposit_crypto_fast": "Fast flow with manager confirmation",
        "deposit_eta": "ETA: 1-5 min",
        "deposit_trc20_text": "Direct wallet transfer route",
        "deposit_network_fee": "Network fee",
        "deposit_card_text": "Card payment through support",
        "deposit_manual_approval": "Manual approval",
        "deposit_speed_instant": "Speed: instant",
        "deposit_fee_low": "Fee: low",
        "deposit_recommended_fast": "Recommended: quick start",
        "deposit_help": "Need help with deposit?",
        "deposit_support_title": "Deposit via support",
        "deposit_support_text": "A manager will send payment details or a payment link after you select a method.",
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
        "worker_page_title": "Worker Panel",
        "worker_hero_title": "Manage clients, funnel and support from one operating desk",
        "worker_hint": "Manage your referrals in real time",
        "worker_ref_link": "Referral link",
        "worker_funnel_title": "Referral Funnel",
        "worker_manage_lead": "Manage clients, limits, and statuses in one view. Click a client card to open profile.",
        "worker_client_page_title": "Client Card",
        "worker_client_hero_title": "Full control over client state, limits and CRM history",
        "worker_client_hero_text": "Track balance, limits, funnel stage and client actions from one screen.",
        "worker_stat_referrals": "REFERRALS",
        "worker_stat_active_day": "ACTIVE 24H",
        "worker_stat_with_deposit": "WITH DEPOSIT",
        "worker_stat_vip_trade": "VIP / TRADE",
        "worker_search_placeholder": "Search by name, ID or username",
        "worker_filter_all": "All",
        "worker_filter_favorite": "Favorites",
        "worker_filter_blocked": "Blocked",
        "worker_filter_verified": "KYC",
        "worker_filter_trade_off": "Trade off",
        "worker_filter_withdraw_off": "Withdraw off",
        "worker_filter_support": "Support",
        "worker_transfer_to": "Transfer to worker",
        "worker_badge_blocked": "Blocked",
        "worker_badge_favorite": "Favorite",
        "worker_badge_active": "Active",
        "worker_unblock_btn": "Unblock",
        "worker_block_btn": "Block",
        "worker_transfer_btn": "Transfer",
        "worker_card_btn": "Card",
        "worker_recent_events": "Recent referral events",
        "worker_stage": "Stage",
        "worker_back_to_panel": "Back to panel",
        "worker_stat_deals": "DEALS",
        "worker_luck": "LUCK",
        "worker_client_path": "Client path",
        "worker_status_limits": "Statuses and limits",
        "worker_min_deposit": "Min deposit",
        "worker_min_withdraw": "Min withdraw",
        "worker_min_trade": "Min trade",
        "worker_coeff": "Coeff",
        "worker_manage_client": "Client controls",
        "worker_event_history": "Event history",
        "worker_events_empty": "No events yet.",
        "worker_tags": "Tags",
        "worker_no_tags": "No tags",
        "worker_note": "Note",
        "worker_no_notes": "No notes yet.",
        "worker_funnel_stages": "Funnel stages",
        "worker_support_center": "Support center",
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
        "worker_page_title": "Worker Panel",
        "worker_ref_link": "Referral link",
        "worker_stat_referrals": "REFERRALS",
        "worker_stat_active_day": "ACTIVE 24H",
        "worker_stat_with_deposit": "WITH DEPOSIT",
        "worker_stat_vip_trade": "VIP / TRADE",
        "worker_funnel_title": "Referral Funnel",
        "worker_manage_lead": "Manage clients, limits, and statuses in one view. Click a client card to open profile.",
        "worker_search_placeholder": "Search by name, ID or username",
        "worker_filter_all": "All",
        "worker_filter_favorite": "Favorites",
        "worker_filter_blocked": "Blocked",
        "worker_filter_verified": "KYC",
        "worker_filter_trade_off": "Trade off",
        "worker_filter_withdraw_off": "Withdraw off",
        "worker_filter_support": "Support",
        "worker_transfer_to": "Transfer to worker",
        "worker_badge_blocked": "Blocked",
        "worker_badge_favorite": "Favorite",
        "worker_badge_active": "Active",
        "worker_unblock_btn": "Unblock",
        "worker_block_btn": "Block",
        "worker_transfer_btn": "Transfer",
        "worker_card_btn": "Card",
        "worker_recent_events": "Recent referral events",
        "deal_profit": "Profit",
        "deal_loss": "Loss",
        "deal_up": "UP",
        "deal_down": "DOWN",
        "worker_feed_online": "CRM feed: online",
        "worker_feed_live": "CRM feed: live",
        "worker_feed_reconnect": "CRM feed: reconnect",
        "worker_feed_polling": "CRM feed: polling",
        "client_feed_online": "Client feed: online",
        "client_feed_live": "Client feed: live",
        "client_feed_reconnect": "Client feed: reconnect",
        "client_feed_polling": "Client feed: polling",
        "market_mode_live": "Live",
        "market_mode_synthetic": "Synthetic",
        "market_mark": "Mark",
        "market_spread": "Spread",
        "market_high": "24H High",
        "market_low": "24H Low",
        "market_latest_tape": "Latest tape",
        "market_open_from_card": "Open the chart or trade directly from this market card.",
    },
    "uk": {
        "nav_home": "Головна",
        "nav_markets": "Ринки",
        "nav_trade": "Торгівля",
        "nav_deposit": "Поповнити",
        "nav_withdraw": "Вивести",
        "nav_deals": "Угоди",
        "nav_profile": "Профіль",
        "nav_admin": "Адмінка",
        "profile_title": "Профіль акаунта",
        "quick_trade": "Відкрити угоду",
        "quick_deposit": "Поповнити",
        "quick_history": "Історія угод",
        "quick_profile": "Повний профіль",
        "quick_verify": "Пройти верифікацію",
        "quick_worker": "Панель воркера",
        "quick_admin": "Адмін-панель",
        "live_tape": "Стрічка ринку (live)",
        "home_balance": "Загальний баланс",
        "home_success": "Успішні",
        "home_fail": "Неуспішні",
        "home_pairs": "Популярні пари",
        "home_recent": "Останні угоди",
        "home_no_deals": "Угод поки немає.",
        "home_hero_title": "Точка входу в ринок, угоди та контроль рахунку",
        "home_hero_text": "Дивись баланс, ризики, останні угоди та market pulse в одному командному екрані.",
        "home_go_trade": "До торгівлі",
        "home_market_pulse": "Пульс ринку",
        "home_winrate": "Вінрейт",
        "home_exposure": "Експозиція",
        "home_risk_load": "Ризик навантаження",
        "markets_title": "Ринки",
        "markets_hero_title": "Скануй рух ринку та відкривай потрібний актив без зайвих кроків",
        "markets_hero_text": "Лідери зростання, watchlist, живі ціни та швидкий перехід до угоди або графіка в одному списку.",
        "markets_open_terminal": "Відкрити термінал",
        "markets_scan_mode": "Режим огляду",
        "markets_live_scanner": "Живий сканер",
        "markets_scan_text": "Відстежуй ринок за фільтрами, зростанням, падінням та власною watchlist.",
        "markets_next_step": "Наступний крок",
        "markets_next_step_title": "Обери актив і відкрий trade desk",
        "markets_next_step_text": "Кожен рядок уже веде в інструмент, графік та швидку угоду.",
        "markets_working_list": "Робочий список інструментів",
        "markets_filter_all": "Усі",
        "markets_filter_watch": "Обране",
        "markets_filter_gainers": "Зростання",
        "markets_filter_losers": "Падіння",
        "markets_filter_volatile": "Волатильні",
        "markets_search_placeholder": "Пошук: BTC, Bitcoin, ETH...",
        "markets_sort_gainers": "Лідери зростання",
        "markets_sort_losers": "Лідери падіння",
        "markets_sort_price_high": "Ціна: вище",
        "markets_sort_price_low": "Ціна: нижче",
        "markets_sort_alpha": "За алфавітом",
        "markets_insights": "Інсайти ринку",
        "markets_instrument_drawer": "Панель інструмента",
        "trade_title": "Futures Terminal",
        "trade_hero_title": "Відкрий угоду швидко та контролюй ризик в одному екрані",
        "trade_hero_text": "Живий ринок, швидкий вхід, контроль експозиції та результат угоди без стрибків між розділами.",
        "trade_full_chart": "Повний графік",
        "trade_flow_rate": "0 угод/хв",
        "trade_symbols_active": "0 активних символів",
        "trade_active_mode": "Активний режим",
        "trade_turbo_futures": "Турбо-ф'ючерси",
        "trade_turbo_text": "Швидкий вхід з таймером і live-панеллю",
        "trade_market_label": "Ринок",
        "trade_market_mark": "Поточний mark у столі",
        "trade_market_sync": "Книга цін, стрічка та chart preview синхронізовані",
        "trade_market_mode": "Режим ринку",
        "trade_awaiting_feed": "Очікування потоку",
        "trade_zero_positions": "0 позицій",
        "trade_execution_desk": "Торговий стіл",
        "trade_build_setup": "Збери параметри угоди",
        "trade_ready": "готово",
        "trade_position_base": "База позиції",
        "trade_risk_percent": "Ризик %",
        "trade_leverage_timing": "Плече та час",
        "trade_exit_control": "Керування виходом",
        "trade_direction_title": "Напрям угоди",
        "trade_preview_note": "Перед відкриттям ти побачиш фінальне підтвердження з оцінкою сценарію.",
        "trade_direction": "Напрям",
        "trade_potential_tp": "Потенціал TP",
        "trade_potential_sl": "Потенціал SL",
        "trade_confirm_title": "Підтвердження угоди",
        "trade_asset": "Актив",
        "trade_side": "Напрям",
        "trade_risk_load": "Ризик навантаження",
        "trade_scenario_title": "Сценарій",
        "trade_entry": "Вхід",
        "trade_tp_zone": "TP зона",
        "trade_sl_zone": "SL зона",
        "trade_confirm_note": "Оцінка, не гарантія. Фінальний результат залежить від руху ринку.",
        "trade_cancel": "Скасувати",
        "trade_confirm_open": "Підтвердити та відкрити",
        "trade_balance": "Баланс",
        "trade_pair": "Пара",
        "trade_amount": "Сума",
        "trade_leverage": "Плече",
        "trade_expiration": "Експірація",
        "trade_long": "ЛОНГ",
        "trade_short": "ШОРТ",
        "trade_create": "Створити угоду",
        "trade_order_book": "Стакан заявок",
        "trade_open_trades": "Відкритих угод",
        "trade_open_chart": "Відкрити графік",
        "trade_chart_title": "Біржовий графік",
        "trade_chart_back": "Назад до торгівлі",
        "trade_chart_live": "Живий графік",
        "trade_chart_asset": "Актив",
        "trade_chart_timeframe": "Таймфрейм",
        "chart_reset": "Скинути",
        "chart_fit": "Фокус",
        "chart_crosshair": "Курсор",
        "chart_live_lock": "Live-режим",
        "trade_feed_booting": "Ринок: підключення",
        "trade_feed_live": "Ринок: live",
        "trade_feed_reconnect": "Ринок: перепідключення",
        "trade_feed_polling": "Ринок: polling",
        "trade_countdown": "До завершення",
        "trade_status_open": "Угода відкрита",
        "trade_status_closed": "Угода закрита",
        "trade_open_positions": "Відкриті позиції",
        "trade_close_now": "Закрити зараз",
        "deals_title": "Історія угод",
        "deals_empty": "Історія поки порожня.",
        "deposit_title": "Поповнення балансу",
        "deposit_hero_title": "Поповнюй рахунок швидко та обирай зручний маршрут оплати",
        "deposit_hero_text": "Обери метод, надішли заявку та отримай актуальні реквізити або посилання на оплату.",
        "deposit_to_profile": "До профілю",
        "deposit_strategy": "Стратегія поповнення",
        "deposit_strategy_title": "Обери швидкість, маршрут і зручність",
        "deposit_strategy_text": "Crypto bot для швидкого старту, TRC20 для прямого переказу, картка через підтримку.",
        "deposit_choose_route": "Обери маршрут поповнення",
        "deposit_lead": "Обери метод оплати. Після заявки підтримка надішле актуальні реквізити або посилання на оплату.",
        "deposit_method": "Метод оплати",
        "deposit_amount": "Сума",
        "deposit_way": "Спосіб",
        "deposit_go_trade": "До торгівлі",
        "deposit_open_markets": "Відкрити ринки",
        "deposit_crypto_fast": "Швидке підтвердження через менеджера",
        "deposit_eta": "Час: 1-5 хв",
        "deposit_trc20_text": "Прямий переказ на гаманець",
        "deposit_network_fee": "Комісія мережі",
        "deposit_card_text": "Оплата карткою через підтримку",
        "deposit_manual_approval": "Підтвердження оператором",
        "deposit_speed_instant": "Швидкість: миттєво",
        "deposit_fee_low": "Комісія: мінімальна",
        "deposit_recommended_fast": "Рекомендація: для швидкого старту",
        "deposit_help": "Потрібна допомога з поповненням?",
        "deposit_support_title": "Поповнення через підтримку",
        "deposit_support_text": "Менеджер надішле реквізити або посилання на оплату після вибору методу.",
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
        "worker_page_title": "Панель воркера",
        "worker_hero_title": "Керуй клієнтами, воронкою та підтримкою з одного робочого центру",
        "worker_hint": "Керування вашими рефералами в реальному часі",
        "worker_ref_link": "Реферальне посилання",
        "worker_funnel_title": "Реферальна воронка",
        "worker_manage_lead": "Керуйте клієнтами, лімітами та статусами в одному вікні. Натисніть на картку клієнта, щоб відкрити профіль.",
        "worker_client_page_title": "Картка клієнта",
        "worker_client_hero_title": "Повний контроль по клієнту, статусах і CRM-історії",
        "worker_client_hero_text": "Стеж за балансом, обмеженнями, етапом воронки та діями по клієнту в одному вікні.",
        "worker_stat_referrals": "РЕФЕРАЛИ",
        "worker_stat_active_day": "АКТИВНІ 24Г",
        "worker_stat_with_deposit": "З ПОПОВНЕННЯМ",
        "worker_stat_vip_trade": "VIP / ТОРГІВЛЯ",
        "worker_search_placeholder": "Пошук за ім'ям, ID або username",
        "worker_filter_all": "Усі",
        "worker_filter_favorite": "Обрані",
        "worker_filter_blocked": "Заблоковані",
        "worker_filter_verified": "KYC",
        "worker_filter_trade_off": "Торгівля вимк.",
        "worker_filter_withdraw_off": "Вивід вимк.",
        "worker_filter_support": "Підтримка",
        "worker_transfer_to": "Передати воркеру",
        "worker_badge_blocked": "Заблокований",
        "worker_badge_favorite": "Обраний",
        "worker_badge_active": "Активний",
        "worker_unblock_btn": "Розблокувати",
        "worker_block_btn": "Блокувати",
        "worker_transfer_btn": "Передати",
        "worker_card_btn": "Картка",
        "worker_recent_events": "Останні події по рефералах",
        "worker_stage": "Етап",
        "worker_back_to_panel": "Назад до панелі",
        "worker_stat_deals": "УГОД",
        "worker_luck": "ВДАЧА",
        "worker_client_path": "Шлях клієнта",
        "worker_status_limits": "Статуси та ліміти",
        "worker_min_deposit": "Мін. поповнення",
        "worker_min_withdraw": "Мін. вивід",
        "worker_min_trade": "Мін. угода",
        "worker_coeff": "Коеф.",
        "worker_manage_client": "Керування клієнтом",
        "worker_event_history": "Історія подій",
        "worker_events_empty": "Подій поки немає.",
        "worker_tags": "Теги",
        "worker_no_tags": "Тегів немає",
        "worker_note": "Нотатка",
        "worker_no_notes": "Нотаток поки немає.",
        "worker_funnel_stages": "Етапи воронки",
        "worker_support_center": "Центр підтримки",
        "js_trade_opening": "Відкриваємо угоду...",
        "js_trade_error": "не вдалося відкрити угоду",
        "js_trade_done": "Угода завершена",
        "js_trade_started": "Угода відкрита, іде відлік",
        "js_trade_waiting": "До завершення",
        "js_trade_balance": "Новий баланс",
        "js_trade_rate": "Курс",
        "js_reason_time": "За часом",
        "js_reason_tp": "Тейк-профіт",
        "js_reason_sl": "Стоп-лосс",
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
        "worker_page_title": "Панель воркера",
        "worker_ref_link": "Реферальне посилання",
        "worker_stat_referrals": "РЕФЕРАЛИ",
        "worker_stat_active_day": "АКТИВНІ 24Г",
        "worker_stat_with_deposit": "З ПОПОВНЕННЯМ",
        "worker_stat_vip_trade": "VIP / ТОРГІВЛЯ",
        "worker_funnel_title": "Воронка реферала",
        "worker_manage_lead": "Керуйте клієнтами, лімітами та статусами в одному вікні. Натисніть картку клієнта для деталей.",
        "worker_search_placeholder": "Пошук за ім'ям, ID або username",
        "worker_filter_all": "Усі",
        "worker_filter_favorite": "Обрані",
        "worker_filter_blocked": "Заблоковані",
        "worker_filter_verified": "KYC",
        "worker_filter_trade_off": "Торгівля вимк",
        "worker_filter_withdraw_off": "Вивід вимк",
        "worker_filter_support": "Підтримка",
        "worker_transfer_to": "Передати воркеру",
        "worker_badge_blocked": "Заблокований",
        "worker_badge_favorite": "Обраний",
        "worker_badge_active": "Активний",
        "worker_unblock_btn": "Розблок",
        "worker_block_btn": "Блок",
        "worker_transfer_btn": "Передати",
        "worker_card_btn": "Картка",
        "worker_recent_events": "Останні події рефералів",
        "deal_profit": "Прибуток",
        "deal_loss": "Збиток",
        "deal_up": "ВГОРУ",
        "deal_down": "ВНИЗ",
        "worker_feed_online": "CRM-стрічка: онлайн",
        "worker_feed_live": "CRM-стрічка: live",
        "worker_feed_reconnect": "CRM-стрічка: перепідключення",
        "worker_feed_polling": "CRM-стрічка: polling",
        "client_feed_online": "Стрічка клієнта: онлайн",
        "client_feed_live": "Стрічка клієнта: live",
        "client_feed_reconnect": "Стрічка клієнта: перепідключення",
        "client_feed_polling": "Стрічка клієнта: polling",
        "market_mode_live": "Реальний",
        "market_mode_synthetic": "Синтетичний",
        "market_mark": "Марка",
        "market_spread": "Спред",
        "market_high": "Макс. 24г",
        "market_low": "Мін. 24г",
        "market_latest_tape": "Останній тік",
        "market_open_from_card": "Відкрийте графік або угоду прямо з картки ринку.",
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
            "INSERT OR IGNORE INTO users(tg_id, first_name, username, language, currency) VALUES (?, ?, ?, ?, ?)",
            (tg_id, first_name, username, "en", "USD"),
        )
        await conn.execute(
            "UPDATE users SET first_name = ?, username = ?, language = COALESCE(language, ?), currency = COALESCE(currency, ?) WHERE tg_id = ?",
            (first_name, username, "en", "USD", tg_id),
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
    data.setdefault("global_min_deposit_usdt", str(bot.DEFAULT_MIN_DEPOSIT_USDT))
    data.setdefault("global_min_trade_usdt", str(bot.DEFAULT_MIN_TRADE_USDT))
    return data


def is_admin_session(request: Request) -> bool:
    return bool(request.session.get("is_admin"))


async def is_admin_request(request: Request) -> bool:
    if is_admin_session(request):
        return True
    tg_id = await get_current_user_id(request)
    if not tg_id:
        return False
    user = await fetch_one("SELECT is_admin FROM users WHERE tg_id = ?", (tg_id,))
    is_admin = bool(tg_id in bot.config.admin_ids) or (bool(user["is_admin"]) if user else False)
    if is_admin:
        request.session["is_admin"] = True
    return is_admin


async def get_or_pick_user_id() -> int:
    if ALLOW_DEFAULT_TG_FALLBACK and DEFAULT_TG_ID:
        return DEFAULT_TG_ID
    return 0


async def get_current_user_id(request: Request) -> int:
    from_session = request.session.get(USER_SESSION_KEY)
    if from_session:
        return int(from_session)
    return await get_or_pick_user_id()


async def resolve_api_user_id(
    request: Request,
    payload_tg_id: int | None = None,
    *,
    require_session: bool = True,
) -> tuple[int | None, JSONResponse | None]:
    session_tg_id = request.session.get(USER_SESSION_KEY)
    fallback_tg_id = None if session_tg_id else await get_current_user_id(request)
    resolved = resolve_session_user_id(
        int(session_tg_id) if session_tg_id is not None else None,
        int(fallback_tg_id) if fallback_tg_id else None,
        int(payload_tg_id) if payload_tg_id is not None else None,
        require_session=require_session,
    )
    if resolved.error:
        return None, JSONResponse({"ok": False, "error": resolved.error}, status_code=int(resolved.status_code))
    return int(resolved.user_id), None


async def get_nav_user(tg_id: int):
    if not tg_id:
        return None
    return await fetch_one(
        "SELECT tg_id, first_name, username, language, currency, balance, is_admin, is_worker, created_at FROM users WHERE tg_id = ?",
        (tg_id,),
    )


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
    await bot.notify_admin_referral_activity(
        client_tg_id=client_tg_id,
        title="Пополнение реферала",
        details=f"{stage_text}. Сумма: {amount_text}. Метод: {deposit_method_label(method or 'Не указан')}.",
    )


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


FUNNEL_STAGES = [
    "new",
    "web_opened",
    "deposit_interest",
    "support_wait",
    "deposited",
    "trading",
    "withdrawal",
    "vip",
]


def normalize_funnel_stage(stage: str | None) -> str:
    value = (stage or "").strip().lower()
    return value if value in FUNNEL_STAGES else "new"


def parse_tags(raw: str | None) -> list[str]:
    return [part.strip() for part in (raw or "").split(",") if part.strip()]


def row_to_dict(row):
    return dict(row) if row is not None else None


def to_json_safe(value):
    if isinstance(value, dict):
        return {k: to_json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [to_json_safe(v) for v in value]
    if isinstance(value, datetime):
        return value.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
    if isinstance(value, Decimal):
        return float(value)
    return value


async def ensure_deposit_support_ticket(
    client_tg_id: int,
    worker_tg_id: int | None,
    source: str,
    amount: float | None,
    currency: str | None,
    method: str | None,
    deposit_id: int | None = None,
):
    existing = await bot.get_latest_open_support_ticket(client_tg_id, "deposit")
    if existing:
        await bot.update_support_ticket_status(
            int(existing["id"]),
            "new",
            last_message=f"Пополнение {float(amount or 0):.2f} {(currency or 'USD')} через {deposit_method_label(method or '')}",
        )
        return int(existing["id"])
    return await bot.create_support_ticket(
        client_tg_id=client_tg_id,
        worker_tg_id=worker_tg_id,
        source=source,
        topic="deposit",
        subject=f"Пополнение через {deposit_method_label(method or '')}",
        status="new",
        last_message=f"Пополнение {float(amount or 0):.2f} {(currency or 'USD')} через {deposit_method_label(method or '')}",
        meta={"amount": amount, "currency": currency, "method": method, "deposit_id": deposit_id},
    )


async def fetch_worker_clients_rows(worker_tg_id: int):
    try:
        return await fetch_all(
            """
            SELECT wc.id, wc.client_tg_id, wc.min_deposit, wc.min_withdraw, wc.verified, wc.withdraw_enabled,
                   wc.min_trade_amount, wc.trade_coefficient, wc.auto_reject_trades, wc.trading_enabled, wc.favorite, wc.blocked, wc.crm_note, wc.tags, wc.funnel_stage, wc.last_activity_at,
                   u.first_name, u.username, u.balance, u.currency, cl.luck_percent
            FROM worker_clients wc
            LEFT JOIN users u ON u.tg_id = wc.client_tg_id
            LEFT JOIN client_luck cl ON cl.worker_tg_id = wc.worker_tg_id AND cl.client_tg_id = wc.client_tg_id
            WHERE wc.worker_tg_id = ?
            ORDER BY (wc.last_activity_at IS NOT NULL) DESC, wc.last_activity_at DESC, wc.created_at DESC, wc.id DESC
            LIMIT 300
            """,
            (worker_tg_id,),
        )
    except Exception:
        return await fetch_all(
            """
            SELECT wc.id, wc.client_tg_id, wc.min_deposit, wc.min_withdraw, wc.verified, wc.withdraw_enabled,
                   wc.min_trade_amount, 1 AS trade_coefficient, 0 AS auto_reject_trades, wc.trading_enabled, wc.favorite, wc.blocked, '' AS crm_note, '' AS tags, 'new' AS funnel_stage, wc.created_at AS last_activity_at,
                   u.first_name, u.username, u.balance, u.currency, 0 AS luck_percent
            FROM worker_clients wc
            LEFT JOIN users u ON u.tg_id = wc.client_tg_id
            WHERE wc.worker_tg_id = ?
            ORDER BY wc.id DESC
            LIMIT 300
            """,
            (worker_tg_id,),
        )


async def fetch_worker_summary(worker_tg_id: int) -> dict:
    active_cutoff = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    try:
        row = await fetch_one(
            """
            SELECT
                COUNT(*) AS total_clients,
                COALESCE(SUM(CASE WHEN favorite = 1 THEN 1 ELSE 0 END), 0) AS favorites,
                COALESCE(SUM(CASE WHEN blocked = 1 THEN 1 ELSE 0 END), 0) AS blocked,
                COALESCE(SUM(CASE WHEN funnel_stage = 'deposited' THEN 1 ELSE 0 END), 0) AS deposited,
                COALESCE(SUM(CASE WHEN funnel_stage = 'trading' THEN 1 ELSE 0 END), 0) AS trading,
                COALESCE(SUM(CASE WHEN funnel_stage = 'vip' THEN 1 ELSE 0 END), 0) AS vip,
                COALESCE(SUM(CASE WHEN last_activity_at IS NOT NULL AND last_activity_at >= ? THEN 1 ELSE 0 END), 0) AS active_day
            FROM worker_clients
            WHERE worker_tg_id = ?
            """,
            (active_cutoff, worker_tg_id),
        )
        return row_to_dict(row) or {
            "total_clients": 0,
            "favorites": 0,
            "blocked": 0,
            "deposited": 0,
            "trading": 0,
            "vip": 0,
            "active_day": 0,
        }
    except Exception:
        fallback = await fetch_one("SELECT COUNT(*) AS total_clients FROM worker_clients WHERE worker_tg_id = ?", (worker_tg_id,))
        total = int(fallback["total_clients"] if fallback else 0)
        return {
            "total_clients": total,
            "favorites": 0,
            "blocked": 0,
            "deposited": 0,
            "trading": 0,
            "vip": 0,
            "active_day": 0,
        }


async def fetch_worker_support_tickets(worker_tg_id: int, limit: int = 20):
    try:
        return await fetch_all(
            """
            SELECT st.id, st.client_tg_id, st.source, st.topic, st.status, st.subject, st.last_message,
                   st.assigned_to, st.updated_at, u.first_name, u.username
            FROM support_tickets st
            LEFT JOIN users u ON u.tg_id = st.client_tg_id
            WHERE st.worker_tg_id = ?
            ORDER BY st.id DESC
            LIMIT ?
            """,
            (worker_tg_id, limit),
        )
    except Exception:
        return []


async def fetch_worker_choices():
    try:
        return await fetch_all(
            """
            SELECT tg_id, first_name, username
            FROM users
            WHERE is_worker = 1
            ORDER BY tg_id DESC
            LIMIT 100
            """
        )
    except Exception:
        return []


async def fetch_admin_dashboard_snapshot() -> dict:
    metrics = await fetch_one(
        """
        SELECT
            (SELECT COUNT(*) FROM users) AS users_count,
            (SELECT COUNT(*) FROM users WHERE is_worker = 1) AS workers_count,
            (SELECT COUNT(*) FROM deals) AS deals_count,
            (SELECT COUNT(*) FROM withdrawals WHERE status = 'pending') AS pending_withdrawals,
            (SELECT COUNT(*) FROM deposit_requests WHERE status = 'pending') AS pending_deposits,
            (SELECT COUNT(*) FROM support_tickets WHERE status IN ('new', 'in_progress')) AS open_support,
            (SELECT COALESCE(SUM(amount), 0) FROM deposit_requests WHERE status = 'approved') AS approved_deposit_sum,
            (SELECT COALESCE(SUM(amount), 0) FROM withdrawals WHERE status = 'approved') AS approved_withdraw_sum
        """
    )
    deposits = await fetch_all(
        """
        SELECT d.id, d.user_tg_id, d.amount, d.currency, d.method, d.status, d.created_at, d.processed_by,
               u.first_name, u.username
        FROM deposit_requests d
        LEFT JOIN users u ON u.tg_id = d.user_tg_id
        ORDER BY d.id DESC
        LIMIT 50
        """
    )
    withdrawals = await fetch_all(
        """
        SELECT w.id, w.user_tg_id, w.amount, w.currency, w.method, w.status, w.created_at, w.processed_by,
               u.first_name, u.username
        FROM withdrawals w
        LEFT JOIN users u ON u.tg_id = w.user_tg_id
        ORDER BY w.id DESC
        LIMIT 50
        """
    )
    support = await fetch_all(
        """
        SELECT st.id, st.client_tg_id, st.worker_tg_id, st.source, st.topic, st.status, st.subject,
               st.last_message, st.assigned_to, st.updated_at, u.first_name, u.username
        FROM support_tickets st
        LEFT JOIN users u ON u.tg_id = st.client_tg_id
        ORDER BY st.id DESC
        LIMIT 40
        """
    )
    audit = await fetch_all(
        """
        SELECT id, admin_actor, target_tg_id, action, details, amount, currency, created_at
        FROM admin_audit_log
        ORDER BY id DESC
        LIMIT 50
        """
    )
    worker_stats = await fetch_all(
        """
        SELECT u.tg_id, u.first_name, u.username,
               COUNT(wc.id) AS clients_count,
               SUM(CASE WHEN wc.favorite = 1 THEN 1 ELSE 0 END) AS favorites_count,
               SUM(CASE WHEN wc.blocked = 1 THEN 1 ELSE 0 END) AS blocked_count
        FROM users u
        LEFT JOIN worker_clients wc ON wc.worker_tg_id = u.tg_id
        WHERE u.is_worker = 1
        GROUP BY u.tg_id, u.first_name, u.username
        ORDER BY clients_count DESC, u.tg_id DESC
        LIMIT 50
        """
    )
    return {
        "metrics": row_to_dict(metrics) or {},
        "deposits": deposits,
        "withdrawals": withdrawals,
        "support": support,
        "audit": audit,
        "worker_stats": worker_stats,
    }


async def build_worker_dashboard_payload(worker_tg_id: int) -> dict:
    return {
        "ok": True,
        "clients": [dict(r) for r in await fetch_worker_clients_rows(worker_tg_id)],
        "activity": [dict(r) for r in await bot.get_worker_activity_events(worker_tg_id, 24)],
        "tickets": [dict(r) for r in await fetch_worker_support_tickets(worker_tg_id, 12)],
        "summary": await fetch_worker_summary(worker_tg_id),
    }


async def build_worker_client_snapshot_payload(worker_tg_id: int, wc_id: int) -> dict:
    client = await fetch_one(
        """
            SELECT wc.id, wc.worker_tg_id, wc.client_tg_id, wc.min_deposit, wc.min_withdraw, wc.verified, wc.withdraw_enabled,
                   wc.min_trade_amount, wc.trade_coefficient, wc.auto_reject_trades, wc.trading_enabled, wc.favorite, wc.blocked, wc.crm_note, wc.tags, wc.funnel_stage, wc.last_activity_at,
                   u.first_name, u.username, u.language, u.currency, u.balance, cl.luck_percent
        FROM worker_clients wc
        LEFT JOIN users u ON u.tg_id = wc.client_tg_id
        LEFT JOIN client_luck cl ON cl.worker_tg_id = wc.worker_tg_id AND cl.client_tg_id = wc.client_tg_id
        WHERE wc.id = ? AND wc.worker_tg_id = ?
        LIMIT 1
        """,
        (wc_id, worker_tg_id),
    )
    if not client:
        return {"ok": False, "error": "Client not found"}
    client_tg_id = int(client["client_tg_id"])
    activity = await fetch_all(
        """
        SELECT id, title, details, amount, currency, created_at, actor_source, event_type
        FROM activity_log
        WHERE worker_tg_id = ? AND client_tg_id = ?
        ORDER BY id DESC
        LIMIT 40
        """,
        (worker_tg_id, client_tg_id),
    )
    tickets = await fetch_all(
        """
        SELECT id, source, topic, status, subject, last_message, assigned_to, updated_at
        FROM support_tickets
        WHERE client_tg_id = ?
        ORDER BY id DESC
        LIMIT 20
        """,
        (client_tg_id,),
    )
    return {
        "ok": True,
        "client": dict(client),
        "stats": await bot.get_user_deal_stats(client_tg_id),
        "pending": await bot.get_user_pending_withdraw_sum(client_tg_id),
        "luck": await bot.get_luck_for_worker_client(worker_tg_id, client_tg_id),
        "activity": [dict(x) for x in activity],
        "tickets": [dict(x) for x in tickets],
        "client_tags": parse_tags(client["tags"]),
    }


async def build_admin_dashboard_payload() -> dict:
    snapshot = await fetch_admin_dashboard_snapshot()
    payload = {
        "ok": True,
        "metrics": snapshot["metrics"],
        "deposits": [dict(r) for r in snapshot["deposits"]],
        "withdrawals": [dict(r) for r in snapshot["withdrawals"]],
        "support": [dict(r) for r in snapshot["support"]],
        "audit": [dict(r) for r in snapshot["audit"]],
        "worker_stats": [dict(r) for r in snapshot["worker_stats"]],
    }
    return to_json_safe(payload)


def normalize_lang_code(lang: str | None) -> str:
    code = (lang or "en").strip().lower()
    if code.startswith("en"):
        return "en"
    if code.startswith("uk"):
        return "uk"
    return "ru"


async def get_lang_and_labels(tg_id: int) -> tuple[str, dict]:
    # session override is handled in route via request.session
    if not tg_id:
        return "en", WEB_I18N["en"]
    row = await fetch_one("SELECT language FROM users WHERE tg_id = ?", (tg_id,))
    lang = normalize_lang_code(row["language"] if row else "en")
    return lang, WEB_I18N.get(lang, WEB_I18N["en"])


def labels_for_lang(lang: str) -> dict:
    return WEB_I18N.get(normalize_lang_code(lang), WEB_I18N["en"])


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
        asset_name = str(asset["name"] or "")
        display_symbol = symbol_from_asset_name(asset_name)
        ticker = ticker_from_asset_name(asset["name"])
        quote = MARKET_SERVICE.get_quote(ticker) or {}
        if not quote and prewarm_left > 0:
            schedule_depth_refresh(ticker, levels=8, min_interval=1.5)
            prewarm_left -= 1
        price = float(quote.get("mark") or 0)
        day_change = float(quote.get("day_change") or 0.0)
        uses_legacy_market = price <= 0 or (ticker == "BTCUSDT" and display_symbol != "BTC")
        if uses_legacy_market:
            legacy_symbol = display_symbol
            price = next_symbol_price(legacy_symbol)
            stats = MARKET_DAY_STATS.get(legacy_symbol) or {"open": price}
            day_open = float(stats.get("open", price) or price)
            day_change = round(((price - day_open) / day_open) * 100, 2) if day_open else 0.0
        rows.append(
            {
                "id": asset["id"],
                "name": asset_name,
                "symbol": display_symbol,
                "market_ref": asset_name,
                "ticker": ticker,
                "price": _format_price(price),
                "day_change": round(day_change, 2),
                "market_mode": "synthetic" if uses_legacy_market else "live",
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
    result = []
    for c in out:
        o = float(c["o"])
        close = float(c["c"])
        hi = max(float(c["h"]), o, close)
        lo = min(float(c["l"]), o, close)
        span = max(0.000001, hi - lo)
        result.append(
            {
                "t": int(c["t"]),
                "o": round(o, 6),
                "h": round(hi, 6),
                "l": round(max(0.00001, lo), 6),
                "c": round(close, 6),
                # Stable synthetic volume derived from real candle span.
                "v": round(max(1.0, float(c.get("v", 0.0)) * 160.0 + span * 90.0), 2),
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
    await bot.resume_open_trade_monitors()
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


@app.middleware("http")
async def disable_html_cache(request: Request, call_next):
    response = await call_next(request)
    content_type = response.headers.get("content-type", "")
    if "text/html" in content_type.lower():
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


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
    tg_id = int(user["id"])
    request.session[USER_SESSION_KEY] = tg_id
    lang_row = await fetch_one("SELECT language FROM users WHERE tg_id = ?", (tg_id,))
    request.session[LANG_SESSION_KEY] = normalize_lang_code(lang_row["language"] if lang_row else "en")
    return JSONResponse({"ok": True, "tg_id": tg_id})


class SetLangPayload(BaseModel):
    lang: str


@app.post("/api/lang", response_class=JSONResponse)
async def api_set_lang(payload: SetLangPayload, request: Request):
    lang = normalize_lang_code(payload.lang)
    request.session[LANG_SESSION_KEY] = lang
    tg_id = await get_current_user_id(request)
    if tg_id:
        await execute_query("UPDATE users SET language = ? WHERE tg_id = ?", (lang, tg_id))
    return JSONResponse({"ok": True, "lang": lang})


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    tg_id = await get_current_user_id(request)
    lang, labels = await get_request_lang_labels(request, tg_id)
    user = await get_nav_user(tg_id)
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
        {
            "request": request,
            "page": "markets",
            "title": "Legend Trading",
            "markets": await generate_market_rows(assets),
            "lang": lang,
            "labels": labels,
            "user": await get_nav_user(tg_id),
        },
    )


@app.get("/markets/{asset_name}", response_class=HTMLResponse)
async def market_detail(request: Request, asset_name: str):
    tg_id = await get_current_user_id(request)
    lang, labels = await get_request_lang_labels(request, tg_id)
    snapshot = await build_market_snapshot_payload(asset_name)
    return templates.TemplateResponse(
        "market_detail.html",
        {
            "request": request,
            "page": "markets",
            "title": f"{snapshot['asset_name']} | Legend Trading",
            "snapshot": snapshot,
            "back_href": "/markets",
            "lang": lang,
            "labels": labels,
            "user": await get_nav_user(tg_id),
        },
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
    user = await get_nav_user(tg_id)
    initial_open_positions = []
    initial_trade_status = None
    if tg_id:
        await settle_user_open_trades(int(tg_id))
        now = time.time()
        open_trades = await bot.get_open_trades_for_user(int(tg_id))
        for tr in open_trades:
            initial_open_positions.append(
                {
                    "trade_id": str(tr["trade_id"]),
                    "asset_name": str(tr["asset_name"] or ""),
                    "direction": str(tr["direction"] or "up"),
                    "amount": float(tr["amount"] or 0),
                    "currency": str(tr["currency"] or "USD"),
                    "leverage": int(tr["leverage"] or 10),
                    "start_price": float(tr["start_price"] or 0),
                    "remaining": max(0, int(float(tr["close_ts"] or 0) - now)),
                }
            )
        initial_open_positions.sort(key=lambda x: x["remaining"])
        if initial_open_positions:
            first_trade = await bot.get_active_trade(str(initial_open_positions[0]["trade_id"]))
            if first_trade:
                initial_trade_status = {
                    "ok": True,
                    "trade_id": str(first_trade["trade_id"]),
                    "status": str(first_trade["status"] or "open"),
                    "remaining": max(0, int(float(first_trade["close_ts"] or 0) - time.time())),
                    "start_price": float(first_trade["start_price"] or 0),
                    "asset_name": str(first_trade["asset_name"] or ""),
                    "direction": str(first_trade["direction"] or "up"),
                    "amount": float(first_trade["amount"] or 0),
                    "currency": str(first_trade["currency"] or "USD"),
                    "seconds": int(first_trade["seconds"] or 0),
                }
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
            "initial_open_positions": initial_open_positions,
            "initial_trade_status": initial_trade_status,
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
            "user": await get_nav_user(tg_id),
            "lang": lang,
            "labels": labels,
        },
    )


@app.get("/exchange", response_class=HTMLResponse)
async def exchange(request: Request):
    tg_id = await get_current_user_id(request)
    lang, labels = await get_request_lang_labels(request, tg_id)
    user = await get_nav_user(tg_id)
    return templates.TemplateResponse(
        "exchange.html",
        {"request": request, "page": "exchange", "title": "Legend Trading", "tg_id": tg_id, "user": user, "lang": lang, "labels": labels},
    )


@app.get("/deposit", response_class=HTMLResponse)
async def deposit_page(request: Request):
    tg_id = await get_current_user_id(request)
    lang, labels = await get_request_lang_labels(request, tg_id)
    user = await get_nav_user(tg_id)
    deposits = []
    if tg_id:
        deposits = await fetch_all(
            "SELECT id, amount, currency, method, status, created_at FROM deposit_requests WHERE user_tg_id = ? ORDER BY id DESC LIMIT 6",
            (tg_id,),
        )
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
            "deposits": deposits,
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
        worker_id = await bot.get_worker_for_client(tg_id)
        await execute_query(
            "UPDATE worker_clients SET funnel_stage = ?, last_activity_at = CURRENT_TIMESTAMP WHERE client_tg_id = ?",
            ("support_wait", tg_id),
        )
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
        await ensure_deposit_support_ticket(
            client_tg_id=tg_id,
            worker_tg_id=worker_id,
            source="web",
            amount=amount,
            currency=(user["currency"] if user and user["currency"] else "USD"),
            method=method,
            deposit_id=deposit_id,
        )
    target = (bot.config.support_url or "").strip() or "/deposit"
    return RedirectResponse(url=target, status_code=302)


@app.get("/withdraw", response_class=HTMLResponse)
async def withdraw_page(request: Request):
    tg_id = await get_current_user_id(request)
    lang, labels = await get_request_lang_labels(request, tg_id)
    user = await get_nav_user(tg_id)
    access = await bot.get_client_access_flags(tg_id) if tg_id else {"blocked": False, "withdraw_enabled": False}
    pending = await bot.get_user_pending_withdraw_sum(tg_id) if tg_id else 0.0
    withdrawals = []
    if tg_id:
        withdrawals = await fetch_all(
            "SELECT id, amount, currency, method, details, status, created_at FROM withdrawals WHERE user_tg_id = ? ORDER BY id DESC LIMIT 8",
            (tg_id,),
        )
    return templates.TemplateResponse(
        "withdraw.html",
        {
            "request": request,
            "page": "withdraw",
            "title": "Legend Trading",
            "tg_id": tg_id,
            "user": user,
            "pending": pending,
            "withdrawals": withdrawals,
            "access": access,
            "support_url": bot.config.support_url,
            "support_contact": bot.support_contact_text(),
            "lang": lang,
            "labels": labels,
        },
    )


@app.get("/deals", response_class=HTMLResponse)
async def deals(request: Request):
    tg_id = await get_current_user_id(request)
    lang, labels = await get_request_lang_labels(request, tg_id)
    user = await get_nav_user(tg_id)
    rows = await fetch_all(
        "SELECT id, asset_name, direction, amount, currency, is_win, profit, created_at FROM deals WHERE user_tg_id = ? ORDER BY id DESC LIMIT 200",
        (tg_id,),
    )
    return templates.TemplateResponse(
        "deals.html",
        {
            "request": request,
            "page": "deals",
            "title": "Legend Trading",
            "deals": rows,
            "tg_id": tg_id,
            "lang": lang,
            "labels": labels,
            "tape": current_tape_items(25),
            "user": user,
        },
    )


@app.get("/deals/{deal_id}", response_class=HTMLResponse)
async def deal_detail(request: Request, deal_id: int):
    tg_id = await get_current_user_id(request)
    lang, labels = await get_request_lang_labels(request, tg_id)
    user = await get_nav_user(tg_id)
    deal = await fetch_one(
        """
        SELECT id, asset_name, direction, amount, currency, leverage, start_price, end_price,
               change_percent, is_win, profit, created_at
        FROM deals
        WHERE id = ? AND user_tg_id = ?
        """,
        (deal_id, tg_id),
    )
    if not deal:
        return RedirectResponse(url="/deals", status_code=302)
    return templates.TemplateResponse(
        "deal_detail.html",
        {
            "request": request,
            "page": "deals",
            "title": f"Deal #{deal_id} | Legend Trading",
            "deal": deal,
            "back_href": "/deals",
            "lang": lang,
            "labels": labels,
            "user": user,
        },
    )


@app.get("/support")
async def support_page_redirect(request: Request):
    tg_id = await get_current_user_id(request)
    if tg_id:
        user = await fetch_one("SELECT first_name, username FROM users WHERE tg_id = ?", (tg_id,))
        worker_id = await bot.get_worker_for_client(tg_id)
        existing = await bot.get_latest_open_support_ticket(tg_id, "general")
        if existing:
            await bot.update_support_ticket_status(int(existing["id"]), "new", last_message="Клиент открыл раздел техподдержки в WebApp")
        else:
            await bot.create_support_ticket(
                client_tg_id=tg_id,
                worker_tg_id=worker_id,
                source="web",
                topic="general",
                subject="Обращение в техподдержку",
                status="new",
                last_message="Клиент открыл раздел техподдержки в WebApp",
            )
        await log_web_activity_for_worker(
            client_tg_id=tg_id,
            actor_tg_id=tg_id,
            event_type="web_support_section_opened",
            title="Открыта техподдержка",
            details="Лохматый открыл раздел техподдержки в WebApp.",
        )
    target = (bot.config.support_url or "").strip() or "/profile"
    return RedirectResponse(url=target, status_code=302)


@app.get("/verify")
async def verification_page_redirect(request: Request):
    tg_id = await get_current_user_id(request)
    if tg_id:
        user = await fetch_one("SELECT first_name, username FROM users WHERE tg_id = ?", (tg_id,))
        worker_id = await bot.get_worker_for_client(tg_id)
        existing = await bot.get_latest_open_support_ticket(tg_id, "verification")
        if existing:
            await bot.update_support_ticket_status(int(existing["id"]), "new", last_message="Клиент открыл раздел верификации в WebApp")
        else:
            await bot.create_support_ticket(
                client_tg_id=tg_id,
                worker_tg_id=worker_id,
                source="web",
                topic="verification",
                subject="Запрос на верификацию",
                status="new",
                last_message="Клиент открыл раздел верификации в WebApp",
            )
        await log_web_activity_for_worker(
            client_tg_id=tg_id,
            actor_tg_id=tg_id,
            event_type="web_verification_opened",
            title="Открыта верификация",
            details="Лохматый открыл раздел верификации в WebApp.",
        )
    target = (bot.config.support_url or "").strip() or "/profile"
    return RedirectResponse(url=target, status_code=302)


@app.get("/profile", response_class=HTMLResponse)
async def profile_page(request: Request):
    tg_id = await get_current_user_id(request)
    lang, labels = await get_request_lang_labels(request, tg_id)
    if not tg_id:
        return templates.TemplateResponse(
            "profile.html",
            {"request": request, "page": "profile", "title": "Legend Trading", "user": None, "stats": {"total": 0, "wins": 0, "losses": 0, "total_profit": 0.0}, "pending": 0.0, "tg_id": 0, "withdrawals": [], "deposits": [], "lang": lang, "labels": labels},
        )
    user = await get_nav_user(tg_id)
    stats = await bot.get_user_deal_stats(tg_id) if tg_id else {"wins": 0, "losses": 0, "total": 0, "total_profit": 0.0}
    pending = await bot.get_user_pending_withdraw_sum(tg_id) if tg_id else 0.0
    access = await bot.get_client_access_flags(tg_id) if tg_id else {"verified": False, "blocked": False, "withdraw_enabled": False, "trading_enabled": True}
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
            "access": access,
            "tg_id": tg_id,
            "withdrawals": withdrawals,
            "deposits": deposits,
            "support_url": bot.config.support_url,
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
    try:
        rows = await fetch_worker_clients_rows(tg_id)
    except Exception:
        rows = []
    try:
        activity = await bot.get_worker_activity_events(tg_id, 24)
    except Exception:
        activity = []
    try:
        summary = await fetch_worker_summary(tg_id)
    except Exception:
        summary = {"total_clients": 0, "favorites": 0, "blocked": 0, "deposited": 0, "trading": 0, "vip": 0, "active_day": 0}
    try:
        tickets = await fetch_worker_support_tickets(tg_id, 12)
    except Exception:
        tickets = []
    try:
        worker_choices = await fetch_worker_choices()
    except Exception:
        worker_choices = []
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
            "summary": summary,
            "tickets": tickets,
            "worker_choices": worker_choices,
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
    try:
        client = await fetch_one(
            """
            SELECT wc.id, wc.worker_tg_id, wc.client_tg_id, wc.min_deposit, wc.min_withdraw, wc.verified, wc.withdraw_enabled,
                   wc.min_trade_amount, wc.trade_coefficient, wc.auto_reject_trades, wc.trading_enabled, wc.favorite, wc.blocked, wc.crm_note, wc.tags, wc.funnel_stage, wc.last_activity_at, wc.created_at,
                   u.first_name, u.username, u.language, u.currency, u.balance, u.created_at AS user_created_at, cl.luck_percent
            FROM worker_clients wc
            LEFT JOIN users u ON u.tg_id = wc.client_tg_id
            LEFT JOIN client_luck cl ON cl.worker_tg_id = wc.worker_tg_id AND cl.client_tg_id = wc.client_tg_id
            WHERE wc.id = ? AND wc.worker_tg_id = ?
            LIMIT 1
            """,
            (wc_id, tg_id),
        )
    except Exception:
        client = await fetch_one(
            """
            SELECT wc.id, wc.worker_tg_id, wc.client_tg_id, wc.min_deposit, wc.min_withdraw, wc.verified, wc.withdraw_enabled,
                   wc.min_trade_amount, 1 AS trade_coefficient, 0 AS auto_reject_trades, wc.trading_enabled, wc.favorite, wc.blocked, '' AS crm_note, '' AS tags, 'new' AS funnel_stage, wc.created_at AS last_activity_at, wc.created_at,
                   u.first_name, u.username, u.language, u.currency, u.balance, u.created_at AS user_created_at, 0 AS luck_percent
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
    tickets = await fetch_all(
        """
        SELECT id, source, topic, status, subject, last_message, assigned_to, updated_at
        FROM support_tickets
        WHERE client_tg_id = ?
        ORDER BY id DESC
        LIMIT 20
        """,
        (client_tg_id,),
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
            "tickets": tickets,
            "client_tags": parse_tags(client["tags"]),
            "funnel_stages": FUNNEL_STAGES,
        },
    )


@app.get("/api/worker/clients", response_class=JSONResponse)
async def api_worker_clients(request: Request):
    tg_id = await get_current_user_id(request)
    user = await fetch_one("SELECT is_worker FROM users WHERE tg_id = ?", (tg_id,))
    if not user or not bool(user["is_worker"]):
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)
    rows = await fetch_worker_clients_rows(tg_id)
    items = [dict(r) for r in rows]
    return JSONResponse({"ok": True, "items": items})


@app.get("/api/worker/dashboard", response_class=JSONResponse)
async def api_worker_dashboard(request: Request):
    tg_id = await get_current_user_id(request)
    user = await fetch_one("SELECT is_worker FROM users WHERE tg_id = ?", (tg_id,))
    if not user or not bool(user["is_worker"]):
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)
    return JSONResponse(await build_worker_dashboard_payload(tg_id))


class WorkerClientUpdatePayload(BaseModel):
    wc_id: int
    action: str
    value: float | int | str | None = None


@app.post("/api/worker/client/update", response_class=JSONResponse)
async def api_worker_client_update(payload: WorkerClientUpdatePayload, request: Request):
    tg_id = await get_current_user_id(request)
    user = await fetch_one("SELECT is_worker FROM users WHERE tg_id = ?", (tg_id,))
    if not user or not bool(user["is_worker"]):
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)

    wc = await fetch_one(
        "SELECT id, client_tg_id, worker_tg_id, min_trade_amount, trade_coefficient, auto_reject_trades, verified, withdraw_enabled, trading_enabled, favorite, blocked "
        "FROM worker_clients WHERE id = ?",
        (payload.wc_id,),
    )
    if not wc or int(wc["worker_tg_id"]) != int(tg_id):
        return JSONResponse({"ok": False, "error": "Client not found"}, status_code=404)

    action = payload.action.strip().lower()
    activity_title = "Изменение реферала"
    activity_details = ""
    activity_amount = None
    client_user = await fetch_one(
        "SELECT tg_id, language, currency, balance FROM users WHERE tg_id = ?",
        (int(wc["client_tg_id"]),),
    )
    client_lang = bot.normalize_lang(client_user["language"] if client_user else "en")
    support_markup = bot.support_section_keyboard(client_lang)
    if action == "toggle_verified":
        new_val = 0 if wc["verified"] else 1
        await bot.update_worker_client_field(payload.wc_id, "verified", new_val)
        activity_title = "KYC реферала"
        activity_details = "Верификация включена" if new_val else "Верификация отключена"
        await bot.notify_client_setting_change(
            int(wc["client_tg_id"]),
            bot.build_verification_status_notice(client_lang, bool(new_val)),
            reply_markup=support_markup if not new_val else None,
        )
    elif action == "toggle_withdraw":
        new_val = 0 if wc["withdraw_enabled"] else 1
        await bot.update_worker_client_field(payload.wc_id, "withdraw_enabled", new_val)
        activity_title = "Вывод реферала"
        activity_details = "Вывод разрешён" if new_val else "Вывод отключён"
        await bot.notify_client_setting_change(
            int(wc["client_tg_id"]),
            bot.build_withdraw_status_notice(client_lang, bool(new_val)),
            reply_markup=support_markup if not new_val else None,
        )
    elif action == "toggle_trade":
        new_val = 0 if wc["trading_enabled"] else 1
        await bot.update_worker_client_field(payload.wc_id, "trading_enabled", new_val)
        activity_title = "Торговля реферала"
        activity_details = "Торговля разрешена" if new_val else "Торговля отключена"
        await bot.notify_client_setting_change(
            int(wc["client_tg_id"]),
            bot.build_trade_status_notice(client_lang, bool(new_val)),
            reply_markup=support_markup if not new_val else None,
        )
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
        await bot.notify_client_setting_change(
            int(wc["client_tg_id"]),
            bot.build_block_status_notice(client_lang, bool(new_val)),
            reply_markup=support_markup if new_val else None,
        )
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
    elif action == "set_min_trade_amount":
        val = float(payload.value or 0)
        if val <= 0:
            return JSONResponse({"ok": False, "error": "Value must be > 0"}, status_code=400)
        await bot.update_worker_client_field(payload.wc_id, "min_trade_amount", val)
        activity_title = "Мин. сумма сделки"
        activity_details = f"Установлена минимальная сумма сделки {val:.2f}"
        activity_amount = val
    elif action == "set_trade_coefficient":
        val = float(payload.value or 0)
        if val <= 0 or val > 10:
            return JSONResponse({"ok": False, "error": "Coefficient must be in 0..10"}, status_code=400)
        await bot.update_worker_client_field(payload.wc_id, "trade_coefficient", val)
        activity_title = "Коэффициент сделок"
        activity_details = f"Коэффициент изменён на {val:.2f}"
    elif action == "toggle_auto_reject_trades":
        new_val = 0 if wc["auto_reject_trades"] else 1
        await bot.update_worker_client_field(payload.wc_id, "auto_reject_trades", new_val)
        activity_title = "Авто-отклонение сделок"
        activity_details = "Авто-отклонение включено" if new_val else "Авто-отклонение отключено"
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
        new_balance = await bot.get_user_balance(int(wc["client_tg_id"]))
        await bot.notify_client_setting_change(
            int(wc["client_tg_id"]),
            bot.build_balance_update_notice(client_lang, val, new_balance, client_user["currency"] if client_user else "USD"),
        )
    elif action == "subtract_balance":
        val = float(payload.value or 0)
        if val <= 0:
            return JSONResponse({"ok": False, "error": "Amount must be > 0"}, status_code=400)
        await bot.change_balance(int(wc["client_tg_id"]), -val)
        activity_title = "Списание баланса"
        activity_details = f"Воркер списал баланс {val:.2f}"
        activity_amount = -val
        new_balance = await bot.get_user_balance(int(wc["client_tg_id"]))
        await bot.notify_client_setting_change(
            int(wc["client_tg_id"]),
            bot.build_balance_update_notice(client_lang, -val, new_balance, client_user["currency"] if client_user else "USD"),
        )
    elif action == "set_balance":
        target = await fetch_one("SELECT balance, currency FROM users WHERE tg_id = ?", (int(wc["client_tg_id"]),))
        new_balance = float(payload.value or 0)
        current_balance = float(target["balance"] or 0) if target else 0.0
        delta = new_balance - current_balance
        await bot.change_balance(int(wc["client_tg_id"]), delta)
        activity_title = "Установка баланса"
        activity_details = f"Баланс установлен на {new_balance:.2f}"
        activity_amount = new_balance
        await bot.notify_client_setting_change(
            int(wc["client_tg_id"]),
            bot.build_balance_update_notice(client_lang, delta, new_balance, target["currency"] if target else "USD"),
        )
    elif action == "set_note":
        note = str(payload.value or "").strip()
        await bot.update_worker_client_field(payload.wc_id, "crm_note", note)
        activity_title = "Заметка воркера"
        activity_details = "Обновлена заметка по рефералу"
    elif action == "send_message":
        message_text = str(payload.value or "").strip()
        if not message_text:
            return JSONResponse({"ok": False, "error": "Message is empty"}, status_code=400)
        if len(message_text) > 2000:
            return JSONResponse({"ok": False, "error": "Message is too long"}, status_code=400)
        try:
            await bot.bot.send_message(int(wc["client_tg_id"]), f"💬 Сообщение от поддержки:\n{message_text}")
        except Exception:
            return JSONResponse({"ok": False, "error": "Не удалось отправить сообщение"}, status_code=502)
        ticket = await bot.get_latest_open_support_ticket(int(wc["client_tg_id"]))
        if ticket:
            await bot.update_support_ticket_status(
                int(ticket["id"]),
                "in_progress",
                assigned_to=str(tg_id),
                last_message=message_text,
            )
        else:
            await bot.create_support_ticket(
                client_tg_id=int(wc["client_tg_id"]),
                worker_tg_id=tg_id,
                source="worker_web",
                topic="support",
                subject="Сообщение от воркера",
                status="in_progress",
                last_message=message_text,
                meta={"from": "worker_web"},
            )
        activity_title = "Сообщение рефералу"
        activity_details = f"Воркер отправил сообщение: {message_text[:120]}"
    elif action == "set_tags":
        tags = ",".join(parse_tags(str(payload.value or "")))
        await bot.update_worker_client_field(payload.wc_id, "tags", tags)
        activity_title = "Теги реферала"
        activity_details = f"Теги обновлены: {tags or 'очищены'}"
    elif action == "set_funnel_stage":
        stage = normalize_funnel_stage(str(payload.value or ""))
        await bot.update_worker_client_field(payload.wc_id, "funnel_stage", stage)
        activity_title = "Этап воронки"
        activity_details = f"Этап изменён на {stage}"
    elif action == "transfer_worker":
        new_worker_id = int(float(payload.value or 0))
        target_worker = await fetch_one("SELECT tg_id, is_worker FROM users WHERE tg_id = ?", (new_worker_id,))
        if not target_worker or not bool(target_worker["is_worker"]):
            return JSONResponse({"ok": False, "error": "Новый воркер не найден"}, status_code=404)
        moved = await bot.transfer_worker_client_record(payload.wc_id, new_worker_id)
        if not moved:
            return JSONResponse({"ok": False, "error": "Не удалось передать реферала"}, status_code=400)
        activity_title = "Передача реферала"
        target_worker_row = await fetch_one("SELECT username, first_name FROM users WHERE tg_id = ?", (new_worker_id,))
        worker_label = f"@{target_worker_row['username']}" if target_worker_row and target_worker_row["username"] else (target_worker_row["first_name"] if target_worker_row and target_worker_row["first_name"] else str(new_worker_id))
        activity_details = f"Реферал передан воркеру {worker_label}"
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

    return JSONResponse({"ok": True, "details": activity_details})


@app.get("/api/worker/client/{wc_id}/snapshot", response_class=JSONResponse)
async def api_worker_client_snapshot(wc_id: int, request: Request):
    tg_id = await get_current_user_id(request)
    user = await fetch_one("SELECT is_worker FROM users WHERE tg_id = ?", (tg_id,))
    if not user or not bool(user["is_worker"]):
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)
    payload = await build_worker_client_snapshot_payload(tg_id, wc_id)
    status = 404 if not payload.get("ok") else 200
    return JSONResponse(payload, status_code=status)


@app.get("/admin/login", response_class=HTMLResponse)
async def admin_login_page(request: Request):
    if await is_admin_request(request):
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
    if not await is_admin_request(request):
        return templates.TemplateResponse("admin_login.html", {"request": request, "title": "Legend Trading Admin"})
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
    snapshot = await fetch_admin_dashboard_snapshot()
    metrics = snapshot["metrics"]
    return templates.TemplateResponse(
        "admin_dashboard.html",
        {
            "request": request,
            "title": "Legend Trading Admin",
            "users_count": int(metrics.get("users_count", 0) or 0),
            "workers_count": int(metrics.get("workers_count", 0) or 0),
            "deals_count": int(metrics.get("deals_count", 0) or 0),
            "pending_withdrawals": int(metrics.get("pending_withdrawals", 0) or 0),
            "settings_map": settings_map,
            "assets": assets,
            "users_rows": users_rows,
            "snapshot": snapshot,
        },
    )


@app.get("/admin/user/{tg_id}", response_class=HTMLResponse)
async def admin_user_page(request: Request, tg_id: int):
    if not await is_admin_request(request):
        return templates.TemplateResponse("admin_login.html", {"request": request, "title": "Legend Trading Admin"})
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
    if not await is_admin_request(request):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    allowed_keys = {
        "crypto_bot_url",
        "trc20_address",
        "card_pay_url",
        "card_requisites",
        "support_url",
        "webapp_url",
        "global_min_deposit_usdt",
        "global_min_trade_usdt",
    }
    key = payload.key.strip()
    if key not in allowed_keys:
        return JSONResponse({"ok": False, "error": "Unsupported key"}, status_code=400)
    await bot.set_setting(key, payload.value.strip())
    await bot.create_admin_audit_event(
        admin_actor="admin_web",
        target_tg_id=None,
        action=f"setting_{key}",
        details=f"Обновлена настройка {key}",
        meta={"key": key, "value": payload.value.strip()},
    )
    return JSONResponse({"ok": True})


class AdminAssetPayload(BaseModel):
    name: str


@app.post("/admin/api/assets", response_class=JSONResponse)
async def admin_add_asset(payload: AdminAssetPayload, request: Request):
    if not await is_admin_request(request):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    name = payload.name.strip()
    if len(name) < 2:
        return JSONResponse({"ok": False, "error": "Asset name too short"}, status_code=400)
    await bot.add_ecn_asset(name)
    await refresh_asset_market_map()
    await bot.create_admin_audit_event(
        admin_actor="admin_web",
        target_tg_id=None,
        action="asset_add",
        details=f"Добавлен актив {name}",
        meta={"asset": name},
    )
    return JSONResponse({"ok": True, "name": name})


class AdminUserActionPayload(BaseModel):
    tg_id: int
    action: str
    value: float | int | None = None


@app.post("/admin/api/user/action", response_class=JSONResponse)
async def admin_user_action(payload: AdminUserActionPayload, request: Request):
    if not await is_admin_request(request):
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
    await bot.create_admin_audit_event(
        admin_actor="admin_web",
        target_tg_id=int(payload.tg_id),
        action=action,
        details=details,
        amount=amount,
        currency=currency,
        meta={"action": action},
    )
    return JSONResponse({"ok": True, "details": details})


class AdminProcessPayload(BaseModel):
    entity_id: int
    action: str
    note: str | None = None


@app.get("/admin/api/dashboard", response_class=JSONResponse)
async def admin_dashboard_snapshot(request: Request):
    if not await is_admin_request(request):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    return JSONResponse(await build_admin_dashboard_payload())


@app.post("/admin/api/deposit/process", response_class=JSONResponse)
async def admin_process_deposit(payload: AdminProcessPayload, request: Request):
    if not await is_admin_request(request):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    dep = await fetch_one("SELECT * FROM deposit_requests WHERE id = ?", (payload.entity_id,))
    if not dep:
        return JSONResponse({"ok": False, "error": "Deposit not found"}, status_code=404)
    if str(dep["status"]) != "pending":
        return JSONResponse({"ok": False, "error": "Deposit already processed"}, status_code=400)
    action = payload.action.strip().lower()
    if action not in {"approve", "reject"}:
        return JSONResponse({"ok": False, "error": "Unsupported action"}, status_code=400)
    new_status = "approved" if action == "approve" else "rejected"
    client_user = await fetch_one("SELECT language FROM users WHERE tg_id = ?", (int(dep["user_tg_id"]),))
    client_lang = bot.normalize_lang(client_user["language"] if client_user else "en")
    await execute_query(
        "UPDATE deposit_requests SET status = ?, processed_by = ? WHERE id = ?",
        (new_status, "admin_web", payload.entity_id),
    )
    if new_status == "approved":
        await bot.change_balance(int(dep["user_tg_id"]), float(dep["amount"] or 0))
        await execute_query(
            "UPDATE worker_clients SET funnel_stage = ?, last_activity_at = CURRENT_TIMESTAMP WHERE client_tg_id = ?",
            ("deposited", int(dep["user_tg_id"])),
        )
        ticket = await bot.get_latest_open_support_ticket(int(dep["user_tg_id"]), "deposit")
        if ticket:
            await bot.update_support_ticket_status(int(ticket["id"]), "closed", assigned_to="admin_web", last_message="Пополнение подтверждено")
        try:
            section_key = "deposit_status_approved"
            if not await bot.get_section_photo_file_id(section_key):
                section_key = "deposit_status"
            await bot.send_section_chat_message(
                int(dep["user_tg_id"]),
                section_key,
                bot.build_deposit_status_notice(
                    client_lang,
                    True,
                    float(dep["amount"] or 0),
                    dep["currency"] or "USD",
                ),
                reply_markup=bot.support_section_keyboard(client_lang),
            )
        except Exception:
            pass
    else:
        try:
            section_key = "deposit_status_rejected"
            if not await bot.get_section_photo_file_id(section_key):
                section_key = "deposit_status"
            await bot.send_section_chat_message(
                int(dep["user_tg_id"]),
                section_key,
                bot.build_deposit_status_notice(
                    client_lang,
                    False,
                    float(dep["amount"] or 0),
                    dep["currency"] or "USD",
                ),
                reply_markup=bot.support_section_keyboard(client_lang),
            )
        except Exception:
            pass
    details = f"Заявка на пополнение #{payload.entity_id} {('подтверждена' if new_status == 'approved' else 'отклонена')}"
    await log_admin_user_action(
        target_tg_id=int(dep["user_tg_id"]),
        actor_tg_id=None,
        event_type=f"admin_deposit_{new_status}",
        title="Пополнение",
        details=details,
        amount=float(dep["amount"] or 0),
        currency=dep["currency"] or "USD",
        meta={"deposit_id": payload.entity_id, "note": payload.note or ""},
    )
    await bot.create_admin_audit_event(
        admin_actor="admin_web",
        target_tg_id=int(dep["user_tg_id"]),
        action=f"deposit_{new_status}",
        details=details,
        amount=float(dep["amount"] or 0),
        currency=dep["currency"] or "USD",
        meta={"deposit_id": payload.entity_id, "note": payload.note or ""},
    )
    return JSONResponse({"ok": True, "details": details})


@app.post("/admin/api/withdraw/process", response_class=JSONResponse)
async def admin_process_withdraw(payload: AdminProcessPayload, request: Request):
    if not await is_admin_request(request):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    wd = await fetch_one("SELECT * FROM withdrawals WHERE id = ?", (payload.entity_id,))
    if not wd:
        return JSONResponse({"ok": False, "error": "Withdrawal not found"}, status_code=404)
    if str(wd["status"]) != "pending":
        return JSONResponse({"ok": False, "error": "Withdrawal already processed"}, status_code=400)
    action = payload.action.strip().lower()
    if action not in {"approve", "reject"}:
        return JSONResponse({"ok": False, "error": "Unsupported action"}, status_code=400)
    new_status = "approved" if action == "approve" else "rejected"
    await execute_query(
        "UPDATE withdrawals SET status = ?, processed_by = ? WHERE id = ?",
        (new_status, "admin_web", payload.entity_id),
    )
    if new_status == "reject":
        await bot.change_balance(int(wd["user_tg_id"]), float(wd["amount"] or 0))
    else:
        await execute_query(
            "UPDATE worker_clients SET funnel_stage = ?, last_activity_at = CURRENT_TIMESTAMP WHERE client_tg_id = ?",
            ("withdrawal", int(wd["user_tg_id"])),
        )
    details = f"Заявка на вывод #{payload.entity_id} {('подтверждена' if new_status == 'approved' else 'отклонена')}"
    await log_admin_user_action(
        target_tg_id=int(wd["user_tg_id"]),
        actor_tg_id=None,
        event_type=f"admin_withdraw_{new_status}",
        title="Вывод",
        details=details,
        amount=float(wd["amount"] or 0),
        currency=wd["currency"] or "USD",
        meta={"withdrawal_id": payload.entity_id, "note": payload.note or ""},
    )
    await bot.create_admin_audit_event(
        admin_actor="admin_web",
        target_tg_id=int(wd["user_tg_id"]),
        action=f"withdraw_{new_status}",
        details=details,
        amount=float(wd["amount"] or 0),
        currency=wd["currency"] or "USD",
        meta={"withdrawal_id": payload.entity_id, "note": payload.note or ""},
    )
    return JSONResponse({"ok": True, "details": details})


@app.post("/admin/api/support/process", response_class=JSONResponse)
async def admin_process_support(payload: AdminProcessPayload, request: Request):
    if not await is_admin_request(request):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    ticket = await fetch_one("SELECT * FROM support_tickets WHERE id = ?", (payload.entity_id,))
    if not ticket:
        return JSONResponse({"ok": False, "error": "Support ticket not found"}, status_code=404)
    action = payload.action.strip().lower()
    if action == "take":
        status = "in_progress"
        assigned_to = "admin_web"
        details = f"Тикет #{payload.entity_id} взят в работу"
    elif action == "hold":
        status = "on_hold"
        assigned_to = ticket["assigned_to"] or "admin_web"
        details = f"Тикет #{payload.entity_id} поставлен на паузу"
    elif action == "escalate":
        status = "escalated"
        assigned_to = "admin_web"
        details = f"Тикет #{payload.entity_id} эскалирован"
    elif action == "close":
        status = "closed"
        assigned_to = ticket["assigned_to"] or "admin_web"
        details = f"Тикет #{payload.entity_id} закрыт"
    else:
        return JSONResponse({"ok": False, "error": "Unsupported action"}, status_code=400)
    await bot.update_support_ticket_status(int(payload.entity_id), status, assigned_to=assigned_to, last_message=payload.note or ticket["last_message"] or "")
    await log_admin_user_action(
        target_tg_id=int(ticket["client_tg_id"]),
        actor_tg_id=None,
        event_type=f"admin_support_{action}",
        title="Техподдержка",
        details=details,
        meta={"ticket_id": payload.entity_id, "note": payload.note or ""},
    )
    await bot.create_admin_audit_event(
        admin_actor="admin_web",
        target_tg_id=int(ticket["client_tg_id"]),
        action=f"support_{action}",
        details=details,
        meta={"ticket_id": payload.entity_id, "note": payload.note or ""},
    )
    return JSONResponse({"ok": True, "details": details})


class TradeOpenPayload(BaseModel):
    tg_id: int | None = None
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
    request: Request,
    payload: TradeOpenPayload,
):
    tg_id, auth_error = await resolve_api_user_id(request, payload.tg_id, require_session=True)
    if auth_error:
        return auth_error
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
    wc_cfg = await bot.get_client_trade_settings(tg_id)
    min_trade_amount = await bot.get_effective_min_trade_amount(tg_id, currency)
    auto_reject_trades = bool(wc_cfg["auto_reject_trades"]) if wc_cfg else False

    risk_percent = float(payload.risk_percent or 0.0)
    if risk_percent > 0:
        amount = round(balance * (risk_percent / 100.0), 2)

    if wc_cfg and bool(wc_cfg["blocked"]):
        return JSONResponse({"ok": False, "error": "Аккаунт заблокирован для торговли"}, status_code=403)
    if wc_cfg and not bool(wc_cfg["trading_enabled"]):
        return JSONResponse({"ok": False, "error": "Торговля для аккаунта отключена"}, status_code=403)
    if amount < min_trade_amount:
        return JSONResponse({"ok": False, "error": f"Минимальная сумма сделки: {min_trade_amount:.2f}"}, status_code=400)
    if auto_reject_trades:
        return JSONResponse({"ok": False, "error": "Сделки для этого аккаунта временно отклоняются"}, status_code=403)
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
    trade_id = await bot.create_active_trade(
        user_tg_id=tg_id,
        source="web",
        asset_name=asset_name,
        direction=direction,
        amount=amount,
        currency=currency,
        seconds=int(seconds),
        start_price=float(start_price),
        ticker=ticker,
        leverage=int(leverage),
        tp_price=tp_price,
        sl_price=sl_price,
        risk_percent=risk_percent,
    )
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
    await execute_query(
        "UPDATE worker_clients SET funnel_stage = ?, last_activity_at = CURRENT_TIMESTAMP WHERE client_tg_id = ?",
        ("trading", tg_id),
    )
    await bot.notify_worker_trade_event(
        client_tg_id=tg_id,
        asset_name=asset_name,
        direction=direction,
        amount=amount,
        currency=currency,
        seconds=int(seconds),
        leverage=int(leverage),
        trade_id=trade_id,
        source="web",
    )
    await bot.notify_trade_opened(trade_id)
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
async def api_trade_status(request: Request, trade_id: str, tg_id: int | None = None):
    resolved_tg_id, auth_error = await resolve_api_user_id(request, tg_id, require_session=True)
    if auth_error:
        return auth_error

    trade = await bot.get_active_trade(trade_id)
    if not trade:
        return JSONResponse({"ok": False, "error": "Trade not found"}, status_code=404)
    if int(trade["user_tg_id"]) != int(resolved_tg_id):
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)

    trade = await settle_web_trade(trade_id)
    if not trade:
        return JSONResponse({"ok": False, "error": "Trade not found"}, status_code=404)

    remaining = max(0, int(float(trade["close_ts"] or 0) - time.time()))
    payload = {
        "ok": True,
        "trade_id": trade_id,
        "status": trade["status"],
        "remaining": remaining,
        "start_price": float(trade["start_price"] or 0),
        "asset_name": str(trade["asset_name"] or ""),
        "direction": str(trade["direction"] or "up"),
        "amount": float(trade["amount"] or 0),
        "currency": str(trade["currency"] or "USD"),
        "seconds": int(trade["seconds"] or 0),
    }
    if trade["status"] == "closed":
        payload.update(
            {
                "is_win": bool(trade["is_win"]),
                "profit": float(trade["profit"]),
                "balance": float(await bot.get_user_balance(int(resolved_tg_id))),
                "end_price": float(trade["end_price"]),
                "change_percent": float(trade["change_percent"]),
                "close_reason": trade["close_reason"] or "time",
            }
        )
    return JSONResponse(payload)


class ExchangePayload(BaseModel):
    tg_id: int | None = None
    from_currency: str
    to_currency: str
    amount: float


@app.post("/api/exchange", response_class=JSONResponse)
async def api_exchange(
    request: Request,
    payload: ExchangePayload,
):
    tg_id, auth_error = await resolve_api_user_id(request, payload.tg_id, require_session=True)
    if auth_error:
        return auth_error
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
async def api_trade_open_positions(request: Request, tg_id: int | None = None):
    resolved_tg_id, auth_error = await resolve_api_user_id(request, tg_id, require_session=True)
    if auth_error:
        return auth_error

    await settle_user_open_trades(int(resolved_tg_id))
    items = []
    now = time.time()
    for tr in await bot.get_open_trades_for_user(int(resolved_tg_id)):
        items.append(
            {
                "trade_id": tr["trade_id"],
                "asset_name": tr["asset_name"],
                "direction": tr["direction"],
                "amount": tr["amount"],
                "currency": tr["currency"] or "USD",
                "leverage": int(tr["leverage"] or 10),
                "start_price": float(tr["start_price"] or 0.0),
                "seconds": int(tr["seconds"] or 0),
                "remaining": max(0, int(float(tr["close_ts"] or 0) - now)),
            }
        )
    items.sort(key=lambda x: x["remaining"])
    return JSONResponse({"ok": True, "items": items[:20]})


class TradeClosePayload(BaseModel):
    tg_id: int | None = None
    trade_id: str


class TradePartialClosePayload(BaseModel):
    tg_id: int | None = None
    trade_id: str
    ratio: float = 0.5


class TradeReversePayload(BaseModel):
    tg_id: int | None = None
    trade_id: str


@app.post("/api/trade/close", response_class=JSONResponse)
async def api_trade_close(request: Request, payload: TradeClosePayload):
    resolved_tg_id, auth_error = await resolve_api_user_id(request, payload.tg_id, require_session=True)
    if auth_error:
        return auth_error

    tr = await bot.get_active_trade(payload.trade_id)
    if not tr:
        return JSONResponse({"ok": False, "error": "Trade not found"}, status_code=404)
    if int(tr["user_tg_id"]) != int(resolved_tg_id):
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)
    if tr["status"] == "closed":
        return JSONResponse({"ok": True, "already_closed": True})

    await execute_query(
        "UPDATE active_trades SET close_ts = ?, close_reason = ? WHERE trade_id = ? AND status = 'open'",
        (time.time(), "manual", payload.trade_id),
    )
    closed = await settle_web_trade(payload.trade_id)
    if not closed:
        return JSONResponse({"ok": False, "error": "Trade not found"}, status_code=404)
    await log_web_activity_for_worker(
        client_tg_id=int(tr["user_tg_id"]),
        actor_tg_id=int(tr["user_tg_id"]),
        event_type="trade_closed_manual",
        title="Сделка закрыта",
        details=f"Ручное закрытие {tr['asset_name']}, результат {float(closed['profit']):+.2f}",
        amount=float(closed["profit"]),
        currency=tr["currency"] or "USD",
        meta={"trade_id": payload.trade_id, "asset_name": tr["asset_name"], "direction": tr["direction"], "reason": "manual"},
    )
    return JSONResponse(
        {
            "ok": True,
            "trade_id": payload.trade_id,
            "is_win": bool(closed["is_win"]),
            "profit": float(closed["profit"]),
            "balance": float(await bot.get_user_balance(int(resolved_tg_id))),
            "close_reason": closed["close_reason"] or "manual",
        }
    )


@app.post("/api/trade/close_partial", response_class=JSONResponse)
async def api_trade_close_partial(request: Request, payload: TradePartialClosePayload):
    resolved_tg_id, auth_error = await resolve_api_user_id(request, payload.tg_id, require_session=True)
    if auth_error:
        return auth_error

    tr = await bot.get_active_trade(payload.trade_id)
    if not tr:
        return JSONResponse({"ok": False, "error": "Trade not found"}, status_code=404)
    if int(tr["user_tg_id"]) != int(resolved_tg_id):
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)
    if tr["status"] == "closed":
        return JSONResponse({"ok": False, "error": "Trade already closed"}, status_code=400)

    ratio = float(payload.ratio or 0.0)
    if ratio <= 0.0 or ratio >= 1.0:
        return JSONResponse({"ok": False, "error": "Ratio must be between 0 and 1"}, status_code=400)

    total_amount = float(tr["amount"] or 0.0)
    close_amount = round(total_amount * ratio, 2)
    remain_amount = round(total_amount - close_amount, 2)
    if close_amount <= 0 or remain_amount <= 0:
        return JSONResponse({"ok": False, "error": "Amount is too small for partial close"}, status_code=400)

    now_ts = time.time()
    remain_seconds = max(5, int(float(tr["close_ts"] or 0.0) - now_ts))
    current_mark = await fetch_live_mark_or_none(str(tr["asset_name"] or ""))
    if not current_mark or current_mark <= 0:
        current_mark = float(tr["start_price"] or 0.0) or 1.0

    await execute_query(
        "UPDATE active_trades SET amount = ?, close_ts = ?, close_reason = ? WHERE trade_id = ? AND status = 'open'",
        (close_amount, now_ts, "manual", payload.trade_id),
    )
    closed = await settle_web_trade(payload.trade_id)
    if not closed:
        return JSONResponse({"ok": False, "error": "Failed to settle partial close"}, status_code=500)

    new_trade_id = await bot.create_active_trade(
        user_tg_id=int(tr["user_tg_id"]),
        source=str(tr["source"] or "web"),
        asset_name=str(tr["asset_name"] or ""),
        direction=str(tr["direction"] or "up"),
        amount=remain_amount,
        currency=str(tr["currency"] or "USD"),
        seconds=int(remain_seconds),
        start_price=float(_format_price(current_mark)),
        ticker=str(tr["ticker"] or ""),
        leverage=int(tr["leverage"] or 10),
        tp_price=float(tr["tp_price"]) if tr["tp_price"] is not None else None,
        sl_price=float(tr["sl_price"]) if tr["sl_price"] is not None else None,
        risk_percent=float(tr["risk_percent"] or 0.0),
        close_reason=str(tr["close_reason"] or "time"),
    )
    await bot.notify_trade_opened(new_trade_id)
    await log_web_activity_for_worker(
        client_tg_id=int(tr["user_tg_id"]),
        actor_tg_id=int(tr["user_tg_id"]),
        event_type="trade_partial_close",
        title="Частичное закрытие",
        details=f"Частичное закрытие {str(tr['asset_name'])}: {close_amount:.2f} закрыто, {remain_amount:.2f} переоткрыто",
        amount=float(closed["profit"] or 0.0),
        currency=str(tr["currency"] or "USD"),
        meta={"trade_id": payload.trade_id, "new_trade_id": new_trade_id, "ratio": ratio},
    )
    return JSONResponse(
        {
            "ok": True,
            "closed_trade_id": payload.trade_id,
            "new_trade_id": new_trade_id,
            "closed_profit": float(closed["profit"] or 0.0),
            "remaining_amount": remain_amount,
            "remaining_seconds": remain_seconds,
            "balance": float(await bot.get_user_balance(int(resolved_tg_id))),
        }
    )


@app.post("/api/trade/reverse", response_class=JSONResponse)
async def api_trade_reverse(request: Request, payload: TradeReversePayload):
    resolved_tg_id, auth_error = await resolve_api_user_id(request, payload.tg_id, require_session=True)
    if auth_error:
        return auth_error

    tr = await bot.get_active_trade(payload.trade_id)
    if not tr:
        return JSONResponse({"ok": False, "error": "Trade not found"}, status_code=404)
    if int(tr["user_tg_id"]) != int(resolved_tg_id):
        return JSONResponse({"ok": False, "error": "Forbidden"}, status_code=403)
    if tr["status"] == "closed":
        return JSONResponse({"ok": False, "error": "Trade already closed"}, status_code=400)

    now_ts = time.time()
    remain_seconds = max(10, int(float(tr["close_ts"] or 0.0) - now_ts))
    opposite_direction = "down" if str(tr["direction"] or "up") == "up" else "up"
    current_mark = await fetch_live_mark_or_none(str(tr["asset_name"] or ""))
    if not current_mark or current_mark <= 0:
        current_mark = float(tr["start_price"] or 0.0) or 1.0

    await execute_query(
        "UPDATE active_trades SET close_ts = ?, close_reason = ? WHERE trade_id = ? AND status = 'open'",
        (now_ts, "manual", payload.trade_id),
    )
    closed = await settle_web_trade(payload.trade_id)
    if not closed:
        return JSONResponse({"ok": False, "error": "Failed to close current trade"}, status_code=500)

    new_trade_id = await bot.create_active_trade(
        user_tg_id=int(tr["user_tg_id"]),
        source=str(tr["source"] or "web"),
        asset_name=str(tr["asset_name"] or ""),
        direction=opposite_direction,
        amount=float(tr["amount"] or 0.0),
        currency=str(tr["currency"] or "USD"),
        seconds=int(remain_seconds),
        start_price=float(_format_price(current_mark)),
        ticker=str(tr["ticker"] or ""),
        leverage=int(tr["leverage"] or 10),
        tp_price=float(tr["tp_price"]) if tr["tp_price"] is not None else None,
        sl_price=float(tr["sl_price"]) if tr["sl_price"] is not None else None,
        risk_percent=float(tr["risk_percent"] or 0.0),
        close_reason="time",
    )
    await bot.notify_trade_opened(new_trade_id)
    await log_web_activity_for_worker(
        client_tg_id=int(tr["user_tg_id"]),
        actor_tg_id=int(tr["user_tg_id"]),
        event_type="trade_reversed",
        title="Реверс позиции",
        details=f"Позиция {str(tr['asset_name'])} развернута в {'ЛОНГ' if opposite_direction == 'up' else 'ШОРТ'}",
        amount=float(closed["profit"] or 0.0),
        currency=str(tr["currency"] or "USD"),
        meta={"trade_id": payload.trade_id, "new_trade_id": new_trade_id, "new_direction": opposite_direction},
    )
    return JSONResponse(
        {
            "ok": True,
            "closed_trade_id": payload.trade_id,
            "new_trade_id": new_trade_id,
            "new_direction": opposite_direction,
            "closed_profit": float(closed["profit"] or 0.0),
            "balance": float(await bot.get_user_balance(int(resolved_tg_id))),
        }
    )


class DepositRequestPayload(BaseModel):
    tg_id: int | None = None
    amount: float
    method: str  # crypto | trc20 | card


@app.post("/api/deposit/request", response_class=JSONResponse)
async def api_deposit_request(request: Request, payload: DepositRequestPayload):
    resolved_tg_id, auth_error = await resolve_api_user_id(request, payload.tg_id, require_session=True)
    if auth_error:
        return auth_error

    user = await fetch_one("SELECT currency, first_name, username FROM users WHERE tg_id = ?", (resolved_tg_id,))
    if not user:
        return JSONResponse({"ok": False, "error": "Пользователь не найден"}, status_code=404)
    wc_cfg = await bot.get_client_trade_settings(int(resolved_tg_id))
    if wc_cfg and bool(wc_cfg["blocked"]):
        return JSONResponse({"ok": False, "error": "Аккаунт временно заблокирован. Обратитесь в поддержку."}, status_code=403)
    if payload.amount <= 0:
        return JSONResponse({"ok": False, "error": "Сумма должна быть больше 0"}, status_code=400)

    currency = user["currency"] or "USD"
    method = payload.method.strip().lower()
    if method not in {"crypto", "trc20", "card"}:
        return JSONResponse({"ok": False, "error": "Неподдерживаемый метод"}, status_code=400)
    effective_min_deposit = await bot.get_effective_min_deposit_amount(int(resolved_tg_id), currency)
    global_min_deposit_usdt = await bot.get_global_min_deposit_usdt()
    if float(payload.amount) < effective_min_deposit:
        return JSONResponse(
            {
                "ok": False,
                "error": f"Минимальное пополнение: {effective_min_deposit:.2f} {currency} (эквивалент {global_min_deposit_usdt:.2f} USDT)",
            },
            status_code=400,
        )

    await notify_worker_deposit_event(
        client_tg_id=int(resolved_tg_id),
        first_name=user["first_name"],
        username=user["username"],
        amount=payload.amount,
        currency=currency,
        method=method,
        stage="request_created",
    )
    worker_id = await bot.get_worker_for_client(int(resolved_tg_id))
    dep_id = await bot.create_deposit_request(int(resolved_tg_id), payload.amount, currency, method)
    method_label = deposit_method_label(method)
    await execute_query(
        "UPDATE worker_clients SET funnel_stage = ?, last_activity_at = CURRENT_TIMESTAMP WHERE client_tg_id = ?",
        ("support_wait", int(resolved_tg_id)),
    )
    await ensure_deposit_support_ticket(
        client_tg_id=int(resolved_tg_id),
        worker_tg_id=worker_id,
        source="web",
        amount=payload.amount,
        currency=currency,
        method=method,
        deposit_id=dep_id,
    )
    await bot.notify_admin_deposit_request(
        dep_id=dep_id,
        user_tg_id=int(resolved_tg_id),
        amount=float(payload.amount),
        currency=currency,
        method=method,
        source="web",
        worker_tg_id=worker_id,
        first_name=user["first_name"] if user else None,
        username=user["username"] if user else None,
    )
    await log_web_activity_for_worker(
        client_tg_id=int(resolved_tg_id),
        actor_tg_id=int(resolved_tg_id),
        event_type="web_deposit_support_opened",
        title="Переход в поддержку по пополнению",
        details=f"Лохматый перешёл в поддержку по пополнению через WebApp. Метод: {method_label}.",
        amount=payload.amount,
        currency=currency,
        meta={"deposit_id": dep_id, "method": method},
    )
    await notify_worker_deposit_event(
        client_tg_id=int(resolved_tg_id),
        first_name=user["first_name"],
        username=user["username"],
        amount=payload.amount,
        currency=currency,
        method=method,
        stage="support_opened",
        deposit_id=dep_id,
    )

    support_contact = bot.support_contact_text()
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


class WithdrawRequestPayload(BaseModel):
    tg_id: int | None = None
    amount: float
    method: str  # trc20 | card
    details: str


@app.post("/api/withdraw/request", response_class=JSONResponse)
async def api_withdraw_request(request: Request, payload: WithdrawRequestPayload):
    resolved_tg_id, auth_error = await resolve_api_user_id(request, payload.tg_id, require_session=True)
    if auth_error:
        return auth_error

    user = await fetch_one("SELECT currency, first_name, username, balance FROM users WHERE tg_id = ?", (resolved_tg_id,))
    if not user:
        return JSONResponse({"ok": False, "error": "Пользователь не найден"}, status_code=404)
    access = await bot.get_client_access_flags(int(resolved_tg_id))
    if access["blocked"]:
        return JSONResponse({"ok": False, "error": "Аккаунт временно заблокирован. Обратитесь в поддержку."}, status_code=403)
    if not access["withdraw_enabled"]:
        return JSONResponse({"ok": False, "error": "Вывод средств временно отключён. Обратитесь в поддержку."}, status_code=403)
    if payload.amount <= 0:
        return JSONResponse({"ok": False, "error": "Сумма должна быть больше 0"}, status_code=400)

    currency = user["currency"] or "USD"
    balance = float(user["balance"] or 0.0)
    if float(payload.amount) > balance:
        return JSONResponse({"ok": False, "error": f"Недостаточно средств. Доступно: {balance:.2f} {currency}."}, status_code=400)

    method = payload.method.strip().lower()
    if method not in {"trc20", "card"}:
        return JSONResponse({"ok": False, "error": "Неподдерживаемый метод"}, status_code=400)
    details = (payload.details or "").strip()
    if not details:
        return JSONResponse({"ok": False, "error": "Укажите реквизиты для вывода"}, status_code=400)

    wd_id = await bot.create_withdrawal(int(resolved_tg_id), payload.amount, currency, method, details)
    await log_web_activity_for_worker(
        client_tg_id=int(resolved_tg_id),
        actor_tg_id=int(resolved_tg_id),
        event_type="web_withdraw_request",
        title="Заявка на вывод",
        details=f"Лохматый создал заявку на вывод через WebApp. Метод: {deposit_method_label(method)}.",
        amount=payload.amount,
        currency=currency,
        meta={"withdrawal_id": wd_id, "method": method},
    )
    await bot.notify_worker_withdraw_event(
        client_tg_id=int(resolved_tg_id),
        first_name=user["first_name"],
        username=user["username"],
        amount=payload.amount,
        currency=currency,
        method=method,
        withdrawal_id=wd_id,
        source="web",
    )

    details_label = "Карта" if method == "card" else "Кошелёк"
    text_admin = (
        "💸 <b>Новая заявка на вывод</b>\n\n"
        f"ID заявки: <b>{wd_id}</b>\n"
        f"Пользователь: <a href='tg://user?id={int(resolved_tg_id)}'>{user['first_name'] or 'Пользователь'}</a>\n"
        f"TG ID: <code>{int(resolved_tg_id)}</code>\n"
        f"Сумма: {float(payload.amount):.2f} {currency}\n"
        f"Метод: {deposit_method_label(method)}\n"
        f"{details_label}: <code>{html.escape(details)}</code>"
    )
    for admin_id in bot.config.admin_ids:
        with contextlib.suppress(Exception):
            await bot.bot.send_message(admin_id, text_admin, reply_markup=bot.withdrawal_admin_keyboard(wd_id))
    if bot.config.log_chat_id:
        with contextlib.suppress(Exception):
            await bot.bot.send_message(bot.config.log_chat_id, text_admin, reply_markup=bot.withdrawal_admin_keyboard(wd_id))

    return JSONResponse({"ok": True, "withdrawal_id": wd_id, "message": "Заявка на вывод отправлена на обработку."})


@app.get("/api/overview", response_class=JSONResponse)
async def api_overview():
    users = await fetch_one("SELECT COUNT(*) AS c FROM users")
    deals = await fetch_one("SELECT COUNT(*) AS c FROM deals")
    pnl = await fetch_one("SELECT COALESCE(SUM(profit), 0) AS s FROM deals")
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


async def build_market_snapshot_payload(symbol: str = "BTC") -> dict:
    ticker = ticker_from_symbol_input(symbol)
    requested_name = (symbol or "").strip()
    requested_display_symbol = symbol_from_asset_name(requested_name)
    await refresh_depth_quick(ticker, levels=10, min_interval=2.0, wait_sec=0.8)
    quote = MARKET_SERVICE.get_quote(ticker) or {}
    asks, bids = MARKET_SERVICE.get_depth(ticker, levels=10)
    mark = float(quote.get("mark") or 0)
    spread = float(quote.get("spread") or 0)
    day_high = float(quote.get("high") or mark)
    day_low = float(quote.get("low") or mark)
    uses_legacy_market = mark <= 0 or (ticker == "BTCUSDT" and requested_display_symbol != "BTC")
    if uses_legacy_market:
        legacy_symbol = requested_display_symbol if requested_display_symbol != "UNKN" else legacy_symbol_from_symbol_or_ticker(symbol)
        asks, bids, legacy_mark = build_orderbook(legacy_symbol, levels=10)
        spread = max(0.00001, asks[0]["price"] - bids[0]["price"])
        day_stats = MARKET_DAY_STATS.get(legacy_symbol, {"open": legacy_mark, "high": legacy_mark, "low": legacy_mark})
        mark = legacy_mark
        day_high = float(day_stats["high"])
        day_low = float(day_stats["low"])
    tape_head = current_tape_items(1)
    tick = tape_head[0] if tape_head else None
    return {
        "ok": True,
        "symbol": requested_display_symbol if requested_display_symbol != "UNKN" else ticker_to_symbol(ticker),
        "asset_name": requested_name or ticker_to_symbol(ticker),
        "ticker": ticker,
        "market_mode": "synthetic" if uses_legacy_market else "live",
        "ts": int(time.time()),
        "mark": _format_price(mark),
        "spread": round(float(spread), 5 if mark < 1 else 2),
        "high": _format_price(day_high),
        "low": _format_price(day_low),
        "asks": asks,
        "bids": bids,
        "tick": tick,
    }


@app.get("/api/market/snapshot", response_class=JSONResponse)
async def api_market_snapshot(symbol: str = "BTC"):
    return JSONResponse(await build_market_snapshot_payload(symbol))


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
    trade = await bot.get_active_trade(trade_id)
    if not trade:
        return None
    if trade["status"] == "closed":
        return trade
    ticker = trade["ticker"] or ticker_from_asset_name(trade["asset_name"])
    live_mark = await fetch_live_mark_or_none(ticker)
    if live_mark is None or live_mark <= 0:
        live_mark = next_symbol_price(symbol_from_asset_name(trade["asset_name"]))
    now_ts = time.time()
    direction = trade["direction"]
    close_reason = trade["close_reason"] or "time"
    if close_reason != "manual" and trade["tp_price"] is not None:
        tp_price = float(trade["tp_price"])
        if (direction == "up" and live_mark >= tp_price) or (direction == "down" and live_mark <= tp_price):
            await execute_query("UPDATE active_trades SET close_ts = ?, close_reason = ? WHERE trade_id = ?", (now_ts, "tp", trade_id))
            close_reason = "tp"
    if close_reason != "manual" and trade["sl_price"] is not None:
        sl_price = float(trade["sl_price"])
        if (direction == "up" and live_mark <= sl_price) or (direction == "down" and live_mark >= sl_price):
            await execute_query("UPDATE active_trades SET close_ts = ?, close_reason = ? WHERE trade_id = ?", (now_ts, "sl", trade_id))
            close_reason = "sl"
    trade = await bot.get_active_trade(trade_id)
    if now_ts < float(trade["close_ts"] or now_ts):
        return trade
    return await bot.settle_active_trade(trade_id, live_end_price=float(live_mark), close_reason=close_reason)


async def settle_user_open_trades(tg_id: int):
    for trade in await bot.get_open_trades_for_user(int(tg_id)):
        await settle_web_trade(str(trade["trade_id"]))


@app.websocket("/ws/user")
async def ws_user(websocket: WebSocket):
    await websocket.accept()
    tg_id = int((websocket.session or {}).get(USER_SESSION_KEY) or 0)
    # Backward compatibility for clients that still send tg_id in first frame.
    if tg_id <= 0:
        with contextlib.suppress(Exception):
            first = await asyncio.wait_for(websocket.receive_text(), timeout=3.0)
            payload = json.loads(first)
            tg_id = int(payload.get("tg_id") or 0)

    if tg_id <= 0:
        with contextlib.suppress(Exception):
            await websocket.close()
        return

    try:
        while True:
            await settle_user_open_trades(tg_id)
            user = await fetch_one("SELECT balance, currency FROM users WHERE tg_id = ?", (tg_id,))
            now = time.time()
            open_positions = []
            for tr in await bot.get_open_trades_for_user(tg_id):
                open_positions.append(
                    {
                        "trade_id": tr["trade_id"],
                        "asset_name": tr["asset_name"],
                        "direction": tr["direction"],
                        "amount": tr["amount"],
                        "currency": tr["currency"] or "USD",
                        "leverage": int(tr["leverage"] or 10),
                        "start_price": float(tr["start_price"] or 0.0),
                        "seconds": int(tr["seconds"] or 0),
                        "remaining": max(0, int(float(tr["close_ts"] or 0) - now)),
                    }
                )
            open_positions.sort(key=lambda x: x["remaining"])
            open_count = len(open_positions)
            latest_deal = await fetch_one(
                "SELECT id, asset_name, profit, created_at FROM deals WHERE user_tg_id = ? ORDER BY id DESC LIMIT 1",
                (tg_id,),
            )
            latest_deal_payload = None
            if latest_deal:
                latest_deal_payload = dict(latest_deal)
                created_at = latest_deal_payload.get("created_at")
                if created_at is not None:
                    latest_deal_payload["created_at"] = str(created_at)
            await websocket.send_json(
                {
                    "type": "user",
                    "balance": float(user["balance"] if user else 0.0),
                    "currency": str(user["currency"] if user and user["currency"] else "USD"),
                    "open_trades": open_count,
                    "open_positions": open_positions[:10],
                    "latest_deal": latest_deal_payload,
                }
            )
            await asyncio.sleep(1.2)
    except WebSocketDisconnect:
        return
    except Exception:
        with contextlib.suppress(Exception):
            await websocket.close()
        return


@app.websocket("/ws/live")
async def ws_live(websocket: WebSocket):
    await websocket.accept()
    session_tg_id = int((websocket.session or {}).get(USER_SESSION_KEY) or 0)
    session_is_admin = bool((websocket.session or {}).get("is_admin"))
    try:
        first = await websocket.receive_text()
        payload = json.loads(first)
    except Exception:
        await websocket.close()
        return

    scope = str(payload.get("scope") or "").strip().lower()
    payload_tg_id = int(payload.get("tg_id") or 0)
    tg_id = session_tg_id or payload_tg_id
    wc_id = int(payload.get("wc_id") or 0)

    if tg_id <= 0:
        await websocket.close()
        return

    if scope == "worker":
        user = await fetch_one("SELECT is_worker FROM users WHERE tg_id = ?", (tg_id,))
        if not user or not bool(user["is_worker"]):
            await websocket.close()
            return
    elif scope == "worker_client":
        user = await fetch_one("SELECT is_worker FROM users WHERE tg_id = ?", (tg_id,))
        if not user or not bool(user["is_worker"]) or wc_id <= 0:
            await websocket.close()
            return
    elif scope == "admin":
        if not session_is_admin:
            row = await fetch_one("SELECT is_admin FROM users WHERE tg_id = ?", (tg_id,))
            if not row or not bool(row["is_admin"]):
                await websocket.close()
                return
    else:
        await websocket.close()
        return

    try:
        while True:
            if scope == "worker":
                data = await build_worker_dashboard_payload(tg_id)
            elif scope == "worker_client":
                data = await build_worker_client_snapshot_payload(tg_id, wc_id)
            else:
                data = await build_admin_dashboard_payload()
            data["type"] = scope
            await websocket.send_json(data)
            await asyncio.sleep(1.6)
    except WebSocketDisconnect:
        return
