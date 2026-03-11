import os
import json
from dataclasses import dataclass
from typing import Optional

import db_compat as aiosqlite

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, WebAppInfo
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv
import asyncio
import random


load_dotenv()

DB_PATH = os.getenv("DB_PATH", "bot.db")
BOT_USERNAME = ""  # will be filled in main()


@dataclass
class Config:
    bot_token: str
    admin_ids: list[int]
    log_chat_id: Optional[int]
    crypto_bot_url: str
    trc20_address: str
    support_url: str
    webapp_url: str
    card_pay_url: str
    card_requisites: str


config = Config(
    bot_token=os.getenv("BOT_TOKEN", ""),
    admin_ids=[int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()],
    log_chat_id=int(os.getenv("LOG_CHAT_ID")) if os.getenv("LOG_CHAT_ID") else None,
    crypto_bot_url=os.getenv("CRYPTO_BOT_URL", ""),
    trc20_address=os.getenv("TRC20_ADDRESS", ""),
    support_url=os.getenv("SUPPORT_URL", "https://t.me/your_support_chat"),
    webapp_url=(os.getenv("WEBAPP_URL") or os.getenv("WEBHOOK_BASE_URL") or "").rstrip("/"),
    card_pay_url=os.getenv("CARD_PAY_URL", ""),
    card_requisites=os.getenv("CARD_REQUISITES", ""),
)

bot = Bot(token=config.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# client_id -> worker_id
ACTIVE_DIALOGS_CLIENT: dict[int, int] = {}
# worker_id -> client_id
ACTIVE_DIALOGS_WORKER: dict[int, int] = {}


class DepositStates(StatesGroup):
    waiting_amount = State()
    waiting_method = State()


I18N = {
    "ru": {
        "open_app": "🚀 Открыть приложение",
        "portfolio": "📁 Портфель",
        "open_ecn": "📈 Открыть ECN",
        "info": "ℹ️ Инфо",
        "support": "🌐 Тех. Поддержка",
        "deposit": "📥Пополнить",
        "withdraw": "📤Вывести",
        "verify": "✅Верификация",
        "my_deals": "📑Мои сделки",
        "settings": "⚙️Настройки",
        "worker_panel": "⚒Панель воркера",
        "admin_panel": "🛠Админ-панель",
        "pay_card": "💳Банковской картой",
        "deposit_cancel": "❌Отмена",
        "deposit_enter_amount": "📥 На сколько вы хотите пополнить баланс? Введите сумму в {currency}.",
        "deposit_invalid_amount": "❗ Введите корректную положительную сумму.",
        "deposit_choose_method": "Вы хотите пополнить баланс на <b>{amount:.2f} {currency}</b>.\n\nВыберите удобный способ оплаты:",
        "deposit_cancelled": "❌ Пополнение отменено.",
        "profile_title": "🧾 <b>Профиль</b>",
        "menu_welcome": "👋 Приветствую, <b>{name}</b>!\n\nЭто телеграм-бот криптоплатформы для торговли фьючерсами.\nЧерез него вы сможете управлять своим аккаунтом, балансом и сделками.\n\n👇 Выберите раздел:",
        "menu_info_text": "ℹ️ Информация\n\n📣 Новости: https://www.youtube.com/channel/UCVj_rwnR1p-7Da15L8MwRNQ\n📄 Пользовательское соглашение: https://telegra.ph/Polzovatelskoe-soglashenie-03-27-13",
        "menu_support_text": "📩 Связаться с технической поддержкой можно здесь:\n{url}",
        "profile_id": "└ Идентификатор: <code>{id}</code>",
        "profile_username": "└ Username: @{username}",
        "profile_username_none": "└ Username: —",
        "profile_language": "└ Язык: {lang_value}",
        "profile_currency": "└ Валюта: {currency}",
        "profile_verification": "└ Верификация: Нет",
        "profile_worker": "└ Воркер: <code>{worker}</code>",
        "profile_worker_none": "└ Воркер: —",
        "profile_deals_title": "📊 <b>Информация о сделках</b>",
        "profile_deals_total": "└ Всего сделок: {total} шт.",
        "profile_deals_wins": "└ Успешные сделки: {wins} шт.",
        "profile_deals_losses": "└ Безуспешные сделки: {losses} шт.",
        "profile_deals_pnl": "└ PnL: {pnl:.2f} {currency}",
        "profile_balance_title": "💰 <b>Баланс</b>",
        "profile_balance_value": "└ Баланс: {balance:.2f} {currency}",
        "profile_pending_value": "└ На выводе: {pending:.2f} {currency}",
        "settings_title": "🧩 <b>Настройки</b>\n\n📘 Язык интерфейса: <b>{lang_value}</b>\n💱 Валюта: <b>{cur}</b>\n\nВыберите, что хотите изменить:",
        "settings_choose_lang": "🌐 Выберите язык интерфейса:",
        "settings_choose_currency": "💱 Выберите валюту:",
        "settings_btn_lang": "🌐Язык интерфейса",
        "settings_btn_currency": "💱Валюта",
        "settings_btn_back": "⬅️В профиль",
        "my_deals_empty": "📑 У вас пока нет завершённых сделок.",
        "my_deals_title": "📑 <b>Последние сделки</b>",
    },
    "en": {
        "open_app": "🚀 Open App",
        "portfolio": "📁 Portfolio",
        "open_ecn": "📈 Open ECN",
        "info": "ℹ️ Info",
        "support": "🌐 Support",
        "deposit": "📥Deposit",
        "withdraw": "📤Withdraw",
        "verify": "✅Verification",
        "my_deals": "📑My Deals",
        "settings": "⚙️Settings",
        "worker_panel": "⚒Worker Panel",
        "admin_panel": "🛠Admin Panel",
        "pay_card": "💳By Bank Card",
        "deposit_cancel": "❌Cancel",
        "deposit_enter_amount": "📥 How much do you want to deposit? Enter amount in {currency}.",
        "deposit_invalid_amount": "❗ Enter a valid positive amount.",
        "deposit_choose_method": "You want to top up <b>{amount:.2f} {currency}</b>.\n\nChoose payment method:",
        "deposit_cancelled": "❌ Deposit canceled.",
        "profile_title": "🧾 <b>Profile</b>",
        "menu_welcome": "👋 Welcome, <b>{name}</b>!\n\nThis is a Telegram crypto futures trading bot.\nYou can manage your account, balance, and deals here.\n\n👇 Choose a section:",
        "menu_info_text": "ℹ️ Information\n\n📣 News: https://www.youtube.com/channel/UCVj_rwnR1p-7Da15L8MwRNQ\n📄 User agreement: https://telegra.ph/Polzovatelskoe-soglashenie-03-27-13",
        "menu_support_text": "📩 Contact technical support here:\n{url}",
        "profile_id": "└ ID: <code>{id}</code>",
        "profile_username": "└ Username: @{username}",
        "profile_username_none": "└ Username: —",
        "profile_language": "└ Language: {lang_value}",
        "profile_currency": "└ Currency: {currency}",
        "profile_verification": "└ Verification: No",
        "profile_worker": "└ Worker: <code>{worker}</code>",
        "profile_worker_none": "└ Worker: —",
        "profile_deals_title": "📊 <b>Deals stats</b>",
        "profile_deals_total": "└ Total deals: {total}",
        "profile_deals_wins": "└ Winning deals: {wins}",
        "profile_deals_losses": "└ Losing deals: {losses}",
        "profile_deals_pnl": "└ PnL: {pnl:.2f} {currency}",
        "profile_balance_title": "💰 <b>Balance</b>",
        "profile_balance_value": "└ Balance: {balance:.2f} {currency}",
        "profile_pending_value": "└ Pending withdraw: {pending:.2f} {currency}",
        "settings_title": "🧩 <b>Settings</b>\n\n📘 Interface language: <b>{lang_value}</b>\n💱 Currency: <b>{cur}</b>\n\nChoose what to change:",
        "settings_choose_lang": "🌐 Choose interface language:",
        "settings_choose_currency": "💱 Choose currency:",
        "settings_btn_lang": "🌐Language",
        "settings_btn_currency": "💱Currency",
        "settings_btn_back": "⬅️Back to profile",
        "my_deals_empty": "📑 You have no completed deals yet.",
        "my_deals_title": "📑 <b>Recent deals</b>",
    },
    "uk": {
        "open_app": "🚀 Відкрити застосунок",
        "portfolio": "📁 Портфель",
        "open_ecn": "📈 Відкрити ECN",
        "info": "ℹ️ Інфо",
        "support": "🌐 Тех. Підтримка",
        "deposit": "📥Поповнити",
        "withdraw": "📤Вивести",
        "verify": "✅Верифікація",
        "my_deals": "📑Мої угоди",
        "settings": "⚙️Налаштування",
        "worker_panel": "⚒Панель воркера",
        "admin_panel": "🛠Адмін-панель",
        "pay_card": "💳Банківською карткою",
        "deposit_cancel": "❌Скасувати",
        "deposit_enter_amount": "📥 На яку суму ви хочете поповнити баланс? Вкажіть суму в {currency}.",
        "deposit_invalid_amount": "❗ Введіть коректну додатну суму.",
        "deposit_choose_method": "Ви хочете поповнити баланс на <b>{amount:.2f} {currency}</b>.\n\nОберіть спосіб оплати:",
        "deposit_cancelled": "❌ Поповнення скасовано.",
        "profile_title": "🧾 <b>Профіль</b>",
        "menu_welcome": "👋 Вітаю, <b>{name}</b>!\n\nЦе Telegram-бот криптоплатформи для торгівлі ф'ючерсами.\nТут ви можете керувати акаунтом, балансом і угодами.\n\n👇 Оберіть розділ:",
        "menu_info_text": "ℹ️ Інформація\n\n📣 Новини: https://www.youtube.com/channel/UCVj_rwnR1p-7Da15L8MwRNQ\n📄 Угода користувача: https://telegra.ph/Polzovatelskoe-soglashenie-03-27-13",
        "menu_support_text": "📩 Зв'язатися з техпідтримкою можна тут:\n{url}",
        "profile_id": "└ Ідентифікатор: <code>{id}</code>",
        "profile_username": "└ Username: @{username}",
        "profile_username_none": "└ Username: —",
        "profile_language": "└ Мова: {lang_value}",
        "profile_currency": "└ Валюта: {currency}",
        "profile_verification": "└ Верифікація: Ні",
        "profile_worker": "└ Воркер: <code>{worker}</code>",
        "profile_worker_none": "└ Воркер: —",
        "profile_deals_title": "📊 <b>Інформація про угоди</b>",
        "profile_deals_total": "└ Всього угод: {total} шт.",
        "profile_deals_wins": "└ Успішні угоди: {wins} шт.",
        "profile_deals_losses": "└ Неуспішні угоди: {losses} шт.",
        "profile_deals_pnl": "└ PnL: {pnl:.2f} {currency}",
        "profile_balance_title": "💰 <b>Баланс</b>",
        "profile_balance_value": "└ Баланс: {balance:.2f} {currency}",
        "profile_pending_value": "└ На виводі: {pending:.2f} {currency}",
        "settings_title": "🧩 <b>Налаштування</b>\n\n📘 Мова інтерфейсу: <b>{lang_value}</b>\n💱 Валюта: <b>{cur}</b>\n\nОберіть, що хочете змінити:",
        "settings_choose_lang": "🌐 Оберіть мову інтерфейсу:",
        "settings_choose_currency": "💱 Оберіть валюту:",
        "settings_btn_lang": "🌐Мова інтерфейсу",
        "settings_btn_currency": "💱Валюта",
        "settings_btn_back": "⬅️До профілю",
        "my_deals_empty": "📑 У вас ще немає завершених угод.",
        "my_deals_title": "📑 <b>Останні угоди</b>",
    },
}


def normalize_lang(lang: str | None) -> str:
    code = (lang or "ru").strip().lower()
    if code.startswith("uk"):
        return "uk"
    if code.startswith("en"):
        return "en"
    return "ru"


def t(lang: str | None, key: str, **kwargs) -> str:
    lang_code = normalize_lang(lang)
    text = I18N.get(lang_code, I18N["ru"]).get(key, I18N["ru"].get(key, key))
    return text.format(**kwargs) if kwargs else text


def tr(lang: str | None, ru: str, en: str, uk: str) -> str:
    code = normalize_lang(lang)
    if code == "en":
        return en
    if code == "uk":
        return uk
    return ru


def support_contact_text() -> str:
    raw = (config.support_url or "").strip()
    if not raw:
        return "не задано админом"
    if raw.startswith("https://t.me/"):
        nick = raw.rsplit("/", 1)[-1].strip()
        if nick:
            return f"@{nick} ({raw})"
    return raw


class WithdrawStates(StatesGroup):
    waiting_amount = State()
    waiting_card = State()
    waiting_wallet = State()


class AdminStates(StatesGroup):
    waiting_worker_id = State()


class AdminPaymentStates(StatesGroup):
    waiting_crypto_url = State()
    waiting_trc20 = State()
    waiting_card_url = State()
    waiting_card_requisites = State()
    waiting_support_url = State()
    waiting_webapp_url = State()
    waiting_asset_name = State()


class WorkerServiceStates(StatesGroup):
    waiting_min_dep = State()
    waiting_min_wd = State()


class WorkerClientStates(StatesGroup):
    waiting_balance_amount = State()
    waiting_min_dep = State()
    waiting_min_wd = State()
    waiting_transfer_worker = State()


class ECNStates(StatesGroup):
    choosing_asset = State()
    choosing_direction = State()
    waiting_amount = State()
    choosing_expiration = State()


class WorkerLuckStates(StatesGroup):
    waiting_luck_percent = State()


async def init_db():
    is_pg = aiosqlite.using_postgres()

    users_ddl = (
        """CREATE TABLE IF NOT EXISTS users(
                id BIGSERIAL PRIMARY KEY,
                tg_id BIGINT UNIQUE,
                first_name TEXT,
                username TEXT,
                language TEXT,
                currency TEXT,
                accepted_rules INTEGER DEFAULT 0,
                balance DOUBLE PRECISION DEFAULT 0,
                is_admin INTEGER DEFAULT 0,
                is_worker INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        if is_pg
        else
        """CREATE TABLE IF NOT EXISTS users(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER UNIQUE,
                first_name TEXT,
                username TEXT,
                language TEXT,
                currency TEXT,
                accepted_rules INTEGER DEFAULT 0,
                balance REAL DEFAULT 0,
                is_admin INTEGER DEFAULT 0,
                is_worker INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )"""
    )
    referrals_ddl = (
        """CREATE TABLE IF NOT EXISTS referrals(
                id BIGSERIAL PRIMARY KEY,
                worker_tg_id BIGINT,
                invited_tg_id BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(worker_tg_id, invited_tg_id)
            )"""
        if is_pg
        else
        """CREATE TABLE IF NOT EXISTS referrals(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_tg_id INTEGER,
                invited_tg_id INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(worker_tg_id, invited_tg_id)
            )"""
    )
    withdrawals_ddl = (
        """CREATE TABLE IF NOT EXISTS withdrawals(
                id BIGSERIAL PRIMARY KEY,
                user_tg_id BIGINT,
                amount DOUBLE PRECISION,
                currency TEXT,
                method TEXT,
                details TEXT,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        if is_pg
        else
        """CREATE TABLE IF NOT EXISTS withdrawals(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_tg_id INTEGER,
                amount REAL,
                currency TEXT,
                method TEXT,
                details TEXT,
                status TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )"""
    )
    deposit_requests_ddl = (
        """CREATE TABLE IF NOT EXISTS deposit_requests(
                id BIGSERIAL PRIMARY KEY,
                user_tg_id BIGINT,
                amount DOUBLE PRECISION,
                currency TEXT,
                method TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        if is_pg
        else
        """CREATE TABLE IF NOT EXISTS deposit_requests(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_tg_id INTEGER,
                amount REAL,
                currency TEXT,
                method TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )"""
    )
    worker_clients_ddl = (
        """CREATE TABLE IF NOT EXISTS worker_clients(
                id BIGSERIAL PRIMARY KEY,
                worker_tg_id BIGINT,
                client_tg_id BIGINT,
                min_deposit DOUBLE PRECISION DEFAULT 0,
                min_withdraw DOUBLE PRECISION DEFAULT 0,
                verified INTEGER DEFAULT 0,
                withdraw_enabled INTEGER DEFAULT 1,
                trading_enabled INTEGER DEFAULT 1,
                favorite INTEGER DEFAULT 0,
                blocked INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(worker_tg_id, client_tg_id)
            )"""
        if is_pg
        else
        """CREATE TABLE IF NOT EXISTS worker_clients(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_tg_id INTEGER,
                client_tg_id INTEGER,
                min_deposit REAL DEFAULT 0,
                min_withdraw REAL DEFAULT 0,
                verified INTEGER DEFAULT 0,
                withdraw_enabled INTEGER DEFAULT 1,
                trading_enabled INTEGER DEFAULT 1,
                favorite INTEGER DEFAULT 0,
                blocked INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(worker_tg_id, client_tg_id)
            )"""
    )
    client_luck_ddl = (
        """CREATE TABLE IF NOT EXISTS client_luck(
                id BIGSERIAL PRIMARY KEY,
                worker_tg_id BIGINT,
                client_tg_id BIGINT,
                luck_percent DOUBLE PRECISION,
                UNIQUE(worker_tg_id, client_tg_id)
            )"""
        if is_pg
        else
        """CREATE TABLE IF NOT EXISTS client_luck(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_tg_id INTEGER,
                client_tg_id INTEGER,
                luck_percent REAL,
                UNIQUE(worker_tg_id, client_tg_id)
            )"""
    )
    ecn_assets_ddl = (
        """CREATE TABLE IF NOT EXISTS ecn_assets(
                id BIGSERIAL PRIMARY KEY,
                name TEXT UNIQUE
            )"""
        if is_pg
        else
        """CREATE TABLE IF NOT EXISTS ecn_assets(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE
            )"""
    )
    deals_ddl = (
        """CREATE TABLE IF NOT EXISTS deals(
                id BIGSERIAL PRIMARY KEY,
                user_tg_id BIGINT,
                asset_name TEXT,
                direction TEXT,
                amount DOUBLE PRECISION,
                currency TEXT,
                start_price DOUBLE PRECISION,
                end_price DOUBLE PRECISION,
                change_percent DOUBLE PRECISION,
                is_win INTEGER,
                profit DOUBLE PRECISION,
                expires_in_sec INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        if is_pg
        else
        """CREATE TABLE IF NOT EXISTS deals(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_tg_id INTEGER,
                asset_name TEXT,
                direction TEXT,
                amount REAL,
                currency TEXT,
                start_price REAL,
                end_price REAL,
                change_percent REAL,
                is_win INTEGER,
                profit REAL,
                expires_in_sec INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )"""
    )
    activity_log_ddl = (
        """CREATE TABLE IF NOT EXISTS activity_log(
                id BIGSERIAL PRIMARY KEY,
                worker_tg_id BIGINT,
                client_tg_id BIGINT,
                actor_tg_id BIGINT,
                actor_source TEXT,
                event_type TEXT,
                title TEXT,
                details TEXT,
                amount DOUBLE PRECISION,
                currency TEXT,
                meta_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        if is_pg
        else
        """CREATE TABLE IF NOT EXISTS activity_log(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_tg_id INTEGER,
                client_tg_id INTEGER,
                actor_tg_id INTEGER,
                actor_source TEXT,
                event_type TEXT,
                title TEXT,
                details TEXT,
                amount REAL,
                currency TEXT,
                meta_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )"""
    )
    support_tickets_ddl = (
        """CREATE TABLE IF NOT EXISTS support_tickets(
                id BIGSERIAL PRIMARY KEY,
                client_tg_id BIGINT,
                worker_tg_id BIGINT,
                source TEXT,
                topic TEXT,
                status TEXT DEFAULT 'new',
                subject TEXT,
                last_message TEXT,
                meta_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        if is_pg
        else
        """CREATE TABLE IF NOT EXISTS support_tickets(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_tg_id INTEGER,
                worker_tg_id INTEGER,
                source TEXT,
                topic TEXT,
                status TEXT DEFAULT 'new',
                subject TEXT,
                last_message TEXT,
                meta_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )"""
    )
    admin_audit_log_ddl = (
        """CREATE TABLE IF NOT EXISTS admin_audit_log(
                id BIGSERIAL PRIMARY KEY,
                admin_actor TEXT,
                target_tg_id BIGINT,
                action TEXT,
                details TEXT,
                amount DOUBLE PRECISION,
                currency TEXT,
                meta_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        if is_pg
        else
        """CREATE TABLE IF NOT EXISTS admin_audit_log(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_actor TEXT,
                target_tg_id INTEGER,
                action TEXT,
                details TEXT,
                amount REAL,
                currency TEXT,
                meta_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )"""
    )

    async with aiosqlite.connect(DB_PATH) as db:
        if not is_pg:
            await db.execute("PRAGMA journal_mode=WAL")

        await db.execute(users_ddl)

        await db.execute(referrals_ddl)

        await db.execute(withdrawals_ddl)
        await db.execute(deposit_requests_ddl)

        await db.execute(
            """CREATE TABLE IF NOT EXISTS settings(
                key TEXT PRIMARY KEY,
                value TEXT
            )"""
        )

        await db.execute(worker_clients_ddl)

        await db.execute(
            """CREATE TABLE IF NOT EXISTS worker_settings(
                worker_tg_id INTEGER PRIMARY KEY,
                min_deposit REAL DEFAULT 0,
                min_withdraw REAL DEFAULT 0
            )"""
        )

        await db.execute(client_luck_ddl)

        await db.execute(ecn_assets_ddl)

        await db.execute(deals_ddl)
        await db.execute(activity_log_ddl)
        await db.execute(support_tickets_ddl)
        await db.execute(admin_audit_log_ddl)

        # Lightweight migrations for CRM/support features.
        for sql in (
            "ALTER TABLE worker_clients ADD COLUMN crm_note TEXT DEFAULT ''",
            "ALTER TABLE worker_clients ADD COLUMN tags TEXT DEFAULT ''",
            "ALTER TABLE worker_clients ADD COLUMN funnel_stage TEXT DEFAULT 'new'",
            "ALTER TABLE worker_clients ADD COLUMN last_activity_at TEXT",
            "ALTER TABLE deposit_requests ADD COLUMN processed_by TEXT",
            "ALTER TABLE withdrawals ADD COLUMN processed_by TEXT",
            "ALTER TABLE support_tickets ADD COLUMN assigned_to TEXT",
        ):
            try:
                await db.execute(sql)
            except Exception:
                pass

        # Расширяем список активов ECN (insert-ignore для уже существующих)
        default_assets = [
            "Bitcoin", "Ethereum", "Solana", "Dogecoin", "Litecoin", "XRP", "Cardano", "Avalanche",
            "Polkadot", "Chainlink", "Toncoin", "TRON", "Stellar", "Monero", "Bitcoin Cash",
            "Shiba Inu", "Pepe", "Arbitrum", "Optimism", "Near Protocol", "Uniswap", "Aptos",
            "Sui", "Kaspa", "Fantom", "Algorand", "Injective", "Render", "Filecoin", "Hedera",
            "Maker", "Polygon", "Sei", "Jupiter", "Celestia", "Mantle", "Floki", "Bonk",
            "Aave", "The Graph", "EOS", "Tezos", "Sandbox", "Decentraland", "Gala", "Cronos",
            "VeChain", "NEO", "Thorchain", "PancakeSwap", "Curve DAO", "Lido DAO", "Frax", "Beam",
            "Worldcoin", "Blur", "Rocket Pool", "Pendle", "ZKsync", "Metis", "dYdX", "Starknet",
        ]
        await db.executemany(
            "INSERT OR IGNORE INTO ecn_assets(name) VALUES (?)",
            [(name,) for name in default_assets],
        )

        await db.commit()

        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT key, value FROM settings")
        rows = await cur.fetchall()
        for row in rows:
            if row["key"] == "crypto_bot_url":
                config.crypto_bot_url = row["value"]
            elif row["key"] == "trc20_address":
                config.trc20_address = row["value"]
            elif row["key"] == "card_pay_url":
                config.card_pay_url = row["value"]
            elif row["key"] == "card_requisites":
                config.card_requisites = row["value"]
            elif row["key"] == "support_url":
                config.support_url = row["value"]
            elif row["key"] == "webapp_url":
                config.webapp_url = row["value"]

async def get_user_row(tg_user) -> aiosqlite.Row:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users WHERE tg_id = ?", (tg_user.id,))
        row = await cur.fetchone()
        if row:
            return row

        is_admin = 1 if tg_user.id in config.admin_ids else 0
        is_worker = 0

        await db.execute(
            "INSERT INTO users(tg_id, first_name, username, is_admin, is_worker) "
            "VALUES (?, ?, ?, ?, ?)",
            (tg_user.id, tg_user.first_name, tg_user.username, is_admin, is_worker),
        )
        await db.commit()

        cur = await db.execute("SELECT * FROM users WHERE tg_id = ?", (tg_user.id,))
        return await cur.fetchone()


async def set_accepted_rules(tg_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET accepted_rules = 1 WHERE tg_id = ?", (tg_id,))
        await db.commit()


async def set_language(tg_id: int, lang: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET language = ? WHERE tg_id = ?", (lang, tg_id))
        await db.commit()


async def set_currency(tg_id: int, currency: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET currency = ? WHERE tg_id = ?", (currency, tg_id))
        await db.commit()


async def change_balance(tg_id: int, delta: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET balance = balance + ? WHERE tg_id = ?", (delta, tg_id))
        await db.commit()


async def get_user_balance(tg_id: int) -> float:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT IFNULL(balance, 0) FROM users WHERE tg_id = ?", (tg_id,))
        row = await cur.fetchone()
        return float(row[0]) if row else 0.0


async def save_deal(
    user_tg_id: int,
    asset_name: str,
    direction: str,
    amount: float,
    currency: str,
    start_price: float,
    end_price: float,
    change_percent: float,
    is_win: bool,
    profit: float,
    expires_in_sec: int,
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO deals(
                user_tg_id, asset_name, direction, amount, currency,
                start_price, end_price, change_percent, is_win, profit, expires_in_sec
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                user_tg_id,
                asset_name,
                direction,
                amount,
                currency,
                start_price,
                end_price,
                change_percent,
                1 if is_win else 0,
                profit,
                expires_in_sec,
            ),
        )
        await db.commit()


async def get_user_deal_stats(user_tg_id: int) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN is_win = 1 THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN is_win = 0 THEN 1 ELSE 0 END) AS losses,
                    IFNULL(SUM(profit), 0) AS total_profit
               FROM deals
               WHERE user_tg_id = ?""",
            (user_tg_id,),
        )
        row = await cur.fetchone()
        return {
            "total": int(row["total"] or 0),
            "wins": int(row["wins"] or 0),
            "losses": int(row["losses"] or 0),
            "total_profit": float(row["total_profit"] or 0.0),
        }


async def get_user_deals(user_tg_id: int, limit: int = 10):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """SELECT id, asset_name, direction, amount, currency, is_win, profit, created_at
               FROM deals
               WHERE user_tg_id = ?
               ORDER BY id DESC
               LIMIT ?""",
            (user_tg_id, limit),
        )
        return await cur.fetchall()


async def save_referral(worker_tg_id: int, invited_tg_id: int):
    if worker_tg_id == invited_tg_id:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO referrals(worker_tg_id, invited_tg_id) VALUES (?, ?)",
            (worker_tg_id, invited_tg_id),
        )
        await db.commit()

async def ensure_worker_client(worker_tg_id: int, client_tg_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute(
            "INSERT OR IGNORE INTO worker_clients(worker_tg_id, client_tg_id) VALUES (?, ?)",
            (worker_tg_id, client_tg_id),
        )
        await db.commit()
        cur = await db.execute(
            "SELECT id FROM worker_clients WHERE worker_tg_id = ? AND client_tg_id = ?",
            (worker_tg_id, client_tg_id),
        )
        row = await cur.fetchone()
        return int(row["id"])


async def get_referral_count(worker_tg_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM worker_clients WHERE worker_tg_id = ?",
            (worker_tg_id,),
        )
        return int((await cur.fetchone())[0])


async def get_worker_for_client(client_tg_id: int) -> Optional[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT worker_tg_id FROM worker_clients WHERE client_tg_id = ? ORDER BY id ASC LIMIT 1",
            (client_tg_id,),
        )
        row = await cur.fetchone()
        return int(row[0]) if row else None

async def create_withdrawal(user_tg_id: int, amount: float, currency: str, method: str, details: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO withdrawals(user_tg_id, amount, currency, method, details, status) "
            "VALUES (?, ?, ?, ?, ?, 'pending')",
            (user_tg_id, amount, currency, method, details),
        )
        await db.commit()
        return cur.lastrowid


async def create_deposit_request(user_tg_id: int, amount: float, currency: str, method: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO deposit_requests(user_tg_id, amount, currency, method, status) VALUES (?, ?, ?, ?, 'pending')",
            (user_tg_id, amount, currency, method),
        )
        await db.commit()
        return cur.lastrowid


async def create_activity_event(
    worker_tg_id: int | None,
    client_tg_id: int | None,
    actor_tg_id: int | None,
    actor_source: str,
    event_type: str,
    title: str,
    details: str = "",
    amount: float | None = None,
    currency: str | None = None,
    meta: dict | None = None,
) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO activity_log(worker_tg_id, client_tg_id, actor_tg_id, actor_source, event_type, title, details, amount, currency, meta_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                worker_tg_id,
                client_tg_id,
                actor_tg_id,
                actor_source,
                event_type,
                title,
                details,
                amount,
                currency,
                json.dumps(meta or {}, ensure_ascii=True),
            ),
        )
        await db.commit()
        return int(cur.lastrowid)


async def get_worker_activity_events(worker_tg_id: int, limit: int = 30):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT al.id, al.client_tg_id, al.actor_tg_id, al.actor_source, al.event_type, al.title, al.details,
                   al.amount, al.currency, al.created_at, u.first_name, u.username
            FROM activity_log al
            LEFT JOIN users u ON u.tg_id = al.client_tg_id
            WHERE al.worker_tg_id = ?
            ORDER BY al.id DESC
            LIMIT ?
            """,
            (worker_tg_id, limit),
        )
        return await cur.fetchall()


async def log_activity_for_worker_client(
    client_tg_id: int,
    actor_tg_id: int | None,
    actor_source: str,
    event_type: str,
    title: str,
    details: str = "",
    amount: float | None = None,
    currency: str | None = None,
    meta: dict | None = None,
):
    worker_tg_id = await get_worker_for_client(client_tg_id)
    if not worker_tg_id:
        return
    await create_activity_event(
        worker_tg_id=worker_tg_id,
        client_tg_id=client_tg_id,
        actor_tg_id=actor_tg_id,
        actor_source=actor_source,
        event_type=event_type,
        title=title,
        details=details,
        amount=amount,
        currency=currency,
        meta=meta,
    )
    await touch_worker_client_activity(client_tg_id)


def deposit_method_label(method: str) -> str:
    return {
        "crypto": "Crypto bot",
        "trc20": "TRC20 USDT",
        "card": "Банковская карта",
    }.get((method or "").strip().lower(), method or "Неизвестно")


async def notify_worker_bot_deposit_event(
    client_tg_id: int,
    first_name: str | None,
    username: str | None,
    amount: float | None,
    currency: str | None,
    method: str | None,
    stage: str,
    deposit_id: int | None = None,
):
    worker_id = await get_worker_for_client(client_tg_id)
    if not worker_id:
        return
    stage_text = {
        "request_created": "создал заявку на пополнение через бота",
        "support_opened": "перешёл в техподдержку по пополнению через бота",
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
    try:
        await bot.send_message(worker_id, text)
    except Exception:
        pass


async def touch_worker_client_activity(client_tg_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE worker_clients SET last_activity_at = CURRENT_TIMESTAMP WHERE client_tg_id = ?",
            (client_tg_id,),
        )
        await db.commit()


async def create_support_ticket(
    client_tg_id: int,
    worker_tg_id: int | None,
    source: str,
    topic: str,
    subject: str,
    status: str = "new",
    last_message: str = "",
    meta: dict | None = None,
) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO support_tickets(client_tg_id, worker_tg_id, source, topic, status, subject, last_message, meta_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                client_tg_id,
                worker_tg_id,
                source,
                topic,
                status,
                subject,
                last_message,
                json.dumps(meta or {}, ensure_ascii=True),
            ),
        )
        await db.commit()
        return int(cur.lastrowid)


async def update_support_ticket_status(ticket_id: int, status: str, assigned_to: str | None = None, last_message: str | None = None):
    async with aiosqlite.connect(DB_PATH) as db:
        sets = ["status = ?", "updated_at = CURRENT_TIMESTAMP"]
        params: list = [status]
        if assigned_to is not None:
            sets.append("assigned_to = ?")
            params.append(assigned_to)
        if last_message is not None:
            sets.append("last_message = ?")
            params.append(last_message)
        params.append(ticket_id)
        await db.execute(f"UPDATE support_tickets SET {', '.join(sets)} WHERE id = ?", tuple(params))
        await db.commit()


async def transfer_worker_client_record(wc_id: int, new_worker_tg_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT id, client_tg_id, worker_tg_id FROM worker_clients WHERE id = ?", (wc_id,))
        row = await cur.fetchone()
        if not row:
            return False
        existing = await db.execute(
            "SELECT id FROM worker_clients WHERE worker_tg_id = ? AND client_tg_id = ? AND id != ?",
            (new_worker_tg_id, row["client_tg_id"], wc_id),
        )
        dupe = await existing.fetchone()
        if dupe:
            return False
        await db.execute(
            "UPDATE worker_clients SET worker_tg_id = ?, last_activity_at = CURRENT_TIMESTAMP WHERE id = ?",
            (new_worker_tg_id, wc_id),
        )
        await db.execute(
            "UPDATE support_tickets SET worker_tg_id = ? WHERE client_tg_id = ?",
            (new_worker_tg_id, row["client_tg_id"]),
        )
        await db.commit()
        return True


async def get_latest_open_support_ticket(client_tg_id: int, topic: str | None = None) -> Optional[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if topic:
            cur = await db.execute(
                "SELECT * FROM support_tickets WHERE client_tg_id = ? AND topic = ? AND status IN ('new', 'in_progress') ORDER BY id DESC LIMIT 1",
                (client_tg_id, topic),
            )
        else:
            cur = await db.execute(
                "SELECT * FROM support_tickets WHERE client_tg_id = ? AND status IN ('new', 'in_progress') ORDER BY id DESC LIMIT 1",
                (client_tg_id,),
            )
        return await cur.fetchone()


async def create_admin_audit_event(
    admin_actor: str,
    target_tg_id: int | None,
    action: str,
    details: str,
    amount: float | None = None,
    currency: str | None = None,
    meta: dict | None = None,
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO admin_audit_log(admin_actor, target_tg_id, action, details, amount, currency, meta_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                admin_actor,
                target_tg_id,
                action,
                details,
                amount,
                currency,
                json.dumps(meta or {}, ensure_ascii=True),
            ),
        )
        await db.commit()


async def get_deposit_request(dep_id: int) -> Optional[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM deposit_requests WHERE id = ?", (dep_id,))
        return await cur.fetchone()


async def set_deposit_request_status(dep_id: int, status: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE deposit_requests SET status = ? WHERE id = ?", (status, dep_id))
        await db.commit()


async def get_user_pending_withdraw_sum(user_tg_id: int) -> float:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT IFNULL(SUM(amount), 0) FROM withdrawals WHERE user_tg_id = ? AND status = 'pending'",
            (user_tg_id,),
        )
        row = await cur.fetchone()
        return float(row[0]) if row else 0.0


async def get_luck_percent_for_client(client_tg_id: int) -> Optional[float]:
    """
    Возвращает luck_percent для клиента, если он задан воркером.
    Если не задано, возвращает None.
    """
    worker_id = await get_worker_for_client(client_tg_id)
    if not worker_id:
        return None
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT luck_percent FROM client_luck WHERE worker_tg_id = ? AND client_tg_id = ?",
            (worker_id, client_tg_id),
        )
        row = await cur.fetchone()
        if not row:
            return None
        return float(row["luck_percent"]) if row["luck_percent"] is not None else None


async def get_ecn_assets():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT id, name FROM ecn_assets ORDER BY id ASC")
        return await cur.fetchall()


async def add_ecn_asset(name: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO ecn_assets(name) VALUES (?)",
            (name.strip(),),
        )
        await db.commit()



async def get_withdrawal(w_id: int) -> Optional[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM withdrawals WHERE id = ?", (w_id,))
        return await cur.fetchone()


async def set_withdrawal_status(w_id: int, status: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE withdrawals SET status = ? WHERE id = ?", (status, w_id))
        await db.commit()


async def set_worker_flag(tg_id: int, is_worker: bool = True):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET is_worker = ? WHERE tg_id = ?", (1 if is_worker else 0, tg_id))
        await db.commit()

async def get_admin_stats_text() -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT COUNT(*) AS c FROM users")
        total_users = (await cur.fetchone())["c"]
        cur = await db.execute("SELECT COUNT(*) AS c FROM users WHERE is_worker = 1")
        total_workers = (await cur.fetchone())["c"]
        cur = await db.execute("SELECT IFNULL(SUM(balance),0) AS s FROM users")
        total_balance = (await cur.fetchone())["s"]
        cur = await db.execute("SELECT COUNT(*) AS c FROM withdrawals WHERE status = 'pending'")
        pending_withdrawals = (await cur.fetchone())["c"]

    return (
        "📊 <b>Статистика</b>\n\n"
        f"• Пользователей: <b>{total_users}</b>\n"
        f"• Воркеров: <b>{total_workers}</b>\n"
        f"• Суммарный баланс: <b>{total_balance:.2f}</b>\n"
        f"• Заявок на вывод (ожидают): <b>{pending_withdrawals}</b>\n"
    )

async def get_workers_with_ref_counts():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """SELECT u.tg_id, u.first_name, u.username,
                       COUNT(wc.client_tg_id) AS ref_count
                   FROM users u
                   LEFT JOIN worker_clients wc ON u.tg_id = wc.worker_tg_id
                   WHERE u.is_worker = 1
                   GROUP BY u.tg_id, u.first_name, u.username
                   ORDER BY ref_count DESC, u.tg_id ASC"""
        )
        return await cur.fetchall()


async def get_referrals_for_worker(worker_tg_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """SELECT wc.*, u.first_name, u.username
                   FROM worker_clients wc
                   JOIN users u ON u.tg_id = wc.client_tg_id
                   WHERE wc.worker_tg_id = ?
                   ORDER BY wc.id ASC""",
            (worker_tg_id,),
        )
        return await cur.fetchall()

async def set_setting(key: str, value: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO settings(key,value)
                   VALUES(?,?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
            (key, value),
        )
        await db.commit()
    if key == "crypto_bot_url":
        config.crypto_bot_url = value
    elif key == "trc20_address":
        config.trc20_address = value
    elif key == "card_pay_url":
        config.card_pay_url = value
    elif key == "card_requisites":
        config.card_requisites = value
    elif key == "support_url":
        config.support_url = value
    elif key == "webapp_url":
        config.webapp_url = value

async def get_worker_clients_list(worker_tg_id: int, favorites_only: bool = False):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if favorites_only:
            cur = await db.execute(
                """SELECT wc.*, u.first_name, u.username
                       FROM worker_clients wc
                       JOIN users u ON u.tg_id = wc.client_tg_id
                       WHERE wc.worker_tg_id = ? AND wc.favorite = 1
                       ORDER BY wc.id DESC""",
                (worker_tg_id,),
            )
        else:
            cur = await db.execute(
                """SELECT wc.*, u.first_name, u.username
                       FROM worker_clients wc
                       JOIN users u ON u.tg_id = wc.client_tg_id
                       WHERE wc.worker_tg_id = ?
                       ORDER BY wc.id DESC""",
                (worker_tg_id,),
            )
        return await cur.fetchall()


async def get_worker_client_by_id(wc_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """SELECT wc.*, u.first_name, u.username, u.balance, u.currency
                   FROM worker_clients wc
                   JOIN users u ON u.tg_id = wc.client_tg_id
                   WHERE wc.id = ?""", (wc_id,),
        )
        return await cur.fetchone()
async def get_luck_for_worker_client(worker_tg_id: int, client_tg_id: int) -> Optional[float]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT luck_percent FROM client_luck WHERE worker_tg_id = ? AND client_tg_id = ?",
            (worker_tg_id, client_tg_id),
        )
        row = await cur.fetchone()
        if not row:
            return None
        return float(row["luck_percent"]) if row["luck_percent"] is not None else None


async def set_client_luck(worker_tg_id: int, client_tg_id: int, luck_percent: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO client_luck(worker_tg_id, client_tg_id, luck_percent)
               VALUES(?,?,?)
               ON CONFLICT(worker_tg_id, client_tg_id)
               DO UPDATE SET luck_percent=excluded.luck_percent""",
            (worker_tg_id, client_tg_id, luck_percent),
        )
        await db.commit()



async def update_worker_client_field(wc_id: int, field: str, value):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"UPDATE worker_clients SET {field} = ? WHERE id = ?", (value, wc_id))
        await db.commit()

async def get_worker_settings(worker_tg_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM worker_settings WHERE worker_tg_id = ?", (worker_tg_id,))
        row = await cur.fetchone()
        if row:
            return row
        return {"worker_tg_id": worker_tg_id, "min_deposit": 0, "min_withdraw": 0}


async def set_worker_setting(worker_tg_id: int, field: str, value: float):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO worker_settings(worker_tg_id,min_deposit,min_withdraw)
                   VALUES(?,0,0)
                   ON CONFLICT(worker_tg_id) DO NOTHING""",
            (worker_tg_id,),
        )
        await db.execute(
            f"UPDATE worker_settings SET {field} = ? WHERE worker_tg_id = ?",
            (value, worker_tg_id),
        )
        await db.commit()

def rules_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="✅Accept the rules/Принять правила", callback_data="accept_rules")
    kb.adjust(1)
    return kb.as_markup()


LANG_BUTTONS = [
    ("Русский", "ru"),
    ("English", "en"),
    ("Deutsch", "de"),
    ("Español", "es"),
    ("Українська", "uk"),
    ("Français", "fr"),
    ("Italiano", "it"),
    ("Português", "pt"),
    ("中文", "zh"),
    ("한국어", "ko"),
    ("Türkçe", "tr"),
    ("日本語", "ja"),
    ("हिन्दी", "hi"),
    ("Tiếng Việt", "vi"),
    ("ไทย", "th"),
    ("Bahasa Indo", "id"),
    ("Polski", "pl"),
    ("Nederlands", "nl"),
]


def language_keyboard():
    kb = InlineKeyboardBuilder()
    for text, code in LANG_BUTTONS:
        kb.button(text=text, callback_data=f"lang:{code}")
    kb.adjust(3)
    return kb.as_markup()


CURRENCIES = [
    "RUB", "UAH", "CNY", "MXN", "TRY", "GBP", "CAD", "NOK",
    "BYN", "USD", "JPY", "BRL", "SGD", "CHF", "NZD",
    "KZT", "EUR", "SAR", "INR", "KRW", "AUD", "SEK", "DKK",
]


def currency_keyboard():
    kb = InlineKeyboardBuilder()
    for cur in CURRENCIES:
        kb.button(text=cur, callback_data=f"cur:{cur}")
    kb.adjust(4)
    return kb.as_markup()

def main_menu_keyboard(lang: str = "ru"):
    keyboard_rows = []
    if config.webapp_url:
        keyboard_rows.append(
            [KeyboardButton(text=t(lang, "open_app"), web_app=WebAppInfo(url=config.webapp_url))]
        )
    return ReplyKeyboardMarkup(
        keyboard=keyboard_rows + [
            [KeyboardButton(text=t(lang, "portfolio"))],
            [KeyboardButton(text=t(lang, "open_ecn"))],
            [
                KeyboardButton(text=t(lang, "info")),
                KeyboardButton(text=t(lang, "support")),
            ],
        ],
        resize_keyboard=True,
    )


def profile_keyboard(is_admin: bool, is_worker: bool, lang: str = "ru"):
    kb = InlineKeyboardBuilder()
    if config.webapp_url:
        kb.button(text=t(lang, "open_app"), web_app=WebAppInfo(url=config.webapp_url))
    kb.button(text=t(lang, "deposit"), callback_data="deposit")
    kb.button(text=t(lang, "withdraw"), callback_data="withdraw")
    kb.button(text=t(lang, "verify"), callback_data="verify")
    kb.button(text=t(lang, "my_deals"), callback_data="my_deals")
    kb.button(text=t(lang, "settings"), callback_data="settings")
    if is_worker:
        kb.button(text=t(lang, "worker_panel"), callback_data="open_worker_panel")
    if is_admin:
        kb.button(text=t(lang, "admin_panel"), callback_data="open_admin_panel")
    if config.webapp_url:
        kb.adjust(1, 2, 2, 1, 1, 1)
    else:
        kb.adjust(2, 2, 1, 1, 1)
    return kb.as_markup()


def deposit_method_keyboard(lang: str = "ru"):
    kb = InlineKeyboardBuilder()
    kb.button(text="💎Crypto bot", callback_data="pay_crypto")
    kb.button(text="👛По адресу TRC20 USDT", callback_data="pay_trc20")
    kb.button(text=t(lang, "pay_card"), callback_data="pay_card")
    kb.button(text=t(lang, "deposit_cancel"), callback_data="cancel_deposit")
    kb.adjust(1)
    return kb.as_markup()


def deposit_support_keyboard(lang: str = "ru"):
    kb = InlineKeyboardBuilder()
    if config.support_url:
        kb.button(
            text=tr(
                lang,
                "🛟 Перейти в техподдержку",
                "🛟 Open support",
                "🛟 Перейти в техпідтримку",
            ),
            url=config.support_url,
        )
    kb.adjust(1)
    return kb.as_markup()


def card_payment_keyboard():
    kb = InlineKeyboardBuilder()
    if config.card_pay_url:
        kb.button(text="💳 Перейти к оплате картой", url=config.card_pay_url)
    if config.support_url:
        kb.button(text="🛟 Связаться с поддержкой", url=config.support_url)
    kb.adjust(1)
    return kb.as_markup()


def admin_deposit_check_keyboard(dep_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(
        text="✅ Подтвердить оплату",
        callback_data=f"admin_dep_confirm:{dep_id}",
    )
    kb.button(
        text="❌ Отклонить оплату",
        callback_data=f"admin_dep_reject:{dep_id}",
    )
    kb.adjust(2)
    return kb.as_markup()




def withdraw_method_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="💳На карту", callback_data="wd_card")
    kb.button(text="👛На кошелек TRC20 USDT", callback_data="wd_trc20")
    kb.button(text="❌Отмена", callback_data="wd_cancel")
    kb.adjust(1)
    return kb.as_markup()


def withdrawal_admin_keyboard(withdrawal_id: int):
    kb = InlineKeyboardBuilder()
    kb.button(text="✅Подтвердить", callback_data=f"wd_ok:{withdrawal_id}")
    kb.button(text="❌Отклонить", callback_data=f"wd_no:{withdrawal_id}")
    kb.adjust(2)
    return kb.as_markup()


def verification_keyboard():
    kb = InlineKeyboardBuilder()
    if config.support_url:
        kb.button(text="📩Поддержка", url=config.support_url)
    kb.button(text="⬅️В профиль", callback_data="open_profile")
    kb.adjust(1)
    return kb.as_markup()


def settings_keyboard(lang: str = "ru"):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "settings_btn_lang"), callback_data="settings_lang")
    kb.button(text=t(lang, "settings_btn_currency"), callback_data="settings_currency")
    kb.button(text=t(lang, "settings_btn_back"), callback_data="open_profile")
    kb.adjust(1)
    return kb.as_markup()

def admin_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="📊Статистика", callback_data="admin_stats")
    kb.button(text="👷Добавить воркера", callback_data="admin_add_worker")
    kb.button(text="📜Воркеры и рефералы", callback_data="admin_workers")
    kb.button(text="💳Платёжные реквизиты", callback_data="admin_payments")
    kb.button(text="🪙Активы ECN", callback_data="admin_assets")
    kb.button(text="⬅️В профиль", callback_data="open_profile")
    kb.adjust(1)
    return kb.as_markup()


def workers_list_keyboard(rows):
    kb = InlineKeyboardBuilder()
    for row in rows:
        tg_id = row["tg_id"]
        ref_count = row["ref_count"]
        btn_text = f"{tg_id} • реф: {ref_count}"
        kb.button(text=btn_text, callback_data=f"admin_worker:{tg_id}")
    kb.button(text="⬅️Админ-панель", callback_data="open_admin_panel")
    kb.adjust(1)
    return kb.as_markup()


def worker_panel_keyboard():
    kb = InlineKeyboardBuilder()
    kb.button(text="🐑 Лохматые", callback_data="worker_sheeps")
    kb.button(text="⚙️Settings сервиса", callback_data="worker_settings")
    kb.adjust(1)
    return kb.as_markup()

def worker_sheeps_keyboard(wc_rows):
    kb = InlineKeyboardBuilder()
    for row in wc_rows[:10]:
        label_name = row["first_name"] or "без имени"
        btn_text = f"/n{row['id']} — {label_name}"
        kb.button(text=btn_text, callback_data=f"wc_profile:{row['id']}")
    kb.button(text="День", callback_data="sheep_period:day")
    kb.button(text="Неделя", callback_data="sheep_period:week")
    kb.button(text="Месяц", callback_data="sheep_period:month")
    kb.button(text="• Всё время •", callback_data="sheep_period:all")
    kb.button(text="⭐ Избранные", callback_data="sheep_fav")
    kb.button(text="🔍 Поиск по ID", callback_data="sheep_search")
    kb.button(text="⬅ Назад", callback_data="open_worker_panel")
    kb.adjust(1)
    return kb.as_markup()


def worker_client_profile_keyboard(wc_id: int, flags: dict, balance: float, currency: str, min_dep: float, min_wd: float):
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 Adj. balance", callback_data=f"wc_adj_balance:{wc_id}")
    kb.button(text="☘️Удача", callback_data=f"wc_luck:{wc_id}")
    kb.button(text="📨 Min. deposit", callback_data=f"wc_min_dep:{wc_id}")
    kb.button(text="📨 Min. withdraw", callback_data=f"wc_min_wd:{wc_id}")
    kb.button(text=("✅ Вериф" if not flags["verified"] else "❌ Вериф"), callback_data=f"wc_toggle_verif:{wc_id}")
    kb.button(text=("✅ Withdraw" if not flags["withdraw_enabled"] else "❌ Withdraw"), callback_data=f"wc_toggle_withdraw:{wc_id}")
    kb.button(text=("✅ Покупка" if not flags["trading_enabled"] else "❌ Покупка"), callback_data=f"wc_toggle_trade:{wc_id}")
    kb.button(text=("⭐ В избранное" if not flags["favorite"] else "⭐ Убрать из избранного"), callback_data=f"wc_toggle_fav:{wc_id}")
    kb.button(text="📤 Передать лохматого", callback_data=f"wc_transfer:{wc_id}")
    kb.button(
        text=("🔒 Заблокировать" if not flags["blocked"] else "🔓 Разблокировать"),
        callback_data=f"wc_toggle_block:{wc_id}",
    )
    kb.button(text="💬 Начать диалог", callback_data=f"wc_chat_start:{wc_id}")
    kb.button(text="⬅ Назад", callback_data="worker_sheeps")
    kb.adjust(1)
    return kb.as_markup()

def is_admin_id(user_id: int) -> bool:
    return user_id in config.admin_ids

# ================== HANDLERS ==================

# --- /start + referral ---

@dp.message(CommandStart())
async def cmd_start(message: Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) > 1:
        payload = parts[1]
        if payload.startswith("ref"):
            try:
                worker_id = int(payload[3:])
                await save_referral(worker_id, message.from_user.id)
                wc_id = await ensure_worker_client(worker_id, message.from_user.id)
                text_worker = (
                    "🆕 Новый лохматый по вашей ссылке: "
                    f"/n{wc_id} — ID: <code>{message.from_user.id}</code>\n"
                    f"({message.from_user.full_name})"
                )
                try:
                    await bot.send_message(worker_id, text_worker)
                except Exception:
                    pass
            except ValueError:
                pass

    user_row = await get_user_row(message.from_user)
    if not user_row["accepted_rules"]:
        text = (
            "👋 Привет!\n\n"
            "Это бот для работы с торговой платформой.\n"
            "Перед началом использования, пожалуйста, ознакомься с "
            '<a href="https://telegra.ph/Polzovatelskoe-soglashenie-03-27-13">'
            "Политикой и условиями пользования</a>.\n\n"
            "Нажимая кнопку ниже, ты подтверждаешь, что согласен с правилами."
        )
        await message.answer(text, reply_markup=rules_keyboard())
    else:
        # Если язык и валюта уже выбраны — сразу открываем личный кабинет
        if user_row["language"] and user_row["currency"]:
            await send_profile(message)
        else:
            # На случай, если правила приняты, но настройки не завершены
            lang = normalize_lang(user_row["language"])
            await message.answer(
                tr(lang, "🌐 Выберите язык интерфейса:", "🌐 Choose interface language:", "🌐 Оберіть мову інтерфейсу:"),
                reply_markup=language_keyboard(),
            )


@dp.callback_query(F.data == "accept_rules")
async def on_accept_rules(callback: CallbackQuery):
    await set_accepted_rules(callback.from_user.id)
    await callback.message.edit_reply_markup()
    await callback.message.answer("🌐 Choose interface language / Выберите язык / Оберіть мову:", reply_markup=language_keyboard())
    await callback.answer()


@dp.callback_query(F.data.startswith("lang:"))
async def on_language_selected(callback: CallbackQuery):
    lang_code = callback.data.split(":", 1)[1]
    await set_language(callback.from_user.id, lang_code)
    await callback.message.edit_reply_markup()
    await callback.message.answer(tr(lang_code, "💱 Выберите валюту:", "💱 Choose currency:", "💱 Оберіть валюту:"), reply_markup=currency_keyboard())
    await callback.answer()


@dp.callback_query(F.data.startswith("cur:"))
async def on_currency_selected(callback: CallbackQuery):
    currency = callback.data.split(":", 1)[1]
    await set_currency(callback.from_user.id, currency)
    await callback.message.edit_reply_markup()
    await send_main_menu(callback.message)
    await callback.answer()


async def send_main_menu(message: Message):
    user_row = await get_user_row(message.from_user)
    lang = normalize_lang(user_row["language"])
    text = t(lang, "menu_welcome", name=message.from_user.first_name)
    await message.answer(text, reply_markup=main_menu_keyboard(lang))


# ---------- MAIN MENU BUTTONS & ECN DEMO ----------

def ecn_assets_keyboard(rows):
    kb = InlineKeyboardBuilder()
    for row in rows:
        kb.button(
            text=row["name"],
            callback_data=f"ecn_asset:{row['id']}",
        )
    kb.adjust(2)
    return kb.as_markup()


def ecn_direction_keyboard(asset_id: int, lang: str = "ru"):
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(lang, "📈 Повышение", "📈 Up", "📈 Вгору"), callback_data=f"ecn_dir:up:{asset_id}")
    kb.button(text=tr(lang, "📉 Понижение", "📉 Down", "📉 Вниз"), callback_data=f"ecn_dir:down:{asset_id}")
    kb.adjust(2)
    return kb.as_markup()


def ecn_time_keyboard(lang: str = "ru"):
    kb = InlineKeyboardBuilder()
    kb.button(text=tr(lang, "30 сек.", "30 sec", "30 с"), callback_data="ecn_time:30")
    kb.button(text=tr(lang, "1 мин.", "1 min", "1 хв"), callback_data="ecn_time:60")
    kb.button(text=tr(lang, "3 мин.", "3 min", "3 хв"), callback_data="ecn_time:180")
    kb.button(text=tr(lang, "5 мин.", "5 min", "5 хв"), callback_data="ecn_time:300")
    kb.button(text=tr(lang, "10 мин.", "10 min", "10 хв"), callback_data="ecn_time:600")
    kb.adjust(2)
    return kb.as_markup()


@dp.message(F.text.in_({"📁 Портфель", "📁 Portfolio", "📁 Портфель"}))
async def menu_portfolio(message: Message):
    await send_profile(message)


@dp.message(F.text.in_({"ℹ️ Инфо", "ℹ️ Info", "ℹ️ Інфо"}))
async def menu_info(message: Message):
    user_row = await get_user_row(message.from_user)
    lang = normalize_lang(user_row["language"])
    text = t(lang, "menu_info_text")
    await message.answer(text, reply_markup=main_menu_keyboard(lang))


@dp.message(F.text.in_({"🌐 Тех. Поддержка", "🌐 Support", "🌐 Тех. Підтримка"}))
async def menu_support(message: Message):
    user_row = await get_user_row(message.from_user)
    lang = normalize_lang(user_row["language"])
    worker_id = await get_worker_for_client(message.from_user.id)
    existing = await get_latest_open_support_ticket(message.from_user.id, "general")
    if existing:
        await update_support_ticket_status(int(existing["id"]), "new", last_message="Клиент открыл раздел техподдержки в Telegram-боте")
    else:
        await create_support_ticket(
            client_tg_id=message.from_user.id,
            worker_tg_id=worker_id,
            source="bot",
            topic="general",
            subject="Обращение в техподдержку",
            status="new",
            last_message="Клиент открыл раздел техподдержки в Telegram-боте",
        )
    await log_activity_for_worker_client(
        client_tg_id=message.from_user.id,
        actor_tg_id=message.from_user.id,
        actor_source="bot",
        event_type="bot_support_section_opened",
        title="Открыта техподдержка",
        details="Реферал открыл раздел техподдержки в Telegram-боте.",
    )
    await message.answer(
        t(lang, "menu_support_text", url=config.support_url),
        reply_markup=main_menu_keyboard(lang),
    )


@dp.message(F.text.in_({"📈 Открыть ECN", "📈 Open ECN", "📈 Відкрити ECN"}))
async def menu_open_ecn(message: Message, state: FSMContext):
    await start_ecn_flow(message, state)


async def start_ecn_flow(msg, state: FSMContext):
    if isinstance(msg, CallbackQuery):
        message = msg.message
        tg_user = msg.from_user
    else:
        message = msg
        tg_user = msg.from_user

    user_row = await get_user_row(tg_user)
    lang = normalize_lang(user_row["language"])
    balance = user_row["balance"] or 0.0
    currency = user_row["currency"] or "USD"

    if balance <= 0:
        await message.answer(
            tr(lang, f"⚠️ На вашем балансе 0 {currency}. Пополните баланс, чтобы открыть сделку.", f"⚠️ Your balance is 0 {currency}. Top up balance to open a deal.", f"⚠️ На вашому балансі 0 {currency}. Поповніть баланс, щоб відкрити угоду.")
        )
        return

    assets = await get_ecn_assets()
    if not assets:
        await message.answer(tr(lang, "❗ Список активов ECN пока пуст. Обратитесь к администратору.", "❗ ECN assets list is empty. Contact admin.", "❗ Список активів ECN порожній. Зверніться до адміністратора."))
        return

    text_lines = [tr(lang, "Самые востребованные активы:", "Top assets:", "Топ активи:")]
    for row in assets:
        text_lines.append(f"└ {row['name']}")
    text = "\n".join(text_lines)

    await message.answer(text, reply_markup=ecn_assets_keyboard(assets))
    await state.clear()


@dp.callback_query(F.data.startswith("ecn_asset:"))
async def ecn_choose_asset(callback: CallbackQuery, state: FSMContext):
    asset_id = int(callback.data.split(":", 1)[1])
    assets = await get_ecn_assets()
    user_row = await get_user_row(callback.from_user)
    lang = normalize_lang(user_row["language"])
    asset = next((a for a in assets if a["id"] == asset_id), None)
    if not asset:
        await callback.answer(tr(lang, "Актив не найден.", "Asset not found.", "Актив не знайдено."), show_alert=True)
        return

    await state.update_data(asset_id=asset_id, asset_name=asset["name"])
    await state.set_state(ECNStates.choosing_direction)

    text = (
        f"{tr(lang, 'Открытие сделки', 'Open deal', 'Відкриття угоди')} [{asset['name']}]\n"
        f"{tr(lang, 'Выберите, в какую сторону пойдет график:', 'Choose direction:', 'Оберіть напрям:')}"
    )
    await callback.message.answer(text, reply_markup=ecn_direction_keyboard(asset_id, lang))
    await callback.answer()


@dp.callback_query(F.data.startswith("ecn_dir:"))
async def ecn_choose_direction(callback: CallbackQuery, state: FSMContext):
    _, direction, asset_id_str = callback.data.split(":")
    asset_id = int(asset_id_str)
    data = await state.get_data()
    user_row = await get_user_row(callback.from_user)
    lang = normalize_lang(user_row["language"])
    asset_name = data.get("asset_name")

    if not asset_name or data.get("asset_id") != asset_id:
        assets = await get_ecn_assets()
        asset = next((a for a in assets if a["id"] == asset_id), None)
        if not asset:
            await callback.answer(tr(lang, "Актив не найден.", "Asset not found.", "Актив не знайдено."), show_alert=True)
            return
        asset_name = asset["name"]
        await state.update_data(asset_id=asset_id, asset_name=asset_name)

    await state.update_data(direction=direction)
    await state.set_state(ECNStates.waiting_amount)

    user_row = await get_user_row(callback.from_user)
    currency = user_row["currency"] or "USD"

    text = (
        f"{tr(lang, 'Открытие сделки', 'Open deal', 'Відкриття угоди')} [{asset_name}]\n"
        f"{tr(lang, 'Направление', 'Direction', 'Напрям')}: {tr(lang, 'Повышение', 'Up', 'Вгору') if direction == 'up' else tr(lang, 'Понижение', 'Down', 'Вниз')}\n\n"
        f"{tr(lang, 'Введите сумму для открытия сделки', 'Enter amount to open deal', 'Введіть суму для відкриття угоди')} {currency}:"
    )
    await callback.message.answer(text)
    await callback.answer()


@dp.message(ECNStates.waiting_amount)
async def ecn_enter_amount(message: Message, state: FSMContext):
    data = await state.get_data()
    asset_name = data.get("asset_name")
    direction = data.get("direction")

    # Проверяем баланс пользователя
    user_row = await get_user_row(message.from_user)
    lang = normalize_lang(user_row["language"])
    balance = user_row["balance"] or 0.0
    currency = user_row["currency"] or "USD"

    if balance <= 0:
        await message.answer(
            tr(lang, f"⚠️ На вашем балансе 0 {currency}. Пополните баланс, чтобы открыть сделку.", f"⚠️ Your balance is 0 {currency}. Top up balance to open a deal.", f"⚠️ На вашому балансі 0 {currency}. Поповніть баланс, щоб відкрити угоду.")
        )
        await state.clear()
        return

    try:
        amount = float(message.text.replace(",", "."))
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer(tr(lang, "❗ Введите положительное число (сумму сделки).", "❗ Enter a positive number (deal amount).", "❗ Введіть додатне число (суму угоди)."))
        return

    if amount > balance:
        await message.answer(
            tr(lang, f"⚠️ Недостаточно средств. На балансе {balance:.2f} {currency}.", f"⚠️ Not enough funds. Balance: {balance:.2f} {currency}.", f"⚠️ Недостатньо коштів. Баланс: {balance:.2f} {currency}.")
        )
        return

    await state.update_data(amount=amount)
    await state.set_state(ECNStates.choosing_expiration)

    text = (
        f"{tr(lang, 'Открытие сделки', 'Open deal', 'Відкриття угоди')} [{asset_name}]\n"
        f"{tr(lang, 'Направление', 'Direction', 'Напрям')}: {tr(lang, 'Повышение', 'Up', 'Вгору') if direction == 'up' else tr(lang, 'Понижение', 'Down', 'Вниз')}\n"
        f"{tr(lang, 'Сумма', 'Amount', 'Сума')}: {amount:.2f} {currency}\n\n"
        f"{tr(lang, 'Выберите время фиксации:', 'Choose expiration time:', 'Оберіть час експірації:')}"
    )
    await message.answer(text, reply_markup=ecn_time_keyboard(lang))


@dp.callback_query(F.data.startswith("ecn_time:"))
async def ecn_choose_time(callback: CallbackQuery, state: FSMContext):
    seconds = int(callback.data.split(":", 1)[1])
    data = await state.get_data()
    asset_name = data.get("asset_name")
    direction = data.get("direction")
    amount = data.get("amount")

    user_row = await get_user_row(callback.from_user)
    lang = normalize_lang(user_row["language"])
    currency = user_row["currency"] or "USD"
    balance = user_row["balance"] or 0.0

    if not amount or amount <= 0:
        await callback.answer(tr(lang, "Сумма сделки не определена. Начните заново.", "Deal amount is not set. Start again.", "Суму угоди не визначено. Почніть заново."), show_alert=True)
        await state.clear()
        return

    if amount > balance:
        await callback.answer(tr(lang, "Недостаточно средств для открытия сделки.", "Insufficient funds to open deal.", "Недостатньо коштів для відкриття угоди."), show_alert=True)
        await state.clear()
        return

    await change_balance(callback.from_user.id, -amount)

    await state.clear()

    direction_text = tr(lang, "Повышение", "Up", "Вгору") if direction == "up" else tr(lang, "Понижение", "Down", "Вниз")

    text = (
        f"{tr(lang, 'Зарегистрирована заявка на сделку', 'Deal request created', 'Зареєстровано запит на угоду')}\n"
        f"└ {tr(lang, 'Актив', 'Asset', 'Актив')}: {asset_name}\n"
        f"└ {tr(lang, 'Направление', 'Direction', 'Напрям')}: {direction_text}\n"
        f"└ {tr(lang, 'Сумма', 'Amount', 'Сума')}: {amount:.2f} {currency}\n"
        f"└ {tr(lang, 'Время до фиксации', 'Time left', 'Час до фіксації')}: {seconds} {tr(lang, 'сек.', 'sec', 'с')}"
    )

    msg = await callback.message.answer(text)
    await callback.answer()

    asyncio.create_task(
        run_demo_deal(
            user_tg_id=callback.from_user.id,
            chat_id=msg.chat.id,
            message_id=msg.message_id,
            asset_name=asset_name,
            direction=direction,
            amount=amount,
            currency=currency,
            seconds=seconds,
        )
    )


async def run_demo_deal(
    user_tg_id: int,
    chat_id: int,
    message_id: int,
    asset_name: str,
    direction: str,
    amount: float,
    currency: str,
    seconds: int,
):
    user = await get_user_by_tg_id(user_tg_id)
    lang = normalize_lang(user["language"] if user else "ru")
    start_price = round(random.uniform(10, 100_000), 2)

    remaining = seconds
    while remaining > 0:
        try:
            text = (
                f"{tr(lang, 'Зарегистрирована заявка на сделку', 'Deal request created', 'Зареєстровано запит на угоду')}\n"
                f"└ {tr(lang, 'Актив', 'Asset', 'Актив')}: {asset_name}\n"
                f"└ {tr(lang, 'Направление', 'Direction', 'Напрям')}: {tr(lang, 'Повышение', 'Up', 'Вгору') if direction == 'up' else tr(lang, 'Понижение', 'Down', 'Вниз')}\n"
                f"└ {tr(lang, 'Сумма', 'Amount', 'Сума')}: {amount:.2f} {currency}\n"
                f"└ {tr(lang, 'Время до фиксации', 'Time left', 'Час до фіксації')}: {remaining} {tr(lang, 'сек.', 'sec', 'с')}"
            )
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
            )
        except Exception:
            pass

        await asyncio.sleep(1)
        remaining -= 1

    luck_percent = await get_luck_percent_for_client(user_tg_id)
    if luck_percent is None:
        win_prob = 0.5
    else:
        win_prob = max(0.0, min(1.0, luck_percent / 100.0))

    is_win = random.random() < win_prob
    asset_key = (asset_name or "").lower()
    if "bitcoin" in asset_key or "btc" in asset_key:
        base_vol = 0.25
    elif "doge" in asset_key or "pepe" in asset_key or "shiba" in asset_key:
        base_vol = 1.2
    elif "sol" in asset_key or "avax" in asset_key or "injective" in asset_key:
        base_vol = 0.75
    else:
        base_vol = 0.45

    impulse = random.uniform(0.8, 1.7)
    change_percent = random.uniform(base_vol * 0.35, base_vol * impulse)
    if (direction == "up" and is_win) or (direction == "down" and not is_win):
        end_price = start_price * (1 + change_percent / 100)
    else:
        end_price = start_price * (1 - change_percent / 100)

    payout_rate = random.choice([0.55, 0.6, 0.62, 0.65])
    profit = amount * payout_rate if is_win else -amount

    if is_win:
        await change_balance(user_tg_id, amount + (amount * payout_rate))

    header = f"{tr(lang, 'Завершенная сделка', 'Completed deal', 'Завершена угода')} [{asset_name}]"
    result_lines = [
        header,
        f"{tr(lang, 'Позиция', 'Position', 'Позиція')}: {tr(lang, 'Повышение', 'Up', 'Вгору') if direction == 'up' else tr(lang, 'Понижение', 'Down', 'Вниз')}",
        f"{tr(lang, 'Сумма', 'Amount', 'Сума')}: {amount:.2f} {currency}",
        f"{tr(lang, 'Начальный курс', 'Start price', 'Початкова ціна')}: {start_price:.2f} USD",
        f"{tr(lang, 'Курс в конце сделки', 'End price', 'Кінцева ціна')}: {end_price:.2f} USD ({change_percent:.3f}%)",
    ]

    if is_win:
        result_lines.append(tr(lang, f"✅ Успешная сделка. Вы получили {profit:.2f} {currency}", f"✅ Winning deal. You received {profit:.2f} {currency}", f"✅ Успішна угода. Ви отримали {profit:.2f} {currency}"))
    else:
        result_lines.append(tr(lang, f"❌ Сделка неуспешная. -{amount:.2f} {currency}", f"❌ Losing deal. -{amount:.2f} {currency}", f"❌ Неуспішна угода. -{amount:.2f} {currency}"))

    await save_deal(
        user_tg_id=user_tg_id,
        asset_name=asset_name,
        direction=direction,
        amount=amount,
        currency=currency,
        start_price=start_price,
        end_price=end_price,
        change_percent=change_percent,
        is_win=is_win,
        profit=profit,
        expires_in_sec=seconds,
    )

    await bot.send_message(chat_id, "\n".join(result_lines))

# ---------- PROFILE ----------

@dp.callback_query(F.data == "open_profile")
async def on_open_profile(callback: CallbackQuery):
    await send_profile(callback)


async def send_profile(callback_or_msg):
    if isinstance(callback_or_msg, CallbackQuery):
        tg_user = callback_or_msg.from_user
        msg = callback_or_msg.message
    else:
        tg_user = callback_or_msg.from_user
        msg = callback_or_msg

    user_row = await get_user_row(tg_user)
    lang = normalize_lang(user_row["language"])
    currency = user_row["currency"] or "USD"
    balance = user_row["balance"] or 0.0
    is_admin = bool(user_row["is_admin"]) or is_admin_id(tg_user.id)
    is_worker = bool(user_row["is_worker"])
    deal_stats = await get_user_deal_stats(tg_user.id)
    pending_withdraw = await get_user_pending_withdraw_sum(tg_user.id)
    worker_id = await get_worker_for_client(tg_user.id)

    text_lines = [
        t(lang, "profile_title"),
        t(lang, "profile_id", id=tg_user.id),
        t(lang, "profile_username", username=tg_user.username) if tg_user.username else t(lang, "profile_username_none"),
        t(lang, "profile_language", lang_value=lang.upper()),
        t(lang, "profile_currency", currency=currency),
        t(lang, "profile_verification"),
        t(lang, "profile_worker", worker=worker_id) if worker_id else t(lang, "profile_worker_none"),
        "",
        t(lang, "profile_deals_title"),
        t(lang, "profile_deals_total", total=deal_stats["total"]),
        t(lang, "profile_deals_wins", wins=deal_stats["wins"]),
        t(lang, "profile_deals_losses", losses=deal_stats["losses"]),
        t(lang, "profile_deals_pnl", pnl=deal_stats["total_profit"], currency=currency),
        "",
        t(lang, "profile_balance_title"),
        t(lang, "profile_balance_value", balance=balance, currency=currency),
        t(lang, "profile_pending_value", pending=pending_withdraw, currency=currency),
    ]
    text = "\n".join(text_lines)
    await msg.answer(text, reply_markup=profile_keyboard(is_admin, is_worker, lang))

# ---------- DEPOSIT ----------

@dp.callback_query(F.data == "deposit")
async def on_deposit(callback: CallbackQuery, state: FSMContext):
    user_row = await get_user_row(callback.from_user)
    lang = normalize_lang(user_row["language"])
    currency = user_row["currency"] or "USD"
    await callback.message.answer(t(lang, "deposit_enter_amount", currency=currency))
    await state.set_state(DepositStates.waiting_amount)
    await callback.answer()


@dp.message(DepositStates.waiting_amount)
async def deposit_amount_entered(message: Message, state: FSMContext):
    user_row = await get_user_row(message.from_user)
    lang = normalize_lang(user_row["language"])
    currency = user_row["currency"] or "USD"
    try:
        amount = float(message.text.replace(",", "."))
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer(t(lang, "deposit_invalid_amount"))
        return
    await state.update_data(deposit_amount=amount)
    await state.set_state(DepositStates.waiting_method)
    await message.answer(
        t(lang, "deposit_choose_method", amount=amount, currency=currency),
        reply_markup=deposit_method_keyboard(lang),
    )


@dp.callback_query(DepositStates.waiting_method, F.data == "cancel_deposit")
async def on_cancel_deposit(callback: CallbackQuery, state: FSMContext):
    user_row = await get_user_row(callback.from_user)
    lang = normalize_lang(user_row["language"])
    await state.clear()
    await callback.message.answer(t(lang, "deposit_cancelled"))
    await callback.answer()


async def send_deposit_to_support(callback: CallbackQuery, state: FSMContext, method: str):
    data = await state.get_data()
    amount = data.get("deposit_amount")
    user_row = await get_user_row(callback.from_user)
    lang = normalize_lang(user_row["language"])
    currency = user_row["currency"] or "USD"
    if amount is None:
        await callback.message.answer(
            tr(
                lang,
                "Сумма пополнения не найдена. Начните заново.",
                "Deposit amount not found. Start again.",
                "Суму поповнення не знайдено. Почніть заново.",
            )
        )
        await callback.answer()
        return

    dep_id = await create_deposit_request(callback.from_user.id, amount, currency, method)
    await log_activity_for_worker_client(
        client_tg_id=callback.from_user.id,
        actor_tg_id=callback.from_user.id,
        actor_source="bot",
        event_type="bot_deposit_request",
        title="Заявка на пополнение",
        details=f"Реферал создал заявку на пополнение через Telegram-бота. Метод: {deposit_method_label(method)}.",
        amount=amount,
        currency=currency,
        meta={"deposit_id": dep_id, "method": method},
    )
    await notify_worker_bot_deposit_event(
        client_tg_id=callback.from_user.id,
        first_name=callback.from_user.first_name,
        username=callback.from_user.username,
        amount=amount,
        currency=currency,
        method=method,
        stage="request_created",
        deposit_id=dep_id,
    )
    await log_activity_for_worker_client(
        client_tg_id=callback.from_user.id,
        actor_tg_id=callback.from_user.id,
        actor_source="bot",
        event_type="bot_deposit_support_opened",
        title="Переход в поддержку по пополнению",
        details=f"Реферал перешёл в поддержку по пополнению через Telegram-бота. Метод: {deposit_method_label(method)}.",
        amount=amount,
        currency=currency,
        meta={"deposit_id": dep_id, "method": method},
    )
    await notify_worker_bot_deposit_event(
        client_tg_id=callback.from_user.id,
        first_name=callback.from_user.first_name,
        username=callback.from_user.username,
        amount=amount,
        currency=currency,
        method=method,
        stage="support_opened",
        deposit_id=dep_id,
    )
    await create_support_ticket(
        client_tg_id=callback.from_user.id,
        worker_tg_id=await get_worker_for_client(callback.from_user.id),
        source="bot",
        topic="deposit",
        subject=f"Пополнение через {deposit_method_label(method)}",
        status="new",
        last_message=f"Клиент создал заявку на пополнение {amount:.2f} {currency}",
        meta={"deposit_id": dep_id, "method": method, "amount": amount, "currency": currency},
    )
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE worker_clients SET funnel_stage = 'support_wait', last_activity_at = CURRENT_TIMESTAMP WHERE client_tg_id = ?",
            (callback.from_user.id,),
        )
        await db.commit()

    text = (
        f"{tr(lang, 'Способ оплаты', 'Payment method', 'Спосіб оплати')}: <b>{deposit_method_label(method)}</b>\n\n"
        f"{tr(lang, 'Сумма к оплате', 'Amount to pay', 'Сума до оплати')}: <b>{amount:.2f} {currency}</b>\n\n"
        f"{tr(lang, 'Заявка создана. Для завершения пополнения перейдите в техподдержку. Там вам передадут актуальные инструкции для оплаты.', 'Request created. Open support to complete the deposit. They will provide the current payment instructions there.', 'Заявку створено. Для завершення поповнення перейдіть у техпідтримку. Там вам нададуть актуальні інструкції для оплати.')}"
    )
    await callback.message.answer(text, reply_markup=deposit_support_keyboard(lang))
    await state.clear()
    await callback.answer()


@dp.callback_query(DepositStates.waiting_method, F.data == "pay_crypto")
async def on_pay_crypto(callback: CallbackQuery, state: FSMContext):
    await send_deposit_to_support(callback, state, "crypto")


@dp.callback_query(DepositStates.waiting_method, F.data == "pay_trc20")
async def on_pay_trc20(callback: CallbackQuery, state: FSMContext):
    await send_deposit_to_support(callback, state, "trc20")


@dp.callback_query(DepositStates.waiting_method, F.data == "pay_card")
async def on_pay_card(callback: CallbackQuery, state: FSMContext):
    await send_deposit_to_support(callback, state, "card")

@dp.callback_query(F.data == "check_payment")
@dp.callback_query(F.data == "check_payment")
async def on_check_payment(callback: CallbackQuery, state: FSMContext):
    user_row = await get_user_row(callback.from_user)
    lang = normalize_lang(user_row["language"])
    await callback.message.answer(
        tr(
            lang,
            "Для завершения пополнения перейдите в техподдержку. Там вам передадут актуальные инструкции и подтвердят оплату.",
            "Open support to complete the deposit. They will provide the current instructions and confirm the payment there.",
            "Для завершення поповнення перейдіть у техпідтримку. Там вам нададуть актуальні інструкції та підтвердять оплату.",
        ),
        reply_markup=deposit_support_keyboard(lang),
    )
    await callback.answer()


@dp.callback_query(F.data == "check_payment_card")
async def on_check_payment_card(callback: CallbackQuery, state: FSMContext):
    user_row = await get_user_row(callback.from_user)
    lang = normalize_lang(user_row["language"])
    await callback.message.answer(
        tr(
            lang,
            "Для завершения пополнения перейдите в техподдержку. Там вам передадут актуальные инструкции и подтвердят оплату.",
            "Open support to complete the deposit. They will provide the current instructions and confirm the payment there.",
            "Для завершення поповнення перейдіть у техпідтримку. Там вам нададуть актуальні інструкції та підтвердять оплату.",
        ),
        reply_markup=deposit_support_keyboard(lang),
    )
    await callback.answer()


# ---------- WITHDRAW ----------

@dp.callback_query(F.data == "withdraw")
async def on_withdraw(callback: CallbackQuery, state: FSMContext):
    user_row = await get_user_row(callback.from_user)
    lang = normalize_lang(user_row["language"])
    currency = user_row["currency"] or "USD"
    await callback.message.answer(
        tr(lang, f"📤 На какую сумму вы хотите вывести средства? Введите сумму в {currency}.", f"📤 How much do you want to withdraw? Enter amount in {currency}.", f"📤 На яку суму ви хочете вивести кошти? Вкажіть суму в {currency}.")
    )
    await state.set_state(WithdrawStates.waiting_amount)
    await callback.answer()


@dp.message(WithdrawStates.waiting_amount)
async def withdraw_amount_entered(message: Message, state: FSMContext):
    user_row = await get_user_row(message.from_user)
    lang = normalize_lang(user_row["language"])
    currency = user_row["currency"] or "USD"
    balance = user_row["balance"] or 0.0
    try:
        amount = float(message.text.replace(",", "."))
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer(tr(lang, "❗ Введите корректную положительную сумму.", "❗ Enter a valid positive amount.", "❗ Введіть коректну додатну суму."))
        return
    if amount > balance:
        await message.answer(tr(lang, f"❗ Недостаточно средств. Доступно: {balance:.2f} {currency}.", f"❗ Not enough funds. Available: {balance:.2f} {currency}.", f"❗ Недостатньо коштів. Доступно: {balance:.2f} {currency}."))
        await state.clear()
        return
    await state.update_data(withdraw_amount=amount)
    await message.answer(
        tr(lang, f"Вы хотите вывести <b>{amount:.2f} {currency}</b>.\n\nВыберите способ вывода:", f"You want to withdraw <b>{amount:.2f} {currency}</b>.\n\nChoose withdraw method:", f"Ви хочете вивести <b>{amount:.2f} {currency}</b>.\n\nОберіть спосіб виведення:"),
        reply_markup=withdraw_method_keyboard(),
    )


@dp.callback_query(F.data == "wd_cancel")
async def on_withdraw_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("❌ Заявка на вывод отменена.")
    await callback.answer()


@dp.callback_query(F.data == "wd_card")
async def on_withdraw_card(callback: CallbackQuery, state: FSMContext):
    await state.set_state(WithdrawStates.waiting_card)
    await callback.message.answer("💳 Введите номер карты, на которую нужно отправить деньги:")
    await callback.answer()


@dp.callback_query(F.data == "wd_trc20")
async def on_withdraw_trc20(callback: CallbackQuery, state: FSMContext):
    await state.set_state(WithdrawStates.waiting_wallet)
    await callback.message.answer("👛 Введите адрес кошелька TRC20 USDT:")
    await callback.answer()

@dp.message(WithdrawStates.waiting_card)
async def process_withdraw_card(message: Message, state: FSMContext):
    data = await state.get_data()
    amount = data.get("withdraw_amount")
    if amount is None:
        await message.answer("Сумма не найдена. Начните заново.")
        await state.clear()
        return
    user_row = await get_user_row(message.from_user)
    currency = user_row["currency"] or "USD"
    card = message.text.strip()
    wd_id = await create_withdrawal(message.from_user.id, amount, currency, "card", card)
    await log_activity_for_worker_client(
        client_tg_id=message.from_user.id,
        actor_tg_id=message.from_user.id,
        actor_source="bot",
        event_type="bot_withdraw_request",
        title="Заявка на вывод",
        details="Реферал создал заявку на вывод на карту через Telegram-бота.",
        amount=amount,
        currency=currency,
        meta={"withdrawal_id": wd_id, "method": "card"},
    )
    await message.answer("✅ Заявка на вывод отправлена на рассмотрение.")
    await state.clear()

    text_admin = (
        "💸 <b>Новая заявка на вывод</b>\n\n"
        f"ID заявки: <b>{wd_id}</b>\n"
        f"Пользователь: <a href='tg://user?id={message.from_user.id}'>{message.from_user.full_name}</a>\n"
        f"TG ID: <code>{message.from_user.id}</code>\n"
        f"Сумма: {amount:.2f} {currency}\n"
        "Метод: 💳 На карту\n"
        f"Карта: <code>{card}</code>"
    )
    for admin_id in config.admin_ids:
        try:
            await bot.send_message(admin_id, text_admin, reply_markup=withdrawal_admin_keyboard(wd_id))
        except Exception:
            pass
    if config.log_chat_id:
        try:
            await bot.send_message(config.log_chat_id, text_admin, reply_markup=withdrawal_admin_keyboard(wd_id))
        except Exception:
            pass

@dp.message(WithdrawStates.waiting_wallet)
async def process_withdraw_wallet(message: Message, state: FSMContext):
    data = await state.get_data()
    amount = data.get("withdraw_amount")
    if amount is None:
        await message.answer("Сумма не найдена. Начните заново.")
        await state.clear()
        return
    user_row = await get_user_row(message.from_user)
    currency = user_row["currency"] or "USD"
    wallet = message.text.strip()
    wd_id = await create_withdrawal(message.from_user.id, amount, currency, "trc20", wallet)
    await log_activity_for_worker_client(
        client_tg_id=message.from_user.id,
        actor_tg_id=message.from_user.id,
        actor_source="bot",
        event_type="bot_withdraw_request",
        title="Заявка на вывод",
        details="Реферал создал заявку на вывод на TRC20-кошелёк через Telegram-бота.",
        amount=amount,
        currency=currency,
        meta={"withdrawal_id": wd_id, "method": "trc20"},
    )
    await message.answer("✅ Заявка на вывод отправлена на рассмотрение.")
    await state.clear()

    text_admin = (
        "💸 <b>Новая заявка на вывод</b>\n\n"
        f"ID заявки: <b>{wd_id}</b>\n"
        f"Пользователь: <a href='tg://user?id={message.from_user.id}'>{message.from_user.full_name}</a>\n"
        f"TG ID: <code>{message.from_user.id}</code>\n"
        f"Сумма: {amount:.2f} {currency}\n"
        "Метод: 👛 На кошелёк TRC20 USDT\n"
        f"Кошелёк: <code>{wallet}</code>"
    )
    for admin_id in config.admin_ids:
        try:
            await bot.send_message(admin_id, text_admin, reply_markup=withdrawal_admin_keyboard(wd_id))
        except Exception:
            pass
    if config.log_chat_id:
        try:
            await bot.send_message(config.log_chat_id, text_admin, reply_markup=withdrawal_admin_keyboard(wd_id))
        except Exception:
            pass

@dp.callback_query(F.data.startswith("wd_ok:"))
async def approve_withdrawal(callback: CallbackQuery):
    if not is_admin_id(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.")
        return
    w_id = int(callback.data.split(":", 1)[1])
    row = await get_withdrawal(w_id)
    if not row or row["status"] != "pending":
        await callback.answer("Заявка не найдена или уже обработана.")
        return
    user_id = row["user_tg_id"]
    amount = row["amount"]
    currency = row["currency"]

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT balance FROM users WHERE tg_id = ?", (user_id,))
        urow = await cur.fetchone()
        balance = urow["balance"] if urow else 0.0
    if balance < amount:
        await callback.message.answer(
            f"⚠️ Недостаточный баланс пользователя (на счёте {balance:.2f})."
        )
        await callback.answer()
        return

    await change_balance(user_id, -amount)
    await set_withdrawal_status(w_id, "approved")
    await log_activity_for_worker_client(
        client_tg_id=int(user_id),
        actor_tg_id=callback.from_user.id,
        actor_source="bot",
        event_type="bot_withdraw_approved",
        title="Вывод одобрен",
        details="Администратор одобрил заявку на вывод.",
        amount=float(amount),
        currency=currency,
        meta={"withdrawal_id": w_id},
    )
    try:
        await bot.send_message(
            user_id,
            "✅ Ваша заявка на вывод одобрена. Средства поступят в течение 1 часа.",
        )
    except Exception:
        pass
    await callback.message.answer(f"✅ Заявка #{w_id} одобрена.")
    await callback.answer()


@dp.callback_query(F.data.startswith("wd_no:"))
async def reject_withdrawal(callback: CallbackQuery):
    if not is_admin_id(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.")
        return
    w_id = int(callback.data.split(":", 1)[1])
    row = await get_withdrawal(w_id)
    if not row or row["status"] != "pending":
        await callback.answer("Заявка не найдена или уже обработана.")
        return
    user_id = row["user_tg_id"]
    await set_withdrawal_status(w_id, "rejected")
    await log_activity_for_worker_client(
        client_tg_id=int(user_id),
        actor_tg_id=callback.from_user.id,
        actor_source="bot",
        event_type="bot_withdraw_rejected",
        title="Вывод отклонён",
        details="Администратор отклонил заявку на вывод.",
        amount=float(row["amount"]),
        currency=row["currency"],
        meta={"withdrawal_id": w_id},
    )
    try:
        await bot.send_message(
            user_id,
            "❌ Не удалось вывести средства. Обратитесь в техническую поддержку.",
        )
    except Exception:
        pass
    await callback.message.answer(f"❌ Заявка #{w_id} отклонена.")
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_dep_confirm:"))
async def approve_deposit(callback: CallbackQuery):
    if not is_admin_id(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.")
        return
    dep_id = int(callback.data.split(":", 1)[1])
    dep = await get_deposit_request(dep_id)
    if not dep or dep["status"] != "pending":
        await callback.answer("Заявка не найдена или уже обработана.")
        return
    user_id = int(dep["user_tg_id"])
    amount = float(dep["amount"])
    currency = dep["currency"]
    await change_balance(user_id, amount)
    await set_deposit_request_status(dep_id, "approved")
    await log_activity_for_worker_client(
        client_tg_id=user_id,
        actor_tg_id=callback.from_user.id,
        actor_source="bot",
        event_type="bot_deposit_approved",
        title="Пополнение подтверждено",
        details="Администратор подтвердил заявку на пополнение.",
        amount=amount,
        currency=currency,
        meta={"deposit_id": dep_id},
    )
    try:
        await bot.send_message(user_id, f"✅ Пополнение подтверждено: +{amount:.2f} {currency}")
    except Exception:
        pass
    await callback.message.answer(f"✅ Пополнение #{dep_id} подтверждено.")
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_dep_reject:"))
async def reject_deposit(callback: CallbackQuery):
    if not is_admin_id(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.")
        return
    dep_id = int(callback.data.split(":", 1)[1])
    dep = await get_deposit_request(dep_id)
    if not dep or dep["status"] != "pending":
        await callback.answer("Заявка не найдена или уже обработана.")
        return
    await set_deposit_request_status(dep_id, "rejected")
    await log_activity_for_worker_client(
        client_tg_id=int(dep["user_tg_id"]),
        actor_tg_id=callback.from_user.id,
        actor_source="bot",
        event_type="bot_deposit_rejected",
        title="Пополнение отклонено",
        details="Администратор отклонил заявку на пополнение.",
        amount=float(dep["amount"]),
        currency=dep["currency"],
        meta={"deposit_id": dep_id},
    )
    try:
        await bot.send_message(int(dep["user_tg_id"]), "❌ Пополнение отклонено администратором.")
    except Exception:
        pass
    await callback.message.answer(f"❌ Пополнение #{dep_id} отклонено.")
    await callback.answer()

# ---------- VERIFICATION & SETTINGS ----------

@dp.callback_query(F.data == "verify")
async def on_verify(callback: CallbackQuery):
    text = (
        "🔐 <b>Ваш аккаунт не верифицирован</b>\n\n"
        "Для прохождения верификации нажмите кнопку «Поддержка», "
        "затем свяжитесь с агентом, укажите ваш ID и следуйте инструкциям."
    )
    await callback.message.answer(text, reply_markup=verification_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "settings")
async def on_settings(callback: CallbackQuery):
    user_row = await get_user_row(callback.from_user)
    lang = normalize_lang(user_row["language"])
    cur = user_row["currency"] or "не выбрана"
    text = t(lang, "settings_title", lang_value=lang.upper(), cur=cur)
    await callback.message.answer(text, reply_markup=settings_keyboard(lang))
    await callback.answer()


@dp.callback_query(F.data == "settings_lang")
async def on_settings_lang(callback: CallbackQuery):
    user_row = await get_user_row(callback.from_user)
    lang = normalize_lang(user_row["language"])
    await callback.message.answer(t(lang, "settings_choose_lang"), reply_markup=language_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "settings_currency")
async def on_settings_currency(callback: CallbackQuery):
    user_row = await get_user_row(callback.from_user)
    lang = normalize_lang(user_row["language"])
    await callback.message.answer(t(lang, "settings_choose_currency"), reply_markup=currency_keyboard())
    await callback.answer()


@dp.callback_query(F.data == "my_deals")
async def on_my_deals(callback: CallbackQuery):
    user_row = await get_user_row(callback.from_user)
    lang = normalize_lang(user_row["language"])
    rows = await get_user_deals(callback.from_user.id, limit=10)
    if not rows:
        await callback.message.answer(t(lang, "my_deals_empty"))
        await callback.answer()
        return

    lines = [t(lang, "my_deals_title"), ""]
    for row in rows:
        side = "UP" if row["direction"] == "up" else "DOWN"
        outcome = "✅" if row["is_win"] else "❌"
        lines.append(
            f"{outcome} #{row['id']} {row['asset_name']} {side} | "
            f"{row['amount']:.2f} {row['currency']} | PnL: {row['profit']:+.2f}"
        )

    await callback.message.answer("\n".join(lines))
    await callback.answer()

# ---------- WORKER PANEL ----------

@dp.callback_query(F.data == "open_worker_panel")
async def open_worker_panel_cb(callback: CallbackQuery):
    user_row = await get_user_row(callback.from_user)
    if not bool(user_row["is_worker"]):
        await callback.answer("⛔ Воркер-панель доступна только воркерам.")
        return
    await send_worker_panel(callback.message, callback.from_user)
    await callback.answer()


@dp.message(Command("worker"))
async def worker_cmd(message: Message):
    user_row = await get_user_row(message.from_user)
    if not bool(user_row["is_worker"]):
        await message.answer("⛔ Воркер-панель доступна только воркерам.")
        return
    await send_worker_panel(message, message.from_user)


async def send_worker_panel(msg: Message, tg_user):
    global BOT_USERNAME
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref{tg_user.id}"
    ref_count = await get_referral_count(tg_user.id)
    text = (
        "⚒ <b>Панель воркера</b>\n\n"
        f"Ваш ID: <code>{tg_user.id}</code>\n"
        f"Реферальная ссылка:\n<code>{ref_link}</code>\n\n"
        f"Приглашено пользователей: <b>{ref_count}</b>\n\n"
        "Выберите раздел:"
    )
    await msg.answer(text, reply_markup=worker_panel_keyboard())

@dp.callback_query(F.data == "worker_sheeps")
async def worker_sheeps(callback: CallbackQuery):
    user_row = await get_user_row(callback.from_user)
    if not bool(user_row["is_worker"]):
        await callback.answer("⛔ Только для воркеров.")
        return
    rows = await get_worker_clients_list(callback.from_user.id)
    text = "🐑 <b>Список лохматых</b>\n\n"
    if not rows:
        text += "—\nПервая запись: -"
    else:
        first = rows[0]
        text += f"—\nПервая запись: /n{first['id']} (ID {first['client_tg_id']})"
    await callback.message.answer(text, reply_markup=worker_sheeps_keyboard(rows))
    await callback.answer()


@dp.callback_query(F.data.startswith("sheep_period:"))
async def sheep_period_stub(callback: CallbackQuery):
    await callback.answer("Фильтры по периоду пока не реализованы.", show_alert=True)


@dp.callback_query(F.data == "sheep_fav")
async def sheep_fav(callback: CallbackQuery):
    rows = await get_worker_clients_list(callback.from_user.id, favorites_only=True)
    text = "⭐ <b>Избранные лохматые</b>\n\n"
    if not rows:
        text += "— список пуст."
    else:
        first = rows[0]
        text += f"Первая запись: /n{first['id']} (ID {first['client_tg_id']})"
    await callback.message.answer(text, reply_markup=worker_sheeps_keyboard(rows))
    await callback.answer()


@dp.callback_query(F.data == "sheep_search")
async def sheep_search(callback: CallbackQuery):
    await callback.message.answer("🔍 Поиск по ID пока не реализован.")
    await callback.answer()

@dp.message(F.text.startswith("/n"))
async def worker_open_by_command(message: Message):
    user_row = await get_user_row(message.from_user)
    if not bool(user_row["is_worker"]):
        return
    try:
        wc_id = int(message.text[2:])
    except ValueError:
        await message.answer("Некорректный формат. Используйте /n<ID>.")
        return
    await open_worker_client_profile(message, wc_id)


@dp.callback_query(F.data.startswith("wc_profile:"))
async def wc_profile_callback(callback: CallbackQuery):
    wc_id = int(callback.data.split(":", 1)[1])
    user_row = await get_user_row(callback.from_user)
    if not bool(user_row["is_worker"]):
        await callback.answer("⛔ Только для воркеров.")
        return
    await open_worker_client_profile(callback.message, wc_id)
    await callback.answer()

async def open_worker_client_profile(msg: Message, wc_id: int):
    row = await get_worker_client_by_id(wc_id)
    if not row:
        await msg.answer("Лохматый не найден.")
        return
    balance = row["balance"] or 0.0
    currency = row["currency"] or "USD"
    min_dep = row["min_deposit"] or 0
    min_wd = row["min_withdraw"] or 0
    flags = {
        "verified": bool(row["verified"]),
        "withdraw_enabled": bool(row["withdraw_enabled"]),
        "trading_enabled": bool(row["trading_enabled"]),
        "favorite": bool(row["favorite"]),
        "blocked": bool(row["blocked"]),
    }

    # Текущий процент удачи от этого воркера для клиента
    worker_id = msg.from_user.id
    current_luck = await get_luck_for_worker_client(worker_id, row["client_tg_id"])
    luck_text = f"{current_luck:.2f}%" if current_luck is not None else "не установлена"

    text = (
        f"📄 Профиль лохматого /n{wc_id}\n\n"
        "Информация\n"
        f"└ Баланс: {balance:.2f} {currency}\n"
        f"└ Min. deposit: {min_dep} {currency}\n"
        f"└ Min. withdraw: {min_wd} {currency}\n"
        f"└ Удача: {luck_text}\n"
        f"{'✅' if flags['trading_enabled'] else '❌'} Покупка включена/выключена\n"
        f"{'✅' if flags['withdraw_enabled'] else '❌'} Withdrawal enabled\n"
        f"{'✅' if flags['verified'] else '❌'} Верификация\n"
        f"{'🔒 Заблокирован' if flags['blocked'] else '🔓 Не заблокирован'}\n"
    )
    await msg.answer(
        text,
        reply_markup=worker_client_profile_keyboard(wc_id, flags, balance, currency, min_dep, min_wd),
    )




@dp.callback_query(F.data.startswith("wc_luck:"))
async def wc_luck(callback: CallbackQuery, state: FSMContext):
    wc_id = int(callback.data.split(":", 1)[1])
    user_row = await get_user_row(callback.from_user)
    if not bool(user_row["is_worker"]):
        await callback.answer("⛔ Только для воркеров.")
        return

    row = await get_worker_client_by_id(wc_id)
    if not row:
        await callback.message.answer("Лохматый не найден.")
        await callback.answer()
        return

    current_luck = await get_luck_for_worker_client(callback.from_user.id, row["client_tg_id"])
    if current_luck is None:
        text = "☘️ Текущий процент удачи не установлен.\nВведите новое значение от 0 до 100:"
    else:
        text = f"☘️ Текущий процент удачи: {current_luck:.2f}%\nВведите новое значение от 0 до 100:"

    await state.update_data(wc_id=wc_id)
    await state.set_state(WorkerLuckStates.waiting_luck_percent)
    await callback.message.answer(text)
    await callback.answer()


@dp.message(WorkerLuckStates.waiting_luck_percent)
async def wc_luck_percent_entered(message: Message, state: FSMContext):
    data = await state.get_data()
    wc_id = data.get("wc_id")
    try:
        luck_percent = float(message.text.replace(",", "."))
        if not (0 <= luck_percent <= 100):
            raise ValueError
    except ValueError:
        await message.answer("❗ Введите число от 0 до 100 (процент удачи).")
        return

    row = await get_worker_client_by_id(wc_id)
    if not row:
        await message.answer("Лохматый не найден.")
        await state.clear()
        return

    worker_id = message.from_user.id
    client_id = row["client_tg_id"]
    await set_client_luck(worker_id, client_id, luck_percent)
    await message.answer(f"✅ Удача клиента установлена на {luck_percent:.2f}%.")
    await state.clear()


# --- действия воркера по клиенту ---

@dp.callback_query(F.data.startswith("wc_adj_balance:"))
async def wc_adj_balance(callback: CallbackQuery, state: FSMContext):
    wc_id = int(callback.data.split(":", 1)[1])
    await state.update_data(wc_id=wc_id)
    await state.set_state(WorkerClientStates.waiting_balance_amount)
    await callback.message.answer("💰 Введите сумму, на которую увеличить баланс лохматого:")
    await callback.answer()


@dp.message(WorkerClientStates.waiting_balance_amount)
async def wc_adj_balance_amount(message: Message, state: FSMContext):
    data = await state.get_data()
    wc_id = data.get("wc_id")
    try:
        amount = float(message.text.replace(",", "."))
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❗ Введите положительное число.")
        return
    row = await get_worker_client_by_id(wc_id)
    if not row:
        await message.answer("Лохматый не найден.")
        await state.clear()
        return
    client_id = row["client_tg_id"]
    await change_balance(client_id, amount)
    await message.answer(f"✅ Баланс клиента увеличен на {amount:.2f}.")
    try:
        await bot.send_message(client_id, f"💰 Ваш баланс был пополнен на {amount:.2f}.")
    except Exception:
        pass
    await state.clear()

@dp.callback_query(F.data.startswith("wc_min_dep:"))
async def wc_min_dep_cb(callback: CallbackQuery, state: FSMContext):
    wc_id = int(callback.data.split(":", 1)[1])
    await state.update_data(wc_id=wc_id)
    await state.set_state(WorkerClientStates.waiting_min_dep)
    await callback.message.answer("📨 Введите Min. deposit для этого лохматого:")
    await callback.answer()


@dp.message(WorkerClientStates.waiting_min_dep)
async def wc_min_dep_set(message: Message, state: FSMContext):
    data = await state.get_data()
    wc_id = data.get("wc_id")
    try:
        value = float(message.text.replace(",", "."))
        if value < 0:
            raise ValueError
    except ValueError:
        await message.answer("❗ Введите неотрицательное число.")
        return
    await update_worker_client_field(wc_id, "min_deposit", value)
    await message.answer(f"✅ Min. deposit клиента установлен: {value}")
    await state.clear()


@dp.callback_query(F.data.startswith("wc_min_wd:"))
async def wc_min_wd_cb(callback: CallbackQuery, state: FSMContext):
    wc_id = int(callback.data.split(":", 1)[1])
    await state.update_data(wc_id=wc_id)
    await state.set_state(WorkerClientStates.waiting_min_wd)
    await callback.message.answer("📨 Введите Min. withdraw для этого лохматого:")
    await callback.answer()


@dp.message(WorkerClientStates.waiting_min_wd)
async def wc_min_wd_set(message: Message, state: FSMContext):
    data = await state.get_data()
    wc_id = data.get("wc_id")
    try:
        value = float(message.text.replace(",", "."))
        if value < 0:
            raise ValueError
    except ValueError:
        await message.answer("❗ Введите неотрицательное число.")
        return
    await update_worker_client_field(wc_id, "min_withdraw", value)
    await message.answer(f"✅ Min. withdraw клиента установлен: {value}")
    await state.clear()

@dp.callback_query(F.data.startswith("wc_toggle_verif:"))
async def wc_toggle_verif(callback: CallbackQuery):
    wc_id = int(callback.data.split(":", 1)[1])
    row = await get_worker_client_by_id(wc_id)
    if not row:
        await callback.answer("Не найден.")
        return
    new_val = 0 if row["verified"] else 1
    await update_worker_client_field(wc_id, "verified", new_val)
    await open_worker_client_profile(callback.message, wc_id)
    await callback.answer("Статус верификации изменён.")


@dp.callback_query(F.data.startswith("wc_toggle_withdraw:"))
async def wc_toggle_withdraw(callback: CallbackQuery):
    wc_id = int(callback.data.split(":", 1)[1])
    row = await get_worker_client_by_id(wc_id)
    if not row:
        await callback.answer("Не найден.")
        return
    new_val = 0 if row["withdraw_enabled"] else 1
    await update_worker_client_field(wc_id, "withdraw_enabled", new_val)
    await open_worker_client_profile(callback.message, wc_id)
    await callback.answer("Статус вывода изменён.")


@dp.callback_query(F.data.startswith("wc_toggle_trade:"))
async def wc_toggle_trade(callback: CallbackQuery):
    wc_id = int(callback.data.split(":", 1)[1])
    row = await get_worker_client_by_id(wc_id)
    if not row:
        await callback.answer("Не найден.")
        return
    new_val = 0 if row["trading_enabled"] else 1
    await update_worker_client_field(wc_id, "trading_enabled", new_val)
    await open_worker_client_profile(callback.message, wc_id)
    await callback.answer("Статус торговли изменён.")


@dp.callback_query(F.data.startswith("wc_toggle_fav:"))
async def wc_toggle_fav(callback: CallbackQuery):
    wc_id = int(callback.data.split(":", 1)[1])
    row = await get_worker_client_by_id(wc_id)
    if not row:
        await callback.answer("Не найден.")
        return
    new_val = 0 if row["favorite"] else 1
    await update_worker_client_field(wc_id, "favorite", new_val)
    await open_worker_client_profile(callback.message, wc_id)
    await callback.answer("Избранное обновлено.")


@dp.callback_query(F.data.startswith("wc_toggle_block:"))
async def wc_toggle_block(callback: CallbackQuery):
    wc_id = int(callback.data.split(":", 1)[1])
    row = await get_worker_client_by_id(wc_id)
    if not row:
        await callback.answer("Не найден.")
        return
    new_val = 0 if row["blocked"] else 1
    await update_worker_client_field(wc_id, "blocked", new_val)
    await open_worker_client_profile(callback.message, wc_id)
    await callback.answer("Статус блокировки изменён.")

@dp.callback_query(F.data.startswith("wc_transfer:"))
async def wc_transfer_cb(callback: CallbackQuery, state: FSMContext):
    wc_id = int(callback.data.split(":", 1)[1])
    await state.update_data(wc_id=wc_id)
    await state.set_state(WorkerClientStates.waiting_transfer_worker)
    await callback.message.answer("📤 Введите ID воркера, которому хотите передать лохматого:")
    await callback.answer()


@dp.message(WorkerClientStates.waiting_transfer_worker)
async def wc_transfer_set(message: Message, state: FSMContext):
    data = await state.get_data()
    wc_id = data.get("wc_id")
    try:
        new_worker_id = int(message.text.strip())
    except ValueError:
        await message.answer("❗ Введите числовой ID воркера.")
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE worker_clients SET worker_tg_id = ? WHERE id = ?",
            (new_worker_id, wc_id),
        )
        await db.commit()
    await message.answer(f"✅ Лохматый передан воркеру {new_worker_id}.")
    await state.clear()

# ---------- WORKER SERVICE SETTINGS ----------

@dp.callback_query(F.data == "worker_settings")
async def worker_settings_cb(callback: CallbackQuery):
    user_row = await get_user_row(callback.from_user)
    if not bool(user_row["is_worker"]):
        await callback.answer("⛔ Только для воркеров.")
        return
    ws = await get_worker_settings(callback.from_user.id)
    currency = user_row["currency"] or "RUB"
    text = (
        "⚙️ <b>Settings сервиса</b>\n\n"
        "Current values:\n"
        f"— Min. deposit: {ws['min_deposit']} {currency}\n"
        f"— Min. withdraw: {ws['min_withdraw']} {currency}\n"
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="📨 Min. deposit", callback_data="ws_min_dep")
    kb.button(text="📨 Min. withdraw", callback_data="ws_min_wd")
    kb.button(text="⬅ Назад", callback_data="open_worker_panel")
    kb.adjust(1)
    await callback.message.answer(text, reply_markup=kb.as_markup())
    await callback.answer()


@dp.callback_query(F.data == "ws_min_dep")
async def ws_min_dep_cb(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("📨 Введите новое значение Min. deposit для вашей ссылки:")
    await state.set_state(WorkerServiceStates.waiting_min_dep)
    await callback.answer()


@dp.message(WorkerServiceStates.waiting_min_dep)
async def ws_min_dep_set(message: Message, state: FSMContext):
    try:
        value = float(message.text.replace(",", "."))
        if value < 0:
            raise ValueError
    except ValueError:
        await message.answer("❗ Введите неотрицательное число.")
        return
    await set_worker_setting(message.from_user.id, "min_deposit", value)
    await message.answer(f"✅ Min. deposit установлен: {value}")
    await state.clear()


@dp.callback_query(F.data == "ws_min_wd")
async def ws_min_wd_cb(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("📨 Введите новое значение Min. withdraw для вашей ссылки:")
    await state.set_state(WorkerServiceStates.waiting_min_wd)
    await callback.answer()


@dp.message(WorkerServiceStates.waiting_min_wd)
async def ws_min_wd_set(message: Message, state: FSMContext):
    try:
        value = float(message.text.replace(",", "."))
        if value < 0:
            raise ValueError
    except ValueError:
        await message.answer("❗ Введите неотрицательное число.")
        return
    await set_worker_setting(message.from_user.id, "min_withdraw", value)
    await message.answer(f"✅ Min. withdraw установлен: {value}")
    await state.clear()

# ---------- ADMIN PANEL ----------

@dp.callback_query(F.data == "open_admin_panel")
async def open_admin_panel_cb(callback: CallbackQuery):
    if not is_admin_id(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.")
        return
    await send_admin_panel(callback.message)
    await callback.answer()


@dp.message(Command("admin"))
async def admin_cmd(message: Message):
    if not is_admin_id(message.from_user.id):
        await message.answer("⛔ Админ-панель доступна только администраторам.")
        return
    await send_admin_panel(message)


async def send_admin_panel(msg: Message):
    await msg.answer("🛠 <b>Админ-панель</b>\n\nВыберите действие:", reply_markup=admin_keyboard())

@dp.callback_query(F.data == "admin_stats")
async def on_admin_stats(callback: CallbackQuery):
    if not is_admin_id(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.")
        return
    text = await get_admin_stats_text()
    await callback.message.answer(text)
    await callback.answer()


@dp.callback_query(F.data == "admin_add_worker")
async def on_admin_add_worker(callback: CallbackQuery, state: FSMContext):
    if not is_admin_id(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.")
        return
    await callback.message.answer("👷 Отправьте ID пользователя, которому выдать роль воркера.")
    await state.set_state(AdminStates.waiting_worker_id)
    await callback.answer()


@dp.message(AdminStates.waiting_worker_id)
async def admin_set_worker(message: Message, state: FSMContext):
    if not is_admin_id(message.from_user.id):
        await message.answer("⛔ Нет доступа.")
        await state.clear()
        return
    try:
        user_id = int(message.text.strip())
    except ValueError:
        await message.answer("❗ Введите числовой ID.")
        return
    await set_worker_flag(user_id, True)
    await message.answer(f"✅ Пользователь {user_id} назначен воркером.")
    await state.clear()

@dp.callback_query(F.data == "admin_workers")
async def on_admin_workers(callback: CallbackQuery):
    if not is_admin_id(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.")
        return
    rows = await get_workers_with_ref_counts()
    if not rows:
        await callback.message.answer("Пока нет ни одного воркера.")
        await callback.answer()
        return
    text = "👷 <b>Список воркеров</b>\n\n"
    for row in rows:
        username = f"@{row['username']}" if row["username"] else (row["first_name"] or "без имени")
        text += f"• {row['tg_id']} ({username}) — рефов: <b>{row['ref_count']}</b>\n"
    await callback.message.answer(text, reply_markup=workers_list_keyboard(rows))
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_worker:"))
async def on_admin_worker_details(callback: CallbackQuery):
    if not is_admin_id(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.")
        return
    worker_id = int(callback.data.split(":", 1)[1])
    refs = await get_referrals_for_worker(worker_id)
    ref_count = len(refs)
    if ref_count == 0:
        text = f"👷 Воркер <code>{worker_id}</code> пока не привёл ни одного клиента."
    else:
        text = (
            f"👷 <b>Рефералы воркера {worker_id}</b>\n"
            f"Всего: <b>{ref_count}</b>\n\n"
        )
        for i, row in enumerate(refs, start=1):
            username = f"@{row['username']}" if row["username"] else (row["first_name"] or "без имени")
            text += f"{i}. {username} — <code>{row['client_tg_id']}</code>\n"
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️К воркерам", callback_data="admin_workers")
    kb.button(text="⬅️Админ-панель", callback_data="open_admin_panel")
    kb.adjust(1)
    await callback.message.answer(text, reply_markup=kb.as_markup())
    await callback.answer()

@dp.callback_query(F.data == "admin_payments")
async def on_admin_payments(callback: CallbackQuery):
    if not is_admin_id(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.")
        return
    crypto_url = config.crypto_bot_url or "не задана"
    trc20 = config.trc20_address or "не задан"
    card_url = config.card_pay_url or "не задана"
    card_req = config.card_requisites or "не заданы"
    support_url = config.support_url or "не задана"
    webapp_url = config.webapp_url or "не задана"
    text = (
        "💳 <b>Платёжные реквизиты</b>\n\n"
        f"💎 Ссылка на Crypto bot:\n<code>{crypto_url}</code>\n\n"
        f"👛 Адрес TRC20 USDT:\n<code>{trc20}</code>\n\n"
        f"💳 Ссылка оплаты картой:\n<code>{card_url}</code>\n\n"
        f"🏦 Реквизиты карты:\n<code>{card_req}</code>\n\n"
        f"🛟 Ссылка поддержки:\n<code>{support_url}</code>\n\n"
        f"🌐 WebApp URL:\n<code>{webapp_url}</code>\n\n"
        "Выберите, что хотите изменить:"
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="✏️Изменить ссылку Crypto bot", callback_data="admin_set_crypto")
    kb.button(text="✏️Изменить адрес TRC20", callback_data="admin_set_trc20")
    kb.button(text="✏️Изменить ссылку оплаты картой", callback_data="admin_set_card_url")
    kb.button(text="✏️Изменить реквизиты карты", callback_data="admin_set_card_req")
    kb.button(text="✏️Изменить ссылку поддержки", callback_data="admin_set_support")
    kb.button(text="✏️Изменить WebApp URL", callback_data="admin_set_webapp")
    kb.button(text="⬅️Админ-панель", callback_data="open_admin_panel")
    kb.adjust(1)
    await callback.message.answer(text, reply_markup=kb.as_markup())
    await callback.answer()


@dp.callback_query(F.data == "admin_set_crypto")
async def on_admin_set_crypto(callback: CallbackQuery, state: FSMContext):
    if not is_admin_id(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.")
        return
    await callback.message.answer("🔗 Отправьте новую ссылку на Crypto bot:")
    await state.set_state(AdminPaymentStates.waiting_crypto_url)
    await callback.answer()


@dp.callback_query(F.data == "admin_set_trc20")
async def on_admin_set_trc20(callback: CallbackQuery, state: FSMContext):
    if not is_admin_id(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.")
        return
    await callback.message.answer("👛 Отправьте новый адрес TRC20 USDT:")
    await state.set_state(AdminPaymentStates.waiting_trc20)
    await callback.answer()


@dp.callback_query(F.data == "admin_set_card_url")
async def on_admin_set_card_url(callback: CallbackQuery, state: FSMContext):
    if not is_admin_id(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.")
        return
    await callback.message.answer("💳 Отправьте ссылку на оплату банковской картой:")
    await state.set_state(AdminPaymentStates.waiting_card_url)
    await callback.answer()


@dp.callback_query(F.data == "admin_set_card_req")
async def on_admin_set_card_req(callback: CallbackQuery, state: FSMContext):
    if not is_admin_id(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.")
        return
    await callback.message.answer("🏦 Отправьте реквизиты карты (текстом):")
    await state.set_state(AdminPaymentStates.waiting_card_requisites)
    await callback.answer()


@dp.callback_query(F.data == "admin_set_support")
async def on_admin_set_support(callback: CallbackQuery, state: FSMContext):
    if not is_admin_id(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.")
        return
    await callback.message.answer("🛟 Отправьте новую ссылку поддержки:")
    await state.set_state(AdminPaymentStates.waiting_support_url)
    await callback.answer()


@dp.callback_query(F.data == "admin_set_webapp")
async def on_admin_set_webapp(callback: CallbackQuery, state: FSMContext):
    if not is_admin_id(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.")
        return
    await callback.message.answer("🌐 Отправьте новый URL WebApp:")
    await state.set_state(AdminPaymentStates.waiting_webapp_url)
    await callback.answer()


@dp.callback_query(F.data == "admin_assets")
async def on_admin_assets(callback: CallbackQuery, state: FSMContext):
    if not is_admin_id(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.")
        return
    rows = await get_ecn_assets()
    preview = ", ".join([r["name"] for r in rows[:20]])
    text = (
        f"🪙 <b>Активы ECN</b>\n\n"
        f"Всего активов: <b>{len(rows)}</b>\n"
        f"Топ-20: {preview if preview else 'нет'}\n\n"
        "Отправьте название нового актива одним сообщением."
    )
    await state.set_state(AdminPaymentStates.waiting_asset_name)
    await callback.message.answer(text)
    await callback.answer()

@dp.message(AdminPaymentStates.waiting_crypto_url)
async def admin_save_crypto_url(message: Message, state: FSMContext):
    if not is_admin_id(message.from_user.id):
        await message.answer("⛔ Нет доступа.")
        await state.clear()
        return
    new_url = message.text.strip()
    if not (new_url.startswith("http://") or new_url.startswith("https://")):
        await message.answer("❗ Похоже, это не ссылка. Отправьте корректный URL.")
        return
    await set_setting("crypto_bot_url", new_url)
    await message.answer(
        "✅ Ссылка на Crypto bot обновлена.\n"
        f"Текущее значение:\n<code>{new_url}</code>"
    )
    await state.clear()


@dp.message(AdminPaymentStates.waiting_trc20)
async def admin_save_trc20(message: Message, state: FSMContext):
    if not is_admin_id(message.from_user.id):
        await message.answer("⛔ Нет доступа.")
        await state.clear()
        return
    new_addr = message.text.strip()
    if len(new_addr) < 10:
        await message.answer("❗ Слишком короткий адрес. Отправьте корректный TRC20-адрес.")
        return
    await set_setting("trc20_address", new_addr)
    await message.answer(
        "✅ Адрес TRC20 USDT обновлён.\n"
        f"Текущее значение:\n<code>{new_addr}</code>"
    )
    await state.clear()


@dp.message(AdminPaymentStates.waiting_card_url)
async def admin_save_card_url(message: Message, state: FSMContext):
    if not is_admin_id(message.from_user.id):
        await message.answer("⛔ Нет доступа.")
        await state.clear()
        return
    new_url = message.text.strip()
    if not (new_url.startswith("http://") or new_url.startswith("https://")):
        await message.answer("❗ Отправьте корректный URL.")
        return
    await set_setting("card_pay_url", new_url)
    await message.answer(f"✅ Ссылка оплаты картой обновлена:\n<code>{new_url}</code>")
    await state.clear()


@dp.message(AdminPaymentStates.waiting_card_requisites)
async def admin_save_card_requisites(message: Message, state: FSMContext):
    if not is_admin_id(message.from_user.id):
        await message.answer("⛔ Нет доступа.")
        await state.clear()
        return
    value = message.text.strip()
    if len(value) < 8:
        await message.answer("❗ Слишком короткие реквизиты.")
        return
    await set_setting("card_requisites", value)
    await message.answer("✅ Реквизиты карты обновлены.")
    await state.clear()


@dp.message(AdminPaymentStates.waiting_support_url)
async def admin_save_support_url(message: Message, state: FSMContext):
    if not is_admin_id(message.from_user.id):
        await message.answer("⛔ Нет доступа.")
        await state.clear()
        return
    new_url = message.text.strip()
    if not (new_url.startswith("http://") or new_url.startswith("https://") or new_url.startswith("t.me/")):
        await message.answer("❗ Отправьте корректную ссылку поддержки.")
        return
    if new_url.startswith("t.me/"):
        new_url = f"https://{new_url}"
    await set_setting("support_url", new_url)
    await message.answer(f"✅ Ссылка поддержки обновлена:\n<code>{new_url}</code>")
    await state.clear()


@dp.message(AdminPaymentStates.waiting_webapp_url)
async def admin_save_webapp_url(message: Message, state: FSMContext):
    if not is_admin_id(message.from_user.id):
        await message.answer("⛔ Нет доступа.")
        await state.clear()
        return
    new_url = message.text.strip().rstrip("/")
    if not (new_url.startswith("http://") or new_url.startswith("https://")):
        await message.answer("❗ Отправьте корректный URL WebApp.")
        return
    await set_setting("webapp_url", new_url)
    await message.answer(f"✅ WebApp URL обновлён:\n<code>{new_url}</code>")
    await state.clear()


@dp.message(AdminPaymentStates.waiting_asset_name)
async def admin_add_asset_from_text(message: Message, state: FSMContext):
    if not is_admin_id(message.from_user.id):
        await message.answer("⛔ Нет доступа.")
        await state.clear()
        return
    name = message.text.strip()
    if len(name) < 2:
        await message.answer("❗ Слишком короткое название актива.")
        return
    await add_ecn_asset(name)
    await message.answer(f"✅ Актив добавлен (или уже существует): <b>{name}</b>")
    await state.clear()

# ---------- WORKER ↔ CLIENT DIALOG ----------

@dp.callback_query(F.data.startswith("wc_chat_start:"))
async def wc_chat_start(callback: CallbackQuery):
    wc_id = int(callback.data.split(":", 1)[1])
    row = await get_worker_client_by_id(wc_id)
    if not row:
        await callback.answer("Лохматый не найден.")
        return
    client_id = row["client_tg_id"]
    worker_id = callback.from_user.id
    ACTIVE_DIALOGS_CLIENT[client_id] = worker_id
    ACTIVE_DIALOGS_WORKER[worker_id] = client_id
    await log_activity_for_worker_client(
        client_tg_id=int(client_id),
        actor_tg_id=worker_id,
        actor_source="bot",
        event_type="bot_support_chat_started",
        title="Диалог с поддержкой",
        details="Воркер открыл диалог с рефералом в Telegram-боте.",
        meta={"wc_id": wc_id},
    )
    ticket = await get_latest_open_support_ticket(int(client_id))
    if ticket:
        await update_support_ticket_status(int(ticket["id"]), "in_progress", assigned_to=str(worker_id), last_message="Диалог начат в Telegram-боте")
    else:
        await create_support_ticket(
            client_tg_id=int(client_id),
            worker_tg_id=worker_id,
            source="bot",
            topic="general",
            subject="Диалог с поддержкой",
            status="in_progress",
            last_message="Диалог начат в Telegram-боте",
            meta={"wc_id": wc_id},
        )
    kb = InlineKeyboardBuilder()
    kb.button(text="⏹ Завершить диалог", callback_data=f"wc_chat_stop:{client_id}")
    kb.adjust(1)
    await callback.message.answer(
        f"💬 Диалог с лохматым <code>{client_id}</code> начат. Пишите сообщения, "
        "они будут отправляться клиенту.",
        reply_markup=kb.as_markup(),
    )
    try:
        await bot.send_message(
            client_id,
            "💬 С вами связалась техническая поддержка. Можете писать свои вопросы в этот чат.",
        )
    except Exception:
        pass
    await callback.answer()


@dp.callback_query(F.data.startswith("wc_chat_stop:"))
async def wc_chat_stop(callback: CallbackQuery):
    client_id = int(callback.data.split(":", 1)[1])
    worker_id = callback.from_user.id
    ACTIVE_DIALOGS_CLIENT.pop(client_id, None)
    ACTIVE_DIALOGS_WORKER.pop(worker_id, None)
    await log_activity_for_worker_client(
        client_tg_id=client_id,
        actor_tg_id=worker_id,
        actor_source="bot",
        event_type="bot_support_chat_stopped",
        title="Диалог с поддержкой",
        details="Воркер завершил диалог с рефералом в Telegram-боте.",
    )
    ticket = await get_latest_open_support_ticket(client_id)
    if ticket:
        await update_support_ticket_status(int(ticket["id"]), "closed", assigned_to=str(worker_id), last_message="Диалог завершён в Telegram-боте")
    await callback.message.answer("⏹ Диалог с клиентом завершён.")
    try:
        await bot.send_message(client_id, "⏹ Диалог с технической поддержкой завершён.")
    except Exception:
        pass
    await callback.answer()

# ---------- DIALOG ROUTER (must be last message handler) ----------

@dp.message()
async def dialog_router(message: Message):
    user_id = message.from_user.id
    if user_id in ACTIVE_DIALOGS_CLIENT:
        worker_id = ACTIVE_DIALOGS_CLIENT[user_id]
        try:
            await bot.send_message(
                worker_id,
                f"💬 Сообщение от клиента <code>{user_id}</code>:\n{message.text}",
            )
        except Exception:
            pass
        return
    if user_id in ACTIVE_DIALOGS_WORKER:
        client_id = ACTIVE_DIALOGS_WORKER[user_id]
        try:
            await bot.send_message(
                client_id,
                f"💬 Ответ поддержки:\n{message.text}",
            )
        except Exception:
            pass
        return

# ================== RUN ==================

async def main():
    global BOT_USERNAME
    await init_db()
    me = await bot.get_me()
    BOT_USERNAME = me.username
    await dp.start_polling(bot)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
