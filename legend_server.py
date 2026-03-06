import asyncio
import os
import random
import contextlib
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from aiogram.types import Update
from starlette.middleware.sessions import SessionMiddleware

import bot
import db_compat as db

try:
    import asyncpg
except Exception:
    asyncpg = None


BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "legend_web"
DEFAULT_TG_ID = int(os.getenv("WEBAPP_DEFAULT_TG_ID", "0"))
RUN_BOT = os.getenv("RUN_BOT", "1") == "1"
BOT_MODE = os.getenv("BOT_MODE", "webhook").strip().lower()
POLLING_LOCK_KEY = int(os.getenv("BOT_POLLING_LOCK_KEY", "8598101146"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/telegram/webhook")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
SESSION_SECRET = os.getenv("SESSION_SECRET", WEBHOOK_SECRET or "legend_trading_session_secret")
ADMIN_WEB_USERNAME = os.getenv("ADMIN_WEB_USERNAME", "admin")
ADMIN_WEB_PASSWORD = os.getenv("ADMIN_WEB_PASSWORD", "")


polling_task: asyncio.Task | None = None
polling_lock_conn = None


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
    if DEFAULT_TG_ID:
        return DEFAULT_TG_ID
    row = await fetch_one("SELECT tg_id FROM users ORDER BY id DESC LIMIT 1")
    return int(row["tg_id"]) if row else 0


def generate_market_rows(assets) -> list[dict]:
    rows = []
    for asset in assets:
        price = round(random.uniform(1.2, 120000), 2)
        day_change = round(random.uniform(-9.5, 9.5), 2)
        rows.append(
            {
                "id": asset["id"],
                "name": asset["name"],
                "symbol": asset["name"][:4].upper(),
                "price": price,
                "day_change": day_change,
            }
        )
    return rows


@asynccontextmanager
async def lifespan(app: FastAPI):
    global polling_task
    await bot.init_db()
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


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    tg_id = await get_or_pick_user_id()
    user = await fetch_one("SELECT * FROM users WHERE tg_id = ?", (tg_id,))
    deals = await fetch_all(
        "SELECT id, asset_name, direction, amount, currency, is_win, profit, created_at FROM deals WHERE user_tg_id = ? ORDER BY id DESC LIMIT 5",
        (tg_id,),
    )
    stats = await bot.get_user_deal_stats(tg_id) if tg_id else {"wins": 0, "losses": 0, "total": 0, "total_profit": 0.0}
    assets = await fetch_all("SELECT id, name FROM ecn_assets ORDER BY id ASC")
    markets = generate_market_rows(assets)
    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "page": "home",
            "title": "Legend Trading",
            "tg_id": tg_id,
            "user": user,
            "stats": stats,
            "markets": markets[:10],
            "deals": deals,
        },
    )


@app.get("/markets", response_class=HTMLResponse)
async def markets(request: Request):
    assets = await fetch_all("SELECT id, name FROM ecn_assets ORDER BY id ASC")
    return templates.TemplateResponse(
        "markets.html",
        {"request": request, "page": "markets", "title": "Legend Trading", "markets": generate_market_rows(assets)},
    )


@app.get("/trade", response_class=HTMLResponse)
async def trade(request: Request):
    tg_id = await get_or_pick_user_id()
    assets = await fetch_all("SELECT id, name FROM ecn_assets ORDER BY id ASC")
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
        },
    )


@app.get("/exchange", response_class=HTMLResponse)
async def exchange(request: Request):
    tg_id = await get_or_pick_user_id()
    user = await fetch_one("SELECT balance, currency FROM users WHERE tg_id = ?", (tg_id,))
    return templates.TemplateResponse(
        "exchange.html",
        {"request": request, "page": "exchange", "title": "Legend Trading", "tg_id": tg_id, "user": user},
    )


@app.get("/deals", response_class=HTMLResponse)
async def deals(request: Request):
    tg_id = await get_or_pick_user_id()
    rows = await fetch_all(
        "SELECT id, asset_name, direction, amount, currency, is_win, profit, created_at FROM deals WHERE user_tg_id = ? ORDER BY id DESC LIMIT 200",
        (tg_id,),
    )
    return templates.TemplateResponse(
        "deals.html",
        {"request": request, "page": "deals", "title": "Legend Trading", "deals": rows, "tg_id": tg_id},
    )


@app.get("/profile", response_class=HTMLResponse)
async def profile_page(request: Request):
    tg_id = await get_or_pick_user_id()
    user = await fetch_one(
        "SELECT tg_id, first_name, username, language, currency, balance, created_at FROM users WHERE tg_id = ?",
        (tg_id,),
    )
    stats = await bot.get_user_deal_stats(tg_id) if tg_id else {"wins": 0, "losses": 0, "total": 0, "total_profit": 0.0}
    pending = await bot.get_user_pending_withdraw_sum(tg_id) if tg_id else 0.0
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
        },
    )


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
    return JSONResponse({"ok": True, "name": name})


class TradeOpenPayload(BaseModel):
    tg_id: int
    asset_name: str
    direction: str
    amount: float
    seconds: int
    leverage: int = 10


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

    if amount < 100:
        return JSONResponse({"ok": False, "error": "Минимальная сумма сделки: 100"}, status_code=400)

    user = await fetch_one("SELECT balance, currency FROM users WHERE tg_id = ?", (tg_id,))
    if not user:
        return JSONResponse({"ok": False, "error": "Пользователь не найден"}, status_code=404)
    balance = float(user["balance"] or 0.0)
    currency = user["currency"] or "USD"
    if amount > balance:
        return JSONResponse({"ok": False, "error": "Недостаточно средств"}, status_code=400)

    await bot.change_balance(tg_id, -amount)
    start_price = round(random.uniform(10, 100_000), 2)
    luck_percent = await bot.get_luck_percent_for_client(tg_id)
    win_prob = max(0.0, min(1.0, (luck_percent / 100.0) if luck_percent is not None else 0.5))
    is_win = random.random() < win_prob
    change_percent = random.uniform(0.1, 1.0) * max(1, leverage / 5)
    if (direction == "up" and is_win) or (direction == "down" and not is_win):
        end_price = start_price * (1 + change_percent / 100)
    else:
        end_price = start_price * (1 - change_percent / 100)

    payout_rate = 0.6
    profit = amount * payout_rate if is_win else -amount
    if is_win:
        await bot.change_balance(tg_id, amount + (amount * payout_rate))

    await bot.save_deal(
        user_tg_id=tg_id,
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
    new_balance = await bot.get_user_balance(tg_id)
    return JSONResponse(
        {
            "ok": True,
            "is_win": is_win,
            "profit": round(profit, 2),
            "balance": round(new_balance, 2),
            "start_price": start_price,
            "end_price": round(end_price, 2),
            "change_percent": round(change_percent, 3),
        }
    )


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
    return JSONResponse({"ok": True, "rate": round(rate, 4), "received": received, "tg_id": tg_id, "from": from_currency, "to": to_currency})


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
