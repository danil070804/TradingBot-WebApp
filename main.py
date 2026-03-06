from pathlib import Path
import sqlite3

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = (BASE_DIR / ".." / "bot.db").resolve()

app = FastAPI(title="Trading Bot WebApp")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


def fetch_all(query: str, params: tuple = ()) -> list[sqlite3.Row]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(query, params)
        return cur.fetchall()


def fetch_one(query: str, params: tuple = ()) -> sqlite3.Row | None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(query, params)
        return cur.fetchone()


def get_overview() -> dict:
    users = fetch_one("SELECT COUNT(*) AS c FROM users")
    workers = fetch_one("SELECT COUNT(*) AS c FROM users WHERE is_worker = 1")
    balance = fetch_one("SELECT IFNULL(SUM(balance), 0) AS s FROM users")
    deals = fetch_one("SELECT COUNT(*) AS c FROM deals")
    pnl = fetch_one("SELECT IFNULL(SUM(profit), 0) AS s FROM deals")
    withdrawals_pending = fetch_one("SELECT COUNT(*) AS c FROM withdrawals WHERE status = 'pending'")
    withdrawals_total = fetch_one("SELECT IFNULL(SUM(amount), 0) AS s FROM withdrawals")

    recent_deals = fetch_all(
        """SELECT asset_name, direction, amount, currency, is_win, profit, created_at
           FROM deals
           ORDER BY id DESC
           LIMIT 8"""
    )
    recent_withdrawals = fetch_all(
        """SELECT id, user_tg_id, amount, currency, method, status, created_at
           FROM withdrawals
           ORDER BY id DESC
           LIMIT 8"""
    )

    return {
        "users_count": int(users["c"] if users else 0),
        "workers_count": int(workers["c"] if workers else 0),
        "total_balance": float(balance["s"] if balance else 0),
        "deals_count": int(deals["c"] if deals else 0),
        "total_pnl": float(pnl["s"] if pnl else 0),
        "pending_withdrawals_count": int(withdrawals_pending["c"] if withdrawals_pending else 0),
        "withdrawals_total_amount": float(withdrawals_total["s"] if withdrawals_total else 0),
        "recent_deals": [dict(x) for x in recent_deals],
        "recent_withdrawals": [dict(x) for x in recent_withdrawals],
    }


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "overview": get_overview(), "active_page": "dashboard"},
    )


@app.get("/users", response_class=HTMLResponse)
async def users_page(request: Request):
    users = fetch_all(
        """SELECT tg_id, first_name, username, language, currency, balance, is_worker, created_at
           FROM users
           ORDER BY id DESC"""
    )
    return templates.TemplateResponse(
        "users.html", {"request": request, "users": users, "active_page": "users"}
    )


@app.get("/deals", response_class=HTMLResponse)
async def deals_page(request: Request):
    deals = fetch_all(
        """SELECT id, user_tg_id, asset_name, direction, amount, currency, is_win, profit, created_at
           FROM deals
           ORDER BY id DESC
           LIMIT 300"""
    )
    return templates.TemplateResponse(
        "deals.html", {"request": request, "deals": deals, "active_page": "deals"}
    )


@app.get("/withdrawals", response_class=HTMLResponse)
async def withdrawals_page(request: Request):
    withdrawals = fetch_all(
        """SELECT id, user_tg_id, amount, currency, method, details, status, created_at
           FROM withdrawals
           ORDER BY id DESC
           LIMIT 300"""
    )
    return templates.TemplateResponse(
        "withdrawals.html",
        {"request": request, "withdrawals": withdrawals, "active_page": "withdrawals"},
    )


@app.get("/api/overview", response_class=JSONResponse)
async def api_overview():
    return JSONResponse(get_overview())

