import asyncio
import contextlib
import json
import re
import time
from collections import defaultdict, deque
from typing import Any

import aiohttp


TF_TO_INTERVAL = {
    60: "1m",
    300: "5m",
    900: "15m",
    3600: "1h",
}
INTERVAL_TO_TF = {v: k for k, v in TF_TO_INTERVAL.items()}
TRACKED_INTERVALS = ("1m", "5m", "15m", "1h")

ASSET_ALIAS_TO_TICKER = {
    "bitcoin": "BTCUSDT",
    "btc": "BTCUSDT",
    "ethereum": "ETHUSDT",
    "eth": "ETHUSDT",
    "solana": "SOLUSDT",
    "sol": "SOLUSDT",
    "ripple": "XRPUSDT",
    "xrp": "XRPUSDT",
    "dogecoin": "DOGEUSDT",
    "doge": "DOGEUSDT",
    "toncoin": "TONUSDT",
    "ton": "TONUSDT",
    "cardano": "ADAUSDT",
    "ada": "ADAUSDT",
    "avalanche": "AVAXUSDT",
    "avax": "AVAXUSDT",
    "chainlink": "LINKUSDT",
    "link": "LINKUSDT",
    "tron": "TRXUSDT",
    "trx": "TRXUSDT",
    "polygon": "MATICUSDT",
    "matic": "MATICUSDT",
    "polkadot": "DOTUSDT",
    "dot": "DOTUSDT",
    "litecoin": "LTCUSDT",
    "ltc": "LTCUSDT",
}
SUPPORTED_BASES = {t.removesuffix("USDT") for t in ASSET_ALIAS_TO_TICKER.values()}


def _norm_asset(text: str) -> str:
    value = (text or "").strip().lower()
    value = value.replace("-", " ").replace("_", " ")
    value = re.sub(r"\s+", " ", value)
    return value


def asset_to_binance_ticker(asset_name: str) -> str:
    raw = (asset_name or "").strip()
    if not raw:
        return "BTCUSDT"
    up = raw.upper()
    if up.endswith("USDT"):
        return up

    norm = _norm_asset(raw)
    if norm in ASSET_ALIAS_TO_TICKER:
        return ASSET_ALIAS_TO_TICKER[norm]

    compact = re.sub(r"[^A-Za-z0-9]", "", raw).upper()
    if compact in SUPPORTED_BASES:
        return f"{compact}USDT"
    if compact in ASSET_ALIAS_TO_TICKER:
        return ASSET_ALIAS_TO_TICKER[compact]
    # Keep deterministic fallback ticker for unknown assets so app remains operational.
    return "BTCUSDT"


def tf_to_interval(tf_sec: int) -> str:
    return TF_TO_INTERVAL.get(int(tf_sec), "1m")


def interval_to_tf(interval: str) -> int:
    return INTERVAL_TO_TF.get((interval or "").strip(), 60)


def ticker_to_symbol(ticker: str) -> str:
    up = (ticker or "").upper()
    return up[:-4] if up.endswith("USDT") else up


def calculate_trade_profit(amount: float, leverage: int, direction: str, start_price: float, end_price: float) -> float:
    if amount <= 0:
        return 0.0
    if start_price <= 0 or end_price <= 0:
        return -amount
    move = (end_price - start_price) / start_price
    signed_move = move if direction == "up" else -move
    gross = amount * max(1, int(leverage)) * signed_move
    return max(-amount, float(gross))


class BinanceMarketService:
    REST_BASE = "https://api.binance.com"
    WS_BASE = "wss://stream.binance.com:9443/stream"

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None
        self._tasks: list[asyncio.Task] = []
        self._running = False

        self.tickers: set[str] = set()
        self.asset_to_ticker: dict[str, str] = {}

        self.quote_cache: dict[str, dict[str, Any]] = {}
        self.depth_cache: dict[str, dict[str, Any]] = {}
        self.candle_cache: dict[str, dict[str, deque]] = defaultdict(lambda: defaultdict(lambda: deque(maxlen=2000)))
        self.tape: deque = deque(maxlen=240)

    @property
    def running(self) -> bool:
        return self._running

    async def configure_assets(self, assets: list[str]):
        mapping: dict[str, str] = {}
        symbols: set[str] = set()
        for name in assets:
            ticker = asset_to_binance_ticker(name)
            mapping[name] = ticker
            if ticker_to_symbol(ticker) in SUPPORTED_BASES:
                symbols.add(ticker)
        if not symbols:
            symbols = {"BTCUSDT"}
        self.asset_to_ticker = mapping
        self.tickers = symbols

    def resolve_ticker(self, symbol_or_asset: str) -> str:
        raw = (symbol_or_asset or "").strip()
        if not raw:
            return "BTCUSDT"
        up = raw.upper()
        if up.endswith("USDT"):
            return up
        if raw in self.asset_to_ticker:
            return self.asset_to_ticker[raw]
        return asset_to_binance_ticker(raw)

    async def start(self):
        if self._running:
            return
        self._running = True
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=8, connect=2, sock_connect=2, sock_read=4)
        )
        await self._refresh_24h_stats()
        self._tasks = [
            asyncio.create_task(self._ws_loop()),
            asyncio.create_task(self._stats_loop()),
        ]

    async def stop(self):
        self._running = False
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()
        if self._session:
            await self._session.close()
            self._session = None

    async def _fetch_json(self, path: str, params: dict[str, Any] | None = None, max_wait: float | None = None) -> Any:
        if not self._session:
            return None
        url = f"{self.REST_BASE}{path}"
        try:
            req_timeout = aiohttp.ClientTimeout(total=float(max_wait)) if max_wait and max_wait > 0 else None
            async with self._session.get(url, params=params, timeout=req_timeout) as resp:
                if resp.status != 200:
                    return None
                return await resp.json()
        except Exception:
            return None

    async def _stats_loop(self):
        while self._running:
            await self._refresh_24h_stats()
            await asyncio.sleep(25)

    async def _refresh_24h_stats(self):
        if not self.tickers:
            return
        now = int(time.time())
        for ticker in sorted(self.tickers):
            row = await self._fetch_json("/api/v3/ticker/24hr", {"symbol": ticker}, max_wait=1.6)
            if not isinstance(row, dict):
                continue
            bid = float(row.get("bidPrice") or 0)
            ask = float(row.get("askPrice") or 0)
            mark = float(row.get("lastPrice") or 0)
            spread = max(0.0, ask - bid) if bid > 0 and ask > 0 else 0.0
            quote = self.quote_cache.get(ticker, {})
            quote.update(
                {
                    "symbol": ticker,
                    "mark": mark,
                    "bid": bid,
                    "ask": ask,
                    "spread": spread,
                    "high": float(row.get("highPrice") or mark),
                    "low": float(row.get("lowPrice") or mark),
                    "day_change": float(row.get("priceChangePercent") or 0),
                    "ts": now,
                }
            )
            self.quote_cache[ticker] = quote

    def _upsert_ws_kline(self, ticker: str, interval: str, payload: dict[str, Any]):
        if interval not in TRACKED_INTERVALS:
            return
        series = self.candle_cache[ticker][interval]
        ts = int(payload.get("t", 0)) // 1000
        bar = {
            "t": ts,
            "o": float(payload.get("o") or 0),
            "h": float(payload.get("h") or 0),
            "l": float(payload.get("l") or 0),
            "c": float(payload.get("c") or 0),
            "v": float(payload.get("v") or 0),
        }
        if series and series[-1]["t"] == ts:
            series[-1] = bar
        else:
            series.append(bar)

    def _upsert_ws_trade(self, payload: dict[str, Any]):
        ticker = str(payload.get("s") or "").upper()
        if not ticker:
            return
        price = float(payload.get("p") or 0)
        qty = float(payload.get("q") or 0)
        maker = bool(payload.get("m"))
        side = "sell" if maker else "buy"
        trade = {
            "symbol": ticker_to_symbol(ticker),
            "price": round(price, 5 if price < 1 else 2),
            "qty": round(qty, 4),
            "side": side,
            "ts": int(payload.get("T") or time.time() * 1000) // 1000,
        }
        self.tape.appendleft(trade)
        quote = self.quote_cache.get(ticker, {})
        quote["mark"] = price
        quote["ts"] = int(time.time())
        self.quote_cache[ticker] = quote

    def _upsert_ws_book(self, payload: dict[str, Any]):
        ticker = str(payload.get("s") or "").upper()
        if not ticker:
            return
        bid = float(payload.get("b") or 0)
        ask = float(payload.get("a") or 0)
        if bid <= 0 or ask <= 0:
            return
        mark = (bid + ask) / 2.0
        quote = self.quote_cache.get(ticker, {})
        quote.update(
            {
                "symbol": ticker,
                "mark": mark,
                "bid": bid,
                "ask": ask,
                "spread": max(0.0, ask - bid),
                "high": quote.get("high", mark),
                "low": quote.get("low", mark),
                "day_change": quote.get("day_change", 0.0),
                "ts": int(time.time()),
            }
        )
        self.quote_cache[ticker] = quote

    async def _ws_loop(self):
        while self._running:
            if not self.tickers:
                await asyncio.sleep(1)
                continue
            streams: list[str] = []
            for ticker in sorted(self.tickers):
                lower = ticker.lower()
                streams.append(f"{lower}@trade")
                streams.append(f"{lower}@bookTicker")
                for interval in TRACKED_INTERVALS:
                    streams.append(f"{lower}@kline_{interval}")
            url = f"{self.WS_BASE}?streams={'/'.join(streams)}"
            try:
                if not self._session:
                    await asyncio.sleep(2)
                    continue
                async with self._session.ws_connect(url, heartbeat=25) as ws:
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        try:
                            payload = json.loads(msg.data)
                        except Exception:
                            continue
                        data = payload.get("data") if isinstance(payload, dict) else None
                        if not isinstance(data, dict):
                            continue
                        event = data.get("e")
                        if event == "trade":
                            self._upsert_ws_trade(data)
                        elif event == "bookTicker":
                            self._upsert_ws_book(data)
                        elif event == "kline":
                            ticker = str(data.get("s") or "").upper()
                            kline = data.get("k") if isinstance(data.get("k"), dict) else {}
                            interval = str(kline.get("i") or "")
                            if ticker and interval:
                                self._upsert_ws_kline(ticker, interval, kline)
            except Exception:
                await asyncio.sleep(2)

    async def ensure_depth(self, ticker: str, levels: int = 10, max_age: float = 2.0):
        now = time.time()
        cached = self.depth_cache.get(ticker)
        if cached and now - float(cached.get("ts", 0)) < max_age:
            return
        payload = await self._fetch_json(
            "/api/v3/depth",
            {"symbol": ticker, "limit": max(5, min(20, levels))},
            max_wait=1.8,
        )
        if not isinstance(payload, dict):
            return
        asks_raw = payload.get("asks") if isinstance(payload.get("asks"), list) else []
        bids_raw = payload.get("bids") if isinstance(payload.get("bids"), list) else []
        asks = [{"price": float(p), "qty": float(q)} for p, q in asks_raw[:levels]]
        bids = [{"price": float(p), "qty": float(q)} for p, q in bids_raw[:levels]]
        self.depth_cache[ticker] = {"asks": asks, "bids": bids, "ts": now}
        if asks and bids:
            bid = bids[0]["price"]
            ask = asks[0]["price"]
            mark = (bid + ask) / 2.0
            quote = self.quote_cache.get(ticker, {})
            quote.update(
                {
                    "symbol": ticker,
                    "mark": mark,
                    "bid": bid,
                    "ask": ask,
                    "spread": max(0.0, ask - bid),
                    "high": quote.get("high", mark),
                    "low": quote.get("low", mark),
                    "day_change": quote.get("day_change", 0.0),
                    "ts": int(now),
                }
            )
            self.quote_cache[ticker] = quote

    async def ensure_candles(self, ticker: str, tf_sec: int, limit: int = 300):
        interval = tf_to_interval(tf_sec)
        series = self.candle_cache[ticker][interval]
        if len(series) >= min(300, max(20, limit)):
            return
        payload = await self._fetch_json(
            "/api/v3/klines",
            {"symbol": ticker, "interval": interval, "limit": max(20, min(1000, int(limit)))},
            max_wait=2.2,
        )
        if not isinstance(payload, list):
            return
        series.clear()
        for row in payload:
            if not isinstance(row, list) or len(row) < 6:
                continue
            series.append(
                {
                    "t": int(row[0]) // 1000,
                    "o": float(row[1]),
                    "h": float(row[2]),
                    "l": float(row[3]),
                    "c": float(row[4]),
                    "v": float(row[5]),
                }
            )

    def get_quote(self, ticker: str) -> dict[str, Any] | None:
        return self.quote_cache.get(ticker)

    def get_depth(self, ticker: str, levels: int = 10) -> tuple[list[dict], list[dict]]:
        cache = self.depth_cache.get(ticker) or {}
        asks_raw = cache.get("asks") if isinstance(cache.get("asks"), list) else []
        bids_raw = cache.get("bids") if isinstance(cache.get("bids"), list) else []
        asks = [
            {"price": round(float(x["price"]), 5 if float(x["price"]) < 1 else 2), "qty": round(float(x["qty"]), 4)}
            for x in asks_raw[:levels]
        ]
        bids = [
            {"price": round(float(x["price"]), 5 if float(x["price"]) < 1 else 2), "qty": round(float(x["qty"]), 4)}
            for x in bids_raw[:levels]
        ]
        return asks, bids

    def get_candles(self, ticker: str, tf_sec: int, limit: int = 300) -> list[dict]:
        interval = tf_to_interval(tf_sec)
        series = self.candle_cache[ticker][interval]
        data = list(series)[-max(20, min(1000, int(limit))):]
        return [
            {
                "t": int(c["t"]),
                "o": round(float(c["o"]), 6),
                "h": round(float(c["h"]), 6),
                "l": round(float(c["l"]), 6),
                "c": round(float(c["c"]), 6),
                "v": round(float(c.get("v", 0.0)), 4),
            }
            for c in data
        ]
