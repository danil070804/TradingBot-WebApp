const LEGEND_LABELS = window.LEGEND_LABELS || {};
const L = (key, fallback) => LEGEND_LABELS[key] || fallback;

function bindDirectionButtons() {
    const buttons = document.querySelectorAll(".dir-btn");
    const hidden = document.getElementById("direction-input");
    if (!buttons.length || !hidden) return;
    buttons.forEach((btn) => {
        btn.addEventListener("click", () => {
            buttons.forEach((b) => b.classList.remove("active"));
            btn.classList.add("active");
            hidden.value = btn.dataset.dir;
        });
    });
}

function bindTradeForm() {
    const form = document.getElementById("trade-form");
    const result = document.getElementById("trade-result");
    const timer = document.getElementById("trade-timer");
    const progress = document.getElementById("trade-progress");
    if (!form || !result) return;
    form.addEventListener("submit", async (e) => {
        e.preventDefault();
        result.textContent = L("js_trade_opening", "Opening trade...");
        const body = Object.fromEntries(new FormData(form).entries());
        body.tg_id = Number(body.tg_id);
        body.amount = Number(body.amount);
        body.seconds = Number(body.seconds);
        body.leverage = Number(body.leverage);
        body.risk_percent = Number(body.risk_percent || 0);
        body.tp_percent = Number(body.tp_percent || 0);
        body.sl_percent = Number(body.sl_percent || 0);
        try {
            const resp = await fetch("/api/trade/open", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(body),
            });
            const data = await resp.json();
            if (!resp.ok || !data.ok) {
                result.innerHTML = `<span class="neg">Error: ${data.error || L("js_trade_error", "failed to open trade")}</span>`;
                return;
            }
            result.innerHTML = `<span class="pos">${L("js_trade_started", "Trade opened, countdown started")}</span>`;
            const started = Math.floor(Date.now() / 1000);
            const closeAt = Number(data.close_at);
            const total = Math.max(1, Number(data.seconds || 1));
            const updateTimerUi = (remaining) => {
                if (timer) timer.textContent = `${L("js_trade_waiting", "Time left")}: ${remaining}s`;
                if (progress) {
                    const done = Math.min(100, Math.max(0, ((total - remaining) / total) * 100));
                    progress.style.width = `${done}%`;
                }
            };
            updateTimerUi(total);

            const poll = async () => {
                const now = Math.floor(Date.now() / 1000);
                updateTimerUi(Math.max(0, closeAt - now));
                const statusResp = await fetch(`/api/trade/status?trade_id=${encodeURIComponent(data.trade_id)}&tg_id=${body.tg_id}`);
                const status = await statusResp.json();
                if (!statusResp.ok || !status.ok) {
                    result.innerHTML = `<span class="neg">Error: ${status.error || L("js_trade_error", "failed to open trade")}</span>`;
                    return true;
                }
                if (status.status === "closed") {
                    const cls = status.is_win ? "pos" : "neg";
                    const reasonMap = { tp: "TP", sl: "SL", time: "TIME" };
                    const reason = reasonMap[status.close_reason] || "TIME";
                    result.innerHTML =
                        `${L("js_trade_done", "Deal completed")}: <span class="${cls}">${status.profit > 0 ? "+" : ""}${status.profit}</span><br>` +
                        `Reason: ${reason}<br>` +
                        `${L("js_trade_balance", "New balance")}: ${status.balance}<br>` +
                        `${L("js_trade_rate", "Rate")}: ${status.start_price} -> ${status.end_price}`;
                    updateTimerUi(0);
                    return true;
                }
                return false;
            };

            let done = false;
            while (!done) {
                done = await poll();
                if (!done) {
                    await new Promise((resolve) => setTimeout(resolve, 1000));
                }
            }
        } catch (_) {
            result.innerHTML = `<span class="neg">${L("js_network_error", "Network error")}</span>`;
        }
    });
}

function bindTradeControls() {
    const amountInput = document.getElementById("trade-amount-input");
    const riskInput = document.getElementById("risk-input");
    const balanceRaw = document.getElementById("balance-raw");
    const chips = document.querySelectorAll(".chip");
    chips.forEach((chip) => {
        chip.addEventListener("click", () => {
            if (!amountInput) return;
            amountInput.value = chip.dataset.amt;
        });
    });
    const riskChips = document.querySelectorAll(".risk-chip");
    riskChips.forEach((chip) => {
        chip.addEventListener("click", () => {
            if (!riskInput || !amountInput || !balanceRaw) return;
            const rp = Number(chip.dataset.risk || 0);
            const bal = Number(balanceRaw.value || 0);
            riskInput.value = rp.toString();
            const amt = ((bal * rp) / 100).toFixed(2);
            amountInput.value = amt;
        });
    });

    if (riskInput && amountInput && balanceRaw) {
        riskInput.addEventListener("input", () => {
            const rp = Number(riskInput.value || 0);
            const bal = Number(balanceRaw.value || 0);
            if (rp > 0) amountInput.value = ((bal * rp) / 100).toFixed(2);
        });
    }

    const levRange = document.getElementById("lev-range");
    const levVal = document.getElementById("lev-val");
    const levHidden = document.getElementById("lev-hidden");
    if (levRange && levVal && levHidden) {
        const sync = () => {
            levVal.textContent = `${levRange.value}x`;
            levHidden.value = levRange.value;
        };
        levRange.addEventListener("input", sync);
        sync();
    }
}

function bindExchangeForm() {
    const form = document.getElementById("exchange-form");
    const result = document.getElementById("exchange-result");
    if (!form || !result) return;
    form.addEventListener("submit", async (e) => {
        e.preventDefault();
        result.textContent = L("js_exchange_processing", "Processing exchange...");
        const body = Object.fromEntries(new FormData(form).entries());
        body.tg_id = Number(body.tg_id);
        body.amount = Number(body.amount);
        try {
            const resp = await fetch("/api/exchange", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(body),
            });
            const data = await resp.json();
            if (!resp.ok || !data.ok) {
                result.innerHTML = `<span class="neg">Error: ${data.error || L("js_exchange_error", "failed to exchange")}</span>`;
                return;
            }
            result.innerHTML = `${L("js_exchange_rate", "Rate")}: ${data.rate}<br>${L("js_exchange_received", "Received")}: <span class="pos">${data.received} ${data.to}</span>`;
        } catch (_) {
            result.innerHTML = `<span class="neg">${L("js_network_error", "Network error")}</span>`;
        }
    });
}

async function initTelegramAuth() {
    if (!window.Telegram || !window.Telegram.WebApp) return;
    const wa = window.Telegram.WebApp;
    wa.ready();
    wa.expand();
    if (!wa.initData) return;
    try {
        const resp = await fetch("/api/auth/telegram", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ init_data: wa.initData }),
        });
        const data = await resp.json();
        if (resp.ok && data.ok && !sessionStorage.getItem("tg_auth_done")) {
            sessionStorage.setItem("tg_auth_done", "1");
            location.reload();
        }
    } catch (_) {
        // no-op
    }
}

function bindDepositForm() {
    const form = document.getElementById("deposit-form");
    const result = document.getElementById("deposit-result");
    if (!form || !result) return;
    form.addEventListener("submit", async (e) => {
        e.preventDefault();
        result.textContent = L("js_deposit_processing", "Processing...");
        const body = Object.fromEntries(new FormData(form).entries());
        body.tg_id = Number(body.tg_id);
        body.amount = Number(body.amount);
        try {
            const resp = await fetch("/api/deposit/request", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(body),
            });
            const data = await resp.json();
            if (!resp.ok || !data.ok) {
                result.innerHTML = `<span class="neg">Error: ${data.error || L("js_deposit_error", "failed to send")}</span>`;
                return;
            }
            if (data.requires_support) {
                const button = data.support_url
                    ? `<div style="margin-top:8px"><a class="qa-btn" href="${data.support_url}" target="_blank">${L("js_support_btn", "Open Support")}</a></div>`
                    : "";
                result.innerHTML = `<span class="pos">${data.message || L("js_card_support_msg", "For bank card payment, contact support.")}</span>${button}`;
                return;
            }
            result.innerHTML = `<span class="pos">${L("js_deposit_sent", "Request #{id} sent to admin").replace("{id}", data.deposit_id)}</span>`;
        } catch (_) {
            result.innerHTML = `<span class="neg">${L("js_network_error", "Network error")}</span>`;
        }
    });
}

function buildCandleState() {
    return { tf: 30, candles: [] };
}

function pushCandle(state, price, ts) {
    const bucket = Math.floor(ts / state.tf) * state.tf;
    const last = state.candles[state.candles.length - 1];
    if (!last || last.bucket !== bucket) {
        const open = last ? last.close : price;
        state.candles.push({ bucket, open, high: price, low: price, close: price });
        if (state.candles.length > 80) state.candles.shift();
    } else {
        last.high = Math.max(last.high, price);
        last.low = Math.min(last.low, price);
        last.close = price;
    }
}

function drawCandles(canvas, candles) {
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    const w = canvas.clientWidth || canvas.width;
    const h = canvas.clientHeight || canvas.height;
    canvas.width = w;
    canvas.height = h;
    ctx.clearRect(0, 0, w, h);
    if (!candles.length) return;

    const highs = candles.map((c) => c.high);
    const lows = candles.map((c) => c.low);
    const max = Math.max(...highs);
    const min = Math.min(...lows);
    const range = Math.max(0.00001, max - min);
    const pad = 10;
    const usableH = h - pad * 2;
    const cw = Math.max(4, Math.floor((w - pad * 2) / candles.length) - 2);

    for (let i = 0; i < candles.length; i += 1) {
        const c = candles[i];
        const x = pad + i * (cw + 2);
        const yHigh = pad + ((max - c.high) / range) * usableH;
        const yLow = pad + ((max - c.low) / range) * usableH;
        const yOpen = pad + ((max - c.open) / range) * usableH;
        const yClose = pad + ((max - c.close) / range) * usableH;
        const up = c.close >= c.open;
        ctx.strokeStyle = up ? "#8fff4f" : "#ff6f6f";
        ctx.fillStyle = up ? "rgba(143,255,79,0.35)" : "rgba(255,111,111,0.35)";
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(x + cw / 2, yHigh);
        ctx.lineTo(x + cw / 2, yLow);
        ctx.stroke();
        const bodyY = Math.min(yOpen, yClose);
        const bodyH = Math.max(2, Math.abs(yClose - yOpen));
        ctx.fillRect(x, bodyY, cw, bodyH);
        ctx.strokeRect(x, bodyY, cw, bodyH);
    }
}

function bindMarketSocket() {
    const asksWrap = document.getElementById("orderbook-asks");
    const bidsWrap = document.getElementById("orderbook-bids");
    const markEl = document.getElementById("orderbook-mark");
    const pairSelect = document.querySelector('select[name="asset_name"]');
    const canvas = document.getElementById("candle-canvas");
    const tfSelect = document.getElementById("chart-tf");
    if (!asksWrap || !bidsWrap || !markEl || !pairSelect) return;

    const state = buildCandleState();
    if (tfSelect) {
        state.tf = Number(tfSelect.value || 30);
        tfSelect.addEventListener("change", () => {
            state.tf = Number(tfSelect.value || 30);
            state.candles = [];
        });
    }

    const protocol = window.location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${protocol}://${window.location.host}/ws/market`);

    const subscribe = () => {
        ws.send(JSON.stringify({ type: "subscribe", symbol: pairSelect.value || "BTC" }));
    };

    ws.addEventListener("open", () => {
        subscribe();
    });

    pairSelect.addEventListener("change", () => {
        state.candles = [];
        if (ws.readyState === WebSocket.OPEN) subscribe();
    });

    ws.addEventListener("message", (event) => {
        let data = null;
        try {
            data = JSON.parse(event.data);
        } catch (_) {
            return;
        }
        if (!data || data.type !== "market") return;

        markEl.textContent = data.mark;
        asksWrap.innerHTML = (data.asks || [])
            .map((r) => `<div class="book-row ask"><span>${r.price}</span><em>${r.qty}</em></div>`)
            .join("");
        bidsWrap.innerHTML = (data.bids || [])
            .map((r) => `<div class="book-row bid"><span>${r.price}</span><em>${r.qty}</em></div>`)
            .join("");

        pushCandle(state, Number(data.mark), Number(data.ts || Math.floor(Date.now() / 1000)));
        drawCandles(canvas, state.candles);

        if (data.tick) {
            const current = document.querySelectorAll("#market-tape-list .row");
            const wrap = document.getElementById("market-tape-list");
            if (wrap) {
                const row = document.createElement("div");
                row.className = "row mono";
                const sideClass = data.tick.side === "buy" ? "pos" : "neg";
                const sideText = data.tick.side === "buy" ? L("js_side_buy", "BUY") : L("js_side_sell", "SELL");
                row.innerHTML = `<div><b>${data.tick.symbol}</b><small class="${sideClass}">${sideText}</small></div><div>${data.tick.price} • ${data.tick.qty}</div>`;
                wrap.prepend(row);
                if (current.length > 22) current[current.length - 1].remove();
            }
        }
    });

    ws.addEventListener("close", () => {
        setTimeout(bindMarketSocket, 1200);
    });
}

function bindUserSocket() {
    const tg = document.querySelector('input[name="tg_id"]');
    if (!tg) return;
    const tgId = Number(tg.value || 0);
    if (!tgId) return;
    const balEl = document.getElementById("live-balance");
    const curEl = document.getElementById("live-currency");
    const openEl = document.getElementById("live-open-trades");
    const balanceRaw = document.getElementById("balance-raw");

    const protocol = window.location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${protocol}://${window.location.host}/ws/user`);
    ws.addEventListener("open", () => {
        ws.send(JSON.stringify({ tg_id: tgId }));
    });
    ws.addEventListener("message", (event) => {
        let data = null;
        try {
            data = JSON.parse(event.data);
        } catch (_) {
            return;
        }
        if (!data || data.type !== "user") return;
        if (balEl) balEl.textContent = Number(data.balance || 0).toFixed(2);
        if (curEl) curEl.textContent = data.currency || "USD";
        if (openEl) openEl.textContent = String(data.open_trades || 0);
        if (balanceRaw) balanceRaw.value = Number(data.balance || 0).toFixed(4);
    });
}

function bindLangSwitch() {
    const buttons = document.querySelectorAll(".lang-btn");
    if (!buttons.length) return;
    buttons.forEach((btn) => {
        btn.addEventListener("click", async () => {
            const lang = btn.dataset.lang;
            try {
                const resp = await fetch("/api/lang", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ lang }),
                });
                if (resp.ok) location.reload();
            } catch (_) {
                // no-op
            }
        });
    });
}

function bindWorkerPanel() {
    const wrap = document.getElementById("worker-list");
    if (!wrap) return;

    const doUpdate = async (wcId, action, value = null) => {
        const resp = await fetch("/api/worker/client/update", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ wc_id: Number(wcId), action, value }),
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
            alert(data.error || "Update failed");
            return false;
        }
        location.reload();
        return true;
    };

    wrap.querySelectorAll(".worker-act").forEach((btn) => {
        btn.addEventListener("click", async () => {
            const row = btn.closest(".worker-row");
            if (!row) return;
            await doUpdate(row.dataset.wcId, btn.dataset.action);
        });
    });

    wrap.querySelectorAll(".worker-prompt").forEach((btn) => {
        btn.addEventListener("click", async () => {
            const row = btn.closest(".worker-row");
            if (!row) return;
            const label = btn.dataset.label || "Value";
            const valRaw = prompt(`${label}:`);
            if (valRaw === null) return;
            const val = Number(valRaw);
            if (Number.isNaN(val)) {
                alert("Invalid number");
                return;
            }
            await doUpdate(row.dataset.wcId, btn.dataset.action, val);
        });
    });
}

function renderTape(items) {
    const wrap = document.getElementById("market-tape-list");
    const pulse = document.getElementById("market-pulse");
    if (!wrap) return;
    wrap.innerHTML = "";
    const pulseItems = [];
    items.forEach((item) => {
        const row = document.createElement("div");
        row.className = "row mono";
        const sideClass = item.side === "buy" ? "pos" : "neg";
        const sideText = item.side === "buy" ? L("js_side_buy", "BUY") : L("js_side_sell", "SELL");
        row.innerHTML = `<div><b>${item.symbol}</b><small class="${sideClass}">${sideText}</small></div><div>${item.price} • ${item.qty}</div>`;
        wrap.appendChild(row);
        pulseItems.push(`<span class="${sideClass}">${item.symbol} ${item.price}</span>`);
    });
    if (pulse) {
        pulse.innerHTML = pulseItems.concat(pulseItems).join("");
    }
}

async function refreshTape() {
    const wrap = document.getElementById("market-tape-list");
    if (!wrap) return;
    try {
        const resp = await fetch("/api/market/tape");
        if (!resp.ok) return;
        const data = await resp.json();
        if (data.ok && Array.isArray(data.items)) {
            renderTape(data.items.slice(0, 20));
        }
    } catch (_) {
        // no-op
    }
}

initTelegramAuth();
bindDirectionButtons();
bindTradeControls();
bindTradeForm();
bindExchangeForm();
bindDepositForm();
bindLangSwitch();
bindWorkerPanel();
bindMarketSocket();
bindUserSocket();
refreshTape();
setInterval(refreshTape, 2200);
