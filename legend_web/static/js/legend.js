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
                    result.innerHTML =
                        `${L("js_trade_done", "Deal completed")}: <span class="${cls}">${status.profit > 0 ? "+" : ""}${status.profit}</span><br>` +
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
    const chips = document.querySelectorAll(".chip");
    chips.forEach((chip) => {
        chip.addEventListener("click", () => {
            if (!amountInput) return;
            amountInput.value = chip.dataset.amt;
        });
    });

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

function bindLiveOrderBook() {
    const asksWrap = document.getElementById("orderbook-asks");
    const bidsWrap = document.getElementById("orderbook-bids");
    const markEl = document.getElementById("orderbook-mark");
    const pairSelect = document.querySelector('select[name="asset_name"]');
    if (!asksWrap || !bidsWrap || !markEl) return;

    const seedBySymbol = (symbol) => {
        const s = (symbol || "BTC").toUpperCase();
        let h = 0;
        for (let i = 0; i < s.length; i += 1) h = (h * 31 + s.charCodeAt(i)) % 100000;
        return 100 + h * 7.3;
    };

    let mark = seedBySymbol(pairSelect ? pairSelect.value : "BTC");

    const draw = () => {
        mark += (Math.random() - 0.5) * Math.max(1, mark * 0.0009);
        mark = Math.max(0.0001, mark);
        markEl.textContent = mark.toFixed(mark >= 100 ? 2 : 5);
        const asks = [];
        const bids = [];
        for (let i = 0; i < 10; i += 1) {
            const spread = (i + 1) * (mark * 0.00035);
            const askPrice = mark + spread + (Math.random() * spread * 0.4);
            const bidPrice = mark - spread - (Math.random() * spread * 0.4);
            const qtyA = (Math.random() * (4 + i * 0.2) + 0.06).toFixed(3);
            const qtyB = (Math.random() * (4 + i * 0.2) + 0.06).toFixed(3);
            asks.push(`<div class="book-row ask"><span>${askPrice.toFixed(askPrice >= 100 ? 2 : 5)}</span><em>${qtyA}</em></div>`);
            bids.push(`<div class="book-row bid"><span>${bidPrice.toFixed(bidPrice >= 100 ? 2 : 5)}</span><em>${qtyB}</em></div>`);
        }
        asksWrap.innerHTML = asks.join("");
        bidsWrap.innerHTML = bids.join("");
    };

    if (pairSelect) {
        pairSelect.addEventListener("change", () => {
            mark = seedBySymbol(pairSelect.value);
            draw();
        });
    }
    draw();
    setInterval(draw, 900);
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
bindLiveOrderBook();
refreshTape();
setInterval(refreshTape, 2200);
