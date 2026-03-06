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
    if (!form || !result) return;
    form.addEventListener("submit", async (e) => {
        e.preventDefault();
        result.textContent = "Открываем сделку...";
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
                result.innerHTML = `<span class="neg">Ошибка: ${data.error || "не удалось открыть сделку"}</span>`;
                return;
            }
            const cls = data.is_win ? "pos" : "neg";
            result.innerHTML =
                `Сделка завершена: <span class="${cls}">${data.profit > 0 ? "+" : ""}${data.profit}</span><br>` +
                `Новый баланс: ${data.balance}<br>` +
                `Курс: ${data.start_price} -> ${data.end_price}`;
        } catch (_) {
            result.innerHTML = `<span class="neg">Сетевая ошибка</span>`;
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
        result.textContent = "Обрабатываем обмен...";
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
                result.innerHTML = `<span class="neg">Ошибка: ${data.error || "не удалось обменять"}</span>`;
                return;
            }
            result.innerHTML = `Курс: ${data.rate}<br>Получено: <span class="pos">${data.received} ${data.to}</span>`;
        } catch (_) {
            result.innerHTML = `<span class="neg">Сетевая ошибка</span>`;
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
        result.textContent = "Обрабатываем...";
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
                result.innerHTML = `<span class="neg">Ошибка: ${data.error || "не удалось отправить"}</span>`;
                return;
            }
            if (data.requires_support) {
                result.innerHTML = `<span class="pos">${data.message}</span>`;
                if (data.support_url) {
                    window.open(data.support_url, "_blank");
                }
                return;
            }
            result.innerHTML = `<span class="pos">Заявка #${data.deposit_id} отправлена админу</span>`;
        } catch (_) {
            result.innerHTML = `<span class="neg">Сетевая ошибка</span>`;
        }
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

function renderTape(items) {
    const wrap = document.getElementById("market-tape-list");
    if (!wrap) return;
    wrap.innerHTML = "";
    items.forEach((item) => {
        const row = document.createElement("div");
        row.className = "row mono";
        const sideClass = item.side === "buy" ? "pos" : "neg";
        row.innerHTML = `<div><b>${item.symbol}</b><small class="${sideClass}">${item.side.toUpperCase()}</small></div><div>${item.price} • ${item.qty}</div>`;
        wrap.appendChild(row);
    });
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
refreshTape();
setInterval(refreshTape, 2200);
