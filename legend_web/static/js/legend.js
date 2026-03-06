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

bindDirectionButtons();
bindTradeForm();
bindExchangeForm();
