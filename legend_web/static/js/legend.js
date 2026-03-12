const LEGEND_LABELS = window.LEGEND_LABELS || {};
const L = (key, fallback) => LEGEND_LABELS[key] || fallback;
let MARKET_SOCKET_RUNTIME = null;
let LIVE_TRADE_PANEL_ID = null;
const INITIAL_OPEN_POSITIONS = Array.isArray(window.LEGEND_INITIAL_OPEN_POSITIONS) ? window.LEGEND_INITIAL_OPEN_POSITIONS : [];
const INITIAL_TRADE_STATUS = window.LEGEND_INITIAL_TRADE_STATUS || null;
const LIVE_STATE = {
    mark: null,
    spread: null,
    high: null,
    low: null,
    mode: null,
};
const PROFILE_ACTIVITY_SEEN = new Set();

function uiLang() {
    return String(document.body?.dataset?.lang || "en").toLowerCase();
}

function emptyStateConfig(kind) {
    const lang = uiLang();
    const dict = {
        en: {
            positions: { title: "No open positions yet", text: "Open your first position from the trade form and track it live here.", cta: "Open Trade", href: "/trade" },
            tape: { title: "Tape is warming up", text: "Live prints will appear here as soon as market feed starts streaming.", cta: "Open Markets", href: "/markets" },
            activity: { title: "No live activity yet", text: "When a deal closes, the latest result will appear in this feed.", cta: "Go to Trade", href: "/trade" },
            markets: { title: "No markets for this filter", text: "Try another filter or clear search to see more instruments.", cta: "Reset Filters", href: "#reset-markets" },
        },
        ru: {
            positions: { title: "Пока нет открытых позиций", text: "Откройте первую сделку в терминале, и она появится здесь в реальном времени.", cta: "Открыть сделку", href: "/trade" },
            tape: { title: "Лента прогревается", text: "Сделки рынка появятся здесь сразу после запуска потока данных.", cta: "Открыть рынки", href: "/markets" },
            activity: { title: "Пока нет живой активности", text: "После закрытия сделки ее результат появится в этой ленте.", cta: "Перейти к торговле", href: "/trade" },
            markets: { title: "По этому фильтру ничего не найдено", text: "Смените фильтр или очистите поиск, чтобы увидеть больше инструментов.", cta: "Сбросить фильтр", href: "#reset-markets" },
        },
        uk: {
            positions: { title: "Поки немає відкритих позицій", text: "Відкрийте першу угоду в терміналі, і вона з'явиться тут у реальному часі.", cta: "Відкрити угоду", href: "/trade" },
            tape: { title: "Стрічка прогрівається", text: "Ринкові принти з'являться тут одразу після запуску потоку.", cta: "Відкрити ринки", href: "/markets" },
            activity: { title: "Поки немає живої активності", text: "Після закриття угоди її результат з'явиться в цій стрічці.", cta: "Перейти до торгівлі", href: "/trade" },
            markets: { title: "За цим фільтром нічого не знайдено", text: "Змініть фільтр або очистіть пошук, щоб побачити більше інструментів.", cta: "Скинути фільтр", href: "#reset-markets" },
        },
    };
    const src = dict[lang] || dict.en;
    return src[kind] || src.positions;
}

function emptyStateMarkup(kind) {
    const cfg = emptyStateConfig(kind);
    return `
        <div class="empty-state" data-empty-kind="${kind}">
            <div class="empty-state-head">
                <span class="empty-orb"></span>
                <div class="empty-title">${cfg.title}</div>
            </div>
            <div class="empty-text">${cfg.text}</div>
            <a class="qa-btn empty-cta" href="${cfg.href}">${cfg.cta}</a>
        </div>
    `;
}

function mountEmptyState(container, kind) {
    if (!container) return;
    container.innerHTML = emptyStateMarkup(kind);
    const cta = container.querySelector(".empty-cta");
    if (!cta) return;
    cta.addEventListener("click", (event) => {
        const href = cta.getAttribute("href") || "";
        if (href !== "#reset-markets") return;
        event.preventDefault();
        const search = document.getElementById("markets-search");
        const sort = document.getElementById("markets-sort");
        const allChip = document.querySelector('.market-filter-chip[data-filter-mode="all"]');
        if (search) search.value = "";
        if (sort) sort.value = "change_desc";
        if (allChip) allChip.click();
        const e = new Event("input", { bubbles: true });
        search?.dispatchEvent(e);
    });
}

function initPageIntroRibbon() {
    const ribbon = document.getElementById("page-intro-ribbon");
    const page = document.body?.dataset?.page || "";
    if (!ribbon || !["trade", "markets", "profile"].includes(page)) return;
    const lang = uiLang();
    const copy = {
        en: {
            trade: { title: "Execution Desk Ready", text: "Live feed, chart and risk deck are synced. You can open positions with one flow.", cta: "Open Full Chart", href: "/trade/chart" },
            markets: { title: "Market Scanner Live", text: "Use watchlist stars, filters and sort to build your personal trading universe.", cta: "Open Trade Terminal", href: "/trade" },
            profile: { title: "Control Center", text: "Track account health, open risk and latest activity in one panel.", cta: "Go To Markets", href: "/markets" },
        },
        ru: {
            trade: { title: "Торговый стол готов", text: "Лента, график и риск-панель синхронизированы. Сделку можно открыть в один поток.", cta: "Открыть полный график", href: "/trade/chart" },
            markets: { title: "Сканер рынков в онлайне", text: "Используйте watchlist, фильтры и сортировку для своего торгового пула.", cta: "Открыть терминал", href: "/trade" },
            profile: { title: "Центр управления", text: "Следите за состоянием счета, риском позиций и активностью в одном окне.", cta: "Перейти к рынкам", href: "/markets" },
        },
        uk: {
            trade: { title: "Торговий стіл готовий", text: "Стрічка, графік і панель ризику синхронізовані. Угоду можна відкрити в один потік.", cta: "Відкрити повний графік", href: "/trade/chart" },
            markets: { title: "Сканер ринків онлайн", text: "Використовуйте watchlist, фільтри та сортування для власного торгового пулу.", cta: "Відкрити термінал", href: "/trade" },
            profile: { title: "Центр керування", text: "Відстежуйте стан рахунку, ризик позицій і активність в одному вікні.", cta: "Перейти до ринків", href: "/markets" },
        },
    };
    const src = copy[lang] || copy.en;
    const cfg = src[page];
    if (!cfg) return;

    let seen = false;
    try {
        const key = `legend_intro_seen_${page}`;
        seen = sessionStorage.getItem(key) === "1";
        sessionStorage.setItem(key, "1");
    } catch (_) {
        seen = false;
    }
    if (seen) {
        ribbon.remove();
        return;
    }

    ribbon.innerHTML = `
        <b>${cfg.title}</b>
        <small>${cfg.text}</small>
        <div class="page-intro-actions">
            <a class="qa-btn" href="${cfg.href}">${cfg.cta}</a>
            <button type="button" class="chip page-intro-close">OK</button>
        </div>
    `;
    ribbon.hidden = false;
    requestAnimationFrame(() => ribbon.classList.add("show"));
    const close = () => {
        ribbon.classList.remove("show");
        window.setTimeout(() => ribbon.remove(), 220);
    };
    ribbon.querySelector(".page-intro-close")?.addEventListener("click", close);
    window.setTimeout(close, 6200);
}

function animateNumericText(el, nextValue, options = {}) {
    if (!el || !Number.isFinite(Number(nextValue))) return;
    const {
        decimals = 2,
        duration = 360,
        prefix = "",
        suffix = "",
        signed = false,
    } = options;
    const prevRaw = Number(el.dataset.numValue || 0);
    const next = Number(nextValue);
    if (Math.abs(next - prevRaw) < 1e-9) {
        const sameText = signed
            ? `${prefix}${next >= 0 ? "+" : ""}${next.toFixed(decimals)}${suffix}`
            : `${prefix}${next.toFixed(decimals)}${suffix}`;
        el.textContent = sameText;
        el.dataset.numValue = String(next);
        return;
    }
    const start = performance.now();
    const diff = next - prevRaw;
    const render = (value) => {
        const text = signed
            ? `${prefix}${value >= 0 ? "+" : ""}${value.toFixed(decimals)}${suffix}`
            : `${prefix}${value.toFixed(decimals)}${suffix}`;
        el.textContent = text;
    };
    const tick = (now) => {
        const t = Math.min(1, (now - start) / duration);
        const eased = 1 - Math.pow(1 - t, 3);
        render(prevRaw + diff * eased);
        if (t < 1) {
            requestAnimationFrame(tick);
        } else {
            el.dataset.numValue = String(next);
            render(next);
        }
    };
    requestAnimationFrame(tick);
}

function updateExposureWidgets(items, currency = "USD") {
    const safe = Array.isArray(items) ? items : [];
    const exposure = safe.reduce((acc, item) => acc + Number(item.amount || 0), 0);
    const openCount = safe.length;
    const liveBalance = Number((document.getElementById("live-balance") || {}).textContent || 0);
    const homeBalance = Number((document.getElementById("home-live-balance") || {}).textContent || 0);
    const baseBalance = liveBalance > 0 ? liveBalance : homeBalance;
    const riskLoad = baseBalance > 0 ? Math.min(100, (exposure / baseBalance) * 100) : 0;

    const dashExposure = document.getElementById("dash-exposure");
    const dashRiskLabel = document.getElementById("dash-risk-label");
    const dashRiskFill = document.getElementById("dash-risk-fill");
    const qOpenExp = document.getElementById("q-open-exp");
    const qOpenCount = document.getElementById("q-open-count");

    if (dashExposure) dashExposure.textContent = `${exposure.toFixed(2)} ${currency}`;
    if (dashRiskLabel) dashRiskLabel.textContent = `${riskLoad.toFixed(1)}%`;
    if (dashRiskFill) dashRiskFill.style.width = `${riskLoad.toFixed(1)}%`;
    if (qOpenExp) qOpenExp.textContent = `${exposure.toFixed(2)} ${currency}`;
    if (qOpenCount) qOpenCount.textContent = `${openCount} ${openCount === 1 ? "position" : "positions"}`;
    const profileRiskLabel = document.getElementById("profile-open-risk");
    const profileRiskFill = document.getElementById("profile-risk-fill");
    if (profileRiskLabel) profileRiskLabel.textContent = `${riskLoad.toFixed(1)}%`;
    if (profileRiskFill) profileRiskFill.style.width = `${riskLoad.toFixed(1)}%`;
}

function currentUiCurrency() {
    const tradeCurrencyEl = document.getElementById("live-currency");
    const homeCurrencyEl = document.getElementById("home-live-currency");
    return (tradeCurrencyEl && tradeCurrencyEl.textContent) || (homeCurrencyEl && homeCurrencyEl.textContent) || "USD";
}

function profileActivityText(deal) {
    const lang = uiLang();
    const isRu = lang === "ru";
    const isUk = lang === "uk";
    const symbol = deal.asset_name || deal.symbol || "Market";
    const pnl = Number(deal.profit || 0);
    const sign = pnl >= 0 ? "+" : "";
    if (isRu) return `${symbol}: ${sign}${pnl.toFixed(2)}`;
    if (isUk) return `${symbol}: ${sign}${pnl.toFixed(2)}`;
    return `${symbol}: ${sign}${pnl.toFixed(2)}`;
}

function profileActivityTime(raw) {
    if (!raw) return "--";
    const d = new Date(raw);
    if (!Number.isFinite(d.getTime())) return String(raw);
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function updateProfileLatestDeal(latestDeal) {
    const wrap = document.getElementById("profile-live-activity");
    if (!wrap || !latestDeal || !latestDeal.id) return;
    const key = String(latestDeal.id);
    if (PROFILE_ACTIVITY_SEEN.has(key)) return;
    PROFILE_ACTIVITY_SEEN.add(key);
    const row = document.createElement("div");
    const profit = Number(latestDeal.profit || 0);
    row.className = "row profile-row";
    row.innerHTML = `
        <div>
            <b>${profileActivityText(latestDeal)}</b>
            <small>${profileActivityTime(latestDeal.created_at)}</small>
        </div>
        <div><b class="${profit >= 0 ? "pos" : "neg"}">${profit >= 0 ? "+" : ""}${profit.toFixed(2)}</b></div>
    `;
    const empty = wrap.querySelector(".empty");
    if (empty) empty.remove();
    const emptyState = wrap.querySelector(".empty-state");
    if (emptyState) emptyState.remove();
    wrap.prepend(row);
    while (wrap.children.length > 6) {
        wrap.lastElementChild.remove();
    }
}

function updateTradeInsight() {
    const direction = (document.getElementById("direction-input") || {}).value || "up";
    const amount = Number((document.getElementById("trade-amount-input") || {}).value || 0);
    const tp = Number((document.querySelector('input[name="tp_percent"]') || {}).value || 0);
    const sl = Number((document.querySelector('input[name="sl_percent"]') || {}).value || 0);
    const insightDir = document.getElementById("insight-direction");
    const insightTp = document.getElementById("insight-tp");
    const insightSl = document.getElementById("insight-sl");
    if (insightDir) insightDir.textContent = direction === "up" ? "LONG" : "SHORT";
    if (insightTp) insightTp.textContent = tp > 0 ? `+${((amount * tp) / 100).toFixed(2)}` : "--";
    if (insightSl) insightSl.textContent = sl > 0 ? `-${((amount * sl) / 100).toFixed(2)}` : "--";
}

function initAppPreloader() {
    const overlay = document.getElementById("app-preloader");
    const fill = document.getElementById("preloader-bar-fill");
    const stage = document.getElementById("preloader-stage");
    const percent = document.getElementById("preloader-percent");
    if (!overlay || !fill) return;
    let seen = false;
    try {
        seen = sessionStorage.getItem("legend_webapp_loader_seen") === "1";
    } catch (_) {
        // no-op
    }
    if (document.body?.dataset?.preloader === "skip" || seen) {
        overlay.classList.add("done");
        return;
    }

    const lang = (document.body?.dataset?.lang || "en").toLowerCase();
    const isUk = lang === "uk";
    const isRu = lang === "ru";
    const stagePhrases = isRu
        ? [
            "ИНИЦИАЛИЗАЦИЯ ПЛАТФОРМЫ...",
            "СИНХРОНИЗАЦИЯ РЫНКА...",
            "ПОДКЛЮЧЕНИЕ ТЕРМИНАЛА...",
            "ЗАГРУЗКА РАБОЧЕГО СТОЛА...",
            "ГОТОВО К РАБОТЕ",
        ]
        : isUk
            ? [
                "ІНІЦІАЛІЗАЦІЯ ПЛАТФОРМИ...",
                "СИНХРОНІЗАЦІЯ РИНКУ...",
                "ПІДКЛЮЧЕННЯ ТЕРМІНАЛУ...",
                "ЗАВАНТАЖЕННЯ РОБОЧОГО ПРОСТОРУ...",
                "ГОТОВО ДО РОБОТИ",
            ]
            : [
                "INITIALIZING PLATFORM...",
                "SYNCING LIVE MARKET...",
                "CONNECTING TERMINAL...",
                "LOADING WORKSPACE...",
                "READY TO TRADE",
            ];

    const conn = navigator.connection || {};
    const slowNetwork = ["slow-2g", "2g", "3g"].includes(conn.effectiveType || "");
    const reducedMotion = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    const startedAt = Date.now();
    const minVisibleMs = reducedMotion ? 900 : (slowNetwork ? 2600 : 1800);
    const maxVisibleMs = reducedMotion ? 2200 : (slowNetwork ? 6400 : 4800);
    const progressDurationMs = reducedMotion ? 1200 : (slowNetwork ? 3900 : 2800);
    let stageIdx = 0;
    let visualPct = 2;
    overlay.dataset.stage = "0";
    if (stage) stage.textContent = stagePhrases[0];
    if (percent) percent.textContent = "2%";

    const stageTimer = window.setInterval(() => {
        if (!stage) return;
        stageIdx = Math.min(stagePhrases.length - 1, stageIdx + 1);
        stage.textContent = stagePhrases[stageIdx];
        overlay.dataset.stage = String(stageIdx);
    }, reducedMotion ? 420 : 600);

    let closed = false;
    let raf = 0;

    const easeOut = (x) => 1 - Math.pow(1 - x, 3.2);
    const tick = () => {
        if (closed) return;
        const elapsed = Date.now() - startedAt;
        const ratio = Math.max(0, Math.min(1, elapsed / progressDurationMs));
        const eased = easeOut(ratio);
        const base = 4 + (eased * 86);
        const micro = reducedMotion ? 0 : (Math.sin(elapsed / 140) * 0.45 + Math.sin(elapsed / 70) * 0.25);
        const pct = Math.max(2, Math.min(95, base + micro));
        visualPct = Math.max(visualPct, pct);
        const rounded = Math.min(95, Math.floor(visualPct));
        fill.style.width = `${visualPct.toFixed(2)}%`;
        if (percent) percent.textContent = `${rounded}%`;
        if (rounded > 22 && stageIdx < 1) {
            stageIdx = 1;
            if (stage) stage.textContent = stagePhrases[stageIdx];
            overlay.dataset.stage = String(stageIdx);
        } else if (rounded > 47 && stageIdx < 2) {
            stageIdx = 2;
            if (stage) stage.textContent = stagePhrases[stageIdx];
            overlay.dataset.stage = String(stageIdx);
        } else if (rounded > 72 && stageIdx < 3) {
            stageIdx = 3;
            if (stage) stage.textContent = stagePhrases[stageIdx];
            overlay.dataset.stage = String(stageIdx);
        }
        raf = window.requestAnimationFrame(tick);
    };
    raf = window.requestAnimationFrame(tick);

    const close = () => {
        if (closed) return;
        closed = true;
        if (raf) window.cancelAnimationFrame(raf);
        window.clearInterval(stageTimer);
        overlay.classList.add("finalizing");
        overlay.dataset.stage = "4";
        if (stage) stage.textContent = stagePhrases[4];
        try {
            sessionStorage.setItem("legend_webapp_loader_seen", "1");
        } catch (_) {
            // no-op
        }
        fill.style.width = "100%";
        if (percent) percent.textContent = "100%";
        const elapsed = Date.now() - startedAt;
        const waitMore = Math.max(0, minVisibleMs - elapsed);
        window.setTimeout(() => overlay.classList.add("done"), waitMore + (reducedMotion ? 30 : 120));
    };

    if (document.readyState === "complete") {
        window.setTimeout(close, reducedMotion ? 180 : 520);
    } else {
        window.addEventListener("load", () => window.setTimeout(close, reducedMotion ? 180 : 520), { once: true });
    }
    window.setTimeout(close, maxVisibleMs);
}

function applyRuntimeProfile() {
    const body = document.body;
    if (!body) return;
    const isTelegram = Boolean(window.Telegram && window.Telegram.WebApp);
    const lowViewport = Math.min(window.innerWidth || 0, window.innerHeight || 0) <= 430;
    if (isTelegram || lowViewport) {
        body.classList.add("perf-lite");
    }
}

function reasonLabel(reason) {
    if (reason === "tp") return L("js_reason_tp", "Take Profit");
    if (reason === "sl") return L("js_reason_sl", "Stop Loss");
    if (reason === "manual") return L("js_reason_manual", "Manual close");
    return L("js_reason_time", "By timer");
}

function updateTradePanelFromStatus(status) {
    const timer = document.getElementById("trade-timer");
    const progress = document.getElementById("trade-progress");
    const result = document.getElementById("trade-result");
    if (!timer || !result) return;

    const remaining = Math.max(0, Number(status.remaining || 0));
    const total = Math.max(1, Number(status.seconds || 1));
    timer.textContent = `${L("js_trade_waiting", "Time left")}: ${remaining}s`;
    if (progress) {
        const done = Math.min(100, Math.max(0, ((total - remaining) / total) * 100));
        progress.style.width = `${done}%`;
    }

    if (status.status === "closed") {
        const cls = status.is_win ? "pos" : "neg";
        const reason = reasonLabel(status.close_reason);
        result.innerHTML =
            `${L("js_trade_done", "Deal completed")}: <span class="${cls}">${status.profit > 0 ? "+" : ""}${status.profit}</span><br>` +
            `Reason: ${reason}<br>` +
            `${L("js_trade_balance", "New balance")}: ${status.balance}<br>` +
            `${L("js_trade_rate", "Rate")}: ${status.start_price} -> ${status.end_price}`;
        LIVE_TRADE_PANEL_ID = null;
        return;
    }

    const side = status.direction === "up" ? L("trade_long", "LONG") : L("trade_short", "SHORT");
    result.innerHTML =
        `<span class="pos">${L("js_trade_started", "Trade opened, countdown started")}</span><br>` +
        `${status.asset_name} ${side} · ${Number(status.amount || 0).toFixed(2)} ${status.currency || "USD"}`;
}

async function syncTradePanel(tradeId, tgId) {
    if (!tradeId || !tgId) return;
    LIVE_TRADE_PANEL_ID = tradeId;
    try {
        const statusResp = await fetch(`/api/trade/status?trade_id=${encodeURIComponent(tradeId)}&tg_id=${tgId}`);
        const status = await statusResp.json();
        if (!statusResp.ok || !status.ok) return;
        updateTradePanelFromStatus(status);
    } catch (_) {
        // no-op
    }
}

function initInteractiveFeedback() {
    const wa = window.Telegram && window.Telegram.WebApp;
    const tapBuzz = () => {
        try {
            wa?.HapticFeedback?.impactOccurred?.("light");
        } catch (_) {
            // no-op
        }
    };
    const candidates = document.querySelectorAll("button, .chip, .dir-btn, .qa-btn, .bottom-nav a, .lang-btn, .submit");
    candidates.forEach((node) => {
        node.classList.add("interactive");
        node.addEventListener("click", (event) => {
            const rect = node.getBoundingClientRect();
            const x = event.clientX - rect.left;
            const y = event.clientY - rect.top;
            node.style.setProperty("--rip-x", `${x}px`);
            node.style.setProperty("--rip-y", `${y}px`);
            node.classList.remove("pulse");
            // restart animation
            void node.offsetWidth;
            node.classList.add("pulse");
            tapBuzz();
        });
    });
}

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

function formatExpiration(seconds) {
    const sec = Number(seconds || 0);
    const lang = uiLang();
    if (lang === "ru") {
        if (sec >= 60 && sec % 60 === 0) return `${sec / 60} мин`;
        return `${sec} сек`;
    }
    if (lang === "uk") {
        if (sec >= 60 && sec % 60 === 0) return `${sec / 60} хв`;
        return `${sec} с`;
    }
    if (sec >= 60 && sec % 60 === 0) return `${sec / 60} min`;
    return `${sec}s`;
}

async function showTradeConfirmSheet(payload) {
    const overlay = document.getElementById("trade-confirm-overlay");
    if (!overlay) {
        const side = payload.direction === "up" ? "LONG" : "SHORT";
        return window.confirm(`${payload.asset} ${side} ${payload.amount.toFixed(2)} ${payload.currency}`);
    }
    const lang = uiLang();
    const isRu = lang === "ru";
    const isUk = lang === "uk";
    const mapText = {
        title: isRu ? "Подтверждение сделки" : isUk ? "Підтвердження угоди" : "Confirm Trade",
        sideUp: isRu ? "ЛОНГ" : isUk ? "ЛОНГ" : "LONG",
        sideDown: isRu ? "ШОРТ" : isUk ? "ШОРТ" : "SHORT",
        risk: isRu ? "Оценка, не гарантия. Финальный результат зависит от движения рынка." : isUk ? "Оцінка, не гарантія. Фінальний результат залежить від руху ринку." : "Estimate only, not guaranteed. Final result depends on live market movement.",
        confirm: isRu ? "Подтвердить и открыть" : isUk ? "Підтвердити та відкрити" : "Confirm & Open",
        cancel: isRu ? "Отмена" : isUk ? "Скасувати" : "Cancel",
    };
    const set = (id, value) => {
        const el = document.getElementById(id);
        if (el) el.textContent = value;
    };
    const sideLabel = payload.direction === "up" ? mapText.sideUp : mapText.sideDown;
    set("tc-title", mapText.title);
    set("tc-asset", payload.asset);
    set("tc-side", sideLabel);
    set("tc-amount", `${payload.amount.toFixed(2)} ${payload.currency}`);
    set("tc-lev", `${payload.leverage}x`);
    set("tc-exp", formatExpiration(payload.seconds));
    set("tc-mark", payload.mark > 0 ? payload.mark.toFixed(4) : "--");
    set("tc-tp", payload.tpValue > 0 ? `+${payload.tpValue.toFixed(2)} ${payload.currency}` : "--");
    set("tc-sl", payload.slValue > 0 ? `-${payload.slValue.toFixed(2)} ${payload.currency}` : "--");
    set("tc-risk-load", `${payload.riskLoad.toFixed(1)}%`);
    set("tc-note", mapText.risk);
    const confirmBtn = document.getElementById("tc-confirm");
    const cancelBtn = document.getElementById("tc-cancel");
    const cancelTopBtn = document.getElementById("tc-cancel-top");
    if (confirmBtn) confirmBtn.textContent = mapText.confirm;
    if (cancelBtn) cancelBtn.textContent = mapText.cancel;

    overlay.hidden = false;
    requestAnimationFrame(() => overlay.classList.add("show"));
    try {
        window.Telegram?.WebApp?.HapticFeedback?.impactOccurred?.("medium");
    } catch (_) {
        // no-op
    }

    return new Promise((resolve) => {
        let settled = false;
        const finish = (ok) => {
            if (settled) return;
            settled = true;
            overlay.classList.remove("show");
            window.setTimeout(() => {
                overlay.hidden = true;
            }, 220);
            confirmBtn?.removeEventListener("click", onConfirm);
            cancelBtn?.removeEventListener("click", onCancel);
            cancelTopBtn?.removeEventListener("click", onCancel);
            overlay.removeEventListener("click", onOverlay);
            resolve(ok);
        };
        const onConfirm = () => finish(true);
        const onCancel = () => finish(false);
        const onOverlay = (event) => {
            if (event.target === overlay) finish(false);
        };
        confirmBtn?.addEventListener("click", onConfirm, { once: true });
        cancelBtn?.addEventListener("click", onCancel, { once: true });
        cancelTopBtn?.addEventListener("click", onCancel, { once: true });
        overlay.addEventListener("click", onOverlay);
    });
}

function bindTradeForm() {
    const form = document.getElementById("trade-form");
    const result = document.getElementById("trade-result");
    const timer = document.getElementById("trade-timer");
    const progress = document.getElementById("trade-progress");
    const submitBtn = form ? form.querySelector('button[type="submit"]') : null;
    if (!form || !result) return;
    const selectedAsset = new URLSearchParams(window.location.search).get("asset");
    if (selectedAsset) {
        const assetSelect = form.querySelector('select[name="asset_name"]');
        if (assetSelect) {
            assetSelect.value = selectedAsset;
        }
    }
    let busy = false;
    form.addEventListener("submit", async (e) => {
        e.preventDefault();
        if (busy) return;
        if (form.dataset.confirmPass !== "1") {
            const pre = Object.fromEntries(new FormData(form).entries());
            const amount = Number(pre.amount || 0);
            const seconds = Number(pre.seconds || 0);
            const leverage = Number(pre.leverage || 10);
            const tpPercent = Number(pre.tp_percent || 0);
            const slPercent = Number(pre.sl_percent || 0);
            const currency = currentUiCurrency();
            const balance = Number((document.getElementById("live-balance") || {}).textContent || (document.getElementById("home-live-balance") || {}).textContent || 0);
            const riskLoad = balance > 0 ? Math.min(100, (amount / balance) * 100) : 0;
            const mark = Number((document.getElementById("stat-mark") || {}).textContent || (document.getElementById("orderbook-mark") || {}).textContent || 0);
            const ok = await showTradeConfirmSheet({
                asset: pre.asset_name || "Market",
                direction: pre.direction || "up",
                amount,
                seconds,
                leverage,
                currency,
                mark,
                tpValue: tpPercent > 0 ? (amount * tpPercent) / 100 : 0,
                slValue: slPercent > 0 ? (amount * slPercent) / 100 : 0,
                riskLoad,
            });
            if (!ok) return;
            form.dataset.confirmPass = "1";
            if (typeof form.requestSubmit === "function") {
                form.requestSubmit();
            } else {
                form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
            }
            return;
        }
        form.dataset.confirmPass = "0";
        busy = true;
        if (submitBtn) submitBtn.disabled = true;
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
                busy = false;
                if (submitBtn) submitBtn.disabled = false;
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
                let statusResp = null;
                let status = null;
                try {
                    statusResp = await fetch(`/api/trade/status?trade_id=${encodeURIComponent(data.trade_id)}&tg_id=${body.tg_id}`);
                    status = await statusResp.json();
                } catch (_) {
                    return false;
                }
                if (!statusResp.ok || !status.ok) return false;
                if (status.status === "closed") {
                    const cls = status.is_win ? "pos" : "neg";
                    const reason = reasonLabel(status.close_reason);
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
            const hardStopAt = closeAt + 25;
            while (!done) {
                done = await poll();
                if (!done && Math.floor(Date.now() / 1000) > hardStopAt) {
                    result.innerHTML = `<span class="neg">${L("js_trade_error", "failed to open trade")}</span>`;
                    break;
                }
                if (!done) {
                    await new Promise((resolve) => setTimeout(resolve, 1000));
                }
            }
            busy = false;
            if (submitBtn) submitBtn.disabled = false;
        } catch (_) {
            result.innerHTML = `<span class="neg">${L("js_network_error", "Network error")}</span>`;
            busy = false;
            if (submitBtn) submitBtn.disabled = false;
        }
    });
}

function bindTradeControls() {
    const amountInput = document.getElementById("trade-amount-input");
    const riskInput = document.getElementById("risk-input");
    const balanceRaw = document.getElementById("balance-raw");
    const riskPctLabel = document.getElementById("q-risk-pct");
    const riskAmtLabel = document.getElementById("q-risk-amt");
    const syncRiskDeck = () => {
        if (!riskInput || !amountInput) return;
        const rp = Number(riskInput.value || 0);
        const amt = Number(amountInput.value || 0);
        if (riskPctLabel) riskPctLabel.textContent = `${rp.toFixed(1)}%`;
        if (riskAmtLabel) riskAmtLabel.textContent = `${amt.toFixed(2)} ${currentUiCurrency()}`;
        updateTradeInsight();
    };
    const chips = document.querySelectorAll(".chip");
    chips.forEach((chip) => {
        chip.addEventListener("click", () => {
            if (!amountInput) return;
            amountInput.value = chip.dataset.amt;
            syncRiskDeck();
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
            syncRiskDeck();
        });
    });

    if (riskInput && amountInput && balanceRaw) {
        riskInput.addEventListener("input", () => {
            const rp = Number(riskInput.value || 0);
            const bal = Number(balanceRaw.value || 0);
            if (rp > 0) amountInput.value = ((bal * rp) / 100).toFixed(2);
            syncRiskDeck();
        });
        amountInput.addEventListener("input", syncRiskDeck);
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
    document.querySelectorAll('input[name="tp_percent"], input[name="sl_percent"]').forEach((el) => {
        el.addEventListener("input", updateTradeInsight);
    });
    document.querySelectorAll(".dir-btn").forEach((el) => {
        el.addEventListener("click", updateTradeInsight);
    });
    syncRiskDeck();
    updateTradeInsight();
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
                const supportEntryUrl = data.support_entry_url || "";
                const button = supportEntryUrl
                    ? `<div class="result-action"><a class="qa-btn" href="${supportEntryUrl}">${L("js_support_btn", "Open Support")}</a></div>`
                    : "";
                result.innerHTML = `<span class="pos">${data.message || L("js_card_support_msg", "For bank card payment, contact support.")}</span>${button}`;
                if (data.redirect_to_support && supportEntryUrl) {
                    window.setTimeout(() => {
                        window.location.href = supportEntryUrl;
                    }, 900);
                }
                return;
            }
            result.innerHTML = `<span class="pos">${L("js_deposit_sent", "Request #{id} sent to admin").replace("{id}", data.deposit_id)}</span>`;
        } catch (_) {
            result.innerHTML = `<span class="neg">${L("js_network_error", "Network error")}</span>`;
        }
    });
}

function bindDepositMethodCards() {
    const wrap = document.getElementById("deposit-method-cards");
    const methodSelect = document.getElementById("deposit-method");
    const hint = document.getElementById("deposit-method-hint");
    if (!wrap || !methodSelect) return;
    const cards = Array.from(wrap.querySelectorAll(".deposit-method-card[data-method]"));
    if (!cards.length) return;
    const lang = uiLang();
    const hints = {
        en: {
            crypto: "Crypto bot route is selected. Support will send final instructions after request submission.",
            trc20: "TRC20 route selected. Verify the wallet and network in support chat before transfer.",
            card: "Card method selected. Support will provide secure payment instructions in chat.",
        },
        ru: {
            crypto: "Выбран Crypto bot. Поддержка отправит финальные реквизиты после заявки.",
            trc20: "Выбран TRC20. Перед переводом уточните сеть и кошелёк у поддержки.",
            card: "Выбрана банковская карта. Поддержка отправит безопасные инструкции по оплате в чате.",
        },
        uk: {
            crypto: "Обрано Crypto bot. Підтримка надішле фінальні реквізити після заявки.",
            trc20: "Обрано TRC20. Перед переказом уточніть мережу та гаманець у підтримки.",
            card: "Обрано банківську картку. Підтримка надішле безпечні інструкції в чаті.",
        },
    };
    const dict = hints[lang] || hints.en;

    const sync = (method) => {
        cards.forEach((card) => card.classList.toggle("active", card.dataset.method === method));
        methodSelect.value = method;
        if (hint) hint.textContent = dict[method] || dict.crypto;
    };

    cards.forEach((card) => {
        card.addEventListener("click", () => sync(card.dataset.method || "crypto"));
    });
    methodSelect.addEventListener("change", () => sync(methodSelect.value || "crypto"));
    sync(methodSelect.value || cards[0].dataset.method || "crypto");
}

function buildCandleState() {
    return { tf: 60, candles: [], lastSmoothMark: null };
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

function updateMarketStats(data) {
    const setStatValue = (el, value) => {
        if (!el) return;
        el.textContent = `${value ?? "--"}`;
        el.classList.remove("loading");
    };
    const m = document.getElementById("stat-mark");
    const s = document.getElementById("stat-spread");
    const h = document.getElementById("stat-high");
    const l = document.getElementById("stat-low");
    setStatValue(m, data.mark);
    setStatValue(s, data.spread);
    setStatValue(h, data.high);
    setStatValue(l, data.low);
    LIVE_STATE.mark = Number(data.mark || 0);
    LIVE_STATE.spread = Number(data.spread || 0);
    LIVE_STATE.high = Number(data.high || 0);
    LIVE_STATE.low = Number(data.low || 0);
    LIVE_STATE.mode = data.market_mode || LIVE_STATE.mode;
    const qMode = document.getElementById("q-market-mode");
    const qSignal = document.getElementById("q-market-signal");
    if (qMode) qMode.textContent = LIVE_STATE.mode ? String(LIVE_STATE.mode).toUpperCase() : "LIVE";
    if (qSignal) {
        const span = Math.max(0, LIVE_STATE.high - LIVE_STATE.low);
        if (LIVE_STATE.spread > 0 && span > 0) {
            const pressure = Math.min(100, (LIVE_STATE.spread / span) * 100);
            qSignal.textContent = pressure > 34 ? "High volatility" : pressure > 16 ? "Balanced flow" : "Tight spread";
        } else {
            qSignal.textContent = "Streaming";
        }
    }
}

function pushMiniTapeTick(tick) {
    const mini = document.getElementById("mini-tape");
    if (!mini || !tick) return;
    const sideClass = tick.side === "buy" ? "pos" : "neg";
    const sideText = tick.side === "buy" ? L("js_side_buy", "BUY") : L("js_side_sell", "SELL");
    const node = document.createElement("div");
    node.className = "row mono";
    node.innerHTML = `<div><b>${tick.symbol}</b><small class="${sideClass}">${sideText}</small></div><div>${tick.price} • ${tick.qty}</div>`;
    mini.prepend(node);
    while (mini.children.length > 8) mini.lastElementChild.remove();
}

function bindMarketSocket() {
    const asksWrap = document.getElementById("orderbook-asks");
    const bidsWrap = document.getElementById("orderbook-bids");
    const markEl = document.getElementById("orderbook-mark");
    const pairSelect = document.querySelector('select[name="asset_name"]');
    const chartSymbolSelect = document.getElementById("chart-symbol-select");
    const tvChartEl = document.getElementById("tv-chart");
    const openChartBtn = document.getElementById("open-chart-btn");
    const liveDot = document.getElementById("market-live-dot");
    const liveText = document.getElementById("market-live-text");
    const flowText = document.getElementById("market-flow");
    const participantsText = document.getElementById("market-participants");
    const canvas = document.getElementById("candle-canvas");
    const tfSelect = document.getElementById("chart-tf");
    const webLabels = window.LEGEND_LABELS || {};
    if (!asksWrap || !bidsWrap || !markEl || !pairSelect) return;
    if (MARKET_SOCKET_RUNTIME && typeof MARKET_SOCKET_RUNTIME.cleanup === "function") {
        MARKET_SOCKET_RUNTIME.cleanup(true);
    }

    [markEl, document.getElementById("stat-mark"), document.getElementById("stat-spread"), document.getElementById("stat-high"), document.getElementById("stat-low")]
        .filter(Boolean)
        .forEach((el) => {
            if ((el.textContent || "").includes("--")) el.classList.add("loading");
        });

    const state = buildCandleState();
    if (tfSelect) {
        state.tf = Number(tfSelect.value || 60);
    }
    state.lastBar = null;
    const chartLimit = Number((tvChartEl && tvChartEl.dataset.limit) || 300);
    const maxBars = Math.max(120, chartLimit || 120);
    let liveLockEnabled = true;
    let crosshairEnabled = true;
    if (chartSymbolSelect && pairSelect && !chartSymbolSelect.value) {
        chartSymbolSelect.value = pairSelect.value || "";
    }

    const getActiveSymbol = () => {
        const v = (chartSymbolSelect && chartSymbolSelect.value) || (pairSelect && pairSelect.value) || "BTC";
        return v;
    };
    const normalizeSymbol = (raw) => String(raw || "").trim().toUpperCase().replace(/[^A-Z0-9]/g, "");
    const symbolAliases = (raw) => {
        const n = normalizeSymbol(raw);
        const aliases = new Set([n]);
        const map = {
            BITCOIN: ["BTC", "BTCUSDT", "XBT"],
            ETHEREUM: ["ETH", "ETHUSDT"],
            TONCOIN: ["TON", "TONUSDT"],
            SHIBAINU: ["SHIB", "SHIBUSDT"],
            CARDANO: ["ADA", "ADAUSDT"],
            SOLANA: ["SOL", "SOLUSDT"],
            RIPPLE: ["XRP", "XRPUSDT"],
            TRON: ["TRX", "TRXUSDT"],
        };
        Object.entries(map).forEach(([name, group]) => {
            if (n === name || group.includes(n)) {
                aliases.add(name);
                group.forEach((x) => aliases.add(x));
            }
        });
        return aliases;
    };
    const symbolsMatch = (incoming, active) => {
        if (!incoming) return true;
        const incomingAliases = symbolAliases(incoming);
        const activeAliases = symbolAliases(active);
        for (const x of incomingAliases) {
            if (activeAliases.has(x)) return true;
        }
        return false;
    };

    const hasLW = Boolean(window.LightweightCharts && tvChartEl);
    let lwChart = null;
    let candleSeries = null;
    let volumeSeries = null;
    let brandingTimer = null;
    let onResize = null;
    if (hasLW) {
        lwChart = window.LightweightCharts.createChart(tvChartEl, {
            width: tvChartEl.clientWidth || 430,
            height: tvChartEl.clientHeight || 360,
            layout: { background: { color: "#07131f" }, textColor: "#9db4c8" },
            rightPriceScale: { borderColor: "rgba(140, 186, 219, 0.18)" },
            timeScale: {
                borderColor: "rgba(140, 186, 219, 0.18)",
                timeVisible: true,
                secondsVisible: false,
                rightOffset: 2,
                barSpacing: 8,
                minBarSpacing: 4,
            },
            grid: {
                vertLines: { color: "rgba(128, 173, 207, 0.10)" },
                horzLines: { color: "rgba(128, 173, 207, 0.10)" },
            },
            crosshair: { mode: crosshairEnabled ? 1 : 0 },
            localization: { locale: "en-US" },
        });
        candleSeries = lwChart.addCandlestickSeries({
            upColor: "#00d69f",
            downColor: "#ff4f72",
            wickUpColor: "#00d69f",
            wickDownColor: "#ff4f72",
            borderUpColor: "#00d69f",
            borderDownColor: "#ff4f72",
            borderVisible: true,
            priceLineColor: "#45c0ff",
            priceLineWidth: 1,
        });
        volumeSeries = lwChart.addHistogramSeries({
            priceFormat: { type: "volume" },
            priceScaleId: "vol",
            lastValueVisible: false,
            priceLineVisible: false,
        });
        lwChart.priceScale("vol").applyOptions({
            visible: false,
            scaleMargins: { top: 0.80, bottom: 0.02 },
        });
        lwChart.priceScale("right").applyOptions({
            scaleMargins: { top: 0.05, bottom: 0.20 },
        });
        onResize = () => {
            lwChart.applyOptions({ width: tvChartEl.clientWidth || 430 });
        };
        window.addEventListener("resize", onResize);
        // Remove third-party branding elements inside chart container.
        const scrubBranding = () => {
            if (!tvChartEl) return;
            tvChartEl.querySelectorAll("a, iframe, img").forEach((el) => {
                const txt = (el.textContent || "").toLowerCase();
                const href = (el.getAttribute("href") || "").toLowerCase();
                if (href.includes("tradingview") || txt.includes("tradingview")) {
                    el.remove();
                }
            });
        };
        scrubBranding();
        brandingTimer = setInterval(scrubBranding, 2500);
        setTimeout(() => {
            if (brandingTimer) {
                clearInterval(brandingTimer);
                brandingTimer = null;
            }
        }, 14000);
    } else if (canvas) {
        if (tvChartEl) tvChartEl.style.display = "none";
        canvas.style.display = "block";
    }

    const normalizeCandles = (candlesRaw) =>
        (candlesRaw || []).map((c) => ({
            time: Number(c.t),
            open: Number(c.o),
            high: Number(c.h),
            low: Number(c.l),
            close: Number(c.c),
            volume: Number(c.v || 0),
        }));

    const setChartData = (candles) => {
        if (!Array.isArray(candles) || !candles.length) return;
        state.candles = candles;
        state.lastBar = candles[candles.length - 1];
        state.lastSmoothMark = Number(state.lastBar.close || state.lastBar.open || 0) || null;
        if (lwChart && candleSeries && volumeSeries) {
            candleSeries.setData(
                candles.map((c) => ({
                    time: c.time,
                    open: c.open,
                    high: c.high,
                    low: c.low,
                    close: c.close,
                }))
            );
            volumeSeries.setData(
                candles.map((c) => ({
                    time: c.time,
                    value: c.volume || 0,
                    color: c.close >= c.open ? "rgba(0,210,201,0.22)" : "rgba(255,55,95,0.22)",
                }))
            );
            lwChart.timeScale().fitContent();
            if (liveLockEnabled) lwChart.timeScale().scrollToRealTime();
            return;
        }
        drawCandles(
            canvas,
            candles.map((c) => ({ bucket: c.time, open: c.open, high: c.high, low: c.low, close: c.close }))
        );
    };

    const updateLiveBar = (mark, ts) => {
        const incomingMark = Number(mark);
        if (!Number.isFinite(incomingMark) || incomingMark <= 0) return;
        if (!state.lastSmoothMark || !Number.isFinite(state.lastSmoothMark)) {
            state.lastSmoothMark = incomingMark;
        }
        const ref = Number(state.lastSmoothMark);
        // Guard against outlier ticks that can break the candle structure.
        if (ref > 0) {
            const jumpRatio = Math.abs(incomingMark - ref) / ref;
            if (jumpRatio > 0.12) {
                return;
            }
        }
        const maxDrift = Math.max(ref * 0.0035, 0.00001); // keep intrabar updates exchange-like, not spiky
        const drift = Math.max(-maxDrift, Math.min(maxDrift, incomingMark - ref));
        const clampedMark = ref + drift;
        const smoothMark = ref * 0.72 + clampedMark * 0.28;
        mark = Number(smoothMark.toFixed(8));
        state.lastSmoothMark = mark;

        const tf = Number(state.tf || 60);
        const bucket = Math.floor(ts / tf) * tf;
        const prev = state.lastBar;
        if (!prev) {
            state.lastBar = { time: bucket, open: mark, high: mark, low: mark, close: mark, volume: 0 };
        } else if (prev.time !== bucket) {
            state.lastBar = {
                time: bucket,
                open: prev.close,
                high: mark,
                low: mark,
                close: mark,
                volume: Math.max(1, Math.abs(mark - prev.close) * 1000),
            };
            state.candles.push(state.lastBar);
            if (state.candles.length > maxBars) state.candles.shift();
        } else {
            prev.high = Math.max(prev.high, mark);
            prev.low = Math.min(prev.low, mark);
            prev.close = mark;
            prev.volume = (prev.volume || 0) + Math.max(0.5, Math.abs(mark - prev.open) * 100);
            state.lastBar = prev;
        }

        if (lwChart && candleSeries && volumeSeries) {
            candleSeries.update({
                time: state.lastBar.time,
                open: state.lastBar.open,
                high: state.lastBar.high,
                low: state.lastBar.low,
                close: state.lastBar.close,
            });
            volumeSeries.update({
                time: state.lastBar.time,
                value: state.lastBar.volume || 0,
                color: state.lastBar.close >= state.lastBar.open ? "rgba(0,210,201,0.22)" : "rgba(255,55,95,0.22)",
            });
            if (liveLockEnabled) lwChart.timeScale().scrollToRealTime();
        } else {
            drawCandles(
                canvas,
                state.candles.map((c) => ({ bucket: c.time, open: c.open, high: c.high, low: c.low, close: c.close }))
            );
        }
    };

    const primeCandleHistory = async () => {
        const sym = encodeURIComponent(getActiveSymbol());
        const tf = Number(state.tf || 60);
        try {
            const resp = await fetch(`/api/market/candles?symbol=${sym}&tf=${tf}&limit=${chartLimit}`);
            const data = await resp.json();
            if (!resp.ok || !data.ok || !Array.isArray(data.candles)) return;
            setChartData(normalizeCandles(data.candles));
            if (lwChart) {
                lwChart.applyOptions({
                    timeScale: {
                        borderColor: "rgba(255,255,255,0.08)",
                        timeVisible: true,
                        secondsVisible: tf < 60,
                    },
                });
            }
        } catch (_) {
            // no-op
        }
    };

    if (tfSelect) {
        tfSelect.addEventListener("change", () => {
            state.tf = Number(tfSelect.value || 60);
            primeCandleHistory();
        });
    }

    if (openChartBtn) {
        const syncOpenHref = () => {
            const sym = encodeURIComponent(getActiveSymbol());
            openChartBtn.href = `/trade/chart?symbol=${sym}`;
        };
        syncOpenHref();
        if (chartSymbolSelect) chartSymbolSelect.addEventListener("change", syncOpenHref);
        if (pairSelect) pairSelect.addEventListener("change", syncOpenHref);
    }

    const tfButtons = document.querySelectorAll(".chart-tf-btn[data-tf]");
    if (tfButtons.length && tfSelect) {
        const setActiveTfBtn = (tfValue) => {
            tfButtons.forEach((btn) => btn.classList.toggle("active", String(btn.dataset.tf) === String(tfValue)));
        };
        setActiveTfBtn(tfSelect.value);
        tfButtons.forEach((btn) => {
            btn.addEventListener("click", () => {
                tfSelect.value = String(btn.dataset.tf || "60");
                setActiveTfBtn(tfSelect.value);
                tfSelect.dispatchEvent(new Event("change"));
            });
        });
    }

    const fitBtn = document.getElementById("chart-fit-btn");
    if (fitBtn && lwChart) {
        fitBtn.addEventListener("click", () => lwChart.timeScale().fitContent());
    }
    const resetBtn = document.getElementById("chart-reset-btn");
    if (resetBtn) {
        resetBtn.addEventListener("click", () => {
            primeCandleHistory();
        });
    }
    const crosshairBtn = document.getElementById("chart-crosshair-btn");
    if (crosshairBtn && lwChart) {
        crosshairBtn.classList.toggle("active", crosshairEnabled);
        crosshairBtn.addEventListener("click", () => {
            crosshairEnabled = !crosshairEnabled;
            lwChart.applyOptions({ crosshair: { mode: crosshairEnabled ? 1 : 0 } });
            crosshairBtn.classList.toggle("active", crosshairEnabled);
        });
    }
    const lockBtn = document.getElementById("chart-lock-btn");
    if (lockBtn) {
        lockBtn.classList.toggle("active", liveLockEnabled);
        lockBtn.addEventListener("click", () => {
            liveLockEnabled = !liveLockEnabled;
            lockBtn.classList.toggle("active", liveLockEnabled);
            if (liveLockEnabled && lwChart) lwChart.timeScale().scrollToRealTime();
        });
    }

    const protocol = window.location.protocol === "https:" ? "wss" : "ws";
    let ws = null;
    let fallbackTimer = null;
    let silenceTimer = null;
    let reconnectTimer = null;
    let lastCandleUpdateMs = 0;
    let lastBookUpdateMs = 0;
    let lastHistorySyncMs = 0;
    let lastMarketMessageMs = 0;
    let reconnectDelayMs = 1200;
    let wsFailStreak = 0;
    let wsDisabledUntil = 0;
    let pollingOnlyMode = false;
    let disposed = false;

    const setFeedState = (live, source = "ws") => {
        if (liveDot) liveDot.classList.toggle("live", !!live);
        if (!liveText) return;
        if (source === "poll") {
            liveText.textContent = live
                ? (webLabels.trade_feed_live ? `${webLabels.trade_feed_live} (polling)` : "Market Feed: live (polling)")
                : (webLabels.trade_feed_polling || "Market Feed: polling");
            return;
        }
        liveText.textContent = live
            ? (webLabels.trade_feed_live || "Market Feed: live")
            : (webLabels.trade_feed_reconnect || "Market Feed: reconnecting");
    };
    const stopTimer = (id) => {
        if (!id) return null;
        clearInterval(id);
        clearTimeout(id);
        return null;
    };
    const stopFallback = () => {
        fallbackTimer = stopTimer(fallbackTimer);
    };
    const cleanup = (closeSocket = false) => {
        disposed = true;
        stopFallback();
        silenceTimer = stopTimer(silenceTimer);
        reconnectTimer = stopTimer(reconnectTimer);
        brandingTimer = stopTimer(brandingTimer);
        if (onResize) {
            window.removeEventListener("resize", onResize);
            onResize = null;
        }
        if (closeSocket && ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
            try {
                ws.close();
            } catch (_) {
                // no-op
            }
        }
        ws = null;
    };
    MARKET_SOCKET_RUNTIME = { cleanup };

    const ensureBookRows = (wrap, type, count) => {
        while (wrap.children.length < count) {
            const row = document.createElement("div");
            row.className = `book-row ${type}`;
            row.innerHTML = "<span>--</span><em>--</em>";
            wrap.appendChild(row);
        }
        while (wrap.children.length > count) {
            wrap.removeChild(wrap.lastElementChild);
        }
    };

    const patchBookRows = (wrap, rows) => {
        if (!wrap || !Array.isArray(rows)) return;
        ensureBookRows(wrap, wrap.id === "orderbook-asks" ? "ask" : "bid", rows.length);
        for (let i = 0; i < rows.length; i += 1) {
            const r = rows[i];
            const el = wrap.children[i];
            if (!el) continue;
            const priceEl = el.querySelector("span");
            const qtyEl = el.querySelector("em");
            const priceText = String(r.price);
            const qtyText = String(r.qty);
            if (priceEl && priceEl.textContent !== priceText) priceEl.textContent = priceText;
            if (qtyEl && qtyEl.textContent !== qtyText) qtyEl.textContent = qtyText;
        }
    };

    const subscribe = () => {
        if (disposed || !ws || ws.readyState !== WebSocket.OPEN) return;
        ws.send(JSON.stringify({ type: "subscribe", symbol: getActiveSymbol() }));
    };

    const applyMarketData = (data, source = "ws") => {
        if (disposed) return;
        if (source === "ws" && !symbolsMatch(data.symbol || data.asset_name || "", getActiveSymbol())) {
            return;
        }
        const markValue = Number(data.mark);
        if (!Number.isFinite(markValue) || markValue <= 0) return;
        if (fallbackTimer && ws && ws.readyState === WebSocket.OPEN) {
            stopFallback();
        }
        lastMarketMessageMs = Date.now();
        setFeedState(true, source);
        markEl.textContent = `${data.mark}`;
        markEl.classList.remove("loading");
        updateMarketStats(data);
        const nowMs = Date.now();
        if (nowMs - lastBookUpdateMs >= 5200) {
            patchBookRows(asksWrap, data.asks || []);
            patchBookRows(bidsWrap, data.bids || []);
            lastBookUpdateMs = nowMs;
        }
        if (nowMs - lastCandleUpdateMs >= 2600) {
            updateLiveBar(markValue, Number(data.ts || Math.floor(Date.now() / 1000)));
            lastCandleUpdateMs = nowMs;
        }
        if (nowMs - lastHistorySyncMs >= 18000) {
            primeCandleHistory();
            lastHistorySyncMs = nowMs;
        }
        pushMiniTapeTick(data.tick);
    };

    const startFallback = () => {
        if (disposed || fallbackTimer) return;
        const pollOnce = async () => {
            try {
                const sym = encodeURIComponent(getActiveSymbol());
                const resp = await fetch(`/api/market/snapshot?symbol=${sym}`);
                if (!resp.ok) return;
                const data = await resp.json();
                if (data.ok) applyMarketData(data, "poll");
            } catch (_) {
                // no-op
            }
        };
        pollOnce();
        fallbackTimer = setInterval(pollOnce, 3200);
    };

    const bootstrapSnapshot = async () => {
        if (disposed) return;
        try {
            const sym = encodeURIComponent(getActiveSymbol());
            const resp = await fetch(`/api/market/snapshot?symbol=${sym}`);
            if (!resp.ok) return;
            const data = await resp.json();
            if (data.ok) applyMarketData(data, "poll");
        } catch (_) {
            // no-op
        }
    };

    pairSelect.addEventListener("change", () => {
        if (disposed) return;
        if (chartSymbolSelect && chartSymbolSelect !== pairSelect) chartSymbolSelect.value = pairSelect.value;
        primeCandleHistory();
        if (ws && ws.readyState === WebSocket.OPEN) subscribe();
    });
    if (chartSymbolSelect && chartSymbolSelect !== pairSelect) {
        chartSymbolSelect.addEventListener("change", () => {
            if (disposed) return;
            if (pairSelect) pairSelect.value = chartSymbolSelect.value;
            primeCandleHistory();
            if (ws && ws.readyState === WebSocket.OPEN) subscribe();
        });
    }

    const connectWs = () => {
        if (disposed) return;
        const now = Date.now();
        if (now < wsDisabledUntil) {
            if (!fallbackTimer) startFallback();
            setFeedState(Boolean(lastMarketMessageMs && now - lastMarketMessageMs < 12000), "poll");
            reconnectTimer = stopTimer(reconnectTimer);
            reconnectTimer = setTimeout(() => connectWs(), wsDisabledUntil - now + 80);
            return;
        }
        if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
        ws = new WebSocket(`${protocol}://${window.location.host}/ws/market`);

        ws.addEventListener("open", () => {
            if (disposed) return;
            reconnectDelayMs = 1200;
            wsFailStreak = 0;
            pollingOnlyMode = false;
            wsDisabledUntil = 0;
            setFeedState(true, "ws");
            lastMarketMessageMs = Date.now();
            subscribe();
            primeCandleHistory();
            bootstrapSnapshot();
            stopFallback();
        });

        ws.addEventListener("message", (event) => {
            let data = null;
            try {
                data = JSON.parse(event.data);
            } catch (_) {
                return;
            }
            if (!data || data.type !== "market") return;
            applyMarketData(data, "ws");
        });

        ws.addEventListener("close", () => {
            if (disposed) return;
            wsFailStreak += 1;
            const nowTs = Date.now();
            if (wsFailStreak >= 3) {
                pollingOnlyMode = true;
                wsDisabledUntil = nowTs + 120000;
            }
            startFallback();
            setFeedState(Boolean(lastMarketMessageMs && nowTs - lastMarketMessageMs < 12000), pollingOnlyMode ? "poll" : "ws");
            reconnectDelayMs = Math.min(7000, Math.round(reconnectDelayMs * 1.35));
            reconnectTimer = stopTimer(reconnectTimer);
            const waitMs = pollingOnlyMode
                ? Math.max(reconnectDelayMs + Math.floor(Math.random() * 240), wsDisabledUntil - nowTs)
                : reconnectDelayMs + Math.floor(Math.random() * 240);
            reconnectTimer = setTimeout(() => connectWs(), waitMs);
        });

        ws.addEventListener("error", () => {
            if (disposed) return;
            startFallback();
        });
    };

    // If WS is silent for too long, rely on snapshot polling but don't force fake reconnect states while socket is open.
    silenceTimer = setInterval(() => {
        if (disposed) return;
        const nowTs = Date.now();
        const hasFreshData = Boolean(lastMarketMessageMs && nowTs - lastMarketMessageMs < 12000);
        if (pollingOnlyMode || nowTs < wsDisabledUntil) {
            startFallback();
            setFeedState(hasFreshData, "poll");
            return;
        }
        const isOpen = Boolean(ws && ws.readyState === WebSocket.OPEN);
        if (!isOpen) {
            startFallback();
            setFeedState(hasFreshData, hasFreshData ? "poll" : "ws");
            return;
        }
        if (lastMarketMessageMs && nowTs - lastMarketMessageMs > 18000) {
            startFallback();
            setFeedState(hasFreshData, hasFreshData ? "poll" : "ws");
            return;
        }
        setFeedState(true, "ws");
    }, 3000);

    // Seed first values quickly and then connect WS stream.
    bootstrapSnapshot();
    startFallback();
    connectWs();
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
    const positionsWrap = document.getElementById("open-positions-list");

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
        if (balEl) animateNumericText(balEl, Number(data.balance || 0), { decimals: 2 });
        if (curEl) curEl.textContent = data.currency || "USD";
        if (openEl) animateNumericText(openEl, Number(data.open_trades || 0), { decimals: 0, duration: 260 });
        if (balanceRaw) balanceRaw.value = Number(data.balance || 0).toFixed(4);
        const homeBal = document.getElementById("home-live-balance");
        const homeCur = document.getElementById("home-live-currency");
        if (homeBal) animateNumericText(homeBal, Number(data.balance || 0), { decimals: 2 });
        if (homeCur) homeCur.textContent = data.currency || "USD";
        if (positionsWrap && Array.isArray(data.open_positions)) {
            renderOpenPositions(data.open_positions, tgId);
            updateExposureWidgets(data.open_positions, data.currency || currentUiCurrency());
            if (data.open_positions.length) {
                syncTradePanel(data.open_positions[0].trade_id, tgId);
            }
        }
        if (data.latest_deal) {
            updateProfileLatestDeal(data.latest_deal);
        } else {
            const activityWrap = document.getElementById("profile-live-activity");
            if (activityWrap && !activityWrap.querySelector(".profile-row")) {
                mountEmptyState(activityWrap, "activity");
            }
        }
    });
}

function renderOpenPositions(items, tgId) {
    const wrap = document.getElementById("open-positions-list");
    if (!wrap) return;
    if (!items.length) {
        mountEmptyState(wrap, "positions");
        const timer = document.getElementById("trade-timer");
        const progress = document.getElementById("trade-progress");
        const result = document.getElementById("trade-result");
        if (timer) timer.textContent = "--";
        if (progress) progress.style.width = "0%";
        if (result) result.innerHTML = "";
        LIVE_TRADE_PANEL_ID = null;
        return;
    }
    wrap.innerHTML = items
        .map((p) => {
            const side = p.direction === "up" ? L("trade_long", "LONG") : L("trade_short", "SHORT");
            const sideClass = p.direction === "up" ? "pos" : "neg";
            return `
            <div class="row position-row" data-trade-id="${p.trade_id}" data-tg-id="${tgId}">
                <div>
                    <b>${p.asset_name} <span class="${sideClass}">${side}</span></b>
                    <small>${Number(p.amount || 0).toFixed(2)} ${(p.currency || "USD")} · ${p.remaining}s · ${Number(p.leverage || 10)}x</small>
                </div>
                <div class="position-actions">
                    <button class="chip pos-close-btn" data-trade-id="${p.trade_id}" data-tg-id="${tgId}">
                        ${L("trade_close_now", "Close Now")}
                    </button>
                    <button class="chip pos-partial-btn" data-trade-id="${p.trade_id}" data-tg-id="${tgId}" data-ratio="0.5">
                        50%
                    </button>
                    <button class="chip pos-reverse-btn" data-trade-id="${p.trade_id}" data-tg-id="${tgId}">
                        Reverse
                    </button>
                </div>
            </div>`;
        })
        .join("");
}

function bindOpenPositionsActions() {
    const wrap = document.getElementById("open-positions-list");
    if (!wrap) return;
    wrap.addEventListener("click", async (e) => {
        const row = e.target.closest(".position-row");
        const clickedClose = e.target.closest(".pos-close-btn");
        if (row && !clickedClose) {
            const tradeId = row.dataset.tradeId;
            const tgId = Number(row.dataset.tgId || 0);
            if (tradeId && tgId) {
                LIVE_TRADE_PANEL_ID = tradeId;
                syncTradePanel(tradeId, tgId);
            }
        }
        const btn = e.target.closest(".pos-close-btn");
        const partialBtn = e.target.closest(".pos-partial-btn");
        const reverseBtn = e.target.closest(".pos-reverse-btn");
        const actionBtn = btn || partialBtn || reverseBtn;
        if (!actionBtn) return;
        const tradeId = actionBtn.dataset.tradeId;
        const tgId = Number(actionBtn.dataset.tgId || 0);
        if (!tradeId || !tgId) return;
        actionBtn.disabled = true;
        try {
            let endpoint = "/api/trade/close";
            let body = { trade_id: tradeId, tg_id: tgId };
            if (partialBtn) {
                endpoint = "/api/trade/close_partial";
                body = { trade_id: tradeId, tg_id: tgId, ratio: Number(partialBtn.dataset.ratio || 0.5) };
            } else if (reverseBtn) {
                endpoint = "/api/trade/reverse";
                body = { trade_id: tradeId, tg_id: tgId };
            }
            const resp = await fetch(endpoint, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(body),
            });
            const data = await resp.json();
            if (!resp.ok || !data.ok) {
                alert(data.error || "Failed to close");
                actionBtn.disabled = false;
                return;
            }
            await refreshOpenPositions();
            if (data.new_trade_id) {
                LIVE_TRADE_PANEL_ID = data.new_trade_id;
                await syncTradePanel(data.new_trade_id, tgId);
            }
        } catch (_) {
            alert("Network error");
            actionBtn.disabled = false;
        }
    });
}

function hydrateInitialTradeState() {
    const tg = document.querySelector('input[name="tg_id"]');
    const positionsWrap = document.getElementById("open-positions-list");
    const openEl = document.getElementById("live-open-trades");
    if (tg && positionsWrap && INITIAL_OPEN_POSITIONS.length) {
        renderOpenPositions(INITIAL_OPEN_POSITIONS, Number(tg.value || 0));
        updateExposureWidgets(INITIAL_OPEN_POSITIONS, currentUiCurrency());
    }
    if (openEl && INITIAL_OPEN_POSITIONS.length) {
        openEl.textContent = String(INITIAL_OPEN_POSITIONS.length);
    }
    if (INITIAL_TRADE_STATUS && INITIAL_TRADE_STATUS.ok) {
        LIVE_TRADE_PANEL_ID = INITIAL_TRADE_STATUS.trade_id || null;
        updateTradePanelFromStatus(INITIAL_TRADE_STATUS);
    }
}

function bindTradeQuickActions() {
    const symbolSelect = document.querySelector('select[name="asset_name"]');
    const chartSymbolSelect = document.getElementById("chart-symbol-select");
    const tfSelect = document.getElementById("chart-tf");
    const openChartBtn = document.getElementById("open-chart-btn");
    const statMark = document.getElementById("stat-mark");
    const statHigh = document.getElementById("stat-high");
    const statLow = document.getElementById("stat-low");
    const statSpread = document.getElementById("stat-spread");

    const openChart = () => {
        const sym = encodeURIComponent((chartSymbolSelect && chartSymbolSelect.value) || (symbolSelect && symbolSelect.value) || "Bitcoin");
        window.location.href = `/trade/chart?symbol=${sym}`;
    };

    [statMark, statHigh, statLow].forEach((el) => {
        if (!el) return;
        const card = el.closest(".stat-card");
        if (!card) return;
        card.title = L("js_open_chart_asset", "Open selected asset chart");
        card.style.cursor = "pointer";
        card.addEventListener("click", openChart);
    });

    if (statSpread) {
        const card = statSpread.closest(".stat-card");
        if (card && tfSelect) {
            card.title = L("js_change_timeframe", "Change timeframe");
            card.style.cursor = "pointer";
            card.addEventListener("click", () => {
                const order = ["60", "300", "900", "3600"];
                const idx = order.indexOf(String(tfSelect.value || "60"));
                const next = order[(idx + 1) % order.length];
                tfSelect.value = next;
                tfSelect.dispatchEvent(new Event("change"));
            });
        }
    }

    if (openChartBtn) {
        openChartBtn.addEventListener("mouseenter", () => {
            openChartBtn.textContent = L("trade_open_chart", "Open Chart");
        });
    }
}

async function refreshOpenPositions() {
    const wrap = document.getElementById("open-positions-list");
    const tg = document.querySelector('input[name="tg_id"]');
    if (!wrap || !tg) return;
    const tgId = Number(tg.value || 0);
    if (!tgId) return;
    try {
        const resp = await fetch(`/api/trade/open_positions?tg_id=${tgId}`);
        const data = await resp.json();
        if (!resp.ok || !data.ok) return;
        renderOpenPositions(data.items || [], tgId);
        updateExposureWidgets(data.items || [], currentUiCurrency());
        if (Array.isArray(data.items) && data.items.length) {
            syncTradePanel(data.items[0].trade_id, tgId);
        }
    } catch (_) {
        // no-op
    }
}

function bindMarketMiniCharts() {
    const canvases = document.querySelectorAll(".mini-spark");
    if (!canvases.length) return;
    canvases.forEach((canvas) => {
        const ctx = canvas.getContext("2d");
        if (!ctx) return;
        const price = Number(canvas.dataset.price || 1);
        const change = Number(canvas.dataset.change || 0);
        const points = [];
        let p = price * (1 - change / 100);
        for (let i = 0; i < 24; i += 1) {
            p += (Math.random() - 0.45) * (Math.abs(change) * 0.015 + price * 0.002);
            points.push(Math.max(0.0001, p));
        }
        const max = Math.max(...points);
        const min = Math.min(...points);
        const range = Math.max(0.00001, max - min);
        const w = canvas.width;
        const h = canvas.height;
        ctx.clearRect(0, 0, w, h);
        ctx.lineWidth = 1.5;
        ctx.strokeStyle = change >= 0 ? "#8fff4f" : "#ff6f6f";
        ctx.beginPath();
        points.forEach((v, i) => {
            const x = (i / (points.length - 1)) * (w - 2) + 1;
            const y = ((max - v) / range) * (h - 6) + 3;
            if (i === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });
        ctx.stroke();
    });
}

async function refreshMarketsLive() {
    const priceNodes = document.querySelectorAll(".m-price[data-symbol]");
    const changeNodes = document.querySelectorAll(".m-change[data-symbol]");
    if (!priceNodes.length && !changeNodes.length) return;
    try {
        const resp = await fetch("/api/markets/live");
        if (!resp.ok) return;
        const data = await resp.json();
        if (!data.ok || !Array.isArray(data.items)) return;
        const bySym = new Map();
        data.items.forEach((x) => bySym.set(String(x.market_ref || x.name || x.symbol || "").toUpperCase(), x));
        priceNodes.forEach((node) => {
            const sym = String(node.dataset.symbol || "").toUpperCase();
            const row = bySym.get(sym);
            if (!row) return;
            node.textContent = `${row.price}`;
        });
        changeNodes.forEach((node) => {
            const sym = String(node.dataset.symbol || "").toUpperCase();
            const row = bySym.get(sym);
            if (!row) return;
            node.textContent = `${row.day_change >= 0 ? "+" : ""}${Number(row.day_change).toFixed(2)}%`;
            node.classList.toggle("pos", Number(row.day_change) >= 0);
            node.classList.toggle("neg", Number(row.day_change) < 0);
        });
    } catch (_) {
        // no-op
    }
}

function renderDealDetailFromRow(row) {
    const card = document.getElementById("deal-detail-card");
    const content = document.getElementById("deal-detail-content");
    if (!card || !content || !row) return;
    const lang = document.body?.dataset?.lang || "ru";
    const isRu = lang === "ru";
    const isUk = lang === "uk";
    const id = row.dataset.dealId || "—";
    const asset = row.dataset.asset || "—";
    const direction = row.dataset.direction === "up"
        ? (isRu ? "ВВЕРХ / ЛОНГ" : isUk ? "ВГОРУ / ЛОНГ" : "UP / LONG")
        : (isRu ? "ВНИЗ / ШОРТ" : isUk ? "ВНИЗ / ШОРТ" : "DOWN / SHORT");
    const amount = Number(row.dataset.amount || 0);
    const currency = row.dataset.currency || "USD";
    const profit = Number(row.dataset.profit || 0);
    const createdAt = row.dataset.createdAt || "—";
    const isWin = Number(row.dataset.isWin || 0) === 1;
    content.innerHTML = `
        <div class="detail-hero">
            <div>
                <strong>#${id}</strong>
                <small>${asset} · ${direction}</small>
            </div>
            <span class="detail-badge ${isWin ? "pos" : "neg"}">${isWin ? (isRu ? "ПРИБЫЛЬ" : isUk ? "ПРИБУТОК" : "PROFIT") : (isRu ? "УБЫТОК" : isUk ? "ЗБИТОК" : "LOSS")}</span>
        </div>
        <div class="detail-grid">
            <div class="detail-cell"><span>${isRu ? "Сумма" : isUk ? "Сума" : "Amount"}</span><b>${amount.toFixed(2)} ${currency}</b></div>
            <div class="detail-cell"><span>PnL</span><b class="${profit >= 0 ? "pos" : "neg"}">${profit >= 0 ? "+" : ""}${profit.toFixed(2)}</b></div>
            <div class="detail-cell"><span>${isRu ? "Открыта" : isUk ? "Відкрито" : "Opened"}</span><b>${createdAt}</b></div>
            <div class="detail-cell"><span>${isRu ? "Статус" : isUk ? "Статус" : "Status"}</span><b>${isWin ? (isRu ? "Закрыта в плюс" : isUk ? "Закрита в плюс" : "Closed in profit") : (isRu ? "Закрыта в минус" : isUk ? "Закрита в мінус" : "Closed in loss")}</b></div>
        </div>
        <div class="detail-note">
            ${isWin
                ? (isRu ? "Эта сделка закрылась в плюс, и результат был зачислен на баланс." : isUk ? "Ця угода закрилася в плюс, і результат було зараховано на баланс." : "This position closed in profit and the result was credited to the account balance.")
                : (isRu ? "Эта сделка закрылась в минус по итоговому движению рынка." : isUk ? "Ця угода закрилася в мінус за підсумковим рухом ринку." : "This position closed with a negative result according to the final market movement.")}
        </div>
    `;
    card.hidden = false;
}

function bindDealHistoryCards() {
    const rows = document.querySelectorAll(".deal-history-row");
    if (!rows.length) return;
    rows.forEach((row, index) => {
        row.addEventListener("click", () => renderDealDetailFromRow(row));
        if (index === 0) renderDealDetailFromRow(row);
    });
}

function renderMarketDetail(snapshot, sourceRow) {
    const card = document.getElementById("market-detail-card");
    const content = document.getElementById("market-detail-content");
    if (!card || !content || !snapshot) return;
    const asks = Array.isArray(snapshot.asks) ? snapshot.asks.slice(0, 3) : [];
    const bids = Array.isArray(snapshot.bids) ? snapshot.bids.slice(0, 3) : [];
    const bookHtml = [
        ...asks.map((item) => `<div class="detail-book-row ask"><span>${item.price}</span><b>${item.qty}</b></div>`),
        ...bids.map((item) => `<div class="detail-book-row bid"><span>${item.price}</span><b>${item.qty}</b></div>`),
    ].join("");
    const marketRef = encodeURIComponent(sourceRow?.dataset.marketRef || snapshot.asset_name || snapshot.symbol || "BTC");
    content.innerHTML = `
        <div class="detail-hero">
            <div>
                <strong>${snapshot.symbol || "—"}</strong>
                <small>${snapshot.asset_name || sourceRow?.dataset.marketName || "Market"}</small>
            </div>
            <span class="detail-badge ${snapshot.market_mode === "live" ? "pos" : ""}">${snapshot.market_mode === "live" ? L("market_mode_live", "Live") : L("market_mode_synthetic", "Synthetic")}</span>
        </div>
        <div class="detail-grid">
            <div class="detail-cell"><span>${L("market_mark", "Mark")}</span><b>${snapshot.mark}</b></div>
            <div class="detail-cell"><span>${L("market_spread", "Spread")}</span><b>${snapshot.spread}</b></div>
            <div class="detail-cell"><span>${L("market_high", "24H High")}</span><b>${snapshot.high}</b></div>
            <div class="detail-cell"><span>${L("market_low", "24H Low")}</span><b>${snapshot.low}</b></div>
        </div>
        <div class="detail-actions">
            <a class="detail-link" href="/trade?asset=${marketRef}">${L("quick_trade", "Open Trade")}</a>
            <a class="detail-link" href="/trade/chart?symbol=${marketRef}">${(window.LEGEND_LABELS && window.LEGEND_LABELS.trade_open_chart) || "Open Chart"}</a>
        </div>
        <div class="detail-note">
            ${snapshot.tick ? `${L("market_latest_tape", "Latest tape")}: ${(snapshot.tick.side || "").toUpperCase()} • ${snapshot.tick.price} • ${snapshot.tick.qty}` : L("market_open_from_card", "Open the chart or trade directly from this market card.")}
        </div>
        <div class="detail-book">${bookHtml}</div>
    `;
    card.hidden = false;
}

async function bindMarketDetailLive() {
    const section = document.querySelector(".market-detail-card[data-market-symbol]");
    const markEl = document.getElementById("md-mark");
    const spreadEl = document.getElementById("md-spread");
    const highEl = document.getElementById("md-high");
    const lowEl = document.getElementById("md-low");
    const noteEl = document.getElementById("md-note");
    const bookEl = document.getElementById("md-book");
    if (!section || !markEl || !spreadEl || !highEl || !lowEl || !bookEl) return;
    const symbol = section.dataset.marketSymbol || "BTC";
    const lang = uiLang();
    const notePrefix = lang === "ru"
        ? "Последний тик"
        : lang === "uk"
            ? "Останній тік"
            : "Latest tape";
    const emptyNote = lang === "ru"
        ? "Рыночные данные обновляются в реальном времени."
        : lang === "uk"
            ? "Ринкові дані оновлюються в реальному часі."
            : "Market data updates in real time.";

    const drawBook = (asks, bids) => {
        const rows = [
            ...(Array.isArray(asks) ? asks.slice(0, 3) : []).map((item) => `<div class="detail-book-row ask"><span>${item.price}</span><b>${item.qty}</b></div>`),
            ...(Array.isArray(bids) ? bids.slice(0, 3) : []).map((item) => `<div class="detail-book-row bid"><span>${item.price}</span><b>${item.qty}</b></div>`),
        ];
        if (!rows.length) return;
        bookEl.innerHTML = rows.join("");
    };

    const patch = (data) => {
        markEl.textContent = String(data.mark ?? "--");
        spreadEl.textContent = String(data.spread ?? "--");
        highEl.textContent = String(data.high ?? "--");
        lowEl.textContent = String(data.low ?? "--");
        drawBook(data.asks, data.bids);
        if (!noteEl) return;
        if (data.tick) {
            const side = String(data.tick.side || "").toUpperCase();
            noteEl.textContent = `${notePrefix}: ${side} • ${data.tick.price} • ${data.tick.qty}`;
        } else {
            noteEl.textContent = emptyNote;
        }
    };

    const poll = async () => {
        try {
            const resp = await fetch(`/api/market/snapshot?symbol=${encodeURIComponent(symbol)}`);
            if (!resp.ok) return;
            const data = await resp.json();
            if (data.ok) patch(data);
        } catch (_) {
            // no-op
        }
    };

    await poll();
    setInterval(poll, 3200);
}

function bindMarketCards() {
    const rows = document.querySelectorAll(".market-card");
    if (!rows.length) return;
    const hasDrawer = Boolean(document.getElementById("market-detail-card"));
    if (!hasDrawer) return;
    const loadSnapshot = async (row) => {
        const ref = row.dataset.marketRef || row.dataset.marketSymbol || "BTC";
        try {
            const resp = await fetch(`/api/market/snapshot?symbol=${encodeURIComponent(ref)}`);
            const data = await resp.json();
            if (!resp.ok || !data.ok) return;
            renderMarketDetail(data, row);
        } catch (_) {
            // no-op
        }
    };
    rows.forEach((row, index) => {
        row.addEventListener("click", (event) => {
            const target = event.target;
            if (target && target.closest(".watch-toggle")) return;
            if (hasDrawer && row.tagName === "A" && !event.metaKey && !event.ctrlKey && !event.shiftKey) {
                event.preventDefault();
            }
            if (hasDrawer) loadSnapshot(row);
        });
        if (index === 0) loadSnapshot(row);
    });
}

function bindMarketsToolbar() {
    const search = document.getElementById("markets-search");
    const sort = document.getElementById("markets-sort");
    const list = document.getElementById("markets-list") || document.querySelector(".list");
    if (!search || !sort || !list) return;
    const filterChips = Array.from(document.querySelectorAll(".market-filter-chip"));
    let filterMode = "all";
    const WATCH_KEY = "legend_watchlist_v1";
    const readWatchlist = () => {
        try {
            const raw = localStorage.getItem(WATCH_KEY) || "[]";
            const arr = JSON.parse(raw);
            return new Set(Array.isArray(arr) ? arr.map((x) => String(x).toUpperCase()) : []);
        } catch (_) {
            return new Set();
        }
    };
    const writeWatchlist = (set) => {
        try {
            localStorage.setItem(WATCH_KEY, JSON.stringify(Array.from(set)));
        } catch (_) {
            // no-op
        }
    };
    const watchlist = readWatchlist();

    const allCards = Array.from(list.querySelectorAll(".market-card"));
    if (!allCards.length) return;

    const normalize = (v) => String(v || "").trim().toLowerCase();
    const toNum = (v) => Number(String(v || "").replace(",", "."));

    const syncWatchButtons = () => {
        allCards.forEach((card) => {
            const btn = card.querySelector(".watch-toggle");
            if (!btn) return;
            const ref = String(btn.dataset.watchRef || card.dataset.marketRef || card.dataset.marketSymbol || "").toUpperCase();
            const active = watchlist.has(ref);
            btn.textContent = active ? "★" : "☆";
            btn.classList.toggle("active", active);
        });
    };

    const updateStats = (cards) => {
        const toSet = (id, value) => {
            const el = document.getElementById(id);
            if (el) el.textContent = value;
        };
        const all = cards.length;
        let gainers = 0;
        let losers = 0;
        let moveSum = 0;
        let top = null;
        let calm = null;
        cards.forEach((card) => {
            const ch = toNum(card.dataset.marketChange);
            moveSum += Math.abs(ch);
            if (ch >= 0) gainers += 1;
            if (ch < 0) losers += 1;
            if (!top || ch > top.change) top = { symbol: card.dataset.marketSymbol || "--", change: ch };
            if (!calm || Math.abs(ch) < Math.abs(calm.change)) calm = { symbol: card.dataset.marketSymbol || "--", change: ch };
        });
        const avgMove = all ? `${(moveSum / all).toFixed(2)}%` : "0.00%";
        toSet("markets-total", String(all));
        toSet("markets-gainers", String(gainers));
        toSet("markets-losers", String(losers));
        toSet("markets-avg-move", avgMove);
        toSet("insight-top-symbol", top ? top.symbol : "--");
        toSet("insight-top-move", top ? `${top.change >= 0 ? "+" : ""}${top.change.toFixed(2)}%` : "--");
        toSet("insight-calm-symbol", calm ? calm.symbol : "--");
        toSet("insight-calm-move", calm ? `${calm.change >= 0 ? "+" : ""}${calm.change.toFixed(2)}%` : "--");
    };

    const apply = () => {
        const q = normalize(search.value);
        const mode = String(sort.value || "change_desc");

        const cards = allCards.filter((card) => {
            const name = normalize(card.dataset.marketName);
            const symbol = normalize(card.dataset.marketSymbol);
            const baseMatch = !q || name.includes(q) || symbol.includes(q);
            if (!baseMatch) return false;
            const ch = toNum(card.dataset.marketChange);
            const ref = normalize(card.dataset.marketRef || card.dataset.marketSymbol);
            if (filterMode === "gainers") return ch >= 0;
            if (filterMode === "losers") return ch < 0;
            if (filterMode === "volatile") return Math.abs(ch) >= 1.0;
            if (filterMode === "watch") return watchlist.has(ref.toUpperCase());
            return true;
        });

        cards.sort((a, b) => {
            const aPrice = toNum(a.dataset.marketPrice);
            const bPrice = toNum(b.dataset.marketPrice);
            const aChange = toNum(a.dataset.marketChange);
            const bChange = toNum(b.dataset.marketChange);
            const aName = normalize(a.dataset.marketName || a.dataset.marketSymbol);
            const bName = normalize(b.dataset.marketName || b.dataset.marketSymbol);
            if (mode === "change_desc") return bChange - aChange;
            if (mode === "change_asc") return aChange - bChange;
            if (mode === "price_desc") return bPrice - aPrice;
            if (mode === "price_asc") return aPrice - bPrice;
            return aName.localeCompare(bName);
        });

        allCards.forEach((card) => {
            card.style.display = "none";
        });
        const oldEmpty = list.querySelector(".markets-empty-state");
        if (oldEmpty) oldEmpty.remove();
        cards.forEach((card) => {
            card.style.display = "";
            card.classList.remove("market-enter");
            void card.offsetWidth;
            card.classList.add("market-enter");
            list.appendChild(card);
        });
        if (!cards.length) {
            const holder = document.createElement("div");
            holder.className = "markets-empty-state";
            mountEmptyState(holder, "markets");
            list.appendChild(holder);
        }
        updateStats(cards);
    };

    search.addEventListener("input", apply);
    sort.addEventListener("change", apply);
    allCards.forEach((card) => {
        const btn = card.querySelector(".watch-toggle");
        if (!btn) return;
        btn.addEventListener("click", (event) => {
            event.preventDefault();
            event.stopPropagation();
            const ref = String(btn.dataset.watchRef || card.dataset.marketRef || card.dataset.marketSymbol || "").toUpperCase();
            if (!ref) return;
            if (watchlist.has(ref)) watchlist.delete(ref);
            else watchlist.add(ref);
            writeWatchlist(watchlist);
            syncWatchButtons();
            if (filterMode === "watch") apply();
        });
    });
    filterChips.forEach((chip) => {
        chip.addEventListener("click", () => {
            filterMode = chip.dataset.filterMode || "all";
            filterChips.forEach((node) => node.classList.remove("active"));
            chip.classList.add("active");
            apply();
        });
    });
    syncWatchButtons();
    apply();
}

function initOnboardingTour() {
    const overlay = document.getElementById("onboarding-overlay");
    const title = document.getElementById("onboarding-title");
    const text = document.getElementById("onboarding-text");
    const dots = document.getElementById("onboarding-dots");
    const skip = document.getElementById("onboarding-skip");
    const next = document.getElementById("onboarding-next");
    if (!overlay || !title || !text || !dots || !skip || !next) return;
    const key = "legend_onboarding_v2_done";
    try {
        if (localStorage.getItem(key) === "1") return;
    } catch (_) {
        return;
    }
    const page = document.body?.dataset?.page || "";
    if (!["home", "markets", "trade"].includes(page)) return;
    const steps = [
        { t: "Welcome", d: "Use bottom navigation to switch between dashboard, markets and trade terminal." },
        { t: "Live Metrics", d: "Watch balance, exposure and feed status in real time. Values update without reload." },
        { t: "Trade Actions", d: "Open position, then use Close / 50% / Reverse controls directly from open positions." },
    ];
    if (page === "markets") {
        steps[1] = { t: "Market Scanner", d: "Use filters, sorting and watchlist stars to build your fast access universe." };
    } else if (page === "trade") {
        steps[1] = { t: "Execution Deck", d: "Use risk templates, leverage and insight block for faster and safer entries." };
    }
    let idx = 0;
    const render = () => {
        const s = steps[idx];
        title.textContent = s.t;
        text.textContent = s.d;
        dots.innerHTML = steps.map((_, i) => `<span class="${i === idx ? "active" : ""}"></span>`).join("");
        next.textContent = idx === steps.length - 1 ? "Start" : "Next";
        overlay.hidden = false;
        requestAnimationFrame(() => overlay.classList.add("show"));
    };
    const finish = () => {
        try {
            localStorage.setItem(key, "1");
        } catch (_) {
            // no-op
        }
        overlay.classList.remove("show");
        setTimeout(() => {
            overlay.hidden = true;
        }, 220);
    };
    skip.addEventListener("click", finish);
    next.addEventListener("click", () => {
        if (idx >= steps.length - 1) {
            finish();
            return;
        }
        idx += 1;
        render();
    });
    render();
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

function bindPageTransitions() {
    const isSamePage = (url) => {
        const current = `${window.location.pathname}${window.location.search}`;
        const next = `${url.pathname}${url.search}`;
        return current === next;
    };

    const haptic = () => {
        try {
            window.Telegram?.WebApp?.HapticFeedback?.impactOccurred?.("light");
        } catch (_) {
            // no-op
        }
    };

    document.querySelectorAll('a[href]').forEach((anchor) => {
        anchor.addEventListener("click", (event) => {
            if (event.defaultPrevented) return;
            if (event.button !== 0) return;
            if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
            const href = anchor.getAttribute("href");
            if (!href || href.startsWith("#") || href.startsWith("javascript:")) return;
            if (anchor.target && anchor.target !== "_self") return;
            if (anchor.hasAttribute("download")) return;
            let url;
            try {
                url = new URL(href, window.location.origin);
            } catch (_) {
                return;
            }
            if (url.origin !== window.location.origin) return;
            if (isSamePage(url)) return;

            event.preventDefault();
            haptic();
            try {
                sessionStorage.setItem("legend_nav_transition", "1");
                if (anchor.closest(".bottom-nav")) {
                    const r = anchor.getBoundingClientRect();
                    sessionStorage.setItem("legend_nav_shared_rect", JSON.stringify({
                        x: r.left,
                        y: r.top,
                        w: r.width,
                        h: r.height,
                        text: (anchor.textContent || "").trim(),
                        ts: Date.now(),
                    }));
                    sessionStorage.setItem("legend_nav_skeleton", "1");
                }
            } catch (_) {
                // no-op
            }
            document.body.classList.add("page-leaving");
            window.setTimeout(() => {
                window.location.href = url.href;
            }, 95);
        });
    });
}

function initBottomNavMotion() {
    const nav = document.querySelector(".bottom-nav");
    if (!nav) return;
    const pill = document.getElementById("nav-active-pill");
    const active = nav.querySelector("a.active");
    if (!pill || !active) return;
    const placePill = (anchor, animate = true) => {
        const navRect = nav.getBoundingClientRect();
        const r = anchor.getBoundingClientRect();
        if (!r.width || !r.height) return;
        nav.classList.add("has-pill");
        if (!animate) {
            pill.style.transition = "none";
        }
        pill.style.width = `${r.width}px`;
        pill.style.height = `${r.height}px`;
        pill.style.transform = `translate3d(${(r.left - navRect.left).toFixed(2)}px, ${(r.top - navRect.top).toFixed(2)}px, 0)`;
        if (!animate) {
            requestAnimationFrame(() => {
                pill.style.transition = "";
            });
        }
    };

    placePill(active, false);
    window.addEventListener("resize", () => placePill(active, false));

    let prev = null;
    try {
        const raw = sessionStorage.getItem("legend_nav_shared_rect");
        if (raw) {
            prev = JSON.parse(raw);
            sessionStorage.removeItem("legend_nav_shared_rect");
        }
    } catch (_) {
        prev = null;
    }
    if (!prev || !Number.isFinite(Number(prev.ts))) return;
    if (Date.now() - Number(prev.ts) > 1600) return;
    if (!(Number(prev.w) > 0 && Number(prev.h) > 0)) return;

    const toRect = active.getBoundingClientRect();
    if (!(toRect.width > 0 && toRect.height > 0)) return;
    const ghost = document.createElement("div");
    ghost.className = "nav-shared-ghost";
    ghost.textContent = prev.text || "";
    ghost.style.left = `${Number(prev.x)}px`;
    ghost.style.top = `${Number(prev.y)}px`;
    ghost.style.width = `${Number(prev.w)}px`;
    ghost.style.height = `${Number(prev.h)}px`;
    document.body.appendChild(ghost);

    const dx = toRect.left - Number(prev.x);
    const dy = toRect.top - Number(prev.y);
    const sx = Number(prev.w) > 0 ? toRect.width / Number(prev.w) : 1;
    const sy = Number(prev.h) > 0 ? toRect.height / Number(prev.h) : 1;
    ghost.animate(
        [
            { transform: "translate3d(0,0,0) scale(1)", opacity: 0.86 },
            { transform: `translate3d(${dx}px, ${dy}px, 0) scale(${sx}, ${sy})`, opacity: 0.08 },
        ],
        { duration: 340, easing: "cubic-bezier(.2,.8,.2,1)", fill: "forwards" }
    );
    window.setTimeout(() => ghost.remove(), 380);
}

function skeletonMarkupByPage(page) {
    if (page === "trade") {
        return `
            <div class="skeleton-stack">
                <div class="skeleton-card">
                    <div class="skeleton-line" style="width:42%"></div>
                    <div class="skeleton-line" style="width:64%"></div>
                    <div class="skeleton-line" style="width:56%"></div>
                </div>
                <div class="skeleton-grid">
                    <div class="skeleton-card"><div class="skeleton-line" style="width:74%"></div><div class="skeleton-line" style="width:48%"></div></div>
                    <div class="skeleton-card"><div class="skeleton-line" style="width:72%"></div><div class="skeleton-line" style="width:42%"></div></div>
                    <div class="skeleton-card"><div class="skeleton-line" style="width:68%"></div><div class="skeleton-line" style="width:36%"></div></div>
                </div>
                <div class="skeleton-card">
                    <div class="skeleton-line" style="width:38%"></div>
                    <div class="skeleton-line" style="width:100%;height:120px;border-radius:12px"></div>
                </div>
            </div>`;
    }
    if (page === "markets") {
        return `
            <div class="skeleton-stack">
                <div class="skeleton-card">
                    <div class="skeleton-line" style="width:40%"></div>
                    <div class="skeleton-grid">
                        <div class="skeleton-line" style="height:56px"></div>
                        <div class="skeleton-line" style="height:56px"></div>
                        <div class="skeleton-line" style="height:56px"></div>
                    </div>
                </div>
                <div class="skeleton-card">
                    <div class="skeleton-chip-row">
                        <div class="skeleton-line" style="height:30px"></div>
                        <div class="skeleton-line" style="height:30px"></div>
                        <div class="skeleton-line" style="height:30px"></div>
                        <div class="skeleton-line" style="height:30px"></div>
                    </div>
                    <div class="skeleton-line" style="width:100%;height:44px;margin-top:10px"></div>
                    <div class="skeleton-line" style="width:100%;height:44px;margin-top:8px"></div>
                    <div class="skeleton-line" style="width:100%;height:44px;margin-top:8px"></div>
                </div>
            </div>`;
    }
    if (page === "profile") {
        return `
            <div class="skeleton-stack">
                <div class="skeleton-card">
                    <div class="skeleton-line" style="width:28%"></div>
                    <div class="skeleton-line" style="width:54%"></div>
                    <div class="skeleton-line" style="width:44%"></div>
                </div>
                <div class="skeleton-grid">
                    <div class="skeleton-card"><div class="skeleton-line" style="height:56px"></div></div>
                    <div class="skeleton-card"><div class="skeleton-line" style="height:56px"></div></div>
                    <div class="skeleton-card"><div class="skeleton-line" style="height:56px"></div></div>
                </div>
                <div class="skeleton-card">
                    <div class="skeleton-line" style="width:36%"></div>
                    <div class="skeleton-line" style="width:100%;height:42px;margin-top:8px"></div>
                    <div class="skeleton-line" style="width:100%;height:42px;margin-top:8px"></div>
                </div>
            </div>`;
    }
    return "";
}

function initPageSkeleton() {
    const holder = document.getElementById("page-skeleton");
    const page = document.body?.dataset?.page || "";
    if (!holder) return;
    if (!["trade", "markets", "profile"].includes(page)) {
        holder.remove();
        return;
    }
    let fromNav = false;
    try {
        fromNav = sessionStorage.getItem("legend_nav_skeleton") === "1";
        if (fromNav) sessionStorage.removeItem("legend_nav_skeleton");
    } catch (_) {
        // no-op
    }
    let firstVisit = false;
    try {
        const key = `legend_seen_${page}`;
        firstVisit = sessionStorage.getItem(key) !== "1";
        sessionStorage.setItem(key, "1");
    } catch (_) {
        firstVisit = false;
    }
    if (!fromNav && !firstVisit) {
        holder.remove();
        return;
    }
    const markup = skeletonMarkupByPage(page);
    if (!markup) {
        holder.remove();
        return;
    }
    holder.innerHTML = markup;
    holder.hidden = false;
    requestAnimationFrame(() => holder.classList.add("show"));

    const reducedMotion = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    const minMs = reducedMotion ? 220 : (fromNav ? 380 : 560);
    const started = Date.now();
    const close = () => {
        if (holder.classList.contains("done")) return;
        const wait = Math.max(0, minMs - (Date.now() - started));
        window.setTimeout(() => {
            holder.classList.add("done");
            window.setTimeout(() => holder.remove(), 260);
        }, wait);
    };
    if (document.readyState === "complete") {
        close();
    } else {
        window.addEventListener("load", close, { once: true });
    }
    window.setTimeout(close, minMs + 1400);
}

function initPageArrivalFx() {
    let fromInternalNav = false;
    try {
        fromInternalNav = sessionStorage.getItem("legend_nav_transition") === "1";
        if (fromInternalNav) sessionStorage.removeItem("legend_nav_transition");
    } catch (_) {
        // no-op
    }
    if (!fromInternalNav || !document.body) return;
    document.body.classList.add("page-enter");
    window.setTimeout(() => document.body.classList.remove("page-enter"), 260);
}

function bindWorkerPanel() {
    const wrap = document.getElementById("worker-list");
    if (!wrap) return;
    const search = document.getElementById("worker-search");
    const filter = document.getElementById("worker-filter");
    const transferTarget = document.getElementById("worker-transfer-target");
    const liveStatus = document.getElementById("worker-live-status");
    const liveMeta = document.getElementById("worker-live-meta");
    const workerId = Number(liveMeta ? liveMeta.dataset.workerId : 0);

    const patchRowState = (row, action, value) => {
        if (!row) return;
        if (action === "toggle_trade") row.dataset.tradeEnabled = row.dataset.tradeEnabled === "1" ? "0" : "1";
        if (action === "toggle_withdraw") row.dataset.withdrawEnabled = row.dataset.withdrawEnabled === "1" ? "0" : "1";
        if (action === "toggle_verified") row.dataset.verified = row.dataset.verified === "1" ? "0" : "1";
        if (action === "toggle_favorite") row.dataset.favorite = row.dataset.favorite === "1" ? "0" : "1";
        if (action === "toggle_block") row.dataset.blocked = row.dataset.blocked === "1" ? "0" : "1";
        if (action === "toggle_auto_reject_trades") row.dataset.autoRejectTrades = row.dataset.autoRejectTrades === "1" ? "0" : "1";
        if (action === "set_funnel_stage") row.dataset.funnelStage = String(value || "");
        if (action === "set_tags") row.dataset.tags = String(value || "");
    };

    const bindActionButtons = () => {
        wrap.querySelectorAll(".worker-act").forEach((btn) => {
            btn.onclick = async () => {
                const row = btn.closest(".worker-row");
                if (!row) return;
                await doUpdate(row.dataset.wcId, btn.dataset.action);
            };
        });

        wrap.querySelectorAll(".worker-prompt").forEach((btn) => {
            btn.onclick = async () => {
                const row = btn.closest(".worker-row");
                if (!row) return;
                const label = btn.dataset.label || "Value";
                const valRaw = prompt(`${label}:`);
                if (valRaw === null) return;
                const val = Number(valRaw);
                if (Number.isNaN(val)) {
                    alert("Enter a valid number");
                    return;
                }
                await doUpdate(row.dataset.wcId, btn.dataset.action, val);
            };
        });

        wrap.querySelectorAll(".worker-text").forEach((btn) => {
            btn.onclick = async () => {
                const row = btn.closest(".worker-row");
                if (!row) return;
                const label = btn.dataset.label || "Value";
                const val = prompt(`${label}:`);
                if (val === null) return;
                await doUpdate(row.dataset.wcId, btn.dataset.action, val);
            };
        });

        wrap.querySelectorAll(".worker-transfer").forEach((btn) => {
            btn.onclick = async () => {
                const row = btn.closest(".worker-row");
                if (!row) return;
                const target = transferTarget ? transferTarget.value : "";
                if (!target) {
                    alert("Select a worker in the top list first");
                    return;
                }
                await doUpdate(row.dataset.wcId, "transfer_worker", Number(target));
            };
        });
    };

    const applyWorkerFilters = () => {
        const q = (search ? search.value : "").trim().toLowerCase();
        const mode = filter ? filter.value : "all";
        wrap.querySelectorAll(".worker-row").forEach((row) => {
            const haystack = String(row.dataset.search || "").toLowerCase();
            const matchesQuery = !q || haystack.includes(q);
            let matchesFilter = true;
            if (mode === "favorite") matchesFilter = row.dataset.favorite === "1";
            if (mode === "blocked") matchesFilter = row.dataset.blocked === "1";
            if (mode === "verified") matchesFilter = row.dataset.verified === "1";
            if (mode === "trade_off") matchesFilter = row.dataset.tradeEnabled === "0";
            if (mode === "withdraw_off") matchesFilter = row.dataset.withdrawEnabled === "0";
            if (mode === "vip") matchesFilter = String(row.dataset.funnelStage || "") === "vip" || String(row.dataset.tags || "").toLowerCase().includes("vip");
            if (mode === "support") matchesFilter = String(row.dataset.funnelStage || "") === "support_wait";
            row.style.display = matchesQuery && matchesFilter ? "" : "none";
        });
    };

    const doUpdate = async (wcId, action, value = null) => {
        const resp = await fetch("/api/worker/client/update", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ wc_id: Number(wcId), action, value }),
        });
        const data = await resp.json();
        if (!resp.ok || !data.ok) {
            alert(data.error || "Failed to update data");
            return false;
        }
        patchRowState(wrap.querySelector(`.worker-row[data-wc-id="${wcId}"]`), action, value);
        await pollWorkerDashboard();
        return true;
    };

    const renderClients = (items) => {
        wrap.innerHTML = "";
        if (!items || !items.length) {
            wrap.innerHTML = '<div class="empty">No referrals yet.</div>';
            return;
        }
        items.forEach((c) => {
            const row = document.createElement("div");
            row.className = "row worker-row";
            row.dataset.wcId = c.id;
            row.dataset.search = `#${c.id} ${c.first_name || "User"} ${c.client_tg_id} ${c.username || ""}`;
            row.dataset.favorite = c.favorite ? "1" : "0";
            row.dataset.blocked = c.blocked ? "1" : "0";
            row.dataset.verified = c.verified ? "1" : "0";
            row.dataset.tradeEnabled = c.trading_enabled ? "1" : "0";
            row.dataset.withdrawEnabled = c.withdraw_enabled ? "1" : "0";
            row.dataset.autoRejectTrades = c.auto_reject_trades ? "1" : "0";
            row.dataset.funnelStage = c.funnel_stage || "new";
            row.dataset.tags = c.tags || "";
            row.innerHTML = `
                <a class="worker-card-link worker-card-main" href="/worker/client/${c.id}">
                    <div class="worker-head">
                        <b>#${c.id} ${c.first_name || "User"}</b>
                        <span class="worker-badge ${c.blocked ? "blocked" : c.favorite ? "favorite" : "active"}">${c.blocked ? "Blocked" : c.favorite ? "Favorite" : "Active"}</span>
                    </div>
                    <small>ID ${c.client_tg_id} · @${c.username || "-"}</small>
                    <small>Balance: ${Number(c.balance || 0).toFixed(2)} ${c.currency || "USD"}</small>
                    <div class="worker-meta">
                        <span>Min deposit: ${Number(c.min_deposit || 0).toFixed(2)}</span>
                        <span>Min withdraw: ${Number(c.min_withdraw || 0).toFixed(2)}</span>
                        <span>Min trade: ${Number(c.min_trade_amount || 100).toFixed(2)}</span>
                        <span>Coeff: ${Number(c.trade_coefficient || 1).toFixed(2)}</span>
                        <span>${c.auto_reject_trades ? "Auto-reject: ON" : "Auto-reject: OFF"}</span>
                        <span>Luck: ${Number(c.luck_percent || 0).toFixed(2)}%</span>
                        <span>Stage: ${c.funnel_stage || "new"}</span>
                        ${c.tags ? `<span>Tags: ${c.tags}</span>` : ""}
                    </div>
                    ${c.crm_note ? `<div class="crm-note-preview">${c.crm_note}</div>` : ""}
                </a>
                <div class="worker-actions">
                    <button class="chip worker-act ${c.trading_enabled ? "state-on" : "state-off"}" data-action="toggle_trade">Trade</button>
                    <button class="chip worker-act ${c.withdraw_enabled ? "state-on" : "state-off"}" data-action="toggle_withdraw">Withdraw</button>
                    <button class="chip worker-act ${c.verified ? "state-on" : "state-off"}" data-action="toggle_verified">KYC</button>
                    <button class="chip worker-act ${c.favorite ? "state-fav" : "state-off"}" data-action="toggle_favorite">Favorite</button>
                    <button class="chip worker-act ${c.blocked ? "state-block" : "state-on"}" data-action="toggle_block">${c.blocked ? "Unblock" : "Block"}</button>
                    <button class="chip worker-prompt" data-action="set_luck" data-label="Luck 0-100">Luck</button>
                    <button class="chip worker-prompt" data-action="set_min_deposit" data-label="Minimum deposit">Min dep</button>
                    <button class="chip worker-prompt" data-action="set_min_withdraw" data-label="Minimum withdraw">Min wd</button>
                    <button class="chip worker-prompt" data-action="add_balance" data-label="Add balance">Balance+</button>
                    <button class="chip worker-prompt" data-action="subtract_balance" data-label="Subtract balance">Balance-</button>
                    <button class="chip worker-prompt" data-action="set_balance" data-label="Set balance">Set bal</button>
                    <button class="chip worker-prompt" data-action="set_min_trade_amount" data-label="Minimum trade amount">Min trade</button>
                    <button class="chip worker-prompt" data-action="set_trade_coefficient" data-label="Trade coefficient">Coeff</button>
                    <button class="chip worker-act ${c.auto_reject_trades ? "state-block" : "state-on"}" data-action="toggle_auto_reject_trades">Auto-reject</button>
                    <button class="chip worker-text" data-action="set_funnel_stage" data-label="Funnel stage">Stage</button>
                    <button class="chip worker-text" data-action="set_tags" data-label="Tags comma-separated">Tags</button>
                    <button class="chip worker-text" data-action="set_note" data-label="Client note">Note</button>
                    <button class="chip worker-transfer">Transfer</button>
                    <a class="chip worker-open" href="/worker/client/${c.id}">Card</a>
                </div>
            `;
            wrap.appendChild(row);
        });
        bindActionButtons();
        applyWorkerFilters();
    };

    const renderSummary = (summary) => {
        const grid = document.getElementById("worker-summary-grid");
        if (!grid || !summary) return;
        const cards = grid.querySelectorAll(".stat-card b");
        if (cards[0]) cards[0].textContent = Number(summary.total_clients || 0);
        if (cards[1]) cards[1].textContent = Number(summary.active_day || 0);
        if (cards[2]) cards[2].textContent = Number(summary.deposited || 0);
        if (cards[3]) cards[3].textContent = `${Number(summary.vip || 0)}/${Number(summary.trading || 0)}`;
    };

    const renderActivity = (items) => {
        const box = document.getElementById("worker-activity-list");
        if (!box) return;
        box.innerHTML = "";
        if (!items || !items.length) {
            box.innerHTML = '<div class="empty">No events yet.</div>';
            return;
        }
        items.forEach((item) => {
            const row = document.createElement("div");
            row.className = "row worker-event-row";
            row.innerHTML = `
                <div class="worker-event-main">
                    <div class="worker-event-top">
                        <b>${item.title || "Event"}</b>
                        <span class="worker-event-time">${item.created_at || ""}</span>
                    </div>
                    <small>${item.first_name || "User"} · ID ${item.client_tg_id || "-"} · @${item.username || "-"}</small>
                    <small>${item.details || "No details"}</small>
                </div>
                ${item.amount !== null && item.amount !== undefined ? `<div class="worker-event-amount">${Number(item.amount).toFixed(2)} ${item.currency || ""}</div>` : ""}
            `;
            box.appendChild(row);
        });
    };

    const renderSupport = (items) => {
        const box = document.getElementById("worker-support-list");
        if (!box) return;
        box.innerHTML = "";
        if (!items || !items.length) {
            box.innerHTML = '<div class="empty">No open tickets yet.</div>';
            return;
        }
        items.forEach((item) => {
            const row = document.createElement("div");
            row.className = "row support-row";
            const badgeClass = item.status === "closed" ? "blocked" : item.status === "in_progress" ? "favorite" : "active";
            row.innerHTML = `
                <div class="worker-event-main">
                    <div class="worker-event-top">
                        <b>#${item.id} ${item.subject || "Ticket"}</b>
                        <span class="worker-event-time">${item.updated_at || ""}</span>
                    </div>
                    <small>${item.first_name || "User"} · ID ${item.client_tg_id || "-"} · @${item.username || "-"}</small>
                    <small>${item.last_message || "No comment"}</small>
                </div>
                <div class="worker-badge ${badgeClass}">${item.status || "new"}</div>
            `;
            box.appendChild(row);
        });
    };

    const pollWorkerDashboard = async () => {
        try {
            const resp = await fetch("/api/worker/dashboard");
            if (!resp.ok) throw new Error("dashboard");
            const data = await resp.json();
            if (!data.ok) throw new Error(data.error || "dashboard");
            renderClients(data.clients);
            renderSummary(data.summary);
            renderActivity(data.activity);
            renderSupport(data.tickets);
            if (liveStatus) liveStatus.textContent = L("worker_feed_online", "CRM feed: online");
        } catch (err) {
            if (liveStatus) liveStatus.textContent = L("worker_feed_reconnect", "CRM feed: reconnect");
        }
    };

    if (search) search.addEventListener("input", applyWorkerFilters);
    if (filter) filter.addEventListener("change", applyWorkerFilters);
    bindActionButtons();
    applyWorkerFilters();

    let fallbackTimer = null;
    const startFallback = () => {
        if (!fallbackTimer) fallbackTimer = setInterval(pollWorkerDashboard, 8000);
    };
    const stopFallback = () => {
        if (fallbackTimer) clearInterval(fallbackTimer);
        fallbackTimer = null;
    };
    if (workerId > 0) {
        const proto = window.location.protocol === "https:" ? "wss" : "ws";
        const ws = new WebSocket(`${proto}://${window.location.host}/ws/live`);
        ws.addEventListener("open", () => {
            stopFallback();
            ws.send(JSON.stringify({ scope: "worker", tg_id: workerId }));
        });
        ws.addEventListener("message", (event) => {
            try {
                const data = JSON.parse(event.data);
                if (!data.ok) return;
                renderClients(data.clients);
                renderSummary(data.summary);
                renderActivity(data.activity);
                renderSupport(data.tickets);
                if (liveStatus) liveStatus.textContent = L("worker_feed_live", "CRM feed: live");
            } catch (_) {}
        });
        ws.addEventListener("close", () => {
            if (liveStatus) liveStatus.textContent = L("worker_feed_polling", "CRM feed: polling");
            startFallback();
        });
        ws.addEventListener("error", () => {
            if (liveStatus) liveStatus.textContent = L("worker_feed_reconnect", "CRM feed: reconnect");
            startFallback();
        });
    } else {
        startFallback();
    }
}

function bindWorkerClientPage() {
    const activityBox = document.getElementById("worker-client-activity");
    if (!activityBox) return;
    const liveStatus = document.getElementById("worker-client-live-status");
    const liveMeta = document.getElementById("worker-client-live-meta");
    const wcId = Number(liveMeta ? liveMeta.dataset.wcId : 0);
    const workerId = Number(liveMeta ? liveMeta.dataset.workerId : 0);
    if (!wcId || !workerId) return;
    const controls = document.getElementById("worker-client-controls");
    const resultBox = document.getElementById("worker-client-control-result");

    const doClientUpdate = async (action, value = null) => {
        const resp = await fetch("/api/worker/client/update", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ wc_id: wcId, action, value }),
        });
        const data = await resp.json();
        if (resultBox) {
            resultBox.innerHTML = resp.ok && data.ok
                ? `<span class="pos">${data.details || "Saved"}</span>`
                : `<span class="neg">${data.error || "Error"}</span>`;
        }
        if (!resp.ok || !data.ok) return false;
        await pollSnapshot();
        return true;
    };

    const bindClientControls = () => {
        if (!controls) return;
        controls.querySelectorAll(".worker-client-toggle").forEach((btn) => {
            btn.onclick = async () => {
                await doClientUpdate(btn.dataset.action);
            };
        });
        controls.querySelectorAll(".worker-client-prompt").forEach((btn) => {
            btn.onclick = async () => {
                const raw = prompt(`${btn.dataset.label || "Value"}:`);
                if (raw === null) return;
                const value = Number(raw);
                if (Number.isNaN(value)) {
                    if (resultBox) resultBox.innerHTML = '<span class="neg">Enter a valid number</span>';
                    return;
                }
                await doClientUpdate(btn.dataset.action, value);
            };
        });
        controls.querySelectorAll(".worker-client-text").forEach((btn) => {
            btn.onclick = async () => {
                const value = prompt(`${btn.dataset.label || "Value"}:`);
                if (value === null) return;
                await doClientUpdate(btn.dataset.action, value);
            };
        });
    };

    const renderItems = (box, items, emptyText, mapper) => {
        if (!box) return;
        box.innerHTML = "";
        if (!items || !items.length) {
            box.innerHTML = `<div class="empty">${emptyText}</div>`;
            return;
        }
        items.forEach((item) => box.appendChild(mapper(item)));
    };

    const pollSnapshot = async () => {
        try {
            const resp = await fetch(`/api/worker/client/${wcId}/snapshot`);
            if (!resp.ok) throw new Error("snapshot");
            const data = await resp.json();
            if (!data.ok) throw new Error(data.error || "snapshot");
            renderItems(activityBox, data.activity, "No events yet.", (item) => {
                const row = document.createElement("div");
                row.className = "row worker-event-row";
                row.innerHTML = `
                    <div class="worker-event-main">
                        <div class="worker-event-top">
                            <b>${item.title || "Event"}</b>
                            <span class="worker-event-time">${item.created_at || ""}</span>
                        </div>
                        <small>${item.details || "No details"}</small>
                        <small>Source: ${item.actor_source || "-"} · Type: ${item.event_type || "-"}</small>
                    </div>
                    ${item.amount !== null && item.amount !== undefined ? `<div class="worker-event-amount">${Number(item.amount).toFixed(2)} ${item.currency || ""}</div>` : ""}
                `;
                return row;
            });
            renderItems(document.getElementById("worker-client-support"), data.tickets, "No tickets yet.", (item) => {
                const row = document.createElement("div");
                row.className = "row support-row";
                row.innerHTML = `
                    <div class="worker-event-main">
                        <div class="worker-event-top">
                            <b>#${item.id} ${item.subject || "Ticket"}</b>
                            <span class="worker-event-time">${item.updated_at || ""}</span>
                        </div>
                        <small>${item.topic || "-"} · ${item.status || "new"}</small>
                        <small>${item.last_message || "No comment"}</small>
                    </div>
                `;
                return row;
            });
            const luckBox = document.getElementById("worker-client-luck");
            if (luckBox) luckBox.textContent = `${Number(data.luck || 0).toFixed(2)}%`;
            const client = data.client || {};
            const balanceBox = document.getElementById("worker-client-balance");
            if (balanceBox) balanceBox.textContent = `${Number(client.balance || 0).toFixed(2)} ${client.currency || "USD"}`;
            const minDepBox = document.getElementById("worker-client-min-deposit");
            if (minDepBox) minDepBox.textContent = `Min deposit: ${Number(client.min_deposit || 0).toFixed(2)}`;
            const minWdBox = document.getElementById("worker-client-min-withdraw");
            if (minWdBox) minWdBox.textContent = `Min withdraw: ${Number(client.min_withdraw || 0).toFixed(2)}`;
            const minTradeBox = document.getElementById("worker-client-min-trade");
            if (minTradeBox) minTradeBox.textContent = `Min trade: ${Number(client.min_trade_amount || 100).toFixed(2)}`;
            const tradeCoefficientBox = document.getElementById("worker-client-trade-coefficient");
            if (tradeCoefficientBox) tradeCoefficientBox.textContent = `Coeff: ${Number(client.trade_coefficient || 1).toFixed(2)}`;
            const autoRejectBox = document.getElementById("worker-client-auto-reject");
            if (autoRejectBox) autoRejectBox.textContent = `Auto-reject: ${client.auto_reject_trades ? "ON" : "OFF"}`;
            const stageBox = document.getElementById("worker-client-stage");
            if (stageBox) stageBox.textContent = `Stage: ${client.funnel_stage || "new"}`;
            const verifiedBox = document.getElementById("worker-client-verified");
            if (verifiedBox) verifiedBox.textContent = `KYC: ${client.verified ? "ON" : "OFF"}`;
            const tradingBox = document.getElementById("worker-client-trading");
            if (tradingBox) tradingBox.textContent = `Trade: ${client.trading_enabled ? "ON" : "OFF"}`;
            const withdrawBox = document.getElementById("worker-client-withdraw");
            if (withdrawBox) withdrawBox.textContent = `Withdraw: ${client.withdraw_enabled ? "ON" : "OFF"}`;
            const favoriteBox = document.getElementById("worker-client-favorite");
            if (favoriteBox) favoriteBox.textContent = `Favorite: ${client.favorite ? "YES" : "NO"}`;
            const blockedBox = document.getElementById("worker-client-blocked");
            if (blockedBox) blockedBox.textContent = `Block: ${client.blocked ? "YES" : "NO"}`;
            if (liveStatus) liveStatus.textContent = L("client_feed_online", "Client feed: online");
        } catch (err) {
            if (liveStatus) liveStatus.textContent = L("client_feed_reconnect", "Client feed: reconnect");
        }
    };

    let fallbackTimer = null;
    const startFallback = () => {
        if (!fallbackTimer) fallbackTimer = setInterval(pollSnapshot, 9000);
    };
    const stopFallback = () => {
        if (fallbackTimer) clearInterval(fallbackTimer);
        fallbackTimer = null;
    };
    const proto = window.location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${proto}://${window.location.host}/ws/live`);
    ws.addEventListener("open", () => {
        stopFallback();
        ws.send(JSON.stringify({ scope: "worker_client", tg_id: workerId, wc_id: wcId }));
    });
    ws.addEventListener("message", (event) => {
        try {
            const data = JSON.parse(event.data);
            if (!data.ok) return;
            renderItems(activityBox, data.activity, "No events yet.", (item) => {
                const row = document.createElement("div");
                row.className = "row worker-event-row";
                row.innerHTML = `
                    <div class="worker-event-main">
                        <div class="worker-event-top">
                            <b>${item.title || "Event"}</b>
                            <span class="worker-event-time">${item.created_at || ""}</span>
                        </div>
                        <small>${item.details || "No details"}</small>
                        <small>Source: ${item.actor_source || "-"} · Type: ${item.event_type || "-"}</small>
                    </div>
                    ${item.amount !== null && item.amount !== undefined ? `<div class="worker-event-amount">${Number(item.amount).toFixed(2)} ${item.currency || ""}</div>` : ""}
                `;
                return row;
            });
            renderItems(document.getElementById("worker-client-support"), data.tickets, "No tickets yet.", (item) => {
                const row = document.createElement("div");
                row.className = "row support-row";
                row.innerHTML = `
                    <div class="worker-event-main">
                        <div class="worker-event-top">
                            <b>#${item.id} ${item.subject || "Ticket"}</b>
                            <span class="worker-event-time">${item.updated_at || ""}</span>
                        </div>
                        <small>${item.topic || "-"} · ${item.status || "new"}</small>
                        <small>${item.last_message || "No comment"}</small>
                    </div>
                `;
                return row;
            });
            const luckBox = document.getElementById("worker-client-luck");
            if (luckBox) luckBox.textContent = `${Number(data.luck || 0).toFixed(2)}%`;
            const client = data.client || {};
            const balanceBox = document.getElementById("worker-client-balance");
            if (balanceBox) balanceBox.textContent = `${Number(client.balance || 0).toFixed(2)} ${client.currency || "USD"}`;
            const minDepBox = document.getElementById("worker-client-min-deposit");
            if (minDepBox) minDepBox.textContent = `Min deposit: ${Number(client.min_deposit || 0).toFixed(2)}`;
            const minWdBox = document.getElementById("worker-client-min-withdraw");
            if (minWdBox) minWdBox.textContent = `Min withdraw: ${Number(client.min_withdraw || 0).toFixed(2)}`;
            const minTradeBox = document.getElementById("worker-client-min-trade");
            if (minTradeBox) minTradeBox.textContent = `Min trade: ${Number(client.min_trade_amount || 100).toFixed(2)}`;
            const tradeCoefficientBox = document.getElementById("worker-client-trade-coefficient");
            if (tradeCoefficientBox) tradeCoefficientBox.textContent = `Coeff: ${Number(client.trade_coefficient || 1).toFixed(2)}`;
            const autoRejectBox = document.getElementById("worker-client-auto-reject");
            if (autoRejectBox) autoRejectBox.textContent = `Auto-reject: ${client.auto_reject_trades ? "ON" : "OFF"}`;
            const stageBox = document.getElementById("worker-client-stage");
            if (stageBox) stageBox.textContent = `Stage: ${client.funnel_stage || "new"}`;
            const verifiedBox = document.getElementById("worker-client-verified");
            if (verifiedBox) verifiedBox.textContent = `KYC: ${client.verified ? "ON" : "OFF"}`;
            const tradingBox = document.getElementById("worker-client-trading");
            if (tradingBox) tradingBox.textContent = `Trade: ${client.trading_enabled ? "ON" : "OFF"}`;
            const withdrawBox = document.getElementById("worker-client-withdraw");
            if (withdrawBox) withdrawBox.textContent = `Withdraw: ${client.withdraw_enabled ? "ON" : "OFF"}`;
            const favoriteBox = document.getElementById("worker-client-favorite");
            if (favoriteBox) favoriteBox.textContent = `Favorite: ${client.favorite ? "YES" : "NO"}`;
            const blockedBox = document.getElementById("worker-client-blocked");
            if (blockedBox) blockedBox.textContent = `Block: ${client.blocked ? "YES" : "NO"}`;
            if (liveStatus) liveStatus.textContent = L("client_feed_live", "Client feed: live");
        } catch (_) {}
    });
    ws.addEventListener("close", () => {
        if (liveStatus) liveStatus.textContent = L("client_feed_polling", "Client feed: polling");
        startFallback();
    });
    ws.addEventListener("error", () => {
        if (liveStatus) liveStatus.textContent = L("client_feed_reconnect", "Client feed: reconnect");
        startFallback();
    });
    bindClientControls();
}

function renderTape(items) {
    const wrap = document.getElementById("market-tape-list");
    const pulse = document.getElementById("market-pulse");
    if (!wrap) return;
    wrap.innerHTML = "";
    if (!Array.isArray(items) || !items.length) {
        mountEmptyState(wrap, "tape");
        if (pulse) pulse.innerHTML = "";
        return;
    }
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
    const flowText = document.getElementById("market-flow");
    const participantsText = document.getElementById("market-participants");
    if (Array.isArray(items) && items.length) {
        const nowSec = Math.floor(Date.now() / 1000);
        const inMin = items.filter((x) => Math.abs(nowSec - Number(x.ts || nowSec)) <= 60);
        const tpm = inMin.length || Math.min(20, items.length);
        const symbols = new Set(items.map((x) => String(x.symbol || "").toUpperCase()).filter(Boolean)).size;
        if (flowText) flowText.textContent = `${tpm} trades/min`;
        if (participantsText) participantsText.textContent = `${symbols} symbols active`;
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

applyRuntimeProfile();
initAppPreloader();
initTelegramAuth();
bindDirectionButtons();
bindTradeControls();
bindTradeForm();
bindExchangeForm();
bindDepositForm();
bindDepositMethodCards();
bindLangSwitch();
bindWorkerPanel();
bindWorkerClientPage();
bindMarketsToolbar();
bindMarketCards();
bindMarketDetailLive();
bindMarketSocket();
bindUserSocket();
bindOpenPositionsActions();
bindMarketMiniCharts();
hydrateInitialTradeState();
bindTradeQuickActions();
bindDealHistoryCards();
initInteractiveFeedback();
initPageSkeleton();
initPageIntroRibbon();
bindPageTransitions();
initBottomNavMotion();
initPageArrivalFx();
initOnboardingTour();

const tapeWrap = document.getElementById("market-tape-list");
if (tapeWrap) {
    refreshTape();
    setInterval(refreshTape, 4200);
}

const openPosWrap = document.getElementById("open-positions-list");
if (openPosWrap) {
    refreshOpenPositions();
    setInterval(refreshOpenPositions, 3000);
}

const hasMarketRows = document.querySelector(".m-price[data-symbol], .m-change[data-symbol]");
if (hasMarketRows) {
    refreshMarketsLive();
    setInterval(refreshMarketsLive, 2400);
}

requestAnimationFrame(() => document.body.classList.add("app-ready"));
