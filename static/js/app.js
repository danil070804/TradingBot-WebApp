async function updateOverviewMetrics() {
    const hasMetrics = document.getElementById("m-users");
    if (!hasMetrics) {
        return;
    }
    try {
        const response = await fetch("/api/overview");
        if (!response.ok) {
            return;
        }
        const data = await response.json();
        document.getElementById("m-users").textContent = data.users_count;
        document.getElementById("m-workers").textContent = data.workers_count;
        document.getElementById("m-deals").textContent = data.deals_count;
        document.getElementById("m-balance").textContent = Number(data.total_balance).toFixed(2);
        document.getElementById("m-pnl").textContent = Number(data.total_pnl).toFixed(2);
        document.getElementById("m-w-pending").textContent = data.pending_withdrawals_count;
    } catch (_) {
        // silent refresh fail
    }
}

updateOverviewMetrics();
setInterval(updateOverviewMetrics, 15000);
