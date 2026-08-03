const HTTP_BASE_URL = "http://localhost:8080/api/v1";
const WS_URL = "ws://localhost:8080/ws";

let userId = null;
let balance = 0.0;
let markets = [];
let selectedMarketIdx = -1;
let activeSymbol = "";

let ws = null;
let priceHistory = [];
let bids = [];
let asks = [];
let trades = [];

const screens = {
  login: document.getElementById("login-screen"),
  dashboard: document.getElementById("dashboard-screen"),
  trading: document.getElementById("trading-screen")
};

function showScreen(name) {
  Object.keys(screens).forEach(key => {
    screens[key].style.display = key === name ? "flex" : "none";
  });
}

async function generateUuid(username) {
  const nil = new Uint8Array(16);

  const encoder = new TextEncoder();
  const nameBytes = encoder.encode(username);

  const combined = new Uint8Array(nil.length + nameBytes.length);
  combined.set(nil, 0);
  combined.set(nameBytes, nil.length);

  const hashBuffer = await crypto.subtle.digest("SHA-1", combined);
  const hash = new Uint8Array(hashBuffer);

  hash[6] = (hash[6] & 0x0f) | 0x50;
  hash[8] = (hash[8] & 0x3f) | 0x80;

  const hex = Array.from(hash.slice(0, 16)).map(b => b.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}

document.getElementById("login-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const username = document.getElementById("username").value.trim();
  const log = document.getElementById("login-log");

  if (!username) {
    log.textContent = "Username cannot be empty.";
    return;
  }

  log.textContent = "Logging in...";
  userId = await generateUuid(username);

  try {
    const balRes = await fetch(`${HTTP_BASE_URL}/accounts/${userId}/balance`);
    if (balRes.ok) {
      const balData = await balRes.json();
      balance = parseFloat(balData.available_balance || "0.0");
    } else {
      const depRes = await fetch(`${HTTP_BASE_URL}/accounts/deposit`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: userId, amount: "10000.00" })
      });
      if (!depRes.ok) {
        throw new Error("Auth request failed.");
      }
      const depData = await depRes.json();
      balance = parseFloat(depData.new_balance || "0.0");
    }

    await fetchMarkets();
    showScreen("dashboard");
    renderDashboard();
  } catch (err) {
    log.textContent = "Error: " + (err.message || "Network request failed. Is the backend running?");
  }
});

async function fetchMarkets() {
  try {
    const res = await fetch(`${HTTP_BASE_URL}/markets`);
    if (res.ok) {
      markets = await res.json();
    }
  } catch (err) {
    console.error("Failed to load markets:", err);
  }
}

setInterval(async () => {
  if (userId && screens.dashboard.style.display !== "none") {
    await fetchMarkets();
    renderDashboard();
  }
}, 10000);

function renderDashboard() {
  document.getElementById("user-id-display").textContent = "User: " + userId;
  document.getElementById("balance-display").textContent = `$${balance.toFixed(2)} USDT`;

  const tbody = document.getElementById("markets-tbody");
  tbody.innerHTML = "";

  markets.forEach((m, idx) => {
    const tr = document.createElement("tr");
    if (idx === selectedMarketIdx) {
      tr.classList.add("selected");
    }

    tr.innerHTML = `
      <td>${m.symbol}</td>
      <td>${m.base_asset}</td>
      <td>${m.quote_asset}</td>
      <td>${m.tick_size}</td>
      <td>${m.lot_size}</td>
      <td>${m.status}</td>
    `;

    tr.addEventListener("click", () => {
      selectedMarketIdx = idx;
      renderDashboard();

      const btn = document.getElementById("btn-enter-market");
      btn.classList.remove("btn-disabled");
      btn.disabled = false;
    });

    tr.addEventListener("dblclick", () => {
      selectedMarketIdx = idx;
      enterTradingDesk();
    });

    tbody.appendChild(tr);
  });
}

document.getElementById("btn-enter-market").addEventListener("click", () => {
  if (selectedMarketIdx >= 0) {
    enterTradingDesk();
  }
});

document.getElementById("btn-logout").addEventListener("click", () => {
  userId = null;
  document.getElementById("username").value = "";
  document.getElementById("password").value = "";
  document.getElementById("login-log").textContent = "";
  selectedMarketIdx = -1;
  const btn = document.getElementById("btn-enter-market");
  btn.classList.add("btn-disabled");
  btn.disabled = true;
  showScreen("login");
});

function enterTradingDesk() {
  const m = markets[selectedMarketIdx];
  activeSymbol = m.symbol;

  priceHistory = Array(50).fill(64250.0);
  bids = [];
  asks = [];
  trades = [];

  document.getElementById("trading-symbol").textContent = activeSymbol;
  document.getElementById("trading-price").textContent = "-";
  document.getElementById("trading-balance").textContent = `$${balance.toFixed(2)} USDT`;
  document.getElementById("order-price").value = "64250";
  document.getElementById("order-qty").value = "0.1";
  document.getElementById("order-log").textContent = "";

  showScreen("trading");
  initWebsocket();
}

document.getElementById("btn-back-to-dashboard").addEventListener("click", () => {
  if (ws) {
    ws.close();
    ws = null;
  }
  showScreen("dashboard");
  renderDashboard();
});

document.querySelectorAll('input[name="side"]').forEach(radio => {
  radio.addEventListener("change", (e) => {
    document.querySelectorAll('input[name="side"]').forEach(el => {
      el.parentElement.classList.remove("active");
    });
    e.target.parentElement.classList.add("active");
  });
});

document.querySelectorAll('input[name="order_type"]').forEach(radio => {
  radio.addEventListener("change", (e) => {
    document.querySelectorAll('input[name="order_type"]').forEach(el => {
      el.parentElement.classList.remove("active");
    });
    e.target.parentElement.classList.add("active");

    const priceField = document.getElementById("price-field");
    if (e.target.value === "MARKET") {
      priceField.style.display = "none";
    } else {
      priceField.style.display = "flex";
    }
  });
});

document.getElementById("order-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const log = document.getElementById("order-log");
  log.textContent = "Submitting order...";

  const side = document.querySelector('input[name="side"]:checked').value;
  const orderType = document.querySelector('input[name="order_type"]:checked').value;
  const qty = document.getElementById("order-qty").value;
  const price = orderType === "LIMIT" ? document.getElementById("order-price").value : null;

  const body = {
    user_id: userId,
    symbol: activeSymbol,
    side: side,
    order_type: orderType,
    quantity: qty,
    price: price,
    trigger_price: null,
    time_in_force: "GTC",
    leverage: 10,
    margin_mode: "ISOLATED",
    reduce_only: false,
    post_only: false
  };

  try {
    const res = await fetch(`${HTTP_BASE_URL}/orders`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    const text = await res.text();
    log.textContent = "Response: " + text;

    const balRes = await fetch(`${HTTP_BASE_URL}/accounts/${userId}/balance`);
    if (balRes.ok) {
      const balData = await balRes.json();
      balance = parseFloat(balData.available_balance || "0.0");
      document.getElementById("trading-balance").textContent = `$${balance.toFixed(2)} USDT`;
    }
  } catch (err) {
    log.textContent = "Error: " + err.message;
  }
});

function initWebsocket() {
  if (ws) ws.close();

  ws = new WebSocket(WS_URL);

  ws.onopen = () => {
    const subMsg = {
      action: "subscribe",
      channels: [
        `orderbook:${activeSymbol}`,
        `trades:${activeSymbol}`
      ]
    };
    ws.send(JSON.stringify(subMsg));
  };

  ws.onmessage = (event) => {
    try {
      const val = JSON.parse(event.data);
      if (val.bids || val.asks) {
        if (val.bids) bids = val.bids;
        if (val.asks) asks = val.asks;
        renderOrderbook();
      } else if (val.taker_side) {
        const time = new Date().toLocaleTimeString();
        const p = parseFloat(val.price || "0.0");
        const q = parseFloat(val.quantity || "0.0");
        const side = val.taker_side;

        document.getElementById("trading-price").textContent = `$${p.toFixed(2)}`;

        priceHistory.push(p);
        if (priceHistory.length > 50) {
          priceHistory.shift();
        }

        trades.unshift({ time, p, q, side });
        if (trades.length > 20) {
          trades.pop();
        }

        renderTrades();
        drawChart();
      }
    } catch (e) {
      console.error("WS Parse Error:", e);
    }
  };
}

function renderOrderbook() {
  const asksDiv = document.getElementById("asks-list");
  const bidsDiv = document.getElementById("bids-list");

  asksDiv.innerHTML = "";
  bidsDiv.innerHTML = "";

  const renderSide = (list, parent, maxQty, isBids) => {
    list.slice(0, 8).forEach(level => {
      const p = parseFloat(level[0]);
      const q = parseFloat(level[1]);

      const row = document.createElement("div");
      row.className = "orderbook-row";

      const pct = Math.min((q / maxQty) * 100, 100);
      const bg = isBids ? "rgba(74, 222, 128, 0.15)" : "rgba(248, 113, 113, 0.15)";
      row.style.background = `linear-gradient(${isBids ? 'left' : 'right'}, ${bg} ${pct}%, transparent ${pct}%)`;
      row.style.background = `linear-gradient(${isBids ? 'to right' : 'to left'}, ${bg} ${pct}%, transparent ${pct}%)`;

      row.innerHTML = `
        <span>${p.toFixed(2)}</span>
        <span>${q.toFixed(4)}</span>
      `;
      parent.appendChild(row);
    });
  };

  const getLimit = (list) => Math.max(...list.slice(0, 8).map(l => parseFloat(l[1])), 1.0);

  if (asks.length > 0) {
    const list = [...asks].slice(0, 8).reverse();
    renderSide(list, asksDiv, getLimit(asks), false);
  }
  if (bids.length > 0) {
    renderSide(bids, bidsDiv, getLimit(bids), true);
  }
}

function renderTrades() {
  const container = document.getElementById("trades-list");
  container.innerHTML = "";

  trades.forEach(t => {
    const row = document.createElement("div");
    row.className = `trade-row ${t.side.toLowerCase()}`;
    row.innerHTML = `
      <span>${t.time}</span>
      <span>${t.side}</span>
      <span>${t.p.toFixed(2)}</span>
      <span>${t.q.toFixed(4)}</span>
    `;
    container.appendChild(row);
  });
}

function drawChart() {
  const canvas = document.getElementById("price-canvas");
  const ctx = canvas.getContext("2d");

  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);

  const w = rect.width;
  const h = rect.height;

  ctx.clearRect(0, 0, w, h);

  if (priceHistory.length === 0) return;

  const min = Math.min(...priceHistory) - 1.0;
  const max = Math.max(...priceHistory) + 1.0;
  const range = max - min || 1.0;

  const getX = (index) => (index / (priceHistory.length - 1)) * (w - 40) + 20;
  const getY = (val) => h - 20 - ((val - min) / range) * (h - 40);

  ctx.beginPath();
  ctx.strokeStyle = "#06b6d4";
  ctx.lineWidth = 2.5;
  ctx.lineJoin = "round";

  priceHistory.forEach((p, idx) => {
    const x = getX(idx);
    const y = getY(p);
    if (idx === 0) {
      ctx.moveTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  });
  ctx.stroke();

  ctx.beginPath();
  ctx.moveTo(getX(0), h - 20);
  priceHistory.forEach((p, idx) => {
    ctx.lineTo(getX(idx), getY(p));
  });
  ctx.lineTo(getX(priceHistory.length - 1), h - 20);
  ctx.closePath();

  const gradient = ctx.createLinearGradient(0, 0, 0, h);
  gradient.addColorStop(0, "rgba(6, 182, 212, 0.25)");
  gradient.addColorStop(1, "rgba(6, 182, 212, 0.0)");
  ctx.fillStyle = gradient;
  ctx.fill();

  ctx.fillStyle = "#475569";
  ctx.font = "10px sans-serif";
  ctx.textAlign = "right";
  ctx.fillText(max.toFixed(1), w - 10, getY(max) + 10);
  ctx.fillText(min.toFixed(1), w - 10, getY(min) - 5);
}

window.addEventListener("resize", drawChart);
