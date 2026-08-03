/* ── Config ─────────────────────────────────────────────────────────── */
const HTTP_BASE_URL = "http://localhost:8080/api/v1";
const WS_URL        = "ws://localhost:8080/ws";

/* ── State ──────────────────────────────────────────────────────────── */
let userId   = null;
let balance  = 0.0;
let markets  = [];
let selectedMarketIdx = -1;
let activeSymbol = "";
let ws = null;
let bids = [], asks = [], trades = [];

// Chart state
let tvChart        = null;
let candleSeries   = null;
let volumeSeries   = null;
let activeInterval = "1m";
let lastCandle     = null;   // { time, open, high, low, close, buyVol, sellVol }
let sessionHigh    = null, sessionLow = null;

const INTERVALS = [
  { label: "1m",  seconds: 60       },
  { label: "5m",  seconds: 300      },
  { label: "15m", seconds: 900      },
  { label: "1h",  seconds: 3600     },
  { label: "4h",  seconds: 14400    },
  { label: "1d",  seconds: 86400    },
];

/* ── Screens ────────────────────────────────────────────────────────── */
const screens = {
  login:     document.getElementById("login-screen"),
  dashboard: document.getElementById("dashboard-screen"),
  trading:   document.getElementById("trading-screen")
};
function showScreen(name) {
  Object.values(screens).forEach(s => s.style.display = "none");
  screens[name].style.display = "flex";
}

/* ── UUID v5 ────────────────────────────────────────────────────────── */
async function generateUuid(username) {
  const encoder = new TextEncoder();
  const combined = new Uint8Array(16 + encoder.encode(username).length);
  combined.set(encoder.encode(username), 16);
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-1", combined));
  hash[6] = (hash[6] & 0x0f) | 0x50;
  hash[8] = (hash[8] & 0x3f) | 0x80;
  const h = Array.from(hash.slice(0,16)).map(b=>b.toString(16).padStart(2,"0")).join("");
  return `${h.slice(0,8)}-${h.slice(8,12)}-${h.slice(12,16)}-${h.slice(16,20)}-${h.slice(20,32)}`;
}

/* ── Login ──────────────────────────────────────────────────────────── */
document.getElementById("login-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const username = document.getElementById("username").value.trim();
  const log = document.getElementById("login-log");
  if (!username) { log.textContent = "Username required."; return; }

  log.textContent = "Authenticating…";
  userId = await generateUuid(username);

  try {
    const depRes = await fetch(`${HTTP_BASE_URL}/accounts/deposit`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ user_id: userId, amount: "10000.00" })
    });
    if (!depRes.ok) {
      const err = await depRes.text().catch(() => "");
      throw new Error(`Deposit failed (${depRes.status}): ${err}`);
    }
    const balRes = await fetch(`${HTTP_BASE_URL}/accounts/${userId}/balance`);
    if (balRes.ok) {
      const d = await balRes.json();
      balance = parseFloat(d.available_balance || "0.0");
    } else { balance = 10000.0; }

    await fetchMarkets();
    showScreen("dashboard");
    renderDashboard();
  } catch (err) {
    log.textContent = "Error: " + (err.message || "Network error");
  }
});

/* ── Markets ────────────────────────────────────────────────────────── */
async function fetchMarkets() {
  try {
    const res = await fetch(`${HTTP_BASE_URL}/markets`);
    if (res.ok) markets = await res.json();
  } catch(e) { console.error("Markets:", e); }
}

setInterval(async () => {
  if (userId && screens.dashboard.style.display !== "none") {
    await fetchMarkets(); renderDashboard();
  }
}, 10000);

/* ── Dashboard ──────────────────────────────────────────────────────── */
function renderDashboard() {
  document.getElementById("user-id-display").textContent = userId ? userId.slice(0,8)+"…" : "—";
  document.getElementById("balance-display").textContent = `$${balance.toLocaleString("en-US",{minimumFractionDigits:2})} USDT`;
  const tbody = document.getElementById("markets-tbody");
  tbody.innerHTML = "";
  markets.forEach((m, idx) => {
    const tr = document.createElement("tr");
    if (idx === selectedMarketIdx) tr.classList.add("selected");
    tr.innerHTML = `
      <td><strong>${m.symbol}</strong></td>
      <td>${m.base_asset}</td><td>${m.quote_asset}</td>
      <td>${m.tick_size}</td><td>${m.lot_size}</td>
      <td><span class="status-badge">${m.status||"ACTIVE"}</span></td>
      <td><button class="enter-row-btn">Trade →</button></td>`;
    tr.querySelector(".enter-row-btn").addEventListener("click", e => {
      e.stopPropagation(); selectedMarketIdx = idx; enterTradingDesk();
    });
    tr.addEventListener("click", () => {
      selectedMarketIdx = idx;
      document.getElementById("btn-enter-market").disabled = false;
      renderDashboard();
    });
    tr.addEventListener("dblclick", () => { selectedMarketIdx = idx; enterTradingDesk(); });
    tbody.appendChild(tr);
  });
}

document.getElementById("btn-enter-market").addEventListener("click", () => {
  if (selectedMarketIdx >= 0) enterTradingDesk();
});
document.getElementById("btn-logout").addEventListener("click", () => {
  userId = null; balance = 0; selectedMarketIdx = -1;
  document.getElementById("username").value = "";
  document.getElementById("password").value = "";
  document.getElementById("login-log").textContent = "";
  showScreen("login");
});

/* ── Enter Trading Desk ─────────────────────────────────────────────── */
async function enterTradingDesk() {
  const m = markets[selectedMarketIdx];
  activeSymbol = m.symbol;
  bids = []; asks = []; trades = [];
  lastCandle = null; sessionHigh = null; sessionLow = null;
  activeInterval = "1m";

  document.getElementById("trading-symbol").textContent  = activeSymbol;
  document.getElementById("trading-price").textContent   = "—";
  document.getElementById("price-change").textContent    = "—";
  document.getElementById("trading-balance").textContent = `$${balance.toLocaleString("en-US",{minimumFractionDigits:2})}`;
  document.getElementById("chart-symbol-label").textContent = activeSymbol + " · PERPETUAL";
  document.getElementById("order-price").value = "64250";
  document.getElementById("order-qty").value   = "0.1";
  document.getElementById("order-log").textContent = "";
  document.getElementById("stat-high").textContent = "—";
  document.getElementById("stat-low").textContent  = "—";
  document.getElementById("stat-vol").textContent  = "—";

  // Set active interval button
  document.querySelectorAll(".interval-btn").forEach(b => {
    b.classList.toggle("active", b.dataset.interval === activeInterval);
  });

  showScreen("trading");
  initTVChart();
  await loadHistoricalCandles();
  initWebsocket();
  updateOrderSummary();
}

document.getElementById("btn-back-to-dashboard").addEventListener("click", () => {
  if (ws) { ws.close(); ws = null; }
  if (tvChart) { tvChart.remove(); tvChart = null; candleSeries = null; volumeSeries = null; }
  showScreen("dashboard"); renderDashboard();
});

/* ── TradingView Chart ──────────────────────────────────────────────── */
function initTVChart() {
  const container = document.getElementById("tv-chart");
  container.innerHTML = "";
  if (tvChart) tvChart.remove();

  tvChart = LightweightCharts.createChart(container, {
    layout: {
      background: { type: "solid", color: "#090c12" },
      textColor: "#64748b",
      fontFamily: "'JetBrains Mono', monospace",
      fontSize: 11,
    },
    grid: {
      vertLines: { color: "#131b2b", style: LightweightCharts.LineStyle.Dotted },
      horzLines: { color: "#131b2b", style: LightweightCharts.LineStyle.Dotted },
    },
    crosshair: {
      mode: LightweightCharts.CrosshairMode.Normal,
      vertLine: { color: "#334155", labelBackgroundColor: "#1a2438", width: 1 },
      horzLine: { color: "#334155", labelBackgroundColor: "#1a2438", width: 1 },
    },
    rightPriceScale: {
      borderColor: "#1f2d44",
      textColor: "#64748b",
      entireTextOnly: true,
    },
    timeScale: {
      borderColor: "#1f2d44",
      textColor: "#64748b",
      timeVisible: true,
      secondsVisible: activeInterval === "1m" || activeInterval === "5m",
      fixLeftEdge: false,
      fixRightEdge: false,
      lockVisibleTimeRangeOnResize: true,
    },
    handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: true },
    handleScale: { axisPressedMouseMove: true, mouseWheel: true, pinch: true },
  });

  // ── Candlestick series ──
  candleSeries = tvChart.addCandlestickSeries({
    upColor:         "#26de81",
    downColor:       "#ff4757",
    borderUpColor:   "#26de81",
    borderDownColor: "#ff4757",
    wickUpColor:     "#26de81",
    wickDownColor:   "#ff4757",
    priceFormat: { type: "price", precision: 2, minMove: 0.01 },
  });

  // ── Volume histogram (pane 2) ──
  volumeSeries = tvChart.addHistogramSeries({
    color: "#26de8150",
    priceFormat: { type: "volume" },
    priceScaleId: "vol",
    lastValueVisible: false,
    priceLineVisible: false,
  });
  tvChart.priceScale("vol").applyOptions({
    scaleMargins: { top: 0.85, bottom: 0 },
  });

  // ── OHLCV Legend ──
  tvChart.subscribeCrosshairMove(param => {
    if (!param || !param.seriesData) return;
    const c = param.seriesData.get(candleSeries);
    const v = param.seriesData.get(volumeSeries);
    if (c) updateOHLCLegend(c.open, c.high, c.low, c.close, v?.value);
  });

  // ── Resize observer ──
  const ro = new ResizeObserver(() => {
    if (tvChart) tvChart.applyOptions({
      width: container.clientWidth,
      height: container.clientHeight
    });
  });
  ro.observe(container);
}

function updateOHLCLegend(o, h, l, c, vol) {
  const isUp = c >= o;
  const color = isUp ? "#26de81" : "#ff4757";
  const change = o ? (((c - o) / o) * 100).toFixed(2) : "0.00";
  document.getElementById("ohlc-legend").innerHTML =
    `<span style="color:#94a3b8">O</span><span style="color:${color}"> ${(+o).toFixed(2)}</span>
     <span style="color:#94a3b8"> H</span><span style="color:${color}"> ${(+h).toFixed(2)}</span>
     <span style="color:#94a3b8"> L</span><span style="color:${color}"> ${(+l).toFixed(2)}</span>
     <span style="color:#94a3b8"> C</span><span style="color:${color}"> ${(+c).toFixed(2)}</span>
     <span style="color:#64748b"> ${isUp?"▲":"▼"}${change}%</span>
     ${vol !== undefined ? `<span style="color:#64748b"> Vol:${(+vol).toFixed(3)}</span>` : ""}`;
}

/* ── Load historical candles from backend ───────────────────────────── */
async function loadHistoricalCandles() {
  if (!candleSeries || !volumeSeries) return;
  try {
    const limit = 500;
    const url = `${HTTP_BASE_URL}/markets/${activeSymbol}/candles?resolution=${activeInterval}&limit=${limit}`;
    const res = await fetch(url);
    if (!res.ok) { console.warn("Candles fetch failed:", res.status); return; }

    const raw = await res.json();
    if (!Array.isArray(raw) || raw.length === 0) return;

    // Sort ascending by timestamp
    const sorted = [...raw].sort((a, b) => a.timestamp - b.timestamp);

    const candles = sorted.map(c => ({
      time:  c.timestamp,
      open:  parseFloat(c.open),
      high:  parseFloat(c.high),
      low:   parseFloat(c.low),
      close: parseFloat(c.close),
    }));

    const volumes = sorted.map(c => {
      const vol   = parseFloat(c.volume) || 0;
      const open  = parseFloat(c.open);
      const close = parseFloat(c.close);
      return {
        time:  c.timestamp,
        value: vol,
        color: close >= open ? "rgba(38,222,129,0.45)" : "rgba(255,71,87,0.45)",
      };
    });

    candleSeries.setData(candles);
    volumeSeries.setData(volumes);
    tvChart.timeScale().fitContent();

    // Seed last candle state from final candle
    const last = candles[candles.length - 1];
    if (last) {
      sessionHigh = Math.max(...candles.map(c => c.high));
      sessionLow  = Math.min(...candles.map(c => c.low));
      document.getElementById("trading-price").textContent = `$${last.close.toFixed(2)}`;
      document.getElementById("stat-high").textContent = `$${sessionHigh.toFixed(2)}`;
      document.getElementById("stat-low").textContent  = `$${sessionLow.toFixed(2)}`;
      updateOHLCLegend(last.open, last.high, last.low, last.close, volumes[volumes.length-1]?.value);
    }
  } catch (e) { console.error("Historical candles error:", e); }
}

/* ── Interval buttons ───────────────────────────────────────────────── */
document.querySelectorAll(".interval-btn").forEach(btn => {
  btn.addEventListener("click", async () => {
    activeInterval = btn.dataset.interval;
    document.querySelectorAll(".interval-btn").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    lastCandle = null;

    // Update seconds visibility on time scale
    if (tvChart) {
      tvChart.timeScale().applyOptions({
        secondsVisible: activeInterval === "1m" || activeInterval === "5m",
      });
    }
    await loadHistoricalCandles();
  });
});

/* ── Live candle update from trade ─────────────────────────────────── */
function updateCandle(price, qty, side) {
  if (!candleSeries || !volumeSeries) return;

  const intervalSec = INTERVALS.find(i => i.label === activeInterval)?.seconds || 60;
  const nowSec  = Math.floor(Date.now() / 1000);
  const bucketT = Math.floor(nowSec / intervalSec) * intervalSec;

  if (!lastCandle || lastCandle.time !== bucketT) {
    // New candle
    lastCandle = {
      time: bucketT,
      open: price, high: price, low: price, close: price,
      buyVol: 0, sellVol: 0,
    };
  } else {
    lastCandle.high  = Math.max(lastCandle.high, price);
    lastCandle.low   = Math.min(lastCandle.low, price);
    lastCandle.close = price;
  }

  if (side === "BUY")  lastCandle.buyVol  += qty;
  if (side === "SELL") lastCandle.sellVol += qty;

  const totalVol = lastCandle.buyVol + lastCandle.sellVol;
  const isUp = lastCandle.close >= lastCandle.open;

  candleSeries.update({
    time: lastCandle.time, open: lastCandle.open,
    high: lastCandle.high, low: lastCandle.low, close: lastCandle.close,
  });
  volumeSeries.update({
    time:  lastCandle.time,
    value: totalVol,
    color: isUp ? "rgba(38,222,129,0.45)" : "rgba(255,71,87,0.45)",
  });

  updateOHLCLegend(lastCandle.open, lastCandle.high, lastCandle.low, lastCandle.close, totalVol);
}

/* ── Buy/Sell tabs ──────────────────────────────────────────────────── */
document.querySelectorAll(".side-tab").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".side-tab").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    const side = btn.dataset.side;
    document.getElementById("order-side").value = side;
    const submitBtn = document.getElementById("submit-order-btn");
    submitBtn.className = side === "BUY" ? "btn btn-buy btn-block" : "btn btn-sell btn-block";
    submitBtn.textContent = side === "BUY" ? "Place Buy Order" : "Place Sell Order";
    updateOrderSummary();
  });
});

/* ── Order type tabs ─────────────────────────────────────────────────── */
document.querySelectorAll(".type-tab").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".type-tab").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById("order-type-val").value = btn.dataset.type;
    const priceGroup = document.querySelector(".order-form .form-group:first-of-type");
    priceGroup.style.display = btn.dataset.type === "MARKET" ? "none" : "flex";
    updateOrderSummary();
  });
});

/* ── Order summary ──────────────────────────────────────────────────── */
function updateOrderSummary() {
  const qty   = parseFloat(document.getElementById("order-qty").value) || 0;
  const price = parseFloat(document.getElementById("order-price").value) || 0;
  document.getElementById("order-total").textContent = `$${(qty*price).toLocaleString("en-US",{minimumFractionDigits:2})} USDT`;
  document.getElementById("order-avail").textContent = `$${balance.toLocaleString("en-US",{minimumFractionDigits:2})}`;
}
document.getElementById("order-qty").addEventListener("input", updateOrderSummary);
document.getElementById("order-price").addEventListener("input", updateOrderSummary);

/* ── Order submit ───────────────────────────────────────────────────── */
document.getElementById("order-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const log       = document.getElementById("order-log");
  const side      = document.getElementById("order-side").value;
  const orderType = document.getElementById("order-type-val").value;
  const qty       = document.getElementById("order-qty").value;
  const price     = orderType === "LIMIT" ? document.getElementById("order-price").value : null;

  log.style.color = "var(--text1)";
  log.textContent = "Submitting…";

  try {
    const res = await fetch(`${HTTP_BASE_URL}/orders`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        user_id: userId, symbol: activeSymbol,
        side, order_type: orderType,
        quantity: qty, price,
        trigger_price: null, time_in_force: "GTC",
        leverage: 10, margin_mode: "ISOLATED",
        reduce_only: false, post_only: false,
      })
    });
    const text = await res.text();
    if (res.ok) {
      log.style.color = "var(--green)";
      log.textContent = `✓ ${side} ${qty} ${activeSymbol} @ ${price || "MARKET"}`;
    } else {
      log.style.color = "var(--red)";
      log.textContent = `✗ ${text}`;
    }
    const balRes = await fetch(`${HTTP_BASE_URL}/accounts/${userId}/balance`);
    if (balRes.ok) {
      const d = await balRes.json();
      balance = parseFloat(d.available_balance || "0.0");
      document.getElementById("trading-balance").textContent = `$${balance.toLocaleString("en-US",{minimumFractionDigits:2})}`;
      updateOrderSummary();
    }
  } catch (err) {
    log.style.color = "var(--red)";
    log.textContent = "Error: " + err.message;
  }
});

/* ── WebSocket ──────────────────────────────────────────────────────── */
function initWebsocket() {
  if (ws) ws.close();
  ws = new WebSocket(WS_URL);
  ws.onopen = () => {
    ws.send(JSON.stringify({ action:"subscribe", channels:[`orderbook:${activeSymbol}`,`trades:${activeSymbol}`] }));
  };
  ws.onmessage = (event) => {
    try {
      const val = JSON.parse(event.data);
      if (val.bids || val.asks) {
        if (val.bids) bids = val.bids;
        if (val.asks) asks = val.asks;
        renderOrderbook();
      } else if (val.taker_side) {
        const p    = parseFloat(val.price || "0.0");
        const q    = parseFloat(val.quantity || "0.0");
        const side = val.taker_side;
        const time = new Date().toLocaleTimeString("en-US",{hour12:false,hour:"2-digit",minute:"2-digit",second:"2-digit"});

        document.getElementById("trading-price").textContent = `$${p.toFixed(2)}`;
        document.getElementById("ob-mid-price").textContent  = p.toFixed(2);

        if (sessionHigh === null || p > sessionHigh) {
          sessionHigh = p;
          document.getElementById("stat-high").textContent = `$${p.toFixed(2)}`;
        }
        if (sessionLow === null || p < sessionLow) {
          sessionLow = p;
          document.getElementById("stat-low").textContent = `$${p.toFixed(2)}`;
        }

        trades.unshift({ time, p, q, side });
        if (trades.length > 30) trades.pop();
        renderTrades();
        updateCandle(p, q, side);
      }
    } catch(_) {}
  };
}

/* ── Orderbook ──────────────────────────────────────────────────────── */
function renderOrderbook() {
  const asksDiv = document.getElementById("asks-list");
  const bidsDiv = document.getElementById("bids-list");
  const maxAskQ = asks.length ? Math.max(...asks.slice(0,14).map(l=>parseFloat(l[1])),1) : 1;
  const maxBidQ = bids.length ? Math.max(...bids.slice(0,14).map(l=>parseFloat(l[1])),1) : 1;

  const displayAsks = [...asks].slice(0,14).reverse();
  asksDiv.innerHTML = displayAsks.map((level, i) => {
    const p = parseFloat(level[0]), q = parseFloat(level[1]);
    const pct = Math.min((q/maxAskQ)*100, 100);
    return `<div class="ob-row" data-idx="${i}" data-price="${p.toFixed(2)}">
      <div class="ob-bar" style="width:${pct}%"></div>
      <span class="ob-price">${p.toFixed(2)}</span>
      <span class="ob-size">${q.toFixed(4)}</span>
      <span class="ob-total">${(p*q).toFixed(1)}</span>
    </div>`;
  }).join("");

  bidsDiv.innerHTML = bids.slice(0,14).map((level, i) => {
    const p = parseFloat(level[0]), q = parseFloat(level[1]);
    const pct = Math.min((q/maxBidQ)*100, 100);
    return `<div class="ob-row" data-idx="${i}" data-price="${p.toFixed(2)}">
      <div class="ob-bar" style="width:${pct}%"></div>
      <span class="ob-price">${p.toFixed(2)}</span>
      <span class="ob-size">${q.toFixed(4)}</span>
      <span class="ob-total">${(p*q).toFixed(1)}</span>
    </div>`;
  }).join("");

  // Fill price on click
  [...asksDiv.querySelectorAll(".ob-row"), ...bidsDiv.querySelectorAll(".ob-row")].forEach(row => {
    row.addEventListener("click", () => {
      document.getElementById("order-price").value = row.dataset.price;
      updateOrderSummary();
    });
  });

  if (asks.length && bids.length) {
    const spread = (parseFloat(asks[0][0]) - parseFloat(bids[0][0])).toFixed(2);
    document.getElementById("ob-spread").textContent = `Spread $${spread}`;
  }
}

/* ── Recent Trades ──────────────────────────────────────────────────── */
function renderTrades() {
  document.getElementById("trades-list").innerHTML = trades.map(t => `
    <div class="trade-row ${t.side.toLowerCase()}">
      <span>${t.time}</span>
      <span><span class="trade-side-badge">${t.side}</span></span>
      <span class="trade-price">${t.p.toFixed(2)}</span>
      <span>${t.q.toFixed(4)}</span>
    </div>`).join("");
}
