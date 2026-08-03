use eframe::egui;
use egui_plot::{Line, Plot, PlotPoints};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::VecDeque,
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use uuid::Uuid;

const WS_URL: &str = "ws://127.0.0.1:8080/ws";
const HTTP_BASE_URL: &str = "http://127.0.0.1:8080/api/v1";

#[derive(Clone, Deserialize)]
struct TuiMarket {
    symbol: String,
    base_asset: String,
    quote_asset: String,
    tick_size: String,
    lot_size: String,
    status: String,
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum Screen {
    Login,
    Dashboard,
    Trading,
}

struct SharedState {
    symbol: String,
    current_price: f64,
    price_history: VecDeque<f64>,
    bids: Vec<(Decimal, Decimal)>,
    asks: Vec<(Decimal, Decimal)>,
    trades: VecDeque<(String, f64, f64, String)>,
    balance: f64,
    margin_locked: f64,
    log_message: String,
    markets: Vec<TuiMarket>,
}

struct DesktopApp {
    screen: Screen,
    user_id: Option<Uuid>,
    username_input: String,
    password_input: String,
    selected_market_idx: usize,

    shared: Arc<Mutex<SharedState>>,
    symbol_tx: tokio::sync::mpsc::Sender<String>,
    client: reqwest::Client,

    side: String,
    order_type: String,
    qty_input: String,
    price_input: String,
}

impl DesktopApp {
    fn new(symbol_tx: tokio::sync::mpsc::Sender<String>, shared: Arc<Mutex<SharedState>>) -> Self {
        Self {
            screen: Screen::Login,
            user_id: None,
            username_input: String::new(),
            password_input: String::new(),
            selected_market_idx: 0,
            shared,
            symbol_tx,
            client: reqwest::Client::new(),
            side: "BUY".to_string(),
            order_type: "LIMIT".to_string(),
            qty_input: "0.1".to_string(),
            price_input: "64250".to_string(),
        }
    }
}

fn main() -> Result<(), eframe::Error> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let _guard = rt.enter();

    let shared = Arc::new(Mutex::new(SharedState {
        symbol: "BTCUSDT".to_string(),
        current_price: 64250.0,
        price_history: VecDeque::from(vec![64250.0; 50]),
        bids: Vec::new(),
        asks: Vec::new(),
        trades: VecDeque::new(),
        balance: 0.0,
        margin_locked: 0.0,
        log_message: "Please enter your username and password to log in.".to_string(),
        markets: Vec::new(),
    }));

    let (symbol_tx, symbol_rx) = tokio::sync::mpsc::channel::<String>(10);

    let shared_ws = shared.clone();
    rt.spawn(async move {
        let _ = run_websocket_listener(shared_ws, symbol_rx).await;
    });

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1100.0, 700.0])
            .with_min_inner_size([800.0, 500.0]),
        ..Default::default()
    };

    let app_shared = shared.clone();
    eframe::run_native(
        "Perps Exchange Desktop",
        options,
        Box::new(|_cc| {
            _cc.egui_ctx.set_visuals(egui::Visuals::dark());
            let shared_bg_ref = app_shared.clone();
            let ctx_clone = _cc.egui_ctx.clone();
            tokio::spawn(async move {
                let client = reqwest::Client::new();
                loop {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    let logged_in = {
                        let s = shared_bg_ref.lock().unwrap();
                        s.log_message.is_empty() || true
                    };
                    if logged_in {
                        let url = format!("{}/markets", HTTP_BASE_URL);
                        if let Ok(res) = client.get(&url).send().await {
                            if let Ok(m_list) = res.json::<Vec<TuiMarket>>().await {
                                let mut s = shared_bg_ref.lock().unwrap();
                                s.markets = m_list;
                                ctx_clone.request_repaint();
                            }
                        }
                    }
                }
            });
            Ok(Box::new(DesktopApp::new(symbol_tx, app_shared)))
        }),
    )
}

async fn run_websocket_listener(
    state: Arc<Mutex<SharedState>>,
    mut symbol_rx: tokio::sync::mpsc::Receiver<String>,
) -> Result<(), Box<dyn Error>> {
    let mut current_symbol = "BTCUSDT".to_string();

    let parse_decimal = |val: &serde_json::Value| -> Decimal {
        if let Some(s) = val.as_str() {
            s.parse::<Decimal>().unwrap_or_default()
        } else {
            val.to_string().parse::<Decimal>().unwrap_or_default()
        }
    };

    let parse_f64 = |val: &serde_json::Value| -> f64 {
        if let Some(s) = val.as_str() {
            s.parse::<f64>().unwrap_or(0.0)
        } else {
            val.as_f64().unwrap_or(0.0)
        }
    };

    loop {
        match connect_async(WS_URL).await {
            Ok((mut ws_stream, _)) => {
                let sub_msg = json!({
                    "action": "subscribe",
                    "channels": [
                        format!("orderbook:{}", current_symbol),
                        format!("trades:{}", current_symbol)
                    ]
                });

                if ws_stream
                    .send(Message::Text(sub_msg.to_string()))
                    .await
                    .is_err()
                {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }

                loop {
                    tokio::select! {
                        Some(new_symbol) = symbol_rx.recv() => {
                            current_symbol = new_symbol;
                            break;
                        }
                        msg_res = ws_stream.next() => {
                            if let Some(Ok(Message::Text(text))) = msg_res {
                                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
                                    if val.get("bids").is_some() {
                                        let mut s = state.lock().unwrap();
                                        if s.symbol == current_symbol {
                                            s.bids.clear();
                                            s.asks.clear();
                                            if let Some(bids_arr) = val.get("bids").and_then(|b| b.as_array()) {
                                                for bid in bids_arr {
                                                    if let (Some(p_val), Some(q_val)) = (bid.get(0), bid.get(1)) {
                                                        let p = parse_decimal(p_val);
                                                        let q = parse_decimal(q_val);
                                                        if !p.is_zero() {
                                                            s.bids.push((p, q));
                                                        }
                                                    }
                                                }
                                            }
                                            if let Some(asks_arr) = val.get("asks").and_then(|a| a.as_array()) {
                                                for ask in asks_arr {
                                                    if let (Some(p_val), Some(q_val)) = (ask.get(0), ask.get(1)) {
                                                        let p = parse_decimal(p_val);
                                                        let q = parse_decimal(q_val);
                                                        if !p.is_zero() {
                                                            s.asks.push((p, q));
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    } else if val.get("taker_side").is_some() {
                                        let price_val = val.get("price").cloned().unwrap_or(serde_json::Value::Null);
                                        let qty_val = val.get("quantity").cloned().unwrap_or(serde_json::Value::Null);
                                        let price = parse_f64(&price_val);
                                        let qty = parse_f64(&qty_val);
                                        let side = val.get("taker_side").and_then(|s| s.as_str()).unwrap_or("BUY").to_string();
                                        let time = chrono::Local::now().format("%H:%M:%S").to_string();

                                        let mut s = state.lock().unwrap();
                                        if s.symbol == current_symbol {
                                            s.current_price = price;
                                            s.price_history.push_back(price);
                                            if s.price_history.len() > 100 {
                                                s.price_history.pop_front();
                                            }
                                            s.trades.push_front((time, price, qty, side));
                                            if s.trades.len() > 30 {
                                                s.trades.pop_back();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(_) => {
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

impl eframe::App for DesktopApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        match self.screen {
            Screen::Login => {
                egui::CentralPanel::default().show(ctx, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.add_space(80.0);
                        ui.heading("PERPS EXCHANGE DESKTOP");
                        ui.add_space(40.0);

                        egui::Grid::new("login_grid")
                            .num_columns(2)
                            .spacing([10.0, 15.0])
                            .show(ui, |ui| {
                                ui.label("Username:");
                                ui.text_edit_singleline(&mut self.username_input);
                                ui.end_row();

                                ui.label("Password:");
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.password_input)
                                        .password(true),
                                );
                                ui.end_row();
                            });

                        ui.add_space(30.0);

                        if ui.button("Log In").clicked() {
                            if self.username_input.trim().is_empty() {
                                let mut s = self.shared.lock().unwrap();
                                s.log_message = "Username cannot be empty.".to_string();
                            } else {
                                let mut s = self.shared.lock().unwrap();
                                s.log_message = "Logging in...".to_string();

                                let u_input = self.username_input.clone();
                                let client_clone = self.client.clone();
                                let state_clone = self.shared.clone();
                                let ctx_clone = ctx.clone();

                                tokio::spawn(async move {
                                    let namespace = Uuid::nil();
                                    let user_id = Uuid::new_v5(&namespace, u_input.as_bytes());
                                    let bal_url =
                                        format!("{}/accounts/{}/balance", HTTP_BASE_URL, user_id);

                                    if let Ok(res) = client_clone.get(&bal_url).send().await {
                                        if let Ok(json_val) = res.json::<serde_json::Value>().await
                                        {
                                            let avail_str = json_val
                                                .get("available_balance")
                                                .and_then(|v| v.as_str())
                                                .unwrap_or("0.0");
                                            let mut balance =
                                                avail_str.parse::<f64>().unwrap_or(0.0);

                                            if balance == 0.0 {
                                                let dep_url =
                                                    format!("{}/accounts/deposit", HTTP_BASE_URL);
                                                let dep_body = json!({
                                                    "user_id": user_id,
                                                    "amount": "10000.00"
                                                });
                                                if let Ok(dep_res) = client_clone
                                                    .post(&dep_url)
                                                    .json(&dep_body)
                                                    .send()
                                                    .await
                                                {
                                                    if let Ok(dep_json) =
                                                        dep_res.json::<serde_json::Value>().await
                                                    {
                                                        let new_bal_str = dep_json
                                                            .get("new_balance")
                                                            .and_then(|v| v.as_str())
                                                            .unwrap_or("0.0");
                                                        balance = new_bal_str
                                                            .parse::<f64>()
                                                            .unwrap_or(0.0);
                                                    }
                                                }
                                            }

                                            let markets_url = format!("{}/markets", HTTP_BASE_URL);
                                            let mut parsed_markets = Vec::new();
                                            if let Ok(m_res) =
                                                client_clone.get(&markets_url).send().await
                                            {
                                                if let Ok(m_list) =
                                                    m_res.json::<Vec<TuiMarket>>().await
                                                {
                                                    parsed_markets = m_list;
                                                }
                                            }

                                            let mut s = state_clone.lock().unwrap();
                                            s.balance = balance;
                                            s.markets = parsed_markets;
                                            s.log_message = String::new();

                                            let mut self_shared = state_clone.clone();
                                            tokio::spawn(async move {
                                                tokio::time::sleep(Duration::from_millis(50)).await;
                                            });
                                        }
                                    }
                                });
                            }
                        }

                        ui.add_space(20.0);
                        let log_msg = {
                            let s = self.shared.lock().unwrap();
                            s.log_message.clone()
                        };
                        if !log_msg.is_empty() {
                            ui.colored_label(egui::Color32::YELLOW, log_msg);
                        }
                    });
                });

                let check_success = {
                    let s = self.shared.lock().unwrap();
                    s.log_message.is_empty() && self.username_input.len() > 0
                };
                if check_success {
                    let namespace = Uuid::nil();
                    self.user_id = Some(Uuid::new_v5(&namespace, self.username_input.as_bytes()));
                    self.screen = Screen::Dashboard;

                    let client_clone = self.client.clone();
                    let state_clone = self.shared.clone();
                    let ctx_clone = ctx.clone();
                    tokio::spawn(async move {
                        let markets_url = format!("{}/markets", HTTP_BASE_URL);
                        if let Ok(m_res) = client_clone.get(&markets_url).send().await {
                            if let Ok(m_list) = m_res.json::<Vec<TuiMarket>>().await {
                                let mut s = state_clone.lock().unwrap();
                                s.log_message = String::new();
                                ctx_clone.request_repaint();
                            }
                        }
                    });
                }
            }
            Screen::Dashboard => {
                let (bal, user_str) = {
                    let s = self.shared.lock().unwrap();
                    (
                        s.balance,
                        self.user_id.map(|id| id.to_string()).unwrap_or_default(),
                    )
                };

                egui::TopBottomPanel::top("dashboard_top").show(ctx, |ui| {
                    ui.horizontal(|ui| {
                        ui.heading("DASHBOARD HUB");
                        ui.label(format!(" | User: {}", user_str));
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            ui.label(format!("Balance: ${:.2} USDT", bal));
                        });
                    });
                });

                egui::CentralPanel::default().show(ctx, |ui| {
                    ui.heading("Available Markets");
                    ui.add_space(10.0);

                    let markets = {
                        let s = self.shared.lock().unwrap();
                        s.markets.clone()
                    };

                    egui::ScrollArea::vertical().show(ui, |ui| {
                        for (idx, m) in markets.iter().enumerate() {
                            let is_selected = self.selected_market_idx == idx;
                            let response = ui.selectable_label(
                                is_selected,
                                format!(
                                    "{} (Base: {} | Quote: {} | Tick: {})",
                                    m.symbol, m.base_asset, m.quote_asset, m.tick_size
                                ),
                            );
                            if response.clicked() {
                                self.selected_market_idx = idx;
                            }
                            if response.double_clicked() {
                                self.selected_market_idx = idx;
                                let selected = m.symbol.clone();
                                {
                                    let mut s = self.shared.lock().unwrap();
                                    s.symbol = selected.clone();
                                    s.price_history = VecDeque::from(vec![s.current_price; 50]);
                                    s.bids.clear();
                                    s.asks.clear();
                                    s.trades.clear();
                                }
                                self.screen = Screen::Trading;
                                let tx = self.symbol_tx.clone();
                                tokio::spawn(async move {
                                    let _ = tx.send(selected).await;
                                });
                            }
                        }
                    });

                    ui.add_space(20.0);
                    if ui.button("Enter Selected Market").clicked() && !markets.is_empty() {
                        let selected = markets[self.selected_market_idx].symbol.clone();
                        {
                            let mut s = self.shared.lock().unwrap();
                            s.symbol = selected.clone();
                            s.price_history = VecDeque::from(vec![s.current_price; 50]);
                            s.bids.clear();
                            s.asks.clear();
                            s.trades.clear();
                        }
                        self.screen = Screen::Trading;
                        let tx = self.symbol_tx.clone();
                        tokio::spawn(async move {
                            let _ = tx.send(selected).await;
                        });
                    }
                    if ui.button("Log Out").clicked() {
                        self.screen = Screen::Login;
                        self.username_input.clear();
                        self.password_input.clear();
                        let mut s = self.shared.lock().unwrap();
                        s.log_message =
                            "Please enter your username and password to log in.".to_string();
                    }
                });
            }
            Screen::Trading => {
                let (sym, price, bids, asks, trades, bal, locked, log_msg) = {
                    let s = self.shared.lock().unwrap();
                    (
                        s.symbol.clone(),
                        s.current_price,
                        s.bids.clone(),
                        s.asks.clone(),
                        s.trades.clone(),
                        s.balance,
                        s.margin_locked,
                        s.log_message.clone(),
                    )
                };

                egui::TopBottomPanel::top("trading_top").show(ctx, |ui| {
                    ui.horizontal(|ui| {
                        if ui.button("Back to Markets").clicked() {
                            self.screen = Screen::Dashboard;
                        }
                        ui.heading(format!("TRADING DESK - {}", sym));
                        ui.label(format!("Price: ${:.2}", price));
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            ui.label(format!("Wallet Balance: ${:.2} USDT", bal));
                        });
                    });
                });

                egui::SidePanel::left("trading_form").width_range(250.0..=350.0).show(ctx, |ui| {
                    ui.heading("PLACE ORDER");
                    ui.add_space(10.0);

                    ui.horizontal(|ui| {
                        ui.radio_value(&mut self.side, "BUY".to_string(), "BUY");
                        ui.radio_value(&mut self.side, "SELL".to_string(), "SELL");
                    });
                    ui.add_space(10.0);

                    ui.horizontal(|ui| {
                        ui.radio_value(&mut self.order_type, "LIMIT".to_string(), "LIMIT");
                        ui.radio_value(&mut self.order_type, "MARKET".to_string(), "MARKET");
                    });
                    ui.add_space(15.0);

                    ui.label("Quantity (BTC):");
                    ui.text_edit_singleline(&mut self.qty_input);
                    ui.add_space(10.0);

                    if self.order_type == "LIMIT" {
                        ui.label("Price (USDT):");
                        ui.text_edit_singleline(&mut self.price_input);
                        ui.add_space(15.0);
                    }

                    if ui.button("Submit Order").clicked() {
                        let client_clone = self.client.clone();
                        let state_clone = self.shared.clone();
                        let u_id = self.user_id.unwrap_or_else(Uuid::nil);
                        let req_body = json!({
                            "user_id": u_id,
                            "symbol": sym,
                            "side": self.side,
                            "order_type": self.order_type,
                            "quantity": self.qty_input,
                            "price": if self.order_type == "LIMIT" { Some(self.price_input.clone()) } else { None },
                            "trigger_price": null,
                            "time_in_force": "GTC",
                            "leverage": 10,
                            "margin_mode": "ISOLATED",
                            "reduce_only": false,
                            "post_only": false
                        });

                        tokio::spawn(async move {
                            let url = format!("{}/orders", HTTP_BASE_URL);
                            match client_clone.post(&url).json(&req_body).send().await {
                                Ok(res) => {
                                    let text = res.text().await.unwrap_or_else(|_| "Empty Response".to_string());
                                    {
                                        let mut s = state_clone.lock().unwrap();
                                        s.log_message = format!("Order Response: {}", text);
                                    }

                                    let bal_url = format!("{}/accounts/{}/balance", HTTP_BASE_URL, u_id);
                                    if let Ok(bal_res) = client_clone.get(&bal_url).send().await {
                                        if let Ok(json_val) = bal_res.json::<serde_json::Value>().await {
                                            let avail_str = json_val.get("available_balance").and_then(|v| v.as_str()).unwrap_or("0.0");
                                            let balance = avail_str.parse::<f64>().unwrap_or(0.0);
                                            {
                                                let mut s = state_clone.lock().unwrap();
                                                s.balance = balance;
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    let mut s = state_clone.lock().unwrap();
                                    s.log_message = format!("Order Fail: {:?}", e);
                                }
                            }
                        });
                    }

                    ui.add_space(20.0);
                    if !log_msg.is_empty() {
                        ui.colored_label(egui::Color32::LIGHT_YELLOW, log_msg);
                    }
                });

                egui::CentralPanel::default().show(ctx, |ui| {
                    ui.columns(2, |columns| {
                        columns[0].vertical(|ui| {
                            ui.heading("ORDERBOOK");
                            ui.add_space(5.0);

                            ui.label("Asks (Sells)");
                            for (p, q) in asks.iter().take(8).rev() {
                                ui.colored_label(
                                    egui::Color32::LIGHT_RED,
                                    format!("{:<10.2} | {:<7.4}", p, q),
                                );
                            }

                            ui.add_space(10.0);
                            ui.label("Bids (Buys)");
                            for (p, q) in bids.iter().take(8) {
                                ui.colored_label(
                                    egui::Color32::GREEN,
                                    format!("{:<10.2} | {:<7.4}", p, q),
                                );
                            }
                        });

                        columns[1].vertical(|ui| {
                            ui.heading("PRICE CHART");
                            let points: PlotPoints = {
                                let s = self.shared.lock().unwrap();
                                s.price_history
                                    .iter()
                                    .enumerate()
                                    .map(|(i, &p)| [i as f64, p])
                                    .collect()
                            };
                            let line =
                                Line::new(points).color(egui::Color32::from_rgb(0, 255, 255));
                            Plot::new("live_chart")
                                .view_aspect(2.0)
                                .show(ui, |plot_ui| plot_ui.line(line));

                            ui.add_space(15.0);
                            ui.heading("RECENT TRADES");
                            for (time, p, q, side) in trades.iter().take(6) {
                                let color = if side == "BUY" {
                                    egui::Color32::GREEN
                                } else {
                                    egui::Color32::LIGHT_RED
                                };
                                ui.horizontal(|ui| {
                                    ui.label(time);
                                    ui.colored_label(color, side);
                                    ui.label(format!("Price: {:.2} | Qty: {:.4}", p, q));
                                });
                            }
                        });
                    });
                });
            }
        }
        ctx.request_repaint();
    }
}
