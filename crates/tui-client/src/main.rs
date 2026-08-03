use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures_util::{SinkExt, StreamExt};
use ratatui::{backend::CrosstermBackend, Terminal};
use rust_decimal::Decimal;
use serde_json::json;
use std::{
    collections::VecDeque,
    error::Error,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use uuid::Uuid;

mod ui;
use ui::{draw_ui, AppState, Screen, TuiMarket};

const WS_URL: &str = "ws://127.0.0.1:8080/ws";
const HTTP_BASE_URL: &str = "http://127.0.0.1:8080/api/v1";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let state = Arc::new(Mutex::new(AppState::new("BTCUSDT".to_string())));
    let (symbol_tx, symbol_rx) = tokio::sync::mpsc::channel::<String>(10);

    let state_clone = state.clone();
    tokio::spawn(async move {
        let _ = run_websocket_listener(state_clone, symbol_rx).await;
    });

    let state_clone_2 = state.clone();
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            let logged_in = {
                let s = state_clone_2.lock().unwrap();
                s.user_id.is_some()
            };
            if logged_in {
                let url = format!("{}/markets", HTTP_BASE_URL);
                if let Ok(res) = client.get(&url).send().await {
                    if let Ok(m_list) = res.json::<Vec<TuiMarket>>().await {
                        let mut s = state_clone_2.lock().unwrap();
                        s.markets = m_list;
                        if s.selected_market_idx >= s.markets.len() {
                            s.selected_market_idx = s.markets.len().saturating_sub(1);
                        }
                    }
                }
            }
        }
    });

    let res = run_app(&mut terminal, state, symbol_tx).await;

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        println!("{:?}", err);
    }

    Ok(())
}

async fn run_websocket_listener(
    state: Arc<Mutex<AppState>>,
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
                                            s.push_price(price);
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

async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    state: Arc<Mutex<AppState>>,
    symbol_tx: tokio::sync::mpsc::Sender<String>,
) -> io::Result<()> {
    let client = reqwest::Client::new();
    let tick_rate = Duration::from_millis(50);
    let mut last_tick = std::time::Instant::now();

    loop {
        {
            let s = state.lock().unwrap();
            terminal.draw(|f| draw_ui(f, &s))?;
        }

        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));

        if crossterm::event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    let screen = {
                        let s = state.lock().unwrap();
                        s.current_screen
                    };

                    match screen {
                        Screen::Login => match key.code {
                            KeyCode::Char('q') | KeyCode::Esc => {
                                return Ok(());
                            }
                            KeyCode::Tab => {
                                let mut s = state.lock().unwrap();
                                s.active_login_field = (s.active_login_field + 1) % 2;
                            }
                            KeyCode::Char(c) => {
                                let mut s = state.lock().unwrap();
                                if s.active_login_field == 0 {
                                    s.username_input.push(c);
                                } else {
                                    s.password_input.push(c);
                                }
                            }
                            KeyCode::Backspace => {
                                let mut s = state.lock().unwrap();
                                if s.active_login_field == 0 {
                                    s.username_input.pop();
                                } else {
                                    s.password_input.pop();
                                }
                            }
                            KeyCode::Enter => {
                                let (u_input, is_empty) = {
                                    let mut s = state.lock().unwrap();
                                    let is_empty = s.username_input.trim().is_empty();
                                    if is_empty {
                                        s.log_message = "Username cannot be empty.".to_string();
                                    } else {
                                        s.log_message =
                                            "Logging in & fetching balances...".to_string();
                                    }
                                    (s.username_input.clone(), is_empty)
                                };

                                if !is_empty {
                                    let client_clone = client.clone();
                                    let state_clone = state.clone();

                                    tokio::spawn(async move {
                                        let namespace = Uuid::nil();
                                        let user_id = Uuid::new_v5(&namespace, u_input.as_bytes());

                                        let bal_url = format!(
                                            "{}/accounts/{}/balance",
                                            HTTP_BASE_URL, user_id
                                        );
                                        match client_clone.get(&bal_url).send().await {
                                            Ok(res) => {
                                                if let Ok(json_val) =
                                                    res.json::<serde_json::Value>().await
                                                {
                                                    let avail_str = json_val
                                                        .get("available_balance")
                                                        .and_then(|v| v.as_str())
                                                        .unwrap_or("0.0");
                                                    let mut balance =
                                                        avail_str.parse::<f64>().unwrap_or(0.0);

                                                    if balance == 0.0 {
                                                        let dep_url = format!(
                                                            "{}/accounts/deposit",
                                                            HTTP_BASE_URL
                                                        );
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
                                                            if let Ok(dep_json) = dep_res
                                                                .json::<serde_json::Value>()
                                                                .await
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

                                                    let markets_url =
                                                        format!("{}/markets", HTTP_BASE_URL);
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
                                                    s.user_id = Some(user_id);
                                                    s.balance = balance;
                                                    s.markets = parsed_markets;
                                                    s.current_screen = Screen::Dashboard;
                                                }
                                            }
                                            Err(e) => {
                                                let mut s = state_clone.lock().unwrap();
                                                s.log_message = format!("Auth Error: {:?}", e);
                                            }
                                        }
                                    });
                                }
                            }
                            _ => {}
                        },
                        Screen::Dashboard => match key.code {
                            KeyCode::Up => {
                                let mut s = state.lock().unwrap();
                                if s.selected_market_idx > 0 {
                                    s.selected_market_idx -= 1;
                                }
                            }
                            KeyCode::Down => {
                                let mut s = state.lock().unwrap();
                                if !s.markets.is_empty()
                                    && s.selected_market_idx < s.markets.len() - 1
                                {
                                    s.selected_market_idx += 1;
                                }
                            }
                            KeyCode::Esc => {
                                let mut s = state.lock().unwrap();
                                s.current_screen = Screen::Login;
                                s.username_input.clear();
                                s.password_input.clear();
                                s.log_message = "Logged out successfully.".to_string();
                            }
                            KeyCode::Enter => {
                                let selected_market = {
                                    let mut s = state.lock().unwrap();
                                    if !s.markets.is_empty() {
                                        let selected_market =
                                            s.markets[s.selected_market_idx].symbol.clone();
                                        s.symbol = selected_market.clone();
                                        s.price_history = VecDeque::from(vec![s.current_price; 50]);
                                        s.bids.clear();
                                        s.asks.clear();
                                        s.trades.clear();
                                        s.current_screen = Screen::Trading;
                                        s.log_message = format!("Trading {} ...", selected_market);
                                        Some(selected_market)
                                    } else {
                                        None
                                    }
                                };

                                if let Some(m) = selected_market {
                                    let _ = symbol_tx.send(m).await;
                                }
                            }
                            _ => {}
                        },
                        Screen::Trading => match key.code {
                            KeyCode::Esc => {
                                let mut s = state.lock().unwrap();
                                s.current_screen = Screen::Dashboard;
                            }
                            KeyCode::Tab => {
                                let mut s = state.lock().unwrap();
                                if s.focus_panel == 0 {
                                    s.focus_panel = 1;
                                    s.active_input_field = 0;
                                } else {
                                    s.active_input_field = (s.active_input_field + 1) % 4;
                                }
                            }
                            KeyCode::BackTab => {
                                let mut s = state.lock().unwrap();
                                s.focus_panel = 0;
                            }
                            KeyCode::Left | KeyCode::Right => {
                                let mut s = state.lock().unwrap();
                                if s.focus_panel == 1 && s.active_input_field == 2 {
                                    s.side = if s.side == "BUY" {
                                        "SELL".to_string()
                                    } else {
                                        "BUY".to_string()
                                    };
                                }
                            }
                            KeyCode::Up | KeyCode::Down => {
                                let mut s = state.lock().unwrap();
                                if s.focus_panel == 1 && s.active_input_field == 3 {
                                    s.order_type = if s.order_type == "LIMIT" {
                                        "MARKET".to_string()
                                    } else {
                                        "LIMIT".to_string()
                                    };
                                }
                            }
                            KeyCode::Char('[') => {
                                let mut s = state.lock().unwrap();
                                s.orderbook_width_pct =
                                    s.orderbook_width_pct.saturating_sub(2).max(15);
                            }
                            KeyCode::Char(']') => {
                                let mut s = state.lock().unwrap();
                                s.orderbook_width_pct = (s.orderbook_width_pct + 2).min(85);
                            }
                            KeyCode::Char(c) => {
                                let mut s = state.lock().unwrap();
                                if s.focus_panel == 1 {
                                    if s.active_input_field == 0 {
                                        s.qty_input.push(c);
                                    } else if s.active_input_field == 1 && s.order_type == "LIMIT" {
                                        s.price_input.push(c);
                                    }
                                }
                            }
                            KeyCode::Backspace => {
                                let mut s = state.lock().unwrap();
                                if s.focus_panel == 1 {
                                    if s.active_input_field == 0 {
                                        s.qty_input.pop();
                                    } else if s.active_input_field == 1 && s.order_type == "LIMIT" {
                                        s.price_input.pop();
                                    }
                                }
                            }
                            KeyCode::Enter => {
                                let order_params = {
                                    let mut s = state.lock().unwrap();
                                    if s.focus_panel == 1 {
                                        s.log_message = "Submitting order...".to_string();
                                        Some((
                                            s.user_id.unwrap_or_else(Uuid::nil),
                                            s.symbol.clone(),
                                            s.side.clone(),
                                            s.order_type.clone(),
                                            s.qty_input.clone(),
                                            if s.order_type == "LIMIT" {
                                                Some(s.price_input.clone())
                                            } else {
                                                None
                                            },
                                        ))
                                    } else {
                                        None
                                    }
                                };

                                if let Some((u_id, sym, side, o_type, qty, price)) = order_params {
                                    let client_clone = client.clone();
                                    let state_clone = state.clone();
                                    let req_body = json!({
                                        "user_id": u_id,
                                        "symbol": sym,
                                        "side": side,
                                        "order_type": o_type,
                                        "quantity": qty,
                                        "price": price,
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
                                                let text = res.text().await.unwrap_or_else(|_| {
                                                    "Empty Response".to_string()
                                                });
                                                update_order_response(&state_clone, text);

                                                let bal_url = format!(
                                                    "{}/accounts/{}/balance",
                                                    HTTP_BASE_URL, u_id
                                                );
                                                if let Ok(bal_res) =
                                                    client_clone.get(&bal_url).send().await
                                                {
                                                    if let Ok(json_val) =
                                                        bal_res.json::<serde_json::Value>().await
                                                    {
                                                        let avail_str = json_val
                                                            .get("available_balance")
                                                            .and_then(|v| v.as_str())
                                                            .unwrap_or("0.0");
                                                        let balance =
                                                            avail_str.parse::<f64>().unwrap_or(0.0);
                                                        update_balance(&state_clone, balance);
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                update_order_fail(&state_clone, format!("{:?}", e));
                                            }
                                        }
                                    });
                                }
                            }
                            _ => {}
                        },
                    }
                }
            }
        }

        if last_tick.elapsed() >= tick_rate {
            last_tick = std::time::Instant::now();
        }
    }
}

fn update_order_response(state: &Arc<Mutex<AppState>>, text: String) {
    let mut s = state.lock().unwrap();
    s.log_message = format!("Order Response: {}", text);
}

fn update_balance(state: &Arc<Mutex<AppState>>, balance: f64) {
    let mut s = state.lock().unwrap();
    s.balance = balance;
}

fn update_order_fail(state: &Arc<Mutex<AppState>>, err_msg: String) {
    let mut s = state.lock().unwrap();
    s.log_message = format!("Order Fail: {}", err_msg);
}
