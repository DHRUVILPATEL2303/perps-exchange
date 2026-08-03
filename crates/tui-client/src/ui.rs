use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{
        Axis, Block, BorderType, Borders, Cell, Chart, Dataset, GraphType, List, ListItem,
        Paragraph, Row, Table,
    },
    Frame,
};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::VecDeque;
use uuid::Uuid;

#[derive(Clone, Deserialize)]
pub struct TuiMarket {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub tick_size: String,
    pub lot_size: String,
    pub status: String,
}

#[derive(PartialEq, Eq, Clone, Copy)]
pub enum Screen {
    Login,
    Dashboard,
    Trading,
}

pub struct AppState {
    pub current_screen: Screen,
    pub user_id: Option<Uuid>,
    pub username_input: String,
    pub password_input: String,
    pub active_login_field: usize,

    pub markets: Vec<TuiMarket>,
    pub selected_market_idx: usize,

    pub symbol: String,
    pub current_price: f64,
    pub price_history: VecDeque<f64>,
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
    pub trades: VecDeque<(String, f64, f64, String)>,
    pub balance: f64,
    pub margin_locked: f64,

    pub side: String,
    pub order_type: String,
    pub qty_input: String,
    pub price_input: String,

    pub active_input_field: usize,
    pub log_message: String,
    pub focus_panel: usize,
    pub orderbook_width_pct: u16,
}

impl AppState {
    pub fn new(symbol: String) -> Self {
        Self {
            current_screen: Screen::Login,
            user_id: None,
            username_input: String::new(),
            password_input: String::new(),
            active_login_field: 0,
            markets: Vec::new(),
            selected_market_idx: 0,
            symbol,
            current_price: 64250.0,
            price_history: VecDeque::from(vec![64250.0; 50]),
            bids: Vec::new(),
            asks: Vec::new(),
            trades: VecDeque::new(),
            balance: 0.0,
            margin_locked: 0.0,
            side: "BUY".to_string(),
            order_type: "LIMIT".to_string(),
            qty_input: "0.1".to_string(),
            price_input: "64250".to_string(),
            active_input_field: 0,
            log_message: "Please enter your username and password to log in.".to_string(),
            focus_panel: 0,
            orderbook_width_pct: 40,
        }
    }

    pub fn push_price(&mut self, price: f64) {
        self.current_price = price;
        self.price_history.push_back(price);
        if self.price_history.len() > 100 {
            self.price_history.pop_front();
        }
    }
}

pub fn draw_ui(frame: &mut Frame, state: &AppState) {
    match state.current_screen {
        Screen::Login => draw_login_screen(frame, state),
        Screen::Dashboard => draw_dashboard_screen(frame, state),
        Screen::Trading => draw_trading_screen(frame, state),
    }
}

fn draw_login_screen(frame: &mut Frame, state: &AppState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(1),
            Constraint::Length(12),
            Constraint::Min(1),
        ])
        .split(frame.area());

    let horizontal_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Min(1),
            Constraint::Length(50),
            Constraint::Min(1),
        ])
        .split(chunks[1]);

    let area = horizontal_chunks[1];

    let block = Block::default()
        .title(" PERPS EXCHANGE LOGIN ")
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Yellow));

    let form_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
        ])
        .split(block.inner(area));

    let u_style = if state.active_login_field == 0 {
        Style::default().fg(Color::Black).bg(Color::Yellow)
    } else {
        Style::default().fg(Color::White)
    };

    let p_style = if state.active_login_field == 1 {
        Style::default().fg(Color::Black).bg(Color::Yellow)
    } else {
        Style::default().fg(Color::White)
    };

    let username_p = Paragraph::new(format!(" Username: {}", state.username_input))
        .style(u_style)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(Color::DarkGray)),
        );

    let masked_password = "*".repeat(state.password_input.len());
    let password_p = Paragraph::new(format!(" Password: {}", masked_password))
        .style(p_style)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(Color::DarkGray)),
        );

    let status_p = Paragraph::new(state.log_message.as_str())
        .style(Style::default().fg(Color::LightYellow))
        .block(Block::default());

    frame.render_widget(block, area);
    frame.render_widget(username_p, form_layout[0]);
    frame.render_widget(password_p, form_layout[1]);
    frame.render_widget(status_p, form_layout[2]);
}

fn draw_dashboard_screen(frame: &mut Frame, state: &AppState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Length(3),
        ])
        .split(frame.area());

    let header_text = vec![
        Span::styled(
            " DASHBOARD HUB ",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(" Wallet Balance: ${:.2} USDT ", state.balance),
            Style::default().fg(Color::Cyan),
        ),
        Span::styled(
            format!(
                " User ID: {} ",
                state
                    .user_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "None".to_string())
            ),
            Style::default().fg(Color::Gray),
        ),
    ];
    let header_p = Paragraph::new(Line::from(header_text)).block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    frame.render_widget(header_p, chunks[0]);

    let rows: Vec<Row> = state
        .markets
        .iter()
        .enumerate()
        .map(|(idx, m)| {
            let style = if idx == state.selected_market_idx {
                Style::default().fg(Color::Black).bg(Color::Yellow)
            } else {
                Style::default().fg(Color::White)
            };
            Row::new(vec![
                Cell::from(m.symbol.as_str()),
                Cell::from(m.base_asset.as_str()),
                Cell::from(m.quote_asset.as_str()),
                Cell::from(m.tick_size.as_str()),
                Cell::from(m.lot_size.as_str()),
                Cell::from(m.status.as_str()),
            ])
            .style(style)
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(20),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Percentage(20),
        ],
    )
    .header(Row::new(vec![
        CellStyled(
            "Symbol",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        CellStyled(
            "Base Asset",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        CellStyled(
            "Quote Asset",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        CellStyled(
            "Tick Size",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        CellStyled(
            "Lot Size",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        CellStyled(
            "Status",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
    ]))
    .block(
        Block::default()
            .title(" AVAILABLE PERPETUAL MARKETS ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::DarkGray)),
    );

    frame.render_widget(table, chunks[1]);

    let footer_p = Paragraph::new(
        "Use Up/Down Arrow keys to highlight a market. Press Enter to trade. Esc to log out.",
    )
    .style(Style::default().fg(Color::DarkGray))
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    frame.render_widget(footer_p, chunks[2]);
}

fn draw_trading_screen(frame: &mut Frame, state: &AppState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(7),
        ])
        .split(frame.area());

    draw_header(frame, chunks[0], state);

    let main_split = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(state.orderbook_width_pct),
            Constraint::Percentage(100 - state.orderbook_width_pct),
        ])
        .split(chunks[1]);

    let left_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(5), Constraint::Length(12)])
        .split(main_split[0]);

    draw_orderbook(frame, left_chunks[0], state);
    draw_order_form(frame, left_chunks[1], state);

    let right_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(5), Constraint::Length(12)])
        .split(main_split[1]);

    draw_chart(frame, right_chunks[0], state);
    draw_trades(frame, right_chunks[1], state);

    draw_positions_and_balances(frame, chunks[2], state);
}

fn draw_header(frame: &mut Frame, area: Rect, state: &AppState) {
    let header_text = vec![
        Span::styled(
            " TRADING DESK ",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(" {} ", state.symbol),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(" Last Price: ${:.2} ", state.current_price),
            Style::default().fg(Color::Cyan),
        ),
        Span::styled(
            format!(" Log: {} ", state.log_message),
            Style::default().fg(Color::LightYellow),
        ),
        Span::styled(" [Esc] Back to Markets ", Style::default().fg(Color::Gray)),
    ];
    let header_line = Line::from(header_text);
    let paragraph = Paragraph::new(header_line).block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    frame.render_widget(paragraph, area);
}

fn draw_orderbook(frame: &mut Frame, area: Rect, state: &AppState) {
    let inner_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    let mut asks_items = Vec::new();
    for (price, qty) in state.asks.iter().take(10).rev() {
        let bar_len = (qty.to_f64().unwrap_or(0.0) * 10.0).min(12.0) as usize;
        let bar = "█".repeat(bar_len);
        let item = ListItem::new(Line::from(vec![
            Span::styled(format!("{:<10.2} ", price), Style::default().fg(Color::Red)),
            Span::styled(format!("{:<7.4} ", qty), Style::default().fg(Color::Gray)),
            Span::styled(bar, Style::default().fg(Color::Rgb(150, 40, 40))),
        ]));
        asks_items.push(item);
    }
    let asks_list = List::new(asks_items).block(
        Block::default()
            .title(" ASKS (Sells) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    frame.render_widget(asks_list, inner_chunks[1]);

    let mut bids_items = Vec::new();
    for (price, qty) in state.bids.iter().take(10) {
        let bar_len = (qty.to_f64().unwrap_or(0.0) * 10.0).min(12.0) as usize;
        let bar = "█".repeat(bar_len);
        let item = ListItem::new(Line::from(vec![
            Span::styled(
                format!("{:<10.2} ", price),
                Style::default().fg(Color::Green),
            ),
            Span::styled(format!("{:<7.4} ", qty), Style::default().fg(Color::Gray)),
            Span::styled(bar, Style::default().fg(Color::Rgb(40, 150, 40))),
        ]));
        bids_items.push(item);
    }
    let bids_list = List::new(bids_items).block(
        Block::default()
            .title(" BIDS (Buys) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    frame.render_widget(bids_list, inner_chunks[0]);
}

fn draw_order_form(frame: &mut Frame, area: Rect, state: &AppState) {
    let focus_style = if state.focus_panel == 1 {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let block = Block::default()
        .title(" ORDER ENTRY ")
        .borders(Borders::ALL)
        .border_style(focus_style);

    let form_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(2),
            Constraint::Length(2),
            Constraint::Length(2),
        ])
        .split(block.inner(area));

    let field_style = |idx: usize| {
        if state.focus_panel == 1 && state.active_input_field == idx {
            Style::default().fg(Color::Black).bg(Color::Yellow)
        } else {
            Style::default().fg(Color::White)
        }
    };

    let side_color = if state.side == "BUY" {
        Color::Green
    } else {
        Color::Red
    };
    let side_line = Line::from(vec![
        Span::raw("Side: "),
        Span::styled(
            format!(" [{}] ", state.side),
            Style::default()
                .fg(side_color)
                .add_modifier(Modifier::BOLD)
                .bg(if state.active_input_field == 2 && state.focus_panel == 1 {
                    Color::Yellow
                } else {
                    Color::Reset
                }),
        ),
        Span::raw(" (Left/Right arrow)"),
    ]);
    frame.render_widget(Paragraph::new(side_line), form_layout[0]);

    let type_line = Line::from(vec![
        Span::raw("Type: "),
        Span::styled(
            format!(" [{}] ", state.order_type),
            Style::default().add_modifier(Modifier::BOLD).bg(
                if state.active_input_field == 3 && state.focus_panel == 1 {
                    Color::Yellow
                } else {
                    Color::Reset
                },
            ),
        ),
        Span::raw(" (Up/Down arrow)"),
    ]);
    frame.render_widget(Paragraph::new(type_line), form_layout[1]);

    let qty_p =
        Paragraph::new(format!("Quantity (BTC): {}", state.qty_input)).style(field_style(0));
    frame.render_widget(qty_p, form_layout[2]);

    let price_str = if state.order_type == "MARKET" {
        "MARKET".to_string()
    } else {
        state.price_input.clone()
    };
    let price_p = Paragraph::new(format!("Price (USDT):   {}", price_str)).style(field_style(1));
    frame.render_widget(price_p, form_layout[3]);

    frame.render_widget(block, area);
}

fn draw_chart(frame: &mut Frame, area: Rect, state: &AppState) {
    let data: Vec<(f64, f64)> = state
        .price_history
        .iter()
        .enumerate()
        .map(|(i, &p)| (i as f64, p))
        .collect();

    let min_p = state
        .price_history
        .iter()
        .fold(f64::INFINITY, |a, &b| a.min(b))
        - 2.0;
    let max_p = state
        .price_history
        .iter()
        .fold(f64::NEG_INFINITY, |a, &b| a.max(b))
        + 2.0;

    let dataset = Dataset::default()
        .name("Trades Line")
        .marker(symbols::Marker::Braille)
        .graph_type(GraphType::Line)
        .style(Style::default().fg(Color::Cyan))
        .data(&data);

    let x_bounds = [0.0, 100.0];
    let y_bounds = [min_p, max_p];

    let chart = Chart::new(vec![dataset])
        .block(
            Block::default()
                .title(" LIVE PRICE CHART ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        )
        .x_axis(
            Axis::default()
                .title("Time Offset")
                .style(Style::default().fg(Color::Gray))
                .bounds(x_bounds),
        )
        .y_axis(
            Axis::default()
                .title("Price")
                .style(Style::default().fg(Color::Gray))
                .bounds(y_bounds)
                .labels(vec![
                    Span::raw(format!("{:.1}", min_p)),
                    Span::raw(format!("{:.1}", (min_p + max_p) / 2.0)),
                    Span::raw(format!("{:.1}", max_p)),
                ]),
        );

    frame.render_widget(chart, area);
}

fn draw_trades(frame: &mut Frame, area: Rect, state: &AppState) {
    let mut items = Vec::new();
    for (time, price, qty, side) in state.trades.iter().take(20) {
        let side_style = if side == "BUY" {
            Style::default().fg(Color::Green)
        } else {
            Style::default().fg(Color::Red)
        };
        let item = ListItem::new(Line::from(vec![
            Span::styled(format!("[{}] ", time), Style::default().fg(Color::DarkGray)),
            Span::styled(format!("{:<8} ", side), side_style),
            Span::styled(
                format!("Price: {:<10.2} ", price),
                Style::default().fg(Color::White),
            ),
            Span::styled(format!("Qty: {:.4}", qty), Style::default().fg(Color::Gray)),
        ]));
        items.push(item);
    }

    let list = List::new(items).block(
        Block::default()
            .title(" RECENT TRADES ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    frame.render_widget(list, area);
}

fn draw_positions_and_balances(frame: &mut Frame, area: Rect, state: &AppState) {
    let position_header = Row::new(vec![
        CellStyled(
            "Asset / Symbol",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        CellStyled(
            "Size",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        CellStyled(
            "Entry Price",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        CellStyled(
            "Mark Price",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        CellStyled(
            "Margin Locked",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        CellStyled(
            "Unrealized PnL",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
    ]);

    let mock_rows = vec![
        Row::new(vec![
            CellStyled("USDT (Wallet Balance)", Style::default().fg(Color::Cyan)),
            CellStyled(
                format!("{:.2}", state.balance),
                Style::default().fg(Color::White),
            ),
            CellStyled("-", Style::default().fg(Color::DarkGray)),
            CellStyled("-", Style::default().fg(Color::DarkGray)),
            CellStyled(
                format!("{:.2}", state.margin_locked),
                Style::default().fg(Color::Gray),
            ),
            CellStyled("-", Style::default().fg(Color::DarkGray)),
        ]),
        Row::new(vec![
            CellStyled(
                format!("{} (Long Position)", state.symbol),
                Style::default().fg(Color::Green),
            ),
            CellStyled("0.25 BTC", Style::default().fg(Color::White)),
            CellStyled("64,150.00", Style::default().fg(Color::White)),
            CellStyled(
                format!("{:.2}", state.current_price),
                Style::default().fg(Color::White),
            ),
            CellStyled("1,603.75", Style::default().fg(Color::Gray)),
            CellStyled(
                format!("{:.2}", 0.25 * (state.current_price - 64150.0)),
                Style::default().fg(if state.current_price >= 64150.0 {
                    Color::Green
                } else {
                    Color::Red
                }),
            ),
        ]),
    ];

    let table = Table::new(
        mock_rows,
        [
            Constraint::Percentage(25),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
        ],
    )
    .header(position_header)
    .block(
        Block::default()
            .title(" POSITIONS & WALLET ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)),
    );

    frame.render_widget(table, area);
}

#[allow(non_snake_case)]
fn CellStyled<T: Into<String>>(text: T, style: Style) -> Span<'static> {
    Span::styled(text.into(), style)
}
