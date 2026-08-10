use crate::presentation::handlers::{
    account_handler::{
        deposit_funds, get_balance, get_deposit_address, get_transaction_history, withdraw_funds,
    }, auth_handler::{get_challenge, get_telegram_token, login}, market_handler::{create_market, get_candles, get_ticker, list_markets, get_recent_trades}, trading_handler::{
        adjust_position_margin, cancel_order, get_open_orders, get_positions, get_trade_history,
        place_order,
    }, ws_handler::ws_index,
};
use crate::presentation::routes::health::health_routes;
use actix_web::web;

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1")
            .route("/auth/challenge", web::post().to(get_challenge))
            .route("/auth/login", web::post().to(login))
            .route("/auth/telegram-token", web::post().to(get_telegram_token))
            .route("/markets", web::get().to(list_markets))
            .route("/markets/{symbol}/ticker", web::get().to(get_ticker))
            .route("/markets/{symbol}/trades", web::get().to(get_recent_trades))
            .route("/markets", web::post().to(create_market))
            .route("/markets/{symbol}/candles", web::get().to(get_candles))
            .route("/accounts/{user_id}/balance", web::get().to(get_balance))
            .route("/accounts/deposit", web::post().to(deposit_funds))
            .route("/accounts/withdraw", web::post().to(withdraw_funds))
            .route(
                "/accounts/{user_id}/transactions",
                web::get().to(get_transaction_history),
            )
            .route(
                "/accounts/{user_id}/deposit-address",
                web::get().to(get_deposit_address),
            )
            .route("/positions/{user_id}", web::get().to(get_positions))
            .route(
                "/positions/adjust-margin",
                web::post().to(adjust_position_margin),
            )
            .route("/orders", web::post().to(place_order))
            .route("/orders/cancel", web::post().to(cancel_order))
            .route("/orders/open/{user_id}", web::get().to(get_open_orders))
            .route(
                "/trades/history/{user_id}",
                web::get().to(get_trade_history),
            ),
    );

    cfg.route("/ws", web::get().to(ws_index));

    health_routes(cfg);
}
