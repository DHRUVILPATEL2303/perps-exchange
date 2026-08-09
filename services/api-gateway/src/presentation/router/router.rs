use crate::presentation::handlers::{
    account_handler::{get_balance, deposit_funds, withdraw_funds, get_transaction_history, get_deposit_address},
    market_handler::{list_markets, get_candles, create_market},
    trading_handler::{cancel_order, get_positions, place_order, get_open_orders, get_trade_history, adjust_position_margin},
    ws_handler::ws_index,
    auth_handler::{get_challenge, login},
};
use crate::presentation::routes::health::health_routes;
use actix_web::web;

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1")
            .route("/auth/challenge", web::post().to(get_challenge))
            .route("/auth/login", web::post().to(login))
            .route("/markets", web::get().to(list_markets))
            .route("/markets", web::post().to(create_market))
            .route("/markets/{symbol}/candles", web::get().to(get_candles))
            .route("/accounts/{user_id}/balance", web::get().to(get_balance))
            .route("/accounts/deposit", web::post().to(deposit_funds))
            .route("/accounts/withdraw", web::post().to(withdraw_funds))
            .route("/accounts/{user_id}/transactions", web::get().to(get_transaction_history))
            .route("/accounts/{user_id}/deposit-address", web::get().to(get_deposit_address))
            .route("/positions/{user_id}", web::get().to(get_positions))
            .route("/positions/adjust-margin", web::post().to(adjust_position_margin))
            .route("/orders", web::post().to(place_order))
            .route("/orders/cancel", web::post().to(cancel_order))
            .route("/orders/open/{user_id}", web::get().to(get_open_orders))
            .route("/trades/history/{user_id}", web::get().to(get_trade_history)),
    );

    cfg.route("/ws", web::get().to(ws_index));

    health_routes(cfg);
}
