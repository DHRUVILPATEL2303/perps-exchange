use crate::presentation::handlers::{
    account_handler::get_balance,
    market_handler::{list_markets, get_candles},
    trading_handler::{cancel_order, get_positions, place_order, get_open_orders},
    ws_handler::ws_index,
};
use crate::presentation::routes::health::health_routes;
use actix_web::web;

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1")
            .route("/markets", web::get().to(list_markets))
            .route("/markets/{symbol}/candles", web::get().to(get_candles))
            .route("/accounts/{user_id}/balance", web::get().to(get_balance))
            .route("/positions/{user_id}", web::get().to(get_positions))
            .route("/orders", web::post().to(place_order))
            .route("/orders/cancel", web::post().to(cancel_order))
            .route("/orders/open/{user_id}", web::get().to(get_open_orders)),
    );

    cfg.route("/ws", web::get().to(ws_index));

    health_routes(cfg);
}
