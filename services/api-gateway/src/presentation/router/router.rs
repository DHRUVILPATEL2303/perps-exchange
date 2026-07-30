use actix_web::web;
use crate::presentation::handlers::{
    market_handler::list_markets,
    account_handler::get_balance,
    trading_handler::{get_positions, place_order, cancel_order},
};
use crate::presentation::routes::health::health_routes;

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1")
            .route("/markets", web::get().to(list_markets))
            .route("/accounts/{user_id}/balance", web::get().to(get_balance))
            .route("/positions/{user_id}", web::get().to(get_positions))
            .route("/orders", web::post().to(place_order))
            .route("/orders/cancel", web::post().to(cancel_order)),
    );

    health_routes(cfg);
}
