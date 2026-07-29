use actix_web::web;

use crate::presentation::rest::controller::market_controller;


pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/markets")
            .route(
                "",
                web::get().to(market_controller::list_markets),
            ),
    );
}