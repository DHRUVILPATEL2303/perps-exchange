use actix_web::web;

use crate::presentation::rest::controller::market_controller::{self, get_market, list_markets};


pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/markets")
            .route("", web::get().to(list_markets))
            .route("/{symbol}", web::get().to(get_market)),
    );
}