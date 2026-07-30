use actix_web::web;

use crate::presentation::rest::controller::market_controller::{self, create_market, get_market, list_markets, update_market};


pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/markets")
            .route("", web::get().to(list_markets))
            .route("", web::post().to(create_market))
            .route("{symbol}", web::patch().to(update_market))
            .route("/{symbol}", web::get().to(get_market)),
    );
}