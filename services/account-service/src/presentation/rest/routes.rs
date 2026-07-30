use actix_web::web;
use crate::presentation::rest::controller::account_controller::{deposit, get_balance, withdraw};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/accounts")
            .route("/deposit", web::post().to(deposit))
            .route("/withdraw", web::post().to(withdraw))
            .route("/{user_id}/{asset}", web::get().to(get_balance)),
    );
}
