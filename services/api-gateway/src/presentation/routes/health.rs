use actix_web::web;

use crate::presentation::handlers::health_handler::health;

pub fn health_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("")
            .route("/health", web::get().to(health))
    );
}