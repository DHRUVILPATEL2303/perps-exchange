use actix_web::web;

use crate::presentation::routes::health::health_routes;
pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    health_routes(cfg);
}
