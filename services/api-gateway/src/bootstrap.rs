use actix_web::{App, HttpServer};

use crate::presentation::router::router::configure_routes;

pub async fn run() -> std::io::Result<()> {
    tracing::info!("Starting API Gateway...");

    HttpServer::new(|| App::new().configure(configure_routes))
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
}
