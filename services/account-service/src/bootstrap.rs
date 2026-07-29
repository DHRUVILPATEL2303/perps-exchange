use std::io::Result;

use actix_web::HttpServer;
use config::app::AppConfig;

use crate::state::AppState;

pub async fn run() -> Result<()> {
    let config = AppConfig::load("account-service").expect("Failed to Load Config");

    let app_state = actix_web::web::Data::new(AppState {
        config: std::sync::Arc::new(config.clone()),
    });

    HttpServer::new(move || actix_web::App::new().app_data(app_state.clone()))
        .bind((config.server.host.clone(), config.server.port.clone()))?
        .run()
        .await
}
