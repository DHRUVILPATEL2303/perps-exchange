use std::sync::Arc;

use crate::{presentation::router::router::configure_routes, state::AppState};
use actix_web::{App, HttpServer};



pub async fn run() -> std::io::Result<()> {


    let app_config= config::settings::AppConfig::load().expect("Failed to Load Configuration");


    tracing::info!("Starting API Gateway...");

    let app_state = actix_web::web::Data::new({
        AppState{
            config : Arc::new(app_config.clone())
        }
    });

    HttpServer::new(move || App::new().app_data(app_state.clone()).configure(configure_routes))
        .bind((app_config.server.host.clone(),app_config.server.port))?
        .run()
        .await
}
