use std::sync::Arc;

use crate::{application::services::health_service::HealthService,infrastructure::repositories::in_memory_health_repository::InMemoryHealthRepository, presentation::router::router::configure_routes, state::AppState};
use actix_web::{App, HttpServer};



pub async fn run() -> std::io::Result<()> {


    let app_config = config::AppConfig::load("api-gateway").expect("Failed to Load Configuration");
    let health_repository = Arc::new(
        InMemoryHealthRepository::new(app_config.app_name.clone())
    );
    let health_service = Arc::new(HealthService::new(health_repository));


    tracing::info!("Starting API Gateway...");

    let app_state = actix_web::web::Data::new({
        AppState{
            config : Arc::new(app_config.clone()),
            health_service : health_service
        }
    });

    HttpServer::new(move || App::new().app_data(app_state.clone()).configure(configure_routes))
        .bind((app_config.server.host.clone(),app_config.server.port))?
        .run()
        .await
}
