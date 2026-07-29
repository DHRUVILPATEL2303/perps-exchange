use std::{io::Result, sync::Arc};

use actix_web::{HttpServer, web::Data};
use config::app::AppConfig;
use database::manager::DatabaseManager;


use crate::{application::services::market_service::MarketService, infrastructure::repositories::postgres_market_repository::PostgresMarketRepository, presentation, state::AppState};

pub async fn run() -> Result<()> {
    let config = AppConfig::load("market-service").expect("Failed to Load Config");

    let db = Arc::new(
        DatabaseManager::new(&config.database)
            .await
            .expect("Database connection failed"),
    );
    
    let repository = Arc::new(
        PostgresMarketRepository::new(db.pool().clone()),
    );
    
    let market_service = Arc::new(
        MarketService::new(repository),
    );
    
    let state = Data::new(AppState {
        config: Arc::new(config.clone()),
        db,
        market_service,
    });

    HttpServer::new(move || 
        actix_web::App::new().app_data(state.clone())
            .configure(presentation::rest::routes::configure)
)
        .bind((config.server.host.clone(), config.server.port.clone()))?
        .run()
        .await
}
