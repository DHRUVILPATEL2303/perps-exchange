use std::{io::Result, sync::Arc};

use actix_web::HttpServer;
use config::app::AppConfig;
use database::manager::DatabaseManager;

use crate::state::AppState;

pub async fn run() -> Result<()> {
    let config = AppConfig::load("market-service").expect("Failed to Load Config");

    let db = DatabaseManager::new(&config.database)
        .await
        .expect("Failed to create database connection");

    if config.database.auto_migrate {
        sqlx::migrate!("./migrations")
            .run(db.pool())
            .await
            .expect("Migration failed");
    }
    let app_state = actix_web::web::Data::new(AppState {
        config: std::sync::Arc::new(config.clone()),
        db: Arc::new(db.clone()),
    });

    HttpServer::new(move || actix_web::App::new().app_data(app_state.clone()))
        .bind((config.server.host.clone(), config.server.port.clone()))?
        .run()
        .await
}
