use std::sync::Arc;
use tokio::sync::Mutex;
use actix_web::{App, HttpServer, web::Data};
use config::app::AppConfig;
use proto::market::market_service_client::MarketServiceClient;
use proto::account::account_service_client::AccountServiceClient;
use proto::trading::trading_service_client::TradingServiceClient;
use crate::presentation::router::router::configure_routes;
use crate::state::AppState;

pub async fn run() -> std::io::Result<()> {
    tracing::info!("Starting API Gateway...");

    let config = AppConfig::load("api-gateway").expect("Failed to load config");

    let market_client = MarketServiceClient::connect("http://127.0.0.1:50051")
        .await
        .expect("Failed to connect to Market Service");

    let trading_client = TradingServiceClient::connect("http://127.0.0.1:50052")
        .await
        .expect("Failed to connect to Trading Service");

    let account_client = AccountServiceClient::connect("http://127.0.0.1:50053")
        .await
        .expect("Failed to connect to Account Service");

    let app_state = Data::new(AppState {
        config: Arc::new(config.clone()),
        market_client,
        account_client,
        trading_client,
    });


    println!("API Gateway started at {}:{}", config.server.host, config.server.port);

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .configure(configure_routes)
    })
    .bind((config.server.host.clone(), config.server.port))?
    .run()
    .await
}
