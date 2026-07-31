use std::sync::Arc;
use tokio::sync::Mutex;
use actix_web::{App, HttpServer, web::Data};
use config::app::AppConfig;
use proto::market::market_service_client::MarketServiceClient;
use proto::account::account_service_client::AccountServiceClient;
use proto::trading::trading_service_client::TradingServiceClient;
use crate::presentation::router::router::configure_routes;
use crate::state::AppState;
use futures_util::StreamExt;

pub async fn run() -> std::io::Result<()> {
    tracing::info!("Starting API Gateway...");

    let config = AppConfig::load("api-gateway").expect("Failed to load config");

    let market_url = std::env::var("MARKET_SERVICE_URL").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let trading_url = std::env::var("TRADING_SERVICE_URL").unwrap_or_else(|_| "http://127.0.0.1:50052".to_string());
    let account_url = std::env::var("ACCOUNT_SERVICE_URL").unwrap_or_else(|_| "http://127.0.0.1:50053".to_string());

    let market_client = MarketServiceClient::connect(market_url)
        .await
        .expect("Failed to connect to Market Service");

    let trading_client = TradingServiceClient::connect(trading_url)
        .await
        .expect("Failed to connect to Trading Service");

    let account_client = AccountServiceClient::connect(account_url)
        .await
        .expect("Failed to connect to Account Service");

    let redis_url = format!("redis://{}:{}", config.redis.host, config.redis.port);
    let redis_client = redis::Client::open(redis_url).expect("Failed to open Redis client");

    let ws_sessions = Arc::new(Mutex::new(Vec::new()));

    let app_state = Data::new(AppState {
        config: Arc::new(config.clone()),
        market_client,
        account_client,
        trading_client,
        redis_client: redis_client.clone(),
        ws_sessions: ws_sessions.clone(),
    });

    let ws_sessions_for_broadcast = ws_sessions.clone();
    let redis_client_for_broadcast = redis_client.clone();
    tokio::spawn(async move {
        loop {
            if let Ok(mut pubsub) = redis_client_for_broadcast.get_async_pubsub().await {
                if pubsub.subscribe("price-ticks").await.is_ok() {
                    let mut msg_stream = pubsub.on_message();
                    while let Some(msg) = msg_stream.next().await {
                        if let Ok(payload) = msg.get_payload::<String>() {
                            let mut sessions = ws_sessions_for_broadcast.lock().await;
                            let mut active_sessions = Vec::new();
                            for mut session in sessions.drain(..) {
                                if session.text(payload.clone()).await.is_ok() {
                                    active_sessions.push(session);
                                }
                            }
                            *sessions = active_sessions;
                        }
                    }
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
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
