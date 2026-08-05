use crate::presentation::handlers::wt_server::run_webtransport_server;
use crate::presentation::router::router::configure_routes;
use crate::state::AppState;
use actix_web::{App, HttpServer, web::Data};
use config::app::AppConfig;
use futures_util::StreamExt;
use proto::account::account_service_client::AccountServiceClient;
use proto::market::market_service_client::MarketServiceClient;
use proto::trading::trading_service_client::TradingServiceClient;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
#[derive(Serialize, Deserialize)]
struct KafkaDepthUpdate {
    symbol: String,
    bids: Vec<(rust_decimal::Decimal, rust_decimal::Decimal)>,
    asks: Vec<(rust_decimal::Decimal, rust_decimal::Decimal)>,
    timestamp: i64,
}

#[derive(Serialize, Deserialize)]
struct KafkaTradeEvent {
    symbol: String,
    price: rust_decimal::Decimal,
    quantity: rust_decimal::Decimal,
    taker_side: String,
    maker_user_id: String,
    taker_user_id: String,
}

use proto::chart::chart_service_client::ChartServiceClient;

pub async fn run() -> std::io::Result<()> {
    tracing::info!("Starting API Gateway...");

    let config = AppConfig::load("api-gateway").expect("Failed to load config");

    let market_url = std::env::var("MARKET_SERVICE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let trading_url = std::env::var("TRADING_SERVICE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:50052".to_string());
    let account_url = std::env::var("ACCOUNT_SERVICE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:50053".to_string());
    let chart_url =
        std::env::var("CHART_SERVICE_URL").unwrap_or_else(|_| "http://127.0.0.1:50058".to_string());

    let market_client = MarketServiceClient::connect(market_url)
        .await
        .expect("Failed to connect to Market Service");

    let pool_size = 16;
    let mut trading_clients = Vec::with_capacity(pool_size);
    for _ in 0..pool_size {
        let client = TradingServiceClient::connect(trading_url.clone())
            .await
            .expect("Failed to connect to Trading Service");
        trading_clients.push(client);
    }
    let trading_pool_index = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let account_client = AccountServiceClient::connect(account_url)
        .await
        .expect("Failed to connect to Account Service");

    let chart_client = ChartServiceClient::connect(chart_url)
        .await
        .expect("Failed to connect to Chart Service");

    let redis_url = format!("redis://{}:{}", config.redis.host, config.redis.port);
    let redis_client = redis::Client::open(redis_url).expect("Failed to open Redis client");

    let ws_sessions = Arc::new(Mutex::new(Vec::new()));

    let app_state = Data::new(AppState {
        config: Arc::new(config.clone()),
        market_client,
        account_client,
        trading_clients,
        trading_pool_index,
        redis_client: redis_client.clone(),
        ws_sessions: ws_sessions.clone(),
        chart_client,
    });

    let redis_for_wt = redis_client.clone();
    tokio::spawn(async move {
        if let Err(e) = run_webtransport_server(redis_for_wt).await {
            tracing::error!("WebTransport Server crashed: {:?}", e);
        }
    });

    println!(
        "API Gateway started at {}:{}",
        config.server.host, config.server.port
    );

    HttpServer::new(move || {
        let cors = actix_cors::Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .max_age(3600);

        App::new()
            .wrap(cors)
            .wrap(actix_web::middleware::Condition::new(
                std::env::var("DISABLE_LOGS").unwrap_or_default() != "true",
                actix_web::middleware::Logger::new("%a \"%r\" %s %b %Dms"),
            ))
            .wrap(telemetry::http::HttpMetrics)
            .service(telemetry::http::metrics_handler)
            .app_data(app_state.clone())
            .configure(configure_routes)
    })
    .bind((config.server.host.clone(), config.server.port))?
    .run()
    .await
}
