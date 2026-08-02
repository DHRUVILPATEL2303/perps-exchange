use std::sync::Arc;
use tokio::sync::Mutex;
use actix_web::{App, HttpServer, web::Data};
use config::app::AppConfig;
use proto::market::market_service_client::MarketServiceClient;
use proto::account::account_service_client::AccountServiceClient;
use proto::trading::trading_service_client::TradingServiceClient;
use crate::presentation::router::router::configure_routes;
use crate::state::AppState;
use crate::presentation::handlers::wt_server::run_webtransport_server;
use futures_util::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use serde::{Deserialize, Serialize};
use redis::AsyncCommands;
#[derive(Serialize,Deserialize)]
struct KafkaDepthUpdate {
    symbol: String,
    bids: Vec<(rust_decimal::Decimal, rust_decimal::Decimal)>,
    asks: Vec<(rust_decimal::Decimal, rust_decimal::Decimal)>,
    timestamp: i64,
}

#[derive(Serialize,Deserialize)]
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

    let market_url = std::env::var("MARKET_SERVICE_URL").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let trading_url = std::env::var("TRADING_SERVICE_URL").unwrap_or_else(|_| "http://127.0.0.1:50052".to_string());
    let account_url = std::env::var("ACCOUNT_SERVICE_URL").unwrap_or_else(|_| "http://127.0.0.1:50053".to_string());
    let chart_url = std::env::var("CHART_SERVICE_URL").unwrap_or_else(|_| "http://127.0.0.1:50058".to_string());

    let market_client = MarketServiceClient::connect(market_url)
        .await
        .expect("Failed to connect to Market Service");

    let trading_client = TradingServiceClient::connect(trading_url)
        .await
        .expect("Failed to connect to Trading Service");

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
        trading_client,
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

   
    let brokers = config.kafka.brokers.join(",");
    let redis_for_kafka = redis_client.clone();
    tokio::spawn(async move {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", "gateway-kafka-group")
            .set("auto.offset.reset", "latest")
            .set("enable.auto.commit", "true")
            .create()
            .expect("Failed to create Kafka consumer for gateway");

        consumer.subscribe(&["orderbook-depth", "execution-reports"])
            .expect("Failed to subscribe to Kafka topics in gateway");

        let mut stream = consumer.stream();
        while let Some(msg_res) = stream.next().await {
            if let Ok(msg) = msg_res {
                if let Some(payload) = msg.payload() {
                    let mut redis_conn = match redis_for_kafka.get_multiplexed_async_connection().await {
                        Ok(conn) => conn,
                        Err(_) => continue,
                    };

                    match msg.topic() {
                        "orderbook-depth" => {
                            if let Ok(depth) = serde_json::from_slice::<KafkaDepthUpdate>(payload) {
                                if let Ok(json_str) = serde_json::to_string(&depth) {
                                    let channel = format!("orderbook:{}", depth.symbol);
                                    let _: Result<(), _> = redis_conn.publish(channel, json_str).await;
                                }
                            }
                        }
                        "execution-reports" => {
                            if let Ok(trade) = serde_json::from_slice::<KafkaTradeEvent>(payload) {
                                if let Ok(json_str) = serde_json::to_string(&trade) {
                                    let channel = format!("trades:{}", trade.symbol);
                                    let _: Result<(), _> = redis_conn.publish(channel, json_str.clone()).await;

                                    let private_maker = format!("private:{}", trade.maker_user_id);
                                    let private_taker = format!("private:{}", trade.taker_user_id);
                                    let _: Result<(), _> = redis_conn.publish(private_maker, json_str.clone()).await;
                                    let _: Result<(), _> = redis_conn.publish(private_taker, json_str).await;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    });

    println!("API Gateway started at {}:{}", config.server.host, config.server.port);

    HttpServer::new(move || {
        App::new()
            .wrap(actix_web::middleware::Logger::new("%a \"%r\" %s %b %Dms"))
            .wrap(telemetry::http::HttpMetrics)
            .service(telemetry::http::metrics_handler)
            .app_data(app_state.clone())
            .configure(configure_routes)
    })
    .bind((config.server.host.clone(), config.server.port))?
    .run()
    .await
}
