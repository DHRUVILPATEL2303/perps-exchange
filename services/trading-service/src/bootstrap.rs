use crate::{
    application::services::position_service::PositionService,
    application::services::trading_service::TradingService,
    grpc::server::TradingGrpcService,
    infrastructure::{
        cache::market_cache::MarketCache,
        grpc::{
            account_client::AccountGrpcClient, market_client::MarketGrpcClient,
            risk_client::RiskGrpcClient,
        },
        kafka::{
            liquidation_consumer::LiquidationConsumer, producer::OrderProducer,
            trading_consumer::TradeConsumer,
        },
        repositories::{
            postgres_order_repository::PostgresOrderRepository,
            postgres_position_repository::PostgresPositionRepository,
            postgres_trade_repository::PostgresTradeRepository,
        },
    },
    state::AppState,
};
use anyhow::Result;
use config::app::AppConfig;
use database::manager::DatabaseManager;
use proto::trading::trading_service_server::TradingServiceServer;
use sqlx::{Connection, PgConnection};
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

pub async fn bootstrap() -> Result<(
    AppState,
    impl std::future::Future<Output = Result<(), tonic::transport::Error>>,
    TradeConsumer,
    LiquidationConsumer,
)> {
    let config = AppConfig::load("trading-service").expect("Failed to load config");

    telemetry::http::spawn_metrics_server(config.server.port);

    let grpc_addr: SocketAddr = format!("{}:{}", config.grpc.host, config.grpc.port)
        .parse()
        .expect("Invalid gRPC address");

    let default_db_url = format!(
        "postgres://{}:{}@{}:{}/postgres",
        config.database.username,
        config.database.password,
        config.database.host,
        config.database.port
    );

    let mut conn = PgConnection::connect(&default_db_url).await?;
    let create_db_query = format!("CREATE DATABASE {}", config.database.database);
    let _ = sqlx::query(&create_db_query).execute(&mut conn).await;

    let db = Arc::new(DatabaseManager::new(&config.database).await?);

    if config.database.auto_migrate {
        sqlx::migrate!("./migrations").run(db.pool()).await?;
    }

    let market_url = std::env::var("MARKET_SERVICE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let account_url = std::env::var("ACCOUNT_SERVICE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:50053".to_string());
    let risk_url =
        std::env::var("RISK_SERVICE_URL").unwrap_or_else(|_| "http://127.0.0.1:50057".to_string());

    println!("Connecting to Market Service...");
    let mut market_client = MarketGrpcClient::connect(market_url).await?;

    println!("Loading markets...");
    let markets = market_client.list_markets().await?;
    println!("Loaded {} markets", markets.len());

    let market_cache = Arc::new(MarketCache::new());
    market_cache.load(markets).await;
    println!(
        "Market cache initialized with {} markets",
        market_cache.len().await
    );

    println!("Connecting to Account Service...");
    let account_client = AccountGrpcClient::connect(account_url).await?;

    println!("Connecting to Risk Service...");
    let risk_client = RiskGrpcClient::connect(risk_url).await?;

    let brokers = config.kafka.brokers.join(",");
    let order_producer = Arc::new(OrderProducer::new(&brokers)?);

    let position_repository = Arc::new(PostgresPositionRepository::new(db.pool().clone()));
    let _trade_repository = Arc::new(PostgresTradeRepository::new(db.pool().clone()));
    let order_repository = Arc::new(PostgresOrderRepository::new(db.pool().clone()));

    let position_service = Arc::new(PositionService::new(
        position_repository.clone(),
        account_client.clone(),
        order_repository.clone(),
    ));
    let trading_service = Arc::new(TradingService::new(market_cache.clone()));

    let price_tracker = crate::domain::price_tracker::PriceTracker::new();
    let redis_url = format!("redis://{}:{}", config.redis.host, config.redis.port);

    crate::infrastructure::kafka::trigger_loop::start_trigger_loop(
        db.pool().clone(),
        redis_url,
        price_tracker.clone(),
        account_client.clone(),
        risk_client.clone(),
        order_producer.clone(),
    );

    let grpc_service = TradingGrpcService {
        position_service: position_service.clone(),
        account_client: account_client.clone(),
        risk_client: risk_client.clone(),
        order_producer: order_producer.clone(),
        order_repository: order_repository.clone(),
        market_cache: market_cache.clone(),
        price_tracker: price_tracker.clone(),
    };

    let grpc_server = Server::builder()
        .layer(telemetry::grpc::GrpcLoggingLayer)
        .add_service(TradingServiceServer::new(grpc_service))
        .serve_with_shutdown(grpc_addr, shutdown_signal());

    let trade_consumer = TradeConsumer::new(
        &brokers,
        "trading-service-group-v2",
        position_service.clone(),
        order_repository.clone(),
    )?;

    let liquidation_consumer = LiquidationConsumer::new(
        &brokers,
        "trading-service-liq-group-v2",
        position_repository,
        account_client,
        order_producer.clone(),
    )?;

    let state = AppState {
        config: Arc::new(config),
        db,
        market_cache,
        trading_service,
        position_service,
        risk_client,
        order_producer,
        order_repository,
    };

    Ok((state, grpc_server, trade_consumer, liquidation_consumer))
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");
}
