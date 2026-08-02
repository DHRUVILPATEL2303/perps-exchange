use std::net::SocketAddr;
use std::sync::Arc;
use anyhow::Result;
use config::app::AppConfig;
use database::manager::DatabaseManager;
use proto::risk::risk_service_server::RiskServiceServer;
use sqlx::{Connection, PgConnection};
use tonic::transport::Server;
use crate::{
    grpc::server::RiskGrpcService, infrastructure::{
        grpc::{account_client::AccountGrpcClient, trading_client::TradingGrpcClient}, kafka::{
            consumer::RiskConsumer, liquidation_consumer::LiquidationConsumer, producer::LiquidationProducer, trade_consumer::TradeConsumer,
        }, repositories::postgres_position_repository::PositionRepository,
    }, price_tracker::price_tracker::PriceTracker, state::AppState,
};
pub async fn bootstrap(price_tracker: PriceTracker) -> Result<(
    AppState, 
    impl std::future::Future<Output = Result<(), tonic::transport::Error>>, 
    RiskConsumer, 
    TradeConsumer,
    LiquidationConsumer,
)> {
    let config = AppConfig::load("risk-engine-service").expect("Failed to load config");

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
        sqlx::migrate!("./migrations")
            .run(db.pool())
            .await?;
    }

    let account_url = std::env::var("ACCOUNT_SERVICE_URL").unwrap_or_else(|_| "http://127.0.0.1:50053".to_string());
    let trading_url = std::env::var("TRADING_SERVICE_URL").unwrap_or_else(|_| "http://127.0.0.1:50052".to_string());
    let account_client = AccountGrpcClient::connect(account_url).await?;
    let trading_client = TradingGrpcClient::connect(trading_url).await?;

    let grpc_service = RiskGrpcService {
        account_client: account_client.clone(),
        db_pool: db.pool().clone(),
    };

    let grpc_server = Server::builder()
        .layer(telemetry::grpc::GrpcLoggingLayer)
        .add_service(RiskServiceServer::new(grpc_service))
        .serve_with_shutdown(grpc_addr, shutdown_signal());

    let brokers = config.kafka.brokers.join(",");
    let producer = Arc::new(LiquidationProducer::new(&brokers)?);
    let risk_consumer = RiskConsumer::new(
        &brokers,
        "risk-engine-group",
        db.pool().clone(),
        producer,
        price_tracker.clone(),
    )?;

    let position_repository = Arc::new(PositionRepository::new(db.pool().clone()));
    let trade_consumer = TradeConsumer::new(
        &brokers,
        "risk-engine-trade-group-v2",
        position_repository.clone(),
        price_tracker.clone(),
    )?;

    let liquidation_consumer = LiquidationConsumer::new(
        &brokers,
        "risk-engine-liq-group-v2",
        position_repository,
    )?;

    let state = AppState {
        config: Arc::new(config),
        db,
        account_client,
        trading_client,
    };

    Ok((state, grpc_server, risk_consumer, trade_consumer, liquidation_consumer))
}


async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");
}
