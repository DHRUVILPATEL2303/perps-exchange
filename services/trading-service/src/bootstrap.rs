use crate::{
    application::services::position_service::PositionService,
    application::services::trading_service::TradingService,
    grpc::server::TradingGrpcService,
    infrastructure::{
        cache::market_cache::MarketCache,
        grpc::{account_client::AccountGrpcClient, market_client::MarketGrpcClient, risk_client::RiskGrpcClient},
        kafka::{trading_consumer::TradeConsumer, producer::OrderProducer},
        repositories::{
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
use tokio::sync::Mutex;
use tonic::transport::Server;

pub async fn bootstrap() -> Result<(
    AppState,
    impl std::future::Future<Output = Result<(), tonic::transport::Error>>,
    TradeConsumer,
)> {
    let config = AppConfig::load("trading-service").expect("Failed to load config");

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

    println!("Connecting to Market Service...");
    let mut market_client = MarketGrpcClient::connect("http://127.0.0.1:50051".into()).await?;

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
    let account_client = Arc::new(Mutex::new(
        AccountGrpcClient::connect("http://127.0.0.1:50053".into()).await?,
    ));

    println!("Connecting to Risk Service...");
    let risk_client = Arc::new(
        RiskGrpcClient::connect("http://127.0.0.1:50057".into()).await?,
    );

    let brokers = config.kafka.brokers.join(",");
    let order_producer = Arc::new(OrderProducer::new(&brokers)?);

    let position_repository = Arc::new(PostgresPositionRepository::new(db.pool().clone()));
    let _trade_repository = Arc::new(PostgresTradeRepository::new(db.pool().clone()));

    let position_service = Arc::new(PositionService::new(position_repository, account_client.clone()));
    let trading_service = Arc::new(TradingService::new(market_cache.clone()));

    let grpc_service = TradingGrpcService {
        position_service: position_service.clone(),
        account_client: account_client.clone(),
        risk_client: risk_client.clone(),
        order_producer: order_producer.clone(),
    };

    let grpc_server = Server::builder()
        .add_service(TradingServiceServer::new(grpc_service))
        .serve_with_shutdown(grpc_addr, shutdown_signal());

    let trade_consumer =
        TradeConsumer::new(&brokers, "trading-service-group", position_service.clone())?;

    let state = AppState {
        config: Arc::new(config),
        db,
        market_cache,
        trading_service,
        position_service,
        risk_client,
        order_producer,
    };

    Ok((state, grpc_server, trade_consumer))
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");
}
