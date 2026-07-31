use std::net::SocketAddr;
use std::sync::Arc;
use anyhow::Result;
use config::app::AppConfig;
use database::manager::DatabaseManager;
use proto::risk::risk_service_server::RiskServiceServer;
use tonic::transport::Server;
use crate::{
    grpc::server::RiskGrpcService,
    infrastructure::{
        grpc::{account_client::AccountGrpcClient, trading_client::TradingGrpcClient},
        kafka::{consumer::RiskConsumer, producer::LiquidationProducer},
    },
    state::AppState,
};

pub async fn bootstrap() -> Result<(AppState, impl std::future::Future<Output = Result<(), tonic::transport::Error>>, RiskConsumer)> {
    let config = AppConfig::load("risk-engine-service").expect("Failed to load config");

    let grpc_addr: SocketAddr = format!("{}:{}", config.grpc.host, config.grpc.port)
        .parse()
        .expect("Invalid gRPC address");

    let db = Arc::new(DatabaseManager::new(&config.database).await?);

    let account_client = AccountGrpcClient::connect("http://127.0.0.1:50053".to_string()).await?;
    let trading_client = TradingGrpcClient::connect("http://127.0.0.1:50052".to_string()).await?;

    let grpc_service = RiskGrpcService {
        account_client: account_client.clone(),
    };

    let grpc_server = Server::builder()
        .add_service(RiskServiceServer::new(grpc_service))
        .serve_with_shutdown(grpc_addr, shutdown_signal());

    let brokers = config.kafka.brokers.join(",");
    let producer = Arc::new(LiquidationProducer::new(&brokers)?);
    let risk_consumer = RiskConsumer::new(
        &brokers,
        "risk-engine-group",
        db.pool().clone(),
        producer,
    )?;

    let state = AppState {
        config: Arc::new(config),
        db,
        account_client,
        trading_client,
    };

    Ok((state, grpc_server, risk_consumer))
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");
}
