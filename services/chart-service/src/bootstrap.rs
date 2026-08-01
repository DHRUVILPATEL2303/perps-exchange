use std::io::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use config::app::AppConfig;
use database::manager::DatabaseManager;
use sqlx::{Connection, PgConnection};
use tonic::transport::Server;
use proto::chart::chart_service_server::ChartServiceServer;
use crate::grpc::server::ChartGrpcService;
use crate::infrastructure::kafka::consumer::run_trade_consumer;

pub async fn run() -> Result<()> {

    let config = AppConfig::load("chart-service").expect("Failed to load config");

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

    let mut conn = PgConnection::connect(&default_db_url)
        .await
        .expect("Failed to connect to TimescaleDB default database");

    let create_db_query = format!("CREATE DATABASE {}", config.database.database);
    let _ = sqlx::query(&create_db_query).execute(&mut conn).await;
    let db = Arc::new(
        DatabaseManager::new(&config.database)
            .await
            .expect("TimescaleDB connection failed"),
    );

    if config.database.auto_migrate {
        sqlx::migrate!("./migrations")
            .run(db.pool())
            .await
            .expect("TimescaleDB Migration failed");
    }

    println!("Chart Service migrations successfully completed.");

    let db_pool = db.pool().clone();
    let brokers = config.kafka.brokers.join(",");
    
    tokio::spawn(async move {
        println!("Starting Kafka trade consumer for Chart Service...");
        if let Err(e) = run_trade_consumer(&brokers, db_pool).await {
            eprintln!("Kafka trade consumer failed: {:?}", e);
        }
    });

    let grpc_service = ChartGrpcService {
        db_pool: db.pool().clone(),
    };

    println!("gRPC Server started at {}", grpc_addr);
    Server::builder()
        .add_service(ChartServiceServer::new(grpc_service))
        .serve_with_shutdown(grpc_addr, shutdown_signal())
        .await
        .map_err(std::io::Error::other)?;

    println!("Chart Service shutting down.");
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");
}
