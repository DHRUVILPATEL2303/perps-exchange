use crate::{
    application::services::account_service::AccountService, grpc::server::AccountGrpcService,
    infrastructure::repositories::postgres_account_repository::PostgresAccountRepository,
    presentation, state::AppState,
};
use actix_web::{HttpServer, web::Data};
use config::app::AppConfig;
use database::manager::DatabaseManager;
use proto::account::account_service_server::AccountServiceServer;
use sqlx::{Connection, PgConnection};
use std::io::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

pub async fn run() -> Result<()> {
    let config = AppConfig::load("account-service").expect("Failed to load config");

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
        .expect("Failed to connect to default postgres database");

    let create_db_query = format!("CREATE DATABASE {}", config.database.database);
    let _ = sqlx::query(&create_db_query).execute(&mut conn).await;

    let db = Arc::new(
        DatabaseManager::new(&config.database)
            .await
            .expect("Database connection failed"),
    );

    if config.database.auto_migrate {
        sqlx::migrate!("./migrations")
            .run(db.pool())
            .await
            .expect("Migration failed");
    }

    let repository = Arc::new(PostgresAccountRepository::new(db.pool().clone()));
    let account_service = Arc::new(AccountService::new(repository));

    let grpc_service = AccountGrpcService {
        service: account_service.clone(),
    };

    let grpc_server = Server::builder()
        .layer(telemetry::grpc::GrpcLoggingLayer)
        .add_service(AccountServiceServer::new(grpc_service))
        .serve_with_shutdown(grpc_addr, shutdown_signal());

    let state = Data::new(AppState {
        config: Arc::new(config.clone()),
        db,
        account_service: account_service.clone(),
    });

    let http_server = HttpServer::new(move || {
        actix_web::App::new()
            .wrap(telemetry::http::HttpMetrics)
            .service(telemetry::http::metrics_handler)
            .app_data(state.clone())
            .configure(presentation::rest::routes::configure)
    })
    .bind((config.server.host.clone(), config.server.port))?
    .run();

    println!(
        "HTTP Server started at {}:{}",
        config.server.host, config.server.port
    );
    println!("gRPC Server started at {}", grpc_addr);

    let brokers = config.kafka.brokers.join(",");
    let deposit_consumer = crate::infrastructure::kafka::deposit_consumer::DepositConsumer::new(
        &brokers,
        "account-service-deposits-group-v1",
        account_service.clone(),
    )
    .expect("Failed to initialize DepositConsumer");

    tokio::spawn(async move {
        deposit_consumer.run().await;
    });

    tokio::try_join!(http_server, async {
        grpc_server.await.map_err(std::io::Error::other)
    })
    .expect("Server failed");

    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");
}
