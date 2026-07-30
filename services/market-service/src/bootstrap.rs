use std::{io::Result, net::SocketAddr, sync::Arc};

use actix_web::{HttpServer, web::Data};
use config::app::AppConfig;
use database::manager::DatabaseManager;
use proto::market::market_service_server::MarketServiceServer;
use tonic::transport::Server;


use crate::{
    application::services::market_service::MarketService, grpc::server::MarketGrpcService, infrastructure::repositories::postgres_market_repository::PostgresMarketRepository, presentation, state::AppState,
};

pub async fn run() -> Result<()> {
    let config = AppConfig::load("market-service").expect("Failed to Load Config");

    let grpc_addr: SocketAddr = format!(
        "{}:{}",
        config.grpc.host,
        config.grpc.port
    )
    .parse()
    .expect("Invalid gRPC address");

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
    let repository = Arc::new(PostgresMarketRepository::new(db.pool().clone()));

    let market_service = Arc::new(MarketService::new(repository));

    let grpc_service = MarketGrpcService {
        service: market_service.clone(),
    };

    let grpc_server = Server::builder()
        .add_service(MarketServiceServer::new(grpc_service))
        .serve_with_shutdown(grpc_addr, shutdown_signal());


    
    let state = Data::new(AppState {
        config: Arc::new(config.clone()),
        db,
        market_service,
    });

   
    let http_server = HttpServer::new(move || {
        actix_web::App::new()
            .app_data(state.clone())
            .configure(presentation::rest::routes::configure)
    })
    .bind((config.server.host.clone(), config.server.port))?
    .run();
    
    println!("HTTP Server started at {}:{}", config.server.host, config.server.port);
    println!("gRPC Server started at {}", grpc_addr);
    
    tokio::try_join!(
        http_server,
        async {
            grpc_server
                .await
                .map_err(std::io::Error::other)
        }
    ).expect("Server Failed");

    
    Ok(())



}


async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");
}