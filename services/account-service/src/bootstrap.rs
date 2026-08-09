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
use solana_client::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;

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

    let solana_rpc_url = std::env::var("SOLANA_RPC_URL")
        .unwrap_or_else(|_| "https://devnet.helius-rpc.com/?api-key=b07f07b6-4c5a-417d-9c31-93300c828917".to_string());
    let rpc_client = Arc::new(RpcClient::new(solana_rpc_url));

    let keypair_path = std::env::var("CUSTODY_ADMIN_KEYPAIR_PATH")
        .unwrap_or_else(|_| "/app/configs/custody-admin-keypair.json".to_string());
    let admin_keypair = Arc::new(load_keypair(&keypair_path).expect("Failed to load admin keypair"));

    let derived_usdc = get_associated_token_address(&admin_keypair.pubkey(), &solana_sdk::pubkey::Pubkey::from_str_const("4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU"));
    let derived_usdt = get_associated_token_address(&admin_keypair.pubkey(), &solana_sdk::pubkey::Pubkey::from_str_const("EJwZeg1u717JhEv6YoRrt8A6gGTLrmKWJxgB7P15fTo3"));
    tracing::info!("!!! ADMIN PUBKEY: {}", admin_keypair.pubkey());
    tracing::info!("!!! DERIVED USDC TREASURY ATA: {}", derived_usdc);
    tracing::info!("!!! DERIVED USDT TREASURY ATA: {}", derived_usdt);

    let repository = Arc::new(PostgresAccountRepository::new(db.pool().clone()));
    let account_service = Arc::new(AccountService::new(repository, rpc_client, admin_keypair));

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

fn load_keypair(path: &str) -> std::io::Result<Keypair> {
    let content = std::fs::read_to_string(path)?;
    let bytes: Vec<u8> = serde_json::from_str(&content)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let keypair = Keypair::from_bytes(&bytes)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(keypair)
}

fn get_associated_token_address(wallet_address: &solana_sdk::pubkey::Pubkey, token_mint_address: &solana_sdk::pubkey::Pubkey) -> solana_sdk::pubkey::Pubkey {
    let spl_associated_token_program_id = solana_sdk::pubkey::Pubkey::from_str_const("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");
    let spl_token_program_id = solana_sdk::pubkey::Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
    
    let (ata, _) = solana_sdk::pubkey::Pubkey::find_program_address(
        &[
            wallet_address.as_ref(),
            spl_token_program_id.as_ref(),
            token_mint_address.as_ref(),
        ],
        &spl_associated_token_program_id,
    );
    ata
}
