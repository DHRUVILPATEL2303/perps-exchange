use anyhow::Result;

pub mod application;
pub mod bootstrap;
pub mod domain;
pub mod grpc;
pub mod infrastructure;
pub mod state;

#[tokio::main]
async fn main() -> Result<()> {
    telemetry::logging::init();
    
    let (state, grpc_server, trade_consumer, liquidation_consumer) = bootstrap::bootstrap().await?;

    println!(
        "Trading Service Ready. Markets: {}",
        state.market_cache.len().await
    );

    tokio::spawn(async move {
        println!("Starting Kafka trade consumer...");
        trade_consumer.run().await;
    });

    tokio::spawn(async move {
        println!("Starting Kafka liquidation consumer...");
        liquidation_consumer.run().await;
    });

    println!("Starting gRPC Server...");
    grpc_server.await?;

    Ok(())
}
