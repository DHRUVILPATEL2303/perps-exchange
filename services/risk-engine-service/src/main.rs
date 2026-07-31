use anyhow::Result;

pub mod bootstrap;
pub mod grpc;
pub mod infrastructure;
pub mod state;

#[tokio::main]
async fn main() -> Result<()> {
    telemetry::logging::init();
    
    let (state, grpc_server, risk_consumer, trade_consumer, liquidation_consumer) = bootstrap::bootstrap().await?;

    println!("Risk Engine Service Ready.");

    tokio::spawn(async move {
        println!("Starting Kafka mark-to-market consumer...");
        risk_consumer.run().await;
    });

    tokio::spawn(async move {
        println!("Starting Kafka position-mirror trade consumer...");
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
