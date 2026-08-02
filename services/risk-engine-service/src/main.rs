use anyhow::Result;

use crate::funding_rate::funding_loop;

pub mod bootstrap;
pub mod grpc;
pub mod infrastructure;
pub mod state;
pub mod price_tracker;
pub mod funding_rate;

#[tokio::main]
async fn main() -> Result<()> {
    telemetry::logging::init();
    
    let price_tracker = price_tracker::price_tracker::PriceTracker::new();
    let (state, grpc_server, risk_consumer, trade_consumer, liquidation_consumer) = 
        bootstrap::bootstrap(price_tracker.clone()).await?;
    telemetry::http::spawn_metrics_server(state.config.server.port);

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

    let state_clone = state.clone();
    let tracker_clone = price_tracker.clone();
    tokio::spawn(async move {
        println!("Starting periodic Funding Loop...");
        funding_loop::run_funding_loop(state_clone, tracker_clone).await;
    });

    println!("Starting gRPC Server...");
    grpc_server.await?;

    Ok(())
}
