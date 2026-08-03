use crate::infrastructure::kafka::consumer::OrderConsumer;
use crate::infrastructure::kafka::producer::TradeProducer;
use anyhow::Result;
use config::app::AppConfig;
use std::sync::Arc;

pub async fn bootstrap() -> Result<()> {
    let config = AppConfig::load("matching-engine").expect("Failed to load config");
    telemetry::http::spawn_metrics_server(config.server.port);


    let brokers = config.kafka.brokers.join(",");

    let producer = Arc::new(TradeProducer::new(&brokers)?);

    let consumer = OrderConsumer::new(&brokers, "matching-engine-group", producer)?;

    tracing::info!("Matching Engine started. Consuming from order-events topic...");

    consumer.run().await;

    Ok(())
}
