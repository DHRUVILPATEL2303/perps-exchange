use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use config::app::AppConfig;
use crate::application::services::matching_service::OrderBook;
use crate::infrastructure::kafka::consumer::OrderConsumer;
use crate::infrastructure::kafka::producer::TradeProducer;

pub async fn bootstrap() -> Result<()> {
    let config = AppConfig::load("matching-engine").expect("Failed to load config");

    let brokers = config.kafka.brokers.join(",");

    let order_books: Arc<Mutex<HashMap<String, OrderBook>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let producer = Arc::new(TradeProducer::new(&brokers)?);

    let consumer = OrderConsumer::new(
        &brokers,
        "matching-engine-group",
        order_books,
        producer,
    )?;

    tracing::info!("Matching Engine started. Consuming from order-events topic...");

    consumer.run().await;

    Ok(())
}
