use anyhow::Result;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use telemetry::metrics::KAFKA_MESSAGES_PRODUCED_TOTAL;
use serde::Serialize;

use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Serialize)]
pub struct KafkaOrderEvent {
    pub id: Uuid,
    pub user_id: Uuid,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub price: String,
    pub quantity: String,
    pub action: String,
    pub timestamp: u64,
    pub leverage: u32,
}

pub struct OrderProducer {
    producer: FutureProducer,
}

impl OrderProducer {
    pub fn new(brokers: &str) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("queue.buffering.max.messages", "1000000")
            .set("batch.num.messages", "10000")
            .set("linger.ms", "5")
            .create()?;

        Ok(Self { producer })
    }

    pub async fn publish_order(&self, event: &KafkaOrderEvent) -> Result<()> {
        let payload = bincode::serialize(event)?;
        let key = event.id.to_string();

        self.producer
            .send(
                FutureRecord::to("order-events")
                    .payload(payload.as_slice())
                    .key(key.as_bytes()),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Kafka send error: {}", e))?;
        
        KAFKA_MESSAGES_PRODUCED_TOTAL.with_label_values(&["order-events"]).inc();

        Ok(())
    }
}
