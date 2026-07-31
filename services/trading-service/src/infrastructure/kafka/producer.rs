use anyhow::Result;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use serde::Serialize;

#[derive(Serialize)]
pub struct KafkaOrderEvent {
    pub id: String,
    pub user_id: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub price: String,
    pub quantity: String,
}

pub struct OrderProducer {
    producer: FutureProducer,
}

impl OrderProducer {
    pub fn new(brokers: &str) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()?;

        Ok(Self { producer })
    }

    pub async fn publish_order(&self, event: &KafkaOrderEvent) -> Result<()> {
        let payload = serde_json::to_string(event)?;
        let key = event.id.clone();

        self.producer
            .send(
                FutureRecord::to("order-events")
                    .payload(payload.as_bytes())
                    .key(key.as_bytes()),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Kafka send error: {}", e))?;

        Ok(())
    }
}
