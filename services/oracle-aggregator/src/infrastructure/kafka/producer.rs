use async_trait::async_trait;
use errors::app_error::RepositoryError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::time::Duration;
use crate::domain::entities::price_tick::PriceTick;
use crate::domain::repositories::price_publisher::PricePublisher;

pub struct KafkaPricePublisher {
    producer: FutureProducer,
    topic: String,
}

impl KafkaPricePublisher {
    pub fn new(brokers: &str, topic: String) -> Self {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .unwrap();
        Self { producer, topic }
    }
}

#[async_trait]
impl PricePublisher for KafkaPricePublisher {
    async fn publish(&self, tick: &PriceTick) -> Result<(), RepositoryError> {
        let serialized = serde_json::to_string(tick)
            .map_err(|e| RepositoryError::Database(sqlx::Error::Protocol(e.to_string())))?;

        let record = FutureRecord::to(&self.topic)
            .key(&tick.symbol)
            .payload(&serialized);

        self.producer
            .send(record, Duration::from_secs(1))
            .await
            .map_err(|(e, _)| RepositoryError::Database(sqlx::Error::Protocol(e.to_string())))?;

        Ok(())
    }
}
