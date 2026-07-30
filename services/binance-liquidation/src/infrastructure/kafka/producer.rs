use async_trait::async_trait;
use errors::app_error::RepositoryError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use std::time::Duration;
use crate::domain::entities::liquidation::Liquidation;
use crate::domain::repositories::liquidation_publisher::LiquidationPublisher;

pub struct KafkaLiquidationPublisher {
    producer: FutureProducer,
    topic: String,
}

impl KafkaLiquidationPublisher {
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
impl LiquidationPublisher for KafkaLiquidationPublisher {
    async fn publish(&self, liquidation: &Liquidation) -> Result<(), RepositoryError> {
        let serialized = serde_json::to_string(liquidation)
            .map_err(|e| RepositoryError::Database(sqlx::Error::Protocol(e.to_string())))?;

        let record = FutureRecord::to(&self.topic)
            .key(&liquidation.symbol)
            .payload(&serialized);

        self.producer
            .send(record, Duration::from_secs(1))
            .await
            .map_err(|(e, _)| RepositoryError::Database(sqlx::Error::Protocol(e.to_string())))?;

        Ok(())
    }
}
