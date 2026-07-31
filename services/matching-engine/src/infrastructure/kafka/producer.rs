use anyhow::Result;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use crate::domain::entities::trade::Trade;

pub struct TradeProducer {
    producer: FutureProducer,
}

impl TradeProducer {
    pub fn new(brokers: &str) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()?;

        Ok(Self { producer })
    }

    pub async fn publish_trade(&self, trade: &Trade) -> Result<()> {
        let payload = serde_json::to_string(trade)?;
        let key = trade.symbol.clone();

        self.producer
            .send(
                FutureRecord::to("execution-reports")
                    .payload(payload.as_bytes())
                    .key(key.as_bytes()),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Kafka send error: {}", e))?;

        Ok(())
    }
}
