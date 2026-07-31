use std::{ptr::read_unaligned, time::Duration};

use anyhow::{Ok, Result};
use rdkafka::{ClientConfig, producer::{FutureProducer, FutureRecord}};
use rust_decimal::Decimal;
use serde::Serialize;

use uuid::Uuid;

#[derive(Serialize)]
pub struct LiquidationEvent {
    pub position_id: Uuid,
    pub user_id: Uuid,
    pub symbol: String,
    pub side: String,
    pub size: Decimal,
    pub entry_price: Decimal,
    pub mark_price: Decimal,
    pub margin: Decimal,
    pub timestamp: u64,
}

pub struct LiquidationProducer {
    pub producer : FutureProducer,
}

impl LiquidationProducer {
    pub fn new(brokers: &str) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
             .set("bootstrap.servers", brokers)
             .set("message.timeout.ms", "5000")
             .create()?;
         Ok(Self { producer })
    }
    pub async fn publish_liquidation(&self, event : &LiquidationEvent) -> Result<()> {
        let payload = serde_json::to_string(event)?;
        let key = event.position_id.to_string();

        self.producer.send(
            FutureRecord::to("liquidations")
                .payload(payload.as_bytes())
                .key(key.as_bytes()),
            Duration::from_secs(5),
        ).await
        .map_err(|(e, _)| anyhow::anyhow!("Kafka send error: {}", e))?;


        Ok(())
    }
}