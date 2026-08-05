use anyhow::Result;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use crate::domain::entities::trade::Trade;
use rust_decimal::Decimal;

#[derive(serde::Serialize)]
pub struct DepthUpdate {
    pub symbol: String,
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
    pub timestamp: i64,
}

pub struct TradeProducer {
    producer: FutureProducer,
}

impl TradeProducer {
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

    pub async fn publish_depth(&self, symbol: &str, bids: Vec<(Decimal, Decimal)>, asks: Vec<(Decimal, Decimal)>) -> Result<()> {
        let update = DepthUpdate {
            symbol: symbol.to_string(),
            bids,
            asks,
            timestamp: chrono::Utc::now().timestamp_millis(),
        };
        let payload = serde_json::to_string(&update)?;

        self.producer
            .send(
                FutureRecord::to("orderbook-depth")
                    .payload(payload.as_bytes())
                    .key(symbol.as_bytes()),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| anyhow::anyhow!("Kafka send error: {}", e))?;

        Ok(())
    }

    pub fn publish_trade_sync(&self, trade: &Trade) -> Result<()> {
        let payload = serde_json::to_string(trade)?;
        let key = trade.symbol.clone();

        match self.producer.send_result(
            FutureRecord::to("execution-reports")
                .payload(payload.as_bytes())
                .key(key.as_bytes())
        ) {
            Ok(_) => Ok(()),
            Err((e, _)) => Err(anyhow::anyhow!("Kafka sync send error: {}", e)),
        }
    }

    pub fn publish_depth_sync(&self, symbol: &str, bids: Vec<(Decimal, Decimal)>, asks: Vec<(Decimal, Decimal)>) -> Result<()> {
        let update = DepthUpdate {
            symbol: symbol.to_string(),
            bids,
            asks,
            timestamp: chrono::Utc::now().timestamp_millis(),
        };
        let payload = serde_json::to_string(&update)?;

        match self.producer.send_result(
            FutureRecord::to("orderbook-depth")
                .payload(payload.as_bytes())
                .key(symbol.as_bytes())
        ) {
            Ok(_) => Ok(()),
            Err((e, _)) => Err(anyhow::anyhow!("Kafka sync send error: {}", e)),
        }
    }
}
