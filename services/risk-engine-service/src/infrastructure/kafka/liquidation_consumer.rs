use std::sync::Arc;
use std::str::FromStr;
use anyhow::Result;
use futures_util::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer, CommitMode};
use rdkafka::message::Message;
use rust_decimal::Decimal;
use serde::Deserialize;
use uuid::Uuid;
use crate::infrastructure::repositories::postgres_position_repository::PositionRepository;

#[derive(Deserialize)]
pub struct LiquidationEvent {
    pub user_id: String,
    pub symbol: String,
    pub side: String,
}

pub struct LiquidationConsumer {
    consumer: StreamConsumer,
    repository: Arc<PositionRepository>,
}

impl LiquidationConsumer {
    pub fn new(
        brokers: &str,
        group_id: &str,
        repository: Arc<PositionRepository>,
    ) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "latest")
            .set("enable.auto.commit", "true")
            .create()?;

        consumer.subscribe(&["liquidations"])?;

        Ok(Self {
            consumer,
            repository,
        })
    }

    pub async fn run(self) {
        let mut stream = self.consumer.stream();

        while let Some(msg_result) = stream.next().await {
            match msg_result {
                Err(e) => {
                    tracing::error!("Risk engine liquidation consumer error: {}", e);
                }
                Ok(msg) => {
                    if let Some(payload) = msg.payload() {
                        if let Ok(event) = serde_json::from_slice::<LiquidationEvent>(payload) {
                            if let Err(e) = self.close_local_position(event).await {
                                tracing::error!("Failed to close risk-engine local position: {:?}", e);
                            }
                            let _ = self.consumer.commit_message(&msg, CommitMode::Async);
                        }
                    }
                }
            }
        }
    }

    async fn close_local_position(&self, event: LiquidationEvent) -> Result<()> {
        let user_id = Uuid::parse_str(&event.user_id)?;
        self.repository.update_position(
            user_id,
            &event.symbol,
            &event.side,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            1,
            Decimal::ZERO,
        ).await?;

        tracing::info!(
            user_id = %user_id,
            symbol = %event.symbol,
            side = %event.side,
            "Risk Engine local mirrored position set to zero size"
        );

        Ok(())
    }
}
