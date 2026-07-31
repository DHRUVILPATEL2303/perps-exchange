use std::sync::Arc;
use anyhow::Result;
use futures_util::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer, CommitMode};
use rdkafka::message::Message;
use rust_decimal::Decimal;
use serde::Deserialize;
use uuid::Uuid;
use crate::infrastructure::repositories::postgres_position_repository::PositionRepository;

#[derive(Deserialize, Debug)]
pub struct TradeEvent {
    pub id: Uuid,
    pub symbol: String,
    pub maker_order_id: Uuid,
    pub taker_order_id: Uuid,
    pub maker_user_id: Uuid,
    pub taker_user_id: Uuid,
    pub price: Decimal,
    pub quantity: Decimal,
    pub taker_side: String,
}

pub struct TradeConsumer {
    consumer: StreamConsumer,
    repository: Arc<PositionRepository>,
}

impl TradeConsumer {
    pub fn new(
        brokers: &str,
        group_id: &str,
        repository: Arc<PositionRepository>,
    ) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .create()?;

        consumer.subscribe(&["execution-reports"])?;

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
                    tracing::error!("Kafka trade consumer error: {}", e);
                }
                Ok(msg) => {
                    if let Some(payload) = msg.payload() {
                        match serde_json::from_slice::<TradeEvent>(payload) {
                            Ok(event) => {
                                if let Err(e) = self.process_trade(event).await {
                                    tracing::error!("Failed to mirror position: {:?}", e);
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Failed to deserialize TradeEvent in Risk Engine: {:?}, payload: {:?}",
                                    e,
                                    String::from_utf8_lossy(payload)
                                );
                            }
                        }
                        let _ = self.consumer.commit_message(&msg, CommitMode::Async);
                    }
                }
            }
        }
    }

    async fn process_trade(&self, event: TradeEvent) -> Result<()> {
        if event.taker_side == "CANCEL" {
            return Ok(());
        }

        let maker_side = if event.taker_side == "BUY" { "SHORT" } else { "LONG" };
        let taker_side = if event.taker_side == "BUY" { "LONG" } else { "SHORT" };

        let leverage = 20;
        let mmr = Decimal::new(5, 3);

        let taker_margin = (event.quantity * event.price) / Decimal::from(leverage);
        let taker_liq = if taker_side == "LONG" {
            event.price - (taker_margin / event.quantity) / (Decimal::ONE - mmr)
        } else {
            event.price + (taker_margin / event.quantity) / (Decimal::ONE + mmr)
        };

        self.repository.update_position(
            event.taker_user_id,
            &event.symbol,
            taker_side,
            event.quantity,
            event.price,
            taker_margin,
            leverage,
            taker_liq,
        ).await?;

        let maker_margin = (event.quantity * event.price) / Decimal::from(leverage);
        let maker_liq = if maker_side == "LONG" {
            event.price - (maker_margin / event.quantity) / (Decimal::ONE - mmr)
        } else {
            event.price + (maker_margin / event.quantity) / (Decimal::ONE + mmr)
        };

        self.repository.update_position(
            event.maker_user_id,
            &event.symbol,
            maker_side,
            event.quantity,
            event.price,
            maker_margin,
            leverage,
            maker_liq,
        ).await?;

        Ok(())
    }
}
