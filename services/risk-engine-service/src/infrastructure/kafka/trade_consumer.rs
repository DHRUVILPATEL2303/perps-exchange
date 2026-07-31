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
pub struct TradeEvent {
    pub symbol: String,
    pub maker_order_id: String,
    pub taker_order_id: String,
    pub maker_user_id: String,
    pub taker_user_id: String,
    pub price: String,
    pub quantity: String,
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
            .set("auto.offset.reset", "latest")
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
                        if let Ok(event) = serde_json::from_slice::<TradeEvent>(payload) {
                            if let Err(e) = self.process_trade(event).await {
                                tracing::error!("Failed to mirror position: {:?}", e);
                            }
                            let _ = self.consumer.commit_message(&msg, CommitMode::Async);
                        }
                    }
                }
            }
        }
    }

    async fn process_trade(&self, event: TradeEvent) -> Result<()> {
        let price = Decimal::from_str(&event.price)?;
        let qty = Decimal::from_str(&event.quantity)?;
        let maker_user = Uuid::parse_str(&event.maker_user_id)?;
        let taker_user = Uuid::parse_str(&event.taker_user_id)?;

        let maker_side = if event.taker_side == "BUY" { "SHORT" } else { "LONG" };
        let taker_side = if event.taker_side == "BUY" { "LONG" } else { "SHORT" };

        let leverage = 20;
        let mmr = Decimal::new(5, 3);

        let taker_margin = (qty * price) / Decimal::from(leverage);
        let taker_liq = if taker_side == "LONG" {
            price - (taker_margin / qty) / (Decimal::ONE - mmr)
        } else {
            price + (taker_margin / qty) / (Decimal::ONE + mmr)
        };

        self.repository.update_position(
            taker_user,
            &event.symbol,
            taker_side,
            qty,
            price,
            taker_margin,
            leverage,
            taker_liq,
        ).await?;

        let maker_margin = (qty * price) / Decimal::from(leverage);
        let maker_liq = if maker_side == "LONG" {
            price - (maker_margin / qty) / (Decimal::ONE - mmr)
        } else {
            price + (maker_margin / qty) / (Decimal::ONE + mmr)
        };

        self.repository.update_position(
            maker_user,
            &event.symbol,
            maker_side,
            qty,
            price,
            maker_margin,
            leverage,
            maker_liq,
        ).await?;

        Ok(())
    }
}
