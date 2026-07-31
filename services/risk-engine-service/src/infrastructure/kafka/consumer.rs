use std::sync::Arc;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use anyhow::Result;
use futures_util::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer, CommitMode};
use rdkafka::message::Message;
use rust_decimal::Decimal;
use serde::Deserialize;
use sqlx::{Pool, Postgres, Row};
use uuid::Uuid;
use crate::infrastructure::kafka::producer::{LiquidationEvent, LiquidationProducer};

#[derive(Deserialize)]
pub struct PriceFeedTick {
    pub symbol: String,
    pub index_price: Decimal,
    pub mark_price: Decimal,
    pub timestamp: u64,
}

pub struct RiskConsumer {
    consumer: StreamConsumer,
    db_pool: Pool<Postgres>,
    producer: Arc<LiquidationProducer>,
}

impl RiskConsumer {
    pub fn new(
        brokers: &str,
        group_id: &str,
        db_pool: Pool<Postgres>,
        producer: Arc<LiquidationProducer>,
    ) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "latest")
            .set("enable.auto.commit", "true")
            .create()?;

        consumer.subscribe(&["price-feed"])?;

        Ok(Self {
            consumer,
            db_pool,
            producer,
        })
    }

    pub async fn run(self) {
        let mut stream = self.consumer.stream();

        while let Some(msg_result) = stream.next().await {
            match msg_result {
                Err(e) => {
                    tracing::error!("Kafka price consumption error: {}", e);
                }
                Ok(msg) => {
                    if let Some(payload) = msg.payload() {
                        if let Ok(tick) = serde_json::from_slice::<PriceFeedTick>(payload) {
                            if let Err(e) = self.check_positions(tick).await {
                                tracing::error!("Failed to check positions: {:?}", e);
                            }
                            let _ = self.consumer.commit_message(&msg, CommitMode::Async);
                        }
                    }
                }
            }
        }
    }

    async fn check_positions(&self, tick: PriceFeedTick) -> Result<()> {
        let rows = sqlx::query(
            r#"
            SELECT id, user_id, symbol, side, size, entry_price, margin, leverage, liquidation_price, margin_mode
            FROM positions
            WHERE symbol = $1 AND size > 0
            "#,
        )
        .bind(&tick.symbol)
        .fetch_all(&self.db_pool)
        .await?;

        let mmr = Decimal::new(5, 3); // MMR = 0.005 (0.5%)

        for row in rows {
            let id: Uuid = row.get("id");
            let user_id: Uuid = row.get("user_id");
            let symbol: String = row.get("symbol");
            let side: String = row.get("side");
            let size: Decimal = row.get("size");
            let entry_price: Decimal = row.get("entry_price");
            let margin: Decimal = row.get("margin");

            let unrealized_pnl = if side == "LONG" {
                size * (tick.mark_price - entry_price)
            } else {
                size * (entry_price - tick.mark_price)
            };

            let margin_balance = margin + unrealized_pnl;
            let maintenance_margin = size * tick.mark_price * mmr;

            if margin_balance < maintenance_margin {
                tracing::warn!(
                    position_id = %id,
                    user_id = %user_id,
                    margin_balance = %margin_balance,
                    maintenance_margin = %maintenance_margin,
                    "Liquidation triggered!"
                );

                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;

                let event = LiquidationEvent {
                    position_id: id,
                    user_id,
                    symbol: symbol.clone(),
                    side: side.clone(),
                    size,
                    entry_price,
                    mark_price: tick.mark_price,
                    margin,
                    timestamp,
                };

                self.producer.publish_liquidation(&event).await?;
            }
        }

        Ok(())
    }
}
