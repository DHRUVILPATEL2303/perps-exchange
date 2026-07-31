use std::sync::Arc;
use anyhow::Result;
use futures_util::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer, CommitMode};
use rdkafka::message::Message;
use rust_decimal::Decimal;
use serde::Deserialize;
use uuid::Uuid;
use tokio::sync::Mutex;
use crate::domain::repositories::position_repository::PositionRepository;
use crate::infrastructure::grpc::account_client::AccountGrpcClient;

#[derive(Deserialize, Debug)]
pub struct LiquidationEvent {
    pub position_id: Uuid,
    pub user_id: Uuid,
    pub symbol: String,
    pub side: String,
    pub margin: Decimal,
}

pub struct LiquidationConsumer {
    consumer: StreamConsumer,
    position_repository: Arc<dyn PositionRepository>,
    account_client: Arc<Mutex<AccountGrpcClient>>,
}

impl LiquidationConsumer {
    pub fn new(
        brokers: &str,
        group_id: &str,
        position_repository: Arc<dyn PositionRepository>,
        account_client: Arc<Mutex<AccountGrpcClient>>,
    ) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .create()?;

        consumer.subscribe(&["liquidations"])?;

        Ok(Self {
            consumer,
            position_repository,
            account_client,
        })
    }

    pub async fn run(self) {
        let mut stream = self.consumer.stream();

        while let Some(msg_result) = stream.next().await {
            match msg_result {
                Err(e) => {
                    tracing::error!("Kafka liquidation consumer error: {}", e);
                }
                Ok(msg) => {
                    if let Some(payload) = msg.payload() {
                        match serde_json::from_slice::<LiquidationEvent>(payload) {
                            Ok(event) => {
                                if let Err(e) = self.execute_liquidation(event).await {
                                    tracing::error!("Failed to execute liquidation: {:?}", e);
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Failed to deserialize LiquidationEvent: {:?}, payload: {:?}",
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

    async fn execute_liquidation(&self, event: LiquidationEvent) -> Result<()> {
        let position_opt = self.position_repository.find_by_user_symbol_side(event.user_id, &event.symbol, &event.side).await?;
        if let Some(mut position) = position_opt {
            if position.size > Decimal::ZERO {
                position.size = Decimal::ZERO;
                position.margin = Decimal::ZERO;
                position.liquidation_price = Decimal::ZERO;
                position.updated_at = chrono::Utc::now();
                self.position_repository.update(position).await?;

                let mut client = self.account_client.lock().await;
                let _ = client.release_margin(event.user_id.to_string(), event.margin.to_string(), event.position_id.to_string()).await;
                let _ = client.adjust_margin(event.user_id.to_string(), (-event.margin).to_string(), "PNL".to_string()).await;

                tracing::info!(
                    user_id = %event.user_id,
                    symbol = %event.symbol,
                    side = %event.side,
                    "Position successfully closed out by liquidator"
                );
            }
        }

        Ok(())
    }
}
