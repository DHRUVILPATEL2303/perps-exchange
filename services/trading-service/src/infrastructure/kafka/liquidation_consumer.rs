use crate::domain::repositories::position_repository::PositionRepository;
use crate::infrastructure::grpc::account_client::AccountGrpcClient;
use crate::infrastructure::kafka::producer::KafkaOrderEvent;
use anyhow::Result;
use futures_util::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

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
    order_producer: Arc<crate::infrastructure::kafka::producer::OrderProducer>,
}

impl LiquidationConsumer {
    pub fn new(
        brokers: &str,
        group_id: &str,
        position_repository: Arc<dyn PositionRepository>,
        account_client: Arc<Mutex<AccountGrpcClient>>,
        order_producer: Arc<crate::infrastructure::kafka::producer::OrderProducer>,
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
            order_producer,
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
        let position_opt = self
            .position_repository
            .find_by_user_symbol_side(event.user_id, &event.symbol, &event.side)
            .await?;

        if let Some(position) = position_opt {
            if position.size > Decimal::ZERO {
                let close_side = if position.side == "LONG" {
                    "SELL"
                } else {
                    "BUY"
                };

                let kill_order = crate::infrastructure::kafka::producer::KafkaOrderEvent {
                    id: Uuid::new_v4().to_string(),
                    user_id: event.user_id.to_string(),
                    symbol: event.symbol.clone(),
                    side: close_side.to_string(),
                    order_type: "MARKET".to_string(),
                    price: "0".to_string(),
                    quantity: position.size.to_string(),
                    action: "CREATE".to_string(),
                };

                self.order_producer.publish_order(&kill_order).await?;

                tracing::warn!(
                    user_id = %event.user_id,
                    symbol = %event.symbol,
                    side = %close_side,
                    "Sent Market Kill Order to Matching Engine to liquidate position"
                );
            }
        }
        Ok(())
    }
}
