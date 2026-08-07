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
use crate::application::usecase::account_usecase::AccountUseCase;

#[derive(Deserialize)]
struct KafkaDepositEvent {
    pub user_id: Uuid,
    pub amount: String,
    pub asset: String,
    pub tx_hash: String,
}

pub struct DepositConsumer {
    consumer: StreamConsumer,
    account_service: Arc<dyn AccountUseCase>,
}

impl DepositConsumer {
    pub fn new(
        brokers: &str,
        group_id: &str,
        account_service: Arc<dyn AccountUseCase>,
    ) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .create()?;

        consumer.subscribe(&["solana-deposits"])?;

        Ok(Self {
            consumer,
            account_service,
        })
    }

    pub async fn run(self) {
        let mut stream = self.consumer.stream();

        while let Some(msg_result) = stream.next().await {
            match msg_result {
                Err(e) => {
                    tracing::error!("Solana deposits Kafka consumer error: {}", e);
                }
                Ok(msg) => {
                    if let Some(payload) = msg.payload() {
                        if let Ok(event) = serde_json::from_slice::<KafkaDepositEvent>(payload) {
                            if let Err(e) = self.process_deposit_event(event).await {
                                tracing::error!("Failed to process deposit event: {:?}", e);
                            }
                            let _ = self.consumer.commit_message(&msg, CommitMode::Async);
                        }
                    }
                }
            }
        }
    }

    async fn process_deposit_event(&self, event: KafkaDepositEvent) -> Result<()> {
        let amount = Decimal::from_str(&event.amount)?;
        
        tracing::info!(
            "Processing on-chain deposit for user_id {}: {} {} (tx: {})",
            event.user_id, amount, event.asset, event.tx_hash
        );

        self.account_service
            .adjust_margin(event.user_id, &event.asset, amount, "DEPOSIT", Some(event.tx_hash))
            .await?;

        Ok(())
    }
}
