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
use crate::{
    application::usecase::position_usecase::PositionUseCase,
    domain::repositories::order_repository::OrderRepository,
};

#[derive(Deserialize)]
pub struct TradeEvent {
    pub id: String,
    pub symbol: String,
    pub maker_order_id: String,
    pub taker_order_id: String,
    pub maker_user_id: String,
    pub taker_user_id: String,
    pub price: String,
    pub quantity: String,
    pub taker_side: String,
    pub executed_at: String,
}

pub struct TradeConsumer {
    consumer: StreamConsumer,
    position_service: Arc<dyn PositionUseCase>,
    order_repository: Arc<dyn OrderRepository>,
}

impl TradeConsumer {
    pub fn new(
        brokers: &str,
        group_id: &str,
        position_service: Arc<dyn PositionUseCase>,
        order_repository: Arc<dyn OrderRepository>,
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
            position_service,
            order_repository,
        })
    }

    pub async fn run(self) {
        let mut stream = self.consumer.stream();

        while let Some(msg_result) = stream.next().await {
            match msg_result {
                Err(e) => {
                    tracing::error!("Kafka consumption error: {}", e);
                }
                Ok(msg) => {
                    if let Some(payload) = msg.payload() {
                        if let Ok(event) = serde_json::from_slice::<TradeEvent>(payload) {
                            if let Err(e) = self.process_trade_event(event).await {
                                tracing::error!("Failed to process trade: {:?}", e);
                            }
                            let _ = self.consumer.commit_message(&msg, CommitMode::Async);
                        }
                    }
                }
            }
        }
    }

    async fn process_trade_event(&self, event: TradeEvent) -> Result<()> {
        if event.taker_side == "CANCEL" {
            let order_id = Uuid::parse_str(&event.maker_order_id)?;
            let user_id = Uuid::parse_str(&event.maker_user_id)?;
            let price = Decimal::from_str(&event.price)?;
            let qty = Decimal::from_str(&event.quantity)?;
            
            let leverage = Decimal::from(20);
            let margin_to_release = (qty * price) / leverage;

            self.order_repository.update_status(order_id, "CANCELLED").await?;

            let services_host = std::env::var("SERVICES_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
            let mut client = proto::account::account_service_client::AccountServiceClient::connect(format!("http://{}:50053", services_host)).await?;
            let request = tonic::Request::new(proto::account::ReleaseMarginRequest {
                user_id: user_id.to_string(),
                amount: margin_to_release.to_string(),
                reference_id: order_id.to_string(),
            });
            let _ = client.release_margin(request).await?;

            tracing::info!(
                order_id = %order_id,
                user_id = %user_id,
                released_margin = %margin_to_release,
                "Order cancelled in matching book; margin successfully un-frozen and database status set to CANCELLED"
            );
            return Ok(());
        }

        let price = Decimal::from_str(&event.price)?;
        let qty = Decimal::from_str(&event.quantity)?;
        
        let maker_user = Uuid::parse_str(&event.maker_user_id)?;
        let maker_order = Uuid::parse_str(&event.maker_order_id)?;

        let taker_user = Uuid::parse_str(&event.taker_user_id)?;
        let taker_order = Uuid::parse_str(&event.taker_order_id)?;

        self.order_repository.update_status(maker_order, "FILLED").await?;
        self.order_repository.update_status(taker_order, "FILLED").await?;

        let maker_side = if event.taker_side == "BUY" { "SELL" } else { "BUY" };
        let taker_side = &event.taker_side;

        self.position_service.update_position_on_fill(
            taker_user,
            &event.symbol,
            taker_side,
            price,
            qty,
            20,
            taker_order,
        ).await?;

        self.position_service.update_position_on_fill(
            maker_user,
            &event.symbol,
            maker_side,
            price,
            qty,
            20,
            maker_order,
        ).await?;

        Ok(())
    }
}
