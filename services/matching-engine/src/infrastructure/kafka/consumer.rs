use anyhow::Result;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::ClientConfig;
use rdkafka::message::Message;
use rdkafka::config::RDKafkaLogLevel;
use serde::Deserialize;
use rust_decimal::Decimal;
use rdkafka::consumer::CommitMode;
use futures_util::StreamExt;
use uuid::Uuid;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::HashMap;
use crate::application::services::matching_service::OrderBook;
use crate::domain::entities::order::{BookOrder, OrderSide, OrderStatus, OrderType};
use crate::infrastructure::kafka::producer::TradeProducer;

#[derive(Deserialize)]
pub struct IncomingOrder {
    pub id: String,
    pub user_id: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub price: String,
    pub quantity: String,
}

pub struct OrderConsumer {
    consumer: StreamConsumer,
    order_books: Arc<Mutex<HashMap<String, OrderBook>>>,
    producer: Arc<TradeProducer>,
}

impl OrderConsumer {
    pub fn new(
        brokers: &str,
        group_id: &str,
        order_books: Arc<Mutex<HashMap<String, OrderBook>>>,
        producer: Arc<TradeProducer>,
    ) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "true")
            .set_log_level(RDKafkaLogLevel::Warning)
            .create()?;

        consumer.subscribe(&["order-events"])?;

        Ok(Self {
            consumer,
            order_books,
            producer,
        })
    }

    pub async fn run(self) {
    

        let consumer = self.consumer;
        let order_books = self.order_books;
        let producer = self.producer;

        let mut stream = consumer.stream();

        tracing::info!("Matching engine consuming from order-events...");

        while let Some(msg_result) = stream.next().await {
            match msg_result {
                Err(e) => {
                    tracing::error!("Kafka error: {}", e);
                }
                Ok(msg) => {
                    if let Some(payload) = msg.payload() {
                        match serde_json::from_slice::<IncomingOrder>(payload) {
                            Ok(incoming) => {
                                let order = match Self::parse_order(incoming) {
                                    Ok(o) => o,
                                    Err(e) => {
                                        tracing::error!("Order parse error: {}", e);
                                        continue;
                                    }
                                };

                                let symbol = order.symbol.clone();
                                let mut books = order_books.lock().await;
                                let book = books
                                    .entry(symbol.clone())
                                    .or_insert_with(|| OrderBook::new(symbol.clone()));

                                let trades = book.match_order(order);

                                for trade in trades {
                                    tracing::info!(
                                        symbol = %trade.symbol,
                                        price = %trade.price,
                                        qty = %trade.quantity,
                                        "Trade executed"
                                    );
                                    if let Err(e) = producer.publish_trade(&trade).await {
                                        tracing::error!("Failed to publish trade: {}", e);
                                    }
                                }

                                let _ = consumer.commit_message(&msg, CommitMode::Async);
                            }
                            Err(e) => {
                                tracing::error!("Deserialize error: {}", e);
                            }
                        }
                    }
                }
            }
        }
    }

    fn parse_order(incoming: IncomingOrder) -> Result<BookOrder> {
        let side = match incoming.side.as_str() {
            "BUY" | "LONG" => OrderSide::Buy,
            "SELL" | "SHORT" => OrderSide::Sell,
            _ => anyhow::bail!("Unknown side: {}", incoming.side),
        };

        let order_type = match incoming.order_type.as_str() {
            "LIMIT" => OrderType::Limit,
            "MARKET" => OrderType::Market,
            _ => anyhow::bail!("Unknown order type: {}", incoming.order_type),
        };

        Ok(BookOrder {
            id: Uuid::parse_str(&incoming.id)?,
            user_id: Uuid::parse_str(&incoming.user_id)?,
            symbol: incoming.symbol,
            side,
            order_type,
            price: Decimal::from_str(&incoming.price)?,
            quantity: Decimal::from_str(&incoming.quantity)?,
            filled_quantity: Decimal::ZERO,
            status: OrderStatus::Open,
            created_at: chrono::Utc::now(),
        })
    }
}
