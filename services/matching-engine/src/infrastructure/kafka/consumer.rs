use crate::application::services::matching_service::OrderBook;
use crate::domain::entities::order::{BookOrder, OrderSide, OrderStatus, OrderType};
use crate::domain::entities::trade::Trade;
use crate::infrastructure::kafka::producer::TradeProducer;
use anyhow::Result;
use chrono::Utc;
use futures_util::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Deserialize)]
pub struct IncomingOrder {
    pub id: String,
    pub user_id: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub price: String,
    pub quantity: String,
    pub action: Option<String>,
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
                                let order_id = match Uuid::parse_str(&incoming.id) {
                                    Ok(uid) => uid,
                                    Err(e) => {
                                        tracing::error!("Invalid order UUID: {}", e);
                                        continue;
                                    }
                                };
                                let user_id = match Uuid::parse_str(&incoming.user_id) {
                                    Ok(uid) => uid,
                                    Err(e) => {
                                        tracing::error!("Invalid user UUID: {}", e);
                                        continue;
                                    }
                                };

                                let symbol = incoming.symbol.clone();
                                let side = match incoming.side.as_str() {
                                    "BUY" => OrderSide::Buy,
                                    "SELL" => OrderSide::Sell,
                                    _ => OrderSide::Buy,
                                };

                                if incoming.action == Some("CANCEL".to_string()) {
                                    let mut books = order_books.lock().await;
                                    let book = books
                                        .entry(symbol.clone())
                                        .or_insert_with(|| OrderBook::new(symbol.clone()));
                                    if let Some((price, qty)) = book.cancel_order(order_id, &side) {
                                        let cancel_trade = Trade {
                                            id: Uuid::new_v4(),
                                            symbol: symbol.clone(),
                                            maker_order_id: order_id,
                                            taker_order_id: Uuid::nil(),
                                            maker_user_id: user_id,
                                            taker_user_id: Uuid::nil(),
                                            price,
                                            quantity: qty,
                                            taker_side: "CANCEL".to_string(),
                                            executed_at: Utc::now(),
                                        };
                                        let _ = producer.publish_trade(&cancel_trade).await;

                                        let (bids, asks) = book.get_l2_depth(10);
                                        let _ = producer.publish_depth(&symbol, bids, asks).await;
                                    }
                                } else {
                                    let mut books = order_books.lock().await;
                                    let book = books
                                        .entry(symbol.clone())
                                        .or_insert_with(|| OrderBook::new(symbol.clone()));

                                    let limit_price = if incoming.order_type == "LIMIT" {
                                        Some(Decimal::from_str(&incoming.price).unwrap())
                                    } else {
                                        None
                                    };

                                    let taker = BookOrder {
                                        id: order_id,
                                        user_id,
                                        symbol: symbol.clone(),
                                        side: match side {
                                            OrderSide::Buy => OrderSide::Buy,
                                            OrderSide::Sell => OrderSide::Sell,
                                        },
                                        order_type: match incoming.order_type.as_str() {
                                            "LIMIT" => OrderType::Limit,
                                            "MARKET" => OrderType::Market,
                                            _ => OrderType::Limit,
                                        },
                                        price: limit_price.unwrap_or(Decimal::ZERO),
                                        quantity: Decimal::from_str(&incoming.quantity).unwrap(),
                                        filled_quantity: Decimal::ZERO,
                                        status: OrderStatus::New,
                                        created_at: Utc::now(),
                                    };

                                    let trades = book.match_order(taker);
                                    for trade in trades {
                                        let _ = producer.publish_trade(&trade).await;
                                    }

                                    let (bids, asks) = book.get_l2_depth(10);
                                    let _ = producer.publish_depth(&symbol, bids, asks).await;
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to deserialize incoming order: {}", e);
                            }
                        }
                        let _ = consumer.commit_message(&msg, CommitMode::Async);
                    }
                }
            }
        }
    }
}
