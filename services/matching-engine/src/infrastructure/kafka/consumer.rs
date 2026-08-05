use crate::application::services::matching_service::OrderBook;
use crate::domain::entities::order::{BookOrder, OrderSide, OrderStatus, OrderType};
use crate::domain::entities::trade::Trade;
use crate::infrastructure::kafka::producer::TradeProducer;
use anyhow::Result;
use chrono::Utc;
use rustc_hash::FxHashMap;
use futures_util::StreamExt;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use telemetry::metrics::{MATCHING_DURATION_SECONDS, ORDERS_PROCESSED_TOTAL, KAFKA_MESSAGES_CONSUMED_TOTAL};
use tokio::sync::mpsc;
use tracing::{Instrument, info_span};
use uuid::Uuid;

#[derive(Deserialize, Debug)]
pub struct IncomingOrder {
    pub id: Uuid,
    pub user_id: Uuid,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub price: String,
    pub quantity: String,
    pub action: String,
    pub timestamp: u64,
}

pub struct OrderConsumer {
    consumer: StreamConsumer,
    producer: Arc<TradeProducer>,
}

impl OrderConsumer {
    pub fn new(brokers: &str, group_id: &str, producer: Arc<TradeProducer>) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", "matching-engine-group")
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .set("fetch.message.max.bytes", "104857600")
            .set("receive.message.max.bytes", "104857600")
            .set("queued.max.messages.kbytes", "1048576")
            .set("fetch.wait.max.ms", "500")
            .set("debug", "consumer,cgrp,topic")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()?;

        consumer.subscribe(&["order-events"])?;

        Ok(Self {
            consumer,
            producer,
        })
    }

    pub async fn run(self) {
        let consumer = self.consumer;
        let producer = self.producer;
        
        let mut router: FxHashMap<String, mpsc::Sender<IncomingOrder>> = FxHashMap::default();

        let mut stream = consumer.stream();

        tracing::info!("Matching engine consuming from order-events...");

        while let Some(msg_result) = stream.next().await {
            match msg_result {
                Err(e) => tracing::error!("Kafka error: {}", e),
                Ok(msg) => {
                    KAFKA_MESSAGES_CONSUMED_TOTAL.with_label_values(&["order-events"]).inc();
                    if let Some(payload) = msg.payload() {
                        match bincode::deserialize::<IncomingOrder>(payload) {
                            Ok(incoming) => {
                                let symbol = incoming.symbol.clone();

                                let tx = {
                                    if let Some(tx) = router.get(&symbol) {
                                        tx.clone()
                                    } else {
                                        let (tx, rx) = mpsc::channel(100_000); // Bounded channel to prevent OOM
                                        router.insert(symbol.clone(), tx.clone());

                                        let p = producer.clone();
                                        tokio::spawn(symbol_worker(symbol.clone(), rx, p));

                                        tx
                                    }
                                };

                                if let Err(e) = tx.send(incoming).await {
                                    tracing::error!(
                                        "Failed to route order to symbol worker: {}",
                                        e
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to deserialize incoming order: {}", e)
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn symbol_worker(
    symbol: String,
    mut rx: mpsc::Receiver<IncomingOrder>,
    producer: Arc<TradeProducer>,
) {
    tracing::info!("Started dedicated matching worker for {}", symbol);
    let mut book = OrderBook::new(symbol.clone());
    let mut depth_interval = tokio::time::interval(tokio::time::Duration::from_millis(100));

    loop {
        tokio::select! {
            incoming_opt = rx.recv() => {
                let incoming = match incoming_opt {
                    Some(msg) => msg,
                    None => break, // Channel closed, exit worker
                };

                let start_time = Instant::now();

                if incoming.timestamp > 0 {
                    let sent_ts_us = incoming.timestamp;
                    let now_us = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_micros() as u64;
                    if now_us >= sent_ts_us {
                        let transit_sec = (now_us - sent_ts_us) as f64 / 1_000_000.0;
                        telemetry::metrics::ORDER_TRANSIT_DURATION_SECONDS
                            .with_label_values(&[&symbol])
                            .observe(transit_sec);
                    }
                }



        let order_id = incoming.id;
        let user_id = incoming.user_id;

        let side = match incoming.side.as_str() {
            "BUY" => OrderSide::Buy,
            "SELL" => OrderSide::Sell,
            _ => OrderSide::Buy,
        };

        if incoming.action == "CANCEL" {
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

                if let Err(e) = producer.publish_trade_sync(&cancel_trade) {
                    tracing::error!("Failed to publish cancel trade: {}", e);
                }
            }
            ORDERS_PROCESSED_TOTAL.with_label_values(&[&symbol, "success_cancel"]).inc();
        } else {
            let limit_price = if incoming.order_type == "LIMIT" {
                Some(Decimal::from_str(&incoming.price).unwrap_or(Decimal::ZERO))
            } else {
                None
            };

            let taker = BookOrder {
                id: order_id,
                user_id: user_id,
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
                quantity: Decimal::from_str(&incoming.quantity).unwrap_or(Decimal::ZERO),
                filled_quantity: Decimal::ZERO,
                status: OrderStatus::New,
                created_at: Utc::now(),
            };

            let trades = book.match_order(taker);
            for trade in trades {
                if let Err(e) = producer.publish_trade_sync(&trade) {
                    tracing::error!("Failed to publish trade: {}", e);
                }
            }
            ORDERS_PROCESSED_TOTAL.with_label_values(&[&symbol, "success_match"]).inc();
        }

        let duration = start_time.elapsed().as_secs_f64();
        MATCHING_DURATION_SECONDS.with_label_values(&[&symbol]).observe(duration);
            }
            _ = depth_interval.tick() => {
                let (bids, asks) = book.get_l2_depth(10);
                if let Err(e) = producer.publish_depth_sync(&symbol, bids, asks) {
                    tracing::error!("Failed to publish depth: {}", e);
                }
            }
        }
    }
}
