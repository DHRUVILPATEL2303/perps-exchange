use crate::application::services::matching_service::OrderBook;
use crate::domain::entities::order::{BookOrder, OrderSide, OrderStatus, OrderType};
use crate::domain::entities::trade::Trade;
use crate::infrastructure::kafka::producer::TradeProducer;
use anyhow::Result;
use chrono::Utc;
use futures_util::{StreamExt, FutureExt};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rust_decimal::Decimal;
use rustc_hash::FxHashMap;
use serde::Deserialize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use telemetry::metrics::{
    KAFKA_MESSAGES_CONSUMED_TOTAL, MATCHING_DURATION_SECONDS, ORDERS_PROCESSED_TOTAL,
    ORDER_MATCH_PURE_DURATION_SECONDS, ORDER_CANCEL_PURE_DURATION_SECONDS, ORDER_CHANNEL_LATENCY_SECONDS,
    KAFKA_POLL_DURATION_SECONDS, KAFKA_MESSAGES_PER_POLL, ORDER_DESERIALIZE_DURATION_SECONDS,
};
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
    pub leverage: u32,
    pub reduce_only: bool,
    
    #[serde(skip)]
    pub local_received_timestamp: u64,
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
            .set("max.poll.interval.ms", "900000")
            .set("debug", "consumer,cgrp,topic")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()?;

        consumer.subscribe(&["order-events"])?;

        Ok(Self { consumer, producer })
    }

    pub async fn run(self) {
        let consumer = self.consumer;
        let producer = self.producer;

        let mut router: FxHashMap<String, mpsc::Sender<IncomingOrder>> = FxHashMap::default();

        let mut stream = consumer.stream();

        tracing::info!("Matching engine consuming from order-events...");

        let mut start_poll = Instant::now();

        while let Some(msg_result) = stream.next().await {
            let poll_duration = start_poll.elapsed().as_secs_f64();
            KAFKA_POLL_DURATION_SECONDS
                .with_label_values(&["order-events"])
                .observe(poll_duration);

            let mut batch = vec![msg_result];
            while let Some(Some(next_msg_result)) = stream.next().now_or_never() {
                batch.push(next_msg_result);
                if batch.len() >= 10000 {
                    break;
                }
            }

            KAFKA_MESSAGES_PER_POLL
                .with_label_values(&["order-events"])
                .observe(batch.len() as f64);

            for msg_result in batch {
                match msg_result {
                    Err(e) => tracing::error!("Kafka error: {}", e),
                    Ok(msg) => {
                        KAFKA_MESSAGES_CONSUMED_TOTAL
                            .with_label_values(&["order-events"])
                            .inc();
                        if let Some(payload) = msg.payload() {
                            let start_deserialize = Instant::now();
                            let deserialize_res = bincode::deserialize::<IncomingOrder>(payload);
                            let deserialize_duration = start_deserialize.elapsed().as_secs_f64();
                            ORDER_DESERIALIZE_DURATION_SECONDS
                                .with_label_values(&["order-events"])
                                .observe(deserialize_duration);

                            match deserialize_res {
                                Ok(mut incoming) => {
                                    let symbol = incoming.symbol.clone();
                                    incoming.local_received_timestamp = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_micros() as u64;

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

            start_poll = Instant::now();
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
    depth_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut order_count = 0u64;

    let (trade_tx, mut trade_rx) = mpsc::channel(100_000);
    let p_trades = producer.clone();
    tokio::spawn(async move {
        while let Some(trade) = trade_rx.recv().await {
            if let Err(e) = p_trades.publish_trade_sync(&trade).await {
                tracing::error!("Failed to publish trade: {}", e);
            }
        }
    });

    let (depth_tx, mut depth_rx) = mpsc::channel(100);
    let p_depth = producer.clone();
    let d_symbol = symbol.clone();
    tokio::spawn(async move {
        while let Some((bids, asks)) = depth_rx.recv().await {
            if let Err(e) = p_depth.publish_depth_sync(&d_symbol, bids, asks).await {
                tracing::error!("Failed to publish depth: {}", e);
            }
        }
    });

    loop {
        tokio::select! {
            incoming_opt = rx.recv() => {
                let incoming = match incoming_opt {
                    Some(msg) => msg,
                    None => break, // Channel closed, exit worker
                };

                order_count += 1;
                if order_count % 1000 == 0 {
                    tokio::task::yield_now().await;
                }

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

                let now_us = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as u64;
                if incoming.local_received_timestamp > 0 && now_us >= incoming.local_received_timestamp {
                    let channel_latency = (now_us - incoming.local_received_timestamp) as f64 / 1_000_000.0;
                    ORDER_CHANNEL_LATENCY_SECONDS
                        .with_label_values(&[&symbol])
                        .observe(channel_latency);
                }



        let order_id = incoming.id;
        let user_id = incoming.user_id;

        let side = match incoming.side.as_str() {
            "BUY" => OrderSide::Buy,
            "SELL" => OrderSide::Sell,
            _ => OrderSide::Buy,
        };

        if incoming.action == "CANCEL" {
            let start_cancel = Instant::now();
            let cancel_res = book.cancel_order(order_id, &side);
            let cancel_duration = start_cancel.elapsed().as_secs_f64();
            ORDER_CANCEL_PURE_DURATION_SECONDS
                .with_label_values(&[&symbol])
                .observe(cancel_duration);

            if let Some((price, qty, leverage)) = cancel_res {
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
                    maker_leverage: leverage,
                    taker_leverage: 0,
                };

                let _ = trade_tx.try_send(cancel_trade);
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
                leverage: incoming.leverage,
            };

            let start_match = Instant::now();
            let trades = book.match_order(taker);
            let match_duration = start_match.elapsed().as_secs_f64();
            ORDER_MATCH_PURE_DURATION_SECONDS
                .with_label_values(&[&symbol])
                .observe(match_duration);

            for trade in trades {
                let _ = trade_tx.try_send(trade);
            }
            ORDERS_PROCESSED_TOTAL.with_label_values(&[&symbol, "success_match"]).inc();
        }

        let duration = start_time.elapsed().as_secs_f64();
        MATCHING_DURATION_SECONDS.with_label_values(&[&symbol]).observe(duration);
            }
            _ = depth_interval.tick() => {
                let (bids, asks) = book.get_l2_depth(10);
                let _ = depth_tx.try_send((bids, asks));
            }
        }
    }
}
