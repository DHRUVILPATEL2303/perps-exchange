use crate::application::services::matching_service::OrderBook;
use crate::domain::entities::order::{BookOrder, OrderSide, OrderStatus, OrderType};
use crate::domain::entities::trade::Trade;
use crate::infrastructure::kafka::producer::TradeProducer;
use anyhow::Result;
use chrono::Utc;
use dashmap::DashMap;
use futures_util::StreamExt;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use uuid::Uuid;
use telemetry::metrics::{ORDERS_PROCESSED_TOTAL, MATCHING_DURATION_SECONDS};
use tracing::{info_span, Instrument};

#[derive(Deserialize, Debug)]
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
    router: Arc<DashMap<String, mpsc::UnboundedSender<IncomingOrder>>>,
    producer: Arc<TradeProducer>,
}

impl OrderConsumer {
    pub fn new(
        brokers: &str,
        group_id: &str,
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
            router: Arc::new(DashMap::new()),
            producer,
        })
    }

    pub async fn run(self) {
        let consumer = self.consumer;
        let router = self.router;
        let producer = self.producer;

        let mut stream = consumer.stream();

        tracing::info!("Matching engine consuming from order-events...");

        while let Some(msg_result) = stream.next().await {
            match msg_result {
                Err(e) => tracing::error!("Kafka error: {}", e),
                Ok(msg) => {
                    if let Some(payload) = msg.payload() {
                        match serde_json::from_slice::<IncomingOrder>(payload) {
                            Ok(incoming) => {
                                let symbol = incoming.symbol.clone();
                                
                                let tx = {
                                    if let Some(tx) = router.get(&symbol) {
                                        tx.clone()
                                    } else {
                                        let (tx, rx) = mpsc::unbounded_channel();
                                        router.insert(symbol.clone(), tx.clone());
                                        
                                        let p = producer.clone();
                                        tokio::spawn(symbol_worker(symbol.clone(), rx, p));
                                        
                                        tx
                                    }
                                };

                                if let Err(e) = tx.send(incoming) {
                                    tracing::error!("Failed to route order to symbol worker: {}", e);
                                }
                            }
                            Err(e) => tracing::error!("Failed to deserialize incoming order: {}", e),
                        }
                    }
                }
            }
        }
    }
}

async fn symbol_worker(
    symbol: String,
    mut rx: mpsc::UnboundedReceiver<IncomingOrder>,
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
                
                let span = info_span!("match_order", symbol = %symbol, order_id = %incoming.id);
                let _enter = span.enter();

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

        let side = match incoming.side.as_str() {
            "BUY" => OrderSide::Buy,
            "SELL" => OrderSide::Sell,
            _ => OrderSide::Buy,
        };

        if incoming.action == Some("CANCEL".to_string()) {
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
                
                let p = producer.clone();
                tokio::spawn(async move {
                    let _ = p.publish_trade(&cancel_trade).await;
                });
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
                let p = producer.clone();
                tokio::spawn(async move {
                    let _ = p.publish_trade(&trade).await;
                });
            }
            ORDERS_PROCESSED_TOTAL.with_label_values(&[&symbol, "success_match"]).inc();
        }
        
        let duration = start_time.elapsed().as_secs_f64();
        MATCHING_DURATION_SECONDS.with_label_values(&[&symbol]).observe(duration);
            }
            _ = depth_interval.tick() => {
                // Throttle depth publishing to every 100ms
                let (bids, asks) = book.get_l2_depth(10);
                let p = producer.clone();
                let sym = symbol.clone();
                tokio::spawn(async move {
                    let _ = p.publish_depth(&sym, bids, asks).await;
                });
            }
        }
    }
}
