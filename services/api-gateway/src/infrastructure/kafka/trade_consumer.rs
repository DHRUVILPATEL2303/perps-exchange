use std::str::FromStr;
use anyhow::Result;
use futures_util::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use crate::state::AppState;

#[derive(Deserialize, Serialize, Clone)]
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
    pub maker_leverage: u32,
    pub taker_leverage: u32,
}

#[derive(Serialize)]
struct UserNotification {
    pub user_id: String,
    pub message: String,
}

pub async fn run_trade_consumer(state: AppState) -> Result<()> {
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "api-gateway-trades-group")
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true")
        .create()?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()?;

    consumer.subscribe(&["execution-reports"])?;

    let mut stream = consumer.stream();
    while let Some(msg_result) = stream.next().await {
        match msg_result {
            Err(e) => {
                tracing::error!("Kafka consumption error in gateway: {}", e);
            }
            Ok(msg) => {
                if let Some(payload) = msg.payload() {
                    if let Ok(event) = serde_json::from_slice::<TradeEvent>(payload) {
                        let ws_sessions = state.ws_sessions.clone();
                        let event_json = match serde_json::to_string(&event) {
                            Ok(j) => j,
                            Err(_) => continue,
                        };

                        let mut sessions = ws_sessions.lock().await;

                        let _maker_online = if let Some(user_sessions) = sessions.get_mut(&event.maker_user_id) {
                            let mut i = 0;
                            while i < user_sessions.len() {
                                let mut to_remove = false;
                                {
                                    let (_, sess) = &mut user_sessions[i];
                                    if sess.text(event_json.clone()).await.is_err() {
                                        to_remove = true;
                                    }
                                }
                                if to_remove {
                                    user_sessions.remove(i);
                                } else {
                                    i += 1;
                                }
                            }
                            !user_sessions.is_empty()
                        } else {
                            false
                        };

                        let price_formatted = rust_decimal::Decimal::from_str(&event.price)
                            .map(|d| format!("{:.2}", d))
                            .unwrap_or_else(|_| event.price.clone());
                        let qty_formatted = rust_decimal::Decimal::from_str(&event.quantity)
                            .map(|d| format!("{:.2}", d))
                            .unwrap_or_else(|_| event.quantity.clone());

                        let maker_side = if event.taker_side == "BUY" { "SELL" } else { "BUY" };
                        let message = format!(
                            "🟢 **Order Executed (Maker)**\nSymbol: {}\nSide: {}\nPrice: {}\nQty: {}",
                            event.symbol, maker_side, price_formatted, qty_formatted
                        );
                        let notif = UserNotification {
                            user_id: event.maker_user_id.clone(),
                            message,
                        };
                        if let Ok(payload) = serde_json::to_vec(&notif) {
                            let _ = producer.send(
                                FutureRecord::to("user-notifications")
                                    .payload(&payload)
                                    .key(event.maker_user_id.as_bytes()),
                                Duration::from_secs(5),
                            ).await;
                        }

                        let _taker_online = if let Some(user_sessions) = sessions.get_mut(&event.taker_user_id) {
                            let mut i = 0;
                            while i < user_sessions.len() {
                                let mut to_remove = false;
                                {
                                    let (_, sess) = &mut user_sessions[i];
                                    if sess.text(event_json.clone()).await.is_err() {
                                        to_remove = true;
                                    }
                                }
                                if to_remove {
                                    user_sessions.remove(i);
                                } else {
                                    i += 1;
                                }
                            }
                            !user_sessions.is_empty()
                        } else {
                            false
                        };

                        let message = format!(
                            "🟢 **Order Executed (Taker)**\nSymbol: {}\nSide: {}\nPrice: {}\nQty: {}",
                            event.symbol, event.taker_side, price_formatted, qty_formatted
                        );
                        let notif = UserNotification {
                            user_id: event.taker_user_id.clone(),
                            message,
                        };
                        if let Ok(payload) = serde_json::to_vec(&notif) {
                            let _ = producer.send(
                                FutureRecord::to("user-notifications")
                                    .payload(&payload)
                                    .key(event.taker_user_id.as_bytes()),
                                Duration::from_secs(5),
                            ).await;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
