use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::str::FromStr;
use serde_json::json;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use crate::domain::entities::price_tick::PriceTick;
use crate::domain::repositories::price_publisher::PricePublisher;

pub struct AggregatorService {
    publisher: Arc<dyn PricePublisher>,
    binance_price: Arc<Mutex<Option<Decimal>>>,
    coinbase_price: Arc<Mutex<Option<Decimal>>>,
}

impl AggregatorService {
    pub fn new(publisher: Arc<dyn PricePublisher>) -> Self {
        Self {
            publisher,
            binance_price: Arc::new(Mutex::new(None)),
            coinbase_price: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn start(&self) -> Result<()> {
        let binance_price = self.binance_price.clone();
        tokio::spawn(async move {
            let url = "wss://stream.binance.com:9443/ws/btcusdt@ticker";
            loop {
                tracing::info!("Connecting to Binance Spot WebSocket...");
                match connect_async(url).await {
                    Ok((ws_stream, _)) => {
                        tracing::info!("Connected to Binance Spot WebSocket");
                        let (_, mut read) = ws_stream.split();
                        while let Some(msg) = read.next().await {
                            match msg {
                                Ok(m) => {
                                    if let Ok(text) = m.to_text() {
                                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(text) {
                                            if let Some(price_str) = json.get("c").and_then(|v| v.as_str()) {
                                                if let Ok(price) = Decimal::from_str(price_str) {
                                                    let mut lock = binance_price.lock().unwrap();
                                                    *lock = Some(price);
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Binance WebSocket read error: {:?}", e);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Binance WebSocket connection error: {:?}", e);
                    }
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });

        let coinbase_price = self.coinbase_price.clone();
        tokio::spawn(async move {
            let url = "wss://ws-feed.exchange.coinbase.com";
            loop {
                tracing::info!("Connecting to Coinbase Spot WebSocket...");
                match connect_async(url).await {
                    Ok((mut ws_stream, _)) => {
                        tracing::info!("Connected to Coinbase Spot WebSocket, subscribing...");
                        let sub_msg = json!({
                            "type": "subscribe",
                            "product_ids": ["BTC-USD"],
                            "channels": ["ticker"]
                        });
                        if let Err(e) = ws_stream.send(Message::Text(sub_msg.to_string().into())).await {
                            tracing::error!("Coinbase subscription send error: {:?}", e);
                        }

                        let (_, mut read) = ws_stream.split();
                        while let Some(msg) = read.next().await {
                            match msg {
                                Ok(m) => {
                                    if let Ok(text) = m.to_text() {
                                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(text) {
                                            if let Some(price_str) = json.get("price").and_then(|v| v.as_str()) {
                                                if let Ok(price) = Decimal::from_str(price_str) {
                                                    let mut lock = coinbase_price.lock().unwrap();
                                                    *lock = Some(price);
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Coinbase WebSocket read error: {:?}", e);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Coinbase WebSocket connection error: {:?}", e);
                    }
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });

        let publisher = self.publisher.clone();
        let binance_price = self.binance_price.clone();
        let coinbase_price = self.coinbase_price.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                interval.tick().await;

                let b_price = { *binance_price.lock().unwrap() };
                let c_price = { *coinbase_price.lock().unwrap() };

                let index_price = match (b_price, c_price) {
                    (Some(b), Some(c)) => Some((b + c) / Decimal::from(2)),
                    (Some(b), None) => Some(b),
                    (None, Some(c)) => Some(c),
                    (None, None) => None,
                };

                if let Some(price) = index_price {
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;

                    let tick = PriceTick {
                        symbol: "BTCUSDT".to_string(),
                        index_price: price,
                        mark_price: price,
                        timestamp,
                    };

                    tracing::info!(
                        "Aggregated Price: {} | Index: {} | Mark: {}",
                        tick.symbol,
                        tick.index_price,
                        tick.mark_price
                    );

                    if let Err(e) = publisher.publish(&tick).await {
                        tracing::error!("Failed to publish price feed: {:?}", e);
                    }
                }
            }
        });

        Ok(())
    }
}
