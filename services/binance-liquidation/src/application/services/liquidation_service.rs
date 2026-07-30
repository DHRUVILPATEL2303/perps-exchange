use std::sync::Arc;
use std::time::Duration;
use anyhow::{Result, bail};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio_tungstenite::connect_async;
use crate::domain::entities::liquidation::Liquidation;
use crate::domain::repositories::liquidation_publisher::LiquidationPublisher;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BinanceLiquidationPayload {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "S")]
    side: String,
    #[serde(rename = "o")]
    order_type: String,
    #[serde(rename = "f")]
    time_in_force: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "ap")]
    average_price: String,
    #[serde(rename = "X")]
    status: String,
    #[serde(rename = "T")]
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BinanceWSMessage {
    #[serde(rename = "o")]
    order: BinanceLiquidationPayload,
}

pub struct ListenerService {
    publisher: Arc<dyn LiquidationPublisher>,
    ws_url: String,
}

impl ListenerService {
    pub fn new(publisher: Arc<dyn LiquidationPublisher>, ws_url: String) -> Self {
        Self { publisher, ws_url }
    }

    pub async fn start(&self) -> Result<()> {
        loop {
            tracing::info!("Connecting to WebSocket: {}", self.ws_url);
            match connect_async(&self.ws_url).await {
                Ok((ws_stream, _)) => {
                    tracing::info!("Connected successfully to websocket");
                    let (_, mut read) = ws_stream.split();

                    while let Some(msg) = read.next().await {
                        match msg {
                            Ok(message) => {
                                if let Ok(text) = message.to_text() {
                                    if let Ok(parsed) = serde_json::from_str::<BinanceWSMessage>(text) {
                                        let liquidation = Liquidation {
                                            symbol: parsed.order.symbol,
                                            side: parsed.order.side,
                                            order_type: parsed.order.order_type,
                                            time_in_force: parsed.order.time_in_force,
                                            quantity: parsed.order.quantity,
                                            price: parsed.order.price,
                                            average_price: parsed.order.average_price,
                                            status: parsed.order.status,
                                            timestamp: parsed.order.timestamp,
                                        };

                                        tracing::info!(
                                            "Parsed Liquidation: {} | {} | {} | Price: {} | Qty: {}",
                                            liquidation.symbol,
                                            liquidation.side,
                                            liquidation.status,
                                            liquidation.price,
                                            liquidation.quantity
                                        );

                                        if let Err(e) = self.publisher.publish(&liquidation).await {
                                            tracing::error!("Publish error: {:?}", e);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("Read message error: {:?}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Connection error: {:?}, retrying in 5 seconds...", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}
