use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::str::FromStr;
use serde_json::json;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use redis::AsyncCommands;
use crate::domain::entities::price_tick::PriceTick;
use crate::domain::repositories::price_publisher::PricePublisher;
use proto::market::{market_service_client::MarketServiceClient, ListMarketsRequest};

pub struct AggregatorService {
    publisher: Arc<dyn PricePublisher>,
    redis_client: redis::Client,
    market_service_url: String,
    binance_prices: Arc<Mutex<HashMap<String, Decimal>>>,
    coinbase_prices: Arc<Mutex<HashMap<String, Decimal>>>,
    active_symbols: Arc<Mutex<HashMap<String, Arc<AtomicBool>>>>,
}

impl AggregatorService {
    pub fn new(
        publisher: Arc<dyn PricePublisher>,
        redis_client: redis::Client,
        market_service_url: String,
    ) -> Self {
        Self {
            publisher,
            redis_client,
            market_service_url,
            binance_prices: Arc::new(Mutex::new(HashMap::new())),
            coinbase_prices: Arc::new(Mutex::new(HashMap::new())),
            active_symbols: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn start(&self) -> Result<()> {
        let market_service_url = self.market_service_url.clone();
        let binance_prices = self.binance_prices.clone();
        let coinbase_prices = self.coinbase_prices.clone();
        let active_symbols = self.active_symbols.clone();

        tokio::spawn(async move {
            loop {
                let active_markets = match MarketServiceClient::connect(market_service_url.clone()).await {
                    Ok(mut client) => {
                        match client.list_markets(ListMarketsRequest {}).await {
                            Ok(res) => res.into_inner().markets,
                            Err(e) => {
                                tracing::error!("Failed to list markets: {:?}", e);
                                Vec::new()
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to connect to market service: {:?}", e);
                        Vec::new()
                    }
                };

                if !active_markets.is_empty() {
                    let mut current_symbols = std::collections::HashSet::new();
                    for market in &active_markets {
                        if market.status == "ACTIVE" {
                            current_symbols.insert(market.symbol.clone());
                        }
                    }

                    let mut active_lock = active_symbols.lock().unwrap();

                    let to_remove: Vec<String> = active_lock
                        .keys()
                        .filter(|sym| !current_symbols.contains(*sym))
                        .cloned()
                        .collect();

                    for sym in to_remove {
                        if let Some(shutdown_flag) = active_lock.remove(&sym) {
                            shutdown_flag.store(true, Ordering::Relaxed);
                        }
                        let mut bp_lock = binance_prices.lock().unwrap();
                        bp_lock.remove(&sym);
                        let mut cp_lock = coinbase_prices.lock().unwrap();
                        cp_lock.remove(&sym);
                    }

                    for market in active_markets {
                        if market.status != "ACTIVE" {
                            continue;
                        }

                        let symbol = market.symbol.clone();
                        if !active_lock.contains_key(&symbol) {
                            let shutdown_flag = Arc::new(AtomicBool::new(false));
                            active_lock.insert(symbol.clone(), shutdown_flag.clone());

                            let base_asset = market.base_asset.clone();
                            let quote_asset = market.quote_asset.clone();

                            let bp = binance_prices.clone();
                            let sym_clone1 = symbol.clone();
                            let sd_clone1 = shutdown_flag.clone();
                            tokio::spawn(async move {
                                let lowercase_symbol = sym_clone1.to_lowercase();
                                let url = format!("wss://stream.binance.com:9443/ws/{}@ticker", lowercase_symbol);
                                while !sd_clone1.load(Ordering::Relaxed) {
                                    match connect_async(&url).await {
                                        Ok((ws_stream, _)) => {
                                            let (_, mut read) = ws_stream.split();
                                            while let Some(msg) = read.next().await {
                                                if sd_clone1.load(Ordering::Relaxed) {
                                                    break;
                                                }
                                                match msg {
                                                    Ok(m) => {
                                                        if let Ok(text) = m.to_text() {
                                                            if let Ok(json) = serde_json::from_str::<serde_json::Value>(text) {
                                                                if let Some(price_str) = json.get("c").and_then(|v| v.as_str()) {
                                                                    if let Ok(price) = Decimal::from_str(price_str) {
                                                                        let mut lock = bp.lock().unwrap();
                                                                        lock.insert(sym_clone1.clone(), price);
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        tracing::error!("Binance WebSocket read error for {}: {:?}", sym_clone1, e);
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!("Binance WebSocket connection error for {}: {:?}", sym_clone1, e);
                                        }
                                    }
                                    tokio::time::sleep(Duration::from_secs(5)).await;
                                }
                            });

                            let cp = coinbase_prices.clone();
                            let sym_clone2 = symbol.clone();
                            let sd_clone2 = shutdown_flag.clone();
                            tokio::spawn(async move {
                                let url = "wss://ws-feed.exchange.coinbase.com";
                                let product_id = if quote_asset == "USDT" {
                                    format!("{}-USD", base_asset)
                                } else {
                                    format!("{}-{}", base_asset, quote_asset)
                                };
                                while !sd_clone2.load(Ordering::Relaxed) {
                                    match connect_async(url).await {
                                        Ok((mut ws_stream, _)) => {
                                            let sub_msg = json!({
                                                "type": "subscribe",
                                                "product_ids": [product_id],
                                                "channels": ["ticker"]
                                            });
                                            if let Err(e) = ws_stream.send(Message::Text(sub_msg.to_string().into())).await {
                                                tracing::error!("Coinbase subscription send error for {}: {:?}", sym_clone2, e);
                                            }

                                            let (_, mut read) = ws_stream.split();
                                            while let Some(msg) = read.next().await {
                                                if sd_clone2.load(Ordering::Relaxed) {
                                                    break;
                                                }
                                                match msg {
                                                    Ok(m) => {
                                                        if let Ok(text) = m.to_text() {
                                                            if let Ok(json) = serde_json::from_str::<serde_json::Value>(text) {
                                                                if let Some(price_str) = json.get("price").and_then(|v| v.as_str()) {
                                                                    if let Ok(price) = Decimal::from_str(price_str) {
                                                                        let mut lock = cp.lock().unwrap();
                                                                        lock.insert(sym_clone2.clone(), price);
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        tracing::error!("Coinbase WebSocket read error for {}: {:?}", sym_clone2, e);
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!("Coinbase WebSocket connection error for {}: {:?}", sym_clone2, e);
                                        }
                                    }
                                    tokio::time::sleep(Duration::from_secs(5)).await;
                                }
                            });
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });

        let publisher = self.publisher.clone();
        let binance_prices = self.binance_prices.clone();
        let coinbase_prices = self.coinbase_prices.clone();
        let redis_client = self.redis_client.clone();
        let active_symbols = self.active_symbols.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                interval.tick().await;

                let symbols: Vec<String> = {
                    let lock = active_symbols.lock().unwrap();
                    lock.keys().cloned().collect()
                };

                for symbol in symbols {
                    let b_price = {
                        let lock = binance_prices.lock().unwrap();
                        lock.get(&symbol).cloned()
                    };
                    let c_price = {
                        let lock = coinbase_prices.lock().unwrap();
                        lock.get(&symbol).cloned()
                    };

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
                            symbol: symbol.clone(),
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
                            tracing::error!("Failed to publish price feed for {}: {:?}", symbol, e);
                        }

                        if let Ok(mut conn) = redis_client.get_multiplexed_async_connection().await {
                            if let Ok(payload) = serde_json::to_string(&tick) {
                                let _: Result<(), _> = conn.publish("price-ticks", payload).await;
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }
}
