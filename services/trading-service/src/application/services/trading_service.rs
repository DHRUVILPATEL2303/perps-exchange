use std::sync::Arc;

use anyhow::{Result, bail};

use crate::{domain::entities::market::Market, infrastructure::cache::market_cache::MarketCache};

#[derive(Clone)]
pub struct TradingService {
    market_cache: Arc<MarketCache>,
}

impl TradingService {
    pub fn new(market_cache: Arc<MarketCache>) -> Self {
        Self { market_cache }
    }

    pub async fn get_market(&self, symbol: &str) -> Result<Market> {
        match self.market_cache.get(symbol).await {
            Some(market) => Ok(market),
            None => bail!("Market {} not found", symbol),
        }
    }

    pub async fn market_exists(&self, symbol: &str) -> bool {
        self.market_cache.contains(symbol).await
    }

    pub async fn markets(&self) -> Vec<Market> {
        self.market_cache.list().await
    }
}
