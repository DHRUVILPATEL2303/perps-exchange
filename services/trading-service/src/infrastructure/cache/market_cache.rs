use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use crate::domain::entities::market::Market;

#[derive(Clone)]
pub struct MarketCache {
    markets: Arc<RwLock<HashMap<String, Market>>>,
}

impl MarketCache {
    pub fn new() -> Self {
        Self {
            markets: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn insert(&self, market: Market) {
        self.markets
            .write()
            .await
            .insert(market.symbol.clone(), market);
    }

    pub async fn get(&self, symbol: &str) -> Option<Market> {
        self.markets.read().await.get(symbol).cloned()
    }

    pub async fn remove(&self, symbol: &str) {
        self.markets.write().await.remove(symbol);
    }

    pub async fn load<I>(&self, markets: I)
    where
        I: IntoIterator<Item = Market>,
    {
        let mut cache = self.markets.write().await;

        cache.clear();

        for market in markets {
            cache.insert(market.symbol.clone(), market);
        }
    }

    pub async fn list(&self) -> Vec<Market> {
        self.markets.read().await.values().cloned().collect()
    }

    pub async fn len(&self) -> usize {
        self.markets.read().await.len()
    }

    pub async fn contains(&self, symbol: &str) -> bool {
        self.markets.read().await.contains_key(symbol)
    }
}
