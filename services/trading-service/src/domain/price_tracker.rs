use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use rust_decimal::Decimal;

#[derive(Clone, Default)]
pub struct PriceTracker {
    prices: Arc<RwLock<HashMap<String, Decimal>>>,
}

impl PriceTracker {
    pub fn new() -> Self {
        Self {
            prices: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn set_price(&self, symbol: String, price: Decimal) {
        self.prices.write().await.insert(symbol, price);
    }

    pub async fn get_price(&self, symbol: &str) -> Option<Decimal> {
        self.prices.read().await.get(symbol).cloned()
    }
}
