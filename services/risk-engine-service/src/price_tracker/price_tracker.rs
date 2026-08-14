use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use rust_decimal::Decimal;

#[derive(Clone, Default)]
pub struct PriceTracker {
    spot_prices: Arc<Mutex<HashMap<String, Decimal>>>,
    perp_prices: Arc<Mutex<HashMap<String, Decimal>>>,
}

impl PriceTracker {
    pub fn new() -> Self {
        Self {
            spot_prices: Arc::new(Mutex::new(HashMap::new())),
            perp_prices: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set_spot_price(&self, symbol: String, price: Decimal) {
        let mut lock = self.spot_prices.lock().unwrap();
        lock.insert(symbol, price);
    }

    pub fn set_perp_price(&self, symbol: String, price: Decimal) {
        let mut lock = self.perp_prices.lock().unwrap();
        lock.insert(symbol, price);
    }

    pub fn get_spot_price(&self, symbol: &str) -> Option<Decimal> {
        let lock = self.spot_prices.lock().unwrap();
        lock.get(symbol).cloned()
    }

    pub fn get_perp_price(&self, symbol: &str) -> Option<Decimal> {
        let lock = self.perp_prices.lock().unwrap();
        lock.get(symbol).cloned()
    }
}
