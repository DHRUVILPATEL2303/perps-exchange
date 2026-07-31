use std::sync::{Arc, Mutex};
use rust_decimal::Decimal;

#[derive(Clone, Default)]
pub struct PriceTracker {
    spot_price: Arc<Mutex<Option<Decimal>>>,
    perp_price: Arc<Mutex<Option<Decimal>>>,
}

impl PriceTracker {
    pub fn new() -> Self {
        Self {
            spot_price: Arc::new(Mutex::new(None)),
            perp_price: Arc::new(Mutex::new(None)),
        }
    }

    pub fn set_spot_price(&self, price: Decimal) {
        let mut lock = self.spot_price.lock().unwrap();
        *lock = Some(price);
    }

    pub fn set_perp_price(&self, price: Decimal) {
        let mut lock = self.perp_price.lock().unwrap();
        *lock = Some(price);
    }

    pub fn get_prices(&self) -> (Option<Decimal>, Option<Decimal>) {
        let spot = *self.spot_price.lock().unwrap();
        let perp = *self.perp_price.lock().unwrap();
        (spot, perp)
    }
}
