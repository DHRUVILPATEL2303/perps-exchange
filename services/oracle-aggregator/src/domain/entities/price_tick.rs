use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceTick {
    pub symbol: String,
    pub index_price: Decimal,
    pub mark_price: Decimal,
    pub timestamp: u64,
}
