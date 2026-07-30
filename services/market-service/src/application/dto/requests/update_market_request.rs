use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateMarketRequest {
    pub tick_size: Decimal,
    pub lot_size: Decimal,
    pub min_qty: Decimal,
    pub max_leverage: u16,
    pub status: String,
}
