use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct CreateMarketRequest {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub tick_size: Decimal,
    pub lot_size: Decimal,
    pub min_qty: Decimal,
    pub max_leverage: u16,
}
