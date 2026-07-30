use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub struct Market {
    pub id: String,

    pub symbol: String,

    pub base_asset: String,

    pub quote_asset: String,

    pub tick_size: Decimal,

    pub lot_size: Decimal,

    pub min_qty: Decimal,

    pub max_leverage: u32,

    pub status: MarketStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketStatus {
    Active,
    Paused,
    Disabled,
}
