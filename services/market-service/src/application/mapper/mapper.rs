use crate::{
    application::dto::response::market_response::MarketResponse,
    domain::entities::market::Market,
};

impl From<Market> for MarketResponse {
    fn from(market: Market) -> Self {
        Self {
            id: market.id,
            symbol: market.symbol,
            base_asset: market.base_asset,
            quote_asset: market.quote_asset,
            tick_size: market.tick_size.to_string(),
            lot_size: market.lot_size.to_string(),
            min_qty: market.min_qty.to_string(),
            max_leverage: market.max_leverage as u16,
            status: market.status.to_string(),
            created_at: market.created_at,
        }
    }
}