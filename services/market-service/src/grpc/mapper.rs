use proto::market::GetMarketResponse as ProtoMarketResponse;

use crate::application::dto::response::market_response::MarketResponse;

impl From<MarketResponse> for ProtoMarketResponse {
    fn from(value: MarketResponse) -> Self {
        Self {
            id: value.id.to_string(),
            symbol: value.symbol,
            base_asset: value.base_asset,
            quote_asset: value.quote_asset,
            tick_size: value.tick_size.to_string(),
            lot_size: value.lot_size.to_string(),
            min_qty: value.min_qty.to_string(),
            max_leverage: value.max_leverage as u32,
            status: value.status,
        }
    }
}
