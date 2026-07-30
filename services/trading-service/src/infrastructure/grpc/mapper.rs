use std::str::FromStr;

use crate::domain::entities::market::{Market, MarketStatus};

use proto::market::GetMarketResponse;
use rust_decimal::Decimal;

impl TryFrom<GetMarketResponse> for Market {
    type Error = anyhow::Error;

    fn try_from(value: GetMarketResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,

            symbol: value.symbol,

            base_asset: value.base_asset,

            quote_asset: value.quote_asset,

            tick_size: Decimal::from_str(&value.tick_size)?,

            lot_size: Decimal::from_str(&value.lot_size)?,

            min_qty: Decimal::from_str(&value.min_qty)?,

            max_leverage: value.max_leverage,

            status: match value.status.as_str() {
                "ACTIVE" => MarketStatus::Active,
                "PAUSED" => MarketStatus::Paused,
                _ => MarketStatus::Disabled,
            },
        })
    }
}
