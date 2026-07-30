use std::sync::Arc;

use crate::{application::services::trading_service::TradingService, infrastructure::cache::market_cache::MarketCache};

#[derive(Clone)]
pub struct AppState {
    pub trading_service : Arc<TradingService>,
    pub market_cache: Arc<MarketCache>,
}