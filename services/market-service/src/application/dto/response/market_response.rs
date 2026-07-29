use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Serialize)]
pub struct MarketResponse {
    pub id: Uuid,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub tick_size: String,
    pub lot_size: String,
    pub min_qty: String,
    pub max_leverage: u16,
    pub status: String,
    pub created_at: DateTime<Utc>,
}