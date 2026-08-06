use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone, serde::Serialize)]
pub struct Trade {
    pub id: Uuid,
    pub symbol: String,
    pub maker_order_id: Uuid,
    pub taker_order_id: Uuid,
    pub maker_user_id: Uuid,
    pub taker_user_id: Uuid,
    pub price: Decimal,
    pub quantity: Decimal,
    pub taker_side: String,
    pub executed_at: DateTime<Utc>,
    pub maker_leverage: u32,
    pub taker_leverage: u32,
}
