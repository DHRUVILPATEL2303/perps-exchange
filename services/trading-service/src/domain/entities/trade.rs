use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Trade {
    pub id: Uuid,
    pub order_id: Uuid,
    pub user_id: Uuid,
    pub symbol: String,
    pub side: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee: Decimal,
    pub executed_at: DateTime<Utc>,
}
