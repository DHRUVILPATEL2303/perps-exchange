use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Position {
    pub id: Uuid,
    pub user_id: Uuid,
    pub symbol: String,
    pub side: String,
    pub size: Decimal,
    pub entry_price: Decimal,
    pub margin: Decimal,
    pub leverage: i32,
    pub liquidation_price: Decimal,
    pub unrealized_pnl: Decimal,
    pub realized_pnl: Decimal,
    pub margin_mode: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
