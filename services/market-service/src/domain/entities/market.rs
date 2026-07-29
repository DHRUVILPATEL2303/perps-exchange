use chrono::DateTime;
use chrono::Utc;
use rust_decimal::Decimal;
use sqlx::prelude::FromRow;

#[derive(Debug, Clone,FromRow)]
pub struct Market {
    pub id: uuid::Uuid,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub tick_size: Decimal,
    pub lot_size: Decimal,
    pub min_qty: Decimal,
    pub max_leverage: i32,
    pub status: String,

    pub created_at: DateTime<Utc>,

    pub updated_at: DateTime<Utc>,
}
