use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct FundingPayment {
    pub id: Uuid,
    pub user_id: Uuid,
    pub symbol: String,
    pub side: String,
    pub position_size: Decimal,
    pub funding_rate: Decimal,
    pub amount: Decimal,
    pub created_at: DateTime<Utc>,
}
