use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone,sqlx::FromRow,Serialize,Deserialize)]
pub struct Account {
    pub id: Uuid,
    pub user_id: Uuid,
    pub asset: String,
    pub balance: Decimal,
    pub frozen: Decimal,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
