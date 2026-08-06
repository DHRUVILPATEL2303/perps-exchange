use async_trait::async_trait;
use uuid::Uuid;
use rust_decimal::Decimal;
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct OrderEntity {
    pub id: Uuid,
    pub user_id: Uuid,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub status: String,
    pub leverage: i32,
    pub trigger_price: Option<Decimal>,
    pub trigger_direction: Option<String>,
}

#[async_trait]
pub trait OrderRepository: Send + Sync {
    async fn create(&self, order: OrderEntity) -> Result<OrderEntity>;
    async fn update_status(&self, id: Uuid, status: &str) -> Result<()>;
    async fn list_open_by_user(&self, user_id: Uuid) -> Result<Vec<OrderEntity>>;
}
