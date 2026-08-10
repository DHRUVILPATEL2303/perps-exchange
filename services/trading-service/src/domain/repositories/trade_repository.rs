use async_trait::async_trait;
use errors::app_error::RepositoryError;
use uuid::Uuid;
use crate::domain::entities::trade::Trade;

#[async_trait]
pub trait TradeRepository: Send + Sync {
    async fn create(&self, trade: Trade) -> Result<Trade, RepositoryError>;
    async fn list_by_user(&self, user_id: Uuid, page: Option<i32>, limit: Option<i32>) -> Result<Vec<Trade>, RepositoryError>;
}
