use async_trait::async_trait;
use errors::app_error::RepositoryError;
use uuid::Uuid;
use crate::domain::entities::pnl_history::PnlHistory;

#[async_trait]
pub trait PnlHistoryRepository: Send + Sync {
    async fn create(&self, entry: PnlHistory) -> Result<PnlHistory, RepositoryError>;
    async fn list_by_user(&self, user_id: Uuid, page: Option<i32>, limit: Option<i32>) -> Result<Vec<PnlHistory>, RepositoryError>;
}
