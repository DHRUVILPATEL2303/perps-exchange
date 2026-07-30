use async_trait::async_trait;
use errors::app_error::RepositoryError;
use uuid::Uuid;

use crate::domain::entities::market::Market;

#[async_trait]
pub trait MarketRepository: Send + Sync {
    async fn create(&self, market: Market) -> Result<Market, RepositoryError>;

    async fn find_by_id(&self, id: Uuid) -> Result<Option<Market>, RepositoryError>;

    async fn find_by_symbol(&self, symbol: &str) -> Result<Option<Market>, RepositoryError>;

    async fn list(&self) -> Result<Vec<Market>, RepositoryError>;

    async fn update(&self, market: Market) -> Result<Market, RepositoryError>;

    async fn delete(&self, id: Uuid) -> Result<(), RepositoryError>;

    async fn delete_by_symbol(&self, symbol: &str) -> Result<(), RepositoryError>;
}
