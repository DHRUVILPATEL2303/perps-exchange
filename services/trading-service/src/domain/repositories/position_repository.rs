use async_trait::async_trait;
use errors::app_error::RepositoryError;
use uuid::Uuid;
use crate::domain::entities::position::Position;

#[async_trait]
pub trait PositionRepository: Send + Sync {
    async fn create(&self, position: Position) -> Result<Position, RepositoryError>;
    async fn find_by_user_symbol_side(&self, user_id: Uuid, symbol: &str, side: &str) -> Result<Option<Position>, RepositoryError>;
    async fn update(&self, position: Position) -> Result<Position, RepositoryError>;
    async fn list_by_user(&self, user_id: Uuid) -> Result<Vec<Position>, RepositoryError>;
}
