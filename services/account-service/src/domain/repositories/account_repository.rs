use async_trait::async_trait;
use errors::app_error::RepositoryError;
use uuid::Uuid;
use crate::domain::entities::account::Account;

#[async_trait]
pub trait AccountRepository: Send + Sync {
    async fn create(&self, account: Account) -> Result<Account, RepositoryError>;
    async fn find_by_user_and_asset(&self, user_id: Uuid, asset: &str) -> Result<Option<Account>, RepositoryError>;
    async fn update(&self, account: Account) -> Result<Account, RepositoryError>;
    async fn list_by_user(&self, user_id: Uuid) -> Result<Vec<Account>, RepositoryError>;
}
