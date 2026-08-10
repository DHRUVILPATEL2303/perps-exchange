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

    async fn lock_margin_atomic(&self, user_id: Uuid, asset: &str, amount: rust_decimal::Decimal) -> Result<(), RepositoryError>;
    async fn release_margin_atomic(&self, user_id: Uuid, asset: &str, amount: rust_decimal::Decimal) -> Result<(), RepositoryError>;
    async fn adjust_margin_atomic(&self, user_id: Uuid, asset: &str, amount: rust_decimal::Decimal, adjustment_type: &str, tx_hash: Option<String>) -> Result<Account, RepositoryError>;

    async fn create_transaction(&self, tx: crate::domain::entities::transaction::Transaction) -> Result<crate::domain::entities::transaction::Transaction, RepositoryError>;
    async fn list_transactions_by_user(&self, user_id: Uuid, page: Option<i32>, limit: Option<i32>) -> Result<Vec<crate::domain::entities::transaction::Transaction>, RepositoryError>;
    async fn initiate_withdrawal(&self, user_id: Uuid, asset: &str, amount: rust_decimal::Decimal) -> Result<(Account, Uuid), RepositoryError>;
    async fn update_transaction_status_and_hash(&self, id: Uuid, status: &str, tx_hash: Option<String>) -> Result<(), RepositoryError>;
    async fn revert_withdrawal(&self, user_id: Uuid, asset: &str, amount: rust_decimal::Decimal, tx_id: Uuid, error_msg: &str) -> Result<Account, RepositoryError>;

    async fn find_custody_address_by_user(&self, user_id: Uuid) -> Result<Option<crate::domain::entities::custody_address::CustodyAddress>, RepositoryError>;
    async fn save_custody_address(&self, custody: crate::domain::entities::custody_address::CustodyAddress) -> Result<crate::domain::entities::custody_address::CustodyAddress, RepositoryError>;
}
