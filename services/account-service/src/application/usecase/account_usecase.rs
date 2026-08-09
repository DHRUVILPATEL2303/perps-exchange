use async_trait::async_trait;
use errors::app_error::ServiceError;
use rust_decimal::Decimal;
use uuid::Uuid;
use crate::domain::entities::account::Account;

#[async_trait]
pub trait AccountUseCase: Send + Sync {
    async fn get_balance(&self, user_id: Uuid, asset: &str) -> Result<Account, ServiceError>;
    async fn lock_margin(&self, user_id: Uuid, asset: &str, amount: Decimal) -> Result<(), ServiceError>;
    async fn release_margin(&self, user_id: Uuid, asset: &str, amount: Decimal) -> Result<(), ServiceError>;
    async fn adjust_margin(&self, user_id: Uuid, asset: &str, amount: Decimal, adjustment_type: &str, tx_hash: Option<String>) -> Result<Account, ServiceError>;
    async fn withdraw_funds(&self, user_id: Uuid, asset: &str, amount: Decimal, destination_address: &str) -> Result<(String, Decimal), ServiceError>;
    async fn get_transaction_history(&self, user_id: Uuid) -> Result<Vec<crate::domain::entities::transaction::Transaction>, ServiceError>;
    async fn get_or_create_custody_address(&self, user_id: Uuid) -> Result<crate::domain::entities::custody_address::CustodyAddress, ServiceError>;
}
