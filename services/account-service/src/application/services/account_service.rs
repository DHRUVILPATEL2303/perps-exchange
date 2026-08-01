use std::sync::Arc;
use async_trait::async_trait;
use chrono::Utc;
use errors::app_error::ServiceError;
use rust_decimal::Decimal;
use uuid::Uuid;
use crate::domain::entities::account::Account;
use crate::domain::repositories::account_repository::AccountRepository;
use crate::application::usecase::account_usecase::AccountUseCase;

pub struct AccountService {
    repository: Arc<dyn AccountRepository>,
}

impl AccountService {
    pub fn new(repository: Arc<dyn AccountRepository>) -> Self {
        Self { repository }
    }
}

#[async_trait]
impl AccountUseCase for AccountService {
    async fn get_balance(&self, user_id: Uuid, asset: &str) -> Result<Account, ServiceError> {
        if let Some(account) = self.repository.find_by_user_and_asset(user_id, asset).await? {
            Ok(account)
        } else {
            let new_account = Account {
                id: Uuid::new_v4(),
                user_id,
                asset: asset.to_string(),
                balance: Decimal::ZERO,
                frozen: Decimal::ZERO,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            };
            let created = self.repository.create(new_account).await?;
            Ok(created)
        }
    }

    async fn lock_margin(&self, user_id: Uuid, asset: &str, amount: Decimal) -> Result<(), ServiceError> {
        self.repository.lock_margin_atomic(user_id, asset, amount).await?;
        Ok(())
    }

    async fn release_margin(&self, user_id: Uuid, asset: &str, amount: Decimal) -> Result<(), ServiceError> {
        self.repository.release_margin_atomic(user_id, asset, amount).await?;
        Ok(())
    }

    async fn adjust_margin(&self, user_id: Uuid, asset: &str, amount: Decimal, adjustment_type: &str) -> Result<Account, ServiceError> {
        let updated = self.repository.adjust_margin_atomic(user_id, asset, amount, adjustment_type).await?;
        Ok(updated)
    }


}
