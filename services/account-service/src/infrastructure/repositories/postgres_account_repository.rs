use async_trait::async_trait;
use errors::app_error::RepositoryError;
use sqlx::{Pool, Postgres};
use uuid::Uuid;
use crate::domain::entities::account::Account;
use crate::domain::repositories::account_repository::AccountRepository;

pub struct PostgresAccountRepository {
    pool: Pool<Postgres>,
}

impl PostgresAccountRepository {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl AccountRepository for PostgresAccountRepository {
    async fn create(&self, account: Account) -> Result<Account, RepositoryError> {
        let created = sqlx::query_as::<_, Account>(
            r#"
            INSERT INTO accounts (id, user_id, asset, balance, frozen, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id, user_id, asset, balance, frozen, created_at, updated_at
            "#,
        )
        .bind(account.id)
        .bind(account.user_id)
        .bind(account.asset)
        .bind(account.balance)
        .bind(account.frozen)
        .bind(account.created_at)
        .bind(account.updated_at)
        .fetch_one(&self.pool)
        .await?;

        Ok(created)
    }

    async fn find_by_user_and_asset(&self, user_id: Uuid, asset: &str) -> Result<Option<Account>, RepositoryError> {
        let account = sqlx::query_as::<_, Account>(
            r#"
            SELECT id, user_id, asset, balance, frozen, created_at, updated_at
            FROM accounts
            WHERE user_id = $1 AND asset = $2
            "#,
        )
        .bind(user_id)
        .bind(asset)
        .fetch_optional(&self.pool)
        .await?;

        Ok(account)
    }

    async fn update(&self, account: Account) -> Result<Account, RepositoryError> {
        let updated = sqlx::query_as::<_, Account>(
            r#"
            UPDATE accounts
            SET balance = $1, frozen = $2, updated_at = $3
            WHERE id = $4
            RETURNING id, user_id, asset, balance, frozen, created_at, updated_at
            "#,
        )
        .bind(account.balance)
        .bind(account.frozen)
        .bind(account.updated_at)
        .bind(account.id)
        .fetch_one(&self.pool)
        .await?;

        Ok(updated)
    }

    async fn list_by_user(&self, user_id: Uuid) -> Result<Vec<Account>, RepositoryError> {
        let accounts = sqlx::query_as::<_, Account>(
            r#"
            SELECT id, user_id, asset, balance, frozen, created_at, updated_at
            FROM accounts
            WHERE user_id = $1
            "#,
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(accounts)
    }
    
    async fn lock_margin_atomic(&self, user_id: Uuid, asset: &str, amount: rust_decimal::Decimal) -> Result<(), RepositoryError> {
        let mut tx = self.pool.begin().await?;

        let account = sqlx::query_as::<_, Account>(
            r#"
            SELECT id, user_id, asset, balance, frozen, created_at, updated_at
            FROM accounts
            WHERE user_id = $1 AND asset = $2
            FOR UPDATE
            "#,
        )
        .bind(user_id)
        .bind(asset)
        .fetch_optional(&mut *tx)
        .await?;

        let account = match account {
            Some(a) => a,
            None => {
                tx.rollback().await?;
                return Err(RepositoryError::NotFound);
            }
        };

        let available = account.balance - account.frozen;
        if available < amount {
            tx.rollback().await?;
            return Err(RepositoryError::Database(sqlx::Error::Protocol("Insufficient balance".into())));
        }

        sqlx::query(
            r#"
            UPDATE accounts
            SET frozen = frozen + $1, updated_at = NOW()
            WHERE id = $2
            "#,
        )
        .bind(amount)
        .bind(account.id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn release_margin_atomic(&self, user_id: Uuid, asset: &str, amount: rust_decimal::Decimal) -> Result<(), RepositoryError> {
        let mut tx = self.pool.begin().await?;

        
        let account = sqlx::query_as::<_, Account>(
            r#"
            SELECT id, user_id, asset, balance, frozen, created_at, updated_at
            FROM accounts
            WHERE user_id = $1 AND asset = $2
            FOR UPDATE
            "#,
        )
        .bind(user_id)
        .bind(asset)
        .fetch_optional(&mut *tx)
        .await?;

        let account = match account {
            Some(a) => a,
            None => {
                tx.rollback().await?;
                return Err(RepositoryError::NotFound);
            }
        };

        let new_frozen = if account.frozen < amount {
            rust_decimal::Decimal::ZERO
        } else {
            account.frozen - amount
        };

        sqlx::query(
            r#"
            UPDATE accounts
            SET frozen = $1, updated_at = NOW()
            WHERE id = $2
            "#,
        )
        .bind(new_frozen)
        .bind(account.id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn adjust_margin_atomic(
        &self,
        user_id: Uuid,
        asset: &str,
        amount: rust_decimal::Decimal,
        adjustment_type: &str,
    ) -> Result<Account, RepositoryError> {
        let mut tx = self.pool.begin().await?;

    
        let account = sqlx::query_as::<_, Account>(
            r#"
            SELECT id, user_id, asset, balance, frozen, created_at, updated_at
            FROM accounts
            WHERE user_id = $1 AND asset = $2
            FOR UPDATE
            "#,
        )
        .bind(user_id)
        .bind(asset)
        .fetch_optional(&mut *tx)
        .await?;

        let mut account = match account {
            Some(a) => a,
            None => {
                tx.rollback().await?;
                return Err(RepositoryError::NotFound);
            }
        };

        match adjustment_type {
            "DEPOSIT" => {
                account.balance += amount;
            }
            "WITHDRAW" => {
                let available = account.balance - account.frozen;
                if available < amount {
                    tx.rollback().await?;
                    return Err(RepositoryError::Database(sqlx::Error::Protocol("Insufficient balance".into())));
                }
                account.balance -= amount;
            }
            "PNL" | "FUNDING" => {
                account.balance += amount;
            }
            _ => {
                tx.rollback().await?;
                return Err(RepositoryError::Database(sqlx::Error::Protocol("Invalid adjustment type".into())));
            }
        }

    
        let updated = sqlx::query_as::<_, Account>(
            r#"
            UPDATE accounts
            SET balance = $1, updated_at = NOW()
            WHERE id = $2
            RETURNING id, user_id, asset, balance, frozen, created_at, updated_at
            "#,
        )
        .bind(account.balance)
        .bind(account.id)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(updated)
    }

}
