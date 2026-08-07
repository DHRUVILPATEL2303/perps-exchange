use crate::domain::entities::account::Account;
use crate::domain::entities::transaction::Transaction;
use crate::domain::entities::custody_address::CustodyAddress;
use crate::domain::repositories::account_repository::AccountRepository;
use async_trait::async_trait;
use errors::app_error::RepositoryError;
use sqlx::{Pool, Postgres};
use uuid::Uuid;

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

    async fn find_by_user_and_asset(
        &self,
        user_id: Uuid,
        asset: &str,
    ) -> Result<Option<Account>, RepositoryError> {
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

    async fn lock_margin_atomic(
        &self,
        user_id: Uuid,
        asset: &str,
        amount: rust_decimal::Decimal,
    ) -> Result<(), RepositoryError> {
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
            return Err(RepositoryError::Database(sqlx::Error::Protocol(
                "Insufficient balance".into(),
            )));
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

    async fn release_margin_atomic(
        &self,
        user_id: Uuid,
        asset: &str,
        amount: rust_decimal::Decimal,
    ) -> Result<(), RepositoryError> {
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
        tx_hash: Option<String>,
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
                if adjustment_type != "DEPOSIT" {
                    tx.rollback().await?;
                    return Err(RepositoryError::NotFound);
                }
                let new_account = sqlx::query_as::<_, Account>(
                    r#"
                    INSERT INTO accounts (id, user_id, asset, balance, frozen, created_at, updated_at)
                    VALUES (gen_random_uuid(), $1, $2, 0, 0, NOW(), NOW())
                    RETURNING id, user_id, asset, balance, frozen, created_at, updated_at
                    "#,
                )
                .bind(user_id)
                .bind(asset)
                .fetch_one(&mut *tx)
                .await?;
                new_account
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
                    return Err(RepositoryError::Database(sqlx::Error::Protocol(
                        "Insufficient balance".into(),
                    )));
                }
                account.balance -= amount;
            }
            "PNL" | "FUNDING" | "BANKRUPTCY_CLEAR" | "INSURANCE_PAYOUT" | "INSURANCE_RESCUE"
            | "CLEARANCE_FEE" => {
                account.balance += amount;
            }
            _ => {
                tx.rollback().await?;
                return Err(RepositoryError::Database(sqlx::Error::Protocol(
                    "Invalid adjustment type".into(),
                )));
            }
        }

        if adjustment_type == "DEPOSIT" || adjustment_type == "WITHDRAW" {
            let tx_type = if adjustment_type == "DEPOSIT" {
                "DEPOSIT"
            } else {
                "WITHDRAWAL"
            };
            sqlx::query(
                r#"
                INSERT INTO transactions (id, user_id, asset, amount, transaction_type, status, tx_hash, created_at)
                VALUES ($1, $2, $3, $4, $5, 'SUCCESS', $6, NOW())
                "#,
            )
            .bind(Uuid::new_v4())
            .bind(user_id)
            .bind(asset)
            .bind(amount)
            .bind(tx_type)
            .bind(tx_hash)
            .execute(&mut *tx)
            .await?;
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

    async fn create_transaction(&self, tx: Transaction) -> Result<Transaction, RepositoryError> {
        let created = sqlx::query_as::<_, Transaction>(
            r#"
            INSERT INTO transactions (id, user_id, asset, amount, transaction_type, status, tx_hash, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id, user_id, asset, amount, transaction_type, status, tx_hash, created_at
            "#
        )
        .bind(tx.id)
        .bind(tx.user_id)
        .bind(tx.asset)
        .bind(tx.amount)
        .bind(tx.transaction_type)
        .bind(tx.status)
        .bind(tx.tx_hash)
        .bind(tx.created_at)
        .fetch_one(&self.pool)
        .await?;
        Ok(created)
    }

    async fn list_transactions_by_user(
        &self,
        user_id: Uuid,
    ) -> Result<Vec<Transaction>, RepositoryError> {
        let rows = sqlx::query_as::<_, Transaction>(
            r#"
            SELECT id, user_id, asset, amount, transaction_type, status, tx_hash, created_at
            FROM transactions
            WHERE user_id = $1
            ORDER BY created_at DESC
            "#,
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn find_custody_address_by_user(&self, user_id: Uuid) -> Result<Option<CustodyAddress>, RepositoryError> {
        let row = sqlx::query_as::<_, CustodyAddress>(
            r#"
            SELECT user_id, pda_address, usdc_ata, usdt_ata
            FROM custody_addresses
            WHERE user_id = $1
            "#
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn save_custody_address(&self, custody: CustodyAddress) -> Result<CustodyAddress, RepositoryError> {
        let created = sqlx::query_as::<_, CustodyAddress>(
            r#"
            INSERT INTO custody_addresses (user_id, pda_address, usdc_ata, usdt_ata, created_at)
            VALUES ($1, $2, $3, $4, NOW())
            RETURNING user_id, pda_address, usdc_ata, usdt_ata
            "#
        )
        .bind(custody.user_id)
        .bind(custody.pda_address)
        .bind(custody.usdc_ata)
        .bind(custody.usdt_ata)
        .fetch_one(&self.pool)
        .await?;
        Ok(created)
    }
}
