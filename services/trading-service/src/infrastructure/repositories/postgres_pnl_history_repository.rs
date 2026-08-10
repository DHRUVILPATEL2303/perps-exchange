use crate::domain::entities::pnl_history::PnlHistory;
use crate::domain::repositories::pnl_history_repository::PnlHistoryRepository;
use async_trait::async_trait;
use errors::app_error::RepositoryError;
use sqlx::{Pool, Postgres};
use uuid::Uuid;

pub struct PostgresPnlHistoryRepository {
    pool: Pool<Postgres>,
}

impl PostgresPnlHistoryRepository {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PnlHistoryRepository for PostgresPnlHistoryRepository {
    async fn create(&self, entry: PnlHistory) -> Result<PnlHistory, RepositoryError> {
        let created = sqlx::query_as::<_, PnlHistory>(
            r#"
            INSERT INTO realized_pnl_history (id, user_id, symbol, side, qty, entry_price, exit_price, realized_pnl, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id, user_id, symbol, side, qty, entry_price, exit_price, realized_pnl, created_at
            "#,
        )
        .bind(entry.id)
        .bind(entry.user_id)
        .bind(entry.symbol)
        .bind(entry.side)
        .bind(entry.qty)
        .bind(entry.entry_price)
        .bind(entry.exit_price)
        .bind(entry.realized_pnl)
        .bind(entry.created_at)
        .fetch_one(&self.pool)
        .await?;

        Ok(created)
    }

    async fn list_by_user(
        &self,
        user_id: Uuid,
        page: Option<i32>,
        limit: Option<i32>,
    ) -> Result<Vec<PnlHistory>, RepositoryError> {
        let page = page.unwrap_or(1).max(1);
        let limit = limit.unwrap_or(50).max(1).min(100);
        let offset = (page - 1) * limit;

        let history = sqlx::query_as::<_, PnlHistory>(
            r#"
            SELECT id, user_id, symbol, side, qty, entry_price, exit_price, realized_pnl, created_at
            FROM realized_pnl_history
            WHERE user_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(user_id)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(history)
    }
}
