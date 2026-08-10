use async_trait::async_trait;
use errors::app_error::RepositoryError;
use sqlx::{Pool, Postgres};
use uuid::Uuid;
use crate::domain::entities::trade::Trade;
use crate::domain::repositories::trade_repository::TradeRepository;

pub struct PostgresTradeRepository {
    pool: Pool<Postgres>,
}

impl PostgresTradeRepository {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TradeRepository for PostgresTradeRepository {
    async fn create(&self, trade: Trade) -> Result<Trade, RepositoryError> {
        let created = sqlx::query_as::<_, Trade>(
            r#"
            INSERT INTO trades (id, order_id, user_id, symbol, side, price, quantity, fee, executed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id, order_id, user_id, symbol, side, price, quantity, fee, executed_at
            "#,
        )
        .bind(trade.id)
        .bind(trade.order_id)
        .bind(trade.user_id)
        .bind(trade.symbol)
        .bind(trade.side)
        .bind(trade.price)
        .bind(trade.quantity)
        .bind(trade.fee)
        .bind(trade.executed_at)
        .fetch_one(&self.pool)
        .await?;

        Ok(created)
    }

    async fn list_by_user(&self, user_id: Uuid, page: Option<i32>, limit: Option<i32>) -> Result<Vec<Trade>, RepositoryError> {
        let page = page.unwrap_or(1).max(1);
        let limit = limit.unwrap_or(50).max(1).min(100);
        let offset = (page - 1) * limit;

        let trades = sqlx::query_as::<_, Trade>(
            r#"
            SELECT id, order_id, user_id, symbol, side, price, quantity, fee, executed_at
            FROM trades
            WHERE user_id = $1
            ORDER BY executed_at DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(user_id)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(trades)
    }
}
