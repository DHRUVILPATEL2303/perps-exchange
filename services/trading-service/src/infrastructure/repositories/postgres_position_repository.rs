use async_trait::async_trait;
use errors::app_error::RepositoryError;
use sqlx::{Pool, Postgres};
use uuid::Uuid;
use crate::domain::entities::position::Position;
use crate::domain::repositories::position_repository::PositionRepository;

pub struct PostgresPositionRepository {
    pool: Pool<Postgres>,
}

impl PostgresPositionRepository {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PositionRepository for PostgresPositionRepository {
    async fn create(&self, position: Position) -> Result<Position, RepositoryError> {
        let created = sqlx::query_as::<_, Position>(
            r#"
            INSERT INTO positions (
                id, user_id, symbol, side, size, entry_price, margin, leverage,
                liquidation_price, unrealized_pnl, realized_pnl, margin_mode, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            RETURNING id, user_id, symbol, side, size, entry_price, margin, leverage,
                      liquidation_price, unrealized_pnl, realized_pnl, margin_mode, created_at, updated_at
            "#,
        )
        .bind(position.id)
        .bind(position.user_id)
        .bind(position.symbol)
        .bind(position.side)
        .bind(position.size)
        .bind(position.entry_price)
        .bind(position.margin)
        .bind(position.leverage)
        .bind(position.liquidation_price)
        .bind(position.unrealized_pnl)
        .bind(position.realized_pnl)
        .bind(position.margin_mode)
        .bind(position.created_at)
        .bind(position.updated_at)
        .fetch_one(&self.pool)
        .await?;

        Ok(created)
    }

    async fn find_by_user_symbol_side(&self, user_id: Uuid, symbol: &str, side: &str) -> Result<Option<Position>, RepositoryError> {
        let position = sqlx::query_as::<_, Position>(
            r#"
            SELECT id, user_id, symbol, side, size, entry_price, margin, leverage,
                   liquidation_price, unrealized_pnl, realized_pnl, margin_mode, created_at, updated_at
            FROM positions
            WHERE user_id = $1 AND symbol = $2 AND side = $3
            "#,
        )
        .bind(user_id)
        .bind(symbol)
        .bind(side)
        .fetch_optional(&self.pool)
        .await?;

        Ok(position)
    }

    async fn update(&self, position: Position) -> Result<Position, RepositoryError> {
        let updated = sqlx::query_as::<_, Position>(
            r#"
            UPDATE positions
            SET size = $1, entry_price = $2, margin = $3, leverage = $4,
                liquidation_price = $5, unrealized_pnl = $6, realized_pnl = $7, updated_at = $8
            WHERE id = $9
            RETURNING id, user_id, symbol, side, size, entry_price, margin, leverage,
                      liquidation_price, unrealized_pnl, realized_pnl, margin_mode, created_at, updated_at
            "#,
        )
        .bind(position.size)
        .bind(position.entry_price)
        .bind(position.margin)
        .bind(position.leverage)
        .bind(position.liquidation_price)
        .bind(position.unrealized_pnl)
        .bind(position.realized_pnl)
        .bind(position.updated_at)
        .bind(position.id)
        .fetch_one(&self.pool)
        .await?;

        Ok(updated)
    }

    async fn list_by_user(&self, user_id: Uuid) -> Result<Vec<Position>, RepositoryError> {
        let positions = sqlx::query_as::<_, Position>(
            r#"
            SELECT id, user_id, symbol, side, size, entry_price, margin, leverage,
                   liquidation_price, unrealized_pnl, realized_pnl, margin_mode, created_at, updated_at
            FROM positions
            WHERE user_id = $1
            "#,
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(positions)
    }
}
