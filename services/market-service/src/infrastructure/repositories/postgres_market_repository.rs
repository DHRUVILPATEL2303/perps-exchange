use async_trait::async_trait;
use errors::app_error::RepositoryError;
use uuid::Uuid;

use crate::{
    domain::{
        entities::market::Market,
        repositories::market_repository::MarketRepository,
    },
    
};

pub struct PostgresMarketRepository {
    pool: sqlx::PgPool,
}

impl PostgresMarketRepository {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl MarketRepository for PostgresMarketRepository {
    async fn create(
        &self,
        market: Market,
    ) -> Result<Market, RepositoryError> {
        let market = sqlx::query_as::<_, Market>(
            r#"
            INSERT INTO markets (
                id,
                symbol,
                base_asset,
                quote_asset,
                tick_size,
                lot_size,
                min_qty,
                max_leverage,
                status,
                created_at,
                updated_at
            )
            VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11
            )
            RETURNING *
            "#,
        )
        .bind(market.id)
        .bind(market.symbol)
        .bind(market.base_asset)
        .bind(market.quote_asset)
        .bind(market.tick_size)
        .bind(market.lot_size)
        .bind(market.min_qty)
        .bind(market.max_leverage)
        .bind(market.status)
        .bind(market.created_at)
        .bind(market.updated_at)
        .fetch_one(&self.pool)
        .await?;

        Ok(market)
    }

    async fn find_by_id(
        &self,
        id: Uuid,
    ) -> Result<Option<Market>, RepositoryError> {
        let market = sqlx::query_as::<_, Market>(
            r#"
            SELECT *
            FROM markets
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(market)
    }

    async fn find_by_symbol(
        &self,
        symbol: &str,
    ) -> Result<Option<Market>, RepositoryError> {
        let market = sqlx::query_as::<_, Market>(
            r#"
            SELECT *
            FROM markets
            WHERE symbol = $1
            "#,
        )
        .bind(symbol)
        .fetch_optional(&self.pool)
        .await?;

        Ok(market)
    }

    async fn list(
        &self,
    ) -> Result<Vec<Market>, RepositoryError> {
        let markets = sqlx::query_as::<_, Market>(
            r#"
            SELECT *
            FROM markets
            ORDER BY symbol
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(markets)
    }

    async fn update(
        &self,
        market: Market,
    ) -> Result<(), RepositoryError> {
        sqlx::query(
            r#"
            UPDATE markets
            SET
                symbol = $2,
                base_asset = $3,
                quote_asset = $4,
                tick_size = $5,
                lot_size = $6,
                min_qty = $7,
                max_leverage = $8,
                status = $9,
                updated_at = $10
            WHERE id = $1
            "#,
        )
        .bind(market.id)
        .bind(market.symbol)
        .bind(market.base_asset)
        .bind(market.quote_asset)
        .bind(market.tick_size)
        .bind(market.lot_size)
        .bind(market.min_qty)
        .bind(market.max_leverage)
        .bind(market.status)
        .bind(market.updated_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn delete(
        &self,
        id: Uuid,
    ) -> Result<(), RepositoryError> {
        sqlx::query(
            r#"
            DELETE FROM markets
            WHERE id = $1
            "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}