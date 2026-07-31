use anyhow::Result;
use rust_decimal::Decimal;
use sqlx::{Pool, Postgres, Row};
use uuid::Uuid;

pub struct PositionRepository {
    pool: Pool<Postgres>,
}

impl PositionRepository {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    pub async fn update_position(
        &self,
        user_id: Uuid,
        symbol: &str,
        side: &str,
        size: Decimal,
        entry_price: Decimal,
        margin: Decimal,
        leverage: i32,
        liq_price: Decimal,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO positions (id, user_id, symbol, side, size, entry_price, margin, leverage, liquidation_price, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
            ON CONFLICT (user_id, symbol, side) DO UPDATE
            SET size = EXCLUDED.size,
                entry_price = EXCLUDED.entry_price,
                margin = EXCLUDED.margin,
                liquidation_price = EXCLUDED.liquidation_price,
                updated_at = NOW()
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(user_id)
        .bind(symbol)
        .bind(side)
        .bind(size)
        .bind(entry_price)
        .bind(margin)
        .bind(leverage)
        .bind(liq_price)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn list_active_positions(&self) -> Result<Vec<(Uuid, String, String, Decimal)>> {
        let rows = sqlx::query(
            r#"
            SELECT user_id, symbol, side, size
            FROM positions
            WHERE size > 0
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut positions = Vec::new();
        for row in rows {
            positions.push((
                row.get("user_id"),
                row.get("symbol"),
                row.get("side"),
                row.get("size"),
            ));
        }
        Ok(positions)
    }
}
