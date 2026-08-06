use async_trait::async_trait;
use sqlx::{Pool, Postgres, Row};
use uuid::Uuid;
use anyhow::Result;
use crate::domain::repositories::order_repository::{OrderEntity, OrderRepository};

pub struct PostgresOrderRepository {
    pool: Pool<Postgres>,
}

impl PostgresOrderRepository {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl OrderRepository for PostgresOrderRepository {
    async fn create(&self, order: OrderEntity) -> Result<OrderEntity> {
        sqlx::query(
            r#"
            INSERT INTO orders (id, user_id, symbol, side, order_type, price, quantity, status, leverage, trigger_price, trigger_direction, reduce_only, margin_mode, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), NOW())
            RETURNING id, user_id, symbol, side, order_type, price, quantity, status, leverage, trigger_price, trigger_direction, reduce_only, margin_mode
            "#,
        )
        .bind(order.id)
        .bind(order.user_id)
        .bind(order.symbol)
        .bind(order.side)
        .bind(order.order_type)
        .bind(order.price)
        .bind(order.quantity)
        .bind(order.status)
        .bind(order.leverage) 
        .bind(order.trigger_price)
        .bind(order.trigger_direction)
        .bind(order.reduce_only)
        .bind(order.margin_mode)
        .fetch_one(&self.pool)
        .await
        .map(|row| OrderEntity {
            id: row.get("id"),
            user_id: row.get("user_id"),
            symbol: row.get("symbol"),
            side: row.get("side"),
            order_type: row.get("order_type"),
            price: row.get("price"),
            quantity: row.get("quantity"),
            status: row.get("status"),
            leverage: row.get("leverage"), 
            trigger_price: row.get("trigger_price"),
            trigger_direction: row.get("trigger_direction"),
            reduce_only: row.get("reduce_only"),
            margin_mode: row.get("margin_mode"),
        })
        .map_err(Into::into)
    }


    async fn update_status(&self, id: Uuid, status: &str) -> Result<()> {
        sqlx::query("UPDATE orders SET status = $1, updated_at = NOW() WHERE id = $2")
            .bind(status)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_open_by_user(&self, user_id: Uuid) -> Result<Vec<OrderEntity>> {
        let rows = sqlx::query(
            r#"
            SELECT id, user_id, symbol, side, order_type, price, quantity, status, leverage, trigger_price, trigger_direction, reduce_only, margin_mode
            FROM orders
            WHERE user_id = $1 AND status = 'OPEN'
            "#,
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        let orders = rows
            .into_iter()
            .map(|row| OrderEntity {
                id: row.get("id"),
                user_id: row.get("user_id"),
                symbol: row.get("symbol"),
                side: row.get("side"),
                order_type: row.get("order_type"),
                price: row.get("price"),
                quantity: row.get("quantity"),
                status: row.get("status"),
                leverage: row.get("leverage"),
                trigger_price: row.get("trigger_price"),
                trigger_direction: row.get("trigger_direction"),
                reduce_only: row.get("reduce_only"),
                margin_mode: row.get("margin_mode"),
            })
            .collect();

        Ok(orders)
    }

    async fn find_by_id(&self, id: Uuid) -> Result<Option<OrderEntity>> {
        let row = sqlx::query(
            r#"
            SELECT id, user_id, symbol, side, order_type, price, quantity, status, leverage, trigger_price, trigger_direction, reduce_only, margin_mode
            FROM orders
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(r) = row {
            Ok(Some(OrderEntity {
                id: r.get("id"),
                user_id: r.get("user_id"),
                symbol: r.get("symbol"),
                side: r.get("side"),
                order_type: r.get("order_type"),
                price: r.get("price"),
                quantity: r.get("quantity"),
                status: r.get("status"),
                leverage: r.get("leverage"),
                trigger_price: r.get("trigger_price"),
                trigger_direction: r.get("trigger_direction"),
                reduce_only: r.get("reduce_only"),
                margin_mode: r.get("margin_mode"),
            }))
        } else {
            Ok(None)
        }
    }
}
