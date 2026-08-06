use async_trait::async_trait;
use errors::app_error::ServiceError;
use rust_decimal::Decimal;
use uuid::Uuid;
use crate::domain::entities::position::Position;

#[async_trait]
pub trait PositionUseCase: Send + Sync {
    async fn get_position(&self, user_id: Uuid, symbol: &str, side: &str) -> Result<Option<Position>, ServiceError>;
    async fn list_positions(&self, user_id: Uuid) -> Result<Vec<Position>, ServiceError>;
    async fn update_position_on_fill(
        &self,
        user_id: Uuid,
        symbol: &str,
        trade_side: &str,
        trade_price: Decimal,
        trade_qty: Decimal,
        leverage: i32,
        order_id: Uuid,
    ) -> Result<Position, ServiceError>;

    async fn adjust_isolated_margin(
        &self,
        user_id: Uuid,
        symbol: &str,
        side: &str,
        amount: Decimal,
        is_add: bool,
    ) -> Result<Position, ServiceError>;
}
