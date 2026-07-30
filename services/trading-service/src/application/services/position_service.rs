use std::sync::Arc;
use tokio::sync::Mutex;
use async_trait::async_trait;
use chrono::Utc;
use errors::app_error::ServiceError;
use rust_decimal::Decimal;
use uuid::Uuid;
use crate::{
    domain::{
        entities::position::Position,
        repositories::position_repository::PositionRepository,
    },
    application::usecase::position_usecase::PositionUseCase,
    infrastructure::grpc::account_client::AccountGrpcClient,
};

pub struct PositionService {
    repository: Arc<dyn PositionRepository>,
    account_client: Arc<Mutex<AccountGrpcClient>>,
}

impl PositionService {
    pub fn new(
        repository: Arc<dyn PositionRepository>,
        account_client: Arc<Mutex<AccountGrpcClient>>,
    ) -> Self {
        Self {
            repository,
            account_client,
        }
    }

    fn calculate_liq_price(&self, entry_price: Decimal, margin: Decimal, size: Decimal, side: &str) -> Decimal {
        let mmr = Decimal::new(5, 3); // MMR = 0.005 (0.5%)
        let one = Decimal::ONE;
        
        if size == Decimal::ZERO {
            return Decimal::ZERO;
        }

        let margin_per_size = margin / size;

        if side == "LONG" {
            let num = entry_price - margin_per_size;
            let den = one - mmr;
            num / den
        } else {
            let num = entry_price + margin_per_size;
            let den = one + mmr;
            num / den
        }
    }
}

#[async_trait]
impl PositionUseCase for PositionService {
    async fn get_position(&self, user_id: Uuid, symbol: &str, side: &str) -> Result<Option<Position>, ServiceError> {
        let position = self.repository.find_by_user_symbol_side(user_id, symbol, side).await?;
        Ok(position)
    }

    async fn list_positions(&self, user_id: Uuid) -> Result<Vec<Position>, ServiceError> {
        let positions = self.repository.list_by_user(user_id).await?;
        Ok(positions)
    }

    async fn update_position_on_fill(
        &self,
        user_id: Uuid,
        symbol: &str,
        trade_side: &str,
        trade_price: Decimal,
        trade_qty: Decimal,
        leverage: i32,
        order_id: Uuid,
    ) -> Result<Position, ServiceError> {
        let opposite_side = if trade_side == "BUY" { "SHORT" } else { "LONG" };
        let position_side = if trade_side == "BUY" { "LONG" } else { "SHORT" };

        let mut opposite_pos = self.repository.find_by_user_symbol_side(user_id, symbol, opposite_side).await?;

        if let Some(mut existing) = opposite_pos {
            if existing.size > trade_qty {
                let pnl = if existing.side == "LONG" {
                    trade_qty * (trade_price - existing.entry_price)
                } else {
                    trade_qty * (existing.entry_price - trade_price)
                };

                let released_margin = (trade_qty / existing.size) * existing.margin;

                existing.size -= trade_qty;
                existing.margin -= released_margin;
                existing.realized_pnl += pnl;
                existing.liquidation_price = self.calculate_liq_price(existing.entry_price, existing.margin, existing.size, &existing.side);
                existing.updated_at = Utc::now();

                let updated = self.repository.update(existing).await?;

                let mut client = self.account_client.lock().await;
                let _ = client.release_margin(user_id.to_string(), released_margin.to_string(), order_id.to_string()).await;
                let _ = client.adjust_margin(user_id.to_string(), pnl.to_string(), "PNL".to_string()).await;

                return Ok(updated);
            } else {
                let pnl = if existing.side == "LONG" {
                    existing.size * (trade_price - existing.entry_price)
                } else {
                    existing.size * (existing.entry_price - trade_price)
                };

                let remaining_qty = trade_qty - existing.size;
                let released_margin = existing.margin;

                existing.size = Decimal::ZERO;
                existing.margin = Decimal::ZERO;
                existing.realized_pnl += pnl;
                existing.liquidation_price = Decimal::ZERO;
                existing.updated_at = Utc::now();
                self.repository.update(existing).await?;

                let mut client = self.account_client.lock().await;
                let _ = client.release_margin(user_id.to_string(), released_margin.to_string(), order_id.to_string()).await;
                let _ = client.adjust_margin(user_id.to_string(), pnl.to_string(), "PNL".to_string()).await;

                if remaining_qty > Decimal::ZERO {
                    let new_margin = (remaining_qty * trade_price) / Decimal::from(leverage);
                    let liq_price = self.calculate_liq_price(trade_price, new_margin, remaining_qty, position_side);

                    let new_pos = Position {
                        id: Uuid::new_v4(),
                        user_id,
                        symbol: symbol.to_string(),
                        side: position_side.to_string(),
                        size: remaining_qty,
                        entry_price: trade_price,
                        margin: new_margin,
                        leverage,
                        liquidation_price: liq_price,
                        unrealized_pnl: Decimal::ZERO,
                        realized_pnl: Decimal::ZERO,
                        margin_mode: "ISOLATED".to_string(),
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                    };

                    let created = self.repository.create(new_pos).await?;
                    let _ = client.lock_margin(user_id.to_string(), new_margin.to_string(), order_id.to_string()).await;
                    return Ok(created);
                }

                return Ok(self.repository.find_by_user_symbol_side(user_id, symbol, opposite_side).await?.unwrap());
            }
        }

        let mut existing_pos = self.repository.find_by_user_symbol_side(user_id, symbol, position_side).await?;

        if let Some(mut existing) = existing_pos {
            let new_size = existing.size + trade_qty;
            let new_entry = ((existing.size * existing.entry_price) + (trade_qty * trade_price)) / new_size;
            let added_margin = (trade_qty * trade_price) / Decimal::from(leverage);
            
            existing.size = new_size;
            existing.entry_price = new_entry;
            existing.margin += added_margin;
            existing.liquidation_price = self.calculate_liq_price(existing.entry_price, existing.margin, existing.size, &existing.side);
            existing.updated_at = Utc::now();

            let updated = self.repository.update(existing).await?;

            let mut client = self.account_client.lock().await;
            let _ = client.lock_margin(user_id.to_string(), added_margin.to_string(), order_id.to_string()).await;

            Ok(updated)
        } else {
            let new_margin = (trade_qty * trade_price) / Decimal::from(leverage);
            let liq_price = self.calculate_liq_price(trade_price, new_margin, trade_qty, position_side);

            let new_pos = Position {
                id: Uuid::new_v4(),
                user_id,
                symbol: symbol.to_string(),
                side: position_side.to_string(),
                size: trade_qty,
                entry_price: trade_price,
                margin: new_margin,
                leverage,
                liquidation_price: liq_price,
                unrealized_pnl: Decimal::ZERO,
                realized_pnl: Decimal::ZERO,
                margin_mode: "ISOLATED".to_string(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            };

            let created = self.repository.create(new_pos).await?;

            let mut client = self.account_client.lock().await;
            let _ = client.lock_margin(user_id.to_string(), new_margin.to_string(), order_id.to_string()).await;

            Ok(created)
        }
    }
}
