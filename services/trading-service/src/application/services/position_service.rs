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
        repositories::{position_repository::PositionRepository, order_repository::OrderRepository},
    },
    application::usecase::position_usecase::PositionUseCase,
    infrastructure::grpc::account_client::AccountGrpcClient,
};

pub struct PositionService {
    repository: Arc<dyn PositionRepository>,
    account_client: AccountGrpcClient,
    order_repository: Arc<dyn OrderRepository>,
}

impl PositionService {
    pub fn new(
        repository: Arc<dyn PositionRepository>,
        account_client: AccountGrpcClient,
        order_repository: Arc<dyn OrderRepository>,
    ) -> Self {
        Self {
            repository,
            account_client,
            order_repository,
        }
    }

    fn calculate_liq_price(&self, entry_price: Decimal, margin: Decimal, size: Decimal, side: &str) -> Decimal {
        let mmr = Decimal::new(5, 3);
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

        let mut margin_mode = "ISOLATED".to_string();
        if let Ok(Some(order)) = self.order_repository.find_by_id(order_id).await {
            margin_mode = order.margin_mode;
        }

        let opposite_pos = self.repository.find_by_user_symbol_side(user_id, symbol, opposite_side).await?;

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

                let client = &self.account_client;
                if let Err(e) = client.release_margin(user_id.to_string(), released_margin.to_string(), order_id.to_string()).await {
                    tracing::error!("Failed to release margin: {:?}", e);
                }

                let net_return = released_margin + pnl;
                if net_return < Decimal::ZERO {
                    let deficit = net_return.abs();
                    let insurance_fund_id = "00000000-0000-0000-0000-000000000000";

                    let _ = client.adjust_margin(user_id.to_string(), (-released_margin).to_string(), "BANKRUPTCY_CLEAR".to_string()).await;
                    let _ = client.adjust_margin(insurance_fund_id.to_string(), (-deficit).to_string(), "INSURANCE_PAYOUT".to_string()).await;
                    let _ = client.adjust_margin(user_id.to_string(), deficit.to_string(), "INSURANCE_RESCUE".to_string()).await;
                    
                    tracing::warn!(user_id = %user_id, deficit = %deficit, "User went bankrupt! Insurance Fund covered the deficit.");
                } else {
                    if let Err(e) = client.adjust_margin(user_id.to_string(), pnl.to_string(), "PNL".to_string()).await {
                        tracing::error!("Failed to adjust realized PnL: {:?}", e);
                    }
                }

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

                let client = &self.account_client;
                if let Err(e) = client.release_margin(user_id.to_string(), released_margin.to_string(), order_id.to_string()).await {
                    tracing::error!("Failed to release margin: {:?}", e);
                }

                let net_return = released_margin + pnl;
                if net_return < Decimal::ZERO {
                    let deficit = net_return.abs();
                    let insurance_fund_id = "00000000-0000-0000-0000-000000000000";

                    let _ = client.adjust_margin(user_id.to_string(), (-released_margin).to_string(), "BANKRUPTCY_CLEAR".to_string()).await;
                    let _ = client.adjust_margin(insurance_fund_id.to_string(), (-deficit).to_string(), "INSURANCE_PAYOUT".to_string()).await;
                    let _ = client.adjust_margin(user_id.to_string(), deficit.to_string(), "INSURANCE_RESCUE".to_string()).await;
                    
                    tracing::warn!(user_id = %user_id, deficit = %deficit, "User went bankrupt! Insurance Fund covered the deficit.");
                } else {
                    if let Err(e) = client.adjust_margin(user_id.to_string(), pnl.to_string(), "PNL".to_string()).await {
                        tracing::error!("Failed to adjust realized PnL: {:?}", e);
                    }
                }

                if remaining_qty > Decimal::ZERO {
                    let new_margin = (remaining_qty * trade_price) / Decimal::from(leverage);
                    let liq_price = self.calculate_liq_price(trade_price, new_margin, remaining_qty, position_side);

                    let existing_pos = self.repository.find_by_user_symbol_side(user_id, symbol, position_side).await?;
                    if let Some(mut ext) = existing_pos {
                        ext.size = remaining_qty;
                        ext.entry_price = trade_price;
                        ext.margin = new_margin;
                        ext.leverage = leverage;
                        ext.liquidation_price = liq_price;
                        ext.updated_at = Utc::now();
                        let updated = self.repository.update(ext).await?;
                        return Ok(updated);
                    } else {
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
                            margin_mode: margin_mode.clone(),
                            created_at: Utc::now(),
                            updated_at: Utc::now(),
                        };
                        let created = self.repository.create(new_pos).await?;
                        return Ok(created);
                    }
                }

                return Ok(self.repository.find_by_user_symbol_side(user_id, symbol, opposite_side).await?.unwrap());
            }
        }

        let existing_pos = self.repository.find_by_user_symbol_side(user_id, symbol, position_side).await?;

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
                margin_mode: margin_mode.clone(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
            };

            let created = self.repository.create(new_pos).await?;
            Ok(created)
        }
    }

    async fn adjust_isolated_margin(
        &self,
        user_id: Uuid,
        symbol: &str,
        side: &str,
        amount: Decimal,
        is_add: bool,
    ) -> Result<Position, ServiceError> {
        let mut position = match self.repository.find_by_user_symbol_side(user_id, symbol, side).await? {
            Some(pos) => pos,
            None => return Err(ServiceError::NotFound),
        };

        if position.margin_mode != "ISOLATED" {
            return Err(ServiceError::Validation("Position is not in ISOLATED margin mode".to_string()));
        }

        if amount <= Decimal::ZERO {
            return Err(ServiceError::Validation("Adjustment amount must be positive".to_string()));
        }

        if is_add {
            let res = self.account_client.lock_margin(
                user_id.to_string(),
                amount.to_string(),
                position.id.to_string(),
            ).await.map_err(|e| ServiceError::Validation(e.to_string()))?;

            if !res.success {
                return Err(ServiceError::Validation(res.error_message));
            }

            position.margin += amount;
        } else {
            let mmr_rate = Decimal::new(5, 3); // 0.005
            let mmr_amount = position.size * position.entry_price * mmr_rate;
            let remaining_margin = position.margin - amount;
            
            if remaining_margin <= Decimal::ZERO {
                return Err(ServiceError::Validation("Cannot reduce margin below zero".to_string()));
            }

            let cushion = remaining_margin + position.unrealized_pnl;
            if cushion < mmr_amount {
                return Err(ServiceError::Validation("Reducing margin would trigger immediate liquidation".to_string()));
            }

            let res = self.account_client.release_margin(
                user_id.to_string(),
                amount.to_string(),
                position.id.to_string(),
            ).await.map_err(|e| ServiceError::Validation(e.to_string()))?;

            if !res.success {
                return Err(ServiceError::Validation("Failed to release margin".to_string()));
            }

            position.margin -= amount;
        }

        position.liquidation_price = self.calculate_liq_price(
            position.entry_price,
            position.margin,
            position.size,
            &position.side,
        );
        position.updated_at = Utc::now();

        let updated = self.repository.update(position).await?;
        Ok(updated)
    }
}
