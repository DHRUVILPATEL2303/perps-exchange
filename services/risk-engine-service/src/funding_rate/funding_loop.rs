use std::time::Duration;
use rust_decimal::Decimal;
use uuid::Uuid;
use crate::state::AppState;
use crate::price_tracker::price_tracker::{self, PriceTracker};
use crate::infrastructure::repositories::postgres_position_repository::PositionRepository;

pub async fn run_funding_loop(state: AppState, tracker: PriceTracker) {
    let position_repo = PositionRepository::new(state.db.pool().clone());
    let mut interval = tokio::time::interval(Duration::from_secs(3600));

    loop {
        interval.tick().await;

        match position_repo.list_active_positions().await {
            Ok(positions) => {
                for (user_id, symbol, side, size) in positions {
                    let spot_opt = tracker.get_spot_price(&symbol);
                    let perp_opt = tracker.get_perp_price(&symbol);
                    if let (Some(spot), Some(perp)) = (spot_opt, perp_opt) {
                        if spot.is_zero() {
                            continue;
                        }
                        let base_rate = (perp - spot) / spot;
                        let funding_rate = base_rate.clamp(
                            Decimal::new(-3, 3),
                            Decimal::new(3, 3),
                        );
                        let fee = size * spot * funding_rate;
                        if fee.is_zero() {
                            continue;
                        }
                        let amount = if side == "LONG" {
                            -fee
                        } else {
                            fee
                        };
                        tracing::info!(
                            "Settling funding for user {} on {} ({}): Size = {}, Fee = {}, Adjustment = {}",
                            user_id, symbol, side, size, fee, amount
                        );
                        match state.account_client.adjust_margin(
                            user_id.to_string(),
                            amount.to_string(),
                            "FUNDING".to_string(),
                            Some(symbol.clone()),
                            Some(side.clone()),
                            Some(size.to_string()),
                            Some(funding_rate.to_string()),
                        ).await {
                            Ok(_) => {
                                tracing::info!("Funding settled successfully for user {}", user_id);
                            }
                            Err(e) => {
                                tracing::error!("Failed to adjust margin for user {}: {:?}", user_id, e);
                            }
                        }
                    } else {
                        tracing::info!("Waiting for spot and perp price ticks for {} to calculate funding rate...", symbol);
                    }
                }
            }
            Err(e) => {
                tracing::error!("Failed to query active positions: {:?}", e);
            }
        }
    }
}
