use crate::infrastructure::grpc::account_client::AccountGrpcClient;
use crate::price_tracker::price_tracker::PriceTracker;
use proto::risk::{
    CheckOrderMarginRequest, CheckOrderMarginResponse,
    risk_service_server::RiskService as GrpcRiskService,
};
use rust_decimal::Decimal;
use sqlx::{Pool, Postgres, Row};
use std::str::FromStr;
use tonic::{Request, Response, Status};
use uuid::Uuid;

pub struct RiskGrpcService {
    pub account_client: AccountGrpcClient,
    pub db_pool: Pool<Postgres>,
    pub price_tracker: PriceTracker,
}

#[tonic::async_trait]
impl GrpcRiskService for RiskGrpcService {
    async fn check_order_margin(
        &self,
        request: Request<CheckOrderMarginRequest>,
    ) -> Result<Response<CheckOrderMarginResponse>, Status> {
        let req = request.into_inner();

        let user_id =
            Uuid::parse_str(&req.user_id).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let qty = Decimal::from_str(&req.quantity)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let price =
            Decimal::from_str(&req.price).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let leverage = Decimal::from(req.leverage);

        let opposite_side = if req.side == "BUY" { "SHORT" } else { "LONG" };

        let mut opposite_size = Decimal::ZERO;
        let pos_opt = sqlx::query(
            "SELECT size FROM positions WHERE user_id = $1 AND symbol = $2 AND side = $3",
        )
        .bind(user_id)
        .bind(&req.symbol)
        .bind(opposite_side)
        .fetch_optional(&self.db_pool)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        if let Some(row) = pos_opt {
            opposite_size = row.get("size");
        }

        let net_opening_qty = if qty > opposite_size {
            qty - opposite_size
        } else {
            Decimal::ZERO
        };

        let required_margin = (net_opening_qty * price) / leverage;

        let balance_res = self
            .account_client
            .get_balance(req.user_id.clone(), "USDC".to_string())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let avail_bal = Decimal::from_str(&balance_res.available_balance)
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut total_unrealized_pnl = Decimal::ZERO;
        let active_positions = sqlx::query(
            "SELECT symbol, side, size, entry_price, margin_mode FROM positions WHERE user_id = $1 AND size > 0",
        )
        .bind(user_id)
        .fetch_all(&self.db_pool)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        for pos in active_positions {
            let symbol: String = pos.get("symbol");
            let side: String = pos.get("side");
            let size: Decimal = pos.get("size");
            let entry_price: Decimal = pos.get("entry_price");
            let pos_margin_mode: String = pos.get("margin_mode");

            if pos_margin_mode == "CROSS" {
                let mark_price = self.price_tracker.get_spot_price(&symbol).unwrap_or(entry_price);

                let u_pnl = if side == "LONG" {
                    size * (mark_price - entry_price)
                } else {
                    size * (entry_price - mark_price)
                };
                total_unrealized_pnl += u_pnl;
            }
        }

        let adjusted_avail_bal = avail_bal + total_unrealized_pnl;

        if adjusted_avail_bal >= required_margin {
            Ok(Response::new(CheckOrderMarginResponse {
                approved: true,
                required_margin: required_margin.to_string(),
                rejection_reason: None,
            }))
        } else {
            Ok(Response::new(CheckOrderMarginResponse {
                approved: false,
                required_margin: required_margin.to_string(),
                rejection_reason: Some(format!(
                    "Insufficient available margin (Available: {}, Unrealized PnL: {}, Adjusted: {})",
                    avail_bal, total_unrealized_pnl, adjusted_avail_bal
                )),
            }))
        }
    }
}
