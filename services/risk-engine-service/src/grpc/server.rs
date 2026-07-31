use std::str::FromStr;
use tonic::{Request, Response, Status};
use rust_decimal::Decimal;
use sqlx::{Pool, Postgres, Row};
use uuid::Uuid;
use proto::risk::{
    risk_service_server::RiskService as GrpcRiskService,
    CheckOrderMarginRequest, CheckOrderMarginResponse,
};
use crate::infrastructure::grpc::account_client::AccountGrpcClient;

pub struct RiskGrpcService {
    pub account_client: AccountGrpcClient,
    pub db_pool: Pool<Postgres>,
}

#[tonic::async_trait]
impl GrpcRiskService for RiskGrpcService {
    async fn check_order_margin(
        &self,
        request: Request<CheckOrderMarginRequest>,
    ) -> Result<Response<CheckOrderMarginResponse>, Status> {
        let req = request.into_inner();

        let user_id = Uuid::parse_str(&req.user_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let qty = Decimal::from_str(&req.quantity)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let price = Decimal::from_str(&req.price)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let leverage = Decimal::from(req.leverage);

        let opposite_side = if req.side == "BUY" { "SHORT" } else { "LONG" };

        let mut opposite_size = Decimal::ZERO;
        let pos_opt = sqlx::query(
            "SELECT size FROM positions WHERE user_id = $1 AND symbol = $2 AND side = $3"
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

        let balance_res = self.account_client.get_balance(req.user_id.clone(), "USDT".to_string())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let avail_bal = Decimal::from_str(&balance_res.available_balance)
            .map_err(|e| Status::internal(e.to_string()))?;

        if avail_bal >= required_margin {
            Ok(Response::new(CheckOrderMarginResponse {
                approved: true,
                required_margin: required_margin.to_string(),
                rejection_reason: None,
            }))
        } else {
            Ok(Response::new(CheckOrderMarginResponse {
                approved: false,
                required_margin: required_margin.to_string(),
                rejection_reason: Some("Insufficient available margin".to_string()),
            }))
        }
    }
}
