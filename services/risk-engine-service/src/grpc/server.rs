use std::str::FromStr;
use tonic::{Request, Response, Status};
use rust_decimal::Decimal;
use proto::risk::{
    risk_service_server::RiskService as GrpcRiskService,
    CheckOrderMarginRequest, CheckOrderMarginResponse,
};
use crate::infrastructure::grpc::account_client::AccountGrpcClient;

pub struct RiskGrpcService {
    pub account_client: AccountGrpcClient,
}

#[tonic::async_trait]
impl GrpcRiskService for RiskGrpcService {
    async fn check_order_margin(
        &self,
        request: Request<CheckOrderMarginRequest>,
    ) -> Result<Response<CheckOrderMarginResponse>, Status> {
        let req = request.into_inner();

        let qty = Decimal::from_str(&req.quantity)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let price = Decimal::from_str(&req.price)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let leverage = Decimal::from(req.leverage);

        let required_margin = (qty * price) / leverage;

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
