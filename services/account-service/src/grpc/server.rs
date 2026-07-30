use std::sync::Arc;
use std::str::FromStr;
use tonic::{Request, Response, Status};
use uuid::Uuid;
use rust_decimal::Decimal;
use proto::account::{
    account_service_server::AccountService as GrpcAccountService,
    GetBalanceRequest, GetBalanceResponse,
    LockMarginRequest, LockMarginResponse,
    ReleaseMarginRequest, ReleaseMarginResponse,
    AdjustMarginRequest, AdjustMarginResponse,
};
use crate::application::usecase::account_usecase::AccountUseCase;

pub struct AccountGrpcService {
    pub service: Arc<dyn AccountUseCase>,
}

#[tonic::async_trait]
impl GrpcAccountService for AccountGrpcService {
    async fn get_balance(
        &self,
        request: Request<GetBalanceRequest>,
    ) -> Result<Response<GetBalanceResponse>, Status> {
        let req = request.into_inner();
        let user_id = Uuid::parse_str(&req.user_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        
        let account = self.service.get_balance(user_id, &req.asset)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(GetBalanceResponse {
            available_balance: (account.balance - account.frozen).to_string(),
            locked_balance: account.frozen.to_string(),
        }))
    }

    async fn lock_margin(
        &self,
        request: Request<LockMarginRequest>,
    ) -> Result<Response<LockMarginResponse>, Status> {
        let req = request.into_inner();
        let user_id = Uuid::parse_str(&req.user_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let amount = Decimal::from_str(&req.amount)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        match self.service.lock_margin(user_id, "USDT", amount).await {
            Ok(_) => Ok(Response::new(LockMarginResponse {
                success: true,
                error_message: "".to_string(),
            })),
            Err(e) => Ok(Response::new(LockMarginResponse {
                success: false,
                error_message: e.to_string(),
            })),
        }
    }

    async fn release_margin(
        &self,
        request: Request<ReleaseMarginRequest>,
    ) -> Result<Response<ReleaseMarginResponse>, Status> {
        let req = request.into_inner();
        let user_id = Uuid::parse_str(&req.user_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let amount = Decimal::from_str(&req.amount)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        match self.service.release_margin(user_id, "USDT", amount).await {
            Ok(_) => Ok(Response::new(ReleaseMarginResponse {
                success: true,
            })),
            Err(_) => Ok(Response::new(ReleaseMarginResponse {
                success: false,
            })),
        }
    }

    async fn adjust_margin(
        &self,
        request: Request<AdjustMarginRequest>,
    ) -> Result<Response<AdjustMarginResponse>, Status> {
        let req = request.into_inner();
        let user_id = Uuid::parse_str(&req.user_id)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let amount = Decimal::from_str(&req.amount)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let account = self.service.adjust_margin(user_id, "USDT", amount, &req.adjustment_type)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(AdjustMarginResponse {
            new_balance: account.balance.to_string(),
        }))
    }
}
