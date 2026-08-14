use crate::application::usecase::account_usecase::AccountUseCase;
use proto::account::{
    account_service_server::AccountService as GrpcAccountService, AdjustMarginRequest,
    AdjustMarginResponse, GetBalanceRequest, GetBalanceResponse, GetDepositAddressRequest,
    GetDepositAddressResponse, GetTransactionHistoryRequest, GetTransactionHistoryResponse,
    LockMarginRequest, LockMarginResponse, ReleaseMarginRequest, ReleaseMarginResponse,
    TransactionInfo, WithdrawRequest, WithdrawResponse, GetFundingHistoryRequest,
    GetFundingHistoryResponse, FundingPaymentInfo,
};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use uuid::Uuid;

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
        let user_id =
            Uuid::parse_str(&req.user_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let account = self
            .service
            .get_balance(user_id, &req.asset)
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
        let user_id =
            Uuid::parse_str(&req.user_id).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let amount =
            Decimal::from_str(&req.amount).map_err(|e| Status::invalid_argument(e.to_string()))?;

        match self.service.lock_margin(user_id, "USDC", amount).await {
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
        let user_id =
            Uuid::parse_str(&req.user_id).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let amount =
            Decimal::from_str(&req.amount).map_err(|e| Status::invalid_argument(e.to_string()))?;

        match self.service.release_margin(user_id, "USDC", amount).await {
            Ok(_) => Ok(Response::new(ReleaseMarginResponse { success: true })),
            Err(_) => Ok(Response::new(ReleaseMarginResponse { success: false })),
        }
    }

    async fn adjust_margin(
        &self,
        request: Request<AdjustMarginRequest>,
    ) -> Result<Response<AdjustMarginResponse>, Status> {
        let req = request.into_inner();
        let user_id =
            Uuid::parse_str(&req.user_id).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let amount =
            Decimal::from_str(&req.amount).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let symbol = req.symbol.clone();
        let side = req.side.clone();
        let position_size = if let Some(ref s) = req.position_size {
            Some(Decimal::from_str(s).map_err(|e| Status::invalid_argument(e.to_string()))?)
        } else {
            None
        };
        let funding_rate = if let Some(ref s) = req.funding_rate {
            Some(Decimal::from_str(s).map_err(|e| Status::invalid_argument(e.to_string()))?)
        } else {
            None
        };

        let account = self
            .service
            .adjust_margin(
                user_id,
                "USDC",
                amount,
                &req.adjustment_type,
                None,
                symbol,
                side,
                position_size,
                funding_rate,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(AdjustMarginResponse {
            new_balance: account.balance.to_string(),
        }))
    }

    async fn get_transaction_history(
        &self,
        request: Request<GetTransactionHistoryRequest>,
    ) -> Result<Response<GetTransactionHistoryResponse>, Status> {
        let req = request.into_inner();
        let user_id =
            Uuid::parse_str(&req.user_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let txs = self
            .service
            .get_transaction_history(user_id, req.page, req.limit)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let mapped = txs
            .into_iter()
            .map(|t| TransactionInfo {
                id: t.id.to_string(),
                user_id: t.user_id.to_string(),
                asset: t.asset,
                amount: t.amount.to_string(),
                transaction_type: t.transaction_type,
                status: t.status,
                tx_hash: t.tx_hash.unwrap_or_default(),
                created_at: t.created_at.to_rfc3339(),
            })
            .collect();

        Ok(Response::new(GetTransactionHistoryResponse {
            transactions: mapped,
        }))
    }

    async fn get_deposit_address(
        &self,
        request: Request<GetDepositAddressRequest>,
    ) -> Result<Response<GetDepositAddressResponse>, Status> {
        let req = request.into_inner();
        let user_id =
            Uuid::parse_str(&req.user_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let custody = self
            .service
            .get_or_create_custody_address(user_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(GetDepositAddressResponse {
            pda_address: custody.pda_address,
            usdc_ata: custody.usdc_ata,
            usdt_ata: custody.usdt_ata,
        }))
    }

    async fn withdraw(
        &self,
        request: Request<WithdrawRequest>,
    ) -> Result<Response<WithdrawResponse>, Status> {
        let req = request.into_inner();
        let user_id =
            Uuid::parse_str(&req.user_id).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let amount =
            Decimal::from_str(&req.amount).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let (tx_hash, new_balance) = self
            .service
            .withdraw_funds(user_id, &req.asset, amount, &req.destination_address)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(WithdrawResponse {
            tx_hash,
            new_balance: new_balance.to_string(),
        }))
    }

    async fn get_funding_history(
        &self,
        request: Request<GetFundingHistoryRequest>,
    ) -> Result<Response<GetFundingHistoryResponse>, Status> {
        let req = request.into_inner();
        let user_id =
            Uuid::parse_str(&req.user_id).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let payments = self
            .service
            .get_funding_history(user_id, req.page, req.limit)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let mapped = payments
            .into_iter()
            .map(|p| FundingPaymentInfo {
                id: p.id.to_string(),
                user_id: p.user_id.to_string(),
                symbol: p.symbol,
                side: p.side,
                position_size: p.position_size.to_string(),
                funding_rate: p.funding_rate.to_string(),
                amount: p.amount.to_string(),
                created_at: p.created_at.to_rfc3339(),
            })
            .collect();

        Ok(Response::new(GetFundingHistoryResponse {
            payments: mapped,
        }))
    }
}
