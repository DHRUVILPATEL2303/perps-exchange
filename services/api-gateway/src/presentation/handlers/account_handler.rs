use actix_web::web::{Data, Path, Json, Query};
use actix_web::HttpResponse;
use crate::state::AppState;
use proto::account::{
    GetBalanceRequest, AdjustMarginRequest, GetTransactionHistoryRequest, GetDepositAddressRequest,
    WithdrawRequest,
};
use uuid::Uuid;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct HTTPDepositRequest {
    pub user_id: Uuid,
    pub amount: String,
}

#[derive(Deserialize)]
pub struct HTTPWithdrawRequest {
    pub user_id: Uuid,
    pub amount: String,
    pub asset: String,
    pub destination_address: String,
}

#[derive(Deserialize)]
pub struct BalanceQuery {
    pub asset: Option<String>,
}

pub async fn get_balance(state: Data<AppState>, path: Path<Uuid>, query: Query<BalanceQuery>) -> HttpResponse {
    let user_id = path.into_inner();
    let mut client = state.account_client.clone();
    let asset = query.asset.clone().unwrap_or_else(|| "USDC".to_string());
    
    let req = GetBalanceRequest {
        user_id: user_id.to_string(),
        asset,
    };
    match client.get_balance(req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner()),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn deposit_funds(state: Data<AppState>, body: Json<HTTPDepositRequest>) -> HttpResponse {
    let req = body.into_inner();
    let mut client = state.account_client.clone();
    let grpc_req = AdjustMarginRequest {
        user_id: req.user_id.to_string(),
        amount: req.amount,
        adjustment_type: "DEPOSIT".to_string(),
    };
    match client.adjust_margin(grpc_req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner()),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn withdraw_funds(state: Data<AppState>, body: Json<HTTPWithdrawRequest>) -> HttpResponse {
    let req = body.into_inner();
    let mut client = state.account_client.clone();
    let grpc_req = WithdrawRequest {
        user_id: req.user_id.to_string(),
        amount: req.amount,
        asset: req.asset,
        destination_address: req.destination_address,
    };
    match client.withdraw(grpc_req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner()),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn get_transaction_history(state: Data<AppState>, path: Path<Uuid>) -> HttpResponse {
    let user_id = path.into_inner();
    let mut client = state.account_client.clone();
    let grpc_req = GetTransactionHistoryRequest {
        user_id: user_id.to_string(),
    };
    match client.get_transaction_history(grpc_req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner().transactions),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn get_deposit_address(state: Data<AppState>, path: Path<Uuid>) -> HttpResponse {
    let user_id = path.into_inner();
    let mut client = state.account_client.clone();
    let grpc_req = GetDepositAddressRequest {
        user_id: user_id.to_string(),
    };
    match client.get_deposit_address(grpc_req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner()),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

