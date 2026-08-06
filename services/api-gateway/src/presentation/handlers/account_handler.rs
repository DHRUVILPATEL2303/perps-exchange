use actix_web::web::{Data, Path, Json};
use actix_web::HttpResponse;
use crate::state::AppState;
use proto::account::{GetBalanceRequest, AdjustMarginRequest, GetTransactionHistoryRequest};
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
}

pub async fn get_balance(state: Data<AppState>, path: Path<Uuid>) -> HttpResponse {
    let user_id = path.into_inner();
    let mut client = state.account_client.clone();
    let req = GetBalanceRequest {
        user_id: user_id.to_string(),
        asset: "USDT".to_string(),
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
    let grpc_req = AdjustMarginRequest {
        user_id: req.user_id.to_string(),
        amount: req.amount,
        adjustment_type: "WITHDRAW".to_string(),
    };
    match client.adjust_margin(grpc_req).await {
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

