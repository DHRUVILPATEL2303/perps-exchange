use crate::presentation::handlers::auth_handler::AuthenticatedUser;
use crate::state::AppState;
use actix_web::HttpResponse;
use actix_web::web::{Data, Json, Path, Query};
use proto::account::{
    AdjustMarginRequest, GetBalanceRequest, GetDepositAddressRequest, GetFundingHistoryRequest,
    GetTransactionHistoryRequest, WithdrawRequest,
};
use serde::Deserialize;
use uuid::Uuid;

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

pub async fn get_balance(
    state: Data<AppState>,
    path: Path<Uuid>,
    query: Query<BalanceQuery>,
    user: AuthenticatedUser,
) -> HttpResponse {
    let user_id = path.into_inner();
    if user.user_id != user_id.to_string() {
        return HttpResponse::Forbidden().body("Access denied");
    }
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

pub async fn deposit_funds(
    state: Data<AppState>,
    body: Json<HTTPDepositRequest>,
    user: AuthenticatedUser,
) -> HttpResponse {
    let req = body.into_inner();
    if user.user_id != req.user_id.to_string() {
        return HttpResponse::Forbidden().body("Access denied");
    }
    let mut client = state.account_client.clone();
    let grpc_req = AdjustMarginRequest {
        user_id: req.user_id.to_string(),
        amount: req.amount,
        adjustment_type: "DEPOSIT".to_string(),
        ..Default::default()
    };
    match client.adjust_margin(grpc_req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner()),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn withdraw_funds(
    state: Data<AppState>,
    body: Json<HTTPWithdrawRequest>,
    user: AuthenticatedUser,
) -> HttpResponse {
    let req = body.into_inner();
    if user.user_id != req.user_id.to_string() {
        return HttpResponse::Forbidden().body("Access denied");
    }
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

#[derive(Deserialize)]
pub struct PaginationQuery {
    pub page: Option<i32>,
    pub limit: Option<i32>,
}

pub async fn get_transaction_history(
    state: Data<AppState>,
    path: Path<Uuid>,
    query: Query<PaginationQuery>,
    user: AuthenticatedUser,
) -> HttpResponse {
    let user_id = path.into_inner();
    if user.user_id != user_id.to_string() {
        return HttpResponse::Forbidden().body("Access denied");
    }
    let mut client = state.account_client.clone();
    let grpc_req = GetTransactionHistoryRequest {
        user_id: user_id.to_string(),
        page: query.page,
        limit: query.limit,
    };
    match client.get_transaction_history(grpc_req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner().transactions),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn get_deposit_address(
    state: Data<AppState>,
    path: Path<Uuid>,
    user: AuthenticatedUser,
) -> HttpResponse {
    let user_id = path.into_inner();
    if user.user_id != user_id.to_string() {
        return HttpResponse::Forbidden().body("Access denied");
    }
    let mut client = state.account_client.clone();
    let grpc_req = GetDepositAddressRequest {
        user_id: user_id.to_string(),
    };
    match client.get_deposit_address(grpc_req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner()),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub async fn get_funding_history(
    state: Data<AppState>,
    path: Path<Uuid>,
    query: Query<PaginationQuery>,
    user: AuthenticatedUser,
) -> HttpResponse {
    let user_id = path.into_inner();
    if user.user_id != user_id.to_string() {
        return HttpResponse::Forbidden().body("Access denied");
    }
    let mut client = state.account_client.clone();
    let grpc_req = GetFundingHistoryRequest {
        user_id: user_id.to_string(),
        page: query.page,
        limit: query.limit,
    };
    match client.get_funding_history(grpc_req).await {
        Ok(res) => HttpResponse::Ok().json(res.into_inner().payments),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}
