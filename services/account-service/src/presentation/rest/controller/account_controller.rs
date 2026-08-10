use actix_web::web::{Data, Json, Path};
use actix_web::HttpResponse;
use rust_decimal::Decimal;
use serde::Deserialize;
use uuid::Uuid;
use crate::presentation::rest::error::ApiError;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct AdjustBalanceRequest {
    pub user_id: Uuid,
    pub asset: String,
    pub amount: Decimal,
}

pub async fn get_balance(
    state: Data<AppState>,
    path: Path<(Uuid, String)>,
) -> Result<HttpResponse, ApiError> {
    let (user_id, asset) = path.into_inner();
    let account = state.account_service.get_balance(user_id, &asset).await?;
    Ok(HttpResponse::Ok().json(account))
}

pub async fn deposit(
    state: Data<AppState>,
    request: Json<AdjustBalanceRequest>,
) -> Result<HttpResponse, ApiError> {
    let req = request.into_inner();
    let account = state.account_service.adjust_margin(req.user_id, &req.asset, req.amount, "DEPOSIT", None, None, None, None, None).await?;
    Ok(HttpResponse::Ok().json(account))
}

pub async fn withdraw(
    state: Data<AppState>,
    request: Json<AdjustBalanceRequest>,
) -> Result<HttpResponse, ApiError> {
    let req = request.into_inner();
    let account = state.account_service.adjust_margin(req.user_id, &req.asset, req.amount, "WITHDRAW", None, None, None, None, None).await?;
    Ok(HttpResponse::Ok().json(account))
}
