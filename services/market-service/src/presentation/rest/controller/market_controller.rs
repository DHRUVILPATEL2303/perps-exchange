use actix_web::web::{Json, Path};
use actix_web::{HttpResponse, web::Data};

use crate::application::dto::requests::create_market_request::CreateMarketRequest;
use crate::application::dto::requests::update_market_request::UpdateMarketRequest;
use crate::presentation::rest::error::ApiError;
use crate::state::AppState;

pub async fn list_markets(state: Data<AppState>) -> Result<HttpResponse, ApiError> {
    let markets = state.market_service.list_markets().await?;

    Ok(HttpResponse::Ok().json(markets))
}

pub async fn get_market(
    state: Data<AppState>,
    path: Path<String>,
) -> Result<HttpResponse, ApiError> {
    let symbol = path.into_inner();

    let market = state.market_service.get_market(&symbol).await?;

    match market {
        Some(market) => Ok(HttpResponse::Ok().json(market)),
        None => Ok(HttpResponse::NotFound().finish()),
    }
}

pub async fn create_market(
    state: Data<AppState>,
    request: Json<CreateMarketRequest>,
) -> Result<HttpResponse, ApiError> {
    let market = state
        .market_service
        .create_market(request.into_inner())
        .await?;
    Ok(HttpResponse::Created().json(market))
}

pub async fn update_market(
    state: Data<AppState>,
    path: Path<String>,
    request: Json<UpdateMarketRequest>,
) -> Result<HttpResponse, ApiError> {
    let market = state
        .market_service
        .update_market(&path.into_inner(), request.into_inner())
        .await?;

    Ok(HttpResponse::Ok().json(market))
}

pub async fn delete_market(
    state: Data<AppState>,
    path: Path<String>,
) -> Result<HttpResponse, ApiError> {
    let result = state
        .market_service
        .delete_market(&path.into_inner())
        .await?;
    Ok(HttpResponse::Ok().json(result))
}
