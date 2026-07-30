use actix_web::web::Path;
use actix_web::{
    web::Data,
    HttpResponse,
};


use crate::presentation::rest::error::ApiError;
use crate::{

    state::AppState,
};
use crate::application::usecase::market_usecase::MarketUseCase;

pub async fn list_markets(
    state: Data<AppState>,
) -> Result<HttpResponse, ApiError> {

    let markets = state
        .market_service
        .list_markets()
        .await?;

    Ok(HttpResponse::Ok().json(markets))
}


pub async fn get_market(
    state: Data<AppState>,
    path: Path<String>,
) -> Result<HttpResponse, ApiError> {
    let symbol = path.into_inner();

    let market = state
        .market_service
        .get_market(&symbol)
        .await?;

    match market {
        Some(market) => Ok(HttpResponse::Ok().json(market)),
        None => Ok(HttpResponse::NotFound().finish()),
    }
}