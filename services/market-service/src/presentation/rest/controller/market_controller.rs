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