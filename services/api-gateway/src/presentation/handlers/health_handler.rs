use actix_web::{HttpResponse, web};
use errors::AppError;

use crate::{state::AppState};

pub async fn health(
    state : web::Data<AppState>
) -> Result<HttpResponse,AppError> {
    let health_response = state.health_service.health().await;
    Ok(
        HttpResponse::Ok().json(health_response.clone())
    )
}