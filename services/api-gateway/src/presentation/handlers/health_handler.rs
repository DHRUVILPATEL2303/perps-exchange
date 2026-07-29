use actix_web::{HttpResponse, web};
use errors::AppError;

use crate::{application::dto::health_response::HealthResponse, state::AppState};

pub async fn health(
    state : web::Data<AppState>
) -> Result<HttpResponse,AppError> {
    Ok(
        HttpResponse::Ok()
            .json(
                HealthResponse{
                    status : "OK".to_string(),
                    service : state.config.app_name.clone(),
            })
    )
}