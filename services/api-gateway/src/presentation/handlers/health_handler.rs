use actix_web::{HttpResponse, web};
use errors::AppError;

use crate::state::AppState;

pub async fn health(state: web::Data<AppState>) -> Result<HttpResponse, AppError> {

    Ok(HttpResponse::Ok().body("OK"))
}
